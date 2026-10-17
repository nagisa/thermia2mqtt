//! A Modbus TCP client that makes requests over a pool of connections.
//!
//! The heat pump takes about half a second to respond to a request, and it only deals with one
//! request at a time per connection: a request that arrives while another is still waiting for its
//! response replaces it, and the replaced one is never responded to. Different connections are
//! served independently, so requests are made in parallel by making them over different
//! connections, which are opened as there is demand for them.

use crate::registers::Space;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tracing::debug;

/// What a request was about, for error reporting purposes.
#[derive(Debug, Clone)]
pub enum Request {
    Read(Space, u16, u16),
    Write(Space, u16),
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Request::Read(space, address, count) => {
                write!(f, "reading {count} {space} register(s) starting at {address}")
            }
            Request::Write(space, address) => write!(f, "writing {space} register {address}"),
        }
    }
}

/// The code of an exception response.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ExceptionCode(pub u8);

impl ExceptionCode {
    pub const ILLEGAL_DATA_ADDRESS: ExceptionCode = ExceptionCode(2);
}

impl std::fmt::Debug for ExceptionCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.0 {
            1 => "IllegalFunction",
            2 => "IllegalDataAddress",
            3 => "IllegalDataValue",
            4 => "ServerDeviceFailure",
            5 => "Acknowledge",
            6 => "ServerDeviceBusy",
            8 => "MemoryParityError",
            10 => "GatewayPathUnavailable",
            11 => "GatewayTargetDeviceFailedToRespond",
            code => return write!(f, "Unknown({code})"),
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("could not connect to the heat pump")]
    Connect(#[source] std::io::Error),
    #[error("communication with the heat pump failed")]
    Transport(#[source] std::io::Error),
    #[error("the heat pump did not respond in time")]
    Timeout,
    #[error("the heat pump responded with an exception ({1:?}) to {0}")]
    Exception(Request, ExceptionCode),
}

/// A connection to the heat pump. Its slot in the pool is freed once it is dropped.
struct Connection {
    stream: TcpStream,
    next_transaction: u16,
    /// Has completed an exchange.
    used: bool,
    _slot: OwnedSemaphorePermit,
}

impl Connection {
    async fn open(
        address: SocketAddr,
        timeout: Duration,
        slot: OwnedSemaphorePermit,
    ) -> std::io::Result<Connection> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(address))
            .await
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))??;
        stream.set_nodelay(true)?;
        Ok(Connection { stream, next_transaction: 0, used: false, _slot: slot })
    }

    /// Send a request and wait for the response. Returns its PDU (function code and what follows.)
    ///
    /// The connection is not fit for another request if this fails or is not waited for until the
    /// end.
    async fn exchange(
        &mut self,
        unit_id: u8,
        function: u8,
        args: [u16; 2],
    ) -> std::io::Result<Vec<u8>> {
        let transaction = self.next_transaction;
        self.next_transaction = transaction.wrapping_add(1);
        let mut frame = Vec::with_capacity(12);
        frame.extend(transaction.to_be_bytes());
        frame.extend([0, 0, 0, 6, unit_id, function]);
        frame.extend(args[0].to_be_bytes());
        frame.extend(args[1].to_be_bytes());
        self.stream.write_all(&frame).await?;

        let mut header = [0u8; 7];
        self.stream.read_exact(&mut header).await?;
        let responded = u16::from_be_bytes([header[0], header[1]]);
        let protocol = u16::from_be_bytes([header[2], header[3]]);
        // The length covers the unit id, which is a part of the header. The PDU is no shorter than
        // a function code with an exception code and no longer than the protocol allows.
        let length = usize::from(u16::from_be_bytes([header[4], header[5]]));
        if responded != transaction || protocol != 0 || !(3..=254).contains(&length) {
            let message = format!("unexpected frame header {header:02x?}");
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, message));
        }
        let mut pdu = vec![0; length - 1];
        self.stream.read_exact(&mut pdu).await?;
        Ok(pdu)
    }
}

/// The connections that are not in use, and what has become of a connection that could not be
/// opened.
#[derive(Default)]
struct Idle {
    /// The most recently used connection last. Those used less recently are not needed as much.
    connections: Vec<(Connection, Instant)>,
    failed: Option<std::io::Error>,
}

#[derive(Default)]
struct Pool {
    idle: Mutex<Idle>,
    changed: Notify,
}

impl Pool {
    fn put(&self, connection: std::io::Result<Connection>) {
        let mut idle = self.idle.lock().unwrap_or_else(|e| e.into_inner());
        match connection {
            Ok(connection) => idle.connections.push((connection, Instant::now())),
            Err(e) => idle.failed = Some(e),
        }
        self.changed.notify_one();
    }

    /// The most recently used idle connection. Those that have not been used for `close_after` are
    /// closed.
    fn take(&self, close_after: Duration) -> std::io::Result<Option<Connection>> {
        let mut idle = self.idle.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = idle.failed.take() {
            return Err(e);
        }
        let unused = idle.connections.partition_point(|(_, used)| used.elapsed() > close_after);
        if unused > 0 {
            debug!(count = unused, "closing the connections that are no longer needed");
            idle.connections.drain(..unused);
        }
        Ok(idle.connections.pop().map(|(connection, _)| connection))
    }
}

pub struct Client {
    address: SocketAddr,
    unit_id: u8,
    timeout: Duration,
    max_connections: usize,
    close_after: Duration,
    /// One for each connection that is open or being opened.
    slots: Arc<Semaphore>,
    pool: Arc<Pool>,
    /// The requests wait for a connection in line, the first one holding this.
    turn: tokio::sync::Mutex<()>,
}

impl Client {
    /// Connections are opened when there are requests waiting for one, up to `max_connections` of
    /// them. Connections that have not been used for `close_after` are closed.
    pub fn new(
        address: SocketAddr,
        unit_id: u8,
        timeout: Duration,
        max_connections: usize,
        close_after: Duration,
    ) -> Client {
        let slots = Arc::new(Semaphore::new(max_connections));
        let pool = Arc::new(Pool::default());
        let turn = tokio::sync::Mutex::new(());
        Client { address, unit_id, timeout, max_connections, close_after, slots, pool, turn }
    }

    fn release(&self, connection: Connection) {
        self.pool.put(Ok(connection));
    }

    /// Get a connection: an idle one, or whichever of the connections that become idle and the one
    /// that is opened for the occasion comes first.
    async fn acquire(&self) -> std::io::Result<Connection> {
        let _turn = self.turn.lock().await;
        let mut opening = false;
        loop {
            if let Some(connection) = self.pool.take(self.close_after)? {
                return Ok(connection);
            }
            tokio::select! {
                biased;
                () = self.pool.changed.notified() => {}
                slot = Arc::clone(&self.slots).acquire_owned(), if !opening => {
                    opening = true;
                    let slot = slot.expect("the semaphore is never closed");
                    let open = self.max_connections - self.slots.available_permits();
                    debug!(open, "opening another connection to the heat pump");
                    let (address, timeout) = (self.address, self.timeout);
                    let pool = Arc::clone(&self.pool);
                    // Nothing is cancelled if another connection gets used first. This one joins
                    // the idle ones when it is ready.
                    tokio::spawn(async move {
                        pool.put(Connection::open(address, timeout, slot).await)
                    });
                }
            }
        }
    }

    /// Read out registers. Bits are returned as 0 and 1.
    pub async fn read(&self, space: Space, start: u16, count: u16) -> Result<Vec<u16>, Error> {
        let function = match space {
            Space::Coil => 1,
            Space::DiscreteInput => 2,
            Space::Holding => 3,
            Space::Input => 4,
        };
        // Bits come packed into bytes, with a byte count in front.
        let bytes = if space.is_bit() { count.div_ceil(8) } else { count * 2 };
        let request = Request::Read(space, start, count);
        let data = self
            .transact(&request, function, [start, count], |data| {
                data.len() == 1 + usize::from(bytes) && u16::from(data[0]) == bytes
            })
            .await?;
        let data = &data[1..];
        Ok(if space.is_bit() {
            (0..usize::from(count)).map(|i| u16::from(data[i / 8] >> (i % 8) & 1)).collect()
        } else {
            data.chunks_exact(2).map(|w| u16::from_be_bytes([w[0], w[1]])).collect()
        })
    }

    pub async fn write_coil(&self, address: u16, on: bool) -> Result<(), Error> {
        let request = Request::Write(Space::Coil, address);
        let value = if on { 0xFF00 } else { 0 };
        self.transact(&request, 5, [address, value], |data| data.len() == 4).await.map(drop)
    }

    pub async fn write_register(&self, address: u16, word: u16) -> Result<(), Error> {
        let request = Request::Write(Space::Holding, address);
        self.transact(&request, 6, [address, word], |data| data.len() == 4).await.map(drop)
    }

    /// Make the request and wait for its response. Returns the response without its function code,
    /// provided that `valid` has found it to be in order.
    ///
    /// A connection is only used again if the heat pump has responded properly, which includes
    /// responding with an exception.
    ///
    /// The heat pump may close connections that have been idle for a while. This is found out by
    /// failing to use one, so a request that fails that way on a connection that has been used
    /// before is made again on another connection, for as long as there are connections like that.
    /// A request that fails on a connection that was opened for it is not made again. That is fine
    /// for the requests that are made here: they are reads and writes of single registers.
    async fn transact(
        &self,
        request: &Request,
        function: u8,
        args: [u16; 2],
        valid: impl FnOnce(&[u8]) -> bool,
    ) -> Result<Vec<u8>, Error> {
        let (mut connection, started, mut pdu) = loop {
            let mut connection = self.acquire().await.map_err(Error::Connect)?;
            let started = Instant::now();
            let exchange = connection.exchange(self.unit_id, function, args);
            match tokio::time::timeout(self.timeout, exchange).await {
                Ok(Ok(pdu)) => break (connection, started, pdu),
                Ok(Err(e)) if connection.used && e.kind() != std::io::ErrorKind::InvalidData => {
                    debug!(%request, error = %e, "the connection is gone, trying another one");
                }
                Ok(Err(e)) => return Err(Error::Transport(e)),
                Err(_) => return Err(Error::Timeout),
            }
        };
        connection.used = true;
        debug!(%request, elapsed = ?started.elapsed(), "request completed");
        let code = pdu.remove(0);
        if code == function | 0x80 {
            self.release(connection);
            Err(Error::Exception(request.clone(), ExceptionCode(pdu[0])))
        } else if code == function && valid(&pdu) {
            self.release(connection);
            Ok(pdu)
        } else {
            Err(unexpected(request))
        }
    }
}

fn unexpected(request: &Request) -> Error {
    let message = format!("unexpected response to {request}");
    Error::Transport(std::io::Error::new(std::io::ErrorKind::InvalidData, message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    /// A server that responds to every request as `respond` says, given the number of the request
    /// (counting those of all of the connections) and the request itself. Returns how many
    /// connections it has accepted.
    async fn server(
        respond: impl Fn(usize, &[u8; 12]) -> Vec<u8> + Send + Sync + 'static,
    ) -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted, requests, respond) =
            (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)), Arc::new(respond));
        let seen = Arc::clone(&accepted);
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                seen.fetch_add(1, SeqCst);
                let (requests, respond) = (Arc::clone(&requests), Arc::clone(&respond));
                tokio::spawn(async move {
                    let mut request = [0; 12];
                    while socket.read_exact(&mut request).await.is_ok() {
                        let response = respond(requests.fetch_add(1, SeqCst), &request);
                        if socket.write_all(&response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (address, accepted)
    }

    fn response(request: &[u8; 12], transaction: u16, pdu: &[u8]) -> Vec<u8> {
        let mut frame = Vec::from(transaction.to_be_bytes());
        frame.extend([0, 0, 0, pdu.len() as u8 + 1, request[6]]);
        frame.extend(pdu);
        frame
    }

    #[tokio::test]
    async fn bad_responses_cost_the_connection_and_exceptions_do_not() {
        let transaction = |request: &[u8; 12]| u16::from_be_bytes([request[0], request[1]]);
        let (address, accepted) = server(move |n, request| match n {
            0 => vec![0; 7],
            1 => response(request, transaction(request).wrapping_add(1), &[4, 2, 0, 7]),
            // The function code of a response to some other request.
            2 => response(request, transaction(request), &[3, 2, 0, 7]),
            3 => response(request, transaction(request), &[4 | 0x80, 2]),
            _ => response(request, transaction(request), &[4, 2, 0, 7]),
        })
        .await;
        let client = Client::new(address, 1, Duration::from_secs(2), 1, Duration::from_secs(60));

        for _ in 0..3 {
            let failed = client.read(Space::Input, 1, 1).await;
            assert!(matches!(failed, Err(Error::Transport(_))), "{failed:?}");
        }
        // Every one of the failed requests has cost the connection that it was made on.
        assert_eq!(accepted.load(SeqCst), 3);
        let rejected = client.read(Space::Input, 1, 1).await;
        assert!(
            matches!(rejected, Err(Error::Exception(_, ExceptionCode::ILLEGAL_DATA_ADDRESS))),
            "{rejected:?}"
        );
        assert_eq!(client.read(Space::Input, 1, 1).await.unwrap(), [7]);
        assert_eq!(client.read(Space::Input, 1, 1).await.unwrap(), [7]);
        // The exception has not cost the connection.
        assert_eq!(accepted.load(SeqCst), 4);
    }

    #[tokio::test]
    async fn a_bad_response_on_a_used_connection_is_not_retried() {
        let transaction = |request: &[u8; 12]| u16::from_be_bytes([request[0], request[1]]);
        let (address, accepted) = server(move |n, request| match n {
            1 => vec![0; 7],
            _ => response(request, transaction(request), &[4, 2, 0, 7]),
        })
        .await;
        let client = Client::new(address, 1, Duration::from_secs(2), 1, Duration::from_secs(60));

        assert_eq!(client.read(Space::Input, 1, 1).await.unwrap(), [7]);
        let failed = client.read(Space::Input, 1, 1).await;
        assert!(matches!(failed, Err(Error::Transport(_))), "{failed:?}");
        assert_eq!(accepted.load(SeqCst), 1);
    }

    #[tokio::test]
    async fn a_connection_that_cannot_be_opened_is_not_a_transport_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let client = Client::new(address, 1, Duration::from_secs(2), 1, Duration::from_secs(60));

        let failed = client.read(Space::Input, 1, 1).await;
        assert!(matches!(failed, Err(Error::Connect(_))), "{failed:?}");
    }
}
