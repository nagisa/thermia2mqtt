//! A number of connections to the heat pump, to be used at the same time.
//!
//! The heat pump takes a while to respond to any request (about half a second), but each connection
//! to it is served independently. Requests on a single connection have to wait for each other, so
//! the way to have a number of them in progress is to have a number of connections.

use crate::registers::Space;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::time::Instant;
use tokio_modbus::client::{Context, Reader as _, Writer as _};
use tokio_modbus::{ExceptionCode, Slave};
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

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("communication with the heat pump failed")]
    Transport(#[source] tokio_modbus::Error),
    #[error("the heat pump did not respond in time")]
    Timeout,
    #[error("the heat pump responded with an exception ({1:?}) to {0}")]
    Exception(Request, ExceptionCode),
    #[error("all of the connections to the heat pump have been lost")]
    Closed,
}

pub struct Pool {
    timeout: Duration,
    /// There's a connection in `contexts` for every permit that is available.
    permits: Semaphore,
    contexts: Mutex<Vec<Context>>,
}

/// A connection that is in use.
///
/// A connection is only given back if the request that was using it has completed, i.e. we know
/// that there is nothing left to be read from it. Otherwise it is closed.
struct Checkout<'a> {
    pool: &'a Pool,
    context: Option<Context>,
    complete: bool,
    _permit: SemaphorePermit<'a>,
}

impl Checkout<'_> {
    fn context(&mut self) -> &mut Context {
        self.context.as_mut().expect("context is there until drop")
    }
}

impl Drop for Checkout<'_> {
    fn drop(&mut self) {
        if let Some(context) = self.context.take().filter(|_| self.complete) {
            self.pool.contexts.lock().unwrap_or_else(|e| e.into_inner()).push(context);
        }
    }
}

impl Pool {
    pub(super) async fn connect(
        address: SocketAddr,
        unit_id: u8,
        timeout: Duration,
        connections: usize,
    ) -> std::io::Result<Pool> {
        let mut contexts = Vec::new();
        for _ in 0..connections.max(1) {
            contexts.push(tokio_modbus::client::tcp::connect_slave(address, Slave(unit_id)).await?);
        }
        Ok(Pool {
            timeout,
            permits: Semaphore::new(contexts.len()),
            contexts: Mutex::new(contexts),
        })
    }

    async fn checkout(&self) -> Result<Checkout<'_>, Error> {
        let permit = self.permits.acquire().await.map_err(|_| Error::Closed)?;
        let context = self.contexts.lock().unwrap_or_else(|e| e.into_inner()).pop();
        // Connections that failed are not given back, but nothing is going to be using the pool
        // after a failure.
        let context = context.ok_or(Error::Closed)?;
        Ok(Checkout { pool: self, context: Some(context), complete: false, _permit: permit })
    }

    /// Read out registers. Bits are returned as 0 and 1.
    pub async fn read(&self, space: Space, start: u16, count: u16) -> Result<Vec<u16>, Error> {
        let request = Request::Read(space, start, count);
        let started = Instant::now();
        let mut checkout = self.checkout().await?;
        let words = {
            let context = checkout.context();
            let to_words = |bits: Vec<bool>| bits.into_iter().map(u16::from).collect::<Vec<_>>();
            let response = async {
                match space {
                    Space::Coil => context.read_coils(start, count).await.map(|r| r.map(to_words)),
                    Space::DiscreteInput => {
                        context.read_discrete_inputs(start, count).await.map(|r| r.map(to_words))
                    }
                    Space::Input => context.read_input_registers(start, count).await,
                    Space::Holding => context.read_holding_registers(start, count).await,
                }
            };
            self.wait(response).await
        };
        let words = self.conclude(checkout, request.clone(), words)?;
        debug!(%request, elapsed = ?started.elapsed(), "request completed");
        // The bits come padded up to full bytes.
        Ok(words.into_iter().take(usize::from(count)).collect())
    }

    pub(super) async fn write_coil(&self, address: u16, on: bool) -> Result<(), Error> {
        let request = Request::Write(Space::Coil, address);
        let mut checkout = self.checkout().await?;
        let response = {
            let context = checkout.context();
            self.wait(async { context.write_single_coil(address, on).await }).await
        };
        self.conclude(checkout, request, response)
    }

    pub(super) async fn write_register(&self, address: u16, word: u16) -> Result<(), Error> {
        let request = Request::Write(Space::Holding, address);
        let mut checkout = self.checkout().await?;
        let response = {
            let context = checkout.context();
            self.wait(async { context.write_single_register(address, word).await }).await
        };
        self.conclude(checkout, request, response)
    }

    /// Wait for a response, but not for too long.
    async fn wait<T>(
        &self,
        response: impl Future<Output = tokio_modbus::Result<T>>,
    ) -> Result<tokio_modbus::Result<T>, tokio::time::error::Elapsed> {
        tokio::time::timeout(self.timeout, response).await
    }

    /// Work out what to make of the connection and the response.
    fn conclude<T>(
        &self,
        mut checkout: Checkout<'_>,
        request: Request,
        response: Result<tokio_modbus::Result<T>, tokio::time::error::Elapsed>,
    ) -> Result<T, Error> {
        let response = response.map_err(|_| Error::Timeout)?.map_err(Error::Transport)?;
        // Whatever the response is, it has been received, so the connection is fine.
        checkout.complete = true;
        response.map_err(|code| Error::Exception(request, code))
    }
}
