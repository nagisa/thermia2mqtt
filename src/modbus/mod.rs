//! The Modbus side of things.
//!
//! This layer talks to the heat pump, polls the registers that are of interest, keeps the
//! [`Snapshot`] of their contents up to date and lets the interested party know when the contents
//! have changed. It does not know anything about what the registers mean (that is up to the layers
//! built on top of this one.)

mod plan;
mod pool;
mod snapshot;

pub use pool::{Error as RequestError, Pool, Request};
pub use snapshot::Snapshot;

use crate::registers::{Register, Space};
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt as _};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{debug, info, warn};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("lookup of `{1}` failed")]
    LookupHost(#[source] std::io::Error, String),
    #[error("`{0}` did not resolve to any addresses")]
    NoAddresses(String),
    #[error("could not connect to `{1}`")]
    Connect(#[source] std::io::Error, SocketAddr),
    #[error("communication with the heat pump failed")]
    Request(#[from] RequestError),
    #[error("register {0:?} is not writable")]
    NotWritable(Register),
    #[error("the connection to the heat pump has been closed")]
    Closed,
}

#[derive(clap::Parser, Clone)]
#[group(id = "modbus::Args")]
pub struct Args {
    /// The address of the heat pump's Modbus TCP server.
    ///
    /// Given as `host` or `host:port`. The port is 502 if not specified.
    #[arg(long, short = 't')]
    tcp: String,

    /// The Modbus unit ID to use for the requests.
    ///
    /// Over Modbus TCP the heat pump responds to any unit ID.
    #[arg(long, short = 'i', default_value_t = 1)]
    unit_id: u8,

    /// How long to wait for the heat pump to respond to a request.
    #[arg(long, default_value = "5s")]
    timeout: humantime::Duration,

    /// The most registers (or bits) to read in a single request.
    ///
    /// The heat pump can take a while to respond to each request, so reading more at once makes
    /// for faster polling. The Modbus protocol limits this to 125 for registers (larger values are
    /// treated as 125) and 2000 for bits.
    #[arg(long, default_value_t = 125, value_parser = clap::value_parser!(u16).range(1..=2000))]
    max_read_count: u16,

    /// How many connections to the heat pump to use.
    ///
    /// The heat pump takes about half a second to respond to a request. Each connection waits for
    /// the response before sending the next request, but the heat pump serves the connections
    /// independently, so having more of them makes for faster polling.
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=32))]
    connections: u16,
}

/// A set of registers to be polled together, every `period`.
pub struct PollGroup {
    pub period: Duration,
    pub registers: Vec<Register>,
}

/// A notification that the contents of some of the registers have changed.
///
/// There is one of these for every read out (or write) transaction, no matter how many registers it
/// has covered.
#[derive(Debug)]
pub struct Update {
    /// The documented registers that have a new value. Registers that have never been seen before
    /// are considered to have changed.
    pub changed: Vec<Register>,
}

/// Interact with the heat pump.
#[derive(Clone)]
pub struct Handle {
    snapshot: Arc<RwLock<Snapshot>>,
    commands: mpsc::UnboundedSender<Command>,
}

impl Handle {
    /// A handle that is not connected to anything. For tests of things that need a handle but
    /// never touch the heat pump.
    #[cfg(test)]
    pub(crate) fn detached(snapshot: Snapshot) -> Handle {
        let (commands, _) = mpsc::unbounded_channel();
        Handle { snapshot: Arc::new(RwLock::new(snapshot)), commands }
    }

    /// The current view of the registers.
    ///
    /// Do not hold on to this for long, as it blocks the updates to the view.
    pub fn snapshot(&self) -> RwLockReadGuard<'_, Snapshot> {
        self.snapshot.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Write the specified words to the registers and read the registers back.
    ///
    /// Returns once the new values are a part of the [`Snapshot`].
    pub async fn write(&self, writes: Vec<(Register, u16)>) -> Result<(), Error> {
        let (reply, response) = oneshot::channel();
        self.commands.send(Command::Write { writes, reply }).map_err(|_| Error::Closed)?;
        response.await.map_err(|_| Error::Closed)?
    }
}

/// A connection to the heat pump.
pub struct Device {
    pub handle: Handle,
    pub updates: mpsc::UnboundedReceiver<Update>,
    /// Completes when the connection fails or is closed.
    pub task: tokio::task::JoinHandle<Result<(), Error>>,
}

enum Command {
    Write { writes: Vec<(Register, u16)>, reply: oneshot::Sender<Result<(), Error>> },
}

/// Connect to the heat pump, without polling anything.
pub async fn connect_pool(args: &Args) -> Result<Pool, Error> {
    let target =
        if args.tcp.contains(':') { args.tcp.clone() } else { format!("{}:502", args.tcp) };
    let address = tokio::net::lookup_host(&target)
        .await
        .map_err(|e| Error::LookupHost(e, target.clone()))?
        .next()
        .ok_or_else(|| Error::NoAddresses(target.clone()))?;
    info!(%address, connections = args.connections, "connecting");
    let pool = Pool::connect(address, args.unit_id, *args.timeout, usize::from(args.connections))
        .await
        .map_err(|e| Error::Connect(e, address))?;
    info!("connected");
    Ok(pool)
}

/// Connect to the heat pump and start polling the registers.
pub async fn connect(args: &Args, polls: Vec<PollGroup>) -> Result<Device, Error> {
    let pool = connect_pool(args).await?;

    let snapshot = Arc::new(RwLock::new(Snapshot::new()));
    let (update_tx, updates) = mpsc::unbounded_channel();
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let groups: Vec<Group> =
        polls.into_iter().map(|poll| Group::new(poll, args.max_read_count)).collect();
    for group in &groups {
        debug!(period = ?group.period, reads = ?group.reads, "polling group");
    }
    info!(
        groups = groups.len(),
        requests = groups.iter().map(|g| g.reads.len()).sum::<usize>(),
        connections = args.connections,
        "polling plan"
    );
    let worker = Worker {
        pool: Arc::new(pool),
        snapshot: Arc::clone(&snapshot),
        updates: update_tx,
        groups,
    };
    let task = tokio::task::spawn(worker.run(command_rx));
    Ok(Device { handle: Handle { snapshot, commands: command_tx }, updates, task })
}

/// Read out everything that is readable in a range of addresses, documented or not.
///
/// A request that covers addresses that the heat pump has nothing at is rejected as a whole, so
/// those are split up until the readable addresses are known.
pub async fn scan(
    pool: &Pool,
    space: Space,
    from: u16,
    to: u16,
    chunk: u16,
) -> Result<BTreeMap<u16, u16>, RequestError> {
    let mut found = BTreeMap::new();
    let mut pending: Vec<(u16, u16)> = (u32::from(from)..=u32::from(to))
        .step_by(usize::from(chunk.max(1)))
        .map(|start| {
            let count = (u32::from(to) + 1 - start).min(u32::from(chunk.max(1)));
            (start as u16, count as u16)
        })
        .collect();
    while !pending.is_empty() {
        let requests = pending.iter().map(|&(start, count)| pool.read(space, start, count));
        let responses = futures::future::join_all(requests).await;
        let mut split = Vec::new();
        for (&(start, count), response) in pending.iter().zip(responses) {
            match response {
                Ok(words) => found.extend((start..).zip(words)),
                Err(RequestError::Exception(..)) if count > 1 => {
                    let half = count / 2;
                    split.push((start, half));
                    split.push((start + half, count - half));
                }
                Err(RequestError::Exception(..)) => {}
                Err(e) => return Err(e),
            }
        }
        pending = split;
    }
    Ok(found)
}

#[derive(Debug, Clone, Copy)]
struct Read {
    space: Space,
    start: u16,
    count: u16,
}

struct Group {
    period: Duration,
    reads: Vec<Read>,
    next_due: Instant,
    /// A read out is in progress.
    running: bool,
    failing: bool,
    slow: bool,
}

impl Group {
    fn new(poll: PollGroup, max_read_count: u16) -> Group {
        let mut addresses: BTreeMap<Space, Vec<u16>> = BTreeMap::new();
        for register in poll.registers {
            addresses.entry(register.space()).or_default().push(register.address());
        }
        let mut reads = Vec::new();
        for (space, mut addresses) in addresses {
            addresses.sort_unstable();
            addresses.dedup();
            // The protocol does not allow for more than this many words in a response.
            let limit = if space.is_bit() { max_read_count } else { max_read_count.min(125) };
            for range in plan::find_optimal_ranges(&addresses, limit) {
                let (start, end) = range.into_inner();
                reads.push(Read { space, start, count: end - start + 1 });
            }
        }
        Group {
            period: poll.period,
            reads,
            next_due: Instant::now(),
            running: false,
            failing: false,
            slow: false,
        }
    }
}

/// The outcome of reading out all of a group's registers.
struct Finished {
    group: usize,
    started: Instant,
    reads: Vec<Read>,
    results: Vec<Result<Vec<u16>, RequestError>>,
}

/// The outcome of writing registers, and reading them back.
struct Written {
    writes: Vec<(Register, u16)>,
    reply: oneshot::Sender<Result<(), Error>>,
    /// The words that are in the registers after the writes.
    result: Result<Vec<Vec<u16>>, Error>,
}

enum Done {
    Read(Finished),
    Write(Written),
}

/// Something that is going on with the heat pump.
///
/// These have to be polled all the time: they are waiting for each other to give up their
/// connections, so no one can be left behind while another is waited for.
type InProgress = Pin<Box<dyn Future<Output = Done> + Send>>;

struct Worker {
    pool: Arc<Pool>,
    snapshot: Arc<RwLock<Snapshot>>,
    updates: mpsc::UnboundedSender<Update>,
    groups: Vec<Group>,
}

impl Worker {
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) -> Result<(), Error> {
        let mut in_progress: FuturesUnordered<InProgress> = FuturesUnordered::new();
        loop {
            let wake_up = self.groups.iter().filter(|g| !g.running).map(|g| g.next_due).min();
            tokio::select! {
                biased;
                command = commands.recv() => {
                    // All of the handles are gone, so nobody is interested in this anymore.
                    let Some(command) = command else { return Ok(()) };
                    match command {
                        Command::Write { writes, reply } => {
                            in_progress.push(Box::pin(write_out(Arc::clone(&self.pool), writes, reply)));
                        }
                    }
                }
                done = in_progress.next(), if !in_progress.is_empty() => {
                    match done.expect("there is something in progress") {
                        Done::Read(finished) => self.finish(finished)?,
                        Done::Write(written) => self.finish_write(written)?,
                    }
                }
                _ = async {
                    match wake_up {
                        Some(when) => tokio::time::sleep_until(when).await,
                        None => std::future::pending().await,
                    }
                } => {
                    let now = Instant::now();
                    for (index, group) in self.groups.iter_mut().enumerate() {
                        if group.running || group.next_due > now {
                            continue;
                        }
                        group.running = true;
                        let (pool, reads) = (Arc::clone(&self.pool), group.reads.clone());
                        in_progress.push(Box::pin(read_out(pool, index, reads)));
                    }
                }
            }
        }
    }

    /// Make use of what has been read.
    fn finish(&mut self, finished: Finished) -> Result<(), Error> {
        let Finished { group: index, started, reads, results } = finished;
        let mut changed = Vec::new();
        let mut failed = false;
        for (read, result) in reads.iter().zip(results) {
            match result {
                Ok(words) => self.apply(read.space, read.start, &words, &mut changed),
                // Not every heat pump has every register that is documented in the guide.
                Err(RequestError::Exception(request, code)) => {
                    failed = true;
                    if !self.groups[index].failing {
                        warn!(?code, %request, "the heat pump rejected a request");
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        let group = &mut self.groups[index];
        group.running = false;
        group.failing = failed;
        let took = started.elapsed();
        if took > group.period {
            if !group.slow {
                warn!(
                    ?took,
                    period = ?group.period,
                    requests = reads.len(),
                    "reading the registers takes longer than the polling period, \
                     the heat pump is slow to respond (see `--connections`)"
                );
            }
            group.slow = true;
        } else {
            group.slow = false;
        }
        // Don't try to catch up if we've fallen behind.
        group.next_due = (group.next_due + group.period).max(Instant::now());
        if !changed.is_empty() {
            debug!(count = changed.len(), ?took, "registers changed");
            let _ = self.updates.send(Update { changed });
        }
        Ok(())
    }

    fn apply(&self, space: Space, start: u16, words: &[u16], changed: &mut Vec<Register>) {
        let mut snapshot = self.snapshot.write().unwrap_or_else(|e| e.into_inner());
        for (address, &word) in (start..).zip(words) {
            if snapshot.set(space, address, word) {
                changed.extend(Register::lookup(space, address));
            }
        }
    }

    /// Make use of what has been read back after writing.
    fn finish_write(&mut self, written: Written) -> Result<(), Error> {
        let Written { writes, reply, result } = written;
        match result {
            Ok(words) => {
                let mut changed = Vec::new();
                for (&(register, _), words) in writes.iter().zip(&words) {
                    self.apply(register.space(), register.address(), words, &mut changed);
                }
                if !changed.is_empty() {
                    let _ = self.updates.send(Update { changed });
                }
                let _ = reply.send(Ok(()));
                Ok(())
            }
            // The connection is not to be relied upon after this.
            Err(Error::Request(
                e @ (RequestError::Transport(_) | RequestError::Timeout | RequestError::Closed),
            )) => {
                let _ = reply.send(Err(Error::Closed));
                Err(Error::Request(e))
            }
            Err(e) => {
                let _ = reply.send(Err(e));
                Ok(())
            }
        }
    }
}

/// Write the words into the registers and read the registers back.
async fn write_out(
    pool: Arc<Pool>,
    writes: Vec<(Register, u16)>,
    reply: oneshot::Sender<Result<(), Error>>,
) -> Done {
    let result = async {
        for &(register, word) in &writes {
            let address = register.address();
            match register.space() {
                Space::Coil => pool.write_coil(address, word != 0).await?,
                Space::Holding => pool.write_register(address, word).await?,
                Space::DiscreteInput | Space::Input => return Err(Error::NotWritable(register)),
            }
        }
        // Make sure that what we have is what the heat pump has. It might have refused to take
        // the value without saying anything about it.
        let reads = writes.iter().map(|(r, _)| pool.read(r.space(), r.address(), 1));
        let words = futures::future::join_all(reads).await;
        words.into_iter().map(|words| words.map_err(Error::from)).collect()
    }
    .await;
    Done::Write(Written { writes, reply, result })
}

/// Read out the registers of a group, with all of the requests being on their way at once (as far
/// as the connections allow it.)
async fn read_out(pool: Arc<Pool>, group: usize, reads: Vec<Read>) -> Done {
    let started = Instant::now();
    let results =
        futures::future::join_all(reads.iter().map(|r| pool.read(r.space, r.start, r.count))).await;
    Done::Read(Finished { group, started, reads, results })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser as _;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    /// A heat pump that can only be read. Registers contain `address + 1000 * function code`
    /// and bits are set for odd addresses. Reading input register 319 is an error.
    ///
    /// Each connection is served separately, and every request is responded to after `delay`.
    /// Returns the address and the number of requests seen, and the number of connections seen.
    async fn fake_heat_pump(delay: Duration) -> (SocketAddr, Arc<Counters>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let counters = Arc::new(Counters::default());
        let seen = Arc::clone(&counters);
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                seen.connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    loop {
                        let mut header = [0u8; 12];
                        if socket.read_exact(&mut header).await.is_err() {
                            return;
                        }
                        let function = header[7];
                        assert!(
                            matches!(function, 1..=4 | 6),
                            "only reads and writes of single registers are expected, got {function}"
                        );
                        seen.requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let start = u16::from_be_bytes([header[8], header[9]]);
                        let count = u16::from_be_bytes([header[10], header[11]]);
                        if function == 6 {
                            // Remember the value, and echo the request back.
                            {
                                seen.written.lock().unwrap().insert(start, count);
                            }
                            tokio::time::sleep(delay).await;
                            if socket.write_all(&header).await.is_err() {
                                return;
                            }
                            continue;
                        }
                        let mut pdu = Vec::new();
                        if function == 4 && (start..start + count).contains(&319) {
                            pdu.extend([function | 0x80, 2]);
                        } else if function <= 2 {
                            let bits = (start..start + count).map(|a| a % 2 == 1);
                            let bits: Vec<bool> = bits.collect();
                            let bytes = bits.chunks(8).map(|c| {
                                c.iter().enumerate().fold(0u8, |b, (i, &v)| b | u8::from(v) << i)
                            });
                            let bytes: Vec<u8> = bytes.collect();
                            pdu.extend([function, bytes.len() as u8]);
                            pdu.extend(bytes);
                        } else {
                            pdu.extend([function, (count * 2) as u8]);
                            for address in start..start + count {
                                let written = seen.written.lock().unwrap().get(&address).copied();
                                let word = match (function, written) {
                                    (3, Some(word)) => word,
                                    _ => address + 1000 * u16::from(function),
                                };
                                pdu.extend(word.to_be_bytes());
                            }
                        }
                        let mut response = Vec::new();
                        response.extend(&header[0..4]);
                        response.extend((pdu.len() as u16 + 1).to_be_bytes());
                        response.push(header[6]);
                        response.extend(pdu);
                        tokio::time::sleep(delay).await;
                        if socket.write_all(&response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (address, counters)
    }

    #[derive(Default)]
    struct Counters {
        requests: std::sync::atomic::AtomicUsize,
        connections: std::sync::atomic::AtomicUsize,
        /// What has been written to the holding registers.
        written: Mutex<std::collections::HashMap<u16, u16>>,
    }

    impl Counters {
        fn requests(&self) -> usize {
            self.requests.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn connections(&self) -> usize {
            self.connections.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    fn args(address: SocketAddr, more: &[&str]) -> Args {
        let tcp = address.to_string();
        let mut args = vec!["test", "--tcp", &tcp, "--timeout", "2s"];
        args.extend(more);
        Args::parse_from(args)
    }

    fn one_register_of_each_space() -> Vec<Register> {
        vec![
            Register::coil(9),
            Register::coil(10),
            Register::discrete_input(66),
            Register::input(13),
            Register::input(15),
            Register::holding(22),
        ]
    }

    #[tokio::test]
    async fn polls_all_the_spaces_and_notifies_once_per_transaction() {
        let (address, counters) = fake_heat_pump(Duration::ZERO).await;
        let polls = vec![PollGroup {
            period: Duration::from_millis(50),
            registers: one_register_of_each_space(),
        }];
        let mut device = connect(&args(address, &[]), polls).await.unwrap();

        let first = tokio::time::timeout(Duration::from_secs(5), device.updates.recv())
            .await
            .unwrap()
            .unwrap();
        // Everything was read in this one transaction, so this is one notification for all of it.
        assert_eq!(first.changed.len(), 6, "{first:?}");
        {
            let snapshot = device.handle.snapshot();
            assert_eq!(snapshot.raw(Space::Coil, 9), Some(1));
            assert_eq!(snapshot.raw(Space::Coil, 10), Some(0));
            assert_eq!(snapshot.raw(Space::DiscreteInput, 66), Some(0));
            assert_eq!(snapshot.raw(Space::Input, 13), Some(13 + 4000));
            assert_eq!(snapshot.raw(Space::Holding, 22), Some(22 + 3000));
        }
        // Spaces are read separately, but the addresses inside of them are merged into ranges.
        assert_eq!(counters.requests(), 4);
        assert_eq!(counters.connections(), 4);

        // Nothing changes, so nobody gets told about it.
        let second = tokio::time::timeout(Duration::from_millis(300), device.updates.recv()).await;
        assert!(second.is_err(), "unexpected notification: {second:?}");
        assert!(counters.requests() > 4, "not polled again");
    }

    #[tokio::test]
    async fn requests_are_in_progress_at_the_same_time() {
        let delay = Duration::from_millis(300);
        let poll = |registers| vec![PollGroup { period: Duration::from_secs(60), registers }];

        // Four requests, each taking 300 ms. One after another that would be 1.2 seconds.
        let (address, _) = fake_heat_pump(delay).await;
        let started = Instant::now();
        let mut device =
            connect(&args(address, &[]), poll(one_register_of_each_space())).await.unwrap();
        device.updates.recv().await.unwrap();
        let concurrent = started.elapsed();
        assert!(concurrent < Duration::from_millis(800), "took {concurrent:?}");
        assert!(concurrent >= delay, "took {concurrent:?}");

        let (address, counters) = fake_heat_pump(delay).await;
        let started = Instant::now();
        let mut device =
            connect(&args(address, &["--connections", "1"]), poll(one_register_of_each_space()))
                .await
                .unwrap();
        device.updates.recv().await.unwrap();
        let sequential = started.elapsed();
        assert!(sequential >= delay * 4, "took {sequential:?}");
        assert_eq!(counters.connections(), 1);
    }

    #[tokio::test]
    async fn writes_get_done_while_the_connections_are_busy_polling() {
        // This is the fake heat pump in this very process, nothing else is involved.
        let (address, counters) = fake_heat_pump(Duration::from_millis(100)).await;
        // One register per request, and a lot more requests than there are connections.
        let registers = vec![
            Register::coil(9),
            Register::coil(10),
            Register::input(13),
            Register::input(15),
            Register::input(16),
            Register::input(17),
            Register::holding(22),
            Register::holding(23),
        ];
        let polls = vec![PollGroup { period: Duration::from_millis(10), registers }];
        let args = args(address, &["--connections", "2", "--max-read-count", "1"]);
        let mut device = connect(&args, polls).await.unwrap();
        device.updates.recv().await.unwrap();

        for word in [4550, 4550, 5600] {
            let write = device.handle.write(vec![(Register::holding(22), word)]);
            let written = tokio::time::timeout(Duration::from_secs(5), write).await;
            written.expect("the write did not get done").unwrap();
            assert_eq!(device.handle.snapshot().raw(Space::Holding, 22), Some(word));
        }
        assert_eq!(counters.written.lock().unwrap().get(&22), Some(&5600));
        assert!(!device.task.is_finished());
    }

    #[tokio::test]
    async fn scanning_finds_what_can_be_read() {
        let (address, counters) = fake_heat_pump(Duration::ZERO).await;
        let pool = connect_pool(&args(address, &[])).await.unwrap();
        // The fake heat pump refuses to read input register 319.
        let found = scan(&pool, Space::Input, 300, 330, 10).await.unwrap();
        let expected: Vec<u16> = (300..=330).filter(|a| *a != 319).collect();
        assert_eq!(found.keys().copied().collect::<Vec<_>>(), expected);
        assert_eq!(found[&311], 311 + 4000);
        // The chunk of 10 was split up, but not more than necessary.
        assert!(counters.requests() < 31, "{} requests", counters.requests());
        // Everything is fine with the ones that are not at the edge.
        let found = scan(&pool, Space::Holding, 0, 99, 25).await.unwrap();
        assert_eq!(found.len(), 100);
    }

    #[tokio::test]
    async fn rejected_reads_do_not_stop_the_polling() {
        let (address, counters) = fake_heat_pump(Duration::ZERO).await;
        let polls = vec![
            PollGroup { period: Duration::from_millis(50), registers: vec![Register::input(13)] },
            // The fake heat pump refuses to read input register 319.
            PollGroup {
                period: Duration::from_millis(50),
                registers: vec![Register::input(319), Register::input(311)],
            },
        ];
        let mut device = connect(&args(address, &[]), polls).await.unwrap();
        let update = tokio::time::timeout(Duration::from_secs(5), device.updates.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(!update.changed.is_empty());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!device.task.is_finished());
        assert!(counters.requests() >= 2);
        assert_eq!(device.handle.snapshot().raw(Space::Input, 13), Some(4013));
        assert_eq!(device.handle.snapshot().raw(Space::Input, 311), None);
        // The connection that has seen the rejection is still good to be used.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!device.task.is_finished());
    }
}
