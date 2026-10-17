//! The Modbus side of things.
//!
//! This layer talks to the heat pump, polls the registers that are of interest, keeps the
//! [`Snapshot`] of their contents up to date and lets the interested party know when the contents
//! have changed. It does not know anything about what the registers mean (that is up to the layers
//! built on top of this one.)

mod client;
mod plan;
mod snapshot;

pub use client::{Error as RequestError, Request};
pub use snapshot::Snapshot;

use crate::registers::{Register, Space};
use client::{Client, ExceptionCode};
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt as _};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{debug, info, warn};

/// How often the addresses that the heat pump has nothing at are tried again. The heat pump may
/// have said so only because it was not done starting up.
const RETRY_UNREADABLE_EVERY: Duration = Duration::from_secs(10 * 60);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("lookup of `{1}` failed")]
    LookupHost(#[source] std::io::Error, String),
    #[error("`{0}` did not resolve to any addresses")]
    NoAddresses(String),
    #[error("communication with the heat pump failed")]
    Request(#[from] RequestError),
    #[error("register {0:?} is not writable")]
    NotWritable(Register),
    #[error("the communication with the heat pump has ended")]
    Closed,
}

#[derive(clap::Parser, Clone)]
#[group(id = "modbus::Args")]
pub struct Args {
    /// The address of the heat pump's Modbus TCP server.
    ///
    /// Given as `host` or `host:port`, where `host` may be an IP address. IPv6 literals need to be
    /// in brackets if there is a port (`[fe80::1]:502`). The port is 502 if not specified.
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

    /// How many connections to the heat pump to open at the most.
    ///
    /// The heat pump answers one request per connection at a time, which takes about half a second,
    /// so connections are opened as needed up to this many to make for faster polling.
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u16).range(1..=32))]
    max_connections: u16,
}

/// A set of registers to be polled together, every `period`.
pub struct PollGroup {
    pub period: Duration,
    pub registers: Vec<Register>,
}

/// A notification that something about the registers has changed.
///
/// Sent after a read out or a write that turned something up, no matter how many registers it has
/// covered. Whether connections to the heat pump can be opened is news too.
#[derive(Debug)]
pub struct Update {
    /// The documented registers that have a new value. Registers that have never been seen before
    /// are considered to have changed. This is empty if the only news is that the heat pump has
    /// nothing at some addresses, or whether connections to it can be opened.
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

    /// Change the view of the registers, for the tests of the things that look at it.
    #[cfg(test)]
    pub(crate) fn snapshot_mut(&self) -> std::sync::RwLockWriteGuard<'_, Snapshot> {
        self.snapshot.write().unwrap_or_else(|e| e.into_inner())
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
    /// Completes once all of the handles are gone. Problems with the connection to the heat pump
    /// do not end it: the connections are made again as needed.
    pub task: tokio::task::JoinHandle<()>,
}

enum Command {
    Write { writes: Vec<(Register, u16)>, reply: oneshot::Sender<Result<(), Error>> },
}

async fn resolve(tcp: &str) -> Result<SocketAddr, Error> {
    if let Ok(address) = tcp.parse::<SocketAddr>() {
        return Ok(address);
    }
    if let Ok(ip) = tcp.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, 502));
    }
    let (host, port) = tcp
        .rsplit_once(':')
        .and_then(|(host, port)| Some((host, port.parse::<u16>().ok()?)))
        .unwrap_or((tcp, 502));
    tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| Error::LookupHost(e, tcp.to_owned()))?
        .next()
        .ok_or_else(|| Error::NoAddresses(tcp.to_owned()))
}

/// Set up the communication with the heat pump, without polling anything. The heat pump does not
/// need to be reachable.
async fn create_client(args: &Args, close_after: Duration) -> Result<Client, Error> {
    let address = resolve(&args.tcp).await?;
    info!(%address, max_connections = args.max_connections, "heat pump address resolved");
    let max_connections = usize::from(args.max_connections);
    Ok(Client::new(address, args.unit_id, *args.timeout, max_connections, close_after))
}

/// Start polling the registers of the heat pump.
///
/// The heat pump does not need to be reachable: the polls fail until it is.
pub async fn connect(args: &Args, polls: Vec<PollGroup>) -> Result<Device, Error> {
    connect_polling(args, polls, RETRY_UNREADABLE_EVERY).await
}

async fn connect_polling(
    args: &Args,
    polls: Vec<PollGroup>,
    retry_unreadable_every: Duration,
) -> Result<Device, Error> {
    // Every connection that a poll needs gets used at least once per period, so it stays open.
    let longest_period = polls.iter().map(|poll| poll.period).max().unwrap_or_default();
    let client = create_client(args, 2 * longest_period).await?;

    let snapshot = Arc::new(RwLock::new(Snapshot::new()));
    let (update_tx, updates) = mpsc::unbounded_channel();
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let groups: Vec<Group> = {
        let snapshot = snapshot.read().unwrap_or_else(|e| e.into_inner());
        polls.into_iter().map(|poll| Group::new(poll, args.max_read_count, &snapshot)).collect()
    };
    for group in &groups {
        debug!(period = ?group.period, reads = ?group.reads, "polling group");
    }
    info!(
        groups = groups.len(),
        requests = groups.iter().map(|g| g.reads.len()).sum::<usize>(),
        max_connections = args.max_connections,
        "polling plan"
    );
    let worker = Worker {
        client: Arc::new(client),
        snapshot: Arc::clone(&snapshot),
        updates: update_tx,
        groups,
        max_read_count: args.max_read_count,
        written: HashMap::new(),
        forgotten: HashSet::new(),
        retry_unreadable_every,
        retry_unreadable_at: Instant::now() + retry_unreadable_every,
    };
    let task = tokio::task::spawn(worker.run(command_rx));
    Ok(Device { handle: Handle { snapshot, commands: command_tx }, updates, task })
}

/// What reading a range of addresses has turned up.
#[derive(Debug, Default)]
struct ReadOutcome {
    /// The words that were read, with their addresses.
    words: Vec<(u16, u16)>,
    /// The addresses that the heat pump has nothing at.
    unreadable: Vec<u16>,
}

fn halves(read: Read) -> [Read; 2] {
    let half = read.count / 2;
    [
        Read { count: half, ..read },
        Read { start: read.start + half, count: read.count - half, ..read },
    ]
}

fn is_missing_address(error: &RequestError) -> bool {
    matches!(error, RequestError::Exception(_, ExceptionCode::ILLEGAL_DATA_ADDRESS))
}

/// Whether a connection to the heat pump could not be opened, as opposed to the heat pump failing to
/// respond properly on the connection that was opened.
fn failed_to_connect(error: &RequestError) -> bool {
    matches!(error, RequestError::Connect(_))
}

/// Read a range of addresses, and find out what can be read out of it if it can not be read as a
/// whole.
async fn read_resilient(client: &Client, read: Read) -> Result<ReadOutcome, RequestError> {
    // The heat pump rejects a request if there's an address in the range that it has nothing at.
    // Which addresses these are cannot be guessed. There are holes all over, and what is on one
    // side of a hole says nothing about the other. So the range is split in halves until the parts
    // can be read or are a single address.
    let mut outcome = ReadOutcome::default();
    let mut pending = vec![read];
    let mut first = true;
    while !pending.is_empty() {
        let requests = pending.iter().map(|r| client.read(r.space, r.start, r.count));
        let responses = futures::future::join_all(requests).await;
        let mut next = Vec::new();
        for (&read, response) in pending.iter().zip(responses) {
            match response {
                Ok(words) => outcome.words.extend((read.start..).zip(words)),
                Err(e) if is_missing_address(&e) && read.count > 1 => next.extend(halves(read)),
                Err(e) if is_missing_address(&e) => outcome.unreadable.push(read.start),
                Err(e) => return Err(e),
            }
        }
        if first && !next.is_empty() {
            debug!(?read, "the heat pump rejected the request, splitting it up");
        }
        first = false;
        pending = next;
    }
    Ok(outcome)
}

#[derive(Debug, Clone, Copy)]
struct Read {
    space: Space,
    start: u16,
    count: u16,
}

struct Group {
    period: Duration,
    registers: Vec<Register>,
    reads: Vec<Read>,
    next_due: Instant,
    /// A read out is in progress.
    running: bool,
    failing: bool,
    slow: bool,
}

impl Group {
    fn new(poll: PollGroup, max_read_count: u16, snapshot: &Snapshot) -> Group {
        let mut group = Group {
            period: poll.period,
            registers: poll.registers,
            reads: Vec::new(),
            next_due: Instant::now(),
            running: false,
            failing: false,
            slow: false,
        };
        group.plan(max_read_count, snapshot);
        group
    }

    /// Work out what requests to make, and keep away from where the heat pump has nothing.
    fn plan(&mut self, max_read_count: u16, snapshot: &Snapshot) {
        let mut addresses: BTreeMap<Space, Vec<u16>> = BTreeMap::new();
        for &register in self.registers.iter().filter(|&&r| !snapshot.is_unreadable(r)) {
            addresses.entry(register.space()).or_default().push(register.address());
        }
        self.reads.clear();
        for (space, mut addresses) in addresses {
            addresses.sort_unstable();
            addresses.dedup();
            // The protocol does not allow for more than this many words in a response.
            let limit = if space.is_bit() { max_read_count } else { max_read_count.min(125) };
            let barriers = snapshot.unreadable_addresses(space);
            for range in plan::find_optimal_ranges_around(&addresses, barriers, limit) {
                let (start, end) = range.into_inner();
                self.reads.push(Read { space, start, count: end - start + 1 });
            }
        }
    }
}

/// The outcome of reading out all of a group's registers.
struct Finished {
    group: usize,
    started: Instant,
    reads: Vec<Read>,
    results: Vec<Result<ReadOutcome, RequestError>>,
}

/// The outcome of writing registers, and reading them back.
struct Written {
    writes: Vec<(Register, u16)>,
    reply: oneshot::Sender<Result<(), Error>>,
    /// When the writing was over.
    at: Instant,
    /// The words that are in the registers after the writes.
    result: Result<Vec<Vec<u16>>, Error>,
}

enum Done {
    Read(Finished),
    Write(Written),
}

/// Something that is going on with the heat pump.
type InProgress = Pin<Box<dyn Future<Output = Done> + Send>>;

struct Worker {
    client: Arc<Client>,
    snapshot: Arc<RwLock<Snapshot>>,
    updates: mpsc::UnboundedSender<Update>,
    groups: Vec<Group>,
    max_read_count: u16,
    /// When the writes to the addresses were over. What was read out before that is out of date.
    written: HashMap<(Space, u16), Instant>,
    /// The addresses that were once found to be unreadable and have been given another chance.
    /// Everybody has been told about them already, so finding them unreadable again is not news.
    forgotten: HashSet<(Space, u16)>,
    retry_unreadable_every: Duration,
    retry_unreadable_at: Instant,
}

impl Worker {
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        let mut in_progress: FuturesUnordered<InProgress> = FuturesUnordered::new();
        loop {
            let polls_due = self.groups.iter().filter(|g| !g.running).map(|g| g.next_due);
            let wake_up = polls_due.chain([self.retry_unreadable_at]).min();
            tokio::select! {
                biased;
                command = commands.recv() => {
                    // All of the handles are gone, so nobody is interested in this anymore.
                    let Some(command) = command else { return };
                    match command {
                        Command::Write { writes, reply } => {
                            let client = Arc::clone(&self.client);
                            in_progress.push(Box::pin(write_out(client, writes, reply)));
                        }
                    }
                }
                done = in_progress.next(), if !in_progress.is_empty() => {
                    match done.expect("there is something in progress") {
                        Done::Read(finished) => self.finish(finished),
                        Done::Write(written) => self.finish_write(written),
                    }
                }
                _ = tokio::time::sleep_until(wake_up.expect("there is always the retry")) => {
                    let now = Instant::now();
                    if now >= self.retry_unreadable_at {
                        self.forget_unreadable();
                        self.retry_unreadable_at = now + self.retry_unreadable_every;
                    }
                    for (index, group) in self.groups.iter_mut().enumerate() {
                        if group.running || group.next_due > now {
                            continue;
                        }
                        group.running = true;
                        let (client, reads) = (Arc::clone(&self.client), group.reads.clone());
                        in_progress.push(Box::pin(read_out(client, index, reads)));
                    }
                }
            }
        }
    }

    /// Give the addresses that the heat pump had nothing at another try.
    fn forget_unreadable(&mut self) {
        let mut snapshot = self.snapshot.write().unwrap_or_else(|e| e.into_inner());
        let forgotten = snapshot.forget_unreadable();
        if forgotten.is_empty() {
            return;
        }
        debug!(count = forgotten.len(), "trying the addresses with nothing at them again");
        self.forgotten.extend(forgotten);
        for group in &mut self.groups {
            group.plan(self.max_read_count, &snapshot);
        }
    }

    /// Store the outcome of a read out in the snapshot, and schedule the next one.
    fn finish(&mut self, finished: Finished) {
        let Finished { group: index, started, reads, results } = finished;
        // There is nothing to learn from a read out that has not asked for anything.
        let reachable = (!results.is_empty())
            .then(|| !results.iter().all(|result| result.as_ref().is_err_and(failed_to_connect)));
        let mut changed = Vec::new();
        let mut failed = false;
        let mut discovered = Vec::new();
        for (read, result) in reads.iter().zip(results) {
            match result {
                Ok(outcome) => {
                    // What was read before a write was over might have been in the heat pump
                    // before the write.
                    let up_to_date = |&&(address, _): &&(u16, u16)| {
                        let written = self.written.get(&(read.space, address));
                        written.is_none_or(|&at| at <= started)
                    };
                    let words: Vec<_> = outcome.words.iter().filter(up_to_date).copied().collect();
                    self.apply_pairs(read.space, &words, &mut changed);
                    discovered.extend(outcome.unreadable.into_iter().map(|a| (read.space, a)));
                }
                // The heat pump didn't like this for some reason that isn't about the addresses,
                // or there was no connection to it. It is tried again with the next poll.
                Err(e) => {
                    if !failed && !self.groups[index].failing {
                        warn!(
                            error = (&e as &dyn std::error::Error),
                            period = ?self.groups[index].period,
                            "could not read out the registers"
                        );
                    }
                    failed = true;
                }
            }
        }
        let discovered = self.note_unreadable(discovered);
        let reachability = reachable.is_some_and(|reachable| self.note_reachable(reachable));
        let group = &mut self.groups[index];
        group.running = false;
        if group.failing && !failed {
            info!(period = ?group.period, "the registers can be read out again");
        }
        group.failing = failed;
        let took = started.elapsed();
        if took > group.period {
            if !group.slow {
                warn!(
                    ?took,
                    period = ?group.period,
                    requests = reads.len(),
                    "reading the registers takes longer than the polling period, \
                     the heat pump is slow to respond (see `--max-connections`)"
                );
            }
            group.slow = true;
        } else {
            group.slow = false;
        }
        // Don't try to catch up if we've fallen behind.
        group.next_due = (group.next_due + group.period).max(Instant::now());
        // Finding out that there is nothing at an address is news, too. Those waiting for the
        // register to appear can stop waiting. So is whether the heat pump can be connected to.
        if !changed.is_empty() || discovered || reachability {
            debug!(count = changed.len(), ?took, discovered, "registers changed");
            let _ = self.updates.send(Update { changed });
        }
    }

    /// Remember whether connections to the heat pump can be opened. Returns whether this is news.
    fn note_reachable(&self, reachable: bool) -> bool {
        self.snapshot.write().unwrap_or_else(|e| e.into_inner()).set_reachable(reachable)
    }

    /// Remember the addresses that were found to not be there, and stop asking for them.
    ///
    /// Returns whether there was any news in this.
    fn note_unreadable(&mut self, addresses: Vec<(Space, u16)>) -> bool {
        let mut snapshot = self.snapshot.write().unwrap_or_else(|e| e.into_inner());
        let mut new = Vec::new();
        for (space, address) in addresses {
            if snapshot.set_unreadable(space, address) {
                new.push((space, address));
            }
        }
        if new.is_empty() {
            return false;
        }
        for group in &mut self.groups {
            group.plan(self.max_read_count, &snapshot);
        }
        new.retain(|key| !self.forgotten.contains(key));
        let mut news: BTreeMap<Space, Vec<u16>> = BTreeMap::new();
        for (space, address) in &new {
            news.entry(*space).or_default().push(*address);
        }
        for (space, addresses) in &news {
            info!(%space, ?addresses, "the heat pump has nothing at these addresses");
        }
        !news.is_empty()
    }

    fn apply(&self, space: Space, start: u16, words: &[u16], changed: &mut Vec<Register>) {
        let pairs: Vec<(u16, u16)> = (start..).zip(words.iter().copied()).collect();
        self.apply_pairs(space, &pairs, changed);
    }

    fn apply_pairs(&self, space: Space, words: &[(u16, u16)], changed: &mut Vec<Register>) {
        let mut snapshot = self.snapshot.write().unwrap_or_else(|e| e.into_inner());
        for &(address, word) in words {
            if snapshot.set(space, address, word) {
                changed.extend(Register::lookup(space, address));
            }
        }
    }

    /// Store what has been read back after writing, and let the writer know.
    fn finish_write(&mut self, written: Written) {
        let Written { writes, reply, at, result } = written;
        let unconnected = matches!(&result, Err(Error::Request(e)) if failed_to_connect(e));
        let reachability = self.note_reachable(!unconnected);
        for (register, _) in &writes {
            self.written.insert((register.space(), register.address()), at);
        }
        let mut changed = Vec::new();
        let outcome = result.map(|words| {
            for (&(register, _), words) in writes.iter().zip(&words) {
                self.apply(register.space(), register.address(), words, &mut changed);
            }
        });
        if !changed.is_empty() || reachability {
            let _ = self.updates.send(Update { changed });
        }
        let _ = reply.send(outcome);
    }
}

/// Write the words into the registers and read the registers back.
async fn write_out(
    client: Arc<Client>,
    writes: Vec<(Register, u16)>,
    reply: oneshot::Sender<Result<(), Error>>,
) -> Done {
    let result = async {
        for &(register, word) in &writes {
            let address = register.address();
            match register.space() {
                Space::Coil => client.write_coil(address, word != 0).await?,
                Space::Holding => client.write_register(address, word).await?,
                Space::DiscreteInput | Space::Input => return Err(Error::NotWritable(register)),
            }
        }
        // Make sure that what we have is what the heat pump has. It might have refused to take
        // the value without saying anything about it.
        let reads = writes.iter().map(|(r, _)| client.read(r.space(), r.address(), 1));
        let words = futures::future::join_all(reads).await;
        words.into_iter().map(|words| words.map_err(Error::from)).collect()
    }
    .await;
    // Polls that started before this point might have been served before the heat pump took the
    // values (see `Worker::written`.)
    Done::Write(Written { writes, reply, at: Instant::now(), result })
}

/// Read out the registers of a group, with all of the requests being on their way at once (as far
/// as `--max-connections` allows it.)
async fn read_out(client: Arc<Client>, group: usize, reads: Vec<Read>) -> Done {
    let started = Instant::now();
    let results =
        futures::future::join_all(reads.iter().map(|&read| read_resilient(&client, read))).await;
    Done::Read(Finished { group, started, reads, results })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser as _;
    use std::sync::Mutex;
    use std::sync::atomic::Ordering::SeqCst;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    /// A heat pump that takes writes of single holding registers. Registers contain
    /// `address + 1000 * function code` and bits are set for odd addresses. Reading input register
    /// 319 is an error until the heat pump is `healed`.
    ///
    /// Like the real one, it responds to one request at a time on each connection: a request that
    /// arrives before the previous one has been responded to replaces it, and the replaced one is
    /// never responded to. Responses come `delay` after the latest request. Connections do not
    /// affect each other. With `slow_stale_reads`, reading the holding registers takes three times
    /// as long until something has been written to any of them.
    ///
    /// The `Counters` also control what goes wrong: it can be `silent` or `dropping` connections,
    /// `refusing` them (and hanging up on the ones that are open, as if the heat pump had gone
    /// away), close the connections that have been idle for `close_idle_after_ms`, and make the
    /// input registers `ticking` (change with every read.)
    async fn fake_heat_pump_with(
        delay: Duration,
        slow_stale_reads: bool,
    ) -> (SocketAddr, Arc<Counters>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let counters = Arc::new(Counters::default());
        let seen = Arc::clone(&counters);
        tokio::spawn(async move {
            let mut refusing = seen.refusing.subscribe();
            let mut listener = Some(listener);
            loop {
                if *refusing.borrow_and_update() {
                    // Nothing listens at the address, so the connections are refused.
                    listener = None;
                    refusing.wait_for(|refusing| !refusing).await.unwrap();
                }
                let listener = match &listener {
                    Some(listener) => listener,
                    None => {
                        let bound = tokio::net::TcpListener::bind(address).await.unwrap();
                        listener.insert(bound)
                    }
                };
                // Dropped, and so hung up on, once the heat pump is told to go away.
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        _ = refusing.changed() => break,
                        accepted = listener.accept() => {
                            let (socket, _) = accepted.unwrap();
                            seen.connections.fetch_add(1, SeqCst);
                            if seen.dropping.load(SeqCst) {
                                seen.unanswered.fetch_add(1, SeqCst);
                                continue;
                            }
                            let seen = Arc::clone(&seen);
                            connections.spawn(serve_connection(socket, seen, delay, slow_stale_reads));
                        }
                    }
                }
            }
        });
        (address, counters)
    }

    async fn serve_connection(
        socket: tokio::net::TcpStream,
        seen: Arc<Counters>,
        delay: Duration,
        slow_stale_reads: bool,
    ) {
        let (mut reader, mut writer) = socket.into_split();
        let (mut received, mut chunk) = (Vec::new(), [0; 64]);
        // The one request that is going to be responded to, and when.
        let mut pending: Option<(Instant, Vec<u8>)> = None;
        let mut last_active = Instant::now();
        loop {
            let idle_close = Duration::from_millis(seen.close_idle_after_ms.load(SeqCst));
            let closes_at =
                (pending.is_none() && !idle_close.is_zero()).then(|| last_active + idle_close);
            let due = pending.as_ref().map(|(at, _)| *at);
            tokio::select! {
                read = reader.read(&mut chunk) => {
                    let read = read.unwrap_or(0);
                    if read == 0 {
                        seen.closed.fetch_add(1, SeqCst);
                        return;
                    }
                    received.extend(&chunk[..read]);
                    last_active = Instant::now();
                    while received.len() >= 12 {
                        let request: Vec<u8> = received.drain(..12).collect();
                        if seen.dropping.load(SeqCst) {
                            seen.unanswered.fetch_add(1, SeqCst);
                            return;
                        }
                        if seen.silent.load(SeqCst) {
                            seen.unanswered.fetch_add(1, SeqCst);
                            continue;
                        }
                        if pending.is_some() {
                            seen.replaced.fetch_add(1, SeqCst);
                        }
                        let (delay, response) = answer(&seen, &request, delay, slow_stale_reads);
                        pending = Some((Instant::now() + delay, response));
                    }
                }
                _ = async {
                    match due {
                        Some(due) => tokio::time::sleep_until(due).await,
                        None => std::future::pending().await,
                    }
                } => {
                    let (_, response) = pending.take().unwrap();
                    let _ = writer.write_all(&response).await;
                    last_active = Instant::now();
                }
                () = async {
                    match closes_at {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => return,
            }
        }
    }

    /// What the heat pump responds to a request with, and how long it takes.
    fn answer(
        seen: &Counters,
        header: &[u8],
        mut delay: Duration,
        slow_stale_reads: bool,
    ) -> (Duration, Vec<u8>) {
        let function = header[7];
        assert!(
            matches!(function, 1..=6),
            "only reads and writes of single registers are expected, got {function}"
        );
        seen.requests.fetch_add(1, SeqCst);
        let start = u16::from_be_bytes([header[8], header[9]]);
        let count = u16::from_be_bytes([header[10], header[11]]);
        if function >= 5 {
            // Remember the value. The request is echoed back as the response.
            if function == 6 {
                seen.written.lock().unwrap().insert(start, count);
            }
            return (delay, header.to_vec());
        }
        let mut pdu = Vec::new();
        let holes_in_the_holding_registers = function == 3 && start <= 106 && start + count > 105;
        let rejected_input =
            function == 4 && (start..start + count).contains(&319) && !seen.healed.load(SeqCst);
        if rejected_input || holes_in_the_holding_registers {
            seen.rejected.fetch_add(1, SeqCst);
            pdu.extend([function | 0x80, 2]);
        } else if function <= 2 {
            let bits: Vec<bool> = (start..start + count).map(|a| a % 2 == 1).collect();
            let bytes = bits
                .chunks(8)
                .map(|c| c.iter().enumerate().fold(0u8, |b, (i, &v)| b | u8::from(v) << i));
            let bytes: Vec<u8> = bytes.collect();
            pdu.extend([function, bytes.len() as u8]);
            pdu.extend(bytes);
        } else {
            if slow_stale_reads && function == 3 && seen.written.lock().unwrap().is_empty() {
                delay *= 3;
            }
            pdu.extend([function, (count * 2) as u8]);
            for address in start..start + count {
                let written = seen.written.lock().unwrap().get(&address).copied();
                let word = match (function, written) {
                    (3, Some(word)) => word,
                    (4, _) if seen.ticking.load(SeqCst) => {
                        (address + 1000 * 4).wrapping_add(seen.requests() as u16)
                    }
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
        (delay, response)
    }

    async fn fake_heat_pump(delay: Duration) -> (SocketAddr, Arc<Counters>) {
        fake_heat_pump_with(delay, false).await
    }

    #[derive(Default)]
    struct Counters {
        requests: std::sync::atomic::AtomicUsize,
        connections: std::sync::atomic::AtomicUsize,
        /// How many connections the client has closed.
        closed: std::sync::atomic::AtomicUsize,
        /// How many requests were dropped for having been followed by another before the response.
        replaced: std::sync::atomic::AtomicUsize,
        /// How many requests were rejected for asking about addresses that are not there.
        rejected: std::sync::atomic::AtomicUsize,
        /// Whether the addresses that are rejected are there now.
        healed: std::sync::atomic::AtomicBool,
        /// Whether the requests are received and not responded to.
        silent: std::sync::atomic::AtomicBool,
        /// Whether the connections are closed instead of being served.
        dropping: std::sync::atomic::AtomicBool,
        /// Whether the heat pump is gone: the open connections are closed, and new ones refused.
        refusing: tokio::sync::watch::Sender<bool>,
        /// How many connections and requests were not served because of `silent` and `dropping`.
        unanswered: std::sync::atomic::AtomicUsize,
        /// Connections that have not been used for this many milliseconds are closed. Never if 0.
        close_idle_after_ms: std::sync::atomic::AtomicU64,
        /// Whether the input registers have a different value for every read.
        ticking: std::sync::atomic::AtomicBool,
        /// What has been written to the holding registers.
        written: Mutex<std::collections::HashMap<u16, u16>>,
    }

    impl Counters {
        fn requests(&self) -> usize {
            self.requests.load(SeqCst)
        }

        fn rejected(&self) -> usize {
            self.rejected.load(SeqCst)
        }

        fn connections(&self) -> usize {
            self.connections.load(SeqCst)
        }

        fn closed(&self) -> usize {
            self.closed.load(SeqCst)
        }

        fn replaced(&self) -> usize {
            self.replaced.load(SeqCst)
        }

        fn unanswered(&self) -> usize {
            self.unanswered.load(SeqCst)
        }

        fn refuse(&self, on: bool) {
            self.refusing.send_replace(on);
        }
    }

    fn args(address: SocketAddr, more: &[&str]) -> Args {
        args_with_timeout(address, "2s", more)
    }

    fn args_with_timeout(address: SocketAddr, timeout: &str, more: &[&str]) -> Args {
        let tcp = address.to_string();
        let mut args = vec!["test", "--tcp", &tcp, "--timeout", timeout];
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
    async fn polls_all_the_spaces_and_notifies_once_per_read_out() {
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
        // One read out is one notification, no matter how many registers it covered.
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

        // A poll that changes nothing is not news.
        let second = tokio::time::timeout(Duration::from_millis(300), device.updates.recv()).await;
        assert!(second.is_err(), "unexpected notification: {second:?}");
        assert!(counters.requests() > 4, "not polled again");
    }

    #[tokio::test]
    async fn concurrent_requests_are_served_in_parallel_over_several_connections() {
        let delay = Duration::from_millis(300);
        let (address, counters) = fake_heat_pump(delay).await;
        let client = create_client(&args(address, &[]), Duration::from_secs(60)).await.unwrap();

        // Four requests taking 300 ms each. One after another that would be 1.2 seconds.
        let started = Instant::now();
        let (holding, input, coils, discrete) = tokio::join!(
            client.read(Space::Holding, 22, 2),
            client.read(Space::Input, 13, 1),
            client.read(Space::Coil, 9, 3),
            client.read(Space::DiscreteInput, 66, 1),
        );
        let took = started.elapsed();
        assert_eq!(holding.unwrap(), [3022, 3023]);
        assert_eq!(input.unwrap(), [4013]);
        assert_eq!(coils.unwrap(), [1, 0, 1]);
        assert_eq!(discrete.unwrap(), [0]);
        assert!(took >= delay, "took {took:?}");
        assert!(took < delay * 2, "took {took:?}");
        assert!(counters.connections() > 1);
        assert_eq!(counters.replaced(), 0);
    }

    #[tokio::test]
    async fn requests_one_after_another_share_a_connection() {
        let (address, counters) = fake_heat_pump(Duration::from_millis(20)).await;
        let client = create_client(&args(address, &[]), Duration::from_secs(60)).await.unwrap();
        for _ in 0..5 {
            assert_eq!(client.read(Space::Input, 13, 1).await.unwrap(), [4013]);
        }
        client.write_register(22, 7).await.unwrap();
        assert_eq!(client.read(Space::Holding, 22, 1).await.unwrap(), [7]);
        assert_eq!(counters.connections(), 1);
    }

    #[tokio::test]
    async fn a_single_connection_serialises_the_requests() {
        let delay = Duration::from_millis(100);
        let (address, counters) = fake_heat_pump(delay).await;
        let client =
            create_client(&args(address, &["--max-connections", "1"]), Duration::from_secs(60))
                .await
                .unwrap();

        let started = Instant::now();
        let results =
            futures::future::join_all((0..4).map(|i| client.read(Space::Input, 13 + i, 1))).await;
        let took = started.elapsed();
        for (i, result) in results.into_iter().enumerate() {
            assert_eq!(result.unwrap(), [4013 + i as u16]);
        }
        assert!(took >= delay * 4, "took {took:?}");
        assert_eq!(counters.connections(), 1);
        assert_eq!(counters.replaced(), 0);
    }

    #[tokio::test]
    async fn a_request_that_times_out_does_not_stop_the_later_ones() {
        // Reading the holding registers takes 150 ms, which is too long.
        let (address, counters) = fake_heat_pump_with(Duration::from_millis(50), true).await;
        let client =
            Client::new(address, 1, Duration::from_millis(100), 1, Duration::from_secs(60));

        let slow = client.read(Space::Holding, 22, 1).await;
        assert!(matches!(slow, Err(RequestError::Timeout)), "{slow:?}");
        for _ in 0..3 {
            assert_eq!(client.read(Space::Input, 13, 1).await.unwrap(), [4013]);
        }
        assert_eq!(counters.connections(), 2);
    }

    #[tokio::test]
    async fn connections_opened_in_a_race_are_kept_for_later() {
        let (address, counters) = fake_heat_pump(Duration::ZERO).await;
        let client = Client::new(address, 1, Duration::from_secs(2), 8, Duration::from_secs(60));

        // Requests that find no idle connection open one, and are served by whichever connection
        // comes first. Which it is varies, and either way the other is not for throwing away.
        for _ in 0..20 {
            let reads = (0..4).map(|_| client.read(Space::Input, 13, 1));
            for result in futures::future::join_all(reads).await {
                assert_eq!(result.unwrap(), [4013]);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let opened = counters.connections();
        assert!(opened > 1);
        assert_eq!(counters.closed(), 0);

        // They are all idle by now, and enough for all of them to be used at once.
        let reads = (0..opened).map(|_| client.read(Space::Input, 13, 1));
        for result in futures::future::join_all(reads).await {
            assert_eq!(result.unwrap(), [4013]);
        }
        assert_eq!(counters.connections(), opened);
        assert_eq!(counters.closed(), 0);
    }

    #[tokio::test]
    async fn connections_that_are_no_longer_needed_are_closed() {
        let (address, counters) = fake_heat_pump(Duration::from_millis(50)).await;
        let close_after = Duration::from_millis(300);
        let client = Client::new(address, 1, Duration::from_secs(2), 8, close_after);
        let reads = (0..4).map(|_| client.read(Space::Input, 13, 1));
        for result in futures::future::join_all(reads).await {
            assert_eq!(result.unwrap(), [4013]);
        }
        let opened = counters.connections();
        assert!(opened > 1);

        // From now on one connection is enough.
        let until = Instant::now() + close_after * 3;
        while Instant::now() < until {
            assert_eq!(client.read(Space::Input, 13, 1).await.unwrap(), [4013]);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counters.closed(), opened - 1);
        assert_eq!(counters.connections(), opened);
    }

    #[tokio::test]
    async fn writes_get_done_while_busy_polling() {
        // This is the fake heat pump in this very process, nothing else is involved.
        let (address, counters) = fake_heat_pump(Duration::from_millis(100)).await;
        // One register per request, and a lot more requests than there is room for in flight.
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
        let args = args(address, &["--max-connections", "2", "--max-read-count", "1"]);
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
    async fn holes_are_found_and_avoided() {
        // The fake heat pump has no holding registers 105 and 106, and rejects any request that has
        // them in it.
        let (address, counters) = fake_heat_pump(Duration::ZERO).await;
        let registers = vec![
            Register::holding(22),
            Register::holding(49),
            // This is one of those that are not there.
            Register::holding(105),
            Register::holding(124),
            Register::holding(125),
            Register::input(13),
        ];
        let polls = vec![PollGroup { period: Duration::from_millis(50), registers }];
        let mut device = connect(&args(address, &[]), polls).await.unwrap();

        // Everything that can be read gets read, even though the requests to begin with covered
        // the hole, and the register that is not there is known to be missing.
        while !device.handle.snapshot().unresolved(one_of_each(&[22, 49, 105, 124, 125])).is_empty()
        {
            tokio::time::timeout(Duration::from_secs(5), device.updates.recv())
                .await
                .expect("the registers are sorted out")
                .unwrap();
        }
        {
            let snapshot = device.handle.snapshot();
            for address in [22, 49, 124, 125] {
                assert_eq!(
                    snapshot.raw(Space::Holding, address),
                    Some(address + 3000),
                    "{address}"
                );
            }
            assert!(snapshot.is_unreadable(Register::holding(105)));
            assert!(!snapshot.is_unreadable(Register::holding(49)));
            assert!(snapshot.value(Register::holding(105)).is_none());
        }

        // Having found out, no more requests are made for what is not there.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let rejected = counters.rejected();
        assert!(rejected > 0);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(counters.rejected(), rejected, "still asking for what is not there");
        assert!(!device.task.is_finished());
    }

    fn one_of_each(addresses: &[u16]) -> Vec<Register> {
        addresses.iter().map(|&a| Register::holding(a)).collect()
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
        // What was in the same request as the rejected register is not lost.
        assert_eq!(device.handle.snapshot().raw(Space::Input, 311), Some(4311));
        assert!(device.handle.snapshot().is_unreadable(Register::input(319)));
        // The connection that has seen the rejection is still good to be used.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!device.task.is_finished());
    }

    #[tokio::test]
    async fn a_slow_poll_does_not_undo_a_write() {
        let delay = Duration::from_millis(100);
        let (address, _) = fake_heat_pump_with(delay, true).await;
        let registers = vec![Register::holding(22)];
        let polls = vec![PollGroup { period: Duration::from_millis(10), registers }];
        let mut device = connect(&args(address, &[]), polls).await.unwrap();
        device.updates.recv().await.unwrap();
        assert_eq!(device.handle.snapshot().raw(Space::Holding, 22), Some(3022));

        // The next poll is on its way. It is going to take longer than the write and the reading
        // back do, and it brings back the value from before the write.
        tokio::time::sleep(Duration::from_millis(20)).await;
        device.handle.write(vec![(Register::holding(22), 4550)]).await.unwrap();
        assert_eq!(device.handle.snapshot().raw(Space::Holding, 22), Some(4550));

        let deadline = Instant::now() + Duration::from_secs(1);
        while let Ok(update) = tokio::time::timeout_at(deadline, device.updates.recv()).await {
            update.unwrap();
            assert_eq!(device.handle.snapshot().raw(Space::Holding, 22), Some(4550));
        }
    }

    #[tokio::test]
    async fn addresses_with_nothing_at_them_are_tried_again() {
        let (address, counters) = fake_heat_pump(Duration::ZERO).await;
        let registers = vec![Register::input(13), Register::input(319)];
        let polls = vec![PollGroup { period: Duration::from_millis(20), registers }];
        let retry_every = Duration::from_millis(200);
        let args = args(address, &[]);
        let mut device = connect_polling(&args, polls, retry_every).await.unwrap();
        let first = device.updates.recv().await.unwrap();
        assert_eq!(first.changed, [Register::input(13)]);
        assert!(device.handle.snapshot().is_unreadable(Register::input(319)));

        // Still not there on the retries, which is nothing new.
        let before = counters.rejected();
        let update = tokio::time::timeout(retry_every * 3, device.updates.recv()).await;
        assert!(update.is_err(), "unexpected notification: {update:?}");
        assert!(counters.rejected() > before, "not tried again");

        counters.healed.store(true, std::sync::atomic::Ordering::SeqCst);
        let update = tokio::time::timeout(Duration::from_secs(5), device.updates.recv())
            .await
            .expect("the register is read once it is there")
            .unwrap();
        assert_eq!(update.changed, [Register::input(319)]);
        assert_eq!(device.handle.snapshot().raw(Space::Input, 319), Some(4319));
        assert!(!device.handle.snapshot().is_unreadable(Register::input(319)));
    }

    #[tokio::test]
    async fn a_connection_closed_while_idle_does_not_fail_the_request() {
        let (address, counters) = fake_heat_pump(Duration::ZERO).await;
        counters.close_idle_after_ms.store(50, SeqCst);
        let client = create_client(&args(address, &[]), Duration::from_secs(60)).await.unwrap();
        assert_eq!(client.read(Space::Input, 13, 1).await.unwrap(), [4013]);

        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(client.read(Space::Input, 13, 1).await.unwrap(), [4013]);
        }
        // Every time the connection was gone, and another one was used instead.
        assert_eq!(counters.connections(), 4);
    }

    #[tokio::test]
    async fn all_of_the_connections_closed_while_idle_are_gotten_through() {
        let (address, counters) = fake_heat_pump(Duration::from_millis(100)).await;
        counters.close_idle_after_ms.store(50, SeqCst);
        let client = create_client(&args(address, &[]), Duration::from_secs(60)).await.unwrap();
        let reads = (0..4).map(|_| client.read(Space::Input, 13, 1));
        for result in futures::future::join_all(reads).await {
            assert_eq!(result.unwrap(), [4013]);
        }
        assert!(counters.connections() >= 3);

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(client.read(Space::Input, 13, 1).await.unwrap(), [4013]);
    }

    #[tokio::test]
    async fn a_heat_pump_that_rejects_the_requests_is_reachable() {
        // The fake heat pump has nothing at input register 319.
        let (address, _) = fake_heat_pump(Duration::ZERO).await;
        let polls = vec![PollGroup {
            period: Duration::from_millis(50),
            registers: vec![Register::input(319)],
        }];
        let mut device = connect(&args(address, &[]), polls).await.unwrap();
        device.updates.recv().await.unwrap();
        assert!(device.handle.snapshot().reachable());
    }

    #[tokio::test]
    async fn polling_is_not_disturbed_by_the_heat_pump_closing_idle_connections() {
        let (address, counters) = fake_heat_pump(Duration::ZERO).await;
        // The connection is gone every time before the next poll.
        counters.close_idle_after_ms.store(50, SeqCst);
        counters.ticking.store(true, SeqCst);
        let period = Duration::from_millis(200);
        let polls = vec![PollGroup { period, registers: vec![Register::input(13)] }];
        let mut device = connect(&args(address, &[]), polls).await.unwrap();

        // Every poll brings news, if it works. One that fails costs a whole period.
        let mut updates = 0;
        let deadline = Instant::now() + period * 6;
        while let Ok(update) = tokio::time::timeout_at(deadline, device.updates.recv()).await {
            update.unwrap();
            updates += 1;
        }
        assert!(updates >= 5, "{updates} updates");
        assert!(!device.task.is_finished());
    }

    /// The heat pump has trouble for a while, with `outage` starting and ending it. Polling goes on
    /// and picks up where it was. `unreachable` is whether the trouble is that no connection can be
    /// made: otherwise the heat pump is where it was, even if it does not respond properly.
    async fn polling_survives_the_outage(outage: impl Fn(&Counters, bool), unreachable: bool) {
        let (address, counters) = fake_heat_pump(Duration::ZERO).await;
        let polls = vec![PollGroup {
            period: Duration::from_millis(50),
            registers: vec![Register::input(13)],
        }];
        let args = args_with_timeout(address, "200ms", &[]);
        let mut device = connect(&args, polls).await.unwrap();
        let update = device.updates.recv().await.unwrap();
        assert_eq!(update.changed, [Register::input(13)]);
        assert!(device.handle.snapshot().reachable());
        assert_eq!(device.handle.snapshot().raw(Space::Input, 13), Some(4013));

        outage(&counters, true);
        if unreachable {
            // Nothing has changed in the registers, and that the heat pump is gone is still news.
            let update = tokio::time::timeout(Duration::from_secs(5), device.updates.recv())
                .await
                .expect("the outage is not reported")
                .unwrap();
            assert!(update.changed.is_empty(), "{update:?}");
            assert!(!device.handle.snapshot().reachable());
        } else {
            // Long enough for several polls to fail, each of which takes a while.
            let update = tokio::time::timeout(Duration::from_secs(1), device.updates.recv()).await;
            assert!(update.is_err(), "unexpected notification: {update:?}");
            assert!(counters.unanswered() > 0);
            assert!(device.handle.snapshot().reachable());
        }
        assert!(!device.task.is_finished());

        outage(&counters, false);
        if unreachable {
            let update = tokio::time::timeout(Duration::from_secs(5), device.updates.recv())
                .await
                .expect("polling does not resume once the heat pump is back")
                .unwrap();
            assert!(update.changed.is_empty(), "{update:?}");
            assert!(device.handle.snapshot().reachable());
        }
        counters.ticking.store(true, SeqCst);
        let update = tokio::time::timeout(Duration::from_secs(5), device.updates.recv())
            .await
            .expect("polling does not go on")
            .unwrap();
        assert_eq!(update.changed, [Register::input(13)]);
        assert!(device.handle.snapshot().reachable());
        assert!(!device.task.is_finished());
    }

    #[tokio::test]
    async fn polling_survives_the_heat_pump_dropping_connections() {
        let outage = |counters: &Counters, on| counters.dropping.store(on, SeqCst);
        polling_survives_the_outage(outage, false).await;
    }

    #[tokio::test]
    async fn polling_survives_the_heat_pump_not_responding() {
        let outage = |counters: &Counters, on| counters.silent.store(on, SeqCst);
        polling_survives_the_outage(outage, false).await;
    }

    #[tokio::test]
    async fn polling_survives_the_heat_pump_going_away() {
        let outage = |counters: &Counters, on| counters.refuse(on);
        polling_survives_the_outage(outage, true).await;
    }

    #[tokio::test]
    async fn a_write_that_the_heat_pump_does_not_respond_to_fails_and_the_worker_goes_on() {
        let (address, counters) = fake_heat_pump(Duration::ZERO).await;
        let polls = vec![PollGroup {
            period: Duration::from_secs(3600),
            registers: vec![Register::holding(22)],
        }];
        let args = args_with_timeout(address, "200ms", &[]);
        let mut device = connect(&args, polls).await.unwrap();
        device.updates.recv().await.unwrap();
        assert!(device.handle.snapshot().reachable());

        counters.silent.store(true, SeqCst);
        let write = device.handle.write(vec![(Register::holding(22), 4550)]).await;
        assert!(matches!(write, Err(Error::Request(RequestError::Timeout))), "{write:?}");
        // It is there, it just does not respond.
        assert!(device.handle.snapshot().reachable());
        assert!(!device.task.is_finished());

        counters.silent.store(false, SeqCst);
        device.handle.write(vec![(Register::holding(22), 4550)]).await.unwrap();
        assert_eq!(device.handle.snapshot().raw(Space::Holding, 22), Some(4550));
        assert!(device.handle.snapshot().reachable());
        assert!(!device.task.is_finished());
    }

    #[tokio::test]
    async fn a_write_that_cannot_reach_the_heat_pump_fails_and_the_worker_goes_on() {
        let (address, counters) = fake_heat_pump(Duration::ZERO).await;
        let polls = vec![PollGroup {
            period: Duration::from_secs(3600),
            registers: vec![Register::holding(22)],
        }];
        let mut device = connect(&args(address, &[]), polls).await.unwrap();
        device.updates.recv().await.unwrap();
        assert!(device.handle.snapshot().reachable());

        counters.refuse(true);
        let write = device.handle.write(vec![(Register::holding(22), 4550)]).await;
        assert!(matches!(write, Err(Error::Request(RequestError::Connect(_)))), "{write:?}");
        // The write is how the heat pump was found to be gone, and that is news.
        let update = device.updates.try_recv().expect("the outage is not reported");
        assert!(update.changed.is_empty(), "{update:?}");
        assert!(!device.handle.snapshot().reachable());
        assert!(!device.task.is_finished());

        counters.refuse(false);
        device.handle.write(vec![(Register::holding(22), 4550)]).await.unwrap();
        assert_eq!(device.handle.snapshot().raw(Space::Holding, 22), Some(4550));
        assert!(device.handle.snapshot().reachable());
        assert!(!device.task.is_finished());
    }
}
