//! Monotonic, persisted nonce generation.
//!
//! Kraken requires a strictly increasing nonce per API key. The existing SignalEngine connector
//! uses process uptime, which restarts from zero (verified defect). Here:
//!
//! * `nonce = max(clock_nanos, last + 1)` where `last` is the highest nonce ever handed out for
//!   this key, persisted in a [`NonceStore`] BEFORE the nonce is returned (a crash after
//!   persisting only wastes a nonce; it can never reuse one).
//! * The `max` rule lives inside [`NonceStore::advance`] so a shared store (for example one
//!   Postgres row per key using `GREATEST(last + 1, $1)`) can make it atomic across processes.
//! * Failure to persist is an error; we never hand out an unpersisted nonce.
//!
//! Ordering caveat: strictly increasing at generation time is not enough if two requests are in
//! flight and arrive out of order. The Kraken client therefore serialises "allocate nonce + send"
//! per key (see `KrakenAdapter`).
//!
//! A key must be dedicated to one executor: any other software signing with the same key and a
//! different nonce scale (for example epoch milliseconds) will collide with these nanosecond nonces.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NonceError {
    #[error("nonce store i/o error: {0}")]
    Io(String),
    #[error("nonce store is corrupt: {0}")]
    Corrupt(String),
    #[error("nonce space exhausted")]
    Exhausted,
}

pub trait Clock: Send + Sync {
    /// Nanoseconds since the Unix epoch.
    fn now_nanos(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_nanos(&self) -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => u64::try_from(d.as_nanos()).unwrap_or(u64::MAX),
            Err(_) => 0, // clock before 1970: the store's last+1 rule still keeps us monotonic
        }
    }
}

/// The core rule: `max(candidate, last + 1)`, never below 1.
pub fn next_after(candidate: u64, last: Option<u64>) -> Result<u64, NonceError> {
    let floor = match last {
        Some(l) => l.checked_add(1).ok_or(NonceError::Exhausted)?,
        None => 1,
    };
    Ok(candidate.max(floor))
}

pub trait NonceStore: Send + Sync {
    /// Atomically compute `max(candidate, last + 1)`, persist it durably, and return it.
    fn advance(&self, candidate: u64) -> Result<u64, NonceError>;
    /// Highest nonce handed out so far, if any.
    fn last(&self) -> Result<Option<u64>, NonceError>;
}

#[derive(Default)]
pub struct InMemoryNonceStore {
    last: Mutex<Option<u64>>,
}

impl InMemoryNonceStore {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_last(last: u64) -> Self {
        Self { last: Mutex::new(Some(last)) }
    }
}

impl NonceStore for InMemoryNonceStore {
    fn advance(&self, candidate: u64) -> Result<u64, NonceError> {
        let mut g = self.last.lock().unwrap_or_else(|e| e.into_inner());
        let n = next_after(candidate, *g)?;
        *g = Some(n);
        Ok(n)
    }
    fn last(&self) -> Result<Option<u64>, NonceError> {
        Ok(*self.last.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

type LockTable = Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>;

fn path_lock(path: &Path) -> Arc<Mutex<()>> {
    static TABLE: OnceLock<LockTable> = OnceLock::new();
    let table = TABLE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut g = table.lock().unwrap_or_else(|e| e.into_inner());
    g.entry(path.to_path_buf()).or_default().clone()
}

/// File-backed store: one small file per API key. Survives restarts.
///
/// Serialises all instances pointing at the same path WITHIN one process. It does NOT protect
/// against two processes sharing the file (use one executor per key, or a database-backed store).
/// Writes go to a temp file, are fsynced, then renamed over the target. A corrupt file is an
/// error, never silently reset.
pub struct FileNonceStore {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl FileNonceStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, NonceError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| NonceError::Io(e.to_string()))?;
            }
        }
        let lock = path_lock(&path);
        Ok(Self { path, lock })
    }

    /// `<dir>/kraken-nonce-<key_id>.txt`, where `key_id` is `KrakenCredentials::key_id()`.
    pub fn for_key(dir: impl AsRef<Path>, key_id: &str) -> Result<Self, NonceError> {
        Self::open(dir.as_ref().join(format!("kraken-nonce-{key_id}.txt")))
    }

    fn read(&self) -> Result<Option<u64>, NonceError> {
        match std::fs::read_to_string(&self.path) {
            Ok(s) => s
                .trim()
                .parse::<u64>()
                .map(Some)
                .map_err(|_| NonceError::Corrupt(format!("{} does not contain a u64", self.path.display()))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(NonceError::Io(e.to_string())),
        }
    }

    fn write(&self, value: u64) -> Result<(), NonceError> {
        use std::io::Write;
        let tmp = self.path.with_extension("tmp");
        let io = |e: std::io::Error| NonceError::Io(e.to_string());
        let mut f = std::fs::File::create(&tmp).map_err(io)?;
        f.write_all(format!("{value}\n").as_bytes()).map_err(io)?;
        f.sync_all().map_err(io)?;
        drop(f);
        std::fs::rename(&tmp, &self.path).map_err(io)
    }
}

impl NonceStore for FileNonceStore {
    fn advance(&self, candidate: u64) -> Result<u64, NonceError> {
        let _g = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let n = next_after(candidate, self.read()?)?;
        self.write(n)?;
        Ok(n)
    }
    fn last(&self) -> Result<Option<u64>, NonceError> {
        let _g = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        self.read()
    }
}

/// Clock + store. Cheap to clone; clones share the same store.
#[derive(Clone)]
pub struct NonceGenerator {
    store: Arc<dyn NonceStore>,
    clock: Arc<dyn Clock>,
}

impl NonceGenerator {
    pub fn new(store: Arc<dyn NonceStore>, clock: Arc<dyn Clock>) -> Self {
        Self { store, clock }
    }
    pub fn with_system_clock(store: Arc<dyn NonceStore>) -> Self {
        Self::new(store, Arc::new(SystemClock))
    }
    /// Next strictly increasing nonce, already persisted.
    pub fn next(&self) -> Result<u64, NonceError> {
        self.store.advance(self.clock.now_nanos())
    }
}
