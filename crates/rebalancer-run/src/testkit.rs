//! Small in-memory test doubles for the pipeline's traits. Public so drills in `tests/` (and later services' own
//! tests) can use them; nothing here is used by the pipeline itself.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use rebalancer_core::guard::PricePoint;
use rebalancer_core::Dec;
use reference_rules::Panel;

use crate::data::{DataError, DataSource, SleeveData, SleeveSpec};
use crate::driver::AccountLock;
use crate::record::Alert;
use crate::stores::{KillFlag, Notifier};

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Records every alert; can be told to fail delivery.
#[derive(Default)]
pub struct RecordingNotifier {
    alerts: Mutex<Vec<Alert>>,
    fail: AtomicBool,
}

impl RecordingNotifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn alerts(&self) -> Vec<Alert> {
        lock(&self.alerts).clone()
    }

    pub fn codes(&self) -> Vec<&'static str> {
        lock(&self.alerts).iter().map(|a| a.code.as_str()).collect()
    }

    pub fn count(&self) -> usize {
        lock(&self.alerts).len()
    }

    /// Make delivery fail (the alert is still counted as attempted in the run record).
    pub fn set_failing(&self, failing: bool) {
        self.fail.store(failing, Ordering::SeqCst);
    }
}

impl Notifier for RecordingNotifier {
    fn notify(&self, alert: &Alert) -> Result<(), String> {
        if self.fail.load(Ordering::SeqCst) {
            return Err("sink unavailable".to_string());
        }
        lock(&self.alerts).push(alert.clone());
        Ok(())
    }
}

/// A kill flag you flip by hand.
#[derive(Default)]
pub struct SwitchKillFlag {
    set: AtomicBool,
    unreadable: AtomicBool,
}

impl SwitchKillFlag {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn set(&self, on: bool) {
        self.set.store(on, Ordering::SeqCst);
    }
    pub fn set_unreadable(&self, unreadable: bool) {
        self.unreadable.store(unreadable, Ordering::SeqCst);
    }
}

impl KillFlag for SwitchKillFlag {
    fn is_set(&self) -> Result<bool, String> {
        if self.unreadable.load(Ordering::SeqCst) {
            return Err("kill flag store unreachable".to_string());
        }
        Ok(self.set.load(Ordering::SeqCst))
    }
}

type PriceFn = Box<dyn Fn(&str) -> Option<Dec> + Send + Sync>;

/// Panels by sleeve id plus a price function (for example one that reads the fake exchange's last prices).
pub struct FixtureData {
    panels: Mutex<BTreeMap<String, Panel>>,
    price_fn: Mutex<Option<PriceFn>>,
    error: Mutex<Option<DataError>>,
    price_error: Mutex<Option<DataError>>,
    /// Prices are stamped this many seconds in the past (to test stale-price denials).
    price_lag_secs: Mutex<i64>,
}

impl Default for FixtureData {
    fn default() -> Self {
        Self::new()
    }
}

impl FixtureData {
    pub fn new() -> Self {
        Self {
            panels: Mutex::new(BTreeMap::new()),
            price_fn: Mutex::new(None),
            error: Mutex::new(None),
            price_error: Mutex::new(None),
            price_lag_secs: Mutex::new(0),
        }
    }

    pub fn with_panel(self, sleeve_id: &str, panel: Panel) -> Self {
        lock(&self.panels).insert(sleeve_id.to_string(), panel);
        self
    }

    pub fn with_price_fn(self, f: impl Fn(&str) -> Option<Dec> + Send + Sync + 'static) -> Self {
        *lock(&self.price_fn) = Some(Box::new(f));
        self
    }

    pub fn set_panel(&self, sleeve_id: &str, panel: Panel) {
        lock(&self.panels).insert(sleeve_id.to_string(), panel);
    }

    /// Make every panel request fail.
    pub fn set_error(&self, e: Option<DataError>) {
        *lock(&self.error) = e;
    }

    pub fn set_price_error(&self, e: Option<DataError>) {
        *lock(&self.price_error) = e;
    }

    pub fn set_price_lag_secs(&self, secs: i64) {
        *lock(&self.price_lag_secs) = secs;
    }
}

// ------------------------------------------------------------------------------------------------------------
// AccountLock: an in-process stand-in for the Postgres advisory lock
// ------------------------------------------------------------------------------------------------------------

/// An in-process, `Mutex<BTreeSet>`-backed [`AccountLock`]: `try_lock` refuses (`Ok(None)`) an account id already
/// held by a live guard, and dropping the guard frees it -- the same observable behaviour as the Postgres
/// implementation's session-level advisory lock (held on a dedicated connection, released when the connection
/// drops), without a database. Good enough to test `run_all_due`'s "a second concurrent attempt on the same
/// account is refused, and a crashed/finished attempt's lock frees up" property; it does NOT test cross-PROCESS
/// exclusion (that needs the real Postgres lock, covered by `rebalancer-store`'s own tests).
#[derive(Default)]
pub struct InMemoryAccountLock {
    held: Mutex<BTreeSet<String>>,
}

impl InMemoryAccountLock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_held(&self, account_id: &str) -> bool {
        lock(&self.held).contains(account_id)
    }
}

/// Releases the account's lock when dropped, wherever that drop happens (end of scope, an early `return`, or a
/// caught panic unwinding through it) -- never held past the code that acquired it.
pub struct InMemoryLockGuard<'a> {
    lock: &'a InMemoryAccountLock,
    account_id: String,
}

impl Drop for InMemoryLockGuard<'_> {
    fn drop(&mut self) {
        lock(&self.lock.held).remove(&self.account_id);
    }
}

impl AccountLock for InMemoryAccountLock {
    type Guard<'a> = InMemoryLockGuard<'a>;

    fn try_lock<'a>(&'a self, account_id: &str) -> Result<Option<Self::Guard<'a>>, String> {
        let mut g = lock(&self.held);
        if !g.insert(account_id.to_string()) {
            return Ok(None);
        }
        drop(g);
        Ok(Some(InMemoryLockGuard { lock: self, account_id: account_id.to_string() }))
    }
}

impl DataSource for FixtureData {
    fn sleeve_data(&self, sleeve: &SleeveSpec, _as_of: NaiveDate) -> Result<SleeveData, DataError> {
        if let Some(e) = lock(&self.error).clone() {
            return Err(e);
        }
        lock(&self.panels)
            .get(&sleeve.id)
            .cloned()
            .map(|panel| SleeveData { panel })
            .ok_or_else(|| DataError::new("DATA_UNAVAILABLE", &format!("no panel for sleeve {}", sleeve.id)))
    }

    fn prices(&self, symbols: &[String], now: DateTime<Utc>) -> Result<BTreeMap<String, PricePoint>, DataError> {
        if let Some(e) = lock(&self.price_error).clone() {
            return Err(e);
        }
        let lag = *lock(&self.price_lag_secs);
        let guard = lock(&self.price_fn);
        let Some(f) = guard.as_ref() else { return Ok(BTreeMap::new()) };
        let mut out = BTreeMap::new();
        for s in symbols {
            if let Some(price) = f(s) {
                out.insert(s.clone(), PricePoint { price, as_of: now - Duration::seconds(lag) });
            }
        }
        Ok(out)
    }
}
