//! The clock the pipeline reads and sleeps on. Injectable so runs are deterministic and tests can script races.
//!
//! Production supplies a real clock (`Utc::now`, `thread::sleep`); nothing in this workspace calls the system clock.

use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};

pub trait Clock {
    fn now(&self) -> DateTime<Utc>;
    /// Wait `secs` seconds (poll back-off). A manual clock advances itself instead of blocking.
    fn sleep_secs(&self, secs: u64);
}

type Hook = Box<dyn FnMut() + Send>;

/// A clock that only moves when told to (or when something sleeps on it). An optional hook runs on every
/// `sleep_secs`, which lets a drill inject an event at a precise point of a poll loop (for example "the pending
/// cancel settles while the pipeline waits").
pub struct ManualClock {
    now: Mutex<DateTime<Utc>>,
    slept: Mutex<u64>,
    hook: Mutex<Option<Hook>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl ManualClock {
    pub fn new(start: DateTime<Utc>) -> Self {
        Self { now: Mutex::new(start), slept: Mutex::new(0), hook: Mutex::new(None) }
    }

    pub fn set(&self, t: DateTime<Utc>) {
        *lock(&self.now) = t;
    }

    pub fn advance_secs(&self, secs: i64) {
        let mut n = lock(&self.now);
        *n += Duration::seconds(secs);
    }

    /// Total seconds slept so far.
    pub fn total_slept_secs(&self) -> u64 {
        *lock(&self.slept)
    }

    /// Run `f` (once per call) whenever something sleeps on this clock.
    pub fn set_sleep_hook(&self, f: impl FnMut() + Send + 'static) {
        *lock(&self.hook) = Some(Box::new(f));
    }

    pub fn clear_sleep_hook(&self) {
        *lock(&self.hook) = None;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *lock(&self.now)
    }

    fn sleep_secs(&self, secs: u64) {
        *lock(&self.slept) += secs;
        self.advance_secs(i64::try_from(secs).unwrap_or(i64::MAX));
        // Take the hook out while it runs so it may itself touch the clock.
        let taken = lock(&self.hook).take();
        if let Some(mut h) = taken {
            h();
            let mut slot = lock(&self.hook);
            if slot.is_none() {
                *slot = Some(h);
            }
        }
    }
}
