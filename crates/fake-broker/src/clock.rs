//! A manual clock shared by the exchange, the event log and (optionally) the adapter's nonce
//! generator. It only moves when a test moves it, so runs are deterministic.

use broker_adapters::nonce::Clock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// 2025-09-21T14:00:00Z in nanoseconds since the Unix epoch. An arbitrary but fixed start.
pub const DEFAULT_START_NANOS: u64 = 1_758_463_200_000_000_000;

/// Manual clock. Implements the adapter's [`Clock`], so
/// `NonceGenerator::new(store, broker.clock())` makes the adapter's nonces follow fake time.
#[derive(Debug)]
pub struct FakeClock(AtomicU64);

impl FakeClock {
    pub fn new(start_nanos: u64) -> Self {
        Self(AtomicU64::new(start_nanos))
    }

    pub fn set_nanos(&self, nanos: u64) {
        self.0.store(nanos, Ordering::SeqCst);
    }

    pub fn advance(&self, by: Duration) {
        let n = u64::try_from(by.as_nanos()).expect("duration fits in u64 nanoseconds");
        self.0.fetch_add(n, Ordering::SeqCst);
    }

    pub fn advance_secs(&self, secs: u64) {
        self.advance(Duration::from_secs(secs));
    }

    /// Seconds since the epoch as a float, the shape Kraken uses for `opentm` / `closetm`.
    pub fn now_secs_f64(&self) -> f64 {
        nanos_to_secs(self.now_nanos())
    }
}

impl Default for FakeClock {
    fn default() -> Self {
        Self::new(DEFAULT_START_NANOS)
    }
}

impl Clock for FakeClock {
    fn now_nanos(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Nanoseconds to fractional seconds (timestamps only, never money).
pub fn nanos_to_secs(nanos: u64) -> f64 {
    nanos as f64 / 1e9
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_moves_when_told() {
        let c = FakeClock::new(1_000);
        assert_eq!(c.now_nanos(), 1_000);
        assert_eq!(c.now_nanos(), 1_000);
        c.advance(Duration::from_nanos(5));
        assert_eq!(c.now_nanos(), 1_005);
        c.advance_secs(2);
        assert_eq!(c.now_nanos(), 2_000_001_005);
        c.set_nanos(7);
        assert_eq!(c.now_nanos(), 7);
    }
}
