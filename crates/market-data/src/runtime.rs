//! The impure edges of the source, behind traits so tests never wait and never touch a real clock: the time of day,
//! sleeping between retries, jitter, and the retry / request-budget policies built on them.

use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};

/// Wall-clock time and sleeping. Production: [`SystemClock`]. Tests: `testing::ManualMarketClock`, whose `sleep`
/// only records the delay and moves its own time forward.
pub trait MarketClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
    fn sleep(&self, d: Duration);
}

/// The real clock (`SystemTime`, `thread::sleep`). Needs no chrono `clock` feature.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl MarketClock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        DateTime::<Utc>::from_timestamp(i64::try_from(d.as_secs()).unwrap_or(i64::MAX), d.subsec_nanos()).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
    }

    fn sleep(&self, d: Duration) {
        std::thread::sleep(d);
    }
}

/// A random-ish factor for backoff jitter. Whatever it returns is clamped into `[0.5, 1.0]` by
/// [`RetryPolicy::delay_after`], so a broken implementation can never lengthen a delay past the cap.
pub trait Jitter: Send + Sync {
    fn factor(&self) -> f64;
}

/// Xorshift jitter seeded from the system clock (no `rand` dependency; jitter needs spread, not unpredictability).
#[derive(Debug)]
pub struct SystemJitter {
    state: Mutex<u64>,
}

impl Default for SystemJitter {
    fn default() -> Self {
        let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0x9E37_79B9_7F4A_7C15);
        Self { state: Mutex::new(seed | 1) }
    }
}

impl Jitter for SystemJitter {
    fn factor(&self) -> f64 {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut x = *s;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *s = x;
        0.5 + 0.5 * ((x >> 11) as f64 / (1u64 << 53) as f64)
    }
}

/// Bounded exponential backoff: after the k-th failed attempt wait `min(max_delay, base_delay * 2^(k-1))`, scaled by
/// a jitter factor in `[0.5, 1.0]`; give up after `max_attempts` attempts in total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts per request, first try included (1 = never retry).
    pub max_attempts: u32,
    pub base_delay: Duration,
    /// Hard cap on one wait, jitter included.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { max_attempts: 4, base_delay: Duration::from_millis(500), max_delay: Duration::from_secs(8) }
    }
}

impl RetryPolicy {
    /// The wait after the `failed_attempt`-th failed attempt (1-based).
    pub fn delay_after(&self, failed_attempt: u32, jitter_factor: f64) -> Duration {
        let exp = failed_attempt.saturating_sub(1).min(30);
        let raw = self.base_delay.saturating_mul(1u32 << exp);
        let capped = raw.min(self.max_delay);
        let f = if jitter_factor.is_finite() { jitter_factor.clamp(0.5, 1.0) } else { 1.0 };
        capped.mul_f64(f)
    }
}

/// A cap on requests per tick. The driver has no tick hook, so a "tick" is a fixed time window on the injected clock:
/// at most `max_requests` requests (retries and later pages included) per `window`. When it is spent the source
/// refuses locally with `RateLimited { local_budget: true }` instead of hammering a vendor that may be throttling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetConfig {
    pub max_requests: u32,
    pub window: Duration,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        // One ETF sleeve is 5 requests and one crypto sleeve 2; 40 leaves room for retries and several kinds.
        Self { max_requests: 40, window: Duration::from_secs(60) }
    }
}

#[derive(Debug)]
pub(crate) struct Budget {
    cfg: BudgetConfig,
    state: Mutex<Option<(DateTime<Utc>, u32)>>,
}

impl Budget {
    pub(crate) fn new(cfg: BudgetConfig) -> Self {
        Self { cfg, state: Mutex::new(None) }
    }

    /// Take one request from the budget at `now`; `false` = spent.
    pub(crate) fn take(&self, now: DateTime<Utc>) -> bool {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let window = chrono::Duration::from_std(self.cfg.window).unwrap_or(chrono::Duration::MAX);
        let (start, used) = match *st {
            // A new window starts when the old one has elapsed, or when the clock went backwards.
            Some((start, used)) if now >= start && now.signed_duration_since(start) < window => (start, used),
            _ => (now, 0),
        };
        if used >= self.cfg.max_requests {
            *st = Some((start, used));
            return false;
        }
        *st = Some((start, used + 1));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delays_double_then_hit_the_cap() {
        let p = RetryPolicy { max_attempts: 9, base_delay: Duration::from_secs(1), max_delay: Duration::from_secs(5) };
        let d: Vec<u64> = (1..=6).map(|k| p.delay_after(k, 1.0).as_millis() as u64).collect();
        assert_eq!(d, vec![1000, 2000, 4000, 5000, 5000, 5000]);
    }

    #[test]
    fn jitter_is_clamped_and_never_exceeds_the_cap() {
        let p = RetryPolicy { max_attempts: 9, base_delay: Duration::from_secs(1), max_delay: Duration::from_secs(5) };
        assert_eq!(p.delay_after(1, 0.0), Duration::from_millis(500));
        assert_eq!(p.delay_after(1, 7.0), Duration::from_secs(1));
        assert_eq!(p.delay_after(1, f64::NAN), Duration::from_secs(1));
        assert_eq!(p.delay_after(40, 1.0), Duration::from_secs(5));
        assert_eq!(p.delay_after(u32::MAX, 1.0), Duration::from_secs(5));
        assert_eq!(p.delay_after(0, 1.0), Duration::from_secs(1));
    }

    #[test]
    fn system_jitter_stays_in_range() {
        let j = SystemJitter::default();
        for _ in 0..1000 {
            let f = j.factor();
            assert!((0.5..1.0).contains(&f), "{f}");
        }
    }

    #[test]
    fn budget_counts_per_window_and_resets() {
        let b = Budget::new(BudgetConfig { max_requests: 2, window: Duration::from_secs(60) });
        let t0 = DateTime::<Utc>::from_timestamp(1_800_000_000, 0).unwrap();
        assert!(b.take(t0));
        assert!(b.take(t0 + chrono::Duration::seconds(10)));
        assert!(!b.take(t0 + chrono::Duration::seconds(59)));
        assert!(b.take(t0 + chrono::Duration::seconds(60)), "a new window opens when the old one elapsed");
        assert!(b.take(t0 + chrono::Duration::seconds(61)));
        assert!(!b.take(t0 + chrono::Duration::seconds(62)));
        // a clock that goes backwards starts a fresh window rather than wedging
        assert!(b.take(t0 - chrono::Duration::seconds(5)));
    }
}
