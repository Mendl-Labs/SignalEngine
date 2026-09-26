//! Bar timestamps and when a bar is complete. All of it calendar-free: it needs the US daylight-saving rule (to read a
//! New York midnight) and nothing else, no holiday table.
//!
//! # Conventions (MASSIVE_API_ENTITLEMENTS section 2, PROBED 2026-09-26)
//! * **Stocks and ETFs, range endpoint:** the daily bar's `t` is MIDNIGHT America/New_York of the session date:
//!   04:00:00.000 UTC in daylight time (EDT), 05:00:00.000 UTC in standard time (EST). The bar's date is that local
//!   date, which is also the UTC date of `t`. (`prev` and `grouped` stamp the session END instead; this source never
//!   calls them, and a stamp that is not a New York midnight is refused, see below.)
//! * **Crypto (and FX):** `t` is 00:00:00.000 UTC of the UTC calendar day.
//!
//! A timestamp that is NOT exactly the documented instant is refused (`Err`), never rounded to a date: a vendor that
//! changed convention must fail loudly instead of shifting every bar by a day.
//!
//! # When is a bar complete? (the rule, stated once)
//! A bar dated `D` is complete iff BOTH hold:
//! 1. **the date rule:** `D` is strictly before the run date `as_of` (the UTC date of the scheduled time), for both
//!    asset classes; and
//! 2. **the clock rule:** the source's own clock says the bar's period is over: `now >= complete_at(D) + settle`.
//!    * crypto: the UTC day `D` ends at 00:00 UTC of `D + 1`;
//!    * stocks: the regular session ends at 16:00 New York on `D` (an upper bound: an early close is earlier), plus
//!      the plan's 15-minute delay.
//!
//! Anything else is DROPPED, never returned, so the caller cannot see it. The date rule alone would trust the
//! caller's `as_of`; the clock rule catches an `as_of` that is ahead of the real time. `settle` is a configurable
//! margin (default 0) for a vendor that finalises late; how long the real vendor takes after the close is NOT
//! measured (MASSIVE_API_ENTITLEMENTS section 9), which is why the future two-source gate exists.

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Utc};

/// Which timestamp convention a bar series uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarClock {
    /// Midnight America/New_York (stocks, ETFs).
    StockMidnightNewYork,
    /// 00:00 UTC (crypto; FX uses the same).
    MidnightUtc,
}

/// The vendor delay of the stocks plan (Developer: 15-minute delayed).
pub const STOCK_VENDOR_DELAY_MINUTES: i64 = 15;
/// Regular session end, New York local time.
pub const STOCK_SESSION_END_HOUR_NY: u32 = 16;

fn nth_sunday(year: i32, month: u32, n: u32) -> Option<NaiveDate> {
    let first = NaiveDate::from_ymd_opt(year, month, 1)?;
    let to_sunday = (7 - first.weekday().num_days_from_sunday()) % 7;
    NaiveDate::from_ymd_opt(year, month, 1 + to_sunday + 7 * (n - 1))
}

/// Hours to ADD to New York local midnight of `date` to get UTC: 4 in daylight time (EDT), 5 in standard time (EST).
/// The US rule since 2007: daylight time runs from 02:00 on the second Sunday of March to 02:00 on the first Sunday of
/// November, so the MIDNIGHT of the March changeover Sunday is still standard time and the midnight of the November
/// changeover Sunday is still daylight time. `None` before 2007 (a different rule; the vendor's floor is 2016).
pub fn new_york_midnight_utc_offset_hours(date: NaiveDate) -> Option<u32> {
    let year = date.year();
    if year < 2007 {
        return None;
    }
    let start = nth_sunday(year, 3, 2)?;
    let end = nth_sunday(year, 11, 1)?;
    Some(if date > start && date <= end { 4 } else { 5 })
}

/// The session/UTC date of a bar stamped `t_ms` (milliseconds since the epoch) under `clock`, or the reason it is
/// not a stamp of that convention.
pub fn bar_date(clock: BarClock, t_ms: i64) -> Result<NaiveDate, String> {
    let dt = DateTime::<Utc>::from_timestamp_millis(t_ms).ok_or_else(|| format!("timestamp {t_ms} is out of range"))?;
    let date = dt.date_naive();
    match clock {
        BarClock::MidnightUtc => {
            if dt.time() != NaiveTime::MIN {
                return Err(format!("timestamp {t_ms} ({dt}) is not 00:00:00.000 UTC"));
            }
            Ok(date)
        }
        BarClock::StockMidnightNewYork => {
            let off = new_york_midnight_utc_offset_hours(date).ok_or_else(|| format!("timestamp {t_ms} ({dt}): year not supported by the New York rule"))?;
            let expected = date.and_hms_opt(off, 0, 0).ok_or_else(|| "invalid time".to_string())?.and_utc();
            if dt != expected {
                return Err(format!("timestamp {t_ms} ({dt}) is not midnight New York of {date} ({expected})"));
            }
            Ok(date)
        }
    }
}

/// The instant at which the bar dated `date` is over (before any settle margin); `None` if it cannot be computed.
pub fn bar_complete_at(clock: BarClock, date: NaiveDate) -> Option<DateTime<Utc>> {
    match clock {
        BarClock::MidnightUtc => Some(date.checked_add_signed(Duration::days(1))?.and_time(NaiveTime::MIN).and_utc()),
        BarClock::StockMidnightNewYork => {
            let off = new_york_midnight_utc_offset_hours(date)?;
            let session_end = date.and_hms_opt(STOCK_SESSION_END_HOUR_NY + off, 0, 0)?.and_utc();
            session_end.checked_add_signed(Duration::minutes(STOCK_VENDOR_DELAY_MINUTES))
        }
    }
}

/// The completeness rule of the module docs for one bar.
pub fn is_complete(clock: BarClock, date: NaiveDate, as_of: NaiveDate, now: DateTime<Utc>, settle: std::time::Duration) -> bool {
    if date >= as_of {
        return false;
    }
    let Some(over) = bar_complete_at(clock, date) else { return false };
    let Ok(settle) = Duration::from_std(settle) else { return false };
    let Some(ready) = over.checked_add_signed(settle) else { return false };
    now >= ready
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Weekday;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    fn ms(date: NaiveDate, h: u32, mi: u32) -> i64 {
        date.and_hms_opt(h, mi, 0).unwrap().and_utc().timestamp_millis()
    }

    #[test]
    fn dst_boundaries_2026() {
        // 2026: daylight time starts Sunday 8 March and ends Sunday 1 November.
        assert_eq!(new_york_midnight_utc_offset_hours(d(2026, 3, 7)), Some(5));
        assert_eq!(new_york_midnight_utc_offset_hours(d(2026, 3, 8)), Some(5), "midnight of the changeover Sunday is still EST");
        assert_eq!(new_york_midnight_utc_offset_hours(d(2026, 3, 9)), Some(4));
        assert_eq!(new_york_midnight_utc_offset_hours(d(2026, 10, 30)), Some(4));
        assert_eq!(new_york_midnight_utc_offset_hours(d(2026, 11, 1)), Some(4), "midnight of the November Sunday is still EDT");
        assert_eq!(new_york_midnight_utc_offset_hours(d(2026, 11, 2)), Some(5));
        assert_eq!(new_york_midnight_utc_offset_hours(d(2006, 6, 1)), None);
    }

    #[test]
    fn nth_sunday_is_a_sunday() {
        for y in 2007..2040 {
            for (m, n) in [(3, 2), (11, 1)] {
                let s = nth_sunday(y, m, n).unwrap();
                assert_eq!(s.weekday(), Weekday::Sun, "{s}");
                assert!(s.day() <= 7 * n, "{s}");
            }
        }
    }

    #[test]
    fn stock_stamps_read_the_new_york_date() {
        // EDT: the SPY 2026-09-15 bar the entitlements note quotes is 1789444800000 = 04:00 UTC.
        assert_eq!(bar_date(BarClock::StockMidnightNewYork, 1_789_444_800_000), Ok(d(2026, 9, 15)));
        assert_eq!(bar_date(BarClock::StockMidnightNewYork, ms(d(2026, 1, 15), 5, 0)), Ok(d(2026, 1, 15)), "EST");
        assert_eq!(bar_date(BarClock::StockMidnightNewYork, ms(d(2026, 7, 15), 4, 0)), Ok(d(2026, 7, 15)), "EDT");
    }

    #[test]
    fn stock_stamps_that_are_not_a_new_york_midnight_are_refused() {
        // UTC midnight, the crypto convention
        assert!(bar_date(BarClock::StockMidnightNewYork, ms(d(2026, 7, 15), 0, 0)).is_err());
        // EST offset in summer and EDT offset in winter
        assert!(bar_date(BarClock::StockMidnightNewYork, ms(d(2026, 7, 15), 5, 0)).is_err());
        assert!(bar_date(BarClock::StockMidnightNewYork, ms(d(2026, 1, 15), 4, 0)).is_err());
        // session end (the `prev` / `grouped` convention)
        assert!(bar_date(BarClock::StockMidnightNewYork, ms(d(2026, 7, 15), 20, 0)).is_err());
        // one millisecond off
        assert!(bar_date(BarClock::StockMidnightNewYork, ms(d(2026, 7, 15), 4, 0) + 1).is_err());
    }

    #[test]
    fn crypto_stamps_are_utc_midnight_only() {
        assert_eq!(bar_date(BarClock::MidnightUtc, ms(d(2026, 9, 25), 0, 0)), Ok(d(2026, 9, 25)));
        assert!(bar_date(BarClock::MidnightUtc, ms(d(2026, 9, 25), 4, 0)).is_err());
        assert!(bar_date(BarClock::MidnightUtc, ms(d(2026, 9, 25), 0, 0) + 1).is_err());
        assert!(bar_date(BarClock::MidnightUtc, i64::MAX).is_err());
        assert!(bar_date(BarClock::MidnightUtc, i64::MIN).is_err());
    }

    #[test]
    fn completion_instants() {
        assert_eq!(bar_complete_at(BarClock::MidnightUtc, d(2026, 9, 25)), Some(ms(d(2026, 9, 26), 0, 0)).map(|t| DateTime::from_timestamp_millis(t).unwrap()));
        // EDT: 16:00 New York = 20:00 UTC, plus 15 minutes
        assert_eq!(bar_complete_at(BarClock::StockMidnightNewYork, d(2026, 9, 25)), Some(DateTime::from_timestamp_millis(ms(d(2026, 9, 25), 20, 15)).unwrap()));
        // EST: 21:00 UTC plus 15 minutes
        assert_eq!(bar_complete_at(BarClock::StockMidnightNewYork, d(2026, 12, 15)), Some(DateTime::from_timestamp_millis(ms(d(2026, 12, 15), 21, 15)).unwrap()));
        assert_eq!(bar_complete_at(BarClock::MidnightUtc, NaiveDate::MAX), None);
    }

    #[test]
    fn is_complete_needs_both_the_date_and_the_clock() {
        let now = |d: NaiveDate, h, m| DateTime::from_timestamp_millis(ms(d, h, m)).unwrap();
        let zero = std::time::Duration::ZERO;
        let as_of = d(2026, 9, 26);
        // crypto: yesterday's bar at 00:10Z of the run date
        assert!(is_complete(BarClock::MidnightUtc, d(2026, 9, 25), as_of, now(as_of, 0, 10), zero));
        // today's bar: never, even at 23:59
        assert!(!is_complete(BarClock::MidnightUtc, as_of, as_of, now(as_of, 23, 59), zero));
        // the date rule passes but the clock says the day is not over (as_of ahead of real time)
        assert!(!is_complete(BarClock::MidnightUtc, d(2026, 9, 25), as_of, now(d(2026, 9, 25), 23, 0), zero));
        // settle margin
        assert!(!is_complete(BarClock::MidnightUtc, d(2026, 9, 25), as_of, now(as_of, 0, 10), std::time::Duration::from_secs(20 * 60)));
        assert!(is_complete(BarClock::MidnightUtc, d(2026, 9, 25), as_of, now(as_of, 0, 20), std::time::Duration::from_secs(20 * 60)));
        // stocks: Friday 2026-09-25 session at 00:10Z Saturday, and not yet at 20:00Z on the day itself
        assert!(is_complete(BarClock::StockMidnightNewYork, d(2026, 9, 25), as_of, now(as_of, 0, 10), zero));
        assert!(!is_complete(BarClock::StockMidnightNewYork, d(2026, 9, 25), d(2026, 9, 26), now(d(2026, 9, 25), 20, 10), zero));
    }
}
