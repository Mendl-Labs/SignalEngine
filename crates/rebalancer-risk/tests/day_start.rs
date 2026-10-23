//! The 00:00 UTC day-start anchor (P1-02, `DayStartRule::UtcMidnight`): the first reading at or after 00:00 UTC,
//! inside the anchor window, is the day's start; a later first reading leaves the day with NO start (the daily-loss
//! limit cannot fire and nothing is invented); the anchor is persisted state, so it survives a restart; and the
//! historical `FirstObservation` rule is unchanged.

mod common;

use chrono::NaiveDate;
use common::*;
use rebalancer_risk::overlay::{step, step_with, EquitySnapshot, RiskAction, RiskCode};
use rebalancer_risk::state::{AccountState, AccountStatus, DayStartRule, HaltReason, DEFAULT_DAY_START_MAX_LAG_SECS};

const LAG: i64 = DEFAULT_DAY_START_MAX_LAG_SECS; // 30 minutes
const RULE: DayStartRule = DayStartRule::UtcMidnight { max_lag_secs: LAG };

fn reading(equity: &str, t: &str) -> EquitySnapshot {
    EquitySnapshot::broker_reported("alpaca", "USD", d(equity), at(t))
}

fn date(y: i32, m: u32, dd: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, dd).unwrap()
}

/// Fold readings in, in order, under the 00:00 UTC rule.
fn observe_all(readings: &[(&str, &str)]) -> AccountState {
    readings.iter().fold(AccountState::new("acct"), |s, (e, t)| s.observe_anchored(&reading(e, t), LAG))
}

#[test]
fn the_default_window_is_thirty_minutes_and_the_default_rule_is_the_historical_one() {
    assert_eq!(DEFAULT_DAY_START_MAX_LAG_SECS, 1800);
    assert_eq!(DayStartRule::default(), DayStartRule::FirstObservation);
}

#[test]
fn the_first_reading_at_or_after_midnight_inside_the_window_is_the_day_start() {
    for t in ["2026-10-23T00:00:00Z", "2026-10-23T00:04:59Z", "2026-10-23T00:30:00Z"] {
        let s = observe_all(&[("1000", t)]);
        assert_eq!(s.trading_day(), Some(date(2026, 10, 23)), "{t}");
        assert_eq!(s.day_start_equity(), Some(d("1000")), "{t}: inside the window (inclusive)");
        assert!(!s.day_start_missing(date(2026, 10, 23)));
    }
}

#[test]
fn a_first_reading_after_the_window_leaves_the_day_with_no_start_and_nothing_is_invented() {
    for t in ["2026-10-23T00:30:01Z", "2026-10-23T03:00:00Z", "2026-10-23T15:00:00Z", "2026-10-23T23:59:59Z"] {
        // The previous day's last reading is NOT used as a stand-in, and neither is the late reading itself.
        let s = observe_all(&[("1000", "2026-10-22T23:58:00Z"), ("900", t)]);
        assert_eq!(s.trading_day(), Some(date(2026, 10, 23)), "{t}");
        assert_eq!(s.day_start_equity(), None, "{t}: no anchor");
        assert!(s.day_start_missing(date(2026, 10, 23)), "{t}");
        assert!(!s.day_start_missing(date(2026, 10, 22)), "{t}: the flag is about the recorded day only");
        assert_eq!(s.last_equity(), Some(d("900")), "{t}: the reading itself is still folded in");
    }
}

#[test]
fn a_reading_just_before_midnight_belongs_to_the_previous_day() {
    let s = observe_all(&[("1000", "2026-10-22T23:59:59Z")]);
    assert_eq!(s.trading_day(), Some(date(2026, 10, 22)));
    assert_eq!(s.day_start_equity(), None, "23:59:59 is not inside 00:00 + 30 min of its own date");
    let s = s.observe_anchored(&reading("990", "2026-10-23T00:00:01Z"), LAG);
    assert_eq!((s.trading_day(), s.day_start_equity()), (Some(date(2026, 10, 23)), Some(d("990"))));
}

#[test]
fn later_readings_of_the_same_day_never_move_the_anchor_present_or_missing() {
    let s = observe_all(&[("1000", "2026-10-23T00:02:00Z"), ("1100", "2026-10-23T09:00:00Z"), ("700", "2026-10-23T23:00:00Z")]);
    assert_eq!(s.day_start_equity(), Some(d("1000")));
    let m = observe_all(&[("1000", "2026-10-23T02:00:00Z"), ("1000", "2026-10-23T02:05:00Z"), ("1000", "2026-10-23T23:59:00Z")]);
    assert_eq!(m.day_start_equity(), None, "a missing anchor is never filled in later the same day");
    // The next UTC date anchors afresh.
    let next = m.observe_anchored(&reading("1200", "2026-10-24T00:03:00Z"), LAG);
    assert_eq!((next.trading_day(), next.day_start_equity()), (Some(date(2026, 10, 24)), Some(d("1200"))));
    assert!(!next.day_start_missing(date(2026, 10, 24)));
}

#[test]
fn a_clock_that_goes_backwards_never_re_bases_the_anchor() {
    let s = observe_all(&[("1000", "2026-10-23T00:02:00Z"), ("500", "2026-10-22T00:01:00Z")]);
    assert_eq!((s.trading_day(), s.day_start_equity()), (Some(date(2026, 10, 23)), Some(d("1000"))));
}

#[test]
fn a_negative_window_admits_nothing() {
    let s = AccountState::new("acct").observe_anchored(&reading("1000", "2026-10-23T00:00:00Z"), -1);
    assert!(s.day_start_missing(date(2026, 10, 23)));
}

#[test]
fn the_high_water_mark_and_last_equity_follow_the_historical_rules() {
    let s = observe_all(&[("1000", "2026-10-23T00:02:00Z"), ("1200", "2026-10-23T05:00:00Z"), ("1100", "2026-10-23T06:00:00Z")]);
    assert_eq!(s.hwm(), Some(d("1200")));
    assert_eq!(s.last_equity(), Some(d("1100")));
    assert_eq!(s.last_equity_at(), Some(at("2026-10-23T06:00:00Z")));
    // Frozen while halted, exactly like `observe`.
    let (halted, _) = s.halt(HaltReason::Manual, "test", at("2026-10-23T06:01:00Z"));
    let h2 = halted.observe_anchored(&reading("5000", "2026-10-24T00:01:00Z"), LAG);
    assert_eq!(h2.hwm(), Some(d("1200")));
    assert_eq!(h2.status(), AccountStatus::Halted);
}

// ---------------------------------------------------------------------------------------------------------------
// Restart: the anchor is persisted state
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_restart_mid_day_keeps_the_anchor_and_a_missing_anchor_stays_missing() {
    // Process 1 anchors at 00:03 and stops at 10:00. Process 2 loads the stored record and reads at 14:00.
    let p1 = observe_all(&[("1000", "2026-10-23T00:03:00Z"), ("990", "2026-10-23T10:00:00Z")]);
    let stored = p1.to_record();
    let p2 = AccountState::from_record(stored).expect("a coherent record");
    let p2 = p2.observe_anchored(&reading("980", "2026-10-23T14:00:00Z"), LAG);
    assert_eq!(p2.day_start_equity(), Some(d("1000")), "the restart did not re-anchor");

    // Down across midnight: the restarted process's first reading is at 02:00, outside the window.
    let before = observe_all(&[("1000", "2026-10-22T20:00:00Z")]);
    let restarted = AccountState::from_record(before.to_record()).unwrap().observe_anchored(&reading("1000", "2026-10-23T02:00:00Z"), LAG);
    assert!(restarted.day_start_missing(date(2026, 10, 23)));
    // ... and a second restart later that day still has no anchor (the missing day is persisted too).
    let again = AccountState::from_record(restarted.to_record()).unwrap().observe_anchored(&reading("1000", "2026-10-23T03:00:00Z"), LAG);
    assert!(again.day_start_missing(date(2026, 10, 23)));
}

// ---------------------------------------------------------------------------------------------------------------
// The daily-loss limit on the anchor
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_daily_loss_limit_trips_exactly_at_three_percent_of_the_midnight_value() {
    let p = policy(); // daily loss 3%, shrink at 10%, halt at 20%
    let (s, d0, _) = step_with(&AccountState::new("acct"), &reading("10000", "2026-10-23T00:01:00Z"), date(2026, 10, 23), &p, RULE);
    assert_eq!(d0.action, RiskAction::None);
    // Readings are missing between 00:01 and 15:00 (nothing read for hours): the anchor still measures the day.
    let (s2, near, _) = step_with(&s, &reading("9700.01", "2026-10-23T15:00:00Z"), date(2026, 10, 23), &p, RULE);
    assert_eq!(near.action, RiskAction::None, "2.9999%: no halt");
    assert_eq!(s2.status(), AccountStatus::Active);
    let (s3, trip, _) = step_with(&s2, &reading("9700", "2026-10-23T15:05:00Z"), date(2026, 10, 23), &p, RULE);
    assert_eq!(trip.action, RiskAction::HaltFlatten);
    assert_eq!(trip.halt_reason, Some(HaltReason::DailyLoss));
    assert!(trip.has(RiskCode::DailyLossHalt));
    assert_eq!(s3.status(), AccountStatus::Flattening);
}

#[test]
fn with_no_anchor_the_daily_loss_limit_cannot_fire_but_the_ladder_still_does() {
    let p = policy();
    // Anchor missing for the 23rd (first reading at 06:00).
    let (s, _, _) = step_with(&AccountState::new("acct"), &reading("10000", "2026-10-23T06:00:00Z"), date(2026, 10, 23), &p, RULE);
    assert!(s.day_start_missing(date(2026, 10, 23)));
    // A 5% fall the same day: under a 3% daily limit, but there is no day-start to measure it against.
    let (s2, d5, _) = step_with(&s, &reading("9500", "2026-10-23T09:00:00Z"), date(2026, 10, 23), &p, RULE);
    assert_eq!(d5.action, RiskAction::None, "{:?}", d5.reasons);
    assert!(!d5.has(RiskCode::DailyLossHalt));
    assert_eq!(d5.daily_loss, None, "no daily-loss figure is reported against an invented reference");
    // A 20% fall from the high-water mark halts through the ladder as always.
    let (_, d20, _) = step_with(&s2, &reading("8000", "2026-10-23T10:00:00Z"), date(2026, 10, 23), &p, RULE);
    assert_eq!(d20.halt_reason, Some(HaltReason::DrawdownLadder));
}

#[test]
fn a_crash_overnight_is_measured_from_the_midnight_value_not_the_first_run() {
    // The scenario the old rule could not see: one run a day at 15:00. The midnight reading anchors at 10000; the
    // account falls 4% by 03:00; the run's reading at 15:00 under the old rule would have been its own day-start.
    let p = policy();
    let (s, _, _) = step_with(&AccountState::new("acct"), &reading("10000", "2026-10-23T00:02:00Z"), date(2026, 10, 23), &p, RULE);
    let (_, d, _) = step_with(&s, &reading("9600", "2026-10-23T15:00:00Z"), date(2026, 10, 23), &p, RULE);
    assert_eq!(d.halt_reason, Some(HaltReason::DailyLoss));
    // The historical rule, with only the 15:00 reading of the day, compares it to itself and does not fire.
    let old = AccountState::new("acct").observe(&reading("10000", "2026-10-22T15:00:00Z"), date(2026, 10, 22));
    let (_, d_old, _) = step(&old, &reading("9600", "2026-10-23T15:00:00Z"), date(2026, 10, 23), &p);
    assert_eq!(d_old.action, RiskAction::None);
}

#[test]
fn the_historical_rule_is_unchanged_through_observe_with() {
    let readings = [("1000", "2026-10-23T15:00:00Z", 23u32), ("900", "2026-10-23T16:00:00Z", 23), ("950", "2026-10-24T15:00:00Z", 24), ("940", "2026-10-22T15:00:00Z", 22)];
    let mut a = AccountState::new("acct");
    let mut b = AccountState::new("acct");
    for (e, t, dd) in readings {
        let day = date(2026, 10, dd);
        a = a.observe(&reading(e, t), day);
        b = b.observe_with(&reading(e, t), day, DayStartRule::FirstObservation);
        assert_eq!(a, b, "{t}");
    }
    let p = policy();
    let s = AccountState::new("acct").observe(&reading("10000", "2026-10-23T15:00:00Z"), date(2026, 10, 23));
    assert_eq!(step(&s, &reading("9690", "2026-10-23T16:00:00Z"), date(2026, 10, 23), &p), step_with(&s, &reading("9690", "2026-10-23T16:00:00Z"), date(2026, 10, 23), &p, DayStartRule::FirstObservation));
}

#[test]
fn a_human_resume_re_bases_the_day_start_whatever_the_rule() {
    let s = observe_all(&[("1000", "2026-10-23T03:00:00Z")]);
    assert!(s.day_start_missing(date(2026, 10, 23)));
    let (halted, _) = s.halt(HaltReason::Manual, "test", at("2026-10-23T03:01:00Z"));
    let (resumed, _) = halted.resume(human_approval("reviewed"), &reading("1000", "2026-10-23T04:00:00Z"), date(2026, 10, 23)).unwrap();
    assert_eq!(resumed.day_start_equity(), Some(d("1000")), "the person who resumes sets the reference");
    assert!(!resumed.day_start_missing(date(2026, 10, 23)));
}
