//! R47 (HOLD policy): tell a person when an account the rebalancer EXCLUDES still holds positions.
//!
//! An excluded account (plan missing, ambiguous, bound to another mandate, entry-hash mismatch, ... any
//! `ExclusionReason`) is skipped by the rebalancer: no ladder, daily-loss limit or reconciliation runs on what it
//! holds. The owner's decision is to HOLD: the positions stay under the old limits, new orders are blocked, and the
//! platform ALERTS on a schedule (at first sight, then 1, 7 and 30 days later) showing the current drawdown. It
//! NEVER sells, cancels or places anything: this module only READS the last stored broker snapshot and the stored
//! risk state, and WRITES one `rebalancer_alerts` row per due bucket.
//!
//! * Is there something to alert about? The account's last stored snapshot (`RunStore::last_snapshot`, the
//!   post-run snapshot of its most recent finished run) lists holdings and open order ids. A snapshot with neither is
//!   FLAT: no alert. No snapshot at all (or an unreadable store) is "positions unknown" and DOES alert: an excluded
//!   account is never silently skipped. (There is no live broker probe: an excluded account has no runtime and the
//!   credential broker is not asked for it.)
//! * First seen: no new table. The first alert of an exclusion is the anchor; its `at` is read back from
//!   `rebalancer_alerts` (`dedupe_key` prefix per account and reason). So the schedule survives restarts.
//! * Dedupe: the key is `excluded-positions|<account>|<reason code>|d<bucket>` with bucket in {0, 1, 7, 30}; each
//!   key is written once. A different reason is a different exclusion with its own schedule.
//!
//! Everything here is pure or goes through the [`ExclusionIo`] seam; the Postgres adapter is in `main.rs`.

use chrono::{DateTime, Duration, Utc};
use rebalancer_run::record::{Alert, AlertCode, AlertSeverity, SnapshotSummary};
use rebalancer_store::Exclusion;
use uuid::Uuid;

/// The escalation schedule, in days after the exclusion was first seen: the first alert (0), then 1, 7 and 30.
pub const ESCALATION_DAYS: [i64; 4] = [0, 1, 7, 30];

/// The largest escalation bucket whose day has been reached: `age < 1d` is 0, `1d..7d` is 1, `7d..30d` is 7, `30d+`
/// is 30. A negative age (clock step backwards) is bucket 0.
pub fn escalation_bucket(age: Duration) -> i64 {
    ESCALATION_DAYS.iter().rev().copied().find(|d| age >= Duration::days(*d)).unwrap_or(0)
}

/// Every dedupe key of one (account, reason) exclusion starts with this.
pub fn dedupe_prefix(account_id: &str, reason_code: &str) -> String {
    format!("excluded-positions|{}|{}|", account_id.trim(), reason_code.trim())
}

pub fn dedupe_key(account_id: &str, reason_code: &str, bucket: i64) -> String {
    format!("{}d{bucket}", dedupe_prefix(account_id, reason_code))
}

/// What the stored facts say the account holds.
#[derive(Debug, Clone, PartialEq)]
pub enum Positions {
    /// A snapshot exists and shows neither holdings nor open orders: nothing to alert about.
    Flat,
    /// Holdings (symbol, quantity text) and/or open orders, as of `as_of`, with the equity then.
    Held { holdings: Vec<(String, String)>, open_orders: usize, equity: String, equity_f64: f64, as_of: DateTime<Utc> },
    /// No snapshot on record, or the store could not be read: the alert says so.
    Unknown { why: String },
}

/// Classify a stored snapshot. `Err` from the store is `Unknown` carrying the reason; it is never `Flat`.
pub fn classify(snapshot: Result<Option<SnapshotSummary>, String>) -> Positions {
    match snapshot {
        Err(e) => Positions::Unknown { why: format!("the run store could not be read ({e})") },
        Ok(None) => Positions::Unknown { why: "no broker snapshot is on record for this account, and no live check is possible for an excluded account".to_string() },
        Ok(Some(s)) => {
            let holdings: Vec<(String, String)> = s.holdings.iter().filter(|(_, q)| q.to_f64() != 0.0).map(|(sym, q)| (sym.clone(), q.to_string())).collect();
            let open_orders = s.open_order_ids.len();
            if holdings.is_empty() && open_orders == 0 {
                Positions::Flat
            } else {
                Positions::Held { holdings, open_orders, equity: s.equity.to_string(), equity_f64: s.equity.to_f64(), as_of: s.taken_at }
            }
        }
    }
}

/// `(hwm - equity) / hwm` as a percentage, floored at 0. `None` without a positive high-water mark.
pub fn drawdown_pct(hwm: Option<f64>, equity: f64) -> Option<f64> {
    let hwm = hwm.filter(|h| h.is_finite() && *h > 0.0)?;
    if !equity.is_finite() {
        return None;
    }
    Some((((hwm - equity) / hwm) * 100.0).max(0.0))
}

/// What the alert says is held, in one line.
pub fn holdings_line(p: &Positions) -> String {
    match p {
        Positions::Flat => "no positions".to_string(),
        Positions::Unknown { why } => format!("positions unknown: {why}"),
        Positions::Held { holdings, open_orders, .. } => {
            const SHOWN: usize = 10;
            let mut parts: Vec<String> = holdings.iter().take(SHOWN).map(|(s, q)| format!("{s} {q}")).collect();
            if holdings.len() > SHOWN {
                parts.push(format!("and {} more", holdings.len() - SHOWN));
            }
            let held = if parts.is_empty() { "no positions".to_string() } else { format!("positions {}", parts.join(", ")) };
            format!("{held}; {open_orders} open order(s)")
        }
    }
}

/// The alert message: plain English, no secret. The "What to do" footer is added by the e-mail template.
pub fn message(account_id: &str, reason_code: &str, positions: &Positions, drawdown: Option<f64>, hwm: Option<f64>, first_seen: DateTime<Utc>, bucket: i64) -> String {
    let dd = match (drawdown, positions) {
        (Some(d), Positions::Held { equity, .. }) => format!("Current drawdown: {d:.2}% (equity {equity}, high-water mark {})", hwm.map(|h| format!("{h:.2}")).unwrap_or_else(|| "unknown".into())),
        (None, Positions::Held { equity, .. }) => format!("Current drawdown: unknown (equity {equity}; no high-water mark is on record)"),
        _ => "Current drawdown: unknown (no equity on record)".to_string(),
    };
    let as_of = match positions {
        Positions::Held { as_of, .. } => format!(" as of {}", as_of.to_rfc3339()),
        _ => String::new(),
    };
    let notice = if bucket == 0 { "first notice".to_string() } else { format!("{bucket}-day reminder") };
    format!(
        "ACCOUNT_EXCLUDED_WITH_POSITIONS: account {account_id} is excluded from the rebalancer ({reason_code}) while it holds {}{as_of}. \
         The positions are being HELD under the old limits, not sold; new orders are blocked and no risk ladder, daily-loss limit or reconciliation runs for this account. \
         {dd}. Excluded since {} ({notice}; reminders at 1, 7 and 30 days).",
        holdings_line(positions),
        first_seen.to_rfc3339()
    )
}

/// Given the alerts already stored for (account, reason) as `(dedupe_key, at)`, which bucket is due now, and when
/// was the exclusion first seen? `None` when the due bucket was already raised.
pub fn due_bucket(account_id: &str, reason_code: &str, existing: &[(String, DateTime<Utc>)], now: DateTime<Utc>) -> Option<(i64, DateTime<Utc>)> {
    let first_seen = existing.iter().map(|(_, at)| *at).min().unwrap_or(now);
    let bucket = escalation_bucket(now - first_seen);
    let key = dedupe_key(account_id, reason_code, bucket);
    if existing.iter().any(|(k, _)| *k == key) {
        None
    } else {
        Some((bucket, first_seen))
    }
}

/// Build the stored alert for one due bucket.
pub fn build_alert(account_id: &str, reason_code: &str, bucket: i64, message: String, now: DateTime<Utc>) -> Alert {
    Alert {
        code: AlertCode::AccountExcludedWithPositions,
        severity: AlertSeverity::Critical,
        account_id: account_id.to_string(),
        run_key: format!("excluded|{reason_code}|d{bucket}"),
        message,
        dedupe_key: dedupe_key(account_id, reason_code, bucket),
        at: now,
    }
}

/// The reads and the one write this check needs.
pub trait ExclusionIo {
    /// The account's last stored broker snapshot (`RunStore::last_snapshot`).
    fn last_snapshot(&self, account_id: &str) -> Result<Option<SnapshotSummary>, String>;
    /// The account's stored high-water mark, if any.
    fn high_water_mark(&self, account_id: &str) -> Option<f64>;
    /// Alerts already stored for this account and code whose `dedupe_key` starts with `prefix`: `(dedupe_key, at)`.
    fn prior_alerts(&self, account_id: &str, prefix: &str) -> Result<Vec<(String, DateTime<Utc>)>, String>;
    /// Persist one alert under `tenant_id`.
    fn raise(&self, tenant_id: Uuid, alert: &Alert) -> Result<(), String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// An alert for this bucket was written.
    Raised { bucket: i64 },
    /// The due bucket was raised on an earlier check.
    AlreadyRaised,
    /// The account holds nothing: no alert.
    Flat,
    /// The alert could not be read or written; logged by the caller, retried on the next check.
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checked {
    pub account_id: String,
    pub reason_code: String,
    pub outcome: Outcome,
}

/// Check every excluded account. Never places, cancels or changes anything at a broker; one account's failure
/// never stops another.
pub fn check_excluded(io: &dyn ExclusionIo, exclusions: &[Exclusion], now: DateTime<Utc>) -> Vec<Checked> {
    exclusions
        .iter()
        .map(|ex| {
            let account_id = ex.account_id.to_string();
            let reason_code = ex.reason.code().to_string();
            let outcome = check_one(io, ex.tenant_id, &account_id, &reason_code, now);
            Checked { account_id, reason_code, outcome }
        })
        .collect()
}

fn check_one(io: &dyn ExclusionIo, tenant_id: Uuid, account_id: &str, reason_code: &str, now: DateTime<Utc>) -> Outcome {
    let positions = classify(io.last_snapshot(account_id));
    if positions == Positions::Flat {
        return Outcome::Flat;
    }
    let existing = match io.prior_alerts(account_id, &dedupe_prefix(account_id, reason_code)) {
        Ok(e) => e,
        Err(e) => return Outcome::Failed(format!("could not read the earlier alerts: {e}")),
    };
    let Some((bucket, first_seen)) = due_bucket(account_id, reason_code, &existing, now) else {
        return Outcome::AlreadyRaised;
    };
    let hwm = io.high_water_mark(account_id);
    let drawdown = match &positions {
        Positions::Held { equity_f64, .. } => drawdown_pct(hwm, *equity_f64),
        _ => None,
    };
    let alert = build_alert(account_id, reason_code, bucket, message(account_id, reason_code, &positions, drawdown, hwm, first_seen, bucket), now);
    match io.raise(tenant_id, &alert) {
        Ok(()) => Outcome::Raised { bucket },
        Err(e) => Outcome::Failed(format!("could not persist the alert: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_adapters::Dec;
    use chrono::TimeZone;
    use rebalancer_store::ExclusionReason;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 1, 15, 0, 0).unwrap()
    }

    fn snap(holdings: &[(&str, &str)], orders: usize) -> SnapshotSummary {
        SnapshotSummary {
            taken_at: t0(),
            equity: Dec::parse("90000").unwrap(),
            cash: Dec::parse("1000").unwrap(),
            derived_equity: Dec::parse("90000").unwrap(),
            holdings: holdings.iter().map(|(s, q)| (s.to_string(), Dec::parse(q).unwrap())).collect::<BTreeMap<_, _>>(),
            marks: BTreeMap::new(),
            open_order_ids: (0..orders).map(|i| format!("o{i}")).collect(),
        }
    }

    #[test]
    fn the_bucket_is_the_largest_schedule_day_reached() {
        assert_eq!(escalation_bucket(Duration::seconds(-5)), 0);
        assert_eq!(escalation_bucket(Duration::zero()), 0);
        assert_eq!(escalation_bucket(Duration::hours(23)), 0);
        assert_eq!(escalation_bucket(Duration::days(1)), 1);
        assert_eq!(escalation_bucket(Duration::days(6) + Duration::hours(23)), 1);
        assert_eq!(escalation_bucket(Duration::days(7)), 7);
        assert_eq!(escalation_bucket(Duration::days(29)), 7);
        assert_eq!(escalation_bucket(Duration::days(30)), 30);
        assert_eq!(escalation_bucket(Duration::days(400)), 30);
    }

    #[test]
    fn the_dedupe_key_is_stable_and_per_account_reason_and_bucket() {
        assert_eq!(dedupe_key("acct", "PILOT_NO_PLAN", 7), "excluded-positions|acct|PILOT_NO_PLAN|d7");
        assert_eq!(dedupe_key("acct", "PILOT_NO_PLAN", 7), dedupe_key(" acct ", " PILOT_NO_PLAN ", 7));
        assert_ne!(dedupe_key("acct", "PILOT_NO_PLAN", 7), dedupe_key("acct", "PILOT_NO_PLAN", 30));
        assert_ne!(dedupe_key("acct", "PILOT_NO_PLAN", 7), dedupe_key("acct", "PILOT_ENTRY_MISMATCH", 7));
        assert_ne!(dedupe_key("a", "PILOT_NO_PLAN", 1), dedupe_key("b", "PILOT_NO_PLAN", 1));
        assert!(dedupe_key("acct", "PILOT_NO_PLAN", 1).starts_with(&dedupe_prefix("acct", "PILOT_NO_PLAN")));
    }

    #[test]
    fn the_schedule_raises_at_first_sight_then_once_each_at_1_7_and_30_days_and_not_every_tick() {
        let (acct, reason) = ("acct", "PILOT_NO_PLAN");
        let mut stored: Vec<(String, DateTime<Utc>)> = Vec::new();
        // (days after first sight, expected bucket raised or None)
        let steps: [(Duration, Option<i64>); 11] = [
            (Duration::zero(), Some(0)),
            (Duration::minutes(5), None),
            (Duration::hours(12), None),
            (Duration::days(1), Some(1)),
            (Duration::days(1) + Duration::minutes(5), None),
            (Duration::days(5), None),
            (Duration::days(7), Some(7)),
            (Duration::days(20), None),
            (Duration::days(30), Some(30)),
            (Duration::days(31), None),
            (Duration::days(90), None),
        ];
        for (after, want) in steps {
            let now = t0() + after;
            let got = due_bucket(acct, reason, &stored, now);
            assert_eq!(got.map(|(b, _)| b), want, "after {after:?}");
            if let Some((b, first)) = got {
                assert_eq!(first, if stored.is_empty() { now } else { t0() }, "the anchor is the first alert's time");
                stored.push((dedupe_key(acct, reason, b), now));
            }
        }
        assert_eq!(stored.len(), 4);
    }

    #[test]
    fn a_service_down_for_a_week_raises_the_current_bucket_only() {
        let stored = vec![(dedupe_key("a", "R", 0), t0())];
        let now = t0() + Duration::days(8);
        assert_eq!(due_bucket("a", "R", &stored, now), Some((7, t0())));
    }

    #[test]
    fn a_changed_reason_starts_its_own_schedule() {
        let stored = vec![(dedupe_key("a", "PILOT_NO_PLAN", 0), t0())];
        // the caller reads only the new reason's prefix, so nothing is stored for it
        let now = t0() + Duration::days(10);
        assert_eq!(due_bucket("a", "PILOT_ENTRY_MISMATCH", &[], now), Some((0, now)));
        assert_eq!(due_bucket("a", "PILOT_NO_PLAN", &stored, now), Some((7, t0())));
    }

    #[test]
    fn classify_separates_flat_held_and_unknown() {
        assert_eq!(classify(Ok(Some(snap(&[], 0)))), Positions::Flat);
        assert_eq!(classify(Ok(Some(snap(&[("SPY", "0")], 0)))), Positions::Flat, "a zero quantity is not a position");
        match classify(Ok(Some(snap(&[("SPY", "10"), ("QQQ", "0")], 2)))) {
            Positions::Held { holdings, open_orders, .. } => {
                assert_eq!(holdings.len(), 1);
                assert_eq!(holdings[0].0, "SPY");
                assert_eq!(open_orders, 2);
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(classify(Ok(Some(snap(&[], 1)))), Positions::Held { .. }), "an open order alone is not flat");
        assert!(matches!(classify(Ok(None)), Positions::Unknown { .. }));
        assert!(matches!(classify(Err("db down".into())), Positions::Unknown { .. }), "an unreadable store is never read as flat");
    }

    #[test]
    fn drawdown_is_a_floored_percentage_and_unknown_without_a_high_water_mark() {
        assert_eq!(drawdown_pct(Some(100_000.0), 90_000.0), Some(10.0));
        assert_eq!(drawdown_pct(Some(100_000.0), 110_000.0), Some(0.0));
        assert_eq!(drawdown_pct(None, 90_000.0), None);
        assert_eq!(drawdown_pct(Some(0.0), 90_000.0), None);
        assert_eq!(drawdown_pct(Some(f64::NAN), 90_000.0), None);
    }

    #[test]
    fn the_message_says_held_not_sold_shows_the_drawdown_and_unknown_positions_say_so() {
        let held = classify(Ok(Some(snap(&[("SPY", "10")], 1))));
        let m = message("acct", "PILOT_NO_PLAN", &held, Some(10.0), Some(100_000.0), t0(), 7);
        for want in ["ACCOUNT_EXCLUDED_WITH_POSITIONS", "PILOT_NO_PLAN", "SPY 10", "1 open order(s)", "HELD", "not sold", "Current drawdown: 10.00%", "7-day reminder"] {
            assert!(m.contains(want), "{want} missing from: {m}");
        }
        let unknown = classify(Ok(None));
        let m = message("acct", "PILOT_NO_PLAN", &unknown, None, None, t0(), 0);
        assert!(m.contains("positions unknown"), "{m}");
        assert!(m.contains("first notice"), "{m}");
        assert!(m.contains("Current drawdown: unknown"), "{m}");
    }

    #[test]
    fn the_alert_is_critical_with_the_stable_code_and_key() {
        let a = build_alert("acct", "PILOT_NO_PLAN", 1, "m".into(), t0());
        assert_eq!(a.code.as_str(), "ACCOUNT_EXCLUDED_WITH_POSITIONS");
        assert_eq!(a.severity, AlertSeverity::Critical);
        assert_eq!(a.dedupe_key, "excluded-positions|acct|PILOT_NO_PLAN|d1");
        assert_eq!(a.at, t0());
    }

    // --- the orchestrator, against a fake store -------------------------------------------------------------

    struct Fake {
        snapshot: Result<Option<SnapshotSummary>, String>,
        hwm: Option<f64>,
        stored: RefCell<Vec<(String, DateTime<Utc>)>>,
        raised: RefCell<Vec<(Uuid, Alert)>>,
        fail_write: bool,
    }

    impl Fake {
        fn new(snapshot: Result<Option<SnapshotSummary>, String>) -> Self {
            Fake { snapshot, hwm: Some(100_000.0), stored: RefCell::new(vec![]), raised: RefCell::new(vec![]), fail_write: false }
        }
    }

    impl ExclusionIo for Fake {
        fn last_snapshot(&self, _: &str) -> Result<Option<SnapshotSummary>, String> {
            self.snapshot.clone()
        }
        fn high_water_mark(&self, _: &str) -> Option<f64> {
            self.hwm
        }
        fn prior_alerts(&self, _: &str, prefix: &str) -> Result<Vec<(String, DateTime<Utc>)>, String> {
            Ok(self.stored.borrow().iter().filter(|(k, _)| k.starts_with(prefix)).cloned().collect())
        }
        fn raise(&self, tenant_id: Uuid, alert: &Alert) -> Result<(), String> {
            if self.fail_write {
                return Err("insert refused".into());
            }
            self.stored.borrow_mut().push((alert.dedupe_key.clone(), alert.at));
            self.raised.borrow_mut().push((tenant_id, alert.clone()));
            Ok(())
        }
    }

    fn exclusion() -> Exclusion {
        Exclusion { tenant_id: Uuid::from_u128(0xA), account_id: Uuid::from_u128(0x100), reason: ExclusionReason::NoPilotPlan }
    }

    #[test]
    fn an_excluded_account_with_positions_alerts_once_per_bucket_to_its_tenant() {
        let io = Fake::new(Ok(Some(snap(&[("SPY", "10")], 0))));
        let ex = [exclusion()];
        let first = check_excluded(&io, &ex, t0());
        assert_eq!(first[0].outcome, Outcome::Raised { bucket: 0 });
        assert_eq!(first[0].reason_code, "PILOT_NO_PLAN");
        // every later tick of the same day: nothing new
        for m in [1, 5, 300] {
            assert_eq!(check_excluded(&io, &ex, t0() + Duration::minutes(m))[0].outcome, Outcome::AlreadyRaised);
        }
        assert_eq!(check_excluded(&io, &ex, t0() + Duration::days(1))[0].outcome, Outcome::Raised { bucket: 1 });
        assert_eq!(check_excluded(&io, &ex, t0() + Duration::days(7))[0].outcome, Outcome::Raised { bucket: 7 });
        assert_eq!(check_excluded(&io, &ex, t0() + Duration::days(30))[0].outcome, Outcome::Raised { bucket: 30 });
        let raised = io.raised.borrow();
        assert_eq!(raised.len(), 4);
        assert!(raised.iter().all(|(t, a)| *t == Uuid::from_u128(0xA) && a.severity == AlertSeverity::Critical));
        assert!(raised[0].1.message.contains("Current drawdown: 10.00%"), "{}", raised[0].1.message);
    }

    #[test]
    fn a_flat_account_raises_nothing_but_an_unknown_one_still_does() {
        let flat = Fake::new(Ok(Some(snap(&[], 0))));
        assert_eq!(check_excluded(&flat, &[exclusion()], t0())[0].outcome, Outcome::Flat);
        assert!(flat.raised.borrow().is_empty());

        let unknown = Fake::new(Ok(None));
        assert_eq!(check_excluded(&unknown, &[exclusion()], t0())[0].outcome, Outcome::Raised { bucket: 0 });
        assert!(unknown.raised.borrow()[0].1.message.contains("positions unknown"));

        let unreadable = Fake::new(Err("store down".into()));
        assert_eq!(check_excluded(&unreadable, &[exclusion()], t0())[0].outcome, Outcome::Raised { bucket: 0 });
    }

    #[test]
    fn a_failed_write_is_reported_and_retried_on_the_next_check() {
        let mut io = Fake::new(Ok(Some(snap(&[("SPY", "10")], 0))));
        io.fail_write = true;
        assert!(matches!(check_excluded(&io, &[exclusion()], t0())[0].outcome, Outcome::Failed(_)));
        io.fail_write = false;
        assert_eq!(check_excluded(&io, &[exclusion()], t0() + Duration::minutes(5))[0].outcome, Outcome::Raised { bucket: 0 });
    }
}
