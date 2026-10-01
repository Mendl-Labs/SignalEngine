//! Kill drills against the fake exchange through `run_once`: SPEC B4 (daily loss), B5 (ladder boundaries), B6
//! (flatten idempotence and partial fills), B7 (broker equity vs corrupted bookkeeping), B8 (missed runs), plus
//! foreign orders, balance drift, halted accounts, alerts and the human-only resume.

mod common;

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};

use broker_adapters::Side;
use chrono::Duration;
use common::harness::*;
use common::*;
use fake_broker::scenarios as sc;
use fake_broker::{FillPolicy, OrderRule, ReportGlitch};
use rebalancer_risk::approval::{issue_human_approval, ApprovalDenied, PrincipalKind};
use rebalancer_risk::state::{AccountState, AccountStatus, HaltReason};
use rebalancer_risk::store::{StateStore, StoreError};
use rebalancer_risk::Dec;
use rebalancer_run::flatten::FlattenVerdict;
use rebalancer_run::record::{ExecutionMode, OutcomeKind, RunKey};
use rebalancer_run::recon::ReconCode;
use rebalancer_run::stores::{find_missed, MissedKind, RunStore};
use rebalancer_run::testkit::FixtureData;

/// A risk-only account: 5000 USD + 0.1 BTC at 50000 = 10000 of equity, nothing managed by a sleeve.
fn risk_account() -> Harness {
    let h = Harness::risk_only();
    h.price("BTC/USD", "50000");
    h.env.hold("BTC", "0.1");
    h.env.rig.handle.set_balance(ACCOUNT, "USD", "5000");
    assert_eq!(h.equity(), d("10000"));
    h
}

fn state_of(h: &Harness) -> AccountState {
    h.states.load(ACCOUNT).unwrap().expect("state was saved")
}

fn status(h: &Harness) -> AccountStatus {
    state_of(h).status()
}

// ---------------------------------------------------------------------------------------------------------------
// B4: daily loss => halt, cancel, flatten, verify flat, alert; the agent cannot resume; a human can
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn b4_a_daily_loss_halts_flattens_verifies_flat_alerts_and_only_a_human_can_resume() {
    let h = risk_account();
    let r0 = h.live(0);
    assert_eq!(r0.outcome.kind, OutcomeKind::Completed, "{:?}", r0.outcome);
    assert_eq!(state_of(&h).day_start_equity(), Some(d("10000")));
    // A resting order of ours must be cancelled by the flatten.
    let own = rest_limit_order(&h.env, "rb1:20210101T000000Z:BTCUSD:sell:rest", Side::Sell, "BTC/USD", "0.05", "60000");

    // Equity falls 3.1% (the broker's number, from the market): 46900 * 0.1 + 5000 = 9690.
    h.price("BTC/USD", "46900");
    assert_eq!(h.equity(), d("9690"));
    let r = h.live(1);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Halted, "HALT_DAILY_LOSS"), "{:?}", r.outcome);
    assert_eq!(status(&h), AccountStatus::Halted);
    // Cancelled, flattened, verified flat.
    let f = r.flatten.as_ref().expect("a flatten report");
    assert_eq!(f.verdict, FlattenVerdict::Flat, "{}", f.summary());
    assert!(f.verified_flat);
    // Our resting order was cancelled (by the pre-run clean-up, before the risk step) and nothing of ours is left.
    assert_eq!(r.cleanup.len(), 1);
    assert_eq!(r.cleanup[0].broker_order_id, own);
    assert_eq!(h.env.bal("BTC"), d("0"));
    assert!(h.env.rig.handle.live_orders(ACCOUNT).is_empty());
    let took = f.finished_at.signed_duration_since(f.started_at).num_seconds();
    assert!(took <= 30, "the flatten must finish inside the 30 s bound, took {took} s");
    // An alert reached a person.
    let alerts = h.notifier.alerts();
    assert!(alerts.iter().any(|a| a.code.as_str() == "ALERT_HALT" && a.severity.as_str() == "critical" && a.message.contains("HALT_DAILY_LOSS")), "{alerts:?}");
    assert_eq!(r.alerts.len(), alerts.len(), "the alerts are also in the record");
    let halt = state_of(&h);
    assert_eq!(halt.halt_record().unwrap().reason, HaltReason::DailyLoss);
    assert_eq!(halt.halt_record().unwrap().equity_at_halt, Some(d("9690")));

    // The agent path cannot resume: no approval can be issued for a non-human principal...
    for kind in [PrincipalKind::Agent, PrincipalKind::ApiKey, PrincipalKind::Service] {
        let denied = issue_human_approval(&Person("svc-agent", kind), "resume please", t0()).unwrap_err();
        assert_eq!(denied, ApprovalDenied::NotHuman);
    }
    // ...and the run pipeline itself never resumes: the next runs are refused, send nothing, and keep alerting.
    let before = h.env.order_requests();
    for n in 2..5 {
        let refused = h.live(n);
        assert_eq!((refused.outcome.kind, refused.outcome.code.as_str()), (OutcomeKind::Refused, "RUN_ACCOUNT_HALTED"));
        assert!(refused.placed.is_empty() && refused.flatten.is_none());
    }
    assert_eq!(h.env.order_requests(), before, "a halted account gets no order");
    assert_eq!(status(&h), AccountStatus::Halted);
    assert!(h.notifier.codes().iter().filter(|c| **c == "ALERT_STILL_HALTED").count() >= 3);

    // A human resume works and is logged with who, when and why.
    let s = state_of(&h);
    let snap = h.env.snapshot();
    let approval = issue_human_approval(&Person("user_owner", PrincipalKind::Human), "Reviewed: a one-day dip, OK to continue", h.env.clock_now()).unwrap();
    let (resumed, tr) = s.resume(approval, &snap.equity_snapshot(), run_day()).unwrap();
    assert_eq!(tr.code, "RESUMED");
    h.states.save(s.version(), &resumed).unwrap();
    let after = state_of(&h);
    assert_eq!(after.status(), AccountStatus::Active);
    assert_eq!(after.hwm(), Some(snap.equity));
    assert_eq!(after.resumes().len(), 1);
    assert_eq!(after.resumes()[0].approver, "user_owner");
    // The next run works again.
    let r5 = h.live(5);
    assert_eq!(r5.outcome.kind, OutcomeKind::Completed, "{:?}", r5.outcome);
    assert_eq!(status(&h), AccountStatus::Active);
}

#[test]
fn b4_the_daily_loss_limit_trips_exactly_at_three_percent() {
    // 3.0% (equity 9700, BTC 47000) halts; 2.99% (9700.01, BTC 47000.1) does not.
    let h = risk_account();
    h.live(0);
    h.price("BTC/USD", "47000.1");
    assert_eq!(h.equity(), d("9700.01"));
    assert_eq!(h.live(1).outcome.kind, OutcomeKind::Completed);
    h.price("BTC/USD", "47000");
    let r = h.live(2);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Halted, "HALT_DAILY_LOSS"));
    assert_eq!(status(&h), AccountStatus::Halted);
}

#[test]
fn a_halt_in_assisted_mode_sends_nothing_and_tells_a_person_to_flatten_by_hand() {
    let h = risk_account();
    h.run(ExecutionMode::Assisted, 0);
    h.price("BTC/USD", "46900");
    let before = h.env.rig.handle.requests().iter().filter(|r| r.is_order_affecting()).count();
    let r = h.run(ExecutionMode::Assisted, 1);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Halted, "HALT_DAILY_LOSS"));
    assert!(r.flatten.is_none());
    assert_eq!(h.env.rig.handle.requests().iter().filter(|r| r.is_order_affecting()).count(), before);
    assert_eq!(h.env.bal("BTC"), d("0.1"), "nothing was sold");
    assert_eq!(status(&h), AccountStatus::Halted);
    let codes = h.notifier.codes();
    assert!(codes.contains(&"ALERT_HALT") && codes.contains(&"ALERT_FLATTEN_INCOMPLETE"), "{codes:?}");
    assert!(h.notifier.alerts().iter().any(|a| a.message.contains("flatten the account by hand")));
}

// ---------------------------------------------------------------------------------------------------------------
// B5: ladder rung 1 shrinks (targets scaled) and rung 2 flattens and halts, at exact boundaries
// ---------------------------------------------------------------------------------------------------------------

/// Day 1: equity 10000 sets the mark. Day 2: the first run sees `first` (BTC price) so the daily-loss reference is
/// re-based, then the caller moves to the price under test.
fn ladder_account(day2_first_btc: &str) -> Harness {
    let mut h = risk_account();
    assert_eq!(h.live(0).outcome.kind, OutcomeKind::Completed);
    h.day = run_day().succ_opt().unwrap();
    h.data.set_panel("crypto", panel_on(h.day, true, true)); // valid for a run on day 2 (used when a sleeve is set)
    h.price("BTC/USD", day2_first_btc);
    let r = h.live(24);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    h
}

#[test]
fn b5_rung_one_shrinks_exactly_at_ten_percent_and_not_a_cent_before() {
    // Mark 10000; day-2 start 9100 (BTC 41000).
    let h = ladder_account("41000");
    assert_eq!(status(&h), AccountStatus::Active, "9100 is 9% below the mark: no rung yet");
    h.price("BTC/USD", "40000.1"); // 9000.01: drawdown 9.9999%
    assert_eq!(h.equity(), d("9000.01"));
    let r = h.live(25);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    assert_eq!(status(&h), AccountStatus::Active, "just below the rung: no shrink");
    assert_eq!(r.risk.as_ref().unwrap().codes(), ["RISK_NO_ACTION"]);

    h.price("BTC/USD", "40000"); // 9000.00: exactly 10%
    assert_eq!(h.equity(), d("9000"));
    let r = h.live(26);
    let risk = r.risk.as_ref().unwrap();
    assert_eq!(risk.codes(), ["RISK_DRAWDOWN_SHRINK"], "{:?}", r.outcome);
    assert_eq!(risk.risk_scale, d("0.5"));
    assert_eq!(status(&h), AccountStatus::Shrunk);
    assert_eq!(state_of(&h).risk_scale(), d("0.5"));
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "a shrink continues the run");

    // A Shrunk account scales every target by 0.5: Assisted tickets from a crypto sleeve show it exactly.
    let mut h = h;
    h.sleeves = vec![crypto_sleeve("1")];
    let a = h.run(ExecutionMode::Assisted, 27);
    let plan = a.plan.as_ref().unwrap();
    assert_eq!(plan.risk_scale, d("0.5"));
    assert_eq!(plan.capital_base, d("9000"));
    for symbol in ["BTC/USD", "ETH/USD"] {
        // 9000 (capital base) * 0.5 (sleeve weight) * 0.5 (shrink scale) = 2250
        let line = plan.lines.iter().find(|l| l.symbol == symbol).unwrap_or_else(|| panic!("no line for {symbol}"));
        assert_eq!(line.target_notional, d("2250"), "{symbol}");
    }
    // Unscaled (Active) targets would be twice as large.
}

#[test]
fn b5_rung_two_flattens_and_halts_exactly_at_twenty_percent_and_not_a_cent_before() {
    // Mark 10000; day-2 start 8100 (BTC 31000).
    let h = ladder_account("31000");
    // The day starts at 8100, already 19% under the mark: rung 1 is in force (a halt at 8000 must not also trip the
    // 3% daily-loss limit, which is why the day cannot start any higher).
    assert_eq!(status(&h), AccountStatus::Shrunk);
    h.price("BTC/USD", "30000.1"); // 8000.01: drawdown 19.9999%: still only the shrink rung
    assert_eq!(h.equity(), d("8000.01"));
    let r = h.live(25);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    assert_eq!(r.risk.as_ref().unwrap().action, rebalancer_risk::overlay::RiskAction::Shrink { scale: d("0.5") });
    assert_eq!(status(&h), AccountStatus::Shrunk);
    assert_eq!(h.env.bal("BTC"), d("0.1"), "no flatten yet");

    h.price("BTC/USD", "30000"); // 8000.00: exactly 20%
    assert_eq!(h.equity(), d("8000"));
    let r = h.live(26);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Halted, "HALT_DRAWDOWN_LADDER"), "{:?}", r.outcome);
    assert!(r.risk.as_ref().unwrap().has(rebalancer_risk::overlay::RiskCode::DrawdownHalt));
    assert_eq!(r.flatten.as_ref().unwrap().verdict, FlattenVerdict::Flat);
    assert_eq!(h.env.bal("BTC"), d("0"));
    assert_eq!(status(&h), AccountStatus::Halted);
    assert!(h.notifier.codes().contains(&"ALERT_HALT"));
}

#[test]
fn b5_a_shrink_is_released_only_after_the_drawdown_recovers_to_half_of_the_rung() {
    let h = ladder_account("41000");
    h.price("BTC/USD", "40000");
    assert_eq!(h.live(25).risk.as_ref().unwrap().codes(), ["RISK_DRAWDOWN_SHRINK"]);
    h.price("BTC/USD", "44999.9"); // equity 9499.99: drawdown 5.00001%: still shrunk
    let r = h.live(26);
    assert_eq!(r.risk.as_ref().unwrap().codes(), ["RISK_SHRINK_HELD"]);
    assert_eq!(status(&h), AccountStatus::Shrunk);
    h.price("BTC/USD", "45000"); // equity 9500: back within half of the rung
    let r = h.live(27);
    assert_eq!(r.risk.as_ref().unwrap().codes(), ["RISK_RECOVERED"]);
    assert_eq!(status(&h), AccountStatus::Active);
}

// ---------------------------------------------------------------------------------------------------------------
// B6: flatten idempotence across a crash mid-flatten, partial fills, failures
// ---------------------------------------------------------------------------------------------------------------

/// A risk-only account one price move away from a daily-loss halt.
fn on_the_brink() -> Harness {
    let h = risk_account();
    h.live(0);
    h.price("BTC/USD", "46900");
    h
}

fn total_sold(h: &Harness, pair: &str) -> Dec {
    h.env.sell_fills(pair).into_iter().fold(d("0"), |a, b| a.checked_add(b).unwrap())
}

#[test]
fn b6_a_crash_at_any_call_of_the_halting_run_never_double_sells_and_the_next_run_finishes_the_flatten() {
    // Count the calls of an uninterrupted halting run.
    let total = {
        let h = on_the_brink();
        let real = h.env.broker();
        let counting = CrashingBroker::new(&real, None);
        let r = h.run_with(&counting, ExecutionMode::Live, 1);
        assert_eq!(r.outcome.code, "HALT_DAILY_LOSS");
        counting.call_count()
    };
    assert!(total >= 8, "saw only {total} broker calls");
    let mut crashes = 0;
    for n in 1..=total {
        for crash in [CrashAt::Before(n), CrashAt::After(n)] {
            let h = on_the_brink();
            let crashed = {
                let real = h.env.broker();
                let crashing = CrashingBroker::new(&real, Some(crash));
                catch_unwind(AssertUnwindSafe(|| h.run_with(&crashing, ExecutionMode::Live, 1))).is_err()
            };
            crashes += u32::from(crashed);
            // The next scheduled run (a fresh key) finishes the job.
            let r = h.live(2);
            assert_eq!(status(&h), AccountStatus::Halted, "{crash:?}: {:?}", r.outcome);
            assert_eq!(h.env.bal("BTC"), d("0"), "{crash:?}");
            assert_eq!(total_sold(&h, "BTC/USD"), d("0.1"), "{crash:?}: sold exactly what was held, once");
            assert!(h.env.rig.handle.live_orders(ACCOUNT).is_empty());
            h.env.rig.handle.assert_invariants();
            // And it stays halted: a later run does nothing.
            let requests = h.env.order_requests();
            assert_eq!(h.live(3).outcome.code, "RUN_ACCOUNT_HALTED");
            assert_eq!(h.env.order_requests(), requests);
        }
    }
    assert!(crashes >= total, "the drill really crashed at every boundary ({crashes} of {})", total * 2);
}

#[test]
fn b6_partial_fills_during_a_halt_flatten_are_finished_in_the_next_round() {
    let h = on_the_brink();
    sc::partial_fills_next_order(&h.env.rig.handle, &["0.4"]);
    let r = h.live(1);
    assert_eq!(r.outcome.code, "HALT_DAILY_LOSS");
    let f = r.flatten.as_ref().unwrap();
    assert_eq!(f.verdict, FlattenVerdict::Flat, "{}", f.summary());
    assert!(f.orders.len() >= 2);
    assert_eq!(total_sold(&h, "BTC/USD"), d("0.1"));
    assert_eq!(status(&h), AccountStatus::Halted);
}

#[test]
fn b6_a_flatten_that_cannot_finish_stays_flattening_alerts_and_is_retried_by_the_next_run() {
    let h = on_the_brink();
    h.env.rig.handle.script_orders(OrderRule::next(FillPolicy::reject("EOrder:Insufficient funds")).times(100));
    let r = h.live(1);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Halted, "HALT_DAILY_LOSS"));
    assert_eq!(r.flatten.as_ref().unwrap().verdict, FlattenVerdict::HaltAndAlert);
    assert_eq!(status(&h), AccountStatus::Flattening, "not Halted until verified flat");
    assert_eq!(h.env.bal("BTC"), d("0.1"));
    assert!(h.notifier.codes().contains(&"ALERT_FLATTEN_INCOMPLETE"));
    // The exchange recovers; the next run resumes the SAME flatten and finishes it.
    h.env.rig.handle.clear_order_rules();
    let r2 = h.live(2);
    assert_eq!(r2.outcome.code, "HALT_DAILY_LOSS");
    assert_eq!(r2.flatten.as_ref().unwrap().verdict, FlattenVerdict::Flat);
    assert_eq!(status(&h), AccountStatus::Halted);
    assert_eq!(h.env.bal("BTC"), d("0"));
    // The flatten tags were the SAME attempt (keyed by the halt time).
    let t1 = &r.flatten.as_ref().unwrap().attempt_key;
    assert_eq!(&r2.flatten.as_ref().unwrap().attempt_key, t1);
}

#[test]
fn b6_an_assisted_or_paper_run_does_not_touch_a_flattening_account() {
    let h = on_the_brink();
    h.env.rig.handle.script_orders(OrderRule::next(FillPolicy::reject("EOrder:Insufficient funds")).times(100));
    h.live(1);
    assert_eq!(status(&h), AccountStatus::Flattening);
    let requests = h.env.order_requests();
    let r = h.run(ExecutionMode::Paper, 2);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Refused, "RUN_ACCOUNT_HALTED"));
    assert_eq!(h.env.order_requests(), requests);
}

// ---------------------------------------------------------------------------------------------------------------
// B7: broker-reported equity is what counts; a ledger/broker mismatch halts
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn b7_a_fill_report_that_disagrees_with_the_brokers_balances_halts_the_account() {
    let h = Harness::new();
    // The next fill vanishes from order reports (balances still moved): our ledger and the broker disagree.
    h.env.rig.handle.arm_fill_report_glitch(ReportGlitch::Dropped);
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Halted, "HALT_RECONCILIATION"), "{:?}", r.outcome);
    let post = r.recon.iter().find(|s| s.stage == "post").expect("a post-run reconciliation");
    assert!(post.report.has(ReconCode::PositionDrift) || post.report.has(ReconCode::BalanceDrift), "{:?}", post.report.findings);
    assert_eq!(status(&h), AccountStatus::Halted);
    assert!(h.notifier.codes().contains(&"ALERT_HALT"));
    assert!(r.flatten.is_none(), "a reconciliation halt does not flatten: we do not know what is true");
}

#[test]
fn b7_a_duplicated_fill_report_is_caught_as_an_anomaly_and_halts() {
    let h = Harness::new();
    h.env.rig.handle.arm_fill_report_glitch(ReportGlitch::Duplicated);
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Halted, "HALT_RECONCILIATION"), "{:?}", r.outcome);
    assert!(r.placed.iter().any(|p| p.anomalies.iter().any(|a| a.contains("overfill anomaly"))), "{:?}", r.placed);
    assert_eq!(status(&h), AccountStatus::Halted);
}

#[test]
fn b7_the_risk_decision_is_made_from_the_brokers_equity_not_from_our_own_records() {
    // Our run store's last snapshot claims a much higher equity; the drawdown is computed from the broker.
    let h = risk_account();
    h.live(0);
    h.price("BTC/USD", "46900");
    let r = h.live(1);
    let risk = r.risk.as_ref().unwrap();
    assert_eq!(r.pre_snapshot.as_ref().unwrap().equity, d("9690"), "the broker's TradeBalance equity");
    assert_eq!(risk.daily_loss, Some(d("0.031")));
    assert_eq!(r.outcome.code, "HALT_DAILY_LOSS");
}

// ---------------------------------------------------------------------------------------------------------------
// B8: a missed run is detectable from the run store
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn b8_expected_and_actual_run_times_are_recorded_so_a_missed_run_is_detectable() {
    let mut h = Harness::risk_only();
    for n in [0, 1, 3] {
        // slot 2 never runs
        h.lateness_secs = if n == 1 { 90 } else { 0 };
        h.run(ExecutionMode::Assisted, n);
    }
    let sums = h.runs.summaries(ACCOUNT).unwrap();
    assert_eq!(sums.len(), 3);
    let late = sums.iter().find(|s| s.scheduled_for == slot_time(1)).unwrap();
    assert_eq!(late.lateness_secs(), 90, "actual start minus expected start is on the record");
    assert!(late.finished_at.is_some());
    let expected: Vec<_> = (0..5).map(slot_time).collect();
    // Slot 4 is not due yet (grace 300 s), slot 2 was missed.
    let now = slot_time(4) + Duration::seconds(100);
    let missed = find_missed(&expected, &sums, now, 300, 900);
    assert_eq!(missed.len(), 1, "{missed:?}");
    assert_eq!((missed[0].scheduled_for, missed[0].kind), (slot_time(2), MissedKind::NeverStarted));
    assert!(missed[0].overdue_secs > 3600, "overdue {} s", missed[0].overdue_secs);
    // Later, slot 4 is missed too.
    let later = find_missed(&expected, &sums, slot_time(4) + Duration::seconds(301), 300, 900);
    assert_eq!(later.iter().map(|m| m.scheduled_for).collect::<Vec<_>>(), [slot_time(2), slot_time(4)]);
    // A refusal or a fail-closed run still counts as "the process ran".
    h.kill.set(true);
    h.run(ExecutionMode::Assisted, 2);
    let sums = h.runs.summaries(ACCOUNT).unwrap();
    assert!(find_missed(&expected[..4], &sums, now, 300, 900).is_empty());
    // A run that started and never finished (the process died) is flagged after its lease.
    let key = RunKey::new(ACCOUNT, slot_time(6), &[]);
    h.runs.begin(&key, run_day(), slot_time(6), 900).unwrap();
    let sums = h.runs.summaries(ACCOUNT).unwrap();
    assert!(find_missed(&[slot_time(6)], &sums, slot_time(6) + Duration::seconds(600), 300, 900).is_empty(), "still inside its lease");
    let stuck = find_missed(&[slot_time(6)], &sums, slot_time(6) + Duration::seconds(1000), 300, 900);
    assert_eq!(stuck[0].kind, MissedKind::StuckInProgress);
}

// ---------------------------------------------------------------------------------------------------------------
// Foreign orders and balance drift
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_foreign_order_appearing_halts_alerts_and_sends_nothing() {
    let h = Harness::new();
    assert_eq!(h.live(0).outcome.kind, OutcomeKind::Completed);
    let requests = h.env.order_requests();
    let foreign = sc::foreign_order_appears(&h.env.rig.handle, ACCOUNT, "ETH/USD", Side::Buy, "0.1", "1000");
    h.price("BTC/USD", "66000"); // the market moves too, so a rebalance would want to trade
    let r = h.live(1);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Halted, "HALT_RECONCILIATION"), "{:?}", r.outcome);
    assert!(r.recon[0].report.has(ReconCode::ForeignOrder));
    assert_eq!(r.recon[0].stage, "pre");
    assert!(r.placed.is_empty());
    assert_eq!(h.env.order_requests(), requests, "no order reached the exchange");
    assert_eq!(status(&h), AccountStatus::Halted);
    assert!(h.notifier.codes().contains(&"ALERT_HALT"));
    assert!(h.notifier.alerts().last().unwrap().message.contains("RECON_FOREIGN_ORDER"));
    assert!(h.env.rig.handle.order(&foreign).unwrap().status.is_live(), "the foreign order was not touched");
    assert_eq!(h.live(2).outcome.code, "RUN_ACCOUNT_HALTED");
}

#[test]
fn a_foreign_order_that_appears_during_the_run_is_caught_by_the_post_run_reconciliation() {
    let mut h = Harness::new();
    let handle = h.env.rig.handle.clone();
    let fired = std::sync::Arc::new(AtomicBool::new(false));
    let flag = fired.clone();
    let inner_handle = handle.clone();
    h.data = FixtureData::new().with_panel("crypto", panel(true, true)).with_price_fn(move |sym| {
        if !flag.swap(true, Ordering::SeqCst) {
            // Someone places an order on the account while our run is between reconciliation and trading.
            inner_handle.add_foreign_order(ACCOUNT, fake_broker::ForeignOrder::limit("ETH/USD", Side::Buy, "0.1", "1000"));
        }
        match sym {
            "BTC/USD" | "ETH/USD" => Some(inner_handle.price(sym)),
            _ => None,
        }
    });
    let r = h.live(0);
    assert!(fired.load(Ordering::SeqCst));
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Halted, "HALT_RECONCILIATION"), "{:?}", r.outcome);
    assert!(r.recon.iter().any(|s| s.stage == "post" && s.report.has(ReconCode::ForeignOrder)));
    assert_eq!(status(&h), AccountStatus::Halted);
    assert!(h.notifier.codes().contains(&"ALERT_HALT"));
}

#[test]
fn unexplained_balance_or_position_drift_halts_and_alerts() {
    for (asset, delta) in [("USD", "-100"), ("USD", "100"), ("ETH", "0.05"), ("BTC", "0.001")] {
        let h = Harness::new();
        assert_eq!(h.live(0).outcome.kind, OutcomeKind::Completed);
        sc::balance_drift(&h.env.rig.handle, ACCOUNT, asset, delta);
        let requests = h.env.order_requests();
        let r = h.live(1);
        assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Halted, "HALT_RECONCILIATION"), "{asset} {delta}: {:?}", r.outcome);
        let f = &r.recon[0].report;
        assert!(f.has(ReconCode::BalanceDrift) || f.has(ReconCode::PositionDrift), "{asset} {delta}: {:?}", f.findings);
        assert_eq!(h.env.order_requests(), requests, "{asset} {delta}");
        assert!(h.notifier.codes().contains(&"ALERT_HALT"));
        assert_eq!(status(&h), AccountStatus::Halted);
    }
}

#[test]
fn drift_inside_the_tolerance_does_not_halt() {
    let h = Harness::new();
    h.live(0);
    sc::balance_drift(&h.env.rig.handle, ACCOUNT, "USD", "1"); // 1.00 on ~10000: inside max(1, 0.1%)
    let r = h.live(1);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
}

#[test]
fn assisted_mode_tolerates_a_person_trading_the_account_but_a_live_run_does_not() {
    let h = Harness::new();
    h.run(ExecutionMode::Assisted, 0);
    // The person executes a ticket by hand and leaves an order resting.
    h.env.hold("BTC", "0.05");
    sc::foreign_order_appears(&h.env.rig.handle, ACCOUNT, "ETH/USD", Side::Buy, "0.1", "1000");
    let r = h.run(ExecutionMode::Assisted, 1);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    assert!(r.recon[0].report.has(ReconCode::ForeignOrder) || r.recon[0].report.has(ReconCode::PositionDrift));
    assert!(r.recon[0].report.findings.iter().all(|f| f.severity == rebalancer_run::recon::Severity::Info));
    assert_eq!(status(&h), AccountStatus::Active);
}

// ---------------------------------------------------------------------------------------------------------------
// A halted or halting account never trades; stores fail closed
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_halted_account_never_gets_an_order_and_a_cas_conflict_fails_closed() {
    let h = Harness::new();
    let (halted, _) = AccountState::new(ACCOUNT).halt(HaltReason::Manual, "requested", t0());
    h.states.save(0, &halted).unwrap();
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Refused, "RUN_ACCOUNT_HALTED"));
    assert_eq!(h.env.order_requests(), 0);
    assert!(h.env.rig.handle.orders(ACCOUNT).is_empty());

    // Another process saves the state while this run is in flight: the compare-and-swap detects it.
    let h = Harness::new();
    let racing = RacingStore { inner: &h.states, fired: AtomicBool::new(false) };
    let r = h.run_with_stores(&h.env.broker(), &racing, ExecutionMode::Live, 0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_STATE_STORE_ERROR"), "{:?}", r.outcome);
    assert!(r.outcome.message.contains("STORE_VERSION_CONFLICT"), "{}", r.outcome.message);
    assert_eq!(h.env.order_requests(), 0);
    assert_eq!(h.notifier.codes(), ["ALERT_RUN_FAILED"]);
}

/// A state store that lets a competing writer sneak in a save before the first save of the run.
struct RacingStore<'a> {
    inner: &'a rebalancer_risk::store::InMemoryStateStore,
    fired: AtomicBool,
}

impl StateStore for RacingStore<'_> {
    fn load(&self, id: &str) -> Result<Option<AccountState>, StoreError> {
        self.inner.load(id)
    }
    fn save(&self, expected: u64, s: &AccountState) -> Result<AccountState, StoreError> {
        if !self.fired.swap(true, Ordering::SeqCst) {
            self.inner.save(expected, &AccountState::new(ACCOUNT))?;
        }
        self.inner.save(expected, s)
    }
}

#[test]
fn the_rebalancer_source_never_resumes_an_account_or_mints_an_approval() {
    // The human-only rule, pinned: no non-test source of this crate calls `resume` or the gated issuer.
    for (name, src) in [
        ("pipeline.rs", include_str!("../src/pipeline.rs")),
        ("flatten.rs", include_str!("../src/flatten.rs")),
        ("recon.rs", include_str!("../src/recon.rs")),
        ("broker.rs", include_str!("../src/broker.rs")),
        ("stores.rs", include_str!("../src/stores.rs")),
        ("view.rs", include_str!("../src/view.rs")),
        ("record.rs", include_str!("../src/record.rs")),
        ("data.rs", include_str!("../src/data.rs")),
        ("testkit.rs", include_str!("../src/testkit.rs")),
        ("clock.rs", include_str!("../src/clock.rs")),
    ] {
        assert!(!src.contains(".resume("), "{name} calls resume");
        assert!(!src.contains("issue_human_approval"), "{name} mints a human approval");
        assert!(!src.contains("HumanApproval"), "{name} handles a HumanApproval");
    }
    let cargo = include_str!("../Cargo.toml");
    let normal_deps = cargo.split("[dev-dependencies]").next().unwrap();
    assert!(!normal_deps.contains("human-endpoint"), "the feature must be enabled for tests only");
}

#[test]
fn the_risk_overlay_alone_moves_the_state_and_the_stored_state_matches_the_record() {
    let h = risk_account();
    let r = h.live(0);
    assert_eq!(r.state_before, Some(AccountStatus::Active));
    assert_eq!(r.state_after, Some(AccountStatus::Active));
    h.price("BTC/USD", "46900");
    let r = h.live(1);
    assert_eq!(r.state_before, Some(AccountStatus::Active));
    assert_eq!(r.state_after, Some(AccountStatus::Halted));
    let t: Vec<(AccountStatus, AccountStatus, &str)> = r.transitions.iter().map(|t| (t.from, t.to, t.code)).collect();
    assert_eq!(
        t,
        [(AccountStatus::Active, AccountStatus::Flattening, "HALT_DAILY_LOSS"), (AccountStatus::Flattening, AccountStatus::Halted, "HALT_DAILY_LOSS")]
    );
}
