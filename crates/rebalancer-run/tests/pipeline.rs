//! The run pipeline against the stateful fake Kraken exchange: the end-to-end crypto-trend scenario and the
//! SPEC B1, B2, B3, B9, B10 drills, run-key and fail-closed behaviour, plus the pinned run and alert codes.

mod common;

use std::collections::BTreeMap;

use broker_adapters::Side;
use chrono::NaiveDate;
use common::harness::*;
use common::*;
use fake_broker::scenarios as sc;
use rebalancer_core::policy::{DeploymentLimits, MandateStatus};
use rebalancer_risk::state::AccountStatus;
use rebalancer_risk::store::StateStore;
use rebalancer_run::data::DataError;
use rebalancer_run::pipeline::{RunCode, RunConfig};
use rebalancer_run::record::{AlertCode, ExecutionMode, OutcomeKind, Phase, PlacedOutcome, RunRecord};
use rebalancer_run::stores::RunStore;
use reference_rules::{decide_crypto_trend, Options, Panel, PriceSeries};
use serde_json::json;

fn placed_tags(r: &RunRecord) -> Vec<(String, Side, String)> {
    r.placed.iter().map(|p| (p.tag.clone(), p.side, p.planned_quantity.to_string())).collect()
}

fn planned(r: &RunRecord) -> Vec<(String, Side, String)> {
    r.plan.as_ref().unwrap().orders.iter().map(|o| (o.tag.clone(), o.side, o.quantity.to_string())).collect()
}

// ---------------------------------------------------------------------------------------------------------------
// Pinned codes
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn run_alert_and_outcome_codes_are_pinned_and_unique() {
    let run: Vec<&str> = RunCode::ALL.iter().map(|c| c.as_str()).collect();
    assert_eq!(
        run,
        [
            "RUN_COMPLETED",
            "RUN_KILL_FLAG_SET",
            "RUN_NO_ACTIVE_MANDATE",
            "RUN_MANDATE_NOT_ACTIVE",
            "RUN_MANDATE_INVALID",
            "RUN_ACCOUNT_HALTED",
            "RUN_BUSY",
            "RUN_STORE_UNAVAILABLE",
            "RUN_KILL_FLAG_UNREADABLE",
            "RUN_RISK_POLICY_INVALID",
            "RUN_BROKER_UNREACHABLE",
            "RUN_STATE_STORE_ERROR",
            "RUN_CLEANUP_FAILED",
            "RUN_DATA_ERROR",
            "RUN_RULE_ERROR",
            "RUN_PLAN_ERROR",
            "RUN_NOTHING_PENDING",
            "RUN_DECISION_LEDGER_UNAVAILABLE",
        ]
    );
    assert_eq!(run.iter().collect::<std::collections::BTreeSet<_>>().len(), run.len());
    let alerts: Vec<&str> = AlertCode::ALL.iter().map(|c| c.as_str()).collect();
    assert_eq!(alerts, ["ALERT_HALT", "ALERT_FLATTEN_INCOMPLETE", "ALERT_RUN_FAILED", "ALERT_STILL_HALTED", "ALERT_MANDATE_UNUSABLE"]);
    assert_eq!(
        [OutcomeKind::Completed, OutcomeKind::Refused, OutcomeKind::FailedClosed, OutcomeKind::Halted].map(|k| k.as_str()),
        ["COMPLETED", "REFUSED", "FAILED_CLOSED", "HALTED"]
    );
    assert_eq!([ExecutionMode::Assisted, ExecutionMode::Paper, ExecutionMode::Live].map(|m| m.as_str()), ["assisted", "paper", "live"]);
}

// ---------------------------------------------------------------------------------------------------------------
// End to end: crypto trend rebalance from a fixture panel, Assisted, then Paper, then Live
// ---------------------------------------------------------------------------------------------------------------

const LADDER: &str = include_str!("../../reference-rules/tests/data/ladder_candles.csv");

/// BTC and ETH from the reference tool's real daily candles (they end on 2020-12-31).
fn csv_panel() -> Panel {
    let mut rows: BTreeMap<&str, (Vec<NaiveDate>, Vec<f64>)> = BTreeMap::new();
    for line in LADDER.lines().skip(1) {
        let f: Vec<&str> = line.trim().split(',').collect();
        if f[0] == "BTC" || f[0] == "ETH" {
            let e = rows.entry(f[0]).or_default();
            e.0.push(NaiveDate::parse_from_str(f[1], "%Y-%m-%d").unwrap());
            e.1.push(f[2].parse().unwrap());
        }
    }
    Panel::new(rows.into_iter().map(|(s, (dates, closes))| PriceSeries::new(s, dates, closes).unwrap()).collect()).unwrap()
}

#[test]
fn crypto_trend_rebalance_end_to_end_assisted_then_paper_then_live() {
    let h = Harness::new();
    h.data.set_panel("crypto", csv_panel());
    h.price("BTC/USD", "28921.7"); // the fixture's last closes
    h.price("ETH/USD", "737.67");
    // What the reference rule says on the fixture (both coins trade above their 100-day average on 2020-12-31).
    let decision = decide_crypto_trend(&csv_panel(), NaiveDate::from_ymd_opt(2020, 12, 31).unwrap(), &Options::crypto_live(run_day())).unwrap();
    assert_eq!(decision.invested_weight(), 1.0, "the fixture has both coins long");

    // 1. ASSISTED: tickets, nothing placed, nothing changed at the exchange.
    let before = h.env.rig.handle.balances(ACCOUNT);
    let a = h.run(ExecutionMode::Assisted, 0);
    assert_eq!((a.outcome.kind, a.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{:?}", a.outcome);
    assert_eq!(a.tickets.len(), 2, "one buy per coin");
    assert!(a.tickets.iter().all(|t| t.side == Side::Buy));
    assert!(a.placed.is_empty());
    assert_eq!(h.env.rig.handle.requests().iter().filter(|r| r.is_order_affecting()).count(), 0);
    assert_eq!(h.env.rig.handle.balances(ACCOUNT), before);
    assert_eq!(a.data_fingerprints.len(), 1);
    assert_eq!(a.data_fingerprints[0].1.len(), 64);
    assert_eq!(a.mandate_version, Some(3));
    assert_eq!(a.mandate_standing, "active");
    assert_eq!(a.targets[0].decision_date, NaiveDate::from_ymd_opt(2020, 12, 31).unwrap());

    // 2. PAPER: every order is validated and none created.
    let p = h.run(ExecutionMode::Paper, 1);
    assert_eq!(p.outcome.kind, OutcomeKind::Completed, "{:?}", p.outcome);
    assert_eq!(p.placed.len(), 2);
    assert!(p.placed.iter().all(|o| o.outcome == PlacedOutcome::Validated && o.phase == Phase::Rehearsal && o.broker_order_id.is_none()));
    assert_eq!(placed_tags(&p), planned(&p), "paper sends exactly the plan");
    assert_eq!(h.env.rig.handle.orders(ACCOUNT).len(), 0, "validate-only creates nothing");
    assert_eq!(h.env.rig.handle.balances(ACCOUNT), before);
    assert_eq!(p.recon.iter().map(|r| r.stage).collect::<Vec<_>>(), ["pre", "post"]);

    // 3. LIVE: the placed orders are the plan, and the account ends at the planned weights within venue rounding.
    let l = h.live(2);
    assert_eq!((l.outcome.kind, l.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{:?}", l.outcome);
    let plan = l.plan.as_ref().unwrap();
    assert_eq!(plan.orders.len(), 2);
    assert!(plan.denied.is_empty(), "{:?}", plan.denied);
    assert_eq!(placed_tags(&l), planned(&l), "live sends exactly the plan, in the plan's order");
    assert!(l.placed.iter().all(|o| o.outcome == PlacedOutcome::Filled && o.status == Some(broker_adapters::OrderStatus::Filled)));
    // The Live buys were re-planned on the re-read account and must agree with the first plan (nothing was sold).
    assert!(l.replan.is_none() || l.replan.as_ref().unwrap().orders.len() == 2);
    let btc = plan.orders.iter().find(|o| o.symbol == "BTC/USD").unwrap();
    let eth = plan.orders.iter().find(|o| o.symbol == "ETH/USD").unwrap();
    assert_eq!(h.env.bal("BTC"), btc.quantity, "the exchange holds exactly the planned quantity");
    assert_eq!(h.env.bal("ETH"), eth.quantity);
    let equity = h.equity();
    for line in &plan.orders {
        let weight = line.notional.to_f64() / equity.to_f64();
        assert!((weight - 0.475).abs() < 0.004, "{}: weight {weight}", line.symbol); // 50% target less the 5% reserve and fees
    }
    // Fees were charged at the fake's taker rate; equity is the broker's number.
    assert!(equity < d("10000") && equity > d("9950"), "{equity}");
    // Tags are the deterministic planner tags.
    assert!(l.placed.iter().all(|o| o.tag.starts_with("rb1:20210101T")));
    // The record ties the run to its inputs.
    assert_eq!(l.mandate_hash.len(), 64);
    assert_eq!(l.plan.as_ref().unwrap().inputs_digest.len(), 64);
    assert!(l.post_snapshot.is_some() && l.pre_snapshot.is_some());

    // 4. A second live run converges: nothing left to do.
    let l2 = h.live(3);
    assert_eq!(l2.outcome.kind, OutcomeKind::Completed);
    assert!(l2.placed.is_empty(), "already at target: {:?}", l2.plan.as_ref().unwrap().skipped);
    assert_eq!(h.states.load(ACCOUNT).unwrap().unwrap().status(), AccountStatus::Active);
    h.env.rig.handle.assert_invariants();
}

#[test]
fn step_names_follow_the_documented_order() {
    let h = Harness::new();
    let r = h.live(0);
    assert_eq!(
        r.step_names(),
        ["acquire_run_key", "kill_flag", "mandate", "decisions", "read_account", "cleanup", "reconcile_pre", "risk", "targets", "plan", "execute", "reconcile_post"]
    );
}

// ---------------------------------------------------------------------------------------------------------------
// B1: no active mandate => no live order
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn b1_no_mandate_no_active_mandate_and_invalid_mandate_send_no_live_order() {
    // No mandate at all.
    let mut h = Harness::new();
    h.mandate = None;
    h.envelope = None;
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Refused, "RUN_NO_ACTIVE_MANDATE"));
    assert_eq!(h.env.order_requests(), 0);
    assert_eq!(h.env.rig.handle.requests().len(), 0, "the broker was not even read");
    assert_eq!(h.notifier.codes(), ["ALERT_MANDATE_UNUSABLE"]);

    // Every non-active status refuses Live.
    for status in [MandateStatus::Draft, MandateStatus::Superseded, MandateStatus::Revoked, MandateStatus::Expired] {
        let mut h = Harness::new();
        h.envelope.as_mut().unwrap().status = status;
        let r = h.live(0);
        assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Refused, "RUN_MANDATE_NOT_ACTIVE"), "{status:?}");
        assert!(r.mandate_standing.starts_with("not active"), "{}", r.mandate_standing);
        assert_eq!(h.env.order_requests(), 0, "{status:?}");
        assert!(r.placed.is_empty() && r.tickets.is_empty());
    }
    // Not yet effective.
    let mut h = Harness::new();
    h.envelope.as_mut().unwrap().effective_from = at("2021-01-02T00:00:00Z");
    assert_eq!(h.live(0).outcome.code, "RUN_MANDATE_NOT_ACTIVE");

    // An invalid mandate ("25%" typed as 25).
    let h = Harness::new().with_mandate(|m| m["exposure"]["max_position"] = json!(25));
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Refused, "RUN_MANDATE_INVALID"));
    assert_eq!(h.env.order_requests(), 0);
    assert!(h.notifier.codes().contains(&"ALERT_MANDATE_UNUSABLE"));
}

#[test]
fn b1_a_draft_mandate_may_be_rehearsed_in_paper_and_assisted_but_never_live() {
    let mut h = Harness::new();
    h.envelope.as_mut().unwrap().status = MandateStatus::Draft;
    let paper = h.run(ExecutionMode::Paper, 0);
    assert_eq!(paper.outcome.kind, OutcomeKind::Completed, "{:?}", paper.outcome);
    assert_eq!(paper.mandate_standing, "draft (rehearsal)");
    assert_eq!(paper.placed.len(), 2, "the paper order is accepted");
    let assisted = h.run(ExecutionMode::Assisted, 1);
    assert_eq!(assisted.tickets.len(), 2);
    let live = h.live(2);
    assert_eq!((live.outcome.kind, live.outcome.code.as_str()), (OutcomeKind::Refused, "RUN_MANDATE_NOT_ACTIVE"));
    assert_eq!(h.env.order_requests(), 0);
    // A revoked mandate is not rehearsable in any mode.
    h.envelope.as_mut().unwrap().status = MandateStatus::Revoked;
    assert_eq!(h.run(ExecutionMode::Paper, 3).outcome.code, "RUN_MANDATE_NOT_ACTIVE");
}

// ---------------------------------------------------------------------------------------------------------------
// B2: every guard denial is visible in the run record
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn b2_each_guard_denial_is_visible_in_the_run_record_and_a_compliant_order_passes() {
    // Only ETH is allowed: the BTC buy is denied, the ETH buy goes through, in the same run.
    let h = Harness::new().with_mandate(|m| m["universe"]["instrument_allow"] = json!(["ETH/USD"]));
    let r = h.live(0);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    let plan = r.plan.as_ref().unwrap();
    assert_eq!(plan.denied.len(), 1);
    assert_eq!(plan.denied[0].order.symbol, "BTC/USD");
    assert_eq!(plan.denied[0].codes, ["INSTRUMENT_NOT_ALLOWED"]);
    assert!(plan.denied[0].reasons.iter().all(|x| !x.message.is_empty()));
    // The buys are planned twice (before and after the sells); the denial is recorded in both plans.
    assert_eq!(r.denial_codes(), ["INSTRUMENT_NOT_ALLOWED", "INSTRUMENT_NOT_ALLOWED"]);
    assert_eq!(r.replan.as_ref().unwrap().denied[0].codes, ["INSTRUMENT_NOT_ALLOWED"]);
    assert_eq!(r.placed.len(), 1);
    assert_eq!((r.placed[0].symbol.as_str(), r.placed[0].outcome), ("ETH/USD", PlacedOutcome::Filled));
    assert_eq!(h.env.bal("BTC"), d("0"));
    assert!(h.env.bal("ETH").is_positive());
    assert_eq!(r.mandate_version, Some(3), "the mandate version is on the record");

    // A per-instrument cap of 20% (2000): both buys are cut by the plan's cash logic only if allowed; here the
    // target of 50% exceeds the cap so both are denied MAX_POSITION.
    let h = Harness::new().with_mandate(|m| m["exposure"]["max_position"] = json!(0.2));
    let r = h.live(0);
    assert_eq!(r.denial_codes(), ["MAX_POSITION", "MAX_POSITION"], "{:?}", r.denial_codes());
    assert!(r.placed.is_empty());
    assert_eq!(h.env.order_requests(), 0);

    // A per-order notional cap of 1000.
    let h = Harness::new().with_mandate(|m| m["exposure"]["max_order_notional"]["amount"] = json!("1000.00"));
    let r = h.live(0);
    assert!(r.denial_codes().iter().all(|c| *c == "MAX_ORDER_NOTIONAL"), "{:?}", r.denial_codes());
    assert_eq!(r.denial_codes().len(), 2);

    // Stale prices (older than 300 s) deny every order.
    let h = Harness::new();
    h.data.set_price_lag_secs(400);
    let r = h.live(0);
    assert_eq!(r.denial_codes(), ["PRICE_STALE", "PRICE_STALE"]);
    assert_eq!(h.env.order_requests(), 0);
}

#[test]
fn b2_the_daily_order_and_turnover_counters_carry_across_runs() {
    // At most 2 orders per day: run 1 places both buys, a later run the same trading day has nothing left.
    let mut h = Harness::new().with_mandate(|m| m["exposure"]["max_orders_per_day"] = json!(2));
    h.cfg.min_trade_pct = d("0.0001"); // let small corrective trades through
    let r1 = h.live(0);
    assert_eq!(r1.placed.len(), 2, "{:?}", r1.denial_codes());
    // Move the market so the second run wants to trade again.
    h.price("BTC/USD", "66000");
    h.price("ETH/USD", "3300");
    let r2 = h.live(1);
    assert!(r2.denial_codes().contains(&"MAX_ORDERS_PER_DAY"), "{:?} {:?}", r2.denial_codes(), r2.plan.as_ref().unwrap().skipped);
    assert!(r2.placed.is_empty());
    // Turnover: 0.5 of the 10000 base = 5000 per day; the first run already traded ~9500.
    let h = Harness::new().with_mandate(|m| m["exposure"]["max_turnover_per_day"] = json!(1.0));
    let r1 = h.live(0);
    assert_eq!(r1.placed.len(), 2);
    h.price("BTC/USD", "70000");
    let r2 = h.live(1);
    assert!(r2.placed.is_empty() || r2.denial_codes().contains(&"MAX_TURNOVER_PER_DAY"), "{:?}", r2.denial_codes());
}

// ---------------------------------------------------------------------------------------------------------------
// B3: an expired mandate admits only reducing orders
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn b3_an_expired_mandate_allows_only_reducing_orders() {
    let mut h = Harness::new();
    h.data.set_panel("crypto", panel(true, false)); // BTC long, ETH cash: buy BTC, sell ETH
    h.env.hold("ETH", "2"); // 6000 of ETH to reduce
    h.envelope.as_mut().unwrap().review_by = at("2021-01-01T00:05:00Z"); // expired before the run at 00:10
    let r = h.live(0);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    assert_eq!(r.mandate_standing, "expired");
    assert_eq!(r.denial_codes(), ["MANDATE_EXPIRED"], "the BTC buy is refused: {:?}", r.plan.as_ref().unwrap().denied);
    let live: Vec<_> = r.placed_live().collect();
    assert_eq!(live.len(), 1);
    assert_eq!((live[0].symbol.as_str(), live[0].side, live[0].outcome), ("ETH/USD", Side::Sell, PlacedOutcome::Filled));
    assert_eq!(h.env.bal("ETH"), d("0"));
    assert_eq!(h.env.bal("BTC"), d("0"), "no buy was sent under an expired mandate");
    assert!(r.replan.is_none() || r.replan.as_ref().unwrap().orders.is_empty());
}

// ---------------------------------------------------------------------------------------------------------------
// B9: effective limit = min(mandate, deployment)
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn b9_a_tighter_deployment_is_honoured_and_a_looser_one_is_ignored() {
    // Mandate max_position 50% (5000 of the 10000 base); the deployment asks for 20%.
    let mut tight = Harness::new();
    tight.cfg.deployment = Some(DeploymentLimits { max_position: Some(d("0.2")), ..DeploymentLimits::default() });
    let r = tight.live(0);
    assert_eq!(r.denial_codes(), ["MAX_POSITION", "MAX_POSITION"]);
    assert_eq!(r.deployment_digest.as_deref(), Some("cap=-|pos=0.2|gross=-|net=-|notional=-|orders=-|turnover=-|reserve=-|classes="));
    assert!(r.placed.is_empty());

    // A looser deployment changes nothing: same plan digest as no deployment at all.
    let mut loose = Harness::new();
    loose.cfg.deployment = Some(DeploymentLimits { max_position: Some(d("0.9")), max_gross: Some(d("5")), ..DeploymentLimits::default() });
    let with = loose.run(ExecutionMode::Assisted, 0);
    let without = Harness::new().run(ExecutionMode::Assisted, 0);
    assert_eq!(with.plan.as_ref().unwrap().orders, without.plan.as_ref().unwrap().orders);
    assert!(with.denial_codes().is_empty());

    // The deployment's capital allocation clamps the capital base.
    let mut small = Harness::new();
    small.cfg.deployment = Some(DeploymentLimits { capital_allocation: Some(d("4000")), ..DeploymentLimits::default() });
    let r = small.run(ExecutionMode::Assisted, 0);
    assert_eq!(r.plan.as_ref().unwrap().capital_base, d("4000"));
}

// ---------------------------------------------------------------------------------------------------------------
// B10: assisted and paper change no exchange state
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn b10_assisted_and_paper_runs_change_no_exchange_state_even_with_stale_orders_around() {
    let h = Harness::new();
    // An old order of ours is resting; assisted and paper must not cancel it (or anything).
    let stale = rest_limit_order(&h.env, "rb1:20201231T000000Z:BTCUSD:buy:zzzz", Side::Buy, "BTC/USD", "0.01", "20000");
    let before_balances = h.env.rig.handle.balances(ACCOUNT);
    let before_orders = h.env.rig.handle.orders(ACCOUNT).len();
    let requests_before = h.env.rig.handle.requests().iter().filter(|r| r.is_order_affecting()).count();
    for (mode, n) in [(ExecutionMode::Assisted, 0), (ExecutionMode::Paper, 1)] {
        let r = h.run(mode, n);
        assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{mode:?}: {:?}", r.outcome);
        assert!(r.cleanup.is_empty(), "{mode:?}: nothing is cancelled");
    }
    assert_eq!(h.env.rig.handle.balances(ACCOUNT), before_balances);
    assert_eq!(h.env.rig.handle.orders(ACCOUNT).len(), before_orders);
    assert!(h.env.rig.handle.order(&stale).unwrap().status.is_live(), "the stale order was left alone");
    assert_eq!(h.env.rig.handle.requests().iter().filter(|r| r.is_order_affecting()).count(), requests_before, "not one order-affecting request");
    h.env.rig.handle.assert_invariants();
}

#[test]
fn b10_paper_orders_are_sent_validate_only() {
    let h = Harness::new();
    let _ = h.run(ExecutionMode::Paper, 0);
    let adds = h.env.rig.handle.requests_to(fake_broker::kraken::wire::paths::ADD_ORDER);
    assert_eq!(adds.len(), 2);
    assert!(adds.iter().all(|r| r.param("validate") == Some("true")), "{:?}", adds.iter().map(|r| r.params.clone()).collect::<Vec<_>>());
    assert!(adds.iter().all(|r| !r.is_order_affecting()));
}

// ---------------------------------------------------------------------------------------------------------------
// Run key
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_duplicate_run_key_is_a_no_op_that_returns_the_first_record() {
    let h = Harness::new();
    let first = h.live(0);
    assert_eq!(first.placed.len(), 2);
    let requests = h.env.rig.handle.requests().len();
    let alerts = h.notifier.count();
    let second = h.live(0);
    assert_eq!(second, first, "the FIRST record comes back, untouched");
    assert_eq!(h.env.rig.handle.requests().len(), requests, "not one more request reached the exchange");
    assert_eq!(h.notifier.count(), alerts);
    assert_eq!(h.runs.records().len(), 1);
    // Also when the first run was a refusal.
    h.kill.set(true);
    let refused = h.live(1);
    assert_eq!(refused.outcome.code, "RUN_KILL_FLAG_SET");
    h.kill.set(false);
    assert_eq!(h.live(1), refused, "a finished key stays finished");
    // A different sleeve set or schedule is a different key.
    assert_ne!(h.live(2).key, first.key);
}

#[test]
fn a_run_whose_key_is_in_progress_is_refused_and_not_persisted() {
    let h = Harness::new();
    let key = rebalancer_run::record::RunKey::new(ACCOUNT, slot_time(0), &["crypto"]);
    h.runs.begin(&key, run_day(), slot_time(0), 900).unwrap();
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Refused, "RUN_BUSY"));
    assert_eq!(h.env.rig.handle.requests().len(), 0);
    assert!(h.runs.records().is_empty());
}

#[test]
fn the_record_store_is_append_only() {
    let h = Harness::new();
    let r = h.live(0);
    assert_eq!(h.runs.finish(r).unwrap_err().code(), "RUNSTORE_ALREADY_FINISHED");
    let key = rebalancer_run::record::RunKey::new(ACCOUNT, slot_time(9), &["crypto"]);
    let fake = h.live(0);
    let mut orphan = fake.clone();
    orphan.key = key;
    assert_eq!(h.runs.finish(orphan).unwrap_err().code(), "RUNSTORE_NOT_STARTED");
}

// ---------------------------------------------------------------------------------------------------------------
// Fail closed
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_kill_flag_stops_the_run_before_the_broker_is_touched_and_an_unreadable_flag_fails_closed() {
    let h = Harness::new();
    h.kill.set(true);
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Refused, "RUN_KILL_FLAG_SET"));
    assert_eq!(h.env.rig.handle.requests().len(), 0);
    assert_eq!(r.step_names(), ["acquire_run_key", "kill_flag"]);
    assert_eq!(h.notifier.count(), 0, "a deliberate stop is not an alert");
    h.kill.set(false);
    h.kill.set_unreadable(true);
    let r = h.live(1);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_KILL_FLAG_UNREADABLE"));
    assert_eq!(h.env.rig.handle.requests().len(), 0);
    assert_eq!(h.notifier.codes(), ["ALERT_RUN_FAILED"]);
}

#[test]
fn a_data_error_or_a_rule_refusal_means_no_trade_and_an_alert() {
    // The data source is down.
    let h = Harness::new();
    h.data.set_error(Some(DataError::new("DATA_UNAVAILABLE", "vendor 503")));
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_DATA_ERROR"), "{:?}", r.outcome);
    assert!(r.outcome.message.contains("vendor 503"));
    assert_eq!(h.env.order_requests(), 0);
    assert_eq!(h.notifier.codes(), ["ALERT_RUN_FAILED"]);

    // The prices are down.
    let h = Harness::new();
    h.data.set_price_error(Some(DataError::new("DATA_UNAVAILABLE", "quotes down")));
    let r = h.live(0);
    assert_eq!(r.outcome.code, "RUN_DATA_ERROR");
    assert_eq!(h.env.order_requests(), 0);

    // The panel ends a day early: the reference rule refuses (the decision date is not a bar).
    let h = Harness::new();
    let end = run_day().pred_opt().unwrap().pred_opt().unwrap();
    h.data.set_panel("crypto", Panel::new(vec![synth_series("BTC", end, true, 60000.0), synth_series("ETH", end, true, 3000.0)]).unwrap());
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_RULE_ERROR"), "{:?}", r.outcome);
    assert!(r.outcome.message.contains("reference rule refused"), "{}", r.outcome.message);
    assert_eq!(h.env.order_requests(), 0);
    assert_eq!(h.notifier.codes(), ["ALERT_RUN_FAILED"]);

    // A missing instrument in the panel.
    let h = Harness::new();
    let end = run_day().pred_opt().unwrap();
    h.data.set_panel("crypto", Panel::new(vec![synth_series("BTC", end, true, 60000.0)]).unwrap());
    assert_eq!(h.live(0).outcome.code, "RUN_RULE_ERROR");
}

#[test]
fn the_exchange_down_for_n_calls_means_no_trades_and_alerts_then_the_next_run_recovers() {
    let h = Harness::new();
    sc::broker_unreachable_for(&h.env.rig.handle, 3);
    for n in 0..3 {
        let r = h.live(n);
        assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_BROKER_UNREACHABLE"), "run {n}");
        assert!(r.placed.is_empty());
        assert_eq!(h.env.order_requests(), 0);
    }
    assert_eq!(h.notifier.codes(), ["ALERT_RUN_FAILED"; 3]);
    let r = h.live(3);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    assert_eq!(r.placed.len(), 2, "recovered: the run trades normally");
    assert_eq!(h.notifier.count(), 3, "no further alert");
}

#[test]
fn the_state_store_or_run_store_being_unavailable_fails_closed() {
    let h = Harness::new();
    h.states.fail_next_calls(1);
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_STATE_STORE_ERROR"));
    assert_eq!(h.env.order_requests(), 0);
    let h = Harness::new();
    h.runs.fail_next_calls(1);
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_STORE_UNAVAILABLE"));
    assert_eq!(h.env.rig.handle.requests().len(), 0, "without a run key nothing is read or sent");
    assert_eq!(h.notifier.codes(), ["ALERT_RUN_FAILED"]);
}

#[test]
fn an_undeliverable_alert_is_recorded_and_never_stops_the_pipeline_from_failing_closed() {
    let h = Harness::new();
    h.notifier.set_failing(true);
    h.data.set_error(Some(DataError::new("DATA_UNAVAILABLE", "down")));
    let r = h.live(0);
    assert_eq!(r.outcome.code, "RUN_DATA_ERROR");
    assert_eq!(r.alerts.len(), 1);
    assert_eq!(r.alert_delivery_failures.len(), 1, "{:?}", r.alert_delivery_failures);
    assert_eq!(h.env.order_requests(), 0);
}

#[test]
fn a_run_config_default_is_conservative_and_documented() {
    let c = RunConfig::default();
    assert_eq!((c.min_trade_abs, c.min_trade_pct, c.fee_rate, c.recovery_fraction), (d("10"), d("0.02"), d("0.0026"), d("0.5")));
    assert_eq!((c.max_price_age_secs, c.lease_secs, c.max_polls), (300, 900, 3));
}

// ---------------------------------------------------------------------------------------------------------------
// Clean-up of our own stale orders (Live only)
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_stale_open_order_of_ours_is_cancelled_before_a_live_run_trades() {
    let h = Harness::new();
    let stale = rest_limit_order(&h.env, "rb1:20201231T000000Z:BTCUSD:buy:old0", Side::Buy, "BTC/USD", "0.01", "20000");
    let r = h.live(0);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    assert_eq!(r.cleanup.len(), 1);
    assert_eq!(r.cleanup[0].broker_order_id, stale);
    assert!(!h.env.rig.handle.order(&stale).unwrap().status.is_live());
    assert_eq!(r.placed.len(), 2);
    // Cleanup happens between reading the account and reconciling: the step log says so.
    assert_eq!(r.step_names()[4..7], ["read_account", "cleanup", "reconcile_pre"][..]);
}

#[test]
fn a_cleanup_that_cannot_cancel_fails_closed_with_no_trading() {
    let h = Harness::new();
    rest_limit_order(&h.env, "rb1:20201231T000000Z:BTCUSD:buy:old1", Side::Buy, "BTC/USD", "0.01", "20000");
    h.env.rig.handle.defer_next_cancels(100);
    let r = h.live(0);
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_CLEANUP_FAILED"), "{:?}", r.outcome);
    assert!(r.placed.is_empty());
    assert_eq!(h.notifier.codes(), ["ALERT_RUN_FAILED"]);
}
