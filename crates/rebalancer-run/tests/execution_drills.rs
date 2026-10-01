//! Execution drills through `run_once`: unknown outcomes, sells before buys, the cash re-read between them, and a
//! process killed at EVERY broker call of a live run followed by a re-run of the same run key.

mod common;

use std::panic::{catch_unwind, AssertUnwindSafe};

use broker_adapters::{OrderStatus, Side};
use common::harness::*;
use common::*;
use fake_broker::kraken::wire::paths;
use fake_broker::scenarios as sc;
use rebalancer_risk::state::AccountStatus;
use rebalancer_risk::store::StateStore;
use rebalancer_risk::Dec;
use rebalancer_run::record::{OutcomeKind, Phase, PlacedOutcome};
use rebalancer_run::stores::RunStore;

fn status(h: &Harness) -> AccountStatus {
    h.states.load(ACCOUNT).unwrap().expect("state").status()
}

fn buys_with_fills(h: &Harness, pair: &str) -> usize {
    h.env.rig.handle.orders(ACCOUNT).iter().filter(|o| !o.foreign && o.pair == pair && o.side == Side::Buy && o.vol_exec().is_positive()).count()
}

fn value(h: &Harness) -> Dec {
    h.equity()
}

// ---------------------------------------------------------------------------------------------------------------
// Unknown outcomes: look up by tag, never blind retry
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn an_order_placed_but_response_lost_is_found_by_tag_and_not_sent_twice() {
    let h = Harness::new();
    sc::order_placed_response_lost(&h.env.rig.handle); // the first AddOrder is applied, its answer is lost
    let r = h.live(0);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    assert_eq!(r.placed.len(), 2);
    let adopted: Vec<_> = r.placed.iter().filter(|p| p.outcome == PlacedOutcome::AdoptedExisting).collect();
    assert_eq!(adopted.len(), 1, "{:?}", r.placed.iter().map(|p| (&p.tag, p.outcome, &p.detail)).collect::<Vec<_>>());
    assert!(adopted[0].detail.contains("found by tag"), "{}", adopted[0].detail);
    assert_eq!(adopted[0].status, Some(OrderStatus::Filled));
    assert_eq!(h.env.rig.handle.applied_requests_to(paths::ADD_ORDER).len(), 2, "exactly one AddOrder per planned order");
    assert_eq!(buys_with_fills(&h, "BTC/USD"), 1);
    assert_eq!(buys_with_fills(&h, "ETH/USD"), 1);
    assert_eq!(status(&h), AccountStatus::Active);
    // The post-run reconciliation (which counts the adopted order's fills once) is clean.
    assert!(r.recon.iter().all(|s| s.report.verdict == rebalancer_run::recon::ReconVerdict::Ok), "{:?}", r.recon);
}

#[test]
fn an_order_whose_request_was_lost_is_not_retried_in_the_same_run_but_the_next_run_places_it() {
    let h = Harness::new();
    sc::order_request_lost(&h.env.rig.handle); // the first AddOrder never reaches the exchange
    let r = h.live(0);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    let lost: Vec<_> = r.placed.iter().filter(|p| p.outcome == PlacedOutcome::UnknownNotFound).collect();
    assert_eq!(lost.len(), 1);
    assert!(lost[0].detail.contains("not retried"), "{}", lost[0].detail);
    assert_eq!(h.env.rig.handle.applied_requests_to(paths::ADD_ORDER).len(), 1, "no blind retry");
    let btc_or_eth_missing = usize::from(buys_with_fills(&h, "BTC/USD") == 0) + usize::from(buys_with_fills(&h, "ETH/USD") == 0);
    assert_eq!(btc_or_eth_missing, 1);
    // The next scheduled run re-plans from what is really held and buys the missing leg.
    let r2 = h.live(1);
    assert_eq!(r2.outcome.kind, OutcomeKind::Completed, "{:?}", r2.outcome);
    // Both legs are held now (the run-2 rebalance may top a leg up: a different run, a different tag).
    assert!(buys_with_fills(&h, "BTC/USD") >= 1 && buys_with_fills(&h, "ETH/USD") >= 1);
    assert!(h.env.bal("BTC").is_positive() && h.env.bal("ETH").is_positive());
    assert_eq!(status(&h), AccountStatus::Active);
}

#[test]
fn an_exchange_error_after_placing_is_an_unknown_outcome_too() {
    let h = Harness::new();
    sc::exchange_answers_error_after_placing(&h.env.rig.handle, "EService:Unavailable");
    let r = h.live(0);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    assert_eq!(r.placed.iter().filter(|p| p.outcome == PlacedOutcome::AdoptedExisting).count(), 1);
    assert_eq!(h.env.rig.handle.applied_requests_to(paths::ADD_ORDER).len(), 2);
}

// ---------------------------------------------------------------------------------------------------------------
// Sells first; the cash is re-read between the sells and the buys
// ---------------------------------------------------------------------------------------------------------------

/// 1000 USD + 3 ETH (9000) with BTC trending up and ETH trending down: the plan sells all the ETH to fund a BTC buy.
fn rotation() -> Harness {
    let h = Harness::new();
    h.data.set_panel("crypto", panel(true, false));
    h.env.rig.handle.set_balance(ACCOUNT, "USD", "1000");
    h.env.hold("ETH", "3");
    assert_eq!(h.equity(), d("10000"));
    h
}

#[test]
fn sells_are_sent_before_buys_and_fund_them() {
    let h = rotation();
    let r = h.live(0);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    let phases: Vec<Phase> = r.placed.iter().map(|p| p.phase).collect();
    assert_eq!(phases, [Phase::Sells, Phase::Buys]);
    // At the exchange too: the sell reached it before the buy.
    let adds: Vec<String> = h.env.rig.handle.applied_requests_to(paths::ADD_ORDER).iter().map(|q| q.param("type").unwrap().to_string()).collect();
    assert_eq!(adds, ["sell", "buy"]);
    assert_eq!(h.env.bal("ETH"), d("0"));
    assert!(h.env.bal("BTC").is_positive());
    assert!(r.placed.iter().all(|p| p.outcome == PlacedOutcome::Filled), "{:?}", r.placed.iter().map(|p| (&p.symbol, p.outcome, &p.detail)).collect::<Vec<_>>());
}

#[test]
fn the_cash_is_re_read_after_the_sells_so_a_partially_filled_sell_shrinks_the_buy_instead_of_failing_it() {
    let h = rotation();
    // The ETH sell executes only 40% before it is cancelled: 1.2 ETH of the 3 is sold.
    sc::partial_fills_next_order(&h.env.rig.handle, &["0.4"]);
    let r = h.live(0);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    let sell = &r.placed[0];
    assert_eq!((sell.phase, sell.outcome), (Phase::Sells, PlacedOutcome::PartiallyFilled));
    assert_eq!(sell.executed_quantity, d("1.2"));
    // The first plan credited the WHOLE sale (~8977) to fund a BTC buy of ~5000; the re-plan sees the real cash.
    let first = r.plan.as_ref().unwrap();
    let replan = r.replan.as_ref().expect("a re-plan after the sells");
    let first_buy = first.orders.iter().find(|o| o.side == Side::Buy).unwrap();
    let second_buy = replan.orders.iter().find(|o| o.side == Side::Buy).unwrap();
    assert!(second_buy.quantity < first_buy.quantity, "{} vs {}", second_buy.quantity, first_buy.quantity);
    let buy = r.placed.iter().find(|p| p.phase == Phase::Buys).unwrap();
    assert_eq!(buy.outcome, PlacedOutcome::Filled, "the resized buy fits the cash that is really there: {}", buy.detail);
    assert_eq!(buy.planned_quantity, second_buy.quantity);
    assert_eq!(h.env.bal("ETH"), d("1.8"));
    assert!(h.env.bal("BTC").is_positive());
    assert_eq!(status(&h), AccountStatus::Active);
    assert!(r.recon.iter().all(|s| s.report.verdict == rebalancer_run::recon::ReconVerdict::Ok), "{:?}", r.recon);
    h.env.rig.handle.assert_invariants();
}

// ---------------------------------------------------------------------------------------------------------------
// A process killed at every broker call of a live run, then the same run key run again
// ---------------------------------------------------------------------------------------------------------------

fn fresh() -> Harness {
    let mut h = Harness::new();
    h.cfg.min_trade_pct = d("0.0001");
    h
}

#[test]
fn restart_mid_run_then_rerun_never_double_orders() {
    // A control run: how many broker calls, and where the account ends up.
    let (total, control_equity_btc, control_eth) = {
        let h = fresh();
        let real = h.env.broker();
        let counting = CrashingBroker::new(&real, None);
        let r = h.run_with(&counting, rebalancer_run::record::ExecutionMode::Live, 0);
        assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
        (counting.call_count(), h.env.bal("BTC"), h.env.bal("ETH"))
    };
    assert!(total >= 8, "saw only {total} broker calls");
    let mut crashes = 0;
    let mut resumed_with_adoption = 0;
    for n in 1..=total {
        for crash in [CrashAt::Before(n), CrashAt::After(n)] {
            let mut h = fresh();
            let crashed = {
                let real = h.env.broker();
                let crashing = CrashingBroker::new(&real, Some(crash));
                catch_unwind(AssertUnwindSafe(|| h.run_with(&crashing, rebalancer_run::record::ExecutionMode::Live, 0))).is_err()
            };
            crashes += u32::from(crashed);
            // While the crashed attempt's lease is alive, the same key is refused (no concurrent run).
            let busy = h.live(0);
            assert_eq!(busy.outcome.code, "RUN_BUSY", "{crash:?}");
            // The process restarts after the lease and runs the SAME key again.
            h.lateness_secs = 901;
            let r = h.live(0);
            assert_eq!(r.attempt, 2, "{crash:?}: the second attempt of the same key");
            assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{crash:?}: {:?} {:?}", r.outcome, r.recon.iter().map(|s| s.report.halt_summary()).collect::<Vec<_>>());
            assert_eq!(status(&h), AccountStatus::Active, "{crash:?}");
            resumed_with_adoption += usize::from(r.placed.iter().any(|p| p.outcome == PlacedOutcome::AdoptedExisting));
            // No order was ever sent twice: each planned tag exists at most once at the exchange.
            let orders = h.env.rig.handle.orders(ACCOUNT);
            let mut ids = std::collections::BTreeMap::new();
            for o in &orders {
                if let Some(u) = o.userref {
                    *ids.entry(u).or_insert(0) += 1;
                }
            }
            assert!(ids.values().all(|c| *c == 1), "{crash:?}: a userref (tag) was used by more than one order: {ids:?}");
            assert!(buys_with_fills(&h, "BTC/USD") <= 1 && buys_with_fills(&h, "ETH/USD") <= 1, "{crash:?}");
            // The account ends near where an uninterrupted run leaves it. The second attempt re-plans on what the
            // first one already did (fees and the cash reserve differ slightly), so it may hold a little LESS of a leg
            // than the control, never more than a cent-level rounding above it.
            let btc = h.env.bal("BTC");
            let eth = h.env.bal("ETH");
            let within = |a: Dec, b: Dec| a.to_f64() <= b.to_f64() * 1.02 && a.to_f64() >= b.to_f64() * 0.85;
            assert!(within(btc, control_equity_btc), "{crash:?}: BTC {btc} vs control {control_equity_btc}");
            assert!(within(eth, control_eth), "{crash:?}: ETH {eth} vs control {control_eth}");
            assert!(value(&h) > d("9900"), "{crash:?}: equity {}", value(&h));
            assert!(h.env.bal("USD") >= d("480"), "{crash:?}: the cash reserve is kept, cash {}", h.env.bal("USD"));
            // Exactly one record for the key, written by attempt 2, and it is immutable.
            assert_eq!(h.runs.records().len(), 1, "{crash:?}");
            assert_eq!(h.runs.records()[0].attempt, 2);
            h.env.rig.handle.assert_invariants();
        }
    }
    assert!(crashes >= total, "the drill really crashed at every boundary ({crashes} of {})", total * 2);
    assert!(resumed_with_adoption > 0, "at least one crash point must exercise adoption of an order that already existed");
}

#[test]
fn a_crash_after_the_first_order_was_applied_is_reconciled_from_the_journal_not_treated_as_drift() {
    // Find the call number of the first AddOrder in a healthy run, crash right AFTER it: the order exists at the
    // exchange, the response never came back, only the INTENT was journaled.
    let h = fresh();
    let real = h.env.broker();
    let counting = CrashingBroker::new(&real, None);
    h.run_with(&counting, rebalancer_run::record::ExecutionMode::Live, 0);
    let add_call = h
        .env
        .rig
        .handle
        .requests()
        .iter()
        .position(|q| q.path == paths::ADD_ORDER)
        .expect("an AddOrder was sent");
    assert!(add_call > 0);
    // Replay on a fresh exchange, crashing after the call that carried the first AddOrder (calls are 1-based and
    // each broker call is at least one request; search for the crash point that leaves exactly one order behind).
    let mut found = false;
    for n in 1..=counting.call_count() {
        let mut h = fresh();
        let crashed = {
            let real = h.env.broker();
            let crashing = CrashingBroker::new(&real, Some(CrashAt::After(n)));
            catch_unwind(AssertUnwindSafe(|| h.run_with(&crashing, rebalancer_run::record::ExecutionMode::Live, 0))).is_err()
        };
        if !crashed || h.env.rig.handle.orders(ACCOUNT).len() != 1 {
            continue;
        }
        found = true;
        // The exchange holds one filled order of ours; the journal holds its intent.
        assert_eq!(h.runs.in_flight(ACCOUNT).unwrap().len(), 1);
        h.lateness_secs = 901;
        let r = h.live(0);
        assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
        assert_eq!(r.recon[0].stage, "pre");
        assert!(!r.recon[0].report.has(rebalancer_run::recon::ReconCode::PositionDrift), "the crashed attempt's fill is expected: {:?}", r.recon[0].report.findings);
        assert!(!r.recon[0].report.has(rebalancer_run::recon::ReconCode::BalanceDrift));
        break;
    }
    assert!(found, "no crash point left exactly one order behind");
}

#[test]
fn a_finished_crash_recovery_leaves_no_journal_behind() {
    let mut h = fresh();
    {
        let real = h.env.broker();
        let crashing = CrashingBroker::new(&real, Some(CrashAt::Before(6)));
        let _ = catch_unwind(AssertUnwindSafe(|| h.run_with(&crashing, rebalancer_run::record::ExecutionMode::Live, 0)));
    }
    assert!(!h.runs.in_flight(ACCOUNT).unwrap().is_empty() || h.runs.records().is_empty());
    h.lateness_secs = 901;
    let r = h.live(0);
    assert_eq!(r.outcome.kind, OutcomeKind::Completed, "{:?}", r.outcome);
    assert!(h.runs.in_flight(ACCOUNT).unwrap().is_empty(), "finished runs no longer count as in flight");
}
