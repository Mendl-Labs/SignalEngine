//! When does a decision count as ACTED? (Slice S-3 of the paper-pilot plan, finding 3: acted-but-not-placed.)
//!
//! Before this slice a run that reached its end marked every planned decision acted whatever happened to its orders,
//! so a month could be lost silently. The rule under test (`pipeline` module docs, "When a decision counts as
//! acted"): `D_acted` advances only if EVERY order the plan called for was carried out (or ticketed in Assisted
//! mode), or the book was already at target; otherwise the run ends `Completed` with `RUN_DECISION_NOT_ACTED`,
//! `acted = false`, and an `ALERT_DECISION_NOT_ACTED` (Critical; Warning when every gap is "market closed").
//!
//! The scenarios run the real pipeline against the stateful fake Kraken exchange (no Alpaca fake exists yet; the
//! rule is venue-independent: it reads only `PlacedOutcome`s and the plan). The crypto sleeve is `Daily`, so it is
//! planned on every run; `InMemoryRunStore::last_acted_decision` is nevertheless updated for every acted decision,
//! which is what the store-level assertions read. The ETF (`OnDecision`) end-to-end version is in
//! `etf_pending_decision.rs` (`s3_*`).

mod common;

use broker_adapters::{BrokerError, ErrorClass, ExchangeError, OrderKind, OrderReport, OrderStatus, PlaceOutcome, SentOrder, Side};
use common::harness::*;
use common::*;
use fake_broker::scenarios as sc;
use rebalancer_run::record::{AlertSeverity, ExecutionMode, OutcomeKind, PlacedOrder, PlacedOutcome, Phase, RunRecord};
use rebalancer_run::stores::RunStore;
use serde_json::json;

fn sent(req: &broker_adapters::OrderRequest) -> SentOrder {
    SentOrder { broker_pair: req.symbol.clone(), side: req.side, quantity: req.quantity, price: req.reference_price, userref: 0, validate_only: req.validate_only }
}

fn market_closed() -> BrokerError {
    BrokerError::MarketClosed { next_open: "2026-10-02T13:30:00Z".into(), next_close: "2026-10-02T20:00:00Z".into() }
}

fn refused(req: &broker_adapters::OrderRequest) -> PlaceOutcome {
    PlaceOutcome::Rejected { errors: vec![ExchangeError { code: "EOrder:Insufficient funds".into(), class: ErrorClass::InsufficientFunds }], sent: sent(req) }
}

fn alert_count(h: &Harness) -> usize {
    h.notifier.codes().iter().filter(|c| **c == "ALERT_DECISION_NOT_ACTED").count()
}

fn severity(h: &Harness) -> Option<AlertSeverity> {
    h.notifier.alerts().iter().find(|a| a.code.as_str() == "ALERT_DECISION_NOT_ACTED").map(|a| a.severity)
}

fn acted(r: &RunRecord) -> bool {
    r.decisions.iter().all(|d| d.planned && d.acted)
}

fn not_acted(r: &RunRecord) -> bool {
    r.decisions.iter().all(|d| d.planned && !d.acted)
}

fn store_acted(h: &Harness) -> bool {
    h.runs.last_acted_decision(ACCOUNT, "crypto").unwrap().is_some()
}

// ---------------------------------------------------------------------------------------------------------------
// The per-leg rule, table-driven over every PlacedOutcome
// ---------------------------------------------------------------------------------------------------------------

fn report(executed: &str, status: OrderStatus) -> OrderReport {
    OrderReport {
        broker_order_id: "X1".into(),
        userref: None,
        tag: Some("rb1:t".into()),
        symbol: "BTC/USD".into(),
        side: Some(Side::Buy),
        kind: Some(OrderKind::Market),
        status,
        raw_status: "raw".into(),
        reason: None,
        quantity: d("1"),
        executed_quantity: d(executed),
        avg_price: None,
        cost: None,
        fee: None,
        open_time: None,
        close_time: None,
    }
}

fn leg(outcome: PlacedOutcome, executed: &str, reports: Vec<OrderReport>, detail: &str) -> PlacedOrder {
    PlacedOrder {
        phase: Phase::Buys,
        tag: "rb1:t".into(),
        symbol: "BTC/USD".into(),
        side: Side::Buy,
        planned_quantity: d("1"),
        price: d("100"),
        outcome,
        broker_order_id: None,
        status: None,
        executed_quantity: d(executed),
        reports,
        anomalies: Vec::new(),
        detail: detail.into(),
    }
}

#[test]
fn per_leg_rule_is_pinned_for_every_outcome() {
    // (outcome, executed quantity, reports found later, expected carried)
    let table: Vec<(PlacedOutcome, &str, Vec<OrderReport>, bool)> = vec![
        (PlacedOutcome::Filled, "1", vec![], true),
        (PlacedOutcome::PartiallyFilled, "0.4", vec![], true),
        (PlacedOutcome::Validated, "0", vec![], true),
        (PlacedOutcome::AdoptedExisting, "1", vec![], true),
        (PlacedOutcome::AdoptedExisting, "0", vec![], false), // adopted, then it ended with nothing executed
        (PlacedOutcome::UnknownNotFound, "0", vec![], false),
        (PlacedOutcome::UnknownNotFound, "0", vec![report("0", OrderStatus::Open)], false), // turned up, but still nothing executed
        (PlacedOutcome::UnknownNotFound, "0", vec![report("1", OrderStatus::Filled)], true), // turned up executed
        (PlacedOutcome::NothingExecuted, "0", vec![], false),
        (PlacedOutcome::Rejected, "0", vec![], false),
        (PlacedOutcome::NotSent, "0", vec![], false),
        (PlacedOutcome::Unsettled, "0", vec![], false),
    ];
    for (outcome, executed, reports, want) in table {
        let p = leg(outcome, executed, reports.clone(), "");
        assert_eq!(p.carried(), want, "{outcome:?} executed {executed} reports {}", reports.len());
    }
    // every PlacedOutcome variant is covered by the table above (a new variant must be classified here on purpose)
    let covered = [
        PlacedOutcome::Filled,
        PlacedOutcome::PartiallyFilled,
        PlacedOutcome::NothingExecuted,
        PlacedOutcome::Validated,
        PlacedOutcome::Rejected,
        PlacedOutcome::NotSent,
        PlacedOutcome::AdoptedExisting,
        PlacedOutcome::UnknownNotFound,
        PlacedOutcome::Unsettled,
    ];
    for o in covered {
        // Exhaustive on purpose: a new PlacedOutcome variant must fail to compile here.
        match o {
            PlacedOutcome::Filled
            | PlacedOutcome::PartiallyFilled
            | PlacedOutcome::NothingExecuted
            | PlacedOutcome::Validated
            | PlacedOutcome::Rejected
            | PlacedOutcome::NotSent
            | PlacedOutcome::AdoptedExisting
            | PlacedOutcome::UnknownNotFound
            | PlacedOutcome::Unsettled => (),
        }
    }
}

#[test]
fn only_a_not_sent_leg_with_the_marker_counts_as_market_closed() {
    assert!(leg(PlacedOutcome::NotSent, "0", vec![], "not sent: MARKET_CLOSED: market is closed").refused_market_closed());
    assert!(!leg(PlacedOutcome::NotSent, "0", vec![], "not sent: account cannot trade: blocked").refused_market_closed());
    assert!(!leg(PlacedOutcome::Rejected, "0", vec![], "MARKET_CLOSED").refused_market_closed(), "a venue rejection is not a closed market");
}

// ---------------------------------------------------------------------------------------------------------------
// Run level, table-driven: every outcome combination through the real pipeline
// ---------------------------------------------------------------------------------------------------------------

struct Case {
    name: &'static str,
    run: fn() -> (Harness, RunRecord),
    code: &'static str,
    acted: bool,
    /// The alert severity expected, `None` = no ALERT_DECISION_NOT_ACTED at all.
    alert: Option<AlertSeverity>,
}

/// Fresh harness; the given `place` answers are injected in Live mode.
fn live_with(hook: impl Fn(&broker_adapters::OrderRequest) -> Option<Result<PlaceOutcome, BrokerError>>) -> (Harness, RunRecord) {
    let h = Harness::new();
    let real = h.env.broker();
    let scripted = ScriptedPlaceBroker::new(&real, hook);
    let r = h.run_with(&scripted, ExecutionMode::Live, 0);
    drop(scripted);
    (h, r)
}

fn cases() -> Vec<Case> {
    vec![
        Case { name: "every leg filled", run: || { let h = Harness::new(); let r = h.live(0); (h, r) }, code: "RUN_COMPLETED", acted: true, alert: None },
        Case {
            name: "market closed: no leg sent",
            run: || live_with(|_| Some(Err(market_closed()))),
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Warning),
        },
        Case {
            name: "account blocked (a pre-order refusal): no leg sent",
            run: || live_with(|_| Some(Err(BrokerError::AccountBlocked("trading_blocked".into())))),
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Critical),
        },
        Case {
            name: "pre-order check failed: no leg sent",
            run: || live_with(|_| Some(Err(BrokerError::Preflight("transport: connect".into())))),
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Critical),
        },
        Case {
            name: "every leg refused by the venue (buying power)",
            run: || live_with(|req| Some(Ok(refused(req)))),
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Critical),
        },
        Case {
            name: "partial: BTC leg refused as market closed, ETH leg filled",
            run: || live_with(|req| (req.symbol == "BTC/USD").then(|| Err(market_closed()))),
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Warning),
        },
        Case {
            name: "partial: BTC leg blocked, ETH leg filled (a mix is Critical, not Warning)",
            run: || live_with(|req| (req.symbol == "BTC/USD").then(|| Err(BrokerError::AccountBlocked("x".into())))),
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Critical),
        },
        Case {
            name: "partial: one leg market closed, the other refused by the venue: Critical",
            run: || live_with(|req| Some(if req.symbol == "BTC/USD" { Err(market_closed()) } else { Ok(refused(req)) })),
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Critical),
        },
        Case {
            name: "unknown outcome, the look-up finds nothing (request lost), other leg filled",
            run: || {
                let h = Harness::new();
                sc::order_request_lost(&h.env.rig.handle);
                let r = h.live(0);
                (h, r)
            },
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Critical),
        },
        Case {
            name: "accepted but never executed, cancelled at settle, other leg filled",
            run: || {
                let h = Harness::new();
                sc::no_fill_next_order(&h.env.rig.handle);
                let r = h.live(0);
                (h, r)
            },
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Critical),
        },
        Case {
            name: "one leg partially filled: the order was accepted and traded, so it is carried",
            run: || {
                let h = Harness::new();
                sc::partial_fills_next_order(&h.env.rig.handle, &["0.4"]);
                let r = h.live(0);
                (h, r)
            },
            code: "RUN_COMPLETED",
            acted: true,
            alert: None,
        },
        Case {
            name: "response lost but the order exists: adopted by tag, executed",
            run: || {
                let h = Harness::new();
                sc::order_placed_response_lost(&h.env.rig.handle);
                let r = h.live(0);
                (h, r)
            },
            code: "RUN_COMPLETED",
            acted: true,
            alert: None,
        },
        Case {
            name: "guard denies every order (notional cap): nothing to send, target not reached",
            run: || {
                let h = Harness::new().with_mandate(|v| v["exposure"]["max_order_notional"]["amount"] = json!("100.00"));
                let r = h.live(0);
                (h, r)
            },
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Critical),
        },
        Case {
            name: "planner skips one leg for lack of a price (NoPrice: not dust), the other leg filled",
            run: || {
                let mut h = Harness::new();
                let handle = h.env.rig.handle.clone();
                h.data = rebalancer_run::testkit::FixtureData::new()
                    .with_panel("crypto", panel(true, true))
                    .with_price_fn(move |sym| (sym == "ETH/USD").then(|| handle.price(sym)));
                let r = h.live(0);
                (h, r)
            },
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Critical),
        },
        Case {
            name: "assisted: tickets written, nothing placed (a person now holds the decision)",
            run: || { let h = Harness::new(); let r = h.run(ExecutionMode::Assisted, 0); (h, r) },
            code: "RUN_COMPLETED",
            acted: true,
            alert: None,
        },
        Case {
            name: "assisted but the guard denies every order: no usable ticket",
            run: || {
                let h = Harness::new().with_mandate(|v| v["exposure"]["max_order_notional"]["amount"] = json!("100.00"));
                let r = h.run(ExecutionMode::Assisted, 0);
                (h, r)
            },
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Critical),
        },
        Case {
            name: "paper (validate-only): the venue validated every order",
            run: || { let h = Harness::new(); let r = h.run(ExecutionMode::Paper, 0); (h, r) },
            code: "RUN_COMPLETED",
            acted: true,
            alert: None,
        },
        Case {
            name: "paper: an order that could not even be sent is not acted",
            run: || {
                let h = Harness::new();
                let real = h.env.broker();
                let scripted = ScriptedPlaceBroker::new(&real, |_| Some(Err(BrokerError::Preflight("down".into()))));
                let r = h.run_with(&scripted, ExecutionMode::Paper, 0);
                drop(scripted);
                (h, r)
            },
            code: "RUN_DECISION_NOT_ACTED",
            acted: false,
            alert: Some(AlertSeverity::Critical),
        },
    ]
}

#[test]
fn every_outcome_combination_sets_acted_code_and_alert_as_specified() {
    let mut failures = Vec::new();
    for c in cases() {
        let (h, r) = (c.run)();
        let mut problems = Vec::new();
        // The pipeline ran to its end in every case: kind is Completed (orders that WERE sent stay sent).
        if r.outcome.kind != OutcomeKind::Completed {
            problems.push(format!("kind {:?} ({:?})", r.outcome.kind, r.outcome));
        }
        if r.outcome.code != c.code {
            problems.push(format!("code {} != {} ({})", r.outcome.code, c.code, r.outcome.message));
        }
        if r.decisions.len() != 1 || !r.decisions[0].planned {
            problems.push("the sleeve was not planned".into());
        }
        if acted(&r) != c.acted || not_acted(&r) == c.acted {
            problems.push(format!("acted {} != {}", acted(&r), c.acted));
        }
        if store_acted(&h) != c.acted {
            problems.push(format!("store D_acted moved = {} != {}", store_acted(&h), c.acted));
        }
        if severity(&h) != c.alert {
            problems.push(format!("alert {:?} != {:?}", severity(&h), c.alert));
        }
        if alert_count(&h) != usize::from(c.alert.is_some()) {
            problems.push(format!("{} not-acted alerts", alert_count(&h)));
        }
        // The alert is on the record too (it is part of the audit trail), and the record is what was stored.
        if r.alerts.iter().any(|a| a.code.as_str() == "ALERT_DECISION_NOT_ACTED") != c.alert.is_some() {
            problems.push("record alerts disagree".into());
        }
        if !problems.is_empty() {
            failures.push(format!("{}: {}", c.name, problems.join("; ")));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

// ---------------------------------------------------------------------------------------------------------------
// Specific properties
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_book_already_at_target_counts_as_acted_because_there_is_nothing_to_do() {
    let h = Harness::new();
    let first = h.live(0);
    assert_eq!(first.outcome.code, "RUN_COMPLETED", "{:?}", first.outcome);
    let second = h.live(1);
    let plan = second.plan.as_ref().unwrap();
    assert!(plan.orders.is_empty() && plan.denied.is_empty(), "the account is at target: {:?}", plan.lines);
    assert!(second.placed.is_empty());
    assert_eq!((second.outcome.kind, second.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{:?}", second.outcome);
    assert!(acted(&second), "no orders needed = nothing left undone = acted");
    assert_eq!(alert_count(&h), 0);
}

#[test]
fn a_run_with_no_sleeves_has_no_decision_to_act_on_and_raises_nothing() {
    let h = Harness::risk_only();
    let r = h.live(0);
    assert_eq!(r.outcome.code, "RUN_COMPLETED");
    assert!(r.decisions.is_empty());
    assert_eq!(alert_count(&h), 0);
}

#[test]
fn market_closed_is_recorded_with_its_marker_and_the_next_run_acts_once_on_the_same_decision() {
    let h = Harness::new();
    let real = h.env.broker();
    let closed = ScriptedPlaceBroker::new(&real, |_| Some(Err(market_closed())));
    let r0 = h.run_with(&closed, ExecutionMode::Live, 0);
    assert_eq!(r0.outcome.code, "RUN_DECISION_NOT_ACTED");
    assert_eq!(closed.place_calls(), 2, "both legs were attempted, none reached the exchange");
    assert!(r0.placed.iter().all(|p| p.outcome == PlacedOutcome::NotSent && p.refused_market_closed() && p.detail.contains("MARKET_CLOSED")), "{:?}", r0.placed);
    assert_eq!(h.env.order_requests(), 0, "nothing reached the exchange");
    assert!(!store_acted(&h));
    // Next scheduled run, market open: the same decision is still pending and is now carried out, once.
    let r1 = h.live(1);
    assert_eq!((r1.outcome.kind, r1.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{:?}", r1.outcome);
    assert!(acted(&r1) && store_acted(&h));
    assert_eq!(r1.placed.iter().filter(|p| p.outcome == PlacedOutcome::Filled).count(), 2);
    assert!(h.env.bal("BTC").is_positive() && h.env.bal("ETH").is_positive());
    assert_eq!(alert_count(&h), 1, "one Warning for the closed run, none for the recovery");
}

#[test]
fn partial_execution_leaves_the_decision_pending_and_the_retry_sends_only_the_missing_leg() {
    let h = Harness::new();
    let real = h.env.broker();
    let blocked_btc = ScriptedPlaceBroker::new(&real, |req| (req.symbol == "BTC/USD").then(|| Err(BrokerError::AccountBlocked("trading_blocked".into()))));
    let r0 = h.run_with(&blocked_btc, ExecutionMode::Live, 0);
    assert_eq!(r0.outcome.code, "RUN_DECISION_NOT_ACTED");
    let (filled, missing): (Vec<_>, Vec<_>) = r0.placed.iter().partition(|p| p.carried());
    assert_eq!((filled.len(), missing.len()), (1, 1));
    assert_eq!((filled[0].symbol.as_str(), missing[0].symbol.as_str()), ("ETH/USD", "BTC/USD"));
    assert!(r0.outcome.message.contains("WERE carried out"), "the alert says the account is part-way: {}", r0.outcome.message);
    let eth_after_first = h.env.bal("ETH");
    assert!(eth_after_first.is_positive() && h.env.bal("BTC").is_zero());
    assert!(!store_acted(&h));
    // The post-run reconciliation of the part-filled run is clean: an order that was never sent is not "expected".
    assert!(r0.recon.iter().all(|s| s.report.verdict == rebalancer_run::recon::ReconVerdict::Ok), "{:?}", r0.recon);

    let r1 = h.live(1);
    assert_eq!(r1.outcome.code, "RUN_COMPLETED", "{:?}", r1.outcome);
    // The missing leg (BTC) is sent. ETH may ALSO get a small top-up (the plan is a fresh re-plan of the real
    // account, and a day has passed), but it is never SOLD or re-bought from scratch: the carried leg is not
    // repeated, only topped up if the target moved.
    let symbols: Vec<&str> = r1.placed.iter().map(|p| p.symbol.as_str()).collect();
    assert!(symbols.contains(&"BTC/USD"), "{symbols:?}");
    assert!(r1.placed.iter().filter(|p| p.symbol == "ETH/USD").all(|p| p.side == Side::Buy), "{symbols:?}: no ETH sell");
    assert!(h.env.bal("ETH") >= eth_after_first, "the carried leg is not sold: {} vs {eth_after_first}", h.env.bal("ETH"));
    assert!(h.env.bal("BTC").is_positive() && store_acted(&h));
}

#[test]
fn a_guard_denial_is_named_in_the_alert_message() {
    let h = Harness::new().with_mandate(|v| v["exposure"]["max_order_notional"]["amount"] = json!("4000.00"));
    h.data.set_panel("crypto", panel(true, true));
    let r = h.live(0);
    let plan = r.plan.as_ref().unwrap();
    // Both coins are 50% of 10000 = 5000 per order, above the 4000 cap: both denied (no order at all).
    assert!(plan.orders.is_empty() && plan.denied.len() == 2, "{:?}", plan);
    assert_eq!(r.outcome.code, "RUN_DECISION_NOT_ACTED");
    assert!(r.outcome.message.contains("guard denied"), "{}", r.outcome.message);
    assert!(!store_acted(&h));
}

#[test]
fn a_run_that_did_not_complete_still_acts_on_nothing() {
    // The pre-existing rule is unchanged: a broker that is down fails the run closed, nothing is acted.
    let h = Harness::new();
    let real = h.env.broker();
    let dead = CrashingBroker::new(&real, None);
    h.env.rig.handle.set_balance(ACCOUNT, "USD", "10000");
    sc::broker_unreachable_for(&h.env.rig.handle, 50);
    let r = h.run_with(&dead, ExecutionMode::Live, 0);
    assert_eq!(r.outcome.kind, OutcomeKind::FailedClosed);
    assert!(r.decisions.iter().all(|d| !d.acted));
    assert!(!store_acted(&h));
}
