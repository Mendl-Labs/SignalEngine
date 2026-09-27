//! The paper-only venue policy (slice S-8 of the paper-pilot plan): under `VenuePolicy::PaperOnly` the pipeline runs
//! ONLY a broker that reports a paper connection, in every execution mode, and refuses anything else (a live
//! environment, or a broker that does not say) before the broker is read or an order is sent. This is what lets the
//! pilot use `ExecutionMode::Live` (the only mode that places real Alpaca orders; Alpaca has no validate-only) without
//! ever being able to pair it with a live account.

mod common;

use std::sync::Arc;

use broker_adapters::alpaca::config::{LIVE_BASE_URL, PAPER_BASE_URL};
use broker_adapters::alpaca::{AlpacaAdapter, AlpacaConfig, AlpacaCredentials, Environment, PaperOnlyAlpaca};
use broker_adapters::testing::FakeTransport;
use common::harness::*;
use common::*;
use rebalancer_run::broker::{AlpacaBroker, Broker, VenueEnvironment};
use rebalancer_run::pipeline::{RunCode, RunConfig, VenuePolicy};
use rebalancer_run::record::{AlertSeverity, ExecutionMode, OutcomeKind, RunRecord};

const MODES: [ExecutionMode; 3] = [ExecutionMode::Assisted, ExecutionMode::Paper, ExecutionMode::Live];

fn paper_only_harness() -> Harness {
    let mut h = Harness::new();
    h.cfg.venue_policy = VenuePolicy::PaperOnly;
    h
}

fn assert_refused(r: &RunRecord, h: &Harness, why: &str) {
    assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_VENUE_NOT_PAPER"), "{why}: {:?}", r.outcome);
    assert_eq!(r.step_names(), ["acquire_run_key", "venue_policy"], "{why}: refused before the kill flag, the mandate, the broker read");
    assert!(r.pre_snapshot.is_none() && r.plan.is_none() && r.placed.is_empty() && r.tickets.is_empty(), "{why}");
    assert!(r.decisions.is_empty() && r.decisions.iter().all(|d| !d.acted));
    let alert = h.notifier.alerts().into_iter().find(|a| a.code.as_str() == "ALERT_RUN_FAILED").unwrap_or_else(|| panic!("{why}: no alert"));
    assert_eq!(alert.severity, AlertSeverity::Critical, "{why}");
    assert!(alert.message.contains("RUN_VENUE_NOT_PAPER"), "{why}: {}", alert.message);
    assert!(h.env.rig.handle.requests().is_empty(), "{why}: the exchange received NOTHING (not even a read)");
}

// ---------------------------------------------------------------------------------------------------------------
// The policy
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_default_policy_is_unrestricted_and_the_code_is_pinned() {
    assert_eq!(RunConfig::default().venue_policy, VenuePolicy::Unrestricted);
    assert_eq!(VenuePolicy::default(), VenuePolicy::Unrestricted);
    assert_eq!(RunCode::VenueNotPaper.as_str(), "RUN_VENUE_NOT_PAPER");
    // Unrestricted: every existing caller keeps its behaviour, whatever the broker reports.
    let h = Harness::new();
    assert_eq!(h.env.broker().environment(), VenueEnvironment::Live, "Kraken has no sandbox");
    let r = h.live(0);
    assert_eq!(r.outcome.code, "RUN_COMPLETED", "{:?}", r.outcome);
    assert!(!r.step_names().contains(&"venue_policy"), "no step is added under the default policy");
}

#[test]
fn paper_only_refuses_a_live_environment_broker_in_every_mode_without_touching_it() {
    for (n, mode) in MODES.into_iter().enumerate() {
        let h = paper_only_harness();
        let real = h.env.broker(); // reports Live
        let scripted = ScriptedPlaceBroker::new(&real, |_| None);
        let r = h.run_with(&scripted, mode, n as i64);
        assert_refused(&r, &h, &format!("{mode:?} on a live-environment broker"));
        assert_eq!(scripted.place_calls(), 0, "{mode:?}: no order was even attempted");
    }
}

#[test]
fn paper_only_refuses_a_broker_that_does_not_say_what_it_is() {
    for (n, mode) in MODES.into_iter().enumerate() {
        let h = paper_only_harness();
        let real = h.env.broker();
        let unspecified = CrashingBroker::new(&real, None); // a wrapper that forwards nothing about the environment
        assert_eq!(unspecified.environment(), VenueEnvironment::Unspecified);
        let r = h.run_with(&unspecified, mode, n as i64);
        assert_refused(&r, &h, &format!("{mode:?} on an unspecified broker"));
        assert_eq!(unspecified.call_count(), 0, "the wrapped broker was never called");
    }
}

#[test]
fn paper_only_lets_a_broker_that_reports_paper_run_in_every_mode() {
    for (n, mode) in MODES.into_iter().enumerate() {
        let h = paper_only_harness();
        let real = h.env.broker();
        let mut paper = ScriptedPlaceBroker::new(&real, |_| None);
        paper.forced_environment = Some(VenueEnvironment::Paper);
        let r = h.run_with(&paper, mode, n as i64);
        assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::Completed, "RUN_COMPLETED"), "{mode:?}: {:?}", r.outcome);
        assert_eq!(r.step_names()[..2], ["acquire_run_key", "venue_policy"], "{mode:?}: the policy is the first step after the key");
        if mode == ExecutionMode::Live {
            assert!(r.placed.iter().all(|p| p.carried()) && !r.placed.is_empty(), "Live-on-paper really places orders");
        }
    }
}

#[test]
fn the_refusal_is_recorded_per_slot_and_a_retry_of_the_same_slot_returns_the_first_record() {
    let h = paper_only_harness();
    let real = h.env.broker();
    let first = h.run_with(&real, ExecutionMode::Live, 0);
    assert_eq!(first.outcome.code, "RUN_VENUE_NOT_PAPER");
    let again = h.run_with(&real, ExecutionMode::Live, 0);
    assert_eq!(again, first, "the finished key is a no-op: a misconfiguration is not retried within the slot");
    assert_eq!(h.runs.records().len(), 1);
}

// ---------------------------------------------------------------------------------------------------------------
// The real Alpaca adapters map to the right environment
// ---------------------------------------------------------------------------------------------------------------

const PAPER_KEY: &str = "PKTESTFIXTUREKEY0001";
const LIVE_KEY: &str = "AKTESTFIXTUREKEY0002";
const SECRET: &str = "unit-test-secret-not-a-real-key-9f3a";

fn paper_adapter(t: Arc<FakeTransport>) -> AlpacaAdapter {
    AlpacaAdapter::new(
        AlpacaConfig::new(Environment::Paper, PAPER_BASE_URL).unwrap(),
        AlpacaCredentials::new(Environment::Paper, PAPER_KEY, SECRET).unwrap(),
        t,
    )
    .unwrap()
}

fn live_adapter(t: Arc<FakeTransport>) -> AlpacaAdapter {
    AlpacaAdapter::new(
        AlpacaConfig::new(Environment::Live, LIVE_BASE_URL).unwrap(),
        AlpacaCredentials::new(Environment::Live, LIVE_KEY, SECRET).unwrap(),
        t,
    )
    .unwrap()
}

#[test]
fn the_alpaca_broker_reports_its_adapters_environment() {
    let t = Arc::new(FakeTransport::new());
    assert_eq!(AlpacaBroker::us_etf(&paper_adapter(t.clone())).environment(), VenueEnvironment::Paper);
    assert_eq!(AlpacaBroker::us_etf(&live_adapter(t.clone())).environment(), VenueEnvironment::Live);
    let built = PaperOnlyAlpaca::new(PAPER_KEY, SECRET, PAPER_BASE_URL, t.clone(), Some("rb1:"), true).unwrap();
    assert_eq!(AlpacaBroker::us_etf(built.adapter()).environment(), VenueEnvironment::Paper);
    assert_eq!(t.request_count(), 0);
}

#[test]
fn a_live_alpaca_adapter_is_refused_by_the_paper_only_policy_before_a_single_http_request() {
    for (n, mode) in MODES.into_iter().enumerate() {
        let h = paper_only_harness();
        let t = Arc::new(FakeTransport::new());
        let live = live_adapter(t.clone());
        let broker = AlpacaBroker::us_etf(&live);
        let r = h.run_with(&broker, mode, n as i64);
        assert_eq!((r.outcome.kind, r.outcome.code.as_str()), (OutcomeKind::FailedClosed, "RUN_VENUE_NOT_PAPER"), "{mode:?}");
        assert_eq!(t.request_count(), 0, "{mode:?}: the live API was never called");
    }
}

#[test]
fn a_paper_alpaca_adapter_passes_the_policy_check_then_meets_the_real_pipeline() {
    // With no scripted responses the paper adapter's first read fails: the point is that the POLICY step passed and
    // the run then failed closed on the broker read, not on the policy.
    let h = paper_only_harness();
    let t = Arc::new(FakeTransport::new());
    let paper = paper_adapter(t.clone());
    let broker = AlpacaBroker::us_etf(&paper);
    let r = h.run_with(&broker, ExecutionMode::Assisted, 0);
    assert_ne!(r.outcome.code, "RUN_VENUE_NOT_PAPER", "{:?}", r.outcome);
    assert!(r.step_names().contains(&"venue_policy"));
}
