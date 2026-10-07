//! The paper kill drill against the fake Alpaca exchange (fake-broker). Every test is offline: no network, no real keys,
//! no database. The drill runs the production pipeline (`run_once`) through a real `AlpacaAdapter` talking to the fake.

use std::sync::atomic::{AtomicU32, Ordering};

use broker_adapters::alpaca::{AssetTable, PrepareOptions, PAPER_BASE_URL};
use broker_adapters::{
    BrokerError, CancelOutcome, Dec, OrderReport, OrderRequest, PlaceOutcome, Quote, Side,
};
use chrono::{DateTime, Utc};
use fake_broker::alpaca_rig::AlpacaRig;
use rebalancer_alerts::{OutboundEmail, SendError, Sender};
use rebalancer_core::venue::{AlpacaRules, VenueRules};
use rebalancer_risk::state::{AccountState, HaltReason};
use rebalancer_risk::store::{InMemoryStateStore, StateStore, StoreError};
use rebalancer_run::broker::{AlpacaBroker, Broker, SnapshotError, VenueEnvironment};
use rebalancer_run::clock::ManualClock;
use rebalancer_run::view::BrokerSnapshot;
use rebalancer_service::kill_drill::{
    self, parse_args, refuse_forged_resume, run_drill, Args, DrillDeps, Refusal, ACK_FLAG,
    DRILL_SYMBOL, STEPS,
};

const LIVE_HOST: &str = "https://api.alpaca.markets";

fn start() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-10-06T14:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

/// The fake exchange with the pilot's own-tag prefix and the market open (the drill runs in regular hours).
fn rig() -> AlpacaRig {
    let rig = AlpacaRig::with_config(|mut c| {
        c.own_tag_prefix = Some("rb1:".to_string());
        c
    });
    rig.handle.set_market_open(Some(true));
    rig
}

fn args() -> Args {
    parse_args(&[ACK_FLAG]).unwrap()
}

/// Runs the drill against `broker` with the fake's rules (the built-in asset table, as the rebalancer's own drills do).
fn run_with(
    broker: &dyn Broker,
    sender: Option<&dyn Sender>,
    base_url: &str,
) -> Result<kill_drill::Report, Refusal> {
    let assets = AssetTable::builtin();
    let opts = PrepareOptions {
        allow_crypto: false,
        allow_extended_hours: false,
        min_notional: d("1"),
        own_tag_prefix: Some("rb1:".to_string()),
        refuse_builtin_assets: false,
    };
    let rules = AlpacaRules {
        assets: &assets,
        options: &opts,
    };
    let clock = ManualClock::new(start());
    let deps = DrillDeps {
        base_url,
        broker,
        rules: &rules as &dyn VenueRules,
        clock: &clock,
        sender,
        alert_to: Some("drill-alerts@example.invalid"),
        max_notional: d("10"),
        label: "fake-fingerprint",
    };
    run_drill(&deps, &args())
}

/// A test sender: records every send and answers with a fixed result.
struct FakeSender {
    result: Result<String, SendError>,
    sent: AtomicU32,
}

impl FakeSender {
    fn ok() -> Self {
        Self {
            result: Ok("provider-id-1".to_string()),
            sent: AtomicU32::new(0),
        }
    }
    fn failing(e: SendError) -> Self {
        Self {
            result: Err(e),
            sent: AtomicU32::new(0),
        }
    }
    fn sends(&self) -> u32 {
        self.sent.load(Ordering::SeqCst)
    }
}

impl Sender for FakeSender {
    fn name(&self) -> &'static str {
        "fake"
    }
    fn send(&self, _email: &OutboundEmail) -> Result<String, SendError> {
        self.sent.fetch_add(1, Ordering::SeqCst);
        self.result.clone()
    }
}

/// Wraps a broker to (a) report a chosen environment, (b) optionally refuse sells, and (c) count order placements.
/// Everything else is forwarded unchanged.
struct TestBroker<'a> {
    inner: &'a dyn Broker,
    environment: VenueEnvironment,
    refuse_sells: bool,
    places: AtomicU32,
}

impl<'a> TestBroker<'a> {
    fn new(inner: &'a dyn Broker) -> Self {
        Self {
            inner,
            environment: VenueEnvironment::Paper,
            refuse_sells: false,
            places: AtomicU32::new(0),
        }
    }
    fn placed(&self) -> u32 {
        self.places.load(Ordering::SeqCst)
    }
}

impl Broker for TestBroker<'_> {
    fn venue(&self) -> &'static str {
        self.inner.venue()
    }
    fn environment(&self) -> VenueEnvironment {
        self.environment
    }
    fn snapshot(&self, now: DateTime<Utc>) -> Result<BrokerSnapshot, SnapshotError> {
        self.inner.snapshot(now)
    }
    fn place(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        self.places.fetch_add(1, Ordering::SeqCst);
        if self.refuse_sells && matches!(req.side, Side::Sell) {
            return Err(BrokerError::Unsupported(
                "test: sells are refused".to_string(),
            ));
        }
        self.inner.place(req)
    }
    fn get_order(&self, id: &str) -> Result<OrderReport, BrokerError> {
        self.inner.get_order(id)
    }
    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        self.inner.open_orders()
    }
    fn find_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        self.inner.find_by_tag(tag)
    }
    fn cancel_and_settle(&self, id: &str) -> Result<(CancelOutcome, OrderReport), BrokerError> {
        self.inner.cancel_and_settle(id)
    }
    fn quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        self.inner.quote(symbol)
    }
}

fn names_of(report: &kill_drill::Report) -> Vec<&'static str> {
    report.steps.iter().map(|s| s.name).collect()
}

// ---------------------------------------------------------------------------------------------------------------
// Refusals: nothing is sent
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn refuses_without_the_acknowledgement_flag() {
    assert_eq!(parse_args::<&str>(&[]), Err(Refusal::MissingAck));
    assert_eq!(parse_args(&["--dry-run"]), Err(Refusal::MissingAck));
    assert!(parse_args(&[ACK_FLAG]).is_ok());
}

#[test]
fn refuses_a_live_environment_before_any_order_is_sent() {
    let rig = rig();
    let alpaca = AlpacaBroker::us_etf(&rig.adapter);
    let test = TestBroker {
        inner: &alpaca,
        environment: VenueEnvironment::Live,
        refuse_sells: false,
        places: AtomicU32::new(0),
    };
    let err = run_with(&test, None, PAPER_BASE_URL).unwrap_err();
    assert!(matches!(err, Refusal::NotPaperEnvironment(_)), "{err}");
    assert_eq!(test.placed(), 0);
    assert!(
        rig.handle.orders().is_empty(),
        "a refused drill must not reach the exchange"
    );
}

#[test]
fn refuses_an_unspecified_environment_too() {
    let rig = rig();
    let alpaca = AlpacaBroker::us_etf(&rig.adapter);
    let test = TestBroker {
        inner: &alpaca,
        environment: VenueEnvironment::Unspecified,
        refuse_sells: false,
        places: AtomicU32::new(0),
    };
    assert!(matches!(
        run_with(&test, None, PAPER_BASE_URL),
        Err(Refusal::NotPaperEnvironment(_))
    ));
    assert_eq!(test.placed(), 0);
}

#[test]
fn refuses_a_live_url_before_any_order_is_sent() {
    let rig = rig();
    let alpaca = AlpacaBroker::us_etf(&rig.adapter);
    let test = TestBroker::new(&alpaca);
    let err = run_with(&test, None, LIVE_HOST).unwrap_err();
    assert!(matches!(err, Refusal::NotPaperUrl(_)), "{err}");
    assert_eq!(test.placed(), 0);
    assert!(rig.handle.orders().is_empty());
}

#[test]
fn a_dry_run_prints_the_plan_and_places_nothing() {
    let rig = rig();
    let alpaca = AlpacaBroker::us_etf(&rig.adapter);
    let test = TestBroker::new(&alpaca);
    let dry = parse_args(&["--dry-run", ACK_FLAG]).unwrap();
    let assets = AssetTable::builtin();
    let opts = PrepareOptions {
        allow_crypto: false,
        allow_extended_hours: false,
        min_notional: d("1"),
        own_tag_prefix: Some("rb1:".to_string()),
        refuse_builtin_assets: false,
    };
    let rules = AlpacaRules {
        assets: &assets,
        options: &opts,
    };
    let clock = ManualClock::new(start());
    let deps = DrillDeps {
        base_url: PAPER_BASE_URL,
        broker: &test,
        rules: &rules as &dyn VenueRules,
        clock: &clock,
        sender: None,
        alert_to: None,
        max_notional: d("10"),
        label: "fake-fingerprint",
    };
    let report = run_drill(&deps, &dry).unwrap();
    assert!(report.steps.is_empty(), "a dry run runs no step");
    assert!(report.notes.iter().any(|n| n.contains("DRY RUN")));
    for s in STEPS {
        assert!(
            report.notes.iter().any(|n| n.contains(s)),
            "plan is missing {s}"
        );
    }
    assert_eq!(test.placed(), 0, "a dry run places nothing");
    assert!(
        rig.handle.orders().is_empty(),
        "a dry run reaches nothing on the exchange"
    );
    assert_eq!(rig.handle.position_qty(DRILL_SYMBOL), d("0"));
}

#[test]
fn the_notional_cap_cannot_be_raised() {
    assert!(parse_args(&[ACK_FLAG, "--max-notional", "25"]).is_err());
    assert_eq!(
        parse_args(&[ACK_FLAG, "--max-notional", "4"])
            .unwrap()
            .max_notional,
        d("4")
    );
}

// ---------------------------------------------------------------------------------------------------------------
// The full sequence against the fake broker
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_full_sequence_passes_against_the_fake_broker() {
    let rig = rig();
    let alpaca = AlpacaBroker::us_etf(&rig.adapter);
    let sender = FakeSender::ok();
    let report = run_with(&alpaca, Some(&sender), PAPER_BASE_URL).unwrap();
    assert!(report.passed(), "{}", report.render());
    assert_eq!(names_of(&report), STEPS.to_vec());
    assert_eq!(report.exit_code(), 0);
    // The real account ends flat with nothing open, and the alert was delivered exactly once.
    assert_eq!(rig.handle.position_qty(DRILL_SYMBOL), d("0"));
    assert!(rig.handle.open_orders().is_empty());
    assert_eq!(sender.sends(), 1);
}

#[test]
fn a_fake_that_fails_to_flatten_makes_the_drill_exit_non_zero() {
    let rig = rig();
    let alpaca = AlpacaBroker::us_etf(&rig.adapter);
    let mut test = TestBroker::new(&alpaca);
    test.refuse_sells = true;
    let sender = FakeSender::ok();
    let report = run_with(&test, Some(&sender), PAPER_BASE_URL).unwrap();
    assert!(!report.passed());
    assert_eq!(report.exit_code(), 1);
    let failed: Vec<&str> = report
        .steps
        .iter()
        .filter(|s| !s.passed)
        .map(|s| s.name)
        .collect();
    assert_eq!(failed, vec!["flatten_and_halt"], "{}", report.render());
    // The drill stopped there: nothing after the failed step ran, and the alert was never sent.
    assert_eq!(report.steps.len(), 5);
    assert_eq!(sender.sends(), 0);
    // The residual position is real and is reported, not hidden.
    assert_eq!(rig.handle.position_qty(DRILL_SYMBOL), d("0.01"));
}

#[test]
fn an_alert_that_is_not_delivered_makes_the_drill_fail() {
    let rig = rig();
    let alpaca = AlpacaBroker::us_etf(&rig.adapter);
    let sender = FakeSender::failing(SendError::Disabled);
    let report = run_with(&alpaca, Some(&sender), PAPER_BASE_URL).unwrap();
    assert_eq!(report.exit_code(), 1, "{}", report.render());
    let last = report.steps.last().unwrap();
    assert_eq!(last.name, "alert_delivery");
    assert!(!last.passed);
    assert!(last.detail.contains("failed"), "{}", last.detail);
    assert_eq!(sender.sends(), 1, "the delivery was attempted");
}

#[test]
fn no_sender_means_the_alert_step_fails_rather_than_passing_silently() {
    let rig = rig();
    let alpaca = AlpacaBroker::us_etf(&rig.adapter);
    let report = run_with(&alpaca, None, PAPER_BASE_URL).unwrap();
    assert_eq!(report.exit_code(), 1);
    let last = report.steps.last().unwrap();
    assert_eq!(last.name, "alert_delivery");
    assert!(!last.passed);
}

// ---------------------------------------------------------------------------------------------------------------
// Resume: only a human can take a halted account out of the halt
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_halted_account_cannot_be_saved_active_without_a_human_resume() {
    let store = InMemoryStateStore::new();
    let (halted, _) = AccountState::new("kill-drill").halt(HaltReason::Manual, "drill", start());
    store.save(0, &halted).unwrap();
    let outcome = refuse_forged_resume(&store, "kill-drill").unwrap();
    assert!(outcome.contains("refused"), "{outcome}");
    // The halt is intact after the attempt.
    let still = store.load("kill-drill").unwrap().unwrap();
    assert_eq!(still.status().as_str(), "halted");
    assert!(still.resumes().is_empty());
}

#[test]
fn the_check_fails_loudly_if_a_store_accepted_the_forged_resume() {
    /// A store with the guard removed: it accepts anything. The drill must report that as a broken guard.
    struct PermissiveStore(AccountState);
    impl StateStore for PermissiveStore {
        fn load(&self, _account_id: &str) -> Result<Option<AccountState>, StoreError> {
            Ok(Some(self.0.clone()))
        }
        fn save(
            &self,
            _expected_version: u64,
            new_state: &AccountState,
        ) -> Result<AccountState, StoreError> {
            Ok(new_state.clone())
        }
    }
    let (halted, _) = AccountState::new("kill-drill").halt(HaltReason::Manual, "drill", start());
    let err = refuse_forged_resume(&PermissiveStore(halted), "kill-drill").unwrap_err();
    assert!(err.contains("guard is broken"), "{err}");
}

#[test]
fn the_drill_module_exposes_the_shared_constants_the_owner_relies_on() {
    assert_eq!(kill_drill::ENV_KEY, "ALPACA_PAPER_API_KEY");
    assert_eq!(kill_drill::ENV_SECRET, "ALPACA_PAPER_API_SECRET");
    assert_eq!(kill_drill::DEFAULT_MAX_NOTIONAL, "10");
    assert_eq!(kill_drill::DRILL_QTY, "0.01");
}
