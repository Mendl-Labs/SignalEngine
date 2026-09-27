//! Slice S-6 (`PAPER_PILOT_DRAGONSTONE_PLAN.md`): the real `AccountRuntime` construction path
//! (`rebalancer_service::runtime`). Every test here is offline: `FakeTransport` scripts every HTTP response, no
//! `MASSIVE_API_KEY` is read from a real secrets file (`StaticKeyProvider` supplies a fixed, fake key), and no test
//! ever reaches a real network. See `tests/pilot_source_scan.rs` for the source-level proof that the crate cannot
//! name a live venue at all.

use std::collections::BTreeSet;
use std::sync::Arc;

use broker_adapters::alpaca::config::PAPER_BASE_URL;
use broker_adapters::alpaca::PaperOnlyAlpaca;
use broker_adapters::testing::FakeTransport;
use chrono::{TimeZone, Utc};
use mandate_core::mandate::MandateBody;
use market_data::{MassiveDataSource, StaticKeyProvider};
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_core::venue::VenueRuleBook;
use rebalancer_core::Dec;
use rebalancer_run::clock::ManualClock;
use rebalancer_run::data::{DataSource, SleeveKind, SleeveSpec};
use rebalancer_run::driver::{find_due_runs, run_all_due, AccountRuntime, ActiveAccount, InMemoryAccountSource};
use rebalancer_run::pipeline::VenuePolicy;
use rebalancer_run::record::{ExecutionMode, OutcomeKind};
use rebalancer_run::stores::InMemoryRunStore;
use rebalancer_run::testkit::{FixtureData, InMemoryAccountLock, RecordingNotifier, SwitchKillFlag};
use rebalancer_risk::store::InMemoryStateStore;
use rebalancer_service::pilot::{pilot_run_config, PilotConfig};
use rebalancer_service::runtime::{connect_paper_alpaca, connect_read_only, pilot_data_source, RuntimeError, PILOT_ETF_SYMBOLS};
use std::collections::BTreeMap;

const PAPER_KEY: &str = "PKTESTFIXTUREKEY0001";
const SECRET: &str = "unit-test-secret-not-a-real-key-9f3a";

fn account_json(account_number: &str) -> String {
    format!(
        r#"{{"id":"904837e3-3b76-47ec-b432-046db621571b","account_number":"{account_number}","status":"ACTIVE","currency":"USD",
        "cash":"5000.00","portfolio_value":"5000.00","equity":"5000.00","last_equity":"5000.00","buying_power":"5000.00",
        "trading_blocked":false,"transfers_blocked":false,"account_blocked":false,"pattern_day_trader":false,"shorting_enabled":false}}"#
    )
}

fn asset_json(symbol: &str) -> String {
    format!(
        r#"{{"id":"b28f4066-5c6d-479b-a2af-85dc1a8f16fb","class":"us_equity","exchange":"ARCA","symbol":"{symbol}",
        "name":"{symbol} test asset","status":"active","tradable":true,"marginable":true,"shortable":true,
        "easy_to_borrow":true,"fractionable":true}}"#
    )
}

/// Real fingerprint of [`PAPER_KEY`] (computed the same way `connect_paper_alpaca` does), without any network call.
fn real_fingerprint() -> String {
    let t = Arc::new(FakeTransport::new());
    PaperOnlyAlpaca::new(PAPER_KEY, SECRET, PAPER_BASE_URL, t, Some("rb1:"), true).unwrap().key_id_fingerprint()
}

/// Scripts `GET /v2/account` (with `account_number`) then five `GET /v2/assets/{symbol}` (all tradable and
/// fractionable), in that order -- exactly the request sequence `connect_paper_alpaca` makes.
fn script_happy_path(t: &FakeTransport, account_number: &str) {
    t.enqueue_json(200, &account_json(account_number));
    for sym in PILOT_ETF_SYMBOLS {
        t.enqueue_json(200, &asset_json(sym));
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Mutant 1/4: the fingerprint check -- dropped, or checked in the wrong order relative to the network call.
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_fingerprint_mismatch_is_refused_before_any_network_request() {
    let t = Arc::new(FakeTransport::new());
    // No response scripted at all: if the implementation reached the network before checking the fingerprint,
    // this call would panic/fail on "no scripted response left" instead of returning FingerprintMismatch.
    let err = connect_paper_alpaca(PAPER_KEY, SECRET, PAPER_BASE_URL, t.clone(), "0000000000000000").unwrap_err();
    assert!(matches!(err, RuntimeError::FingerprintMismatch { .. }), "{err}");
    assert_eq!(t.request_count(), 0, "the fingerprint is computed locally; the network must never be touched first");
}

#[test]
fn a_matching_fingerprint_is_accepted_case_insensitively() {
    let fp = real_fingerprint();
    let t = Arc::new(FakeTransport::new());
    script_happy_path(&t, "PA3TESTFIXT1");
    let ok = connect_paper_alpaca(PAPER_KEY, SECRET, PAPER_BASE_URL, t, &fp.to_ascii_uppercase());
    assert!(ok.is_ok(), "{:?}", ok.err());
}

// ---------------------------------------------------------------------------------------------------------------
// Mutant 2: the PA-prefix account-number check -- dropped (calling verify_account() instead of
// verify_paper_account()).
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_paper_account_number_without_the_pa_prefix_is_refused() {
    let fp = real_fingerprint();
    let t = Arc::new(FakeTransport::new());
    script_happy_path(&t, "XX3TESTFIXT1"); // NOT a PA-prefixed account number
    let err = connect_paper_alpaca(PAPER_KEY, SECRET, PAPER_BASE_URL, t, &fp).unwrap_err();
    assert!(matches!(err, RuntimeError::Verify(_)), "{err}");
    assert!(err.to_string().contains("PA") || err.to_string().to_lowercase().contains("paper"), "{err}");
}

#[test]
fn a_paper_account_number_with_the_pa_prefix_is_accepted() {
    let fp = real_fingerprint();
    let t = Arc::new(FakeTransport::new());
    script_happy_path(&t, "PA3TESTFIXT1");
    assert!(connect_paper_alpaca(PAPER_KEY, SECRET, PAPER_BASE_URL, t, &fp).is_ok());
}

// ---------------------------------------------------------------------------------------------------------------
// Mutant 5/6/7: the asset table -- refresh skipped entirely, or fewer than five symbols refreshed.
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn all_five_pilot_etfs_are_refreshed_from_the_api_not_left_as_builtin_rows() {
    let fp = real_fingerprint();
    let t = Arc::new(FakeTransport::new());
    script_happy_path(&t, "PA3TESTFIXT1");
    let pilot = connect_paper_alpaca(PAPER_KEY, SECRET, PAPER_BASE_URL, t.clone(), &fp).unwrap();
    // one GET /v2/account + five GET /v2/assets/{symbol}
    assert_eq!(t.request_count(), 1 + PILOT_ETF_SYMBOLS.len());
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for sym in PILOT_ETF_SYMBOLS {
        let info = pilot.assets().lookup(sym).unwrap_or_else(|| panic!("{sym} missing from the refreshed asset table"));
        assert_eq!(info.source, broker_adapters::alpaca::AssetSource::Api, "{sym}: must be the refreshed API row, not the builtin fallback");
        assert!(info.tradable && info.fractionable, "{sym}");
        seen.insert(sym);
    }
    assert_eq!(seen.len(), 5);
}

#[test]
fn an_asset_refresh_failure_refuses_the_whole_connection() {
    let fp = real_fingerprint();
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &account_json("PA3TESTFIXT1"));
    t.enqueue_json(200, &asset_json("SPY"));
    t.enqueue_json(404, r#"{"message":"asset not found"}"#); // EFA refresh fails
    let err = connect_paper_alpaca(PAPER_KEY, SECRET, PAPER_BASE_URL, t, &fp).unwrap_err();
    assert!(matches!(err, RuntimeError::AssetRefresh { symbol: "EFA", .. }), "{err}");
}

// ---------------------------------------------------------------------------------------------------------------
// The plan's five ETF symbols, pinned.
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_pilot_etf_symbol_list_is_exactly_the_five_the_plan_names() {
    let mut got: Vec<&str> = PILOT_ETF_SYMBOLS.to_vec();
    got.sort_unstable();
    assert_eq!(got, ["DBC", "EFA", "IEF", "SPY", "VNQ"]);
}

// ---------------------------------------------------------------------------------------------------------------
// print-fingerprint: never bypasses the same PA / blocked-account checks, never prints the key.
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn print_fingerprint_also_refuses_a_non_pa_account_and_never_needs_a_plan_row() {
    let t = Arc::new(FakeTransport::new());
    script_happy_path(&t, "XX3TESTFIXT1");
    let err = connect_read_only(PAPER_KEY, SECRET, PAPER_BASE_URL, t).unwrap_err();
    assert!(matches!(err, RuntimeError::Verify(_)), "{err}");
}

#[test]
fn print_fingerprint_succeeds_read_only_with_no_expected_fingerprint_to_compare() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &account_json("PA3TESTFIXT1")); // no asset refresh at all for this subcommand
    let fp = connect_read_only(PAPER_KEY, SECRET, PAPER_BASE_URL, t.clone()).unwrap();
    assert_eq!(fp, real_fingerprint());
    assert_eq!(t.request_count(), 1, "print-fingerprint never refreshes assets: nothing is about to trade");
    assert!(!fp.contains(PAPER_KEY), "the fingerprint must never be (or contain) the key itself");
}

// ---------------------------------------------------------------------------------------------------------------
// Mutant 8/9: the DataSource -- sleeve panels and sizing prices swapped, or a price/panel returned without ever
// asking Massive (a hard-coded stand-in silently wired in instead of the real source).
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn pilot_data_source_sleeve_data_really_calls_massive_not_a_stand_in() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(403, r#"{"message":"forbidden"}"#); // deterministic, never retried (error.rs: 401/403 -> NotAuthorized)
    let keys = StaticKeyProvider::new("not-a-real-massive-key").unwrap();
    let massive = MassiveDataSource::new(keys, t.clone());
    let source = pilot_data_source(&massive);
    let sleeve = SleeveSpec { id: "etf".into(), kind: SleeveKind::EtfTrend, share: Dec::parse("1.0").unwrap(), venue: "alpaca".into(), asset_class: "us_etf".into(), quote: "".into() };
    let err = source.sleeve_data(&sleeve, Utc::now().date_naive()).unwrap_err();
    assert_eq!(t.request_count(), 1, "sleeve_data must reach the real transport exactly once (a stand-in would touch it zero times)");
    assert!(err.code != "DATA_UNSUPPORTED", "must be Massive's own error, not the price-only source's refusal: {err}");
}

#[test]
fn pilot_data_source_prices_really_calls_massive_not_a_stand_in() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(403, r#"{"message":"forbidden"}"#);
    let keys = StaticKeyProvider::new("not-a-real-massive-key").unwrap();
    let massive = MassiveDataSource::new(keys, t.clone());
    let source = pilot_data_source(&massive);
    let err = source.prices(&["SPY".to_string()], Utc::now()).unwrap_err();
    assert_eq!(t.request_count(), 1, "prices must reach the real transport exactly once (a stand-in would touch it zero times)");
    assert!(err.code != "DATA_UNSUPPORTED", "{err}");
}

// ---------------------------------------------------------------------------------------------------------------
// Mutant 10/11/12: pilot_run_config -- the paper-only policy, the price-age tolerance or the recon floor dropped.
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn pilot_run_config_is_paper_only_with_the_pilots_wider_price_age_and_recon_tolerances() {
    let cfg = pilot_run_config();
    assert_eq!(cfg.venue_policy, VenuePolicy::PaperOnly);
    assert_eq!(cfg.max_price_age_secs, 4 * 24 * 60 * 60, "a daily-close price source needs days, not the 300s live-quote default");
    assert_eq!(cfg.tolerances.value_abs, Dec::from_i64(15), "must tolerate a paper dividend, above the $1 platform default");
}

// ---------------------------------------------------------------------------------------------------------------
// The full construction path, driven by the REAL driver (find_due_runs / run_all_due), not just run_once called by
// hand: the broker really reports Paper and is accepted by the real VenuePolicy::PaperOnly check (not asserted
// directly), and the whole AccountRuntime the pilot builds is something the driver can call without panicking.
// Sleeve panels come from FixtureData (no live Massive call): the point of this test is the CONSTRUCTION path, not
// re-testing Massive's own parsing (covered by market-data's own suite).
// ---------------------------------------------------------------------------------------------------------------

fn baseline_mandate() -> MandateBody {
    serde_json::from_str(include_str!("../../mandate-core/tests/fixtures/baseline_mandate.json")).unwrap()
}

#[test]
fn the_constructed_runtime_reports_paper_passes_the_real_policy_check_and_run_once_never_panics() {
    let fp = real_fingerprint();
    let t = Arc::new(FakeTransport::new());
    script_happy_path(&t, "PA3TESTFIXT1");
    let pilot_alpaca = connect_paper_alpaca(PAPER_KEY, SECRET, PAPER_BASE_URL, t, &fp).unwrap();

    // The broker's own reported environment, asserted directly (sanity) AND, below, proved by running it through
    // the actual VenuePolicy::PaperOnly check inside the real pipeline -- never touching a live host either way.
    use rebalancer_run::broker::{Broker, VenueEnvironment};
    assert_eq!(pilot_alpaca.broker().environment(), VenueEnvironment::Paper);

    let rules = pilot_alpaca.rules();
    let venue_rules = VenueRuleBook::new().with("alpaca", &rules);
    let data = FixtureData::new(); // no panel for "etf": sleeve_data will error -- proves data IS consulted, not skipped
    let broker = pilot_alpaca.broker();

    let now = Utc.with_ymd_and_hms(2026, 9, 28, 15, 0, 0).unwrap(); // the ETF sleeve's market-hours run slot
    let sleeve = SleeveSpec { id: "etf".into(), kind: SleeveKind::EtfTrend, share: Dec::parse("1.0").unwrap(), venue: "alpaca".into(), asset_class: "us_etf".into(), quote: "".into() };
    let active = ActiveAccount {
        account_id: "pilot-acct".to_string(),
        tenant_id: "11111111-1111-1111-1111-111111111111".to_string(),
        mandate: baseline_mandate(),
        envelope: MandateEnvelope {
            version: 1,
            status: MandateStatus::Active,
            effective_from: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            review_by: Utc.with_ymd_and_hms(2027, 1, 1, 0, 0, 0).unwrap(),
        },
        plan_approved: true,
        sleeves: vec![sleeve],
        mode: ExecutionMode::Live,
    };
    let source = InMemoryAccountSource::new().with_account(active);

    let clock = ManualClock::new(now);
    let runs = InMemoryRunStore::new();
    let states = InMemoryStateStore::new();
    let notifier = RecordingNotifier::new();
    let kill = SwitchKillFlag::new();
    let lock = InMemoryAccountLock::new();
    let cfg = pilot_run_config();

    let due = find_due_runs(&source, now).expect("enumeration must not fail");
    assert_eq!(due.len(), 1, "the account is due at its ETF market-hours slot");

    let mut runtimes: BTreeMap<String, AccountRuntime<'_>> = BTreeMap::new();
    runtimes.insert("pilot-acct".to_string(), AccountRuntime { broker: &broker, data: &data, venue_rules: &venue_rules });

    // The real driver, not `pipeline::run_once` called directly: proves the constructed AccountRuntime is something
    // `run_all_due` can actually look up and call without panicking.
    let outcomes = run_all_due(due, &runtimes, &states, &runs, &notifier, &kill, &clock, &lock, &cfg);
    assert_eq!(outcomes.len(), 1);
    let record = outcomes.into_iter().next().unwrap().result.unwrap_or_else(|e| panic!("must not fail to be ATTEMPTED at all (that would mean NoRuntime/Panicked): {e}"));

    // It got PAST the paper-only policy step (the broker really reports Paper) ...
    assert!(record.step_names().contains(&"venue_policy"), "{:?}", record.step_names());
    assert_ne!(record.outcome.code, "RUN_VENUE_NOT_PAPER", "{:?}", record.outcome);
    // ... and reached the data layer (which then fails closed on the missing fixture panel -- proving the
    // constructed DataSource is really consulted, not bypassed).
    assert_eq!(record.outcome.kind, OutcomeKind::FailedClosed);
    assert_eq!(record.outcome.code, "RUN_DATA_ERROR", "{:?}", record.outcome);
}

// ---------------------------------------------------------------------------------------------------------------
// REBALANCER_DEMO must never be able to bypass the paper-only startup refusal. This calls the EXACT function
// `main()` calls before anything else runs (PilotConfig::from_lookup), with REBALANCER_DEMO=1 set alongside an
// otherwise-invalid paper-only configuration: a real behavioural check, not source inspection.
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn rebalancer_demo_cannot_bypass_the_paper_only_startup_refusal() {
    let cases: Vec<Vec<(&str, &str)>> = vec![
        vec![("REBALANCER_DEMO", "1")], // paper-only not even set
        vec![("REBALANCER_DEMO", "1"), ("REBALANCER_PAPER_ONLY", "false")],
        vec![("REBALANCER_DEMO", "1"), ("REBALANCER_PAPER_ONLY", "true")], // ids still missing
    ];
    for env in cases {
        let map: std::collections::BTreeMap<&str, &str> = env.into_iter().collect();
        let lookup = |k: &str| map.get(k).map(|v| v.to_string());
        // REBALANCER_DEMO=1 is present in every case; the gate must still refuse exactly as it would without it.
        // `.unwrap_err()` alone is the real behavioural proof: had REBALANCER_DEMO bypassed anything, this would
        // panic here with "called `Result::unwrap_err()` on an `Ok` value" instead of returning a refusal.
        let err = PilotConfig::from_lookup(lookup).unwrap_err();
        assert!(err.code().starts_with("PILOT_"), "{err}");
    }
    // And even with a FULLY valid paper-only configuration, REBALANCER_DEMO is simply never read by the gate: the
    // same ids succeed identically whether it is set or not (nothing branches on it).
    let base = [
        ("REBALANCER_PAPER_ONLY", "true"),
        ("PILOT_TENANT_ID", "11111111-1111-1111-1111-111111111111"),
        ("PILOT_ACCOUNT_ID", "22222222-2222-2222-2222-222222222222"),
    ];
    let without_demo = PilotConfig::from_lookup(|k| base.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string())).unwrap();
    let with_demo = PilotConfig::from_lookup(|k| {
        if k == "REBALANCER_DEMO" {
            return Some("1".to_string());
        }
        base.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string())
    })
    .unwrap();
    assert_eq!(without_demo, with_demo, "REBALANCER_DEMO must have zero effect on the startup gate's decision");
}
