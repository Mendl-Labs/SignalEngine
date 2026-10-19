//! `REBALANCER_SERVE_PROPOSAL_PLANS` (certified-envelopes Phase 0): the startup flag, the optional pilot pair, the
//! generalised `PilotAccountSource`, the paper-credential seam and the per-account runtime builder. Every test is
//! offline (`FakeTransport`), and the paper-only startup refusals are re-asserted WITH the flag on.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use broker_adapters::alpaca::config::PAPER_BASE_URL;
use broker_adapters::alpaca::PaperOnlyAlpaca;
use broker_adapters::testing::FakeTransport;
use chrono::{DateTime, Utc};
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_run::data::{SleeveKind, SleeveSpec};
use rebalancer_run::driver::{AccountSource, ActiveAccount, InMemoryAccountSource};
use rebalancer_run::record::ExecutionMode;
use rebalancer_service::credentials::{
    CredentialError, EnvPilotCredentials, PaperAlpacaKeys, PaperCredentialProvider, ServiceCredentials, TenantCredentialsNotBuilt, ENV_ALPACA_KEY_ID, ENV_ALPACA_KEY_SECRET, NOT_THE_PILOT_PAIR,
    TENANT_CREDENTIALS_NOT_BUILT,
};
use rebalancer_service::pilot::{PilotAccountSource, PilotConfig, PilotPair, PilotRefusal, ENV_ACCOUNT, ENV_PAPER_ONLY, ENV_SERVE_PROPOSAL_PLANS, ENV_TENANT};
use rebalancer_service::runtime::{connect_account_alpaca, connect_account_alpaca_for_sleeves, symbols_for_kind, symbols_for_sleeves, RuntimeError, PILOT_CRYPTO_SYMBOLS, PILOT_ETF_SYMBOLS};
use serde_json::{json, Value};

const TENANT: &str = "3f2a9c1e-5b7d-4e8a-9c0d-1a2b3c4d5e6f";
const ACCOUNT: &str = "8d7c6b5a-4f3e-4d2c-8b1a-0f9e8d7c6b5a";
const OTHER_TENANT: &str = "11111111-2222-4333-8444-555555555555";
const OTHER_ACCOUNT: &str = "99999999-8888-4777-8666-555555555555";
const THIRD_ACCOUNT: &str = "77777777-8888-4777-8666-555555555555";

fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let map: BTreeMap<String, String> = pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect();
    move |k| map.get(k).cloned()
}

fn pilot_env() -> Vec<(&'static str, &'static str)> {
    vec![(ENV_PAPER_ONLY, "true"), (ENV_TENANT, TENANT), (ENV_ACCOUNT, ACCOUNT)]
}

fn with_flag(mut pairs: Vec<(&'static str, &'static str)>, value: &'static str) -> Vec<(&'static str, &'static str)> {
    pairs.push((ENV_SERVE_PROPOSAL_PLANS, value));
    pairs
}

// ---------------------------------------------------------------------------------------------------------------
// Startup: the flag
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_flag_is_off_by_default_and_off_keeps_the_pilot_pair_required() {
    let cfg = PilotConfig::from_lookup(env(&pilot_env())).unwrap();
    assert!(!cfg.serve_proposal_plans);
    assert_eq!(cfg.pilot, Some(PilotPair { tenant_id: TENANT.into(), account_id: ACCOUNT.into() }));
    let cfg = PilotConfig::from_lookup(env(&with_flag(pilot_env(), "false"))).unwrap();
    assert!(!cfg.serve_proposal_plans);
    // Off (unset or false): no ids is still PILOT_ID_MISSING, exactly as before.
    for flag in [None, Some("false")] {
        let mut e = vec![(ENV_PAPER_ONLY, "true")];
        if let Some(v) = flag {
            e.push((ENV_SERVE_PROPOSAL_PLANS, v));
        }
        let r = PilotConfig::from_lookup(env(&e)).unwrap_err();
        assert_eq!(r.code(), "PILOT_ID_MISSING", "{flag:?}");
    }
}

#[test]
fn the_flag_accepts_exactly_true_or_false() {
    for bad in ["TRUE", "True", "1", "yes", "", "on", "true "] {
        // "true " with a trailing space is trimmed and accepted; everything else is refused.
        let r = PilotConfig::from_lookup(env(&with_flag(pilot_env(), bad)));
        if bad.trim() == "true" {
            assert!(r.unwrap().serve_proposal_plans);
        } else {
            let r = r.unwrap_err();
            assert_eq!(r.code(), "PILOT_SERVE_PROPOSAL_PLANS_BAD_VALUE", "{bad:?}");
            assert!(matches!(r, PilotRefusal::ServeProposalPlansBadValue(_)));
            assert!(r.to_string().starts_with(r.code()));
        }
    }
}

#[test]
fn with_the_flag_on_the_pilot_pair_is_optional_but_never_half_configured() {
    let cfg = PilotConfig::from_lookup(env(&[(ENV_PAPER_ONLY, "true"), (ENV_SERVE_PROPOSAL_PLANS, "true")])).unwrap();
    assert!(cfg.serve_proposal_plans);
    assert_eq!(cfg.pilot, None, "no pair: proposal plans only");
    let cfg = PilotConfig::from_lookup(env(&with_flag(pilot_env(), "true"))).unwrap();
    assert_eq!(cfg.pilot, Some(PilotPair { tenant_id: TENANT.into(), account_id: ACCOUNT.into() }));
    for only in [ENV_TENANT, ENV_ACCOUNT] {
        let value = if only == ENV_TENANT { TENANT } else { ACCOUNT };
        let r = PilotConfig::from_lookup(env(&[(ENV_PAPER_ONLY, "true"), (ENV_SERVE_PROPOSAL_PLANS, "true"), (only, value)])).unwrap_err();
        assert_eq!(r.code(), "PILOT_ID_MISSING", "only {only} set");
    }
    let r = PilotConfig::from_lookup(env(&[(ENV_PAPER_ONLY, "true"), (ENV_SERVE_PROPOSAL_PLANS, "true"), (ENV_TENANT, "nope"), (ENV_ACCOUNT, ACCOUNT)])).unwrap_err();
    assert_eq!(r.code(), "PILOT_ID_INVALID");
    assert!(cfg.describe().contains("serving approved paper strategy_plans"));
    assert!(PilotConfig::from_lookup(env(&pilot_env())).unwrap().describe().contains("pilot only"));
}

#[test]
fn the_paper_only_refusals_hold_with_the_flag_on() {
    // The master key present: refused before the flag is even read.
    let r = PilotConfig::from_lookup(env(&[(ENV_PAPER_ONLY, "true"), (ENV_SERVE_PROPOSAL_PLANS, "true"), ("CREDENTIALS_ENCRYPTION_KEY", "x")])).unwrap_err();
    assert_eq!(r.code(), "PILOT_FORBIDDEN_ENV_PRESENT");
    // REBALANCER_PAPER_ONLY still must be exactly true.
    let r = PilotConfig::from_lookup(env(&[(ENV_SERVE_PROPOSAL_PLANS, "true")])).unwrap_err();
    assert_eq!(r.code(), "PILOT_PAPER_ONLY_NOT_SET");
    let r = PilotConfig::from_lookup(env(&[(ENV_PAPER_ONLY, "false"), (ENV_SERVE_PROPOSAL_PLANS, "true")])).unwrap_err();
    assert_eq!(r.code(), "PILOT_PAPER_ONLY_BAD_VALUE");
}

// ---------------------------------------------------------------------------------------------------------------
// Layer 2 with the flag on: the pilot pair plus every other enumerated account, all checked
// ---------------------------------------------------------------------------------------------------------------

fn mandate_body(venues: &[&str]) -> mandate_core::mandate::MandateBody {
    let mut v: Value = serde_json::from_str(include_str!("../../mandate-core/tests/fixtures/baseline_mandate.json")).unwrap();
    v["universe"]["venues"] = json!(venues);
    serde_json::from_value(v).unwrap()
}

fn sleeve(venue: &str, asset_class: &str) -> SleeveSpec {
    SleeveSpec { id: "etf_trend_faber".into(), kind: SleeveKind::EtfTrend, share: rebalancer_run::Dec::parse("1").unwrap(), venue: venue.into(), asset_class: asset_class.into(), quote: "".into(), instruments: Vec::new(), tranche_sessions: 0, lookback: None }
}

fn account(tenant: &str, account: &str, mode: ExecutionMode) -> ActiveAccount {
    let t = |s: &str| s.parse::<DateTime<Utc>>().unwrap();
    ActiveAccount {
        account_id: account.into(),
        tenant_id: tenant.into(),
        mandate: mandate_body(&["alpaca"]),
        envelope: MandateEnvelope { version: 1, status: MandateStatus::Active, effective_from: t("2020-01-01T00:00:00Z"), review_by: t("2099-01-01T00:00:00Z") },
        plan_approved: true,
        sleeves: vec![sleeve("alpaca", "us_etf")],
        mode,
    }
}

fn source(accounts: Vec<ActiveAccount>, cfg: PilotConfig) -> PilotAccountSource<InMemoryAccountSource> {
    let inner = InMemoryAccountSource::new();
    inner.set_accounts(accounts);
    PilotAccountSource::new(inner, cfg)
}

fn flag_on_with_pilot() -> PilotConfig {
    PilotConfig::from_lookup(env(&with_flag(pilot_env(), "true"))).unwrap()
}

fn flag_on_no_pilot() -> PilotConfig {
    PilotConfig::from_lookup(env(&[(ENV_PAPER_ONLY, "true"), (ENV_SERVE_PROPOSAL_PLANS, "true")])).unwrap()
}

fn err_code(s: &PilotAccountSource<InMemoryAccountSource>) -> String {
    let e = s.active_accounts().expect_err("must be refused");
    e.split(':').next().unwrap().to_string()
}

#[test]
fn with_the_flag_on_other_accounts_of_any_tenant_are_served_alongside_the_pilot_pair() {
    let s = source(
        vec![account(OTHER_TENANT, OTHER_ACCOUNT, ExecutionMode::Live), account(TENANT, THIRD_ACCOUNT, ExecutionMode::Assisted), account(TENANT, ACCOUNT, ExecutionMode::Live)],
        flag_on_with_pilot(),
    );
    let served = s.active_accounts().unwrap();
    let ids: BTreeSet<&str> = served.iter().map(|a| a.account_id.as_str()).collect();
    assert_eq!(ids, BTreeSet::from([OTHER_ACCOUNT, THIRD_ACCOUNT, ACCOUNT]));
    assert_eq!(served[0].account_id, ACCOUNT, "the pilot pair is listed first");
    // No pilot pair at all: every enumerated account is served.
    let s = source(vec![account(OTHER_TENANT, OTHER_ACCOUNT, ExecutionMode::Live), account(TENANT, ACCOUNT, ExecutionMode::Live)], flag_on_no_pilot());
    assert_eq!(s.active_accounts().unwrap().len(), 2);
}

#[test]
fn with_the_flag_off_other_accounts_stay_invisible() {
    let s = source(vec![account(OTHER_TENANT, OTHER_ACCOUNT, ExecutionMode::Live), account(TENANT, ACCOUNT, ExecutionMode::Live)], PilotConfig::from_lookup(env(&pilot_env())).unwrap());
    let served = s.active_accounts().unwrap();
    assert_eq!(served.len(), 1);
    assert_eq!(served[0].account_id, ACCOUNT);
}

#[test]
fn every_served_account_gets_the_same_venue_mode_and_asset_class_checks_and_one_bad_account_refuses_the_tick() {
    // A proposal account with a sleeve on Kraken: the whole enumeration is refused, the pilot does not run either.
    let mut bad = account(OTHER_TENANT, OTHER_ACCOUNT, ExecutionMode::Live);
    bad.sleeves = vec![sleeve("kraken", "crypto_spot")];
    let s = source(vec![account(TENANT, ACCOUNT, ExecutionMode::Live), bad], flag_on_with_pilot());
    assert_eq!(err_code(&s), "PILOT_VENUE_NOT_ALLOWED");
    // The validate-only Paper mode is not a served mode for a proposal account either.
    let s = source(vec![account(OTHER_TENANT, OTHER_ACCOUNT, ExecutionMode::Paper)], flag_on_no_pilot());
    assert_eq!(err_code(&s), "PILOT_MODE_NOT_ALLOWED");
    let mut bad = account(OTHER_TENANT, OTHER_ACCOUNT, ExecutionMode::Live);
    bad.sleeves = vec![sleeve("alpaca", "fx_spot")];
    assert_eq!(err_code(&source(vec![bad], flag_on_no_pilot())), "PILOT_ASSET_CLASS_NOT_ALLOWED");
    let mut ok = account(OTHER_TENANT, OTHER_ACCOUNT, ExecutionMode::Live);
    ok.mandate = mandate_body(&["alpaca", "oanda", "kraken"]);
    assert!(source(vec![ok], flag_on_no_pilot()).active_accounts().is_ok(), "mandate-only venues do not block an alpaca-only plan");
    let mut bad = account(OTHER_TENANT, OTHER_ACCOUNT, ExecutionMode::Live);
    bad.sleeves = vec![];
    assert_eq!(err_code(&source(vec![bad], flag_on_no_pilot())), "PILOT_NO_SLEEVES");
}

#[test]
fn the_pilot_pair_rules_still_hold_with_the_flag_on() {
    // The pilot account id under another tenant: still the whole enumeration fails.
    let s = source(vec![account(OTHER_TENANT, ACCOUNT, ExecutionMode::Assisted), account(TENANT, THIRD_ACCOUNT, ExecutionMode::Assisted)], flag_on_with_pilot());
    assert_eq!(err_code(&s), "PILOT_ACCOUNT_UNDER_OTHER_TENANT");
    // Two pilot matches: ambiguous.
    let s = source(vec![account(TENANT, ACCOUNT, ExecutionMode::Assisted), account(TENANT, &ACCOUNT.to_uppercase(), ExecutionMode::Assisted)], flag_on_with_pilot());
    assert_eq!(err_code(&s), "PILOT_ACCOUNT_AMBIGUOUS");
    // No pilot account in the inner source with the flag on: not an error, the others run.
    let s = source(vec![account(OTHER_TENANT, OTHER_ACCOUNT, ExecutionMode::Live)], flag_on_with_pilot());
    assert_eq!(s.active_accounts().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------------------------------------------
// The credential seam
// ---------------------------------------------------------------------------------------------------------------

const PAPER_KEY: &str = "PKTESTFIXTUREKEY0001";
const SECRET: &str = "unit-test-secret-not-a-real-key-9f3a";

fn pair() -> PilotPair {
    PilotPair { tenant_id: TENANT.into(), account_id: ACCOUNT.into() }
}

fn key_env() -> impl Fn(&str) -> Option<String> {
    env(&[(ENV_ALPACA_KEY_ID, PAPER_KEY), (ENV_ALPACA_KEY_SECRET, SECRET)])
}

#[test]
fn the_env_pilot_provider_serves_exactly_the_pilot_pair_and_requires_both_variables() {
    let p = EnvPilotCredentials::from_lookup(pair(), key_env()).unwrap();
    let keys = p.paper_alpaca_keys(&TENANT.to_uppercase(), ACCOUNT).unwrap();
    assert_eq!(keys, PaperAlpacaKeys { key_id: PAPER_KEY.into(), secret: SECRET.into() });
    let err = p.paper_alpaca_keys(OTHER_TENANT, ACCOUNT).unwrap_err();
    assert_eq!(err, CredentialError::NotProvided { tenant_id: OTHER_TENANT.into(), account_id: ACCOUNT.into(), reason: NOT_THE_PILOT_PAIR });
    assert!(p.paper_alpaca_keys(TENANT, OTHER_ACCOUNT).is_err());
    for missing in [ENV_ALPACA_KEY_ID, ENV_ALPACA_KEY_SECRET] {
        let lookup = env(&[(ENV_ALPACA_KEY_ID, PAPER_KEY), (ENV_ALPACA_KEY_SECRET, SECRET), (missing, "  ")]);
        assert_eq!(EnvPilotCredentials::from_lookup(pair(), lookup).unwrap_err(), CredentialError::Missing(missing));
    }
}

#[test]
fn the_service_provider_routes_the_pilot_pair_to_the_environment_and_everything_else_to_the_tenant_provider_which_is_not_built() {
    let svc = ServiceCredentials::new(Some(EnvPilotCredentials::from_lookup(pair(), key_env()).unwrap()), Box::new(TenantCredentialsNotBuilt));
    assert_eq!(svc.paper_alpaca_keys(TENANT, ACCOUNT).unwrap().key_id, PAPER_KEY);
    let err = svc.paper_alpaca_keys(OTHER_TENANT, OTHER_ACCOUNT).unwrap_err();
    assert_eq!(err, CredentialError::NotProvided { tenant_id: OTHER_TENANT.into(), account_id: OTHER_ACCOUNT.into(), reason: TENANT_CREDENTIALS_NOT_BUILT });
    assert!(err.to_string().starts_with("CREDENTIALS_NOT_PROVIDED") && err.to_string().contains(TENANT_CREDENTIALS_NOT_BUILT));
    // Without a pilot pair, nothing is served at all.
    let svc = ServiceCredentials::new(None, Box::new(TenantCredentialsNotBuilt));
    assert!(matches!(svc.paper_alpaca_keys(TENANT, ACCOUNT).unwrap_err(), CredentialError::NotProvided { reason: TENANT_CREDENTIALS_NOT_BUILT, .. }));
}

#[test]
fn keys_never_appear_in_debug_output() {
    let keys = PaperAlpacaKeys { key_id: PAPER_KEY.into(), secret: SECRET.into() };
    let dbg = format!("{keys:?}");
    assert!(!dbg.contains(PAPER_KEY) && !dbg.contains(SECRET), "{dbg}");
    assert!(dbg.contains("redacted"));
    // The providers holding the keys redact too (manual Debug, never derived).
    let env_provider = EnvPilotCredentials::from_lookup(pair(), key_env()).unwrap();
    let dbg = format!("{env_provider:?}");
    assert!(dbg.contains(TENANT) && !dbg.contains(PAPER_KEY) && !dbg.contains(SECRET), "{dbg}");
    let svc = ServiceCredentials::new(Some(env_provider), Box::new(TenantCredentialsNotBuilt));
    let dbg = format!("{svc:?}");
    assert!(!dbg.contains(PAPER_KEY) && !dbg.contains(SECRET), "{dbg}");
}

// ---------------------------------------------------------------------------------------------------------------
// The per-account runtime builder: no fingerprint for a proposal account, PA check kept, assets per sleeve kind
// ---------------------------------------------------------------------------------------------------------------

fn account_json(account_number: &str) -> String {
    format!(
        r#"{{"id":"904837e3-3b76-47ec-b432-046db621571b","account_number":"{account_number}","status":"ACTIVE","currency":"USD",
        "cash":"5000.00","portfolio_value":"5000.00","equity":"5000.00","last_equity":"5000.00","buying_power":"5000.00",
        "trading_blocked":false,"transfers_blocked":false,"account_blocked":false,"pattern_day_trader":false,"shorting_enabled":false}}"#
    )
}

fn asset_json(symbol: &str, class: &str) -> String {
    format!(
        r#"{{"id":"b28f4066-5c6d-479b-a2af-85dc1a8f16fb","class":"{class}","exchange":"ARCA","symbol":"{symbol}",
        "name":"{symbol} test asset","status":"active","tradable":true,"marginable":false,"shortable":false,
        "easy_to_borrow":false,"fractionable":true}}"#
    )
}

fn real_fingerprint() -> String {
    PaperOnlyAlpaca::new(PAPER_KEY, SECRET, PAPER_BASE_URL, Arc::new(FakeTransport::new()), Some("rb1:"), true).unwrap().key_id_fingerprint()
}

#[test]
fn a_proposal_account_connects_without_a_pinned_fingerprint_but_still_needs_a_pa_account_number() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &account_json("XX3TESTFIXT1"));
    let err = connect_account_alpaca(PAPER_KEY, SECRET, PAPER_BASE_URL, t, None, &BTreeSet::from([SleeveKind::EtfTrend])).unwrap_err();
    assert!(matches!(err, RuntimeError::Verify(_)), "{err}");

    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &account_json("PA3TESTFIXT1"));
    for sym in PILOT_ETF_SYMBOLS {
        t.enqueue_json(200, &asset_json(sym, "us_equity"));
    }
    let ok = connect_account_alpaca(PAPER_KEY, SECRET, PAPER_BASE_URL, t.clone(), None, &BTreeSet::from([SleeveKind::EtfTrend])).unwrap();
    assert_eq!(t.request_count(), 1 + PILOT_ETF_SYMBOLS.len());
    assert_eq!(ok.key_id_fingerprint(), real_fingerprint());
}

#[test]
fn a_pinned_fingerprint_is_still_enforced_before_any_request_when_present() {
    let t = Arc::new(FakeTransport::new());
    let err = connect_account_alpaca(PAPER_KEY, SECRET, PAPER_BASE_URL, t.clone(), Some("0000000000000000"), &BTreeSet::from([SleeveKind::EtfTrend])).unwrap_err();
    assert!(matches!(err, RuntimeError::FingerprintMismatch { .. }), "{err}");
    assert_eq!(t.request_count(), 0);
}

#[test]
fn the_asset_refresh_covers_exactly_the_accounts_sleeve_kinds() {
    assert_eq!(symbols_for_kind(SleeveKind::EtfTrend), &PILOT_ETF_SYMBOLS);
    assert_eq!(symbols_for_kind(SleeveKind::CryptoTrend), &PILOT_CRYPTO_SYMBOLS);
    // A crypto-only plan refreshes the two pairs and no ETF.
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &account_json("PA3TESTFIXT1"));
    for sym in PILOT_CRYPTO_SYMBOLS {
        t.enqueue_json(200, &asset_json(sym, "crypto"));
    }
    let ok = connect_account_alpaca(PAPER_KEY, SECRET, PAPER_BASE_URL, t.clone(), None, &BTreeSet::from([SleeveKind::CryptoTrend])).unwrap();
    assert_eq!(t.request_count(), 1 + PILOT_CRYPTO_SYMBOLS.len());
    assert!(ok.assets().lookup("BTC/USD").is_some() && ok.assets().lookup("SPY").is_none());
    // Both kinds: ETFs then crypto (BTreeSet order), seven refreshes.
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &account_json("PA3TESTFIXT1"));
    for sym in PILOT_ETF_SYMBOLS {
        t.enqueue_json(200, &asset_json(sym, "us_equity"));
    }
    for sym in PILOT_CRYPTO_SYMBOLS {
        t.enqueue_json(200, &asset_json(sym, "crypto"));
    }
    let ok = connect_account_alpaca(PAPER_KEY, SECRET, PAPER_BASE_URL, t.clone(), None, &BTreeSet::from([SleeveKind::EtfTrend, SleeveKind::CryptoTrend])).unwrap();
    assert_eq!(t.request_count(), 1 + PILOT_ETF_SYMBOLS.len() + PILOT_CRYPTO_SYMBOLS.len());
    assert!(ok.assets().lookup("VNQ").is_some() && ok.assets().lookup("ETH/USD").is_some());
}

// ---------------------------------------------------------------------------------------------------------------
// Certified-envelopes Phase 4: the asset refresh covers each account's OWN instruments (its proxies)
// ---------------------------------------------------------------------------------------------------------------

fn proxy_etf_sleeve() -> SleeveSpec {
    SleeveSpec { instruments: ["IVV", "EFA", "IEF", "DBC", "VNQ"].iter().map(|s| s.to_string()).collect(), tranche_sessions: 2, lookback: Some(9), ..sleeve("alpaca", "us_etf") }
}

#[test]
fn the_symbols_of_published_sleeves_are_exactly_the_per_kind_symbols_in_the_same_order() {
    let etf = sleeve("alpaca", "us_etf");
    let crypto = SleeveSpec { id: "crypto_trend_100d".into(), kind: SleeveKind::CryptoTrend, asset_class: "crypto_spot".into(), quote: "USD".into(), ..sleeve("alpaca", "us_etf") };
    let want: Vec<String> = PILOT_ETF_SYMBOLS.iter().chain(PILOT_CRYPTO_SYMBOLS.iter()).map(|s| s.to_string()).collect();
    assert_eq!(symbols_for_sleeves(&[etf.clone(), crypto.clone()]), want);
    assert_eq!(symbols_for_sleeves(&[crypto, etf]), want, "kind order, whatever the sleeve order");
}

#[test]
fn a_proxy_account_refreshes_its_proxy_into_the_asset_table_and_not_the_published_symbol() {
    let sleeves = [proxy_etf_sleeve()];
    assert_eq!(symbols_for_sleeves(&sleeves), vec!["IVV", "EFA", "IEF", "DBC", "VNQ"]);
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &account_json("PA3TESTFIXT1"));
    for sym in ["IVV", "EFA", "IEF", "DBC", "VNQ"] {
        t.enqueue_json(200, &asset_json(sym, "us_equity"));
    }
    let ok = connect_account_alpaca_for_sleeves(PAPER_KEY, SECRET, PAPER_BASE_URL, t.clone(), None, &sleeves).unwrap();
    assert_eq!(t.request_count(), 6);
    assert!(ok.assets().lookup("IVV").is_some(), "the proxy is verified into the table");
    assert!(ok.assets().lookup("SPY").is_none(), "the published symbol this account does not trade is not");
}

#[test]
fn an_account_whose_proxy_cannot_be_refreshed_gets_no_runtime() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &account_json("PA3TESTFIXT1"));
    t.enqueue_json(404, r#"{"message":"asset not found"}"#); // IVV is unknown to the venue
    let err = connect_account_alpaca_for_sleeves(PAPER_KEY, SECRET, PAPER_BASE_URL, t, None, &[proxy_etf_sleeve()]).unwrap_err();
    assert!(matches!(&err, RuntimeError::AssetRefresh { symbol, .. } if symbol == "IVV"), "{err}");
}

// ---------------------------------------------------------------------------------------------------------------
// R41 cells: the asset refresh covers the cell's whole universe (9 and 11 names), and refuses an unknown one
// ---------------------------------------------------------------------------------------------------------------

const CELL_SEC9: [&str; 9] = ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"];
const CELL_AC11: [&str; 11] = ["SPY", "EFA", "IEF", "DBC", "VNQ", "IWM", "EEM", "TLT", "LQD", "HYG", "GLD"];

fn cell_etf_sleeve(tickers: &[&str]) -> SleeveSpec {
    SleeveSpec { instruments: tickers.iter().map(|s| s.to_string()).collect(), ..sleeve("alpaca", "us_etf") }
}

#[test]
fn a_cell_account_refreshes_every_instrument_of_its_universe_into_the_verified_asset_table() {
    for tickers in [&CELL_SEC9[..], &CELL_AC11[..]] {
        let sleeves = [cell_etf_sleeve(tickers)];
        assert_eq!(symbols_for_sleeves(&sleeves), tickers.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let t = Arc::new(FakeTransport::new());
        t.enqueue_json(200, &account_json("PA3TESTFIXT1"));
        for sym in tickers {
            t.enqueue_json(200, &asset_json(sym, "us_equity"));
        }
        let ok = connect_account_alpaca_for_sleeves(PAPER_KEY, SECRET, PAPER_BASE_URL, t.clone(), None, &sleeves).unwrap();
        assert_eq!(t.request_count(), 1 + tickers.len(), "one account read and one refresh per instrument");
        for sym in tickers {
            assert!(ok.assets().lookup(sym).is_some(), "{sym} is verified into the table");
        }
    }
}

#[test]
fn a_cell_account_with_an_instrument_the_venue_does_not_know_gets_no_runtime() {
    let t = Arc::new(FakeTransport::new());
    t.enqueue_json(200, &account_json("PA3TESTFIXT1"));
    for sym in &CELL_SEC9[..4] {
        t.enqueue_json(200, &asset_json(sym, "us_equity"));
    }
    t.enqueue_json(404, r#"{"message":"asset not found"}"#); // XLK is unknown to the venue
    let err = connect_account_alpaca_for_sleeves(PAPER_KEY, SECRET, PAPER_BASE_URL, t, None, &[cell_etf_sleeve(&CELL_SEC9)]).unwrap_err();
    assert!(matches!(&err, RuntimeError::AssetRefresh { symbol, .. } if symbol == "XLK"), "{err}");
}
