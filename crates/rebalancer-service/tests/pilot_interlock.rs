//! The paper-pilot interlock of the service (slice S-8): startup refusals, the one-pair account filter, the paper-only
//! run config, the allow-list check and the paper-only Alpaca builder. Every refusal is exercised, each with its code.

use std::collections::BTreeMap;
use std::sync::Arc;

use broker_adapters::testing::FakeTransport;
use chrono::{DateTime, Utc};
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_run::data::{SleeveKind, SleeveSpec};
use rebalancer_run::driver::{AccountSource, ActiveAccount, InMemoryAccountSource};
use rebalancer_run::pipeline::VenuePolicy;
use rebalancer_run::record::ExecutionMode;
use rebalancer_service::pilot::{
    allow_venue_environment, build_paper_alpaca, pilot_run_config, PilotAccountSource, PilotConfig, PilotRefusal, ENV_ACCOUNT, ENV_PAPER_ONLY, ENV_TENANT,
};
use serde_json::{json, Value};

const TENANT: &str = "3f2a9c1e-5b7d-4e8a-9c0d-1a2b3c4d5e6f";
const ACCOUNT: &str = "8d7c6b5a-4f3e-4d2c-8b1a-0f9e8d7c6b5a";
const OTHER_TENANT: &str = "11111111-2222-4333-8444-555555555555";
const OTHER_ACCOUNT: &str = "99999999-8888-4777-8666-555555555555";

fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let map: BTreeMap<String, String> = pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect();
    move |k| map.get(k).cloned()
}

fn good_env() -> Vec<(&'static str, &'static str)> {
    vec![(ENV_PAPER_ONLY, "true"), (ENV_TENANT, TENANT), (ENV_ACCOUNT, ACCOUNT)]
}

fn refusal(pairs: &[(&str, &str)]) -> PilotRefusal {
    PilotConfig::from_lookup(env(pairs)).expect_err("must be refused")
}

// ---------------------------------------------------------------------------------------------------------------
// Layer 1: startup
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_complete_paper_only_environment_is_accepted() {
    let cfg = PilotConfig::from_lookup(env(&good_env())).unwrap();
    assert_eq!((cfg.tenant_id.as_str(), cfg.account_id.as_str()), (TENANT, ACCOUNT));
}

#[test]
fn ids_are_canonicalised_to_lowercase_hyphenated_uuids() {
    let cfg = PilotConfig::from_lookup(env(&[(ENV_PAPER_ONLY, "true"), (ENV_TENANT, "  3F2A9C1E-5B7D-4E8A-9C0D-1A2B3C4D5E6F "), (ENV_ACCOUNT, "8D7C6B5A4F3E4D2C8B1A0F9E8D7C6B5A")])).unwrap();
    assert_eq!((cfg.tenant_id.as_str(), cfg.account_id.as_str()), (TENANT, ACCOUNT));
}

#[test]
fn startup_refuses_unless_paper_only_is_exactly_true() {
    let without: Vec<_> = good_env().into_iter().filter(|(k, _)| *k != ENV_PAPER_ONLY).collect();
    let r = refusal(&without);
    assert_eq!((r.code(), &r), ("PILOT_PAPER_ONLY_NOT_SET", &PilotRefusal::PaperOnlyNotSet));
    for bad in ["false", "TRUE", "True", "1", "yes", "", "true false", "trueish"] {
        let mut e = without.clone();
        e.push((ENV_PAPER_ONLY, bad));
        let r = refusal(&e);
        assert_eq!(r.code(), "PILOT_PAPER_ONLY_BAD_VALUE", "{bad:?}");
    }
}

#[test]
fn startup_refuses_missing_or_invalid_pilot_ids() {
    for (drop_key, code) in [(ENV_TENANT, "PILOT_ID_MISSING"), (ENV_ACCOUNT, "PILOT_ID_MISSING")] {
        let e: Vec<_> = good_env().into_iter().filter(|(k, _)| *k != drop_key).collect();
        assert_eq!(refusal(&e).code(), code, "{drop_key} missing");
        let mut blank = e.clone();
        blank.push((drop_key, "   "));
        assert_eq!(refusal(&blank).code(), code, "{drop_key} blank");
    }
    for (key, bad) in [
        (ENV_TENANT, "not-a-uuid"),
        (ENV_ACCOUNT, "not-a-uuid"),
        (ENV_TENANT, "00000000-0000-0000-0000-000000000000"),
        (ENV_ACCOUNT, "00000000-0000-0000-0000-000000000000"),
        (ENV_TENANT, "3f2a9c1e-5b7d-4e8a-9c0d"),
        (ENV_ACCOUNT, "*"),
        (ENV_ACCOUNT, "8d7c6b5a-4f3e-4d2c-8b1a-0f9e8d7c6b5a; DROP TABLE mandates"),
    ] {
        let mut e: Vec<_> = good_env().into_iter().filter(|(k, _)| *k != key).collect();
        e.push((key, bad));
        assert_eq!(refusal(&e).code(), "PILOT_ID_INVALID", "{key}={bad}");
    }
}

#[test]
fn startup_refuses_when_the_tenant_credential_master_key_is_in_the_environment() {
    for value in ["a-real-looking-key", ""] {
        let mut e = good_env();
        e.push(("CREDENTIALS_ENCRYPTION_KEY", value));
        let r = refusal(&e);
        assert_eq!(r, PilotRefusal::ForbiddenEnvPresent("CREDENTIALS_ENCRYPTION_KEY"), "value {value:?}: even an empty variable counts as present");
        assert_eq!(r.code(), "PILOT_FORBIDDEN_ENV_PRESENT");
        assert!(!r.to_string().contains("a-real-looking-key"), "the value is never echoed");
    }
}

#[test]
fn refusal_codes_are_stable_and_distinct() {
    let all = [
        PilotRefusal::PaperOnlyNotSet,
        PilotRefusal::PaperOnlyBadValue(String::new()),
        PilotRefusal::MissingId("X"),
        PilotRefusal::BadId { name: "X" },
        PilotRefusal::ForbiddenEnvPresent("X"),
        PilotRefusal::AccountUnderOtherTenant { account_id: String::new() },
        PilotRefusal::AmbiguousAccount { count: 2 },
        PilotRefusal::ModeNotAllowed { account_id: String::new(), mode: "paper" },
        PilotRefusal::VenueNotAllowed { what: "x", venue: String::new() },
        PilotRefusal::NoSleeves { account_id: String::new() },
        PilotRefusal::NotPaperEnvironment { venue: String::new(), environment: String::new() },
        PilotRefusal::Adapter(String::new()),
    ];
    let codes: Vec<&str> = all.iter().map(PilotRefusal::code).collect();
    assert_eq!(
        codes,
        [
            "PILOT_PAPER_ONLY_NOT_SET",
            "PILOT_PAPER_ONLY_BAD_VALUE",
            "PILOT_ID_MISSING",
            "PILOT_ID_INVALID",
            "PILOT_FORBIDDEN_ENV_PRESENT",
            "PILOT_ACCOUNT_UNDER_OTHER_TENANT",
            "PILOT_ACCOUNT_AMBIGUOUS",
            "PILOT_MODE_NOT_ALLOWED",
            "PILOT_VENUE_NOT_ALLOWED",
            "PILOT_NO_SLEEVES",
            "PILOT_NOT_PAPER_ENVIRONMENT",
            "PILOT_ADAPTER_REFUSED",
        ]
    );
    assert_eq!(codes.iter().collect::<std::collections::BTreeSet<_>>().len(), codes.len());
    for r in &all {
        assert!(r.to_string().starts_with(r.code()), "{r}");
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Layer 3: the run config
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_pilot_run_config_is_paper_only_and_otherwise_the_defaults() {
    let c = pilot_run_config();
    assert_eq!(c.venue_policy, VenuePolicy::PaperOnly);
    let d = rebalancer_run::pipeline::RunConfig::default();
    assert_eq!((c.max_polls, c.lease_secs, c.max_price_age_secs, c.min_trade_abs), (d.max_polls, d.lease_secs, d.max_price_age_secs, d.min_trade_abs));
}

// ---------------------------------------------------------------------------------------------------------------
// Layer 5: the allow-list
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn only_alpaca_paper_is_on_the_allow_list() {
    assert!(allow_venue_environment("alpaca", "paper").is_ok());
    assert!(allow_venue_environment(" ALPACA ", "Paper ").is_ok());
    for (v, e) in [("alpaca", "live"), ("alpaca", "Live"), ("alpaca", ""), ("alpaca", "paperlive"), ("alpaca", "production"), ("kraken", "paper"), ("oanda", "practice"), ("", "paper"), ("alpaca_paper", "paper")] {
        match allow_venue_environment(v, e) {
            Err(PilotRefusal::NotPaperEnvironment { .. }) => {}
            other => panic!("({v:?}, {e:?}): {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Layer 2: the account source serves exactly one pair
// ---------------------------------------------------------------------------------------------------------------

fn mandate_body(venues: &[&str]) -> mandate_core::mandate::MandateBody {
    let mut v: Value = serde_json::from_str(include_str!("../../mandate-core/tests/fixtures/baseline_mandate.json")).unwrap();
    v["universe"]["venues"] = json!(venues);
    serde_json::from_value(v).unwrap()
}

fn sleeve(venue: &str) -> SleeveSpec {
    SleeveSpec { id: "etf".into(), kind: SleeveKind::EtfTrend, share: rebalancer_run::Dec::parse("1").unwrap(), venue: venue.into(), asset_class: "us_etf".into(), quote: "USD".into() }
}

fn account(tenant: &str, account: &str, mode: ExecutionMode) -> ActiveAccount {
    let t = |s: &str| s.parse::<DateTime<Utc>>().unwrap();
    ActiveAccount {
        account_id: account.into(),
        tenant_id: tenant.into(),
        mandate: mandate_body(&["alpaca"]),
        envelope: MandateEnvelope { version: 1, status: MandateStatus::Active, effective_from: t("2020-01-01T00:00:00Z"), review_by: t("2099-01-01T00:00:00Z") },
        plan_approved: true,
        sleeves: vec![sleeve("alpaca")],
        mode,
    }
}

fn source(accounts: Vec<ActiveAccount>) -> PilotAccountSource<InMemoryAccountSource> {
    let inner = InMemoryAccountSource::new();
    inner.set_accounts(accounts);
    PilotAccountSource::new(inner, PilotConfig::from_lookup(env(&good_env())).unwrap())
}

fn err_code(s: &PilotAccountSource<InMemoryAccountSource>) -> String {
    let e = s.active_accounts().expect_err("must be refused");
    e.split(':').next().unwrap().to_string()
}

#[test]
fn the_pilot_pair_is_served_in_assisted_and_live_on_paper_mode_and_nothing_else_is_visible() {
    for mode in [ExecutionMode::Assisted, ExecutionMode::Live] {
        let s = source(vec![
            account(OTHER_TENANT, OTHER_ACCOUNT, ExecutionMode::Live), // another tenant's LIVE account: invisible, never an error
            account(TENANT, OTHER_ACCOUNT, ExecutionMode::Live),       // same tenant, another account: invisible
            account(TENANT, ACCOUNT, mode),
        ]);
        let served = s.active_accounts().unwrap();
        assert_eq!(served.len(), 1, "{mode:?}");
        assert_eq!((served[0].tenant_id.as_str(), served[0].account_id.as_str(), served[0].mode), (TENANT, ACCOUNT, mode));
    }
}

#[test]
fn no_pilot_account_means_an_empty_enumeration_not_an_error() {
    assert!(source(vec![account(OTHER_TENANT, OTHER_ACCOUNT, ExecutionMode::Live)]).active_accounts().unwrap().is_empty());
    assert!(source(vec![]).active_accounts().unwrap().is_empty());
}

#[test]
fn the_pilot_account_id_under_another_tenant_fails_the_whole_enumeration() {
    let s = source(vec![account(TENANT, OTHER_ACCOUNT, ExecutionMode::Assisted), account(OTHER_TENANT, ACCOUNT, ExecutionMode::Assisted)]);
    assert_eq!(err_code(&s), "PILOT_ACCOUNT_UNDER_OTHER_TENANT");
}

#[test]
fn two_matching_accounts_are_ambiguous() {
    let s = source(vec![account(TENANT, ACCOUNT, ExecutionMode::Assisted), account(TENANT, &ACCOUNT.to_uppercase(), ExecutionMode::Assisted)]);
    assert_eq!(err_code(&s), "PILOT_ACCOUNT_AMBIGUOUS", "ids compare case-insensitively");
}

#[test]
fn the_validate_only_paper_mode_is_not_a_pilot_mode() {
    assert_eq!(err_code(&source(vec![account(TENANT, ACCOUNT, ExecutionMode::Paper)])), "PILOT_MODE_NOT_ALLOWED");
}

#[test]
fn a_non_alpaca_sleeve_or_mandate_venue_or_an_account_without_sleeves_is_refused() {
    for venue in ["kraken", "oanda", "", "alpaca_live"] {
        let mut a = account(TENANT, ACCOUNT, ExecutionMode::Assisted);
        a.sleeves = vec![sleeve("alpaca"), sleeve(venue)];
        assert_eq!(err_code(&source(vec![a])), "PILOT_VENUE_NOT_ALLOWED", "sleeve venue {venue:?}");
        let mut a = account(TENANT, ACCOUNT, ExecutionMode::Assisted);
        a.mandate = mandate_body(&["alpaca", venue]);
        assert_eq!(err_code(&source(vec![a])), "PILOT_VENUE_NOT_ALLOWED", "mandate venue {venue:?}");
    }
    let mut a = account(TENANT, ACCOUNT, ExecutionMode::Assisted);
    a.sleeves = vec![];
    assert_eq!(err_code(&source(vec![a])), "PILOT_NO_SLEEVES");
}

#[test]
fn a_failing_inner_source_surfaces_as_an_adapter_refusal() {
    struct Broken;
    impl AccountSource for Broken {
        fn active_accounts(&self) -> Result<Vec<ActiveAccount>, String> {
            Err("db down".into())
        }
    }
    let s = PilotAccountSource::new(Broken, PilotConfig::from_lookup(env(&good_env())).unwrap());
    let e = s.active_accounts().unwrap_err();
    assert!(e.starts_with("PILOT_ADAPTER_REFUSED") && e.contains("db down"), "{e}");
}

// ---------------------------------------------------------------------------------------------------------------
// Layer 4: the only Alpaca builder
// ---------------------------------------------------------------------------------------------------------------

const PAPER_KEY: &str = "PKTESTFIXTUREKEY0001";
const LIVE_KEY: &str = "AKTESTFIXTUREKEY0002";
const SECRET: &str = "unit-test-secret-not-a-real-key-9f3a";

#[test]
fn the_paper_builder_accepts_a_paper_key_on_the_paper_host_with_the_pilot_settings() {
    let t = Arc::new(FakeTransport::new());
    let built = build_paper_alpaca(PAPER_KEY, SECRET, "https://paper-api.alpaca.markets", t.clone()).unwrap();
    assert_eq!(built.adapter().environment(), broker_adapters::alpaca::Environment::Paper);
    assert_eq!(built.adapter().config().own_tag_prefix.as_deref(), Some("rb1:"));
    assert!(built.adapter().config().refuse_builtin_assets);
    assert_eq!(t.request_count(), 0, "building makes no call");
}

#[test]
fn the_paper_builder_refuses_a_live_key_a_live_host_and_every_look_alike() {
    for (key, url) in [
        (LIVE_KEY, "https://paper-api.alpaca.markets"),
        (PAPER_KEY, "https://api.alpaca.markets"),
        (LIVE_KEY, "https://api.alpaca.markets"),
        ("", "https://paper-api.alpaca.markets"),
        ("CK123", "https://paper-api.alpaca.markets"),
        (PAPER_KEY, "https://paper-api.alpaca.markets.evil.com"),
        (PAPER_KEY, "https://paper-api.alpaca.markets@api.alpaca.markets"),
    ] {
        let t = Arc::new(FakeTransport::new());
        match build_paper_alpaca(key, SECRET, url, t.clone()) {
            Err(r) => {
                assert_eq!(r.code(), "PILOT_ADAPTER_REFUSED", "{key:?} {url}");
                let text = r.to_string();
                assert!(!text.contains(SECRET) && (key.len() < 3 || !text.contains(key)), "no secret is echoed: {text}");
            }
            Ok(_) => panic!("{key:?} {url} must be refused"),
        }
        assert_eq!(t.request_count(), 0);
    }
}
