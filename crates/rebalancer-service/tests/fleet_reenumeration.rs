//! Periodic re-enumeration of the account runtimes (`rebalancer_service::fleet`): a plan approved after startup gets a
//! runtime without a restart, an account that stops being eligible loses it, zero accounts is a normal state, and one
//! account's credential or connect failure never blocks another. Offline: fake credential provider, a string as the
//! "runtime".

use std::cell::RefCell;
use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_run::data::{SleeveKind, SleeveSpec};
use rebalancer_run::driver::ActiveAccount;
use rebalancer_run::record::ExecutionMode;
use rebalancer_service::broker_credentials::BROKER_UNAVAILABLE;
use rebalancer_service::credentials::{CredentialError, PaperAlpacaKeys, PaperCredentialProvider};
use rebalancer_service::fleet::{
    backoff_secs, connect_account, reenumerate_secs_from_lookup, Fleet, ReenumerateSchedule, DEFAULT_REENUMERATE_SECS, ENV_REENUMERATE_SECS, MAX_BACKOFF_SECS,
};
use rebalancer_service::pilot::PilotRefusal;
use rebalancer_service::runtime::RuntimeError;
use rebalancer_store::{PilotAccount, PlanExecution, PlanOrigin};
use serde_json::{json, Value};
use uuid::Uuid;

const TENANT_A: &str = "3f2a9c1e-5b7d-4e8a-9c0d-1a2b3c4d5e6f";
const TENANT_B: &str = "11111111-2222-4333-8444-555555555555";
const ACCT_A: &str = "8d7c6b5a-4f3e-4d2c-8b1a-0f9e8d7c6b5a";
const ACCT_B: &str = "99999999-8888-4777-8666-555555555555";
const SECRET_MARKER: &str = "SUPERSECRETVALUE";
const INTERVAL: i64 = 300;

fn t0() -> DateTime<Utc> {
    "2026-10-16T12:00:00Z".parse().unwrap()
}

fn mandate_body() -> mandate_core::mandate::MandateBody {
    let mut v: Value = serde_json::from_str(include_str!("../../mandate-core/tests/fixtures/baseline_mandate.json")).unwrap();
    v["universe"]["venues"] = json!(["alpaca"]);
    serde_json::from_value(v).unwrap()
}

fn account(tenant: &str, account: &str, plan: Uuid) -> PilotAccount {
    let t = |s: &str| s.parse::<DateTime<Utc>>().unwrap();
    PilotAccount {
        active: ActiveAccount {
            account_id: account.into(),
            tenant_id: tenant.into(),
            mandate: mandate_body(),
            envelope: MandateEnvelope { version: 1, status: MandateStatus::Active, effective_from: t("2020-01-01T00:00:00Z"), review_by: t("2099-01-01T00:00:00Z") },
            plan_approved: true,
            sleeves: vec![SleeveSpec {
                id: "etf_trend_faber".into(),
                kind: SleeveKind::EtfTrend,
                share: rebalancer_run::Dec::parse("1").unwrap(),
                venue: "alpaca".into(),
                asset_class: "us_etf".into(),
                quote: "".into(),
                instruments: Vec::new(),
                tranche_sessions: 0,
                lookback: None,
            }],
            mode: ExecutionMode::Assisted,
        },
        origin: PlanOrigin::Proposal,
        plan_id: plan,
        execution: PlanExecution::PaperOrders,
        credential_label: "paper".into(),
        credential_fingerprint: None,
        entries: Vec::new(),
    }
}

/// A broker stand-in: fails (with a stable reason) for the accounts in `failing`, serves a key for every other.
#[derive(Default)]
struct FakeBroker {
    failing: RefCell<BTreeSet<String>>,
    fetched: RefCell<Vec<String>>,
}

impl PaperCredentialProvider for FakeBroker {
    fn paper_alpaca_keys(&self, tenant_id: &str, account_id: &str) -> Result<PaperAlpacaKeys, CredentialError> {
        self.fetched.borrow_mut().push(account_id.to_string());
        if self.failing.borrow().contains(account_id) {
            return Err(CredentialError::NotProvided { tenant_id: tenant_id.into(), account_id: account_id.into(), reason: BROKER_UNAVAILABLE });
        }
        Ok(PaperAlpacaKeys { key_id: "PKTESTKEYID".into(), secret: SECRET_MARKER.into() })
    }
}

/// The "connect" step: succeeds with a label naming the account, never reads the secret into the label.
/// Returned as a closure (not a fn item) so clippy::result_large_err, which lints fn signatures, does not apply to
/// the production `RuntimeError` this seam has to name.
fn build() -> impl Fn(&PaperAlpacaKeys, &PilotAccount) -> Result<String, RuntimeError> {
    |keys: &PaperAlpacaKeys, acct: &PilotAccount| {
        assert_eq!(keys.secret, SECRET_MARKER);
        Ok(format!("runtime:{}", acct.active.account_id))
    }
}

fn cycle(fleet: &mut Fleet<String>, broker: &FakeBroker, accounts: &[PilotAccount], now: DateTime<Utc>) -> rebalancer_service::fleet::ReconcileReport {
    fleet.reconcile(accounts, now, |a| connect_account(a, broker, build()))
}

#[test]
fn a_new_account_appears_after_tick_one_and_gets_a_runtime() {
    let broker = FakeBroker::default();
    let mut fleet: Fleet<String> = Fleet::new(INTERVAL);
    // Startup: nothing approved yet.
    let r0 = cycle(&mut fleet, &broker, &[], t0());
    assert!(fleet.is_empty());
    assert_eq!(r0, Default::default());
    // A plan is approved; the next cycle finds it.
    let a = account(TENANT_A, ACCT_A, Uuid::from_u128(1));
    let r1 = cycle(&mut fleet, &broker, std::slice::from_ref(&a), t0() + Duration::seconds(INTERVAL));
    assert_eq!(r1.added, vec![ACCT_A.to_string()]);
    assert_eq!(fleet.get(ACCT_A).map(String::as_str), Some(format!("runtime:{ACCT_A}").as_str()));
    assert_eq!(*broker.fetched.borrow(), vec![ACCT_A.to_string()], "the credential was fetched once, for the new account only");
    // The next cycle with nothing new keeps the runtime and fetches nothing.
    let r2 = cycle(&mut fleet, &broker, std::slice::from_ref(&a), t0() + Duration::seconds(2 * INTERVAL));
    assert_eq!((r2.kept, r2.added.len(), r2.failed.len()), (1, 0, 0));
    assert_eq!(broker.fetched.borrow().len(), 1);
}

#[test]
fn an_account_that_is_no_longer_eligible_is_dropped_and_the_other_kept() {
    let broker = FakeBroker::default();
    let mut fleet: Fleet<String> = Fleet::new(INTERVAL);
    let a = account(TENANT_A, ACCT_A, Uuid::from_u128(1));
    let b = account(TENANT_B, ACCT_B, Uuid::from_u128(2));
    cycle(&mut fleet, &broker, &[a, b.clone()], t0());
    assert_eq!(fleet.len(), 2);
    let r = cycle(&mut fleet, &broker, &[b], t0() + Duration::seconds(INTERVAL));
    assert_eq!(r.dropped, vec![ACCT_A.to_string()]);
    assert!(!fleet.contains(ACCT_A));
    assert!(fleet.contains(ACCT_B));
    assert_eq!(r.kept, 1);
}

#[test]
fn zero_accounts_is_a_normal_state_never_an_exit() {
    let broker = FakeBroker::default();
    let mut fleet: Fleet<String> = Fleet::new(INTERVAL);
    for i in 0..5 {
        let r = cycle(&mut fleet, &broker, &[], t0() + Duration::seconds(i * INTERVAL));
        assert!(r.added.is_empty() && r.failed.is_empty() && r.dropped.is_empty());
        assert!(fleet.is_empty());
    }
    assert!(broker.fetched.borrow().is_empty(), "no account, no credential fetch");
    // Losing the last account empties the fleet and the next cycle is still fine.
    let a = account(TENANT_A, ACCT_A, Uuid::from_u128(1));
    cycle(&mut fleet, &broker, std::slice::from_ref(&a), t0());
    let r = cycle(&mut fleet, &broker, &[], t0() + Duration::seconds(INTERVAL));
    assert_eq!(r.dropped.len(), 1);
    assert!(fleet.is_empty());
    assert!(r.summary_line(fleet.len()).contains("0 runtime(s) now"));
}

#[test]
fn a_failing_broker_fetch_for_one_account_does_not_block_another() {
    let broker = FakeBroker::default();
    broker.failing.borrow_mut().insert(ACCT_A.to_string());
    let mut fleet: Fleet<String> = Fleet::new(INTERVAL);
    let a = account(TENANT_A, ACCT_A, Uuid::from_u128(1));
    let b = account(TENANT_B, ACCT_B, Uuid::from_u128(2));
    let accounts = [a, b];

    // A is listed first and fails; B still gets its runtime in the same cycle.
    let r1 = cycle(&mut fleet, &broker, &accounts, t0());
    assert_eq!(r1.added, vec![ACCT_B.to_string()]);
    assert!(fleet.contains(ACCT_B) && !fleet.contains(ACCT_A));
    assert_eq!(r1.failed.len(), 1);
    let f = &r1.failed[0];
    assert_eq!((f.account_id.as_str(), f.tenant_id.as_str()), (ACCT_A, TENANT_A));
    assert_eq!(f.failure.code, BROKER_UNAVAILABLE);
    assert!(f.first_in_streak && f.consecutive == 1);
    assert_eq!(f.retry_at, t0() + Duration::seconds(INTERVAL));
    assert!(!f.failure.detail.contains(SECRET_MARKER) && !format!("{f:?}").contains(SECRET_MARKER));

    // Inside the backoff window A is not retried (no fetch for it), B is untouched.
    let fetched_before = broker.fetched.borrow().len();
    let r2 = cycle(&mut fleet, &broker, &accounts, t0() + Duration::seconds(10));
    assert_eq!(r2.deferred, vec![ACCT_A.to_string()]);
    assert_eq!(broker.fetched.borrow().len(), fetched_before);

    // Next cycle: still failing, second consecutive failure, NOT a new streak (no second alert), doubled delay.
    let now3 = t0() + Duration::seconds(INTERVAL);
    let r3 = cycle(&mut fleet, &broker, &accounts, now3);
    assert_eq!(r3.failed.len(), 1);
    assert!(!r3.failed[0].first_in_streak);
    assert_eq!(r3.failed[0].consecutive, 2);
    assert_eq!(r3.failed[0].retry_at, now3 + Duration::seconds(2 * INTERVAL));
    assert!(fleet.contains(ACCT_B));

    // The broker recovers; once the backoff elapses A joins the fleet and the streak is over.
    broker.failing.borrow_mut().clear();
    let r4 = cycle(&mut fleet, &broker, &accounts, now3 + Duration::seconds(2 * INTERVAL));
    assert_eq!(r4.added, vec![ACCT_A.to_string()]);
    assert_eq!(fleet.len(), 2);
    // A later failure starts a fresh streak (alert again).
    broker.failing.borrow_mut().insert(ACCT_A.to_string());
    let changed = [account(TENANT_A, ACCT_A, Uuid::from_u128(11)), accounts[1].clone()];
    let r5 = cycle(&mut fleet, &broker, &changed, now3 + Duration::seconds(10 * INTERVAL));
    assert_eq!(r5.failed.len(), 1);
    assert!(r5.failed[0].first_in_streak);
}

#[test]
fn a_connect_failure_after_a_good_credential_fetch_is_typed_and_isolated() {
    let broker = FakeBroker::default();
    let mut fleet: Fleet<String> = Fleet::new(INTERVAL);
    let a = account(TENANT_A, ACCT_A, Uuid::from_u128(1));
    let b = account(TENANT_B, ACCT_B, Uuid::from_u128(2));
    let r = fleet.reconcile(&[a, b], t0(), |acct| {
        connect_account(acct, &broker, |keys, acct| {
            if acct.active.account_id == ACCT_A {
                Err(RuntimeError::Connect(PilotRefusal::Adapter(format!("refused for {}", keys.key_id.len()))))
            } else {
                build()(keys, acct)
            }
        })
    });
    assert_eq!(r.failed.len(), 1);
    assert_eq!(r.failed[0].failure.code, "PILOT_RUNTIME_CONNECT");
    assert!(!r.failed[0].failure.detail.contains(SECRET_MARKER));
    assert_eq!(r.added, vec![ACCT_B.to_string()]);
}

#[test]
fn a_replaced_plan_rebuilds_the_runtime_and_a_failed_rebuild_leaves_no_stale_runtime() {
    let broker = FakeBroker::default();
    let mut fleet: Fleet<String> = Fleet::new(INTERVAL);
    cycle(&mut fleet, &broker, &[account(TENANT_A, ACCT_A, Uuid::from_u128(1))], t0());
    let r = cycle(&mut fleet, &broker, &[account(TENANT_A, ACCT_A, Uuid::from_u128(2))], t0() + Duration::seconds(INTERVAL));
    assert_eq!((r.added.len(), r.replaced.len()), (1, 1));
    assert_eq!(broker.fetched.borrow().len(), 2);
    // Another plan replaces it while the broker is down: the old runtime (verified for the old plan) is gone.
    broker.failing.borrow_mut().insert(ACCT_A.to_string());
    let r = cycle(&mut fleet, &broker, &[account(TENANT_A, ACCT_A, Uuid::from_u128(3))], t0() + Duration::seconds(2 * INTERVAL));
    assert_eq!(r.failed.len(), 1);
    assert!(!fleet.contains(ACCT_A));
    assert_eq!(r.failed[0].failure.code, BROKER_UNAVAILABLE);
}

#[test]
fn seeded_runtimes_survive_the_first_cycle_without_a_refetch() {
    let broker = FakeBroker::default();
    let plan = Uuid::from_u128(7);
    let mut fleet = Fleet::with_connected(INTERVAL, [(ACCT_A.to_string(), plan, "startup-runtime".to_string())]);
    let r = cycle(&mut fleet, &broker, &[account(TENANT_A, ACCT_A, plan)], t0());
    assert_eq!(r.kept, 1);
    assert_eq!(fleet.get(ACCT_A).map(String::as_str), Some("startup-runtime"));
    assert!(broker.fetched.borrow().is_empty());
}

#[test]
fn backoff_doubles_and_is_capped() {
    assert_eq!(backoff_secs(300, MAX_BACKOFF_SECS, 1), 300);
    assert_eq!(backoff_secs(300, MAX_BACKOFF_SECS, 2), 600);
    assert_eq!(backoff_secs(300, MAX_BACKOFF_SECS, 3), 1200);
    assert_eq!(backoff_secs(300, MAX_BACKOFF_SECS, 4), 2400);
    assert_eq!(backoff_secs(300, MAX_BACKOFF_SECS, 5), MAX_BACKOFF_SECS);
    assert_eq!(backoff_secs(300, MAX_BACKOFF_SECS, 500), MAX_BACKOFF_SECS);
}

#[test]
fn the_schedule_fires_once_per_interval() {
    let mut s = ReenumerateSchedule::new(300, t0());
    assert!(!s.due(t0() + Duration::seconds(1)), "startup counted as the last enumeration");
    assert!(!s.due(t0() + Duration::seconds(299)));
    assert!(s.due(t0() + Duration::seconds(300)));
    assert!(!s.due(t0() + Duration::seconds(301)));
    assert!(s.due(t0() + Duration::seconds(600)));
}

#[test]
fn the_interval_env_defaults_to_300_and_refuses_junk() {
    let lookup = |v: Option<&'static str>| move |k: &str| if k == ENV_REENUMERATE_SECS { v.map(String::from) } else { None };
    assert_eq!(reenumerate_secs_from_lookup(lookup(None)), Ok(DEFAULT_REENUMERATE_SECS));
    assert_eq!(DEFAULT_REENUMERATE_SECS, 300);
    assert_eq!(reenumerate_secs_from_lookup(lookup(Some("  "))), Ok(300));
    assert_eq!(reenumerate_secs_from_lookup(lookup(Some("60"))), Ok(60));
    for bad in ["0", "-5", "abc", "1.5"] {
        assert!(reenumerate_secs_from_lookup(lookup(Some(bad))).unwrap_err().contains(ENV_REENUMERATE_SECS), "{bad}");
    }
}
