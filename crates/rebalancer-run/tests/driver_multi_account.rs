//! Part 3 of WP4.8: `find_due_runs` / `run_all_due` over MULTIPLE tenant-accounts in one cycle.
//! Extends the fake-broker-driven style of `tests/pipeline.rs` (real `KrakenAdapter` against a fake
//! exchange, per account this time) rather than mocking the pipeline.
//!
//! (a) two accounts both due -> both run, independently; one's failure (fake-broker fault injection)
//!     does not stop the other.
//! (b) an account not yet due is excluded.
//! (c) an account whose mandate is not active is excluded.
//! (d) calling `run_all_due` twice in quick succession does not double-run either account.
//! (e) the Postgres-backed stores' round-trip is covered separately, in `rebalancer-store`'s own test
//!     suite (needs a real Postgres instance; this crate has no database dependency at all).

mod common;

use common::harness::{active_envelope, crypto_mandate_json, crypto_sleeve, mandate_from, panel, slot_time};
use common::{at, book, d, kraken_rules, Env};
use fake_broker::fault::Fault;
use rebalancer_core::policy::MandateStatus;
use rebalancer_run::clock::ManualClock;
use rebalancer_run::data::{SleeveKind, SleeveSpec};
use rebalancer_run::driver::{find_due_runs, run_all_due, ActiveAccount, AccountRuntime, InMemoryAccountSource};
use rebalancer_run::pipeline::RunConfig;
use rebalancer_run::record::{ExecutionMode, OutcomeKind};
use rebalancer_run::stores::InMemoryRunStore;
use rebalancer_run::testkit::{FixtureData, InMemoryAccountLock, RecordingNotifier, SwitchKillFlag};
use rebalancer_risk::store::InMemoryStateStore;
use std::collections::BTreeMap;

fn etf_sleeve(share: &str) -> SleeveSpec {
    SleeveSpec { id: "etf".into(), kind: SleeveKind::EtfTrend, share: d(share), venue: "kraken".into(), asset_class: "crypto_spot".into(), quote: "USD".into() }
}

fn crypto_account(account_id: &str, tenant_id: &str) -> ActiveAccount {
    ActiveAccount {
        account_id: account_id.to_string(),
        tenant_id: tenant_id.to_string(),
        mandate: mandate_from(crypto_mandate_json()),
        envelope: active_envelope(),
        plan_approved: true,
        sleeves: vec![crypto_sleeve("1")],
        mode: ExecutionMode::Live,
    }
}

/// A fresh fake Kraken exchange, priced BTC/ETH both trending up, funded with 10000 USD.
fn fixture_env() -> Env {
    Env::with_usd("10000")
}

fn fixture_data(env: &Env) -> FixtureData {
    let handle = env.rig.handle.clone();
    FixtureData::new().with_panel("crypto", panel(true, true)).with_price_fn(move |sym| match sym {
        "BTC/USD" | "ETH/USD" => Some(handle.price(sym)),
        _ => None,
    })
}

// ---------------------------------------------------------------------------------------------------------------
// (a) two accounts both due -> both run independently; one's failure never stops the other
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_two_accounts_both_due_run_independently_and_one_failure_does_not_stop_the_other() {
    let env_a = fixture_env();
    let env_b = fixture_env();
    let data_a = fixture_data(&env_a);
    let data_b = fixture_data(&env_b);
    let rules_a = kraken_rules(&env_a.pairs);
    let rules_b = kraken_rules(&env_b.pairs);
    let book_a = book(&rules_a);
    let book_b = book(&rules_b);
    let broker_a = env_a.broker();
    let broker_b = env_b.broker();

    // B's exchange refuses every request from here on: a real outage, not a crash inside our code.
    env_b.rig.handle.inject_fault(Fault::timeout().forever());

    let clock = ManualClock::new(slot_time(0));
    let runs = InMemoryRunStore::new();
    let states = InMemoryStateStore::new();
    let notifier = RecordingNotifier::new();
    let kill = SwitchKillFlag::new();
    let lock = InMemoryAccountLock::new();
    let cfg = RunConfig::default();

    let source = InMemoryAccountSource::new().with_account(crypto_account("acct-a", "tenant-a")).with_account(crypto_account("acct-b", "tenant-b"));
    let due = find_due_runs(&source, slot_time(0)).expect("enumeration must not fail");
    assert_eq!(due.len(), 2, "both accounts are due (daily crypto sleeve)");

    let mut runtimes = BTreeMap::new();
    runtimes.insert("acct-a".to_string(), AccountRuntime { broker: &broker_a, data: &data_a, venue_rules: &book_a });
    runtimes.insert("acct-b".to_string(), AccountRuntime { broker: &broker_b, data: &data_b, venue_rules: &book_b });

    let outcomes = run_all_due(due, &runtimes, &states, &runs, &notifier, &kill, &clock, &lock, &cfg);
    assert_eq!(outcomes.len(), 2, "the loop must not abort early: BOTH candidates were attempted");

    let a = outcomes.iter().find(|o| o.spec.account_id == "acct-a").expect("acct-a present");
    let b = outcomes.iter().find(|o| o.spec.account_id == "acct-b").expect("acct-b present");

    let a_rec = a.result.as_ref().expect("acct-a's run_once was reached and returned a record");
    assert_eq!(a_rec.outcome.kind, OutcomeKind::Completed, "{:?}", a_rec.outcome);
    assert!(env_a.bal("BTC").is_positive() && env_a.bal("ETH").is_positive(), "acct-a actually traded");

    let b_rec = b.result.as_ref().expect("acct-b's run_once was reached (the fault is inside the pipeline, not a panic)");
    assert_eq!(b_rec.outcome.kind, OutcomeKind::FailedClosed, "a broker outage must fail closed, not crash the driver: {:?}", b_rec.outcome);
    assert_eq!(b_rec.outcome.code, "RUN_BROKER_UNREACHABLE");
    assert!(env_b.bal("BTC").is_zero() && env_b.bal("ETH").is_zero(), "acct-b's outage must mean NO trading happened for it");
}

// ---------------------------------------------------------------------------------------------------------------
// (b) an account not yet due is excluded
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn b_an_account_not_yet_due_is_excluded() {
    let mut etf_account = crypto_account("acct-etf", "tenant-a");
    etf_account.sleeves = vec![etf_sleeve("1")]; // ETF trend: due only at a calendar month-end.

    let source = InMemoryAccountSource::new().with_account(etf_account);

    // 2021-01-01 is not a month-end: excluded.
    let not_due = find_due_runs(&source, slot_time(0)).unwrap();
    assert!(not_due.is_empty(), "an ETF-only account must not be due on an ordinary day: {not_due:?}");

    // 2021-01-31 IS a month-end: due.
    let month_end = at("2021-01-31T00:10:00Z");
    let due = find_due_runs(&source, month_end).unwrap();
    assert_eq!(due.len(), 1, "the same account IS due once its sleeve's cadence boundary arrives");
    assert_eq!(due[0].account_id, "acct-etf");

    // Before the day's run time (00:10 UTC), even a month-end date is not yet due.
    let too_early = at("2021-01-31T00:00:00Z");
    assert!(find_due_runs(&source, too_early).unwrap().is_empty(), "not due until the day's anchor time");
}

// ---------------------------------------------------------------------------------------------------------------
// (c) an account whose mandate is not active is excluded
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn c_an_account_whose_mandate_is_not_active_is_excluded() {
    for status in [MandateStatus::Draft, MandateStatus::Superseded, MandateStatus::Revoked, MandateStatus::Expired] {
        let mut acct = crypto_account("acct-inactive", "tenant-a");
        acct.envelope.status = status;
        let source = InMemoryAccountSource::new().with_account(acct);
        let due = find_due_runs(&source, slot_time(0)).unwrap();
        assert!(due.is_empty(), "mandate status {status:?} must exclude the account from the due list");
    }

    // A plan that is not approved is excluded the same way, even with an Active mandate.
    let mut unapproved = crypto_account("acct-unapproved", "tenant-a");
    unapproved.plan_approved = false;
    let source = InMemoryAccountSource::new().with_account(unapproved);
    assert!(find_due_runs(&source, slot_time(0)).unwrap().is_empty(), "an unapproved plan must exclude the account too");

    // Sanity: the SAME account, active and approved, IS due.
    let source = InMemoryAccountSource::new().with_account(crypto_account("acct-active", "tenant-a"));
    assert_eq!(find_due_runs(&source, slot_time(0)).unwrap().len(), 1);
}

// ---------------------------------------------------------------------------------------------------------------
// (d) calling run_all_due twice in quick succession does not double-run either account
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn d_calling_run_all_due_twice_in_quick_succession_does_not_double_run_either_account() {
    let env_a = fixture_env();
    let env_b = fixture_env();
    let data_a = fixture_data(&env_a);
    let data_b = fixture_data(&env_b);
    let rules_a = kraken_rules(&env_a.pairs);
    let rules_b = kraken_rules(&env_b.pairs);
    let book_a = book(&rules_a);
    let book_b = book(&rules_b);
    let broker_a = env_a.broker();
    let broker_b = env_b.broker();

    let clock = ManualClock::new(slot_time(0));
    let runs = InMemoryRunStore::new();
    let states = InMemoryStateStore::new();
    let notifier = RecordingNotifier::new();
    let kill = SwitchKillFlag::new();
    let lock = InMemoryAccountLock::new();
    let cfg = RunConfig::default();

    let source = InMemoryAccountSource::new().with_account(crypto_account("acct-a", "tenant-a")).with_account(crypto_account("acct-b", "tenant-b"));

    let mut runtimes = BTreeMap::new();
    runtimes.insert("acct-a".to_string(), AccountRuntime { broker: &broker_a, data: &data_a, venue_rules: &book_a });
    runtimes.insert("acct-b".to_string(), AccountRuntime { broker: &broker_b, data: &data_b, venue_rules: &book_b });

    // First tick: enumerate and run.
    let due1 = find_due_runs(&source, slot_time(0)).unwrap();
    let outcomes1 = run_all_due(due1, &runtimes, &states, &runs, &notifier, &kill, &clock, &lock, &cfg);
    assert!(
        outcomes1.iter().all(|o| matches!(&o.result, Ok(r) if r.outcome.kind == OutcomeKind::Completed)),
        "{:?}",
        outcomes1.iter().map(|o| o.result.as_ref().map(|r| r.outcome.clone())).collect::<Vec<_>>()
    );
    let requests_after_first_a = env_a.rig.handle.requests().len();
    let requests_after_first_b = env_b.rig.handle.requests().len();
    let bal_btc_a_after_first = env_a.bal("BTC");
    let bal_btc_b_after_first = env_b.bal("BTC");
    assert!(bal_btc_a_after_first.is_positive() && bal_btc_b_after_first.is_positive(), "the first tick actually traded both accounts");

    // Second tick, "in quick succession": re-enumerate (same `now`, so the SAME scheduled_for slot) and
    // run again with the exact same shared stores/lock.
    let due2 = find_due_runs(&source, slot_time(0)).unwrap();
    assert_eq!(due2.len(), 2, "enumeration itself is stateless and proposes the same candidates again -- exactly-once is run_all_due's job, not find_due_runs'");
    let outcomes2 = run_all_due(due2, &runtimes, &states, &runs, &notifier, &kill, &clock, &lock, &cfg);
    assert_eq!(outcomes2.len(), 2);
    for o in &outcomes2 {
        let rec = o.result.as_ref().unwrap_or_else(|e| panic!("{}: {e}", o.spec.account_id));
        assert_eq!(rec.outcome.kind, OutcomeKind::Completed, "the SAME finished record is returned untouched (Begin::AlreadyDone), not re-run");
    }

    // No new broker traffic and no balance change: the second call was a pure no-op at the exchange.
    assert_eq!(env_a.rig.handle.requests().len(), requests_after_first_a, "acct-a: zero additional exchange requests on the second call");
    assert_eq!(env_b.rig.handle.requests().len(), requests_after_first_b, "acct-b: zero additional exchange requests on the second call");
    assert_eq!(env_a.bal("BTC"), bal_btc_a_after_first, "acct-a balance unchanged by the second call");
    assert_eq!(env_b.bal("BTC"), bal_btc_b_after_first, "acct-b balance unchanged by the second call");

    // And the lock was released after each run (not left held): a third call still succeeds, proving
    // `run_all_due` does not leak the per-account lock across calls.
    let due3 = find_due_runs(&source, slot_time(0)).unwrap();
    let outcomes3 = run_all_due(due3, &runtimes, &states, &runs, &notifier, &kill, &clock, &lock, &cfg);
    assert!(outcomes3.iter().all(|o| o.result.is_ok()), "a third call must not see a stuck lock either");
}

