//! The Postgres run store has no structured decision ledger until work item W6 (a migration the owner applies), so
//! it must FAIL CLOSED when asked which decision an account last acted on: `DecisionLedgerUnavailable`, never
//! `Ok(None)` ("nothing acted yet": the ETF sleeve would be planned on every run) and never a date ("everything
//! acted": it would never be planned). Needs no database: the connection pool is lazy and this call never uses it.
//! The pipeline's handling of that error (a distinct RunCode, no orders, crypto-only accounts unaffected) is tested
//! in `rebalancer-run/tests/etf_pending_decision.rs` against `testkit::NoLedgerRunStore`, which mirrors this store.

use std::sync::Arc;

use rebalancer_run::stores::{RunStore, RunStoreError};
use rebalancer_store::{AccountTenants, PgRunStore};

#[test]
fn the_postgres_run_store_fails_closed_when_asked_for_the_last_acted_decision() {
    // Nothing listens here; a connection attempt would fail with `Unavailable`, which is a DIFFERENT error, so the
    // assertion below also proves the method never touched the database.
    let pool = rebalancer_store::pg::create_pool("postgres://nobody:nothing@127.0.0.1:1/none", 1).expect("a lazy pool builds without connecting");
    let store = PgRunStore::new(pool, Arc::new(AccountTenants::new())).expect("store builds");

    for sleeve in ["etf", "crypto", "anything"] {
        let got = store.last_acted_decision("acct-1", sleeve);
        match got {
            Err(RunStoreError::DecisionLedgerUnavailable(msg)) => {
                assert!(msg.contains("acct-1") && msg.contains(sleeve), "the message names the account and sleeve: {msg}");
            }
            other => panic!("must fail closed with DecisionLedgerUnavailable, got {other:?}"),
        }
    }
    assert_eq!(RunStoreError::DecisionLedgerUnavailable(String::new()).code(), "RUNSTORE_DECISION_LEDGER_UNAVAILABLE");
}
