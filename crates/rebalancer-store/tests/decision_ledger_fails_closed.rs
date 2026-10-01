//! `PgRunStore` must FAIL CLOSED when asked which decision an account last acted on, whenever it cannot answer
//! TRUSTWORTHILY: never `Ok(None)` ("nothing acted yet": the ETF sleeve would be planned on every run) and never a
//! date ("everything acted": it would never be planned).
//!
//! The paper-pilot decision ledger now exists (`crates/rebalancer-store/src/ledger.rs`,
//! `databaseschema-internal/migrations/2026-09-27-000000_create_rebalancer_pilot_ledger`); its round-trip,
//! monotonicity, restart-survival, missing-table and cross-tenant behaviour are exercised against a real database in
//! `tests/pilot_ledger_db.rs` (env-gated: `REBALANCER_PILOT_TEST_DB`). This file keeps the one fail-closed case that
//! needs NO database at all and is a permanent property, not a stopgap: an account whose tenant was never registered
//! in [`AccountTenants`] (the driver never populated it, or a caller passes an account id the service does not know)
//! is refused before any query is even attempted -- the connection pool here is lazy and unreachable, so a query
//! attempt would surface as `Unavailable`, a DIFFERENT error; this test proves the tenant check comes first.
//! The pipeline's handling of `DecisionLedgerUnavailable` (a distinct RunCode, no orders, crypto-only accounts
//! unaffected) is tested in `rebalancer-run/tests/etf_pending_decision.rs` against `testkit::NoLedgerRunStore`.

use std::sync::Arc;

use rebalancer_run::stores::{RunStore, RunStoreError};
use rebalancer_store::{AccountTenants, PgRunStore};

#[test]
fn the_postgres_run_store_fails_closed_for_an_account_with_no_registered_tenant() {
    // Nothing listens here; a connection attempt would fail with `Unavailable`, which is a DIFFERENT error, so the
    // assertion below also proves the method never touched the database: the tenant-scoping check runs first.
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
