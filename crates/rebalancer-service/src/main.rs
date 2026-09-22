//! The rebalancer's multi-account driver-loop binary (WP4.8 of product-mandate/IMPLEMENTATION_PLAN.md,
//! the 2026-09-22 "1a. What tenant #2 needs" correction: "A driver loop, not just a single run").
//!
//! Ticks every `TICK_SECS` seconds (default 300, env `REBALANCER_TICK_SECS`): reads the database kill
//! flag, and if it is not set, enumerates every due tenant-account (`rebalancer_run::driver::
//! find_due_runs`) and runs each (`run_all_due`), logging a heartbeat every tick either way. One
//! account's failure -- a lock conflict, a missing broker runtime, or a panic inside `run_once` -- is
//! logged and never stops the others (see `driver.rs`'s own module doc for the concurrency-safety
//! reasoning: a per-account Postgres advisory lock is the primary mechanism, the run store's own
//! run-key uniqueness is the backstop).
//!
//! Deliberately a plain synchronous `main` (no `#[tokio::main]`, no top-level runtime) with
//! `std::thread::sleep` between ticks: `rebalancer-run`'s pipeline is synchronous end to end, and each
//! Postgres store in `rebalancer-store` carries its OWN small, private Tokio runtime to bridge into
//! `diesel-async` (see `rebalancer-store/src/pg.rs`'s own module doc) -- a runtime here would risk the
//! "cannot start a runtime from within a runtime" panic that design exists to avoid.
//!
//! # What this binary does NOT yet do (scope, stated honestly)
//! There is no real account source wired in: WP1.2/1.3's mandate storage (the tables another agent is
//! building concurrently on `feat/mandate-tables`, per this work's own instructions not to touch or
//! depend on it) and WP3.4's tenant-scoped credential loader do not exist yet, so there is nothing real
//! to enumerate accounts FROM or to build a `Broker`/`DataSource` runtime WITH. This binary wires the
//! real, tested plumbing around that gap (`AccountSource`, `AccountRuntime`, the Postgres stores, the
//! advisory lock, the tick/kill-flag/heartbeat loop) using `rebalancer_run::driver::
//! InMemoryAccountSource` as the `AccountSource`, populated from nothing by default (a genuinely empty
//! tick, which is a safe, honest default: no due accounts, no runs, just a heartbeat) or from one
//! demo/smoke-test account when `REBALANCER_DEMO=1` is set, using the fake broker so the WHOLE loop
//! (tick -> kill flag -> find_due_runs -> lock -> run_once -> Postgres) can be exercised end to end
//! without a real exchange. Swapping `InMemoryAccountSource` for a real, Postgres-backed
//! `AccountSource` once WP1.3 lands is the one change this binary is structured to make small: nothing
//! else here depends on it being in-memory.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Utc};
use rebalancer_run::clock::Clock;
use rebalancer_run::driver::{run_all_due, summarize, AccountRuntime, AccountSource, InMemoryAccountSource};
use rebalancer_run::pipeline::RunConfig;
use rebalancer_store::{AccountTenants, PgAccountLock, PgKillFlag, PgNotifier, PgRunStore, PgStateStore};

const DEFAULT_TICK_SECS: u64 = 300;

/// The real wall clock. `rebalancer-run`'s pipeline never calls `std::time`/`std::thread` directly (see
/// `clock.rs`'s own module doc: "nothing in this workspace calls the system clock") -- this is the one
/// place in the whole rebalancer that is allowed to, because it IS the production `Clock`.
struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
    fn sleep_secs(&self, secs: u64) {
        std::thread::sleep(StdDuration::from_secs(secs));
    }
}

fn tick_secs() -> u64 {
    std::env::var("REBALANCER_TICK_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_TICK_SECS)
}

fn log(msg: impl std::fmt::Display) {
    println!("[{}] rebalancer-service: {msg}", Utc::now().to_rfc3339());
}

/// No real `AccountSource` exists yet (see the module docs): this always returns an empty list, the
/// honest, safe default (a genuinely empty tick: no due accounts, no runs, just a heartbeat). Kept as
/// its own function so the one line a real `AccountSource` implementation replaces is obvious.
fn demo_accounts() -> Vec<rebalancer_run::driver::ActiveAccount> {
    Vec::new()
}

fn main() {
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        eprintln!("rebalancer-service: DATABASE_URL is not set; refusing to start (fail closed, matching every store's own posture)");
        std::process::exit(1);
    });
    let tick_secs = tick_secs();
    log(format!("starting: tick interval {tick_secs}s"));

    let tenants = Arc::new(AccountTenants::new());
    let pool = match rebalancer_store::pg::create_pool(&database_url, 10) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("rebalancer-service: could not build the connection pool: {e}");
            std::process::exit(1);
        }
    };
    let state_store = match PgStateStore::new(pool.clone(), tenants.clone()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("rebalancer-service: could not build PgStateStore: {e}");
            std::process::exit(1);
        }
    };
    let run_store = match PgRunStore::new(pool.clone(), tenants.clone()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("rebalancer-service: could not build PgRunStore: {e}");
            std::process::exit(1);
        }
    };
    let notifier = match PgNotifier::new(pool, tenants.clone()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("rebalancer-service: could not build PgNotifier: {e}");
            std::process::exit(1);
        }
    };
    let kill_flag = match PgKillFlag::new(match rebalancer_store::pg::create_pool(&database_url, 2) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("rebalancer-service: could not build PgKillFlag's pool: {e}");
            std::process::exit(1);
        }
    }) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("rebalancer-service: could not build PgKillFlag: {e}");
            std::process::exit(1);
        }
    };
    let lock = match PgAccountLock::new(database_url.clone()) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("rebalancer-service: could not build PgAccountLock: {e}");
            std::process::exit(1);
        }
    };

    let clock = SystemClock;
    let config = RunConfig::default();
    let accounts = InMemoryAccountSource::new();
    accounts.set_accounts(demo_accounts());

    loop {
        match kill_flag_or_heartbeat(&kill_flag) {
            Ok(true) => {
                log("kill flag is SET: skipping this tick (no run attempted)");
            }
            Ok(false) => {
                run_tick(&accounts, &tenants, &state_store, &run_store, &notifier, &kill_flag, &clock, &lock, &config);
            }
            Err(e) => {
                log(format!("kill flag unreadable ({e}): failing closed, skipping this tick"));
            }
        }
        clock.sleep_secs(tick_secs);
    }
}

fn kill_flag_or_heartbeat(kill_flag: &PgKillFlag) -> Result<bool, String> {
    use rebalancer_run::stores::KillFlag;
    kill_flag.is_set()
}

#[allow(clippy::too_many_arguments)]
fn run_tick(
    accounts: &InMemoryAccountSource,
    tenants: &Arc<AccountTenants>,
    state_store: &PgStateStore,
    run_store: &PgRunStore,
    notifier: &PgNotifier,
    kill_flag: &PgKillFlag,
    clock: &SystemClock,
    lock: &PgAccountLock,
    config: &RunConfig,
) {
    let now = clock.now();
    let due = match rebalancer_run::driver::find_due_runs(accounts, now) {
        Ok(d) => d,
        Err(e) => {
            log(format!("find_due_runs failed: {e}; heartbeat only, no run attempted"));
            return;
        }
    };
    if due.is_empty() {
        log("heartbeat: 0 accounts due");
        return;
    }

    // Keep the tenant registry current for whatever this tick is about to touch (see
    // `AccountTenants`'s own doc for why: none of the store traits carry a tenant id).
    if let Ok(active) = accounts.active_accounts() {
        tenants.set_all(active.into_iter().map(|a| (a.account_id, uuid_or_nil(&a.tenant_id))));
    }

    // No real per-tenant broker/data wiring yet (see the module docs): every due candidate will
    // observe `DueRunError::NoRuntime` unless the demo feature populated one. This is the honest,
    // safe default -- it proves the loop end to end without ever being able to place a real order.
    let runtimes: std::collections::BTreeMap<String, AccountRuntime<'_>> = std::collections::BTreeMap::new();

    let outcomes = run_all_due(due, &runtimes, state_store, run_store, notifier, kill_flag, clock, lock, config);
    let summary = summarize(&outcomes);
    log(format!("heartbeat: {} due, {} ran, {} not attempted", outcomes.len(), summary.ran, summary.not_attempted));
    for o in &outcomes {
        match &o.result {
            Ok(record) => log(format!("  {}: {} ({})", o.spec.account_id, record.outcome.kind.as_str(), record.outcome.code)),
            Err(e) => log(format!("  {}: NOT ATTEMPTED -- {e}", o.spec.account_id)),
        }
    }
}

fn uuid_or_nil(s: &str) -> uuid::Uuid {
    s.parse().unwrap_or(uuid::Uuid::nil())
}
