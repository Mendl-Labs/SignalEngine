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
//! # The real account source and runtime (slice S-6, `PAPER_PILOT_DRAGONSTONE_PLAN.md`)
//! `run_service` serves exactly the ONE (tenant, account) pair `PilotConfig` names, enumerated from Postgres
//! (`rebalancer_store::PgAccountSource`, slice S-5: the mandate, the owner-authored pilot plan, the derived
//! execution mode -- never more than the mandate grants). At startup it ALSO connects that account's real paper
//! Alpaca broker and the real Massive data source ONCE (`rebalancer_service::runtime::connect_paper_alpaca`):
//! verifies the account (`GET /v2/account`, the `PA` prefix), checks the connected key's fingerprint against the
//! pilot plan row, and refreshes the five ETF assets. That one `AccountRuntime` is reused for every tick; a later
//! tick that finds the account no longer eligible (mandate revoked, plan tightened to `assisted`, ...) simply has
//! nothing due to run against it -- `PgAccountSource`'s own checks run fresh on every tick via `find_due_runs`.
//!
//! There is no in-memory or demo account source anywhere in this binary's startup path any more (the previous
//! `InMemoryAccountSource`/`demo_accounts()` wiring this replaces is gone, not merely unused): `PilotConfig::
//! from_env`'s paper-only checks (`REBALANCER_PAPER_ONLY=true` plus both pilot ids) must already pass before any of
//! the above runs, and nothing past that point reads an env var that would substitute a fake broker or a fake
//! account list -- `tests/runtime_paper_only.rs` proves the startup gate itself refuses regardless of such a
//! variable with a real call to the same function `main` calls, not just by reading the source.
//!
//! Two read-only/print-only subcommands exist for the owner to run BEFORE a pilot plan row exists:
//! `print-fingerprint` (connects read-only and prints the connected key's fingerprint, never the key) and
//! `pilot-template` (prints the `mandate_events` / `rebalancer_pilot_plans` insert template; touches no network,
//! no database).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use broker_adapters::alpaca::PAPER_BASE_URL;
use broker_adapters::transport::reqwest_transport::ReqwestTransport;
use broker_adapters::transport::HttpTransport;
use chrono::{DateTime, Utc};
use market_data::{EnvKeyProvider, MassiveDataSource};
use rebalancer_core::venue::VenueRuleBook;
use rebalancer_run::clock::Clock;
use rebalancer_run::driver::{run_all_due, summarize, AccountRuntime, AccountSource};
use rebalancer_run::pipeline::RunConfig;
use rebalancer_service::pilot::{pilot_run_config, PilotAccountSource, PilotConfig};
use rebalancer_service::runtime::{connect_paper_alpaca, connect_read_only, pilot_data_source, pilot_template_sql};
use rebalancer_store::{AccountTenants, PgAccountLock, PgAccountSource, PgKillFlag, PgNotifier, PgRunStore, PgStateStore, PilotAllowList};

const DEFAULT_TICK_SECS: u64 = 300;
/// Read by both the service and `print-fingerprint`, never logged and never read from a secrets file.
const ENV_ALPACA_KEY_ID: &str = "PILOT_ALPACA_KEY_ID";
const ENV_ALPACA_KEY_SECRET: &str = "PILOT_ALPACA_KEY_SECRET";

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

fn required_env(name: &str) -> String {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!("rebalancer-service: {name} is not set; refusing to start (fail closed)");
            std::process::exit(1);
        }
    }
}

fn build_transport() -> Arc<dyn HttpTransport> {
    match ReqwestTransport::new() {
        Ok(t) => Arc::new(t),
        Err(e) => {
            eprintln!("rebalancer-service: could not build the HTTP transport: {e}");
            std::process::exit(1);
        }
    }
}

fn main() {
    let mut args = std::env::args();
    let _bin = args.next();
    match args.next().as_deref() {
        Some("print-fingerprint") => cmd_print_fingerprint(),
        Some("pilot-template") => cmd_pilot_template(),
        Some(other) => {
            eprintln!("rebalancer-service: unknown subcommand {other:?} (expected: print-fingerprint, pilot-template, or no argument to run the service)");
            std::process::exit(2);
        }
        None => run_service(),
    }
}

/// Connects the pilot's paper Alpaca account read-only and prints its key fingerprint (never the key), so the owner
/// can paste it into the `rebalancer_pilot_plans` row's `credential_fingerprint` column before that row exists.
fn cmd_print_fingerprint() {
    let key_id = required_env(ENV_ALPACA_KEY_ID);
    let secret = required_env(ENV_ALPACA_KEY_SECRET);
    let transport = build_transport();
    match connect_read_only(&key_id, &secret, PAPER_BASE_URL, transport) {
        Ok(fingerprint) => {
            println!("credential_fingerprint: {fingerprint}");
            println!("(paste this into the rebalancer_pilot_plans row's credential_fingerprint column -- this is a fingerprint, never the key itself)");
        }
        Err(e) => {
            eprintln!("rebalancer-service print-fingerprint: {e}");
            std::process::exit(1);
        }
    }
}

/// Prints the `mandate_events` / `rebalancer_pilot_plans` authoring template. No network call, no database
/// connection: this subcommand only formats and prints a string.
fn cmd_pilot_template() {
    print!("{}", pilot_template_sql());
}

fn run_service() {
    // The paper-pilot interlock (see `rebalancer_service::pilot`): this build serves ONE (tenant, account) pair and
    // starts only with REBALANCER_PAPER_ONLY=true, PILOT_TENANT_ID and PILOT_ACCOUNT_ID set, and without the tenant
    // credential master key in its environment. Anything else refuses to start (fail closed).
    let pilot = match PilotConfig::from_env() {
        Ok(p) => p,
        Err(refusal) => {
            eprintln!("rebalancer-service: refusing to start: {refusal}");
            std::process::exit(1);
        }
    };
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        eprintln!("rebalancer-service: DATABASE_URL is not set; refusing to start (fail closed, matching every store's own posture)");
        std::process::exit(1);
    });
    let tick_secs = tick_secs();
    log(format!("starting: tick interval {tick_secs}s; PAPER-ONLY pilot for tenant {} account {}", pilot.tenant_id, pilot.account_id));

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
    let notifier = match PgNotifier::new(pool.clone(), tenants.clone()) {
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

    // --- The real, Postgres-backed account source (slice S-5): allow-listed to exactly the pilot pair.
    let allow = match PilotAllowList::parse(&pilot.tenant_id, &pilot.account_id) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("rebalancer-service: bad pilot allow-list: {e}");
            std::process::exit(1);
        }
    };
    let pg_source = match PgAccountSource::new(pool, allow) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("rebalancer-service: could not build PgAccountSource: {e}");
            std::process::exit(1);
        }
    };

    // --- The real broker/data runtime (slice S-6): built ONCE at startup from whatever PgAccountSource enumerates
    // right now for the pilot account. If it is not eligible yet (no plan, mandate not signed, ...) this refuses to
    // start with no runtime at all -- the honest, safe default -- rather than starting with an empty `runtimes` map
    // that would silently never run anything.
    log("connecting the pilot's real paper broker and data source (one-time startup verification)...");
    let enumeration = match pg_source.enumerate() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("rebalancer-service: could not enumerate the pilot account: {e}");
            std::process::exit(1);
        }
    };
    for excluded in &enumeration.exclusions {
        log(format!("pilot account {} excluded at startup: {}", excluded.account_id, excluded.reason.code()));
    }
    let Some(pilot_account) = enumeration.accounts.first().cloned() else {
        eprintln!("rebalancer-service: the pilot account is not eligible to run (see the exclusion logged above, if any); refusing to start with no runtime");
        std::process::exit(1);
    };

    let alpaca_key_id = required_env(ENV_ALPACA_KEY_ID);
    let alpaca_secret = required_env(ENV_ALPACA_KEY_SECRET);
    let transport = build_transport();
    let pilot_alpaca = match connect_paper_alpaca(&alpaca_key_id, &alpaca_secret, PAPER_BASE_URL, transport.clone(), &pilot_account.credential_fingerprint) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("rebalancer-service: could not connect the pilot's paper Alpaca account: {e}");
            std::process::exit(1);
        }
    };
    log(format!("connected paper Alpaca: key fingerprint {} matches the pilot plan row", pilot_alpaca.key_id_fingerprint()));

    let massive_key = match EnvKeyProvider::from_env() {
        Ok(k) => k,
        Err(e) => {
            eprintln!("rebalancer-service: MASSIVE_API_KEY: {e}");
            std::process::exit(1);
        }
    };
    let massive = MassiveDataSource::new(massive_key, transport);
    let data_source = pilot_data_source(&massive);

    let broker = pilot_alpaca.broker();
    let rules = pilot_alpaca.rules();
    let venue_rules = VenueRuleBook::new().with("alpaca", &rules);

    let mut runtimes: BTreeMap<String, AccountRuntime<'_>> = BTreeMap::new();
    runtimes.insert(pilot_account.active.account_id.clone(), AccountRuntime { broker: &broker, data: &data_source, venue_rules: &venue_rules });

    let clock = SystemClock;
    // Paper-only: the pipeline refuses any broker that does not report a paper connection (`VenuePolicy::PaperOnly`).
    let config: RunConfig = pilot_run_config();
    // From here, every tick asks Postgres afresh who is due and eligible: a plan tightened or a mandate revoked
    // between ticks takes effect on the very next one, with no restart needed.
    let accounts = PilotAccountSource::new(pg_source, pilot);

    loop {
        match kill_flag_or_heartbeat(&kill_flag) {
            Ok(true) => {
                log("kill flag is SET: skipping this tick (no run attempted)");
            }
            Ok(false) => {
                run_tick(&accounts, &tenants, &state_store, &run_store, &notifier, &kill_flag, &clock, &lock, &config, &runtimes);
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
    accounts: &dyn AccountSource,
    tenants: &Arc<AccountTenants>,
    state_store: &PgStateStore,
    run_store: &PgRunStore,
    notifier: &PgNotifier,
    kill_flag: &PgKillFlag,
    clock: &SystemClock,
    lock: &PgAccountLock,
    config: &RunConfig,
    runtimes: &BTreeMap<String, AccountRuntime<'_>>,
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

    let outcomes = run_all_due(due, runtimes, state_store, run_store, notifier, kill_flag, clock, lock, config);
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
