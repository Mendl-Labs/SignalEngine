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
//! `run_service` serves the ONE (tenant, account) pair `PilotConfig` names, enumerated from Postgres
//! (`rebalancer_store::PgAccountSource`, slice S-5: the mandate, the owner-authored pilot plan, the derived
//! execution mode -- never more than the mandate grants). At startup it ALSO connects that account's real paper
//! Alpaca broker and the real Massive data source ONCE (`rebalancer_service::runtime::connect_account_alpaca`):
//! verifies the account (`GET /v2/account`, the `PA` prefix), checks the connected key's fingerprint against the
//! pilot plan row, and refreshes the ETF assets. That `AccountRuntime` is reused for every tick; a later tick that
//! finds the account no longer eligible (mandate revoked, plan tightened to `assisted`, ...) simply has nothing due
//! to run against it -- `PgAccountSource`'s own checks run fresh on every tick via `find_due_runs`.
//!
//! # Proposal-derived plans (certified-envelopes Phase 0, `REBALANCER_SERVE_PROPOSAL_PLANS=true`)
//! With the flag on (default off: everything above is byte-for-byte the pilot), `PgAccountSource` is scoped to
//! `AccountScope::PilotAndProposals`: every account of every tenant with an approved paper `strategy_plans` row is
//! enumerated alongside the pilot pair (which becomes optional), exactly one plan source per account. Each
//! enumerated account gets its own `AccountRuntime` from its paper Alpaca key, obtained through the typed
//! `rebalancer_service::credentials` seam: the pilot pair's key from the environment as before, every other
//! account's from the tenant provider -- which today is `TenantCredentialsNotBuilt` (see that module's doc for the
//! design question: this process must not hold the tenant-credential master key). Such an account is logged and
//! gets no runtime; nothing trades for it. After every tick, each proposal-served account's run outcome is written
//! back to `strategy_plans.reconciliation_*` (`rebalancer_store::PgPlanReconciliation`), and the run rows carry
//! `plan_origin = 'proposal'` through the provenance registry.
//!
//! With the flag on the account runtimes are NOT fixed at startup: every `REBALANCER_REENUMERATE_SECS` (default 300)
//! the source is enumerated again and `rebalancer_service::fleet::Fleet::reconcile` builds a runtime for each newly
//! eligible account (credential broker fetch, connect, verify), rebuilds one whose plan was replaced and drops one
//! no longer eligible. Zero eligible accounts keeps the pod up (heartbeat log); one account's failure is logged,
//! alerted once per failure streak (`ALERT_RUN_FAILED`, `RUNTIME_UNAVAILABLE`) and retried with backoff, and never
//! stops another. With the flag off nothing is re-enumerated and startup behaves exactly as before.
//!
//! There is no in-memory or demo account source anywhere in this binary's startup path any more (the previous
//! `InMemoryAccountSource`/`demo_accounts()` wiring this replaces is gone, not merely unused): `PilotConfig::
//! from_env`'s paper-only checks (`REBALANCER_PAPER_ONLY=true` plus the pilot ids) must already pass before any of
//! the above runs, and nothing past that point reads an env var that would substitute a fake broker or a fake
//! account list -- `tests/runtime_construction.rs::rebalancer_demo_cannot_bypass_the_paper_only_startup_refusal`
//! proves the startup gate itself refuses regardless of such a variable with a real call to the same function
//! `main` calls, not just by reading the source.
//!
//! # The two-source data gate (W9.2, shadow only)
//! `DATA_GATE_MODE=shadow` puts `market_data::TwoSourceGate` between the Massive fetch and the pipeline: every sleeve
//! fetch is compared against its secondary (ETF: Alpaca daily bars on the platform DATA credentials
//! `ALPACA_DATA_KEY_ID` / `ALPACA_DATA_KEY_SECRET`, feed `ALPACA_DATA_FEED`; crypto: Kraken public OHLC), the
//! verdict and both sources' closes are recorded on the run record, and a refusal is raised as
//! `ALERT_DATA_GATE_SHADOW_REFUSE`. The primary panel is ALWAYS what the pipeline decides on. `off` (default) adds
//! nothing; `enforce` refuses to start.
//!
//! Two read-only/print-only subcommands exist for the owner to run BEFORE a pilot plan row exists:
//! `print-fingerprint` (connects read-only and prints the connected key's fingerprint, never the key) and
//! `pilot-template` (prints the `mandate_events` / `rebalancer_pilot_plans` insert template; touches no network,
//! no database).
//!
//! # Alert delivery and the dead-man's heartbeat (W6.1 / W6.2, `rebalancer-alerts`)
//! Both dark by default. With `ALERTS_ENABLED=true` a `DeliveryWorker` runs after every tick's runs, turning the
//! alerts `PgNotifier` persisted into `alert_deliveries` rows (one per verified channel, de-duplicated on
//! `(channel_id, dedupe_key)`) and sending them through Resend (`RESEND_API_KEY` + `ALERT_FROM_EMAIL`; without both
//! the sender is disabled and every delivery is marked `sender_disabled`, never dropped). With `HEARTBEAT_URL` set,
//! every tick that ran pings it and a tick that could not run pings `<url><HEARTBEAT_FAIL_SUFFIX>` (default `/fail`),
//! on a short-timeout transport so the ping can never hold the loop; a failure streak is logged and raised once as
//! `ALERT_HEARTBEAT_FAILED`. Neither can stop a run or change what a run does.

use std::collections::BTreeMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use broker_adapters::alpaca::PAPER_BASE_URL;
use broker_adapters::transport::reqwest_transport::ReqwestTransport;
use broker_adapters::transport::HttpTransport;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use market_data::{DataGateMode, EnvKeyProvider, MassiveDataSource, ENV_DATA_GATE_MODE};
use rebalancer_alerts::{alerts_enabled, sender_from_lookup, DeliveryWorker, Heartbeat, HeartbeatConfig, HeartbeatMonitor, Sender, TickOutcome, DEFAULT_BACKLOG_SECS, ENV_ALERTS_ENABLED, ENV_HEARTBEAT_URL};
use rebalancer_core::venue::VenueRuleBook;
use rebalancer_run::clock::Clock;
use rebalancer_run::data::DataSource;
use rebalancer_run::driver::{run_all_due, summarize, AccountRuntime, AccountSource, DueOutcome};
use rebalancer_run::latency::LatencyRecorder;
use rebalancer_run::pipeline::RunConfig;
use rebalancer_run::record::{Alert, AlertCode, AlertSeverity};
use rebalancer_run::stores::Notifier;
use rebalancer_service::broker_credentials::{BrokerConfig, HttpBrokerCredentials};
use rebalancer_service::credentials::{EnvPilotCredentials, PaperCredentialProvider, ServiceCredentials, TenantCredentialsNotBuilt, ENV_ALPACA_KEY_ID, ENV_ALPACA_KEY_SECRET};
use rebalancer_service::fleet::{connect_account, reenumerate_secs_from_lookup, Fleet, ReenumerateSchedule, DEFAULT_REENUMERATE_SECS};
use rebalancer_service::liveness;
use rebalancer_service::observer::ObserverConfig;
use rebalancer_service::pilot::{pilot_run_config, PilotAccountSource, PilotConfig};
use rebalancer_service::runtime::{connect_account_alpaca_for_sleeves, connect_read_only, pilot_data_source, pilot_data_source_shadow, pilot_template_sql, shadow_gate, PilotAlpaca};
use rebalancer_store::{
    reconciliation_for, AccountScope, AccountTenants, PgAccountLock, PgAccountSource, PgDeliveryLedger, PgKillFlag, PgLatencyStore, PgNotifier, PgPlanReconciliation, PgRunStore, PgStateStore,
    PilotAccount, PilotAllowList, PlanOrigin, PlanProvenanceRegistry,
};

const DEFAULT_TICK_SECS: u64 = 300;
/// The heartbeat ping's own transport budget: connect and total. Far below the tick interval, so the ping can never
/// hold the loop for long enough to miss the next tick.
const HEARTBEAT_CONNECT_TIMEOUT: StdDuration = StdDuration::from_secs(3);
const HEARTBEAT_TOTAL_TIMEOUT: StdDuration = StdDuration::from_secs(5);

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
    parse_tick_secs(std::env::var("REBALANCER_TICK_SECS").ok().as_deref())
}

/// Unset, blank, non-numeric or zero is the default (a zero sleep would spin the loop); otherwise the trimmed value.
fn parse_tick_secs(raw: Option<&str>) -> u64 {
    raw.and_then(|v| v.trim().parse::<u64>().ok()).filter(|n| *n > 0).unwrap_or(DEFAULT_TICK_SECS)
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
        Some("liveness-check") => cmd_liveness_check(args.next().as_deref()),
        Some(other) => {
            eprintln!("rebalancer-service: unknown subcommand {other:?} (expected: print-fingerprint, pilot-template, liveness-check, or no argument to run the service)");
            std::process::exit(2);
        }
        None => run_service(),
    }
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Writes the heartbeat file for the liveness probe. A failure is logged and otherwise ignored: it must never abort or
/// delay a tick (the probe then reports the loop stale, which is the correct, visible consequence).
fn beat(path: &std::path::Path) {
    if let Err(e) = liveness::write_heartbeat(path, unix_now()) {
        log(format!("liveness heartbeat write to {} failed ({e}); continuing", path.display()));
    }
}

/// `rebalancer-service liveness-check [stale_secs]`: the Kubernetes liveness probe. Exit 0 when the heartbeat file is
/// fresh, 1 otherwise. `stale_secs` defaults to max(900, 3 x REBALANCER_TICK_SECS).
fn cmd_liveness_check(arg: Option<&str>) {
    let path = liveness::liveness_path(std::env::var(liveness::ENV_LIVENESS_FILE).ok().as_deref());
    let stale = arg.and_then(|a| a.trim().parse::<u64>().ok()).filter(|n| *n > 0).unwrap_or_else(|| liveness::stale_secs_for_tick(tick_secs()));
    if let Err(e) = liveness::check_file(&path, unix_now(), stale) {
        eprintln!("rebalancer-service: liveness check failed: {e}");
        std::process::exit(1);
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
    // The paper-pilot interlock (see `rebalancer_service::pilot`): this build serves the ONE (tenant, account) pair
    // (plus approved proposal plans only with REBALANCER_SERVE_PROPOSAL_PLANS=true) and starts only with
    // REBALANCER_PAPER_ONLY=true, the pilot ids set (optional only under the flag), and without the tenant credential
    // master key in its environment. Anything else refuses to start (fail closed).
    let pilot = match PilotConfig::from_env() {
        Ok(p) => p,
        Err(refusal) => {
            eprintln!("rebalancer-service: refusing to start: {refusal}");
            std::process::exit(1);
        }
    };
    // Proposal plans need the credential broker (this process cannot decrypt tenant credentials): refuse to start
    // with the flag on and either broker setting missing. Flag off: the broker is never used and not required.
    let broker_config = match BrokerConfig::from_lookup(pilot.serve_proposal_plans, |k| std::env::var(k).ok()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("rebalancer-service: refusing to start: {e}");
            std::process::exit(1);
        }
    };
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        eprintln!("rebalancer-service: DATABASE_URL is not set; refusing to start (fail closed, matching every store's own posture)");
        std::process::exit(1);
    });
    // The latency recorder's flags (W9.1): read up front so a malformed value refuses the start, like every other
    // setting of this binary.
    let observer_cfg = match ObserverConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("rebalancer-service: {e}");
            std::process::exit(1);
        }
    };
    // The re-enumeration interval (flag on only): a malformed value refuses the start like every other setting.
    let reenumerate_secs = if pilot.serve_proposal_plans {
        match reenumerate_secs_from_lookup(|k| std::env::var(k).ok()) {
            Ok(n) => n,
            Err(e) => {
                eprintln!("rebalancer-service: refusing to start: {e}");
                std::process::exit(1);
            }
        }
    } else {
        DEFAULT_REENUMERATE_SECS
    };
    let tick_secs = tick_secs();
    log(format!("starting: tick interval {tick_secs}s; PAPER-ONLY; {}", pilot.describe()));
    log(observer_cfg.describe());

    let tenants = Arc::new(AccountTenants::new());
    // Plan provenance (origin, plan id, paper) travels from the account source to the run store and the
    // reconciliation write-back. Wired only when proposal plans are served, so the pilot-only build writes exactly
    // the run rows it writes today.
    let provenance = pilot.serve_proposal_plans.then(|| Arc::new(PlanProvenanceRegistry::new()));
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
        Ok(s) => match &provenance {
            Some(reg) => s.with_plan_provenance(reg.clone()),
            None => s,
        },
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

    // --- The real, Postgres-backed account source (slice S-5): allow-listed to exactly the pilot pair, or (flag on)
    // the pilot pair plus every approved paper proposal plan in the database.
    let allow = match &pilot.pilot {
        Some(pair) => match PilotAllowList::parse(&pair.tenant_id, &pair.account_id) {
            Ok(a) => Some(a),
            Err(e) => {
                eprintln!("rebalancer-service: bad pilot allow-list: {e}");
                std::process::exit(1);
            }
        },
        None => None,
    };
    let scope = if pilot.serve_proposal_plans {
        AccountScope::PilotAndProposals { pilot: allow }
    } else {
        // `PilotConfig::from_lookup` guarantees the pair when the flag is off.
        AccountScope::Pilot(allow.expect("the pilot pair is required when proposal plans are not served"))
    };
    // --- The latency recorder's store (W9.1). Its tables may not exist yet (the migration is applied by the owner
    // from databaseschema-internal); every call then fails with LATENCY_STORE_UNAVAILABLE, which the recorder logs
    // and alerts once. Nothing here can stop the service from starting or a run from happening.
    let latency_store = match PgLatencyStore::new(pool.clone()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("rebalancer-service: could not build PgLatencyStore: {e}");
            std::process::exit(1);
        }
    };
    let pg_source = match PgAccountSource::with_scope(pool.clone(), scope) {
        Ok(s) => match &provenance {
            Some(reg) => s.with_plan_provenance(reg.clone()),
            None => s,
        },
        Err(e) => {
            eprintln!("rebalancer-service: could not build PgAccountSource: {e}");
            std::process::exit(1);
        }
    };
    // --- R41 cells (rule x universe): the platform's registered cells. The embedded lists are EMPTY today, so no plan
    // sleeve that carries a cell reference can run until a cell is registered in code; a plan with no cell reference
    // never reaches the registry. A defect in the compiled-in sidecars fails closed (an empty registry refuses every cell).
    let pg_source = {
        let registry = match mandate_core::cell_registry::CellRegistry::platform() {
            Ok(r) => r,
            Err(e) => {
                log(format!("the R41 cell registry did not load ({e}); every cell sleeve will be refused"));
                mandate_core::cell_registry::CellRegistry::empty()
            }
        };
        log(format!("R41 cell registry: {} registered cell(s)", registry.len()));
        pg_source.with_cell_verifier(Arc::new(registry))
    };
    // --- The reconciliation write-back for proposal plans (flag on only).
    let reconciliation = if pilot.serve_proposal_plans {
        match PgPlanReconciliation::new(pool) {
            Ok(r) => Some(r),
            Err(e) => {
                eprintln!("rebalancer-service: could not build PgPlanReconciliation: {e}");
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    // --- The real broker/data runtime (slice S-6): built ONCE at startup from whatever PgAccountSource enumerates
    // right now. If no account is eligible yet (no plan, mandate not signed, ...) this refuses to start with no
    // runtime at all -- the honest, safe default -- rather than starting with an empty `runtimes` map that would
    // silently never run anything.
    log("connecting the real paper broker(s) and data source (one-time startup verification)...");
    let enumeration = match pg_source.enumerate() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("rebalancer-service: could not enumerate the accounts: {e}");
            std::process::exit(1);
        }
    };
    for excluded in &enumeration.exclusions {
        log(format!("account {} excluded at startup: {}", excluded.account_id, excluded.reason.code()));
    }
    // With proposal plans served, zero eligible accounts is a normal state (nothing approved yet): the loop below
    // re-enumerates every REBALANCER_REENUMERATE_SECS and builds a runtime when a plan appears. Flag off: unchanged.
    if enumeration.accounts.is_empty() && pilot.serve_proposal_plans {
        log(format!("no account is eligible yet; serving proposal plans, so staying up and re-enumerating every {reenumerate_secs}s"));
    }
    if enumeration.accounts.is_empty() && !pilot.serve_proposal_plans {
        eprintln!("rebalancer-service: no account is eligible to run (see the exclusions logged above, if any); refusing to start with no runtime");
        std::process::exit(1);
    }

    // --- Credentials: the pilot pair's key from the environment (required whenever a pilot pair is configured,
    // exactly as before); every other account through the tenant provider, which is not built (see
    // `rebalancer_service::credentials`).
    let pilot_credentials = match &pilot.pilot {
        Some(pair) => match EnvPilotCredentials::from_lookup(pair.clone(), |k| std::env::var(k).ok()) {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("rebalancer-service: {e}; refusing to start (fail closed)");
                std::process::exit(1);
            }
        },
        None => None,
    };
    let transport = build_transport();
    // Every non-pilot account's paper key comes from the credential broker (flag on, config validated above); with
    // the flag off no non-pilot account is ever enumerated, and the typed "not built" provider stays the answer.
    let tenant_credentials: Box<dyn PaperCredentialProvider> = match broker_config {
        Some(cfg) => Box::new(HttpBrokerCredentials::new(cfg, transport.clone())),
        None => Box::new(TenantCredentialsNotBuilt),
    };
    let credentials = ServiceCredentials::new(pilot_credentials, tenant_credentials);

    let mut connected: Vec<(PilotAccount, PilotAlpaca)> = Vec::new();
    for account in enumeration.accounts {
        let (tenant_id, account_id) = (account.active.tenant_id.clone(), account.active.account_id.clone());
        let is_pilot = account.origin == PlanOrigin::OwnerPilot;
        let keys = match credentials.paper_alpaca_keys(&tenant_id, &account_id) {
            Ok(k) => k,
            Err(e) => {
                if is_pilot {
                    eprintln!("rebalancer-service: the pilot account {account_id} has no credential: {e}; refusing to start");
                    std::process::exit(1);
                }
                log(format!("account {account_id} (tenant {tenant_id}, plan {} origin {}): NO RUNTIME -- {e}", account.plan_id, account.origin.as_str()));
                continue;
            }
        };
        match connect_account_alpaca_for_sleeves(&keys.key_id, &keys.secret, PAPER_BASE_URL, transport.clone(), account.credential_fingerprint.as_deref(), &account.active.sleeves) {
            Ok(alpaca) => {
                log(format!(
                    "connected paper Alpaca for account {account_id} (tenant {tenant_id}, plan {} origin {}): key fingerprint {}{}",
                    account.plan_id,
                    account.origin.as_str(),
                    alpaca.key_id_fingerprint(),
                    if is_pilot { " matches the pilot plan row" } else { "" }
                ));
                connected.push((account, alpaca));
            }
            Err(e) => {
                if is_pilot {
                    eprintln!("rebalancer-service: could not connect the pilot's paper Alpaca account: {e}");
                    std::process::exit(1);
                }
                log(format!("account {account_id} (tenant {tenant_id}): could not connect its paper Alpaca account: {e}; NO RUNTIME"));
            }
        }
    }
    if connected.is_empty() && !pilot.serve_proposal_plans {
        eprintln!("rebalancer-service: no account could be connected (see above); refusing to start with no runtime");
        std::process::exit(1);
    }

    let massive_key = match EnvKeyProvider::from_env() {
        Ok(k) => k,
        Err(e) => {
            eprintln!("rebalancer-service: MASSIVE_API_KEY: {e}");
            std::process::exit(1);
        }
    };
    let massive = MassiveDataSource::new(massive_key, transport.clone());

    // --- The two-source data gate (W9.2): `DATA_GATE_MODE` off (default) or shadow; enforce is refused here, at
    // startup, with the reason, so it cannot be turned on by accident in a build that only records.
    let gate_mode = match DataGateMode::from_lookup(|k| std::env::var(k).ok()) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("rebalancer-service: refusing to start: {e}");
            std::process::exit(1);
        }
    };
    let ungated = pilot_data_source(&massive);
    let shadowed = match gate_mode {
        DataGateMode::Off => {
            log(format!("{ENV_DATA_GATE_MODE}=off: no two-source data gate in the data path"));
            None
        }
        DataGateMode::Shadow => match shadow_gate(&massive, |k| std::env::var(k).ok(), transport.clone()) {
            Ok(gate) => {
                log(format!("{ENV_DATA_GATE_MODE}=shadow: two-source data gate ON in SHADOW mode ({gate:?}); verdicts are recorded and alerted, the primary panel is always used"));
                Some(pilot_data_source_shadow(&massive, gate))
            }
            Err(e) => {
                eprintln!("rebalancer-service: refusing to start: {e}");
                std::process::exit(1);
            }
        },
    };
    let data_source: &dyn DataSource = match &shadowed {
        Some(s) => s,
        None => &ungated,
    };

    // A SECOND Massive source for the latency recorder (same key, same transport): the per-tick request budget is per
    // source instance, so the observer polling seven instruments every tick can never starve the decision fetch, and
    // its unfiltered observations never touch the decision source's provenance log.
    let observer_massive = match EnvKeyProvider::from_env() {
        Ok(k) => MassiveDataSource::new(k, transport.clone()),
        Err(e) => {
            eprintln!("rebalancer-service: MASSIVE_API_KEY (observer): {e}");
            std::process::exit(1);
        }
    };
    let recorder = LatencyRecorder::new(&observer_massive, &latency_store);

    // The connected accounts live in a `Fleet` (owned, keyed by account id). Each tick borrows one broker, one rule
    // set and one venue book per account from it (built in the tick loop); with the flag on the fleet is re-enumerated
    // every `reenumerate_secs` (new plans get a runtime, ineligible accounts lose theirs), with it off it never
    // changes after this point, exactly as before.
    let mut fleet: Fleet<PilotAlpaca> = Fleet::with_connected(reenumerate_secs as i64, connected.into_iter().map(|(a, alpaca)| (a.active.account_id.clone(), a.plan_id, alpaca)));
    log(format!("{} account runtime(s) built", fleet.len()));
    let mut reenumerate_schedule = pilot.serve_proposal_plans.then(|| ReenumerateSchedule::new(reenumerate_secs, Utc::now()));

    let clock = SystemClock;
    // Paper-only: the pipeline refuses any broker that does not report a paper connection (`VenuePolicy::PaperOnly`).
    let config: RunConfig = pilot_run_config();
    // From here, every tick asks Postgres afresh who is due and eligible: a plan tightened or a mandate revoked
    // between ticks takes effect on the very next one, with no restart needed.
    let accounts = PilotAccountSource::new(pg_source, pilot.clone());

    // --- Alert delivery (W6.1): dark unless ALERTS_ENABLED=true. The ledger is a separate small pool so a stuck
    // delivery query can never starve the run stores' connections.
    let delivery = if alerts_enabled(|k| std::env::var(k).ok()) {
        let ledger = match rebalancer_store::pg::create_pool(&database_url, 2).and_then(PgDeliveryLedger::new) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("rebalancer-service: could not build PgDeliveryLedger: {e}");
                std::process::exit(1);
            }
        };
        let (sender, sender_name): (Box<dyn Sender>, &str) = sender_from_lookup(|k| std::env::var(k).ok(), build_transport());
        log(format!("{ENV_ALERTS_ENABLED}=true: alert delivery ON, sender {sender_name} (reading alerts of the last {DEFAULT_BACKLOG_SECS}s first)"));
        if sender_name == "disabled" {
            log("RESEND_API_KEY / ALERT_FROM_EMAIL not both set: deliveries will be recorded with error=sender_disabled and NOT sent");
        }
        Some(DeliveryWorker::new(ledger, sender, clock.now(), ChronoDuration::seconds(DEFAULT_BACKLOG_SECS)))
    } else {
        log(format!("{ENV_ALERTS_ENABLED} is not true: alert delivery OFF (alerts are persisted in rebalancer_alerts only)"));
        None
    };

    // --- Dead-man's heartbeat (W6.2): dark unless HEARTBEAT_URL is set. Its own short-timeout transport.
    let heartbeat_cfg = HeartbeatConfig::from_lookup(|k| std::env::var(k).ok());
    let heartbeat = match &heartbeat_cfg.url {
        Some(url) => {
            let t = match ReqwestTransport::with_timeouts(HEARTBEAT_CONNECT_TIMEOUT, HEARTBEAT_TOTAL_TIMEOUT) {
                Ok(t) => Arc::new(t) as Arc<dyn HttpTransport>,
                Err(e) => {
                    eprintln!("rebalancer-service: could not build the heartbeat transport: {e}");
                    std::process::exit(1);
                }
            };
            log(format!(
                "{ENV_HEARTBEAT_URL} set: dead-man's heartbeat ON to {} (fail suffix {:?}); set the monitor's expected period to the tick interval ({tick_secs}s) plus grace",
                rebalancer_alerts::heartbeat::redact(url),
                heartbeat_cfg.fail_suffix
            ));
            Some(Heartbeat::new(heartbeat_cfg.clone(), t))
        }
        None => {
            log(format!("{ENV_HEARTBEAT_URL} is not set: dead-man's heartbeat OFF"));
            None
        }
    };
    let heartbeat_monitor = HeartbeatMonitor::new();
    let write_back = match (&provenance, &reconciliation) {
        (Some(reg), Some(store)) => Some((reg.as_ref(), store)),
        _ => None,
    };

    let liveness_path = liveness::liveness_path(std::env::var(liveness::ENV_LIVENESS_FILE).ok().as_deref());
    let mut tick_no: u64 = 0;
    loop {
        tick_no += 1;
        beat(&liveness_path);
        // One line per tick, before any work, so a stall inside a tick is visible as "tick N: start" with no "tick N: done".
        log(format!("tick {tick_no}: start; fleet size {}", fleet.len()));
        let outcome = match kill_flag_or_heartbeat(&kill_flag) {
            Ok(true) => {
                log("kill flag is SET: skipping this tick (no run attempted)");
                TickOutcome::Ran
            }
            Ok(false) => {
                if let Some(schedule) = reenumerate_schedule.as_mut() {
                    let now = clock.now();
                    if schedule.due(now) {
                        reenumerate_fleet(accounts.inner(), &mut fleet, &credentials, &transport, &tenants, &notifier, now);
                    }
                }
                let tick_outcome = {
                    // One broker, one rule set and one venue book per runtime in the fleet, borrowed for this tick only;
                    // the vectors are filled completely before anything borrows from them.
                    let brokers: Vec<_> = fleet.iter().map(|(_, a)| a.broker()).collect();
                    let rules: Vec<_> = fleet.iter().map(|(_, a)| a.rules()).collect();
                    let venue_books: Vec<VenueRuleBook<'_>> = rules.iter().map(|r| VenueRuleBook::new().with("alpaca", r)).collect();
                    let mut runtimes: BTreeMap<String, AccountRuntime<'_>> = BTreeMap::new();
                    for (i, (id, _)) in fleet.iter().enumerate() {
                        runtimes.insert(id.clone(), AccountRuntime { broker: &brokers[i], data: data_source, venue_rules: &venue_books[i] });
                    }
                    run_tick(&accounts, &tenants, &state_store, &run_store, &notifier, &kill_flag, &clock, &lock, &config, &runtimes, write_back)
                };
                // After the runs (and their data fetch), the observer; never before, never instead.
                observe_tick(&observer_cfg, &recorder, &accounts, &notifier, &clock);
                tick_outcome
            }
            Err(e) => {
                log(format!("kill flag unreadable ({e}): failing closed, skipping this tick"));
                TickOutcome::Failed
            }
        };
        // After the runs, never before them, and never able to change them: deliver what the runs raised ...
        if let Some(w) = &delivery {
            let report = w.run(clock.now());
            log(format!("alert delivery: {}", report.summary()));
        }
        // ... then tell the outside world this loop is alive (or that it ran and could not do its job).
        if let Some(hb) = &heartbeat {
            let result = hb.ping(outcome);
            log(result.to_string());
            match heartbeat_monitor.observe_and_notify(&result, clock.now(), &notifier) {
                Some(Ok(())) => log("ALERT_HEARTBEAT_FAILED raised (once per failure streak)"),
                Some(Err(e)) => log(format!("ALERT_HEARTBEAT_FAILED could not be persisted either: {e}")),
                None => {}
            }
        }
        log(format!("tick {tick_no}: done ({outcome:?}); fleet size {}; next tick in {tick_secs}s", fleet.len()));
        beat(&liveness_path);
        clock.sleep_secs(tick_secs);
    }
}

/// One re-enumeration cycle (proposal plans served): enumerate the eligible accounts afresh and reconcile the fleet.
/// A failed enumeration keeps the current runtimes; one account's failed credential fetch or connect is logged,
/// alerted once per failure streak and retried with backoff, and never stops another account.
fn reenumerate_fleet(
    source: &PgAccountSource,
    fleet: &mut Fleet<PilotAlpaca>,
    credentials: &ServiceCredentials,
    transport: &Arc<dyn HttpTransport>,
    tenants: &Arc<AccountTenants>,
    notifier: &PgNotifier,
    now: DateTime<Utc>,
) {
    let enumeration = match source.enumerate() {
        Ok(e) => e,
        Err(e) => {
            log(format!("re-enumeration failed: could not enumerate the accounts: {e}; keeping the {} current runtime(s)", fleet.len()));
            return;
        }
    };
    for excluded in &enumeration.exclusions {
        log(format!("account {} excluded: {}", excluded.account_id, excluded.reason.code()));
    }
    let report = fleet.reconcile(&enumeration.accounts, now, |account| {
        connect_account(account, credentials, |keys, acct| {
            let alpaca = connect_account_alpaca_for_sleeves(&keys.key_id, &keys.secret, PAPER_BASE_URL, transport.clone(), acct.credential_fingerprint.as_deref(), &acct.active.sleeves)?;
            log(format!(
                "connected paper Alpaca for account {} (tenant {}, plan {} origin {}): key fingerprint {}",
                acct.active.account_id,
                acct.active.tenant_id,
                acct.plan_id,
                acct.origin.as_str(),
                alpaca.key_id_fingerprint()
            ));
            Ok(alpaca)
        })
    });
    for id in &report.dropped {
        log(format!("account {id} is no longer eligible: runtime dropped"));
    }
    for f in &report.failed {
        log(format!(
            "account {} (tenant {}, plan {} origin {}): NO RUNTIME -- {} (failure #{}, next attempt after {})",
            f.account_id,
            f.tenant_id,
            f.plan_id,
            f.origin.as_str(),
            f.failure,
            f.consecutive,
            f.retry_at.to_rfc3339()
        ));
        if f.first_in_streak {
            tenants.register(&f.account_id, uuid_or_nil(&f.tenant_id));
            let alert = Alert {
                code: AlertCode::RunFailed,
                severity: AlertSeverity::Warning,
                account_id: f.account_id.clone(),
                run_key: format!("runtime|{}", now.to_rfc3339()),
                message: format!("RUNTIME_UNAVAILABLE: no runtime for plan {} ({}); nothing will trade for this account until it connects; retrying with backoff", f.plan_id, f.failure.code),
                dedupe_key: format!("runtime-unavailable|{}|{}", f.account_id, f.failure.code),
                at: now,
            };
            if let Err(e) = notifier.notify(&alert) {
                log(format!("account {}: the runtime-unavailable alert could not be persisted either: {e}", f.account_id));
            }
        }
    }
    log(report.summary_line(fleet.len()));
    if fleet.is_empty() {
        log("heartbeat: no account runtime yet; serving proposal plans, waiting for an eligible account");
    }
}

fn kill_flag_or_heartbeat(kill_flag: &PgKillFlag) -> Result<bool, String> {
    use rebalancer_run::stores::KillFlag;
    kill_flag.is_set()
}

/// One tick's runs. Returns whether the tick RAN (the due scan happened and every due account was attempted, whatever
/// each run's outcome: those alert on their own) or could not (the due scan failed), which is all the dead-man's
/// switch is told. With `write_back` set (proposal plans served), every finished run of a proposal-served account
/// is written to its plan's `reconciliation_*` columns after the runs.
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
    write_back: Option<(&PlanProvenanceRegistry, &PgPlanReconciliation)>,
) -> TickOutcome {
    let now = clock.now();
    let due = match rebalancer_run::driver::find_due_runs(accounts, now) {
        Ok(d) => d,
        Err(e) => {
            log(format!("find_due_runs failed: {e}; heartbeat only, no run attempted"));
            return TickOutcome::Failed;
        }
    };
    log(format!("tick: {} account(s) due, {} runtime(s) in the fleet", due.len(), runtimes.len()));
    if due.is_empty() {
        log("heartbeat: 0 accounts due");
        return TickOutcome::Ran;
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
    if let Some((registry, store)) = write_back {
        write_back_reconciliation(&outcomes, registry, store, clock.now());
    }
    TickOutcome::Ran
}

/// `strategy_plans.reconciliation_*` for every finished run of a proposal-served account this tick. A run that was
/// not attempted (lock busy, no runtime, a panic) says nothing about the account's state and writes nothing; a
/// pilot-plan run has no such columns. A failed write is logged, never fatal: the run record itself is already
/// persisted and is the source of truth.
fn write_back_reconciliation(outcomes: &[DueOutcome], registry: &PlanProvenanceRegistry, store: &PgPlanReconciliation, now: DateTime<Utc>) {
    for o in outcomes {
        let Ok(record) = &o.result else { continue };
        let Some(provenance) = registry.get(&o.spec.account_id) else { continue };
        if provenance.origin != PlanOrigin::Proposal {
            continue;
        }
        let tenant_id = uuid_or_nil(&o.spec.tenant_id);
        if tenant_id.is_nil() {
            log(format!("  {}: plan {} reconciliation NOT written: tenant id {:?} is not a UUID", o.spec.account_id, provenance.plan_id, o.spec.tenant_id));
            continue;
        }
        let (status, detail) = reconciliation_for(record);
        match store.record(tenant_id, provenance.plan_id, status, &detail, now) {
            Ok(()) => log(format!("  {}: plan {} reconciliation_status = {}", o.spec.account_id, provenance.plan_id, status.as_str())),
            Err(e) => log(format!("  {}: plan {} reconciliation NOT written: {e}", o.spec.account_id, provenance.plan_id)),
        }
    }
}

/// The first-seen-latency / revision recorder's observation for this tick (W9.1): every tick, whether or not an
/// account was due (the bars appear around 20:00Z for ETFs and 00:00Z for crypto, inside and outside the run slots).
/// Wrapped in `catch_unwind` like `run_all_due` wraps `run_once`: an observer bug can be logged, never abort the tick
/// loop. It runs after `run_tick`, so it can never delay a run either.
fn observe_tick(cfg: &ObserverConfig, recorder: &LatencyRecorder<'_>, accounts: &dyn AccountSource, notifier: &PgNotifier, clock: &SystemClock) {
    if !cfg.enabled {
        return;
    }
    let sleeves: Vec<rebalancer_run::data::SleeveSpec> = match accounts.active_accounts() {
        Ok(active) => active.into_iter().flat_map(|a| a.sleeves).collect(),
        Err(e) => {
            log(format!("latency recorder: active_accounts failed ({e}); observing the baseline kinds only this tick"));
            Vec::new()
        }
    };
    let instruments = cfg.instruments(sleeves.iter());
    let now = clock.now();
    match panic::catch_unwind(AssertUnwindSafe(|| recorder.observe(&instruments, now, notifier))) {
        Ok(report) => log(report.summary_line()),
        Err(payload) => {
            let msg = payload.downcast_ref::<&str>().map(|s| (*s).to_string()).or_else(|| payload.downcast_ref::<String>().cloned()).unwrap_or_else(|| "non-string panic payload".into());
            log(format!("latency recorder PANICKED (observer only, no run affected): {msg}"));
        }
    }
}

fn uuid_or_nil(s: &str) -> uuid::Uuid {
    s.parse().unwrap_or(uuid::Uuid::nil())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_secs_defaults_on_unset_blank_garbage_or_zero() {
        for raw in [None, Some(""), Some("   "), Some("abc"), Some("-5"), Some("0"), Some("1.5")] {
            assert_eq!(parse_tick_secs(raw), 300, "{raw:?}");
        }
        assert_eq!(parse_tick_secs(Some("60")), 60);
        assert_eq!(parse_tick_secs(Some(" 45 ")), 45);
    }
}
