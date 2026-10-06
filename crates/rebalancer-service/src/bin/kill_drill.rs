//! `kill-drill`: the repeatable paper kill drill (W7.6). The logic lives in `rebalancer_service::kill_drill`; this file is
//! the wiring to the real Alpaca PAPER account and the platform's alert sender.
//!
//! Usage (the owner runs this; nothing in this repository runs it):
//!
//! ```text
//! ALPACA_PAPER_API_KEY=... ALPACA_PAPER_API_SECRET=... KILL_DRILL_ALERT_TO=... RESEND_API_KEY=... ALERT_FROM_EMAIL=... \
//!   cargo run -p rebalancer-service --bin kill-drill -- --i-understand-this-places-paper-orders
//! cargo run -p rebalancer-service --bin kill-drill -- --dry-run --i-understand-this-places-paper-orders
//! ```
//!
//! Exit codes: 0 every step verified; 1 a step failed (named in the report); 2 refused before any order; 3 could not
//! connect to or verify the paper account. Credential values are never printed.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use broker_adapters::alpaca::PAPER_BASE_URL;
use broker_adapters::transport::reqwest_transport::ReqwestTransport;
use broker_adapters::transport::HttpTransport;
use chrono::{DateTime, Utc};
use rebalancer_alerts::sender_from_lookup;
use rebalancer_run::clock::Clock;
use rebalancer_service::kill_drill::{self, DrillDeps, ENV_ALERT_TO, ENV_KEY, ENV_SECRET};
use rebalancer_service::runtime::{connect_paper_alpaca, connect_read_only};

/// The real wall clock. The pipeline never reads the system clock itself; this binary is the one place that does.
struct WallClock;

impl Clock for WallClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn sleep_secs(&self, secs: u64) {
        std::thread::sleep(StdDuration::from_secs(secs));
    }
}

fn main() {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let args = match kill_drill::parse_args(&raw) {
        Ok(a) => a,
        Err(refusal) => {
            eprintln!("kill-drill: refused: {refusal}");
            std::process::exit(2);
        }
    };
    if args.dry_run {
        print!("{}", kill_drill::render_plan(&args));
        std::process::exit(0);
    }

    let key = non_empty_env(ENV_KEY);
    let secret = non_empty_env(ENV_SECRET);
    let (Some(key), Some(secret)) = (key, secret) else {
        eprintln!("kill-drill: refused: {ENV_KEY} and {ENV_SECRET} must both be set in the environment (their values are never printed)");
        std::process::exit(2);
    };

    let transport: Arc<dyn HttpTransport> = match ReqwestTransport::new() {
        Ok(t) => Arc::new(t),
        Err(e) => {
            eprintln!("kill-drill: could not build the HTTP transport: {e}");
            std::process::exit(3);
        }
    };
    // Verify the account first (GET /v2/account and the paper account-number prefix), then connect with the fingerprint
    // the connection itself computed: no plan row is needed for a drill, and the key is checked against itself.
    let fingerprint = match connect_read_only(&key, &secret, PAPER_BASE_URL, transport.clone()) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("kill-drill: could not verify the paper account: {e}");
            std::process::exit(3);
        }
    };
    let pilot = match connect_paper_alpaca(
        &key,
        &secret,
        PAPER_BASE_URL,
        transport.clone(),
        &fingerprint,
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("kill-drill: could not connect the paper broker: {e}");
            std::process::exit(3);
        }
    };
    let broker = pilot.broker();
    let rules = pilot.rules();

    let (sender, sender_name) = sender_from_lookup(|k| std::env::var(k).ok(), transport);
    let alert_to = non_empty_env(ENV_ALERT_TO);
    println!(
        "kill-drill: paper account verified (key fingerprint {fingerprint}); alert sender: {sender_name}; alert recipient {}",
        if alert_to.is_some() { "configured" } else { "NOT configured" }
    );

    let clock = WallClock;
    let deps = DrillDeps {
        base_url: PAPER_BASE_URL,
        broker: &broker,
        rules: &rules,
        clock: &clock,
        sender: Some(sender.as_ref()),
        alert_to: alert_to.as_deref(),
        max_notional: args.max_notional,
        label: &fingerprint,
    };
    match kill_drill::run_drill(&deps, &args) {
        Ok(report) => {
            print!("{}", report.render());
            std::process::exit(report.exit_code());
        }
        Err(refusal) => {
            eprintln!("kill-drill: refused: {refusal}");
            std::process::exit(2);
        }
    }
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}
