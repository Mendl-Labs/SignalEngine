//! Source-scan interlock (slice S-8): the pilot path must not be able to REACH a live venue by construction. These
//! tests read the source of the crates on the pilot path and fail if a live Alpaca environment, host or URL, a Kraken
//! adapter, or a read of the tenant-credential master key ever appears in code (comments are ignored).
//!
//! The scan is deliberately dumb (a substring check per code line): a reviewer who wants to add a live path must
//! change this test in the same PR, which is the point. `scanner_catches_planted_violations` proves it is not vacuous.

use std::fs;
use std::path::{Path, PathBuf};

fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().expect("crates dir").to_path_buf()
}

fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let p = e.unwrap().path();
        if p.is_dir() {
            rs_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// Does `line` contain `token` starting at an identifier boundary? (`VenueEnvironment::Live` is a different thing
/// from `Environment::Live`: the pipeline's own label for "a connection that may move real money".)
fn contains_token(line: &str, token: &str) -> bool {
    line.match_indices(token).any(|(i, _)| line[..i].chars().next_back().is_none_or(|c| !(c.is_alphanumeric() || c == '_')))
}

/// Code lines (comment-only lines removed) containing any of `tokens`, as `(line number, line, token)`.
fn scan(src: &str, tokens: &[&str]) -> Vec<(usize, String, String)> {
    let mut hits = Vec::new();
    for (i, line) in src.lines().enumerate() {
        if line.trim_start().starts_with("//") {
            continue;
        }
        for t in tokens {
            if contains_token(line, t) {
                hits.push((i + 1, line.trim().to_string(), (*t).to_string()));
            }
        }
    }
    hits
}

fn scan_tree(rel: &str, tokens: &[&str]) -> (usize, Vec<String>) {
    let mut files = Vec::new();
    rs_files(&crates_dir().join(rel), &mut files);
    let mut hits = Vec::new();
    for f in &files {
        for (n, line, tok) in scan(&fs::read_to_string(f).unwrap(), tokens) {
            hits.push(format!("{}:{n}: `{tok}` in `{line}`", f.display()));
        }
    }
    (files.len(), hits)
}

/// Things that name a LIVE Alpaca connection.
const LIVE_ALPACA: &[&str] = &["Environment::Live", "LIVE_BASE_URL", "LIVE_HOST", "https://api.alpaca.markets", "\"api.alpaca.markets\"", "LiveTradingAck"];

#[test]
fn the_service_source_names_no_live_venue_no_kraken_adapter_and_never_reads_the_master_key() {
    let mut tokens: Vec<&str> = LIVE_ALPACA.to_vec();
    // Kraken, OANDA and Bitstamp are not pilot venues (the pilot serves Alpaca only, see `pilot::PILOT_VENUE`).
    tokens.extend([
        "KrakenAdapter",
        "KrakenBroker",
        "KrakenConfig",
        "OandaConfig",
        "OandaAdapter",
        "OandaBroker",
        "BitstampAdapter",
        "BitstampBroker",
        "BitstampConfig",
        "bitstamp",
        "smartorderrouter",
        "MultiTenantDbProvider",
        "hostbuilder",
    ]);
    let (files, hits) = scan_tree("rebalancer-service/src", &tokens);
    assert!(files >= 3, "the scan saw the service sources ({files} files)");
    assert!(hits.is_empty(), "the pilot service must not reach a live venue:\n{}", hits.join("\n"));

    // The master key: it may be NAMED only in the list of forbidden environment variables, never read.
    let (_, key_hits) = scan_tree("rebalancer-service/src", &["CREDENTIALS_ENCRYPTION_KEY"]);
    for h in &key_hits {
        assert!(h.contains("FORBIDDEN_ENV"), "the master key may only appear in the FORBIDDEN_ENV list: {h}");
    }
    assert!(!key_hits.is_empty(), "the refusal list still names it (or the scan is broken)");
    let (_, reads) = scan_tree("rebalancer-service/src", &["env::var(\"CREDENTIALS", "var(\"CREDENTIALS_ENCRYPTION_KEY"]);
    assert!(reads.is_empty(), "{}", reads.join("\n"));
}

#[test]
fn the_pipeline_crate_names_no_live_alpaca_environment_host_or_url() {
    let (files, hits) = scan_tree("rebalancer-run/src", LIVE_ALPACA);
    assert!(files >= 10, "the scan saw the pipeline sources ({files} files)");
    assert!(hits.is_empty(), "the pipeline must not be able to name a live Alpaca environment:\n{}", hits.join("\n"));
}

#[test]
fn the_paper_only_adapter_module_names_no_live_alpaca_environment_host_or_url() {
    let path = crates_dir().join("broker-adapters/src/alpaca/paper_only.rs");
    let hits = scan(&fs::read_to_string(&path).unwrap(), LIVE_ALPACA);
    assert!(hits.is_empty(), "{hits:?}");
}

fn normal_deps(rel: &str) -> String {
    let manifest = fs::read_to_string(crates_dir().join(rel)).unwrap();
    // dependency tables only: everything before a [dev-dependencies] section is a normal dependency
    manifest.split("[dev-dependencies]").next().unwrap().to_string()
}

fn assert_no_legacy_crates(rel: &str, normal: &str) {
    for legacy in ["smartorderrouter", "hostbuilder", "executionhandler", "signaldispatcher", "exchangemetricaggregator", "strategyloader"] {
        assert!(!normal.lines().filter(|l| !l.trim_start().starts_with('#')).any(|l| l.trim_start().starts_with(legacy)), "{rel} depends on {legacy}");
    }
}

#[test]
fn the_service_does_not_depend_on_the_legacy_live_trading_crates() {
    // rebalancer-run / rebalancer-store never need a real HTTP client (the pipeline is pure, the stores talk only to
    // Postgres): the real transport stays off there, always.
    for rel in ["rebalancer-run/Cargo.toml", "rebalancer-store/Cargo.toml"] {
        let normal = normal_deps(rel);
        assert_no_legacy_crates(rel, &normal);
        assert!(!normal.contains("reqwest-transport"), "{rel} enables the real transport, which it should never need");
    }

    // rebalancer-service: the pilot builder slice (S-6) turns the real transport ON deliberately (this is the
    // comment the pre-S-6 version of this test told S-6 to update) -- for exactly the paper Alpaca adapter and the
    // Massive data source it wires as the production `Broker`/`DataSource`, and ONLY as a `broker-adapters` feature,
    // never as its own separate HTTP dependency (a second HTTP stack would be an unreviewed way to reach the network).
    let rel = "rebalancer-service/Cargo.toml";
    let normal = normal_deps(rel);
    assert_no_legacy_crates(rel, &normal);
    assert!(normal.contains("reqwest-transport"), "{rel} must turn the real HTTP transport on (S-6)");
    assert!(
        normal.lines().filter(|l| !l.trim_start().starts_with('#')).any(|l| l.trim_start().starts_with("broker-adapters") && l.contains("reqwest-transport")),
        "{rel}: reqwest-transport must be requested as a feature of the broker-adapters dependency line, not floated free"
    );
    assert!(
        !normal.lines().filter(|l| !l.trim_start().starts_with('#')).any(|l| l.trim_start().starts_with("reqwest ")),
        "{rel} must not depend on reqwest directly; only through broker-adapters' feature"
    );
}

#[test]
fn scanner_catches_planted_violations_and_ignores_comments() {
    let planted = "fn f() {\n    let e = Environment::Live;\n    let u = \"https://api.alpaca.markets\";\n}\n// Environment::Live in a comment\n/// LIVE_HOST in a doc comment\n";
    let hits = scan(planted, LIVE_ALPACA);
    let toks: Vec<&str> = hits.iter().map(|h| h.2.as_str()).collect();
    assert_eq!(toks, ["Environment::Live", "https://api.alpaca.markets"]);
    assert_eq!(hits[0].0, 2);
    assert!(scan("let p = \"https://paper-api.alpaca.markets\";", LIVE_ALPACA).is_empty(), "the paper URL is not the live one");
    assert!(scan("VenueEnvironment::Live => \"live\",", LIVE_ALPACA).is_empty(), "the pipeline's own label is not the adapter's live environment");
    assert_eq!(scan("broker_adapters::alpaca::Environment::Live", LIVE_ALPACA).len(), 1, "a path-qualified use IS caught");
}

/// Bitstamp is not wired into the pilot, which stays Alpaca-only. The pilot path is the service and the pipeline
/// crate. Neither may name the Bitstamp adapter, its market-data source or its module. Wiring one in is a deliberate
/// decision that must change this list in the same PR.
const BITSTAMP: &[&str] = &["Bitstamp", "bitstamp", "BITSTAMP"];

#[test]
fn the_pilot_path_does_not_reference_bitstamp() {
    for rel in ["rebalancer-service/src", "rebalancer-run/src"] {
        let (files, hits) = scan_tree(rel, BITSTAMP);
        assert!(files >= 3, "the scan saw the sources of {rel} ({files} files)");
        assert!(hits.is_empty(), "the pilot path must not reference Bitstamp (the pilot stays Alpaca-only):
{}", hits.join("
"));
    }
}

#[test]
fn the_bitstamp_scanner_catches_a_planted_reference() {
    assert!(!scan("let b = broker_adapters::bitstamp::BitstampAdapter::new(c, t, g);", BITSTAMP).is_empty());
    assert!(!scan("let s = BitstampTickerSource::new(t);", BITSTAMP).is_empty());
}
