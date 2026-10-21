//! Source-scan test required by ruling R54 (A1, K5): the read-only sync code must never be able to change an order.
//!
//! The build fails if this crate, or any sibling crate under `crates/broker-sync*`, mentions an order-changing API
//! (by name, in code or in comments) or touches credential-handling / master-key code. This file is the one file
//! exempt from the scan, because it has to contain the forbidden words.

use std::fs;
use std::path::{Path, PathBuf};

/// Order-changing APIs of the adapters and of the broker REST surfaces. Matched as plain substrings.
const FORBIDDEN_ORDER_APIS: &[&str] = &[
    "place_order",
    "submit_order",
    "cancel_order",
    "replace_order",
    "close_position",
    "flatten_all",
    "cancel_and_settle",
    "PlaceOutcome",
    "OrderRequest",
    "PreparedOrder",
    "PaperOnlyAlpaca",
    "withdraw",
];

/// Credential-handling and master-key code belongs to the Engine and the credential broker, never to the reader.
const FORBIDDEN_CREDENTIAL_CODE: &[&str] = &[
    "CREDENTIALS_ENCRYPTION_KEY",
    "master_key",
    "AlpacaCredentials",
    "encrypted_api_key",
    "decrypt",
];

fn workspace_crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().expect("crates dir").to_path_buf()
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = fs::read_dir(dir) else { return };
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            if p.file_name().is_some_and(|n| n.to_string_lossy() == "target") {
                continue;
            }
            collect(&p, out);
        } else if p.extension().is_some_and(|e| e.to_string_lossy() == "rs") || p.file_name().is_some_and(|n| n.to_string_lossy() == "Cargo.toml") {
            out.push(p);
        }
    }
}

fn scanned_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    let rd = fs::read_dir(workspace_crates_dir()).expect("read crates dir");
    for entry in rd.flatten() {
        let p = entry.path();
        let is_sync_crate = p.is_dir() && p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("broker-sync"));
        if is_sync_crate {
            collect(&p, &mut files);
        }
    }
    files.retain(|f| f.file_name().is_none_or(|n| n.to_string_lossy() != "source_scan.rs"));
    files.sort();
    files
}

/// Returns `(file, forbidden word)` for every hit.
fn hits(files: &[PathBuf], words: &[&str]) -> Vec<(String, String)> {
    let mut found = Vec::new();
    for f in files {
        let text = fs::read_to_string(f).unwrap_or_default();
        for w in words {
            if text.contains(w) {
                found.push((f.display().to_string(), (*w).to_string()));
            }
        }
    }
    found
}

#[test]
fn scan_covers_this_crates_sources() {
    let files = scanned_files();
    let names: Vec<String> = files.iter().map(|f| f.display().to_string().replace('\\', "/")).collect();
    for expected in ["broker-sync-core/src/lib.rs", "broker-sync-core/src/reader.rs", "broker-sync-core/Cargo.toml"] {
        assert!(names.iter().any(|n| n.ends_with(expected)), "scan did not reach {expected}: {names:?}");
    }
}

#[test]
fn no_broker_sync_crate_references_an_order_changing_api() {
    let found = hits(&scanned_files(), FORBIDDEN_ORDER_APIS);
    assert!(found.is_empty(), "order-changing API referenced from the read-only sync code: {found:?}");
}

#[test]
fn no_broker_sync_crate_touches_credential_or_master_key_code() {
    // Shipped code only (src and Cargo.toml): tests must build a fake-transport adapter, which needs a test credential
    // constant, so test files are exempt from this one check (the order-API scan above still covers them).
    let shipped: Vec<PathBuf> =
        scanned_files().into_iter().filter(|f| !f.display().to_string().replace('\\', "/").contains("/tests/")).collect();
    let found = hits(&shipped, FORBIDDEN_CREDENTIAL_CODE);
    assert!(found.is_empty(), "credential-handling code referenced from the read-only sync code: {found:?}");
}

#[test]
fn the_scanner_itself_detects_a_planted_violation() {
    let dir = std::env::temp_dir().join(format!("broker_sync_scan_selftest_{}", std::process::id()));
    fs::create_dir_all(&dir).expect("temp dir");
    let planted = dir.join("planted.rs");
    fs::write(&planted, "fn oops(a: &dyn Adapter) { a.place_order(); }").expect("write planted file");
    let found = hits(std::slice::from_ref(&planted), FORBIDDEN_ORDER_APIS);
    let _ = fs::remove_dir_all(&dir);
    assert_eq!(found.len(), 1, "scanner missed a planted place_order: {found:?}");
}
