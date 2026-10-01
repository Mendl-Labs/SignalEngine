//! Golden tests that make drift between this copy and the Engine's mandate module DETECTABLE.
//!
//! The baseline mandate in `fixtures/baseline_mandate.json` is the same JSON as `base()` in the Engine's
//! `mandate.rs` tests. If the Engine changes a field, a default, an enum spelling or the serialization order of
//! `MandateBody`, its `canonical_hash` of this baseline changes; a later comparison of the Engine's value with the
//! pinned one below then shows the copies have drifted (and which copy moved).
//!
//! Provenance of the pins: computed once from this copy, whose two source files are byte-identical to the Engine's
//! blobs (blob hashes in SOURCE.md). The mandate hash was also reproduced by an independent re-implementation of
//! the serialization (a Python script following the struct declaration order), so it is not merely "whatever the
//! Rust code printed".

use mandate_core::mandate::{canonical_hash, validate, validate_json, MandateBody};
use mandate_core::strategy_library::{entry_hash, seed_library};

const BASELINE: &str = include_str!("fixtures/baseline_mandate.json");

/// `canonical_hash` of the baseline mandate (SHA-256 hex of the canonical serialization).
const PINNED_BASELINE_HASH: &str = "d0bf63969c12e36666b621c65f784e1ec7b6ad0500a0658bf4c93a55ccfe742e";

/// The exact bytes that are hashed. Pinned as text so a drift shows WHERE the serialization moved, not only that
/// the hash changed.
const PINNED_BASELINE_CANONICAL_JSON: &str = r#"{"basis":{"capital_source":"own","jurisdiction":"US-GA","acknowledgements":[{"doc":"own_capital_attestation","doc_version":"2026-10","at":"2026-09-21T12:00:00Z","by":"user_1"},{"doc":"risk_disclosure","doc_version":"2026-10","at":"2026-09-21T12:00:00Z","by":"user_1"}]},"capital":{"allocated":{"amount":"5000.00","ccy":"USD"},"min_cash_reserve":0.05},"universe":{"venues":["alpaca","kraken"],"asset_classes":["us_etf","crypto_spot"],"instrument_allow":["SPY","EFA","BTC/USD","ETH/USD"],"instrument_deny":[],"shorting":false,"derivatives":false,"leverage_max_gross":1.0},"exposure":{"max_position":0.25,"max_asset_class":{"crypto_spot":0.6},"max_gross":1.0,"max_net":1.0,"max_order_notional":{"amount":"1500.00","ccy":"USD"},"max_orders_per_day":20,"max_turnover_per_day":0.5},"loss":{"daily_loss_limit":0.03,"drawdown_ladder":[{"at":0.1,"action":"shrink","scale":0.5},{"at":0.2,"action":"halt_flatten","scale":null}],"resume_after_halt":"human_only"},"autonomy":{"level":"L3","may":["place_orders","cancel_orders","resize_within_limits","rebalance","halt","flatten"],"needs_confirmation":["add_strategy","remove_strategy","raise_target_risk"],"never":["change_limits","resume_after_halt","change_credentials","withdraw"]},"reporting":{"digest":"daily","alert_channels":["email"]}}"#;

fn baseline() -> MandateBody {
    let v: serde_json::Value = serde_json::from_str(BASELINE).expect("baseline fixture is JSON");
    validate_json(&v).expect("baseline mandate is valid")
}

#[test]
fn baseline_is_valid_and_canonical_json_is_pinned() {
    let body = baseline();
    assert_eq!(validate(&body), vec![]);
    assert_eq!(serde_json::to_string(&body).unwrap(), PINNED_BASELINE_CANONICAL_JSON);
}

#[test]
fn baseline_canonical_hash_is_pinned() {
    assert_eq!(canonical_hash(&baseline()), PINNED_BASELINE_HASH);
}

#[test]
fn key_order_and_whitespace_in_the_source_json_do_not_change_the_hash() {
    // Re-serialize through serde_json::Value (which sorts keys) and parse again.
    let v: serde_json::Value = serde_json::from_str(BASELINE).unwrap();
    let compact = serde_json::to_string(&v).unwrap();
    let body: MandateBody = serde_json::from_str(&compact).unwrap();
    assert_eq!(canonical_hash(&body), PINNED_BASELINE_HASH);
}

/// Pinned `entry_hash` of the three seed library entries (same provenance as above).
const PINNED_ENTRY_HASHES: [(&str, &str); 3] = [
    ("crypto_trend_100d", "826fb12ec051bcf1b93f8f4d6d5ddca1eeb03875ad0e687c7153be5d83f344f8"),
    ("etf_trend_faber", "b8ddc2ecf3a52cc8a8f0ae772f3d5148f3071217dc085a50b4ec2d9f6f619b86"),
    ("fx_tsmom_12m", "92c0e2b464a10d627bfc68a2b415feff00a09e2e6cfdbefa724f9181515c4dc9"),
];

#[test]
fn seed_library_entry_hashes_are_pinned() {
    let lib = seed_library().expect("seed library parses");
    assert_eq!(lib.len(), PINNED_ENTRY_HASHES.len());
    for (id, want) in PINNED_ENTRY_HASHES {
        let e = lib.iter().find(|e| e.id == id).unwrap_or_else(|| panic!("no seed entry {id}"));
        assert_eq!(entry_hash(e), want, "seed entry {id} changed");
    }
}
