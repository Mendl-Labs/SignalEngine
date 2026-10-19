//! The Engine's `symbol_in` alias (verbatim `strategy_library.rs`, tested inline there too): the shipped
//! `crypto_trend_100d` v2 declares the bare `BTC`/`ETH`, and a mandate that names the USD pairs still allows it.

use mandate_core::mandate::MandateBody;
use mandate_core::strategy_library::{current_platform_capabilities, eligibility, seed_library, ReasonCode};

const BASELINE: &str = include_str!("fixtures/baseline_mandate.json");

fn crypto() -> mandate_core::strategy_library::LibraryEntry {
    let lib = seed_library().expect("seed library parses");
    lib.into_iter().find(|e| e.id == "crypto_trend_100d").expect("crypto_trend_100d is shipped")
}

fn codes(allow: &[&str], deny: &[&str]) -> Vec<ReasonCode> {
    let mut v: serde_json::Value = serde_json::from_str(BASELINE).unwrap();
    v["universe"]["instrument_allow"] = serde_json::json!(allow);
    v["universe"]["instrument_deny"] = serde_json::json!(deny);
    let m: MandateBody = serde_json::from_value(v).unwrap();
    eligibility(&crypto(), &m, &current_platform_capabilities()).reasons.iter().map(|r| r.code).collect()
}

#[test]
fn the_shipped_crypto_entry_declares_the_bare_base_symbols() {
    let e = crypto();
    assert_eq!(e.version, 2);
    assert_eq!(e.instruments, vec!["BTC".to_string(), "ETH".to_string()]);
}

#[test]
fn usd_pairs_allow_it_and_other_quotes_or_symbols_do_not() {
    assert!(!codes(&["BTC/USD", "ETH/USD"], &[]).contains(&ReasonCode::InstrumentsNotAllowed));
    assert!(!codes(&["btc-usd", "ETHUSD"], &[]).contains(&ReasonCode::InstrumentsNotAllowed));
    assert!(codes(&["SPY"], &[]).contains(&ReasonCode::InstrumentsNotAllowed));
    assert!(codes(&["BTC/EUR", "ETH/USD"], &[]).contains(&ReasonCode::InstrumentsNotAllowed));
    assert!(codes(&["BTC/USD", "ETH/USD"], &["eth/usd"]).contains(&ReasonCode::InstrumentDenied));
}
