//! `OandaRules`: the planner's size rules for OANDA, taken from the adapter's own `prepare_order` and the
//! broker's instrument table. Additive: nothing here touches the Kraken or Alpaca rules.

use broker_adapters::oanda::{InstrumentInfo, InstrumentTable, PrepareOptions};
use broker_adapters::{Dec, Side};
use rebalancer_core::venue::{OandaRules, SizeRefusal, VenueRuleBook, VenueRules};

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

fn row(name: &str, units_dp: u32, min: &str, max: &str) -> InstrumentInfo {
    InstrumentInfo {
        name: name.to_string(),
        kind: "CURRENCY".into(),
        display_precision: 5,
        trade_units_precision: units_dp,
        minimum_trade_size: d(min),
        maximum_order_units: d(max),
        margin_rate: d("0.02"),
        maximum_position_size: None,
    }
}

fn table() -> InstrumentTable {
    InstrumentTable::from_rows([row("EUR_USD", 0, "1", "1000000"), row("DE30_EUR", 1, "0.1", "2500")])
}

#[test]
fn rounds_down_to_whole_units_and_never_up() {
    let (t, o) = (table(), PrepareOptions::default());
    let r = OandaRules { instruments: &t, options: &o };
    let px = d("1.10");
    assert_eq!(r.round_quantity("EUR/USD", Side::Buy, d("1234.99"), px).unwrap(), d("1234"));
    assert_eq!(r.round_quantity("EUR_USD", Side::Sell, d("1234.99"), px).unwrap(), d("1234"), "a magnitude for both sides");
    assert_eq!(r.round_quantity("eurusd", Side::Buy, d("50"), px).unwrap(), d("50"));
    assert_eq!(r.round_quantity("DE30/EUR", Side::Buy, d("2.57"), d("18000")).unwrap(), d("2.5"));
}

#[test]
fn refusals_carry_their_reason() {
    let (t, o) = (table(), PrepareOptions::default());
    let r = OandaRules { instruments: &t, options: &o };
    let px = d("1.10");
    assert_eq!(r.round_quantity("EUR_USD", Side::Buy, d("0.4"), px), Err(SizeRefusal::RoundsToZero));
    assert_eq!(r.round_quantity("DE30_EUR", Side::Buy, d("0.05"), px), Err(SizeRefusal::RoundsToZero));
    // above maximumOrderUnits is refused, not truncated
    assert!(matches!(r.round_quantity("EUR_USD", Side::Buy, d("2000000"), px), Err(SizeRefusal::Other(m)) if m.contains("maximumOrderUnits")));
    // no row (nothing is built in) and unparseable names are unknown instruments
    assert_eq!(r.round_quantity("GBP_USD", Side::Buy, d("1000"), px), Err(SizeRefusal::UnknownInstrument("GBP_USD".into())));
    assert_eq!(r.round_quantity("???", Side::Buy, d("1000"), px), Err(SizeRefusal::UnknownInstrument("???".into())));
    let empty = InstrumentTable::new();
    let r = OandaRules { instruments: &empty, options: &o };
    assert!(matches!(r.round_quantity("EUR_USD", Side::Buy, d("1000"), px), Err(SizeRefusal::UnknownInstrument(_))));
}

#[test]
fn minimum_trade_size_is_enforced() {
    let t = InstrumentTable::from_rows([row("EUR_USD", 0, "10", "1000000")]);
    let o = PrepareOptions::default();
    let r = OandaRules { instruments: &t, options: &o };
    assert_eq!(r.round_quantity("EUR_USD", Side::Buy, d("9"), d("1")), Err(SizeRefusal::BelowMinQuantity { min: d("10") }));
    assert_eq!(r.round_quantity("EUR_USD", Side::Buy, d("10"), d("1")), Ok(d("10")));
}

#[test]
fn the_reference_price_does_not_matter() {
    let (t, o) = (table(), PrepareOptions::default());
    let r = OandaRules { instruments: &t, options: &o };
    for px in ["0.0001", "1.1", "100000"] {
        assert_eq!(r.round_quantity("EUR_USD", Side::Buy, d("500"), d(px)), Ok(d("500")), "{px}");
    }
}

#[test]
fn the_fingerprint_changes_with_the_rule_row_and_is_stable_otherwise() {
    let o = PrepareOptions::default();
    let t1 = table();
    let t2 = InstrumentTable::from_rows([row("EUR_USD", 0, "1", "500000")]);
    let (r1, r2) = (OandaRules { instruments: &t1, options: &o }, OandaRules { instruments: &t2, options: &o });
    assert_eq!(r1.fingerprint("EUR/USD"), r1.fingerprint("EUR_USD"));
    assert_ne!(r1.fingerprint("EUR_USD"), r2.fingerprint("EUR_USD"), "a changed rule changes the plan digest");
    assert!(r1.fingerprint("EUR_USD").starts_with("oanda:"));
    assert!(r1.fingerprint("GBP_USD").contains("None"), "an unknown instrument fingerprints as such");
}

#[test]
fn it_plugs_into_the_rule_book_under_the_venue_name_oanda() {
    let (t, o) = (table(), PrepareOptions::default());
    let r = OandaRules { instruments: &t, options: &o };
    let book = VenueRuleBook::new().with("OANDA", &r);
    assert!(book.get("oanda").is_some());
    assert!(book.get("kraken").is_none(), "adding oanda adds no other venue");
    assert_eq!(book.get(" Oanda ").unwrap().round_quantity("EUR_USD", Side::Buy, d("7.9"), d("1")), Ok(d("7")));
}
