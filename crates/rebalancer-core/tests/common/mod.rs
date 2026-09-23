//! Shared fixtures and a tiny seeded PRNG for the rebalancer-core integration tests.
#![allow(dead_code)]

use chrono::{DateTime, Utc};
use mandate_core::mandate::MandateBody;
use rebalancer_core::guard::{AccountView, DayCounters, Position, PricePoint, ProposedOrder};
use rebalancer_core::policy::{MandateEnvelope, MandateStatus, Policy};
use rebalancer_core::venue::{InstrumentRules, InstrumentVenueRule};
use rebalancer_core::Dec;
use serde_json::Value;

pub const BASELINE: &str = include_str!("../../../mandate-core/tests/fixtures/baseline_mandate.json");

pub fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

pub fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

pub fn now() -> DateTime<Utc> {
    at("2026-09-21T15:00:00Z")
}

pub fn body_with(edit: impl FnOnce(&mut Value)) -> MandateBody {
    let mut v: Value = serde_json::from_str(BASELINE).unwrap();
    edit(&mut v);
    serde_json::from_value(v).expect("edited baseline still parses")
}

pub fn active_envelope() -> MandateEnvelope {
    MandateEnvelope {
        version: 3,
        status: MandateStatus::Active,
        effective_from: at("2026-09-01T00:00:00Z"),
        review_by: at("2026-12-31T00:00:00Z"),
    }
}

/// An ACTIVE policy compiled from the baseline mandate with `edit` applied to its JSON.
pub fn policy_with(edit: impl FnOnce(&mut Value)) -> Policy {
    Policy::compile(&body_with(edit)).with_envelope(active_envelope())
}

pub fn policy() -> Policy {
    policy_with(|_| {})
}

pub fn venue_and_class(symbol: &str) -> (&'static str, &'static str) {
    match symbol {
        "BTC/USD" | "ETH/USD" => ("kraken", "crypto_spot"),
        _ => ("alpaca", "us_etf"),
    }
}

/// Venue facts under which every instrument the signed tests use may be sold short (Alpaca ETFs and Kraken spot
/// margin pairs). The signed guard denies a short in an instrument it has no rule for, so a test that wants shorts
/// to be allowed must say so with data, as a real caller would read it from the broker.
pub fn shortable_everywhere() -> InstrumentRules {
    let mut r = InstrumentRules::new();
    for s in ["SPY", "EFA", "IEF", "DBC", "VNQ", "QQQ"] {
        r = r.with_rule("alpaca", s, InstrumentVenueRule::shortable());
    }
    for s in ["BTC/USD", "ETH/USD"] {
        r = r.with_rule("kraken", s, InstrumentVenueRule::shortable());
    }
    r
}

pub fn pos(symbol: &str, qty: &str, mv: &str) -> Position {
    let (venue, class) = venue_and_class(symbol);
    Position {
        symbol: symbol.to_string(),
        venue: venue.to_string(),
        asset_class: class.to_string(),
        quantity: d(qty),
        market_value: d(mv),
    }
}

pub fn account(equity: &str, cash: &str, positions: Vec<Position>) -> AccountView {
    AccountView {
        account_id: "acct-1".to_string(),
        ccy: "USD".to_string(),
        equity: d(equity),
        cash: d(cash),
        positions,
        halted: false,
        now: now(),
    }
}

/// A fresh account: 5000 equity, all cash, no positions.
pub fn flat() -> AccountView {
    account("5000", "5000", vec![])
}

pub fn order(symbol: &str, side: broker_adapters::Side, qty: &str, price: &str) -> ProposedOrder {
    let (venue, class) = venue_and_class(symbol);
    ProposedOrder {
        venue: venue.to_string(),
        asset_class: class.to_string(),
        symbol: symbol.to_string(),
        side,
        quantity: d(qty),
        price: Some(PricePoint { price: d(price), as_of: now() }),
        est_fee: Dec::ZERO,
        uses_margin: false,
        is_derivative: false,
    }
}

pub fn buy(symbol: &str, qty: &str, price: &str) -> ProposedOrder {
    order(symbol, broker_adapters::Side::Buy, qty, price)
}

pub fn sell(symbol: &str, qty: &str, price: &str) -> ProposedOrder {
    order(symbol, broker_adapters::Side::Sell, qty, price)
}

pub fn day0() -> DayCounters {
    DayCounters::ZERO
}

/// SplitMix64: a tiny deterministic PRNG (no `rand` dependency), the same generator reference-rules uses.
pub struct SplitMix64(pub u64);

impl SplitMix64 {
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform integer in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next_u64() % (hi - lo + 1)
    }

    pub fn chance(&mut self, percent: u64) -> bool {
        self.range(0, 99) < percent
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.range(0, items.len() as u64 - 1) as usize]
    }
}
