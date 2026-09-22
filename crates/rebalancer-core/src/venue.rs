//! Venue size rules for the planner, taken from the broker adapters and NOT re-stated here.
//!
//! The planner asks a [`VenueRules`] to turn a wanted quantity into one the venue will accept. The two provided
//! implementations call the adapters' own `prepare_order` (Kraken: lot decimals, `ordermin`, `costmin`, pair
//! status; Alpaca: fractionable vs whole shares, minimum order size, minimum notional, tradable status), with a
//! market-order probe request. Two consequences worth knowing:
//! * no venue constant lives in this crate, and a change to an adapter table or rule changes the plan with it;
//! * a quantity the planner emits is one the adapter's `prepare_order` accepts unchanged, so the order the plan
//!   proposes is the order that will be sent (the adapter rounds down again; that is a no-op on a rounded value).
//!
//! Sizes are ALWAYS rounded down or refused; nothing is ever bumped up to a minimum.

use std::collections::BTreeMap;

use broker_adapters::alpaca::{self, AssetTable};
use broker_adapters::kraken::order::{prepare_order as kraken_prepare, PrepareOptions as KrakenPrepareOptions};
use broker_adapters::kraken::pairs::PairTable;
use broker_adapters::{BrokerError, Dec, OrderRequest, Side};

/// Why a quantity could not be turned into a sendable order size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SizeRefusal {
    UnknownInstrument(String),
    NotTradable(String),
    RoundsToZero,
    BelowMinQuantity { min: Dec },
    BelowMinCost { min: Dec },
    Other(String),
}

impl std::fmt::Display for SizeRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SizeRefusal::UnknownInstrument(s) => write!(f, "venue has no rules for {s}"),
            SizeRefusal::NotTradable(s) => write!(f, "not tradable: {s}"),
            SizeRefusal::RoundsToZero => write!(f, "quantity rounds down to zero at the venue's precision"),
            SizeRefusal::BelowMinQuantity { min } => write!(f, "below the venue minimum quantity {min}"),
            SizeRefusal::BelowMinCost { min } => write!(f, "below the venue minimum order value {min}"),
            SizeRefusal::Other(s) => write!(f, "{s}"),
        }
    }
}

pub trait VenueRules {
    /// The quantity to send for a wish of `quantity` at reference `price`: rounded DOWN to the venue's precision,
    /// or a refusal. Never larger than `quantity`.
    fn round_quantity(&self, symbol: &str, side: Side, quantity: Dec, price: Dec) -> Result<Dec, SizeRefusal>;

    /// Deterministic text of the rules that apply to `symbol`; part of the plan's inputs digest, so a changed rule
    /// table changes the digest.
    fn fingerprint(&self, symbol: &str) -> String;
}

fn map_error(symbol: &str, e: BrokerError) -> SizeRefusal {
    match e {
        BrokerError::QuantityRoundsToZero { .. } => SizeRefusal::RoundsToZero,
        BrokerError::BelowMinQuantity { min, .. } => SizeRefusal::BelowMinQuantity { min },
        BrokerError::BelowMinCost { min, .. } => SizeRefusal::BelowMinCost { min },
        BrokerError::PairNotTradable { status, .. } => SizeRefusal::NotTradable(status),
        BrokerError::UnknownSymbol(_) => SizeRefusal::UnknownInstrument(symbol.to_string()),
        other => SizeRefusal::Other(other.to_string()),
    }
}

/// Kraken spot rules from a [`PairTable`].
pub struct KrakenRules<'a> {
    pub pairs: &'a PairTable,
}

impl VenueRules for KrakenRules<'_> {
    fn round_quantity(&self, symbol: &str, side: Side, quantity: Dec, price: Dec) -> Result<Dec, SizeRefusal> {
        let pair = self.pairs.lookup(symbol).ok_or_else(|| SizeRefusal::UnknownInstrument(symbol.to_string()))?;
        let mut req = OrderRequest::market("rb1:size-probe", symbol, side, quantity);
        req.reference_price = Some(price);
        kraken_prepare(&req, pair, 1, &KrakenPrepareOptions::default())
            .map(|p| p.volume)
            .map_err(|e| map_error(symbol, e))
    }

    fn fingerprint(&self, symbol: &str) -> String {
        format!("kraken:{symbol}:{:?}", self.pairs.lookup(symbol))
    }
}

/// Alpaca equities rules from an [`AssetTable`] and the adapter's [`alpaca::PrepareOptions`] (minimum notional,
/// tag prefix, ...).
pub struct AlpacaRules<'a> {
    pub assets: &'a AssetTable,
    pub options: &'a alpaca::PrepareOptions,
}

impl VenueRules for AlpacaRules<'_> {
    fn round_quantity(&self, symbol: &str, side: Side, quantity: Dec, price: Dec) -> Result<Dec, SizeRefusal> {
        let asset = self.assets.lookup(symbol).ok_or_else(|| SizeRefusal::UnknownInstrument(symbol.to_string()))?;
        let prefix = self.options.own_tag_prefix.clone().unwrap_or_default();
        let mut req = OrderRequest::market(&format!("{prefix}size-probe"), symbol, side, quantity);
        req.reference_price = Some(price);
        alpaca::order::prepare_order(&req, asset, self.options)
            .map(|p| p.quantity)
            .map_err(|e| map_error(symbol, e))
    }

    fn fingerprint(&self, symbol: &str) -> String {
        format!("alpaca:{symbol}:{:?}:min_notional={}", self.assets.lookup(symbol), self.options.min_notional)
    }
}

/// The venue rule sets a plan may use, keyed by lower-case venue name (`"kraken"`, `"alpaca"`).
#[derive(Default)]
pub struct VenueRuleBook<'a> {
    by_venue: BTreeMap<String, &'a dyn VenueRules>,
}

impl<'a> VenueRuleBook<'a> {
    pub fn new() -> Self {
        Self { by_venue: BTreeMap::new() }
    }

    pub fn with(mut self, venue: &str, rules: &'a dyn VenueRules) -> Self {
        self.by_venue.insert(venue.trim().to_lowercase(), rules);
        self
    }

    pub fn get(&self, venue: &str) -> Option<&'a dyn VenueRules> {
        self.by_venue.get(&venue.trim().to_lowercase()).copied()
    }
}
