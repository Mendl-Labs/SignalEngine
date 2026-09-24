//! Venue size rules for the planner, taken from the broker adapters and NOT re-stated here.
//!
//! The planner asks a [`VenueRules`] to turn a wanted quantity into one the venue will accept. The provided
//! implementations call the adapters' own `prepare_order` (Kraken: lot decimals, `ordermin`, `costmin`, pair
//! status; Alpaca: fractionable vs whole shares, minimum order size, minimum notional, tradable status; OANDA:
//! `tradeUnitsPrecision`, `minimumTradeSize`, `maximumOrderUnits`), with a market-order probe request. Two
//! consequences worth knowing:
//! * no venue constant lives in this crate, and a change to an adapter table or rule changes the plan with it;
//! * a quantity the planner emits is one the adapter's `prepare_order` accepts unchanged, so the order the plan
//!   proposes is the order that will be sent (the adapter rounds down again; that is a no-op on a rounded value).
//!
//! Sizes are ALWAYS rounded down or refused; nothing is ever bumped up to a minimum.

use std::collections::{BTreeMap, BTreeSet};

use broker_adapters::alpaca::{self, AssetTable};
use broker_adapters::kraken::order::{prepare_order as kraken_prepare, PrepareOptions as KrakenPrepareOptions};
use broker_adapters::kraken::pairs::PairTable;
use broker_adapters::oanda;
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

/// OANDA FX rules from an [`InstrumentTable`](oanda::InstrumentTable) (the broker's own instrument list: unit
/// precision, minimum trade size, maximum order units) and the adapter's [`oanda::PrepareOptions`]. Added after the
/// Kraken and Alpaca rules and independent of them. Unlike Kraken/Alpaca there is NO minimum notional at OANDA, so
/// the reference `price` argument is ignored; the returned quantity is a MAGNITUDE (the side gives the sign).
/// A table with no row for the instrument (nothing is built in) is `UnknownInstrument`, never a guess.
pub struct OandaRules<'a> {
    pub instruments: &'a oanda::InstrumentTable,
    pub options: &'a oanda::PrepareOptions,
}

impl VenueRules for OandaRules<'_> {
    fn round_quantity(&self, symbol: &str, side: Side, quantity: Dec, _price: Dec) -> Result<Dec, SizeRefusal> {
        let info = self.instruments.lookup(symbol).ok_or_else(|| SizeRefusal::UnknownInstrument(symbol.to_string()))?;
        let prefix = self.options.own_tag_prefix.clone().unwrap_or_default();
        let req = OrderRequest::market(&format!("{prefix}size-probe"), symbol, side, quantity);
        oanda::order::prepare_order(&req, info, self.options).map(|p| p.quantity).map_err(|e| map_error(symbol, e))
    }

    fn fingerprint(&self, symbol: &str) -> String {
        // Canonical spelling, so `EUR/USD` and `EUR_USD` give the same plan digest.
        let canonical = oanda::canonical_symbol(symbol).unwrap_or_else(|_| symbol.to_string());
        format!("oanda:{canonical}:{:?}", self.instruments.lookup(symbol))
    }
}

/// Whether one instrument may be sold short on a venue, as the venue/account/instrument say (NOT the mandate: the
/// mandate's own `universe.shorting` is a separate, additional condition).
///
/// This is DATA the caller reads from the broker (Alpaca: account `shorting_enabled`, account equity >= $2,000, asset
/// `shortable` / `easy_to_borrow`, crypto never; Kraken: a margin-enabled pair with a non-empty `leverage_sell`
/// and room under `short_position_limit`, pair `status`; OANDA: every position is margin based). Nothing here is a
/// constant of this crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShortPolicy {
    /// The venue, account or instrument cannot be sold short. `reason` says why, for the denial message.
    Forbidden { reason: String },
    /// A short may be opened.
    Allowed,
    /// A short may be opened only against an approved borrow locate (Alpaca hard-to-borrow). The guard accepts it only
    /// when the caller ALSO lists a locate for that instrument in [`InstrumentRules::with_locate`].
    NeedsLocate,
}

/// What one instrument on one venue allows, for a SIGNED (short and/or levered) plan. Every field is a fact from the
/// venue, supplied by the caller; the guard invents no default. A rule that is ABSENT for an instrument means the
/// venue's short facts are unknown, and a short in it is denied (fail closed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentVenueRule {
    pub short: ShortPolicy,
    /// The largest gross exposure, as a multiple of the capital base, a book that includes an order in this
    /// instrument may reach (Kraken: the pair's largest `leverage_sell`/`leverage_buy`; OANDA: `1 / marginRate`).
    /// It joins the mandate's caps in the effective gross cap (see the guard docs). `None`: the venue states none.
    pub max_leverage: Option<Dec>,
    /// The largest position, in instrument UNITS (either side), the venue accepts (OANDA `maximumPositionSize`;
    /// Kraken `short_position_limit` / `long_position_limit`). An order that would take the position above it is
    /// refused, never shrunk. `None`: the venue states none.
    pub max_position_units: Option<Dec>,
}

impl InstrumentVenueRule {
    fn with_short(short: ShortPolicy) -> Self {
        Self { short, max_leverage: None, max_position_units: None }
    }

    /// Shorting is allowed.
    pub fn shortable() -> Self {
        Self::with_short(ShortPolicy::Allowed)
    }

    /// Shorting is not possible (for example every crypto asset on Alpaca).
    pub fn never_shortable(reason: &str) -> Self {
        Self::with_short(ShortPolicy::Forbidden { reason: reason.to_string() })
    }

    /// Shorting needs a borrow locate first.
    pub fn short_needs_locate() -> Self {
        Self::with_short(ShortPolicy::NeedsLocate)
    }

    pub fn with_max_leverage(mut self, max: Dec) -> Self {
        self.max_leverage = Some(max);
        self
    }

    pub fn with_max_position_units(mut self, max: Dec) -> Self {
        self.max_position_units = Some(max);
        self
    }

    fn fingerprint(&self) -> String {
        let short = match &self.short {
            ShortPolicy::Forbidden { reason } => format!("forbidden({reason})"),
            ShortPolicy::Allowed => "allowed".to_string(),
            ShortPolicy::NeedsLocate => "needs_locate".to_string(),
        };
        let f = |d: Option<Dec>| d.map_or("-".to_string(), |v| v.normalized().to_string());
        format!("short={short}|max_leverage={}|max_position_units={}", f(self.max_leverage), f(self.max_position_units))
    }
}

/// Per-instrument venue facts and borrow locates for a SIGNED plan, keyed by (venue, symbol) (venue trimmed and
/// lower-cased, symbol trimmed and upper-cased). Empty by default: with no entry for an instrument, a short in it is
/// denied. A long-only plan never consults it.
///
/// The caller fills it from the broker, one entry per instrument it is willing to short or to cap:
/// * Alpaca: `Allowed` only when the account has `shorting_enabled` and equity >= 2000 and the asset is `shortable`
///   and easy-to-borrow; `NeedsLocate` for a shortable hard-to-borrow asset (plus a locate); `Forbidden` otherwise and
///   always for crypto.
/// * Kraken: `Allowed` for a pair whose status permits it, that has margin (`leverage_sell` non-empty) and room under
///   `short_position_limit`, with `max_leverage` from the arrays; `Forbidden` for a pair without margin.
/// * OANDA: `Allowed` (every position is margin based) with `max_leverage` from `marginRate` and `max_position_units`
///   from `maximumPositionSize`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InstrumentRules {
    rules: BTreeMap<(String, String), InstrumentVenueRule>,
    locates: BTreeSet<(String, String)>,
}

fn rule_key(venue: &str, symbol: &str) -> (String, String) {
    (venue.trim().to_lowercase(), symbol.trim().to_uppercase())
}

impl InstrumentRules {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_rule(mut self, venue: &str, symbol: &str, rule: InstrumentVenueRule) -> Self {
        self.rules.insert(rule_key(venue, symbol), rule);
        self
    }

    /// Record an approved borrow locate for `symbol`. Presence only: this crate does not track its size or expiry.
    pub fn with_locate(mut self, venue: &str, symbol: &str) -> Self {
        self.locates.insert(rule_key(venue, symbol));
        self
    }

    pub fn rule(&self, venue: &str, symbol: &str) -> Option<&InstrumentVenueRule> {
        self.rules.get(&rule_key(venue, symbol))
    }

    pub fn has_locate(&self, venue: &str, symbol: &str) -> bool {
        self.locates.contains(&rule_key(venue, symbol))
    }

    /// Deterministic text of what is known about one instrument; part of a signed plan's inputs digest.
    pub fn fingerprint(&self, venue: &str, symbol: &str) -> String {
        let rule = self.rule(venue, symbol).map_or("absent".to_string(), InstrumentVenueRule::fingerprint);
        format!("{rule}|locate={}", self.has_locate(venue, symbol))
    }
}

/// The venue rule sets a plan may use, keyed by lower-case venue name (`"kraken"`, `"alpaca"`, `"oanda"`), plus the per-instrument
/// facts a SIGNED plan needs ([`InstrumentRules`], empty by default).
#[derive(Default)]
pub struct VenueRuleBook<'a> {
    by_venue: BTreeMap<String, &'a dyn VenueRules>,
    instruments: InstrumentRules,
}

impl<'a> VenueRuleBook<'a> {
    pub fn new() -> Self {
        Self { by_venue: BTreeMap::new(), instruments: InstrumentRules::new() }
    }

    pub fn with(mut self, venue: &str, rules: &'a dyn VenueRules) -> Self {
        self.by_venue.insert(venue.trim().to_lowercase(), rules);
        self
    }

    pub fn get(&self, venue: &str) -> Option<&'a dyn VenueRules> {
        self.by_venue.get(&venue.trim().to_lowercase()).copied()
    }

    /// Supply the per-instrument facts a signed plan's guard needs (replaces any set before).
    pub fn with_instrument_rules(mut self, instruments: InstrumentRules) -> Self {
        self.instruments = instruments;
        self
    }

    pub fn instrument_rules(&self) -> &InstrumentRules {
        &self.instruments
    }
}
