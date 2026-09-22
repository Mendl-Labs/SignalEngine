//! Pure order preparation: validation, rounding, time-in-force, and the `POST /v2/orders` body.
//!
//! Rules (all deliberate, all tested):
//! * `time_in_force` is ALWAYS sent (SignalEngine omitted it: ANALYSIS.md item 1, INFERRED).
//!   Market orders go out as `day`. Limit orders default to `day`; `Gtc` and `Ioc` map to `gtc` and
//!   `ioc`. A fractional quantity requires `day` (FROM-MEMORY-OF-DOCS), so `gtc`/`ioc` on one is
//!   refused rather than silently changed.
//! * Quantity is rounded DOWN (see [`crate::alpaca::assets`]); zero or below-minimum is refused.
//! * A limit price is rounded to the price increment in the direction never worse than requested
//!   (buys down, sells up). Increment: the asset's `price_increment`, else $0.01 at or above $1 and
//!   $0.0001 below (FROM-MEMORY-OF-DOCS: Alpaca rejects sub-penny prices above $1).
//! * `client_order_id` (<= 128 chars) is the caller's tag, verbatim. It is never truncated or
//!   hashed: a silently changed id would break the lookup-by-tag reconciliation.
//! * `validate_only`, `reduce_only`, `post_only` are refused as `Unsupported`: Alpaca has no
//!   validate-only endpoint, and equities have neither reduce-only nor post-only. Silently
//!   dropping a safety flag is worse than an error.
//! * Symbols containing `/` (crypto pairs) are refused: this adapter is for equities.

use crate::alpaca::assets::{AssetInfo, AssetSource};
use crate::decimal::{Dec, Rounding};
use crate::error::BrokerError;
use crate::types::{OrderKind, OrderRequest, SentOrder, Side, TimeInForce};
use serde_json::{json, Map, Value};

/// Alpaca's documented client_order_id limit.
pub const MAX_CLIENT_ORDER_ID_LEN: usize = 128;

#[derive(Debug, Clone)]
pub struct PrepareOptions {
    pub allow_extended_hours: bool,
    pub min_notional: Dec,
    pub own_tag_prefix: Option<String>,
    pub refuse_builtin_assets: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedOrder {
    pub symbol: String,
    pub side: Side,
    pub requested_quantity: Dec,
    pub quantity: Dec,
    /// True when the (rounded) quantity is not a whole number of shares.
    pub fractional: bool,
    /// `None` for a market order.
    pub limit_price: Option<Dec>,
    /// `day`, `gtc` or `ioc`.
    pub time_in_force: &'static str,
    pub extended_hours: bool,
    pub client_order_id: String,
    pub asset_source: AssetSource,
    /// True when no price was available to check the minimum notional.
    pub cost_check_skipped: bool,
}

/// Normalise and validate an equity symbol (uppercase; letters, digits, `.` and `-`).
pub fn normalize_symbol(symbol: &str) -> Result<String, BrokerError> {
    let s = symbol.trim();
    if let Some((base, quote)) = s.split_once('/') {
        let alnum = |t: &str| !t.is_empty() && t.bytes().all(|b| b.is_ascii_alphanumeric());
        if alnum(base) && alnum(quote) {
            return Err(BrokerError::Unsupported(format!(
                "{s}: crypto pairs are not supported by the Alpaca equities adapter"
            )));
        }
        return Err(BrokerError::InvalidRequest(format!("invalid equity symbol {symbol:?}")));
    }
    let up = s.to_ascii_uppercase();
    let ok = !up.is_empty()
        && up.len() <= 15
        && up.starts_with(|c: char| c.is_ascii_alphanumeric())
        && up.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-');
    if !ok {
        return Err(BrokerError::InvalidRequest(format!("invalid equity symbol {symbol:?}")));
    }
    Ok(up)
}

fn validate_tag(tag: &str, opts: &PrepareOptions) -> Result<(), BrokerError> {
    if tag.is_empty() {
        return Err(BrokerError::InvalidRequest("order tag (client_order_id) must not be empty".into()));
    }
    if tag.len() > MAX_CLIENT_ORDER_ID_LEN {
        return Err(BrokerError::InvalidRequest(format!(
            "order tag is {} characters; Alpaca's client_order_id limit is {MAX_CLIENT_ORDER_ID_LEN}",
            tag.len()
        )));
    }
    if !tag.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
        return Err(BrokerError::InvalidRequest("order tag must be printable ASCII".into()));
    }
    if let Some(prefix) = &opts.own_tag_prefix {
        if !tag.starts_with(prefix.as_str()) {
            return Err(BrokerError::InvalidRequest(format!(
                "order tag {tag:?} does not start with the configured own_tag_prefix {prefix:?}"
            )));
        }
    }
    Ok(())
}

fn map_dec(e: crate::decimal::DecError) -> BrokerError {
    BrokerError::InvalidRequest(e.to_string())
}

pub fn prepare_order(req: &OrderRequest, asset: &AssetInfo, opts: &PrepareOptions) -> Result<PreparedOrder, BrokerError> {
    let symbol = normalize_symbol(&req.symbol)?;
    if symbol != asset.symbol {
        return Err(BrokerError::InvalidRequest(format!("asset row {} does not match order symbol {symbol}", asset.symbol)));
    }
    if let Some(why) = asset.not_tradable_reason() {
        return Err(BrokerError::PairNotTradable { symbol, status: why });
    }
    if opts.refuse_builtin_assets && asset.source == AssetSource::Builtin {
        return Err(BrokerError::Config(format!(
            "{symbol}: only a built-in (unverified) asset row exists and refuse_builtin_assets is set; call refresh_asset first"
        )));
    }
    if req.validate_only {
        return Err(BrokerError::Unsupported(
            "Alpaca has no validate-only order endpoint; use the paper environment, or `prepare` for a local dry run".into(),
        ));
    }
    if req.reduce_only {
        return Err(BrokerError::Unsupported(
            "reduce_only is not available for Alpaca equities; size the order to the held position".into(),
        ));
    }
    if req.post_only {
        return Err(BrokerError::Unsupported("post_only is not available for Alpaca equities".into()));
    }
    if !req.quantity.is_positive() {
        return Err(BrokerError::InvalidRequest("quantity must be positive".into()));
    }
    validate_tag(&req.tag, opts)?;

    let quantity = asset.round_quantity(req.quantity)?;
    let fractional = quantity != quantity.round_dp(0, Rounding::Floor).map_err(map_dec)?;

    let is_limit = matches!(req.kind, OrderKind::Limit { .. });
    let time_in_force = match (&req.kind, req.time_in_force) {
        (OrderKind::Market, Some(_)) => {
            return Err(BrokerError::InvalidRequest(
                "time_in_force is only configurable on limit orders; market orders are sent as `day`".into(),
            ))
        }
        (OrderKind::Market, None) => "day",
        (OrderKind::Limit { .. }, None) => "day",
        (OrderKind::Limit { .. }, Some(TimeInForce::Gtc)) => "gtc",
        (OrderKind::Limit { .. }, Some(TimeInForce::Ioc)) => "ioc",
    };
    if fractional && time_in_force != "day" {
        return Err(BrokerError::InvalidRequest(format!(
            "{symbol}: a fractional quantity ({quantity}) requires time_in_force `day`, not `{time_in_force}`"
        )));
    }

    let limit_price = match req.kind {
        OrderKind::Market => None,
        OrderKind::Limit { price } => {
            if !price.is_positive() {
                return Err(BrokerError::InvalidPrice("limit price must be positive".into()));
            }
            let increment = match asset.price_increment {
                Some(i) => i,
                None if price >= Dec::from_i64(1) => Dec::new(1, 2).map_err(map_dec)?,
                None => Dec::new(1, 4).map_err(map_dec)?,
            };
            let mode = match req.side {
                Side::Buy => Rounding::Floor,
                Side::Sell => Rounding::Ceil,
            };
            let p = price
                .round_to_multiple(increment, mode)
                .map_err(|e| BrokerError::InvalidPrice(e.to_string()))?
                .normalized();
            if !p.is_positive() {
                return Err(BrokerError::InvalidPrice(format!("limit price {price} rounds to zero")));
            }
            Some(p)
        }
    };

    let mut cost_check_skipped = false;
    if opts.min_notional.is_positive() {
        match limit_price.or(req.reference_price) {
            Some(px) => {
                let cost = quantity.checked_mul(px).ok_or_else(|| BrokerError::InvalidRequest("cost overflow".into()))?;
                if cost < opts.min_notional {
                    return Err(BrokerError::BelowMinCost { symbol, min: opts.min_notional, cost });
                }
            }
            None => cost_check_skipped = true,
        }
    }

    Ok(PreparedOrder {
        symbol,
        side: req.side,
        requested_quantity: req.quantity,
        quantity,
        fractional,
        limit_price,
        time_in_force,
        extended_hours: opts.allow_extended_hours && is_limit && time_in_force == "day",
        client_order_id: req.tag.clone(),
        asset_source: asset.source,
        cost_check_skipped,
    })
}

impl PreparedOrder {
    pub fn is_market(&self) -> bool {
        self.limit_price.is_none()
    }

    /// The `POST /v2/orders` JSON body. Quantities and prices are exact decimal STRINGS.
    pub fn to_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("symbol".into(), json!(self.symbol));
        m.insert("qty".into(), json!(self.quantity.normalized().to_string()));
        m.insert("side".into(), json!(self.side.as_str()));
        m.insert("type".into(), json!(if self.is_market() { "market" } else { "limit" }));
        m.insert("time_in_force".into(), json!(self.time_in_force));
        m.insert("client_order_id".into(), json!(self.client_order_id));
        if let Some(p) = &self.limit_price {
            m.insert("limit_price".into(), json!(p.normalized().to_string()));
        }
        if self.extended_hours {
            m.insert("extended_hours".into(), json!(true));
        }
        Value::Object(m)
    }

    /// Neutral record of what is sent. Alpaca has no integer userref: `userref` is 0 and unused
    /// (the string `client_order_id` is the idempotency key).
    pub fn sent(&self) -> SentOrder {
        SentOrder {
            broker_pair: self.symbol.clone(),
            side: self.side,
            quantity: self.quantity,
            price: self.limit_price,
            userref: 0,
            validate_only: false,
        }
    }
}
