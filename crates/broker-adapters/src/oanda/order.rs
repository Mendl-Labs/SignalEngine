//! Pure order preparation: validation, rounding, time-in-force, position fill and the
//! `POST /v3/accounts/{id}/orders` body.
//!
//! Rules (all deliberate, all tested):
//!
//! * **A market order stays a market order.** The legacy SignalEngine connector turns any signal
//!   that carries a price into a LIMIT/GTC order (VERIFIED-FROM-REPO-CODE,
//!   `build_oanda_order_params`). Here the order type comes ONLY from
//!   [`OrderKind`](crate::types::OrderKind): `reference_price` is never sent and never changes the
//!   type, and a `Limit` request is never turned into a market order either.
//! * **Units are signed integers-as-strings**: positive buys, negative sells (VERIFIED-FROM-REPO-CODE:
//!   the legacy connector's units rule and its `orderFillTransaction.units` parse). Units are
//!   rounded DOWN (toward zero in magnitude) to the instrument's `tradeUnitsPrecision`; a result of
//!   zero, below `minimumTradeSize`, or above `maximumOrderUnits` is refused, never bumped.
//! * **`timeInForce` and `positionFill` are always sent explicitly.** Market: `FOK` by default (the
//!   legacy connector's choice, VERIFIED-FROM-REPO-CODE), `IOC` when asked, `Gtc` refused (a market
//!   order cannot rest). Limit: `GTC` by default, `Gtc`/`Ioc` map to `GTC`/`IOC`. `positionFill` is
//!   `DEFAULT` (netting), or `REDUCE_ONLY` when the request says `reduce_only`.
//! * **Limit prices** are rounded to the instrument's `displayPrecision` in the direction never
//!   worse than requested (buys down, sells up).
//! * **Idempotency key**: `clientExtensions.id` is the caller's tag, verbatim; it is never
//!   truncated or hashed (a changed id would break the lookup-by-tag reconciliation). `clientExtensions.tag`
//!   is the fixed configured marker.
//! * `validate_only` and `post_only` are refused as `Unsupported` (OANDA has neither for these
//!   orders); silently dropping a safety flag is worse than an error.
//!
//! FROM-MEMORY-OF-DOCS: the body shape `{"order": {"type", "instrument", "units", "timeInForce",
//! "positionFill", "price", "clientExtensions": {"id", "tag", "comment"}}}` beyond the fields the
//! legacy connector sends (`type`, `instrument`, `units`, `timeInForce`, `positionFill`, `price`),
//! the `REDUCE_ONLY` fill mode, the 128-character client-id limit, and that `FOK`/`IOC` are the
//! documented time-in-force values of a market order.

use crate::decimal::{Dec, Rounding};
use crate::error::BrokerError;
use crate::oanda::instrument::{canonical_symbol, normalize_instrument, InstrumentInfo};
use crate::types::{OrderKind, OrderRequest, SentOrder, Side, TimeInForce};
use serde_json::{json, Map, Value};

/// OANDA's limit on a client id / tag. MEASURED on a practice account (2026-09-23): 128 characters are accepted and 129
/// are refused with HTTP 400 `CLIENT_ORDER_ID_INVALID`; `: . - _ / space` and a non-ASCII letter are accepted. This
/// adapter is stricter on characters (printable ASCII only) and never truncates or hashes an id.
pub const MAX_CLIENT_ID_LEN: usize = 128;

#[derive(Debug, Clone)]
pub struct PrepareOptions {
    pub own_tag_prefix: Option<String>,
    /// `clientExtensions.tag`.
    pub client_tag: String,
}

impl Default for PrepareOptions {
    fn default() -> Self {
        Self { own_tag_prefix: None, client_tag: crate::oanda::config::DEFAULT_CLIENT_TAG.to_string() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedOrder {
    /// `EUR_USD`.
    pub instrument: String,
    pub side: Side,
    pub requested_quantity: Dec,
    /// Rounded magnitude.
    pub quantity: Dec,
    /// Rounded SIGNED units as sent: negative for a sell.
    pub units: Dec,
    /// `None` for a market order.
    pub limit_price: Option<Dec>,
    /// `FOK`, `IOC` or `GTC`.
    pub time_in_force: &'static str,
    /// `DEFAULT` or `REDUCE_ONLY`.
    pub position_fill: &'static str,
    /// `clientExtensions.id`: our idempotency tag.
    pub client_id: String,
    pub client_tag: String,
}

/// Validate a client id / tag (used for orders and for lookups).
pub fn validate_client_id(tag: &str, own_tag_prefix: Option<&str>) -> Result<(), BrokerError> {
    if tag.is_empty() {
        return Err(BrokerError::InvalidRequest("order tag (clientExtensions.id) must not be empty".into()));
    }
    if tag.len() > MAX_CLIENT_ID_LEN {
        return Err(BrokerError::InvalidRequest(format!(
            "order tag is {} characters; the client id limit is {MAX_CLIENT_ID_LEN}",
            tag.len()
        )));
    }
    if !tag.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
        return Err(BrokerError::InvalidRequest("order tag must be printable ASCII".into()));
    }
    if let Some(prefix) = own_tag_prefix {
        if !tag.starts_with(prefix) {
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

pub fn prepare_order(req: &OrderRequest, info: &InstrumentInfo, opts: &PrepareOptions) -> Result<PreparedOrder, BrokerError> {
    let instrument = normalize_instrument(&req.symbol)?;
    if instrument != info.name {
        return Err(BrokerError::InvalidRequest(format!("instrument row {} does not match order symbol {instrument}", info.name)));
    }
    if req.validate_only {
        return Err(BrokerError::Unsupported(
            "OANDA has no validate-only order endpoint; use the practice environment, or `prepare` for a local dry run".into(),
        ));
    }
    if req.post_only {
        return Err(BrokerError::Unsupported("post_only is not available for OANDA orders".into()));
    }
    if !req.quantity.is_positive() {
        return Err(BrokerError::InvalidRequest("quantity must be positive (the side gives the sign)".into()));
    }
    validate_client_id(&req.tag, opts.own_tag_prefix.as_deref())?;
    let symbol = canonical_symbol(&instrument)?;

    let quantity = req.quantity.round_dp(info.trade_units_precision, Rounding::Floor).map_err(map_dec)?;
    if quantity.is_zero() {
        return Err(BrokerError::QuantityRoundsToZero { symbol, requested: req.quantity, rounded: quantity });
    }
    if quantity < info.minimum_trade_size {
        return Err(BrokerError::BelowMinQuantity { symbol, min: info.minimum_trade_size, rounded: quantity });
    }
    if quantity > info.maximum_order_units {
        return Err(BrokerError::InvalidRequest(format!(
            "{symbol}: {quantity} units exceeds the instrument's maximumOrderUnits {}; refusing to truncate",
            info.maximum_order_units
        )));
    }
    let units = match req.side {
        Side::Buy => quantity,
        Side::Sell => Dec::new(-quantity.units(), quantity.scale()).map_err(map_dec)?,
    };

    let time_in_force = match (&req.kind, req.time_in_force) {
        (OrderKind::Market, None) => "FOK",
        (OrderKind::Market, Some(TimeInForce::Ioc)) => "IOC",
        (OrderKind::Market, Some(TimeInForce::Gtc)) => {
            return Err(BrokerError::InvalidRequest("a market order cannot be GTC; use FOK (default) or IOC".into()))
        }
        (OrderKind::Limit { .. }, None) | (OrderKind::Limit { .. }, Some(TimeInForce::Gtc)) => "GTC",
        (OrderKind::Limit { .. }, Some(TimeInForce::Ioc)) => "IOC",
    };

    let limit_price = match req.kind {
        OrderKind::Market => None,
        OrderKind::Limit { price } => {
            if !price.is_positive() {
                return Err(BrokerError::InvalidPrice("limit price must be positive".into()));
            }
            let mode = match req.side {
                Side::Buy => Rounding::Floor,
                Side::Sell => Rounding::Ceil,
            };
            let p = price.round_dp(info.display_precision, mode).map_err(|e| BrokerError::InvalidPrice(e.to_string()))?;
            if !p.is_positive() {
                return Err(BrokerError::InvalidPrice(format!("limit price {price} rounds to zero")));
            }
            Some(p)
        }
    };

    Ok(PreparedOrder {
        instrument,
        side: req.side,
        requested_quantity: req.quantity,
        quantity,
        units,
        limit_price,
        time_in_force,
        position_fill: if req.reduce_only { "REDUCE_ONLY" } else { "DEFAULT" },
        client_id: req.tag.clone(),
        client_tag: opts.client_tag.clone(),
    })
}

/// Refuse (never truncate) an order that would take the NET position beyond the instrument's `maximumPositionSize`.
///
/// `net_units` is the current signed net position (positive long, negative short) and `order_units` the signed units
/// about to be sent. A missing or zero cap (`maximum_position_size == None`; OANDA's `"0"` means "no cap") never refuses.
/// An order that only shrinks the position on the side it is already on (even from a position that is already over
/// the cap) is always allowed: refusing a reduction would trap the account. Flipping through zero into an over-cap
/// position on the other side is refused.
pub fn check_position_cap(info: &InstrumentInfo, net_units: Dec, order_units: Dec) -> Result<(), BrokerError> {
    let Some(cap) = info.maximum_position_size else { return Ok(()) };
    let after = net_units
        .checked_add(order_units)
        .ok_or_else(|| BrokerError::InvalidRequest(format!("{}: position arithmetic overflowed", info.name)))?;
    let (abs_after, abs_now) = (abs(after), abs(net_units));
    // Allowed even beyond the cap: an order that only shrinks the position on the side it is already on.
    let shrinks_same_side = abs_after <= abs_now && (after.is_zero() || net_units.is_zero() || after.is_negative() == net_units.is_negative());
    if abs_after > cap && !shrinks_same_side {
        return Err(BrokerError::InvalidRequest(format!(
            "{}: the order would take the net position from {net_units} to {after} units, beyond the instrument's maximumPositionSize {cap}; refusing to truncate",
            info.name
        )));
    }
    Ok(())
}

fn abs(d: Dec) -> Dec {
    if d.is_negative() {
        Dec::new(-d.units(), d.scale()).unwrap_or(d)
    } else {
        d
    }
}

impl PreparedOrder {
    pub fn is_market(&self) -> bool {
        self.limit_price.is_none()
    }

    /// The `POST /v3/accounts/{id}/orders` JSON body. Units and prices are exact decimal STRINGS.
    pub fn to_json(&self) -> Value {
        let mut o = Map::new();
        o.insert("type".into(), json!(if self.is_market() { "MARKET" } else { "LIMIT" }));
        o.insert("instrument".into(), json!(self.instrument));
        o.insert("units".into(), json!(self.units.normalized().to_string()));
        o.insert("timeInForce".into(), json!(self.time_in_force));
        o.insert("positionFill".into(), json!(self.position_fill));
        o.insert("clientExtensions".into(), json!({ "id": self.client_id, "tag": self.client_tag }));
        if let Some(p) = &self.limit_price {
            o.insert("price".into(), json!(p.to_string()));
        }
        json!({ "order": Value::Object(o) })
    }

    /// Neutral record of what is sent. OANDA has no integer userref: `userref` is 0 and unused.
    pub fn sent(&self) -> SentOrder {
        SentOrder {
            broker_pair: self.instrument.clone(),
            side: self.side,
            quantity: self.quantity,
            price: self.limit_price,
            userref: 0,
            validate_only: false,
        }
    }
}
