//! Order request shape for Bitstamp (documented endpoints, https://www.bitstamp.net/api/; UNVERIFIED items are marked).
//!
//! Documented endpoints used: `POST /api/v2/buy/{market_symbol}/` (limit: `price`, `amount`, `client_order_id`) and
//! `POST /api/v2/buy/market/{market_symbol}/` (market: `amount`, `client_order_id`), with the sell equivalents.
//! Market symbols are lowercase with no separator (`btcusd`, `ethusd`), per the documented examples.
//!
//! The documentation page states no validate-only or test mode for orders, so `validate_only` is refused. Post-only,
//! reduce-only and time-in-force have no documented parameter here, so they are refused rather than silently dropped.
//!
//! This module only BUILDS the request. [`super::BitstampAdapter::place_order`] never sends it: order placement is
//! disabled until a paper decision is made.

use crate::error::BrokerError;
use crate::types::{OrderKind, OrderRequest, Side};

/// Canonical symbol to Bitstamp market symbol. Only the two pairs in scope are allowed.
pub fn market_symbol(canonical: &str) -> Result<&'static str, BrokerError> {
    match canonical {
        "BTC/USD" => Ok("btcusd"),
        "ETH/USD" => Ok("ethusd"),
        other => Err(BrokerError::UnknownSymbol(other.to_string())),
    }
}

/// Maximum amount precision sent. UNVERIFIED: the documentation page does not state per-pair precision.
pub const MAX_AMOUNT_DECIMALS: u32 = 8;

/// A built, unsent order request: the documented path and form parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderWire {
    pub path: String,
    pub params: Vec<(String, String)>,
}

fn side_path(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

pub fn build_order(req: &OrderRequest) -> Result<OrderWire, BrokerError> {
    if req.validate_only {
        return Err(BrokerError::Unsupported("validate-only: the Bitstamp documentation states no validate mode".into()));
    }
    if req.post_only || req.reduce_only || req.time_in_force.is_some() {
        return Err(BrokerError::Unsupported(
            "post-only, reduce-only and time-in-force have no documented Bitstamp parameter".into(),
        ));
    }
    let market = market_symbol(&req.symbol)?;
    if req.tag.trim().is_empty() {
        return Err(BrokerError::InvalidRequest("order tag is empty".into()));
    }
    if !req.quantity.is_positive() {
        return Err(BrokerError::InvalidRequest("order quantity must be positive".into()));
    }
    if req.quantity.decimals() > MAX_AMOUNT_DECIMALS {
        return Err(BrokerError::InvalidRequest(format!("quantity has more than {MAX_AMOUNT_DECIMALS} decimals")));
    }
    let amount = req.quantity.to_fixed(MAX_AMOUNT_DECIMALS).map_err(|e| BrokerError::InvalidRequest(e.to_string()))?;
    let side = side_path(req.side);
    let client_order_id = ("client_order_id".to_string(), req.tag.clone());
    match req.kind {
        OrderKind::Market => Ok(OrderWire {
            path: format!("/api/v2/{side}/market/{market}/"),
            params: vec![("amount".to_string(), amount), client_order_id],
        }),
        OrderKind::Limit { price } => {
            if !price.is_positive() {
                return Err(BrokerError::InvalidPrice("limit price must be positive".into()));
            }
            Ok(OrderWire {
                path: format!("/api/v2/{side}/{market}/"),
                params: vec![("price".to_string(), price.to_string()), ("amount".to_string(), amount), client_order_id],
            })
        }
    }
}
