//! Response interpretation for the Bitstamp v2 API. Pure: every function takes already-received bytes.
//!
//! Documented error shape (https://www.bitstamp.net/api/): `{"response_code": "...", "response_explanation": "..."}`.
//! Only `response_code` is carried into the error; the explanation is free text and is dropped. The HTTP status for
//! a rejected request is not on the documentation page, so any status with a `response_code` body is an exchange
//! rejection, and a bare status without one is classified by its code (401/403 auth, 429 rate limit, 5xx outcome
//! unknown).

use serde_json::Value;

use crate::decimal::Dec;
use crate::error::{BrokerError, ErrorClass, ExchangeError};
use crate::types::{BalanceEntry, BalanceKind, Balances, CancelOutcome, OrderReport, OrderStatus};

pub(crate) fn malformed(what: &str) -> BrokerError {
    BrokerError::Malformed(what.to_string())
}

/// Interpret one HTTP response. Returns the parsed JSON on success.
pub fn interpret(status: u16, body: &str) -> Result<Value, BrokerError> {
    let value: Option<Value> = serde_json::from_str(body).ok();
    if let Some(Value::Object(map)) = &value {
        if let Some(code) = map.get("response_code") {
            return Err(BrokerError::Exchange(vec![ExchangeError { code: code_text(code), class: ErrorClass::Other }]));
        }
    }
    match status {
        200 => value.ok_or_else(|| malformed("response body is not JSON")),
        429 => Err(BrokerError::RateLimited { retry_after_secs: None, message: "HTTP 429".to_string() }),
        401 | 403 => {
            Err(BrokerError::Exchange(vec![ExchangeError { code: format!("HTTP_{status}"), class: ErrorClass::Auth }]))
        }
        500..=599 => Err(BrokerError::Exchange(vec![ExchangeError {
            code: format!("HTTP_{status}"),
            class: ErrorClass::ServiceUnavailable,
        }])),
        other => Err(BrokerError::Http(other)),
    }
}

fn code_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => "UNKNOWN".to_string(),
    }
}

/// A field that is a JSON string or a JSON number, as text.
pub(crate) fn text_field(v: &Value, name: &str) -> Result<String, BrokerError> {
    match v.get(name) {
        Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
        Some(Value::Number(n)) => Ok(n.to_string()),
        _ => Err(malformed(&format!("missing or empty field `{name}`"))),
    }
}

/// A decimal field carried as a JSON string (Bitstamp's convention in the documented examples).
pub(crate) fn dec_field(v: &Value, name: &str) -> Result<Dec, BrokerError> {
    let text = text_field(v, name)?;
    Dec::parse(&text).map_err(|_| malformed(&format!("field `{name}` is not a decimal")))
}

/// `a - b` for non-negative decimals, or `None` when it would be negative or overflow.
pub(crate) fn dec_sub(a: Dec, b: Dec) -> Option<Dec> {
    let scale = a.scale().max(b.scale());
    let ua = a.units().checked_mul(10i128.checked_pow(scale - a.scale())?)?;
    let ub = b.units().checked_mul(10i128.checked_pow(scale - b.scale())?)?;
    let diff = ua.checked_sub(ub)?;
    if diff < 0 {
        return None;
    }
    Dec::new(diff, scale).ok()
}

/// `POST /api/v2/account_balances/`: an array of `{currency, total, available, reserved}`. The `Spot` entry carries
/// `available` (the documented free balance; `reserved` is held by open orders and is not exposed here).
pub fn parse_balances(v: &Value) -> Result<Balances, BrokerError> {
    let items = v.as_array().ok_or_else(|| malformed("account_balances is not an array"))?;
    let mut entries = Vec::with_capacity(items.len());
    for item in items {
        let currency = text_field(item, "currency")?;
        let available = dec_field(item, "available")?;
        if available.is_negative() {
            return Err(malformed("a balance is negative"));
        }
        entries.push(BalanceEntry {
            asset: currency.to_uppercase(),
            raw_asset: currency,
            amount: available,
            kind: BalanceKind::Spot,
        });
    }
    Ok(Balances { entries })
}

/// `POST /api/v2/open_orders/`: an array. `type` is not mapped to a side, because the documentation page does not state
/// its codes (UNVERIFIED), so `side` is `None`. `amount` is taken as the remaining quantity (UNVERIFIED).
pub fn parse_open_orders(v: &Value) -> Result<Vec<OrderReport>, BrokerError> {
    let items = v.as_array().ok_or_else(|| malformed("open_orders is not an array"))?;
    items.iter().map(parse_open_order).collect()
}

fn parse_open_order(v: &Value) -> Result<OrderReport, BrokerError> {
    let id = text_field(v, "id")?;
    let symbol = text_field(v, "market")?;
    let quantity = dec_field(v, "amount_at_create")?;
    let remaining = dec_field(v, "amount")?;
    let executed = dec_sub(quantity, remaining).ok_or_else(|| malformed("remaining amount exceeds the original"))?;
    let status = if executed.is_zero() { OrderStatus::Open } else { OrderStatus::PartiallyFilled };
    Ok(OrderReport {
        broker_order_id: id.clone(),
        userref: None,
        tag: None,
        symbol,
        side: None,
        kind: None,
        status,
        raw_status: "open_orders".to_string(),
        reason: None,
        quantity,
        executed_quantity: executed,
        avg_price: None,
        cost: None,
        fee: None,
        open_time: None,
        close_time: None,
    })
}

/// `POST /api/v2/order_status/`. Only the status `"Open"` is confirmed by the documentation page; any other status is
/// refused as malformed rather than guessed. The documented fields do not carry the original order quantity, so a
/// report is built only for an order with no transactions (nothing executed), where `amount_remaining` is the whole
/// quantity. An order with transactions is refused: its executed quantity cannot be derived from documented fields.
pub fn parse_order_status(v: &Value) -> Result<OrderReport, BrokerError> {
    let id = text_field(v, "id")?;
    let status_text = text_field(v, "status")?;
    if status_text != "Open" {
        return Err(malformed("order status is not in the documented set (only \"Open\" is confirmed)"));
    }
    let transactions = v.get("transactions").and_then(Value::as_array).map(Vec::len).unwrap_or(0);
    if transactions > 0 {
        return Err(BrokerError::Unsupported(
            "order has transactions; executed quantity is not derivable from the documented order_status fields".into(),
        ));
    }
    let remaining = dec_field(v, "amount_remaining")?;
    Ok(OrderReport {
        broker_order_id: id,
        userref: None,
        tag: None,
        symbol: text_field(v, "market")?,
        side: None,
        kind: None,
        status: OrderStatus::Open,
        raw_status: status_text,
        reason: None,
        quantity: remaining,
        executed_quantity: Dec::ZERO,
        avg_price: None,
        cost: None,
        fee: None,
        open_time: None,
        close_time: None,
    })
}

/// `POST /api/v2/cancel_order/`: the documented example answers with `"status": "Canceled"`. Anything else is refused.
pub fn parse_cancel(v: &Value) -> Result<CancelOutcome, BrokerError> {
    match text_field(v, "status")?.as_str() {
        "Canceled" => Ok(CancelOutcome { canceled_count: 1, pending: false }),
        _ => Err(malformed("cancel status is not in the documented set (only \"Canceled\" is confirmed)")),
    }
}
