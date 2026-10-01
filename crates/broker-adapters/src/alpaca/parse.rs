//! Parsing of Alpaca JSON responses and classification of HTTP failures. Pure functions.
//!
//! Stance: fail closed. A field we depend on for accounting or safety (`status`, `filled_qty`,
//! `trading_blocked`, `qty` of a position) that is missing or unparseable is `Malformed`, never a
//! silent zero/false. (SignalEngine's parser used `unwrap_or(0.0)` and dropped the executed
//! quantity of canceled orders: verified defects.)
//!
//! Alpaca sends money and quantities as decimal STRINGS (VERIFIED-FROM-REPO-CODE: SignalEngine's
//! `parse_fill_status` comment and tests). They are parsed to `Dec` exactly; JSON numbers are
//! also accepted.

use crate::alpaca::time::parse_rfc3339;
use crate::decimal::Dec;
use crate::error::{BrokerError, ErrorClass, ExchangeError};
use crate::types::{OrderKind, OrderReport, OrderStatus, Side};
use serde_json::Value;

// ---------------------------------------------------------------- scalar helpers

fn dec_from_value(field: &str, v: &Value) -> Result<Dec, BrokerError> {
    let text = match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return Err(BrokerError::Malformed(format!("field `{field}` is not a number"))),
    };
    Dec::parse(&text).map_err(|_| BrokerError::Malformed(format!("field `{field}` is not a decimal")))
}

fn opt_dec(obj: &Value, field: &str) -> Result<Option<Dec>, BrokerError> {
    match obj.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => dec_from_value(field, v).map(Some),
    }
}

fn req_dec(obj: &Value, field: &str) -> Result<Dec, BrokerError> {
    opt_dec(obj, field)?.ok_or_else(|| BrokerError::Malformed(format!("missing field `{field}`")))
}

fn req_bool(obj: &Value, field: &str) -> Result<bool, BrokerError> {
    obj.get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| BrokerError::Malformed(format!("missing or non-boolean field `{field}`")))
}

fn req_str<'a>(obj: &'a Value, field: &str) -> Result<&'a str, BrokerError> {
    obj.get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| BrokerError::Malformed(format!("missing or empty field `{field}`")))
}

fn opt_str(obj: &Value, field: &str) -> Option<String> {
    obj.get(field).and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string)
}

fn json_of(body: &str, what: &str) -> Result<Value, BrokerError> {
    serde_json::from_str(body).map_err(|_| BrokerError::Malformed(format!("{what} body is not JSON")))
}

// ---------------------------------------------------------------- account

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountInfo {
    /// Paper accounts have an account number starting `PA` (FROM-MEMORY-OF-DOCS).
    pub account_number: Option<String>,
    pub status: String,
    pub currency: Option<String>,
    pub cash: Dec,
    pub equity: Dec,
    pub buying_power: Dec,
    pub portfolio_value: Option<Dec>,
    pub last_equity: Option<Dec>,
    pub trading_blocked: bool,
    pub account_blocked: bool,
    pub pattern_day_trader: bool,
    pub transfers_blocked: Option<bool>,
    pub shorting_enabled: Option<bool>,
}

impl AccountInfo {
    /// Why this account must not trade, if it must not.
    pub fn blocked_reason(&self) -> Option<String> {
        let mut why = Vec::new();
        if self.account_blocked {
            why.push("account_blocked=true".to_string());
        }
        if self.trading_blocked {
            why.push("trading_blocked=true".to_string());
        }
        if !self.status.eq_ignore_ascii_case("ACTIVE") {
            why.push(format!("status={}", self.status));
        }
        if why.is_empty() {
            None
        } else {
            Some(why.join(", "))
        }
    }
}

pub fn parse_account(body: &str) -> Result<AccountInfo, BrokerError> {
    let v = json_of(body, "account")?;
    if !v.is_object() {
        return Err(BrokerError::Malformed("account is not an object".into()));
    }
    Ok(AccountInfo {
        account_number: opt_str(&v, "account_number"),
        status: req_str(&v, "status")?.to_string(),
        currency: opt_str(&v, "currency"),
        cash: req_dec(&v, "cash")?,
        equity: req_dec(&v, "equity")?,
        buying_power: req_dec(&v, "buying_power")?,
        portfolio_value: opt_dec(&v, "portfolio_value")?,
        last_equity: opt_dec(&v, "last_equity")?,
        trading_blocked: req_bool(&v, "trading_blocked")?,
        account_blocked: req_bool(&v, "account_blocked")?,
        pattern_day_trader: req_bool(&v, "pattern_day_trader")?,
        transfers_blocked: v.get("transfers_blocked").and_then(Value::as_bool),
        shorting_enabled: v.get("shorting_enabled").and_then(Value::as_bool),
    })
}

// ---------------------------------------------------------------- positions

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionInfo {
    pub symbol: String,
    /// Absolute quantity (fractional allowed); the direction is in `side`.
    pub qty: Dec,
    pub side: PositionSide,
    pub avg_entry_price: Dec,
    pub market_value: Dec,
    pub qty_available: Option<Dec>,
    pub current_price: Option<Dec>,
    pub unrealized_pl: Option<Dec>,
}

impl PositionInfo {
    /// Quantity with sign: negative for a short position.
    pub fn signed_qty(&self) -> Dec {
        match self.side {
            PositionSide::Long => self.qty,
            PositionSide::Short => Dec::new(-self.qty.units(), self.qty.scale()).unwrap_or(self.qty),
        }
    }
}

fn parse_position_value(v: &Value) -> Result<PositionInfo, BrokerError> {
    let side = match req_str(v, "side")? {
        "long" => PositionSide::Long,
        "short" => PositionSide::Short,
        other => return Err(BrokerError::Malformed(format!("unknown position side {other:?}"))),
    };
    let qty = req_dec(v, "qty")?;
    if qty.is_negative() {
        return Err(BrokerError::Malformed("position qty is negative (direction belongs in `side`)".into()));
    }
    Ok(PositionInfo {
        symbol: req_str(v, "symbol")?.to_ascii_uppercase(),
        qty,
        side,
        avg_entry_price: req_dec(v, "avg_entry_price")?,
        market_value: req_dec(v, "market_value")?,
        qty_available: opt_dec(v, "qty_available")?,
        current_price: opt_dec(v, "current_price")?,
        unrealized_pl: opt_dec(v, "unrealized_pl")?,
    })
}

pub fn parse_positions(body: &str) -> Result<Vec<PositionInfo>, BrokerError> {
    let v = json_of(body, "positions")?;
    let arr = v.as_array().ok_or_else(|| BrokerError::Malformed("positions is not an array".into()))?;
    arr.iter().map(parse_position_value).collect()
}

// ---------------------------------------------------------------- clock / calendar

#[derive(Debug, Clone, PartialEq)]
pub struct ClockInfo {
    pub is_open: bool,
    pub timestamp: String,
    pub next_open: String,
    pub next_close: String,
    /// Seconds since the Unix epoch, parsed from the RFC 3339 strings above.
    pub timestamp_epoch: f64,
    pub next_open_epoch: f64,
    pub next_close_epoch: f64,
}

pub fn parse_clock(body: &str) -> Result<ClockInfo, BrokerError> {
    let v = json_of(body, "clock")?;
    let ts = |f: &str| -> Result<(String, f64), BrokerError> {
        let s = req_str(&v, f)?;
        let e = parse_rfc3339(s).ok_or_else(|| BrokerError::Malformed(format!("clock `{f}` is not an RFC 3339 timestamp")))?;
        Ok((s.to_string(), e))
    };
    let (timestamp, timestamp_epoch) = ts("timestamp")?;
    let (next_open, next_open_epoch) = ts("next_open")?;
    let (next_close, next_close_epoch) = ts("next_close")?;
    Ok(ClockInfo { is_open: req_bool(&v, "is_open")?, timestamp, next_open, next_close, timestamp_epoch, next_open_epoch, next_close_epoch })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalendarDay {
    /// `YYYY-MM-DD`.
    pub date: String,
    /// Regular-session open/close, `HH:MM` exchange local time. Early closes show a short `close`.
    pub open: String,
    pub close: String,
    pub session_open: Option<String>,
    pub session_close: Option<String>,
    pub settlement_date: Option<String>,
}

pub fn parse_calendar(body: &str) -> Result<Vec<CalendarDay>, BrokerError> {
    let v = json_of(body, "calendar")?;
    let arr = v.as_array().ok_or_else(|| BrokerError::Malformed("calendar is not an array".into()))?;
    arr.iter()
        .map(|d| {
            Ok(CalendarDay {
                date: req_str(d, "date")?.to_string(),
                open: req_str(d, "open")?.to_string(),
                close: req_str(d, "close")?.to_string(),
                session_open: opt_str(d, "session_open"),
                session_close: opt_str(d, "session_close"),
                settlement_date: opt_str(d, "settlement_date"),
            })
        })
        .collect()
}

// ---------------------------------------------------------------- orders

/// Derive our status from Alpaca's status and quantities.
///
/// * `pending_new`, `accepted`, `accepted_for_bidding` -> `Pending`; `new` -> `Open`;
///   `pending_cancel` / `pending_replace` are still live -> `Open` (or `PartiallyFilled`).
/// * `partially_filled` -> `PartiallyFilled` (needs `filled_qty > 0`), `filled` -> `Filled`.
/// * `canceled` / `expired` / `done_for_day` are classified by how much executed: all -> `Filled`;
///   `0 < filled < qty` -> `PartiallyFilledThenCanceled` / `PartiallyFilledThenExpired` with the
///   executed quantity kept (SignalEngine dropped it: verified defect); nothing -> `Canceled` /
///   `Expired`. `done_for_day` is treated as expiry of a day order.
/// * `rejected` -> `Rejected`.
/// * Anything else (`replaced`, `stopped`, `suspended`, `calculated`, unknown) is `Malformed`:
///   we never guess accounting.
///
/// `filled_qty > qty` is impossible on a healthy broker (a duplicated or corrupt fill report). It
/// is NOT mapped to `Filled` with an inflated quantity, which a caller doing position accounting
/// would double count: it is an `Err(Malformed("overfill anomaly ..."))` in every status (same
/// rule as the Kraken parser), so the caller halts and reconciles against positions.
///
/// `qty` is `None` for an order placed by notional (Alpaca sends `qty: null`); with no requested
/// quantity there is nothing to compare, so the overfill check cannot apply to those orders.
pub fn derive_status(raw: &str, qty: Option<Dec>, executed: Dec) -> Result<OrderStatus, BrokerError> {
    if executed.is_negative() {
        return Err(BrokerError::Malformed("negative filled_qty".into()));
    }
    if let Some(q) = qty {
        if executed > q {
            return Err(BrokerError::Malformed(format!(
                "overfill anomaly: filled_qty {executed} exceeds qty {q} (status {raw:?}); refusing to report an inflated fill"
            )));
        }
    }
    let has_exec = executed.is_positive();
    let live = |has: bool| if has { OrderStatus::PartiallyFilled } else { OrderStatus::Open };
    Ok(match raw {
        "pending_new" | "accepted" | "accepted_for_bidding" => {
            if has_exec {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Pending
            }
        }
        "new" | "pending_cancel" | "pending_replace" => live(has_exec),
        "partially_filled" => {
            if !has_exec {
                return Err(BrokerError::Malformed("status partially_filled with filled_qty 0".into()));
            }
            OrderStatus::PartiallyFilled
        }
        "filled" => {
            if !has_exec {
                return Err(BrokerError::Malformed("status filled with filled_qty 0".into()));
            }
            if let Some(q) = qty {
                if executed < q {
                    return Err(BrokerError::Malformed("status filled but filled_qty < qty".into()));
                }
            }
            OrderStatus::Filled
        }
        "canceled" | "expired" | "done_for_day" => {
            let expiry = raw != "canceled";
            match qty {
                Some(q) if executed >= q => OrderStatus::Filled,
                _ if has_exec => {
                    if expiry {
                        OrderStatus::PartiallyFilledThenExpired
                    } else {
                        OrderStatus::PartiallyFilledThenCanceled
                    }
                }
                _ => {
                    if expiry {
                        OrderStatus::Expired
                    } else {
                        OrderStatus::Canceled
                    }
                }
            }
        }
        "rejected" => {
            if has_exec {
                return Err(BrokerError::Malformed("status rejected with filled_qty > 0".into()));
            }
            OrderStatus::Rejected
        }
        other => return Err(BrokerError::Malformed(format!("unsupported order status {other:?}"))),
    })
}

/// Raw `client_order_id` of an order object.
pub fn client_order_id_of(v: &Value) -> Option<String> {
    opt_str(v, "client_order_id")
}

/// Parse one order object. `own_tag_prefix`: when set, `OrderReport::tag` is only filled for
/// orders whose client_order_id starts with it.
///
/// For an order placed by NOTIONAL (`qty` null) the requested quantity is unknown: `quantity`
/// mirrors `filled_qty` (zero until something fills).
pub fn parse_order_value(v: &Value, own_tag_prefix: Option<&str>) -> Result<OrderReport, BrokerError> {
    if !v.is_object() {
        return Err(BrokerError::Malformed("order is not an object".into()));
    }
    let id = req_str(v, "id")?.to_string();
    let raw_status = req_str(v, "status").map_err(|_| BrokerError::Malformed(format!("order {id}: missing status")))?.to_string();
    let qty = opt_dec(v, "qty")?;
    let executed_quantity = req_dec(v, "filled_qty").map_err(|_| BrokerError::Malformed(format!("order {id}: missing filled_qty")))?;
    let status = derive_status(&raw_status, qty, executed_quantity).map_err(|e| match e {
        BrokerError::Malformed(m) => BrokerError::Malformed(format!("order {id}: {m}")),
        other => other,
    })?;

    let side = match v.get("side").and_then(Value::as_str) {
        Some("buy") => Some(Side::Buy),
        Some("sell") => Some(Side::Sell),
        _ => None,
    };
    let type_str = v.get("type").or_else(|| v.get("order_type")).and_then(Value::as_str);
    let kind = match type_str {
        Some("market") => Some(OrderKind::Market),
        Some("limit") => match opt_dec(v, "limit_price")? {
            Some(price) if price.is_positive() => Some(OrderKind::Limit { price }),
            _ => None,
        },
        _ => None,
    };
    let avg_price = if executed_quantity.is_positive() {
        opt_dec(v, "filled_avg_price")?.filter(|p| p.is_positive())
    } else {
        None
    };
    let cost = match avg_price {
        Some(p) => executed_quantity.checked_mul(p),
        None => None,
    };
    let cid = client_order_id_of(v);
    let tag = match (&cid, own_tag_prefix) {
        (Some(c), Some(prefix)) if c.starts_with(prefix) => Some(c.clone()),
        (Some(_), Some(_)) => None,
        (Some(c), None) => Some(c.clone()),
        (None, _) => None,
    };
    let reason = opt_str(v, "reject_reason").or_else(|| opt_str(v, "reason")).or_else(|| {
        (status == OrderStatus::Rejected).then(|| "rejected by Alpaca (the order object carries no reject reason)".to_string())
    });
    // Prefer the timestamp that matches how the order ended (a canceled partial fill also has an
    // earlier `filled_at`), then fall back to any of them.
    let preferred = match status {
        OrderStatus::Filled => "filled_at",
        OrderStatus::Canceled | OrderStatus::PartiallyFilledThenCanceled => "canceled_at",
        OrderStatus::Expired | OrderStatus::PartiallyFilledThenExpired => "expired_at",
        OrderStatus::Rejected => "failed_at",
        _ => "",
    };
    let close_at = std::iter::once(preferred)
        .chain(["filled_at", "canceled_at", "expired_at", "failed_at"])
        .filter(|f| !f.is_empty())
        .filter_map(|f| opt_str(v, f))
        .find_map(|s| parse_rfc3339(&s));

    Ok(OrderReport {
        broker_order_id: id,
        userref: None,
        tag,
        symbol: opt_str(v, "symbol").unwrap_or_default().to_ascii_uppercase(),
        side,
        kind,
        status,
        raw_status,
        reason,
        quantity: qty.unwrap_or(executed_quantity),
        executed_quantity,
        avg_price,
        cost,
        fee: None,
        open_time: opt_str(v, "submitted_at").or_else(|| opt_str(v, "created_at")).and_then(|s| parse_rfc3339(&s)),
        close_time: if status.is_terminal() { close_at } else { None },
    })
}

pub fn parse_order(body: &str, own_tag_prefix: Option<&str>) -> Result<OrderReport, BrokerError> {
    parse_order_value(&json_of(body, "order")?, own_tag_prefix)
}

pub fn parse_orders(body: &str, own_tag_prefix: Option<&str>) -> Result<Vec<OrderReport>, BrokerError> {
    let v = json_of(body, "orders")?;
    let arr = v.as_array().ok_or_else(|| BrokerError::Malformed("orders is not an array".into()))?;
    arr.iter().map(|o| parse_order_value(o, own_tag_prefix)).collect()
}

// ---------------------------------------------------------------- flatten (multi-status)

#[derive(Debug, Clone, PartialEq)]
pub struct FlattenEntry {
    pub symbol: String,
    /// Per-position HTTP status inside the 207 body.
    pub http_status: u16,
    /// The closing order, when that position's close was accepted.
    pub order: Option<OrderReport>,
    /// The broker's message, when it was not.
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct FlattenReport {
    pub entries: Vec<FlattenEntry>,
}

impl FlattenReport {
    /// True when every position's close order was accepted (an empty report, meaning no open
    /// positions, is also ok). This only says the close ORDERS were accepted: verify flat with
    /// `get_positions` after they fill.
    pub fn all_ok(&self) -> bool {
        self.entries.iter().all(|e| (200..300).contains(&e.http_status) && e.order.is_some())
    }
    pub fn failed(&self) -> Vec<&FlattenEntry> {
        self.entries.iter().filter(|e| !((200..300).contains(&e.http_status) && e.order.is_some())).collect()
    }
}

pub fn parse_flatten(body: &str, own_tag_prefix: Option<&str>) -> Result<FlattenReport, BrokerError> {
    let v = json_of(body, "close-all-positions")?;
    let arr = v.as_array().ok_or_else(|| BrokerError::Malformed("close-all-positions body is not an array".into()))?;
    let mut entries = Vec::with_capacity(arr.len());
    for item in arr {
        let symbol = req_str(item, "symbol")?.to_ascii_uppercase();
        let status = item
            .get("status")
            .and_then(Value::as_u64)
            .and_then(|s| u16::try_from(s).ok())
            .ok_or_else(|| BrokerError::Malformed(format!("close result for {symbol}: missing numeric status")))?;
        let body_v = item.get("body").cloned().unwrap_or(Value::Null);
        if (200..300).contains(&status) {
            let order = parse_order_value(&body_v, own_tag_prefix)?;
            entries.push(FlattenEntry { symbol, http_status: status, order: Some(order), error: None });
        } else {
            let msg = body_v.get("message").and_then(Value::as_str).unwrap_or("no message").to_string();
            entries.push(FlattenEntry { symbol, http_status: status, order: None, error: Some(msg) });
        }
    }
    Ok(FlattenReport { entries })
}

// ---------------------------------------------------------------- HTTP failure classification

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub code: Option<String>,
    pub message: String,
}

const MAX_MESSAGE_CHARS: usize = 300;

/// Alpaca error bodies are `{"code": 40310000, "message": "..."}` (FROM-MEMORY-OF-DOCS). Any other
/// body is kept as (truncated) text.
pub fn parse_error_body(body: &str) -> ApiError {
    let trunc = |s: &str| -> String { s.trim().chars().take(MAX_MESSAGE_CHARS).collect() };
    match serde_json::from_str::<Value>(body) {
        Ok(v) if v.is_object() => {
            let code = match v.get("code") {
                Some(Value::Number(n)) => Some(n.to_string()),
                Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
                _ => None,
            };
            let message = v.get("message").and_then(Value::as_str).map(trunc).unwrap_or_else(|| trunc(body));
            ApiError { code, message }
        }
        _ => ApiError { code: None, message: trunc(body) },
    }
}

/// What an unsuccessful HTTP response means. Whether the ORDER outcome is unknown is decided by
/// the caller (placement treats `Server` and `Other` as unknown, everything else as definite).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpFailure {
    /// 401, or a 403 that is not an order-level denial.
    Auth { status: u16, api: ApiError },
    /// 429. `retry_after_secs` from the `Retry-After` header when it is a whole number of seconds.
    RateLimited { retry_after_secs: Option<u64>, api: ApiError },
    NotFound { api: ApiError },
    /// A definite business rejection: 422 (bad/unsupported order), or 403 for insufficient buying
    /// power or shares.
    Rejected { status: u16, class: ErrorClass, api: ApiError },
    /// 422 saying the client_order_id already exists: an order with our tag EXISTS.
    DuplicateClientOrderId { api: ApiError },
    /// 5xx: the request may or may not have been processed.
    Server { status: u16, api: ApiError },
    /// Any other status.
    Other { status: u16, api: ApiError },
}

fn has_any(msg: &str, needles: &[&str]) -> bool {
    let m = msg.to_ascii_lowercase();
    needles.iter().any(|n| m.contains(n))
}

const FUNDS_PATTERNS: [&str; 4] = ["insufficient buying power", "insufficient qty", "insufficient day trading buying power", "insufficient balance"];
const DENIAL_PATTERNS: [&str; 4] = ["pattern day trad", "trade denied", "cannot be sold short", "not shortable"];
const ARGUMENT_PATTERNS: [&str; 7] = ["fractional", "fractionable", "must be", "invalid", "required", "not recognized", "unprocessable"];

/// Classify the message of a definite rejection.
pub fn classify_rejection(message: &str) -> ErrorClass {
    if has_any(message, &FUNDS_PATTERNS) {
        ErrorClass::InsufficientFunds
    } else if has_any(message, &ARGUMENT_PATTERNS) {
        ErrorClass::InvalidArguments
    } else {
        ErrorClass::OrderRejected
    }
}

pub fn classify_http_failure(status: u16, body: &str, retry_after: Option<&str>) -> HttpFailure {
    let api = parse_error_body(body);
    match status {
        401 => HttpFailure::Auth { status, api },
        // Alpaca answers 403 both for bad credentials and for order-level denials such as
        // insufficient buying power (FROM-MEMORY-OF-DOCS). Order-level denials are definite
        // rejections; everything else on 403 is an auth failure.
        403 if has_any(&api.message, &FUNDS_PATTERNS) => {
            HttpFailure::Rejected { status, class: ErrorClass::InsufficientFunds, api }
        }
        403 if has_any(&api.message, &DENIAL_PATTERNS) => HttpFailure::Rejected { status, class: ErrorClass::OrderRejected, api },
        403 => HttpFailure::Auth { status, api },
        404 => HttpFailure::NotFound { api },
        422 if has_any(&api.message, &["client_order_id must be unique", "client_order_id already exists"]) => {
            HttpFailure::DuplicateClientOrderId { api }
        }
        422 => {
            let class = classify_rejection(&api.message);
            HttpFailure::Rejected { status, class, api }
        }
        429 => HttpFailure::RateLimited {
            retry_after_secs: retry_after.and_then(|v| v.trim().parse::<u64>().ok()),
            api,
        },
        500..=599 => HttpFailure::Server { status, api },
        _ => HttpFailure::Other { status, api },
    }
}

impl ApiError {
    /// `alpaca:<status> <code>: <message>` (the code part is omitted when the body had none).
    pub fn code_string(&self, status: u16) -> String {
        match &self.code {
            Some(c) => format!("alpaca:{status} {c}: {}", self.message),
            None => format!("alpaca:{status}: {}", self.message),
        }
    }
}

impl HttpFailure {
    pub fn status(&self) -> u16 {
        match self {
            HttpFailure::Auth { status, .. }
            | HttpFailure::Rejected { status, .. }
            | HttpFailure::Server { status, .. }
            | HttpFailure::Other { status, .. } => *status,
            HttpFailure::RateLimited { .. } => 429,
            HttpFailure::NotFound { .. } => 404,
            HttpFailure::DuplicateClientOrderId { .. } => 422,
        }
    }

    /// The exchange-style error for a DEFINITE failure. `None` for outcomes that carry no
    /// classified error (5xx, unexpected status, 429, 404).
    pub fn exchange_error(&self) -> Option<ExchangeError> {
        match self {
            HttpFailure::Auth { status, api } => Some(ExchangeError { code: api.code_string(*status), class: ErrorClass::Auth }),
            HttpFailure::Rejected { status, class, api } => Some(ExchangeError { code: api.code_string(*status), class: *class }),
            HttpFailure::DuplicateClientOrderId { api } => {
                Some(ExchangeError { code: api.code_string(422), class: ErrorClass::OrderRejected })
            }
            _ => None,
        }
    }

    /// Conversion for read-style calls (everything except order placement).
    pub fn into_broker_error(self) -> BrokerError {
        match self {
            HttpFailure::RateLimited { retry_after_secs, api } => BrokerError::RateLimited { retry_after_secs, message: api.message },
            HttpFailure::NotFound { api } => BrokerError::NotFound(api.message),
            HttpFailure::Server { status, .. } | HttpFailure::Other { status, .. } => BrokerError::Http(status),
            other => match other.exchange_error() {
                Some(e) => BrokerError::Exchange(vec![e]),
                None => BrokerError::Http(other.status()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Dec {
        Dec::parse(s).unwrap()
    }

    #[test]
    fn status_table() {
        let (q, z, half) = (d("2"), d("0"), d("1"));
        let cases: [(&str, Dec, OrderStatus); 21] = [
            ("pending_new", z, OrderStatus::Pending),
            ("accepted", z, OrderStatus::Pending),
            ("accepted_for_bidding", z, OrderStatus::Pending),
            ("new", z, OrderStatus::Open),
            ("new", half, OrderStatus::PartiallyFilled),
            ("pending_cancel", z, OrderStatus::Open),
            ("pending_cancel", half, OrderStatus::PartiallyFilled),
            ("partially_filled", half, OrderStatus::PartiallyFilled),
            ("filled", q, OrderStatus::Filled),
            ("canceled", z, OrderStatus::Canceled),
            ("canceled", half, OrderStatus::PartiallyFilledThenCanceled),
            ("canceled", q, OrderStatus::Filled),
            ("expired", z, OrderStatus::Expired),
            ("expired", half, OrderStatus::PartiallyFilledThenExpired),
            ("expired", q, OrderStatus::Filled),
            ("done_for_day", z, OrderStatus::Expired),
            ("done_for_day", half, OrderStatus::PartiallyFilledThenExpired),
            ("done_for_day", q, OrderStatus::Filled),
            ("rejected", z, OrderStatus::Rejected),
            ("pending_replace", z, OrderStatus::Open),
            ("accepted", half, OrderStatus::PartiallyFilled),
        ];
        for (raw, exec, want) in cases {
            assert_eq!(derive_status(raw, Some(q), exec).unwrap(), want, "{raw} exec={exec}");
        }
    }

    #[test]
    fn inconsistent_or_unknown_status_fails_closed() {
        let q = Some(d("2"));
        for (raw, exec) in [
            ("filled", d("0")),
            ("filled", d("1")),
            ("partially_filled", d("0")),
            ("rejected", d("1")),
            ("replaced", d("0")),
            ("stopped", d("0")),
            ("suspended", d("0")),
            ("calculated", d("0")),
            ("bogus", d("0")),
            ("new", d("-1")),
        ] {
            assert!(derive_status(raw, q, exec).is_err(), "{raw} {exec}");
        }
    }

    fn is_overfill(e: &BrokerError) -> bool {
        matches!(e, BrokerError::Malformed(m) if m.contains("overfill anomaly"))
    }

    #[test]
    fn overfill_is_an_anomaly_in_every_status_never_an_inflated_filled() {
        let q = Some(d("2"));
        for raw in [
            "pending_new",
            "accepted",
            "accepted_for_bidding",
            "new",
            "pending_cancel",
            "pending_replace",
            "partially_filled",
            "filled",
            "canceled",
            "expired",
            "done_for_day",
            "rejected",
        ] {
            let e = derive_status(raw, q, d("2.5")).unwrap_err();
            assert!(is_overfill(&e), "{raw}: {e:?}");
        }
    }

    #[test]
    fn the_smallest_overfill_is_caught_and_an_exact_fill_is_not() {
        let q = Some(d("1.5"));
        assert!(is_overfill(&derive_status("filled", q, d("1.50000001")).unwrap_err()));
        assert!(is_overfill(&derive_status("canceled", q, d("1.50000001")).unwrap_err()));
        // equality is exact-decimal: scale does not matter
        assert_eq!(derive_status("filled", q, d("1.500")).unwrap(), OrderStatus::Filled);
        assert_eq!(derive_status("canceled", q, d("1.5")).unwrap(), OrderStatus::Filled);
        assert_eq!(derive_status("expired", q, d("1.49999999")).unwrap(), OrderStatus::PartiallyFilledThenExpired);
    }

    #[test]
    fn overfill_cannot_be_judged_for_notional_orders_and_stays_unchanged() {
        for raw in ["filled", "canceled", "partially_filled", "new"] {
            assert!(derive_status(raw, None, d("1000000")).is_ok(), "{raw}");
        }
    }

    #[test]
    fn parse_order_value_rejects_an_overfilled_order_and_names_it() {
        let order = |status: &str, qty: &str, filled: &str| {
            serde_json::json!({
                "id": "61e69015-8549-4bfd-b9c3-01e75843f47d", "client_order_id": "t", "status": status,
                "symbol": "SPY", "side": "buy", "type": "market", "qty": qty, "filled_qty": filled,
                "filled_avg_price": "500.0"
            })
        };
        let e = parse_order_value(&order("filled", "1.5", "3"), None).unwrap_err();
        match &e {
            BrokerError::Malformed(m) => {
                assert!(m.contains("61e69015-8549-4bfd-b9c3-01e75843f47d") && m.contains("overfill anomaly"), "{m}");
            }
            other => panic!("{other:?}"),
        }
        assert!(is_overfill(&parse_order_value(&order("canceled", "1.5", "1.6"), None).unwrap_err()));
        let ok = parse_order_value(&order("filled", "1.5", "1.5"), None).unwrap();
        assert_eq!((ok.status, ok.executed_quantity), (OrderStatus::Filled, d("1.5")));
    }

    #[test]
    fn notional_orders_have_no_qty_to_compare() {
        assert_eq!(derive_status("canceled", None, d("0.4")).unwrap(), OrderStatus::PartiallyFilledThenCanceled);
        assert_eq!(derive_status("filled", None, d("0.4")).unwrap(), OrderStatus::Filled);
    }

    #[test]
    fn http_failure_classification_table() {
        use HttpFailure::*;
        let f = |s, b: &str| classify_http_failure(s, b, None);
        assert!(matches!(f(401, r#"{"code":40110000,"message":"request is not authorized"}"#), Auth { .. }));
        assert!(matches!(f(403, r#"{"message":"forbidden."}"#), Auth { .. }));
        assert!(matches!(
            f(403, r#"{"code":40310000,"message":"insufficient buying power"}"#),
            Rejected { class: ErrorClass::InsufficientFunds, .. }
        ));
        assert!(matches!(f(403, r#"{"message":"trade denied due to pattern day trading protection"}"#), Rejected { class: ErrorClass::OrderRejected, .. }));
        assert!(matches!(f(404, r#"{"code":40410000,"message":"order not found"}"#), NotFound { .. }));
        assert!(matches!(f(422, r#"{"code":42210000,"message":"asset \"X\" is not fractionable"}"#), Rejected { class: ErrorClass::InvalidArguments, .. }));
        assert!(matches!(f(422, r#"{"message":"insufficient qty available for order"}"#), Rejected { class: ErrorClass::InsufficientFunds, .. }));
        assert!(matches!(f(422, r#"{"message":"client_order_id must be unique"}"#), DuplicateClientOrderId { .. }));
        assert!(matches!(f(422, r#"{"message":"something else"}"#), Rejected { class: ErrorClass::OrderRejected, .. }));
        assert!(matches!(f(500, "oops"), Server { status: 500, .. }));
        assert!(matches!(f(504, "<html>"), Server { status: 504, .. }));
        assert!(matches!(f(418, ""), Other { status: 418, .. }));
    }

    #[test]
    fn retry_after_only_accepts_whole_seconds() {
        let r = |h: Option<&str>| match classify_http_failure(429, "{}", h) {
            HttpFailure::RateLimited { retry_after_secs, .. } => retry_after_secs,
            other => panic!("{other:?}"),
        };
        assert_eq!(r(Some("30")), Some(30));
        assert_eq!(r(Some(" 2 ")), Some(2));
        assert_eq!(r(Some("Wed, 21 Oct 2026 07:28:00 GMT")), None);
        assert_eq!(r(None), None);
    }

    #[test]
    fn error_body_is_truncated_and_non_json_survives() {
        let long = "x".repeat(1000);
        assert_eq!(parse_error_body(&long).message.len(), MAX_MESSAGE_CHARS);
        assert_eq!(parse_error_body("<html>Bad Gateway</html>").code, None);
        assert_eq!(parse_error_body(r#"{"code":"E1","message":"m"}"#).code.as_deref(), Some("E1"));
    }
}
