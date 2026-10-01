//! Parsing of OANDA v20 JSON responses and classification of HTTP failures. Pure functions.
//!
//! Stance: fail closed (same as the Alpaca parser). A field we depend on for accounting or safety
//! (`state`, `units`, `balance`, `NAV`, `hedgingEnabled`, a fill's `price`) that is missing or
//! unparseable is `Malformed`, never a silent zero. OANDA sends amounts, units and prices as
//! decimal STRINGS; they are parsed to `Dec` exactly (JSON numbers are accepted too).
//!
//! PROVENANCE. Shapes MEASURED on an OANDA practice account on 2026-09-23 (recorded, sanitised responses under
//! `tests/fixtures/oanda/real/`): the order-create response (market fill, resting limit, short sale), the pending-order
//! lookup and its 404, cancel and cancel-again, position close (long, short, one-sided, nothing open), the reject
//! shapes, the unknown-instrument `InvalidParameterException`, and `transactions/sinceid`. Everything else here
//! (the account summary, instruments, pricing and flat/never-traded positions were recorded in a second session; still authored:
//! held positions, order resources of FILLED/CANCELLED orders read by numeric
//! id, `GET /transactions/{id}`, held-position bodies, cancel-at-creation bodies, 401/403/429/5xx bodies) is still FROM-MEMORY-OF-DOCS and is
//! exercised only by hand-authored fixtures (labelled as such; see `tests/fixtures/oanda/README.md`).
//! The legacy connector's parse of `orderCreateTransaction.id` / `orderFillTransaction.{id, units, price}` is
//! VERIFIED-FROM-REPO-CODE and now also matches the recordings.

use crate::decimal::Dec;
use crate::error::{BrokerError, ErrorClass, ExchangeError};
use crate::oanda::instrument::normalize_instrument;
use crate::types::{OrderKind, OrderReport, OrderStatus, Side};
use serde_json::Value;

// ---------------------------------------------------------------- scalar helpers

pub(crate) fn dec_from_value(field: &str, v: &Value) -> Result<Dec, BrokerError> {
    let text = match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return Err(BrokerError::Malformed(format!("field `{field}` is not a number"))),
    };
    Dec::parse(&text).map_err(|_| BrokerError::Malformed(format!("field `{field}` is not a decimal")))
}

pub(crate) fn opt_dec(obj: &Value, field: &str) -> Result<Option<Dec>, BrokerError> {
    match obj.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => dec_from_value(field, v).map(Some),
    }
}

pub(crate) fn req_dec(obj: &Value, field: &str) -> Result<Dec, BrokerError> {
    opt_dec(obj, field)?.ok_or_else(|| BrokerError::Malformed(format!("missing field `{field}`")))
}

pub(crate) fn req_bool(obj: &Value, field: &str) -> Result<bool, BrokerError> {
    obj.get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| BrokerError::Malformed(format!("missing or non-boolean field `{field}`")))
}

pub(crate) fn req_str<'a>(obj: &'a Value, field: &str) -> Result<&'a str, BrokerError> {
    obj.get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| BrokerError::Malformed(format!("missing or empty field `{field}`")))
}

pub(crate) fn opt_str(obj: &Value, field: &str) -> Option<String> {
    obj.get(field).and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string)
}

pub(crate) fn opt_u32(obj: &Value, field: &str) -> Option<u32> {
    obj.get(field).and_then(Value::as_u64).and_then(|n| u32::try_from(n).ok())
}

pub(crate) fn json_of(body: &str, what: &str) -> Result<Value, BrokerError> {
    serde_json::from_str(body).map_err(|_| BrokerError::Malformed(format!("{what} body is not JSON")))
}

/// Timestamps come back as `"1758463200.123456789"` (seconds since the epoch) because the adapter
/// asks for `Accept-Datetime-Format: UNIX`. Informational only: `None` when absent or unparseable.
pub(crate) fn opt_time(obj: &Value, field: &str) -> Option<f64> {
    match obj.get(field)? {
        Value::String(s) => s.trim().parse::<f64>().ok().filter(|t| t.is_finite()),
        Value::Number(n) => n.as_f64().filter(|t| t.is_finite()),
        _ => None,
    }
}

// ---------------------------------------------------------------- account summary

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSummary {
    pub id: String,
    /// Account (home) currency, upper-case.
    pub currency: String,
    /// Realised balance, excluding unrealised P&L.
    pub balance: Dec,
    /// Net asset value, documented as `balance + unrealizedPL`.
    pub nav: Dec,
    pub unrealized_pl: Dec,
    pub margin_used: Dec,
    pub margin_available: Dec,
    pub position_value: Option<Dec>,
    pub margin_closeout_percent: Option<Dec>,
    pub margin_rate: Option<Dec>,
    pub pl: Option<Dec>,
    pub open_trade_count: Option<u32>,
    pub open_position_count: Option<u32>,
    pub pending_order_count: Option<u32>,
    /// Hedging accounts hold long and short of one instrument at once; this adapter does not
    /// support them (netting is assumed everywhere).
    pub hedging_enabled: bool,
    pub last_transaction_id: Option<String>,
}

/// Parse `GET /v3/accounts/{id}/summary`: `{"account": {...}, "lastTransactionID": "..."}`.
pub fn parse_account_summary(body: &str) -> Result<AccountSummary, BrokerError> {
    let root = json_of(body, "account summary")?;
    let a = root.get("account").filter(|a| a.is_object()).ok_or_else(|| BrokerError::Malformed("account summary has no `account` object".into()))?;
    Ok(AccountSummary {
        id: req_str(a, "id")?.to_string(),
        currency: req_str(a, "currency")?.to_ascii_uppercase(),
        balance: req_dec(a, "balance")?,
        nav: req_dec(a, "NAV")?,
        unrealized_pl: req_dec(a, "unrealizedPL")?,
        margin_used: req_dec(a, "marginUsed")?,
        margin_available: req_dec(a, "marginAvailable")?,
        position_value: opt_dec(a, "positionValue")?,
        margin_closeout_percent: opt_dec(a, "marginCloseoutPercent")?,
        margin_rate: opt_dec(a, "marginRate")?,
        pl: opt_dec(a, "pl")?,
        open_trade_count: opt_u32(a, "openTradeCount"),
        open_position_count: opt_u32(a, "openPositionCount"),
        pending_order_count: opt_u32(a, "pendingOrderCount"),
        hedging_enabled: req_bool(a, "hedgingEnabled")?,
        last_transaction_id: opt_str(&root, "lastTransactionID").or_else(|| opt_str(a, "lastTransactionID")),
    })
}

// ---------------------------------------------------------------- positions

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OandaPosition {
    /// Normalised OANDA name, `EUR_USD`.
    pub instrument: String,
    /// Long units, zero or positive.
    pub long_units: Dec,
    /// Short units, zero or NEGATIVE.
    pub short_units: Dec,
    pub long_average_price: Option<Dec>,
    pub short_average_price: Option<Dec>,
    /// Unrealised P&L of the whole position in the ACCOUNT currency.
    pub unrealized_pl: Dec,
    pub margin_used: Option<Dec>,
}

impl OandaPosition {
    /// Signed net units: positive long, negative short.
    pub fn net_units(&self) -> Dec {
        self.long_units.checked_add(self.short_units).unwrap_or(self.long_units)
    }

    /// Both sides at zero units. MEASURED on a practice account: an instrument that was traded before and is flat now is
    /// still returned by `GET /positions/<i>` (HTTP 200) and listed by `GET /positions`, with `long.units` and
    /// `short.units` of `"0"` (and non-zero `pl`). A flat record is NOT a held position.
    pub fn is_flat(&self) -> bool {
        self.long_units.is_zero() && self.short_units.is_zero()
    }

    /// Both sides non-zero: only possible on a hedging account.
    pub fn is_hedged(&self) -> bool {
        self.long_units.is_positive() && self.short_units.is_negative()
    }
}

fn parse_position_value(v: &Value) -> Result<OandaPosition, BrokerError> {
    let instrument = normalize_instrument(req_str(v, "instrument")?)?;
    let side = |name: &str| -> Result<&Value, BrokerError> {
        v.get(name).filter(|s| s.is_object()).ok_or_else(|| BrokerError::Malformed(format!("position {instrument}: missing `{name}` object")))
    };
    let (long, short) = (side("long")?, side("short")?);
    let long_units = req_dec(long, "units")?;
    let short_units = req_dec(short, "units")?;
    if long_units.is_negative() {
        return Err(BrokerError::Malformed(format!("position {instrument}: long units are negative")));
    }
    if short_units.is_positive() {
        return Err(BrokerError::Malformed(format!("position {instrument}: short units are positive (they are reported negative)")));
    }
    Ok(OandaPosition {
        long_average_price: opt_dec(long, "averagePrice")?,
        short_average_price: opt_dec(short, "averagePrice")?,
        unrealized_pl: req_dec(v, "unrealizedPL")?,
        margin_used: opt_dec(v, "marginUsed")?,
        instrument,
        long_units,
        short_units,
    })
}

/// Parse `GET /v3/accounts/{id}/openPositions` or `GET /v3/accounts/{id}/positions`: `{"positions": [...]}`. Rows with
/// zero units on both sides are dropped: `openPositions` never sends them, but `positions` (all) lists every instrument
/// ever traded, flat ones included (MEASURED), and a flat row is not a holding.
pub fn parse_open_positions(body: &str) -> Result<Vec<OandaPosition>, BrokerError> {
    let v = json_of(body, "open positions")?;
    let arr = v.get("positions").and_then(Value::as_array).ok_or_else(|| BrokerError::Malformed("open positions has no `positions` array".into()))?;
    let mut out = Vec::new();
    for p in arr {
        let pos = parse_position_value(p)?;
        if !(pos.long_units.is_zero() && pos.short_units.is_zero()) {
            out.push(pos);
        }
    }
    out.sort_by(|a, b| a.instrument.cmp(&b.instrument));
    Ok(out)
}

/// Parse `GET /v3/accounts/{id}/positions/{instrument}`: `{"position": {...}}`. The result may be FLAT (both sides zero,
/// see [`OandaPosition::is_flat`]): callers that ask "is anything held" must check that.
pub fn parse_single_position(body: &str) -> Result<OandaPosition, BrokerError> {
    let v = json_of(body, "position")?;
    let p = v.get("position").ok_or_else(|| BrokerError::Malformed("position response has no `position`".into()))?;
    parse_position_value(p)
}

// ---------------------------------------------------------------- pricing

#[derive(Debug, Clone, PartialEq)]
pub struct PriceQuote {
    pub instrument: String,
    pub tradeable: bool,
    pub status: Option<String>,
    /// Best bid / ask of the first liquidity bucket. `None` when the side is empty (market closed).
    pub bid: Option<Dec>,
    pub ask: Option<Dec>,
    pub closeout_bid: Option<Dec>,
    pub closeout_ask: Option<Dec>,
    pub time: Option<f64>,
}

impl PriceQuote {
    /// `(bid + ask) / 2`, exact. `None` unless both sides exist and the book is not crossed.
    pub fn mid(&self) -> Option<Dec> {
        let (b, a) = (self.bid?, self.ask?);
        if a < b {
            return None;
        }
        b.checked_add(a)?.checked_mul(Dec::new(5, 1).ok()?)
    }

    /// The mid to VALUE a position at: the top-of-book mid when both sides exist, otherwise (a closed market has no
    /// liquidity on either side) the mid of the broker's own `closeoutBid` / `closeoutAsk`, which OANDA documents as the
    /// prices it uses to close out a position when there is no liquidity (FROM-MEMORY-OF-DOCS). `None` when neither
    /// pair is usable or the pair is crossed. Never used to send an order.
    pub fn valuation_mid(&self) -> Option<Dec> {
        if self.bid.is_some() && self.ask.is_some() {
            return self.mid();
        }
        let (b, a) = (self.closeout_bid?, self.closeout_ask?);
        if a < b {
            return None;
        }
        b.checked_add(a)?.checked_mul(Dec::new(5, 1).ok()?)
    }
}

/// `homeConversions` row: the factor that converts a Position Value quoted in `currency` into the
/// account currency (requested with `includeHomeConversions=true`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeConversion {
    pub currency: String,
    pub account_gain: Option<Dec>,
    pub account_loss: Option<Dec>,
    pub position_value: Dec,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pricing {
    pub prices: Vec<PriceQuote>,
    pub home_conversions: Vec<HomeConversion>,
}

impl Pricing {
    pub fn price(&self, instrument: &str) -> Option<&PriceQuote> {
        self.prices.iter().find(|p| p.instrument == instrument)
    }

    pub fn conversion(&self, currency: &str) -> Option<&HomeConversion> {
        self.home_conversions.iter().find(|c| c.currency.eq_ignore_ascii_case(currency))
    }
}

fn first_price(v: &Value, side: &str) -> Result<Option<Dec>, BrokerError> {
    match v.get(side).and_then(Value::as_array).and_then(|a| a.first()) {
        None => Ok(None),
        Some(bucket) => req_dec(bucket, "price").map(Some),
    }
}

/// Parse `GET /v3/accounts/{id}/pricing`.
pub fn parse_pricing(body: &str) -> Result<Pricing, BrokerError> {
    let v = json_of(body, "pricing")?;
    let arr = v.get("prices").and_then(Value::as_array).ok_or_else(|| BrokerError::Malformed("pricing response has no `prices` array".into()))?;
    let mut prices = Vec::new();
    for p in arr {
        let instrument = normalize_instrument(req_str(p, "instrument")?)?;
        // `tradeable` is documented as a bool; `status` (`tradeable`, `non-tradeable`, ...) is kept verbatim.
        let status = opt_str(p, "status");
        let tradeable = match p.get("tradeable").and_then(Value::as_bool) {
            Some(t) => t,
            None => status.as_deref() == Some("tradeable"),
        };
        prices.push(PriceQuote {
            instrument,
            tradeable,
            status,
            bid: first_price(p, "bids")?,
            ask: first_price(p, "asks")?,
            closeout_bid: opt_dec(p, "closeoutBid")?,
            closeout_ask: opt_dec(p, "closeoutAsk")?,
            time: opt_time(p, "time"),
        });
    }
    let mut home_conversions = Vec::new();
    if let Some(arr) = v.get("homeConversions").and_then(Value::as_array) {
        for c in arr {
            home_conversions.push(HomeConversion {
                currency: req_str(c, "currency")?.to_ascii_uppercase(),
                account_gain: opt_dec(c, "accountGain")?,
                account_loss: opt_dec(c, "accountLoss")?,
                position_value: req_dec(c, "positionValue")?,
            });
        }
    }
    Ok(Pricing { prices, home_conversions })
}

// ---------------------------------------------------------------- transactions

/// `ORDER_FILL` transaction (the only place a fill's quantity and price live: the order resource
/// itself carries no fill price or quantity).
#[derive(Debug, Clone, PartialEq)]
pub struct FillInfo {
    pub id: String,
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub instrument: String,
    /// Signed executed units (negative for a sell).
    pub units: Dec,
    /// Average price of the fill in the quote currency.
    pub price: Dec,
    /// Realised P&L booked by this fill, account currency.
    pub pl: Option<Dec>,
    /// Commission, account currency (usually 0 on FX: the cost is the spread).
    pub commission: Option<Dec>,
    pub financing: Option<Dec>,
    pub reason: Option<String>,
    pub time: Option<f64>,
}

/// `ORDER_CANCEL` transaction.
#[derive(Debug, Clone, PartialEq)]
pub struct CancelInfo {
    pub id: String,
    pub order_id: String,
    pub client_order_id: Option<String>,
    /// `OrderCancelReason`, verbatim (`CLIENT_REQUEST`, `INSUFFICIENT_MARGIN`, `MARKET_HALTED`, ...).
    pub reason: String,
    pub time: Option<f64>,
}

/// `*_ORDER_REJECT` transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectInfo {
    pub id: Option<String>,
    /// `TransactionRejectReason`, verbatim (`INSUFFICIENT_MARGIN`, `UNITS_INVALID`, ...).
    pub reason: String,
    pub client_order_id: Option<String>,
}

/// `*_ORDER` create transaction (`MARKET_ORDER`, `LIMIT_ORDER`, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateInfo {
    pub id: String,
    /// The transaction type, verbatim (`MARKET_ORDER`, `LIMIT_ORDER`, ...).
    pub kind: String,
    pub instrument: Option<String>,
    pub units: Option<Dec>,
    pub price: Option<Dec>,
    pub time_in_force: Option<String>,
    pub client_id: Option<String>,
}

pub fn client_id_of(v: &Value) -> Option<String> {
    v.get("clientExtensions").and_then(|c| opt_str(c, "id"))
}

pub(crate) fn parse_fill(v: &Value) -> Result<FillInfo, BrokerError> {
    Ok(FillInfo {
        id: req_str(v, "id")?.to_string(),
        order_id: req_str(v, "orderID")?.to_string(),
        client_order_id: opt_str(v, "clientOrderID"),
        instrument: normalize_instrument(req_str(v, "instrument")?)?,
        units: req_dec(v, "units")?,
        price: req_dec(v, "price")?,
        pl: opt_dec(v, "pl")?,
        commission: opt_dec(v, "commission")?,
        financing: opt_dec(v, "financing")?,
        reason: opt_str(v, "reason"),
        time: opt_time(v, "time"),
    })
}

pub(crate) fn parse_cancel(v: &Value) -> Result<CancelInfo, BrokerError> {
    Ok(CancelInfo {
        id: req_str(v, "id")?.to_string(),
        order_id: req_str(v, "orderID")?.to_string(),
        client_order_id: opt_str(v, "clientOrderID"),
        reason: req_str(v, "reason")?.to_string(),
        time: opt_time(v, "time"),
    })
}

pub(crate) fn parse_reject(v: &Value) -> RejectInfo {
    RejectInfo {
        id: opt_str(v, "id"),
        reason: opt_str(v, "rejectReason").unwrap_or_else(|| "UNSPECIFIED".to_string()),
        client_order_id: opt_str(v, "clientOrderID").or_else(|| client_id_of(v)),
    }
}

pub(crate) fn parse_create(v: &Value) -> Result<CreateInfo, BrokerError> {
    Ok(CreateInfo {
        id: req_str(v, "id")?.to_string(),
        kind: opt_str(v, "type").unwrap_or_default(),
        instrument: opt_str(v, "instrument").map(|i| normalize_instrument(&i)).transpose()?,
        units: opt_dec(v, "units")?,
        price: opt_dec(v, "price")?,
        time_in_force: opt_str(v, "timeInForce"),
        client_id: client_id_of(v),
    })
}

/// The transactions of one order attempt inside a create/close response. `prefix` is `order`
/// (create), or `longOrder` / `shortOrder` (position close): the keys are `<prefix>CreateTransaction`,
/// `<prefix>FillTransaction`, `<prefix>CancelTransaction`, `<prefix>RejectTransaction`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OrderTransactions {
    pub create: Option<CreateInfo>,
    pub fill: Option<FillInfo>,
    pub cancel: Option<CancelInfo>,
    pub reject: Option<RejectInfo>,
}

impl OrderTransactions {
    pub fn is_empty(&self) -> bool {
        self.create.is_none() && self.fill.is_none() && self.cancel.is_none() && self.reject.is_none()
    }
}

fn group(root: &Value, prefix: &str) -> Result<OrderTransactions, BrokerError> {
    let key = |suffix: &str| format!("{prefix}{suffix}Transaction");
    let get = |suffix: &str| root.get(key(suffix)).filter(|v| v.is_object());
    Ok(OrderTransactions {
        create: get("Create").map(parse_create).transpose()?,
        fill: get("Fill").map(parse_fill).transpose()?,
        cancel: get("Cancel").map(parse_cancel).transpose()?,
        reject: get("Reject").map(parse_reject),
    })
}

/// Parse the body of a `POST /orders` response (success or a 400 rejection).
pub fn parse_order_transactions(body: &str, prefix: &str) -> Result<OrderTransactions, BrokerError> {
    let root = json_of(body, "order")?;
    if !root.is_object() {
        return Err(BrokerError::Malformed("order response is not an object".into()));
    }
    group(&root, prefix)
}

/// Parse `GET /transactions/{id}`: `{"transaction": {...}}` as an `ORDER_FILL`.
pub fn parse_fill_transaction(body: &str) -> Result<FillInfo, BrokerError> {
    let root = json_of(body, "transaction")?;
    let t = root.get("transaction").ok_or_else(|| BrokerError::Malformed("transaction response has no `transaction`".into()))?;
    match t.get("type").and_then(Value::as_str) {
        Some("ORDER_FILL") => parse_fill(t),
        other => Err(BrokerError::Malformed(format!("expected an ORDER_FILL transaction, got {other:?}"))),
    }
}

/// Parse `GET /transactions/{id}`: `{"transaction": {...}}` as an `ORDER_CANCEL`.
pub fn parse_cancel_transaction(body: &str) -> Result<CancelInfo, BrokerError> {
    let root = json_of(body, "transaction")?;
    let t = root.get("transaction").ok_or_else(|| BrokerError::Malformed("transaction response has no `transaction`".into()))?;
    match t.get("type").and_then(Value::as_str) {
        Some("ORDER_CANCEL") => parse_cancel(t),
        other => Err(BrokerError::Malformed(format!("expected an ORDER_CANCEL transaction, got {other:?}"))),
    }
}

/// Parse the 200 body of `PUT /orders/{id}/cancel`.
pub fn parse_cancel_response(body: &str) -> Result<CancelInfo, BrokerError> {
    let root = json_of(body, "cancel")?;
    let t = root.get("orderCancelTransaction").ok_or_else(|| BrokerError::Malformed("cancel response has no `orderCancelTransaction`".into()))?;
    parse_cancel(t)
}

// ---------------------------------------------------------------- transaction stream (sinceid)

/// One transaction of a `GET /transactions/sinceid` page, with the two fields the tag scan needs pulled out.
///
/// MEASURED (practice account, 2026-09-23): a `MARKET_ORDER` transaction carries the client id as
/// `clientExtensions.id`; its `ORDER_FILL` (and `ORDER_CANCEL`, `ORDER_CANCEL_REJECT`) carry it as the top-level
/// `clientOrderID`. Reject transactions echo `clientExtensions` when the request carried them (NOT yet measured with a
/// client id present).
#[derive(Debug, Clone)]
pub struct TxnRecord {
    /// Numeric transaction id (OANDA ids are integers, increasing within an account).
    pub id: u64,
    pub id_text: String,
    /// `MARKET_ORDER`, `ORDER_FILL`, `ORDER_CANCEL`, `MARKET_ORDER_REJECT`, ...
    pub kind: String,
    /// `orderID` (fills, cancels).
    pub order_id: Option<String>,
    /// `clientOrderID`, else `clientExtensions.id`.
    pub client_id: Option<String>,
    pub raw: Value,
}

/// A `sinceid` response: the transactions with id greater than `after_id`, and the account's last transaction id.
#[derive(Debug, Clone)]
pub struct TxnPage {
    pub after_id: u64,
    pub records: Vec<TxnRecord>,
    pub last_transaction_id: u64,
}

impl TxnPage {
    /// `Ok` when the page provably holds EVERY transaction in `(after_id, last_transaction_id]`: it starts at
    /// `after_id + 1`, ends at `last_transaction_id`, and an empty page is only consistent with
    /// `last_transaction_id == after_id`. Anything else (a page the server truncated, gaps, ids out of range, a page
    /// that ends before the account's last id) is `Err(reason)`: the absence of a tag in it proves nothing.
    /// Relies on OANDA transaction ids being consecutive within an account, which the recorded practice-account
    /// stream showed (30, 31, 32, ... including reject transactions); if that ever fails the result is a refusal to
    /// prove (safe), never a false "not found".
    pub fn completeness(&self) -> Result<(), String> {
        let (first, last) = match (self.records.first(), self.records.last()) {
            (Some(f), Some(l)) => (f.id, l.id),
            _ => {
                return if self.last_transaction_id == self.after_id {
                    Ok(())
                } else {
                    Err(format!("empty page after id {} but the account's last transaction id is {}", self.after_id, self.last_transaction_id))
                }
            }
        };
        if first != self.after_id + 1 {
            return Err(format!("page starts at id {first}, not {}", self.after_id + 1));
        }
        if last != self.last_transaction_id {
            return Err(format!("page ends at id {last} but the account's last transaction id is {}", self.last_transaction_id));
        }
        for w in self.records.windows(2) {
            if w[1].id != w[0].id + 1 {
                return Err(format!("gap between transaction {} and {}", w[0].id, w[1].id));
            }
        }
        Ok(())
    }
}

/// Parse `GET /transactions/sinceid?id=<after_id>`: `{"transactions": [...], "lastTransactionID": "..."}`.
pub fn parse_transactions_page(body: &str, after_id: u64) -> Result<TxnPage, BrokerError> {
    let root = json_of(body, "transactions")?;
    let arr = root
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or_else(|| BrokerError::Malformed("transactions response has no `transactions` array".into()))?;
    let last_text = req_str(&root, "lastTransactionID")?;
    let last_transaction_id = last_text.parse::<u64>().map_err(|_| BrokerError::Malformed(format!("lastTransactionID {last_text:?} is not numeric")))?;
    let mut records = Vec::with_capacity(arr.len());
    for t in arr {
        let id_text = req_str(t, "id")?.to_string();
        let id = id_text.parse::<u64>().map_err(|_| BrokerError::Malformed(format!("transaction id {id_text:?} is not numeric")))?;
        if id <= after_id {
            return Err(BrokerError::Malformed(format!("transaction {id} is not after the requested id {after_id}")));
        }
        records.push(TxnRecord {
            id,
            id_text,
            kind: req_str(t, "type")?.to_string(),
            order_id: opt_str(t, "orderID"),
            client_id: opt_str(t, "clientOrderID").or_else(|| client_id_of(t)),
            raw: t.clone(),
        });
    }
    Ok(TxnPage { after_id, records, last_transaction_id })
}

// ---------------------------------------------------------------- order resources

/// One order as `GET /orders/{spec}` or `GET /pendingOrders` returns it. Fill details are NOT here.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderResource {
    pub id: String,
    /// `PENDING`, `FILLED`, `TRIGGERED` or `CANCELLED`, verbatim.
    pub state: String,
    /// `MARKET`, `LIMIT`, `STOP`, `TAKE_PROFIT`, ...
    pub kind: String,
    pub instrument: Option<String>,
    /// Signed ordered units; `None` for orders without units (linked stop-loss / take-profit).
    pub units: Option<Dec>,
    pub price: Option<Dec>,
    pub client_id: Option<String>,
    pub time_in_force: Option<String>,
    pub filling_transaction_id: Option<String>,
    pub cancelling_transaction_id: Option<String>,
    pub create_time: Option<f64>,
    pub filled_time: Option<f64>,
    pub cancelled_time: Option<f64>,
}

fn parse_order_resource_value(v: &Value) -> Result<OrderResource, BrokerError> {
    if !v.is_object() {
        return Err(BrokerError::Malformed("order is not an object".into()));
    }
    Ok(OrderResource {
        id: req_str(v, "id")?.to_string(),
        state: req_str(v, "state")?.to_string(),
        kind: req_str(v, "type")?.to_string(),
        instrument: opt_str(v, "instrument").map(|i| normalize_instrument(&i)).transpose()?,
        units: opt_dec(v, "units")?,
        price: opt_dec(v, "price")?,
        client_id: client_id_of(v),
        time_in_force: opt_str(v, "timeInForce"),
        filling_transaction_id: opt_str(v, "fillingTransactionID"),
        cancelling_transaction_id: opt_str(v, "cancellingTransactionID"),
        create_time: opt_time(v, "createTime"),
        filled_time: opt_time(v, "filledTime"),
        cancelled_time: opt_time(v, "cancelledTime"),
    })
}

/// Parse `GET /orders/{spec}`: `{"order": {...}}`.
pub fn parse_order_resource(body: &str) -> Result<OrderResource, BrokerError> {
    let root = json_of(body, "order")?;
    let o = root.get("order").ok_or_else(|| BrokerError::Malformed("order response has no `order`".into()))?;
    parse_order_resource_value(o)
}

/// Parse `GET /pendingOrders`: `{"orders": [...]}`.
pub fn parse_order_resources(body: &str) -> Result<Vec<OrderResource>, BrokerError> {
    let root = json_of(body, "pending orders")?;
    let arr = root.get("orders").and_then(Value::as_array).ok_or_else(|| BrokerError::Malformed("orders response has no `orders` array".into()))?;
    arr.iter().map(parse_order_resource_value).collect()
}

fn abs_dec(d: Dec) -> Dec {
    if d.is_negative() {
        Dec::new(-d.units(), d.scale()).unwrap_or(d)
    } else {
        d
    }
}

/// Our status from OANDA's order `state`, the quantity ordered and the quantity actually executed.
///
/// * `PENDING` and `TRIGGERED` are still live -> `Open` (an executed quantity on a live order is a
///   contradiction: `Malformed`).
/// * `FILLED` needs an executed quantity: all of it -> `Filled`; part of it -> the order is over and
///   the remainder was dropped -> `PartiallyFilledThenCanceled` with the executed quantity kept.
/// * `CANCELLED` -> `Canceled`, or `Expired` when the cancel reason is `TIME_IN_FORCE_EXPIRED`; with
///   an executed quantity the `PartiallyFilledThen...` variants.
/// * executed greater than ordered is a duplicated or corrupt report: `Malformed("overfill anomaly ...")`
///   in every state (the rebalancer's flatten halts on that phrase).
/// * anything else is `Malformed`: we never guess accounting.
pub fn derive_order_status(state: &str, ordered_abs: Option<Dec>, executed_abs: Dec, cancel_reason: Option<&str>) -> Result<OrderStatus, BrokerError> {
    if executed_abs.is_negative() {
        return Err(BrokerError::Malformed("negative executed quantity".into()));
    }
    if let Some(q) = ordered_abs {
        if executed_abs > q {
            return Err(BrokerError::Malformed(format!(
                "overfill anomaly: executed units {executed_abs} exceed ordered units {q} (state {state:?}); refusing to report an inflated fill"
            )));
        }
    }
    let has_exec = executed_abs.is_positive();
    let expiry = cancel_reason == Some("TIME_IN_FORCE_EXPIRED");
    Ok(match state {
        "PENDING" | "TRIGGERED" => {
            if has_exec {
                return Err(BrokerError::Malformed(format!("state {state} but an executed quantity was reported")));
            }
            OrderStatus::Open
        }
        "FILLED" => {
            if !has_exec {
                return Err(BrokerError::Malformed("state FILLED but no executed quantity (missing or empty fill transaction)".into()));
            }
            match ordered_abs {
                Some(q) if executed_abs < q => OrderStatus::PartiallyFilledThenCanceled,
                _ => OrderStatus::Filled,
            }
        }
        "CANCELLED" => match (has_exec, expiry) {
            (true, true) => OrderStatus::PartiallyFilledThenExpired,
            (true, false) => OrderStatus::PartiallyFilledThenCanceled,
            (false, true) => OrderStatus::Expired,
            (false, false) => OrderStatus::Canceled,
        },
        other => return Err(BrokerError::Malformed(format!("unsupported order state {other:?}"))),
    })
}

impl OrderResource {
    /// Build the neutral report. `fill` is the `ORDER_FILL` transaction named by
    /// `filling_transaction_id` (needed when the order is FILLED); `cancel_reason` comes from the
    /// cancelling transaction when it could be read. `own_prefix` marks foreign orders: an order
    /// whose client id lacks it reports `tag: None`.
    pub fn into_report(
        &self,
        fill: Option<&FillInfo>,
        cancel_reason: Option<&str>,
        own_prefix: Option<&str>,
    ) -> Result<OrderReport, BrokerError> {
        if let Some(f) = fill {
            if f.order_id != self.id {
                return Err(BrokerError::Malformed(format!("fill transaction {} belongs to order {}, not {}", f.id, f.order_id, self.id)));
            }
        }
        let ordered_abs = self.units.map(abs_dec);
        let executed_abs = fill.map(|f| abs_dec(f.units)).unwrap_or(Dec::ZERO);
        let status = derive_order_status(&self.state, ordered_abs, executed_abs, cancel_reason)?;
        let side = match self.units {
            Some(u) if u.is_positive() => Some(Side::Buy),
            Some(u) if u.is_negative() => Some(Side::Sell),
            _ => None,
        };
        if let (Some(f), Some(u)) = (fill, self.units) {
            if f.units.is_negative() != u.is_negative() {
                return Err(BrokerError::Malformed(format!("fill {} has the opposite sign to order {}", f.id, self.id)));
            }
        }
        let kind = match (self.kind.as_str(), self.price) {
            ("MARKET", _) => Some(OrderKind::Market),
            ("LIMIT", Some(price)) => Some(OrderKind::Limit { price }),
            _ => None,
        };
        // Orders that are not plain MARKET/LIMIT (a foreign stop-loss, take-profit, ...) keep their
        // type in `raw_status` so nobody mistakes them for ours.
        let raw_status = if matches!(self.kind.as_str(), "MARKET" | "LIMIT") { self.state.clone() } else { format!("{}/{}", self.state, self.kind) };
        let tag = match (&self.client_id, own_prefix) {
            (Some(id), Some(p)) if !id.starts_with(p) => None,
            (Some(id), _) => Some(id.clone()),
            (None, _) => None,
        };
        let symbol = match &self.instrument {
            Some(i) => i.replacen('_', "/", 1),
            None => String::new(),
        };
        let has_fills = executed_abs.is_positive();
        Ok(OrderReport {
            broker_order_id: self.id.clone(),
            userref: None,
            tag,
            symbol,
            side,
            kind,
            status,
            raw_status,
            reason: cancel_reason.map(str::to_string),
            quantity: ordered_abs.unwrap_or(Dec::ZERO),
            executed_quantity: executed_abs,
            avg_price: fill.filter(|_| has_fills).map(|f| f.price),
            // OANDA reports no cost figure; we do not invent one.
            cost: None,
            fee: fill.and_then(|f| f.commission),
            open_time: self.create_time,
            close_time: self.filled_time.or(self.cancelled_time),
        })
    }
}

// ---------------------------------------------------------------- errors

/// The parsed error body of a non-2xx response. `code` and `message` are OANDA's
/// `errorCode` / `errorMessage`; `reject_reason` is the `rejectReason` of a reject transaction
/// found in the body, when there is one.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApiError {
    pub code: Option<String>,
    pub message: String,
    pub reject_reason: Option<String>,
}

const REJECT_KEYS: [&str; 4] =
    ["orderRejectTransaction", "orderCancelRejectTransaction", "longOrderRejectTransaction", "shortOrderRejectTransaction"];

pub fn parse_error_body(body: &str) -> ApiError {
    match serde_json::from_str::<Value>(body) {
        Ok(v) if v.is_object() => ApiError {
            code: opt_str(&v, "errorCode"),
            message: opt_str(&v, "errorMessage").or_else(|| opt_str(&v, "message")).unwrap_or_else(|| "no error message".to_string()),
            reject_reason: REJECT_KEYS.iter().find_map(|k| v.get(*k).and_then(|t| opt_str(t, "rejectReason"))),
        },
        // A gateway page or empty body: do not echo it.
        _ => ApiError { code: None, message: format!("non-JSON error body ({} bytes)", body.len()), reject_reason: None },
    }
}

impl ApiError {
    /// Replace every occurrence of a secret in the text fields (a server that echoes the
    /// Authorization header must not leak the token into our errors).
    pub fn scrubbed(mut self, secrets: &[&str]) -> Self {
        for s in secrets.iter().filter(|s| !s.is_empty()) {
            self.message = self.message.replace(s, "<redacted>");
            if let Some(c) = &mut self.code {
                *c = c.replace(s, "<redacted>");
            }
            if let Some(r) = &mut self.reject_reason {
                *r = r.replace(s, "<redacted>");
            }
        }
        self
    }

    /// `oanda:<status> <reason-or-code>: <message>`.
    pub fn code_string(&self, status: u16) -> String {
        match self.reject_reason.as_ref().or(self.code.as_ref()) {
            Some(c) => format!("oanda:{status} {c}: {}", self.message),
            None => format!("oanda:{status}: {}", self.message),
        }
    }
}

/// Classification of an HTTP failure (non-2xx). Placement treats `Server` and `Other` as UNKNOWN
/// (the request may have been processed), everything else as definite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpFailure {
    /// 401 or 403.
    Auth { status: u16, api: ApiError },
    /// 429, with `Retry-After` when it is a whole number of seconds.
    RateLimited { retry_after_secs: Option<u64>, api: ApiError },
    NotFound { api: ApiError },
    /// 400: the request was refused as a whole (a reject transaction, or a bad request).
    Rejected { status: u16, class: ErrorClass, api: ApiError },
    /// 5xx: the request may or may not have been processed.
    Server { status: u16, api: ApiError },
    /// Any other status.
    Other { status: u16, api: ApiError },
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    let t = text.to_ascii_uppercase();
    needles.iter().any(|n| t.contains(n))
}

/// Class of a reject reason / error code, from the values MEASURED on a practice account (2026-09-23):
/// `UNITS_LIMIT_EXCEEDED`, `UNITS_INVALID`, `TIME_IN_FORCE_INVALID`, `UNITS_PRECISION_EXCEEDED` and
/// `CLIENT_ORDER_ID_INVALID` (HTTP 400 with a `MARKET_ORDER_REJECT` whose `rejectReason` equals the `errorCode`), and
/// the unknown-instrument shape (HTTP 400, `errorCode` `oanda::rest::core::InvalidParameterException`, NO reject
/// transaction). `None` for a value this table does not know (the caller falls back to keyword matching).
pub fn classify_reject_reason(reason: &str) -> Option<ErrorClass> {
    Some(match reason {
        "UNITS_LIMIT_EXCEEDED" | "UNITS_INVALID" | "UNITS_PRECISION_EXCEEDED" | "UNITS_MINIMUM_NOT_MET" | "TIME_IN_FORCE_INVALID"
        | "CLIENT_ORDER_ID_INVALID" | "PRICE_PRECISION_EXCEEDED" | "INSTRUMENT_INVALID" | INVALID_PARAMETER_EXCEPTION => ErrorClass::InvalidArguments,
        "ORDER_DOESNT_EXIST" | "NO_SUCH_ORDER" | "NO_SUCH_POSITION" | "CLOSEOUT_POSITION_DOESNT_EXIST" => ErrorClass::UnknownOrder,
        _ => return None,
    })
}

/// The `errorCode` OANDA answers with (HTTP 400, no reject transaction) for a request whose parameter it cannot parse,
/// for example an unknown instrument name. MEASURED 2026-09-23.
pub const INVALID_PARAMETER_EXCEPTION: &str = "oanda::rest::core::InvalidParameterException";

/// Class of a definite rejection from its reason / code / message.
pub fn classify_rejection(api: &ApiError) -> ErrorClass {
    if let Some(class) = api.reject_reason.as_deref().and_then(classify_reject_reason).or_else(|| api.code.as_deref().and_then(classify_reject_reason)) {
        return class;
    }
    let hay = format!(
        "{} {} {}",
        api.reject_reason.as_deref().unwrap_or(""),
        api.code.as_deref().unwrap_or(""),
        api.message
    );
    if contains_any(&hay, &["INSUFFICIENT_MARGIN", "INSUFFICIENT_FUNDS", "INSUFFICIENT MARGIN", "INSUFFICIENT_LIQUIDITY_FOR_MARGIN"]) {
        ErrorClass::InsufficientFunds
    } else if contains_any(&hay, &["INVALID", "PRECISION_EXCEEDED", "_MISSING", "MINIMUM", "MAXIMUM", "_NOT_SPECIFIED"]) {
        ErrorClass::InvalidArguments
    } else {
        ErrorClass::OrderRejected
    }
}

/// Class of an `OrderCancelReason` for an order cancelled at creation.
pub fn classify_cancel_reason(reason: &str) -> ErrorClass {
    if reason.contains("INSUFFICIENT_MARGIN") {
        ErrorClass::InsufficientFunds
    } else {
        ErrorClass::OrderRejected
    }
}

pub fn classify_http_failure(status: u16, body: &str, retry_after: Option<&str>, secrets: &[&str]) -> HttpFailure {
    let api = parse_error_body(body).scrubbed(secrets);
    match status {
        401 | 403 => HttpFailure::Auth { status, api },
        404 => HttpFailure::NotFound { api },
        400 => {
            let class = classify_rejection(&api);
            HttpFailure::Rejected { status, class, api }
        }
        429 => HttpFailure::RateLimited { retry_after_secs: retry_after.and_then(|v| v.trim().parse::<u64>().ok()), api },
        500..=599 => HttpFailure::Server { status, api },
        _ => HttpFailure::Other { status, api },
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
        }
    }

    pub fn api(&self) -> &ApiError {
        match self {
            HttpFailure::Auth { api, .. }
            | HttpFailure::RateLimited { api, .. }
            | HttpFailure::NotFound { api }
            | HttpFailure::Rejected { api, .. }
            | HttpFailure::Server { api, .. }
            | HttpFailure::Other { api, .. } => api,
        }
    }

    /// The exchange-style error for a DEFINITE failure; `None` for 5xx / unexpected statuses /
    /// 429 / 404, which carry no classified error.
    pub fn exchange_error(&self) -> Option<ExchangeError> {
        match self {
            HttpFailure::Auth { status, api } => Some(ExchangeError { code: api.code_string(*status), class: ErrorClass::Auth }),
            HttpFailure::Rejected { status, class, api } => Some(ExchangeError { code: api.code_string(*status), class: *class }),
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
        let (q, z, half) = (d("100"), d("0"), d("40"));
        let cases: [(&str, Dec, Option<&str>, OrderStatus); 10] = [
            ("PENDING", z, None, OrderStatus::Open),
            ("TRIGGERED", z, None, OrderStatus::Open),
            ("FILLED", q, None, OrderStatus::Filled),
            ("FILLED", half, None, OrderStatus::PartiallyFilledThenCanceled),
            ("CANCELLED", z, Some("CLIENT_REQUEST"), OrderStatus::Canceled),
            ("CANCELLED", z, None, OrderStatus::Canceled),
            ("CANCELLED", z, Some("TIME_IN_FORCE_EXPIRED"), OrderStatus::Expired),
            ("CANCELLED", half, Some("CLIENT_REQUEST"), OrderStatus::PartiallyFilledThenCanceled),
            ("CANCELLED", half, Some("TIME_IN_FORCE_EXPIRED"), OrderStatus::PartiallyFilledThenExpired),
            ("CANCELLED", z, Some("INSUFFICIENT_MARGIN"), OrderStatus::Canceled),
        ];
        for (state, exec, reason, want) in cases {
            assert_eq!(derive_order_status(state, Some(q), exec, reason).unwrap(), want, "{state} {exec} {reason:?}");
        }
    }

    #[test]
    fn inconsistent_or_unknown_state_fails_closed() {
        let q = Some(d("100"));
        for (state, exec) in [("FILLED", d("0")), ("PENDING", d("10")), ("BOGUS", d("0")), ("", d("0")), ("filled", d("100"))] {
            assert!(derive_order_status(state, q, exec, None).is_err(), "{state} {exec}");
        }
        match derive_order_status("FILLED", q, d("101"), None) {
            Err(BrokerError::Malformed(m)) => assert!(m.contains("overfill anomaly"), "{m}"),
            other => panic!("{other:?}"),
        }
        assert!(derive_order_status("CANCELLED", q, d("-1"), None).is_err());
    }

    #[test]
    fn http_failure_classification_table() {
        use HttpFailure::*;
        let f = |s, b: &str| classify_http_failure(s, b, None, &[]);
        assert!(matches!(f(401, r#"{"errorMessage":"Insufficient authorization to perform request."}"#), Auth { status: 401, .. }));
        assert!(matches!(f(403, r#"{"errorMessage":"Forbidden"}"#), Auth { status: 403, .. }));
        assert!(matches!(f(404, r#"{"errorCode":"ORDER_DOES_NOT_EXIST","errorMessage":"x"}"#), NotFound { .. }));
        assert!(matches!(
            f(400, r#"{"orderRejectTransaction":{"type":"MARKET_ORDER_REJECT","rejectReason":"INSUFFICIENT_MARGIN"},"errorCode":"INSUFFICIENT_MARGIN","errorMessage":"m"}"#),
            Rejected { class: ErrorClass::InsufficientFunds, .. }
        ));
        assert!(matches!(
            f(400, r#"{"orderRejectTransaction":{"rejectReason":"UNITS_INVALID"},"errorMessage":"m"}"#),
            Rejected { class: ErrorClass::InvalidArguments, .. }
        ));
        assert!(matches!(f(400, r#"{"errorMessage":"weird"}"#), Rejected { class: ErrorClass::OrderRejected, .. }));
        assert!(matches!(f(429, "{}"), RateLimited { .. }));
        assert!(matches!(f(500, "oops"), Server { status: 500, .. }));
        assert!(matches!(f(503, "<html>"), Server { status: 503, .. }));
        assert!(matches!(f(418, ""), Other { status: 418, .. }));
    }

    #[test]
    fn non_json_error_bodies_are_not_echoed() {
        let api = parse_error_body("<html>secret-gateway-page</html>");
        assert!(!api.message.contains("secret-gateway-page"));
    }

    #[test]
    fn scrub_replaces_secrets_everywhere() {
        let api = parse_error_body(r#"{"errorCode":"TOKEN-9","errorMessage":"bad header Bearer TOKEN-9 rejected"}"#).scrubbed(&["TOKEN-9"]);
        assert!(!api.message.contains("TOKEN-9") && !api.code.unwrap().contains("TOKEN-9"));
    }
}
