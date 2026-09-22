//! Parsing of Kraken JSON responses. Pure functions over `&str` / `serde_json::Value`.
//!
//! Stance: fail closed. A field we depend on for accounting (`status`, `vol`, `vol_exec`) that is
//! missing or unparseable is a `Malformed` error, never a silent zero. (The SignalEngine parser
//! used `unwrap_or(0.0)` and dropped `vol_exec` for canceled orders: verified defects.)

use crate::decimal::Dec;
use crate::error::{BrokerError, ErrorClass, ExchangeError};
use crate::kraken::pairs::{normalize_asset, PairInfo, PairTable};
use crate::kraken::userref::UserrefMap;
use crate::types::{
    BalanceEntry, Balances, CancelOutcome, OrderKind, OrderReport, OrderStatus, Quote, Side,
};
use serde_json::Value;

/// A parsed, error-free Kraken envelope.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub result: Option<Value>,
    /// Entries of the `error` array that start with `W` (warnings).
    pub warnings: Vec<String>,
}

/// Classify a Kraken error string. FROM-MEMORY-OF-DOCS: the code list is not exhaustive.
pub fn classify(code: &str) -> ErrorClass {
    let c = code;
    if c.starts_with("EAPI:Invalid nonce") {
        ErrorClass::InvalidNonce
    } else if c.starts_with("EAPI:Invalid key")
        || c.starts_with("EAPI:Invalid signature")
        || c.starts_with("EAPI:Invalid session")
        || c.starts_with("EGeneral:Permission denied")
    {
        ErrorClass::Auth
    } else if c.contains("Rate limit exceeded") || c.starts_with("EGeneral:Temporary lockout") {
        ErrorClass::RateLimited
    } else if c.starts_with("EOrder:Insufficient funds") || c.starts_with("EOrder:Insufficient margin") {
        ErrorClass::InsufficientFunds
    } else if c.starts_with("EOrder:Unknown order") || c.starts_with("EOrder:Invalid order") {
        ErrorClass::UnknownOrder
    } else if c.starts_with("EGeneral:Invalid arguments")
        || c.starts_with("EQuery:Unknown asset pair")
        || c.starts_with("EQuery:Invalid asset pair")
    {
        ErrorClass::InvalidArguments
    } else if c.starts_with("EService:Market in ") {
        ErrorClass::OrderRejected
    } else if c.starts_with("EService:") || c.starts_with("EGeneral:Internal error") {
        ErrorClass::ServiceUnavailable
    } else if c.starts_with("EOrder:") || c.starts_with("ETrade:") {
        ErrorClass::OrderRejected
    } else {
        ErrorClass::Other
    }
}

/// Parse the outer `{"error":[...],"result":...}` envelope.
///
/// * `error` absent or not an array of strings: `Malformed` (a real reply always has it).
/// * Any entry not starting with `W` counts as an error (fail closed).
/// * All-`W` entries are warnings and the call succeeds.
pub fn parse_envelope(body: &str) -> Result<Envelope, BrokerError> {
    let v: Value = serde_json::from_str(body).map_err(|_| BrokerError::Malformed("body is not JSON".into()))?;
    let errs = v
        .get("error")
        .and_then(Value::as_array)
        .ok_or_else(|| BrokerError::Malformed("missing `error` array".into()))?;
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    for e in errs {
        let s = e.as_str().ok_or_else(|| BrokerError::Malformed("non-string entry in `error`".into()))?;
        if s.starts_with('W') {
            warnings.push(s.to_string());
        } else {
            errors.push(ExchangeError { code: s.to_string(), class: classify(s) });
        }
    }
    if !errors.is_empty() {
        return Err(BrokerError::Exchange(errors));
    }
    Ok(Envelope { result: v.get("result").cloned(), warnings })
}

fn result_of(env: &Envelope) -> Result<&Value, BrokerError> {
    env.result.as_ref().ok_or_else(|| BrokerError::Malformed("missing `result`".into()))
}

fn dec_from_value(field: &str, v: &Value) -> Result<Dec, BrokerError> {
    let text = match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return Err(BrokerError::Malformed(format!("field `{field}` is not a number"))),
    };
    Dec::parse(&text).map_err(|_| BrokerError::Malformed(format!("field `{field}` is not a decimal")))
}

/// Optional decimal: absent or null gives `None`; present but unparseable is an error.
fn opt_dec(obj: &Value, field: &str) -> Result<Option<Dec>, BrokerError> {
    match obj.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => dec_from_value(field, v).map(Some),
    }
}

fn req_dec(obj: &Value, field: &str) -> Result<Dec, BrokerError> {
    opt_dec(obj, field)?.ok_or_else(|| BrokerError::Malformed(format!("missing field `{field}`")))
}

// ---------------------------------------------------------------- balances

pub fn parse_balances(env: &Envelope) -> Result<Balances, BrokerError> {
    let obj = result_of(env)?
        .as_object()
        .ok_or_else(|| BrokerError::Malformed("Balance result is not an object".into()))?;
    let mut entries = Vec::with_capacity(obj.len());
    for (raw, v) in obj {
        let amount = dec_from_value(raw, v)?;
        let (asset, kind) = normalize_asset(raw);
        entries.push(BalanceEntry { raw_asset: raw.clone(), asset, amount, kind });
    }
    entries.sort_by(|a, b| a.raw_asset.cmp(&b.raw_asset));
    Ok(Balances { entries })
}

/// `TradeBalance` fields (all in the requested asset, default ZUSD). Absent fields are `None`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TradeBalance {
    /// Equivalent balance (combined balance of all currencies).
    pub equivalent_balance: Option<Dec>,
    /// Trade balance (combined balance of all equity currencies).
    pub trade_balance: Option<Dec>,
    pub margin: Option<Dec>,
    pub unrealized_pnl: Option<Dec>,
    pub cost_basis: Option<Dec>,
    pub valuation: Option<Dec>,
    pub equity: Option<Dec>,
    pub free_margin: Option<Dec>,
    pub margin_level: Option<Dec>,
}

pub fn parse_trade_balance(env: &Envelope) -> Result<TradeBalance, BrokerError> {
    let r = result_of(env)?;
    if !r.is_object() {
        return Err(BrokerError::Malformed("TradeBalance result is not an object".into()));
    }
    Ok(TradeBalance {
        equivalent_balance: opt_dec(r, "eb")?,
        trade_balance: opt_dec(r, "tb")?,
        margin: opt_dec(r, "m")?,
        unrealized_pnl: opt_dec(r, "n")?,
        cost_basis: opt_dec(r, "c")?,
        valuation: opt_dec(r, "v")?,
        equity: opt_dec(r, "e")?,
        free_margin: opt_dec(r, "mf")?,
        margin_level: opt_dec(r, "ml")?,
    })
}

// ---------------------------------------------------------------- ticker

fn first_of(entry: &Value, key: &str) -> Result<Dec, BrokerError> {
    let first = entry
        .get(key)
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .ok_or_else(|| BrokerError::Malformed(format!("Ticker field `{key}` missing")))?;
    dec_from_value(key, first)
}

pub fn parse_ticker(env: &Envelope, pair: &PairInfo) -> Result<Quote, BrokerError> {
    let obj = result_of(env)?
        .as_object()
        .ok_or_else(|| BrokerError::Malformed("Ticker result is not an object".into()))?;
    let entry = obj
        .get(&pair.rest_name)
        .or_else(|| obj.get(&pair.altname))
        .or_else(|| if obj.len() == 1 { obj.values().next() } else { None })
        .ok_or_else(|| BrokerError::Malformed(format!("Ticker has no entry for {}", pair.altname)))?;
    let ask = first_of(entry, "a")?;
    let bid = first_of(entry, "b")?;
    let last = first_of(entry, "c")?;
    if !ask.is_positive() || !bid.is_positive() || !last.is_positive() {
        return Err(BrokerError::Malformed("Ticker prices must be positive".into()));
    }
    if ask < bid {
        return Err(BrokerError::Malformed("Ticker is crossed (ask < bid)".into()));
    }
    Ok(Quote { symbol: pair.canonical.clone(), bid, ask, last })
}

// ---------------------------------------------------------------- AddOrder / CancelOrder

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddOrderResult {
    pub txid: Option<String>,
    pub description: Option<String>,
}

pub fn parse_add_order(env: &Envelope) -> Result<AddOrderResult, BrokerError> {
    let r = result_of(env)?;
    let description = r.pointer("/descr/order").and_then(Value::as_str).map(str::to_string);
    let txid = match r.get("txid") {
        None | Some(Value::Null) => None,
        Some(Value::Array(a)) => match a.first() {
            Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
            None => None,
            _ => return Err(BrokerError::Malformed("`txid` entry is not a string".into())),
        },
        Some(_) => return Err(BrokerError::Malformed("`txid` is not an array".into())),
    };
    Ok(AddOrderResult { txid, description })
}

pub fn parse_cancel(env: &Envelope) -> Result<CancelOutcome, BrokerError> {
    let r = result_of(env)?;
    let count = r
        .get("count")
        .and_then(Value::as_u64)
        .ok_or_else(|| BrokerError::Malformed("CancelOrder result has no numeric `count`".into()))?;
    let pending = r.get("pending").map(|p| !p.is_null()).unwrap_or(false);
    Ok(CancelOutcome { canceled_count: u32::try_from(count).unwrap_or(u32::MAX), pending })
}

// ---------------------------------------------------------------- order info

/// Derive our status from Kraken's status and quantities.
///
/// Terminal orders (`closed`, `canceled`, `expired`) are classified by how much executed:
/// everything executed is `Filled`; some executed is `PartiallyFilledThen...` with the executed
/// quantity preserved; nothing executed is `Canceled` / `Expired`.
///
/// `vol_exec > vol` is impossible on a healthy exchange (a duplicated or corrupt fill report). It
/// is NOT mapped to `Filled` with an inflated quantity, which a caller doing position accounting
/// would double count: it is an `Err(Malformed("overfill anomaly ..."))` in every status, so the
/// caller halts and reconciles against balances.
pub fn derive_status(raw: &str, volume: Dec, executed: Dec) -> Result<OrderStatus, BrokerError> {
    if executed.is_negative() {
        return Err(BrokerError::Malformed("negative vol_exec".into()));
    }
    if executed > volume {
        return Err(BrokerError::Malformed(format!(
            "overfill anomaly: vol_exec {executed} exceeds vol {volume} (status {raw:?}); refusing to report an inflated fill"
        )));
    }
    Ok(match raw {
        "pending" => OrderStatus::Pending,
        "open" if executed.is_positive() => OrderStatus::PartiallyFilled,
        "open" => OrderStatus::Open,
        "closed" | "canceled" | "expired" => {
            if executed >= volume {
                OrderStatus::Filled
            } else if executed.is_positive() {
                if raw == "expired" {
                    OrderStatus::PartiallyFilledThenExpired
                } else {
                    OrderStatus::PartiallyFilledThenCanceled
                }
            } else if raw == "expired" {
                OrderStatus::Expired
            } else {
                OrderStatus::Canceled
            }
        }
        other => return Err(BrokerError::Malformed(format!("unknown order status {other:?}"))),
    })
}

fn userref_of(info: &Value) -> Result<Option<i32>, BrokerError> {
    match info.get("userref") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_i64()
            .and_then(|v| i32::try_from(v).ok())
            .map(Some)
            .ok_or_else(|| BrokerError::Malformed("userref out of int32 range".into())),
        Some(Value::String(s)) => s
            .parse::<i32>()
            .map(Some)
            .map_err(|_| BrokerError::Malformed("userref is not an int32".into())),
        Some(_) => Err(BrokerError::Malformed("userref has an unexpected type".into())),
    }
}

pub fn parse_order_info(
    txid: &str,
    info: &Value,
    pairs: &PairTable,
    userrefs: &UserrefMap,
) -> Result<OrderReport, BrokerError> {
    if !info.is_object() {
        return Err(BrokerError::Malformed(format!("order {txid} is not an object")));
    }
    let raw_status = info
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| BrokerError::Malformed(format!("order {txid}: missing status")))?
        .to_string();
    let quantity = req_dec(info, "vol")?;
    let executed_quantity = req_dec(info, "vol_exec")?;
    let status = derive_status(&raw_status, quantity, executed_quantity).map_err(|e| match e {
        BrokerError::Malformed(m) => BrokerError::Malformed(format!("order {txid}: {m}")),
        other => other,
    })?;

    let descr = info.get("descr");
    let raw_pair = descr.and_then(|d| d.get("pair")).and_then(Value::as_str).unwrap_or("");
    let symbol = pairs.lookup(raw_pair).map(|p| p.canonical.clone()).unwrap_or_else(|| raw_pair.to_string());
    let side = match descr.and_then(|d| d.get("type")).and_then(Value::as_str) {
        Some("buy") => Some(Side::Buy),
        Some("sell") => Some(Side::Sell),
        _ => None,
    };
    let kind = match descr.and_then(|d| d.get("ordertype")).and_then(Value::as_str) {
        Some("market") => Some(OrderKind::Market),
        Some("limit") => match descr.and_then(|d| opt_dec(d, "price").transpose()).transpose()? {
            Some(price) if price.is_positive() => Some(OrderKind::Limit { price }),
            _ => None,
        },
        _ => None,
    };

    let avg_price = if executed_quantity.is_positive() {
        opt_dec(info, "price")?.filter(|p| p.is_positive())
    } else {
        None
    };
    let userref = userref_of(info)?;
    let tag = userref.and_then(|u| userrefs.tag_for(u)).map(str::to_string);
    let reason = info.get("reason").and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string);

    Ok(OrderReport {
        broker_order_id: txid.to_string(),
        userref,
        tag,
        symbol,
        side,
        kind,
        status,
        raw_status,
        reason,
        quantity,
        executed_quantity,
        avg_price,
        cost: opt_dec(info, "cost")?,
        fee: opt_dec(info, "fee")?,
        open_time: info.get("opentm").and_then(Value::as_f64),
        close_time: info.get("closetm").and_then(Value::as_f64),
    })
}

fn parse_order_map(
    map: &Value,
    what: &str,
    pairs: &PairTable,
    userrefs: &UserrefMap,
) -> Result<Vec<OrderReport>, BrokerError> {
    let obj = map.as_object().ok_or_else(|| BrokerError::Malformed(format!("{what} is not an object")))?;
    let mut out = Vec::with_capacity(obj.len());
    for (txid, info) in obj {
        out.push(parse_order_info(txid, info, pairs, userrefs)?);
    }
    out.sort_by(|a, b| a.broker_order_id.cmp(&b.broker_order_id));
    Ok(out)
}

/// `QueryOrders`: `result` is `{txid: info, ...}`.
pub fn parse_query_orders(env: &Envelope, pairs: &PairTable, userrefs: &UserrefMap) -> Result<Vec<OrderReport>, BrokerError> {
    parse_order_map(result_of(env)?, "QueryOrders result", pairs, userrefs)
}

/// `OpenOrders`: `result` is `{"open": {txid: info, ...}}`.
pub fn parse_open_orders(env: &Envelope, pairs: &PairTable, userrefs: &UserrefMap) -> Result<Vec<OrderReport>, BrokerError> {
    let open = result_of(env)?
        .get("open")
        .ok_or_else(|| BrokerError::Malformed("OpenOrders result has no `open`".into()))?;
    parse_order_map(open, "OpenOrders `open`", pairs, userrefs)
}

/// `ClosedOrders`: `result` is `{"closed": {txid: info}, "count": n}`. If `count` says there are
/// more orders than were returned (pagination), fail rather than under-report.
pub fn parse_closed_orders(env: &Envelope, pairs: &PairTable, userrefs: &UserrefMap) -> Result<Vec<OrderReport>, BrokerError> {
    let r = result_of(env)?;
    let closed = r
        .get("closed")
        .ok_or_else(|| BrokerError::Malformed("ClosedOrders result has no `closed`".into()))?;
    let orders = parse_order_map(closed, "ClosedOrders `closed`", pairs, userrefs)?;
    if let Some(count) = r.get("count").and_then(Value::as_u64) {
        if count > orders.len() as u64 {
            return Err(BrokerError::Malformed(format!(
                "ClosedOrders is paginated ({count} total, {} returned); refusing to under-report",
                orders.len()
            )));
        }
    }
    Ok(orders)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn d(s: &str) -> Dec {
        Dec::parse(s).unwrap()
    }

    fn is_overfill(e: &BrokerError) -> bool {
        matches!(e, BrokerError::Malformed(m) if m.contains("overfill anomaly"))
    }

    #[test]
    fn overfill_is_an_anomaly_in_every_status_never_an_inflated_filled() {
        for raw in ["pending", "open", "closed", "canceled", "expired"] {
            let e = derive_status(raw, d("1"), d("1.02")).unwrap_err();
            assert!(is_overfill(&e), "{raw}: {e:?}");
        }
    }

    #[test]
    fn the_smallest_possible_overfill_is_caught_and_an_exact_fill_is_not() {
        assert!(is_overfill(&derive_status("closed", d("1.00000000"), d("1.00000001")).unwrap_err()));
        assert_eq!(derive_status("closed", d("1.00000000"), d("1")).unwrap(), OrderStatus::Filled);
        assert_eq!(derive_status("canceled", d("1"), d("1.00000000")).unwrap(), OrderStatus::Filled);
        assert_eq!(derive_status("open", d("1"), d("0.99999999")).unwrap(), OrderStatus::PartiallyFilled);
    }

    fn order_json(status: &str, vol: &str, exec: &str) -> Value {
        json!({
            "userref": null,
            "status": status,
            "descr": {"pair": "XBTUSD", "type": "buy", "ordertype": "market", "price": "0"},
            "vol": vol, "vol_exec": exec, "cost": "600.0", "fee": "0.96", "price": "60000.0"
        })
    }

    #[test]
    fn parse_order_info_rejects_a_duplicated_fill_report_and_names_the_order() {
        let pairs = PairTable::builtin();
        let refs = UserrefMap::new();
        let e = parse_order_info("OOVER1-AAAAA-BBBBBB", &order_json("closed", "0.01000000", "0.02000000"), &pairs, &refs)
            .unwrap_err();
        match &e {
            BrokerError::Malformed(m) => {
                assert!(m.contains("OOVER1-AAAAA-BBBBBB"), "{m}");
                assert!(m.contains("overfill anomaly") && m.contains("0.02000000") && m.contains("0.01000000"), "{m}");
            }
            other => panic!("{other:?}"),
        }
        // the legitimate neighbours still parse
        let ok = parse_order_info("OOK", &order_json("closed", "0.01000000", "0.01000000"), &pairs, &refs).unwrap();
        assert_eq!((ok.status, ok.executed_quantity), (OrderStatus::Filled, d("0.01")));
    }

    #[test]
    fn one_overfilled_order_fails_the_whole_listing_so_the_caller_halts() {
        let pairs = PairTable::builtin();
        let refs = UserrefMap::new();
        let env = Envelope {
            result: Some(json!({
                "OGOOD-AAAAA-BBBBBB": order_json("closed", "0.01", "0.01"),
                "OOVER-AAAAA-BBBBBB": order_json("canceled", "0.01", "0.03"),
            })),
            warnings: vec![],
        };
        let e = parse_query_orders(&env, &pairs, &refs).unwrap_err();
        assert!(is_overfill(&e), "{e:?}");
        let open = Envelope { result: Some(json!({"open": {"OOVER-AAAAA-BBBBBB": order_json("open", "0.01", "0.02")}})), warnings: vec![] };
        assert!(is_overfill(&parse_open_orders(&open, &pairs, &refs).unwrap_err()));
        let closed = Envelope { result: Some(json!({"closed": {"OOVER-AAAAA-BBBBBB": order_json("closed", "0.01", "0.02")}, "count": 1})), warnings: vec![] };
        assert!(is_overfill(&parse_closed_orders(&closed, &pairs, &refs).unwrap_err()));
    }
}
