//! Kraken endpoint handlers. Each returns the `result` value or the list of error strings.

use super::wire::{canonical_asset, kraken_asset_code, param, reject_codes};
use super::KeyPermissions;
use crate::broker::World;
use crate::clock::nanos_to_secs;
use crate::exchange::{CancelTarget, NewOrder, Order, OrderStatus, PairSpec, Placed, Reject, Tif};
use crate::money::fixed;
use broker_adapters::{Dec, OrderKind, Side};
use serde_json::{json, Map, Value};

type Reply = Result<Value, Vec<String>>;

fn denied() -> Vec<String> {
    vec!["EGeneral:Permission denied".to_string()]
}

fn invalid(field: &str) -> Vec<String> {
    reject_codes(&Reject::InvalidArguments(field.to_string()))
}

fn rejected(r: &Reject) -> Vec<String> {
    reject_codes(r)
}

fn parse_userref(params: &[(String, String)]) -> Result<Option<i32>, Vec<String>> {
    match param(params, "userref") {
        None | Some("") => Ok(None),
        Some(s) => s.parse::<i32>().map(Some).map_err(|_| invalid("userref")),
    }
}

// ---------------------------------------------------------------- public

pub(super) fn ticker(w: &World, params: &[(String, String)]) -> Reply {
    let names = param(params, "pair").ok_or_else(|| invalid(""))?;
    let mut out = Map::new();
    for name in names.split(',') {
        let spec = w.core.resolve_wire_pair(name).ok_or_else(|| rejected(&Reject::UnknownPair(name.to_string())))?;
        let m = w.core.market(&spec.canonical).expect("resolved pair has a market");
        let dp = spec.pair_decimals.max(5);
        out.insert(
            spec.rest_name.clone(),
            json!({
                "a": [fixed(m.ask, dp), "1", "1.000"],
                "b": [fixed(m.bid, dp), "1", "1.000"],
                "c": [fixed(m.last, dp), "0.00100000"],
                "v": ["1000.00000000", "2000.00000000"],
                "p": [fixed(m.last, dp), fixed(m.last, dp)],
                "t": [1000, 2000],
                "l": [fixed(m.bid, dp), fixed(m.bid, dp)],
                "h": [fixed(m.ask, dp), fixed(m.ask, dp)],
                "o": fixed(m.last, dp),
            }),
        );
    }
    Ok(Value::Object(out))
}

fn asset_pair_json(s: &PairSpec) -> Value {
    json!({
        "altname": s.altname,
        "wsname": s.ws_name,
        "aclass_base": "currency",
        "base": kraken_asset_code(s.base()),
        "aclass_quote": "currency",
        "quote": kraken_asset_code(s.quote()),
        "lot": "unit",
        "pair_decimals": s.pair_decimals,
        "lot_decimals": s.lot_decimals,
        "lot_multiplier": 1,
        "ordermin": s.order_min.to_string(),
        "costmin": s.cost_min.to_string(),
        "tick_size": s.tick_size.to_string(),
        "status": s.status,
    })
}

pub(super) fn asset_pairs(w: &World, params: &[(String, String)]) -> Reply {
    let mut out = Map::new();
    match param(params, "pair") {
        Some(names) => {
            for name in names.split(',') {
                let s = w.core.resolve_wire_pair(name).ok_or_else(|| rejected(&Reject::UnknownPair(name.to_string())))?;
                out.insert(s.rest_name.clone(), asset_pair_json(s));
            }
        }
        None => {
            for s in w.core.pair_specs() {
                out.insert(s.rest_name.clone(), asset_pair_json(s));
            }
        }
    }
    Ok(Value::Object(out))
}

// ---------------------------------------------------------------- balances

pub(super) fn balance(w: &World, account: &str, perms: KeyPermissions) -> Reply {
    if !perms.query_funds {
        return Err(denied());
    }
    let mut out = Map::new();
    for (asset, amount) in w.core.balances(account) {
        if amount.is_zero() {
            continue; // Kraken leaves empty balances out
        }
        out.insert(kraken_asset_code(&asset), Value::String(amount.to_string()));
    }
    Ok(Value::Object(out))
}

pub(super) fn trade_balance(w: &World, account: &str, perms: KeyPermissions, params: &[(String, String)]) -> Reply {
    if !perms.query_funds {
        return Err(denied());
    }
    let asset = canonical_asset(param(params, "asset").unwrap_or("ZUSD"));
    let known = asset == "USD" || w.core.pair_specs().iter().any(|s| s.quote() == asset);
    if !known {
        return Err(invalid("asset"));
    }
    let equity = w.core.equity(account, &asset).to_string();
    // Spot only: no margin, no open positions. `eb` = `tb` = `e` = combined balance.
    Ok(json!({ "eb": equity, "tb": equity, "m": "0.0000", "n": "0.0000", "c": "0.0000", "v": "0.0000", "e": equity, "mf": equity }))
}

// ---------------------------------------------------------------- orders

fn describe(spec: &PairSpec, side: Side, kind: OrderKind, volume: Dec) -> String {
    let vol = fixed(volume, spec.lot_decimals);
    match kind {
        OrderKind::Market => format!("{} {vol} {} @ market", side.as_str(), spec.altname),
        OrderKind::Limit { price } => {
            format!("{} {vol} {} @ limit {}", side.as_str(), spec.altname, fixed(price, spec.pair_decimals))
        }
    }
}

pub(super) fn add_order(w: &mut World, now: u64, account: &str, perms: KeyPermissions, params: &[(String, String)]) -> Reply {
    if !perms.create_orders {
        return Err(denied());
    }
    let pair_name = param(params, "pair").ok_or_else(|| invalid("pair"))?;
    let spec = w
        .core
        .resolve_wire_pair(pair_name)
        .ok_or_else(|| rejected(&Reject::UnknownPair(pair_name.to_string())))?
        .clone();
    let side = match param(params, "type") {
        Some("buy") => Side::Buy,
        Some("sell") => Side::Sell,
        _ => return Err(invalid("type")),
    };
    let volume = param(params, "volume").and_then(|s| Dec::parse(s).ok()).ok_or_else(|| invalid("volume"))?;
    let kind = match param(params, "ordertype") {
        Some("market") => OrderKind::Market,
        Some("limit") => {
            let price = param(params, "price").and_then(|s| Dec::parse(s).ok()).ok_or_else(|| invalid("price"))?;
            OrderKind::Limit { price }
        }
        _ => return Err(invalid("ordertype")),
    };
    let tif = match param(params, "timeinforce") {
        None | Some("GTC") => Tif::Gtc,
        Some("IOC") => Tif::Ioc,
        Some(_) => return Err(invalid("timeinforce")),
    };
    let mut post_only = false;
    if let Some(flags) = param(params, "oflags") {
        for f in flags.split(',').filter(|f| !f.is_empty()) {
            match f {
                "post" => post_only = true,
                "fciq" | "nompp" => {}
                _ => return Err(invalid("oflags")),
            }
        }
    }
    match param(params, "reduce_only") {
        None | Some("false") => {}
        // Margin-only on the real exchange; this fake is a spot venue.
        Some(_) => return Err(invalid("reduce_only")),
    }
    let validate = match param(params, "validate") {
        None | Some("false") => false,
        Some("true") => true,
        Some(_) => return Err(invalid("validate")),
    };
    let userref = parse_userref(params)?;
    let new = NewOrder { account: account.to_string(), pair: spec.canonical.clone(), side, kind, volume, userref, tif, post_only, validate };
    let descr = json!({ "order": describe(&spec, side, kind, volume) });
    match w.core.place(now, &new).map_err(|r| rejected(&r))? {
        Placed::Validated => Ok(json!({ "descr": descr })),
        Placed::Created { txid } => Ok(json!({ "descr": descr, "txid": [txid] })),
    }
}

pub(super) fn cancel_order(w: &mut World, now: u64, account: &str, perms: KeyPermissions, params: &[(String, String)]) -> Reply {
    if !perms.cancel_orders {
        return Err(denied());
    }
    let id = param(params, "txid").filter(|s| !s.is_empty()).ok_or_else(|| invalid("txid"))?;
    // `txid` accepts a transaction id or a user reference.
    let is_txid = w.core.orders_of(account).iter().any(|o| o.txid == id);
    let target = if is_txid {
        CancelTarget::Txid(id.to_string())
    } else if let Ok(u) = id.parse::<i32>() {
        CancelTarget::Userref(u)
    } else {
        CancelTarget::Txid(id.to_string())
    };
    let r = w.core.cancel(now, account, &target).map_err(|r| rejected(&r))?;
    let mut out = json!({ "count": r.count });
    if r.pending {
        out["pending"] = json!(r.txids);
    }
    Ok(out)
}

fn order_info(o: &Order, spec: &PairSpec) -> Value {
    let (ordertype, limit_price) = match o.kind {
        OrderKind::Market => ("market", "0".to_string()),
        OrderKind::Limit { price } => ("limit", fixed(price, spec.pair_decimals)),
    };
    let mut flags = vec!["fciq"];
    if o.post_only {
        flags.push("post");
    }
    let vol_exec = o.reported_vol_exec();
    let mut v = json!({
        "refid": null,
        "userref": o.userref,
        "status": o.status.as_kraken(),
        "reason": o.reason,
        "opentm": nanos_to_secs(o.open_time_nanos),
        "starttm": 0,
        "expiretm": 0,
        "descr": {
            "pair": spec.altname,
            "type": o.side.as_str(),
            "ordertype": ordertype,
            "price": limit_price,
            "price2": "0",
            "leverage": "none",
            "order": describe(spec, o.side, o.kind, o.volume),
            "close": "",
        },
        "vol": fixed(o.volume, spec.lot_decimals),
        "vol_exec": fixed(vol_exec, spec.lot_decimals),
        "cost": o.reported_cost().to_string(),
        "fee": o.reported_fee().to_string(),
        "price": o.reported_avg_price().map(|p| p.to_string()).unwrap_or_else(|| "0".to_string()),
        "stopprice": "0.00000",
        "limitprice": "0.00000",
        "misc": "",
        "oflags": flags.join(","),
    });
    if let Some(t) = o.close_time_nanos {
        v["closetm"] = json!(nanos_to_secs(t));
    }
    v
}

fn info_of(w: &World, o: &Order) -> Value {
    let spec = &w.core.market(&o.pair).expect("order pair exists").spec;
    order_info(o, spec)
}

pub(super) fn query_orders(w: &World, account: &str, perms: KeyPermissions, params: &[(String, String)]) -> Reply {
    if !perms.query_orders {
        return Err(denied());
    }
    let ids = param(params, "txid").filter(|s| !s.is_empty()).ok_or_else(|| invalid("txid"))?;
    let userref = parse_userref(params)?;
    let mine = w.core.orders_of(account);
    let mut out = Map::new();
    for id in ids.split(',') {
        let o = mine.iter().find(|o| o.txid == id).ok_or_else(|| rejected(&Reject::UnknownOrder))?;
        if userref.is_some() && o.userref != userref {
            continue;
        }
        out.insert(o.txid.clone(), info_of(w, o));
    }
    Ok(Value::Object(out))
}

pub(super) fn open_orders(w: &World, account: &str, perms: KeyPermissions, params: &[(String, String)]) -> Reply {
    if !perms.query_orders {
        return Err(denied());
    }
    let userref = parse_userref(params)?;
    let mut open = Map::new();
    for o in w.core.orders_of(account).into_iter().filter(|o| o.status.is_live()) {
        if userref.is_some() && o.userref != userref {
            continue;
        }
        open.insert(o.txid.clone(), info_of(w, o));
    }
    Ok(json!({ "open": open }))
}

pub(super) fn closed_orders(w: &World, account: &str, perms: KeyPermissions, params: &[(String, String)]) -> Reply {
    if !perms.query_orders {
        return Err(denied());
    }
    let userref = parse_userref(params)?;
    let ofs: usize = match param(params, "ofs") {
        None | Some("") => 0,
        Some(s) => s.parse().map_err(|_| invalid("ofs"))?,
    };
    let mut closed: Vec<&Order> = w
        .core
        .orders_of(account)
        .into_iter()
        .filter(|o| matches!(o.status, OrderStatus::Closed | OrderStatus::Canceled | OrderStatus::Expired))
        .filter(|o| userref.is_none() || o.userref == userref)
        .collect();
    // Most recently closed first, like the real endpoint.
    closed.sort_by(|a, b| b.close_time_nanos.cmp(&a.close_time_nanos).then(b.seq.cmp(&a.seq)));
    let total = closed.len();
    let mut page = Map::new();
    for o in closed.into_iter().skip(ofs).take(w.kraken.closed_page_size) {
        page.insert(o.txid.clone(), info_of(w, o));
    }
    Ok(json!({ "closed": page, "count": total }))
}
