//! Kraken pair table (precision and minimum order sizes) and asset-name mapping.
//!
//! The built-in table is written from MEMORY OF KRAKEN'S PUBLIC `AssetPairs` DATA and is NOT
//! VERIFIED: minimums in particular change over time. The intended workflow is to load the live
//! public `AssetPairs` response ([`PairTable::from_asset_pairs_json`], no credentials needed) or
//! apply hand overrides ([`PairTable::apply_overrides_json`]) and treat the built-in rows only as
//! a fallback. An order rejected by us for size is safe; an order below Kraken's real minimum is
//! rejected by Kraken, so either error is loud rather than silent.

use crate::decimal::Dec;
use crate::error::BrokerError;
use crate::types::BalanceKind;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairSource {
    /// Hard-coded in this crate (unverified, from memory).
    Builtin,
    /// Parsed from a Kraken `AssetPairs` response.
    AssetPairs,
    /// Some fields replaced by an operator override.
    Override,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairInfo {
    /// Canonical `BASE/QUOTE`, e.g. `BTC/USD`.
    pub canonical: String,
    /// Kraken altname, used as the `pair` parameter we send, e.g. `XBTUSD`.
    pub altname: String,
    /// Kraken result key, e.g. `XXBTZUSD`.
    pub rest_name: String,
    /// Websocket name, e.g. `XBT/USD`.
    pub ws_name: String,
    /// Kraken asset codes.
    pub base_asset: String,
    pub quote_asset: String,
    /// Price decimals.
    pub pair_decimals: u32,
    /// Volume decimals.
    pub lot_decimals: u32,
    /// Minimum order volume in base units.
    pub order_min: Dec,
    /// Minimum order cost in quote units, if known.
    pub cost_min: Option<Dec>,
    /// Price tick, if it differs from 10^-pair_decimals.
    pub tick_size: Option<Dec>,
    /// Kraken trading status (`online`, `cancel_only`, ...) when known.
    pub status: Option<String>,
    pub source: PairSource,
}

impl PairInfo {
    fn matches(&self, name: &str) -> bool {
        let n = name.trim();
        self.canonical.eq_ignore_ascii_case(n)
            || self.altname.eq_ignore_ascii_case(n)
            || self.rest_name.eq_ignore_ascii_case(n)
            || self.ws_name.eq_ignore_ascii_case(n)
    }
}

/// Legacy Kraken asset codes that carry an `X`/`Z` prefix. Explicit list: stripping by shape
/// would wrongly mangle real tickers such as `ZEUS` or `XION`.
const LEGACY_PREFIXED: [&str; 17] = [
    "XXBT", "XETH", "XETC", "XLTC", "XMLN", "XREP", "XXDG", "XXLM", "XXMR", "XXRP", "XZEC", "ZUSD", "ZEUR", "ZGBP",
    "ZCAD", "ZJPY", "ZAUD",
];

/// Kraken asset code -> canonical ticker, plus whether it is a plain spot balance or an
/// earn/staked variant (suffix `.S .F .M .P .B`).
pub fn normalize_asset(raw: &str) -> (String, BalanceKind) {
    let upper = raw.trim().to_ascii_uppercase();
    let (base, kind) = match upper.split_once('.') {
        Some((b, _suffix)) => (b.to_string(), BalanceKind::Earn),
        None => (upper.clone(), BalanceKind::Spot),
    };
    let stripped = if LEGACY_PREFIXED.contains(&base.as_str()) { base[1..].to_string() } else { base };
    let canonical = match stripped.as_str() {
        "XBT" => "BTC".to_string(),
        "XDG" => "DOGE".to_string(),
        _ => stripped,
    };
    (canonical, kind)
}

#[derive(Debug, Clone, Default)]
pub struct PairTable {
    pairs: Vec<PairInfo>,
}

#[derive(Deserialize)]
struct RawAssetPair {
    altname: String,
    wsname: Option<String>,
    base: String,
    quote: String,
    pair_decimals: u32,
    lot_decimals: u32,
    ordermin: Dec,
    costmin: Option<Dec>,
    tick_size: Option<Dec>,
    status: Option<String>,
}

#[derive(Deserialize)]
struct PairOverride {
    pair_decimals: Option<u32>,
    lot_decimals: Option<u32>,
    ordermin: Option<Dec>,
    costmin: Option<Dec>,
    tick_size: Option<Dec>,
    status: Option<String>,
}

impl PairTable {
    /// Built-in fallback rows for BTC/USD and ETH/USD. UNVERIFIED (from memory of Kraken data).
    pub fn builtin() -> Self {
        let d = |s: &str| Dec::parse(s).expect("builtin pair literal");
        let row = |canonical: &str,
                   altname: &str,
                   rest: &str,
                   ws: &str,
                   base: &str,
                   pd: u32,
                   min: &str,
                   tick: &str| PairInfo {
            canonical: canonical.to_string(),
            altname: altname.to_string(),
            rest_name: rest.to_string(),
            ws_name: ws.to_string(),
            base_asset: base.to_string(),
            quote_asset: "ZUSD".to_string(),
            pair_decimals: pd,
            lot_decimals: 8,
            order_min: d(min),
            cost_min: Some(d("0.5")),
            tick_size: Some(d(tick)),
            status: None,
            source: PairSource::Builtin,
        };
        Self {
            pairs: vec![
                row("BTC/USD", "XBTUSD", "XXBTZUSD", "XBT/USD", "XXBT", 1, "0.0001", "0.1"),
                row("ETH/USD", "ETHUSD", "XETHZUSD", "ETH/USD", "XETH", 2, "0.002", "0.01"),
            ],
        }
    }

    /// Build a table from a Kraken public `AssetPairs` response, either the full envelope
    /// (`{"error":[],"result":{...}}`) or just the `result` object. Dark-pool keys (`*.d`) are skipped.
    pub fn from_asset_pairs_json(json: &str) -> Result<Self, BrokerError> {
        let v: serde_json::Value = serde_json::from_str(json).map_err(|e| BrokerError::Malformed(e.to_string()))?;
        let result = match v.get("result") {
            Some(r) => r,
            None => &v,
        };
        let obj = result
            .as_object()
            .ok_or_else(|| BrokerError::Malformed("AssetPairs result is not an object".into()))?;
        let mut pairs = Vec::new();
        for (key, val) in obj {
            if key.ends_with(".d") {
                continue;
            }
            let raw: RawAssetPair = serde_json::from_value(val.clone())
                .map_err(|e| BrokerError::Malformed(format!("AssetPairs entry {key}: {e}")))?;
            let (b, _) = normalize_asset(&raw.base);
            let (q, _) = normalize_asset(&raw.quote);
            pairs.push(PairInfo {
                canonical: format!("{b}/{q}"),
                altname: raw.altname.clone(),
                rest_name: key.clone(),
                ws_name: raw.wsname.unwrap_or_else(|| raw.altname.clone()),
                base_asset: raw.base,
                quote_asset: raw.quote,
                pair_decimals: raw.pair_decimals,
                lot_decimals: raw.lot_decimals,
                order_min: raw.ordermin,
                cost_min: raw.costmin,
                tick_size: raw.tick_size,
                status: raw.status,
                source: PairSource::AssetPairs,
            });
        }
        Ok(Self { pairs })
    }

    /// Apply partial overrides: `{"XBTUSD": {"ordermin": "0.0002", "lot_decimals": 8}}`. Keys may
    /// be any alias of a pair already in the table (altname, REST name, ws name, canonical).
    /// Unknown pairs are an error: an override cannot invent the symbol mapping.
    pub fn apply_overrides_json(&mut self, json: &str) -> Result<(), BrokerError> {
        let map: BTreeMap<String, PairOverride> =
            serde_json::from_str(json).map_err(|e| BrokerError::Config(format!("pair overrides: {e}")))?;
        for (name, ov) in map {
            let p = self
                .pairs
                .iter_mut()
                .find(|p| p.matches(&name))
                .ok_or_else(|| BrokerError::Config(format!("override for unknown pair {name}")))?;
            if let Some(v) = ov.pair_decimals {
                p.pair_decimals = v;
            }
            if let Some(v) = ov.lot_decimals {
                p.lot_decimals = v;
            }
            if let Some(v) = ov.ordermin {
                p.order_min = v;
            }
            if let Some(v) = ov.costmin {
                p.cost_min = Some(v);
            }
            if let Some(v) = ov.tick_size {
                p.tick_size = Some(v);
            }
            if let Some(v) = ov.status {
                p.status = Some(v);
            }
            p.source = PairSource::Override;
        }
        Ok(())
    }

    /// Insert or replace a row (matched by altname).
    pub fn upsert(&mut self, info: PairInfo) {
        match self.pairs.iter_mut().find(|p| p.altname.eq_ignore_ascii_case(&info.altname)) {
            Some(slot) => *slot = info,
            None => self.pairs.push(info),
        }
    }

    /// Replace/extend this table with every row of `other`.
    pub fn merge(&mut self, other: PairTable) {
        for p in other.pairs {
            self.upsert(p);
        }
    }

    /// Look up by canonical name, altname, REST name or ws name (case-insensitive).
    pub fn lookup(&self, name: &str) -> Option<&PairInfo> {
        self.pairs.iter().find(|p| p.matches(name))
    }

    pub fn pairs(&self) -> &[PairInfo] {
        &self.pairs
    }
}
