//! Kraken spot adapter.
//!
//! Endpoint paths and field names are VERIFIED-FROM-REPO-CODE where they appear in SignalEngine's
//! `generic/config.rs` (`/0/private/AddOrder`, `CancelOrder`, `QueryOrders`, `Balance`, POST form
//! encoding, `API-Key`/`API-Sign` headers, `userref`, `pair`/`type`/`ordertype`/`volume`/`price`
//! parameters) and in its parser fixtures (`QueryOrders` result keyed by txid with
//! `status`/`vol_exec`/`price`; `AddOrder` result `{descr:{order}, txid:[..]}`; `error` array).
//! Everything else (OpenOrders, ClosedOrders, TradeBalance, Ticker layout, `validate`,
//! `reduce_only`, `timeinforce`, `oflags`, error-code names, the `userref` filters) is
//! FROM-MEMORY-OF-DOCS and needs a live validate-only / read-only smoke test (WP3.5).

pub mod auth;
pub mod order;
pub mod pairs;
pub mod parse;
pub mod userref;

use crate::error::{BrokerError, ErrorClass, ExchangeError};
use crate::nonce::NonceGenerator;
use crate::transport::{HttpResponse, HttpTransport, TransportError};
use crate::types::{
    Balances, BrokerAdapter, CancelOutcome, OrderReport, OrderRequest, PlaceOutcome, Quote,
};
use auth::{build_private_request, build_public_request, KrakenCredentials};
use order::{prepare_order, PrepareOptions, PreparedOrder};
use pairs::{PairInfo, PairTable};
use parse::{parse_envelope, Envelope, TradeBalance};
use std::fmt;
use std::sync::{Arc, Mutex};
use userref::UserrefMap;

pub const DEFAULT_BASE_URL: &str = "https://api.kraken.com";

pub mod paths {
    pub const ADD_ORDER: &str = "/0/private/AddOrder";
    pub const CANCEL_ORDER: &str = "/0/private/CancelOrder";
    pub const QUERY_ORDERS: &str = "/0/private/QueryOrders";
    pub const OPEN_ORDERS: &str = "/0/private/OpenOrders";
    pub const CLOSED_ORDERS: &str = "/0/private/ClosedOrders";
    pub const BALANCE: &str = "/0/private/Balance";
    pub const TRADE_BALANCE: &str = "/0/private/TradeBalance";
    pub const TICKER: &str = "/0/public/Ticker";
}

#[derive(Debug, Clone)]
pub struct KrakenConfig {
    pub base_url: String,
    /// Allow `reduce_only` (margin only). Default false: spot orders with it are refused.
    pub allow_reduce_only: bool,
    /// Send `validate=true` on every order regardless of the request (paper mode).
    pub force_validate: bool,
}

impl Default for KrakenConfig {
    fn default() -> Self {
        Self { base_url: DEFAULT_BASE_URL.to_string(), allow_reduce_only: false, force_validate: false }
    }
}

/// Internal call failure, kept fine-grained so placement can tell "definitely not sent" from
/// "outcome unknown".
enum CallError {
    Local(BrokerError),
    Transport(TransportError),
    Http(u16),
    Malformed(String),
    Exchange(Vec<ExchangeError>),
}

impl From<CallError> for BrokerError {
    fn from(e: CallError) -> Self {
        match e {
            CallError::Local(b) => b,
            CallError::Transport(t) => BrokerError::Transport(t),
            CallError::Http(s) => BrokerError::Http(s),
            CallError::Malformed(m) => BrokerError::Malformed(m),
            CallError::Exchange(v) => BrokerError::Exchange(v),
        }
    }
}

fn interpret(resp: HttpResponse) -> Result<Envelope, CallError> {
    match parse_envelope(&resp.body) {
        Ok(env) if resp.status == 200 => Ok(env),
        Ok(_) => Err(CallError::Http(resp.status)),
        Err(BrokerError::Exchange(e)) => Err(CallError::Exchange(e)),
        Err(BrokerError::Malformed(_)) if resp.status != 200 => Err(CallError::Http(resp.status)),
        Err(BrokerError::Malformed(m)) => Err(CallError::Malformed(m)),
        Err(other) => Err(CallError::Local(other)),
    }
}

pub struct KrakenAdapter {
    config: KrakenConfig,
    creds: KrakenCredentials,
    transport: Arc<dyn HttpTransport>,
    nonces: NonceGenerator,
    pairs: PairTable,
    userrefs: Mutex<UserrefMap>,
    /// Held across "allocate nonce + send" so requests reach Kraken in nonce order.
    send_lock: Mutex<()>,
}

impl fmt::Debug for KrakenAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KrakenAdapter")
            .field("base_url", &self.config.base_url)
            .field("credentials", &self.creds)
            .field("force_validate", &self.config.force_validate)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl KrakenAdapter {
    pub fn new(
        config: KrakenConfig,
        creds: KrakenCredentials,
        transport: Arc<dyn HttpTransport>,
        nonces: NonceGenerator,
        pairs: PairTable,
    ) -> Self {
        Self {
            config,
            creds,
            transport,
            nonces,
            pairs,
            userrefs: Mutex::new(UserrefMap::new()),
            send_lock: Mutex::new(()),
        }
    }

    /// Start from a persisted userref table.
    pub fn with_userref_map(self, map: UserrefMap) -> Self {
        *lock(&self.userrefs) = map;
        self
    }

    pub fn key_id(&self) -> String {
        self.creds.key_id()
    }

    pub fn pair_table(&self) -> &PairTable {
        &self.pairs
    }

    /// Snapshot of the tag <-> userref table. Persist it (or the new rows) before sending.
    pub fn userref_snapshot(&self) -> UserrefMap {
        lock(&self.userrefs).clone()
    }

    /// Assign (or fetch) the userref for `tag` without sending anything, so the caller can persist
    /// it BEFORE `place_order`.
    pub fn reserve_userref(&self, tag: &str) -> Result<i32, BrokerError> {
        lock(&self.userrefs).assign(tag).map_err(|e| BrokerError::Userref(e.to_string()))
    }

    /// Pure: validate and round a request into the exact order that would be sent. Assigns the tag's userref.
    pub fn prepare(&self, req: &OrderRequest) -> Result<PreparedOrder, BrokerError> {
        let pair = self.pair_for(&req.symbol)?;
        let userref = self.reserve_userref(&req.tag)?;
        prepare_order(
            req,
            pair,
            userref,
            &PrepareOptions { allow_reduce_only: self.config.allow_reduce_only, force_validate: self.config.force_validate },
        )
    }

    fn pair_for(&self, symbol: &str) -> Result<&PairInfo, BrokerError> {
        self.pairs.lookup(symbol).ok_or_else(|| BrokerError::UnknownSymbol(symbol.to_string()))
    }

    fn private_call(&self, path: &str, params: &[(String, String)]) -> Result<Envelope, CallError> {
        let _guard = lock(&self.send_lock);
        let nonce = self.nonces.next().map_err(|e| CallError::Local(BrokerError::Nonce(e)))?;
        let req = build_private_request(&self.creds, &self.config.base_url, path, nonce, params);
        let resp = self.transport.execute(&req).map_err(CallError::Transport)?;
        interpret(resp)
    }

    fn public_call(&self, path: &str, params: &[(String, String)]) -> Result<Envelope, CallError> {
        let req = build_public_request(&self.config.base_url, path, params);
        let resp = self.transport.execute(&req).map_err(CallError::Transport)?;
        interpret(resp)
    }

    /// `TradeBalance` in `asset` (default ZUSD on Kraken's side when `None`).
    pub fn trade_balance(&self, asset: Option<&str>) -> Result<TradeBalance, BrokerError> {
        let params: Vec<(String, String)> = asset.map(|a| vec![("asset".to_string(), a.to_string())]).unwrap_or_default();
        let env = self.private_call(paths::TRADE_BALANCE, &params)?;
        parse::parse_trade_balance(&env)
    }

    /// Cancel every open order carrying the userref assigned to `tag` (Kraken accepts a userref
    /// in the `txid` field of `CancelOrder`, FROM-MEMORY-OF-DOCS).
    pub fn cancel_by_tag(&self, tag: &str) -> Result<CancelOutcome, BrokerError> {
        let userref = lock(&self.userrefs).get_userref(tag).ok_or_else(|| BrokerError::UnknownTag(tag.to_string()))?;
        let env = self.private_call(paths::CANCEL_ORDER, &[("txid".to_string(), userref.to_string())])?;
        parse::parse_cancel(&env)
    }

    /// Cancel, then re-query so executed quantity from a partial fill is captured.
    ///
    /// If Kraken answers the cancel with "unknown order" (`EOrder:Unknown order`), the order is
    /// no longer cancelable, typically because it FILLED before the cancel landed. That is not a
    /// failure of the settle: the order is re-queried and the final report is returned together
    /// with `CancelOutcome { canceled_count: 0, pending: false }` (nothing was canceled). The
    /// report is the truth: check its status (`Filled`, `PartiallyFilledThenCanceled`, ...); an
    /// order still `Open` after a refused cancel is returned as such, not hidden. If the
    /// re-query also finds no such order, `BrokerError::CancelTargetNotFound` is returned.
    pub fn cancel_and_settle(&self, broker_order_id: &str) -> Result<(CancelOutcome, OrderReport), BrokerError> {
        let all_unknown = |errs: &[ExchangeError]| !errs.is_empty() && errs.iter().all(|e| e.class == ErrorClass::UnknownOrder);
        let outcome = match self.cancel_order(broker_order_id) {
            Ok(o) => o,
            Err(BrokerError::Exchange(errs)) if all_unknown(&errs) => {
                return match self.get_order(broker_order_id) {
                    Ok(report) => Ok((CancelOutcome { canceled_count: 0, pending: false }, report)),
                    Err(BrokerError::NotFound(_)) => Err(BrokerError::CancelTargetNotFound(broker_order_id.to_string())),
                    Err(BrokerError::Exchange(e)) if all_unknown(&e) => {
                        Err(BrokerError::CancelTargetNotFound(broker_order_id.to_string()))
                    }
                    Err(other) => Err(other),
                };
            }
            Err(e) => return Err(e),
        };
        let report = self.get_order(broker_order_id)?;
        Ok((outcome, report))
    }

    fn orders_with_userref(&self, path: &str, userref: i32) -> Result<Vec<OrderReport>, BrokerError> {
        let env = self.private_call(path, &[("userref".to_string(), userref.to_string())])?;
        let map = lock(&self.userrefs);
        if path == paths::CLOSED_ORDERS {
            Ok(parse::parse_closed_orders(&env, &self.pairs, &map)?)
        } else {
            Ok(parse::parse_open_orders(&env, &self.pairs, &map)?)
        }
    }
}

impl BrokerAdapter for KrakenAdapter {
    fn broker_name(&self) -> &'static str {
        "kraken"
    }

    fn get_balances(&self) -> Result<Balances, BrokerError> {
        let env = self.private_call(paths::BALANCE, &[])?;
        parse::parse_balances(&env)
    }

    fn get_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        let pair = self.pair_for(symbol)?;
        let env = self.public_call(paths::TICKER, &[("pair".to_string(), pair.altname.clone())])?;
        parse::parse_ticker(&env, pair)
    }

    fn place_order(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        let prepared = self.prepare(req)?;
        let params = prepared.to_params()?;
        let sent = prepared.sent();
        let unknown = |reason: String, sent| Ok(PlaceOutcome::UnknownOutcome { reason, sent });

        match self.private_call(paths::ADD_ORDER, &params) {
            Ok(env) => {
                let parsed = match parse::parse_add_order(&env) {
                    Ok(p) => p,
                    Err(e) => return unknown(format!("AddOrder success but unparseable: {e}"), sent),
                };
                match (sent.validate_only, parsed.txid) {
                    (true, None) => Ok(PlaceOutcome::ValidatedOnly {
                        description: parsed.description,
                        sent,
                        warnings: env.warnings,
                    }),
                    (true, Some(txid)) => unknown(
                        format!("validate=true but the response carried txid {txid}: an order may have been placed"),
                        sent,
                    ),
                    (false, Some(txid)) => Ok(PlaceOutcome::Accepted {
                        broker_order_id: txid,
                        description: parsed.description,
                        sent,
                        warnings: env.warnings,
                    }),
                    (false, None) => unknown("AddOrder success without a txid".to_string(), sent),
                }
            }
            Err(CallError::Local(e)) => Err(e),
            Err(CallError::Transport(t)) if t.request_definitely_not_sent() => Err(BrokerError::Transport(t)),
            Err(CallError::Transport(t)) => unknown(t.to_string(), sent),
            Err(CallError::Http(s)) => unknown(format!("HTTP status {s}"), sent),
            Err(CallError::Malformed(m)) => unknown(format!("malformed response: {m}"), sent),
            Err(CallError::Exchange(errors)) => {
                if errors.iter().any(ExchangeError::outcome_unknown) {
                    let codes = errors.iter().map(|e| e.code.as_str()).collect::<Vec<_>>().join(", ");
                    unknown(format!("exchange reported {codes}"), sent)
                } else {
                    Ok(PlaceOutcome::Rejected { errors, sent })
                }
            }
        }
    }

    fn get_order(&self, broker_order_id: &str) -> Result<OrderReport, BrokerError> {
        let env = self.private_call(paths::QUERY_ORDERS, &[("txid".to_string(), broker_order_id.to_string())])?;
        let reports = {
            let map = lock(&self.userrefs);
            parse::parse_query_orders(&env, &self.pairs, &map)?
        };
        reports
            .into_iter()
            .find(|r| r.broker_order_id == broker_order_id)
            .ok_or_else(|| BrokerError::NotFound(broker_order_id.to_string()))
    }

    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        let env = self.private_call(paths::OPEN_ORDERS, &[])?;
        let map = lock(&self.userrefs);
        parse::parse_open_orders(&env, &self.pairs, &map)
    }

    fn find_orders_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        let userref = lock(&self.userrefs).get_userref(tag).ok_or_else(|| BrokerError::UnknownTag(tag.to_string()))?;
        let mut out = self.orders_with_userref(paths::OPEN_ORDERS, userref)?;
        for r in self.orders_with_userref(paths::CLOSED_ORDERS, userref)? {
            if !out.iter().any(|o| o.broker_order_id == r.broker_order_id) {
                out.push(r);
            }
        }
        // Defensive: never trust a server-side filter we could not verify live.
        out.retain(|r| r.userref == Some(userref));
        Ok(out)
    }

    fn cancel_order(&self, broker_order_id: &str) -> Result<CancelOutcome, BrokerError> {
        let env = self.private_call(paths::CANCEL_ORDER, &[("txid".to_string(), broker_order_id.to_string())])?;
        parse::parse_cancel(&env)
    }
}
