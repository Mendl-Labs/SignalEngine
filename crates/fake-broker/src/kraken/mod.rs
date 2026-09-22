//! The Kraken wire front end: a stateful [`HttpTransport`] that parses the adapter's requests,
//! enforces Kraken's authentication rules (API key, signature, strictly increasing nonce per
//! key), drives the exchange core, and renders Kraken-shaped JSON.
//!
//! Order of checks on a private call: key, signature, nonce, permission, arguments, execution.
//! A request rejected at the key or signature step never moves the key's nonce. Once the nonce
//! check passes, the nonce is consumed even if the request then fails on permissions or
//! arguments, so replaying it is an `EAPI:Invalid nonce`. (Order assumed from Kraken's
//! documented behaviour; not verified against the live exchange.)

mod endpoints;
pub mod wire;

use crate::broker::{Shared, World};
use crate::fault::{transport_error, FaultKind, Timing};
use crate::log::{Delivered, Origin, RequestRecord};
use broker_adapters::nonce::Clock;
use broker_adapters::transport::{HttpMethod, HttpRequest, HttpResponse, HttpTransport, TransportError};
use std::collections::BTreeMap;
use std::sync::Arc;
use wire::{error_body, paths};

/// What an API key is allowed to do. Kraken keys carry permissions; the rebalancer's key must be
/// trade-only, with no withdrawal permission (which this fake does not even offer an endpoint for).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyPermissions {
    /// `Balance`, `TradeBalance`.
    pub query_funds: bool,
    /// `OpenOrders`, `ClosedOrders`, `QueryOrders`.
    pub query_orders: bool,
    /// `AddOrder`.
    pub create_orders: bool,
    /// `CancelOrder`.
    pub cancel_orders: bool,
}

impl KeyPermissions {
    pub fn all() -> Self {
        Self { query_funds: true, query_orders: true, create_orders: true, cancel_orders: true }
    }
    /// A key that can look but not touch.
    pub fn read_only() -> Self {
        Self { query_funds: true, query_orders: true, create_orders: false, cancel_orders: false }
    }
}

impl Default for KeyPermissions {
    fn default() -> Self {
        Self::all()
    }
}

pub(crate) struct KeyState {
    pub account: String,
    pub secret: Vec<u8>,
    pub highest_nonce: Option<u64>,
    pub perms: KeyPermissions,
    pub revoked: bool,
}

pub(crate) struct KrakenState {
    pub host: String,
    pub keys: BTreeMap<String, KeyState>,
    pub closed_page_size: usize,
    /// Requests held back by `Timing::Delayed` faults, waiting for `deliver_delayed`.
    pub delayed: Vec<HttpRequest>,
}

impl KrakenState {
    pub fn new(host: String, closed_page_size: usize) -> Self {
        Self { host, keys: BTreeMap::new(), closed_page_size, delayed: Vec::new() }
    }
}

/// The `HttpTransport` handed to `KrakenAdapter::new`. Obtain it from
/// `FakeBroker::kraken_transport()`.
pub struct KrakenTransport {
    shared: Arc<Shared>,
}

impl KrakenTransport {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }

    /// Process one request end to end: host check, fault injection, dispatch, logging.
    pub(crate) fn call(&self, req: &HttpRequest, origin: Origin) -> Result<HttpResponse, TransportError> {
        let now = self.shared.clock.now_nanos();
        let mut w = self.shared.lock();
        let seq = w.seq();
        let mut rec = RequestRecord {
            seq,
            time_nanos: now,
            origin,
            method: req.method,
            path: String::new(),
            api_key: req.header("API-Key").map(str::to_string),
            params: Vec::new(),
            fault: None,
            reached_exchange: false,
            produced: None,
            delivered: Delivered::Response { status: 0, body: String::new() },
        };

        let Some((host, path, query)) = wire::split_url(&req.url) else {
            let e = TransportError::ConnectFailed(format!("invalid url {:?}", req.url));
            rec.delivered = Delivered::Transport(e.clone());
            w.record(rec);
            return Err(e);
        };
        rec.path = path.to_string();
        rec.params = match (&req.body, query.is_empty()) {
            (Some(b), _) => wire::parse_form(b).unwrap_or_default(),
            (None, false) => wire::parse_form(query).unwrap_or_default(),
            (None, true) => Vec::new(),
        };
        if !host.eq_ignore_ascii_case(&w.kraken.host) {
            let e = TransportError::ConnectFailed(format!("could not resolve host {host} (this fake serves {})", w.kraken.host));
            rec.delivered = Delivered::Transport(e.clone());
            w.record(rec);
            return Err(e);
        }

        // Faults never apply to the test's own "other process" calls.
        let fault = if origin == Origin::Adapter { w.faults.take(path) } else { None };
        rec.fault = fault.clone();

        if let Some((kind, timing @ (Timing::BeforeApply | Timing::Delayed))) = &fault {
            // The request does not reach the exchange now: nothing is applied, no nonce is
            // consumed. A delayed request is kept and lands later.
            if *timing == Timing::Delayed {
                w.kraken.delayed.push(req.clone());
            }
            let out = fault_outcome(kind);
            rec.delivered = delivered_of(&out);
            w.record(rec);
            return out;
        }

        let (status, body) = dispatch(&mut w, now, req, path);
        rec.reached_exchange = true;
        rec.produced = Some((status, body.clone()));

        let out = match &fault {
            // The exchange already applied the request; only the response is lost or replaced.
            Some((kind, Timing::AfterApply)) => fault_outcome(kind),
            Some((_, Timing::BeforeApply | Timing::Delayed)) => unreachable!("handled above"),
            None => Ok(HttpResponse { status, body }),
        };
        rec.delivered = delivered_of(&out);
        w.record(rec);
        out
    }
}

impl KrakenTransport {
    /// Let every request held back by a `Timing::Delayed` fault reach the exchange now, in the
    /// order it was sent. Returns what the exchange answered to each.
    pub(crate) fn deliver_delayed(&self) -> Vec<HttpResponse> {
        let held = std::mem::take(&mut self.shared.lock().kraken.delayed);
        held.iter()
            .map(|r| match self.call(r, Origin::Delayed) {
                Ok(resp) => resp,
                Err(e) => HttpResponse { status: 0, body: e.to_string() },
            })
            .collect()
    }
}

impl HttpTransport for KrakenTransport {
    fn execute(&self, req: &HttpRequest) -> Result<HttpResponse, TransportError> {
        self.call(req, Origin::Adapter)
    }
}

fn delivered_of(out: &Result<HttpResponse, TransportError>) -> Delivered {
    match out {
        Ok(r) => Delivered::Response { status: r.status, body: r.body.clone() },
        Err(e) => Delivered::Transport(e.clone()),
    }
}

/// What the caller sees for a fault.
fn fault_outcome(kind: &FaultKind) -> Result<HttpResponse, TransportError> {
    if let Some(e) = transport_error(kind) {
        return Err(e);
    }
    Ok(match kind {
        FaultKind::Http { status, body } => HttpResponse { status: *status, body: body.clone() },
        FaultKind::MalformedBody(body) => HttpResponse { status: 200, body: body.clone() },
        FaultKind::RateLimit => HttpResponse { status: 200, body: error_body(&["EAPI:Rate limit exceeded".to_string()]) },
        FaultKind::ExchangeError(code) => HttpResponse { status: 200, body: error_body(std::slice::from_ref(code)) },
        FaultKind::Timeout | FaultKind::ConnectFailed | FaultKind::IoError => unreachable!("handled by transport_error"),
    })
}

fn dispatch(w: &mut World, now: u64, req: &HttpRequest, path: &str) -> (u16, String) {
    match path {
        paths::TICKER | paths::ASSET_PAIRS => {
            let query = wire::split_url(&req.url).map(|(_, _, q)| q).unwrap_or("");
            let Some(params) = wire::parse_form(query) else {
                return (200, error_body(&["EGeneral:Invalid arguments".to_string()]));
            };
            let result = if path == paths::TICKER {
                endpoints::ticker(w, &params)
            } else {
                endpoints::asset_pairs(w, &params)
            };
            respond(result)
        }
        paths::BALANCE
        | paths::TRADE_BALANCE
        | paths::ADD_ORDER
        | paths::CANCEL_ORDER
        | paths::QUERY_ORDERS
        | paths::OPEN_ORDERS
        | paths::CLOSED_ORDERS => private(w, now, req, path),
        _ => (404, "Unknown method".to_string()),
    }
}

fn respond(result: Result<serde_json::Value, Vec<String>>) -> (u16, String) {
    match result {
        Ok(v) => (200, wire::ok_body(v)),
        Err(codes) => (200, error_body(&codes)),
    }
}

fn err1(code: &str) -> (u16, String) {
    (200, error_body(&[code.to_string()]))
}

fn private(w: &mut World, now: u64, req: &HttpRequest, path: &str) -> (u16, String) {
    if req.method != HttpMethod::Post {
        return (405, "Method Not Allowed".to_string());
    }
    // 1. key
    let Some(api_key) = req.header("API-Key") else { return err1("EAPI:Invalid key") };
    let Some(key) = w.kraken.keys.get_mut(api_key).filter(|k| !k.revoked) else { return err1("EAPI:Invalid key") };
    // 2. signature (over the exact body bytes and path)
    let body = req.body.as_deref().unwrap_or("");
    let Some(nonce_text) = wire::raw_nonce(body) else { return err1("EAPI:Invalid nonce") };
    let Some(sig) = req.header("API-Sign") else { return err1("EAPI:Invalid signature") };
    if !wire::verify(&key.secret, path, nonce_text, body, sig) {
        return err1("EAPI:Invalid signature");
    }
    // 3. nonce: strictly greater than the highest ever accepted for this key
    let Ok(nonce) = nonce_text.parse::<u64>() else { return err1("EAPI:Invalid nonce") };
    if key.highest_nonce.is_some_and(|h| nonce <= h) {
        return err1("EAPI:Invalid nonce");
    }
    key.highest_nonce = Some(nonce);
    let (account, perms) = (key.account.clone(), key.perms);
    // 4. arguments
    let Some(params) = wire::parse_form(body) else { return err1("EGeneral:Invalid arguments") };
    let result = match path {
        paths::BALANCE => endpoints::balance(w, &account, perms),
        paths::TRADE_BALANCE => endpoints::trade_balance(w, &account, perms, &params),
        paths::ADD_ORDER => endpoints::add_order(w, now, &account, perms, &params),
        paths::CANCEL_ORDER => endpoints::cancel_order(w, now, &account, perms, &params),
        paths::QUERY_ORDERS => endpoints::query_orders(w, &account, perms, &params),
        paths::OPEN_ORDERS => endpoints::open_orders(w, &account, perms, &params),
        paths::CLOSED_ORDERS => endpoints::closed_orders(w, &account, perms, &params),
        _ => unreachable!("routed in dispatch"),
    };
    respond(result)
}
