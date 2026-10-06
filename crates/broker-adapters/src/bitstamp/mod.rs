//! Bitstamp spot adapter: BTC/USD and ETH/USD, Bitstamp REST API v2.
//!
//! * Environment: [`Environment::Live`] only. The documentation page lists a test server
//!   (`sandbox.bitstamp.net`), but no paper decision has been made, so this adapter never uses it and reports itself as
//!   live. Under `VenuePolicy::PaperOnly` a live (or unspecified) broker is refused everywhere.
//! * Order placement: DISABLED. The documentation states no validate-only mode, so [`BrokerAdapter::place_order`]
//!   builds and checks the request ([`order::build_order`]) and then refuses. Nothing is sent.
//! * Private calls (balances, open orders, order status, cancel) first ask the injected [`WithdrawalPermissionCheck`]
//!   and send nothing unless it passes. The only built-in implementation, [`FailClosed`], always refuses. The real
//!   withdrawal-permission check is built in another PR and plugs in here; this module does not implement its own.
//! * Credentials come from the environment only (see [`auth::BitstampCredentials::from_env`]).
//! * The public ticker needs no key ([`fetch_ticker`]).

pub mod auth;
pub mod market;
pub mod order;
pub mod parse;

use std::fmt;
use std::sync::Arc;

use crate::error::BrokerError;
use crate::transport::{HttpMethod, HttpRequest, HttpTransport};
use crate::types::{Balances, BrokerAdapter, CancelOutcome, OrderReport, OrderRequest, PlaceOutcome, Quote};

use auth::{BitstampCredentials, MillisClock, NonceMinter, SystemMillis};
use market::TickerQuote;

pub const PRODUCTION_BASE_URL: &str = "https://www.bitstamp.net";
pub const HOST: &str = "www.bitstamp.net";

pub mod paths {
    pub const ACCOUNT_BALANCES: &str = "/api/v2/account_balances/";
    pub const OPEN_ORDERS: &str = "/api/v2/open_orders/";
    pub const ORDER_STATUS: &str = "/api/v2/order_status/";
    pub const CANCEL_ORDER: &str = "/api/v2/cancel_order/";
}

/// What the venue can move. Only live is ever reported (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    Live,
}

impl Environment {
    pub fn as_str(self) -> &'static str {
        match self {
            Environment::Live => "live",
        }
    }
}

/// The withdrawal-permission gate. Implemented by the PR that builds the real check; every private call asks it first.
pub trait WithdrawalPermissionCheck: Send + Sync {
    /// `Ok(())` only when the key is verified to lack withdrawal permission. Errors must not contain the key.
    fn check(&self, creds: &BitstampCredentials) -> Result<(), BrokerError>;
}

/// The built-in gate: refuses everything. Private calls cannot run until a real gate is supplied.
#[derive(Debug, Clone, Copy, Default)]
pub struct FailClosed;

impl WithdrawalPermissionCheck for FailClosed {
    fn check(&self, _creds: &BitstampCredentials) -> Result<(), BrokerError> {
        Err(BrokerError::Credentials(
            "withdrawal-permission check is not available in this build; private calls are refused (fail-closed)".into(),
        ))
    }
}

/// The public ticker for one canonical symbol (`BTC/USD` or `ETH/USD`). No key, no gate.
pub fn fetch_ticker(transport: &dyn HttpTransport, symbol: &str) -> Result<TickerQuote, BrokerError> {
    let market = order::market_symbol(symbol)?;
    let req = HttpRequest {
        method: HttpMethod::Get,
        url: format!("{PRODUCTION_BASE_URL}{}", market::ticker_path(market)),
        headers: vec![("Accept".to_string(), "application/json".to_string())],
        body: None,
    };
    let resp = transport.execute_detailed(&req)?;
    let v = parse::interpret(resp.status, &resp.body)?;
    market::parse_ticker(symbol, &v)
}

fn numeric_id(id: &str) -> Result<&str, BrokerError> {
    if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
        return Err(BrokerError::InvalidRequest("order id must be a non-empty decimal string".into()));
    }
    Ok(id)
}

pub struct BitstampAdapter {
    creds: BitstampCredentials,
    transport: Arc<dyn HttpTransport>,
    gate: Box<dyn WithdrawalPermissionCheck>,
    clock: Arc<dyn MillisClock>,
    nonces: NonceMinter,
}

impl fmt::Debug for BitstampAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BitstampAdapter")
            .field("host", &HOST)
            .field("environment", &self.environment().as_str())
            .field("credentials", &self.creds)
            .finish_non_exhaustive()
    }
}

impl BitstampAdapter {
    pub fn new(
        creds: BitstampCredentials,
        transport: Arc<dyn HttpTransport>,
        gate: Box<dyn WithdrawalPermissionCheck>,
    ) -> Self {
        Self { creds, transport, gate, clock: Arc::new(SystemMillis), nonces: NonceMinter::new() }
    }

    pub fn with_clock(mut self, clock: Arc<dyn MillisClock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn environment(&self) -> Environment {
        Environment::Live
    }

    fn private_call(&self, path: &str, params: &[(String, String)]) -> Result<serde_json::Value, BrokerError> {
        self.gate.check(&self.creds)?;
        let millis = self.clock.now_millis();
        let nonce = self.nonces.mint(millis);
        let req = auth::build_private_request(&self.creds, HOST, path, &nonce, millis, params);
        let resp = self.transport.execute_detailed(&req)?;
        parse::interpret(resp.status, &resp.body)
    }
}

impl BrokerAdapter for BitstampAdapter {
    fn broker_name(&self) -> &'static str {
        "bitstamp"
    }

    fn get_balances(&self) -> Result<Balances, BrokerError> {
        let v = self.private_call(paths::ACCOUNT_BALANCES, &[])?;
        parse::parse_balances(&v)
    }

    fn get_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        let t = fetch_ticker(&*self.transport, symbol)?;
        Ok(Quote { symbol: t.symbol, bid: t.bid, ask: t.ask, last: t.last })
    }

    fn place_order(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        // Validates the request shape (and refuses validate-only, post-only, reduce-only, time-in-force) without
        // sending anything. The send itself is deliberately absent until a paper decision is made.
        order::build_order(req)?;
        Err(BrokerError::Unsupported(
            "Bitstamp order placement is disabled: no validate-only mode is documented and no paper decision has been made".into(),
        ))
    }

    fn get_order(&self, broker_order_id: &str) -> Result<OrderReport, BrokerError> {
        let id = numeric_id(broker_order_id)?;
        let v = self.private_call(paths::ORDER_STATUS, &[("id".to_string(), id.to_string())])?;
        parse::parse_order_status(&v)
    }

    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        let v = self.private_call(paths::OPEN_ORDERS, &[])?;
        parse::parse_open_orders(&v)
    }

    fn find_orders_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        Err(BrokerError::Unsupported(format!(
            "lookup by tag {tag:?}: no order is ever placed by this adapter, so there is nothing to find"
        )))
    }

    fn cancel_order(&self, broker_order_id: &str) -> Result<CancelOutcome, BrokerError> {
        let id = numeric_id(broker_order_id)?;
        let v = self.private_call(paths::CANCEL_ORDER, &[("id".to_string(), id.to_string())])?;
        parse::parse_cancel(&v)
    }
}
