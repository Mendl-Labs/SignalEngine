//! OANDA v20 (fxPractice / fxTrade) FX adapter, HTTP behind [`HttpTransport`].
//!
//! # Scope and status
//!
//! Verification Rungs 1 and 2 only: every test runs against hand-authored fixtures or the local
//! in-process fake broker. **This adapter has never spoken to a real OANDA server**, practice or
//! live. It is NOT wired into any live-ready list, and the legacy `executionhandler` OANDA path is
//! untouched.
//!
//! # Provenance of the facts encoded here
//!
//! VERIFIED-FROM-REPO-CODE (SignalEngine `exchanges/generic/{config.rs,connector.rs}`, the legacy
//! OANDA preset): bearer-token auth; the practice REST host `https://api-fxpractice.oanda.com`;
//! `POST /v3/accounts/{account_id}/orders`; `GET /v3/accounts/{account_id}/summary`; instruments
//! named `EUR_USD`; the order body `{"order": {"type", "instrument", "units", "timeInForce",
//! "positionFill", "price"}}` with `units` a signed integer string (negative = sell), `MARKET` +
//! `FOK` + `positionFill: DEFAULT`; the create response's `orderCreateTransaction.id` and
//! `orderFillTransaction.{id, units, price}`; the error body `{"errorCode", "errorMessage"}`. The
//! legacy connector's conversion of a priced market signal into LIMIT/GTC is a verified defect (not
//! reproduced here), as is its `cancel_order_path`/`order_status_path` pointing at the collection.
//!
//! FROM-MEMORY-OF-DOCS (NOT verified anywhere in this repository; each needs the practice-account
//! smoke test, Rung 3):
//! * the live REST host `https://api-fxtrade.oanda.com`, and the account-id prefixes `101-`
//!   (practice) / `001-` (live);
//! * `GET /v3/accounts/{id}/openPositions`, `/positions/{instrument}`, `/pendingOrders`,
//!   `/orders/{orderSpecifier}`, `/transactions/{transactionID}`, `/pricing?instruments=...&includeHomeConversions=true`,
//!   `/instruments`, and their response shapes (see `parse`);
//! * that an `orderSpecifier` of `@<clientOrderID>` finds an order by its client id, INCLUDING
//!   already FILLED or CANCELLED orders (the whole idempotency scheme rests on this);
//! * that the order resource carries no fill quantity/price (they live on the `ORDER_FILL`
//!   transaction named by `fillingTransactionID`), that a cancelled order names its
//!   `cancellingTransactionID`, and the `OrderCancelReason` / `TransactionRejectReason` strings;
//! * that a create response is HTTP 201 with `orderCreateTransaction` plus either
//!   `orderFillTransaction` or `orderCancelTransaction` (FOK), and that a refused create is HTTP 400
//!   with `orderRejectTransaction`;
//! * `PUT /v3/accounts/{id}/orders/{orderID}/cancel` (200 with `orderCancelTransaction`, 404 when
//!   there is nothing pending to cancel);
//! * `PUT /v3/accounts/{id}/positions/{instrument}/close` with body `{"longUnits": "ALL"}` /
//!   `{"shortUnits": "ALL"}` (+ `longClientExtensions` / `shortClientExtensions`) and the response keys
//!   `long/shortOrder{Create,Fill,Cancel,Reject}Transaction`;
//! * the headers `Accept-Datetime-Format: UNIX` (timestamps as epoch-second strings);
//! * `clientExtensions.id` uniqueness rules: NOT relied on. The adapter never depends on OANDA
//!   rejecting a duplicate id; it looks the tag up BEFORE sending;
//! * HTTP 401/403 for auth failures, 429 for rate limiting, 404 for unknown order/account.
//!
//! Test fixtures are hand-written to those documented shapes and labelled as such. They are NOT
//! recordings from a live or practice account.
//!
//! # Error model (same contract as Kraken and Alpaca)
//!
//! * `place_order` returns `Err(_)` only when the order was definitely NOT sent (local refusal,
//!   pre-order check or tag-lookup failure, connect failure, HTTP 429).
//! * `PlaceOutcome::Rejected` is a definite refusal (HTTP 400 / 401 / 403, an order cancelled at
//!   creation such as `INSUFFICIENT_MARGIN`, or a tag that already belongs to a dead or different order).
//! * `PlaceOutcome::UnknownOutcome` (timeouts, I/O errors, 5xx, an unparseable or inconsistent
//!   success) means the order MAY exist: look it up with `find_orders_by_tag` before any retry.
//! * **Restart safety**: `place_order` looks the tag up first. If an order with the tag exists it is
//!   ADOPTED (returned as `Accepted`, nothing sent), so re-placing after a crash or an unknown outcome
//!   cannot double-submit.

pub mod auth;
pub mod config;
pub mod instrument;
pub mod order;
pub mod parse;

pub use auth::{OandaCredentials, OandaToken};
pub use config::{Environment, LiveTradingAck, OandaConfig, LIVE_BASE_URL, PRACTICE_BASE_URL};
pub use instrument::{canonical_symbol, normalize_instrument, InstrumentInfo, InstrumentTable};
pub use order::{PrepareOptions, PreparedOrder};
pub use parse::{
    AccountSummary, ApiError, CancelInfo, FillInfo, HomeConversion, HttpFailure, OandaPosition, OrderResource, PriceQuote, Pricing,
};

use crate::decimal::Dec;
use crate::error::{BrokerError, ErrorClass, ExchangeError};
use crate::transport::{HttpMethod, HttpRequest, HttpResponseDetailed, HttpTransport, TransportError};
use crate::types::{
    BalanceEntry, BalanceKind, Balances, BrokerAdapter, CancelOutcome, OrderReport, OrderRequest, PlaceOutcome, Quote,
};
use serde_json::json;
use std::fmt;
use std::sync::{Arc, Mutex};

enum CallError {
    Transport(TransportError),
    Failure(HttpFailure),
}

/// Result of [`OandaAdapter::close_position`].
#[derive(Debug, Clone, PartialEq)]
pub enum CloseOutcome {
    /// No open position in the instrument (nothing was sent).
    NothingToClose,
    /// The close order filled.
    Closed { broker_order_id: String, fill: FillInfo },
    /// An order with this tag already existed (an earlier attempt): nothing was sent. Look at the report.
    AlreadyDone { report: OrderReport },
    /// A definite refusal.
    Rejected { errors: Vec<ExchangeError> },
    /// The close may or may not have happened: read positions and look the tag up before anything else.
    UnknownOutcome { reason: String },
}

pub struct OandaAdapter {
    config: OandaConfig,
    creds: OandaCredentials,
    transport: Arc<dyn HttpTransport>,
    instruments: Mutex<InstrumentTable>,
}

impl fmt::Debug for OandaAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OandaAdapter")
            .field("environment", &self.config.environment())
            .field("base_url", &self.config.base_url())
            .field("credentials", &self.creds)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Percent-encode per RFC 3986 unreserved characters.
fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// An id that is safe to place in a URL path (OANDA order and transaction ids are numeric strings).
fn path_id(id: &str) -> Result<&str, BrokerError> {
    if !id.is_empty() && id.len() <= 64 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
        Ok(id)
    } else {
        Err(BrokerError::InvalidRequest(format!("invalid order id {id:?}")))
    }
}

impl OandaAdapter {
    /// Refuses (with `Config` / `Credentials`) a base URL that does not match the environment, and
    /// credentials marked for a different environment than the config. The instrument table starts
    /// EMPTY: call [`refresh_instruments`](Self::refresh_instruments) (or [`set_instruments`](Self::set_instruments))
    /// before placing an order.
    pub fn new(config: OandaConfig, creds: OandaCredentials, transport: Arc<dyn HttpTransport>) -> Result<Self, BrokerError> {
        let config = config.validated()?;
        if creds.environment() != config.environment() {
            return Err(BrokerError::Credentials(format!(
                "credentials are marked {} but the adapter is configured for {}; refusing to send them",
                creds.environment().as_str(),
                config.environment().as_str()
            )));
        }
        Ok(Self { config, creds, transport, instruments: Mutex::new(InstrumentTable::new()) })
    }

    pub fn environment(&self) -> Environment {
        self.config.environment()
    }

    pub fn base_url(&self) -> &str {
        self.config.base_url()
    }

    pub fn account_id(&self) -> &str {
        self.creds.account_id()
    }

    pub fn token_fingerprint(&self) -> String {
        self.creds.token_fingerprint()
    }

    pub fn config(&self) -> &OandaConfig {
        &self.config
    }

    // ------------------------------------------------------------ instruments

    pub fn set_instruments(&self, table: InstrumentTable) {
        *lock(&self.instruments) = table;
    }

    pub fn upsert_instrument(&self, info: InstrumentInfo) {
        lock(&self.instruments).upsert(info);
    }

    pub fn instrument(&self, name: &str) -> Option<InstrumentInfo> {
        lock(&self.instruments).lookup(name).cloned()
    }

    /// A snapshot of the whole table (for the rebalancer's venue rules).
    pub fn instruments(&self) -> InstrumentTable {
        lock(&self.instruments).clone()
    }

    /// `GET /v3/accounts/{id}/instruments`; replaces the table and returns the row count.
    pub fn refresh_instruments(&self) -> Result<usize, BrokerError> {
        let body = self.read(HttpMethod::Get, &self.acct_path("/instruments"))?.body;
        let table = InstrumentTable::from_instruments_json(&body)?;
        let n = table.len();
        *lock(&self.instruments) = table;
        Ok(n)
    }

    // ------------------------------------------------------------ transport plumbing

    fn acct_path(&self, tail: &str) -> String {
        format!("/v3/accounts/{}{}", self.creds.account_id(), tail)
    }

    fn build_request(&self, method: HttpMethod, path_and_query: &str, body: Option<String>) -> HttpRequest {
        let mut headers = self.creds.headers();
        headers.push(("Accept".to_string(), "application/json".to_string()));
        headers.push(("Accept-Datetime-Format".to_string(), "UNIX".to_string()));
        if body.is_some() {
            headers.push(("Content-Type".to_string(), "application/json".to_string()));
        }
        HttpRequest { method, url: format!("{}{}", self.config.base_url(), path_and_query), headers, body }
    }

    fn call(&self, method: HttpMethod, path_and_query: &str, body: Option<String>) -> Result<HttpResponseDetailed, CallError> {
        let req = self.build_request(method, path_and_query, body);
        let resp = self.transport.execute_detailed(&req).map_err(CallError::Transport)?;
        if (200..300).contains(&resp.status) {
            Ok(resp)
        } else {
            let token = self.creds.token().expose();
            Err(CallError::Failure(parse::classify_http_failure(resp.status, &resp.body, resp.header("Retry-After"), &[token])))
        }
    }

    fn read_error(&self, e: CallError) -> BrokerError {
        match e {
            CallError::Transport(t) => BrokerError::Transport(t),
            CallError::Failure(f) => f.into_broker_error(),
        }
    }

    fn read(&self, method: HttpMethod, path_and_query: &str) -> Result<HttpResponseDetailed, BrokerError> {
        self.call(method, path_and_query, None).map_err(|e| self.read_error(e))
    }

    /// Error for a pre-order check: nothing has been sent, so transport and server trouble both
    /// become `Preflight` (never a bare `Transport(Timeout)` that a caller could mistake for an
    /// unknown order outcome).
    fn preflight_error(e: CallError) -> BrokerError {
        match e {
            CallError::Transport(t) => BrokerError::Preflight(format!("transport: {t}")),
            CallError::Failure(HttpFailure::Server { status, .. }) | CallError::Failure(HttpFailure::Other { status, .. }) => {
                BrokerError::Preflight(format!("HTTP status {status}"))
            }
            CallError::Failure(f) => f.into_broker_error(),
        }
    }

    fn own_prefix(&self) -> Option<&str> {
        self.config.own_tag_prefix()
    }

    // ------------------------------------------------------------ account

    pub fn get_account_summary(&self) -> Result<AccountSummary, BrokerError> {
        parse::parse_account_summary(&self.read(HttpMethod::Get, &self.acct_path("/summary"))?.body)
    }

    /// The summary must be for the configured account and the account must not be a hedging
    /// account (this adapter assumes netting).
    pub fn check_account(&self, s: &AccountSummary) -> Result<(), BrokerError> {
        if s.id != self.creds.account_id() {
            return Err(BrokerError::Credentials(format!(
                "the summary is for account {} but the adapter is configured for {}",
                s.id,
                self.creds.account_id()
            )));
        }
        if s.hedging_enabled {
            return Err(BrokerError::AccountBlocked(
                "hedgingEnabled=true: this adapter supports netting accounts only (a position must have one net direction)".into(),
            ));
        }
        Ok(())
    }

    pub fn verify_account(&self) -> Result<AccountSummary, BrokerError> {
        let s = self.get_account_summary()?;
        self.check_account(&s)?;
        Ok(s)
    }

    pub fn get_open_positions(&self) -> Result<Vec<OandaPosition>, BrokerError> {
        parse::parse_open_positions(&self.read(HttpMethod::Get, &self.acct_path("/openPositions"))?.body)
    }

    /// `GET /positions/{instrument}`; `Ok(None)` when OANDA answers 404 (no such position).
    pub fn get_position(&self, symbol: &str) -> Result<Option<OandaPosition>, BrokerError> {
        let inst = normalize_instrument(symbol)?;
        match self.call(HttpMethod::Get, &self.acct_path(&format!("/positions/{}", enc(&inst))), None) {
            Ok(resp) => {
                let p = parse::parse_single_position(&resp.body)?;
                if p.instrument != inst {
                    return Err(BrokerError::Malformed(format!("asked for position {inst} but received {}", p.instrument)));
                }
                Ok(Some(p))
            }
            Err(CallError::Failure(HttpFailure::NotFound { .. })) => Ok(None),
            Err(e) => Err(self.read_error(e)),
        }
    }

    /// `GET /pricing` for the given instruments, asking for home conversions.
    pub fn get_pricing(&self, symbols: &[&str]) -> Result<Pricing, BrokerError> {
        if symbols.is_empty() {
            return Err(BrokerError::InvalidRequest("pricing needs at least one instrument".into()));
        }
        let names: Result<Vec<String>, BrokerError> = symbols.iter().map(|s| normalize_instrument(s)).collect();
        let pq = format!("{}?instruments={}&includeHomeConversions=true", self.acct_path("/pricing"), enc(&names?.join(",")));
        parse::parse_pricing(&self.read(HttpMethod::Get, &pq)?.body)
    }

    // ------------------------------------------------------------ orders

    /// Pure: validate and round a request into the exact order that would be sent. Sends nothing.
    pub fn prepare(&self, req: &OrderRequest) -> Result<PreparedOrder, BrokerError> {
        let inst = normalize_instrument(&req.symbol)?;
        let info = self.instrument(&inst).ok_or_else(|| BrokerError::UnknownSymbol(inst.clone()))?;
        order::prepare_order(
            req,
            &info,
            &PrepareOptions { own_tag_prefix: self.own_prefix().map(str::to_string), client_tag: self.config.client_tag().to_string() },
        )
    }

    fn preflight(&self) -> Result<(), BrokerError> {
        let body = self.call(HttpMethod::Get, &self.acct_path("/summary"), None).map_err(Self::preflight_error)?.body;
        let s = parse::parse_account_summary(&body).map_err(|e| BrokerError::Preflight(format!("account summary unusable: {e}")))?;
        self.check_account(&s)
    }

    fn fetch_fill(&self, transaction_id: &str) -> Result<FillInfo, BrokerError> {
        let id = path_id(transaction_id)?;
        parse::parse_fill_transaction(&self.read(HttpMethod::Get, &self.acct_path(&format!("/transactions/{id}")))?.body)
    }

    /// Reason of the cancelling transaction, best effort: a failure to read it leaves the reason
    /// unknown (the order is still reported cancelled with nothing executed).
    fn fetch_cancel_reason(&self, transaction_id: &str) -> Option<String> {
        let id = path_id(transaction_id).ok()?;
        let resp = self.call(HttpMethod::Get, &self.acct_path(&format!("/transactions/{id}")), None).ok()?;
        parse::parse_cancel_transaction(&resp.body).ok().map(|c| c.reason)
    }

    /// Fetch one order by specifier (an order id, or `@<client id>` percent-encoded) and complete
    /// it with its fill / cancel transaction.
    fn get_order_by_specifier(&self, spec_path: &str) -> Result<OrderReport, BrokerError> {
        let body = self.read(HttpMethod::Get, &self.acct_path(&format!("/orders/{spec_path}")))?.body;
        let resource = parse::parse_order_resource(&body)?;
        self.complete(&resource)
    }

    fn complete(&self, resource: &OrderResource) -> Result<OrderReport, BrokerError> {
        let fill = if resource.state == "FILLED" {
            let tid = resource
                .filling_transaction_id
                .as_deref()
                .ok_or_else(|| BrokerError::Malformed(format!("order {} is FILLED but names no fillingTransactionID", resource.id)))?;
            Some(self.fetch_fill(tid)?)
        } else {
            None
        };
        let reason = if resource.state == "CANCELLED" {
            resource.cancelling_transaction_id.as_deref().and_then(|t| self.fetch_cancel_reason(t))
        } else {
            None
        };
        resource.into_report(fill.as_ref(), reason.as_deref(), self.own_prefix())
    }

    /// `GET /orders/@<tag>`. `Ok(None)` when OANDA answers 404. NOTE: a 404 is only read as "no such
    /// order" once the account itself has been confirmed to exist (`place_order`'s pre-check does that).
    pub fn get_order_by_tag(&self, tag: &str) -> Result<Option<OrderReport>, BrokerError> {
        order::validate_client_id(tag, None)?;
        match self.get_order_by_specifier(&format!("%40{}", enc(tag))) {
            Ok(report) => {
                // Defensive: never trust an unverified server-side filter.
                if report.tag.as_deref() != Some(tag) && self.own_prefix().is_none_or(|p| tag.starts_with(p)) {
                    return Err(BrokerError::Malformed(format!("lookup by client id returned an order that does not carry {tag:?}")));
                }
                Ok(Some(report))
            }
            Err(BrokerError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn get_pending_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        let body = self.read(HttpMethod::Get, &self.acct_path("/pendingOrders"))?.body;
        parse::parse_order_resources(&body)?.iter().map(|r| r.into_report(None, None, self.own_prefix())).collect()
    }

    /// Cancel, then re-query so anything that happened first (a fill) is captured.
    ///
    /// If the cancel is refused because nothing is pending to cancel (404, or a 400 reject), the
    /// order is re-queried and the final report is returned with
    /// `CancelOutcome { canceled_count: 0, pending: false }`. If the re-query finds no such order
    /// either, `BrokerError::CancelTargetNotFound` is returned.
    pub fn cancel_and_settle(&self, broker_order_id: &str) -> Result<(CancelOutcome, OrderReport), BrokerError> {
        let id = path_id(broker_order_id)?;
        let outcome = match self.call(HttpMethod::Put, &self.acct_path(&format!("/orders/{id}/cancel")), None) {
            Ok(resp) => {
                parse::parse_cancel_response(&resp.body).map_err(|e| BrokerError::Malformed(format!("cancel accepted but its response is unusable: {e}")))?;
                CancelOutcome { canceled_count: 1, pending: false }
            }
            Err(CallError::Failure(HttpFailure::NotFound { .. } | HttpFailure::Rejected { .. })) => {
                return match self.get_order(broker_order_id) {
                    Ok(report) => Ok((CancelOutcome { canceled_count: 0, pending: false }, report)),
                    Err(BrokerError::NotFound(_)) => Err(BrokerError::CancelTargetNotFound(broker_order_id.to_string())),
                    Err(other) => Err(other),
                };
            }
            Err(e) => return Err(self.read_error(e)),
        };
        let report = self.get_order(broker_order_id)?;
        Ok((outcome, report))
    }

    // ------------------------------------------------------------ close / flatten one instrument

    /// Close the whole position in one instrument with `PUT /positions/{instrument}/close`.
    ///
    /// `tag` is the idempotency key: it is sent as `longClientExtensions.id` / `shortClientExtensions.id`
    /// and looked up FIRST, so calling again after a crash or an unknown outcome adopts the earlier
    /// close instead of sending a second one. A hedged position is refused (`Unsupported`). A
    /// `Transport(Timeout)` or 5xx is `UnknownOutcome`: read positions before doing anything else.
    pub fn close_position(&self, symbol: &str, tag: &str) -> Result<CloseOutcome, BrokerError> {
        let inst = normalize_instrument(symbol)?;
        order::validate_client_id(tag, self.own_prefix())?;
        self.preflight()?;
        match self.get_order_by_tag(tag) {
            Ok(Some(report)) => return Ok(CloseOutcome::AlreadyDone { report }),
            Ok(None) => {}
            Err(e) => return Err(BrokerError::Preflight(format!("tag lookup failed, nothing was sent: {e}"))),
        }
        let pos = match self.get_position(&inst)? {
            Some(p) => p,
            None => return Ok(CloseOutcome::NothingToClose),
        };
        if pos.is_hedged() {
            return Err(BrokerError::Unsupported(format!("{inst}: hedged position (long and short units both open)")));
        }
        let ext = json!({ "id": tag, "tag": self.config.client_tag() });
        let (side_prefix, body) = if pos.long_units.is_positive() {
            ("longOrder", json!({ "longUnits": "ALL", "longClientExtensions": ext }))
        } else if pos.short_units.is_negative() {
            ("shortOrder", json!({ "shortUnits": "ALL", "shortClientExtensions": ext }))
        } else {
            return Ok(CloseOutcome::NothingToClose);
        };
        let path = self.acct_path(&format!("/positions/{}/close", enc(&inst)));
        let unknown = |reason: String| Ok(CloseOutcome::UnknownOutcome { reason });
        match self.call(HttpMethod::Put, &path, Some(body.to_string())) {
            Ok(resp) => {
                let t = match parse::parse_order_transactions(&resp.body, side_prefix) {
                    Ok(t) => t,
                    Err(_) => return unknown(format!("HTTP {} but the close response is unusable", resp.status)),
                };
                if let Some(r) = &t.reject {
                    return Ok(CloseOutcome::Rejected { errors: vec![reject_error(resp.status, &r.reason)] });
                }
                let create_ok = t.create.as_ref().is_some_and(|c| c.client_id.as_deref() == Some(tag));
                match (t.fill, t.cancel, create_ok) {
                    (Some(fill), _, true) => {
                        if fill.instrument != inst {
                            return unknown(format!("close fill is for {} not {inst}", fill.instrument));
                        }
                        Ok(CloseOutcome::Closed { broker_order_id: fill.order_id.clone(), fill })
                    }
                    (None, Some(c), true) => Ok(CloseOutcome::Rejected {
                        errors: vec![ExchangeError {
                            code: format!("oanda:{} {}: close order cancelled at creation", resp.status, c.reason),
                            class: parse::classify_cancel_reason(&c.reason),
                        }],
                    }),
                    _ => unknown(format!("HTTP {} but the close response has no matching create+fill for tag", resp.status)),
                }
            }
            Err(CallError::Transport(t)) if t.request_definitely_not_sent() => Err(BrokerError::Transport(t)),
            Err(CallError::Transport(t)) => unknown(t.to_string()),
            Err(CallError::Failure(f)) => match f {
                HttpFailure::RateLimited { .. } => Err(f.into_broker_error()),
                HttpFailure::Server { status, .. } | HttpFailure::Other { status, .. } => unknown(format!("HTTP status {status}")),
                HttpFailure::NotFound { api } => unknown(format!("HTTP 404 on close ({})", api.message)),
                definite => match definite.exchange_error() {
                    Some(e) => Ok(CloseOutcome::Rejected { errors: vec![e] }),
                    None => unknown(format!("HTTP status {}", definite.status())),
                },
            },
        }
    }
}

fn reject_error(status: u16, reason: &str) -> ExchangeError {
    let api = ApiError { code: None, message: "order rejected".into(), reject_reason: Some(reason.to_string()) };
    ExchangeError { code: api.code_string(status), class: parse::classify_rejection(&api) }
}

impl OandaAdapter {
    fn interpret_create_response(&self, status: u16, body: &str, prepared: &PreparedOrder) -> PlaceOutcome {
        let sent = prepared.sent();
        let unknown = |reason: String| PlaceOutcome::UnknownOutcome { reason, sent: sent.clone() };
        let t = match parse::parse_order_transactions(body, "order") {
            Ok(t) => t,
            Err(e) => return unknown(format!("HTTP {status} but the order response is unusable: {e}")),
        };
        if let Some(r) = &t.reject {
            return PlaceOutcome::Rejected { errors: vec![reject_error(status, &r.reason)], sent };
        }
        let Some(create) = &t.create else {
            return unknown(format!("HTTP {status} but the response has no orderCreateTransaction"));
        };
        if create.client_id.as_deref() != Some(prepared.client_id.as_str()) {
            return unknown(format!("order {} came back with a different or missing clientExtensions.id than the one sent", create.id));
        }
        let description = format!(
            "{} {} {} {} {}",
            prepared.side.as_str(),
            prepared.quantity.normalized(),
            prepared.instrument,
            if prepared.is_market() { "market" } else { "limit" },
            prepared.time_in_force
        );
        let mut warnings = Vec::new();
        if let Some(fill) = &t.fill {
            if fill.order_id != create.id {
                return unknown(format!("fill transaction {} names order {} but the created order is {}", fill.id, fill.order_id, create.id));
            }
            if fill.instrument != prepared.instrument || fill.units.is_negative() != prepared.units.is_negative() {
                return unknown(format!("fill {} does not match the order sent (instrument or direction differs)", fill.id));
            }
            let filled = magnitude(fill.units);
            if filled != prepared.quantity {
                warnings.push(format!("filled {filled} of {} units", prepared.quantity));
            }
        } else if let Some(c) = &t.cancel {
            if c.order_id != create.id {
                return unknown(format!("cancel transaction {} names order {} but the created order is {}", c.id, c.order_id, create.id));
            }
            return PlaceOutcome::Rejected {
                errors: vec![ExchangeError {
                    code: format!("oanda:{status} {}: order cancelled at creation with nothing executed", c.reason),
                    class: parse::classify_cancel_reason(&c.reason),
                }],
                sent,
            };
        } else if prepared.is_market() {
            warnings.push("a market order came back with neither a fill nor a cancel: poll get_order".to_string());
        }
        PlaceOutcome::Accepted { broker_order_id: create.id.clone(), description: Some(description), sent, warnings }
    }

    fn adopt_existing(&self, existing: OrderReport, prepared: &PreparedOrder) -> PlaceOutcome {
        let sent = prepared.sent();
        let same = existing.symbol == prepared.instrument.replacen('_', "/", 1)
            && existing.side == Some(prepared.side)
            && existing.quantity == prepared.quantity;
        if !same {
            return PlaceOutcome::Rejected {
                errors: vec![ExchangeError {
                    code: format!(
                        "oanda: tag {:?} already belongs to order {} ({} {:?} {} {}), which differs from this request",
                        prepared.client_id, existing.broker_order_id, existing.symbol, existing.side, existing.quantity, existing.raw_status
                    ),
                    class: ErrorClass::InvalidArguments,
                }],
                sent,
            };
        }
        if existing.status.is_terminal() && !existing.status.has_fills() {
            return PlaceOutcome::Rejected {
                errors: vec![ExchangeError {
                    code: format!(
                        "oanda: tag {:?} already belongs to order {} which ended {:?} with nothing executed: use a new tag",
                        prepared.client_id, existing.broker_order_id, existing.status
                    ),
                    class: ErrorClass::OrderRejected,
                }],
                sent,
            };
        }
        PlaceOutcome::Accepted {
            broker_order_id: existing.broker_order_id.clone(),
            description: Some(format!("adopted existing order {} ({})", existing.broker_order_id, existing.raw_status)),
            sent,
            warnings: vec!["an order with this tag already existed; nothing was sent".to_string()],
        }
    }
}

fn magnitude(d: Dec) -> Dec {
    if d.is_negative() {
        Dec::new(-d.units(), d.scale()).unwrap_or(d)
    } else {
        d
    }
}

impl BrokerAdapter for OandaAdapter {
    fn broker_name(&self) -> &'static str {
        "oanda"
    }

    /// The account currency (its realised `balance`) plus one `Spot` entry per position with its
    /// NET signed units (negative for a short), named by canonical symbol (`EUR/USD`). Two requests.
    fn get_balances(&self) -> Result<Balances, BrokerError> {
        let s = self.verify_account()?;
        let positions = self.get_open_positions()?;
        let mut entries = vec![BalanceEntry { raw_asset: s.currency.clone(), asset: s.currency.clone(), amount: s.balance, kind: BalanceKind::Spot }];
        for p in &positions {
            if p.is_hedged() {
                return Err(BrokerError::Unsupported(format!("{}: hedged position", p.instrument)));
            }
            let symbol = canonical_symbol(&p.instrument)?;
            entries.push(BalanceEntry { raw_asset: p.instrument.clone(), asset: symbol, amount: p.net_units(), kind: BalanceKind::Spot });
        }
        Ok(Balances { entries })
    }

    /// Top-of-book bid and ask; `last` is the MID, because OANDA reports no last-trade price.
    fn get_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        let inst = normalize_instrument(symbol)?;
        let pricing = self.get_pricing(&[inst.as_str()])?;
        let p = pricing.price(&inst).ok_or_else(|| BrokerError::Malformed(format!("pricing response has no price for {inst}")))?;
        if !p.tradeable {
            return Err(BrokerError::PairNotTradable { symbol: canonical_symbol(&inst)?, status: p.status.clone().unwrap_or_else(|| "not tradeable".into()) });
        }
        let (Some(bid), Some(ask)) = (p.bid, p.ask) else {
            return Err(BrokerError::Malformed(format!("{inst}: price has no bid or no ask")));
        };
        let mid = p.mid().ok_or_else(|| BrokerError::Malformed(format!("{inst}: crossed or unusable quote (bid {bid}, ask {ask})")))?;
        Ok(Quote { symbol: canonical_symbol(&inst)?, bid, ask, last: mid })
    }

    fn place_order(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        let prepared = self.prepare(req)?;
        // Pre-checks: any Err below means NOTHING was sent.
        self.preflight()?;
        match self.get_order_by_tag(&prepared.client_id) {
            Ok(Some(existing)) => return Ok(self.adopt_existing(existing, &prepared)),
            Ok(None) => {}
            Err(e) => return Err(BrokerError::Preflight(format!("tag lookup failed, nothing was sent: {e}"))),
        }
        let sent = prepared.sent();
        let unknown = |reason: String| Ok(PlaceOutcome::UnknownOutcome { reason, sent: sent.clone() });

        match self.call(HttpMethod::Post, &self.acct_path("/orders"), Some(prepared.to_json().to_string())) {
            Ok(resp) => Ok(self.interpret_create_response(resp.status, &resp.body, &prepared)),
            Err(CallError::Transport(t)) if t.request_definitely_not_sent() => Err(BrokerError::Transport(t)),
            Err(CallError::Transport(t)) => unknown(t.to_string()),
            Err(CallError::Failure(f)) => match f {
                HttpFailure::RateLimited { .. } => Err(f.into_broker_error()),
                HttpFailure::Server { status, .. } | HttpFailure::Other { status, .. } => unknown(format!("HTTP status {status}")),
                HttpFailure::NotFound { api } => unknown(format!("HTTP 404 on order placement ({})", api.message)),
                definite => match definite.exchange_error() {
                    Some(e) => Ok(PlaceOutcome::Rejected { errors: vec![e], sent }),
                    None => unknown(format!("HTTP status {}", definite.status())),
                },
            },
        }
    }

    fn get_order(&self, broker_order_id: &str) -> Result<OrderReport, BrokerError> {
        let id = path_id(broker_order_id)?;
        let report = self.get_order_by_specifier(&enc(id))?;
        // Defensive: the response must be the order we asked about.
        if report.broker_order_id != id {
            return Err(BrokerError::Malformed(format!("asked for order {id} but received {}", report.broker_order_id)));
        }
        Ok(report)
    }

    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        self.get_pending_orders()
    }

    /// At most one order per tag. Empty when none.
    fn find_orders_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        Ok(self.get_order_by_tag(tag)?.into_iter().collect())
    }

    /// `PUT /orders/{id}/cancel`. OANDA cancels synchronously, so `pending` is false, but the caller
    /// must still follow with `get_order`: a 200 only says the cancel transaction exists, and the
    /// order may have filled first (then the PUT is refused, which is an error here).
    fn cancel_order(&self, broker_order_id: &str) -> Result<CancelOutcome, BrokerError> {
        let id = path_id(broker_order_id)?;
        let resp = self.read(HttpMethod::Put, &self.acct_path(&format!("/orders/{id}/cancel")))?;
        parse::parse_cancel_response(&resp.body).map_err(|e| BrokerError::Malformed(format!("cancel accepted but its response is unusable: {e}")))?;
        Ok(CancelOutcome { canceled_count: 1, pending: false })
    }
}
