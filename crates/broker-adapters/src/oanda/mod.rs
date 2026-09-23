//! OANDA v20 (fxPractice / fxTrade) FX adapter, HTTP behind [`HttpTransport`].
//!
//! # Scope and status
//!
//! Verified against hand-authored fixtures, the in-process fake broker, and (since 2026-09-23) RECORDED, sanitised
//! responses from an OANDA PRACTICE account (`tests/fixtures/oanda/real/`). It has never been run against a LIVE
//! account, and the one real-transport smoke test (`tests/oanda_practice_smoke.rs`, env-gated, `#[ignore]`d) has not yet
//! been run by anyone. It is NOT wired into any live-ready list, and the legacy `executionhandler` OANDA path is
//! untouched.
//!
//! # What real OANDA does (MEASURED on a practice account, 2026-09-23) and what that forced
//!
//! 1. `GET /orders/@<clientID>` finds ONLY PENDING orders. A FILLED market order and a CANCELLED limit order are both
//!    HTTP 404 `NO_SUCH_ORDER`. The first version of this adapter ("look the tag up, adopt the existing order") could
//!    not work for market orders.
//! 2. Client ids are NOT unique: posting the same `clientExtensions.id` again after a fill produced a second fill. The
//!    id can never deduplicate anything at the broker.
//! 3. A placement CAN be found through the transaction stream: `lastTransactionID` from `/summary` read BEFORE sending,
//!    then `GET /transactions/sinceid?id=<that>` returns the `MARKET_ORDER` (`clientExtensions.id`) and `ORDER_FILL` /
//!    `ORDER_CANCEL` (`clientOrderID`) transactions of the attempt.
//! 4. Client ids: 128 characters accepted, 129 refused (`CLIENT_ORDER_ID_INVALID`).
//! 5. `PUT /positions/{i}/close` needs `{"longUnits":"ALL","shortUnits":"NONE"}`: `ALL` for a side that does not
//!    exist is HTTP 400; nothing open at all is 404 `CLOSEOUT_POSITION_DOESNT_EXIST`.
//! 6. Refusals are HTTP 400 with a `*_ORDER_REJECT` transaction whose `rejectReason` equals the `errorCode`
//!    (`UNITS_LIMIT_EXCEEDED`, `UNITS_INVALID`, `TIME_IN_FORCE_INVALID`, `UNITS_PRECISION_EXCEEDED`,
//!    `CLIENT_ORDER_ID_INVALID`). An unknown instrument is HTTP 400 `oanda::rest::core::InvalidParameterException`
//!    with NO reject transaction. A cancel of a missing order is 404 `ORDER_DOESNT_EXIST`.
//! 7. `maximumPositionSize` is the string `"0"` on every instrument checked: no cap, never a zero limit.
//!
//! Still FROM-MEMORY-OF-DOCS (not measured; covered only by authored fixtures): the account summary, instruments,
//! open positions and pricing bodies, `GET /orders/<numeric id>` for FILLED / CANCELLED orders, `GET /transactions/<id>`,
//! the cancel-at-creation bodies (`INSUFFICIENT_MARGIN`, `MARKET_HALTED`), 401/403/429/5xx bodies, whether a position
//! close echoes `longClientExtensions` onto its transactions, `Retry-After`, weekend / halted-market behaviour, and
//! anything about the LIVE host. VERIFIED-FROM-REPO-CODE (legacy SignalEngine connector): bearer auth, the practice
//! host, `POST /v3/accounts/{id}/orders`, instrument naming, signed-string `units`.
//!
//! # Idempotency: the transaction-stream protocol
//!
//! The tag (`clientExtensions.id`) is a LABEL, never a guard. The guard is a checkpoint in the account's transaction
//! stream:
//!
//! `place_order(req)`
//! 1. validate and round locally (nothing sent); read `/summary` (account checks; its `lastTransactionID` is `L`);
//! 2. **has this tag been placed?** For a resting LIMIT order: `GET /orders/@tag` (the one lookup by client id OANDA
//!    answers). Then a transaction scan (`sinceid`): if THIS process already holds a checkpoint for the tag (an earlier
//!    attempt), scan everything since that FIRST checkpoint; otherwise scan the last `restart_scan_window` ids
//!    (default 400) counted back from `L`, or the whole history if it is shorter. A hit is resolved to
//!    `Accepted` / `Rejected` and NOTHING is sent. Two orders carrying one tag is `UnknownOutcome` (`DUPLICATE_TAG`).
//!    A scan that is incomplete or inconsistent never means "not found": before any send it is `Err(Preflight)`, after
//!    an earlier attempt it is `UnknownOutcome`;
//! 3. refuse (never truncate) an order that would exceed the instrument's `maximumPositionSize` (when it has one);
//! 4. record the checkpoint (`L`, or the earlier attempt's) and `POST`;
//! 5. a DEFINITIVE answer (201 that parses and matches, a 400 reject, 401/403) is returned as it is; 429 and a
//!    connect failure are `Err` (not sent);
//! 6. any AMBIGUOUS outcome (timeout, I/O error after sending, 5xx, 404, a malformed or contradictory 2xx): scan
//!    `sinceid` from the checkpoint. Found: resolved from the transactions (`Accepted` with the fill, `Rejected` when
//!    cancelled at creation, ...). Not found: `UnknownOutcome` whose reason ends `[oanda-tag-checkpoint=<id>]`, and the
//!    checkpoint stays registered so the next `place_order` / `find_orders_by_tag` of the same tag scans since it.
//!
//! **A tag is re-POSTed only when a scan from the FIRST attempt's checkpoint, complete and consistent, found no
//! transaction carrying the tag.** There is no other path to a second POST.
//!
//! `find_orders_by_tag(tag)` = the pending lookup + the same scan. `Ok(vec![])` means "provably not placed since the
//! scan's start" and `Err(LookupInconclusive)` means "cannot tell": callers treat it as possibly placed.
//!
//! ## Restart with no checkpoint
//! The adapter keeps no state across processes. For a tag it holds no checkpoint for it scans the last
//! `restart_scan_window` transaction ids (default 400) from the current `lastTransactionID`, or the account's entire
//! history when that is shorter (`Coverage::FullHistory`, a real proof). A tag found in the window is resolved; a tag not
//! found in a window that does NOT reach the account's start is reported "not found" only for tags that are new to the
//! caller: that answer proves the tag was not used recently, not that it was never used. Two ways to close this:
//! persist [`OandaAdapter::tag_checkpoint`] with the intent journal and restore it with
//! [`OandaAdapter::seed_tag_checkpoint`] (exact coverage since the first attempt), or turn on
//! [`OandaConfig::with_strict_unseen_tags`] (an unseen tag whose scan cannot prove absence becomes `UnknownOutcome` /
//! `LookupInconclusive`, i.e. an alert instead of a send; on an account with more transactions than the window this
//! refuses every new tag).
//!
//! ## Residual risks (documented, not fixed)
//! * A request delayed in the network can land AFTER the scan that found nothing, and a re-POST would then double.
//!   The window is small (seconds) but real; no HTTP client-side rule closes it, the fake's `Delayed` fault reproduces
//!   it, and `tests/oanda_adapter_drills.rs` pins it as a known limit.
//! * Position closes: whether OANDA echoes `longClientExtensions` on the closeout transactions is unmeasured. If it does
//!   not, the scan cannot see a lost close, and `close_position` falls back to the truth that matters (a re-read of the
//!   position): flat means done, still open means a repeated `ALL` close, which is harmless.
//! * Transaction ids are assumed consecutive within an account (the recordings show it); a scan page with a gap is
//!   refused as inconclusive, which errs toward "alert a human".
//! * The registry of attempts is in memory only and grows by one small entry per tag.
//!
//! # Error model (same contract as Kraken and Alpaca)
//!
//! * `place_order` returns `Err(_)` only when the order was definitely NOT sent (local refusal, pre-order check or
//!   tag-scan failure before any send, connect failure, HTTP 429).
//! * `PlaceOutcome::Rejected` is a definite refusal (HTTP 400 / 401 / 403, an order cancelled at creation such as
//!   `INSUFFICIENT_MARGIN`, or a tag that already belongs to a dead or different order).
//! * `PlaceOutcome::UnknownOutcome` means the order MAY exist. The caller must not re-send on its own: call
//!   `place_order` again with the SAME tag (it scans since the first checkpoint) or `find_orders_by_tag`.

pub mod auth;
pub mod config;
pub mod instrument;
pub mod order;
pub mod parse;
pub mod scan;

pub use auth::{OandaCredentials, OandaToken};
pub use config::{Environment, LiveTradingAck, OandaConfig, DEFAULT_RESTART_SCAN_WINDOW, LIVE_BASE_URL, PRACTICE_BASE_URL};
pub use instrument::{canonical_symbol, normalize_instrument, InstrumentInfo, InstrumentTable};
pub use order::{PrepareOptions, PreparedOrder};
pub use parse::{
    AccountSummary, ApiError, CancelInfo, FillInfo, HomeConversion, HttpFailure, OandaPosition, OrderResource, PriceQuote, Pricing,
};
pub use scan::{Coverage, CoverageKind, TagScan, TaggedOrder};

use crate::decimal::Dec;
use crate::error::{BrokerError, ErrorClass, ExchangeError};
use crate::transport::{HttpMethod, HttpRequest, HttpResponseDetailed, HttpTransport, TransportError};
use crate::types::{
    BalanceEntry, BalanceKind, Balances, BrokerAdapter, CancelOutcome, OrderReport, OrderRequest, PlaceOutcome, Quote, SentOrder,
};
use parse::OrderTransactions;
use serde_json::json;
use std::collections::BTreeMap;
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
    /// A close with this tag already exists (an earlier attempt): nothing was sent. Look at the report.
    AlreadyDone { report: OrderReport },
    /// The broker (or a re-read of the position after an ambiguous answer) says the instrument is flat, and no
    /// transaction of ours explains it. The wanted end state holds; who closed it is not established.
    AlreadyFlat { detail: String },
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
    /// Tag -> checkpoint of its first attempt (see the module docs, "Idempotency").
    attempts: Mutex<BTreeMap<String, scan::Attempt>>,
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

/// `lastTransactionID` of an account summary as the numeric checkpoint.
fn checkpoint_of(s: &AccountSummary) -> Result<u64, BrokerError> {
    s.last_transaction_id
        .as_deref()
        .and_then(|t| t.parse::<u64>().ok())
        .ok_or_else(|| BrokerError::Preflight("the account summary carries no usable lastTransactionID, so no idempotency checkpoint can be taken".into()))
}

/// The 404 bodies OANDA sends for "no such order": `NO_SUCH_ORDER` (GET, measured), `ORDER_DOESNT_EXIST` (cancel,
/// measured) and the older `ORDER_DOES_NOT_EXIST` spelling.
fn is_no_such_order(api: &ApiError) -> bool {
    matches!(api.code.as_deref(), Some("NO_SUCH_ORDER" | "ORDER_DOESNT_EXIST" | "ORDER_DOES_NOT_EXIST"))
        || matches!(api.reject_reason.as_deref(), Some("ORDER_DOESNT_EXIST"))
}

fn is_closeout_missing(api: &ApiError) -> bool {
    api.code.as_deref() == Some("CLOSEOUT_POSITION_DOESNT_EXIST") || api.reject_reason.as_deref() == Some("CLOSEOUT_POSITION_DOESNT_EXIST")
}

/// Marker appended to an `UnknownOutcome` reason so a later lookup (or a human) has the checkpoint.
fn checkpoint_marker(checkpoint: u64) -> String {
    format!("[oanda-tag-checkpoint={checkpoint}]")
}

/// A placement answer: what to do with it.
enum Interp {
    /// A definitive outcome; an order may exist (filled, resting, or cancelled at creation).
    Done(PlaceOutcome),
    /// A definitive refusal that created nothing: the tag is still unused.
    Refused(PlaceOutcome),
    /// Cannot be trusted: the order MAY exist. Look in the transaction stream.
    Ambiguous(String),
}

/// A close answer (a short-lived private value: boxing the outcome would only add noise).
#[allow(clippy::large_enum_variant)]
enum CloseInterp {
    Done(CloseOutcome),
    Ambiguous(String),
}

/// What a tag scan says about a placement.
enum Resolved {
    Outcome(PlaceOutcome),
    /// Only refused attempts carry the tag: nothing was created.
    RejectOnly(String),
    Nothing,
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
        Ok(Self { config, creds, transport, instruments: Mutex::new(InstrumentTable::new()), attempts: Mutex::new(BTreeMap::new()) })
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

    /// Read the account summary and check it. Any `Err` here means NOTHING has been sent.
    fn preflight(&self) -> Result<AccountSummary, BrokerError> {
        let body = self.call(HttpMethod::Get, &self.acct_path("/summary"), None).map_err(Self::preflight_error)?.body;
        let s = parse::parse_account_summary(&body).map_err(|e| BrokerError::Preflight(format!("account summary unusable: {e}")))?;
        self.check_account(&s)?;
        Ok(s)
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

    /// `GET /orders/@<tag>`: finds ONLY a PENDING order (measured: a filled market order and a cancelled limit order are
    /// both 404 `NO_SUCH_ORDER`), so this is meaningful for resting limit orders and nothing else. `Ok(None)` for
    /// `NO_SUCH_ORDER`; any other 404 (an unknown account, a body we do not recognise) is an error, never "not found".
    pub fn get_pending_order_by_tag(&self, tag: &str) -> Result<Option<OrderReport>, BrokerError> {
        order::validate_client_id(tag, None)?;
        match self.call(HttpMethod::Get, &self.acct_path(&format!("/orders/%40{}", enc(tag))), None) {
            Ok(resp) => {
                let resource = parse::parse_order_resource(&resp.body)?;
                let report = self.complete(&resource)?;
                // Defensive: never trust an unverified server-side filter.
                if report.tag.as_deref() != Some(tag) && self.own_prefix().is_none_or(|p| tag.starts_with(p)) {
                    return Err(BrokerError::Malformed(format!("lookup by client id returned an order that does not carry {tag:?}")));
                }
                Ok(Some(report))
            }
            Err(CallError::Failure(HttpFailure::NotFound { api })) if is_no_such_order(&api) => Ok(None),
            Err(e) => Err(self.read_error(e)),
        }
    }

    pub fn get_pending_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        let body = self.read(HttpMethod::Get, &self.acct_path("/pendingOrders"))?.body;
        parse::parse_order_resources(&body)?.iter().map(|r| r.into_report(None, None, self.own_prefix())).collect()
    }

    /// Cancel, then re-query so anything that happened first (a fill) is captured.
    ///
    /// If the cancel is refused because nothing is pending to cancel (404 `ORDER_DOESNT_EXIST`, or a 400 reject), the
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

    // ------------------------------------------------------------ resolution of a tag scan

    /// Turn what a tag scan found into a placement outcome. `preexisting`: the scan ran BEFORE sending (the tag was
    /// used earlier); otherwise it ran after an ambiguous answer to this call's own POST.
    fn resolve_scan(&self, scan: &scan::TagScan, prepared: &PreparedOrder, preexisting: bool) -> Resolved {
        let sent = prepared.sent();
        let unknown = |reason: String| Resolved::Outcome(PlaceOutcome::UnknownOutcome { reason, sent: sent.clone() });
        match scan.orders.as_slice() {
            [] => match scan.rejects.last() {
                Some(r) => Resolved::RejectOnly(r.reason.clone()),
                None => Resolved::Nothing,
            },
            [o] => {
                if o.fills.len() > 1 {
                    return unknown(format!("order {} carries {:?} and has {} fills: refusing to interpret it", o.order_id, prepared.client_id, o.fills.len()));
                }
                if let Some(c) = &o.create {
                    let side_ok = c.units.map(|u| u.is_negative() == prepared.units.is_negative()).unwrap_or(true);
                    let qty_ok = c.units.map(|u| magnitude(u) == prepared.quantity).unwrap_or(true);
                    let inst_ok = c.instrument.as_deref().map(|i| i == prepared.instrument).unwrap_or(true);
                    if !(side_ok && qty_ok && inst_ok) {
                        return Resolved::Outcome(PlaceOutcome::Rejected {
                            errors: vec![ExchangeError {
                                code: format!(
                                    "oanda: tag {:?} already belongs to order {} ({} {:?} units), which differs from this request",
                                    prepared.client_id,
                                    o.order_id,
                                    c.instrument.as_deref().unwrap_or("?"),
                                    c.units
                                ),
                                class: ErrorClass::InvalidArguments,
                            }],
                            sent,
                        });
                    }
                }
                let t = OrderTransactions { create: o.create.clone(), fill: o.fills.first().cloned(), cancel: o.cancels.first().cloned(), reject: None };
                match self.interpret_transactions("scan", &t, prepared, preexisting) {
                    Interp::Done(out) | Interp::Refused(out) => Resolved::Outcome(out),
                    Interp::Ambiguous(why) => unknown(why),
                }
            }
            many => {
                let ids: Vec<&str> = many.iter().map(|o| o.order_id.as_str()).collect();
                unknown(format!(
                    "DUPLICATE_TAG: {} orders carry {:?} ({}); a client id is not unique at OANDA, this needs a human",
                    many.len(),
                    prepared.client_id,
                    ids.join(", ")
                ))
            }
        }
    }

    /// The follow-up of an ambiguous POST: scan since the first checkpoint and say what is known.
    fn resolve_ambiguous(&self, prepared: &PreparedOrder, checkpoint: u64, why: String) -> PlaceOutcome {
        let sent = prepared.sent();
        let marker = checkpoint_marker(checkpoint);
        match self.scan_since(&prepared.client_id, checkpoint, scan::CoverageKind::SinceCheckpoint) {
            Err(e) => PlaceOutcome::UnknownOutcome { reason: format!("{why}; the follow-up transaction scan failed ({e}) {marker}"), sent },
            Ok(scan) => match self.resolve_scan(&scan, prepared, false) {
                Resolved::Outcome(o) => o,
                Resolved::RejectOnly(reason) => PlaceOutcome::Rejected { errors: vec![reject_error("scan", &reason)], sent },
                Resolved::Nothing => PlaceOutcome::UnknownOutcome {
                    reason: format!(
                        "{why}; {} holds no transaction with the tag, so the request was lost or is still in flight {marker}",
                        scan.coverage
                    ),
                    sent,
                },
            },
        }
    }

    // ------------------------------------------------------------ close / flatten one instrument

    /// Close the whole position in one instrument with `PUT /positions/{instrument}/close`.
    ///
    /// Only the side that exists is closed: `{"longUnits":"ALL","shortUnits":"NONE"}` for a long,
    /// `{"longUnits":"NONE","shortUnits":"ALL"}` for a short (MEASURED: `ALL` for an absent side is HTTP 400).
    ///
    /// `tag` is sent as `longClientExtensions.id` / `shortClientExtensions.id` and follows the same protocol as
    /// [`place_order`](BrokerAdapter::place_order): a transaction scan before sending, a checkpoint, a scan after an
    /// ambiguous answer. Because it is UNMEASURED whether OANDA echoes those extensions on the closeout transactions,
    /// the position itself is the final arbiter: after an ambiguous or "nothing to close" answer the position is
    /// re-read, and a flat position is reported `AlreadyFlat` (a repeated `ALL` close on a flat position is a harmless
    /// 404, and on an open one it closes what is open now).
    pub fn close_position(&self, symbol: &str, tag: &str) -> Result<CloseOutcome, BrokerError> {
        let inst = normalize_instrument(symbol)?;
        order::validate_client_id(tag, self.own_prefix())?;
        let summary = self.preflight()?;
        let last = checkpoint_of(&summary)?;
        let unknown = |reason: String| Ok(CloseOutcome::UnknownOutcome { reason });
        let (scan, had_checkpoint) = match self.scan_for_tag(tag, last) {
            Ok(x) => x,
            Err(e) if self.attempt(tag).is_some() => {
                return unknown(format!("an earlier attempt of this close exists and the transaction scan failed ({e}); nothing was sent {}", checkpoint_marker(self.attempt(tag).map_or(0, |a| a.checkpoint))))
            }
            Err(e) => return Err(BrokerError::Preflight(format!("transaction scan failed, nothing was sent: {e}"))),
        };
        if let Some(out) = self.close_from_scan(&scan, true)? {
            return Ok(out);
        }
        if let Err(e) = self.strict_check(&scan, had_checkpoint) {
            return unknown(format!("{e}; nothing was sent"));
        }
        let pos = match self.get_position(&inst)? {
            Some(p) => p,
            None => return Ok(CloseOutcome::NothingToClose),
        };
        if pos.is_hedged() {
            return Err(BrokerError::Unsupported(format!("{inst}: hedged position (long and short units both open)")));
        }
        let ext = json!({ "id": tag, "tag": self.config.client_tag() });
        let (side_prefix, body, closing_long) = if pos.long_units.is_positive() {
            ("longOrder", json!({ "longUnits": "ALL", "shortUnits": "NONE", "longClientExtensions": ext }), true)
        } else if pos.short_units.is_negative() {
            ("shortOrder", json!({ "longUnits": "NONE", "shortUnits": "ALL", "shortClientExtensions": ext }), false)
        } else {
            return Ok(CloseOutcome::NothingToClose);
        };
        let fresh = !had_checkpoint;
        let checkpoint = self.attempt(tag).map_or(last, |a| a.checkpoint);
        self.record_attempt(tag, checkpoint);
        let path = self.acct_path(&format!("/positions/{}/close", enc(&inst)));
        let interp = match self.call(HttpMethod::Put, &path, Some(body.to_string())) {
            Ok(resp) => self.interpret_close_response(resp.status, &resp.body, side_prefix, tag, &inst, closing_long),
            Err(CallError::Transport(t)) if t.request_definitely_not_sent() => {
                if fresh {
                    self.forget_attempt(tag);
                }
                return Err(BrokerError::Transport(t));
            }
            Err(CallError::Transport(t)) => CloseInterp::Ambiguous(t.to_string()),
            Err(CallError::Failure(f)) => match f {
                HttpFailure::RateLimited { .. } => {
                    if fresh {
                        self.forget_attempt(tag);
                    }
                    return Err(f.into_broker_error());
                }
                HttpFailure::Server { status, .. } | HttpFailure::Other { status, .. } => CloseInterp::Ambiguous(format!("HTTP status {status}")),
                HttpFailure::NotFound { api } | HttpFailure::Rejected { api, .. } if is_closeout_missing(&api) => {
                    // The broker says there is nothing to close. We read the position a moment ago, so either someone
                    // else closed it, or an earlier attempt of this close did: look before deciding.
                    return self.close_target_missing(tag, &inst, checkpoint);
                }
                HttpFailure::NotFound { api } => CloseInterp::Ambiguous(format!("HTTP 404 on close ({})", api.message)),
                definite => match definite.exchange_error() {
                    Some(e) => CloseInterp::Done(CloseOutcome::Rejected { errors: vec![e] }),
                    None => CloseInterp::Ambiguous(format!("HTTP status {}", definite.status())),
                },
            },
        };
        match interp {
            CloseInterp::Done(out) => {
                if fresh && matches!(out, CloseOutcome::Rejected { .. }) {
                    // A refused close created nothing: the tag is still unused.
                    if let CloseOutcome::Rejected { errors } = &out {
                        if !errors.iter().any(|e| e.code.contains("cancelled at creation")) {
                            self.forget_attempt(tag);
                        }
                    }
                }
                Ok(out)
            }
            CloseInterp::Ambiguous(why) => self.resolve_ambiguous_close(tag, &inst, checkpoint, why),
        }
    }

    fn interpret_close_response(&self, status: u16, body: &str, side_prefix: &str, tag: &str, inst: &str, closing_long: bool) -> CloseInterp {
        let t = match parse::parse_order_transactions(body, side_prefix) {
            Ok(t) => t,
            Err(_) => return CloseInterp::Ambiguous(format!("HTTP {status} but the close response is unusable")),
        };
        if let Some(r) = &t.reject {
            return CloseInterp::Done(CloseOutcome::Rejected { errors: vec![reject_error(&status.to_string(), &r.reason)] });
        }
        // The response answers OUR request. If the create transaction names a client id, it must be ours; if it names
        // none, the broker did not echo the close's extensions (unmeasured), which is not a contradiction.
        let create = t.create.as_ref();
        if create.and_then(|c| c.client_id.as_deref()).is_some_and(|c| c != tag) {
            return CloseInterp::Ambiguous(format!("HTTP {status} but the close came back with a different clientExtensions.id than the one sent"));
        }
        match (t.fill, t.cancel, create) {
            (Some(fill), _, Some(c)) if fill.order_id == c.id => {
                if fill.instrument != inst {
                    return CloseInterp::Ambiguous(format!("close fill is for {} not {inst}", fill.instrument));
                }
                if fill.units.is_negative() != closing_long {
                    return CloseInterp::Ambiguous(format!("close fill {} goes the wrong way for the position closed", fill.id));
                }
                CloseInterp::Done(CloseOutcome::Closed { broker_order_id: fill.order_id.clone(), fill })
            }
            (None, Some(c), Some(create)) if c.order_id == create.id => CloseInterp::Done(CloseOutcome::Rejected {
                errors: vec![ExchangeError {
                    code: format!("oanda:{status} {}: close order cancelled at creation", c.reason),
                    class: parse::classify_cancel_reason(&c.reason),
                }],
            }),
            _ => CloseInterp::Ambiguous(format!("HTTP {status} but the close response has no matching create+fill")),
        }
    }

    /// A scan found something for a close tag. `Ok(None)` = nothing found (carry on).
    fn close_from_scan(&self, scan: &scan::TagScan, preexisting: bool) -> Result<Option<CloseOutcome>, BrokerError> {
        let unknown = |reason: String| Ok(Some(CloseOutcome::UnknownOutcome { reason }));
        match scan.orders.as_slice() {
            [] => Ok(None),
            [o] => {
                if o.fills.len() > 1 {
                    return unknown(format!("close order {} has {} fills: refusing to interpret it", o.order_id, o.fills.len()));
                }
                if let Some(fill) = o.fills.first() {
                    return if preexisting {
                        Ok(Some(CloseOutcome::AlreadyDone { report: o.report(Some(&scan.tag), self.own_prefix())? }))
                    } else {
                        Ok(Some(CloseOutcome::Closed { broker_order_id: o.order_id.clone(), fill: fill.clone() }))
                    };
                }
                if let Some(c) = o.cancels.first() {
                    return Ok(Some(CloseOutcome::Rejected {
                        errors: vec![ExchangeError {
                            code: format!("oanda:scan {}: close order {} was cancelled at creation", c.reason, o.order_id),
                            class: parse::classify_cancel_reason(&c.reason),
                        }],
                    }));
                }
                unknown(format!("close order {} carries the tag but neither filled nor cancelled", o.order_id))
            }
            many => unknown(format!("DUPLICATE_TAG: {} orders carry {:?}; this needs a human", many.len(), scan.tag)),
        }
    }

    /// The broker answered "the position to close does not exist" although we just read it: find out why.
    fn close_target_missing(&self, tag: &str, inst: &str, checkpoint: u64) -> Result<CloseOutcome, BrokerError> {
        let marker = checkpoint_marker(checkpoint);
        match self.scan_since(tag, checkpoint, scan::CoverageKind::SinceCheckpoint) {
            Ok(scan) => {
                if let Some(out) = self.close_from_scan(&scan, false)? {
                    return Ok(out);
                }
            }
            Err(e) => return Ok(CloseOutcome::UnknownOutcome { reason: format!("the broker says there is nothing to close and the transaction scan failed ({e}) {marker}") }),
        }
        match self.get_position(inst) {
            Ok(None) => Ok(CloseOutcome::AlreadyFlat {
                detail: format!("{inst}: the broker says there is no position to close and a re-read shows none; no transaction with the tag explains it"),
            }),
            Ok(Some(p)) if p.long_units.is_zero() && p.short_units.is_zero() => Ok(CloseOutcome::AlreadyFlat {
                detail: format!("{inst}: the broker says there is no position to close and a re-read shows it flat"),
            }),
            Ok(Some(p)) => Ok(CloseOutcome::UnknownOutcome {
                reason: format!("the broker says there is nothing to close in {inst} but the position still shows {} units {marker}", p.net_units()),
            }),
            Err(e) => Ok(CloseOutcome::UnknownOutcome { reason: format!("the broker says there is nothing to close and the position could not be re-read ({e}) {marker}") }),
        }
    }

    /// The follow-up of an ambiguous close: scan since the checkpoint, then re-read the position.
    fn resolve_ambiguous_close(&self, tag: &str, inst: &str, checkpoint: u64, why: String) -> Result<CloseOutcome, BrokerError> {
        let marker = checkpoint_marker(checkpoint);
        match self.scan_since(tag, checkpoint, scan::CoverageKind::SinceCheckpoint) {
            Err(e) => return Ok(CloseOutcome::UnknownOutcome { reason: format!("{why}; the follow-up transaction scan failed ({e}) {marker}") }),
            Ok(scan) => {
                if let Some(out) = self.close_from_scan(&scan, false)? {
                    return Ok(out);
                }
            }
        }
        match self.get_position(inst) {
            Ok(None) => Ok(CloseOutcome::AlreadyFlat {
                detail: format!("{inst}: the close answer was lost ({why}) and the position is now flat; the transaction stream shows no close carrying the tag (the broker may not echo it on closeouts), so who closed it is unverified"),
            }),
            Ok(Some(p)) if p.long_units.is_zero() && p.short_units.is_zero() => Ok(CloseOutcome::AlreadyFlat {
                detail: format!("{inst}: the close answer was lost ({why}) and the position is now flat"),
            }),
            Ok(Some(_)) => Ok(CloseOutcome::UnknownOutcome { reason: format!("{why}; the position is still open and no transaction carries the tag {marker}") }),
            Err(e) => Ok(CloseOutcome::UnknownOutcome { reason: format!("{why}; the position could not be re-read ({e}) {marker}") }),
        }
    }
}

/// `oanda:<source> <reason>: order rejected` with the class from the measured table.
fn reject_error(source: &str, reason: &str) -> ExchangeError {
    let class = parse::classify_reject_reason(reason).unwrap_or_else(|| {
        parse::classify_rejection(&ApiError { code: None, message: "order rejected".into(), reject_reason: Some(reason.to_string()) })
    });
    ExchangeError { code: format!("oanda:{source} {reason}: order rejected"), class }
}

impl OandaAdapter {
    fn interpret_create_response(&self, status: u16, body: &str, prepared: &PreparedOrder) -> Interp {
        let t = match parse::parse_order_transactions(body, "order") {
            Ok(t) => t,
            Err(e) => return Interp::Ambiguous(format!("HTTP {status} but the order response is unusable: {e}")),
        };
        self.interpret_transactions(&status.to_string(), &t, prepared, false)
    }

    /// One order attempt's transactions (from a create response, or from a scan) as a placement outcome. `source` is the
    /// HTTP status or `scan`, and only shapes the message. `preexisting`: the order belongs to an EARLIER attempt.
    fn interpret_transactions(&self, source: &str, t: &OrderTransactions, prepared: &PreparedOrder, preexisting: bool) -> Interp {
        let sent: SentOrder = prepared.sent();
        if let Some(r) = &t.reject {
            return Interp::Refused(PlaceOutcome::Rejected { errors: vec![reject_error(source, &r.reason)], sent });
        }
        let Some(create) = &t.create else {
            return Interp::Ambiguous(format!("{source}: the response has no orderCreateTransaction"));
        };
        if create.client_id.as_deref() != Some(prepared.client_id.as_str()) {
            return Interp::Ambiguous(format!("order {} came back with a different or missing clientExtensions.id than the one sent", create.id));
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
        if preexisting {
            warnings.push("an order with this tag already existed; nothing was sent".to_string());
        }
        if let Some(fill) = &t.fill {
            if fill.order_id != create.id {
                return Interp::Ambiguous(format!("fill transaction {} names order {} but the created order is {}", fill.id, fill.order_id, create.id));
            }
            if fill.instrument != prepared.instrument || fill.units.is_negative() != prepared.units.is_negative() {
                return Interp::Ambiguous(format!("fill {} does not match the order sent (instrument or direction differs)", fill.id));
            }
            let filled = magnitude(fill.units);
            if filled != prepared.quantity {
                warnings.push(format!("filled {filled} of {} units", prepared.quantity));
            }
        } else if let Some(c) = &t.cancel {
            if c.order_id != create.id {
                return Interp::Ambiguous(format!("cancel transaction {} names order {} but the created order is {}", c.id, c.order_id, create.id));
            }
            let suffix = if preexisting { format!(": tag {:?} already belongs to order {} which ended cancelled: use a new tag", prepared.client_id, create.id) } else { String::new() };
            return Interp::Done(PlaceOutcome::Rejected {
                errors: vec![ExchangeError {
                    code: format!("oanda:{source} {}: order cancelled at creation with nothing executed{suffix}", c.reason),
                    class: parse::classify_cancel_reason(&c.reason),
                }],
                sent,
            });
        } else if prepared.is_market() {
            warnings.push("a market order came back with neither a fill nor a cancel: poll get_order".to_string());
        }
        Interp::Done(PlaceOutcome::Accepted { broker_order_id: create.id.clone(), description: Some(description), sent, warnings })
    }

    /// A resting limit order that already carries the tag (found by `GET /orders/@tag`, the one lookup by client id that
    /// works).
    fn adopt_pending(&self, existing: OrderReport, prepared: &PreparedOrder) -> PlaceOutcome {
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

    /// See the module docs, "Idempotency: the transaction-stream protocol". `Err(_)` means nothing was sent.
    fn place_order(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        let prepared = self.prepare(req)?;
        let tag = prepared.client_id.clone();
        let sent = prepared.sent();
        // Pre-checks: any Err below means NOTHING was sent.
        let summary = self.preflight()?;
        let last = checkpoint_of(&summary)?;

        // 1. Has this tag been placed already?
        if !prepared.is_market() {
            match self.get_pending_order_by_tag(&tag) {
                Ok(Some(existing)) => return Ok(self.adopt_pending(existing, &prepared)),
                Ok(None) => {}
                Err(e) => return Err(BrokerError::Preflight(format!("tag lookup failed, nothing was sent: {e}"))),
            }
        }
        let (scan, had_checkpoint) = match self.scan_for_tag(&tag, last) {
            Ok(x) => x,
            Err(e) => {
                return match self.attempt(&tag) {
                    // An earlier attempt may exist and this scan cannot say: that is an unknown outcome, not "nothing sent".
                    Some(a) => Ok(PlaceOutcome::UnknownOutcome {
                        reason: format!("an earlier attempt of this tag exists and the transaction scan failed ({e}); nothing was sent by this call {}", checkpoint_marker(a.checkpoint)),
                        sent,
                    }),
                    None => Err(BrokerError::Preflight(format!("transaction scan failed, nothing was sent: {e}"))),
                };
            }
        };
        match self.resolve_scan(&scan, &prepared, true) {
            Resolved::Outcome(o) => return Ok(o),
            Resolved::RejectOnly(_) | Resolved::Nothing => {}
        }
        if let Err(e) = self.strict_check(&scan, had_checkpoint) {
            return Ok(PlaceOutcome::UnknownOutcome { reason: format!("{e}; nothing was sent"), sent });
        }

        // 2. maximumPositionSize: refuse, never truncate.
        if let Some(info) = self.instrument(&prepared.instrument) {
            if info.maximum_position_size.is_some() {
                let net = match self.get_position(&prepared.instrument) {
                    Ok(p) => p.map(|p| p.net_units()).unwrap_or(Dec::ZERO),
                    Err(e) => return Err(BrokerError::Preflight(format!("could not read the position to check maximumPositionSize, nothing was sent: {e}"))),
                };
                order::check_position_cap(&info, net, prepared.units)?;
            }
        }

        // 3. Record the checkpoint of the FIRST attempt, then send.
        let fresh = !had_checkpoint;
        let checkpoint = self.attempt(&tag).map_or(last, |a| a.checkpoint);
        self.record_attempt(&tag, checkpoint);
        let interp = match self.call(HttpMethod::Post, &self.acct_path("/orders"), Some(prepared.to_json().to_string())) {
            Ok(resp) => self.interpret_create_response(resp.status, &resp.body, &prepared),
            Err(CallError::Transport(t)) if t.request_definitely_not_sent() => {
                if fresh {
                    self.forget_attempt(&tag);
                }
                return Err(BrokerError::Transport(t));
            }
            Err(CallError::Transport(t)) => Interp::Ambiguous(t.to_string()),
            Err(CallError::Failure(f)) => match f {
                HttpFailure::RateLimited { .. } => {
                    if fresh {
                        self.forget_attempt(&tag);
                    }
                    return Err(f.into_broker_error());
                }
                HttpFailure::Server { status, .. } | HttpFailure::Other { status, .. } => Interp::Ambiguous(format!("HTTP status {status}")),
                HttpFailure::NotFound { api } => Interp::Ambiguous(format!("HTTP 404 on order placement ({})", api.message)),
                definite => match definite.exchange_error() {
                    Some(e) => Interp::Refused(PlaceOutcome::Rejected { errors: vec![e], sent: sent.clone() }),
                    None => Interp::Ambiguous(format!("HTTP status {}", definite.status())),
                },
            },
        };
        match interp {
            Interp::Done(out) => Ok(out),
            Interp::Refused(out) => {
                if fresh {
                    // A refusal creates no order: the tag is still unused.
                    self.forget_attempt(&tag);
                }
                Ok(out)
            }
            // 4. Ambiguous: the stream decides; nothing is ever re-sent from here.
            Interp::Ambiguous(why) => Ok(self.resolve_ambiguous(&prepared, checkpoint, why)),
        }
    }

    /// `GET /orders/<id>`; when OANDA answers 404 (unmeasured whether it serves FILLED / CANCELLED orders by numeric id)
    /// the order is rebuilt from the transaction stream.
    fn get_order(&self, broker_order_id: &str) -> Result<OrderReport, BrokerError> {
        let id = path_id(broker_order_id)?;
        match self.get_order_by_specifier(&enc(id)) {
            Ok(report) => {
                // Defensive: the response must be the order we asked about.
                if report.broker_order_id != id {
                    return Err(BrokerError::Malformed(format!("asked for order {id} but received {}", report.broker_order_id)));
                }
                Ok(report)
            }
            Err(BrokerError::NotFound(_)) => self.order_from_transactions(id),
            Err(e) => Err(e),
        }
    }

    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        self.get_pending_orders()
    }

    /// The orders carrying `tag`: the pending lookup (resting orders) plus the transaction scan (everything else). See
    /// the module docs. `Ok(vec![])` = provably not placed since the scan's start; `Err(LookupInconclusive)` = cannot
    /// tell (treat as possibly placed).
    fn find_orders_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        order::validate_client_id(tag, None)?;
        let pending = self.get_pending_order_by_tag(tag)?;
        let last = if self.attempt(tag).is_some() {
            0
        } else {
            let s = self.get_account_summary()?;
            checkpoint_of(&s).map_err(|e| BrokerError::LookupInconclusive(e.to_string()))?
        };
        let scanned = self.scan_for_tag(tag, last);
        let (scan, had) = match scanned {
            Ok(x) => x,
            // A resting order was found: that answers the question even if the history could not be read.
            Err(_) if pending.is_some() => return Ok(pending.into_iter().collect()),
            Err(e) => return Err(e),
        };
        let mut reports: Vec<OrderReport> = Vec::new();
        for o in &scan.orders {
            reports.push(o.report(Some(tag), self.own_prefix())?);
        }
        if let Some(p) = pending {
            if !reports.iter().any(|r| r.broker_order_id == p.broker_order_id) {
                reports.push(p);
            }
        }
        if reports.is_empty() {
            self.strict_check(&scan, had)?;
        }
        Ok(reports)
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
