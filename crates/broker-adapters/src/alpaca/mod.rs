//! Alpaca equities adapter (paper and live), HTTP behind [`HttpTransport`].
//!
//! # Provenance of the facts encoded here
//!
//! VERIFIED-FROM-REPO-CODE (SignalEngine `exchanges/generic/config.rs` `alpaca_paper_definition`,
//! `connector.rs` `parse_fill_status`, its real-payload tests, and the `trade_updates` socket):
//! header auth with `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY`; paper base URL
//! `https://paper-api.alpaca.markets` (hard-coded there: a verified defect); `POST /v2/orders`
//! with body fields `symbol`, `side`, `type`, `qty`, `limit_price`, `client_order_id`;
//! `GET /v2/orders/{id}`, `GET /v2/account`, `GET /v2/clock`; the order object fields `id`,
//! `client_order_id`, `status`, `filled_qty`, `filled_avg_price` as decimal STRINGS with
//! `filled_avg_price` null until a fill exists; status names `filled`, `partially_filled`,
//! `canceled`, `expired`, `done_for_day`, `rejected`, `new`, `accepted`, `pending_new`; the error
//! body carries a `message`; the websocket `trade_updates` stream and its event names. The
//! connector's use of POST for cancel is a verified defect (Alpaca cancels with DELETE), and its
//! mapping of partially filled then canceled orders drops the executed quantity.
//!
//! FROM-MEMORY-OF-DOCS (NOT verified in this repository; needs the paper-account smoke test):
//! everything else, including `DELETE /v2/orders/{id}` (204), `GET /v2/orders:by_client_order_id`,
//! `GET /v2/orders?status=...`, `GET/DELETE /v2/positions[/{symbol}]` and its 207 multi-status
//! body, `GET /v2/calendar`, `GET /v2/assets/{symbol}` fields, the account fields
//! (`trading_blocked`, `account_blocked`, `pattern_day_trader`, `buying_power`, `account_number`),
//! error code numbers, HTTP 403 for insufficient buying power, 422 for unprocessable orders,
//! 429 with `Retry-After`, `client_order_id` uniqueness, the 128 character limit, fractional
//! shares to 9 dp needing `day`, sub-penny price rules, the `PK`/`AK` key prefixes and the `PA`
//! paper account-number prefix. Test fixtures are hand-written to those documented shapes; they
//! are NOT recordings from a live account.
//!
//! # Error model (same contract as Kraken)
//!
//! * `place_order` returns `Err(_)` only when the order was definitely NOT sent (local refusal,
//!   pre-order check failure, connect failure, HTTP 429).
//! * `PlaceOutcome::Rejected` is a definite broker refusal (401/403/422, buying power).
//! * `PlaceOutcome::UnknownOutcome` (timeouts, I/O errors, 5xx, unparseable success, duplicate
//!   client_order_id) means the order MAY exist: look it up with `find_orders_by_tag` before any
//!   retry.
//!
//! Alpaca rejects a second order carrying an existing `client_order_id`, so re-sending after an
//! unknown outcome cannot double-place; it comes back as `UnknownOutcome` telling the caller to
//! look the existing order up.

pub mod assets;
pub mod auth;
pub mod config;
pub mod order;
pub mod parse;
pub mod time;

pub use assets::{AssetInfo, AssetSource, AssetTable, FRACTIONAL_DP};
pub use auth::AlpacaCredentials;
pub use config::{AlpacaConfig, Environment, LIVE_BASE_URL, PAPER_BASE_URL};
pub use order::{PrepareOptions, PreparedOrder};
pub use parse::{
    AccountInfo, CalendarDay, ClockInfo, FlattenEntry, FlattenReport, HttpFailure, PositionInfo, PositionSide,
};

use crate::error::{BrokerError, ErrorClass, ExchangeError};
use crate::transport::{HttpMethod, HttpRequest, HttpResponseDetailed, HttpTransport, TransportError};
use crate::types::{
    BalanceEntry, BalanceKind, Balances, BrokerAdapter, CancelOutcome, OrderReport, OrderRequest, OrderStatus,
    PlaceOutcome, Quote,
};
use serde_json::Value;
use std::fmt;
use std::sync::{Arc, Mutex};

pub mod paths {
    pub const ACCOUNT: &str = "/v2/account";
    pub const POSITIONS: &str = "/v2/positions";
    pub const ORDERS: &str = "/v2/orders";
    pub const ORDER_BY_CLIENT_ID: &str = "/v2/orders:by_client_order_id";
    pub const CLOCK: &str = "/v2/clock";
    pub const CALENDAR: &str = "/v2/calendar";
    pub const ASSETS: &str = "/v2/assets";
}

/// Largest page `GET /v2/orders` returns (FROM-MEMORY-OF-DOCS). A full page is treated as
/// possibly truncated and refused rather than under-reported.
pub const MAX_ORDER_PAGE: u32 = 500;

/// `status` filter of `GET /v2/orders`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderListFilter {
    Open,
    Closed,
    All,
}

impl OrderListFilter {
    fn as_str(self) -> &'static str {
        match self {
            OrderListFilter::Open => "open",
            OrderListFilter::Closed => "closed",
            OrderListFilter::All => "all",
        }
    }
}

enum CallError {
    Transport(TransportError),
    Failure(HttpFailure),
}

pub struct AlpacaAdapter {
    config: AlpacaConfig,
    creds: AlpacaCredentials,
    transport: Arc<dyn HttpTransport>,
    assets: Mutex<AssetTable>,
}

impl fmt::Debug for AlpacaAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlpacaAdapter")
            .field("environment", &self.config.environment)
            .field("base_url", &self.config.base_url)
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

/// An id that is safe to place in a URL path (order ids are UUIDs).
fn path_id(id: &str) -> Result<&str, BrokerError> {
    if !id.is_empty() && id.len() <= 64 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
        Ok(id)
    } else {
        Err(BrokerError::InvalidRequest(format!("invalid order id {id:?}")))
    }
}

impl AlpacaAdapter {
    /// Refuses (with `Config`/`Credentials`) a base URL that does not match the environment, and
    /// credentials marked for a different environment than the config. The asset table starts as
    /// the built-in (unverified) rows.
    pub fn new(
        config: AlpacaConfig,
        creds: AlpacaCredentials,
        transport: Arc<dyn HttpTransport>,
    ) -> Result<Self, BrokerError> {
        let config = config.validated()?;
        if creds.environment() != config.environment {
            return Err(BrokerError::Credentials(format!(
                "credentials are marked {} but the adapter is configured for {}; refusing to send them",
                creds.environment().as_str(),
                config.environment.as_str()
            )));
        }
        Ok(Self { config, creds, transport, assets: Mutex::new(AssetTable::builtin()) })
    }

    pub fn environment(&self) -> Environment {
        self.config.environment
    }

    pub fn base_url(&self) -> &str {
        &self.config.base_url
    }

    pub fn key_id_fingerprint(&self) -> String {
        self.creds.key_id_fingerprint()
    }

    pub fn config(&self) -> &AlpacaConfig {
        &self.config
    }

    // ------------------------------------------------------------ assets

    /// Replace the whole asset table.
    pub fn set_assets(&self, table: AssetTable) {
        *lock(&self.assets) = table;
    }

    pub fn upsert_asset(&self, info: AssetInfo) {
        lock(&self.assets).upsert(info);
    }

    pub fn asset(&self, symbol: &str) -> Option<AssetInfo> {
        lock(&self.assets).lookup(symbol).cloned()
    }

    /// `GET /v2/assets/{symbol}`; stores the parsed row (replacing a built-in one) and returns it.
    pub fn refresh_asset(&self, symbol: &str) -> Result<AssetInfo, BrokerError> {
        let sym = order::normalize_symbol(symbol)?;
        let resp = match self.call(HttpMethod::Get, &format!("{}/{}", paths::ASSETS, enc(&sym)), None) {
            Ok(r) => r,
            Err(CallError::Failure(HttpFailure::NotFound { .. })) => return Err(BrokerError::UnknownSymbol(sym)),
            Err(e) => return Err(self.read_error(e)),
        };
        let info = AssetInfo::parse_json(&resp.body)?;
        if info.symbol != sym {
            return Err(BrokerError::Malformed(format!("asked for asset {sym} but received {}", info.symbol)));
        }
        lock(&self.assets).upsert(info.clone());
        Ok(info)
    }

    // ------------------------------------------------------------ transport plumbing

    fn build_request(&self, method: HttpMethod, path_and_query: &str, body: Option<String>) -> HttpRequest {
        let mut headers = self.creds.headers();
        headers.push(("Accept".to_string(), "application/json".to_string()));
        if body.is_some() {
            headers.push(("Content-Type".to_string(), "application/json".to_string()));
        }
        HttpRequest { method, url: format!("{}{}", self.config.base_url, path_and_query), headers, body }
    }

    fn call(&self, method: HttpMethod, path_and_query: &str, body: Option<String>) -> Result<HttpResponseDetailed, CallError> {
        let req = self.build_request(method, path_and_query, body);
        let resp = self.transport.execute_detailed(&req).map_err(CallError::Transport)?;
        if (200..300).contains(&resp.status) {
            Ok(resp)
        } else {
            Err(CallError::Failure(parse::classify_http_failure(resp.status, &resp.body, resp.header("Retry-After"))))
        }
    }

    /// Error for a read-style call.
    fn read_error(&self, e: CallError) -> BrokerError {
        match e {
            CallError::Transport(t) => BrokerError::Transport(t),
            CallError::Failure(f) => f.into_broker_error(),
        }
    }

    fn read(&self, method: HttpMethod, path_and_query: &str) -> Result<HttpResponseDetailed, BrokerError> {
        self.call(method, path_and_query, None).map_err(|e| self.read_error(e))
    }

    /// Error for a pre-order check: nothing has been sent, so transport trouble and server-side
    /// trouble both become `Preflight` (never a bare `Transport(Timeout)` that a caller could
    /// mistake for an unknown order outcome).
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
        self.config.own_tag_prefix.as_deref()
    }

    // ------------------------------------------------------------ account / positions / clock

    pub fn get_account(&self) -> Result<AccountInfo, BrokerError> {
        parse::parse_account(&self.read(HttpMethod::Get, paths::ACCOUNT)?.body)
    }

    /// Verdict on an account: it must not be blocked, and it must belong to the configured
    /// environment. The environment check has two layers: the host guard plus the fact that an
    /// Alpaca key only works against its own environment's host (a wrong key gets 401/403), and
    /// this account-number check (a live adapter refuses an account number with the paper prefix
    /// `PA`, FROM-MEMORY-OF-DOCS).
    pub fn check_account(&self, acct: &AccountInfo) -> Result<(), BrokerError> {
        if self.config.environment == Environment::Live {
            if let Some(n) = &acct.account_number {
                if n.starts_with("PA") {
                    return Err(BrokerError::Credentials(format!(
                        "adapter is configured LIVE but the account number {n} has the paper prefix (PA)"
                    )));
                }
            }
        }
        match acct.blocked_reason() {
            Some(why) => Err(BrokerError::AccountBlocked(why)),
            None => Ok(()),
        }
    }

    /// `GET /v2/account` plus [`check_account`](Self::check_account).
    pub fn verify_account(&self) -> Result<AccountInfo, BrokerError> {
        let acct = self.get_account()?;
        self.check_account(&acct)?;
        Ok(acct)
    }

    pub fn get_positions(&self) -> Result<Vec<PositionInfo>, BrokerError> {
        parse::parse_positions(&self.read(HttpMethod::Get, paths::POSITIONS)?.body)
    }

    pub fn get_clock(&self) -> Result<ClockInfo, BrokerError> {
        parse::parse_clock(&self.read(HttpMethod::Get, paths::CLOCK)?.body)
    }

    /// Market calendar for `start..=end` (`YYYY-MM-DD` each).
    pub fn get_calendar(&self, start: &str, end: &str) -> Result<Vec<CalendarDay>, BrokerError> {
        for d in [start, end] {
            if !time::is_valid_ymd(d) {
                return Err(BrokerError::InvalidRequest(format!("calendar date {d:?} is not YYYY-MM-DD")));
            }
        }
        if start > end {
            return Err(BrokerError::InvalidRequest("calendar start is after end".into()));
        }
        let pq = format!("{}?start={}&end={}", paths::CALENDAR, enc(start), enc(end));
        parse::parse_calendar(&self.read(HttpMethod::Get, &pq)?.body)
    }

    // ------------------------------------------------------------ orders

    /// Pure: validate and round a request into the exact order that would be sent. Sends nothing.
    pub fn prepare(&self, req: &OrderRequest) -> Result<PreparedOrder, BrokerError> {
        let sym = order::normalize_symbol(&req.symbol)?;
        let asset = self.asset(&sym).ok_or_else(|| BrokerError::UnknownSymbol(sym.clone()))?;
        order::prepare_order(
            req,
            &asset,
            &PrepareOptions {
                allow_extended_hours: self.config.allow_extended_hours,
                min_notional: self.config.min_notional,
                own_tag_prefix: self.config.own_tag_prefix.clone(),
                refuse_builtin_assets: self.config.refuse_builtin_assets,
            },
        )
    }

    /// Account and market-hours checks before the POST. Any `Err` means nothing was sent.
    fn preflight(&self, prepared: &PreparedOrder) -> Result<(), BrokerError> {
        let body = self.call(HttpMethod::Get, paths::ACCOUNT, None).map_err(Self::preflight_error)?.body;
        let acct = parse::parse_account(&body).map_err(|e| BrokerError::Preflight(format!("account response unusable: {e}")))?;
        self.check_account(&acct)?;
        if prepared.is_market() && !self.config.allow_extended_hours {
            let body = self.call(HttpMethod::Get, paths::CLOCK, None).map_err(Self::preflight_error)?.body;
            let clock = parse::parse_clock(&body).map_err(|e| BrokerError::Preflight(format!("clock response unusable: {e}")))?;
            if !clock.is_open {
                return Err(BrokerError::MarketClosed { next_open: clock.next_open, next_close: clock.next_close });
            }
        }
        Ok(())
    }

    /// `GET /v2/orders`. A page as large as `limit` is refused (possible truncation).
    pub fn list_orders(&self, filter: OrderListFilter, limit: u32, symbols: &[&str]) -> Result<Vec<OrderReport>, BrokerError> {
        if limit == 0 || limit > MAX_ORDER_PAGE {
            return Err(BrokerError::InvalidRequest(format!("order list limit must be 1..={MAX_ORDER_PAGE}")));
        }
        let mut pq = format!("{}?status={}&limit={limit}&direction=desc&nested=false", paths::ORDERS, filter.as_str());
        if !symbols.is_empty() {
            let syms: Result<Vec<String>, BrokerError> = symbols.iter().map(|s| order::normalize_symbol(s)).collect();
            pq.push_str(&format!("&symbols={}", enc(&syms?.join(","))));
        }
        let orders = parse::parse_orders(&self.read(HttpMethod::Get, &pq)?.body, self.own_prefix())?;
        if orders.len() as u32 >= limit {
            return Err(BrokerError::Malformed(format!(
                "order list returned {} orders (the page limit); refusing to under-report",
                orders.len()
            )));
        }
        Ok(orders)
    }

    /// `GET /v2/orders:by_client_order_id`. `Ok(None)` when Alpaca answers 404: no order carries
    /// this tag.
    pub fn get_order_by_tag(&self, tag: &str) -> Result<Option<OrderReport>, BrokerError> {
        if tag.is_empty() || tag.len() > order::MAX_CLIENT_ORDER_ID_LEN {
            return Err(BrokerError::InvalidRequest("tag must be 1..=128 characters".into()));
        }
        let pq = format!("{}?client_order_id={}", paths::ORDER_BY_CLIENT_ID, enc(tag));
        match self.call(HttpMethod::Get, &pq, None) {
            Ok(resp) => {
                let v: Value = serde_json::from_str(&resp.body).map_err(|_| BrokerError::Malformed("order body is not JSON".into()))?;
                let report = parse::parse_order_value(&v, self.own_prefix())?;
                // Defensive: never trust an unverified server-side filter.
                if parse::client_order_id_of(&v).as_deref() != Some(tag) {
                    return Err(BrokerError::Malformed(format!("by_client_order_id returned an order with a different client_order_id than {tag:?}")));
                }
                Ok(Some(report))
            }
            Err(CallError::Failure(HttpFailure::NotFound { .. })) => Ok(None),
            Err(e) => Err(self.read_error(e)),
        }
    }

    /// Cancel, then re-query so executed quantity from a partial fill is captured.
    ///
    /// If the cancel is refused because there is nothing to cancel (422, "order is not
    /// cancelable": the order already filled, was canceled or expired; or 404), the order is
    /// re-queried and the final report is returned with
    /// `CancelOutcome { canceled_count: 0, pending: false }`. If the re-query finds no such order
    /// either, `BrokerError::CancelTargetNotFound` is returned. Any other failure is returned as
    /// `cancel_order` would return it.
    pub fn cancel_and_settle(&self, broker_order_id: &str) -> Result<(CancelOutcome, OrderReport), BrokerError> {
        let id = path_id(broker_order_id)?;
        let outcome = match self.call(HttpMethod::Delete, &format!("{}/{}", paths::ORDERS, id), None) {
            Ok(_) => CancelOutcome { canceled_count: 1, pending: true },
            Err(CallError::Failure(HttpFailure::Rejected { status: 422, .. } | HttpFailure::NotFound { .. })) => {
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

    // ------------------------------------------------------------ flatten

    /// `DELETE /v2/positions/{symbol}`: submit the order that closes one position. `Ok(None)` when
    /// there is no such position (404). NOTE: a `Transport(Timeout)` or 5xx here is an UNKNOWN
    /// outcome: read `get_positions` and `open_orders` before doing anything else. If shares are
    /// held by open orders Alpaca refuses; use [`flatten_all`](Self::flatten_all) or cancel first.
    pub fn close_position(&self, symbol: &str) -> Result<Option<OrderReport>, BrokerError> {
        let sym = order::normalize_symbol(symbol)?;
        match self.call(HttpMethod::Delete, &format!("{}/{}", paths::POSITIONS, enc(&sym)), None) {
            Ok(resp) => Ok(Some(parse::parse_order(&resp.body, self.own_prefix())?)),
            Err(CallError::Failure(HttpFailure::NotFound { .. })) => Ok(None),
            Err(e) => Err(self.read_error(e)),
        }
    }

    /// `DELETE /v2/positions?cancel_orders=true`: cancel ALL open orders on the account (ours and
    /// foreign) and submit closing orders for every position. The 207 body reports each position
    /// separately: check [`FlattenReport::all_ok`] and then verify with `get_positions` once the
    /// orders have filled. A timeout or 5xx is an unknown outcome: read state before retrying.
    pub fn flatten_all(&self) -> Result<FlattenReport, BrokerError> {
        let resp = self.read(HttpMethod::Delete, &format!("{}?cancel_orders=true", paths::POSITIONS))?;
        parse::parse_flatten(&resp.body, self.own_prefix())
    }
}

impl BrokerAdapter for AlpacaAdapter {
    fn broker_name(&self) -> &'static str {
        "alpaca"
    }

    /// Cash (as `USD`, or the account currency) plus one `Spot` entry per position with its signed
    /// quantity (negative for a short). Two requests: account and positions.
    fn get_balances(&self) -> Result<Balances, BrokerError> {
        let acct = self.get_account()?;
        let positions = self.get_positions()?;
        let currency = acct.currency.clone().unwrap_or_else(|| "USD".to_string()).to_ascii_uppercase();
        let mut entries = vec![BalanceEntry { raw_asset: currency.clone(), asset: currency, amount: acct.cash, kind: BalanceKind::Spot }];
        let mut pos: Vec<BalanceEntry> = positions
            .iter()
            .map(|p| BalanceEntry { raw_asset: p.symbol.clone(), asset: p.symbol.clone(), amount: p.signed_qty(), kind: BalanceKind::Spot })
            .collect();
        pos.sort_by(|a, b| a.raw_asset.cmp(&b.raw_asset));
        entries.extend(pos);
        Ok(Balances { entries })
    }

    /// Not implemented: Alpaca quotes come from the separate market-data API (another host, feed
    /// entitlements), which is outside WP3.2.
    fn get_quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        Err(BrokerError::Unsupported(format!(
            "{symbol}: quotes come from the Alpaca market-data API, which this adapter does not call; \
             take prices from the data layer and pass them as OrderRequest::reference_price"
        )))
    }

    fn place_order(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        let prepared = self.prepare(req)?;
        self.preflight(&prepared)?;
        let sent = prepared.sent();
        let unknown = |reason: String| Ok(PlaceOutcome::UnknownOutcome { reason, sent: sent.clone() });

        match self.call(HttpMethod::Post, paths::ORDERS, Some(prepared.to_json().to_string())) {
            Ok(resp) => {
                let v: Value = match serde_json::from_str(&resp.body) {
                    Ok(v) => v,
                    Err(_) => return unknown(format!("HTTP {} but the body is not JSON", resp.status)),
                };
                let report = match parse::parse_order_value(&v, self.own_prefix()) {
                    Ok(r) => r,
                    Err(e) => return unknown(format!("HTTP {} but the order response is unusable: {e}", resp.status)),
                };
                if parse::client_order_id_of(&v).as_deref() != Some(prepared.client_order_id.as_str()) {
                    return unknown(format!(
                        "order {} came back with a different client_order_id than the one sent",
                        report.broker_order_id
                    ));
                }
                if report.status == OrderStatus::Rejected {
                    return Ok(PlaceOutcome::Rejected {
                        errors: vec![ExchangeError {
                            code: format!("alpaca: order {} was returned with status rejected", report.broker_order_id),
                            class: ErrorClass::OrderRejected,
                        }],
                        sent,
                    });
                }
                let description = format!(
                    "{} {} {} {} {}",
                    prepared.side.as_str(),
                    prepared.quantity.normalized(),
                    prepared.symbol,
                    if prepared.is_market() { "market" } else { "limit" },
                    prepared.time_in_force
                );
                Ok(PlaceOutcome::Accepted { broker_order_id: report.broker_order_id, description: Some(description), sent, warnings: Vec::new() })
            }
            Err(CallError::Transport(t)) if t.request_definitely_not_sent() => Err(BrokerError::Transport(t)),
            Err(CallError::Transport(t)) => unknown(t.to_string()),
            Err(CallError::Failure(f)) => match f {
                HttpFailure::RateLimited { .. } => Err(f.into_broker_error()),
                HttpFailure::DuplicateClientOrderId { api } => unknown(format!(
                    "an order with this client_order_id already exists ({}): look it up by tag",
                    api.message
                )),
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
        let resp = self.read(HttpMethod::Get, &format!("{}/{}", paths::ORDERS, id))?;
        let report = parse::parse_order(&resp.body, self.own_prefix())?;
        // Defensive: the response must be the order we asked about.
        if !report.broker_order_id.eq_ignore_ascii_case(id) {
            return Err(BrokerError::Malformed(format!("asked for order {id} but received {}", report.broker_order_id)));
        }
        Ok(report)
    }

    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        self.list_orders(OrderListFilter::Open, MAX_ORDER_PAGE, &[])
    }

    /// At most one order per tag (Alpaca enforces client_order_id uniqueness). Empty when none.
    fn find_orders_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        Ok(self.get_order_by_tag(tag)?.into_iter().collect())
    }

    /// `DELETE /v2/orders/{id}` (SignalEngine POSTs here: verified defect). A 204 means the cancel
    /// was ACCEPTED, not that it took effect: `pending` is always true and the caller must follow
    /// with `get_order`, because the order may have filled (partly or wholly) first.
    fn cancel_order(&self, broker_order_id: &str) -> Result<CancelOutcome, BrokerError> {
        let id = path_id(broker_order_id)?;
        self.read(HttpMethod::Delete, &format!("{}/{}", paths::ORDERS, id))?;
        Ok(CancelOutcome { canceled_count: 1, pending: true })
    }
}
