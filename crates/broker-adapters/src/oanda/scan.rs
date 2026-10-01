//! The idempotency machinery of the OANDA adapter: the attempt registry (one transaction-stream checkpoint per tag)
//! and the scan of `GET /transactions/sinceid` for the transactions that carry a tag.
//!
//! WHY THIS EXISTS. Measured on an OANDA practice account (2026-09-23), the client id cannot be the idempotency key:
//! `GET /orders/@<id>` finds only PENDING orders (a filled market order is a 404), and a client id is NOT unique (posting
//! the same id after a fill produced a second fill). What does work is the transaction stream: read `lastTransactionID`
//! (the CHECKPOINT) before sending, and after any ambiguous outcome read `transactions/sinceid?id=<checkpoint>`, whose
//! `MARKET_ORDER` (`clientExtensions.id`) and `ORDER_FILL` / `ORDER_CANCEL` (`clientOrderID`) transactions name the tag.
//!
//! A scan is only worth something if it is COMPLETE, so [`TxnPage::completeness`](super::parse::TxnPage::completeness)
//! is enforced: a truncated or inconsistent page is [`BrokerError::LookupInconclusive`], never "not found".

use super::parse::{self, CancelInfo, CreateInfo, FillInfo, OrderResource, RejectInfo, TxnPage, TxnRecord};
use super::{lock, OandaAdapter};
use crate::error::BrokerError;
use crate::transport::HttpMethod;
use crate::types::OrderReport;
use std::collections::BTreeSet;

/// How far back a scan reaches, and therefore what an empty result proves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageKind {
    /// Everything since the checkpoint recorded before the FIRST attempt of the tag: an empty result proves the tag was
    /// not placed since then (modulo a request still in flight, see the module docs of `broker_adapters::oanda`).
    SinceCheckpoint,
    /// The most recent transaction ids only (this process has no checkpoint for the tag): an empty result proves only
    /// that the tag is not among them.
    RecentWindow,
    /// The account's whole history since its creation transaction: an empty result proves the tag was never used.
    FullHistory,
}

/// The transaction ids a scan covered: `(after_id, through_id]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Coverage {
    pub kind: CoverageKind,
    pub after_id: u64,
    pub through_id: u64,
}

impl std::fmt::Display for Coverage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = match self.kind {
            CoverageKind::SinceCheckpoint => "since the checkpoint",
            CoverageKind::RecentWindow => "recent window only",
            CoverageKind::FullHistory => "full history",
        };
        write!(f, "transactions ({}, {}] ({what})", self.after_id, self.through_id)
    }
}

/// Every transaction of one order that the scan attributed to a tag.
#[derive(Debug, Clone, PartialEq)]
pub struct TaggedOrder {
    pub order_id: String,
    /// The `*_ORDER` create transaction, when it lies inside the scanned window.
    pub create: Option<CreateInfo>,
    pub fills: Vec<FillInfo>,
    pub cancels: Vec<CancelInfo>,
}

/// The outcome of one scan for one tag.
#[derive(Debug, Clone, PartialEq)]
pub struct TagScan {
    pub tag: String,
    pub coverage: Coverage,
    /// Orders carrying the tag. More than one means the tag was used twice (OANDA does not enforce uniqueness).
    pub orders: Vec<TaggedOrder>,
    /// `*_ORDER_REJECT` transactions carrying the tag: attempts that were refused and created nothing.
    pub rejects: Vec<RejectInfo>,
}

impl TagScan {
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty() && self.rejects.is_empty()
    }
}

/// What this process knows about a tag it has (or may have) sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Attempt {
    /// `lastTransactionID` read before the FIRST send of the tag. Never moves forward: a later scan must still cover
    /// everything since the first attempt.
    pub checkpoint: u64,
}

impl TaggedOrder {
    /// The order as `GET /orders/{id}` would have described it, rebuilt from its transactions.
    pub(crate) fn resource(&self, tag: Option<&str>) -> Result<OrderResource, BrokerError> {
        let create = self.create.as_ref().ok_or_else(|| {
            BrokerError::LookupInconclusive(format!(
                "order {} carries {tag:?} on its fill or cancel but its creation lies outside the scanned window",
                self.order_id
            ))
        })?;
        let (state, filling, cancelling) = match (self.fills.first(), self.cancels.first()) {
            (Some(f), _) => ("FILLED", Some(f.id.clone()), None),
            (None, Some(c)) => ("CANCELLED", None, Some(c.id.clone())),
            (None, None) => ("PENDING", None, None),
        };
        let kind = create.kind.strip_suffix("_ORDER").unwrap_or(&create.kind).to_string();
        Ok(OrderResource {
            id: self.order_id.clone(),
            state: state.to_string(),
            kind,
            instrument: create.instrument.clone(),
            units: create.units,
            price: create.price,
            client_id: tag.map(str::to_string),
            time_in_force: create.time_in_force.clone(),
            filling_transaction_id: filling,
            cancelling_transaction_id: cancelling,
            create_time: None,
            filled_time: self.fills.first().and_then(|f| f.time),
            cancelled_time: self.cancels.first().and_then(|c| c.time),
        })
    }

    /// The neutral report. More than one fill for one order is refused (never seen; not guessed at).
    pub(crate) fn report(&self, tag: Option<&str>, own_prefix: Option<&str>) -> Result<OrderReport, BrokerError> {
        if self.fills.len() > 1 {
            return Err(BrokerError::Malformed(format!("order {} has {} fill transactions; refusing to guess its executed quantity", self.order_id, self.fills.len())));
        }
        let resource = self.resource(tag)?;
        let reason = self.cancels.first().map(|c| c.reason.clone());
        resource.into_report(self.fills.first(), reason.as_deref(), own_prefix)
    }
}

pub(super) fn is_create_kind(kind: &str) -> bool {
    kind.ends_with("_ORDER")
}

/// A refused request: `MARKET_ORDER_REJECT`, `LIMIT_ORDER_REJECT`, ... (NOT `ORDER_CANCEL_REJECT`, which is a refused
/// cancel of an existing order).
fn is_order_reject_kind(kind: &str) -> bool {
    kind.ends_with("_ORDER_REJECT") && kind != "ORDER_CANCEL_REJECT"
}

/// Attribute the page's transactions to `tag`.
fn build_scan(tag: &str, page: &TxnPage, coverage: Coverage) -> TagScan {
    let mut order_ids: BTreeSet<String> = BTreeSet::new();
    let mut rejects = Vec::new();
    for r in &page.records {
        if r.client_id.as_deref() != Some(tag) {
            continue;
        }
        match r.kind.as_str() {
            k if is_create_kind(k) => {
                order_ids.insert(r.id_text.clone());
            }
            "ORDER_FILL" | "ORDER_CANCEL" => {
                if let Some(o) = &r.order_id {
                    order_ids.insert(o.clone());
                }
            }
            k if is_order_reject_kind(k) => rejects.push(parse::parse_reject(&r.raw)),
            _ => {}
        }
    }
    let of_order = |r: &TxnRecord, oid: &str| r.order_id.as_deref() == Some(oid);
    let mut orders = Vec::new();
    for oid in order_ids {
        let mut o = TaggedOrder { order_id: oid.clone(), create: None, fills: Vec::new(), cancels: Vec::new() };
        for r in &page.records {
            if is_create_kind(&r.kind) && r.id_text == oid {
                o.create = parse::parse_create(&r.raw).ok();
            } else if r.kind == "ORDER_FILL" && of_order(r, &oid) {
                if let Ok(f) = parse::parse_fill(&r.raw) {
                    o.fills.push(f);
                }
            } else if r.kind == "ORDER_CANCEL" && of_order(r, &oid) {
                if let Ok(c) = parse::parse_cancel(&r.raw) {
                    o.cancels.push(c);
                }
            }
        }
        orders.push(o);
    }
    // numeric order (ids are integers; the set above sorted them as text)
    orders.sort_by_key(|o| o.order_id.parse::<u64>().unwrap_or(u64::MAX));
    TagScan { tag: tag.to_string(), coverage, orders, rejects }
}

impl OandaAdapter {
    // ---------------------------------------------------------------- attempt registry

    /// The checkpoint (`lastTransactionID` read before the first send) this process holds for `tag`, if it has sent it.
    /// Persist it next to the intent journal: after a restart, [`seed_tag_checkpoint`](Self::seed_tag_checkpoint) gives
    /// the lookup exact coverage instead of the bounded recent window.
    pub fn tag_checkpoint(&self, tag: &str) -> Option<u64> {
        lock(&self.attempts).get(tag).map(|a| a.checkpoint)
    }

    /// Restore a checkpoint saved before a restart. An existing (lower) checkpoint wins: a scan must reach back to the
    /// EARLIEST known attempt.
    pub fn seed_tag_checkpoint(&self, tag: &str, checkpoint: u64) {
        let mut m = lock(&self.attempts);
        let e = m.entry(tag.to_string()).or_insert(Attempt { checkpoint });
        e.checkpoint = e.checkpoint.min(checkpoint);
    }

    pub(super) fn attempt(&self, tag: &str) -> Option<Attempt> {
        lock(&self.attempts).get(tag).copied()
    }

    pub(super) fn record_attempt(&self, tag: &str, checkpoint: u64) {
        lock(&self.attempts).entry(tag.to_string()).or_insert(Attempt { checkpoint });
    }

    pub(super) fn forget_attempt(&self, tag: &str) {
        lock(&self.attempts).remove(tag);
    }

    // ---------------------------------------------------------------- the scan

    /// An order rebuilt from the transaction stream: the fallback of `get_order` when `GET /orders/<id>` answers 404
    /// (whether OANDA serves FILLED / CANCELLED orders by numeric id is not measured; by client id it does NOT).
    /// Scans from the order's own id (its create transaction id IS the order id) to the end of the stream.
    pub(super) fn order_from_transactions(&self, id: &str) -> Result<OrderReport, BrokerError> {
        let not_found = || BrokerError::NotFound(format!("order {id} is not in the transaction stream"));
        let n: u64 = id.parse().map_err(|_| not_found())?;
        if n == 0 {
            return Err(not_found());
        }
        let page = self.fetch_page(n - 1)?;
        match page.records.first() {
            Some(r) if r.id == n && is_create_kind(&r.kind) => {}
            _ => return Err(not_found()),
        }
        page.completeness().map_err(|why| BrokerError::LookupInconclusive(format!("the transaction scan from order {id} is incomplete: {why}")))?;
        let create = page.records.iter().find(|r| r.id == n).and_then(|r| parse::parse_create(&r.raw).ok()).ok_or_else(not_found)?;
        let mut o = TaggedOrder { order_id: id.to_string(), create: Some(create.clone()), fills: Vec::new(), cancels: Vec::new() };
        for r in &page.records {
            if r.order_id.as_deref() != Some(id) {
                continue;
            }
            if r.kind == "ORDER_FILL" {
                o.fills.push(parse::parse_fill(&r.raw)?);
            } else if r.kind == "ORDER_CANCEL" {
                o.cancels.push(parse::parse_cancel(&r.raw)?);
            }
        }
        o.report(create.client_id.as_deref(), self.own_prefix())
    }

    /// `GET /transactions/sinceid?id=<after_id>`, parsed. Transport and HTTP failures are returned as the plain read
    /// errors; completeness is NOT checked here.
    pub(super) fn fetch_page(&self, after_id: u64) -> Result<TxnPage, BrokerError> {
        let body = self.read(HttpMethod::Get, &self.acct_path(&format!("/transactions/sinceid?id={after_id}")))?.body;
        parse::parse_transactions_page(&body, after_id)
    }

    /// Scan everything after `after_id` for `tag`. The page must be complete (see `TxnPage::completeness`), otherwise
    /// [`BrokerError::LookupInconclusive`].
    pub fn scan_since(&self, tag: &str, after_id: u64, kind: CoverageKind) -> Result<TagScan, BrokerError> {
        let page = self.fetch_page(after_id)?;
        page.completeness().map_err(|why| BrokerError::LookupInconclusive(format!("the transaction scan after id {after_id} is incomplete: {why}")))?;
        let coverage = Coverage { kind, after_id, through_id: page.last_transaction_id };
        Ok(build_scan(tag, &page, coverage))
    }

    /// The scan that answers "was this tag placed?" from this process's point of view.
    ///
    /// * this process holds a checkpoint for the tag: everything since it ([`CoverageKind::SinceCheckpoint`]);
    /// * it does not (a restart, or a tag it never sent): the last `restart_scan_window` transaction ids counted back
    ///   from `current_last` (the `lastTransactionID` the caller just read), or the account's whole history when that is
    ///   shorter ([`CoverageKind::RecentWindow`] / [`CoverageKind::FullHistory`]).
    ///
    /// The bool says whether a checkpoint existed.
    pub(super) fn scan_for_tag(&self, tag: &str, current_last: u64) -> Result<(TagScan, bool), BrokerError> {
        if let Some(a) = self.attempt(tag) {
            return Ok((self.scan_since(tag, a.checkpoint, CoverageKind::SinceCheckpoint)?, true));
        }
        let after = current_last.saturating_sub(self.config.restart_scan_window());
        if current_last <= 1 {
            // A brand-new account: there is nothing to scan and nothing can carry the tag.
            let coverage = Coverage { kind: CoverageKind::FullHistory, after_id: 0, through_id: current_last };
            return Ok((TagScan { tag: tag.to_string(), coverage, orders: Vec::new(), rejects: Vec::new() }, false));
        }
        // Transaction 1 is the account's creation, never an order: a window that starts there covers the whole history.
        let (after, kind) = if after <= 1 { (1, CoverageKind::FullHistory) } else { (after, CoverageKind::RecentWindow) };
        Ok((self.scan_since(tag, after, kind)?, false))
    }

    /// `Err(LookupInconclusive)` for a scan of a tag this process has no checkpoint for whose window does not prove
    /// absence, when strict mode is on. `Ok` otherwise.
    pub(super) fn strict_check(&self, scan: &TagScan, had_checkpoint: bool) -> Result<(), BrokerError> {
        if self.config.strict_unseen_tags() && !had_checkpoint && scan.coverage.kind == CoverageKind::RecentWindow {
            return Err(BrokerError::LookupInconclusive(format!(
                "no checkpoint is held for {:?} (a restart, or a tag never sent by this process) and the scan covered only {}; \
                 strict mode refuses to call the tag unused",
                scan.tag, scan.coverage
            )));
        }
        Ok(())
    }
}
