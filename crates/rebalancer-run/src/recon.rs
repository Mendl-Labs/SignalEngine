//! Reconciliation: does the broker's account match what we expect? Any UNEXPLAINED finding means
//! [`ReconVerdict::HaltAndAlert`]: the run stops trading, the account halts and a person is alerted. Nothing here
//! calls a broker; the pipeline reads the account and looks orders up, then hands the facts to [`reconcile`].
//!
//! # What is checked (each has a stable machine code, [`ReconCode`])
//! * **Foreign open orders** (`RECON_FOREIGN_ORDER`). An open order is OURS when its tag starts with
//!   [`crate::broker::OWN_TAG_PREFIX`] (`rb1:`) or when its broker id is one we recorded at placement
//!   (`known_order_ids`). Anything else is foreign: someone else (a person in the Kraken UI, another process on the
//!   same key) is trading this account. Halts.
//! * **Unexplained drift** (`RECON_POSITION_DRIFT`, `RECON_BALANCE_DRIFT`). The broker's cash and holdings are
//!   compared with a [`ReconBaseline`]: before a run, the previous run's post-run snapshot; after a run, that
//!   snapshot plus the effect of the fills our own orders report. A difference whose VALUE exceeds
//!   `max(value_abs, equity_pct * equity)` halts (defaults: 1.00 in the account currency, 0.1% of equity). A deposit
//!   or withdrawal is drift too: it halts, and a person resumes (which re-baselines).
//! * **Missing orders** (`RECON_ORDER_MISSING`): an order we recorded as accepted (we hold its broker id) that the
//!   broker no longer knows. Halts. An order whose placement outcome was unknown and was NOT found is only noted.
//! * **Duplicate fills** (`RECON_DUPLICATE_FILL`): a report whose executed quantity exceeds the order's quantity
//!   (the adapters refuse to report an inflated fill and raise an "overfill anomaly"), more executed quantity under
//!   one tag than we planned, or more than one filled order under one tag. Halts.
//! * **Stale views** (`RECON_STALE_VIEW`): the snapshot is older than `max_view_age_secs` (default 120) or stamped in
//!   the future. Halts.
//! * **Equity cross-check** (`RECON_EQUITY_MISMATCH`): the broker's equity differs from `cash + holdings` beyond
//!   `max(value_abs, derived_equity_pct * equity)` (default 1%). Halts (an unpriced or staked asset, or a valuation
//!   gap).
//! * **Unvalued holdings** (`RECON_UNVALUED_HOLDING`) and **negative cash** (`RECON_NEGATIVE_CASH`, margin in use).
//!   Halt.
//! * Informational, never halting: `RECON_NO_BASELINE` (first run: drift cannot be judged) and
//!   `RECON_OWN_OPEN_ORDER` (one of our orders is still resting; the pipeline cancels it).

use std::collections::{BTreeMap, BTreeSet};

use broker_adapters::{Dec, OrderReport, Side};
use chrono::{DateTime, Duration, Utc};
use rebalancer_core::dec_math::{abs, add, mul, sub};

use crate::broker::is_own_order;
use crate::view::BrokerSnapshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReconCode {
    ForeignOrder,
    PositionDrift,
    BalanceDrift,
    OrderMissing,
    DuplicateFill,
    StaleView,
    EquityMismatch,
    UnvaluedHolding,
    NegativeCash,
    NoBaseline,
    OwnOpenOrder,
}

impl ReconCode {
    pub const ALL: [ReconCode; 11] = [
        ReconCode::ForeignOrder,
        ReconCode::PositionDrift,
        ReconCode::BalanceDrift,
        ReconCode::OrderMissing,
        ReconCode::DuplicateFill,
        ReconCode::StaleView,
        ReconCode::EquityMismatch,
        ReconCode::UnvaluedHolding,
        ReconCode::NegativeCash,
        ReconCode::NoBaseline,
        ReconCode::OwnOpenOrder,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ReconCode::ForeignOrder => "RECON_FOREIGN_ORDER",
            ReconCode::PositionDrift => "RECON_POSITION_DRIFT",
            ReconCode::BalanceDrift => "RECON_BALANCE_DRIFT",
            ReconCode::OrderMissing => "RECON_ORDER_MISSING",
            ReconCode::DuplicateFill => "RECON_DUPLICATE_FILL",
            ReconCode::StaleView => "RECON_STALE_VIEW",
            ReconCode::EquityMismatch => "RECON_EQUITY_MISMATCH",
            ReconCode::UnvaluedHolding => "RECON_UNVALUED_HOLDING",
            ReconCode::NegativeCash => "RECON_NEGATIVE_CASH",
            ReconCode::NoBaseline => "RECON_NO_BASELINE",
            ReconCode::OwnOpenOrder => "RECON_OWN_OPEN_ORDER",
        }
    }
}

impl std::fmt::Display for ReconCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Recorded, does not stop the run.
    Info,
    /// Unexplained: halt and alert.
    Halt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconFinding {
    pub code: ReconCode,
    pub severity: Severity,
    pub symbol: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconVerdict {
    Ok,
    HaltAndAlert,
}

impl ReconVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            ReconVerdict::Ok => "OK",
            ReconVerdict::HaltAndAlert => "HALT_AND_ALERT",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconReport {
    pub at: DateTime<Utc>,
    pub findings: Vec<ReconFinding>,
    pub verdict: ReconVerdict,
}

impl ReconReport {
    pub fn has(&self, code: ReconCode) -> bool {
        self.findings.iter().any(|f| f.code == code)
    }
    pub fn codes(&self) -> Vec<&'static str> {
        self.findings.iter().map(|f| f.code.as_str()).collect()
    }
    pub fn halt_findings(&self) -> impl Iterator<Item = &ReconFinding> {
        self.findings.iter().filter(|f| f.severity == Severity::Halt)
    }
    /// One line per halting finding, for the halt record and the alert.
    pub fn halt_summary(&self) -> String {
        self.halt_findings().map(|f| format!("{}: {}", f.code, f.message)).collect::<Vec<_>>().join(" | ")
    }
}

/// Reconciliation tolerances. All are parameters; the defaults are conservative for a small account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconTolerances {
    /// Absolute floor of the drift threshold, in the account currency.
    pub value_abs: Dec,
    /// Drift threshold as a fraction of equity.
    pub equity_pct: Dec,
    /// Threshold for the broker-equity vs cash-plus-holdings cross-check, as a fraction of equity.
    pub derived_equity_pct: Dec,
    /// A snapshot older than this many seconds is stale.
    pub max_view_age_secs: i64,
    /// A snapshot stamped further than this many seconds in the future is stale.
    pub max_future_skew_secs: i64,
}

impl Default for ReconTolerances {
    fn default() -> Self {
        Self {
            value_abs: Dec::from_i64(1),
            equity_pct: Dec::new(1, 3).unwrap_or(Dec::ZERO),
            derived_equity_pct: Dec::new(1, 2).unwrap_or(Dec::ZERO),
            max_view_age_secs: 120,
            max_future_skew_secs: 5,
        }
    }
}

impl ReconTolerances {
    /// `max(value_abs, pct * equity)`.
    pub fn threshold(&self, equity: Dec, pct: Dec) -> Dec {
        match mul(pct, equity) {
            Ok(p) if p > self.value_abs => p,
            _ => self.value_abs,
        }
    }
}

/// What the account should look like: cash and quantities by symbol (upper case), plus the marks known when the
/// baseline was taken (to value a quantity that has vanished from the current view).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconBaseline {
    pub cash: Dec,
    pub holdings: BTreeMap<String, Dec>,
    pub marks: BTreeMap<String, Dec>,
}

/// One order we placed (or tried to) and everything the broker told us about it.
#[derive(Debug, Clone, PartialEq)]
pub struct ExpectedOrder {
    pub tag: String,
    pub symbol: String,
    pub side: Side,
    pub planned_quantity: Dec,
    /// The broker id, when placement was accepted (or when a by-tag lookup found the order).
    pub broker_order_id: Option<String>,
    /// Reports the broker returned for this order (by id and/or by tag), possibly none.
    pub reports: Vec<OrderReport>,
    /// Problems met while reading the order (an "overfill anomaly" from the adapter, for example).
    pub anomalies: Vec<String>,
    /// True when the order was really sent (Live). Paper and assisted orders do not exist at the broker.
    pub expect_exists: bool,
}

fn unique_reports(o: &ExpectedOrder) -> Vec<&OrderReport> {
    let mut seen = BTreeSet::new();
    o.reports.iter().filter(|r| seen.insert(r.broker_order_id.clone())).collect()
}

impl ReconBaseline {
    pub fn from_snapshot(s: &BrokerSnapshot) -> Self {
        let mut holdings = BTreeMap::new();
        let mut marks = BTreeMap::new();
        for h in &s.holdings {
            holdings.insert(h.symbol.to_uppercase(), h.quantity);
            if let Some(m) = h.mark {
                marks.insert(h.symbol.to_uppercase(), m);
            }
        }
        Self { cash: s.cash, holdings, marks }
    }

    /// This baseline plus the effect of the fills our orders report: a buy adds the executed quantity and spends
    /// `cost + fee`; a sell removes it and receives `cost - fee`. Reports are de-duplicated by broker order id.
    /// Fees and costs the broker did not report count as zero / `executed * avg_price`. `None` on arithmetic
    /// overflow (the caller treats that as a failure to reconcile).
    pub fn after_orders(&self, orders: &[ExpectedOrder]) -> Option<ReconBaseline> {
        let mut next = self.clone();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for o in orders {
            for r in unique_reports(o) {
                if !seen.insert(r.broker_order_id.clone()) || !r.executed_quantity.is_positive() {
                    continue;
                }
                let cost = match (r.cost, r.avg_price) {
                    (Some(c), _) => c,
                    (None, Some(p)) => mul(r.executed_quantity, p).ok()?,
                    (None, None) => return None,
                };
                let fee = r.fee.unwrap_or(Dec::ZERO);
                let sym = o.symbol.to_uppercase();
                let q = next.holdings.get(&sym).copied().unwrap_or(Dec::ZERO);
                match o.side {
                    Side::Buy => {
                        next.holdings.insert(sym, add(q, r.executed_quantity).ok()?);
                        next.cash = sub(next.cash, add(cost, fee).ok()?).ok()?;
                    }
                    Side::Sell => {
                        next.holdings.insert(sym, sub(q, r.executed_quantity).ok()?);
                        next.cash = add(next.cash, sub(cost, fee).ok()?).ok()?;
                    }
                }
            }
        }
        Some(next)
    }
}

pub struct ReconInput<'a> {
    pub view: &'a BrokerSnapshot,
    pub now: DateTime<Utc>,
    /// Broker ids of orders we placed in earlier runs.
    pub known_order_ids: &'a BTreeSet<String>,
    /// What the account should hold; `None` = no baseline yet.
    pub baseline: Option<&'a ReconBaseline>,
    /// Orders of the current run (empty before trading).
    pub orders: &'a [ExpectedOrder],
    pub tolerances: &'a ReconTolerances,
}

fn finding(code: ReconCode, severity: Severity, symbol: Option<&str>, message: String) -> ReconFinding {
    ReconFinding { code, severity, symbol: symbol.map(str::to_string), message }
}

/// Reconcile. Pure. A failure of the arithmetic itself is reported as a halting finding (fail closed).
pub fn reconcile(input: &ReconInput<'_>) -> ReconReport {
    let mut findings = Vec::new();
    check(input, &mut findings);
    let verdict = if findings.iter().any(|f| f.severity == Severity::Halt) { ReconVerdict::HaltAndAlert } else { ReconVerdict::Ok };
    ReconReport { at: input.now, findings, verdict }
}

fn check(input: &ReconInput<'_>, out: &mut Vec<ReconFinding>) {
    let v = input.view;
    let tol = input.tolerances;

    // Stale view.
    let age = input.now.signed_duration_since(v.taken_at);
    if age > Duration::seconds(tol.max_view_age_secs) {
        out.push(finding(
            ReconCode::StaleView,
            Severity::Halt,
            None,
            format!("the account view is {} s old (limit {} s)", age.num_seconds(), tol.max_view_age_secs),
        ));
    } else if age < Duration::seconds(-tol.max_future_skew_secs) {
        out.push(finding(
            ReconCode::StaleView,
            Severity::Halt,
            None,
            format!("the account view is stamped {} s in the future", -age.num_seconds()),
        ));
    }

    // Open orders: foreign ones halt, our own are noted.
    for o in &v.open_orders {
        if is_own_order(o, input.known_order_ids) {
            out.push(finding(
                ReconCode::OwnOpenOrder,
                Severity::Info,
                Some(&o.symbol),
                format!("our order {} is still open ({:?})", o.broker_order_id, o.status),
            ));
        } else {
            out.push(finding(
                ReconCode::ForeignOrder,
                Severity::Halt,
                Some(&o.symbol),
                format!("open order {} on {} carries no tag of ours (tag {:?}): someone else is trading this account", o.broker_order_id, o.symbol, o.tag),
            ));
        }
    }

    // Holdings we cannot price, margin.
    for u in &v.unvalued {
        out.push(finding(ReconCode::UnvaluedHolding, Severity::Halt, Some(&u.asset), format!("{} {} cannot be valued: {}", u.quantity, u.asset, u.reason)));
    }
    if v.cash.is_negative() {
        out.push(finding(ReconCode::NegativeCash, Severity::Halt, None, format!("cash is {}: margin or an unexplained debit", v.cash)));
    }

    // Broker equity vs our arithmetic over the broker's own numbers.
    match sub(v.equity, v.derived_equity).and_then(abs) {
        Ok(gap) => {
            let limit = tol.threshold(v.equity, tol.derived_equity_pct);
            if gap > limit {
                out.push(finding(
                    ReconCode::EquityMismatch,
                    Severity::Halt,
                    None,
                    format!("broker equity {} differs from cash plus holdings {} by {gap} (limit {limit})", v.equity, v.derived_equity),
                ));
            }
        }
        Err(e) => out.push(finding(ReconCode::EquityMismatch, Severity::Halt, None, format!("equity cross-check failed: {e}"))),
    }

    // Drift against the baseline.
    match input.baseline {
        None => out.push(finding(ReconCode::NoBaseline, Severity::Info, None, "no baseline: drift cannot be judged on the first run".to_string())),
        Some(b) => check_drift(v, b, tol, out),
    }

    // Our own orders.
    for o in input.orders {
        check_order(o, out);
    }
}

fn check_drift(v: &BrokerSnapshot, b: &ReconBaseline, tol: &ReconTolerances, out: &mut Vec<ReconFinding>) {
    let limit = tol.threshold(v.equity, tol.equity_pct);
    match sub(v.cash, b.cash).and_then(abs) {
        Ok(d) if d > limit => out.push(finding(
            ReconCode::BalanceDrift,
            Severity::Halt,
            None,
            format!("cash is {} but {} was expected: unexplained difference {d} (limit {limit})", v.cash, b.cash),
        )),
        Ok(_) => {}
        Err(e) => out.push(finding(ReconCode::BalanceDrift, Severity::Halt, None, format!("cash comparison failed: {e}"))),
    }
    let mut symbols: BTreeSet<String> = b.holdings.keys().cloned().collect();
    symbols.extend(v.holdings.iter().map(|h| h.symbol.to_uppercase()));
    for sym in symbols {
        let actual = v.quantity_of(&sym);
        let expected = b.holdings.get(&sym).copied().unwrap_or(Dec::ZERO);
        let delta = match sub(actual, expected).and_then(abs) {
            Ok(d) => d,
            Err(e) => {
                out.push(finding(ReconCode::PositionDrift, Severity::Halt, Some(&sym), format!("quantity comparison failed: {e}")));
                continue;
            }
        };
        if delta.is_zero() {
            continue;
        }
        let mark = v.holding(&sym).and_then(|h| h.mark).or_else(|| b.marks.get(&sym).copied());
        match mark {
            Some(m) => match mul(delta, m) {
                Ok(value) if value > limit => out.push(finding(
                    ReconCode::PositionDrift,
                    Severity::Halt,
                    Some(&sym),
                    format!("{sym}: holding {actual}, expected {expected}: unexplained {delta} worth {value} (limit {limit})"),
                )),
                Ok(_) => {}
                Err(e) => out.push(finding(ReconCode::PositionDrift, Severity::Halt, Some(&sym), format!("valuation failed: {e}"))),
            },
            None => out.push(finding(
                ReconCode::PositionDrift,
                Severity::Halt,
                Some(&sym),
                format!("{sym}: holding {actual}, expected {expected} and no price to judge the difference"),
            )),
        }
    }
}

fn check_order(o: &ExpectedOrder, out: &mut Vec<ReconFinding>) {
    if !o.expect_exists {
        return;
    }
    for a in &o.anomalies {
        out.push(finding(ReconCode::DuplicateFill, Severity::Halt, Some(&o.symbol), format!("{}: {a}", o.tag)));
    }
    let reports = unique_reports(o);
    if reports.is_empty() {
        match &o.broker_order_id {
            Some(id) => out.push(finding(
                ReconCode::OrderMissing,
                Severity::Halt,
                Some(&o.symbol),
                format!("order {id} ({}) was accepted but the broker no longer knows it", o.tag),
            )),
            None if o.anomalies.is_empty() => out.push(finding(
                ReconCode::OrderMissing,
                Severity::Info,
                Some(&o.symbol),
                format!("{}: placement outcome was unknown and no such order exists at the broker (not placed)", o.tag),
            )),
            None => {}
        }
        return;
    }
    let mut executed = Dec::ZERO;
    let mut filled_orders = 0;
    for r in &reports {
        if r.executed_quantity > r.quantity {
            out.push(finding(
                ReconCode::DuplicateFill,
                Severity::Halt,
                Some(&o.symbol),
                format!("order {} executed {} of {}", r.broker_order_id, r.executed_quantity, r.quantity),
            ));
        }
        if r.executed_quantity.is_positive() {
            filled_orders += 1;
        }
        executed = add(executed, r.executed_quantity).unwrap_or(executed);
    }
    if filled_orders > 1 {
        out.push(finding(
            ReconCode::DuplicateFill,
            Severity::Halt,
            Some(&o.symbol),
            format!("{} orders carrying {} executed (double placement)", filled_orders, o.tag),
        ));
    } else if executed > o.planned_quantity {
        out.push(finding(
            ReconCode::DuplicateFill,
            Severity::Halt,
            Some(&o.symbol),
            format!("{} executed {executed}, more than the {} planned", o.tag, o.planned_quantity),
        ));
    }
}
