//! The paper kill drill (W7.6 of `product-mandate/SENIOR_RESEARCHER_GAP_CLOSURE_PLAN.md`, MVP verification gate 3).
//!
//! A repeatable drill against an Alpaca PAPER account. It drives the rebalancer's production pipeline (`run_once`,
//! `pilot_run_config()`, `ExecutionMode::Live`, `VenuePolicy::PaperOnly`) through a scripted breach: shrink, then
//! flatten, then halt inside a time bound. A later run and a forged resume must both be refused.
//!
//! The breach is injected at the broker-snapshot seam: the equity the risk ladder reads is replaced (equity and the
//! broker's own cross-check together, so reconciliation does not fire first). Everything else is production: the
//! risk overlay, the state machine, the flatten and the pre-trade guard all run as they do in a real drawdown, against
//! the real paper account's positions and orders.
//!
//! Nothing here can resume a halt. The only resume path needs a `HumanApproval`, which `rebalancer-risk` issues only
//! behind its `human-endpoint` feature. This crate never enables that feature and never calls the issuer. The one
//! resume-adjacent check, [`refuse_forged_resume`], goes through the state store's own transition guard.
//!
//! Stop rule: the drill stops at the first failed step and places no further orders. A failed run can therefore leave
//! the paper position open; the report states what the account looked like when the drill stopped.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

use broker_adapters::alpaca::PAPER_BASE_URL;
use broker_adapters::{
    BrokerError, CancelOutcome, Dec, OrderReport, OrderRequest, OrderStatus, PlaceOutcome, Quote,
    Side,
};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use mandate_core::mandate::{self, MandateBody};
use rebalancer_alerts::{OutboundEmail, Sender};
use rebalancer_core::dec_math::{div_floor, mul, sub};
use rebalancer_core::guard::PricePoint;
use rebalancer_core::policy::{MandateEnvelope, MandateStatus};
use rebalancer_core::venue::{VenueRuleBook, VenueRules};
use rebalancer_risk::overlay::RiskCode;
use rebalancer_risk::state::{AccountState, AccountStatus};
use rebalancer_risk::store::{InMemoryStateStore, StateStore, StoreError};
use rebalancer_run::broker::{Broker, SnapshotError, VenueEnvironment, OWN_TAG_PREFIX};
use rebalancer_run::clock::Clock;
use rebalancer_run::data::{DataError, DataSource, SleeveData, SleeveSpec};
use rebalancer_run::flatten::FlattenVerdict;
use rebalancer_run::pipeline::{run_once, RunConfig, RunContext};
use rebalancer_run::record::{ExecutionMode, OutcomeKind, RunRecord};
use rebalancer_run::stores::InMemoryRunStore;
use rebalancer_run::testkit::{RecordingNotifier, SwitchKillFlag};
use rebalancer_run::view::BrokerSnapshot;
use serde_json::json;

use crate::pilot::pilot_run_config;

/// The one instrument the drill trades: liquid, and fractional shares are supported on Alpaca paper.
pub const DRILL_SYMBOL: &str = "SPY";
/// Shares opened: 0.01 SPY is about $6 at $600. The notional cap is enforced by the limit price (see `open_position`).
pub const DRILL_QTY: &str = "0.01";
/// The default and the hard ceiling of the opening notional, in dollars. `--max-notional` can only lower it.
pub const DEFAULT_MAX_NOTIONAL: &str = "10";
/// Required on every invocation, dry runs included.
pub const ACK_FLAG: &str = "--i-understand-this-places-paper-orders";
/// Paper credentials, read from the environment only. Values are never printed, logged or written.
pub const ENV_KEY: &str = "ALPACA_PAPER_API_KEY";
pub const ENV_SECRET: &str = "ALPACA_PAPER_API_SECRET";
/// Recipient of the drill's alert e-mail. Delivery needs this AND a configured sender (`RESEND_API_KEY` plus
/// `ALERT_FROM_EMAIL`); without both the alert step fails and the drill exits non-zero.
pub const ENV_ALERT_TO: &str = "KILL_DRILL_ALERT_TO";
/// The drill mandate's ladder: shrink at 2 % (scale 0.5), halt and flatten at 4 %, daily-loss limit 3 %.
const SHRINK_AT: &str = "0.02";
const HALT_AT: &str = "0.04";
const SHRINK_SCALE: &str = "0.5";
const DAILY_LOSS_LIMIT: &str = "0.03";
/// Scripted breaches, as drawdowns from the starting equity: 2.5 % lands in the shrink band, 4.5 % in the halt band.
pub const SHRINK_BREACH: &str = "0.025";
pub const HALT_BREACH: &str = "0.045";
/// The halting run (flatten, verified flat, halted) must finish within this many seconds of starting.
pub const HALT_BOUND_SECS: i64 = 120;
/// Polls (one second apart) for the opening order to settle, and for the pipeline's flatten to verify.
const OPEN_FILL_POLLS: u32 = 15;
const FLATTEN_POLLS: u32 = 10;
/// Halt reasons the drill accepts for the scripted halt (the ladder rung or the daily-loss limit, whichever the
/// overlay evaluates first at 4.5 %).
const HALT_REASONS: [&str; 2] = ["HALT_DRAWDOWN_LADDER", "HALT_DAILY_LOSS"];
/// The steps, in order. A passing drill has every one of them verified.
pub const STEPS: [&str; 8] = [
    "preflight",
    "open_position",
    "baseline_run",
    "shrink",
    "flatten_and_halt",
    "resume_refused",
    "final_state",
    "alert_delivery",
];

/// Why the drill refused to run. A refusal happens before any order can be sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    MissingAck,
    UnknownArgument(String),
    BadNotional(String),
    NotPaperEnvironment(String),
    NotPaperUrl(String),
}

impl Refusal {
    pub fn code(&self) -> &'static str {
        match self {
            Refusal::MissingAck => "KILL_DRILL_NOT_ACKNOWLEDGED",
            Refusal::UnknownArgument(_) => "KILL_DRILL_UNKNOWN_ARGUMENT",
            Refusal::BadNotional(_) => "KILL_DRILL_BAD_NOTIONAL",
            Refusal::NotPaperEnvironment(_) => "KILL_DRILL_NOT_PAPER_ENVIRONMENT",
            Refusal::NotPaperUrl(_) => "KILL_DRILL_NOT_PAPER_URL",
        }
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refusal::MissingAck => write!(
                f,
                "{}: the flag {ACK_FLAG} is required on every invocation, dry runs included",
                self.code()
            ),
            Refusal::UnknownArgument(a) => {
                write!(f, "{}: {a:?} is not a kill-drill argument", self.code())
            }
            Refusal::BadNotional(v) => write!(
                f,
                "{}: {v} (the notional must be above 0 and at most {DEFAULT_MAX_NOTIONAL})",
                self.code()
            ),
            Refusal::NotPaperEnvironment(e) => write!(
                f,
                "{}: the venue reports environment {e:?}; only paper is accepted",
                self.code()
            ),
            Refusal::NotPaperUrl(u) => write!(
                f,
                "{}: base URL {u:?} is not the Alpaca paper host",
                self.code()
            ),
        }
    }
}

impl std::error::Error for Refusal {}

/// Parsed command line.
#[derive(Debug, Clone, PartialEq)]
pub struct Args {
    pub dry_run: bool,
    pub max_notional: Dec,
}

/// Parse the arguments (without the program name). Refuses unless [`ACK_FLAG`] is present, anything unknown is refused,
/// and `--max-notional` can only lower the default cap.
pub fn parse_args<S: AsRef<str>>(raw: &[S]) -> Result<Args, Refusal> {
    let mut dry_run = false;
    let mut acknowledged = false;
    let mut notional: Option<String> = None;
    let mut it = raw.iter().map(|s| s.as_ref());
    while let Some(arg) = it.next() {
        match arg {
            "--dry-run" => dry_run = true,
            ACK_FLAG => acknowledged = true,
            "--max-notional" => match it.next() {
                Some(v) => notional = Some(v.to_string()),
                None => {
                    return Err(Refusal::BadNotional(
                        "--max-notional needs a value".to_string(),
                    ))
                }
            },
            other => return Err(Refusal::UnknownArgument(other.to_string())),
        }
    }
    if !acknowledged {
        return Err(Refusal::MissingAck);
    }
    let max_notional = parse_max_notional(notional.as_deref().unwrap_or(DEFAULT_MAX_NOTIONAL))?;
    Ok(Args {
        dry_run,
        max_notional,
    })
}

/// A notional above 0 and at most the default cap. Lowering is the only direction allowed.
pub fn parse_max_notional(raw: &str) -> Result<Dec, Refusal> {
    let cap = Dec::parse(DEFAULT_MAX_NOTIONAL).unwrap_or_else(|_| Dec::from_i64(10));
    match Dec::parse(raw.trim()) {
        Ok(v) if v.is_positive() && v <= cap => Ok(v),
        _ => Err(Refusal::BadNotional(format!("{raw:?}"))),
    }
}

/// The target check: the venue must report Paper, and the base URL must be the Alpaca paper host (or loopback, which
/// only the fake-broker tests use). Anything else, including a live host, is refused before the broker is touched.
pub fn check_target(base_url: &str, environment: VenueEnvironment) -> Result<(), Refusal> {
    if environment != VenueEnvironment::Paper {
        return Err(Refusal::NotPaperEnvironment(
            environment.as_str().to_string(),
        ));
    }
    let url = base_url.trim().trim_end_matches('/');
    if url == PAPER_BASE_URL.trim_end_matches('/') || is_loopback(url) {
        Ok(())
    } else {
        Err(Refusal::NotPaperUrl(base_url.to_string()))
    }
}

fn is_loopback(url: &str) -> bool {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    let authority = rest.split('/').next().unwrap_or("");
    let host = authority.rsplit_once(':').map_or(authority, |(h, _)| h);
    matches!(host, "127.0.0.1" | "localhost")
}

/// What the drill will do, printed by `--dry-run`. Pure: no broker, no database, no credentials.
pub fn render_plan(args: &Args) -> String {
    let cap = &args.max_notional;
    let mut out = String::new();
    out.push_str("kill-drill DRY RUN: nothing is sent, no broker or database is contacted, no credentials are read.\n");
    out.push_str(&format!(
        "venue: Alpaca paper ({PAPER_BASE_URL}); any other host or environment is refused\n"
    ));
    out.push_str(&format!(
        "instrument: {DRILL_SYMBOL}; opening quantity {DRILL_QTY}; notional cap {cap} (limit ceiling per share is cap / quantity, rounded down)\n"
    ));
    out.push_str(&format!("drill mandate: ladder shrink {SHRINK_AT} (scale {SHRINK_SCALE}) and halt {HALT_AT}; daily-loss limit {DAILY_LOSS_LIMIT}\n"));
    out.push_str("steps:\n");
    out.push_str(
        "  1 preflight: the paper account must be flat with no open orders, or the drill refuses\n",
    );
    out.push_str("  2 open_position: one limit buy of the opening quantity at or below the ceiling (a fill costs at most the cap)\n");
    out.push_str("  3 baseline_run: production pipeline run (Live, paper-only), no breach: expect completed, active, starting equity as the high-water mark\n");
    out.push_str(&format!("  4 shrink: equity down {SHRINK_BREACH} (shrink rung {SHRINK_AT}): expect shrunk at scale {SHRINK_SCALE}, no flatten, position unchanged\n"));
    out.push_str(&format!(
        "  5 flatten_and_halt: equity down {HALT_BREACH} (halt rung {HALT_AT}): expect flatten verified flat and halted within {HALT_BOUND_SECS}s\n"
    ));
    out.push_str("  6 resume_refused: a later run is refused RUN_ACCOUNT_HALTED with no order; a forged halted-to-active save is refused by the store\n");
    out.push_str("  7 final_state: position flat, no open orders, state halted (read-only)\n");
    out.push_str(&format!(
        "  8 alert_delivery: a critical ALERT_HALT was raised and delivered through the alert sender to {ENV_ALERT_TO}\n"
    ));
    out.push_str("exit codes: 0 every step verified; 1 a step failed (named in the report); 2 refused before any order; 3 could not connect or verify the account\n");
    out
}

/// One verified (or failed) step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
}

/// The drill's outcome. `passed()` is true only when every step in [`STEPS`] ran and verified.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub steps: Vec<Step>,
    pub notes: Vec<String>,
}

impl Report {
    pub fn passed(&self) -> bool {
        self.steps.len() == STEPS.len() && self.steps.iter().all(|s| s.passed)
    }

    pub fn exit_code(&self) -> i32 {
        if self.passed() {
            0
        } else {
            1
        }
    }

    pub fn render(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        for n in &self.notes {
            let _ = writeln!(out, "{n}");
        }
        for s in &self.steps {
            let tag = if s.passed { "PASS" } else { "FAIL" };
            let _ = writeln!(out, "{tag} {}: {}", s.name, s.detail);
        }
        for name in STEPS.iter().skip(self.steps.len()) {
            let _ = writeln!(out, "NOT RUN {name}");
        }
        let _ = writeln!(
            out,
            "RESULT: {}",
            if self.passed() { "PASS" } else { "FAIL" }
        );
        out
    }
}

/// Everything the drill needs from its caller. The binary builds these from the real paper connection; the tests build
/// them from the fake exchange.
pub struct DrillDeps<'a> {
    /// The base URL the broker was built against. Checked with the broker's own environment before any order.
    pub base_url: &'a str,
    pub broker: &'a dyn Broker,
    pub rules: &'a dyn VenueRules,
    pub clock: &'a dyn Clock,
    /// The alert sender (the platform's own Resend sender in production). `None` means delivery cannot be verified.
    pub sender: Option<&'a dyn Sender>,
    /// The alert recipient. `None` means delivery cannot be verified.
    pub alert_to: Option<&'a str>,
    /// The opening notional cap (see [`parse_max_notional`]).
    pub max_notional: Dec,
    /// Printed in the report and in the alert subject. Use the key fingerprint, never the key.
    pub label: &'a str,
}

/// Run the drill. Refusals return `Err` before any broker call. A dry run returns the plan and calls nothing.
pub fn run_drill(deps: &DrillDeps<'_>, args: &Args) -> Result<Report, Refusal> {
    if args.dry_run {
        return Ok(Report {
            steps: Vec::new(),
            notes: vec![render_plan(args)],
        });
    }
    check_target(deps.base_url, deps.broker.environment())?;

    let mut report = Report::default();
    let mut drill = match Drill::new(deps) {
        Ok(d) => d,
        Err(e) => {
            record(&mut report, STEPS[0], Err(format!("setup: {e}")));
            return Ok(report);
        }
    };
    let verified = record(&mut report, STEPS[0], drill.preflight())
        && record(&mut report, STEPS[1], drill.open_position())
        && record(&mut report, STEPS[2], drill.baseline_run())
        && record(&mut report, STEPS[3], drill.shrink())
        && record(&mut report, STEPS[4], drill.flatten_and_halt())
        && record(&mut report, STEPS[5], drill.resume_refused())
        && record(&mut report, STEPS[6], drill.final_state());
    if verified {
        let summary = report.render();
        record(&mut report, STEPS[7], drill.alert_delivery(&summary));
    }
    Ok(report)
}

type StepResult = Result<String, String>;

fn record(report: &mut Report, name: &'static str, result: StepResult) -> bool {
    let (passed, detail) = match result {
        Ok(d) => (true, d),
        Err(d) => (false, d),
    };
    report.steps.push(Step {
        name,
        passed,
        detail,
    });
    passed
}

/// The state the drill carries from step to step.
struct Drill<'a> {
    broker: &'a dyn Broker,
    clock: &'a dyn Clock,
    sender: Option<&'a dyn Sender>,
    alert_to: Option<&'a str>,
    max_notional: Dec,
    label: &'a str,
    account: String,
    breach: BreachBroker<'a>,
    marks: MarksSource<'a>,
    book: VenueRuleBook<'a>,
    states: InMemoryStateStore,
    runs: InMemoryRunStore,
    notifier: RecordingNotifier,
    kill: SwitchKillFlag,
    mandate: MandateBody,
    envelope: MandateEnvelope,
    config: RunConfig,
    last_slot: Option<DateTime<Utc>>,
    base_equity: Option<Dec>,
}

impl<'a> Drill<'a> {
    fn new(deps: &DrillDeps<'a>) -> Result<Self, String> {
        let now = deps.clock.now();
        let mandate = drill_mandate()?;
        let mut config = pilot_run_config();
        config.max_polls = FLATTEN_POLLS;
        Ok(Self {
            broker: deps.broker,
            clock: deps.clock,
            sender: deps.sender,
            alert_to: deps.alert_to,
            max_notional: deps.max_notional,
            label: deps.label,
            account: "kill-drill".to_string(),
            breach: BreachBroker::new(deps.broker),
            marks: MarksSource {
                broker: deps.broker,
            },
            book: VenueRuleBook::new().with("alpaca", deps.rules),
            states: InMemoryStateStore::new(),
            runs: InMemoryRunStore::new(),
            notifier: RecordingNotifier::new(),
            kill: SwitchKillFlag::new(),
            mandate,
            envelope: drill_envelope(now),
            config,
            last_slot: None,
            base_equity: None,
        })
    }

    fn snapshot(&self) -> Result<BrokerSnapshot, String> {
        self.broker
            .snapshot(self.clock.now())
            .map_err(|e| format!("broker read failed: {e}"))
    }

    fn state(&self) -> Result<AccountState, String> {
        self.states
            .load(&self.account)
            .map_err(|e| format!("state load failed: {e}"))?
            .ok_or_else(|| "no account state was saved".to_string())
    }

    fn spy_quantity(snap: &BrokerSnapshot) -> Dec {
        snap.holding(DRILL_SYMBOL).map_or(Dec::ZERO, |h| h.quantity)
    }

    /// The equity the baseline run saw, reduced by `fraction`.
    fn breached_equity(&self, fraction: &str) -> Result<Dec, String> {
        let base = self
            .base_equity
            .ok_or("the baseline run has not set the starting equity")?;
        let f = Dec::parse(fraction).map_err(|_| format!("bad breach fraction {fraction}"))?;
        let loss = mul(base, f).map_err(|e| format!("breach arithmetic: {e:?}"))?;
        sub(base, loss).map_err(|e| format!("breach arithmetic: {e:?}"))
    }

    /// One production pipeline run in Live mode, at the next strictly later slot.
    fn live_run(&mut self) -> RunRecord {
        let scheduled = next_slot(self.clock, &mut self.last_slot);
        let ctx = RunContext {
            account_id: &self.account,
            scheduled_for: scheduled,
            trading_day: scheduled.date_naive(),
            mode: ExecutionMode::Live,
            sleeves: &[],
            mandate: Some(&self.mandate),
            envelope: Some(&self.envelope),
            broker: &self.breach,
            data: &self.marks,
            state_store: &self.states,
            clock: self.clock,
            runs: &self.runs,
            notifier: &self.notifier,
            kill_flag: &self.kill,
            venue_rules: &self.book,
            config: &self.config,
            cache: None,
        };
        run_once(&ctx)
    }

    fn preflight(&self) -> StepResult {
        let snap = self.snapshot()?;
        let held: Vec<String> = snap
            .holdings
            .iter()
            .filter(|h| !h.quantity.is_zero())
            .map(|h| format!("{} {}", h.symbol, h.quantity))
            .collect();
        if !held.is_empty() {
            return Err(format!("the paper account is not flat ({}); the drill refuses to run against a non-flat account", held.join(", ")));
        }
        if !snap.open_orders.is_empty() {
            return Err(format!("the paper account has {} open order(s); the drill refuses to run against open orders", snap.open_orders.len()));
        }
        Ok(format!(
            "flat with no open orders; starting equity {}, cash {}",
            snap.equity, snap.cash
        ))
    }

    /// The one step that bypasses `run_once`: a single capped limit buy. Its cap is in the price, not in a check after
    /// the fact: quantity times limit is at most the notional cap by construction.
    fn open_position(&mut self) -> StepResult {
        let qty = Dec::parse(DRILL_QTY).map_err(|_| "DRILL_QTY does not parse".to_string())?;
        let limit = div_floor(self.max_notional, qty, 2)
            .map_err(|e| format!("cannot size the limit: {e:?}"))?;
        let stamp = self.clock.now().format("%Y%m%dT%H%M%SZ");
        let tag = format!("{OWN_TAG_PREFIX}killdrill:open:{stamp}");
        let req = OrderRequest::limit(&tag, DRILL_SYMBOL, Side::Buy, qty, limit);
        let id = match self.broker.place(&req).map_err(|e| format!("the opening order was refused before it was sent: {e}"))? {
            PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
            PlaceOutcome::UnknownOutcome { reason, .. } => {
                return Err(format!("the opening order's outcome is unknown ({reason}); it may exist under tag {tag}; check the account by hand before any re-run"))
            }
            PlaceOutcome::Rejected { errors, .. } => return Err(format!("the opening order was rejected: {errors:?}")),
            PlaceOutcome::ValidatedOnly { .. } => return Err("the opening order was validate-only; nothing was sent".to_string()),
        };

        let mut settled: Option<OrderReport> = None;
        for _ in 0..OPEN_FILL_POLLS {
            let r = self
                .broker
                .get_order(&id)
                .map_err(|e| format!("opening order lookup failed: {e}"))?;
            if r.status.is_terminal() {
                settled = Some(r);
                break;
            }
            self.clock.sleep_secs(1);
        }
        let Some(report) = settled else {
            let (_, last) = self.broker.cancel_and_settle(&id).map_err(|e| {
                format!("the opening order did not settle and the cancel failed: {e}")
            })?;
            return Err(format!("the opening order did not settle in {OPEN_FILL_POLLS}s; cancel requested (last status {:?})", last.status));
        };
        if !matches!(report.status, OrderStatus::Filled) || report.executed_quantity != qty {
            return Err(format!(
                "the opening order ended {:?} with {} executed of {}; a partial position may exist, check it by hand",
                report.status, report.executed_quantity, report.quantity
            ));
        }
        let snap = self.snapshot()?;
        let held = Self::spy_quantity(&snap);
        if held != qty {
            return Err(format!(
                "after the fill the account holds {held} {DRILL_SYMBOL}, expected {qty}"
            ));
        }
        let value = snap
            .holding(DRILL_SYMBOL)
            .map_or(Dec::ZERO, |h| h.market_value);
        if value > self.max_notional {
            return Err(format!(
                "the position is worth {value} at the broker mark, above the cap {}",
                self.max_notional
            ));
        }
        Ok(format!("bought {qty} {DRILL_SYMBOL} at a limit ceiling of {limit} (cap {}); order {id} filled; position value {value}", self.max_notional))
    }

    /// No breach. Sets the starting equity as the high-water mark for the scripted breaches that follow.
    fn baseline_run(&mut self) -> StepResult {
        let equity = self.snapshot()?.equity;
        self.breach.set_equity(Some(equity));
        self.base_equity = Some(equity);
        let rec = self.live_run();
        expect_outcome(&rec, OutcomeKind::Completed)?;
        let st = self.state()?;
        if st.status() != AccountStatus::Active {
            return Err(format!(
                "expected the account to be active, it is {}",
                st.status().as_str()
            ));
        }
        if st.hwm() != Some(equity) {
            return Err(format!(
                "the high-water mark is {:?}, expected the starting equity {equity}",
                st.hwm()
            ));
        }
        Ok(format!(
            "run completed ({}); status active; high-water mark {equity}",
            rec.outcome.code
        ))
    }

    /// Breach at the shrink band. The rung must shrink (scale 0.5), must not flatten, and must not move the position.
    fn shrink(&mut self) -> StepResult {
        let equity = self.breached_equity(SHRINK_BREACH)?;
        self.breach.set_equity(Some(equity));
        let before = Self::spy_quantity(&self.snapshot()?);
        let rec = self.live_run();
        expect_outcome(&rec, OutcomeKind::Completed)?;
        let risk = rec
            .risk
            .as_ref()
            .ok_or("the run recorded no risk decision")?;
        if !risk.has(RiskCode::DrawdownShrink) {
            return Err(format!(
                "expected RISK_DRAWDOWN_SHRINK, the run recorded {:?}",
                risk.codes()
            ));
        }
        if rec.flatten.is_some() {
            return Err("a shrink must not flatten, but the run flattened".to_string());
        }
        let st = self.state()?;
        if st.status() != AccountStatus::Shrunk {
            return Err(format!(
                "expected the account to be shrunk, it is {}",
                st.status().as_str()
            ));
        }
        let scale =
            Dec::parse(SHRINK_SCALE).map_err(|_| "SHRINK_SCALE does not parse".to_string())?;
        if st.risk_scale() != scale {
            return Err(format!(
                "expected a risk scale of {scale}, got {}",
                st.risk_scale()
            ));
        }
        let after = Self::spy_quantity(&self.snapshot()?);
        if after != before {
            return Err(format!(
                "the shrink changed the position from {before} to {after}"
            ));
        }
        Ok(format!("equity {equity} ({SHRINK_BREACH} below the mark): status shrunk at scale {SHRINK_SCALE}, no flatten, position unchanged at {after}"))
    }

    /// Breach at the halt band. The run must flatten, verify flat, halt, and finish within the bound. Verified against
    /// a fresh read of the real account, not the pipeline's own record.
    fn flatten_and_halt(&mut self) -> StepResult {
        let equity = self.breached_equity(HALT_BREACH)?;
        self.breach.set_equity(Some(equity));
        let started = self.clock.now();
        let rec = self.live_run();
        let elapsed = self.clock.now() - started;
        expect_outcome(&rec, OutcomeKind::Halted)?;
        if !HALT_REASONS.contains(&rec.outcome.code.as_str()) {
            return Err(format!(
                "the halt reason is {}, expected one of {HALT_REASONS:?}",
                rec.outcome.code
            ));
        }
        let flatten = rec.flatten.as_ref().ok_or("the halt recorded no flatten")?;
        if flatten.verdict != FlattenVerdict::Flat || !flatten.verified_flat {
            return Err(format!(
                "the flatten did not verify flat: {}",
                flatten.summary()
            ));
        }
        let st = self.state()?;
        if st.status() != AccountStatus::Halted {
            return Err(format!(
                "expected the account to be halted, it is {}",
                st.status().as_str()
            ));
        }
        let snap = self.snapshot()?;
        let residual = Self::spy_quantity(&snap);
        if !residual.is_zero() {
            return Err(format!(
                "after the flatten the account still holds {residual} {DRILL_SYMBOL}"
            ));
        }
        if !snap.open_orders.is_empty() {
            return Err(format!(
                "after the flatten {} open order(s) remain",
                snap.open_orders.len()
            ));
        }
        if elapsed > Duration::seconds(HALT_BOUND_SECS) {
            return Err(format!(
                "the halt took {}s, over the {HALT_BOUND_SECS}s bound",
                elapsed.num_seconds()
            ));
        }
        Ok(format!("equity {equity} ({HALT_BREACH} below the mark): {}; flattened and verified flat in {}s; status halted; {DRILL_SYMBOL} 0", rec.outcome.code, elapsed.num_seconds()))
    }

    /// The agent cannot resume: a later run is refused without touching the broker, and a forged halted-to-active save
    /// is refused by the store's own guard.
    fn resume_refused(&mut self) -> StepResult {
        let placed_before = self.breach.place_count();
        let rec = self.live_run();
        if rec.outcome.kind != OutcomeKind::Refused || rec.outcome.code != "RUN_ACCOUNT_HALTED" {
            return Err(format!(
                "a run after the halt ended {} ({}), expected refused RUN_ACCOUNT_HALTED",
                rec.outcome.kind.as_str(),
                rec.outcome.code
            ));
        }
        if !rec.placed.is_empty() || rec.flatten.is_some() {
            return Err("a run after the halt touched orders".to_string());
        }
        if self.breach.place_count() != placed_before {
            return Err("the broker received an order after the halt".to_string());
        }
        let forged = refuse_forged_resume(&self.states, &self.account)?;
        let st = self.state()?;
        if st.status() != AccountStatus::Halted || !st.resumes().is_empty() {
            return Err("the halt state changed while the resume was being refused".to_string());
        }
        Ok(format!(
            "a later run was refused ({}) with no order sent; {forged}",
            rec.outcome.code
        ))
    }

    /// Read-only: the real account is flat, has no open orders, and the state is halted.
    fn final_state(&self) -> StepResult {
        let snap = self.snapshot()?;
        let residual = Self::spy_quantity(&snap);
        if !residual.is_zero() {
            return Err(format!("the account holds {residual} {DRILL_SYMBOL}"));
        }
        if !snap.open_orders.is_empty() {
            return Err(format!("{} open order(s) remain", snap.open_orders.len()));
        }
        let st = self.state()?;
        if st.status() != AccountStatus::Halted {
            return Err(format!(
                "the account is {}, expected halted",
                st.status().as_str()
            ));
        }
        Ok(format!(
            "{DRILL_SYMBOL} position flat, no open orders, state halted (version {})",
            st.version()
        ))
    }

    /// The pipeline must have raised a critical `ALERT_HALT`, and the drill must actually deliver a message about it
    /// through the platform's sender. An attempt that fails is reported as a failure, never as a pass.
    fn alert_delivery(&self, summary: &str) -> StepResult {
        let raised = self
            .notifier
            .alerts()
            .iter()
            .any(|a| a.code.as_str() == "ALERT_HALT" && a.severity.as_str() == "critical");
        if !raised {
            return Err("the pipeline raised no critical ALERT_HALT for the halt".to_string());
        }
        let no_delivery = format!("no delivery attempted: set {ENV_ALERT_TO} and a sender (RESEND_API_KEY and ALERT_FROM_EMAIL) to verify delivery");
        let to = self.alert_to.ok_or_else(|| no_delivery.clone())?;
        let sender = self.sender.ok_or(no_delivery)?;
        let email = OutboundEmail {
            to: to.to_string(),
            subject: format!("[Mendl Labs] kill drill passed ({})", self.label),
            body: format!("{summary}\n"),
        };
        match sender.send(&email) {
            Ok(id) => Ok(format!(
                "ALERT_HALT raised (critical) and delivered via {} (provider id {id})",
                sender.name()
            )),
            Err(e) => Err(format!(
                "ALERT_HALT raised but delivery via {} failed: {e}",
                sender.name()
            )),
        }
    }
}

fn expect_outcome(rec: &RunRecord, kind: OutcomeKind) -> Result<(), String> {
    if rec.outcome.kind == kind {
        Ok(())
    } else {
        Err(format!(
            "the run ended {} ({}): {}",
            rec.outcome.kind.as_str(),
            rec.outcome.code,
            rec.outcome.message
        ))
    }
}

/// The next scheduled slot: the clock's now, strictly after the previous slot (the run key is the slot).
fn next_slot(clock: &dyn Clock, last: &mut Option<DateTime<Utc>>) -> DateTime<Utc> {
    let mut now = clock.now();
    while let Some(prev) = *last {
        if now > prev {
            break;
        }
        clock.sleep_secs(1);
        now = clock.now();
    }
    *last = Some(now);
    now
}

/// Asks the state store to save a halted account as active with no resume record. The store must refuse it: the only
/// legal way out of a halt appends a human resume record, and nothing in this drill can create one.
pub fn refuse_forged_resume(store: &dyn StateStore, account: &str) -> Result<String, String> {
    let current = store
        .load(account)
        .map_err(|e| format!("state load failed: {e}"))?
        .ok_or_else(|| "no account state to test against".to_string())?;
    let mut record = current.to_record();
    record.status = AccountStatus::Active;
    record.halt = None;
    record.risk_scale = Dec::from_i64(1);
    record.shrink_rung = None;
    let forged = AccountState::from_record(record)
        .map_err(|e| format!("could not build the forged state: {e}"))?;
    match store.save(current.version(), &forged) {
        Err(StoreError::IllegalTransition(why)) => Ok(format!("a halted-to-active save with no human resume was refused by the store ({why})")),
        Err(other) => Err(format!("the forged resume was refused with {} instead of STORE_ILLEGAL_TRANSITION", other.code())),
        Ok(_) => Err("the store accepted a halted-to-active save with no human resume: the resume guard is broken".to_string()),
    }
}

/// The drill's mandate: the paper pilot's shape, restricted to SPY on Alpaca, with the drill's ladder. It must pass the
/// same validator the production mandates pass.
fn drill_mandate() -> Result<MandateBody, String> {
    let v = json!({
        "basis": {
            "capital_source": "own",
            "jurisdiction": "US-GA",
            "acknowledgements": [
                {"doc": "own_capital_attestation", "doc_version": "2026-10", "at": "2026-10-06T00:00:00Z", "by": "kill-drill"},
                {"doc": "risk_disclosure", "doc_version": "2026-10", "at": "2026-10-06T00:00:00Z", "by": "kill-drill"}
            ]
        },
        "capital": {"allocated": {"amount": "5000.00", "ccy": "USD"}, "min_cash_reserve": 0.05},
        "universe": {
            "venues": ["alpaca"], "asset_classes": ["us_etf"], "instrument_allow": [DRILL_SYMBOL], "instrument_deny": [],
            "shorting": false, "derivatives": false, "leverage_max_gross": 1.0
        },
        "exposure": {
            "max_position": 0.25, "max_asset_class": {"us_etf": 1.0}, "max_gross": 1.0, "max_net": 1.0,
            "max_order_notional": {"amount": "1500.00", "ccy": "USD"}, "max_orders_per_day": 20, "max_turnover_per_day": 0.5
        },
        "loss": {
            "daily_loss_limit": 0.03,
            "drawdown_ladder": [
                {"at": 0.02, "action": "shrink", "scale": 0.5},
                {"at": 0.04, "action": "halt_flatten"}
            ],
            "resume_after_halt": "human_only"
        },
        "autonomy": {
            "level": "L2",
            "may": ["place_orders", "cancel_orders", "resize_within_limits", "rebalance", "halt", "flatten"],
            "needs_confirmation": ["add_strategy", "remove_strategy", "raise_target_risk"],
            "never": ["change_limits", "resume_after_halt", "change_credentials", "withdraw"]
        },
        "reporting": {"digest": "daily", "alert_channels": ["email"]}
    });
    let body: MandateBody =
        serde_json::from_value(v).map_err(|e| format!("the drill mandate did not parse: {e}"))?;
    let violations = mandate::validate(&body);
    if !violations.is_empty() {
        let why: Vec<String> = violations
            .iter()
            .map(|x| format!("{}: {}", x.field, x.message))
            .collect();
        return Err(format!("the drill mandate is invalid: {}", why.join("; ")));
    }
    Ok(body)
}

fn drill_envelope(now: DateTime<Utc>) -> MandateEnvelope {
    MandateEnvelope {
        version: 1,
        status: MandateStatus::Active,
        effective_from: now - Duration::days(1),
        review_by: now + Duration::days(30),
    }
}

// ---------------------------------------------------------------------------------------------------------------
// The injected breach: a broker wrapper that replaces the equity the pipeline reads. Positions, orders and the
// environment are the real broker's. `environment()` MUST be forwarded: the default is Unspecified, which the
// paper-only policy refuses.
// ---------------------------------------------------------------------------------------------------------------

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

struct BreachBroker<'a> {
    inner: &'a dyn Broker,
    equity: Mutex<Option<Dec>>,
    placed: Mutex<u32>,
}

impl<'a> BreachBroker<'a> {
    fn new(inner: &'a dyn Broker) -> Self {
        Self {
            inner,
            equity: Mutex::new(None),
            placed: Mutex::new(0),
        }
    }

    fn set_equity(&self, equity: Option<Dec>) {
        *lock(&self.equity) = equity;
    }

    fn place_count(&self) -> u32 {
        *lock(&self.placed)
    }
}

impl Broker for BreachBroker<'_> {
    fn venue(&self) -> &'static str {
        self.inner.venue()
    }

    fn environment(&self) -> VenueEnvironment {
        self.inner.environment()
    }

    fn snapshot(&self, now: DateTime<Utc>) -> Result<BrokerSnapshot, SnapshotError> {
        let mut s = self.inner.snapshot(now)?;
        if let Some(e) = *lock(&self.equity) {
            s.equity = e;
            s.derived_equity = e;
        }
        Ok(s)
    }

    fn place(&self, req: &OrderRequest) -> Result<PlaceOutcome, BrokerError> {
        *lock(&self.placed) += 1;
        self.inner.place(req)
    }

    fn get_order(&self, broker_order_id: &str) -> Result<OrderReport, BrokerError> {
        self.inner.get_order(broker_order_id)
    }

    fn open_orders(&self) -> Result<Vec<OrderReport>, BrokerError> {
        self.inner.open_orders()
    }

    fn find_by_tag(&self, tag: &str) -> Result<Vec<OrderReport>, BrokerError> {
        self.inner.find_by_tag(tag)
    }

    fn cancel_and_settle(
        &self,
        broker_order_id: &str,
    ) -> Result<(CancelOutcome, OrderReport), BrokerError> {
        self.inner.cancel_and_settle(broker_order_id)
    }

    fn quote(&self, symbol: &str) -> Result<Quote, BrokerError> {
        self.inner.quote(symbol)
    }
}

/// Sizing and valuation prices for the pipeline, from the broker's own marks. The drill runs no sleeves, so only held
/// positions are priced. Using the broker's marks avoids a market-data key the drill does not need.
struct MarksSource<'a> {
    broker: &'a dyn Broker,
}

impl DataSource for MarksSource<'_> {
    fn sleeve_data(&self, sleeve: &SleeveSpec, _as_of: NaiveDate) -> Result<SleeveData, DataError> {
        Err(DataError::new(
            "DATA_UNSUPPORTED",
            &format!("the kill drill runs no sleeves (asked for {})", sleeve.id),
        ))
    }

    fn prices(
        &self,
        symbols: &[String],
        now: DateTime<Utc>,
    ) -> Result<BTreeMap<String, PricePoint>, DataError> {
        let mut out = BTreeMap::new();
        if symbols.is_empty() {
            return Ok(out);
        }
        let snap = self
            .broker
            .snapshot(now)
            .map_err(|e| DataError::new("DATA_UNAVAILABLE", &e.to_string()))?;
        for sym in symbols {
            let holding = snap.holding(sym).ok_or_else(|| {
                DataError::new(
                    "DATA_UNAVAILABLE",
                    &format!("{sym}: not held, so no broker mark"),
                )
            })?;
            let price = holding.mark.ok_or_else(|| {
                DataError::new(
                    "DATA_UNAVAILABLE",
                    &format!("{sym}: the broker gave no mark"),
                )
            })?;
            out.insert(
                sym.clone(),
                PricePoint {
                    price,
                    as_of: snap.taken_at,
                },
            );
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ack_flag_is_required_even_for_a_dry_run() {
        assert_eq!(parse_args::<&str>(&[]), Err(Refusal::MissingAck));
        assert_eq!(parse_args(&["--dry-run"]), Err(Refusal::MissingAck));
        assert!(parse_args(&["--dry-run", ACK_FLAG]).is_ok());
    }

    #[test]
    fn unknown_arguments_are_refused() {
        assert!(matches!(
            parse_args(&[ACK_FLAG, "--live"]),
            Err(Refusal::UnknownArgument(_))
        ));
    }

    #[test]
    fn the_notional_can_only_go_down() {
        assert!(parse_max_notional("5").is_ok());
        assert!(parse_max_notional("10").is_ok());
        assert!(matches!(
            parse_max_notional("10.01"),
            Err(Refusal::BadNotional(_))
        ));
        assert!(matches!(
            parse_max_notional("0"),
            Err(Refusal::BadNotional(_))
        ));
        assert!(matches!(
            parse_max_notional("-1"),
            Err(Refusal::BadNotional(_))
        ));
        assert!(matches!(
            parse_max_notional("ten"),
            Err(Refusal::BadNotional(_))
        ));
    }

    #[test]
    fn only_the_paper_host_and_paper_environment_pass() {
        assert!(check_target(PAPER_BASE_URL, VenueEnvironment::Paper).is_ok());
        assert!(check_target("http://127.0.0.1:9", VenueEnvironment::Paper).is_ok());
        assert!(matches!(
            check_target("https://not-the-paper-host.example.invalid", VenueEnvironment::Paper),
            Err(Refusal::NotPaperUrl(_))
        ));
        assert!(matches!(
            check_target(PAPER_BASE_URL, VenueEnvironment::Live),
            Err(Refusal::NotPaperEnvironment(_))
        ));
        assert!(matches!(
            check_target(PAPER_BASE_URL, VenueEnvironment::Unspecified),
            Err(Refusal::NotPaperEnvironment(_))
        ));
    }

    #[test]
    fn the_drill_mandate_validates_and_carries_the_drill_ladder() {
        let m = drill_mandate().unwrap();
        assert_eq!(m.loss.drawdown_ladder.len(), 2);
        assert_eq!(m.universe.instrument_allow, vec![DRILL_SYMBOL.to_string()]);
        // The literals in the mandate JSON must be the constants the plan prints and the breaches are sized from.
        assert_eq!(
            m.loss.daily_loss_limit,
            DAILY_LOSS_LIMIT.parse::<f64>().unwrap()
        );
        assert_eq!(
            m.loss.drawdown_ladder[0].at,
            SHRINK_AT.parse::<f64>().unwrap()
        );
        assert_eq!(
            m.loss.drawdown_ladder[1].at,
            HALT_AT.parse::<f64>().unwrap()
        );
    }

    #[test]
    fn scripted_breaches_land_in_the_intended_bands() {
        let shrink: f64 = SHRINK_BREACH.parse().unwrap();
        let halt: f64 = HALT_BREACH.parse().unwrap();
        let shrink_at: f64 = SHRINK_AT.parse().unwrap();
        let halt_at: f64 = HALT_AT.parse().unwrap();
        let daily: f64 = DAILY_LOSS_LIMIT.parse().unwrap();
        assert!(
            shrink >= shrink_at && shrink < halt_at,
            "the shrink breach must sit between the rungs"
        );
        assert!(halt >= halt_at, "the halt breach must reach the halt rung");
        assert!(
            shrink < daily,
            "the shrink breach must stay under the daily-loss limit or the daily halt fires first"
        );
    }

    #[test]
    fn the_plan_names_every_step_and_the_exit_codes() {
        let plan = render_plan(&Args {
            dry_run: true,
            max_notional: Dec::from_i64(10),
        });
        for s in STEPS {
            assert!(plan.contains(s), "plan is missing {s}");
        }
        assert!(plan.contains("exit codes"));
    }
}
