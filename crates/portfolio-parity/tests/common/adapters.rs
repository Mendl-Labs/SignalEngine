//! The backtester side of the replay: rules, per-sleeve delay emulation and the construction adapter.
//!
//! # Why the rules are written here and not taken from `weightsim-rules`
//! The parity claim needs the rule code on both sides to be the SAME code. The pipeline calls `reference_rules` at the
//! rev SignalEngine pins; these adapters call the very same crate (the workspace dependency), so a difference in a
//! decision cannot come from the rule. They mirror `BacktestingCore/weightsim-rules/src/adapters.rs` (replay options:
//! `Options::etf_replay(MonthEndMode::Explicit)`, `Options::crypto_replay()`), with one change: the history handed to
//! the rule is a trailing WINDOW of bars (the rules only look at ten month-ends / 100 days), so a 30-year run is linear
//! and not quadratic.

use std::collections::BTreeMap;

use chrono::{Datelike, NaiveDate};
use portfolio_construct as pc;
use reference_rules::{
    decide_crypto_trend, decide_etf_trend, GapPolicy, MonthEndMode, Options, Panel as RrPanel, PriceSeries, RuleError,
    CRYPTO_SMA_DAYS, CRYPTO_SYMBOLS, ETF_SYMBOLS,
};
use weightsim as ws;
use weightsim::Construct as _;
use weightsim::{Date, DecisionSchedule, HistoryView, RebalancePolicy, RefusalKind, RuleRefusal, WeightRule};

pub const ETF_WINDOW_BARS: usize = 700;
pub const CRYPTO_WINDOW_BARS: usize = 130;

pub fn to_naive(d: Date) -> NaiveDate {
    NaiveDate::from_ymd_opt(d.year(), u32::from(d.month()), u32::from(d.day())).expect("a weightsim Date is valid")
}

pub fn from_naive(n: NaiveDate) -> Date {
    Date::new(n.year(), n.month() as u8, n.day() as u8).expect("a chrono date is valid")
}

fn map_rule_error(e: RuleError) -> RuleRefusal {
    let kind = match e {
        RuleError::InsufficientHistory { .. } => RefusalKind::Warmup,
        RuleError::DataGap { .. } | RuleError::StaleData { .. } => RefusalKind::Data,
        _ => RefusalKind::Other,
    };
    RuleRefusal::new(kind, "rule_error", e.to_string())
}

fn build_panel(symbols: &[&str], dates: &[Date], closes: &[&[f64]], window: usize) -> Result<RrPanel, RuleRefusal> {
    let lo = dates.len().saturating_sub(window);
    let naive: Vec<NaiveDate> = dates[lo..].iter().map(|d| to_naive(*d)).collect();
    let mut series = Vec::with_capacity(symbols.len());
    for (s, c) in symbols.iter().zip(closes) {
        series.push(PriceSeries::new(*s, naive.clone(), c[lo..].to_vec()).map_err(map_rule_error)?);
    }
    RrPanel::new(series).map_err(map_rule_error)
}

fn ordered(symbols: &[&str], got: impl Iterator<Item = (String, f64)>) -> Result<Vec<f64>, RuleRefusal> {
    let mut out = Vec::new();
    for (want, (sym, w)) in symbols.iter().zip(got) {
        if *want != sym {
            return Err(RuleRefusal::new(RefusalKind::Other, "instrument_order", format!("expected {want}, got {sym}")));
        }
        out.push(w);
    }
    Ok(out)
}

/// ETF decision for a history whose last date is the decision date.
pub fn etf_weights(dates: &[Date], closes: &[&[f64]]) -> Result<Vec<f64>, RuleRefusal> {
    let panel = build_panel(&ETF_SYMBOLS, dates, closes, ETF_WINDOW_BARS)?;
    let date = to_naive(*dates.last().expect("a decision needs a bar"));
    let d = decide_etf_trend(&panel, date, &Options::etf_replay(MonthEndMode::Explicit)).map_err(map_rule_error)?;
    ordered(&ETF_SYMBOLS, d.instruments.iter().map(|i| (i.symbol.clone(), i.weight)))
}

/// Crypto decision for a history whose last date is the decision date.
pub fn crypto_weights(dates: &[Date], closes: &[&[f64]]) -> Result<Vec<f64>, RuleRefusal> {
    let panel = build_panel(&CRYPTO_SYMBOLS, dates, closes, CRYPTO_WINDOW_BARS)?;
    let date = to_naive(*dates.last().expect("a decision needs a bar"));
    let opts = Options { gap_policy: GapPolicy::Unchecked, ..Options::crypto_replay() };
    let d = decide_crypto_trend(&panel, date, &opts).map_err(map_rule_error)?;
    ordered(&CRYPTO_SYMBOLS, d.instruments.iter().map(|i| (i.symbol.clone(), i.weight)))
}

fn columns<'a>(h: &HistoryView<'a>) -> Vec<&'a [f64]> {
    (0..h.n_assets()).map(|i| h.closes(i)).collect()
}

/// `etf_trend_faber` as a weightsim rule: decides at the last bar of each month (own calendar), units drift between.
#[derive(Clone, Copy, Debug, Default)]
pub struct EtfRule;

impl WeightRule for EtfRule {
    fn id(&self) -> &'static str {
        "etf_trend_faber"
    }
    fn impl_version(&self) -> String {
        "portfolio-parity adapter over reference-rules".into()
    }
    fn universe(&self) -> &[&'static str] {
        &ETF_SYMBOLS
    }
    fn decision_schedule(&self) -> DecisionSchedule {
        DecisionSchedule::LastBarOfMonth
    }
    fn rebalance_policy(&self) -> RebalancePolicy {
        RebalancePolicy::OnDecision
    }
    fn min_history_bars(&self) -> usize {
        1
    }
    fn target_weights(&self, h: &HistoryView<'_>) -> Result<Vec<f64>, RuleRefusal> {
        etf_weights(h.dates(), &columns(h))
    }
}

/// `crypto_trend_100d`: decides every bar, restored to the standing weights every bar.
#[derive(Clone, Copy, Debug, Default)]
pub struct CryptoRule;

impl WeightRule for CryptoRule {
    fn id(&self) -> &'static str {
        "crypto_trend_100d"
    }
    fn impl_version(&self) -> String {
        "portfolio-parity adapter over reference-rules".into()
    }
    fn universe(&self) -> &[&'static str] {
        &CRYPTO_SYMBOLS
    }
    fn decision_schedule(&self) -> DecisionSchedule {
        DecisionSchedule::Daily
    }
    fn rebalance_policy(&self) -> RebalancePolicy {
        RebalancePolicy::EveryBar
    }
    fn min_history_bars(&self) -> usize {
        CRYPTO_SMA_DAYS
    }
    fn target_weights(&self, h: &HistoryView<'_>) -> Result<Vec<f64>, RuleRefusal> {
        crypto_weights(h.dates(), &columns(h))
    }
}

/// CADENCE-ONLY stand-in with the ETF rule's schedule, policy, universe and history need but constant weights (a
/// 30-year cadence run needs the calendar, not the signal, and is much cheaper this way).
#[derive(Clone, Copy, Debug, Default)]
pub struct CadenceEtf;

impl WeightRule for CadenceEtf {
    fn id(&self) -> &'static str {
        "cadence_etf"
    }
    fn impl_version(&self) -> String {
        "cadence stand-in".into()
    }
    fn universe(&self) -> &[&'static str] {
        &ETF_SYMBOLS
    }
    fn decision_schedule(&self) -> DecisionSchedule {
        DecisionSchedule::LastBarOfMonth
    }
    fn rebalance_policy(&self) -> RebalancePolicy {
        RebalancePolicy::OnDecision
    }
    fn min_history_bars(&self) -> usize {
        1
    }
    fn target_weights(&self, h: &HistoryView<'_>) -> Result<Vec<f64>, RuleRefusal> {
        Ok(vec![0.2; h.n_assets()])
    }
}

/// CADENCE-ONLY stand-in with the crypto rule's schedule and policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct CadenceCrypto;

impl WeightRule for CadenceCrypto {
    fn id(&self) -> &'static str {
        "cadence_crypto"
    }
    fn impl_version(&self) -> String {
        "cadence stand-in".into()
    }
    fn universe(&self) -> &[&'static str] {
        &CRYPTO_SYMBOLS
    }
    fn decision_schedule(&self) -> DecisionSchedule {
        DecisionSchedule::Daily
    }
    fn rebalance_policy(&self) -> RebalancePolicy {
        RebalancePolicy::EveryBar
    }
    fn min_history_bars(&self) -> usize {
        CRYPTO_SMA_DAYS
    }
    fn target_weights(&self, h: &HistoryView<'_>) -> Result<Vec<f64>, RuleRefusal> {
        Ok(vec![0.5; h.n_assets()])
    }
}

/// EMULATION of a per-sleeve `execution_delay_bars = 1` (council Ruling 4) inside a book whose single native delay is
/// 0: on the bar AFTER a month-end (own calendar) the rule returns the decision it would have taken on the month-end
/// bar, computed on the history through that month-end; on every other bar it refuses (kept standing under
/// `OnRefusal::HoldPrevious`, which is what the test configures). With policy `OnDecision` the target becomes
/// effective, and is planned, exactly on the first bar of the new month: the fill bar of a native `d = 1`. It is NOT
/// the missing native feature (the ledger keeps that as a KNOWN GAP): it only shows that the gap is limited to it.
#[derive(Clone, Copy, Debug, Default)]
pub struct DelayedEtf;

impl WeightRule for DelayedEtf {
    fn id(&self) -> &'static str {
        "etf_trend_faber_delayed_by_one_own_bar_emulation"
    }
    fn impl_version(&self) -> String {
        "portfolio-parity emulation of a per-sleeve execution delay of one own bar".into()
    }
    fn universe(&self) -> &[&'static str] {
        &ETF_SYMBOLS
    }
    fn decision_schedule(&self) -> DecisionSchedule {
        DecisionSchedule::Daily
    }
    fn rebalance_policy(&self) -> RebalancePolicy {
        RebalancePolicy::OnDecision
    }
    fn min_history_bars(&self) -> usize {
        2
    }
    fn target_weights(&self, h: &HistoryView<'_>) -> Result<Vec<f64>, RuleRefusal> {
        let n = h.len();
        let dates = h.dates();
        if n >= 2 && !dates[n - 2].same_month(dates[n - 1]) {
            let cols: Vec<&[f64]> = (0..h.n_assets()).map(|i| &h.closes(i)[..n - 1]).collect();
            etf_weights(&dates[..n - 1], &cols)
        } else {
            Err(RuleRefusal::new(RefusalKind::Other, "not_a_delayed_decision_bar", "no month-end on the previous own bar"))
        }
    }
}

// -------------------------------------------------------------------------------------------------------------
// PcConstruct: portfolio-construct behind the weightsim `Construct` boundary
// -------------------------------------------------------------------------------------------------------------

/// `portfolio-construct::construct` (f64) as the book simulator's construction, configured like the live pipeline:
/// floor rounding of quantities by a lot table, the planner's trade filter, `min(equity, allocated)`, a cash budget
/// with the mandate's cash reserve and the planner's ESTIMATED fee (0.0026 by default; the fake broker charges
/// nothing), `target_dp = 8`, planner-faithful limits, and, when `live_two_phase`, the live executor's order of
/// operations: the sells first, then the cash is "re-read" and the buys are re-planned on the real cash
/// (`credit_sell_proceeds = false`), and only the buys of that second plan are executed.
pub struct PcConstruct {
    /// The pipeline's symbol of each book instrument (`SPY`, `BTC/USD`).
    pub symbols: Vec<String>,
    pub classes: Vec<String>,
    pub rounder: pc::LotRounder,
    pub reserve_fraction: f64,
    pub sizing_fee_rate: f64,
    pub live_two_phase: bool,
    pub limits: pc::Limits,
}

impl PcConstruct {
    /// The book of the replay: instruments in book order = the ETFs, then the crypto pairs (only those present).
    pub fn for_instruments(symbols: &[String], reserve_fraction: f64, sizing_fee_rate: f64) -> PcConstruct {
        let mut rounder = pc::LotRounder::new();
        let mut classes = Vec::new();
        for s in symbols {
            if super::world::World::is_etf(s) {
                rounder = rounder.with(s, pc::LotRule::new(0));
                classes.push("us_etf".to_string());
            } else {
                rounder = rounder.with(s, pc::LotRule::new(8));
                classes.push("crypto_spot".to_string());
            }
        }
        PcConstruct {
            symbols: symbols.to_vec(),
            classes,
            rounder,
            reserve_fraction,
            sizing_fee_rate,
            live_two_phase: true,
            limits: pc::Limits::long_only_unit().with_policy(pc::LimitPolicy::PlannerFaithful),
        }
    }
}

impl ws::Construct for PcConstruct {
    fn construct(&self, i: &ws::ConstructInputs<'_>) -> Result<ws::ConstructOutput, ws::ConstructRefusal> {
        let n = i.marks.len();
        let sleeves: Vec<pc::SleeveTargets> = i
            .sleeves
            .iter()
            .enumerate()
            .map(|(k, s)| {
                pc::SleeveTargets::long_only(
                    &format!("s{k:02}"),
                    s.share,
                    s.instruments.iter().copied().zip(s.weights.iter().copied()).collect(),
                )
            })
            .collect();
        let filter = match i.policy.trade_filter {
            Some(f) => pc::TradeFilter::new(f.min_abs, f.min_pct),
            None => pc::TradeFilter::NONE,
        };
        let risk = pc::RiskScale::new(1.0, i.policy.risk_scale);
        let run = |units: &[f64], cash: f64, credit: bool| -> pc::ConstructOutput {
            let facts: Vec<pc::InstrumentFacts> = (0..n)
                .map(|j| {
                    pc::InstrumentFacts::new(&self.symbols[j], "alpaca", &self.classes[j], i.marks[j])
                        .with_held(units[j])
                        .with_in_scope(i.planned[j])
                })
                .collect();
            let inputs = pc::ConstructInputs {
                equity: i.equity,
                allocated_capital: i.policy.allocated_capital,
                sleeves: &sleeves,
                risk_scale: risk,
                limits: &self.limits,
                instruments: &facts,
                margin: &pc::NoMargin,
                trade_filter: filter,
                rounding: Some(&self.rounder),
                funding: pc::Funding::Cash {
                    cash,
                    reserve_fraction: self.reserve_fraction,
                    fee_rate: self.sizing_fee_rate,
                    credit_sell_proceeds: credit,
                },
                target_dp: Some(pc::TARGET_DP),
                unmanaged_gross: 0.0,
            };
            pc::construct(&inputs).unwrap_or_else(|e| panic!("portfolio-construct refused the replay book: {e}"))
        };
        // Applying a trade to f64 holdings. A SELL is clamped to what is held: `portfolio_construct`'s lot rounding snaps a
        // value at most 4 ulps below a quantum UP onto it, so a full exit of float-accumulated units can be sized ONE ULP
        // ABOVE the holding; unclamped it would leave a -9e-16 "short" that a long-only book then never touches again
        // (`ShortPositionHeld`). The live planner cannot do this (exact decimals, `min(wish, held)`). Pinned by
        // `t4_f64_full_exit_can_be_sized_above_the_holding_pinned`.
        let apply = |units: &mut [f64], cash: &mut f64, t: &pc::TradeIntent| {
            let j = t.instrument;
            match t.side {
                pc::Side::Buy => {
                    units[j] += t.quantity;
                    *cash -= t.notional;
                }
                pc::Side::Sell => {
                    let q = if t.reducing { t.quantity.min(units[j].max(0.0)) } else { t.quantity };
                    units[j] -= q;
                    *cash += q * t.price;
                }
            }
            if units[j].abs() < 1e-9 {
                units[j] = 0.0;
            }
        };
        let mut units = i.units.to_vec();
        let mut cash = i.cash;
        let first = run(&units, cash, true);
        if self.live_two_phase {
            for t in first.trades.iter().filter(|t| t.reducing) {
                apply(&mut units, &mut cash, t);
            }
            let second = run(&units, cash, false);
            for t in second.trades.iter().filter(|t| !t.reducing && t.side == pc::Side::Buy) {
                apply(&mut units, &mut cash, t);
            }
        } else {
            for t in &first.trades {
                apply(&mut units, &mut cash, t);
            }
        }
        let cb = first.capital_base;
        let target_weight: Vec<Option<f64>> =
            (0..n).map(|j| if first.named[j] { Some(first.target_notional[j] / cb) } else { None }).collect();
        let mut traded = vec![0.0; n];
        let mut traded_total = 0.0;
        for j in 0..n {
            traded[j] = (units[j] - i.units[j]).abs() * i.marks[j];
            traded_total += traded[j];
        }
        let skipped = first
            .skipped
            .iter()
            .filter_map(|s| match s.reason {
                pc::SkipReason::BelowMinAbs { .. } => Some(ws::construct::Skip { instrument: s.instrument, reason: ws::construct::SkipReason::BelowMinAbs }),
                pc::SkipReason::BelowMinPct { .. } => Some(ws::construct::Skip { instrument: s.instrument, reason: ws::construct::SkipReason::BelowMinPct }),
                _ => None,
            })
            .collect();
        Ok(ws::ConstructOutput { target_weight, capital_base: cb, units_new: units, traded, traded_total, skipped })
    }

    fn inverse_vol_shares(&self, gross_returns: &[&[f64]], lookback: usize, total: f64) -> Option<Vec<f64>> {
        ws::MinimalConstruct.inverse_vol_shares(gross_returns, lookback, total)
    }
}

/// A `BTreeMap` of the book's units by symbol at account bar `k` (for readable comparisons).
pub fn units_by_symbol(res: &ws::BookResult, k: usize) -> BTreeMap<String, f64> {
    let row = res.inst_row(&res.units, k);
    res.instruments.iter().cloned().zip(row.iter().copied()).collect()
}

/// The pipeline's symbol of a book instrument (`BTC` becomes `BTC/USD`).
pub fn se_symbol(instrument: &str) -> String {
    if CRYPTO_SYMBOLS.contains(&instrument) {
        format!("{instrument}/USD")
    } else {
        instrument.to_string()
    }
}

pub fn month_of(d: NaiveDate) -> (i32, u32) {
    (d.year(), d.month())
}
