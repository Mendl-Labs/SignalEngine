//! Result types.

use chrono::NaiveDate;

/// Long-only signal: hold the instrument or hold cash in its place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Long,
    Cash,
}

impl Signal {
    /// 1 for `Long`, 0 for `Cash` (the reference tool's `signal` field).
    pub fn as_int(self) -> u8 {
        match self {
            Signal::Long => 1,
            Signal::Cash => 0,
        }
    }
}

/// Decision for one instrument.
#[derive(Debug, Clone, PartialEq)]
pub struct InstrumentDecision {
    pub symbol: String,
    /// The close that was compared (month-end close for the ETF rule, the decision-day close for crypto).
    pub close: f64,
    /// Simple average of the window INCLUDING `close`: exact mean rounded once to f64. The signal itself is
    /// decided in exact arithmetic (see `exact`), not by comparing these two f64 values.
    pub sma: f64,
    pub signal: Signal,
    /// Fraction of the SLEEVE's equity: 0.20 (ETF) or 0.50 (crypto) when `Long`, otherwise 0.
    pub weight: f64,
}

/// ETF trend decision at a month-end. `instruments` follow `ETF_SYMBOLS` order.
#[derive(Debug, Clone, PartialEq)]
pub struct EtfDecision {
    pub decision_date: NaiveDate,
    /// The ten month-end bars (oldest first, last = `decision_date`) the averages were taken over.
    pub month_end_dates: Vec<NaiveDate>,
    pub instruments: Vec<InstrumentDecision>,
}

/// Crypto trend decision on a day. `instruments` follow `CRYPTO_SYMBOLS` order.
#[derive(Debug, Clone, PartialEq)]
pub struct CryptoDecision {
    pub decision_date: NaiveDate,
    /// First bar of the 100-bar window per instrument (in the same order as `instruments`).
    pub window_start: Vec<NaiveDate>,
    pub instruments: Vec<InstrumentDecision>,
}

macro_rules! sleeve_helpers {
    ($t:ty) => {
        impl $t {
            pub fn get(&self, symbol: &str) -> Option<&InstrumentDecision> {
                self.instruments.iter().find(|i| i.symbol == symbol)
            }
            /// Sum of target weights (fraction of the sleeve invested).
            pub fn invested_weight(&self) -> f64 {
                self.instruments.iter().map(|i| i.weight).sum()
            }
            /// Fraction of the sleeve left in cash.
            pub fn cash_weight(&self) -> f64 {
                1.0 - self.invested_weight()
            }
        }
    };
}
sleeve_helpers!(EtfDecision);
sleeve_helpers!(CryptoDecision);

/// Decision for one FX pair in the time-series-momentum sleeve.
#[derive(Debug, Clone, PartialEq)]
pub struct FxInstrumentDecision {
    pub symbol: String,
    /// The decision-date close (informational).
    pub close: f64,
    /// `sign(month-end close now / month-end close 12 month-ends earlier - 1)`: +1, -1 or 0 (0 => weight 0).
    pub sign: i8,
    /// 60-day sample standard deviation of daily returns times `sqrt(ppy)` (the reference's `sigma`).
    pub sigma: f64,
    /// SIGNED fraction of the SLEEVE's equity: negative = short, magnitude may exceed 1 (leverage), capped at
    /// `FX_WEIGHT_CAP` in absolute value.
    pub weight: f64,
    /// True when the cap was applied (`|vol_scale * sign / sigma| > FX_WEIGHT_CAP`).
    pub clipped: bool,
}

/// FX time-series-momentum decision at a month-end. `instruments` follow `FX_SYMBOLS` order.
#[derive(Debug, Clone, PartialEq)]
pub struct FxTsmomDecision {
    pub decision_date: NaiveDate,
    /// The explicit history window start the caller passed: bars dated before it were ignored. It determines
    /// `ppy` and therefore every weight (see the crate docs, "FX momentum: the ppy window").
    pub history_start: NaiveDate,
    /// First date of the joint calendar (all seven pairs have a bar) inside the window.
    pub first_joint_date: NaiveDate,
    /// Number of joint-calendar bars from `first_joint_date` to `decision_date` (the reference's `len(c)`).
    pub joint_bars: usize,
    /// Dates inside the window on which at least one pair, but not all seven, had a bar: silently dropped by the
    /// reference's joint `dropna`, dropped here too but REPORTED so the caller can apply its own limit.
    pub dropped_bars: usize,
    /// Observations per year: `(joint_bars - 1) / ((decision_date - first_joint_date).days / 365.25)`.
    pub ppy: f64,
    /// Joint volatility scale `k = 0.10 / (sleeve daily-return std * sqrt(ppy))`, before the cap.
    pub vol_scale: f64,
    /// The 13 month-ends of the joint calendar (oldest first, last = `decision_date`).
    pub month_end_dates: Vec<NaiveDate>,
    pub instruments: Vec<FxInstrumentDecision>,
}

impl FxTsmomDecision {
    pub fn get(&self, symbol: &str) -> Option<&FxInstrumentDecision> {
        self.instruments.iter().find(|i| i.symbol == symbol)
    }
    /// Sum of |weight| (gross exposure as a multiple of sleeve equity).
    pub fn gross_weight(&self) -> f64 {
        self.instruments.iter().map(|i| i.weight.abs()).sum()
    }
    /// Sum of signed weights.
    pub fn net_weight(&self) -> f64 {
        self.instruments.iter().map(|i| i.weight).sum()
    }
}
