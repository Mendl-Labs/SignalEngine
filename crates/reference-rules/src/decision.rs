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
