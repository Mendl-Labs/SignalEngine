//! Documented-strategy decision rules for the Mendl Labs rebalancer: ETF trend and crypto trend.
//!
//! Pure functions: no I/O, no network, no async, no clock (the run date is an explicit argument).
//! Ported from the reference tool `stage1-record/tool/ticket.py` (`decide_s1`, `decide_s3`); the reference's
//! OUTPUT files are the golden data in `tests/`.
//!
//! # Rules
//! * **ETF trend** (`decide_etf_trend`): at a month-end, 20% of the sleeve in each of SPY/EFA/IEF/DBC/VNQ whose
//!   month-end close is strictly above the simple average of its last 10 month-end closes including the current
//!   one, else cash.
//! * **Crypto trend** (`decide_crypto_trend`): each day, 50% of the sleeve in each of BTC/ETH whose close is
//!   strictly above the simple average of its last 100 daily closes including today's, else cash. Long only.
//!
//! # Interpretation choices a reviewer must confirm
//! 1. *Strictly above*: `close == average` is NOT above. Compared in exact integer arithmetic (`exact`), so
//!    binary rounding cannot turn a tie into a signal.
//! 2. *SMA includes the current bar* (the documented reading, SCORECARD amendment "R1"), for both rules.
//! 3. *Month-end* = the last bar of a calendar month in the data. No holiday table: a month is known to be over
//!    only when a later-month bar exists (`MonthEndMode::NextMonthBar`, live default), or the caller passes a
//!    decision date it asserts to be the final session (`MonthEndMode::Explicit`). In both modes a later bar in
//!    the same month refuses (`NotMonthEnd`). The reference tool's weekday heuristic for a "provisional"
//!    month-end is deliberately not ported, and the reference `decide_s1` accepts ANY date as month-end (its
//!    check is vacuous); this port does not.
//! 4. *Truncation*: only bars dated on or before the decision date are used; later bars are ignored.
//! 5. *Instruments are validated independently* (no joint dropna). The ETF month-end dates must agree across
//!    the five ETFs (`MonthEndMismatch`).
//! 6. *Gaps*: ETF: at most `ETF_MAX_MISSING_WEEKDAYS` (3) weekdays between consecutive bars from the first
//!    month-end used to the decision date. Crypto: no missing calendar day inside the 100-bar window
//!    (`CRYPTO_MAX_MISSING_DAYS` = 0). The reference tool does not check gaps (it counts rows).
//! 7. *Staleness / forming bars* (only when `Options::as_of` is set): a bar dated on or after `as_of` is refused
//!    (`FormingBar`); the newest bar may be at most 5 days (ETF) / 1 day (crypto) older than `as_of`.
//! 8. *Prices* are closes as delivered (price return, not total return); this crate does not adjust anything.
//! 9. *Weights* are fractions of the sleeve, not of the account.
//!
//! # Residual risk (cannot be fixed without an exchange calendar or an external completeness check)
//! A single missing ordinary session (for example the true last session of a month absent from the vendor data)
//! is indistinguishable from a holiday. The cross-ETF month-end agreement check and the next-month-bar rule
//! catch most cases; the data gate (WP2.3) must still confirm the expected last session.

pub mod decision;
pub mod error;
pub mod fingerprint;
pub mod months;
pub mod options;
pub mod series;

mod checks;
mod crypto;
mod etf;
mod exact;

pub use crypto::{
    decide_crypto_trend, CRYPTO_SMA_DAYS, CRYPTO_SYMBOLS, CRYPTO_WEIGHT_PER_INSTRUMENT,
};
pub use decision::{CryptoDecision, EtfDecision, InstrumentDecision, Signal};
pub use error::RuleError;
pub use etf::{decide_etf_trend, ETF_SMA_MONTH_ENDS, ETF_SYMBOLS, ETF_WEIGHT_PER_INSTRUMENT};
pub use fingerprint::data_fingerprint;
pub use months::{completed_month_end_dates, is_calendar_month_end, latest_decision_date, month_end_dates};
pub use options::{GapPolicy, MonthEndMode, Options};
pub use series::{Panel, PriceSeries};
