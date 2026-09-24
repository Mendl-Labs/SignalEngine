//! Documented-strategy decision rules for the Mendl Labs rebalancer: ETF trend, crypto trend and FX momentum.
//!
//! Pure functions: no I/O, no network, no async, no clock (the run date is an explicit argument).
//! Ported from the reference tool `stage1-record/tool/ticket.py` (`decide_s1`, `decide_s2`, `decide_s3`); the
//! reference's OUTPUT files are the golden data in `tests/`.
//!
//! # Rules
//! * **ETF trend** (`decide_etf_trend`): at a month-end, 20% of the sleeve in each of SPY/EFA/IEF/DBC/VNQ whose
//!   month-end close is strictly above the simple average of its last 10 month-end closes including the current
//!   one, else cash.
//! * **Crypto trend** (`decide_crypto_trend`): each day, 50% of the sleeve in each of BTC/ETH whose close is
//!   strictly above the simple average of its last 100 daily closes including today's, else cash. Long only.
//! * **FX momentum** (`decide_fx_tsmom`): at a month-end, for each of EURUSD/GBPUSD/USDJPY/AUDUSD/USDCAD/USDCHF/
//!   NZDUSD, the sign of the return over the last 12 month-ends (13 month-ends needed), sized `sign / sigma` with
//!   `sigma` the 60-day sample std of daily returns times `sqrt(ppy)`, the whole sleeve scaled to 10% annual
//!   volatility (`fx_joint_vol_scale`), each weight capped at +-3 after scaling. Weights are SIGNED fractions of
//!   the sleeve (short = negative, gross well above 1). See `fx.rs` for the formula and the `ppy` window quirk.
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
//! 9. *Weights* are fractions of the sleeve, not of the account. ETF and crypto weights are in [0, 0.5]; FX weights
//!    are signed, in [-3, 3], and their sum of magnitudes is a leverage multiple (about 2-4x on the ladder history).
//! 10. *FX momentum: the `ppy` window* (reference quirk, reproduced). The reference computes observations-per-year
//!     over EVERY joint-calendar row it is given up to the decision date, so `sigma`, the joint scale and every
//!     weight depend on how much history was supplied (backtest: 11 years; live ticket: `asof - 520 days`).
//!     `history_start` is therefore a required argument of `decide_fx_tsmom`, echoed in the result together with
//!     `first_joint_date`, `joint_bars`, `dropped_bars` and `ppy`; there is no default. `ppy` also counts
//!     weekend-dated bars, so it is about 310 on the ladder data, not 260.
//! 11. *FX momentum: joint calendar.* The seven pairs' bars are inner-joined on date inside the window (the
//!     reference's `dropna`), because vendor FX calendars differ pair by pair (Sunday/Saturday-dated bars, single
//!     missing days). Returns are taken between consecutive JOINT bars, so a day missing for one pair widens the
//!     return interval for all seven. Dropped dates are counted and reported (`dropped_bars`); the gap policy is
//!     applied to the joint calendar over the window that fixes sign and sigma (the oldest of the 13 month-ends,
//!     or the 61 bars behind the 60 returns, whichever is older, to the decision date). The reference does not
//!     check gaps at all; the ladder history has a 23-day joint hole in Sept-Oct 2019, so decisions whose window
//!     contains it are refused under `Options::fx_replay` and only replayable with `GapPolicy::Unchecked`.
//!     Month-end status is judged per pair on its own series (as for ETFs), not on the joint calendar; the
//!     reference accepts ANY date as a month-end (its check is vacuous), this port does not.
//! 12. *FX momentum: sign* uses the reference's own arithmetic, `now / then - 1.0` compared with zero, rather than
//!     `now > then`. For positive finite prices the two are identical (the correctly rounded ratio of two distinct
//!     doubles is never exactly 1: adjacent doubles differ by more than half a unit in the last place of 1); the
//!     reference form is kept for fidelity and tested at that boundary. `sign 0` (equal closes) gives weight 0; if
//!     every sign is 0 the sleeve has no volatility to scale and the rule refuses (`DegenerateSleeveVolatility`)
//!     where the reference would emit NaN.
//! 13. *FX momentum: minimum history.* 13 joint month-ends AND 70 joint bars (the reference's `len(c) < 70`),
//!     not just the 61 bars that 60 returns need. Zero or non-finite 60-day volatility of any pair refuses
//!     (`ZeroVolatility`; the reference would emit inf/NaN).
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
mod fx;

pub use crypto::{
    decide_crypto_trend, CRYPTO_SMA_DAYS, CRYPTO_SYMBOLS, CRYPTO_WEIGHT_PER_INSTRUMENT,
};
pub use decision::{
    CryptoDecision, EtfDecision, FxInstrumentDecision, FxTsmomDecision, InstrumentDecision, Signal,
};
pub use error::RuleError;
pub use etf::{decide_etf_trend, ETF_SMA_MONTH_ENDS, ETF_SYMBOLS, ETF_WEIGHT_PER_INSTRUMENT};
pub use fingerprint::data_fingerprint;
pub use fx::{
    decide_fx_tsmom, fx_history_start, fx_joint_vol_scale, fx_periods_per_year, FX_JOINT_CALENDAR,
    FX_MIN_JOINT_BARS, FX_MOMENTUM_MONTH_ENDS, FX_REFERENCE_LOOKBACK_DAYS, FX_SLEEVE_VOL_TARGET,
    FX_SYMBOLS, FX_VOL_WINDOW, FX_WEIGHT_CAP,
};
pub use months::{completed_month_end_dates, is_calendar_month_end, latest_decision_date, month_end_dates};
pub use options::{GapPolicy, MonthEndMode, Options};
pub use series::{Panel, PriceSeries};
