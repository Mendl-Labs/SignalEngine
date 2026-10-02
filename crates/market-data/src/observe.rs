//! `MassiveDataSource` as the vendor side of the first-seen-latency / revision recorder
//! (`rebalancer_run::latency::RecentBarsSource`, W9.1 / COUNCIL_DATA_GATE R25).
//!
//! The recorder asks, on every driver tick, for the newest bars of each instrument AS THE VENDOR SHOWS THEM NOW. This
//! impl maps a recorder [`Instrument`] to the vendor ticker and timestamp convention the sleeve fetch uses for the
//! same instrument (ETF: the symbol itself on the New York midnight clock; crypto: `X:{symbol}{quote}` on the UTC
//! midnight clock), asks for the last [`policy::LOOKBACK_CALENDAR_DAYS`] calendar days up to and including today,
//! and hands back every bar with its nominal close ([`crate::time::nominal_close_at`]). Which of those bars are
//! observable is the recorder's pre-registered policy, not this crate's.
//!
//! Give the recorder its OWN `MassiveDataSource` (same key, same transport): the per-tick request budget is per
//! source instance, so an observer polling seven instruments every tick can never starve the decision fetch.

use chrono::{DateTime, Duration, Utc};
use rebalancer_run::data::SleeveKind;
use rebalancer_run::latency::{
    policy, BarValues, Instrument, RecentBars, RecentBarsSource, VendorBar,
};

use crate::source::{MassiveDataSource, SOURCE_ID};
use crate::time::BarClock;

/// The vendor ticker and clock of a recorder instrument: exactly what `fetch_sleeve` uses for the same symbol.
pub fn vendor_ticker(instrument: &Instrument) -> (String, BarClock) {
    match instrument.kind {
        SleeveKind::EtfTrend => (instrument.symbol.clone(), BarClock::StockMidnightNewYork),
        SleeveKind::CryptoTrend => (
            format!("X:{}{}", instrument.symbol, instrument.quote),
            BarClock::MidnightUtc,
        ),
    }
}

impl RecentBarsSource for MassiveDataSource {
    fn source_id(&self) -> &'static str {
        SOURCE_ID
    }

    fn recent_bars(
        &self,
        instrument: &Instrument,
        now: DateTime<Utc>,
    ) -> Result<RecentBars, String> {
        let (ticker, clock) = vendor_ticker(instrument);
        let to = now.date_naive();
        let from = to
            .checked_sub_signed(Duration::days(policy::LOOKBACK_CALENDAR_DAYS))
            .ok_or_else(|| "DATA_UNSUPPORTED: date out of range".to_string())?;
        let observed = self
            .observe_recent_bars(&ticker, clock, from, to)
            .map_err(|e| format!("{}: {e}", e.kind().code()))?;
        let bars = observed
            .bars
            .into_iter()
            .map(|b| VendorBar {
                bar_date: b.date,
                nominal_close_at: b.nominal_close_at,
                values: BarValues {
                    open: b.open,
                    high: b.high,
                    low: b.low,
                    close: b.close,
                    volume: b.volume,
                },
            })
            .collect();
        // One page in practice (ten calendar days); the first page's hash identifies the response.
        Ok(RecentBars {
            bars,
            response_sha256: observed.raw_sha256.first().cloned(),
        })
    }
}
