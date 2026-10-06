//! `BitstampTickerSource`: current BTC/USD and ETH/USD prices from Bitstamp's PUBLIC ticker (`GET /api/v2/ticker/{m}/`,
//! no key, no withdrawal gate). A `DataSource` in the same shape as the other production sources.
//!
//! * Prices: the mid of the best bid and best ask, valued at the ticker's own timestamp. A symbol outside BTC/USD and
//!   ETH/USD is simply absent, as the `DataSource` contract requires. An outage or a malformed ticker is an `Err`.
//! * Sleeves: refused. The ticker carries the current price only, not history. Compose with a bar source via
//!   `rebalancer_run::data` / `market_data::WithPrices` when a panel is needed.
//! * Documented-shape fixtures only; nothing here has run against the live endpoint (see the PR).

use std::collections::BTreeMap;
use std::sync::Arc;

use broker_adapters::bitstamp::{fetch_ticker, order::market_symbol};
use broker_adapters::transport::HttpTransport;
use chrono::{DateTime, NaiveDate, Utc};
use rebalancer_core::guard::PricePoint;
use rebalancer_run::data::{DataError, DataSource, SleeveData, SleeveSpec};

pub struct BitstampTickerSource {
    transport: Arc<dyn HttpTransport>,
}

impl BitstampTickerSource {
    pub fn new(transport: Arc<dyn HttpTransport>) -> Self {
        Self { transport }
    }
}

impl DataSource for BitstampTickerSource {
    fn sleeve_data(&self, _sleeve: &SleeveSpec, _as_of: NaiveDate) -> Result<SleeveData, DataError> {
        Err(DataError::new(
            "BITSTAMP_NO_HISTORY",
            "the Bitstamp ticker serves current prices only, not daily bars; compose it with a bar source",
        ))
    }

    fn prices(&self, symbols: &[String], _now: DateTime<Utc>) -> Result<BTreeMap<String, PricePoint>, DataError> {
        let mut out = BTreeMap::new();
        for symbol in symbols {
            if market_symbol(symbol).is_err() {
                continue;
            }
            let quote = fetch_ticker(&*self.transport, symbol)
                .map_err(|e| DataError::new("BITSTAMP_UNAVAILABLE", &format!("{symbol}: {e}")))?;
            let price = quote
                .mid()
                .ok_or_else(|| DataError::new("BITSTAMP_MALFORMED", &format!("{symbol}: mid price overflows")))?;
            let as_of = DateTime::<Utc>::from_timestamp(quote.timestamp_secs, 0)
                .ok_or_else(|| DataError::new("BITSTAMP_MALFORMED", &format!("{symbol}: timestamp out of range")))?;
            out.insert(symbol.clone(), PricePoint { price, as_of });
        }
        Ok(out)
    }
}
