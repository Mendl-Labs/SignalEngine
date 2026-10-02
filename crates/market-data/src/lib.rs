//! Production market data for the rebalancer: [`MassiveDataSource`], a `rebalancer_run::data::DataSource` over the
//! Massive (formerly Polygon.io) daily-aggregates REST API.
//!
//! * HTTP goes through `broker_adapters::transport::HttpTransport` (the abstraction the broker adapters use; its
//!   optional reqwest implementation is behind this crate's `live` feature). There is no other HTTP stack here.
//! * The key is read once from `MASSIVE_API_KEY` ([`EnvKeyProvider`]) and sent ONLY as `Authorization: Bearer`
//!   ([`secret`]): never in a URL, an error, a provenance record or a `Debug` rendering.
//! * Only COMPLETE bars are returned (rule below); everything the vendor sends is validated; every failure is a typed
//!   [`MassiveError`] ([`error`]: the taxonomy and its failure classes).
//! * Transient failures (429, 5xx, timeouts) are retried with bounded, jittered exponential backoff on an injected
//!   [`MarketClock`] (tests never sleep), inside a per-tick request budget; 403 and malformed responses never are.
//! * Each fetch leaves a [`Provenance`] per instrument.
//!
//! # The completeness rule (details and conventions in [`time`])
//! A bar dated `D` is returned only if `D < as_of` (the run date, for both asset classes) AND the source's own clock
//! says `D`'s period is over (crypto: 00:00 UTC of `D + 1`; stocks: 16:00 New York on `D` plus the plan's 15-minute
//! delay), plus an optional settle margin. Every other bar is dropped and listed in
//! `Provenance::dropped_incomplete`. A missing or stale bar is an error, never a substitution.
//!
//! # Time zones
//! Stocks (range endpoint): `t` must be exactly midnight America/New_York (04:00 UTC in EDT, 05:00 UTC in EST, by the US
//! daylight-saving rule) and the bar's date is that local date. Crypto: `t` must be exactly 00:00:00.000 UTC. A stamp
//! of any other instant is refused as malformed (a changed vendor convention must fail loudly, not shift bars by a day).
//!
//! # Caches
//! This source keeps no response cache of its own (the per-tick memo is `rebalancer_run::decision::EvalCache`, keyed by
//! `(kind, venue, asset_class, quote, as_of)`, which this source's panel never varies within). Toward intermediaries
//! every request carries `Cache-Control: no-cache, no-store` and `Pragma: no-cache`, no query-string nonce is added
//! (an unknown parameter could change the vendor's semantics and the recorded request path), and a response with an
//! `Age` header above 60 seconds is treated as cache-served and retried. VERIFIED (from the recorded response headers,
//! MASSIVE_API_ENTITLEMENTS section 1.8): the vendor sends `content-type`, `content-length`, `date`, `x-request-id`,
//! `x-polygon-cluster-name`, `strict-transport-security` and `vary`; no `cache-control`, `etag`, `age` or
//! `last-modified`. NOT VERIFIED: whether an edge cache in front of the vendor honours the request headers; that needs
//! a live probe of a bar that changes (the in-progress crypto day). Independent of caches, a stale answer cannot be
//! served as fresh: the newest complete bar must be the one the run date requires, else `StaleData`.
//!
//! # The decorator seam (two-source gate, COUNCIL_DATA_GATE R18-R20; NOT built here)
//! [`SleeveFetcher`] is the typed seam: `fetch_sleeve(sleeve, as_of) -> Result<FetchedSleeve, SleeveError>`, where
//! `FetchedSleeve` = the panel + one [`Provenance`] per instrument (source id, request paths without keys, fetch time,
//! first/last bar, last close, bar count, `data_fingerprint`-compatible fingerprint, raw-response hashes, request ids) and
//! `SleeveError` = the failure class + the sleeve kind + the run date. A gate implements `DataSource` itself over two
//! `SleeveFetcher`s (Massive as primary, an Alpaca/Kraken/OANDA reader as secondary), compares closes with both sets
//! of provenance in hand, and keeps the failure classes distinguishable (`ErrorKind`, `FailureClass`: Transient,
//! Settling, Deterministic, R19) instead of collapsing them into a `DataError` string. [`SleevesFrom`] adapts any
//! fetcher to a `DataSource`, and [`WithPrices`] takes sizing prices from a different source.
//!
//! # The two-source gate, SHADOW mode (W9.2; built here, `gate`)
//! [`TwoSourceGate`] is that decorator: Massive primary, [`AlpacaBarsSource`] (ETF) / [`KrakenOhlcSource`] (crypto)
//! secondaries, the pure comparison in `gate::compare` under the R19 tolerances (`gate::policy`, versioned and
//! hashed). In shadow mode the verdict is recorded on the fetched sleeve (`FetchedSleeve::gate`, through to the run
//! record) and the primary panel is ALWAYS returned; `DATA_GATE_MODE=enforce` is rejected at startup.
//!
//! Deliberately NOT here: enforce mode, L2 decision-state agreement (KNIFE_EDGE_HOLD), the correction ledger, the
//! retry WINDOW across ticks (this source retries within one fetch only), an OANDA reader / FX, any exchange calendar
//! (no holiday table, no early-close table), and sizing prices (`prices` refuses; see [`WithPrices`]).

#![forbid(unsafe_code)]

pub mod aggs;
pub mod alpaca_bars;
pub mod error;
pub mod gate;
pub mod kraken_ohlc;
pub mod runtime;
pub mod secret;
pub mod source;
pub mod testing;
pub mod time;
pub mod url;

pub use alpaca_bars::AlpacaBarsSource;
pub use error::{ErrorKind, FailureClass, MassiveError, SleeveError};
pub use gate::{DataGateMode, Policy, TwoSourceGate, ENV_DATA_GATE_MODE, POLICY_VERSION};
pub use kraken_ohlc::KrakenOhlcSource;
pub use runtime::{BudgetConfig, Jitter, MarketClock, RetryPolicy, SystemClock, SystemJitter};
pub use secret::{EnvKeyProvider, KeyError, KeyProvider, SecretString, StaticKeyProvider};
pub use source::{
    Completeness, ConfigError, DailyBars, FetchedSleeve, MassiveConfig, MassiveDataSource, Provenance, SleeveFetcher, SleevesFrom, WithPrices, CRYPTO_HISTORY_DAYS, DEFAULT_BASE_URL,
    ETF_HISTORY_DAYS, MAX_RESPONSE_AGE_SECS, SOURCE_ID,
};
