//! Real `AccountRuntime` construction for the paper pilot (slice S-6 of
//! `product-mandate/PAPER_PILOT_DRAGONSTONE_PLAN.md`).
//!
//! [`rebalancer_store::PgAccountSource`] (slice S-5) deliberately builds NO broker, reads NO secret and calls no
//! vendor -- it only decides WHICH account may run and hands back a [`rebalancer_store::PilotAccount`] (the mandate,
//! sleeves, mode, and the plan's pinned `credential_fingerprint`). This module is the other half: given that
//! descriptor plus credentials read from the process environment (never a secret file), it connects the real paper
//! Alpaca broker and the real Massive data source and produces the pieces `rebalancer_run::driver::AccountRuntime`
//! borrows.
//!
//! # The fingerprint check
//! `rebalancer_pilot_plans.credential_fingerprint` (databaseschema-internal
//! `2026-09-27-000000_create_rebalancer_pilot_ledger/up.sql`) is `VARCHAR(16) CHECK (credential_fingerprint ~
//! '^[0-9a-f]{16}$')`: the first 8 bytes of SHA-256 of the paper key id, hex. `AlpacaCredentials::key_id_fingerprint`
//! (`broker-adapters`, merged in #39) computes exactly that from the connected key. [`connect_paper_alpaca`] computes
//! it from the environment credentials BEFORE any network call and refuses (fail closed) if it disagrees with the
//! plan row's pinned value: a key swapped between the pilot's own paper account and any other paper account can
//! never silently start trading under a plan that was authored for a different key.
//!
//! # The `PA` account-number check
//! Already built into `broker_adapters::alpaca::PaperOnlyAlpaca::verify_paper_account` (#39): `GET /v2/account` plus
//! `require_paper_account_number`. The general `AlpacaAdapter::check_account` only refuses a LIVE adapter whose
//! account number looks like a paper one (`PA` prefix) -- it does NOT require a PAPER adapter's account number to
//! start with `PA`. [`connect_paper_alpaca`] calls `verify_paper_account`, not the weaker `verify_account`, so that
//! positive check runs on the pilot's startup path.
//!
//! # The sizing-price source (the plan's sketch does not name one)
//! `market_data::MassiveDataSource::prices` is deliberately unimplemented (its own module doc: "Deliberately NOT
//! here: ... sizing prices (`prices` refuses; see `WithPrices`)"): Massive stocks are daily bars, not a quote feed,
//! and Alpaca's own `get_quote` is `Unsupported` for the same reason (its market-data API is a separate integration,
//! out of scope here). [`LastClosePrices`] is the piece this slice adds: the sizing price of a symbol is its last
//! COMPLETE daily close, fetched with the crate's own `MassiveDataSource::fetch_daily_bars` (already public, built
//! for exactly this "one instrument, one window" use). It is composed with the sleeve-panel source through the
//! crate's existing decorator seam (`market_data::SleevesFrom` + `market_data::WithPrices`, from #41) rather than
//! adding a new one -- see [`pilot_data_source`]. Because a daily close can be up to a few calendar days old (a
//! Friday close used across a weekend, a holiday), [`crate::pilot::pilot_run_config`] raises
//! `RunConfig::max_price_age_secs` accordingly (see that function's own doc for the exact value and why).
//!
//! # Deviations from the plan's S-6 sketch
//! * The sketch spells out `AlpacaCredentials::new` / `AlpacaConfig::new` / `AlpacaAdapter::new` and the `PA` check
//!   as if they still needed to be written; by the time this slice landed, #39 had already built exactly that
//!   (`PaperOnlyAlpaca::new` and `verify_paper_account`) as the seam `crate::pilot::build_paper_alpaca` calls. This
//!   module therefore calls `build_paper_alpaca` once rather than duplicating adapter construction.
//! * `reqwest-transport` (the cargo feature) already existed in `broker-adapters` (added ahead of this slice, not by
//!   it): there was nothing to add there. This slice's own Cargo.toml change is turning the feature ON for
//!   `rebalancer-service` (see that crate's `Cargo.toml` and `tests/pilot_source_scan.rs`, which is updated in the
//!   same commit per its own comment: "the pilot builder slice (S-6) must do that on purpose and update this test").
//! * No new "sizing price" abstraction was added to `market-data`: [`LastClosePrices`] lives here (in
//!   `rebalancer-service`, this slice's own crate) and reuses `market-data`'s existing decorator seam as-is, per the
//!   COUNCIL_DATA_GATE instruction not to build the future two-source gate here but to leave the seam usable by it.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use broker_adapters::alpaca::{AssetTable, PaperOnlyAlpaca, PrepareOptions};
use broker_adapters::transport::HttpTransport;
use broker_adapters::BrokerError;
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use market_data::time::{bar_complete_at, BarClock};
use market_data::{AlpacaBarsSource, KrakenOhlcSource, MassiveDataSource, SleevesFrom, TwoSourceGate, WithPrices};
use rebalancer_core::guard::PricePoint;
use rebalancer_core::venue::AlpacaRules;
use rebalancer_core::Dec;
use rebalancer_run::broker::AlpacaBroker;
use rebalancer_run::data::{DataError, DataSource, SleeveData, SleeveKind, SleeveSpec};

use crate::pilot::{build_paper_alpaca, PilotRefusal};

/// The pilot's one ETF sleeve, exactly (plan section 1, `PAPER_PILOT_DRAGONSTONE_PLAN.md`).
pub const PILOT_ETF_SYMBOLS: [&str; 5] = ["SPY", "EFA", "IEF", "DBC", "VNQ"];

/// A sizing price is trusted for this many calendar days of lookback when asking Massive for the last complete
/// close (weekends plus a holiday plus one day of margin). Bars older than this inside the window are simply not
/// returned by `fetch_daily_bars`'s own completeness rule; this is only how far back the request itself reaches.
pub const PRICE_LOOKBACK_DAYS: i64 = 10;

/// Why the pilot's runtime could not be built. Every variant fails closed: no partial runtime is ever returned.
#[derive(Debug)]
pub enum RuntimeError {
    /// `build_paper_alpaca` refused before any network call (bad key prefix, bad base URL, ...).
    Connect(PilotRefusal),
    /// The connected key's fingerprint does not match the pilot plan row's `credential_fingerprint`. Computed from
    /// the environment credentials alone, so this is checked BEFORE `Verify` below and needs no network call.
    FingerprintMismatch { expected: String, actual: String },
    /// `GET /v2/account` failed, the account is blocked, or (via `verify_paper_account`) its account number does not
    /// start with `PA`.
    Verify(BrokerError),
    /// `GET /v2/assets/{symbol}` failed for one of the five pilot ETFs.
    AssetRefresh { symbol: &'static str, source: BrokerError },
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RuntimeError::Connect(r) => write!(f, "PILOT_RUNTIME_CONNECT: {r}"),
            RuntimeError::FingerprintMismatch { expected, actual } => write!(
                f,
                "PILOT_RUNTIME_FINGERPRINT_MISMATCH: the connected key fingerprints as {actual}, but the pilot plan row pins {expected}; refusing to connect"
            ),
            RuntimeError::Verify(e) => write!(f, "PILOT_RUNTIME_VERIFY: {e}"),
            RuntimeError::AssetRefresh { symbol, source } => write!(f, "PILOT_RUNTIME_ASSET_REFRESH: {symbol}: {source}"),
        }
    }
}

impl std::error::Error for RuntimeError {}

/// Case-insensitive, whitespace-trimmed comparison of a connected key's fingerprint against the pilot plan row's
/// pinned `credential_fingerprint`. Pure (no I/O), so the check itself is unit-testable without a network call or a
/// database. Both the schema's `CHECK` and `AlpacaCredentials::key_id_fingerprint` produce lower-case hex, but this
/// is intentionally forgiving of case: the fact being checked is "the same key", not "the same textual casing".
pub fn fingerprint_matches(expected: &str, actual: &str) -> bool {
    let (expected, actual) = (expected.trim(), actual.trim());
    !expected.is_empty() && !actual.is_empty() && expected.eq_ignore_ascii_case(actual)
}

/// The connected paper Alpaca broker plus the asset table / prepare options it was verified and refreshed against.
/// Everything [`AlpacaBroker`] and [`AlpacaRules`] need to borrow lives here, owned, so `broker()` / `rules()` can
/// hand out borrows with the lifetime `AccountRuntime` needs.
#[derive(Debug)]
pub struct PilotAlpaca {
    adapter: PaperOnlyAlpaca,
    assets: AssetTable,
    options: PrepareOptions,
}

impl PilotAlpaca {
    /// The `rebalancer_run::broker::Broker` implementation: `AlpacaBroker::us_etf` over the connected adapter. Its
    /// `environment()` is the adapter's own (constructor-verified) environment -- always `Paper` for anything built
    /// through this module, since `PaperOnlyAlpaca` cannot be constructed any other way.
    pub fn broker(&self) -> AlpacaBroker<'_> {
        AlpacaBroker::us_etf(self.adapter.adapter())
    }

    /// The `rebalancer_core::venue::VenueRules` implementation for the `VenueRuleBook`, from the SAME asset table
    /// and prepare options the adapter itself uses to size and validate an order (so a size the planner emits is
    /// exactly the size the adapter will accept, per `venue.rs`'s own module doc).
    pub fn rules(&self) -> AlpacaRules<'_> {
        AlpacaRules { assets: &self.assets, options: &self.options }
    }

    pub fn key_id_fingerprint(&self) -> String {
        self.adapter.key_id_fingerprint()
    }

    pub fn assets(&self) -> &AssetTable {
        &self.assets
    }
}

/// Connect the pilot's ONE paper Alpaca broker: build (paper-only, by construction), check the fingerprint against
/// the pilot plan row (before any network call), verify the account (`GET /v2/account` + the `PA` prefix check),
/// then refresh the asset table for the five pilot ETFs (each `GET /v2/assets/{symbol}` also updates the adapter's
/// OWN internal table as a side effect, which is what lets `refuse_builtin_assets = true` accept these five symbols
/// once this function returns `Ok`).
pub fn connect_paper_alpaca(
    key_id: &str,
    secret: &str,
    base_url: &str,
    transport: Arc<dyn HttpTransport>,
    expected_fingerprint: &str,
) -> Result<PilotAlpaca, RuntimeError> {
    let adapter = build_paper_alpaca(key_id, secret, base_url, transport).map_err(RuntimeError::Connect)?;
    let actual = adapter.key_id_fingerprint();
    if !fingerprint_matches(expected_fingerprint, &actual) {
        return Err(RuntimeError::FingerprintMismatch { expected: expected_fingerprint.trim().to_string(), actual });
    }
    adapter.verify_paper_account().map_err(RuntimeError::Verify)?;

    let mut assets = AssetTable::empty();
    for sym in PILOT_ETF_SYMBOLS {
        let info = adapter.adapter().refresh_asset(sym).map_err(|source| RuntimeError::AssetRefresh { symbol: sym, source })?;
        assets.upsert(info);
    }
    let cfg = adapter.adapter().config();
    let options = PrepareOptions {
        allow_extended_hours: cfg.allow_extended_hours,
        min_notional: cfg.min_notional,
        own_tag_prefix: cfg.own_tag_prefix.clone(),
        refuse_builtin_assets: cfg.refuse_builtin_assets,
    };
    Ok(PilotAlpaca { adapter, assets, options })
}

/// The read-only connection `print-fingerprint` uses: build (paper-only) and verify (`GET /v2/account`, the `PA`
/// check), with no fingerprint to compare yet (that is what this subcommand is FOR: discovering the fingerprint
/// before the plan row exists) and no asset refresh (nothing is about to trade). Never returns the key itself, only
/// its fingerprint.
pub fn connect_read_only(key_id: &str, secret: &str, base_url: &str, transport: Arc<dyn HttpTransport>) -> Result<String, RuntimeError> {
    let adapter = build_paper_alpaca(key_id, secret, base_url, transport).map_err(RuntimeError::Connect)?;
    adapter.verify_paper_account().map_err(RuntimeError::Verify)?;
    Ok(adapter.key_id_fingerprint())
}

// ---------------------------------------------------------------------------------------------------------------
// Sizing prices: the last complete daily close, composed onto the sleeve-panel source through market-data's own
// decorator seam (SleevesFrom + WithPrices, #41) -- nothing new is added to market-data itself.
// ---------------------------------------------------------------------------------------------------------------

/// A `DataSource` whose ONLY job is sizing prices: the last COMPLETE daily close of each requested symbol, via
/// `MassiveDataSource::fetch_daily_bars`. `sleeve_data` is unsupported (compose with [`pilot_data_source`], never
/// use this alone as an account's `DataSource`).
pub struct LastClosePrices<'a> {
    massive: &'a MassiveDataSource,
    lookback_days: i64,
}

impl<'a> LastClosePrices<'a> {
    pub fn new(massive: &'a MassiveDataSource) -> Self {
        Self { massive, lookback_days: PRICE_LOOKBACK_DAYS }
    }
}

impl DataSource for LastClosePrices<'_> {
    fn sleeve_data(&self, sleeve: &SleeveSpec, _as_of: NaiveDate) -> Result<SleeveData, DataError> {
        Err(DataError::new(
            "DATA_UNSUPPORTED",
            &format!("LastClosePrices supplies sizing prices only (asked for sleeve {}); compose with market_data::WithPrices", sleeve.id),
        ))
    }

    /// ETF tickers are their own vendor ticker (see `market_data`'s `ETF_SYMBOLS`), so `symbols` is passed straight
    /// through to `fetch_daily_bars`. A symbol with no complete bar inside the lookback window is a hard `Err`
    /// (never a silent gap): the planner needs a price for every held or targeted symbol to value the account.
    fn prices(&self, symbols: &[String], now: DateTime<Utc>) -> Result<BTreeMap<String, PricePoint>, DataError> {
        let as_of = now.date_naive();
        let from = as_of - ChronoDuration::days(self.lookback_days);
        let mut out = BTreeMap::new();
        for sym in symbols {
            let bars = self.massive.fetch_daily_bars(sym, BarClock::StockMidnightNewYork, from, as_of, as_of).map_err(DataError::from)?;
            let (Some(date), Some(close)) = (bars.dates.last().copied(), bars.closes.last().copied()) else {
                return Err(DataError::new("DATA_UNAVAILABLE", &format!("{sym}: no complete daily bar in the last {} days", self.lookback_days)));
            };
            let price = Dec::parse(&close.to_string())
                .map_err(|_| DataError::new("DATA_MALFORMED", &format!("{sym}: close {close} is not representable as a decimal")))?;
            let stamp = bar_complete_at(BarClock::StockMidnightNewYork, date).unwrap_or(now);
            out.insert(sym.clone(), PricePoint { price, as_of: stamp });
        }
        Ok(out)
    }
}

/// The pilot's `DataSource`: sleeve panels from Massive, sizing prices the last complete Massive close, composed
/// through `market_data`'s existing decorator seam (never a new one). This is what `AccountRuntime::data` borrows.
pub fn pilot_data_source(massive: &MassiveDataSource) -> WithPrices<SleevesFrom<&MassiveDataSource>, LastClosePrices<'_>> {
    WithPrices { sleeves: SleevesFrom(massive), prices: LastClosePrices::new(massive) }
}

/// The pilot's `DataSource` with the two-source gate in SHADOW mode (W9.2, `DATA_GATE_MODE=shadow`): the same
/// Massive panels and sizing prices as [`pilot_data_source`], with [`TwoSourceGate`] between the fetch and the
/// pipeline so every sleeve fetch is compared against its secondary and the verdict travels on the run record. The
/// primary panel is always what the pipeline sees; nothing refuses yet.
pub fn pilot_data_source_shadow<'a>(massive: &'a MassiveDataSource, gate: TwoSourceGate<&'a MassiveDataSource>) -> WithPrices<SleevesFrom<TwoSourceGate<&'a MassiveDataSource>>, LastClosePrices<'a>> {
    WithPrices { sleeves: SleevesFrom(gate), prices: LastClosePrices::new(massive) }
}

/// The shadow gate's secondaries for the pilot, from the process environment: Alpaca daily bars for ETF sleeves
/// (platform DATA credentials `ALPACA_DATA_KEY_ID` / `ALPACA_DATA_KEY_SECRET`, never the pilot's brokerage key:
/// R21b) and Kraken public OHLC for crypto sleeves (no credentials). Refuses (fail closed) when the Alpaca data
/// credentials are absent: a shadow gate with no ETF secondary would record `REFUSE_SECONDARY_NOT_CONFIGURED` on
/// every run, which is noise, not a measurement.
pub fn shadow_gate(massive: &MassiveDataSource, lookup: impl Fn(&str) -> Option<String>, transport: Arc<dyn HttpTransport>) -> Result<TwoSourceGate<&MassiveDataSource>, String> {
    let alpaca = AlpacaBarsSource::from_lookup(&lookup, transport.clone())
        .map_err(|e| format!("DATA_GATE_SECONDARY_MISSING: the ETF secondary (Alpaca daily bars) needs {} and {}: {e}", market_data::alpaca_bars::ENV_KEY_ID, market_data::alpaca_bars::ENV_KEY_SECRET))?;
    let kraken = KrakenOhlcSource::new(transport);
    Ok(TwoSourceGate::shadow(massive).with_secondary(SleeveKind::EtfTrend, Arc::new(alpaca)).with_secondary(SleeveKind::CryptoTrend, Arc::new(kraken)))
}

// ---------------------------------------------------------------------------------------------------------------
// pilot-template
// ---------------------------------------------------------------------------------------------------------------

/// A template for the owner-authored `mandate_events` and `rebalancer_pilot_plans` rows (databaseschema-internal
/// `2026-09-27-000000_create_rebalancer_pilot_ledger/up.sql`; `mandates`/`mandate_events` from
/// `2026-09-22-000004_create_mandate_tables`). Every column the migration's guard trigger or CHECK constraints
/// require is present; placeholders are `<UPPER_SNAKE_CASE>`. This is print-only: nothing here executes anything.
pub fn pilot_template_sql() -> String {
    r#"-- Pilot plan authoring template (product-mandate/PAPER_PILOT_DRAGONSTONE_PLAN.md). Run by the OWNER, by hand,
-- against the real database -- this command only prints text, it never connects to anything.
--
-- Before running this: the mandate itself (an `mandates` row, status 'active', L2, `place_orders` granted) must
-- already exist and be signed; get its id and version from there. Get <CREDENTIAL_FINGERPRINT> by running
-- `rebalancer-service print-fingerprint` with PILOT_ALPACA_KEY_ID / PILOT_ALPACA_KEY_SECRET set to the paper
-- account's real credentials (it never prints the key itself, only its fingerprint).
BEGIN;

-- 1. Record why the mandate was granted (mandate_events; separate from the copy embedded in mandates.body).
INSERT INTO mandate_events (tenant_id, mandate_id, kind, actor, reason, evidence)
VALUES (
    '<TENANT_UUID>',
    '<MANDATE_UUID>',
    'granted',
    '<owner clerk id or email>',
    '<why this mandate is granted (free text)>',
    '{}'::jsonb
);

-- 2. The owner-authored, paper-only pilot plan itself. The guard trigger refuses this insert unless: the credential
--    is a paper/testnet Alpaca credential of this tenant, enabled and not deleted; the mandate is the ACTIVE,
--    signed one of this tenant/account at exactly mandate_version; the mandate is not L3; and (since execution here
--    is paper_orders) the mandate is L2 with place_orders granted. body_hash may be omitted (left NULL): the
--    trigger computes and fills it from body.
INSERT INTO rebalancer_pilot_plans (
    tenant_id, account_id, mandate_id, mandate_version, execution, authored_by, reason,
    acknowledgements, body, entry_id, entry_version, entry_hash, credential_fingerprint
) VALUES (
    '<TENANT_UUID>',
    '<ACCOUNT_CREDENTIAL_UUID>',
    '<MANDATE_UUID>',
    <MANDATE_VERSION_INTEGER>,
    'paper_orders',
    '<owner clerk id or email>',
    '<reason the owner is starting this pilot now, at least 20 characters>',
    '{"not_proposal_derived": true, "entry_evidence": "reference_only", "entry_citation_check": "failed",
      "no_edge_claimed": true, "paper_only": true, "entry_on_decision_in_force": "<YYYY-MM-DD>"}'::jsonb,
    '{"sleeves": [{"sleeve_id": "etf", "entry_id": "etf_trend_faber", "entry_version": 1,
                   "entry_hash": "<64_HEX_CHAR_ENTRY_HASH>", "kind": "etf_trend", "share": "1.0",
                   "venue": "alpaca", "asset_class": "us_etf", "quote": ""}]}'::jsonb,
    'etf_trend_faber',
    1,
    '<64_HEX_CHAR_ENTRY_HASH>',
    '<16_HEX_CHAR_CREDENTIAL_FINGERPRINT>'
);

COMMIT;

-- Sanity check after running: the newest row (highest seq) for this account_id in rebalancer_pilot_plans is the
-- one above, and `rebalancer-service` (PILOT_TENANT_ID / PILOT_ACCOUNT_ID pointed at these ids) enumerates exactly
-- one account with no exclusion.
"#
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_matches_is_case_insensitive_and_trims_but_never_matches_empty() {
        assert!(fingerprint_matches("0123456789abcdef", "0123456789ABCDEF"));
        assert!(fingerprint_matches("  0123456789abcdef  ", "0123456789abcdef"));
        assert!(!fingerprint_matches("0123456789abcdef", "fedcba9876543210"));
        assert!(!fingerprint_matches("", ""));
        assert!(!fingerprint_matches("0123456789abcdef", ""));
        assert!(!fingerprint_matches("", "0123456789abcdef"));
    }

    #[test]
    fn the_pilot_etf_symbols_are_exactly_the_five_the_plan_names() {
        let mut got = PILOT_ETF_SYMBOLS;
        got.sort_unstable();
        let mut want = ["SPY", "EFA", "IEF", "DBC", "VNQ"];
        want.sort_unstable();
        assert_eq!(got, want);
    }

    #[test]
    fn the_pilot_template_names_every_required_column_and_no_secret() {
        let sql = pilot_template_sql();
        for col in [
            "mandate_events",
            "rebalancer_pilot_plans",
            "tenant_id",
            "account_id",
            "mandate_id",
            "mandate_version",
            "execution",
            "authored_by",
            "reason",
            "acknowledgements",
            "entry_on_decision_in_force",
            "not_proposal_derived",
            "body",
            "entry_id",
            "entry_version",
            "entry_hash",
            "credential_fingerprint",
            "paper_orders",
        ] {
            assert!(sql.contains(col), "template is missing `{col}`:\n{sql}");
        }
        assert!(!sql.contains("APCA-API"), "the template must never carry a real credential shape");
    }
}
