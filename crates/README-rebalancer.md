# Rebalancer crates

`reference-rules` (now in the public BacktestingCore repository, see `crates/reference-rules/MOVED.md`), `broker-adapters`, `fake-broker`, `mandate-core`, `rebalancer-core`,
`rebalancer-risk`, `rebalancer-run` are the MVP rebalancer. They are deliberately isolated from
`crates/executionhandler` and `crates/hostbuilder` (and every other pre-existing crate in this
workspace) — no dependency, import, or reference in either direction. `executionhandler` and
`hostbuilder` carry known live-trading bugs (a broken Kraken nonce, tenant-blind credential
loading) that this rebalancer must not inherit.

Moved here from a standalone local repo (`C:\Users\ikenn\Projects\rebalancer`) on 2026-09-22. The
code was already built and tested there (740+ tests, clippy clean) before the move; this was a
relocation, not new development. See
`C:\Users\ikenn\Projects\product-mandate\IMPLEMENTATION_PLAN.md` for the design.

## OANDA (added on `feat/oanda-adapter`; fixture- and fake-verified, practice-measured, never run live)

`broker-adapters/src/oanda` is an OANDA v20 FX adapter with explicit `Environment{Practice, Live}` host
validation (a live config needs a `LiveTradingAck`). It is tested against (a) RECORDED, sanitised responses from a
practice account (2026-09-23, `tests/fixtures/oanda/real`), (b) hand-authored fixtures for the shapes not yet measured
(`tests/fixtures/oanda`, labelled "authored from documentation, not recorded", see the README there), and (c) an in-process
fake (`fake-broker/src/oanda.rs`) that reproduces the measured behaviours. It has NEVER run against the live host and is in
no live-ready list; the legacy `executionhandler` OANDA path is untouched.

What the practice account taught (`product-mandate/VENUE_FACTS.md`): `GET /orders/@clientID` finds only PENDING orders and a
client id is not unique, so idempotency is a transaction-stream protocol (checkpoint `lastTransactionID`, scan
`transactions/sinceid` for the tag, re-send only after a complete scan from the first checkpoint found nothing); a position
close sends `ALL` only for the side that exists; `maximumPositionSize` `"0"` means no cap. The protocol, its restart window and
its residual risks are in the module docs of `broker_adapters::oanda`. `tests/oanda_practice_smoke.rs` is an env-gated,
`#[ignore]`d smoke test for the owner to run against the practice host. `rebalancer-run::view::oanda_snapshot` and
`OandaRules` are additive; the planner, guard and reconciliation are still spot-shaped (no leverage) and are not
correct for FX yet.

## market-data (the first real `DataSource`: Massive daily bars)

`crates/market-data` implements `rebalancer_run::data::DataSource` over the Massive (formerly Polygon.io) daily-aggregates
REST API. It depends on `rebalancer-run` (never the reverse), reuses `broker-adapters`' `HttpTransport` (no new HTTP stack;
the real reqwest transport is behind the opt-in `live` feature) and adds no dependency to any shipped binary. It returns only
COMPLETE bars (dated before the run date AND over by its own clock), validates every response, retries transient failures
with bounded, jittered backoff on an injectable clock, never sends the API key anywhere but the `Authorization: Bearer`
header, and leaves a provenance record per instrument. It is fixture-tested with hand-written responses in the documented
shape; it has NOT been run against the real API (an env-gated live test, `tests/live.rs`, is for the owner). Not built yet, on
purpose: the two-source gate (a decorator over `market_data::SleeveFetcher`), the retry window across ticks, the
Alpaca/Kraken/OANDA readers and any exchange calendar. See the crate docs for the completeness rule, the failure taxonomy
and the decorator seam. `MassiveDataSource::prices` refuses: sizing prices come from a different source (`WithPrices`).

## Latency recorder (W9.1: first-seen latency and revisions, COUNCIL_DATA_GATE R25)

`rebalancer_run::latency` is an OBSERVER that runs after every service tick: it asks the vendor for the newest bars of
every instrument the sleeves use (plus, by default, both sleeve kinds' instruments), records the FIRST sighting of each
`(instrument, bar date)` with its latency from the venue's nominal close (ETF: 16:00 America/New_York of the session,
DST-aware, vendor delay not subtracted, early closes not modelled; crypto: 00:00 UTC of the next day), and appends a
revision row whenever a bar on file comes back with different values (exact equality on open/high/low/close/volume).
Its policy (five newest closed bars per tick, exact-equality revisions, the 20-session evidence threshold) is
pre-registered as data (`latency::policy`, `POLICY_VERSION`, fixed 2026-10-02 before any data) and stamped on every
row; `latency::summary` is what the paper-cycle gate reads (per instrument: sessions, p50/p90/max latency, revisions,
threshold met). A failure is logged and alerted once (`ALERT_LATENCY_RECORDER_FAILED`, Warning) and never affects a
run. Flags: `LATENCY_RECORDER_ENABLED` (default `true`; it is read-only and the council said start now),
`LATENCY_RECORDER_KINDS` (default `etf_trend,crypto_trend`). Storage: `rebalancer_store::PgLatencyStore` over two
append-only, platform-level (no tenant) tables whose migration is kept in
`crates/rebalancer-store/migrations/2026-10-02-000000_create_rebalancer_bar_observations` for the owner to copy into
`databaseschema-internal` verbatim; until it is applied the recorder reports `LATENCY_STORE_UNAVAILABLE` every tick.

## rebalancer-alerts (W6.1 alert delivery + W6.2 dead-man's heartbeat; dark behind `ALERTS_ENABLED` / `HEARTBEAT_URL`)

`rebalancer_store::PgNotifier` only INSERTs into `rebalancer_alerts`. `crates/rebalancer-alerts` is the delivery half: a
`DeliveryWorker` the service runs after every tick's runs. It reads the alerts raised since its watermark (first tick: the
last 24 h) and, for each verified enabled channel that applies (the alert's tenant's channels for every severity, platform-scope
channels for `warning`/`critical`), inserts one `alert_deliveries` row, insert-if-absent on `(channel_id, dedupe_key)` -- that
unique key is the de-duplication (a halted account reminded every tick produces one e-mail) -- then sends each unsent,
unerrored row through a `Sender` and marks it ONCE (`sent_at` + `provider_message_id`, or `error`). Subject
`[Mendl Labs] <severity> <code> <account>`, plain text; the halt / flatten / failed-run / not-acted / heartbeat codes carry a
one-line "what to do" footer. The Postgres ledger (`rebalancer_store::PgDeliveryLedger`, raw SQL like every other store) fails
closed with `ALERT_STORE_UNAVAILABLE` when `alert_channels` / `alert_deliveries` are absent -- those tables are the
`databaseschema-internal` migration `feat/w6-1-alert-channels` (not renamed here); the service role needs `SELECT` on
`alert_channels` and `SELECT, INSERT, UPDATE` on `alert_deliveries`.

Environment: `ALERTS_ENABLED` (exactly `true` to turn delivery on; default off), `RESEND_API_KEY` + `ALERT_FROM_EMAIL` (the
Resend sender, `POST https://api.resend.com/emails`; with either unset the sender is DISABLED and every delivery is recorded
with `error = 'sender_disabled'`, never dropped and never retried). The key is sent only as `Authorization: Bearer` and is
never logged. Nothing in this crate can stop or change a run: a ledger or provider failure is logged in the tick summary.

Heartbeat: with `HEARTBEAT_URL` set (healthchecks.io style), every tick that RAN -- including a tick with nothing due and a
tick skipped by the kill flag -- POSTs the URL; a tick that could not run (kill flag unreadable, due scan failed) POSTs
`<HEARTBEAT_URL><HEARTBEAT_FAIL_SUFFIX>` (default `/fail`). The ping uses its own transport with a 3 s connect / 5 s total
timeout so it can never hold the loop; a failure is logged (URL redacted to host + last 4 characters) and raised once per
failure streak as `ALERT_HEARTBEAT_FAILED` (warning, account `platform`) through the notifier, so it reaches the platform
channels like any other alert. **Set the monitor's expected period to the tick interval (`REBALANCER_TICK_SECS`, default
300 s) plus a grace that covers one slow tick (the runs plus the delivery pass); e.g. period 5 min, grace 5 min.** The
monitor owns the alarm: if the pings stop, it pages; this process never decides anything from a ping's outcome.

## Two-source data gate, SHADOW mode (W9.2; COUNCIL_DATA_GATE R15/R18/R19/R20; `DATA_GATE_MODE`)

`market_data::TwoSourceGate` is the decorator over `SleeveFetcher` the crate docs promised: Massive stays the primary (the
source whose definition the certified rule uses); the secondary is `AlpacaBarsSource` for ETF sleeves (`GET
https://data.alpaca.markets/v2/stocks/bars`, `adjustment=split`, platform DATA credentials `ALPACA_DATA_KEY_ID` /
`ALPACA_DATA_KEY_SECRET`, feed `ALPACA_DATA_FEED` default `sip`; never a tenant's brokerage key, R21b) and `KrakenOhlcSource`
for crypto sleeves (`GET https://api.kraken.com/0/public/OHLC?interval=1440`, 720 daily candles, no credentials). Both readers
are NEW (no Alpaca market-data client and no Kraken OHLC reader existed; the broker adapters cover trading and Kraken's
`Ticker` only), written to the documented shapes and fixture-tested, not yet run against the real endpoints.

The comparison (`gate::compare`) is pure and symmetric under a source swap (every check runs on both sides; the compared
dates are the UNION of what both sources report). Inputs per R18.2: ETF, the last ten completed month-end closes of each
source plus the decision date and the newest session; crypto, the last 100 completed UTC closes with yesterday's at its own
tolerance. Refusals per R18.3: `REFUSE_MISSING_BAR` (a), `REFUSE_DATE_MISMATCH` (b), `REFUSE_L1_OVER_TOLERANCE` (c),
`REFUSE_GAP` (d, more than 3 missing weekdays), `REFUSE_SPLIT_ONE_SOURCE` (e, a day-over-day ratio within 1% of an integer or
its reciprocal on one source and not the other), `REFUSE_SECONDARY_UNAVAILABLE` / `REFUSE_SECONDARY_NOT_CONFIGURED` (g, never
single-source), `REFUSE_BAR_AT_OR_AFTER_AS_OF` (h). Flags: `FLAG_L1_OVER_FLAG`, `FLAG_NEWEST_SESSION_DIFFERS`. Tolerances
(`gate::policy`, R19, strictly-greater-than, `|a-b|/min(a,b)` in bp): ETF month-end flag 25 / refuse 50; crypto decision day
50 / 150; crypto other bars 100 / 500. The policy is versioned (`R19-2026-09-26`) and SHA-256 hashed; a test pins the hash.
NOT in this slice: L2 decision-state agreement (KNIFE_EDGE_HOLD), the correction ledger, arbitration, the cross-tick retry
window, OANDA / FX.

Modes: `DATA_GATE_MODE=off` (default) adds nothing; `shadow` runs the comparison on every sleeve fetch, records the report
(`SleeveDecision::data_gate` on the run record: both closes, per-input differences in bp, verdict, reasons, decision dates,
both fingerprints, policy version and hash) and raises `ALERT_DATA_GATE_SHADOW_REFUSE` (warning, one per sleeve + instrument +
date) when the verdict is `REFUSE`; the pipeline ALWAYS decides on the primary panel, so the owner sees what enforce would do
without it doing anything. `enforce` is REJECTED at startup with the reason. Shadow mode refuses to start without the Alpaca
data credentials (a gate with no ETF secondary would record a refusal on every run).
