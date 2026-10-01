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
