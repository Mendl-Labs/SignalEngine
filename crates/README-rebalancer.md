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
