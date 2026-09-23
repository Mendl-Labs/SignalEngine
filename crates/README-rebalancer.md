# Rebalancer crates

`reference-rules`, `broker-adapters`, `fake-broker`, `mandate-core`, `rebalancer-core`,
`rebalancer-risk`, `rebalancer-run` are the MVP rebalancer. They are deliberately isolated from
`crates/executionhandler` and `crates/hostbuilder` (and every other pre-existing crate in this
workspace) — no dependency, import, or reference in either direction. `executionhandler` and
`hostbuilder` carry known live-trading bugs (a broken Kraken nonce, tenant-blind credential
loading) that this rebalancer must not inherit.

Moved here from a standalone local repo (`C:\Users\ikenn\Projects\rebalancer`) on 2026-09-22. The
code was already built and tested there (740+ tests, clippy clean) before the move; this was a
relocation, not new development. See
`C:\Users\ikenn\Projects\product-mandate\IMPLEMENTATION_PLAN.md` for the design.

## OANDA (added on `feat/oanda-adapter`; offline-verified only)

`broker-adapters/src/oanda` is an OANDA v20 FX adapter with explicit `Environment{Practice, Live}` host
validation (a live config needs a `LiveTradingAck`), tested against hand-authored fixtures
(`tests/fixtures/oanda`, labelled "authored from documentation, not recorded") and against an in-process fake
(`fake-broker/src/oanda.rs`). It has NEVER spoken to a real OANDA server, is in no live-ready list, and the legacy
`executionhandler` OANDA path is untouched. Rung 3 (a real practice account) is the only thing that can check the
response shapes and the `@clientId` order lookup it relies on for idempotency. `rebalancer-run::view::oanda_snapshot`
and `OandaRules` are additive; the planner, guard and reconciliation are still spot-shaped (no leverage) and are not
correct for FX yet.
