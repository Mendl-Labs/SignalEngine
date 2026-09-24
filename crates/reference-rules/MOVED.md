# reference-rules moved to BacktestingCore

The decision rules (ETF trend, crypto trend, FX momentum) now live in the public BacktestingCore repository
(`reference-rules/`, same crate name and API), so the backtester and this rebalancer consume ONE implementation
(`product-mandate/BACKTESTER_TRUTH_DESIGN.md`, stage T2). `rebalancer-core` and `rebalancer-run` depend on it through
`[workspace.dependencies]` in the workspace `Cargo.toml` (a git dependency on BacktestingCore).

This directory is no longer a crate. It keeps only `tests/data/`, unchanged, because the `rebalancer-run` tests
(`tests/pipeline.rs`, `tests/etf_assisted.rs`) `include_str!` the ladder candles from here. The ladder candles and
the shadow/FX goldens in it are vendor-derived data in a public repository; whether to delete them (and re-point those two tests
at a synthetic panel) is an owner decision that this change deliberately does not take.
