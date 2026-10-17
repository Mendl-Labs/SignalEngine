# Source of the copied files

`src/mandate.rs`, `src/strategy_library.rs` and `data/strategy_library/*.json` are VERBATIM copies (byte for byte,
no edits) from the Engine repository (`C:\Users\ikenn\Projects\TradingPlatform\BacktestingEngine`, local branch
`feat/strategy-library`, not merged to a shared branch at the time of copying).

Copied on 2026-09-21 with `git show feat/strategy-library:<path>`.

| Item | Value |
|---|---|
| Source commit (`git rev-parse feat/strategy-library`) | `ad1bf5105d6be8705d20b5e90d41bdef0609b2b2` |
| `program/src/mandate.rs` blob | `f72d3d7120b881c85b76b7d0e8267d13dd166d6c` |
| `program/src/strategy_library.rs` blob | `357be72897a06a1bf46d0dcb631cda3668ebb20e` |
| `program/data/strategy_library/crypto_trend_100d.json` blob | `856d43b4cf7ed0bcdc9a192fc951a4601a7eb4d0` |
| `program/data/strategy_library/etf_trend_faber.json` blob | `4163c039e4e100152c9875db7c0ccc48869b811e` |
| `program/data/strategy_library/fx_tsmom_12m.json` blob | `69e729b0a486626520add3a659feb9e4c95de254` |

Verify a copy is still identical to the source (run in the Engine repo, compare with `git hash-object --no-filters`
of the file here; this repo has `core.autocrlf=true`, so a Windows checkout may show CRLF in the working tree while
the committed blob stays LF, hence `--no-filters` on a normalised file, or compare `git rev-parse HEAD:<path>` here):

    git -C BacktestingEngine rev-parse feat/strategy-library:program/src/mandate.rs
    git -C rebalancer rev-parse HEAD:mandate-core/src/mandate.rs

## What is NOT verbatim

- `src/lib.rs` (new): declares `pub mod mandate; pub mod strategy_library;` so the Engine's `use crate::mandate::...`
  and `include_str!("../data/strategy_library/...")` paths resolve UNCHANGED. No `use` line needed editing.
- `tests/mandate_unit_tests.rs`: generated copy of the `tests` module at the bottom of `mandate.rs`; only
  `use super::*;` is replaced by `use mandate_core::mandate::*;`. (The inline module also still runs as part of the
  lib tests, as it is part of the verbatim file.)
- `tests/fixtures/baseline_mandate.json` and `tests/golden.rs` (new): the baseline mandate from `base()` in the
  Engine's mandate tests, and the pinned values below.

## Pinned values (drift detectors, `tests/golden.rs`)

- `canonical_hash(baseline)` = `d0bf63969c12e36666b621c65f784e1ec7b6ad0500a0658bf4c93a55ccfe742e`.
  Computed from this copy and reproduced by an independent implementation of the canonical serialization (a small
  Python script following the struct field order). The Engine crate itself was NOT built for this (disk budget and
  build time); the equality of the source blobs above is what ties the value to the Engine. When the Engine is next
  built, run its own `canonical_hash` on `tests/fixtures/baseline_mandate.json` and compare with this value.
- `entry_hash` of the seed entries: crypto_trend_100d `826fb12e...44f8`, etf_trend_faber `b8ddc2ec...9b86`,
  fx_tsmom_12m `92c0e2b4...4dc9` (full values in `tests/golden.rs`).

## Updating

Change the Engine first. Then re-copy the files, update every hash above, regenerate
`tests/mandate_unit_tests.rs`, and re-pin `tests/golden.rs` only after reading the diff of the canonical JSON.


## R41 cell types (added 2026-10-17; rule x universe cells, certified-envelopes / R41-A)

`src/replication/canon.rs` and `src/replication/universe.rs` are VERBATIM copies from the Engine (`BacktestingEngine`,
`origin/main` at `07a03267401a6596849056ada0f6892ff769a80f`, PR #301 "R41 slice 2 -- universe, cell and family-ledger types").
`src/replication/cell.rs` is that file with exactly two edits, both because the Engine's `ROBUST_PREMIA` citation table is
Engine-only: the `use ...::robust_premia::{premium_documents_universe, ROBUST_PREMIA}` line and the "premium and
documented_universes" check inside `validate` are removed, and the file's inline `#[cfg(test)]` modules (which build Engine
`LibraryEntry` JSON and read the premia table) are not copied (replaced by `tests/cell_registry.rs`).

| Item | Engine blob (`origin/main:program/...`) |
|---|---|
| `src/replication/canon.rs` | `f663e34b68e28df36301265bff1d854e81725ee1` |
| `src/replication/universe.rs` | `f8b9648271068d9677f34bbf046c8698198c8b45` |
| `src/replication/cell.rs` (before the two edits) | `76db6f401bdffe14623f85386ddc7093a89190b8` |

NOT verbatim (new here): `src/replication/mod.rs`, `src/citation_verification/` (only the `AssetClass` enum, so
`universe.rs` compiles unchanged; its serialised form is the Engine's), `src/cell_registry.rs` (the registry and the
claim check the rebalancer uses) and `tests/cell_registry.rs`. The fixtures in `tests/fixtures/r41/` are the Engine's
`program/tests/fixtures/r41/` files, byte for byte; `tests/cell_registry.rs` pins the Engine's universe and cell hashes
over them, so any drift of the copied canonical-JSON code from the Engine fails a test here.

The embedded universe and cell lists (`EMBEDDED_UNIVERSE_JSON`, `EMBEDDED_CELL_JSON`) are EMPTY, as in the Engine. To
register a cell, add the sidecars to the Engine first, then copy them into those lists here; nothing is read from the
environment by the rebalancer (`CellRegistry::platform` uses the embedded lists only).
