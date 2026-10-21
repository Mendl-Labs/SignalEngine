# Source of the copied files

## CURRENT sync (2026-10-17): Engine `origin/main` `d219b8638f4620b128f844483b85423f8142a6da`

`src/mandate.rs`, `src/strategy_library.rs` and `data/strategy_library/*.json` (+ `test_fixtures/`) are VERBATIM copies
(byte for byte) of the Engine's files at that commit. `src/mandate.rs` was already identical (same blob); `strategy_library.rs`
is the Engine's current file (it was a 2026-09-21 copy that lacked `AgentVerified`, `citation_claims`, `operating_procedure`,
`backtest_window`, `known_deviations`, `flagship`, and shipped `etf_trend_faber` v1 / `fx_tsmom_12m`; the Engine now ships
`etf_trend_faber` v2, `crypto_trend_100d` v1 and `etf_cross_sectional_momentum` v2, and no FX entry).

| Item | Engine blob (`origin/main:<path>`) |
|---|---|
| `program/src/mandate.rs` blob | `f72d3d7120b881c85b76b7d0e8267d13dd166d6c` |
| `program/src/strategy_library.rs` blob | `3fb255b207797708501348ee451c4bbc3609da3c` |
| `program/data/strategy_library/etf_trend_faber.json` blob | `d63ccbf3bf50c9aba7796df2743d05fd8cda9bc0` |
| `program/data/strategy_library/crypto_trend_100d.json` blob | `b30e688c0f27904542deeb2cf833e2bf24092a17` |
| `program/data/strategy_library/etf_cross_sectional_momentum.json` blob | `df6657cf6996f9ffbd15ac7b3165960fba866e15` |
| `program/data/strategy_library/test_fixtures/etf_trend_faber.json` blob | `f5caa6fa25d6809ee7e481b6815c88db2fe379d4` |
| `program/data/strategy_library/test_fixtures/crypto_trend_100d.json` blob | `c29dc8420eb968198f9fac841012d47a52db3809` |
| `program/data/strategy_library/test_fixtures/fx_tsmom_12m.json` blob | `02926e470cb606fd2a03e06880096a6792fb4ae1` |

Verify (blobs are over the LF content; this repo has `core.autocrlf=true`): `git -C BacktestingEngine rev-parse origin/main:program/src/strategy_library.rs`
against `git hash-object --no-filters crates/mandate-core/src/strategy_library.rs`.

The Engine-only files `*.envelope.json`, `etf_trend_faber_citation_check.json` and `universe_rules/` are not copied (the
embedded seed does not read them).

### Cross-repo parity

`tests/golden.rs::seed_library_entry_hashes_are_pinned` pins `entry_hash` of the three seed entries. The Engine pins the
SAME three values in `program/tests/seed_entry_hashes_pinned.rs` (BacktestingEngine), so drift on either side fails CI there
or here. Update both together.

### Legacy pilot library (NOT synced, deliberately)

`src/legacy_pilot_library.rs` + `data/legacy_pilot_library/*.json` are the previous copy (Engine `feat/strategy-library`
`ad1bf5105d6be8705d20b5e90d41bdef0609b2b2`, blobs below), byte for byte except the `include_str!` paths. The owner pilot row
pins `etf_trend_faber` VERSION 1 at the hash computed over that schema (`b8ddc2ec...9b86`); the Engine no longer ships v1,
so the pilot's entry check resolves against this module and its pinned hash is unchanged. Proposal plans use the synced
library. Delete the module together with the pilot row.

---

# Historical: the 2026-09-21 copy (now `legacy_pilot_library`)

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

## Re-sync 2026-10-18 (Engine `origin/main` acb0820f, after PRs #311 and #312)

`src/strategy_library.rs` and `data/strategy_library/crypto_trend_100d.json` were re-copied verbatim with
`git show origin/main:program/<path>`; every other copied file is unchanged (blobs compared equal).

| Item | Blob |
|---|---|
| `program/src/strategy_library.rs` | `afdb563f495a295326756992128df5099a93fc2a` |
| `program/data/strategy_library/crypto_trend_100d.json` | `935ff276f903e2892737b992d444d976fc7b0cf3` |

What changed in the copy:

- `crypto_trend_100d` is now **v2** (evidence window 2020-03-16..2026-10-03, instruments `BTC`/`ETH` and parameters exactly
  the compiled rule's, `sharpe_net` declared, no band). Its `entry_hash` is
  `843a1bae14489f3f017f9d60b7145dab66695f185abfc630f87a5bf5dd9268a6` (pinned in `tests/golden.rs` and, identically, in
  the Engine's `program/tests/seed_entry_hashes_pinned.rs`).
- `strategy_library::symbol_in` (new, verbatim from the Engine): an ENTRY's bare `BTC`/`ETH` also matches the mandate's
  `<BASE>/USD` in the eligibility allow/deny checks. Covered by `tests/crypto_alias.rs` here.
- The Engine's R38 envelope `crypto_trend_100d.envelope.json` (hash `aaa37add59c8161cb22c47082d60dca9147392e7e2ec8229cbbfc3a579ba82d7`)
  is Engine-only: this crate carries no envelope files, so there is nothing to copy for it.

The rebalancer IMAGE must be rebuilt after this merges (the proposal-plan entry hash the rebalancer enforces, PILOT_ENTRY_MISMATCH, is
computed from this crate's embedded library).

## Re-sync 2026-10-19 (Engine PR "R45-E", branch fix/seed-entries-band-wording)

Text-only reword of `verification_note` in two seed entries (no declared band is not a non-admissibility finding; the
platform derives a conservative band per R45-v1). Re-copied verbatim; all other copied files unchanged.

| Item | Blob |
|---|---|
| `program/data/strategy_library/crypto_trend_100d.json` | `aea6bd04f5215f69f0f27cf8fad486f4ddcda47e` |
| `program/data/strategy_library/etf_cross_sectional_momentum.json` | `b7948493bc736198d2c55e622aed96a748bc2513` |

`entry_hash` (pinned in `tests/golden.rs` and in the Engine's `seed_entry_hashes_pinned.rs`):

- `crypto_trend_100d` v2: `843a1bae…` -> `155714130c283c8e901c47e7f8b1805733c475ad16387b71e03b3293097d7991`
- `etf_cross_sectional_momentum` v2: `22e148b9…` -> `bcac9efb72dc947e846a80fef6b9b0b4d33123eeb5ad3e1f1ab1291e1f7ca347`

`rule_hash` and `claim_hash` are unchanged (neither covers `verification_note`). The rebalancer image must be rebuilt
after this merges (proposal plans pin `entry_hash`).

## Re-sync 2026-10-21 (Engine PR "R45-F", branch fix/r45f-net-range-declaration; GEM seeded)

Two things in one verbatim re-copy: (1) R45-F, a declaration-only correction (a null-bound `sharpe_net` range added after the
gross range of `etf_cross_sectional_momentum` v2 and `etf_dual_relative_momentum` v1, plus GEM's stale pre-certification text
reworded; no rule, parameter, instrument or window change); (2) the Engine's GEM seeding (#331), which this crate had not yet
received: `src/strategy_library.rs` gains the fourth `SEED_JSON` entry and `etf_dual_relative_momentum.json` is new here.
Merge only after Engine PR #333 merges. Rebuild the rebalancer image after this merges.

| Item | Blob |
|---|---|
| `program/src/strategy_library.rs` | `9455acba46ced3fd4ebf6378275da9945d390883` |
| `program/data/strategy_library/etf_cross_sectional_momentum.json` | `5942f4724ceb519332383ba47c16791a9b975fa8` |
| `program/data/strategy_library/etf_dual_relative_momentum.json` | `915dcda6bfcfe48f7b7968ddb600dc5c720fae2e` |

`entry_hash` (pinned in `tests/golden.rs` and in the Engine's `seed_entry_hashes_pinned.rs`):

- `etf_cross_sectional_momentum` v2: `bcac9efb...` -> `43ca4eaf53e2372f4d7ee13038f285bf284dffb858958f89b0c9d7be7b272f41`
- `etf_dual_relative_momentum` v1 (new here): `c661113773307ff3e698791c8fb2d75858331d32f18c6de6722ce51fb3bf78ab`
- `etf_trend_faber` v2 and `crypto_trend_100d` v2 unchanged.

`rule_hash` is unchanged; `claim_hash` moves for the two entries (it covers `expected_ranges`).
