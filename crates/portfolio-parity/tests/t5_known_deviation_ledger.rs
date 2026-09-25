//! PARITY TEST 5 (design 5.4 #5): the KNOWN-DEVIATION LEDGER.
//!
//! A machine-readable, test-time ledger of every divergence the parity tests observed and ACCEPT, each tagged and each
//! pinned by a test that asserts the divergence itself: if a fix (or a regression) moves either side, the pinning test
//! fails and this ledger must be re-stated on purpose. The ledger test also:
//!
//! * prints the whole table as `LEDGER|...` lines (run with `--nocapture`), one per entry, machine-readable;
//! * checks that every `pinned_by` name is a real test function in the parity test sources, that every test named
//!   `*_pinned` is listed by some entry, and that ids are unique and tags are from the closed set;
//! * pins the canonical text of the table with a digest, so that editing a row without updating the digest fails.
//!
//! Tags: `EXPECTED-DIVERGENCE` (both sides behave as coded; the difference is understood and accepted),
//! `KNOWN GAP` (something the backtester cannot express yet), `MAPPING` (a row of the Amendment 12 D1-D8 list, pointing
//! at the entries that carry it), `NOT-TESTED` (outside what the five tests exercise, listed so it is not mistaken for a
//! pass).

use std::collections::BTreeSet;

mod common;

const SOURCES: [(&str, &str); 4] = [
    ("t1_target_parity.rs", include_str!("t1_target_parity.rs")),
    ("t2_ladder_parity.rs", include_str!("t2_ladder_parity.rs")),
    ("t3_cadence_parity.rs", include_str!("t3_cadence_parity.rs")),
    ("t4_pipeline_replay.rs", include_str!("t4_pipeline_replay.rs")),
];

struct Entry {
    id: &'static str,
    tag: &'static str,
    /// What differs, in one sentence.
    what: &'static str,
    /// Which side does what.
    sides: &'static str,
    /// What the tests measured or asserted.
    observed: &'static str,
    /// Tests that assert the divergence itself (or, for MAPPING/NOT-TESTED rows, the entries/tests that carry it).
    pinned_by: &'static [&'static str],
    /// What would make the entry disappear (or "none").
    follow_up: &'static str,
}

const LEDGER: &[Entry] = &[
    Entry {
        id: "PER_ORDER_DENIAL_VS_WHOLE_BOOK_REFUSAL",
        tag: "EXPECTED-DIVERGENCE",
        what: "council R1/R2 (refuse the WHOLE book on a breached limit, never a partial book) are rulings, not yet code in SignalEngine",
        sides: "planner + guard: plans the book, denies only the offending ORDER per order (MAX_POSITION, SHORTING_FORBIDDEN, ...) and places the rest; construct(RefuseWholeBook): PositionAboveCap / ShortingForbidden for the whole book; construct(PlannerFaithful) reproduces the planner",
        observed: "position cap: planner places EFA+IEF and denies SPY (MAX_POSITION), construct refuses whole; shorting: planner places the long leg, denies the short (SHORTING_FORBIDDEN), construct refuses whole",
        pinned_by: &["t1_r1_r2_per_order_denial_vs_whole_book_refusal_pinned", "t1_shorting_forbidden_is_a_per_order_denial_not_a_whole_book_refusal_pinned"],
        follow_up: "implement R1/R2 in the planner (a plan-level whole-book refusal), then flip these tests",
    },
    Entry {
        id: "GUARD_DENIAL_WHEN_ALLOCATION_BINDS",
        tag: "EXPECTED-DIVERGENCE",
        what: "when equity exceeds the mandate's allocated capital the capital base is the allocation while the positions carry the excess equity, so gross/net/asset-class already sit above the cap; the per-order guard denies an INCREASING order (MAX_GROSS, MAX_NET, MAX_ASSET_CLASS) that the guard-less backtester places",
        sides: "pipeline: 27 crypto buy orders denied in the 396-day replay (allocated 100_500 on a 100_000 account; first on 2019-03-31: equity 106_433, capital base 100_500, the buy would take gross to 101_404 against the cap 100_500); simulate_book(allocated_capital): no guard, places them",
        observed: "target weights agree on every run (<= 1e-9); the two accounts agree until the first denial and diverge after it (equity up to 1.2e-3 relative, positions up to 7.5 units)",
        pinned_by: &["t4_allocated_capital_binding_makes_the_guard_deny_orders_the_backtester_places_pinned"],
        follow_up: "model the guard's exposure checks in the backtester, or size the plan so that it cannot breach them; decide with the owner",
    },
    Entry {
        id: "LOT_ROUNDING_STAND_IN",
        tag: "EXPECTED-DIVERGENCE",
        what: "quantity rounding: the planner floors exact decimals by the adapters' venue tables; portfolio-construct floors f64 with a 4-ulp snap by a caller-supplied lot table; weightsim's own MinimalConstruct has none (fractional units)",
        sides: "planner: whole ETF shares, crypto 8 decimals (test table), Decimal floor; construct: LotRounder floor_dp; weightsim native: units = notional / price",
        observed: "with the lot table both sides place identical ETF share counts (1365/1365 position cells exact) and crypto quantities within 5.7e-14 (float noise, one quantum is 1e-8); weightsim's native construction differs from the pipeline by up to 1.9e-4 of equity over the 13-month ETF window",
        pinned_by: &["etf_only_account_replay_agrees_with_simulate_book_at_delay_1", "crypto_only_account_replay_agrees_with_simulate_book_at_delay_0", "t4_native_minimal_construct_versus_pipeline_measures_the_stand_in_gap_pinned"],
        follow_up: "none (the lot table is live-only data; the stand-in is measured, not claimed equal)",
    },
    Entry {
        id: "FEE_AND_CASH_RESERVE_STAND_IN",
        tag: "EXPECTED-DIVERGENCE",
        what: "the planner sizes buys against cash minus the mandate's cash reserve (5% of the capital base in the baseline) and an ESTIMATED fee (RunConfig.fee_rate 0.0026) even when the venue charges nothing; weightsim's Budget policy has reserve 0 and fee = the account's own cost (Amendment 12 D3)",
        sides: "pipeline + PcConstruct(reserve 0.05, fee 0.0026): identical; weightsim native Budget: fully invested",
        observed: "same measurement as LOT_ROUNDING_STAND_IN (the two stand-ins are measured together)",
        pinned_by: &["t4_native_minimal_construct_versus_pipeline_measures_the_stand_in_gap_pinned"],
        follow_up: "none",
    },
    Entry {
        id: "DECIMAL_QUANTUM_EDGE_BAND",
        tag: "EXPECTED-DIVERGENCE",
        what: "Decimal versus f64 at a limit: the planner compares exact decimals, construct compares with a 1e-12 relative tolerance (EDGE_TOL), so a book that exceeds a cap by ONE 8-decimal quantum is refused by the planner and permitted by construct",
        sides: "planner: gross 1.5 x cb against a cap of 1.4999999999999 x cb: GrossAboveCap, excess exactly 1e-8; construct: permits",
        observed: "over 20,000 random books targets agree within 1 quantum + float noise (max 1.0012e-8) and no outcome differs outside the 1e-9 band",
        pinned_by: &["t1_decimal_quantum_edge_band_is_a_pinned_expected_divergence", "twenty_thousand_random_books_agree_on_targets_filter_and_refusals"],
        follow_up: "none (design 5.2 level 2: within one Decimal quantum after round-toward-zero)",
    },
    Entry {
        id: "LADDER_EDGE_BAND",
        tag: "EXPECTED-DIVERGENCE",
        what: "the same band for the drawdown ladder: an equity within 1e-12 relative of a trigger may be classified differently by the decimal ladder (exact, inclusive) and the f64 ladder (1e-12 tolerance)",
        sides: "high-water mark 1e14 dollars, equity one cent above the shrink trigger: decimal does not shrink, f64 shrinks",
        observed: "10,000 generated paths (598,543 observations) agree rung for rung with 0 boundary divergences; every exact trigger/release/daily-loss boundary and one cent either side agree",
        pinned_by: &["ladder_edge_band_case_is_a_pinned_expected_divergence", "ten_thousand_generated_paths_agree_rung_for_rung_away_from_boundaries"],
        follow_up: "none",
    },
    Entry {
        id: "ZERO_RISK_SCALE",
        tag: "EXPECTED-DIVERGENCE",
        what: "construct allows a ladder scale of 0 (a halted book flattens: every target is 0); the planner refuses a risk scale of 0 (BadRiskScale) because a live halt flattens through the flatten path, not through a zero-scale plan",
        sides: "planner: PlanError::BadRiskScale; construct: all targets 0, one sell",
        observed: "as stated",
        pinned_by: &["t1_zero_risk_scale_planner_refuses_construct_flattens_pinned"],
        follow_up: "none",
    },
    Entry {
        id: "FILLS_SAME_CLOSE_VS_VENUE",
        tag: "EXPECTED-DIVERGENCE",
        what: "the backtester fills at the close the decision was sized on; a venue fills at the price of the moment (Amendment 12 D6)",
        sides: "SimBroker with a 10 bps adverse fill offset versus the same-close simulate_book",
        observed: "the equity paths diverge (first at the first fill) and the loss is bounded by the offset times the traded notional",
        pinned_by: &["t4_fill_price_offset_moves_equity_off_the_same_close_backtest_pinned"],
        follow_up: "none: a declared, measured cost preset replaces same-close in the live-realistic layer (council Ruling 4)",
    },
    Entry {
        id: "MIXED_ACCOUNT_PER_SLEEVE_DELAY",
        tag: "KNOWN GAP (needs weightsim per-sleeve execution delay; council Ruling 4)",
        what: "weightsim's execution_delay_bars is ONE integer for the whole book; the pipeline's mixed ETF + crypto account is ETF delay 1 / crypto delay 0",
        sides: "native d=0: the ETF sleeve fills one own bar EARLIER than the pipeline (13 of 13 rebalances), equity and positions differ from the first ETF trade on; native d=1 would delay the crypto sleeve instead",
        observed: "AGREES under native d=0: the crypto sleeve's decisions and target weights on every run (max 9.3e-14), the ETF decision dates. DOES NOT AGREE: equity (up to 1.2e-1 relative), positions, orders. With the delay EMULATED by a rule that decides one own bar later the whole mixed account agrees (equity 9e-16, 93 orders identical, targets 1.9e-16)",
        pinned_by: &["t4_mixed_account_native_delay_is_a_known_gap_pinned", "mixed_account_replay_agrees_when_the_etf_delay_is_emulated"],
        follow_up: "Core: per-sleeve execution_delay_bars in weightsim (being built in parallel); then replace the emulation by the native setting and flip the pinned test",
    },
    Entry {
        id: "ENTRY_IN_FORCE_DECISION",
        tag: "EXPECTED-DIVERGENCE",
        what: "at its first run the pipeline plans the ETF sleeve on the decision in force (council Ruling 9(a)); a backtest account is flat until its first month-end decision",
        sides: "pipeline: entry planned on day 1 (a mid-month start of 2010-03-16 acts on the February month-end); simulate_book(PerSleeve): first planned bar 2010-04-01",
        observed: "when the window is aligned so the entry coincides with a regular action day the two agree on all 10,957 run days",
        pinned_by: &["t3_mid_month_entry_plans_on_day_one_while_the_backtest_waits_a_month_pinned"],
        follow_up: "a backtest option `start_invested_on_decision_in_force` (design decision)",
    },
    Entry {
        id: "CALENDAR_MONTH_END_VS_DATA_CALENDAR",
        tag: "EXPECTED-DIVERGENCE",
        what: "wall-clock versus data-calendar cadence: portfolio-construct::schedule still ships Cadence::CalendarMonthEnd (the pre-#37 ETF cadence) beside LastBarOfMonth; the pipeline no longer uses the wall-clock predicate",
        sides: "pipeline: ETF acts on the run after the first session of a month (day 2 or later, never on a calendar month-end); schedule::CalendarMonthEnd: due on the last calendar day",
        observed: "over 30 years: 360 pipeline actions; 107 of 360 calendar month-ends are not sessions (weekend/holiday) and the two month-end predicates disagree on exactly 107 sessions; 51 actions land on a Saturday or Sunday (order queued over the weekend); 3,375 run days have no ETF bar (no-op runs); PerSleeve cadence equals the pipeline on all 10,957 days",
        pinned_by: &["thirty_years_of_run_days_and_pending_sleeves_versus_the_backtester_cadence"],
        follow_up: "Core W8: schedule.rs replaces CalendarMonthEnd with a pending-decision cadence",
    },
    Entry {
        id: "F1_ALL_SLEEVES_ON_ANY_DUE_LEGACY",
        tag: "EXPECTED-DIVERGENCE",
        what: "BookCadence::AllSleevesOnAnyDue (finding F1, the pre-#37 driver) is kept as a labelled legacy mode in weightsim and schedule; the pipeline is now only-due (council Ruling 3)",
        sides: "AllSleevesOnAnyDue plans the ETF sleeve on every open ETF bar (7,582 bars, 7,222 extra re-plans over 30 years); the pipeline plans it 360 times; at the ORDER level an ETF+crypto account places no ETF order on a day the ETF decision is not pending",
        observed: "as stated; PerSleeve + only-due is what the replay agrees with",
        pinned_by: &["thirty_years_of_run_days_and_pending_sleeves_versus_the_backtester_cadence", "mixed_account_replay_agrees_when_the_etf_delay_is_emulated"],
        follow_up: "none (kept for the cadence_mode_swapped mutant)",
    },
    Entry {
        id: "F64_FULL_EXIT_ABOVE_HOLDING",
        tag: "EXPECTED-DIVERGENCE",
        what: "portfolio-construct's lot rounding snaps a value at most 4 ulps below a quantum UP; on a full exit of float-accumulated units the SELL is sized one ulp above the holding (a -9e-16 residual); a long-only caller then never touches that instrument again (ShortPositionHeld). The planner cannot: exact decimals and min(wish, held)",
        sides: "construct: sell 4.94944549 against a holding of 4.949445489999999; planner: never above the held quantity",
        observed: "found by the replay (the position stayed flat for months); the test adapter clamps sells to the holding and snaps |units| < 1e-9 to 0",
        pinned_by: &["t4_f64_full_exit_can_be_sized_above_the_holding_pinned"],
        follow_up: "Core portfolio-construct: clamp reductions to the holding after rounding",
    },
    Entry {
        id: "UNMANAGED_GROSS_CALLER_CONTRACT",
        tag: "EXPECTED-DIVERGENCE",
        what: "construct's unmanaged_gross is the CALLER's number; the planner counts as unmanaged every position it has no line for, including a short held in an instrument a long-only sleeve names (ShortPositionHeld)",
        sides: "planner + a caller that counts it: BuyingPowerRequired; a caller that counts only the instruments no sleeve names: no margin need",
        observed: "exposed by the first run of the random books (planner BuyingPowerRequired vs construct Ok on a book with a held short); the glue now counts it and 20,000 books agree",
        pinned_by: &["t1_short_held_in_a_long_only_instrument_counts_as_unmanaged_exposure_pinned"],
        follow_up: "document the contract in portfolio-construct, or derive it inside construct from the held units",
    },
    Entry {
        id: "PLANNER_CASH_SCALING_OVERFLOW_AT_SCALE_18",
        tag: "EXPECTED-DIVERGENCE",
        what: "LATENT, not reachable with the shipped venue tables: when the buys do not fit the cash the planner computes div_floor(usable, total, 18); with a total at scale 18 and a usable budget above about 170 currency units the exact i128 numerator overflows and the plan FAILS CLOSED (Math(Overflow), RUN_PLAN_ERROR)",
        sides: "planner + a VenueRules returning an unrounded 18-decimal quantity: PlanError::Math; the shipped tables return at most 8-9 decimals: fine; construct (f64): no such limit",
        observed: "found by this suite's first test venue table (identity rounding); fixed there by rounding to 8 decimals",
        pinned_by: &["t1_planner_cash_scaling_overflows_for_a_scale_18_quantity_pinned"],
        follow_up: "rebalancer-core: normalise the scale of `total` (or widen the intermediate) before div_floor; low priority",
    },
    Entry {
        id: "AMENDMENT12_D1_DECIMAL_VS_F64",
        tag: "MAPPING",
        what: "D1 (the planner rounds weights down and targets toward zero at 8 decimals; the key is unrounded f64)",
        sides: "see DECIMAL_QUANTUM_EDGE_BAND",
        observed: "targets equal within one quantum (1e-8) + float noise",
        pinned_by: &["twenty_thousand_random_books_agree_on_targets_filter_and_refusals"],
        follow_up: "none",
    },
    Entry {
        id: "AMENDMENT12_D2_LOTS",
        tag: "MAPPING",
        what: "D2 (the planner floors quantities by venue rules; the key holds fractional units)",
        sides: "see LOT_ROUNDING_STAND_IN",
        observed: "with the lot table shared, identical",
        pinned_by: &["etf_only_account_replay_agrees_with_simulate_book_at_delay_1"],
        follow_up: "none",
    },
    Entry {
        id: "AMENDMENT12_D3_CASH",
        tag: "MAPPING",
        what: "D3 (cash: key certification versus budget; planner reserve and estimated fee)",
        sides: "see FEE_AND_CASH_RESERVE_STAND_IN",
        observed: "with reserve and fee shared, identical",
        pinned_by: &["t4_native_minimal_construct_versus_pipeline_measures_the_stand_in_gap_pinned"],
        follow_up: "none",
    },
    Entry {
        id: "AMENDMENT12_D4_GROSS_CAP",
        tag: "MAPPING",
        what: "D4 (gross cap: the planner refuses signed plans whole; per-order guard denials and the whole-book R1 ruling are not in the key)",
        sides: "see PER_ORDER_DENIAL_VS_WHOLE_BOOK_REFUSAL and DECIMAL_QUANTUM_EDGE_BAND",
        observed: "gross-cap refuse/permit identical over 20,000 random books except within 1e-9 of the boundary",
        pinned_by: &["gross_cap_boundary_exact_and_relative_neighbours_agree"],
        follow_up: "none",
    },
    Entry {
        id: "AMENDMENT12_D5_TRADE_FILTER",
        tag: "MAPPING",
        what: "D5 (the trade filter's absolute minimum is in currency units; the key needs an account size to convert it)",
        sides: "the replay runs simulate_book at initial_equity 100_000 so 10 currency units means the same on both sides",
        observed: "filter skip sets identical (5,099 absolute-floor and 8,072 percentage-band skips compared)",
        pinned_by: &["trade_filter_ties_and_neighbours_agree"],
        follow_up: "none",
    },
    Entry {
        id: "AMENDMENT12_D6_FILLS",
        tag: "MAPPING",
        what: "D6 (fills and prices: same-close versus venue prices at run time)",
        sides: "see FILLS_SAME_CLOSE_VS_VENUE",
        observed: "as stated there",
        pinned_by: &["t4_fill_price_offset_moves_equity_off_the_same_close_backtest_pinned"],
        follow_up: "none",
    },
    Entry {
        id: "AMENDMENT12_D7_CADENCE_TIMING",
        tag: "MAPPING",
        what: "D7 (cadence timing: the driver's wall-clock month-end versus the rule's data-calendar decision; the U3 month-late lag)",
        sides: "VERIFIED at run time by the U3 tests and here: post-#37 the pipeline acts on the first run after the first session of the month, on the previous last session's decision, exactly once a month; see CALENDAR_MONTH_END_VS_DATA_CALENDAR",
        observed: "360 of 360 months match an independent calendar oracle over 30 years",
        pinned_by: &["thirty_years_of_run_days_and_pending_sleeves_versus_the_backtester_cadence"],
        follow_up: "record D7 as verified in the next amendment (Amendment 12 is hashed)",
    },
    Entry {
        id: "AMENDMENT12_D8_NOT_IN_THE_KEY",
        tag: "NOT-TESTED",
        what: "D8 (drawdown ladder and daily-loss limit in the loop, mandate limits and per-order guard, margin, shorting availability, borrow, financing, data-gate refusals and staleness, venue rejections, the FX sleeve, broker positions the sleeves do not manage)",
        sides: "the ladder is compared as a state machine (test 2) but NOT wired into simulate_book's Overlay hook and NOT replayed through run_once with a drawdown; the guard is exercised only through GUARD_DENIAL_WHEN_ALLOCATION_BINDS; margin, shorting, financing, FX, data-gate refusals and venue rejections are not replayed",
        observed: "none",
        pinned_by: &["ten_thousand_generated_paths_agree_rung_for_rung_away_from_boundaries"],
        follow_up: "extend the replay with the overlay hook and a drawdown path; add an FX signed-sleeve account when the plumbing lands",
    },
    Entry {
        id: "NOT_TESTED_REAL_VENDOR_AND_POSTGRES_LEDGER",
        tag: "NOT-TESTED",
        what: "the vendor is the ASSUMED 00:10Z provider (only bars dated strictly before the run date); D_acted lives in the in-memory run store (the Postgres store has no decision ledger yet); no vendor data, synthetic worlds only",
        sides: "n/a",
        observed: "none",
        pinned_by: &["thirty_years_of_run_days_and_pending_sleeves_versus_the_backtester_cadence"],
        follow_up: "W2 vendor latency probe (owner gate), W6 decision-ledger migration",
    },
];

const TAGS: [&str; 4] = ["EXPECTED-DIVERGENCE", "KNOWN GAP (needs weightsim per-sleeve execution delay; council Ruling 4)", "MAPPING", "NOT-TESTED"];

/// FNV-1a 64 of the canonical text (every field of every entry, in order).
fn canonical() -> String {
    let mut s = String::new();
    for e in LEDGER {
        s.push_str(&format!("{}|{}|{}|{}|{}|{}|{}\n", e.id, e.tag, e.what, e.sides, e.observed, e.pinned_by.join(","), e.follow_up));
    }
    s
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// The digest of the ledger text. Editing any row changes it: update it deliberately, in the same commit, after
/// re-reading the pinning tests.
const LEDGER_DIGEST: u64 = 0xc9b4_ad3d_7303_b4e9;

#[test]
fn the_known_deviation_ledger_is_printed_complete_and_pinned() {
    // ---- print (machine-readable, one line per entry)
    for e in LEDGER {
        println!("LEDGER|{}|{}|pinned_by={}|follow_up={}|{}", e.id, e.tag, e.pinned_by.join(","), e.follow_up, e.what);
    }
    let digest = fnv1a(canonical().as_bytes());
    println!("LEDGER-DIGEST {digest:#018x} entries={}", LEDGER.len());

    // ---- structure
    let mut ids = BTreeSet::new();
    for e in LEDGER {
        assert!(ids.insert(e.id), "duplicate ledger id {}", e.id);
        assert!(TAGS.contains(&e.tag), "{}: unknown tag {}", e.id, e.tag);
        assert!(!e.pinned_by.is_empty(), "{}: every entry names the test that pins it", e.id);
        assert!(!e.what.is_empty() && !e.sides.is_empty() && !e.observed.is_empty() && !e.follow_up.is_empty(), "{}: all fields are filled", e.id);
    }
    // ---- every pinning test exists
    let all_src: String = SOURCES.iter().map(|(_, s)| *s).collect::<Vec<_>>().join("\n");
    for e in LEDGER {
        for t in e.pinned_by {
            assert!(all_src.contains(&format!("fn {t}(")), "{}: pinned_by names `{t}`, which is not a test function in the parity sources", e.id);
        }
    }
    // ---- every `*_pinned` test is listed by some entry (a pinned divergence cannot be left out of the ledger)
    let listed: BTreeSet<&str> = LEDGER.iter().flat_map(|e| e.pinned_by.iter().copied()).collect();
    for (file, src) in SOURCES {
        for line in src.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("fn ") {
                let name = rest.split('(').next().unwrap_or("");
                if name.ends_with("_pinned") || name.contains("_pinned_") || name.ends_with("_expected_divergence") {
                    assert!(listed.contains(name), "{file}: `{name}` pins a divergence but no ledger entry lists it");
                }
            }
        }
    }
    // ---- the table itself is pinned
    assert_eq!(digest, LEDGER_DIGEST, "the ledger text changed: re-read the pinning tests, then update LEDGER_DIGEST to {digest:#018x}");
}
