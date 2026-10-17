//! The R41 cell registry: a plan sleeve's cell must equal a platform-registered cell, hash for hash.
//!
//! The pinned hashes below are the Engine's own (`replication::universe::test_support` / `cell::test_support` at
//! the commit named in SOURCE.md), so a drift between the copied canonical-JSON code and the Engine's fails here.

use std::collections::BTreeMap;

use mandate_core::cell_registry::{CellClaim, CellRefusal, CellRegistry, PILOT_CELL_NOT_REGISTERED, PILOT_CELL_NOT_RUNNABLE};
use mandate_core::replication::cell::{cell_hash, parse_cell, AdmissionReason, Cell, CellStatus, EnvelopeRef, WeightPolicy};
use mandate_core::replication::universe::{parse_universe, universe_hash, Slot, Universe};
use mandate_core::strategy_library::{entry_hash, seed_library};

const AC5_JSON: &str = include_str!("fixtures/r41/test_ac5_v1.universe.json");
const SEC9_JSON: &str = include_str!("fixtures/r41/test_sec9_v1.universe.json");
const CELL_JSON: &str = include_str!("fixtures/r41/test_rule__test_ac5_v1.cell.json");
const AC5_HASH: &str = "6a6f5b38cfbda82827ea1fafc8b036cca90675b1562636cc54c3db41ce4efe21";
const SEC9_HASH: &str = "5640a303ff3bf03078aa7ef14af5a5e8a8b984906bfcbe189c5fc7d19dabd99a";
const CELL_HASH: &str = "d6037f98b0b86f8fa570a161c029ce44cb8dbeb91b2637d850c4bd54b4ee129e";

const RULE: &str = "etf_trend_faber";

fn sec9() -> Universe {
    parse_universe(SEC9_JSON).unwrap()
}

/// 11 slots: the 5 fixture slots plus six fixed-set ETFs (a test universe, not a real R41 universe).
fn ac11() -> Universe {
    let mut u = parse_universe(AC5_JSON).unwrap();
    u.id = "TEST-AC11".to_string();
    for t in ["IWM", "EEM", "TLT", "LQD", "HYG", "GLD"] {
        u.slots.push(Slot { asset_class_slot: format!("slot_{t}"), filled_by: "fixed-set".to_string(), ticker: t.to_string(), aum: None });
    }
    assert_eq!(u.n(), 11);
    u
}

fn cell_for(u: &Universe, policy: WeightPolicy, status: CellStatus) -> Cell {
    let decided = !matches!(status, CellStatus::Preregistered);
    Cell {
        rule_entry_id: RULE.to_string(),
        rule_entry_version: 1,
        universe_id: u.id.clone(),
        universe_version: u.version,
        universe_hash: universe_hash(u),
        family_id: "TEST-FAMILY".to_string(),
        premium_ref: "trend_time_series_momentum".to_string(),
        weight_policy: policy,
        n: (policy == WeightPolicy::EqualOverN).then_some(u.n() as u32),
        derived_weight: policy.derive(u.n()),
        envelope_ref: EnvelopeRef { envelope_id: "test.envelope".to_string(), envelope_hash: "cd".repeat(32) },
        cost_model_id: u.cost_model_id.clone(),
        ruling: Some("R41".to_string()),
        preregistered_at: Some("2026-10-17".to_string()),
        status,
        evaluation_refs: if decided { vec!["run-1".to_string()] } else { vec![] },
        admission: matches!(status, CellStatus::Admitted | CellStatus::PaperOnly | CellStatus::Held).then(|| AdmissionReason {
            t_months: 96,
            power_at_sr_0_5: 0.38,
            family_n: 4,
            expected_spurious: 0.05 * 4.0,
            rho_to_canonical: Some(0.8),
            reason: "test".to_string(),
        }),
    }
}

fn registry(universes: &[Universe], cells: Vec<Cell>) -> Result<CellRegistry, String> {
    let map: BTreeMap<String, Universe> = universes.iter().map(|u| (u.key(), u.clone())).collect();
    CellRegistry::from_parts(&map, cells, &seed_library().unwrap())
}

fn rule_hash() -> String {
    entry_hash(seed_library().unwrap().iter().find(|e| e.id == RULE && e.version == 1).unwrap())
}

fn claim_for(c: &Cell, u: &Universe) -> CellClaim {
    CellClaim { rule_entry_id: c.rule_entry_id.clone(), rule_entry_version: c.rule_entry_version, rule_entry_hash: rule_hash(), universe_hash: universe_hash(u), cell_hash: cell_hash(c), weight_policy: c.weight_policy }
}

#[test]
fn the_copied_canonical_hash_reproduces_the_engines_pinned_values() {
    assert_eq!(universe_hash(&parse_universe(AC5_JSON).unwrap()), AC5_HASH);
    assert_eq!(universe_hash(&sec9()), SEC9_HASH);
    assert_eq!(cell_hash(&parse_cell(CELL_JSON).unwrap()), CELL_HASH);
}

#[test]
fn a_registered_cell_with_9_or_11_instruments_verifies_and_yields_the_universes_instruments_in_slot_order() {
    let (u9, u11) = (sec9(), ac11());
    let (c9, c11) = (cell_for(&u9, WeightPolicy::EqualOverN, CellStatus::Admitted), cell_for(&u11, WeightPolicy::EqualOverN, CellStatus::PaperOnly));
    let reg = registry(&[u9.clone(), u11.clone()], vec![c9.clone(), c11.clone()]).unwrap();
    assert_eq!(reg.len(), 2);

    let v9 = reg.verify(&claim_for(&c9, &u9)).unwrap();
    assert_eq!(v9.n(), 9);
    assert_eq!(v9.instruments, ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"].map(String::from));
    assert_eq!(v9.derived_weight.map(f64::to_bits), Some((1.0_f64 / 9.0).to_bits()));
    assert_eq!(v9.weight_policy, WeightPolicy::EqualOverN);

    let v11 = reg.verify(&claim_for(&c11, &u11)).unwrap();
    assert_eq!(v11.n(), 11);
    assert_eq!(v11.instruments[..5], u11.slots[..5].iter().map(|s| s.ticker.clone()).collect::<Vec<_>>()[..]);
    assert_eq!(v11.derived_weight.map(f64::to_bits), Some((1.0_f64 / 11.0).to_bits()));
}

#[test]
fn an_unregistered_cell_is_refused_with_the_typed_code_by_the_empty_registry_and_by_the_platform_registry() {
    let u = sec9();
    let c = cell_for(&u, WeightPolicy::EqualOverN, CellStatus::Admitted);
    let claim = claim_for(&c, &u);
    for reg in [CellRegistry::empty(), CellRegistry::platform().unwrap()] {
        assert!(reg.is_empty(), "the embedded cell list is empty today");
        let err = reg.verify(&claim).unwrap_err();
        assert!(matches!(err, CellRefusal::NotRegistered(_)), "{err:?}");
        assert_eq!(err.code(), PILOT_CELL_NOT_REGISTERED);
        assert_eq!(PILOT_CELL_NOT_REGISTERED, "PILOT_CELL_NOT_REGISTERED");
    }
}

#[test]
fn every_component_of_the_claim_must_equal_the_registered_cell() {
    let u = sec9();
    let c = cell_for(&u, WeightPolicy::EqualOverN, CellStatus::Admitted);
    let reg = registry(&[u.clone()], vec![c.clone()]).unwrap();
    let good = claim_for(&c, &u);
    reg.verify(&good).unwrap();

    let bad = |f: &dyn Fn(&mut CellClaim)| {
        let mut x = good.clone();
        f(&mut x);
        let err = reg.verify(&x).unwrap_err();
        assert_eq!(err.code(), PILOT_CELL_NOT_REGISTERED, "{err:?}");
    };
    bad(&|x| x.cell_hash = "0".repeat(64));
    bad(&|x| x.universe_hash = "0".repeat(64));
    bad(&|x| x.rule_entry_hash = "0".repeat(64));
    bad(&|x| x.rule_entry_id = "crypto_trend_100d".to_string());
    bad(&|x| x.rule_entry_version = 2);
    bad(&|x| x.weight_policy = WeightPolicy::FixedConstant);
}

#[test]
fn only_admitted_and_paper_only_cells_are_runnable() {
    let u = sec9();
    for status in [CellStatus::Preregistered, CellStatus::Evaluated, CellStatus::Held] {
        let c = cell_for(&u, WeightPolicy::EqualOverN, status);
        let reg = registry(&[u.clone()], vec![c.clone()]).unwrap();
        let err = reg.verify(&claim_for(&c, &u)).unwrap_err();
        assert!(matches!(err, CellRefusal::NotRunnable(_)), "{status:?}: {err:?}");
        assert_eq!(err.code(), PILOT_CELL_NOT_RUNNABLE);
    }
}

#[test]
fn the_registry_refuses_to_load_a_cell_the_engines_validator_would_refuse() {
    let u = sec9();
    // A derived weight that is not exactly 1/N.
    let mut c = cell_for(&u, WeightPolicy::EqualOverN, CellStatus::Admitted);
    c.derived_weight = Some(0.2);
    assert!(registry(&[u.clone()], vec![c]).unwrap_err().contains("derived_weight"));
    // A trend (equal_1_over_N) cell on a universe with fewer than 5 instruments.
    let mut small = sec9();
    small.slots.truncate(4);
    let c = cell_for(&small, WeightPolicy::EqualOverN, CellStatus::Admitted);
    assert!(registry(&[small], vec![c]).unwrap_err().contains("N >= 5"));
    // A cell bound to a universe hash that is not the universe file's.
    let mut c = cell_for(&u, WeightPolicy::EqualOverN, CellStatus::Admitted);
    c.universe_hash = "0".repeat(64);
    assert!(registry(&[u.clone()], vec![c]).unwrap_err().contains("universe_hash"));
    // A cell of a rule entry the platform library does not hold.
    let mut c = cell_for(&u, WeightPolicy::EqualOverN, CellStatus::Admitted);
    c.rule_entry_id = "no_such_rule".to_string();
    assert!(registry(&[u.clone()], vec![c]).unwrap_err().contains("no library entry"));
    // A cell whose universe is not loaded.
    let c = cell_for(&u, WeightPolicy::EqualOverN, CellStatus::Admitted);
    assert!(registry(&[], vec![c]).unwrap_err().contains("no universe"));
}

#[test]
fn recording_an_outcome_does_not_change_the_hash_a_plan_claims() {
    let u = sec9();
    let pre = cell_for(&u, WeightPolicy::EqualOverN, CellStatus::Preregistered);
    let admitted = cell_for(&u, WeightPolicy::EqualOverN, CellStatus::Admitted);
    assert_eq!(cell_hash(&pre), cell_hash(&admitted), "the claim is made against the pre-registered part only");
}
