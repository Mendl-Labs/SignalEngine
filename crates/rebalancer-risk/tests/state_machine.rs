//! The account state machine, the human-approval token and the compare-and-swap store.

mod common;

use common::*;
use rebalancer_risk::approval::{issue_human_approval, ApprovalDenied, PrincipalKind};
use rebalancer_risk::overlay::step;
use rebalancer_risk::state::{AccountState, AccountStateRecord, AccountStatus, HaltReason, ResumeError};
use rebalancer_risk::store::{InMemoryStateStore, StateStore, StoreError};
use rebalancer_risk::Dec;

fn flattening() -> AccountState {
    let p = policy();
    let (s, _, _) = step(&state_with_hwm("10000", "7000"), &snap("7000"), day(2), &p);
    assert_eq!(s.status(), AccountStatus::Flattening);
    s
}

fn halted() -> AccountState {
    flattening().complete_flatten(true).0
}

// ---------------------------------------------------------------------------------------------------------------
// Transitions
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn a_new_account_is_active_at_full_size_with_version_zero() {
    let s = AccountState::new("a1");
    assert_eq!((s.status(), s.risk_scale(), s.version()), (AccountStatus::Active, d("1"), 0));
    assert!(s.hwm().is_none() && s.halt_record().is_none() && s.resumes().is_empty());
    assert_eq!(s.account_id(), "a1");
}

#[test]
fn flattening_is_transient_and_only_a_verified_flat_reaches_halted() {
    let f = flattening();
    assert_eq!(f.risk_scale(), Dec::ZERO);
    // A failed flatten keeps Flattening and counts the attempt.
    let (still, tr) = f.complete_flatten(false);
    assert_eq!(still.status(), AccountStatus::Flattening);
    assert!(tr.is_none());
    assert_eq!(still.halt_record().unwrap().failed_flatten_attempts, 1);
    let (still2, _) = still.complete_flatten(false);
    assert_eq!(still2.halt_record().unwrap().failed_flatten_attempts, 2);
    // A verified flat halts.
    let (h, tr) = still2.complete_flatten(true);
    assert_eq!(h.status(), AccountStatus::Halted);
    let tr = tr.unwrap();
    assert_eq!((tr.from, tr.to, tr.code), (AccountStatus::Flattening, AccountStatus::Halted, "HALT_DRAWDOWN_LADDER"));
    // complete_flatten is a no-op outside Flattening.
    assert_eq!(h.complete_flatten(true).0, h);
    assert_eq!(AccountState::new("a").complete_flatten(true).0.status(), AccountStatus::Active);
}

#[test]
fn a_direct_halt_records_the_reason_and_keeps_the_first_one() {
    let s = state_with_hwm("10000", "10000");
    let (h, tr) = s.halt(HaltReason::Reconciliation, "foreign order X", t0());
    assert_eq!(h.status(), AccountStatus::Halted);
    assert_eq!(tr.unwrap().code, "HALT_RECONCILIATION");
    assert_eq!(h.halt_record().unwrap().detail, "foreign order X");
    let (again, tr) = h.halt(HaltReason::Manual, "later", t0());
    assert_eq!(again, h, "a second halt changes nothing (first reason kept)");
    assert!(tr.is_none());
    // A halt request while flattening must not skip the flatten.
    let (still, tr) = flattening().halt(HaltReason::Manual, "x", t0());
    assert_eq!(still.status(), AccountStatus::Flattening);
    assert!(tr.is_none());
    // begin_flatten on a halted account is a no-op too.
    assert_eq!(h.begin_flatten(HaltReason::DailyLoss, "x", t0()).0, h);
}

#[test]
fn a_halted_account_cannot_be_flattened_or_shrunk_by_any_transition_function() {
    let h = halted();
    assert_eq!(h.begin_flatten(HaltReason::Manual, "x", t0()).0.status(), AccountStatus::Halted);
    assert_eq!(h.halt(HaltReason::Manual, "x", t0()).0.status(), AccountStatus::Halted);
    assert_eq!(h.complete_flatten(false).0.status(), AccountStatus::Halted);
    assert_eq!(h.observe(&snap("999999"), day(9)).status(), AccountStatus::Halted);
}

// ---------------------------------------------------------------------------------------------------------------
// Resume
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn resume_reactivates_resets_the_mark_rebases_the_day_and_records_who_when_and_why() {
    let h = halted();
    assert_eq!(h.hwm(), Some(d("10000")));
    let approval = human_approval("Reviewed the fills; the drop was a data glitch.");
    let (s, tr) = h.resume(approval, &snap("7500"), day(3)).unwrap();
    assert_eq!(s.status(), AccountStatus::Active);
    assert_eq!(s.risk_scale(), d("1"));
    assert_eq!(s.hwm(), Some(d("7500")), "the high-water mark is reset to the equity at resume");
    assert_eq!(s.day_start_equity(), Some(d("7500")), "the daily-loss reference is re-based");
    assert_eq!(s.trading_day(), Some(day(3)));
    assert!(s.halt_record().is_none());
    assert_eq!((tr.from, tr.to, tr.code), (AccountStatus::Halted, AccountStatus::Active, "RESUMED"));
    let r = &s.resumes()[0];
    assert_eq!(r.approver, "user_owner");
    assert_eq!(r.at, t0());
    assert_eq!(r.note, "Reviewed the fills; the drop was a data glitch.");
    assert_eq!(r.equity_at_resume, d("7500"));
    assert_eq!(r.hwm_before, Some(d("10000")));
    assert_eq!(r.halt_reason, HaltReason::DrawdownLadder);
    // The reset means the very next observation at the same equity is not a drawdown or a daily loss.
    let (after, dec, _) = step(&s, &snap("7500"), day(3), &policy());
    assert_eq!(after.status(), AccountStatus::Active);
    assert_eq!(dec.codes(), ["RISK_NO_ACTION"]);
}

#[test]
fn resume_refuses_anything_that_is_not_a_finished_halt() {
    let approval = || human_approval("n");
    let err = flattening().resume(approval(), &snap("8000"), day(3)).unwrap_err();
    assert_eq!(err, ResumeError::FlattenIncomplete);
    assert_eq!(err.code(), "RESUME_FLATTEN_INCOMPLETE");
    let err = AccountState::new("a").resume(approval(), &snap("8000"), day(3)).unwrap_err();
    assert_eq!(err.code(), "RESUME_NOT_HALTED");
    let err = halted().resume(approval(), &snap("0"), day(3)).unwrap_err();
    assert_eq!(err.code(), "RESUME_EQUITY_INVALID");
    assert_eq!(halted().resume(approval(), &snap("-5"), day(3)).unwrap_err().code(), "RESUME_EQUITY_INVALID");
}

#[test]
fn a_manual_halt_can_be_resumed_the_same_way() {
    let (h, _) = state_with_hwm("10000", "10000").halt(HaltReason::Manual, "requested by the owner", t0());
    let (s, _) = h.resume(human_approval("ok"), &snap("10000"), day(2)).unwrap();
    assert_eq!(s.status(), AccountStatus::Active);
    assert_eq!(s.resumes()[0].halt_reason, HaltReason::Manual);
    // A second halt/resume cycle appends a second record.
    let (h2, _) = s.halt(HaltReason::Manual, "again", t0());
    let (s2, _) = h2.resume(human_approval("ok again"), &snap("10000"), day(2)).unwrap();
    assert_eq!(s2.resumes().len(), 2);
}

// ---------------------------------------------------------------------------------------------------------------
// The approval token
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn only_an_authenticated_human_with_a_note_gets_an_approval() {
    use common::Person;
    for kind in [PrincipalKind::Agent, PrincipalKind::ApiKey, PrincipalKind::Service] {
        let e = issue_human_approval(&Person("svc_agent", kind), "please resume", t0()).unwrap_err();
        assert_eq!(e, ApprovalDenied::NotHuman, "{kind:?}");
        assert_eq!(e.code(), "APPROVAL_NOT_HUMAN");
    }
    let human = Person("user_1", PrincipalKind::Human);
    assert_eq!(issue_human_approval(&human, "", t0()).unwrap_err(), ApprovalDenied::NoteRequired);
    assert_eq!(issue_human_approval(&human, "   ", t0()).unwrap_err().code(), "APPROVAL_NOTE_REQUIRED");
    assert_eq!(issue_human_approval(&Person("  ", PrincipalKind::Human), "n", t0()).unwrap_err().code(), "APPROVAL_SUBJECT_REQUIRED");
    let ok = issue_human_approval(&human, "  looked at it  ", t0()).unwrap();
    assert_eq!((ok.approver(), ok.note(), ok.at()), ("user_1", "looked at it", t0()));
}

// ---------------------------------------------------------------------------------------------------------------
// Persistence boundary and the compare-and-swap store
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn records_round_trip_and_incoherent_records_are_refused() {
    for s in [AccountState::new("a"), state_with_hwm("10000", "9000"), flattening(), halted()] {
        assert_eq!(AccountState::from_record(s.to_record()).unwrap(), s);
    }
    let base = halted().to_record();
    let bad = |edit: &dyn Fn(&mut AccountStateRecord)| {
        let mut r = base.clone();
        edit(&mut r);
        AccountState::from_record(r).is_err()
    };
    assert!(bad(&|r| r.halt = None), "a halted status needs a halt record");
    assert!(bad(&|r| r.status = AccountStatus::Active), "an active status must not carry a halt record");
    assert!(bad(&|r| r.risk_scale = d("0")));
    assert!(bad(&|r| r.risk_scale = d("1.5")));
    let mut shrunk = state_with_hwm("10000", "9000").to_record();
    shrunk.status = AccountStatus::Shrunk;
    assert!(AccountState::from_record(shrunk).is_err(), "shrunk needs a rung");
}

#[test]
fn save_is_compare_and_swap_and_the_store_assigns_the_version() {
    let store = InMemoryStateStore::new();
    assert!(store.load("acct").unwrap().is_none());
    let s0 = AccountState::new("acct");
    let s1 = store.save(0, &s0).unwrap();
    assert_eq!(s1.version(), 1);
    assert_eq!(store.load("acct").unwrap().unwrap(), s1);
    // A stale writer (still thinks version 0) loses; nothing changes.
    let e = store.save(0, &s1.observe(&snap("9999"), day(1))).unwrap_err();
    assert_eq!(e, StoreError::VersionConflict { expected: 0, actual: 1 });
    assert_eq!(store.load("acct").unwrap().unwrap(), s1);
    // The store ignores the version inside the state and assigns expected + 1.
    let s2 = store.save(1, &s1.observe(&snap("9999"), day(1))).unwrap();
    assert_eq!(s2.version(), 2);
    assert_eq!(s2.last_equity(), Some(d("9999")));
    // Saving ahead of the stored version conflicts too.
    assert_eq!(store.save(5, &s2).unwrap_err(), StoreError::VersionConflict { expected: 5, actual: 2 });
    assert_eq!(store.saves(), 2);
}

#[test]
fn two_workers_that_read_the_same_version_cannot_both_win() {
    let store = InMemoryStateStore::new();
    let base = store.save(0, &AccountState::new("acct")).unwrap();
    let a = store.load("acct").unwrap().unwrap();
    let b = store.load("acct").unwrap().unwrap();
    assert_eq!(a.version(), b.version());
    let a_wins = store.save(a.version(), &a.observe(&snap("10000"), day(1)));
    let b_loses = store.save(b.version(), &b.observe(&snap("9000"), day(1)));
    assert!(a_wins.is_ok());
    assert_eq!(b_loses.unwrap_err(), StoreError::VersionConflict { expected: base.version(), actual: base.version() + 1 });
    assert_eq!(store.load("acct").unwrap().unwrap().last_equity(), Some(d("10000")));
}

#[test]
fn concurrent_threads_racing_on_one_version_produce_exactly_one_winner_per_round() {
    use std::sync::{Arc, Barrier};
    let store = Arc::new(InMemoryStateStore::new());
    store.save(0, &AccountState::new("acct")).unwrap();
    for round in 0..40u32 {
        let current = store.load("acct").unwrap().unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8u32)
            .map(|i| {
                let store = store.clone();
                let barrier = barrier.clone();
                let mine = current.observe(&snap(&format!("{}", 10_000 + i)), day(1));
                let v = current.version();
                std::thread::spawn(move || {
                    barrier.wait();
                    store.save(v, &mine)
                })
            })
            .collect();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let wins = results.iter().filter(|r| r.is_ok()).count();
        let conflicts = results.iter().filter(|r| matches!(r, Err(StoreError::VersionConflict { .. }))).count();
        assert_eq!((wins, conflicts), (1, 7), "round {round}: {results:?}");
        assert_eq!(store.load("acct").unwrap().unwrap().version(), current.version() + 1);
    }
}

#[test]
fn the_store_refuses_to_leave_a_halt_without_a_recorded_resume() {
    let store = InMemoryStateStore::new();
    let h = store.save(0, &halted()).unwrap();
    // Forge an Active successor with no resume record (only possible through the persistence boundary).
    let mut forged = h.to_record();
    forged.status = AccountStatus::Active;
    forged.halt = None;
    let forged = AccountState::from_record(forged).unwrap();
    let e = store.save(h.version(), &forged).unwrap_err();
    assert_eq!(e.code(), "STORE_ILLEGAL_TRANSITION", "{e}");
    assert_eq!(store.load("acct").unwrap().unwrap().status(), AccountStatus::Halted);
    // A real resume passes.
    let (resumed, _) = h.resume(human_approval("checked"), &snap("7000"), day(3)).unwrap();
    let saved = store.save(h.version(), &resumed).unwrap();
    assert_eq!(saved.status(), AccountStatus::Active);
    // Flattening -> Active is refused as well, and so is a rewritten resume history.
    let store2 = InMemoryStateStore::new();
    let f = store2.save(0, &flattening()).unwrap();
    let mut forged = f.to_record();
    forged.status = AccountStatus::Active;
    forged.halt = None;
    assert!(store2.save(f.version(), &AccountState::from_record(forged).unwrap()).is_err());
    let mut wiped = saved.to_record();
    wiped.resumes.clear();
    assert!(store.save(saved.version(), &AccountState::from_record(wiped).unwrap()).is_err(), "the resume history is append-only");
}

#[test]
fn an_unavailable_store_fails_closed_and_recovers() {
    let store = InMemoryStateStore::new();
    store.fail_next_calls(2);
    assert_eq!(store.load("a").unwrap_err().code(), "STORE_UNAVAILABLE");
    assert_eq!(store.save(0, &AccountState::new("a")).unwrap_err().code(), "STORE_UNAVAILABLE");
    assert!(store.save(0, &AccountState::new("a")).is_ok());
}
