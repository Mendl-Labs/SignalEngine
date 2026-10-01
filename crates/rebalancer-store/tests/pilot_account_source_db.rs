//! `PgAccountSource` against a real database (paper-pilot slice S-5): enumeration of the pilot allow-list, every
//! refusal that needs the database to prove (as opposed to `account_source.rs`'s own pure-data unit tests), and that
//! it builds NO broker (the descriptor alone). Env-gated: see `tests/common/mod.rs`.

mod common;

use common::{create, Opts, TestDb};
use rebalancer_run::driver::AccountSource;
use rebalancer_run::record::ExecutionMode;
use rebalancer_store::{PgAccountSource, PilotAllowList};
use uuid::Uuid;

fn source(db: &TestDb, allow: PilotAllowList) -> PgAccountSource {
    PgAccountSource::new(db.pool(4), allow).expect("account source")
}

#[test]
fn the_happy_path_enumerates_the_allow_listed_account_with_no_broker_built() {
    let Some(db) = create("account_source_happy_path", Opts::default()) else { return };
    let pilot = db.seed_pilot("assisted");
    let allow = PilotAllowList::new(pilot.tenant, [pilot.account]).unwrap();
    let src = source(&db, allow);

    let enumeration = src.enumerate().expect("enumerate");
    assert!(enumeration.exclusions.is_empty(), "{:?}", enumeration.exclusions);
    assert_eq!(enumeration.accounts.len(), 1);
    let a = &enumeration.accounts[0];
    assert_eq!(a.active.account_id, pilot.account.to_string());
    assert_eq!(a.active.tenant_id, pilot.tenant.to_string());
    assert_eq!(a.active.mode, ExecutionMode::Assisted);
    assert_eq!(a.plan_id, pilot.plan);
    assert_eq!(a.active.sleeves.len(), 1);
    assert_eq!(a.active.sleeves[0].id, "etf");

    // AccountSource::active_accounts (the trait the driver calls) returns the same descriptor, structurally.
    let via_trait = src.active_accounts().expect("active_accounts");
    assert_eq!(via_trait.len(), 1);
    assert_eq!(via_trait[0], a.active);

    // No broker or data connection exists anywhere in the descriptor's type: this is a compile-time property, but
    // assert the intent in words too -- ActiveAccount carries only ids, the mandate, the envelope and sleeve specs.
    assert!(a.credential_label.starts_with("label-"));
}

#[test]
fn a_second_paper_orders_row_grants_orders_only_when_the_mandate_allows_them() {
    let Some(db) = create("account_source_paper_orders", Opts::default()) else { return };
    let pilot = db.seed_pilot("paper_orders");
    let src = source(&db, PilotAllowList::new(pilot.tenant, [pilot.account]).unwrap());
    let e = src.enumerate().unwrap();
    assert_eq!(e.accounts[0].active.mode, ExecutionMode::Live, "L2 + paper_orders + place_orders granted = the order-placing pipeline mode");
}

#[test]
fn orders_not_granted_by_the_mandate_refuses_the_account_even_with_a_paper_orders_plan() {
    // The plan-guard trigger already refuses AUTHORING a paper_orders plan without place_orders granted (proved in
    // databaseschema-internal's own constraints.sql), so this exact combination cannot arise on the normal insert
    // path. It CAN arise if the row predates a later, tighter mandate edit, so this seeds the row with triggers
    // suspended (superuser only) to prove the ACCOUNT SOURCE has its own, independent copy of the same rule -- true
    // defense in depth, not "the database happens to have caught it".
    let Some(db) = create("account_source_orders_not_granted", Opts::default()) else { return };
    let tenant = db.seed_tenant("T");
    let account = db.seed_credential(tenant, "alpaca_paper", true);
    let (mandate, _) = db.seed_mandate(tenant, account, 1, "active", "L2", true, false); // may NOT place_orders
    db.seed_plan_bypassing_triggers(tenant, account, mandate, 1, "paper_orders", "alpaca", "etf_trend", None);
    let e = source(&db, PilotAllowList::new(tenant, [account]).unwrap()).enumerate().unwrap();
    assert!(e.accounts.is_empty());
    assert_eq!(e.exclusions[0].reason.code(), "PILOT_ORDERS_NOT_GRANTED");
}

#[test]
fn an_inactive_mandate_excludes_the_account() {
    let Some(db) = create("account_source_inactive_mandate", Opts::default()) else { return };
    for status in ["draft", "superseded", "revoked", "expired"] {
        let tenant = db.seed_tenant(&format!("T-{status}"));
        let account = db.seed_credential(tenant, "alpaca_paper", true);
        let (mandate, _) = db.seed_mandate(tenant, account, 1, status, "L2", true, true);
        // The plan trigger itself requires an ACTIVE mandate, so a non-active mandate cannot even carry a plan --
        // this proves the interlock exists at BOTH layers (the DB trigger and the account source).
        let err = db.try_seed_plan(tenant, account, mandate, 1, "assisted").unwrap_err();
        assert!(err.contains("not an active, signed mandate"), "{status}: {err}");
        let e = source(&db, PilotAllowList::new(tenant, [account]).unwrap()).enumerate().unwrap();
        assert_eq!(e.exclusions[0].reason.code(), "PILOT_NO_ACTIVE_MANDATE", "{status}");
    }
}

#[test]
fn an_unsigned_mandate_excludes_the_account() {
    // The plan-guard trigger itself refuses authoring a plan against an unsigned mandate (status active AND
    // granted_at IS NULL cannot both hold on the normal insert path), so build the plan bypassing triggers to prove
    // the ACCOUNT SOURCE has its own, independent signed-ness check (defense in depth).
    let Some(db) = create("account_source_unsigned_mandate", Opts::default()) else { return };
    let tenant = db.seed_tenant("T");
    let account = db.seed_credential(tenant, "alpaca_paper", true);
    let (mandate, _) = db.seed_mandate(tenant, account, 1, "active", "L2", false, true);
    db.seed_plan_bypassing_triggers(tenant, account, mandate, 1, "assisted", "alpaca", "etf_trend", None);
    let e = source(&db, PilotAllowList::new(tenant, [account]).unwrap()).enumerate().unwrap();
    assert_eq!(e.exclusions[0].reason.code(), "PILOT_MANDATE_NOT_SIGNED");
}

#[test]
fn a_mandate_past_its_review_date_still_enumerates_as_active_the_pipeline_computes_the_standing() {
    let Some(db) = create("account_source_expired_review", Opts::default()) else { return };
    let tenant = db.seed_tenant("T");
    let account = db.seed_credential(tenant, "alpaca_paper", true);
    let (mandate, _) = db.seed_mandate(tenant, account, 1, "active", "L2", true, true);
    db.exec(&format!("UPDATE mandates SET review_by = now() - interval '1 day' WHERE id = '{mandate}'"));
    db.seed_plan(tenant, account, mandate, 1, "assisted");
    let e = source(&db, PilotAllowList::new(tenant, [account]).unwrap()).enumerate().unwrap();
    assert!(e.exclusions.is_empty(), "{:?}", e.exclusions);
    assert!(e.accounts[0].active.envelope.review_by < chrono::Utc::now(), "the real, past review_by is handed over as-is");
}

#[test]
fn a_missing_plan_excludes_the_account() {
    let Some(db) = create("account_source_missing_plan", Opts::default()) else { return };
    let tenant = db.seed_tenant("T");
    let account = db.seed_credential(tenant, "alpaca_paper", true);
    db.seed_mandate(tenant, account, 1, "active", "L2", true, true);
    let e = source(&db, PilotAllowList::new(tenant, [account]).unwrap()).enumerate().unwrap();
    assert_eq!(e.exclusions[0].reason.code(), "PILOT_NO_PLAN");
}

#[test]
fn a_non_allow_listed_account_is_never_read_even_though_it_is_fully_valid() {
    let Some(db) = create("account_source_not_allow_listed", Opts::default()) else { return };
    let pilot_in = db.seed_pilot("assisted");
    let pilot_out = db.seed_pilot("assisted"); // fully valid, but NOT on the allow-list
    let e = source(&db, PilotAllowList::new(pilot_in.tenant, [pilot_in.account]).unwrap()).enumerate().unwrap();
    assert_eq!(e.accounts.len(), 1);
    assert_eq!(e.accounts[0].active.account_id, pilot_in.account.to_string());
    assert_ne!(pilot_out.account, pilot_in.account);
    // An allow-list naming an id that exists under a DIFFERENT tenant than the one on the list finds nothing.
    let cross = source(&db, PilotAllowList::new(pilot_in.tenant, [pilot_out.account]).unwrap()).enumerate().unwrap();
    assert_eq!(cross.exclusions[0].reason.code(), "PILOT_CREDENTIAL_NOT_FOUND");
}

#[test]
fn an_allow_listed_id_that_does_not_exist_is_reported_not_silently_dropped() {
    let Some(db) = create("account_source_nonexistent", Opts::default()) else { return };
    let tenant = db.seed_tenant("T");
    let ghost = Uuid::new_v4();
    let e = source(&db, PilotAllowList::new(tenant, [ghost]).unwrap()).enumerate().unwrap();
    assert!(e.accounts.is_empty());
    assert_eq!(e.exclusions, vec![rebalancer_store::Exclusion { account_id: ghost, reason: rebalancer_store::ExclusionReason::CredentialNotFound }]);
}

#[test]
fn a_non_paper_or_non_alpaca_credential_is_excluded_by_the_account_source_too() {
    let Some(db) = create("account_source_bad_credential", Opts::default()) else { return };
    let tenant = db.seed_tenant("T");
    let live = db.seed_credential(tenant, "alpaca", false);
    let (m1, _) = db.seed_mandate(tenant, live, 1, "active", "L2", true, true);
    let e1 = db.try_seed_plan(tenant, live, m1, 1, "assisted").unwrap_err();
    assert!(e1.contains("not a paper/testnet credential"));
    let kraken = db.seed_credential(tenant, "kraken", true);
    let (m2, _) = db.seed_mandate(tenant, kraken, 1, "active", "L2", true, true);
    let e2 = db.try_seed_plan(tenant, kraken, m2, 1, "assisted").unwrap_err();
    assert!(e2.contains("not Alpaca"));
    // Both trigger-refused at insert time; the account source correctly reports "no plan" for either.
    let e = source(&db, PilotAllowList::new(tenant, [live, kraken]).unwrap()).enumerate().unwrap();
    assert_eq!(e.exclusions.len(), 2);
    assert!(e.exclusions.iter().all(|x| x.reason.code() == "PILOT_NO_PLAN" || x.reason.code() == "PILOT_NOT_PAPER_CREDENTIAL" || x.reason.code() == "PILOT_NOT_ALPACA"));
}

#[test]
fn a_proposal_derived_plan_on_the_same_account_excludes_it() {
    let Some(db) = create("account_source_strategy_plan", Opts::default()) else { return };
    let pilot = db.seed_pilot("assisted");
    db.exec(&format!(
        "INSERT INTO proposals (id, tenant_id, account_id, mandate_id, mandate_version, status, body, body_hash, generated_by) \
         VALUES (gen_random_uuid(), '{t}', '{a}', '{m}', 1, 'draft', '{{}}', '{h}', 'test');",
        t = pilot.tenant,
        a = pilot.account,
        m = pilot.mandate,
        h = "d".repeat(64)
    ));
    let prop: String = db.scalar(&format!("SELECT id::text AS v FROM proposals WHERE account_id = '{}' LIMIT 1", pilot.account)).unwrap();
    db.exec(&format!(
        "INSERT INTO strategy_plans (tenant_id, proposal_id, account_id, mandate_id, mandate_version, body, body_hash) \
         VALUES ('{t}', '{p}', '{a}', '{m}', 1, '{{}}', '{h}');",
        t = pilot.tenant,
        p = prop,
        a = pilot.account,
        m = pilot.mandate,
        h = "e".repeat(64)
    ));
    let e = source(&db, PilotAllowList::new(pilot.tenant, [pilot.account]).unwrap()).enumerate().unwrap();
    assert_eq!(e.exclusions[0].reason.code(), "PILOT_STRATEGY_PLAN_PRESENT");
}

#[test]
fn a_plan_bound_to_a_stale_mandate_version_is_excluded_once_a_new_version_is_active() {
    // Models the real sequence: author the plan while v1 is active, then a NEW mandate version supersedes it
    // (the mandate table's own trigger allows exactly this transition) -- the plan is now stale, never rebound.
    let Some(db) = create("account_source_stale_plan_version", Opts::default()) else { return };
    let tenant = db.seed_tenant("T");
    let account = db.seed_credential(tenant, "alpaca_paper", true);
    let (m1, _) = db.seed_mandate(tenant, account, 1, "active", "L2", true, true);
    let plan = db.seed_plan(tenant, account, m1, 1, "assisted");
    db.exec(&format!("UPDATE mandates SET status = 'superseded' WHERE id = '{m1}'"));
    let (_m2, _) = db.seed_mandate(tenant, account, 2, "active", "L2", true, true);
    let e = source(&db, PilotAllowList::new(tenant, [account]).unwrap()).enumerate().unwrap();
    assert_eq!(e.exclusions[0].reason.code(), "PILOT_PLAN_BOUND_TO_OTHER_MANDATE");
    let _ = plan;
}

#[test]
fn tenant_a_cannot_see_tenant_bs_pilot_even_with_the_same_credential_and_mandate_ids_reused_pattern() {
    let Some(db) = create("account_source_tenant_scoping", Opts::default()) else { return };
    let pilot_a = db.seed_pilot("assisted");
    let pilot_b = db.seed_pilot("paper_orders");
    // An allow-list of tenant A naming tenant B's account id: refused, never returns tenant B's data.
    let e = source(&db, PilotAllowList::new(pilot_a.tenant, [pilot_b.account]).unwrap()).enumerate().unwrap();
    assert!(e.accounts.is_empty());
    assert_eq!(e.exclusions[0].reason.code(), "PILOT_CREDENTIAL_NOT_FOUND");
    // A well-formed allow-list of tenant A only ever returns tenant A's account.
    let e = source(&db, PilotAllowList::new(pilot_a.tenant, [pilot_a.account]).unwrap()).enumerate().unwrap();
    assert_eq!(e.accounts.len(), 1);
    assert_eq!(e.accounts[0].active.tenant_id, pilot_a.tenant.to_string());
}

#[test]
fn the_service_role_can_enumerate_without_any_secret_credential_column_privilege() {
    // Proves the account source's own SQL selects no *_encrypted column: it runs fine as the restricted role that
    // has no SELECT privilege on those columns at all (see tests/common/mod.rs's role grants).
    let Some(db) = create("account_source_role_privileges", Opts::default()) else { return };
    let pilot = db.seed_pilot("assisted");
    let src = PgAccountSource::new(db.pool(4), PilotAllowList::new(pilot.tenant, [pilot.account]).unwrap()).unwrap();
    let e = src.enumerate().expect("enumerate must succeed with only the non-secret credential columns granted");
    assert_eq!(e.accounts.len(), 1);
}

#[test]
fn last_exclusions_and_the_plan_provenance_registry_are_kept_current_on_each_enumeration() {
    let Some(db) = create("account_source_provenance", Opts::default()) else { return };
    let pilot = db.seed_pilot("assisted");
    let registry = std::sync::Arc::new(rebalancer_store::PlanProvenanceRegistry::new());
    let src = PgAccountSource::new(db.pool(4), PilotAllowList::new(pilot.tenant, [pilot.account]).unwrap()).unwrap().with_plan_provenance(registry.clone());
    assert!(src.last_exclusions().is_empty());
    src.enumerate().unwrap();
    assert!(src.last_exclusions().is_empty());
    let p = registry.get(&pilot.account.to_string()).expect("provenance registered after enumeration");
    assert_eq!(p.plan_id, pilot.plan);
    assert_eq!(p.venue_environment.as_str(), "paper");
    assert_eq!(p.origin.as_str(), "owner_pilot");

    // Break it (drop the plan by revoking the mandate cannot be done -- append-only -- so instead widen the
    // allow-list to a ghost id) and enumerate again: exclusions and the registry both refresh, they do not merge.
    let ghost = Uuid::new_v4();
    let src2 = PgAccountSource::new(db.pool(4), PilotAllowList::new(pilot.tenant, [ghost]).unwrap()).unwrap().with_plan_provenance(registry.clone());
    src2.enumerate().unwrap();
    assert_eq!(src2.last_exclusions().len(), 1);
    assert!(registry.get(&pilot.account.to_string()).is_none(), "a fresh enumeration replaces the whole registry, it does not accumulate stale accounts");
}
