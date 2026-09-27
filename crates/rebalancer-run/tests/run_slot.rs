//! The daily run slot per sleeve kind (slice S-4 of the paper-pilot plan): the ETF sleeve runs at 15:00:00Z, inside US
//! regular trading hours in BOTH daylight-saving regimes; a crypto-only account keeps 00:10:00Z; an account is one
//! slot, never one per sleeve; the run key stays unique per (account, slot) and stable within a day.
//!
//! The market-hours arithmetic is checked against an independent, hand-written US Eastern-time model (second Sunday of
//! March 02:00 local to first Sunday of November 02:00 local, the rule in force since 2007) for every calendar day of
//! thirty years, weekends and holidays included (the driver does not special-case them: a run on a closed day either
//! has nothing pending or has its orders refused as "market closed", see `acted_semantics.rs`).

mod common;

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use common::harness::*;
use common::*;
use rebalancer_run::data::{SleeveKind, SleeveSpec};
use rebalancer_run::driver::{
    account_run_slot_utc, find_due_runs, run_slot_on, run_slot_utc, ActiveAccount, InMemoryAccountSource, DAILY_RUN_HOUR_UTC,
    DAILY_RUN_MINUTE_UTC, MARKET_HOURS_RUN_HOUR_UTC, MARKET_HOURS_RUN_MINUTE_UTC,
};
use rebalancer_run::record::{ExecutionMode, RunKey};

fn etf(share: &str) -> SleeveSpec {
    SleeveSpec { id: "etf".into(), kind: SleeveKind::EtfTrend, share: d(share), venue: "alpaca".into(), asset_class: "us_etf".into(), quote: "USD".into() }
}

fn acct(id: &str, sleeves: Vec<SleeveSpec>) -> ActiveAccount {
    ActiveAccount {
        account_id: id.into(),
        tenant_id: "tenant".into(),
        mandate: mandate_from(crypto_mandate_json()),
        envelope: active_envelope(),
        plan_approved: true,
        sleeves,
        mode: ExecutionMode::Assisted,
    }
}

fn due_at(a: &ActiveAccount, now: DateTime<Utc>) -> Vec<rebalancer_run::driver::RunSpec> {
    let src = InMemoryAccountSource::new();
    src.set_accounts(vec![a.clone()]);
    find_due_runs(&src, now).unwrap()
}

fn key_of(spec: &rebalancer_run::driver::RunSpec) -> RunKey {
    let ids: Vec<&str> = spec.sleeves.iter().map(|s| s.id.as_str()).collect();
    RunKey::new(&spec.account_id, spec.scheduled_for, &ids)
}

// ---------------------------------------------------------------------------------------------------------------
// An independent US Eastern-time model
// ---------------------------------------------------------------------------------------------------------------

fn nth_sunday(y: i32, m: u32, n: i64) -> NaiveDate {
    let mut d = NaiveDate::from_ymd_opt(y, m, 1).unwrap();
    while d.weekday() != Weekday::Sun {
        d += Duration::days(1);
    }
    d + Duration::days(7 * (n - 1))
}

/// Is US daylight saving time (EDT, UTC-4) in force at the UTC instant? Rule since 2007: from the second Sunday of
/// March at 02:00 local standard time (07:00Z) to the first Sunday of November at 02:00 local daylight time (06:00Z).
fn edt_in_force(t: DateTime<Utc>) -> bool {
    let y = t.year();
    let start = nth_sunday(y, 3, 2).and_hms_opt(7, 0, 0).unwrap().and_utc();
    let end = nth_sunday(y, 11, 1).and_hms_opt(6, 0, 0).unwrap().and_utc();
    t >= start && t < end
}

/// Local New York wall-clock minutes since midnight of a UTC instant.
fn ny_minutes(t: DateTime<Utc>) -> i64 {
    let off = if edt_in_force(t) { -4 } else { -5 };
    let local = t + Duration::hours(off);
    i64::from(local.hour()) * 60 + i64::from(local.minute())
}

const OPEN: i64 = 9 * 60 + 30;
const CLOSE: i64 = 16 * 60;
const EARLY_CLOSE: i64 = 13 * 60;

fn sweep() -> impl Iterator<Item = NaiveDate> {
    let from = NaiveDate::from_ymd_opt(2007, 1, 1).unwrap();
    let to = NaiveDate::from_ymd_opt(2036, 12, 31).unwrap();
    (0..=(to - from).num_days()).map(move |i| from + Duration::days(i))
}

// ---------------------------------------------------------------------------------------------------------------
// The slots
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn slot_constants_and_per_kind_slots_are_pinned() {
    assert_eq!((DAILY_RUN_HOUR_UTC, DAILY_RUN_MINUTE_UTC), (0, 10), "crypto keeps 00:10Z");
    assert_eq!((MARKET_HOURS_RUN_HOUR_UTC, MARKET_HOURS_RUN_MINUTE_UTC), (15, 0), "ETF: 15:00Z");
    assert_eq!(run_slot_utc(SleeveKind::CryptoTrend), (0, 10));
    assert_eq!(run_slot_utc(SleeveKind::EtfTrend), (15, 0));
}

#[test]
fn an_account_has_one_slot_the_market_hours_one_if_any_sleeve_needs_the_market() {
    assert_eq!(account_run_slot_utc(&[]), None, "no sleeve, no slot");
    assert_eq!(account_run_slot_utc(&[crypto_sleeve("1")]), Some((0, 10)));
    assert_eq!(account_run_slot_utc(&[etf("1")]), Some((15, 0)));
    assert_eq!(account_run_slot_utc(&[crypto_sleeve("0.5"), etf("0.5")]), Some((15, 0)), "a mixed account runs ONCE, at the market-hours slot");
    assert_eq!(account_run_slot_utc(&[etf("0.5"), crypto_sleeve("0.5")]), Some((15, 0)), "order of the sleeves does not matter");
    let day = NaiveDate::from_ymd_opt(2026, 10, 2).unwrap();
    assert_eq!(run_slot_on(&[etf("1")], day), Some(at("2026-10-02T15:00:00Z")));
    assert_eq!(run_slot_on(&[crypto_sleeve("1")], day), Some(at("2026-10-02T00:10:00Z")));
    assert_eq!(run_slot_on(&[], day), None);
}

#[test]
fn the_etf_slot_is_inside_regular_hours_in_both_dst_regimes_on_every_day_of_thirty_years() {
    let (mut summer, mut winter) = (0u32, 0u32);
    for day in sweep() {
        let slot = run_slot_on(&[etf("1")], day).unwrap();
        assert_eq!(slot.date_naive(), day);
        let local = ny_minutes(slot);
        if edt_in_force(slot) {
            summer += 1;
            assert_eq!(local, 11 * 60, "{day}: 15:00Z is 11:00 EDT");
        } else {
            winter += 1;
            assert_eq!(local, 10 * 60, "{day}: 15:00Z is 10:00 EST");
        }
        assert!(local >= OPEN + 30, "{day}: at least 30 minutes after the open (local minute {local})");
        assert!(local <= EARLY_CLOSE - 90, "{day}: at least 90 minutes before even an early (13:00) close (local minute {local})");
        assert!(local < CLOSE, "{day}");
    }
    assert!(summer > 5000 && winter > 3000, "both regimes were exercised ({summer} summer days, {winter} winter days)");
}

#[test]
fn the_dst_transition_weeks_keep_the_slot_at_15z_and_move_only_the_local_hour() {
    // 2019: DST began Sun 03-10 and ended Sun 11-03. 2026: began Sun 03-08, ends Sun 11-01.
    for (day, local_hour) in [
        ("2019-03-08", 10), ("2019-03-11", 11), ("2019-11-01", 11), ("2019-11-04", 10),
        ("2026-03-06", 10), ("2026-03-09", 11), ("2026-10-30", 11), ("2026-11-02", 10),
    ] {
        let day: NaiveDate = day.parse().unwrap();
        let slot = run_slot_on(&[etf("1")], day).unwrap();
        assert_eq!(slot, Utc.from_utc_datetime(&day.and_hms_opt(15, 0, 0).unwrap()));
        assert_eq!(ny_minutes(slot), local_hour * 60, "{day}");
    }
}

#[test]
fn the_crypto_slot_stays_at_0010z_every_day_of_thirty_years() {
    for day in sweep() {
        assert_eq!(run_slot_on(&[crypto_sleeve("1")], day), Some(at(&format!("{day}T00:10:00Z"))), "{day}");
    }
}

// ---------------------------------------------------------------------------------------------------------------
// find_due_runs at the slot boundaries, for every day of thirty years
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn due_runs_appear_exactly_at_the_slot_and_carry_that_days_slot_for_thirty_years() {
    let etf_only = acct("etf-acct", vec![etf("1")]);
    let crypto_only = acct("crypto-acct", vec![crypto_sleeve("1")]);
    let mixed = acct("mixed-acct", vec![etf("0.5"), crypto_sleeve("0.5")]);
    let at_time = |day: NaiveDate, hms: (u32, u32, u32)| Utc.from_utc_datetime(&day.and_hms_opt(hms.0, hms.1, hms.2).unwrap());
    for day in sweep() {
        for (a, slot_hms) in [(&etf_only, (15, 0, 0)), (&mixed, (15, 0, 0)), (&crypto_only, (0, 10, 0))] {
            let slot = at_time(day, slot_hms);
            let before = slot - Duration::seconds(1);
            assert!(due_at(a, before).is_empty(), "{day} {}: nothing due one second before the slot", a.account_id);
            for now in [slot, slot + Duration::seconds(1), at_time(day, (18, 0, 0)), at_time(day, (23, 59, 59))] {
                if now < slot {
                    continue;
                }
                let due = due_at(a, now);
                assert_eq!(due.len(), 1, "{day} {}: due at {now}", a.account_id);
                assert_eq!(due[0].scheduled_for, slot, "{day} {}: the run is scheduled for THAT day's slot", a.account_id);
                assert_eq!(due[0].trading_day, day, "{day} {}", a.account_id);
            }
        }
        // a crypto-only account is also due at 15:00Z, still on ITS OWN 00:10Z slot (not moved to the market slot)
        let due = due_at(&crypto_only, at_time(day, (15, 0, 0)));
        assert_eq!(due[0].scheduled_for, at_time(day, (0, 10, 0)), "{day}");
        // the ETF-only account is not due at the crypto slot
        assert!(due_at(&etf_only, at_time(day, (0, 10, 0))).is_empty(), "{day}: the ETF sleeve is NOT run at 00:10Z any more");
    }
}

#[test]
fn run_keys_are_unique_per_account_and_slot_and_stable_within_a_day() {
    let etf_only = acct("etf-acct", vec![etf("1")]);
    let mixed = acct("mixed-acct", vec![etf("0.5"), crypto_sleeve("0.5")]);
    let crypto_only = acct("crypto-acct", vec![crypto_sleeve("1")]);
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut per_day: BTreeMap<NaiveDate, usize> = BTreeMap::new();
    for day in sweep() {
        let times = [
            Utc.from_utc_datetime(&day.and_hms_opt(15, 0, 0).unwrap()),
            Utc.from_utc_datetime(&day.and_hms_opt(15, 5, 0).unwrap()),
            Utc.from_utc_datetime(&day.and_hms_opt(23, 59, 59).unwrap()),
        ];
        for a in [&etf_only, &mixed, &crypto_only] {
            let keys: Vec<RunKey> = times.iter().map(|t| key_of(&due_at(a, *t)[0])).collect();
            assert!(keys.windows(2).all(|w| w[0] == w[1]), "{day} {}: the key does not change within the day (idempotent)", a.account_id);
            assert!(seen.insert(keys[0].canonical()), "{day} {}: duplicate key {}", a.account_id, keys[0].canonical());
            *per_day.entry(day).or_default() += 1;
        }
    }
    assert_eq!(seen.len(), sweep().count() * 3, "one distinct key per (account, day)");
    // The mixed account has exactly ONE key per day, not one per sleeve.
    let day = NaiveDate::from_ymd_opt(2026, 10, 2).unwrap();
    let all_day: Vec<RunKey> = (0..24).map(|h| Utc.from_utc_datetime(&day.and_hms_opt(h, 30, 0).unwrap())).filter_map(|t| due_at(&mixed, t).into_iter().next().map(|s| key_of(&s))).collect();
    assert!(!all_day.is_empty() && all_day.windows(2).all(|w| w[0] == w[1]), "a mixed account is one run per day");
    assert_eq!(all_day[0].canonical(), "mixed-acct|2026-10-02T15:00:00+00:00|crypto+etf");
    // Same account id and day but different sleeve sets / slots never collide.
    let a = key_of(&due_at(&acct("same", vec![etf("1")]), at("2026-10-02T15:00:00Z"))[0]);
    let b = key_of(&due_at(&acct("same", vec![crypto_sleeve("1")]), at("2026-10-02T15:00:00Z"))[0]);
    assert_ne!(a, b);
}

#[test]
fn weekends_and_market_holidays_are_not_skipped_by_the_driver() {
    // The driver runs every calendar day; the pipeline (and the venue's clock) decide what a closed day means.
    let a = acct("etf-acct", vec![etf("1")]);
    for day in ["2020-05-02", "2020-05-03", "2019-11-28", "2019-12-25", "2019-07-04", "2021-01-01", "2021-04-02", "2019-11-29"] {
        let due = due_at(&a, at(&format!("{day}T15:00:00Z")));
        assert_eq!(due.len(), 1, "{day}");
        assert_eq!(due[0].scheduled_for, at(&format!("{day}T15:00:00Z")), "{day}");
    }
}
