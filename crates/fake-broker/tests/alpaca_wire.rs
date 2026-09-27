//! The fake Alpaca exchange plus the REAL `AlpacaAdapter`: request-shape fidelity, `client_order_id` idempotency, the
//! market clock/calendar's from-scratch accuracy on the specific historical dates the paper-pilot plan names, and
//! fault-injection basics. These are RIG-level tests (the wire boundary); the pipeline-level journey and drill tests
//! live in `crates/rebalancer-run/tests/alpaca_journey.rs` and `alpaca_drills.rs`.

use broker_adapters::alpaca::assets::AssetInfo;
use broker_adapters::alpaca::config::PAPER_BASE_URL;
use broker_adapters::alpaca::{AlpacaConfig, Environment};
use broker_adapters::transport::HttpMethod;
use broker_adapters::{BrokerAdapter, BrokerError, Dec, OrderRequest, PlaceOutcome, Side};
use fake_broker::alpaca::{calendar, AssetSpec, DEFAULT_ACCOUNT_NUMBER, DEFAULT_KEY_ID};
use fake_broker::alpaca_rig::AlpacaRig;
use fake_broker::Fault;

fn d(s: &str) -> Dec {
    Dec::parse(s).unwrap()
}

/// A rig whose clock starts inside a known regular session (2026-09-22 is an ordinary EDT Tuesday; the fake's
/// default start instant is a Sunday, deliberately -- a test that cares about the closed-market path sets its own
/// clock, and one that doesn't should not have to know or care what day the exchange's clock happens to start on).
fn rig() -> AlpacaRig {
    let r = AlpacaRig::with_config(|mut c| {
        c.own_tag_prefix = Some("rb1:".to_string());
        c
    });
    r.clock.set_nanos(nanos_at(2026, 9, 22, 15, 0));
    r
}

// ---------------------------------------------------------------------------------------------------------------
// Request/response shape fidelity
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_rig_serves_paper_credentials_and_a_pa_account_number() {
    let r = rig();
    assert_eq!(r.key_id, DEFAULT_KEY_ID);
    assert!(r.key_id.starts_with("PK"), "{}", r.key_id);
    assert_eq!(r.handle.account_number(), DEFAULT_ACCOUNT_NUMBER);
    assert!(r.handle.account_number().starts_with("PA"));
    let acct = r.adapter.verify_account().unwrap();
    assert_eq!(acct.account_number.as_deref(), Some(DEFAULT_ACCOUNT_NUMBER));
    assert_eq!(acct.cash, d("5000"));
}

#[test]
fn a_market_buy_round_trips_the_exact_shape_and_updates_cash_and_positions() {
    let r = rig();
    let out = r.adapter.place_order(&OrderRequest::market("rb1:t1", "SPY", Side::Buy, d("2"))).unwrap();
    let broker_order_id = match out {
        PlaceOutcome::Accepted { broker_order_id, sent, .. } => {
            assert_eq!((sent.quantity, sent.price), (d("2"), None));
            broker_order_id
        }
        other => panic!("{other:?}"),
    };
    let report = r.adapter.get_order(&broker_order_id).unwrap();
    assert_eq!(report.status, broker_adapters::OrderStatus::Filled);
    assert_eq!(report.executed_quantity, d("2"));
    assert_eq!(report.avg_price, Some(d("100")));
    // cash moved by exactly qty * price; the position is exactly what was bought
    assert_eq!(r.handle.cash(), d("4800"));
    assert_eq!(r.handle.position_qty("SPY"), d("2"));
    let post = r.adapter.get_positions().unwrap();
    assert_eq!(post.len(), 1);
    assert_eq!((post[0].symbol.as_str(), post[0].qty), ("SPY", d("2")));
    // exactly one POST reached the exchange
    assert_eq!(r.handle.applied(HttpMethod::Post, "/v2/orders").len(), 1);
}

#[test]
fn own_tag_prefix_is_honoured_end_to_end() {
    let r = rig();
    let out = r.adapter.place_order(&OrderRequest::market("rb1:t1", "SPY", Side::Buy, d("1"))).unwrap();
    let id = match out {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    let report = r.adapter.get_order(&id).unwrap();
    assert_eq!(report.tag.as_deref(), Some("rb1:t1"));
    // a foreign order (no prefix) is still reported, but tag is None
    let foreign_id = r.handle.add_foreign_order("SPY", Side::Buy, "1");
    let foreign = r.adapter.get_order(&foreign_id).unwrap();
    assert_eq!(foreign.tag, None);
}

// ---------------------------------------------------------------------------------------------------------------
// client_order_id idempotency: the adapter's own idempotency contract
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn resubmitting_the_same_client_order_id_never_places_a_second_order() {
    let r = rig();
    let first = r.adapter.place_order(&OrderRequest::market("rb1:once", "SPY", Side::Buy, d("1"))).unwrap();
    let first_id = match first {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    // resubmitting the SAME client_order_id (the adapter's own idempotency key) is refused by the venue, so the
    // adapter reports it as an unknown outcome telling the caller to look the existing order up by tag -- never a
    // second order.
    match r.adapter.place_order(&OrderRequest::market("rb1:once", "SPY", Side::Buy, d("1"))) {
        Ok(PlaceOutcome::UnknownOutcome { reason, .. }) => assert!(reason.contains("already exists") && reason.contains("look it up by tag"), "{reason}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(r.handle.orders_with_client_id("rb1:once").len(), 1, "exactly one order carries this tag");
    let found = r.adapter.find_orders_by_tag("rb1:once").unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].broker_order_id, first_id);
    // a THIRD resubmission still finds the same one order, deterministically (idempotent lookup)
    let found_again = r.adapter.find_orders_by_tag("rb1:once").unwrap();
    assert_eq!(found_again, found);
    assert_eq!(r.handle.position_qty("SPY"), d("1"), "only one fill happened");
}

#[test]
fn client_order_id_uniqueness_is_enforced_across_every_status_not_only_pending() {
    let r = rig();
    // Fill the first order fully, THEN try to reuse its tag: still refused (Alpaca's uniqueness is not scoped to
    // pending orders the way OANDA's client id reuse is).
    let out = r.adapter.place_order(&OrderRequest::market("rb1:done", "SPY", Side::Buy, d("1"))).unwrap();
    assert!(matches!(out, PlaceOutcome::Accepted { .. }));
    assert_eq!(r.handle.orders_with_client_id("rb1:done")[0].status, "filled");
    match r.adapter.place_order(&OrderRequest::market("rb1:done", "EFA", Side::Buy, d("1"))) {
        Ok(PlaceOutcome::UnknownOutcome { .. }) => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(r.handle.orders_with_client_id("rb1:done").len(), 1, "the second attempt never became a real order");
}

// ---------------------------------------------------------------------------------------------------------------
// Calendar / clock accuracy on the specific historical dates the paper-pilot plan names (from-scratch, not copied
// from `portfolio-parity/tests/t6_market_closed_retry.rs`, which tests a different, construction-level fake broker)
// ---------------------------------------------------------------------------------------------------------------

fn nanos_at(y: i32, m: u32, d: u32, h: u32, mi: u32) -> u64 {
    use chrono::{NaiveDate, TimeZone, Utc};
    let dt = Utc.from_utc_datetime(&NaiveDate::from_ymd_opt(y, m, d).unwrap().and_hms_opt(h, mi, 0).unwrap());
    dt.timestamp() as u64 * 1_000_000_000
}

#[test]
fn november_2019_saturday_action_day_is_closed_and_friday_and_monday_are_sessions() {
    // The first November 2019 session is Fri 11-01; the action day (the day after) is Saturday 11-02.
    assert!(calendar::is_session(chrono::NaiveDate::from_ymd_opt(2019, 11, 1).unwrap()));
    assert!(!calendar::is_session(chrono::NaiveDate::from_ymd_opt(2019, 11, 2).unwrap()), "Saturday");
    assert!(!calendar::is_session(chrono::NaiveDate::from_ymd_opt(2019, 11, 3).unwrap()), "Sunday");
    assert!(calendar::is_session(chrono::NaiveDate::from_ymd_opt(2019, 11, 4).unwrap()), "Monday");

    let r = rig();
    r.clock.set_nanos(nanos_at(2019, 11, 2, 15, 0)); // Saturday, 15:00Z
    let c = r.adapter.get_clock().unwrap();
    assert!(!c.is_open);
    assert_eq!(c.next_open, "2019-11-04T09:30:00-05:00", "the next session, EST (fall-back already happened Nov 3)");
}

#[test]
fn april_2021_good_friday_then_weekend_is_closed_and_the_session_either_side_is_open() {
    let gf = chrono::NaiveDate::from_ymd_opt(2021, 4, 2).unwrap();
    assert!(!calendar::is_session(gf), "Good Friday 2021-04-02");
    assert!(calendar::is_session(chrono::NaiveDate::from_ymd_opt(2021, 4, 1).unwrap()), "Thu 04-01");
    assert!(!calendar::is_session(chrono::NaiveDate::from_ymd_opt(2021, 4, 3).unwrap()), "Saturday");
    assert!(!calendar::is_session(chrono::NaiveDate::from_ymd_opt(2021, 4, 4).unwrap()), "Sunday");
    assert!(calendar::is_session(chrono::NaiveDate::from_ymd_opt(2021, 4, 5).unwrap()), "Monday 04-05");

    let r = rig();
    r.clock.set_nanos(nanos_at(2021, 4, 2, 15, 0)); // Good Friday, 15:00Z
    let c = r.adapter.get_clock().unwrap();
    assert!(!c.is_open);
    assert_eq!(c.next_open, "2021-04-05T09:30:00-04:00", "EDT is in force by April 5 2021 (DST started 03-14)");
}

#[test]
fn dst_transition_weeks_move_the_utc_offset_but_never_the_930_1600_local_session() {
    // 2026: DST begins Sun 2026-03-08, ends Sun 2026-11-01 (matches rebalancer-run/tests/run_slot.rs's own model).
    for (day, edt) in [("2026-03-06", false), ("2026-03-09", true), ("2026-10-30", true), ("2026-11-02", false)] {
        let d: chrono::NaiveDate = day.parse().unwrap();
        assert_eq!(calendar::edt_in_force(d), edt, "{day}");
        let (open, close) = calendar::session_bounds_utc(d);
        let want_open_hour = if edt { 13 } else { 14 }; // 09:30 ET -> 13:30Z (EDT) or 14:30Z (EST)
        assert_eq!((open.format("%H:%M").to_string()), format!("{want_open_hour:02}:30"), "{day}");
        let want_close_hour = if edt { 20 } else { 21 };
        assert_eq!(close.format("%H:%M").to_string(), format!("{want_close_hour:02}:00"), "{day}");
    }
}

#[test]
fn clock_is_open_exactly_during_the_regular_session_and_closes_at_16_00_local() {
    let r = rig();
    // 2026-09-22 is an ordinary EDT Tuesday (09:30 ET = 13:30Z, 16:00 ET = 20:00Z).
    r.clock.set_nanos(nanos_at(2026, 9, 22, 13, 29)); // 09:29 EDT: one minute before the open
    assert!(!r.adapter.get_clock().unwrap().is_open);
    r.clock.set_nanos(nanos_at(2026, 9, 22, 13, 30)); // 09:30 EDT: the open
    assert!(r.adapter.get_clock().unwrap().is_open);
    r.clock.set_nanos(nanos_at(2026, 9, 22, 19, 59)); // 15:59 EDT
    assert!(r.adapter.get_clock().unwrap().is_open);
    r.clock.set_nanos(nanos_at(2026, 9, 22, 20, 0)); // 16:00 EDT: closed
    let c = r.adapter.get_clock().unwrap();
    assert!(!c.is_open);
    assert_eq!(c.next_open, "2026-09-23T09:30:00-04:00", "tomorrow's open, still EDT");
    assert_eq!(c.next_close, "2026-09-23T16:00:00-04:00", "tomorrow's close");
}

#[test]
fn the_exchange_itself_refuses_to_fill_a_market_order_while_closed_not_only_the_adapters_preflight() {
    // The adapter's own client-side preflight already refuses a market order locally when its clock read says
    // closed (proven throughout `alpaca_drills.rs`'s market-closed drill), so a market order normally never even
    // reaches the exchange while shut. This test proves the EXCHANGE's own enforcement independently, by using
    // `allow_extended_hours` to bypass the adapter's local check (the documented way a market order legitimately
    // reaches a closed exchange) and confirming the fake does not silently fill it anyway.
    let r = AlpacaRig::with_config(|mut c| {
        c.own_tag_prefix = Some("rb1:".to_string());
        c.allow_extended_hours = true;
        c
    });
    r.clock.set_nanos(nanos_at(2026, 9, 19, 15, 0)); // 2026-09-19 is a Saturday: closed
    assert!(!r.adapter.get_clock().unwrap().is_open);
    let out = r.adapter.place_order(&OrderRequest::market("rb1:closed", "SPY", Side::Buy, d("1"))).unwrap();
    let id = match out {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    let report = r.adapter.get_order(&id).unwrap();
    assert_ne!(report.status, broker_adapters::OrderStatus::Filled, "the exchange must not fill a market order while it is closed: {report:?}");
    assert!(r.handle.position_qty("SPY").is_zero(), "nothing was actually bought");
    // The market opens; the still-resting order now fills (proves it genuinely stayed pending, not silently lost).
    r.clock.set_nanos(nanos_at(2026, 9, 21, 15, 0)); // the following Monday, EDT session
    r.handle.set_price("SPY", "101"); // a price move re-attempts every resting order
    let report = r.adapter.get_order(&id).unwrap();
    assert_eq!(report.status, broker_adapters::OrderStatus::Filled, "{report:?}");
}

#[test]
fn early_close_days_close_at_1300_local_on_the_calendar_endpoint() {
    let r = rig();
    // Day after Thanksgiving 2026: Thanksgiving is 2026-11-26 (4th Thursday), so 2026-11-27 is the early close.
    assert!(calendar::is_early_close(chrono::NaiveDate::from_ymd_opt(2026, 11, 27).unwrap()));
    assert!(calendar::is_early_close(chrono::NaiveDate::from_ymd_opt(2026, 12, 24).unwrap()), "Christmas Eve, a Thursday in 2026");
    let days = r.adapter.get_calendar("2026-11-25", "2026-11-30").unwrap();
    let fri = days.iter().find(|d| d.date == "2026-11-27").unwrap();
    assert_eq!(fri.close, "13:00");
    let mon = days.iter().find(|d| d.date == "2026-11-30").unwrap();
    assert_eq!(mon.close, "16:00");
    // Thanksgiving Day itself (2026-11-26) and the weekend are not in the list at all
    assert!(!days.iter().any(|d| d.date == "2026-11-26"), "Thanksgiving is a holiday, not a session");
    assert!(!days.iter().any(|d| d.date == "2026-11-28" || d.date == "2026-11-29"), "weekend");
}

// ---------------------------------------------------------------------------------------------------------------
// Asset table: whole-share vs fractional, over the real wire shape
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_five_pilot_etfs_default_to_fractionable_matching_the_adapters_own_documented_but_unverified_assumption() {
    let r = rig();
    for sym in ["SPY", "EFA", "IEF", "DBC", "VNQ"] {
        let a = r.adapter.asset(sym).unwrap_or_else(|| panic!("{sym}"));
        assert!(a.fractionable, "{sym}");
        assert_eq!(a.source, broker_adapters::alpaca::AssetSource::Api, "{sym}: loaded over the wire, not the builtin fallback");
    }
}

#[test]
fn a_whole_share_asset_rounds_down_to_integer_quantities_over_the_real_wire() {
    let r = rig();
    r.handle.set_asset(AssetSpec::whole_share("BRKA"), "1000");
    let info: AssetInfo = r.adapter.refresh_asset("BRKA").unwrap();
    assert!(!info.fractionable);
    let out = r.adapter.place_order(&OrderRequest::market("rb1:brka", "BRKA", Side::Buy, d("1.9"))).unwrap();
    match out {
        PlaceOutcome::Accepted { sent, .. } => assert_eq!(sent.quantity, d("1")),
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Fault injection basics (fuller drills live in rebalancer-run/tests/alpaca_drills.rs)
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn an_after_apply_timeout_on_placement_is_an_unknown_outcome_and_the_order_is_found_by_tag() {
    let r = rig();
    r.handle.inject_fault(Fault::timeout().after_apply().on_path(&r.handle.path("/v2/orders")));
    match r.adapter.place_order(&OrderRequest::market("rb1:lost", "SPY", Side::Buy, d("1"))) {
        Ok(PlaceOutcome::UnknownOutcome { .. }) => {}
        other => panic!("{other:?}"),
    }
    // the exchange DID apply it
    assert_eq!(r.handle.orders_with_client_id("rb1:lost").len(), 1);
    let found = r.adapter.find_orders_by_tag("rb1:lost").unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].status, broker_adapters::OrderStatus::Filled);
}

#[test]
fn a_before_apply_connect_failure_never_reaches_the_exchange() {
    let r = rig();
    r.handle.inject_fault(Fault::connect_failed().on_path(&r.handle.path("/v2/orders")));
    match r.adapter.place_order(&OrderRequest::market("rb1:never", "SPY", Side::Buy, d("1"))) {
        Err(BrokerError::Transport(e)) => assert!(e.request_definitely_not_sent()),
        other => panic!("{other:?}"),
    }
    assert!(r.handle.orders_with_client_id("rb1:never").is_empty());
}

#[test]
fn a_5xx_on_the_account_read_becomes_a_preflight_error_before_anything_is_sent() {
    let r = rig();
    r.handle.inject_fault(Fault::http(503).on_path(&r.handle.path("/v2/account")));
    match r.adapter.place_order(&OrderRequest::market("rb1:x", "SPY", Side::Buy, d("1"))) {
        Err(BrokerError::Preflight(_)) => {}
        other => panic!("{other:?}"),
    }
    assert!(r.handle.applied(HttpMethod::Post, "/v2/orders").is_empty());
}

#[test]
fn a_malformed_body_on_placement_is_an_unknown_outcome_never_a_false_rejection() {
    let r = rig();
    r.handle.inject_fault(Fault::malformed_body().on_path(&r.handle.path("/v2/orders")));
    match r.adapter.place_order(&OrderRequest::market("rb1:garbled", "SPY", Side::Buy, d("1"))) {
        Ok(PlaceOutcome::UnknownOutcome { .. }) => {}
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_rate_limit_fault_is_reported_as_rate_limited_with_the_retry_after_header() {
    let r = rig();
    r.handle.inject_fault(Fault::rate_limit().on_path(&r.handle.path("/v2/account")));
    match r.adapter.place_order(&OrderRequest::market("rb1:x", "SPY", Side::Buy, d("1"))) {
        Err(BrokerError::RateLimited { retry_after_secs, .. }) => assert_eq!(retry_after_secs, Some(1)),
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Restart / no persistent state
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn restarting_the_adapter_keeps_no_state_and_the_rebuilt_one_still_finds_the_same_order_by_tag() {
    let mut r = rig();
    let out = r.adapter.place_order(&OrderRequest::market("rb1:persist", "SPY", Side::Buy, d("1"))).unwrap();
    let id = match out {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("{other:?}"),
    };
    r.restart_adapter();
    let found = r.adapter.find_orders_by_tag("rb1:persist").unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].broker_order_id, id);
    // and a fresh adapter still refuses to reuse the tag
    match r.adapter.place_order(&OrderRequest::market("rb1:persist", "EFA", Side::Buy, d("1"))) {
        Ok(PlaceOutcome::UnknownOutcome { .. }) => {}
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Invariants
// ---------------------------------------------------------------------------------------------------------------

#[test]
fn invariants_hold_after_a_sequence_of_buys_and_a_sell() {
    let r = rig();
    r.adapter.place_order(&OrderRequest::market("rb1:1", "SPY", Side::Buy, d("3"))).unwrap();
    r.handle.set_price("SPY", "110");
    r.adapter.place_order(&OrderRequest::market("rb1:2", "SPY", Side::Buy, d("1"))).unwrap();
    r.handle.set_price("SPY", "105");
    r.adapter.place_order(&OrderRequest::market("rb1:3", "SPY", Side::Sell, d("2"))).unwrap();
    r.handle.assert_invariants();
    assert_eq!(r.handle.position_qty("SPY"), d("2"));
}

#[test]
fn a_paper_only_config_still_uses_the_paper_host_against_the_fake() {
    let cfg = AlpacaConfig::new(Environment::Paper, PAPER_BASE_URL).unwrap();
    assert_eq!(cfg.base_url, PAPER_BASE_URL);
}
