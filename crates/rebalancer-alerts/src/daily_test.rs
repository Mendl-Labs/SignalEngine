//! Proof that the alert channel works (P0-04 follow-up to the S-3 chart change; IMPLEMENTATION_PLAN_V2 Phase 0):
//!
//! * [`DailyTestAlert`]: ONE `ALERT_DAILY_TEST` a day at a configured UTC time (`ALERT_TEST_UTC`, `HH:MM`, default
//!   `13:00`; `off` disables it). It goes through the same path as every real alert (`rebalancer_alerts` row ->
//!   `DeliveryWorker` -> e-mail), so a day without it means a halt alert would not have arrived either. Severity
//!   `warning`, so platform channels receive it (they take warning and critical only). Its dedupe key carries the
//!   date (`platform:daily_test:<YYYY-MM-DD>`): the delivery ledger's `(channel_id, dedupe_key)` key lets exactly one
//!   e-mail per channel per day through, even when a restart raises the row twice. A failed insert is retried on the
//!   next tick (the day is marked done only once the row is persisted).
//! * [`delivery_problems`] / [`delivery_unconfigured_alert`]: at startup, ONE `ALERT_DELIVERY_UNCONFIGURED`
//!   (Critical) when the service would not be able to tell anyone about a halt: delivery is off
//!   (`ALERTS_ENABLED` is not `true`), the sender is disabled (no `RESEND_API_KEY` / `ALERT_FROM_EMAIL`), there is no
//!   verified, enabled alert channel (or the channels cannot be read), or the dead-man's heartbeat is off. The S-3
//!   chart change made the Secret refs optional, so a typo in a Secret name gives a running pod that is silently
//!   deaf; this row makes that state visible in `rebalancer_alerts` (and the Record page) and in the log, and it is
//!   delivered by any channel that does work.
//!
//! Neither can stop a run or change what a run does.

use std::sync::Mutex;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use rebalancer_run::record::{Alert, AlertCode, AlertSeverity};
use rebalancer_run::stores::Notifier;

use crate::heartbeat::HEARTBEAT_ACCOUNT;
use crate::sender::SENDER_DISABLED_NAME;

pub const ENV_ALERT_TEST_UTC: &str = "ALERT_TEST_UTC";
/// 13:00 UTC: 09:00 in New York in summer, 08:00 in winter.
pub const DEFAULT_TEST_TIME_UTC: &str = "13:00";

/// `ALERT_TEST_UTC`: unset or blank is [`DEFAULT_TEST_TIME_UTC`]; `off` (any case) disables the test; otherwise an
/// `HH:MM` UTC time. Anything else is an error (the service refuses to start, like every other malformed setting).
pub fn test_time_from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Option<NaiveTime>, String> {
    let raw = lookup(ENV_ALERT_TEST_UTC).map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    let text = match raw {
        None => DEFAULT_TEST_TIME_UTC.to_string(),
        Some(v) if v.eq_ignore_ascii_case("off") => return Ok(None),
        Some(v) => v,
    };
    match NaiveTime::parse_from_str(&text, "%H:%M") {
        Ok(t) => Ok(Some(t)),
        Err(_) => Err(format!("{ENV_ALERT_TEST_UTC} is {text:?}; it must be a UTC time as HH:MM (for example 13:00) or off")),
    }
}

/// The dedupe key of the test alert of `day`.
pub fn test_dedupe_key(day: NaiveDate) -> String {
    format!("{HEARTBEAT_ACCOUNT}:daily_test:{day}")
}

/// The test alert of `day`, scheduled at `at` UTC, raised at `now`.
pub fn test_alert(day: NaiveDate, at: NaiveTime, now: DateTime<Utc>) -> Alert {
    Alert {
        code: AlertCode::DailyTest,
        severity: AlertSeverity::Warning,
        account_id: HEARTBEAT_ACCOUNT.to_string(),
        run_key: format!("daily_test|{day}"),
        message: format!(
            "Daily test alert for {day} (scheduled every day at {} UTC): alert delivery from the rebalancer works and nothing needs doing. If a day passes without this message, alert delivery is broken and a halt alert would not reach you either.",
            at.format("%H:%M")
        ),
        dedupe_key: test_dedupe_key(day),
        at: now,
    }
}

/// One test alert per UTC day, at or after the configured time. See the module docs.
#[derive(Debug)]
pub struct DailyTestAlert {
    at: NaiveTime,
    /// The last UTC day whose test alert was persisted.
    done: Mutex<Option<NaiveDate>>,
}

impl DailyTestAlert {
    pub fn new(at: NaiveTime) -> Self {
        Self { at, done: Mutex::new(None) }
    }

    pub fn at(&self) -> NaiveTime {
        self.at
    }

    /// The alert due at `now`, if any: today's, once the scheduled time has passed and today's was not persisted yet.
    /// A service started after the time raises today's at once (the delivery ledger collapses a second one).
    pub fn due(&self, now: DateTime<Utc>) -> Option<Alert> {
        let today = now.date_naive();
        if now.time() < self.at {
            return None;
        }
        if *self.done.lock().unwrap_or_else(|e| e.into_inner()) == Some(today) {
            return None;
        }
        Some(test_alert(today, self.at, now))
    }

    /// `due` + persist through `notifier`. `None` when nothing was due; `Some(Ok)` when today's alert was persisted
    /// (the day is then done); `Some(Err)` when the insert failed (retried on the next call).
    pub fn raise_if_due(&self, now: DateTime<Utc>, notifier: &dyn Notifier) -> Option<Result<(), String>> {
        let alert = self.due(now)?;
        let result = notifier.notify(&alert);
        if result.is_ok() {
            *self.done.lock().unwrap_or_else(|e| e.into_inner()) = Some(now.date_naive());
        }
        Some(result)
    }
}

/// What the service knows about its alert path at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryCheck {
    /// `ALERTS_ENABLED=true` (the delivery worker was built).
    pub alerts_enabled: bool,
    /// The sender's name (`resend`, or [`SENDER_DISABLED_NAME`]); `None` when delivery is off.
    pub sender: Option<String>,
    /// The number of verified, enabled channels, or why they could not be read; `None` when delivery is off.
    pub channels: Option<Result<usize, String>>,
    /// `HEARTBEAT_URL` is set.
    pub heartbeat_enabled: bool,
}

/// Every reason no person would be told about a halt. Empty means the alert path is configured.
pub fn delivery_problems(check: &DeliveryCheck) -> Vec<String> {
    let mut out = Vec::new();
    if !check.alerts_enabled {
        out.push("ALERTS_ENABLED is not true: alerts are only stored in rebalancer_alerts, nothing is sent".to_string());
    }
    if check.sender.as_deref() == Some(SENDER_DISABLED_NAME) {
        out.push("RESEND_API_KEY / ALERT_FROM_EMAIL are not both set: every delivery is recorded as sender_disabled and not sent".to_string());
    }
    match &check.channels {
        Some(Ok(0)) => out.push("there is no verified, enabled alert channel (alert_channels): no alert has an address to go to".to_string()),
        Some(Err(e)) => out.push(format!("the alert channels could not be read ({e})")),
        Some(Ok(_)) | None => {}
    }
    if !check.heartbeat_enabled {
        out.push("HEARTBEAT_URL is not set: if this service stops, no external monitor notices".to_string());
    }
    out
}

/// The startup alert for `problems` (`None` when there are none). One per start: the dedupe key carries the start
/// time.
pub fn delivery_unconfigured_alert(problems: &[String], now: DateTime<Utc>) -> Option<Alert> {
    if problems.is_empty() {
        return None;
    }
    Some(Alert {
        code: AlertCode::DeliveryUnconfigured,
        severity: AlertSeverity::Critical,
        account_id: HEARTBEAT_ACCOUNT.to_string(),
        run_key: format!("startup|{}", now.to_rfc3339()),
        message: format!(
            "the rebalancer started without a working alert path, so a halt or a failure may reach no one: {}",
            problems.iter().enumerate().map(|(i, p)| format!("({}) {p}", i + 1)).collect::<Vec<_>>().join(" ")
        ),
        dedupe_key: format!("{HEARTBEAT_ACCOUNT}:delivery_unconfigured:{}", now.to_rfc3339()),
        at: now,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rebalancer_run::testkit::RecordingNotifier;

    fn t(h: u32, m: u32, s: u32, day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, day, h, m, s).unwrap()
    }

    fn time(s: &str) -> NaiveTime {
        NaiveTime::parse_from_str(s, "%H:%M").unwrap()
    }

    fn lookup(v: Option<&str>) -> impl Fn(&str) -> Option<String> + '_ {
        move |k| if k == ENV_ALERT_TEST_UTC { v.map(str::to_string) } else { None }
    }

    #[test]
    fn the_test_time_defaults_to_thirteen_hundred_utc_and_can_be_turned_off() {
        assert_eq!(test_time_from_lookup(lookup(None)).unwrap(), Some(time("13:00")));
        assert_eq!(test_time_from_lookup(lookup(Some("  "))).unwrap(), Some(time("13:00")));
        assert_eq!(test_time_from_lookup(lookup(Some(" 07:30 "))).unwrap(), Some(time("07:30")));
        assert_eq!(test_time_from_lookup(lookup(Some("00:00"))).unwrap(), Some(time("00:00")));
        for off in ["off", "OFF", " Off "] {
            assert_eq!(test_time_from_lookup(lookup(Some(off))).unwrap(), None, "{off}");
        }
        for bad in ["25:00", "13:60", "noon", "13", "13:00:00:00", "-1:00"] {
            let e = test_time_from_lookup(lookup(Some(bad))).unwrap_err();
            assert!(e.contains(ENV_ALERT_TEST_UTC), "{bad}: {e}");
        }
    }

    #[test]
    fn one_test_alert_a_day_at_or_after_the_configured_time() {
        let d = DailyTestAlert::new(time("13:00"));
        let n = RecordingNotifier::new();
        assert!(d.raise_if_due(t(12, 59, 59, 23), &n).is_none(), "not before the time");
        assert_eq!(d.raise_if_due(t(13, 0, 0, 23), &n), Some(Ok(())), "exactly at the time");
        assert!(d.raise_if_due(t(13, 5, 0, 23), &n).is_none(), "once a day");
        assert!(d.raise_if_due(t(23, 59, 0, 23), &n).is_none());
        assert!(d.raise_if_due(t(0, 5, 0, 24), &n).is_none(), "the next day waits for its time too");
        assert_eq!(d.raise_if_due(t(13, 4, 0, 24), &n), Some(Ok(())));
        let alerts = n.alerts();
        assert_eq!(alerts.len(), 2);
        let a = &alerts[0];
        assert_eq!(a.code.as_str(), "ALERT_DAILY_TEST");
        assert_eq!(a.severity.as_str(), "warning", "platform channels take warning and critical only");
        assert_eq!(a.account_id, "platform");
        assert_eq!(a.dedupe_key, "platform:daily_test:2026-10-23");
        assert_eq!(alerts[1].dedupe_key, "platform:daily_test:2026-10-24", "a new key every day, so the ledger never collapses two days");
        assert!(a.message.contains("13:00 UTC"), "{}", a.message);
    }

    #[test]
    fn a_service_started_after_the_time_sends_todays_test_at_once() {
        let d = DailyTestAlert::new(time("13:00"));
        let n = RecordingNotifier::new();
        assert_eq!(d.raise_if_due(t(18, 0, 0, 23), &n), Some(Ok(())));
        assert_eq!(n.count(), 1);
    }

    #[test]
    fn a_failed_insert_is_retried_on_the_next_tick() {
        let d = DailyTestAlert::new(time("13:00"));
        let n = RecordingNotifier::new();
        n.set_failing(true);
        assert!(matches!(d.raise_if_due(t(13, 0, 0, 23), &n), Some(Err(_))));
        n.set_failing(false);
        assert_eq!(d.raise_if_due(t(13, 5, 0, 23), &n), Some(Ok(())), "the day was not marked done");
        assert!(d.raise_if_due(t(13, 10, 0, 23), &n).is_none());
        assert_eq!(n.count(), 1);
    }

    fn healthy() -> DeliveryCheck {
        DeliveryCheck { alerts_enabled: true, sender: Some("resend".into()), channels: Some(Ok(1)), heartbeat_enabled: true }
    }

    #[test]
    fn a_configured_alert_path_raises_nothing_at_startup() {
        assert!(delivery_problems(&healthy()).is_empty());
        assert!(delivery_unconfigured_alert(&[], t(9, 0, 0, 23)).is_none());
    }

    #[test]
    fn every_way_the_alert_path_is_deaf_is_named() {
        let off = DeliveryCheck { alerts_enabled: false, sender: None, channels: None, heartbeat_enabled: true };
        let p = delivery_problems(&off);
        assert_eq!(p.len(), 1);
        assert!(p[0].contains("ALERTS_ENABLED"));
        let no_sender = DeliveryCheck { sender: Some(SENDER_DISABLED_NAME.into()), ..healthy() };
        assert!(delivery_problems(&no_sender)[0].contains("RESEND_API_KEY"));
        let no_channel = DeliveryCheck { channels: Some(Ok(0)), ..healthy() };
        assert!(delivery_problems(&no_channel)[0].contains("no verified, enabled alert channel"));
        let unreadable = DeliveryCheck { channels: Some(Err("ALERT_STORE_UNAVAILABLE: relation does not exist".into())), ..healthy() };
        assert!(delivery_problems(&unreadable)[0].contains("could not be read"));
        let no_heartbeat = DeliveryCheck { heartbeat_enabled: false, ..healthy() };
        assert!(delivery_problems(&no_heartbeat)[0].contains("HEARTBEAT_URL"));
        let everything = DeliveryCheck { alerts_enabled: false, sender: None, channels: None, heartbeat_enabled: false };
        assert_eq!(delivery_problems(&everything).len(), 2);
    }

    #[test]
    fn the_startup_alert_is_critical_lists_every_problem_and_is_one_per_start() {
        let problems = delivery_problems(&DeliveryCheck { alerts_enabled: false, sender: None, channels: None, heartbeat_enabled: false });
        let a = delivery_unconfigured_alert(&problems, t(9, 0, 0, 23)).unwrap();
        assert_eq!((a.code.as_str(), a.severity.as_str()), ("ALERT_DELIVERY_UNCONFIGURED", "critical"));
        assert_eq!(a.account_id, "platform");
        assert!(a.message.contains("(1) ALERTS_ENABLED") && a.message.contains("(2) HEARTBEAT_URL"), "{}", a.message);
        let b = delivery_unconfigured_alert(&problems, t(9, 5, 0, 23)).unwrap();
        assert_ne!(a.dedupe_key, b.dedupe_key, "a second start is a second notice");
    }
}
