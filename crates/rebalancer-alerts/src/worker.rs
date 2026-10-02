//! [`DeliveryWorker`]: one `run` per service tick, after the runs. See the crate docs for the design; the two steps
//! are (1) INGEST: alerts since the watermark become delivery rows, insert-if-absent per applicable channel, and
//! (2) SEND: every unsent, unerrored delivery row is handed to the sender and marked once.
//!
//! # The watermark
//! `rebalancer_alerts` has no "delivered" column and this crate adds none (the alert row is the pipeline's record,
//! not the worker's). The worker instead remembers the newest `created_at` it has fully ingested and reads strictly
//! newer rows next tick. At startup the watermark is `now - backlog` (default 24 h), so a restart re-reads the
//! recent alerts and the `(channel_id, dedupe_key)` unique key discards what was already ingested. The watermark
//! advances only after EVERY insert of the batch succeeded; a failed batch is retried whole next tick, idempotently.
//! An alert whose tenant has no channel and whose severity reaches no platform channel produces no row and is simply
//! passed over (the alert itself stays in `rebalancer_alerts`).

use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};

use crate::ledger::{Channel, DeliveryLedger, LedgerError, NewDelivery, PendingAlert};
use crate::sender::{OutboundEmail, SendError, Sender};
use crate::severity::{channel_applies, delivery_severity};
use crate::template::render;

pub const ENV_ALERTS_ENABLED: &str = "ALERTS_ENABLED";
/// How far back the first tick after a start reads alerts.
pub const DEFAULT_BACKLOG_SECS: i64 = 24 * 60 * 60;
/// Alerts ingested per tick, deliveries sent per tick: bounded so one bad tick cannot run away.
pub const ALERTS_PER_TICK: usize = 200;
pub const SENDS_PER_TICK: usize = 100;

/// `ALERTS_ENABLED` must be exactly `true` (trimmed) to turn delivery on; anything else, including unset, is off.
pub fn alerts_enabled(lookup: impl Fn(&str) -> Option<String>) -> bool {
    lookup(ENV_ALERTS_ENABLED).is_some_and(|v| v.trim() == "true")
}

/// What one tick of the worker did (for the service log).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    pub alerts_seen: usize,
    pub deliveries_inserted: usize,
    pub deliveries_deduped: usize,
    /// Alerts that matched no channel at all.
    pub alerts_without_channel: usize,
    pub sent: usize,
    pub send_errors: usize,
    /// Ledger failures (`ALERT_STORE_UNAVAILABLE: ...`); the tick stopped at the first one of each step.
    pub failures: Vec<String>,
}

impl TickReport {
    pub fn summary(&self) -> String {
        format!(
            "alerts {} seen, deliveries {} inserted / {} deduped, {} alert(s) without a channel, {} sent, {} send error(s){}",
            self.alerts_seen,
            self.deliveries_inserted,
            self.deliveries_deduped,
            self.alerts_without_channel,
            self.sent,
            self.send_errors,
            if self.failures.is_empty() { String::new() } else { format!("; FAILED: {}", self.failures.join(" | ")) }
        )
    }
}

pub struct DeliveryWorker<L: DeliveryLedger, S: Sender + ?Sized> {
    ledger: L,
    sender: Box<S>,
    watermark: Mutex<DateTime<Utc>>,
}

impl<L: DeliveryLedger, S: Sender + ?Sized> DeliveryWorker<L, S> {
    /// `now` is the start time; the first tick reads alerts created after `now - backlog`.
    pub fn new(ledger: L, sender: Box<S>, now: DateTime<Utc>, backlog: Duration) -> Self {
        Self { ledger, sender, watermark: Mutex::new(now - backlog) }
    }

    pub fn watermark(&self) -> DateTime<Utc> {
        *self.watermark.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn sender_name(&self) -> &'static str {
        self.sender.name()
    }

    pub fn ledger(&self) -> &L {
        &self.ledger
    }

    /// The channels an alert goes to: its tenant's (any severity) plus the platform's (warning and critical).
    pub fn channels_for(alert: &PendingAlert, channels: &[Channel]) -> Vec<Channel> {
        let severity = delivery_severity(&alert.code, &alert.severity);
        channels
            .iter()
            .filter(|c| channel_applies(c.scope, severity))
            .filter(|c| match c.scope {
                crate::ledger::ChannelScope::Platform => true,
                crate::ledger::ChannelScope::Tenant => match (&c.tenant_id, &alert.tenant_id) {
                    (Some(ct), Some(at)) => ct.trim().eq_ignore_ascii_case(at.trim()),
                    _ => false,
                },
            })
            .cloned()
            .collect()
    }

    fn ingest(&self, now: DateTime<Utc>, report: &mut TickReport) -> Result<(), LedgerError> {
        let since = self.watermark();
        let alerts = self.ledger.alerts_since(since, ALERTS_PER_TICK)?;
        if alerts.is_empty() {
            return Ok(());
        }
        let channels = self.ledger.channels()?;
        let mut newest = since;
        for alert in &alerts {
            report.alerts_seen += 1;
            let severity = delivery_severity(&alert.code, &alert.severity);
            let targets = Self::channels_for(alert, &channels);
            if targets.is_empty() {
                report.alerts_without_channel += 1;
            }
            let rendered = render(alert, severity);
            for c in targets {
                let d = NewDelivery {
                    channel_id: c.id.clone(),
                    scope: c.scope,
                    tenant_id: c.tenant_id.clone(),
                    severity: severity.as_str().to_string(),
                    dedupe_key: alert.dedupe_key.clone(),
                    subject: rendered.subject.clone(),
                    body: rendered.body.clone(),
                };
                // A failure here leaves the watermark where it was: the whole batch is re-read next tick and the
                // unique key makes the retry idempotent.
                if self.ledger.insert_if_absent(&d, now)? {
                    report.deliveries_inserted += 1;
                } else {
                    report.deliveries_deduped += 1;
                }
            }
            if alert.created_at > newest {
                newest = alert.created_at;
            }
        }
        *self.watermark.lock().unwrap_or_else(|e| e.into_inner()) = newest;
        Ok(())
    }

    fn send_pending(&self, now: DateTime<Utc>, report: &mut TickReport) -> Result<(), LedgerError> {
        for d in self.ledger.undelivered(SENDS_PER_TICK)? {
            if d.kind.trim() != "email" {
                self.ledger.mark_error(&d.id, &format!("unsupported_channel_kind:{}", d.kind.trim()))?;
                report.send_errors += 1;
                continue;
            }
            let email = OutboundEmail { to: d.address.clone(), subject: d.subject.clone(), body: d.body.clone() };
            match self.sender.send(&email) {
                Ok(id) => {
                    self.ledger.mark_sent(&d.id, &id, now)?;
                    report.sent += 1;
                }
                Err(e) => {
                    let text: String = match &e {
                        SendError::Disabled => crate::sender::SENDER_DISABLED_ERROR.to_string(),
                        other => other.ledger_text(),
                    };
                    self.ledger.mark_error(&d.id, &text)?;
                    report.send_errors += 1;
                }
            }
        }
        Ok(())
    }

    /// One tick: ingest, then send. Never panics on a ledger failure; the report says what stopped.
    pub fn run(&self, now: DateTime<Utc>) -> TickReport {
        let mut report = TickReport::default();
        if let Err(e) = self.ingest(now, &mut report) {
            report.failures.push(format!("ingest: {e}"));
        }
        if let Err(e) = self.send_pending(now, &mut report) {
            report.failures.push(format!("send: {e}"));
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::{ChannelScope, InMemoryLedger};
    use crate::sender::{DisabledSender, SendError, SENDER_DISABLED_ERROR};
    use chrono::TimeZone;
    use std::sync::Mutex as StdMutex;

    const T_A: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    const T_B: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 2, 15, 0, 0).unwrap()
    }

    fn alert(id: &str, tenant: Option<&str>, account: &str, code: &str, severity: &str, dedupe: &str, secs: i64) -> PendingAlert {
        PendingAlert {
            id: id.into(),
            tenant_id: tenant.map(str::to_string),
            account_id: account.into(),
            code: code.into(),
            severity: severity.into(),
            message: format!("{code} happened"),
            run_key: format!("{account}|run"),
            dedupe_key: format!("{account}:{dedupe}"),
            at: t0() + Duration::seconds(secs),
            created_at: t0() + Duration::seconds(secs),
        }
    }

    fn channel(id: &str, scope: ChannelScope, tenant: Option<&str>, address: &str) -> Channel {
        Channel { id: id.into(), scope, tenant_id: tenant.map(str::to_string), kind: "email".into(), address: address.into() }
    }

    /// A sender that records what it sent and answers with scripted results.
    struct ScriptedSender {
        sent: StdMutex<Vec<OutboundEmail>>,
        results: StdMutex<Vec<Result<String, SendError>>>,
    }

    impl ScriptedSender {
        fn ok() -> Self {
            Self { sent: StdMutex::new(Vec::new()), results: StdMutex::new(Vec::new()) }
        }
        fn script(&self, r: Result<String, SendError>) {
            self.results.lock().unwrap().push(r);
        }
        fn sent(&self) -> Vec<OutboundEmail> {
            self.sent.lock().unwrap().clone()
        }
    }

    impl Sender for ScriptedSender {
        fn name(&self) -> &'static str {
            "scripted"
        }
        fn send(&self, email: &OutboundEmail) -> Result<String, SendError> {
            self.sent.lock().unwrap().push(email.clone());
            let mut r = self.results.lock().unwrap();
            if r.is_empty() {
                Ok(format!("msg-{}", self.sent.lock().unwrap().len()))
            } else {
                r.remove(0)
            }
        }
    }

    fn worker<S: Sender>(ledger: InMemoryLedger, sender: S) -> DeliveryWorker<InMemoryLedger, S> {
        DeliveryWorker::new(ledger, Box::new(sender), t0(), Duration::seconds(DEFAULT_BACKLOG_SECS))
    }

    #[test]
    fn the_flag_is_off_unless_exactly_true() {
        assert!(!alerts_enabled(|_| None));
        assert!(!alerts_enabled(|_| Some("1".into())));
        assert!(!alerts_enabled(|_| Some("TRUE".into())));
        assert!(alerts_enabled(|_| Some(" true ".into())));
    }

    #[test]
    fn one_delivery_per_applicable_channel_and_the_dedupe_key_collapses_repeats() {
        let ledger = InMemoryLedger::new();
        ledger.add_channel(channel("c-a", ChannelScope::Tenant, Some(T_A), "a@example.com"));
        ledger.add_channel(channel("c-b", ChannelScope::Tenant, Some(T_B), "b@example.com"));
        ledger.add_channel(channel("c-p", ChannelScope::Platform, None, "ops@example.com"));
        // the same halted account reminded on three ticks: same dedupe key
        ledger.add_alert(alert("1", Some(T_A), "acct-a", "ALERT_STILL_HALTED", "warning", "halted", 1));
        ledger.add_alert(alert("2", Some(T_A), "acct-a", "ALERT_STILL_HALTED", "warning", "halted", 2));
        ledger.add_alert(alert("3", Some(T_A), "acct-a", "ALERT_STILL_HALTED", "warning", "halted", 3));
        let w = worker(ledger, ScriptedSender::ok());
        let r = w.run(t0() + Duration::seconds(10));
        assert!(r.failures.is_empty(), "{r:?}");
        assert_eq!(r.alerts_seen, 3);
        // tenant A's channel + the platform channel (warning), never tenant B's
        assert_eq!(r.deliveries_inserted, 2, "{r:?}");
        assert_eq!(r.deliveries_deduped, 4, "two more alerts x two channels, all deduped");
        let d = w.ledger().deliveries();
        assert_eq!(d.len(), 2);
        let mut to: Vec<&str> = d.iter().map(|x| x.delivery.channel_id.as_str()).collect();
        to.sort_unstable();
        assert_eq!(to, ["c-a", "c-p"]);
        assert!(d.iter().all(|x| x.delivery.dedupe_key == "acct-a:halted" && x.delivery.severity == "warning"));
        assert_eq!(d.iter().find(|x| x.delivery.channel_id == "c-p").unwrap().delivery.scope, ChannelScope::Platform);
        assert_eq!(d.iter().find(|x| x.delivery.channel_id == "c-p").unwrap().delivery.tenant_id, None);
        assert_eq!(d.iter().find(|x| x.delivery.channel_id == "c-a").unwrap().delivery.tenant_id.as_deref(), Some(T_A));
        // ... and both were sent, once each, marked with the provider id
        assert_eq!(r.sent, 2);
        assert!(d.iter().all(|x| x.sent_at.is_some() && x.provider_message_id.is_some() && x.error.is_none()));
        assert_eq!(w.watermark(), t0() + Duration::seconds(3));

        // the next tick sees nothing new and sends nothing again
        let r2 = w.run(t0() + Duration::seconds(20));
        assert_eq!(r2, TickReport::default());
    }

    #[test]
    fn info_alerts_reach_tenant_channels_only_and_platform_alerts_reach_the_platform_only() {
        let ledger = InMemoryLedger::new();
        ledger.add_channel(channel("c-a", ChannelScope::Tenant, Some(T_A), "a@example.com"));
        ledger.add_channel(channel("c-p", ChannelScope::Platform, None, "ops@example.com"));
        ledger.add_alert(alert("1", Some(T_A), "acct-a", "ALERT_MANDATE_UNUSABLE", "info", "mandate", 1));
        // an alert about the platform itself (nil tenant): the heartbeat
        ledger.add_alert(alert("2", None, "platform", "ALERT_HEARTBEAT_FAILED", "warning", "heartbeat", 2));
        // a tenant with no channel and a severity below the platform's: nowhere to go, not an error
        ledger.add_alert(alert("3", Some(T_B), "acct-b", "ALERT_MANDATE_UNUSABLE", "info", "mandate", 3));
        let w = worker(ledger, ScriptedSender::ok());
        let r = w.run(t0() + Duration::seconds(10));
        assert!(r.failures.is_empty());
        assert_eq!(r.deliveries_inserted, 2);
        assert_eq!(r.alerts_without_channel, 1);
        let d = w.ledger().deliveries();
        assert_eq!(d.iter().find(|x| x.delivery.dedupe_key == "acct-a:mandate").unwrap().delivery.channel_id, "c-a");
        assert_eq!(d.iter().find(|x| x.delivery.dedupe_key == "platform:heartbeat").unwrap().delivery.channel_id, "c-p");
        assert_eq!(w.watermark(), t0() + Duration::seconds(3), "a channel-less alert still advances the watermark");
    }

    #[test]
    fn a_bad_stored_severity_is_graded_by_the_code_and_still_reaches_the_platform() {
        let ledger = InMemoryLedger::new();
        ledger.add_channel(channel("c-p", ChannelScope::Platform, None, "ops@example.com"));
        ledger.add_alert(alert("1", Some(T_A), "acct-a", "ALERT_HALT", "???", "halt", 1));
        let w = worker(ledger, ScriptedSender::ok());
        let r = w.run(t0() + Duration::seconds(10));
        assert_eq!(r.deliveries_inserted, 1);
        let d = &w.ledger().deliveries()[0];
        assert_eq!(d.delivery.severity, "critical");
        assert_eq!(d.delivery.subject, "[Mendl Labs] critical ALERT_HALT acct-a");
        assert!(d.delivery.body.contains("What to do:"));
    }

    #[test]
    fn sender_disabled_marks_every_delivery_once_and_never_drops_or_retries_it() {
        let ledger = InMemoryLedger::new();
        ledger.add_channel(channel("c-p", ChannelScope::Platform, None, "ops@example.com"));
        ledger.add_alert(alert("1", Some(T_A), "acct-a", "ALERT_HALT", "critical", "halt", 1));
        let w = worker(ledger, DisabledSender);
        assert_eq!(w.sender_name(), "disabled");
        let r = w.run(t0() + Duration::seconds(10));
        assert!(r.failures.is_empty());
        assert_eq!((r.deliveries_inserted, r.sent, r.send_errors), (1, 0, 1));
        let d = w.ledger().deliveries();
        assert_eq!(d.len(), 1, "the row is kept as the audit trail");
        assert_eq!(d[0].error.as_deref(), Some(SENDER_DISABLED_ERROR));
        assert!(d[0].sent_at.is_none());
        // next tick: marked rows are not candidates again
        let r2 = w.run(t0() + Duration::seconds(20));
        assert_eq!((r2.sent, r2.send_errors), (0, 0));
        assert_eq!(w.ledger().deliveries().len(), 1);
    }

    #[test]
    fn a_provider_failure_is_recorded_once_and_a_crash_between_insert_and_send_is_recovered_next_tick() {
        let ledger = InMemoryLedger::new();
        ledger.add_channel(channel("c-p", ChannelScope::Platform, None, "ops@example.com"));
        ledger.add_alert(alert("1", Some(T_A), "acct-a", "ALERT_HALT", "critical", "halt", 1));
        ledger.add_alert(alert("2", Some(T_A), "acct-a", "ALERT_RUN_FAILED", "critical", "failed:x", 2));
        let s = ScriptedSender::ok();
        s.script(Err(SendError::Rejected { status: 422, detail: "bad address".into() }));
        s.script(Ok("msg-ok".into()));
        let w = worker(ledger, s);
        let r = w.run(t0() + Duration::seconds(10));
        assert_eq!((r.sent, r.send_errors), (1, 1));
        let d = w.ledger().deliveries();
        let failed = d.iter().find(|x| x.delivery.dedupe_key == "acct-a:halt").unwrap();
        assert_eq!(failed.error.as_deref(), Some("provider_rejected:422:bad address"));
        let sent = d.iter().find(|x| x.delivery.dedupe_key == "acct-a:failed:x").unwrap();
        assert_eq!(sent.provider_message_id.as_deref(), Some("msg-ok"));

        // "crash" between insert and send: a row with neither sent_at nor error is picked up by the next tick
        let now = t0() + Duration::seconds(30);
        w.ledger()
            .insert_if_absent(
                &NewDelivery {
                    channel_id: "c-p".into(),
                    scope: ChannelScope::Platform,
                    tenant_id: None,
                    severity: "critical".into(),
                    dedupe_key: "acct-a:orphan".into(),
                    subject: "s".into(),
                    body: "b".into(),
                },
                now,
            )
            .unwrap();
        let r2 = w.run(now);
        assert_eq!((r2.alerts_seen, r2.sent), (0, 1));
        assert_eq!(w.sender.sent().len(), 3);
    }

    #[test]
    fn a_ledger_failure_stops_the_step_keeps_the_watermark_and_is_reported_not_panicked() {
        let ledger = InMemoryLedger::new();
        ledger.add_channel(channel("c-p", ChannelScope::Platform, None, "ops@example.com"));
        ledger.add_alert(alert("1", Some(T_A), "acct-a", "ALERT_HALT", "critical", "halt", 1));
        ledger.add_alert(alert("2", Some(T_A), "acct-b", "ALERT_HALT", "critical", "halt", 2));
        // alerts_since ok, channels ok, first insert ok, second insert FAILS, then the send step's read fails too
        let w = worker(ledger, ScriptedSender::ok());
        let before = w.watermark();
        // fail the 4th and 5th calls: [alerts_since, channels, insert(1), insert(2)=fail, undelivered=fail]
        w.ledger().fail_after(3, 2);
        let r = w.run(t0() + Duration::seconds(10));
        assert_eq!(r.failures.len(), 2, "{r:?}");
        assert!(r.failures[0].starts_with("ingest: ALERT_STORE_UNAVAILABLE"));
        assert!(r.failures[1].starts_with("send: ALERT_STORE_UNAVAILABLE"));
        assert_eq!(w.watermark(), before, "a failed batch does not advance the watermark");
        assert_eq!(w.ledger().deliveries().len(), 1);

        // the next tick re-reads the whole batch: the first insert is deduped, the second now succeeds
        let r2 = w.run(t0() + Duration::seconds(20));
        assert!(r2.failures.is_empty(), "{r2:?}");
        assert_eq!((r2.deliveries_deduped, r2.deliveries_inserted, r2.sent), (1, 1, 2));
        assert_eq!(w.watermark(), t0() + Duration::seconds(2));
    }

    #[test]
    fn a_channel_of_an_unsupported_kind_is_marked_not_sent() {
        let ledger = InMemoryLedger::new();
        ledger.add_channel(Channel { id: "c-s".into(), scope: ChannelScope::Platform, tenant_id: None, kind: "sms".into(), address: "+1555".into() });
        ledger.add_alert(alert("1", Some(T_A), "acct-a", "ALERT_HALT", "critical", "halt", 1));
        let w = worker(ledger, ScriptedSender::ok());
        let r = w.run(t0() + Duration::seconds(10));
        assert_eq!((r.deliveries_inserted, r.sent, r.send_errors), (1, 0, 1));
        assert_eq!(w.ledger().deliveries()[0].error.as_deref(), Some("unsupported_channel_kind:sms"));
        assert!(w.sender.sent().is_empty());
    }
}
