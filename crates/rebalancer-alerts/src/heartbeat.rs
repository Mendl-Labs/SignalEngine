//! The dead-man's switch (W6.2 / IMPL WP5.2): after every tick the service pings an external monitor
//! (healthchecks.io style: a GET or POST to a per-check URL means "alive"; `<url>/fail` means "I ran and something is
//! wrong"). The MONITOR owns the alarm: if the pings stop, it pages. Nothing in this process decides anything from a
//! ping's outcome; the ping is bounded by its transport's timeout, errors are logged, and a failure streak raises
//! ONE `ALERT_HEARTBEAT_FAILED` through the notifier (so the alert, like every other, is in `rebalancer_alerts` and
//! reaches the platform channels).
//!
//! The monitor's expected period must be the tick interval plus a grace (see `README-rebalancer.md`): a tick that
//! ran but found nothing due still pings (the switch watches that the LOOP runs, not that trades happen).

use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;

use broker_adapters::transport::{HttpMethod, HttpRequest, HttpTransport};
use chrono::{DateTime, Utc};
use rebalancer_run::record::{Alert, AlertCode, AlertSeverity};
use rebalancer_run::stores::Notifier;

pub const ENV_HEARTBEAT_URL: &str = "HEARTBEAT_URL";
pub const ENV_HEARTBEAT_FAIL_SUFFIX: &str = "HEARTBEAT_FAIL_SUFFIX";
pub const DEFAULT_FAIL_SUFFIX: &str = "/fail";
/// The account id and dedupe key of the heartbeat alert: the alert is about the platform, not an account.
pub const HEARTBEAT_ACCOUNT: &str = "platform";
pub const HEARTBEAT_DEDUPE_KEY: &str = "platform:heartbeat_failed";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatConfig {
    /// `None` = disabled (logged once at startup by the service).
    pub url: Option<String>,
    pub fail_suffix: String,
}

impl HeartbeatConfig {
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let url = lookup(ENV_HEARTBEAT_URL).map(|s| s.trim().trim_end_matches('/').to_string()).filter(|s| !s.is_empty());
        let fail_suffix = lookup(ENV_HEARTBEAT_FAIL_SUFFIX).map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).unwrap_or_else(|| DEFAULT_FAIL_SUFFIX.to_string());
        Self { url, fail_suffix }
    }

    pub fn disabled() -> Self {
        Self { url: None, fail_suffix: DEFAULT_FAIL_SUFFIX.to_string() }
    }

    pub fn enabled(&self) -> bool {
        self.url.is_some()
    }
}

/// How the tick went, as far as the switch cares: the loop RAN (whatever the runs did) or it could not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickOutcome {
    /// The tick executed: the kill flag was read and, if clear, the due scan and the runs happened (any run outcome,
    /// including zero due or every run failing closed, counts: those alert on their own).
    Ran,
    /// The tick could not execute (kill flag unreadable, due scan failed).
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatResult {
    Disabled,
    /// The monitor answered 2xx to `url`.
    Pinged { url: String },
    /// The ping did not reach the monitor or it answered non-2xx. Text is bounded and never carries a secret.
    Failed { url: String, detail: String },
}

impl HeartbeatResult {
    pub fn failed(&self) -> bool {
        matches!(self, HeartbeatResult::Failed { .. })
    }
}

impl fmt::Display for HeartbeatResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HeartbeatResult::Disabled => write!(f, "heartbeat disabled"),
            HeartbeatResult::Pinged { url } => write!(f, "heartbeat pinged {}", redact(url)),
            HeartbeatResult::Failed { url, detail } => write!(f, "heartbeat FAILED for {}: {detail}", redact(url)),
        }
    }
}

/// A healthchecks.io URL is itself the secret (a UUID): logs show the host and the last 4 characters only.
pub fn redact(url: &str) -> String {
    let host = url.split("://").nth(1).unwrap_or(url).split('/').next().unwrap_or("");
    let tail: String = url.chars().rev().take(4).collect::<Vec<_>>().into_iter().rev().collect();
    format!("{host}/...{tail}")
}

pub struct Heartbeat {
    cfg: HeartbeatConfig,
    transport: Arc<dyn HttpTransport>,
}

impl Heartbeat {
    /// `transport` should be a SHORT-timeout transport (the service builds one with a few seconds' budget): the tick
    /// loop waits for the ping at most that long and never for the monitor's goodwill.
    pub fn new(cfg: HeartbeatConfig, transport: Arc<dyn HttpTransport>) -> Self {
        Self { cfg, transport }
    }

    pub fn config(&self) -> &HeartbeatConfig {
        &self.cfg
    }

    fn url_for(&self, outcome: TickOutcome) -> Option<String> {
        let base = self.cfg.url.as_ref()?;
        Some(match outcome {
            TickOutcome::Ran => base.clone(),
            TickOutcome::Failed => format!("{base}{}", self.cfg.fail_suffix),
        })
    }

    /// One ping. Never panics, never retries, never blocks past the transport's own timeout.
    pub fn ping(&self, outcome: TickOutcome) -> HeartbeatResult {
        let Some(url) = self.url_for(outcome) else { return HeartbeatResult::Disabled };
        let req = HttpRequest { method: HttpMethod::Post, url: url.clone(), headers: vec![("User-Agent".to_string(), "mendl-rebalancer-heartbeat".to_string())], body: Some(String::new()) };
        match self.transport.execute(&req) {
            Ok(r) if (200..300).contains(&r.status) => HeartbeatResult::Pinged { url },
            Ok(r) => HeartbeatResult::Failed { url, detail: format!("HTTP {}", r.status) },
            Err(e) => HeartbeatResult::Failed { url, detail: e.to_string().chars().take(160).collect() },
        }
    }
}

/// Keeps the failure-streak state and turns a transition INTO failure into one alert. Recovery resets the streak so
/// the next failure alerts again; the delivery ledger's unique key still collapses repeats that share the dedupe key.
#[derive(Default)]
pub struct HeartbeatMonitor {
    failing: Mutex<bool>,
}

impl HeartbeatMonitor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_failing(&self) -> bool {
        *self.failing.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record the ping's result; `Some(alert)` exactly when a failure streak starts.
    pub fn observe(&self, result: &HeartbeatResult, now: DateTime<Utc>) -> Option<Alert> {
        let mut failing = self.failing.lock().unwrap_or_else(|e| e.into_inner());
        match result {
            HeartbeatResult::Failed { url, detail } => {
                if *failing {
                    return None;
                }
                *failing = true;
                Some(Alert {
                    code: AlertCode::HeartbeatFailed,
                    severity: AlertSeverity::Warning,
                    account_id: HEARTBEAT_ACCOUNT.to_string(),
                    run_key: format!("heartbeat|{}", now.to_rfc3339()),
                    message: format!("the dead-man's heartbeat to {} failed: {detail}; the external monitor will page if this persists", redact(url)),
                    dedupe_key: HEARTBEAT_DEDUPE_KEY.to_string(),
                    at: now,
                })
            }
            HeartbeatResult::Pinged { .. } | HeartbeatResult::Disabled => {
                *failing = false;
                None
            }
        }
    }

    /// `observe` + hand the alert to the notifier. Returns what to log: the alert code when one was raised and the
    /// notifier's error (if any). Never fails the caller.
    pub fn observe_and_notify(&self, result: &HeartbeatResult, now: DateTime<Utc>, notifier: &dyn Notifier) -> Option<Result<(), String>> {
        let alert = self.observe(result, now)?;
        Some(notifier.notify(&alert))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_adapters::testing::FakeTransport;
    use broker_adapters::transport::{HttpResponse, TransportError};
    use chrono::TimeZone;
    use rebalancer_run::testkit::RecordingNotifier;
    use std::time::Instant;

    const URL: &str = "https://hc-ping.com/0b6a4e2c-1111-2222-3333-444455556666";

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 2, 15, 0, 0).unwrap()
    }

    fn cfg(url: Option<&str>) -> HeartbeatConfig {
        HeartbeatConfig::from_lookup(|k| if k == ENV_HEARTBEAT_URL { url.map(str::to_string) } else { None })
    }

    #[test]
    fn config_reads_the_url_and_the_suffix_with_the_documented_defaults() {
        assert_eq!(cfg(None), HeartbeatConfig::disabled());
        assert!(!cfg(None).enabled());
        let c = cfg(Some(&format!("  {URL}/  ")));
        assert_eq!(c.url.as_deref(), Some(URL));
        assert_eq!(c.fail_suffix, "/fail");
        let c = HeartbeatConfig::from_lookup(|k| match k {
            ENV_HEARTBEAT_URL => Some(URL.into()),
            ENV_HEARTBEAT_FAIL_SUFFIX => Some("/0".into()),
            _ => None,
        });
        assert_eq!(c.fail_suffix, "/0");
    }

    #[test]
    fn disabled_never_sends_a_request() {
        let t = Arc::new(FakeTransport::new());
        let hb = Heartbeat::new(HeartbeatConfig::disabled(), t.clone());
        assert_eq!(hb.ping(TickOutcome::Ran), HeartbeatResult::Disabled);
        assert_eq!(hb.ping(TickOutcome::Failed), HeartbeatResult::Disabled);
        assert_eq!(t.request_count(), 0);
    }

    #[test]
    fn a_ran_tick_pings_the_url_and_a_failed_tick_pings_the_fail_url() {
        let t = Arc::new(FakeTransport::new());
        t.set_handler(|_| Ok(HttpResponse { status: 200, body: "OK".into() }));
        let hb = Heartbeat::new(cfg(Some(URL)), t.clone());
        assert_eq!(hb.ping(TickOutcome::Ran), HeartbeatResult::Pinged { url: URL.into() });
        assert_eq!(hb.ping(TickOutcome::Failed), HeartbeatResult::Pinged { url: format!("{URL}/fail") });
        let reqs = t.requests();
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[0].url, URL);
        assert_eq!(reqs[1].url, format!("{URL}/fail"));
        assert!(reqs.iter().all(|r| r.method == HttpMethod::Post));
    }

    #[test]
    fn the_ping_never_blocks_the_tick_and_a_failure_is_a_value_not_a_panic() {
        // A transport that fails the way a real one does after its (short) timeout: the ping returns at once with a
        // Failed value; the caller's tick loop is never held.
        let t = Arc::new(FakeTransport::new());
        t.set_handler(|_| Err(TransportError::Timeout));
        let hb = Heartbeat::new(cfg(Some(URL)), t.clone());
        let started = Instant::now();
        let r = hb.ping(TickOutcome::Ran);
        assert!(started.elapsed().as_millis() < 1000, "the fake transport answers immediately; the ping must not add waiting of its own");
        assert_eq!(r, HeartbeatResult::Failed { url: URL.into(), detail: "request timed out".into() });
        assert!(r.failed());

        // a non-2xx answer is a failure too
        t.set_handler(|_| Ok(HttpResponse { status: 404, body: "not found".into() }));
        assert_eq!(hb.ping(TickOutcome::Ran), HeartbeatResult::Failed { url: URL.into(), detail: "HTTP 404".into() });

        // the log line never shows the whole URL (it is the monitor's secret)
        let line = r.to_string();
        assert!(!line.contains("0b6a4e2c-1111"), "{line}");
        assert!(line.contains("hc-ping.com/...6666"), "{line}");
    }

    #[test]
    fn the_monitor_raises_one_warning_per_failure_streak_through_the_notifier() {
        let n = RecordingNotifier::new();
        let m = HeartbeatMonitor::new();
        let fail = HeartbeatResult::Failed { url: URL.into(), detail: "request timed out".into() };
        let ok = HeartbeatResult::Pinged { url: URL.into() };

        assert_eq!(m.observe_and_notify(&ok, now(), &n), None);
        assert_eq!(m.observe_and_notify(&fail, now(), &n), Some(Ok(())));
        assert_eq!(m.observe_and_notify(&fail, now(), &n), None, "the streak continues: no second alert");
        assert_eq!(m.observe_and_notify(&fail, now(), &n), None);
        assert!(m.is_failing());
        assert_eq!(m.observe_and_notify(&ok, now(), &n), None, "recovery is silent");
        assert!(!m.is_failing());
        assert_eq!(m.observe_and_notify(&fail, now(), &n), Some(Ok(())), "a new streak alerts again");

        let alerts = n.alerts();
        assert_eq!(alerts.len(), 2);
        let a = &alerts[0];
        assert_eq!(a.code, AlertCode::HeartbeatFailed);
        assert_eq!(a.code.as_str(), "ALERT_HEARTBEAT_FAILED");
        assert_eq!(a.severity, AlertSeverity::Warning);
        assert_eq!(a.account_id, HEARTBEAT_ACCOUNT);
        assert_eq!(a.dedupe_key, HEARTBEAT_DEDUPE_KEY);
        assert!(a.message.contains("request timed out"));
        assert!(!a.message.contains("0b6a4e2c-1111"), "the alert never carries the whole URL: {}", a.message);

        // a notifier outage is reported to the caller as a value, never a panic
        n.set_failing(true);
        let m2 = HeartbeatMonitor::new();
        assert!(matches!(m2.observe_and_notify(&fail, now(), &n), Some(Err(_))));
    }
}
