//! The outbound e-mail seam: [`Sender`], a [`DisabledSender`] (no credentials configured: every delivery is marked
//! `sender_disabled`, never dropped) and [`ResendSender`] (`POST https://api.resend.com/emails`, bearer key).
//!
//! The Resend key is read from `RESEND_API_KEY` and sent ONLY as `Authorization: Bearer`; it is never logged, never
//! part of an error and never in a `Debug` rendering. `ALERT_FROM_EMAIL` is the sender address. With either unset the
//! sender is [`DisabledSender`].

use std::fmt;
use std::sync::Arc;

use broker_adapters::transport::{HttpMethod, HttpRequest, HttpTransport};
use serde_json::{json, Value};

pub const ENV_RESEND_API_KEY: &str = "RESEND_API_KEY";
pub const ENV_ALERT_FROM_EMAIL: &str = "ALERT_FROM_EMAIL";
pub const RESEND_BASE_URL: &str = "https://api.resend.com";
/// The `alert_deliveries.error` text of a delivery nobody could send because no sender is configured.
pub const SENDER_DISABLED_ERROR: &str = "sender_disabled";
/// Error texts are bounded so a vendor body can never bloat the ledger.
const MAX_ERROR_CHARS: usize = 240;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundEmail {
    pub to: String,
    pub subject: String,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError {
    /// No sender is configured. The delivery row is marked `sender_disabled`.
    Disabled,
    /// The provider answered with a non-2xx status.
    Rejected { status: u16, detail: String },
    /// The request did not complete (connect failure, timeout, I/O).
    Transport(String),
    /// A 2xx without the documented `id`.
    Malformed(String),
}

impl SendError {
    /// The bounded text written to `alert_deliveries.error`.
    pub fn ledger_text(&self) -> String {
        let s = match self {
            SendError::Disabled => SENDER_DISABLED_ERROR.to_string(),
            SendError::Rejected { status, detail } => format!("provider_rejected:{status}:{detail}"),
            SendError::Transport(d) => format!("transport:{d}"),
            SendError::Malformed(d) => format!("malformed_response:{d}"),
        };
        bounded(&s)
    }
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.ledger_text())
    }
}

fn bounded(s: &str) -> String {
    let s: String = s.chars().filter(|c| !c.is_control()).collect();
    if s.chars().count() > MAX_ERROR_CHARS {
        let cut: String = s.chars().take(MAX_ERROR_CHARS).collect();
        format!("{cut}...")
    } else {
        s
    }
}

pub trait Sender: Send + Sync {
    /// Stable name for logs (`disabled`, `resend`).
    fn name(&self) -> &'static str;
    /// `Ok(provider message id)`.
    fn send(&self, email: &OutboundEmail) -> Result<String, SendError>;
}

/// No credentials: every send is `SendError::Disabled`.
#[derive(Debug, Default, Clone, Copy)]
pub struct DisabledSender;

impl Sender for DisabledSender {
    fn name(&self) -> &'static str {
        "disabled"
    }
    fn send(&self, _email: &OutboundEmail) -> Result<String, SendError> {
        Err(SendError::Disabled)
    }
}

/// `POST {base_url}/emails` with `{"from","to":[..],"subject","text"}`; the documented success body is `{"id": ..}`.
pub struct ResendSender {
    api_key: String,
    from: String,
    base_url: String,
    transport: Arc<dyn HttpTransport>,
}

impl fmt::Debug for ResendSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResendSender").field("from", &self.from).field("base_url", &self.base_url).field("api_key", &"<redacted>").finish()
    }
}

impl ResendSender {
    pub fn new(api_key: &str, from: &str, transport: Arc<dyn HttpTransport>) -> Self {
        Self { api_key: api_key.trim().to_string(), from: from.trim().to_string(), base_url: RESEND_BASE_URL.to_string(), transport }
    }

    /// Tests point this at a fake host; production never changes it.
    pub fn with_base_url(mut self, base_url: &str) -> Self {
        self.base_url = base_url.trim_end_matches('/').to_string();
        self
    }

    pub fn from(&self) -> &str {
        &self.from
    }

    fn scrub(&self, s: &str) -> String {
        if self.api_key.is_empty() {
            s.to_string()
        } else {
            s.replace(&self.api_key, "<redacted>")
        }
    }
}

impl Sender for ResendSender {
    fn name(&self) -> &'static str {
        "resend"
    }

    fn send(&self, email: &OutboundEmail) -> Result<String, SendError> {
        let payload = json!({ "from": self.from, "to": [email.to], "subject": email.subject, "text": email.body });
        let req = HttpRequest {
            method: HttpMethod::Post,
            url: format!("{}/emails", self.base_url),
            headers: vec![
                ("Authorization".to_string(), format!("Bearer {}", self.api_key)),
                ("Content-Type".to_string(), "application/json".to_string()),
                ("Accept".to_string(), "application/json".to_string()),
            ],
            body: Some(payload.to_string()),
        };
        let resp = self.transport.execute(&req).map_err(|e| SendError::Transport(self.scrub(&e.to_string())))?;
        if !(200..300).contains(&resp.status) {
            let detail = serde_json::from_str::<Value>(&resp.body)
                .ok()
                .and_then(|v| v.get("message").and_then(Value::as_str).map(str::to_string))
                .unwrap_or_else(|| "no message in the body".to_string());
            return Err(SendError::Rejected { status: resp.status, detail: self.scrub(&detail) });
        }
        let id = serde_json::from_str::<Value>(&resp.body)
            .ok()
            .and_then(|v| v.get("id").and_then(Value::as_str).map(str::to_string))
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| SendError::Malformed(format!("HTTP {} without an id", resp.status)))?;
        Ok(id)
    }
}

/// The sender the environment configures: [`ResendSender`] when both `RESEND_API_KEY` and `ALERT_FROM_EMAIL` are
/// set and non-empty, else [`DisabledSender`]. The second value says which (for the startup log).
pub fn sender_from_lookup(lookup: impl Fn(&str) -> Option<String>, transport: Arc<dyn HttpTransport>) -> (Box<dyn Sender>, &'static str) {
    let key = lookup(ENV_RESEND_API_KEY).map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let from = lookup(ENV_ALERT_FROM_EMAIL).map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    match (key, from) {
        (Some(k), Some(f)) => (Box::new(ResendSender::new(&k, &f, transport)), "resend"),
        _ => (Box::new(DisabledSender), "disabled"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_adapters::testing::FakeTransport;
    use broker_adapters::transport::TransportError;

    const KEY: &str = "re_test_9f3b7c1d2e4a_not_a_real_key";

    fn email() -> OutboundEmail {
        OutboundEmail { to: "owner@example.com".into(), subject: "[Mendl Labs] critical ALERT_HALT acct".into(), body: "body".into() }
    }

    #[test]
    fn resend_posts_the_documented_shape_with_the_bearer_key_and_returns_the_id() {
        let t = Arc::new(FakeTransport::new());
        t.enqueue_json(200, r#"{"id":"49a3999c-0ce1-4ea6-ab68-afcd6dc2e794"}"#);
        let s = ResendSender::new(KEY, "alerts@mendllabs.ai", t.clone());
        assert_eq!(s.send(&email()).unwrap(), "49a3999c-0ce1-4ea6-ab68-afcd6dc2e794");
        let req = &t.requests()[0];
        assert_eq!(req.method, HttpMethod::Post);
        assert_eq!(req.url, "https://api.resend.com/emails");
        assert_eq!(req.header("authorization"), Some(format!("Bearer {KEY}").as_str()));
        let body: Value = serde_json::from_str(req.body.as_deref().unwrap()).unwrap();
        assert_eq!(body["from"], "alerts@mendllabs.ai");
        assert_eq!(body["to"], json!(["owner@example.com"]));
        assert_eq!(body["subject"], "[Mendl Labs] critical ALERT_HALT acct");
        assert_eq!(body["text"], "body");
        assert!(!format!("{s:?}").contains(KEY), "Debug must not print the key");
    }

    #[test]
    fn rejections_transport_failures_and_malformed_bodies_are_typed_and_never_carry_the_key() {
        let t = Arc::new(FakeTransport::new());
        let s = ResendSender::new(KEY, "alerts@mendllabs.ai", t.clone());
        t.enqueue_json(422, &format!(r#"{{"statusCode":422,"message":"Invalid `to` field for key {KEY}","name":"validation_error"}}"#));
        let e = s.send(&email()).unwrap_err();
        assert!(matches!(e, SendError::Rejected { status: 422, .. }));
        assert!(!e.ledger_text().contains(KEY), "{e}");
        assert!(e.ledger_text().starts_with("provider_rejected:422:"));

        t.enqueue_error(TransportError::Timeout);
        let e = s.send(&email()).unwrap_err();
        assert!(matches!(e, SendError::Transport(_)));
        assert_eq!(e.ledger_text(), "transport:request timed out");

        t.enqueue_json(200, r#"{"ok":true}"#);
        let e = s.send(&email()).unwrap_err();
        assert!(matches!(e, SendError::Malformed(_)));

        let long = "x".repeat(1000);
        t.enqueue_json(500, &format!(r#"{{"message":"{long}"}}"#));
        let e = s.send(&email()).unwrap_err();
        assert!(e.ledger_text().chars().count() <= MAX_ERROR_CHARS + 3);
    }

    #[test]
    fn the_sender_is_disabled_unless_both_variables_are_set() {
        let t: Arc<dyn HttpTransport> = Arc::new(FakeTransport::new());
        let env = |vars: &'static [(&'static str, &'static str)]| move |k: &str| vars.iter().find(|(n, _)| *n == k).map(|(_, v)| (*v).to_string());
        assert_eq!(sender_from_lookup(env(&[]), t.clone()).1, "disabled");
        assert_eq!(sender_from_lookup(env(&[(ENV_RESEND_API_KEY, KEY)]), t.clone()).1, "disabled");
        assert_eq!(sender_from_lookup(env(&[(ENV_ALERT_FROM_EMAIL, "a@b.c")]), t.clone()).1, "disabled");
        assert_eq!(sender_from_lookup(env(&[(ENV_RESEND_API_KEY, "  "), (ENV_ALERT_FROM_EMAIL, "a@b.c")]), t.clone()).1, "disabled");
        let (s, name) = sender_from_lookup(env(&[(ENV_RESEND_API_KEY, KEY), (ENV_ALERT_FROM_EMAIL, "a@b.c")]), t.clone());
        assert_eq!(name, "resend");
        assert_eq!(s.name(), "resend");
        assert_eq!(DisabledSender.send(&email()).unwrap_err(), SendError::Disabled);
        assert_eq!(SendError::Disabled.ledger_text(), SENDER_DISABLED_ERROR);
    }
}
