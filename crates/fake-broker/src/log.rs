//! The event log: every request the exchange saw, what it answered, what the caller actually
//! received, plus a note for each control action a test performed. Tests assert on it ("exactly
//! one AddOrder reached the exchange", "no order-affecting request while halted") and dump it
//! when something fails.

use crate::fault::{FaultKind, Timing};
use broker_adapters::transport::{HttpMethod, TransportError};
use std::fmt::Write as _;

/// Who sent a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// Through the `HttpTransport` handed to the adapter.
    Adapter,
    /// Injected by a test to play another process using the same API key.
    External,
    /// An earlier adapter request that a `Timing::Delayed` fault held back and the test released.
    Delayed,
}

/// What the caller of `HttpTransport::execute` saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivered {
    Response { status: u16, body: String },
    Transport(TransportError),
}

#[derive(Debug, Clone)]
pub struct RequestRecord {
    pub seq: u64,
    pub time_nanos: u64,
    pub origin: Origin,
    pub method: HttpMethod,
    pub path: String,
    pub api_key: Option<String>,
    /// Decoded form (or query) parameters in request order; `nonce` included for private calls.
    pub params: Vec<(String, String)>,
    pub fault: Option<(FaultKind, Timing)>,
    /// True when the exchange processed the request (state may have changed). False when a
    /// `BeforeApply` fault intercepted it or the host did not match.
    pub reached_exchange: bool,
    /// The response the exchange produced, even if the caller never received it.
    pub produced: Option<(u16, String)>,
    /// What the caller saw.
    pub delivered: Delivered,
}

impl RequestRecord {
    pub fn param(&self, key: &str) -> Option<&str> {
        self.params.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    /// Kraken error strings in the response the exchange produced (empty when it succeeded or
    /// never produced one).
    pub fn produced_errors(&self) -> Vec<String> {
        let Some((_, body)) = &self.produced else { return Vec::new() };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else { return Vec::new() };
        v.get("error")
            .and_then(|e| e.as_array())
            .map(|a| a.iter().filter_map(|s| s.as_str().map(str::to_string)).collect())
            .unwrap_or_default()
    }

    /// True for a request that reached the exchange and may have created or cancelled an order.
    pub fn is_order_affecting(&self) -> bool {
        self.reached_exchange && (self.path.ends_with("/AddOrder") || self.path.ends_with("/CancelOrder"))
            && self.param("validate") != Some("true")
    }
}

#[derive(Debug, Clone)]
pub struct ControlRecord {
    pub seq: u64,
    pub time_nanos: u64,
    pub note: String,
}

#[derive(Debug, Clone)]
pub enum LogEntry {
    Request(RequestRecord),
    Control(ControlRecord),
}

impl LogEntry {
    pub fn seq(&self) -> u64 {
        match self {
            LogEntry::Request(r) => r.seq,
            LogEntry::Control(c) => c.seq,
        }
    }
}

/// Render the log for humans (failure messages, debugging).
pub fn render(entries: &[LogEntry]) -> String {
    let mut out = String::new();
    for e in entries {
        match e {
            LogEntry::Control(c) => {
                let _ = writeln!(out, "#{:04} t={} CONTROL {}", c.seq, c.time_nanos, c.note);
            }
            LogEntry::Request(r) => {
                let params: Vec<String> = r
                    .params
                    .iter()
                    .filter(|(k, _)| k != "nonce")
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect();
                let nonce = r.param("nonce").map(|n| format!(" nonce={n}")).unwrap_or_default();
                let origin = match r.origin {
                    Origin::Adapter => "",
                    Origin::External => " [other-process]",
                    Origin::Delayed => " [delayed-delivery]",
                };
                let _ = write!(out, "#{:04} t={} {:?} {}{origin}{nonce} [{}]", r.seq, r.time_nanos, r.method, r.path, params.join("&"));
                if let Some((k, t)) = &r.fault {
                    let _ = write!(out, " FAULT {k:?}/{t:?}");
                }
                if !r.reached_exchange {
                    let _ = write!(out, " (not applied)");
                }
                match &r.delivered {
                    Delivered::Response { status, body } => {
                        let _ = write!(out, " -> {status} {body}");
                    }
                    Delivered::Transport(e) => {
                        let _ = write!(out, " -> transport error: {e}");
                        if let Some((s, b)) = &r.produced {
                            let _ = write!(out, " (exchange had produced {s} {b})");
                        }
                    }
                }
                out.push('\n');
            }
        }
    }
    out
}
