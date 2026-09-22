//! Fault injection at the HTTP boundary. A fault fires for the next N matching requests and
//! decides two things: WHAT the caller sees, and WHETHER the exchange applied the request.
//!
//! * [`Timing::BeforeApply`]: the request never reached (or was refused by) the exchange. State
//!   is untouched, the nonce is not consumed.
//! * [`Timing::AfterApply`]: the exchange processed the request normally (orders created,
//!   nonce consumed) and only the response is replaced or lost. This is the dangerous
//!   "unknown outcome" case: the caller cannot tell from the response that anything happened.
//! * [`Timing::Delayed`]: the caller gets the failure now but the request is still in flight and
//!   reaches the exchange later. Because Kraken rejects any nonce not above the highest seen, a
//!   late request loses to every newer request that got there first.

use broker_adapters::transport::TransportError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timing {
    BeforeApply,
    AfterApply,
    /// The caller sees the failure now, but the request is still in flight: the exchange
    /// processes it later, when the test calls `deliver_delayed`. The request is authenticated
    /// and nonce-checked at THAT moment, exactly as a request that arrives late would be.
    Delayed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultKind {
    /// `TransportError::Timeout`.
    Timeout,
    /// `TransportError::ConnectFailed`. Only meaningful before the request is applied.
    ConnectFailed,
    /// `TransportError::Io` (connection reset mid-flight).
    IoError,
    /// An HTTP status with a body, for example a gateway's 502 page.
    Http { status: u16, body: String },
    /// HTTP 200 with a body that is not valid JSON.
    MalformedBody(String),
    /// Kraken's rate-limit error, `EAPI:Rate limit exceeded`, in a 200 response.
    RateLimit,
    /// Any Kraken error string in a 200 response, for example `EService:Unavailable`. With
    /// [`Timing::AfterApply`] this is the "exchange placed the order but answered with an error"
    /// case.
    ExchangeError(String),
}

/// Which requests a fault applies to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestMatcher {
    /// Exact URL path, for example `/0/private/AddOrder`. `None` = any path.
    pub path: Option<String>,
}

impl RequestMatcher {
    pub(crate) fn matches(&self, path: &str) -> bool {
        self.path.as_deref().is_none_or(|p| p == path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fault {
    pub kind: FaultKind,
    pub timing: Timing,
    /// How many matching requests still get the fault.
    pub remaining: u32,
    /// How many matching requests pass through cleanly before the fault starts.
    pub skip: u32,
    pub matcher: RequestMatcher,
}

impl Fault {
    fn new(kind: FaultKind) -> Self {
        Self { kind, timing: Timing::BeforeApply, remaining: 1, skip: 0, matcher: RequestMatcher::default() }
    }

    pub fn timeout() -> Self {
        Self::new(FaultKind::Timeout)
    }
    pub fn connect_failed() -> Self {
        Self::new(FaultKind::ConnectFailed)
    }
    pub fn io_error() -> Self {
        Self::new(FaultKind::IoError)
    }
    pub fn http(status: u16) -> Self {
        Self::new(FaultKind::Http { status, body: format!("<html><body>HTTP {status}</body></html>") })
    }
    pub fn http_with_body(status: u16, body: &str) -> Self {
        Self::new(FaultKind::Http { status, body: body.to_string() })
    }
    pub fn malformed_body() -> Self {
        Self::new(FaultKind::MalformedBody(r#"{"error":[],"result":{"descr":{"order":"buy 0.0"#.to_string()))
    }
    pub fn rate_limit() -> Self {
        Self::new(FaultKind::RateLimit)
    }
    pub fn exchange_error(code: &str) -> Self {
        Self::new(FaultKind::ExchangeError(code.to_string()))
    }

    /// Apply the request first, then replace or lose the response.
    pub fn after_apply(mut self) -> Self {
        assert!(
            self.kind != FaultKind::ConnectFailed,
            "fake-broker: a connect failure means the request was never sent, so it cannot be AfterApply"
        );
        self.timing = Timing::AfterApply;
        self
    }
    /// The caller sees the failure now; the request lands at the exchange later (see
    /// [`Timing::Delayed`]).
    pub fn delayed(mut self) -> Self {
        assert!(
            self.kind != FaultKind::ConnectFailed,
            "fake-broker: a connect failure means the request was never sent, so it cannot be Delayed"
        );
        self.timing = Timing::Delayed;
        self
    }
    /// Fire for the next `n` matching requests (default 1).
    pub fn times(mut self, n: u32) -> Self {
        self.remaining = n;
        self
    }
    /// Fire until cleared: an outage.
    pub fn forever(self) -> Self {
        self.times(u32::MAX)
    }
    /// Let the first `n` matching requests through untouched, then start faulting.
    pub fn after_requests(mut self, n: u32) -> Self {
        self.skip = n;
        self
    }
    pub fn on_path(mut self, path: &str) -> Self {
        self.matcher.path = Some(path.to_string());
        self
    }
}

/// FIFO of pending faults. The first fault whose matcher matches decides the request.
#[derive(Debug, Default)]
pub struct FaultQueue {
    faults: Vec<Fault>,
}

impl FaultQueue {
    pub fn push(&mut self, fault: Fault) {
        self.faults.push(fault);
    }

    pub fn clear(&mut self) {
        self.faults.clear();
    }

    pub fn pending(&self) -> &[Fault] {
        &self.faults
    }

    /// Decide the fate of one request. `None` = pass through cleanly.
    pub fn take(&mut self, path: &str) -> Option<(FaultKind, Timing)> {
        let i = self.faults.iter().position(|f| f.remaining > 0 && f.matcher.matches(path))?;
        let f = &mut self.faults[i];
        if f.skip > 0 {
            f.skip -= 1;
            return None;
        }
        let out = (f.kind.clone(), f.timing);
        if f.remaining != u32::MAX {
            f.remaining -= 1;
        }
        if f.remaining == 0 {
            self.faults.remove(i);
        }
        Some(out)
    }
}

/// The transport-level error a fault produces, if it produces one.
pub(crate) fn transport_error(kind: &FaultKind) -> Option<TransportError> {
    match kind {
        FaultKind::Timeout => Some(TransportError::Timeout),
        FaultKind::ConnectFailed => Some(TransportError::ConnectFailed("connection refused (injected)".into())),
        FaultKind::IoError => Some(TransportError::Io("connection reset by peer (injected)".into())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_skips_and_matchers() {
        let mut q = FaultQueue::default();
        q.push(Fault::timeout().times(2).after_requests(1).on_path("/a"));
        assert_eq!(q.take("/b"), None, "other paths are untouched");
        assert_eq!(q.take("/a"), None, "first matching request is skipped");
        assert!(q.take("/a").is_some());
        assert!(q.take("/a").is_some());
        assert_eq!(q.take("/a"), None, "exhausted");
        assert!(q.pending().is_empty());
    }

    #[test]
    fn forever_never_runs_out_until_cleared() {
        let mut q = FaultQueue::default();
        q.push(Fault::rate_limit().forever());
        for _ in 0..1000 {
            assert!(q.take("/x").is_some());
        }
        q.clear();
        assert!(q.take("/x").is_none());
    }

    #[test]
    #[should_panic(expected = "never sent")]
    fn connect_failure_cannot_be_after_apply() {
        let _ = Fault::connect_failed().after_apply();
    }
}
