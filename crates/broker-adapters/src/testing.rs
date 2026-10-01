//! Offline test doubles: [`FakeTransport`] and [`ManualClock`]. Public so the rebalancer's own
//! tests can use them; nothing here touches the network.

use crate::nonce::Clock;
use crate::transport::{HttpRequest, HttpResponse, HttpResponseDetailed, HttpTransport, TransportError};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

type Handler = Box<dyn Fn(&HttpRequest) -> Result<HttpResponseDetailed, TransportError> + Send + Sync>;

/// In-memory transport. Responses are either scripted (FIFO) or produced by a handler; every
/// request is recorded in arrival order.
#[derive(Default)]
pub struct FakeTransport {
    queue: Mutex<VecDeque<Result<HttpResponseDetailed, TransportError>>>,
    handler: Mutex<Option<Handler>>,
    recorded: Mutex<Vec<HttpRequest>>,
}

impl FakeTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue_json(&self, status: u16, body: &str) -> &Self {
        self.enqueue_json_with_headers(status, body, &[])
    }

    /// Like [`enqueue_json`](Self::enqueue_json) with response headers (visible to adapters that
    /// call `execute_detailed`, e.g. Alpaca reading `Retry-After`).
    pub fn enqueue_json_with_headers(&self, status: u16, body: &str, headers: &[(&str, &str)]) -> &Self {
        let headers = headers.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect();
        self.queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(Ok(HttpResponseDetailed { status, body: body.to_string(), headers }));
        self
    }

    pub fn enqueue_error(&self, err: TransportError) -> &Self {
        self.queue.lock().unwrap_or_else(|e| e.into_inner()).push_back(Err(err));
        self
    }

    pub fn set_handler(
        &self,
        f: impl Fn(&HttpRequest) -> Result<HttpResponse, TransportError> + Send + Sync + 'static,
    ) {
        *self.handler.lock().unwrap_or_else(|e| e.into_inner()) = Some(Box::new(move |req| f(req).map(Into::into)));
    }

    /// Like [`set_handler`](Self::set_handler) but the handler can also return response headers.
    pub fn set_handler_detailed(
        &self,
        f: impl Fn(&HttpRequest) -> Result<HttpResponseDetailed, TransportError> + Send + Sync + 'static,
    ) {
        *self.handler.lock().unwrap_or_else(|e| e.into_inner()) = Some(Box::new(f));
    }

    pub fn requests(&self) -> Vec<HttpRequest> {
        self.recorded.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn request_count(&self) -> usize {
        self.recorded.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

impl HttpTransport for FakeTransport {
    fn execute(&self, req: &HttpRequest) -> Result<HttpResponse, TransportError> {
        self.execute_detailed(req).map(|r| HttpResponse { status: r.status, body: r.body })
    }

    fn execute_detailed(&self, req: &HttpRequest) -> Result<HttpResponseDetailed, TransportError> {
        self.recorded.lock().unwrap_or_else(|e| e.into_inner()).push(req.clone());
        if let Some(h) = self.handler.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
            return h(req);
        }
        self.queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front()
            .unwrap_or_else(|| Err(TransportError::Io("FakeTransport: no scripted response left".into())))
    }
}

/// A clock the test sets by hand (can also run backwards).
pub struct ManualClock(AtomicU64);

impl ManualClock {
    pub fn new(nanos: u64) -> Self {
        Self(AtomicU64::new(nanos))
    }
    pub fn set(&self, nanos: u64) {
        self.0.store(nanos, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_nanos(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
