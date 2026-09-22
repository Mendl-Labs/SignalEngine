//! HTTP behind a trait so all adapter logic is testable offline.

use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    /// Added for Alpaca (cancel order, close position). Kraken uses only GET and POST.
    Delete,
}

#[derive(Clone)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
}

impl HttpRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

/// Header names whose values must never appear in logs.
const SENSITIVE_HEADERS: [&str; 5] =
    ["api-key", "api-sign", "authorization", "apca-api-key-id", "apca-api-secret-key"];

impl fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers: Vec<(&str, &str)> = self
            .headers
            .iter()
            .map(|(k, v)| {
                if SENSITIVE_HEADERS.iter().any(|s| k.eq_ignore_ascii_case(s)) {
                    (k.as_str(), "<redacted>")
                } else {
                    (k.as_str(), v.as_str())
                }
            })
            .collect();
        f.debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .field("headers", &headers)
            .field("body_len", &self.body.as_ref().map(String::len))
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

/// A response that also carries the HTTP headers. Adapters that need a header (Alpaca reads
/// `Retry-After` on HTTP 429) call [`HttpTransport::execute_detailed`]. Kept separate from
/// [`HttpResponse`] so existing struct literals of `HttpResponse` keep compiling.
#[derive(Debug, Clone)]
pub struct HttpResponseDetailed {
    pub status: u16,
    pub body: String,
    pub headers: Vec<(String, String)>,
}

impl HttpResponseDetailed {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

impl From<HttpResponse> for HttpResponseDetailed {
    fn from(r: HttpResponse) -> Self {
        Self { status: r.status, body: r.body, headers: Vec::new() }
    }
}

/// Transport failures, split by what they say about whether the request reached the server.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    /// The connection could not be established: the request was definitely NOT sent.
    #[error("connect failed: {0}")]
    ConnectFailed(String),
    /// The request may have been processed. Outcome unknown.
    #[error("request timed out")]
    Timeout,
    /// Any other I/O failure after the request may have been sent. Outcome unknown.
    #[error("i/o error: {0}")]
    Io(String),
}

impl TransportError {
    pub fn request_definitely_not_sent(&self) -> bool {
        matches!(self, TransportError::ConnectFailed(_))
    }
}

pub trait HttpTransport: Send + Sync {
    fn execute(&self, req: &HttpRequest) -> Result<HttpResponse, TransportError>;

    /// Like [`execute`](Self::execute) but also returns the response headers. The default calls
    /// `execute` and reports no headers; transports that can see headers override it.
    fn execute_detailed(&self, req: &HttpRequest) -> Result<HttpResponseDetailed, TransportError> {
        self.execute(req).map(Into::into)
    }
}

impl<T: HttpTransport + ?Sized> HttpTransport for Arc<T> {
    fn execute(&self, req: &HttpRequest) -> Result<HttpResponse, TransportError> {
        (**self).execute(req)
    }
    fn execute_detailed(&self, req: &HttpRequest) -> Result<HttpResponseDetailed, TransportError> {
        (**self).execute_detailed(req)
    }
}

impl<T: HttpTransport + ?Sized> HttpTransport for Box<T> {
    fn execute(&self, req: &HttpRequest) -> Result<HttpResponse, TransportError> {
        (**self).execute(req)
    }
    fn execute_detailed(&self, req: &HttpRequest) -> Result<HttpResponseDetailed, TransportError> {
        (**self).execute_detailed(req)
    }
}

/// Real transport on `reqwest::blocking`. Optional; not compiled by default.
#[cfg(feature = "reqwest-transport")]
pub mod reqwest_transport {
    use super::{HttpMethod, HttpRequest, HttpResponse, HttpResponseDetailed, HttpTransport, TransportError};
    use std::time::Duration;

    pub struct ReqwestTransport {
        client: reqwest::blocking::Client,
    }

    impl ReqwestTransport {
        pub fn new() -> Result<Self, TransportError> {
            let client = reqwest::blocking::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| TransportError::Io(e.to_string()))?;
            Ok(Self { client })
        }
    }

    impl HttpTransport for ReqwestTransport {
        fn execute(&self, req: &HttpRequest) -> Result<HttpResponse, TransportError> {
            self.execute_detailed(req).map(|r| HttpResponse { status: r.status, body: r.body })
        }

        fn execute_detailed(&self, req: &HttpRequest) -> Result<HttpResponseDetailed, TransportError> {
            let mut b = match req.method {
                HttpMethod::Get => self.client.get(&req.url),
                HttpMethod::Post => self.client.post(&req.url),
                HttpMethod::Delete => self.client.delete(&req.url),
            };
            for (k, v) in &req.headers {
                b = b.header(k.as_str(), v.as_str());
            }
            if let Some(body) = &req.body {
                b = b.body(body.clone());
            }
            let resp = b.send().map_err(|e| {
                if e.is_timeout() {
                    TransportError::Timeout
                } else if e.is_connect() {
                    TransportError::ConnectFailed(e.to_string())
                } else {
                    TransportError::Io(e.to_string())
                }
            })?;
            let status = resp.status().as_u16();
            let headers: Vec<(String, String)> = resp
                .headers()
                .iter()
                .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.as_str().to_string(), v.to_string())))
                .collect();
            let body = resp.text().map_err(|e| TransportError::Io(e.to_string()))?;
            Ok(HttpResponseDetailed { status, body, headers })
        }
    }
}
