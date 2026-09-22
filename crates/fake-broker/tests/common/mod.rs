//! Shared helpers for the integration tests. Not every test file uses every helper.
#![allow(dead_code)]

use broker_adapters::kraken::auth::{build_private_request, KrakenCredentials};
use broker_adapters::transport::{HttpMethod, HttpRequest, HttpResponse, HttpTransport, TransportError};
use fake_broker::testkit::KrakenRig;
use fake_broker::{default_secret_b64, DEFAULT_API_KEY};
use serde_json::Value;
use std::cell::Cell;

pub const BASE: &str = "https://api.kraken.com";

pub fn creds() -> KrakenCredentials {
    KrakenCredentials::new(DEFAULT_API_KEY, &default_secret_b64()).unwrap()
}

pub fn pairs(params: &[(&str, &str)]) -> Vec<(String, String)> {
    params.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

/// Sends hand-built, correctly signed private requests with an incrementing nonce, straight at
/// the wire (no adapter), so the tests can see the raw JSON.
pub struct Wire<'a> {
    pub rig: &'a KrakenRig,
    pub nonce: Cell<u64>,
}

impl<'a> Wire<'a> {
    pub fn new(rig: &'a KrakenRig) -> Self {
        Self { rig, nonce: Cell::new(1_000) }
    }

    pub fn next_nonce(&self) -> u64 {
        let n = self.nonce.get() + 1;
        self.nonce.set(n);
        n
    }

    pub fn post(&self, path: &str, params: &[(&str, &str)]) -> Value {
        self.post_with_nonce(path, self.next_nonce(), params)
    }

    pub fn post_with_nonce(&self, path: &str, nonce: u64, params: &[(&str, &str)]) -> Value {
        let req = build_private_request(&creds(), BASE, path, nonce, &pairs(params));
        let resp = self.rig.transport.execute(&req).expect("transport ok");
        assert_eq!(resp.status, 200, "{}", resp.body);
        serde_json::from_str(&resp.body).expect("json body")
    }

    pub fn send(&self, req: &HttpRequest) -> Result<HttpResponse, TransportError> {
        self.rig.transport.execute(req)
    }

    /// `result` of a call that must have succeeded.
    pub fn ok(&self, path: &str, params: &[(&str, &str)]) -> Value {
        let v = self.post(path, params);
        assert!(errors(&v).is_empty(), "unexpected error {v}");
        v["result"].clone()
    }

    /// Error strings of a call that must have failed.
    pub fn err(&self, path: &str, params: &[(&str, &str)]) -> Vec<String> {
        let v = self.post(path, params);
        let e = errors(&v);
        assert!(!e.is_empty(), "expected an error, got {v}");
        e
    }

    pub fn get(&self, url: &str) -> Value {
        let req = HttpRequest { method: HttpMethod::Get, url: url.to_string(), headers: Vec::new(), body: None };
        let resp = self.rig.transport.execute(&req).unwrap();
        serde_json::from_str(&resp.body).unwrap()
    }
}

pub fn errors(v: &Value) -> Vec<String> {
    v["error"].as_array().map(|a| a.iter().map(|s| s.as_str().unwrap().to_string()).collect()).unwrap_or_default()
}

pub const ADD: &str = "/0/private/AddOrder";
pub const CANCEL: &str = "/0/private/CancelOrder";
pub const QUERY: &str = "/0/private/QueryOrders";
pub const OPEN: &str = "/0/private/OpenOrders";
pub const CLOSED: &str = "/0/private/ClosedOrders";
pub const BALANCE: &str = "/0/private/Balance";
pub const TRADE_BALANCE: &str = "/0/private/TradeBalance";

/// Sign `body` for `path` with the default account's secret using an INDEPENDENT copy of the
/// algorithm (so tests can sign bodies the adapter would never produce, such as a bad nonce).
pub fn sign_text(path: &str, nonce_text: &str, body: &str) -> String {
    use base64::Engine;
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256, Sha512};
    let secret = base64::engine::general_purpose::STANDARD.decode(default_secret_b64()).unwrap();
    let mut sha = Sha256::new();
    sha.update(nonce_text.as_bytes());
    sha.update(body.as_bytes());
    let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(&secret).unwrap();
    mac.update(path.as_bytes());
    mac.update(&sha.finalize());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}
