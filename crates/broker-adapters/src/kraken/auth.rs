//! Kraken request signing and credential handling.
//!
//! Algorithm (VERIFIED-FROM-REPO-CODE in SignalEngine `generic/auth.rs` `compute_kraken_signature`,
//! and consistent with FROM-MEMORY-OF-DOCS):
//!
//! ```text
//! API-Sign = base64( HMAC-SHA512( key = base64_decode(secret),
//!                                 msg = uri_path_bytes || SHA256( nonce_decimal_string || urlencoded_post_body ) ) )
//! ```
//!
//! The POST body itself must contain `nonce=<same value>`; the signature covers the exact body
//! bytes that are sent. Everything here is pure.

use crate::error::BrokerError;
use crate::transport::{HttpMethod, HttpRequest};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256, Sha512};
use std::fmt;

/// Secret key bytes. No `Clone`, `Display`, or `Serialize`; `Debug` is redacted; best-effort wipe
/// on drop.
struct SecretBytes(Vec<u8>);

impl fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl Drop for SecretBytes {
    fn drop(&mut self) {
        self.0.fill(0);
        std::hint::black_box(&mut self.0);
    }
}

/// API key plus decoded secret. `Debug` never prints either value.
pub struct KrakenCredentials {
    api_key: String,
    secret: SecretBytes,
}

impl KrakenCredentials {
    /// `secret_b64` is the base64 private key exactly as Kraken displays it. The error message
    /// deliberately says nothing about the input.
    pub fn new(api_key: impl Into<String>, secret_b64: &str) -> Result<Self, BrokerError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(BrokerError::Credentials("api key is empty".into()));
        }
        let bytes = B64
            .decode(secret_b64.trim())
            .map_err(|_| BrokerError::Credentials("secret is not valid base64".into()))?;
        if bytes.is_empty() {
            return Err(BrokerError::Credentials("secret is empty".into()));
        }
        Ok(Self { api_key, secret: SecretBytes(bytes) })
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Stable, non-secret identifier for this key (first 8 bytes of SHA-256(api_key), hex).
    /// Use it to name the per-key nonce file and to correlate logs.
    pub fn key_id(&self) -> String {
        let d = Sha256::digest(self.api_key.as_bytes());
        d[..8].iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Compute `API-Sign` for `path`, `nonce`, and the exact encoded `body` (which must include
    /// the nonce field).
    pub fn sign(&self, path: &str, nonce: u64, body: &str) -> String {
        sign_with_secret(&self.secret.0, path, nonce, body)
    }
}

impl fmt::Debug for KrakenCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KrakenCredentials")
            .field("api_key", &"<redacted>")
            .field("key_id", &self.key_id())
            .field("secret", &self.secret)
            .finish()
    }
}

/// The raw algorithm over an already-decoded secret. Exposed for the cross-check tests.
pub fn sign_with_secret(secret: &[u8], path: &str, nonce: u64, body: &str) -> String {
    let mut sha = Sha256::new();
    sha.update(nonce.to_string().as_bytes());
    sha.update(body.as_bytes());
    let digest = sha.finalize();

    let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(path.as_bytes());
    mac.update(&digest);
    B64.encode(mac.finalize().into_bytes())
}

/// Percent-encode per RFC 3986 (unreserved characters kept, everything else `%XX`, space as `%20`).
pub fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// `k=v&k=v` in the given order, keys and values percent-encoded.
pub fn encode_form(params: &[(String, String)]) -> String {
    params.iter().map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v))).collect::<Vec<_>>().join("&")
}

/// Build a signed private POST. `nonce` is placed FIRST in the body and signed over.
pub fn build_private_request(
    creds: &KrakenCredentials,
    base_url: &str,
    path: &str,
    nonce: u64,
    params: &[(String, String)],
) -> HttpRequest {
    let mut all: Vec<(String, String)> = Vec::with_capacity(params.len() + 1);
    all.push(("nonce".to_string(), nonce.to_string()));
    all.extend(params.iter().filter(|(k, _)| k != "nonce").cloned());
    let body = encode_form(&all);
    let sig = creds.sign(path, nonce, &body);
    HttpRequest {
        method: HttpMethod::Post,
        url: format!("{}{}", base_url.trim_end_matches('/'), path),
        headers: vec![
            ("API-Key".to_string(), creds.api_key().to_string()),
            ("API-Sign".to_string(), sig),
            ("Content-Type".to_string(), "application/x-www-form-urlencoded; charset=utf-8".to_string()),
        ],
        body: Some(body),
    }
}

/// Build an unsigned public GET.
pub fn build_public_request(base_url: &str, path: &str, params: &[(String, String)]) -> HttpRequest {
    let mut url = format!("{}{}", base_url.trim_end_matches('/'), path);
    if !params.is_empty() {
        url.push('?');
        url.push_str(&encode_form(params));
    }
    HttpRequest { method: HttpMethod::Get, url, headers: Vec::new(), body: None }
}
