//! Bitstamp v2 credentials, private-request signing, and per-request nonce and timestamp.
//!
//! Signing, as stated on the documentation page (https://www.bitstamp.net/api/, fetched 2026-10-06; the lowercase hex
//! output and the exact message layout are UNVERIFIED against a live server, see the PR):
//!
//! ```text
//! message   = "BITSTAMP " + api_key + verb + host + path + query + content_type + nonce + timestamp_ms + "v2" + body
//! signature = lowercase_hex( HMAC-SHA256(key = api_secret, msg = message) )
//! ```
//!
//! `query` is always empty here (every private call is a POST). A request with no body sends no `Content-Type` header
//! and uses the empty string in its place in the message.
//!
//! Credentials come from the environment only ([`BitstampCredentials::from_env`]). Nothing here reads a file, and
//! neither the key nor the secret is ever rendered by `Debug`, an error, or a header dump.

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::error::BrokerError;
use crate::kraken::auth::encode_form;
use crate::transport::{HttpMethod, HttpRequest};

pub const ENV_API_KEY: &str = "BITSTAMP_API_KEY";
pub const ENV_API_SECRET: &str = "BITSTAMP_API_SECRET";
pub const AUTH_VERSION: &str = "v2";

/// API key and secret. No `Clone`, `Display` or `Serialize`; `Debug` is redacted.
pub struct BitstampCredentials {
    api_key: String,
    secret: String,
}

impl BitstampCredentials {
    /// Validates presence only. The error text names the problem, never the value.
    pub fn new(api_key: &str, secret: &str) -> Result<Self, BrokerError> {
        let api_key = api_key.trim();
        let secret = secret.trim();
        if api_key.is_empty() {
            return Err(BrokerError::Credentials("api key is empty".into()));
        }
        if secret.is_empty() {
            return Err(BrokerError::Credentials("api secret is empty".into()));
        }
        Ok(Self { api_key: api_key.to_string(), secret: secret.to_string() })
    }

    /// Reads [`ENV_API_KEY`] and [`ENV_API_SECRET`] from the process environment.
    pub fn from_env() -> Result<Self, BrokerError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// The lookup seam behind [`from_env`](Self::from_env). Tests use it so they never touch the process environment.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, BrokerError> {
        let key = lookup(ENV_API_KEY).ok_or_else(|| BrokerError::Credentials(format!("{ENV_API_KEY} is not set")))?;
        let secret =
            lookup(ENV_API_SECRET).ok_or_else(|| BrokerError::Credentials(format!("{ENV_API_SECRET} is not set")))?;
        Self::new(&key, &secret)
    }

    /// Stable, non-secret identifier for this key (first 8 bytes of SHA-256(api_key), hex), for correlating logs.
    pub fn key_id(&self) -> String {
        let d = Sha256::digest(self.api_key.as_bytes());
        d[..8].iter().map(|b| format!("{b:02x}")).collect()
    }

    fn api_key(&self) -> &str {
        &self.api_key
    }

    fn secret(&self) -> &[u8] {
        self.secret.as_bytes()
    }
}

impl fmt::Debug for BitstampCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BitstampCredentials")
            .field("api_key", &"<redacted>")
            .field("key_id", &self.key_id())
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// The clock behind `X-Auth-Timestamp` (UTC milliseconds). Injected so tests are deterministic.
pub trait MillisClock: Send + Sync {
    fn now_millis(&self) -> u64;
}

/// The real clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemMillis;

impl MillisClock for SystemMillis {
    fn now_millis(&self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)).unwrap_or(0)
    }
}

/// Mints the `X-Auth-Nonce` values: a lowercase, 36-character, UUID-shaped string. Bitstamp accepts each nonce once
/// within 150 seconds, so a nonce is built from the millisecond clock (48 bits), a per-minter counter (16 bits) and a
/// per-process random seed (64 bits). Two calls in one millisecond differ in the counter; two processes differ in
/// the seed.
#[derive(Debug)]
pub struct NonceMinter {
    seed: u64,
    counter: AtomicU64,
}

impl Default for NonceMinter {
    fn default() -> Self {
        Self::new()
    }
}

impl NonceMinter {
    pub fn new() -> Self {
        let seed = RandomState::new().build_hasher().finish();
        Self { seed, counter: AtomicU64::new(0) }
    }

    pub fn mint(&self, millis: u64) -> String {
        let count = self.counter.fetch_add(1, Ordering::Relaxed) & 0xffff;
        let high = ((millis & 0xffff_ffff_ffff) << 16) | count;
        let hex = format!("{high:016x}{:016x}", self.seed);
        format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
    }
}

/// The fields the signature covers, in the order the documented message lists them.
pub struct SigningInput<'a> {
    pub verb: &'a str,
    pub host: &'a str,
    pub path: &'a str,
    pub content_type: &'a str,
    pub nonce: &'a str,
    pub timestamp_ms: u64,
    pub body: &'a str,
}

/// The documented signature over the message above, as lowercase hex.
pub fn signature(creds: &BitstampCredentials, input: &SigningInput<'_>) -> String {
    let SigningInput { verb, host, path, content_type, nonce, timestamp_ms, body } = *input;
    let message = format!(
        "BITSTAMP {}{verb}{host}{path}{content_type}{nonce}{timestamp_ms}{AUTH_VERSION}{body}",
        creds.api_key()
    );
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(creds.secret()).expect("HMAC accepts any key length");
    mac.update(message.as_bytes());
    mac.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

/// Build a signed private POST. `params` are form-encoded in the given order; an empty list means no body.
pub fn build_private_request(
    creds: &BitstampCredentials,
    host: &str,
    path: &str,
    nonce: &str,
    timestamp_ms: u64,
    params: &[(String, String)],
) -> HttpRequest {
    let body = encode_form(params);
    let content_type = if body.is_empty() { "" } else { "application/x-www-form-urlencoded" };
    let input = SigningInput { verb: "POST", host, path, content_type, nonce, timestamp_ms, body: &body };
    let sig = signature(creds, &input);
    let mut headers = vec![
        ("X-Auth".to_string(), format!("BITSTAMP {}", creds.api_key())),
        ("X-Auth-Signature".to_string(), sig),
        ("X-Auth-Nonce".to_string(), nonce.to_string()),
        ("X-Auth-Timestamp".to_string(), timestamp_ms.to_string()),
        ("X-Auth-Version".to_string(), AUTH_VERSION.to_string()),
    ];
    let body = if body.is_empty() {
        None
    } else {
        headers.push(("Content-Type".to_string(), content_type.to_string()));
        Some(body)
    };
    HttpRequest { method: HttpMethod::Post, url: format!("https://{host}{path}"), headers, body }
}
