//! The API key: where it comes from, how it is held, and how it is kept out of everything else.
//!
//! The key is sent in exactly one place, the `Authorization: Bearer` header of a request (see `source.rs`). It is
//! never part of a URL, an error message, a provenance record, a log line or a `Debug` rendering:
//! * [`SecretString`] has a redacting `Debug` and no `Display`;
//! * [`KeyProvider`] is the seam: production reads `MASSIVE_API_KEY` once at construction ([`EnvKeyProvider`]), tests
//!   hand in a fixed key ([`StaticKeyProvider`]);
//! * [`scrub`] removes the key value (and any `apiKey=...` fragment) from text that is about to become an error.

use std::fmt;

/// Text that must never be printed. `Debug` shows a placeholder; there is deliberately no `Display`.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The raw value. Call sites are the request builder and [`scrub`]; keep it that way.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error("environment variable {0} is not set (or is not valid unicode)")]
    Missing(String),
    #[error("the API key is empty")]
    Empty,
    #[error("the API key contains characters that cannot be sent in an HTTP header")]
    NotHeaderSafe,
}

/// Validate and trim a raw key: non-empty, printable ASCII without spaces (so it can never inject a header line).
pub fn validate_key(raw: &str) -> Result<SecretString, KeyError> {
    let t = raw.trim();
    if t.is_empty() {
        return Err(KeyError::Empty);
    }
    if !t.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err(KeyError::NotHeaderSafe);
    }
    Ok(SecretString::new(t))
}

/// Where the source gets its key. `api_key` is called for every request, so a provider may rotate; it must never
/// log the key.
pub trait KeyProvider: Send + Sync {
    fn api_key(&self) -> Result<SecretString, KeyError>;
}

/// The key of the environment variable `MASSIVE_API_KEY`, read ONCE when the provider is built.
pub struct EnvKeyProvider {
    var: String,
    key: SecretString,
}

impl EnvKeyProvider {
    pub const DEFAULT_VAR: &'static str = "MASSIVE_API_KEY";

    pub fn from_env() -> Result<Self, KeyError> {
        Self::from_var(Self::DEFAULT_VAR)
    }

    pub fn from_var(name: &str) -> Result<Self, KeyError> {
        let raw = std::env::var(name).map_err(|_| KeyError::Missing(name.to_string()))?;
        Ok(Self { var: name.to_string(), key: validate_key(&raw)? })
    }
}

impl fmt::Debug for EnvKeyProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvKeyProvider").field("var", &self.var).field("key", &"<redacted>").finish()
    }
}

impl KeyProvider for EnvKeyProvider {
    fn api_key(&self) -> Result<SecretString, KeyError> {
        Ok(self.key.clone())
    }
}

/// A fixed key (tests, or a wiring crate that already holds the key).
pub struct StaticKeyProvider(SecretString);

impl StaticKeyProvider {
    pub fn new(key: &str) -> Result<Self, KeyError> {
        Ok(Self(validate_key(key)?))
    }
}

impl fmt::Debug for StaticKeyProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StaticKeyProvider(<redacted>)")
    }
}

impl KeyProvider for StaticKeyProvider {
    fn api_key(&self) -> Result<SecretString, KeyError> {
        Ok(self.0.clone())
    }
}

/// Query-parameter names (compared case-insensitively, after percent-decoding) that carry a key.
pub(crate) fn is_key_param(name: &str) -> bool {
    let decoded = percent_decode_lossy(name).to_ascii_lowercase();
    matches!(decoded.as_str(), "apikey" | "api_key" | "api-key")
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// `%XX` decoding for parameter NAMES (never panics; an invalid escape is kept literally).
pub(crate) fn percent_decode_lossy(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Remove the key from text that is about to be shown: every occurrence of `secret` becomes `<redacted>`, and any
/// `apikey=<value>` fragment (any case) loses its value even when the value is not the configured key.
pub fn scrub(text: &str, secret: Option<&str>) -> String {
    let mut out = match secret {
        Some(s) if !s.is_empty() => text.replace(s, "<redacted>"),
        _ => text.to_string(),
    };
    for needle in ["apikey=", "api_key=", "api-key="] {
        let mut from = 0usize;
        loop {
            let lower = out.to_ascii_lowercase();
            let Some(rel) = lower[from..].find(needle) else { break };
            let value_start = from + rel + needle.len();
            let value_end = out[value_start..]
                .find(|c: char| c == '&' || c == '#' || c.is_whitespace() || c == '"' || c == '\'' || c == ')')
                .map(|n| value_start + n)
                .unwrap_or(out.len());
            out.replace_range(value_start..value_end, "<redacted>");
            from = value_start + "<redacted>".len();
            if from >= out.len() {
                break;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_shows_the_key() {
        let s = SecretString::new("sup3rs3cretvalue");
        assert!(!format!("{s:?}").contains("sup3rs3cret"));
        assert!(!format!("{s:#?}").contains("sup3rs3cret"));
        let p = StaticKeyProvider::new("sup3rs3cretvalue").unwrap();
        assert!(!format!("{p:?}").contains("sup3rs3cret"));
    }

    #[test]
    fn validate_rejects_empty_and_header_injection() {
        assert_eq!(validate_key("  ").unwrap_err(), KeyError::Empty);
        assert_eq!(validate_key("abc\r\nX-Evil: 1").unwrap_err(), KeyError::NotHeaderSafe);
        assert_eq!(validate_key("ab cd").unwrap_err(), KeyError::NotHeaderSafe);
        assert_eq!(validate_key("k\u{e9}y").unwrap_err(), KeyError::NotHeaderSafe);
        assert_eq!(validate_key(" abc123 ").unwrap().expose(), "abc123");
    }

    #[test]
    fn scrub_removes_the_value_and_any_apikey_fragment() {
        let t = scrub("GET https://h/x?cursor=1&apiKey=LEAKED&z=2 failed; also KEYVALUE1", Some("KEYVALUE1"));
        assert!(!t.contains("LEAKED") && !t.contains("KEYVALUE1"), "{t}");
        assert!(t.contains("z=2"));
        assert_eq!(scrub("APIKEY=abc", None), "APIKEY=<redacted>");
        assert_eq!(scrub("nothing here", Some("")), "nothing here");
    }

    #[test]
    fn scrub_is_total_on_odd_input() {
        for s in ["", "apikey=", "apikey=&apikey=", "%", "%%%", "apiKey=\u{1F600}x", "\u{e9}apikey=\u{e9}"] {
            let _ = scrub(s, Some("k"));
            let _ = percent_decode_lossy(s);
        }
    }

    #[test]
    fn key_param_names_are_matched_after_decoding() {
        assert!(is_key_param("apiKey"));
        assert!(is_key_param("APIKEY"));
        assert!(is_key_param("api%4Bey"));
        assert!(is_key_param("api_key"));
        assert!(!is_key_param("cursor"));
        assert!(!is_key_param("limit"));
    }
}
