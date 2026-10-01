//! URL handling for the two places a URL is built or accepted: the base URL of the configuration, and the `next_url`
//! a paginated response hands back.
//!
//! The rule for `next_url`: the vendor is trusted to say WHERE the next page is, never to tell us what credentials to
//! send. So before it is followed it is checked (https, same host as the configured base URL, no user info) and
//! rebuilt WITHOUT any `apiKey` / `api_key` query parameter (and without any parameter that carries the configured
//! key's value); the caller then re-adds the key as the `Authorization: Bearer` header, exactly as for the first page.
//! Nothing here panics on any input.

use crate::secret::is_key_param;

/// A `next_url` that is safe to follow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SanitizedUrl {
    /// The absolute URL to request (no key anywhere in it).
    pub url: String,
    /// The same request as a path (+ query) for provenance: the `cursor` value is elided.
    pub record: String,
    /// A key-bearing query parameter was present in the vendor's URL and was removed.
    pub scrubbed: bool,
}

/// `scheme://host[:port]` of the base URL, validated: https only, no user info, no path, query or fragment.
/// Returns the authority (`host[:port]`).
pub fn base_authority(base_url: &str) -> Result<String, String> {
    let trimmed = base_url.trim_end_matches('/');
    let rest = trimmed.strip_prefix("https://").ok_or_else(|| "the base URL must start with https:// (the key is never sent in clear)".to_string())?;
    if rest.is_empty() || rest.contains(['/', '?', '#', '@', ' ']) || rest.chars().any(|c| c.is_control()) {
        return Err("the base URL must be exactly https://host[:port] (no path, query, fragment or user info)".to_string());
    }
    Ok(rest.to_ascii_lowercase())
}

/// Path plus query of a request for the provenance record: `cursor` values are elided (an opaque token that could in
/// principle embed anything), everything else is kept.
pub fn record_of(path: &str, query: &str) -> String {
    let parts: Vec<String> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|seg| match seg.split_once('=') {
            Some((n, _)) if n.eq_ignore_ascii_case("cursor") => format!("{n}=<elided>"),
            _ => seg.to_string(),
        })
        .collect();
    if parts.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{}", parts.join("&"))
    }
}

/// Check and clean a vendor-supplied `next_url` (see the module docs). `secret` is the configured key value, used only
/// to make sure it appears nowhere in the rebuilt URL.
pub fn sanitize_next_url(next: &str, authority: &str, secret: Option<&str>) -> Result<SanitizedUrl, String> {
    if next.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err("next_url contains whitespace or control characters".to_string());
    }
    let rest = next.strip_prefix("https://").ok_or_else(|| "next_url is not an https URL".to_string())?;
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (auth, tail) = rest.split_at(end);
    if !auth.eq_ignore_ascii_case(authority) {
        return Err("next_url points to a different host than the configured one".to_string());
    }
    let tail = tail.split('#').next().unwrap_or("");
    let (path, query) = match tail.split_once('?') {
        Some((p, q)) => (p, q),
        None => (tail, ""),
    };
    let path = if path.is_empty() { "/" } else { path };
    if !path.starts_with('/') {
        return Err("next_url has no valid path".to_string());
    }

    let mut kept: Vec<&str> = Vec::new();
    let mut scrubbed = false;
    for seg in query.split('&') {
        if seg.is_empty() {
            continue;
        }
        let name = seg.split('=').next().unwrap_or("");
        let carries_secret = secret.is_some_and(|k| !k.is_empty() && seg.contains(k));
        if is_key_param(name) || carries_secret {
            scrubbed = true;
            continue;
        }
        kept.push(seg);
    }
    let query = kept.join("&");
    let url = if query.is_empty() { format!("https://{authority}{path}") } else { format!("https://{authority}{path}?{query}") };
    if secret.is_some_and(|k| !k.is_empty() && url.contains(k)) {
        return Err("next_url would carry the API key in its path".to_string());
    }
    Ok(SanitizedUrl { url, record: record_of(path, &query), scrubbed })
}

#[cfg(test)]
mod tests {
    use super::*;

    const AUTH: &str = "api.massive.com";

    #[test]
    fn base_url_rules() {
        assert_eq!(base_authority("https://api.massive.com").unwrap(), "api.massive.com");
        assert_eq!(base_authority("https://API.massive.com/").unwrap(), "api.massive.com");
        assert_eq!(base_authority("https://localhost:8443").unwrap(), "localhost:8443");
        for bad in ["http://api.massive.com", "api.massive.com", "https://", "https://u:p@api.massive.com", "https://a.b/v2", "https://a.b?x=1", "https://a.b#f", "https://a b", ""] {
            assert!(base_authority(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn plain_next_url_passes_and_records_without_the_cursor() {
        let s = sanitize_next_url("https://api.massive.com/v2/aggs/ticker/SPY/range/1/day/2020-01-01/2020-12-31?cursor=YWJj&adjusted=true", AUTH, None).unwrap();
        assert_eq!(s.url, "https://api.massive.com/v2/aggs/ticker/SPY/range/1/day/2020-01-01/2020-12-31?cursor=YWJj&adjusted=true");
        assert_eq!(s.record, "/v2/aggs/ticker/SPY/range/1/day/2020-01-01/2020-12-31?cursor=<elided>&adjusted=true");
        assert!(!s.scrubbed);
    }

    #[test]
    fn api_key_parameters_are_removed_in_every_position_and_case() {
        for u in [
            "https://api.massive.com/p?apiKey=SECRETK&cursor=c1",
            "https://api.massive.com/p?cursor=c1&apiKey=SECRETK",
            "https://api.massive.com/p?cursor=c1&APIKEY=SECRETK&limit=5",
            "https://api.massive.com/p?api_key=SECRETK&cursor=c1",
            "https://api.massive.com/p?cursor=c1&api%4Bey=SECRETK",
            "https://api.massive.com/p?cursor=c1&other=SECRETK",
        ] {
            let s = sanitize_next_url(u, AUTH, Some("SECRETK")).unwrap();
            assert!(!s.url.contains("SECRETK") && !s.record.contains("SECRETK"), "{u} -> {}", s.url);
            assert!(s.url.contains("cursor=c1"), "{u} -> {}", s.url);
            assert!(s.scrubbed, "{u}");
        }
        let only = sanitize_next_url("https://api.massive.com/p?apiKey=X", AUTH, None).unwrap();
        assert_eq!(only.url, "https://api.massive.com/p");
    }

    #[test]
    fn foreign_hosts_schemes_and_tricks_are_refused() {
        for u in [
            "http://api.massive.com/p?cursor=1",
            "https://evil.example/p?cursor=1",
            "https://api.massive.com.evil.example/p",
            "https://api.massive.com@evil.example/p",
            "https://evil.example@api.massive.com/p",
            "https://user:pw@api.massive.com/p",
            "//api.massive.com/p",
            "/v2/relative?cursor=1",
            "https://api.massive.com:444/p",
            "https://api.massive.com/p q",
            "https://api.massive.com/p\r\nX: y",
            "",
        ] {
            assert!(sanitize_next_url(u, AUTH, Some("K")).is_err(), "{u}");
        }
    }

    #[test]
    fn fragment_is_dropped_and_bare_host_gets_a_root_path() {
        assert_eq!(sanitize_next_url("https://api.massive.com/p?cursor=1#frag", AUTH, None).unwrap().url, "https://api.massive.com/p?cursor=1");
        assert_eq!(sanitize_next_url("https://api.massive.com", AUTH, None).unwrap().url, "https://api.massive.com/");
    }

    #[test]
    fn key_in_the_path_is_refused() {
        assert!(sanitize_next_url("https://api.massive.com/v2/SECRETK/x", AUTH, Some("SECRETK")).is_err());
    }

    #[test]
    fn total_on_odd_input() {
        for u in ["https://", "https:///", "https://api.massive.com?", "https://api.massive.com?&&&", "https://api.massive.com/?=", "https://api.massive.com/\u{1F600}?a=\u{e9}", "https://\u{e9}"] {
            let _ = sanitize_next_url(u, AUTH, Some("k"));
        }
    }
}
