//! Kraken wire format helpers: URL and form handling, request signing (an independent
//! implementation of the published algorithm, deliberately NOT the adapter's own function),
//! asset codes, and the mapping from core rejections to Kraken error strings.

use crate::exchange::Reject;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::{Digest, Sha256, Sha512};

pub mod paths {
    pub const ADD_ORDER: &str = "/0/private/AddOrder";
    pub const CANCEL_ORDER: &str = "/0/private/CancelOrder";
    pub const QUERY_ORDERS: &str = "/0/private/QueryOrders";
    pub const OPEN_ORDERS: &str = "/0/private/OpenOrders";
    pub const CLOSED_ORDERS: &str = "/0/private/ClosedOrders";
    pub const BALANCE: &str = "/0/private/Balance";
    pub const TRADE_BALANCE: &str = "/0/private/TradeBalance";
    pub const TICKER: &str = "/0/public/Ticker";
    pub const ASSET_PAIRS: &str = "/0/public/AssetPairs";
}

/// `(host, path, query)` of an absolute URL. `None` if there is no scheme or host.
pub(crate) fn split_url(url: &str) -> Option<(&str, &str, &str)> {
    let rest = url.split_once("://")?.1;
    let (host, path_and_query) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    if host.is_empty() {
        return None;
    }
    let (path, query) = path_and_query.split_once('?').unwrap_or((path_and_query, ""));
    Some((host, path, query))
}

/// Percent-decode (`+` is a space, as in form encoding). `None` on a malformed escape.
pub(crate) fn url_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let hex = s.get(i + 1..i + 3)?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

/// RFC 3986 percent-encoding (unreserved kept). Used when the fake plays another process.
pub(crate) fn url_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Parse `k=v&k=v` into decoded pairs, in order. `None` on a malformed escape.
pub(crate) fn parse_form(s: &str) -> Option<Vec<(String, String)>> {
    if s.is_empty() {
        return Some(Vec::new());
    }
    s.split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            Some((url_decode(k)?, url_decode(v)?))
        })
        .collect()
}

pub(crate) fn param<'a>(params: &'a [(String, String)], key: &str) -> Option<&'a str> {
    params.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

/// The raw text of the `nonce` field exactly as sent (the signature covers the text, not the
/// parsed integer).
pub(crate) fn raw_nonce(body: &str) -> Option<&str> {
    body.split('&').find_map(|kv| kv.strip_prefix("nonce="))
}

fn message_digest(path: &str, nonce_text: &str, body: &str) -> Vec<u8> {
    // message = path || SHA256(nonce || body)
    let mut sha = Sha256::new();
    sha.update(nonce_text.as_bytes());
    sha.update(body.as_bytes());
    let mut msg = path.as_bytes().to_vec();
    msg.extend_from_slice(&sha.finalize());
    msg
}

/// `API-Sign` = base64(HMAC-SHA512(base64_decode(secret), path || SHA256(nonce || body))).
pub(crate) fn sign(secret: &[u8], path: &str, nonce_text: &str, body: &str) -> String {
    let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(&message_digest(path, nonce_text, body));
    B64.encode(mac.finalize().into_bytes())
}

/// Constant-time signature check.
pub(crate) fn verify(secret: &[u8], path: &str, nonce_text: &str, body: &str, signature_b64: &str) -> bool {
    let Ok(sig) = B64.decode(signature_b64.trim()) else { return false };
    let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(&message_digest(path, nonce_text, body));
    mac.verify_slice(&sig).is_ok()
}

/// Canonical ticker to Kraken's asset code. Legacy assets carry an `X`/`Z` prefix; anything else
/// (and anything already dotted, like `ETH2.S`) passes through unchanged.
pub(crate) fn kraken_asset_code(canonical: &str) -> String {
    match canonical {
        "BTC" => "XXBT",
        "ETH" => "XETH",
        "LTC" => "XLTC",
        "XRP" => "XXRP",
        "DOGE" => "XXDG",
        "USD" => "ZUSD",
        "EUR" => "ZEUR",
        "GBP" => "ZGBP",
        other => other,
    }
    .to_string()
}

/// Kraken's asset code (or a plain ticker) to the canonical ticker used by the core.
pub(crate) fn canonical_asset(raw: &str) -> String {
    let up = raw.trim().to_ascii_uppercase();
    match up.as_str() {
        "XXBT" | "XBT" => "BTC",
        "XETH" => "ETH",
        "XLTC" => "LTC",
        "XXRP" => "XRP",
        "XXDG" | "XDG" => "DOGE",
        "ZUSD" => "USD",
        "ZEUR" => "EUR",
        "ZGBP" => "GBP",
        other => other,
    }
    .to_string()
}

/// Kraken error strings for a core rejection. Strings the adapter's classifier knows are used
/// verbatim; the rest follow Kraken's documented `E<Category>:<Message>` shape.
pub(crate) fn reject_codes(r: &Reject) -> Vec<String> {
    match r {
        Reject::UnknownPair(_) => vec!["EQuery:Unknown asset pair".into()],
        Reject::InvalidArguments(field) if field.is_empty() => vec!["EGeneral:Invalid arguments".into()],
        Reject::InvalidArguments(field) => vec![format!("EGeneral:Invalid arguments:{field}")],
        Reject::OrderMinimumNotMet => vec!["EOrder:Order minimum not met".into()],
        Reject::CostMinimumNotMet => vec!["EOrder:Cost minimum not met".into()],
        Reject::InsufficientFunds => vec!["EOrder:Insufficient funds".into()],
        Reject::PostOnlyWouldCross => vec!["EOrder:Post only order".into()],
        Reject::MarketMode(status) => vec![format!("EService:Market in {status} mode")],
        Reject::UnknownOrder => vec!["EOrder:Unknown order".into()],
        Reject::Scripted(codes) => codes.clone(),
    }
}

pub(crate) fn ok_body(result: Value) -> String {
    json!({ "error": [], "result": result }).to_string()
}

pub(crate) fn error_body(codes: &[String]) -> String {
    json!({ "error": codes }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documented_kraken_signature_example_verifies() {
        // Kraken's published AddOrder example (also used by the adapter's tests, cross-checked
        // there against an independent Python implementation).
        let secret = B64
            .decode("kQH5HW/8p1uGOVjbgWA7FunAmGO8lsSUXNsu3eow76sz84Q18fWxnyRzBHCd3pd5nE9qa99HAZtuZuj6F1huXg==")
            .unwrap();
        let body = "nonce=1616492376594&ordertype=limit&pair=XBTUSD&price=37500&type=buy&volume=1.25";
        let sig = "4/dpxb3iT4tp/ZCVEwSnEsLxx0bqyhLpdfOpc6fn7OR8+UClSV5n9E6aSS8MPtnRfp32bAb0nmbRn6H8ndwLUQ==";
        assert!(verify(&secret, "/0/private/AddOrder", "1616492376594", body, sig));
        assert_eq!(sign(&secret, "/0/private/AddOrder", "1616492376594", body), sig);
        assert!(!verify(&secret, "/0/private/CancelOrder", "1616492376594", body, sig), "path is signed");
        assert!(!verify(&secret, "/0/private/AddOrder", "1616492376595", body, sig), "nonce is signed");
        assert!(!verify(&secret, "/0/private/AddOrder", "1616492376594", &body.replace("1.25", "1.26"), sig), "body is signed");
        assert!(!verify(&secret, "/0/private/AddOrder", "1616492376594", body, "not base64!"));
    }

    #[test]
    fn url_split_form_and_decode() {
        assert_eq!(split_url("https://api.kraken.com/0/public/Ticker?pair=XBTUSD"), Some(("api.kraken.com", "/0/public/Ticker", "pair=XBTUSD")));
        assert_eq!(split_url("https://h"), Some(("h", "/", "")));
        assert_eq!(split_url("nonsense"), None);
        assert_eq!(parse_form("a=1&b=x%20y&c=p+q&d="), Some(vec![
            ("a".into(), "1".into()), ("b".into(), "x y".into()), ("c".into(), "p q".into()), ("d".into(), "".into())]));
        assert_eq!(parse_form("a=%zz"), None);
        assert_eq!(url_decode(&url_encode("a b&c=d/é")).as_deref(), Some("a b&c=d/é"));
        assert_eq!(raw_nonce("nonce=17&pair=x"), Some("17"));
        assert_eq!(raw_nonce("pair=x"), None);
    }

    #[test]
    fn asset_code_round_trip() {
        for a in ["BTC", "ETH", "USD", "EUR", "DOGE", "USDT", "SOL"] {
            assert_eq!(canonical_asset(&kraken_asset_code(a)), a);
        }
        assert_eq!(kraken_asset_code("ETH2.S"), "ETH2.S");
        assert_eq!(canonical_asset("XBT"), "BTC");
    }
}
