//! Parsing and validating ONE page of a Massive daily-aggregates response. A total function of its input: any bytes
//! give `Ok` or `Err`, never a panic.
//!
//! The documented shape (`GET /v2/aggs/ticker/{t}/range/1/day/{from}/{to}`; see the recorded shapes in
//! MASSIVE_API_ENTITLEMENTS section 8):
//!
//! ```json
//! {"ticker":"SPY","queryCount":3,"resultsCount":3,"adjusted":true,"status":"OK","request_id":"...","count":3,
//!  "results":[{"v":..,"vw":..,"o":..,"c":<close>,"h":..,"l":..,"t":<ms since epoch>,"n":..}, ...],
//!  "next_url":"https://api.massive.com/...?cursor=..."}
//! ```
//! Checked here: JSON object; `status` is `OK` or `DELAYED` (the delayed stocks plan reports `DELAYED`); `ticker` is
//! exactly the requested vendor ticker; `adjusted` is `true` (a split-unadjusted series would break the rules);
//! `resultsCount`, when present, equals the number of results; every result has an integer `t` and a finite `c > 0`;
//! `next_url`, when present, is a non-empty string. Dates, ordering and completeness are checked by the caller.
//! An empty result set (`resultsCount: 0`, the `results` key absent or `[]`) is returned as zero bars.

use serde_json::Value;

use crate::secret::scrub;

/// One validated page.
#[derive(Debug, Clone, PartialEq)]
pub struct AggsPage {
    /// `(t in ms since the epoch, close)` per result, in the vendor's order.
    pub bars: Vec<(i64, f64)>,
    pub next_url: Option<String>,
    pub request_id: Option<String>,
}

/// Longest excerpt of vendor-supplied text that may enter an error message.
const EXCERPT: usize = 80;

/// Vendor text, made safe to embed in a message: control characters dropped, length bounded.
pub(crate) fn bounded(s: &str) -> String {
    s.chars().filter(|c| !c.is_control()).take(EXCERPT).collect()
}

/// `secret` (the configured key, when known) is removed from any vendor text echoed into an error BEFORE that text is
/// shortened, so a truncation can never leave half a key behind.
pub fn parse_aggs_page(body: &[u8], expected_ticker: &str, secret: Option<&str>) -> Result<AggsPage, String> {
    let quote = |s: &str| bounded(&scrub(s, secret));
    let v: Value = serde_json::from_slice(body).map_err(|_| "the body is not valid JSON".to_string())?;
    let obj = v.as_object().ok_or_else(|| "the body is not a JSON object".to_string())?;

    match obj.get("status").and_then(Value::as_str) {
        Some("OK") | Some("DELAYED") => {}
        Some(other) => return Err(format!("status is {:?}, expected OK or DELAYED", quote(other))),
        None => return Err("the `status` field is missing or not a string".to_string()),
    }
    match obj.get("ticker").and_then(Value::as_str) {
        Some(t) if t == expected_ticker => {}
        Some(other) => return Err(format!("ticker is {:?}, expected {expected_ticker:?}", quote(other))),
        None => return Err("the `ticker` field is missing or not a string".to_string()),
    }
    match obj.get("adjusted") {
        Some(Value::Bool(true)) => {}
        Some(_) => return Err("`adjusted` is not true: the series is not split-adjusted".to_string()),
        None => return Err("the `adjusted` field is missing".to_string()),
    }

    let results: &[Value] = match obj.get("results") {
        None | Some(Value::Null) => &[],
        Some(Value::Array(a)) => a.as_slice(),
        Some(_) => return Err("`results` is not an array".to_string()),
    };
    if let Some(rc) = obj.get("resultsCount") {
        match rc.as_u64() {
            Some(n) if n == results.len() as u64 => {}
            Some(n) => return Err(format!("resultsCount is {n} but {} result(s) are present", results.len())),
            None => return Err("`resultsCount` is not a non-negative integer".to_string()),
        }
    }

    let mut bars = Vec::with_capacity(results.len());
    for (i, r) in results.iter().enumerate() {
        let r = r.as_object().ok_or_else(|| format!("result {i} is not an object"))?;
        let t = r.get("t").and_then(Value::as_i64).ok_or_else(|| format!("result {i}: `t` is missing or not an integer"))?;
        let c = r.get("c").and_then(Value::as_f64).ok_or_else(|| format!("result {i}: `c` is missing or not a number"))?;
        if !(c.is_finite() && c > 0.0) {
            return Err(format!("result {i}: the close is not a finite positive number"));
        }
        bars.push((t, c));
    }

    let next_url = match obj.get("next_url") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(_) => return Err("`next_url` is not a non-empty string".to_string()),
    };
    let request_id = obj
        .get("request_id")
        .and_then(Value::as_str)
        .map(|s| s.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_').take(64).collect::<String>())
        .filter(|s| !s.is_empty());

    Ok(AggsPage { bars, next_url, request_id })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(extra: &str) -> String {
        format!(r#"{{"ticker":"SPY","queryCount":2,"resultsCount":2,"adjusted":true,"status":"OK","request_id":"abc-123","results":[{{"c":10.5,"t":1000}},{{"c":11,"t":2000}}]{extra}}}"#)
    }

    #[test]
    fn parses_the_documented_shape() {
        let p = parse_aggs_page(page("").as_bytes(), "SPY", None).unwrap();
        assert_eq!(p.bars, vec![(1000, 10.5), (2000, 11.0)]);
        assert_eq!(p.next_url, None);
        assert_eq!(p.request_id.as_deref(), Some("abc-123"));
    }

    #[test]
    fn delayed_status_is_accepted() {
        let b = page("").replace(r#""status":"OK""#, r#""status":"DELAYED""#);
        assert!(parse_aggs_page(b.as_bytes(), "SPY", None).is_ok());
    }

    #[test]
    fn refuses_what_it_should() {
        let good = page("");
        let bad: Vec<(String, &str)> = vec![
            (good.replace(r#""status":"OK""#, r#""status":"ERROR""#), "status"),
            (good.replace(r#""status":"OK","#, ""), "status missing"),
            (good.replace(r#""ticker":"SPY""#, r#""ticker":"QQQ""#), "ticker"),
            (good.replace(r#""adjusted":true"#, r#""adjusted":false"#), "adjusted false"),
            (good.replace(r#""adjusted":true,"#, ""), "adjusted missing"),
            (good.replace(r#""resultsCount":2"#, r#""resultsCount":3"#), "count mismatch"),
            (good.replace(r#""c":10.5"#, r#""c":0"#), "zero close"),
            (good.replace(r#""c":10.5"#, r#""c":-3"#), "negative close"),
            (good.replace(r#""c":10.5"#, r#""c":"10.5""#), "string close"),
            (good.replace(r#""c":10.5"#, r#""c":null"#), "null close"),
            (good.replace(r#""t":1000"#, r#""t":1000.5"#), "float t"),
            (good.replace(r#""t":1000"#, r#""t":"1000""#), "string t"),
            (page(r#","next_url":""#).to_string() + "\"", "unterminated"),
            (page(r#","next_url":5"#), "numeric next_url"),
            (good.replace(r#""results":["#, r#""results":{"a":["#).replace("}]}", "}]}}"), "results object"),
            ("[]".to_string(), "array body"),
            ("null".to_string(), "null body"),
            (String::new(), "empty body"),
            ("not json".to_string(), "garbage"),
        ];
        for (b, why) in bad {
            assert!(parse_aggs_page(b.as_bytes(), "SPY", None).is_err(), "should refuse: {why}");
        }
    }

    #[test]
    fn empty_results_are_zero_bars() {
        let e = r#"{"ticker":"SPY","queryCount":0,"resultsCount":0,"adjusted":true,"status":"OK","request_id":"r","count":0}"#;
        assert_eq!(parse_aggs_page(e.as_bytes(), "SPY", None).unwrap().bars, vec![]);
        let e2 = r#"{"ticker":"SPY","resultsCount":0,"adjusted":true,"status":"OK","results":[]}"#;
        assert!(parse_aggs_page(e2.as_bytes(), "SPY", None).unwrap().bars.is_empty());
        let lie = r#"{"ticker":"SPY","resultsCount":4,"adjusted":true,"status":"OK"}"#;
        assert!(parse_aggs_page(lie.as_bytes(), "SPY", None).is_err(), "resultsCount says 4 but nothing is there");
    }

    #[test]
    fn next_url_is_returned_verbatim_for_the_caller_to_sanitise() {
        let p = parse_aggs_page(page(r#","next_url":"https://api.massive.com/v2/x?cursor=abc&apiKey=SECRET""#).as_bytes(), "SPY", None).unwrap();
        assert_eq!(p.next_url.as_deref(), Some("https://api.massive.com/v2/x?cursor=abc&apiKey=SECRET"));
    }

    #[test]
    fn vendor_text_in_messages_is_bounded_and_clean() {
        let long = "x".repeat(500);
        let b = format!(r#"{{"status":"{long}\u0007"}}"#);
        let e = parse_aggs_page(b.as_bytes(), "SPY", None).unwrap_err();
        assert!(e.len() < 200, "{e}");
        assert!(!e.contains('\u{7}'));
    }

    #[test]
    fn total_on_arbitrary_bytes() {
        let mut x: u64 = 0x1234_5678_9abc_def1;
        for _ in 0..2000 {
            let n = (x % 64) as usize;
            let mut buf = Vec::new();
            for _ in 0..n {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                buf.push((x >> 24) as u8);
            }
            let _ = parse_aggs_page(&buf, "SPY", None);
        }
    }
}
