//! Signing cross-checks.
//!
//! Three independent sources must agree byte for byte:
//! 1. this crate's signer,
//! 2. an independent Python implementation (tests/fixtures/gen_signing_vectors.py, output
//!    committed as signing_vectors.json),
//! 3. the SignalEngine connector's algorithm (copied verbatim below as an oracle).
//!
//! The first vector is Kraken's documented AddOrder example (remembered, not fetched); it is
//! checked too, and the Python side agrees with it.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use broker_adapters::kraken::auth::{
    build_private_request, build_public_request, encode_form, sign_with_secret, url_encode, KrakenCredentials,
};
use broker_adapters::transport::HttpMethod;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256, Sha512};

#[derive(Deserialize)]
struct Vector {
    name: String,
    secret: String,
    path: String,
    body: String,
    nonce: String,
    sig: String,
}

fn vectors() -> Vec<Vector> {
    serde_json::from_str(include_str!("fixtures/signing_vectors.json")).unwrap()
}

/// Verbatim logic of SignalEngine `generic/auth.rs::compute_kraken_signature`
/// (message = path bytes ++ SHA256(nonce ++ body); HMAC-SHA512 with the decoded secret; base64).
fn signalengine_oracle(secret_key: &[u8], path: &str, nonce: u64, body: &str) -> String {
    let nonce_body = format!("{}{}", nonce, body);
    let mut sha256 = Sha256::new();
    sha256.update(nonce_body.as_bytes());
    let sha256_result = sha256.finalize();

    let mut message = path.as_bytes().to_vec();
    message.extend_from_slice(&sha256_result);

    let mut mac = Hmac::<Sha512>::new_from_slice(secret_key).expect("HMAC can take key of any size");
    mac.update(&message);
    let result = mac.finalize();
    B64.encode(result.into_bytes())
}

#[test]
fn matches_independent_python_vectors_byte_for_byte() {
    let vs = vectors();
    assert!(vs.len() >= 6);
    for v in vs {
        let secret = B64.decode(&v.secret).unwrap();
        let nonce: u64 = v.nonce.parse().unwrap();
        assert_eq!(sign_with_secret(&secret, &v.path, nonce, &v.body), v.sig, "vector {}", v.name);
        let creds = KrakenCredentials::new("key", &v.secret).unwrap();
        assert_eq!(creds.sign(&v.path, nonce, &v.body), v.sig, "credentials.sign {}", v.name);
    }
}

#[test]
fn matches_signalengine_algorithm() {
    for v in vectors() {
        let secret = B64.decode(&v.secret).unwrap();
        let nonce: u64 = v.nonce.parse().unwrap();
        assert_eq!(
            sign_with_secret(&secret, &v.path, nonce, &v.body),
            signalengine_oracle(&secret, &v.path, nonce, &v.body),
            "vector {}",
            v.name
        );
    }
}

#[test]
fn kraken_documented_example_vector() {
    // Remembered from Kraken's docs; the independent Python agrees with it.
    let secret = "kQH5HW/8p1uGOVjbgWA7FunAmGO8lsSUXNsu3eow76sz84Q18fWxnyRzBHCd3pd5nE9qa99HAZtuZuj6F1huXg==";
    let body = "nonce=1616492376594&ordertype=limit&pair=XBTUSD&price=37500&type=buy&volume=1.25";
    let creds = KrakenCredentials::new("k", secret).unwrap();
    assert_eq!(
        creds.sign("/0/private/AddOrder", 1_616_492_376_594, body),
        "4/dpxb3iT4tp/ZCVEwSnEsLxx0bqyhLpdfOpc6fn7OR8+UClSV5n9E6aSS8MPtnRfp32bAb0nmbRn6H8ndwLUQ=="
    );
}

#[test]
fn signature_depends_on_path_nonce_and_body() {
    let secret = B64.encode(b"another-unit-test-secret");
    let creds = KrakenCredentials::new("k", &secret).unwrap();
    let base = creds.sign("/0/private/AddOrder", 100, "nonce=100&a=1");
    assert_ne!(base, creds.sign("/0/private/CancelOrder", 100, "nonce=100&a=1"), "path must matter");
    assert_ne!(base, creds.sign("/0/private/AddOrder", 101, "nonce=100&a=1"), "nonce must matter");
    assert_ne!(base, creds.sign("/0/private/AddOrder", 100, "nonce=100&a=2"), "body must matter");
    // path || sha256(...) is NOT sha256(path || ...): guard against reordering.
    let mut wrong = Sha256::new();
    wrong.update(b"/0/private/AddOrder");
    wrong.update(b"100nonce=100&a=1");
    let mut mac = Hmac::<Sha512>::new_from_slice(b"another-unit-test-secret").unwrap();
    mac.update(&wrong.finalize());
    assert_ne!(base, B64.encode(mac.finalize().into_bytes()));
}

#[test]
fn private_request_signs_the_exact_body_it_sends_with_nonce_first() {
    let secret = B64.encode(bytes(64));
    let creds = KrakenCredentials::new("APIKEY-123", &secret).unwrap();
    let params = vec![
        ("pair".to_string(), "XBTUSD".to_string()),
        ("volume".to_string(), "0.0025".to_string()),
        ("nonce".to_string(), "999".to_string()), // a caller-supplied nonce must never win
    ];
    let req = build_private_request(&creds, "https://api.kraken.com/", "/0/private/AddOrder", 1_758_463_200_123_456_789, &params);
    assert_eq!(req.method, HttpMethod::Post);
    assert_eq!(req.url, "https://api.kraken.com/0/private/AddOrder");
    let body = req.body.clone().unwrap();
    assert_eq!(body, "nonce=1758463200123456789&pair=XBTUSD&volume=0.0025");
    assert_eq!(req.header("API-Key"), Some("APIKEY-123"));
    let expected = sign_with_secret(&bytes(64), "/0/private/AddOrder", 1_758_463_200_123_456_789, &body);
    assert_eq!(req.header("api-sign"), Some(expected.as_str()));
    assert_eq!(req.header("Content-Type"), Some("application/x-www-form-urlencoded; charset=utf-8"));
}

fn bytes(n: u8) -> Vec<u8> {
    (0..n).collect()
}

#[test]
fn form_encoding() {
    assert_eq!(url_encode("a b+c/d,e~f-g_h.i"), "a%20b%2Bc%2Fd%2Ce~f-g_h.i");
    let p = vec![("txid".to_string(), "A,B".to_string()), ("k y".to_string(), "v".to_string())];
    assert_eq!(encode_form(&p), "txid=A%2CB&k%20y=v");
    let r = build_public_request("https://api.kraken.com", "/0/public/Ticker", &[("pair".into(), "XBTUSD".into())]);
    assert_eq!(r.url, "https://api.kraken.com/0/public/Ticker?pair=XBTUSD");
    assert!(r.headers.is_empty() && r.body.is_none());
}

#[test]
fn credentials_debug_never_contains_secret_or_key() {
    let secret_b64 = "kQH5HW/8p1uGOVjbgWA7FunAmGO8lsSUXNsu3eow76sz84Q18fWxnyRzBHCd3pd5nE9qa99HAZtuZuj6F1huXg==";
    let api_key = "SUPERSECRETAPIKEY-abcdef";
    let creds = KrakenCredentials::new(api_key, secret_b64).unwrap();
    for text in [format!("{creds:?}"), format!("{creds:#?}")] {
        assert!(!text.contains(secret_b64), "{text}");
        assert!(!text.contains(&secret_b64[..12]), "{text}");
        assert!(!text.contains(api_key), "{text}");
        assert!(!text.contains("SUPERSECRET"), "{text}");
        assert!(text.contains("<redacted>"));
    }
    // decoded secret bytes must not leak either (e.g. as a byte list)
    let decoded = B64.decode(secret_b64).unwrap();
    let as_list = format!("{:?}", decoded);
    assert!(!format!("{creds:?}").contains(&as_list[..20]));
}

#[test]
fn bad_secret_error_does_not_echo_input() {
    let bad = "this-is-not-base64-SECRETMATERIAL!!";
    let err = KrakenCredentials::new("k", bad).unwrap_err().to_string();
    assert!(!err.contains("SECRETMATERIAL"), "{err}");
    assert!(KrakenCredentials::new("", &B64.encode(b"x")).is_err());
    assert!(KrakenCredentials::new("k", "").is_err());
}

#[test]
fn key_id_is_stable_and_not_the_key() {
    let a = KrakenCredentials::new("key-one", &B64.encode(b"s")).unwrap();
    let b = KrakenCredentials::new("key-one", &B64.encode(b"different")).unwrap();
    let c = KrakenCredentials::new("key-two", &B64.encode(b"s")).unwrap();
    assert_eq!(a.key_id(), b.key_id());
    assert_ne!(a.key_id(), c.key_id());
    assert_eq!(a.key_id().len(), 16);
    assert!(!a.key_id().contains("key"));
}

#[test]
fn http_request_debug_redacts_auth_headers() {
    let creds = KrakenCredentials::new("APIKEY-XYZ", &B64.encode(b"s3cret")).unwrap();
    let req = build_private_request(&creds, "https://api.kraken.com", "/0/private/Balance", 5, &[]);
    let dbg = format!("{req:?}");
    assert!(!dbg.contains("APIKEY-XYZ"));
    assert!(!dbg.contains(req.header("API-Sign").unwrap()));
    assert!(dbg.contains("<redacted>"));
}
