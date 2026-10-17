//! Canonical JSON and SHA-256 for hashing a `LibraryEntry`'s binding fields (T4a; design section 2.3).
//!
//! Copied BYTE-FOR-BYTE from `feat/pf4a-classifier-translator`'s `program/src/portfolio_engine/canon.rs` (PF4 slice
//! A), per the plan's explicit instruction, so that PR and this one do not depend on each other: whichever merges
//! first, the other's canonical form is identical, not merely compatible.
//!
//! `serde_json::Value` may keep insertion order (the `preserve_order` feature is a workspace-wide switch that any
//! dependency can flip), so canonical form is produced here explicitly: keys sorted by byte order at every level, no
//! whitespace, numbers exactly as `serde_json` prints them (`ryu` shortest round-trip form, deterministic), `-0.0`
//! printed as `0.0`, strings escaped by a fixed rule set. Non-finite floats cannot occur in a `Value` (they become
//! `null` at conversion); nothing in `binding.rs` feeds a non-finite number into a `Value` in the first place.

use serde_json::Value;
use sha2::{Digest, Sha256};

pub fn canonical_json(v: &Value) -> String {
    let mut out = String::new();
    write_value(v, &mut out);
    out
}

fn write_value(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f == 0.0 && n.is_f64() && f.is_sign_negative() {
                    out.push_str("0.0");
                    return;
                }
            }
            out.push_str(&n.to_string());
        }
        Value::String(s) => write_string(s, out),
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(x, out);
            }
            out.push(']');
        }
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(k, out);
                out.push(':');
                if let Some(x) = m.get(k.as_str()) {
                    write_value(x, out);
                }
            }
            out.push('}');
        }
    }
}

fn write_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Lowercase hex SHA-256.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let d = h.finalize();
    let mut s = String::with_capacity(64);
    for b in d.iter() {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// SHA-256 of the canonical JSON of a value.
pub fn canonical_hash(v: &Value) -> String {
    sha256_hex(canonical_json(v).as_bytes())
}
