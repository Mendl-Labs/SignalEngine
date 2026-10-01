//! Deterministic client-order-id scheme for Kraken.
//!
//! Kraken's client reference on `AddOrder` is `userref`, a signed 32-bit integer (FROM-MEMORY OF
//! DOCS). We encode our string order tag into it as follows:
//!
//! 1. `candidate(tag)` = first 4 bytes (big endian) of `SHA-256("mendl-userref-v1\0" || tag)`,
//!    masked to 31 bits, with 0 remapped to 1. So the value is in `1..=i32::MAX`.
//!    (0 is avoided because Kraken uses null/0 for "no userref"; negatives are avoided so the
//!    value is never confused with a sign-extended parse.)
//! 2. [`UserrefMap::assign`] keeps a persistent two-way table `tag <-> userref`. If the candidate
//!    is already owned by a DIFFERENT tag (a collision), it linearly probes `candidate+1`,
//!    wrapping from `i32::MAX` to 1, until it finds a free value.
//!
//! Collision handling and its limits:
//! * Birthday bound: with n tags the collision chance is about n^2 / 2^32 (n = 10,000 gives
//!   about 2 percent), so collisions WILL eventually happen and are handled, not ignored.
//! * The table is the source of truth, not the hash. Once a collision has been probed, the tag's
//!   userref can only be recovered from the table. The rebalancer must therefore persist the
//!   (tag, userref) pair BEFORE sending the order (use `KrakenAdapter::reserve_userref`) and
//!   rebuild the map from its orders table at start-up (`UserrefMap::from_entries`).
//! * `assign` is idempotent: the same tag always returns the same userref.
//! * Tags are not recoverable from a userref without the table. An order whose userref is not
//!   in the table is FOREIGN (placed by someone else on this key) and must be treated as such.
//! * Kraken does not (to our knowledge) reject duplicate userrefs, so the userref is a
//!   correlation id, not a dedupe key. Retrying an order whose first attempt may have reached
//!   the exchange requires a lookup by userref first.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UserrefError {
    #[error("order tag must not be empty")]
    EmptyTag,
    #[error("userref space exhausted")]
    Exhausted,
    #[error("userref {0} is outside 1..=2147483647")]
    OutOfRange(i64),
    #[error("conflicting entries for {0}")]
    Conflict(String),
    #[error("invalid userref table json: {0}")]
    Json(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserrefEntry {
    pub tag: String,
    pub userref: i32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserrefMap {
    by_tag: BTreeMap<String, i32>,
    by_ref: BTreeMap<i32, String>,
}

/// The hash-derived first choice for a tag, before collision probing.
pub fn candidate(tag: &str) -> i32 {
    let mut h = Sha256::new();
    h.update(b"mendl-userref-v1\0");
    h.update(tag.as_bytes());
    let d = h.finalize();
    let v = u32::from_be_bytes([d[0], d[1], d[2], d[3]]) & 0x7FFF_FFFF;
    if v == 0 {
        1
    } else {
        v as i32
    }
}

fn next_probe(r: i32) -> i32 {
    if r == i32::MAX {
        1
    } else {
        r + 1
    }
}

impl UserrefMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild from persisted entries. Rejects out-of-range values and any tag or userref that
    /// appears with two different partners.
    pub fn from_entries(entries: impl IntoIterator<Item = UserrefEntry>) -> Result<Self, UserrefError> {
        let mut m = Self::new();
        for e in entries {
            if e.tag.is_empty() {
                return Err(UserrefError::EmptyTag);
            }
            if e.userref < 1 {
                return Err(UserrefError::OutOfRange(i64::from(e.userref)));
            }
            match (m.by_tag.get(&e.tag), m.by_ref.get(&e.userref)) {
                (None, None) => {
                    m.by_tag.insert(e.tag.clone(), e.userref);
                    m.by_ref.insert(e.userref, e.tag);
                }
                (Some(r), Some(t)) if *r == e.userref && *t == e.tag => {}
                _ => return Err(UserrefError::Conflict(format!("{} <-> {}", e.tag, e.userref))),
            }
        }
        Ok(m)
    }

    pub fn from_json(json: &str) -> Result<Self, UserrefError> {
        let entries: Vec<UserrefEntry> = serde_json::from_str(json).map_err(|e| UserrefError::Json(e.to_string()))?;
        Self::from_entries(entries)
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.entries()).unwrap_or_else(|_| "[]".to_string())
    }

    pub fn entries(&self) -> Vec<UserrefEntry> {
        self.by_tag.iter().map(|(t, r)| UserrefEntry { tag: t.clone(), userref: *r }).collect()
    }

    pub fn len(&self) -> usize {
        self.by_tag.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_tag.is_empty()
    }

    pub fn get_userref(&self, tag: &str) -> Option<i32> {
        self.by_tag.get(tag).copied()
    }

    pub fn tag_for(&self, userref: i32) -> Option<&str> {
        self.by_ref.get(&userref).map(String::as_str)
    }

    /// Return the tag's userref, assigning (and remembering) one if it has none yet.
    pub fn assign(&mut self, tag: &str) -> Result<i32, UserrefError> {
        if tag.is_empty() {
            return Err(UserrefError::EmptyTag);
        }
        if let Some(r) = self.by_tag.get(tag) {
            return Ok(*r);
        }
        let start = candidate(tag);
        let mut r = start;
        loop {
            if !self.by_ref.contains_key(&r) {
                self.by_tag.insert(tag.to_string(), r);
                self.by_ref.insert(r, tag.to_string());
                return Ok(r);
            }
            r = next_probe(r);
            if r == start {
                return Err(UserrefError::Exhausted);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_wraps_from_i32_max_to_one() {
        assert_eq!(next_probe(i32::MAX), 1);
        assert_eq!(next_probe(1), 2);
    }
}
