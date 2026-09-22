//! Data fingerprint for the run record.

use sha2::{Digest, Sha256};

use crate::series::Panel;

/// SHA-256 (lowercase hex, 64 chars) over a canonical serialization of the whole panel:
///
/// ```text
/// mendl-reference-rules-panel-v1\n
/// then, for each symbol in ascending byte order:
///   S <symbol-byte-length>:<symbol> <bar-count>\n
///   then per bar: <YYYY-MM-DD> <16 lowercase hex digits of the close's IEEE-754 bits>\n
/// ```
///
/// It depends only on symbols, dates and exact close values: not on insertion order, and any changed, added or
/// removed bar changes it. It is NOT compatible with the reference tool's 16-hex `data_fingerprint`.
pub fn data_fingerprint(panel: &Panel) -> String {
    let mut h = Sha256::new();
    h.update(b"mendl-reference-rules-panel-v1\n");
    for s in panel.iter() {
        h.update(format!("S {}:{} {}\n", s.symbol().len(), s.symbol(), s.len()).as_bytes());
        for (d, c) in s.dates().iter().zip(s.closes()) {
            h.update(format!("{} {:016x}\n", d.format("%Y-%m-%d"), c.to_bits()).as_bytes());
        }
    }
    hex::encode(h.finalize())
}
