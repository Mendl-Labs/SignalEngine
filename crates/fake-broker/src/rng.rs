//! A tiny seeded generator (SplitMix64). The exchange uses it to mint order ids; tests use it for
//! randomised-but-reproducible sequences. Not cryptographic and not meant to be.

#[derive(Debug, Clone)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.0)
    }

    /// Uniform-ish value in `0..n` (`n > 0`). Modulo bias is irrelevant here.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0);
        self.next_u64() % n
    }

    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        self.below(denominator) < numerator
    }
}

/// The SplitMix64 finalizer: a bijection on `u64`.
pub fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

fn encode(mut v: u64, len: usize) -> String {
    let mut out = vec![b'A'; len];
    for slot in out.iter_mut().rev() {
        *slot = ALPHABET[(v & 31) as usize];
        v >>= 5;
    }
    String::from_utf8(out).expect("alphabet is ascii")
}

/// Deterministic Kraken-looking identifier `<prefix>XXXXX-XXXXX-XXXXXX`. Unique per
/// `(seed, counter)` because the last group encodes the counter itself.
pub fn kraken_style_id(prefix: char, seed: u64, counter: u64) -> String {
    let m = mix(seed ^ mix(counter));
    format!("{prefix}{}-{}-{}", encode(m, 5), encode(m >> 25, 5), encode(counter, 6))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn same_seed_same_sequence() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        assert_eq!((0..10).map(|_| a.next_u64()).collect::<Vec<_>>(), (0..10).map(|_| b.next_u64()).collect::<Vec<_>>());
        assert_ne!(SplitMix64::new(1).next_u64(), SplitMix64::new(2).next_u64());
    }

    #[test]
    fn ids_look_like_kraken_and_never_repeat() {
        let mut seen = BTreeSet::new();
        for i in 0..5000 {
            let id = kraken_style_id('O', 0xF4CE, i);
            assert_eq!(id.len(), 1 + 5 + 1 + 5 + 1 + 6);
            assert!(id.starts_with('O'));
            assert!(seen.insert(id));
        }
        assert_eq!(kraken_style_id('O', 7, 3), kraken_style_id('O', 7, 3));
        assert_ne!(kraken_style_id('O', 7, 3), kraken_style_id('O', 8, 3));
    }
}
