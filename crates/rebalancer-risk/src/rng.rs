//! SplitMix64: a tiny deterministic PRNG so property tests and drills need no `rand` dependency and are exactly
//! reproducible from a seed. Not cryptographic and not meant to be.

#[derive(Debug, Clone)]
pub struct SplitMix64(pub u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform-ish integer in `lo..=hi` (modulo bias is irrelevant here).
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi);
        lo + self.next_u64() % (hi - lo + 1)
    }

    /// True with probability `percent`/100.
    pub fn chance(&mut self, percent: u64) -> bool {
        self.range(0, 99) < percent
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.range(0, items.len() as u64 - 1) as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence_and_known_first_values() {
        let mut a = SplitMix64::new(0);
        let mut b = SplitMix64::new(0);
        let av: Vec<u64> = (0..5).map(|_| a.next_u64()).collect();
        let bv: Vec<u64> = (0..5).map(|_| b.next_u64()).collect();
        assert_eq!(av, bv);
        // Published SplitMix64 reference outputs for seed 0.
        assert_eq!(av[0], 0xE220_A839_7B1D_CDAF);
        assert_eq!(av[1], 0x6E78_9E6A_A1B9_65F4);
        assert_ne!(SplitMix64::new(1).next_u64(), SplitMix64::new(2).next_u64());
    }

    #[test]
    fn range_is_inclusive_and_bounded() {
        let mut r = SplitMix64::new(9);
        let mut seen = [false; 4];
        for _ in 0..200 {
            let v = r.range(3, 6);
            assert!((3..=6).contains(&v));
            seen[(v - 3) as usize] = true;
        }
        assert!(seen.iter().all(|s| *s));
    }
}
