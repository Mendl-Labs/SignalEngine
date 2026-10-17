//! NOT a copy of the Engine's `robust_premia.rs` (the citation table is Engine-only and stays there). Only the
//! `AssetClass` enum is reproduced, byte-compatible in its serialised form (`"us_etf"`, `"crypto_spot"`, `"fx"`),
//! so a universe file parses and hashes identically here and in the Engine.

use serde::{Deserialize, Serialize};

/// Serialises to the SAME strings as the Engine's `AssetClass` (`"us_etf"`, `"crypto_spot"`, `"fx"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetClass {
    UsEtf,
    CryptoSpot,
    Fx,
}

impl AssetClass {
    pub fn as_str(self) -> &'static str {
        match self {
            AssetClass::UsEtf => "us_etf",
            AssetClass::CryptoSpot => "crypto_spot",
            AssetClass::Fx => "fx",
        }
    }
}
