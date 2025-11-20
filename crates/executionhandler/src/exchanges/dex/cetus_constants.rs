//! Cetus Protocol Constants
//!
//! Real package IDs, pool addresses, and type tags for Cetus on SUI

/// Cetus package addresses by network
pub mod packages {
    /// Cetus CLMM (Concentrated Liquidity Market Maker) package on mainnet
    pub const MAINNET_CLMM: &str = "0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb";
    
    /// Cetus CLMM package on testnet
    pub const TESTNET_CLMM: &str = "0x0868b71c0cba55bf0faf6c40df8c179c67a4d0ba0e79965b68b3d72d7dfbf666";
    
    /// Cetus CLMM package on devnet
    pub const DEVNET_CLMM: &str = "0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb";
    
    /// Global configuration object
    pub const GLOBAL_CONFIG: &str = "0xdaa46292632c3c4d8f31f23ea0f9b36a28ff3677e9684980e4438403a67a3d8f";
}

/// Common coin types on SUI
pub mod coin_types {
    /// SUI native token
    pub const SUI: &str = "0x2::sui::SUI";
    
    /// USDC on SUI (Wormhole bridged)
    pub const USDC: &str = "0x5d4b302506645c37ff133b98c4b50a5ae14841659738d6d733d59d0d217a93bf::coin::COIN";
    
    /// USDT on SUI (Wormhole bridged)
    pub const USDT: &str = "0xc060006111016b8a020ad5b33834984a437aaa7d3c74c18e09a95d48aceab08c::coin::COIN";
    
    /// WETH on SUI (Wormhole bridged)
    pub const WETH: &str = "0xaf8cd5edc19c4512f4259f0bee101a40d41ebed738ade5874359610ef8eeced5::coin::COIN";
    
    /// CETUS token
    pub const CETUS: &str = "0x06864a6f921804860930db6ddbe2e16acdf8504495ea7481637a1c8b9a8fe54b::cetus::CETUS";
}

/// Known pool addresses on mainnet
pub mod mainnet_pools {
    /// SUI-USDC pool (0.3% fee tier)
    pub const SUI_USDC: &str = "0xcf994611fd4c48e277ce3ffd4d4364c914af2c3cbb05f7bf6facd371de688630";
    
    /// SUI-USDT pool (0.3% fee tier)
    pub const SUI_USDT: &str = "0x06d8af9e6afd27262db436f0d37b304a041f710c3ea1fa4c3a9bab36b3569ad3";
    
    /// USDC-USDT pool (0.01% fee tier - stablecoin pair)
    pub const USDC_USDT: &str = "0xb8d7d9e66a60c239e7a60110efcf8de6c705580ed924d0dde141f4a0e2c90105";
    
    /// SUI-CETUS pool (0.3% fee tier)
    pub const SUI_CETUS: &str = "0x2e041f3fd93646dcc877f783c1f2b7fa62d30271bdef1f21ef002cebf857bded";
}

/// Known pool addresses on devnet
pub mod devnet_pools {
    /// SUI-USDC pool on devnet (example)
    pub const SUI_USDC: &str = "0x0254747f5ca059a1972cd7f6016485d51392a3fde608107b93bbaebea550f703";
}

/// Fee tier configurations
pub mod fee_tiers {
    /// 0.01% fee tier (1 bps) - typically for stablecoins
    pub const TIER_1_BPS: u32 = 1;
    
    /// 0.05% fee tier (5 bps)
    pub const TIER_5_BPS: u32 = 5;
    
    /// 0.3% fee tier (30 bps) - most common
    pub const TIER_30_BPS: u32 = 30;
    
    /// 1% fee tier (100 bps) - exotic pairs
    pub const TIER_100_BPS: u32 = 100;
}

/// Tick spacings corresponding to fee tiers
pub mod tick_spacings {
    /// Tick spacing for 0.01% fee tier
    pub const SPACING_1: u32 = 1;
    
    /// Tick spacing for 0.05% fee tier
    pub const SPACING_10: u32 = 10;
    
    /// Tick spacing for 0.3% fee tier
    pub const SPACING_60: u32 = 60;
    
    /// Tick spacing for 1% fee tier
    pub const SPACING_200: u32 = 200;
}

/// Get package address for network
pub fn get_clmm_package(is_mainnet: bool, is_devnet: bool) -> &'static str {
    if is_mainnet {
        packages::MAINNET_CLMM
    } else if is_devnet {
        packages::DEVNET_CLMM
    } else {
        packages::TESTNET_CLMM
    }
}

/// Get pool address for trading pair
pub fn get_pool_address(token_a: &str, token_b: &str, is_devnet: bool) -> Option<&'static str> {
    if is_devnet {
        match (token_a, token_b) {
            ("SUI", "USDC") | ("USDC", "SUI") => Some(devnet_pools::SUI_USDC),
            _ => None,
        }
    } else {
        // Mainnet pools
        match (token_a, token_b) {
            ("SUI", "USDC") | ("USDC", "SUI") => Some(mainnet_pools::SUI_USDC),
            ("SUI", "USDT") | ("USDT", "SUI") => Some(mainnet_pools::SUI_USDT),
            ("USDC", "USDT") | ("USDT", "USDC") => Some(mainnet_pools::USDC_USDT),
            ("SUI", "CETUS") | ("CETUS", "SUI") => Some(mainnet_pools::SUI_CETUS),
            _ => None,
        }
    }
}

/// Get full coin type from symbol
pub fn get_coin_type(symbol: &str) -> Option<&'static str> {
    match symbol.to_uppercase().as_str() {
        "SUI" => Some(coin_types::SUI),
        "USDC" => Some(coin_types::USDC),
        "USDT" => Some(coin_types::USDT),
        "WETH" => Some(coin_types::WETH),
        "CETUS" => Some(coin_types::CETUS),
        _ => None,
    }
}

/// Cetus swap function names
pub mod swap_functions {
    /// Swap from token A to token B
    pub const SWAP_A2B: &str = "swap_a2b";
    
    /// Swap from token B to token A
    pub const SWAP_B2A: &str = "swap_b2a";
    
    /// Flash swap (advanced)
    pub const FLASH_SWAP: &str = "flash_swap";
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_get_pool_address() {
        // Mainnet
        assert!(get_pool_address("SUI", "USDC", false).is_some());
        assert!(get_pool_address("USDC", "SUI", false).is_some());
        assert_eq!(
            get_pool_address("SUI", "USDC", false),
            get_pool_address("USDC", "SUI", false)
        );
        
        // Unknown pair
        assert!(get_pool_address("FOO", "BAR", false).is_none());
    }
    
    #[test]
    fn test_get_coin_type() {
        assert_eq!(get_coin_type("SUI"), Some(coin_types::SUI));
        assert_eq!(get_coin_type("sui"), Some(coin_types::SUI));
        assert_eq!(get_coin_type("USDC"), Some(coin_types::USDC));
        assert!(get_coin_type("UNKNOWN").is_none());
    }
}
