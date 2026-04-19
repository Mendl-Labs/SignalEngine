//! DeepBook Protocol Constants
//!
//! Pool addresses and configuration for DeepBook CLOB on SUI

/// DeepBook package addresses by network
pub mod packages {
    /// DeepBook V2 package on mainnet
    pub const MAINNET_V2: &str = "0xdee9";
    
    /// DeepBook V2 package on testnet
    pub const TESTNET_V2: &str = "0xdee9";
    
    /// DeepBook V2 package on devnet
    pub const DEVNET_V2: &str = "0xdee9";
}

/// Known pool IDs on mainnet
pub mod mainnet_pools {
    /// SUI-USDC pool
    pub const SUI_USDC: &str = "0x86699b7f1789b629481f0956aab0b956bc0c7d0c30b19f92a75cec29ffe0b4e1";
    
    /// SUI-USDT pool
    pub const SUI_USDT: &str = "0x";
    
    /// USDC-USDT pool (stable pair)
    pub const USDC_USDT: &str = "0x";
}

/// Known pool IDs on devnet
pub mod devnet_pools {
    /// SUI-USDC pool on devnet
    pub const SUI_USDC: &str = "0x";
}

/// Order side
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    /// Buy (bid)
    Bid,
    /// Sell (ask)
    Ask,
}

/// Order restriction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderRestriction {
    /// No restriction
    NoRestriction,
    /// Immediate or cancel
    ImmediateOrCancel,
    /// Fill or kill
    FillOrKill,
    /// Post only (must be maker)
    PostOnly,
}

/// Get package address for network
pub fn get_deepbook_package(is_mainnet: bool, is_devnet: bool) -> &'static str {
    if is_mainnet {
        packages::MAINNET_V2
    } else if is_devnet {
        packages::DEVNET_V2
    } else {
        packages::TESTNET_V2
    }
}

/// Get pool address for trading pair
pub fn get_pool_address(base: &str, quote: &str, is_devnet: bool) -> Option<&'static str> {
    if is_devnet {
        match (base, quote) {
            ("SUI", "USDC") => Some(devnet_pools::SUI_USDC),
            _ => None,
        }
    } else {
        // Mainnet pools
        match (base, quote) {
            ("SUI", "USDC") => Some(mainnet_pools::SUI_USDC),
            ("SUI", "USDT") => Some(mainnet_pools::SUI_USDT),
            ("USDC", "USDT") => Some(mainnet_pools::USDC_USDT),
            _ => None,
        }
    }
}

/// DeepBook function names
pub mod functions {
    /// Place limit order
    pub const PLACE_LIMIT_ORDER: &str = "place_limit_order";
    
    /// Place market order
    pub const PLACE_MARKET_ORDER: &str = "place_market_order";
    
    /// Cancel order
    pub const CANCEL_ORDER: &str = "cancel_order";
    
    /// Cancel all orders
    pub const CANCEL_ALL_ORDERS: &str = "cancel_all_orders";
}

/// Lot size and tick size for different pools
pub mod lot_sizes {
    /// SUI-USDC lot size (minimum order size)
    pub const SUI_USDC_LOT: u64 = 1_000_000; // 0.001 SUI
    
    /// SUI-USDC tick size (minimum price increment)
    pub const SUI_USDC_TICK: u64 = 1000; // 0.000001 USDC
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_pool_address_mainnet() {
        assert!(get_pool_address("SUI", "USDC", false).is_some());
        assert!(get_pool_address("SUI", "USDT", false).is_some());
        assert!(get_pool_address("USDC", "USDT", false).is_some());
        assert!(get_pool_address("UNKNOWN", "USDC", false).is_none());
    }

    #[test]
    fn test_get_pool_address_devnet() {
        assert!(get_pool_address("SUI", "USDC", true).is_some());
        assert!(get_pool_address("SUI", "USDT", true).is_none());
    }

    #[test]
    fn test_package_addresses_non_empty() {
        assert!(!packages::MAINNET_V2.is_empty());
        assert!(!packages::TESTNET_V2.is_empty());
        assert!(!packages::DEVNET_V2.is_empty());
    }

    #[test]
    fn test_get_deepbook_package() {
        assert_eq!(get_deepbook_package(true, false), packages::MAINNET_V2);
        assert_eq!(get_deepbook_package(false, true), packages::DEVNET_V2);
        assert_eq!(get_deepbook_package(false, false), packages::TESTNET_V2);
    }

    #[test]
    fn test_function_names_non_empty() {
        assert!(!functions::PLACE_LIMIT_ORDER.is_empty());
        assert!(!functions::PLACE_MARKET_ORDER.is_empty());
        assert!(!functions::CANCEL_ORDER.is_empty());
        assert!(!functions::CANCEL_ALL_ORDERS.is_empty());
    }

    #[test]
    fn test_lot_sizes_positive() {
        assert!(lot_sizes::SUI_USDC_LOT > 0);
        assert!(lot_sizes::SUI_USDC_TICK > 0);
    }
}
