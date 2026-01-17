/// Input validation utilities for trading system safety
/// 
/// This module provides centralized validation functions to prevent:
/// - Market manipulation through invalid parameters
/// - System crashes from malformed data
/// - Financial losses from extreme values
use std::collections::HashSet;

pub struct TradingValidator {
    pub allowed_symbols: HashSet<String>,
    pub max_quantity: f64,
    pub min_quantity: f64,
    pub max_price: f64,
    pub min_price: f64,
}

impl Default for TradingValidator {
    fn default() -> Self {
        let mut allowed_symbols = HashSet::new();
        allowed_symbols.insert("BTC/USD".to_string());
        allowed_symbols.insert("ETH/USD".to_string());
        allowed_symbols.insert("BTC/USDT".to_string());
        allowed_symbols.insert("ETH/USDT".to_string());
        
        Self {
            allowed_symbols,
            max_quantity: 1_000_000.0,
            min_quantity: 1e-8,
            max_price: 10_000_000.0,
            min_price: 1e-6,
        }
    }
}

impl TradingValidator {
    /// Validate trading symbol format and whitelist
    pub fn validate_symbol(&self, symbol: &str) -> Result<(), String> {
        if symbol.is_empty() || symbol.len() > 20 {
            return Err("Symbol must be 1-20 characters long".to_string());
        }
        
        if !symbol.chars().all(|c| c.is_ascii_alphanumeric() || c == '/' || c == '-' || c == '_') {
            return Err("Symbol contains invalid characters".to_string());
        }
        
        // Optional whitelist check (can be disabled for flexibility)
        if !self.allowed_symbols.is_empty() && !self.allowed_symbols.contains(symbol) {
            return Err(format!("Symbol '{}' is not in allowed symbols list", symbol));
        }
        
        Ok(())
    }
    
    /// Validate trading quantity
    pub fn validate_quantity(&self, quantity: f64) -> Result<(), String> {
        if !quantity.is_finite() {
            return Err("Quantity must be finite (not NaN or infinite)".to_string());
        }
        
        if quantity <= 0.0 {
            return Err("Quantity must be positive".to_string());
        }
        
        if quantity > self.max_quantity {
            return Err(format!("Quantity {} exceeds maximum allowed {}", quantity, self.max_quantity));
        }
        
        if quantity < self.min_quantity {
            return Err(format!("Quantity {} below minimum precision {}", quantity, self.min_quantity));
        }
        
        Ok(())
    }
    
    /// Validate trading price
    pub fn validate_price(&self, price: f64) -> Result<(), String> {
        if !price.is_finite() {
            return Err("Price must be finite (not NaN or infinite)".to_string());
        }
        
        if price <= 0.0 {
            return Err("Price must be positive".to_string());
        }
        
        if price > self.max_price {
            return Err(format!("Price {} exceeds maximum allowed {}", price, self.max_price));
        }
        
        if price < self.min_price {
            return Err(format!("Price {} below minimum precision {}", price, self.min_price));
        }
        
        Ok(())
    }
    
    /// Validate order ID
    pub fn validate_order_id(&self, order_id: u64) -> Result<(), String> {
        if order_id == 0 {
            return Err("Order ID cannot be zero".to_string());
        }
        
        if order_id > u64::MAX - 1000 {
            return Err("Order ID too large (reserved range)".to_string());
        }
        
        Ok(())
    }
    
    /// Validate exchange name
    pub fn validate_exchange(&self, exchange: &str) -> Result<(), String> {
        if exchange.is_empty() || exchange.len() > 50 {
            return Err("Exchange name must be 1-50 characters long".to_string());
        }
        
        if !exchange.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
            return Err("Exchange name contains invalid characters".to_string());
        }
        
        Ok(())
    }
    
    /// Validate fees
    pub fn validate_fees(&self, fees: f64) -> Result<(), String> {
        if !fees.is_finite() {
            return Err("Fees must be finite (not NaN or infinite)".to_string());
        }
        
        if fees < 0.0 {
            return Err("Fees cannot be negative".to_string());
        }
        
        if fees > 1000.0 {
            return Err("Fees exceed maximum reasonable amount (1000)".to_string());
        }
        
        Ok(())
    }
    
    /// Comprehensive validation for a trading operation
    pub fn validate_trade(&self, symbol: &str, quantity: f64, price: f64, fees: f64) -> Result<(), String> {
        self.validate_symbol(symbol)?;
        self.validate_quantity(quantity)?;
        self.validate_price(price)?;
        self.validate_fees(fees)?;
        Ok(())
    }
}

/// Global validator instance
pub static VALIDATOR: std::sync::LazyLock<TradingValidator> = std::sync::LazyLock::new(|| {
    TradingValidator::default()
});

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_symbol_validation() {
        let validator = TradingValidator::default();
        
        // Valid symbols (in whitelist)
        assert!(validator.validate_symbol("BTC/USD").is_ok());
        assert!(validator.validate_symbol("ETH/USDT").is_ok());
        
        // Invalid symbols
        assert!(validator.validate_symbol("").is_err());
        assert!(validator.validate_symbol("BTC USD").is_err()); // Space not allowed
        assert!(validator.validate_symbol("BTC@USD").is_err()); // @ not allowed
        assert!(validator.validate_symbol("ETH-USDT").is_err()); // Not in whitelist
    }
    
    #[test]
    fn test_quantity_validation() {
        let validator = TradingValidator::default();
        
        // Valid quantities
        assert!(validator.validate_quantity(1.0).is_ok());
        assert!(validator.validate_quantity(0.001).is_ok());
        
        // Invalid quantities
        assert!(validator.validate_quantity(0.0).is_err());
        assert!(validator.validate_quantity(-1.0).is_err());
        assert!(validator.validate_quantity(f64::NAN).is_err());
        assert!(validator.validate_quantity(f64::INFINITY).is_err());
    }
    
    #[test]
    fn test_price_validation() {
        let validator = TradingValidator::default();
        
        // Valid prices
        assert!(validator.validate_price(50000.0).is_ok());
        assert!(validator.validate_price(0.001).is_ok());
        
        // Invalid prices
        assert!(validator.validate_price(0.0).is_err());
        assert!(validator.validate_price(-1000.0).is_err());
        assert!(validator.validate_price(f64::NAN).is_err());
    }
}