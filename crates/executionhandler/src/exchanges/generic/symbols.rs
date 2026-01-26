//! Symbol conversion for different exchanges.
//!
//! Each exchange uses different symbol formats:
//! - Kraken: XXBTZUSD (X prefix for crypto, Z prefix for fiat, BTC -> XBT)
//! - Coinbase: BTC-USD (dash separator)
//! - Binance: BTCUSD (no separator)
//! - OKX: BTC-USD (dash separator)

use std::collections::HashMap;
use super::config::SymbolFormat;

/// Symbol converter for an exchange
#[derive(Debug, Clone)]
pub struct SymbolConverter {
    format: SymbolFormat,
    /// Reverse mappings for parsing exchange symbols back to standard format
    reverse_mappings: HashMap<String, String>,
}

impl SymbolConverter {
    /// Create a new symbol converter from exchange format config
    pub fn new(format: SymbolFormat) -> Self {
        // Build reverse mappings
        let reverse_mappings: HashMap<String, String> = format
            .custom_mappings
            .iter()
            .map(|(k, v)| (v.clone(), k.clone()))
            .collect();
        
        Self {
            format,
            reverse_mappings,
        }
    }
    
    /// Convert standard symbol (e.g., "BTC/USD") to exchange format
    pub fn to_exchange_format(&self, symbol: &str) -> String {
        // Split on common separators
        let parts: Vec<&str> = symbol.split(|c| c == '/' || c == '-' || c == '_').collect();
        
        if parts.len() != 2 {
            // Return as-is if not a pair
            return if self.format.uppercase {
                symbol.to_uppercase()
            } else {
                symbol.to_lowercase()
            };
        }
        
        let base = parts[0];
        let quote = parts[1];
        
        // Apply custom mappings (e.g., BTC -> XBT for Kraken)
        let mapped_base = self.format.custom_mappings
            .get(&base.to_uppercase())
            .map(|s| s.as_str())
            .unwrap_or(base);
        
        let mapped_quote = self.format.custom_mappings
            .get(&quote.to_uppercase())
            .map(|s| s.as_str())
            .unwrap_or(quote);
        
        // Apply prefixes (Kraken style: X for crypto, Z for fiat)
        let prefixed_base = format!("{}{}", self.format.base_prefix, mapped_base);
        let prefixed_quote = format!("{}{}", self.format.quote_prefix, mapped_quote);
        
        // Combine with separator
        let result = format!("{}{}{}", prefixed_base, self.format.separator, prefixed_quote);
        
        // Apply case
        if self.format.uppercase {
            result.to_uppercase()
        } else {
            result.to_lowercase()
        }
    }
    
    /// Convert exchange format back to standard symbol (e.g., "XXBTZUSD" -> "BTC/USD")
    pub fn from_exchange_format(&self, exchange_symbol: &str) -> String {
        let symbol = exchange_symbol.to_uppercase();
        
        // Try to find and remove prefixes
        let without_base_prefix = if !self.format.base_prefix.is_empty() 
            && symbol.starts_with(&self.format.base_prefix.to_uppercase()) 
        {
            &symbol[self.format.base_prefix.len()..]
        } else {
            &symbol
        };
        
        // Split by separator if present
        let parts: Vec<&str> = if self.format.separator.is_empty() {
            // No separator - need to guess the split point
            // Common patterns: 3+3 (BTCUSD), 3+4 (BTCUSDT), 4+3 (ETHBTC)
            vec![without_base_prefix] // Can't reliably split without separator
        } else {
            without_base_prefix.split(&self.format.separator).collect()
        };
        
        if parts.len() == 2 {
            // Apply reverse mappings
            let base = self.reverse_mappings
                .get(parts[0])
                .map(|s| s.as_str())
                .unwrap_or(parts[0]);
            
            // Remove quote prefix if present
            let quote_part = if !self.format.quote_prefix.is_empty() 
                && parts[1].starts_with(&self.format.quote_prefix.to_uppercase()) 
            {
                &parts[1][self.format.quote_prefix.len()..]
            } else {
                parts[1]
            };
            
            let quote = self.reverse_mappings
                .get(quote_part)
                .map(|s| s.as_str())
                .unwrap_or(quote_part);
            
            format!("{}/{}", base, quote)
        } else {
            // Return as-is with standard separator
            exchange_symbol.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    
    #[test]
    fn test_kraken_format() {
        let format = SymbolFormat {
            separator: "".to_string(),
            uppercase: true,
            custom_mappings: [("BTC".to_string(), "XBT".to_string())].into_iter().collect(),
            base_prefix: "X".to_string(),
            quote_prefix: "Z".to_string(),
        };
        
        let converter = SymbolConverter::new(format);
        assert_eq!(converter.to_exchange_format("BTC/USD"), "XXBTZUSD");
        assert_eq!(converter.to_exchange_format("ETH/USD"), "XETHZUSD");
    }
    
    #[test]
    fn test_binance_format() {
        let format = SymbolFormat {
            separator: "".to_string(),
            uppercase: true,
            custom_mappings: HashMap::new(),
            base_prefix: "".to_string(),
            quote_prefix: "".to_string(),
        };
        
        let converter = SymbolConverter::new(format);
        assert_eq!(converter.to_exchange_format("BTC/USD"), "BTCUSD");
        assert_eq!(converter.to_exchange_format("ETH/USDT"), "ETHUSDT");
    }
    
    #[test]
    fn test_coinbase_format() {
        let format = SymbolFormat {
            separator: "-".to_string(),
            uppercase: true,
            custom_mappings: HashMap::new(),
            base_prefix: "".to_string(),
            quote_prefix: "".to_string(),
        };
        
        let converter = SymbolConverter::new(format);
        assert_eq!(converter.to_exchange_format("BTC/USD"), "BTC-USD");
        assert_eq!(converter.to_exchange_format("ETH/USD"), "ETH-USD");
    }
    
    #[test]
    fn test_gemini_lowercase() {
        let format = SymbolFormat {
            separator: "".to_string(),
            uppercase: false,
            custom_mappings: HashMap::new(),
            base_prefix: "".to_string(),
            quote_prefix: "".to_string(),
        };
        
        let converter = SymbolConverter::new(format);
        assert_eq!(converter.to_exchange_format("BTC/USD"), "btcusd");
    }
}
