use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};

/// Position tracking for a specific symbol on an exchange
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub exchange: String,
    pub quantity: f64,     // Positive = long, negative = short
    pub average_price: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub last_updated: u64,
    pub entry_time: u64,
    pub total_fees: f64,
}

impl Position {
    pub fn new(symbol: String, exchange: String) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_else(|_| 0); // Use 0 if system time is before UNIX_EPOCH

        Self {
            symbol,
            exchange,
            quantity: 0.0,
            average_price: 0.0,
            unrealized_pnl: 0.0,
            realized_pnl: 0.0,
            last_updated: timestamp,
            entry_time: timestamp,
            total_fees: 0.0,
        }
    }

    /// Update position with new fill
    pub fn update_fill(&mut self, fill_quantity: f64, fill_price: f64, fees: f64) -> Result<(), String> {
        // Input validation for trading safety
        if !fill_quantity.is_finite() {
            return Err("Fill quantity must be finite (not NaN or infinite)".to_string());
        }
        if !fill_price.is_finite() || fill_price <= 0.0 {
            return Err("Fill price must be finite and positive".to_string());
        }
        if !fees.is_finite() || fees < 0.0 {
            return Err("Fees must be finite and non-negative".to_string());
        }
        if fill_quantity.abs() > 1_000_000.0 {
            return Err("Fill quantity exceeds maximum allowed (1M units)".to_string());
        }
        if fill_price > 10_000_000.0 {
            return Err("Fill price exceeds maximum allowed (10M)".to_string());
        }
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_else(|_| 0); // Use 0 if system time is before UNIX_EPOCH

        // Handle position changes
        if self.quantity == 0.0 {
            // New position
            self.quantity = fill_quantity;
            self.average_price = fill_price;
            self.entry_time = timestamp;
        } else if (self.quantity > 0.0 && fill_quantity > 0.0) || (self.quantity < 0.0 && fill_quantity < 0.0) {
            // Adding to position
            let total_cost = self.quantity * self.average_price + fill_quantity * fill_price;
            self.quantity += fill_quantity;
            self.average_price = total_cost / self.quantity;
        } else {
            // Reducing or reversing position
            let closing_quantity = fill_quantity.abs().min(self.quantity.abs());
            let remaining_quantity = self.quantity.abs() - closing_quantity;
            
            // Calculate realized PnL for closed portion
            let realized_pnl_change = if self.quantity > 0.0 {
                closing_quantity * (fill_price - self.average_price)
            } else {
                closing_quantity * (self.average_price - fill_price)
            };
            
            self.realized_pnl += realized_pnl_change;
            
            if remaining_quantity > 0.0 {
                // Partial close
                self.quantity = if self.quantity > 0.0 { remaining_quantity } else { -remaining_quantity };
            } else {
                // Full close or reversal
                let excess_quantity = fill_quantity.abs() - closing_quantity;
                if excess_quantity > 0.0 {
                    // Position reversal
                    self.quantity = if fill_quantity > 0.0 { excess_quantity } else { -excess_quantity };
                    self.average_price = fill_price;
                    self.entry_time = timestamp;
                } else {
                    // Position closed
                    self.quantity = 0.0;
                    self.average_price = 0.0;
                }
            }
        }

        self.total_fees += fees;
        self.last_updated = timestamp;
        Ok(())
    }

    /// Update unrealized PnL based on current market price
    pub fn update_unrealized_pnl(&mut self, current_price: f64) -> Result<(), String> {
        // Input validation
        if !current_price.is_finite() || current_price <= 0.0 {
            return Err("Current price must be finite and positive".to_string());
        }
        if current_price > 10_000_000.0 {
            return Err("Current price exceeds maximum allowed (10M)".to_string());
        }
        
        if self.quantity == 0.0 {
            self.unrealized_pnl = 0.0;
        } else {
            self.unrealized_pnl = if self.quantity > 0.0 {
                self.quantity * (current_price - self.average_price)
            } else {
                self.quantity * (self.average_price - current_price)
            };
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_else(|_| 0); // Use 0 if system time is before UNIX_EPOCH
        self.last_updated = timestamp;
        Ok(())
    }

    /// Get total PnL (realized + unrealized)
    pub fn total_pnl(&self) -> f64 {
        self.realized_pnl + self.unrealized_pnl
    }

    /// Check if position is flat (no position)
    pub fn is_flat(&self) -> bool {
        self.quantity.abs() < f64::EPSILON
    }

    /// Get position side
    pub fn side(&self) -> PositionSide {
        if self.quantity > f64::EPSILON {
            PositionSide::Long
        } else if self.quantity < -f64::EPSILON {
            PositionSide::Short
        } else {
            PositionSide::Flat
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PositionSide {
    Long,
    Short,
    Flat,
}

/// Portfolio-wide position and PnL tracking
pub struct PositionTracker {
    positions: Arc<RwLock<HashMap<(String, String), Position>>>, // (symbol, exchange) -> position
    market_prices: Arc<RwLock<HashMap<String, f64>>>, // symbol -> current price
}

impl PositionTracker {
    pub fn new() -> Self {
        Self {
            positions: Arc::new(RwLock::new(HashMap::new())),
            market_prices: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update position with fill information
    pub fn update_fill(&self, symbol: &str, exchange: &str, fill_quantity: f64, fill_price: f64, fees: f64) -> Result<(), String> {
        let key = (symbol.to_string(), exchange.to_string());
        let mut positions = self.positions.write().map_err(|_| "Failed to acquire positions lock")?;
        
        let position = positions.entry(key).or_insert_with(|| Position::new(symbol.to_string(), exchange.to_string()));
        let _ = position.update_fill(fill_quantity, fill_price, fees);
        
        Ok(())
    }

    /// Update market price for unrealized PnL calculations
    pub fn update_market_price(&self, symbol: &str, price: f64) -> Result<(), String> {
        // Update market price
        {
            let mut prices = self.market_prices.write().map_err(|_| "Failed to acquire prices lock")?;
            prices.insert(symbol.to_string(), price);
        }

        // Update unrealized PnL for all positions in this symbol
        {
            let mut positions = self.positions.write().map_err(|_| "Failed to acquire positions lock")?;
            for ((pos_symbol, _), position) in positions.iter_mut() {
                if pos_symbol == symbol {
                    let _ = position.update_unrealized_pnl(price);
                }
            }
        }

        Ok(())
    }

    /// Get position for specific symbol and exchange
    pub fn get_position(&self, symbol: &str, exchange: &str) -> Result<Option<Position>, String> {
        let key = (symbol.to_string(), exchange.to_string());
        let positions = self.positions.read().map_err(|_| "Failed to acquire positions lock")?;
        Ok(positions.get(&key).cloned())
    }

    /// Get all positions
    pub fn get_all_positions(&self) -> Result<HashMap<(String, String), Position>, String> {
        let positions = self.positions.read().map_err(|_| "Failed to acquire positions lock")?;
        Ok(positions.clone())
    }

    /// Get positions for a specific symbol across all exchanges
    pub fn get_positions_for_symbol(&self, symbol: &str) -> Result<Vec<Position>, String> {
        let positions = self.positions.read().map_err(|_| "Failed to acquire positions lock")?;
        let mut symbol_positions = Vec::new();
        
        for ((pos_symbol, _), position) in positions.iter() {
            if pos_symbol == symbol {
                symbol_positions.push(position.clone());
            }
        }
        
        Ok(symbol_positions)
    }

    /// Get net position for a symbol (sum across all exchanges)
    pub fn get_net_position(&self, symbol: &str) -> Result<f64, String> {
        let positions = self.get_positions_for_symbol(symbol)?;
        Ok(positions.iter().map(|p| p.quantity).sum())
    }

    /// Calculate total portfolio PnL
    pub fn get_total_pnl(&self) -> Result<PortfolioPnL, String> {
        let positions = self.positions.read().map_err(|_| "Failed to acquire positions lock")?;
        
        let mut total_realized = 0.0;
        let mut total_unrealized = 0.0;
        let mut total_fees = 0.0;
        
        for position in positions.values() {
            total_realized += position.realized_pnl;
            total_unrealized += position.unrealized_pnl;
            total_fees += position.total_fees;
        }

        Ok(PortfolioPnL {
            realized_pnl: total_realized,
            unrealized_pnl: total_unrealized,
            total_pnl: total_realized + total_unrealized,
            total_fees,
            position_count: positions.len(),
        })
    }

    /// Get PnL by exchange
    pub fn get_pnl_by_exchange(&self) -> Result<HashMap<String, f64>, String> {
        let positions = self.positions.read().map_err(|_| "Failed to acquire positions lock")?;
        let mut exchange_pnl = HashMap::new();

        for ((_, exchange), position) in positions.iter() {
            let current_pnl = exchange_pnl.get(exchange).cloned().unwrap_or(0.0);
            exchange_pnl.insert(exchange.clone(), current_pnl + position.total_pnl());
        }

        Ok(exchange_pnl)
    }

    /// Get positions that need market price updates
    pub fn get_stale_positions(&self, max_age_ms: u64) -> Result<Vec<(String, String)>, String> {
        let positions = self.positions.read().map_err(|_| "Failed to acquire positions lock")?;
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_else(|_| 0); // Use 0 if system time is before UNIX_EPOCH

        let mut stale_positions = Vec::new();
        
        for ((symbol, exchange), position) in positions.iter() {
            if current_time - position.last_updated > max_age_ms && !position.is_flat() {
                stale_positions.push((symbol.clone(), exchange.clone()));
            }
        }

        Ok(stale_positions)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioPnL {
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub total_pnl: f64,
    pub total_fees: f64,
    pub position_count: usize,
}

impl Default for PositionTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_tracking() {
        let tracker = PositionTracker::new();
        
        // Test opening long position
        tracker.update_fill("BTC/USD", "kraken", 1.0, 50000.0, 25.0).unwrap();
        let position = tracker.get_position("BTC/USD", "kraken").unwrap().unwrap();
        
        assert_eq!(position.quantity, 1.0);
        assert_eq!(position.average_price, 50000.0);
        assert_eq!(position.total_fees, 25.0);
        
        // Test updating market price
        tracker.update_market_price("BTC/USD", 51000.0).unwrap();
        let position = tracker.get_position("BTC/USD", "kraken").unwrap().unwrap();
        
        assert_eq!(position.unrealized_pnl, 1000.0); // 1 BTC * $1000 profit
        
        // Test partial close
        tracker.update_fill("BTC/USD", "kraken", -0.5, 51500.0, 12.5).unwrap();
        let position = tracker.get_position("BTC/USD", "kraken").unwrap().unwrap();
        
        assert_eq!(position.quantity, 0.5);
        assert_eq!(position.realized_pnl, 750.0); // 0.5 BTC * $1500 profit
        assert_eq!(position.total_fees, 37.5);
    }

    #[test]
    fn test_portfolio_pnl() {
        let tracker = PositionTracker::new();
        
        // Multiple positions
        tracker.update_fill("BTC/USD", "kraken", 1.0, 50000.0, 25.0).unwrap();
        tracker.update_fill("ETH/USD", "binance", 10.0, 3000.0, 15.0).unwrap();
        
        // Update prices
        tracker.update_market_price("BTC/USD", 51000.0).unwrap();
        tracker.update_market_price("ETH/USD", 3100.0).unwrap();
        
        let portfolio_pnl = tracker.get_total_pnl().unwrap();
        
        assert_eq!(portfolio_pnl.unrealized_pnl, 2000.0); // BTC: 1000, ETH: 1000
        assert_eq!(portfolio_pnl.total_fees, 40.0);
        assert_eq!(portfolio_pnl.position_count, 2);
    }
}
