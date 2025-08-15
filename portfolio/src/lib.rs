use std::collections::HashMap;
use std::sync::{RwLock, atomic::{AtomicU64, Ordering}};
use std::time::{Instant, Duration};
use protocol::broker::messages::Wallet;
use serde::{Serialize, Deserialize};

/// Represents a cryptocurrency with its balance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CryptoBalance {
    pub symbol: String,
    pub available_balance: f64,
    pub last_updated: u64, // Timestamp in milliseconds
}

/// Portfolio metrics for reporting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioMetrics {
    pub total_value: f64,
    pub value_by_exchange: HashMap<String, f64>,
    pub positions: Vec<Position>,
    pub last_updated: u64,
}

/// Position details for a specific asset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub exchange: String,
    pub quantity: f64,
    pub market_price: f64,
    pub market_value: f64,
}

/// Main wallet structure that aggregates balances across exchanges
pub struct CryptoWallet {
    // Map of exchange -> symbol -> balance
    balances: RwLock<HashMap<String, HashMap<String, CryptoBalance>>>,
    
    // Current market prices for valuation (symbol -> price)
    market_prices: RwLock<HashMap<String, f64>>,
    
    // Performance metrics
    update_count: AtomicU64,
    total_update_time_ns: AtomicU64,
}

/// Message types for standardized exchange communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageType {
    Snapshot,
    Update,
    Error,
}

/// Standardized message structure for exchange updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeMessage {
    pub exchange: String,
    pub message_type: MessageType,
    pub data: Vec<BalanceData>,
    pub timestamp: u64,
}

/// Balance data within exchange messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceData {
    pub symbol: String,
    pub available_balance: f64,
    pub sequence: u64,
}

impl CryptoWallet {
    /// Create a new empty wallet
    pub fn new() -> Self {
        Self {
            balances: RwLock::new(HashMap::new()),
            market_prices: RwLock::new(HashMap::new()),
            update_count: AtomicU64::new(0),
            total_update_time_ns: AtomicU64::new(0),
        }
    }
    
    /// Process a protobuf Wallets message directly
    pub fn process_wallets_message(&self, exchange: &str, wallets: &Vec<Wallet>, timestamp: u64) -> Result<(), String> {
        let start = Instant::now();
        
        // Acquire write lock
        let mut balances = match self.balances.write() {
            Ok(balances) => balances,
            Err(_) => return Err("Failed to acquire balances write lock".to_string()),
        };
        
        // Get or create exchange map
        let exchange_balances = balances
            .entry(exchange.to_string())
            .or_insert_with(HashMap::new);
        
        // Process each wallet
        for wallet in wallets {            
            let crypto_balance = CryptoBalance {
                symbol: wallet.symbol.clone(),
                available_balance: wallet.balance as f64,
                last_updated: timestamp,
            };
            
            exchange_balances.insert(wallet.symbol.clone(), crypto_balance);
        }
        
        // Update performance metrics
        let duration = start.elapsed();
        self.update_count.fetch_add(1, Ordering::Relaxed);
        self.total_update_time_ns.fetch_add(
            duration.as_nanos() as u64,
            Ordering::Relaxed
        );
        
        Ok(())
    }
    
    /// Update market prices for portfolio valuation
    pub fn update_metrics(&self, prices: &HashMap<String, f64>) -> Result<(), String> {
        let mut market_prices = match self.market_prices.write() {
            Ok(prices) => prices,
            Err(_) => return Err("Failed to acquire market prices write lock".to_string()),
        };
        
        // Update prices
        for (symbol, price) in prices.iter() {
            market_prices.insert(symbol.clone(), *price);
        }
        
        Ok(())
    }
    
    /// Get portfolio metrics including valuation
    pub fn get_metrics(&self) -> Result<PortfolioMetrics, String> {
        // Acquire read locks
        let balances = match self.balances.read() {
            Ok(balances) => balances,
            Err(_) => return Err("Failed to acquire balances read lock".to_string()),
        };
        
        let market_prices = match self.market_prices.read() {
            Ok(prices) => prices,
            Err(_) => return Err("Failed to acquire market prices read lock".to_string()),
        };
        
        let mut total_value = 0.0;
        let mut value_by_exchange = HashMap::new();
        let mut positions = Vec::new();
        
        // Calculate metrics for each position
        for (exchange, exchange_balances) in balances.iter() {
            let mut exchange_value = 0.0;
            
            for (symbol, balance) in exchange_balances.iter() {
                // Get market price (default to 0 if not available)
                let market_price = market_prices.get(symbol).unwrap_or(&0.0);
                let market_value = balance.available_balance * market_price;
                
                // Add to totals
                total_value += market_value;
                exchange_value += market_value;
                
                // Create position entry
                if balance.available_balance > 0.0 {
                    positions.push(Position {
                        symbol: symbol.clone(),
                        exchange: exchange.clone(),
                        quantity: balance.available_balance,
                        market_price: *market_price,
                        market_value,
                    });
                }
            }
            
            value_by_exchange.insert(exchange.clone(), exchange_value);
        }
        
        // Sort positions by market value (descending)
        positions.sort_by(|a, b| b.market_value.partial_cmp(&a.market_value).unwrap());
        
        Ok(PortfolioMetrics {
            total_value,
            value_by_exchange,
            positions,
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        })
    }
    
    /// Get balance for a specific cryptocurrency on a specific exchange
    pub fn get_balance(&self, exchange: &str, symbol: &str) -> Result<Option<CryptoBalance>, String> {
        // Acquire read lock
        let balances = match self.balances.read() {
            Ok(balances) => balances,
            Err(_) => return Err("Failed to acquire balances read lock".to_string()),
        };
        
        // Get exchange balances
        if let Some(exchange_balances) = balances.get(exchange) {
            // Get crypto balance
            if let Some(crypto_balance) = exchange_balances.get(symbol) {
                return Ok(Some(crypto_balance.clone()));
            }
        }
        
        Ok(None)
    }
    
    /// Get all balances for a specific exchange
    pub fn get_exchange_balances(&self, exchange: &str) -> Result<HashMap<String, CryptoBalance>, String> {
        // Acquire read lock
        let balances = match self.balances.read() {
            Ok(balances) => balances,
            Err(_) => return Err("Failed to acquire balances read lock".to_string()),
        };
        
        // Get exchange balances
        if let Some(exchange_balances) = balances.get(exchange) {
            return Ok(exchange_balances.clone());
        }
        
        Ok(HashMap::new())
    }
    
    /// Get all balances across all exchanges
    pub fn get_all_balances(&self) -> Result<HashMap<String, HashMap<String, CryptoBalance>>, String> {
        // Acquire read lock
        let balances = match self.balances.read() {
            Ok(balances) => balances,
            Err(_) => return Err("Failed to acquire balances read lock".to_string()),
        };
        
        Ok(balances.clone())
    }
    
    /// Get total balance of a cryptocurrency across all exchanges
    pub fn get_total_balance(&self, symbol: &str) -> Result<f64, String> {
        // Acquire read lock
        let balances = match self.balances.read() {
            Ok(balances) => balances,
            Err(_) => return Err("Failed to acquire balances read lock".to_string()),
        };
        
        let mut total = 0.0;
        
        // Sum balances across all exchanges
        for (_, exchange_balances) in balances.iter() {
            if let Some(crypto_balance) = exchange_balances.get(symbol) {
                total += crypto_balance.available_balance;
            }
        }
        
        Ok(total)
    }
    
    /// Get list of all currencies in the wallet
    pub fn get_all_currencies(&self) -> Result<Vec<String>, String> {
        // Acquire read lock
        let balances = match self.balances.read() {
            Ok(balances) => balances,
            Err(_) => return Err("Failed to acquire balances read lock".to_string()),
        };
        
        let mut currencies = Vec::new();
        let mut seen = std::collections::HashSet::new();
        
        // Collect unique currencies
        for (_, exchange_balances) in balances.iter() {
            for symbol in exchange_balances.keys() {
                if !seen.contains(symbol) {
                    currencies.push(symbol.clone());
                    seen.insert(symbol);
                }
            }
        }
        
        Ok(currencies)
    }
    
    /// Calculate average update time
    pub fn average_update_time(&self) -> Duration {
        let update_count = self.update_count.load(Ordering::Relaxed);
        let total_time = self.total_update_time_ns.load(Ordering::Relaxed);
        
        if update_count > 0 {
            Duration::from_nanos(total_time / update_count)
        } else {
            Duration::from_nanos(0)
        }
    }
    
    /// Get current market prices
    pub fn get_market_prices(&self) -> Result<HashMap<String, f64>, String> {
        let prices = match self.market_prices.read() {
            Ok(prices) => prices,
            Err(_) => return Err("Failed to acquire market prices read lock".to_string()),
        };
        
        Ok(prices.clone())
    }
    
    /// Set market price for a specific symbol
    pub fn set_market_price(&self, symbol: &str, price: f64) -> Result<(), String> {
        let mut market_prices = match self.market_prices.write() {
            Ok(prices) => prices,
            Err(_) => return Err("Failed to acquire market prices write lock".to_string()),
        };
        
        market_prices.insert(symbol.to_string(), price);
        Ok(())
    }
}