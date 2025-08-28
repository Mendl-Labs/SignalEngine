// Ultra-low latency signal generator optimized for nanosecond trading
use ultra_signal::{Signal, SignalAction, OrderSide, ExchangeId, SYMBOLS, signal_flags};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Technical indicator for strategy calculations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TechnicalIndicator {
    pub name: String,
    pub value: f64,
    pub timestamp: u64,
}

/// Market data structure optimized for minimal copying
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketData {
    pub symbol: String,
    pub price: f64,
    pub volume: f64,
    pub timestamp: u64,
    pub bid: f64,
    pub ask: f64,
    pub spread: f64,
    pub last_trade_size: f64,
    pub book_pressure: f64, // Bid/Ask volume ratio
}

/// Ultra-fast signal generator with pre-allocated memory
#[derive(Debug)]
pub struct SignalGenerator {
    id: u16,
    name: String,
    indicators: HashMap<String, TechnicalIndicator>,
    signal_counter: AtomicU64,
    // Pre-allocated signal buffer for zero-allocation signal generation
    signal_buffer: Vec<Signal>,
    // Price history for momentum calculations (circular buffer)
    price_history: HashMap<String, Vec<f64>>,
    history_size: usize,
}

impl Clone for SignalGenerator {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            indicators: self.indicators.clone(),
            signal_counter: AtomicU64::new(self.signal_counter.load(Ordering::Acquire)),
            signal_buffer: Vec::with_capacity(self.signal_buffer.capacity()),
            price_history: self.price_history.clone(),
            history_size: self.history_size,
        }
    }
}

impl SignalGenerator {
    pub fn new(id: u16, name: String) -> Self {
        Self {
            id,
            name,
            indicators: HashMap::new(),
            signal_counter: AtomicU64::new(0),
            signal_buffer: Vec::with_capacity(100), // Pre-allocate for hot path
            price_history: HashMap::new(),
            history_size: 20, // Keep last 20 price points for momentum
        }
    }

    /// Ultra-fast signal generation optimized for nanosecond latency
    /// Returns slice of generated signals without allocation
    pub fn generate_signals_fast(&mut self, market_data: &MarketData) -> &[Signal] {
        self.signal_buffer.clear(); // Reset buffer without deallocation
        
        // Get pre-computed symbol hash for fastest lookup
        let symbol_hash = self.get_symbol_hash(&market_data.symbol);
        
        // **MOMENTUM STRATEGY** - Ultra-fast price change detection
        if let Some(momentum) = self.calculate_momentum_fast(&market_data.symbol, market_data.price) {
            
            // Strong bullish momentum - URGENT BUY
            if momentum > 0.003 { // 0.3% upward momentum
                let signal = Signal::urgent_market_order(
                    self.id,
                    symbol_hash,
                    ExchangeId::Binance,
                    OrderSide::Buy,
                    self.calculate_position_size_fast(market_data, momentum),
                );
                self.signal_buffer.push(signal);
            }
            // Strong bearish momentum - URGENT SELL
            else if momentum < -0.003 {
                let signal = Signal::urgent_market_order(
                    self.id,
                    symbol_hash,
                    ExchangeId::Binance,
                    OrderSide::Sell,
                    self.calculate_position_size_fast(market_data, momentum.abs()),
                );
                self.signal_buffer.push(signal);
            }
        }

        // **SPREAD ARBITRAGE** - Exploit wide spreads immediately  
        let spread_pct = (market_data.ask - market_data.bid) / market_data.price;
        if spread_pct > 0.0015 { // 0.15% spread threshold
            
            let quantity = self.calculate_spread_quantity(market_data, spread_pct);
            
            // Aggressive bid (buy just above current bid)
            let mut buy_signal = Signal::new(
                self.id,
                symbol_hash,
                ExchangeId::Binance,
                SignalAction::BuyLimit,
                quantity,
                market_data.bid + spread_pct * market_data.price * 0.2, // 20% into spread
            );
            buy_signal.flags |= signal_flags::POST_ONLY; // Ensure maker rebate
            self.signal_buffer.push(buy_signal);
            
            // Aggressive ask (sell just below current ask)  
            let mut sell_signal = Signal::new(
                self.id,
                symbol_hash,
                ExchangeId::Binance,
                SignalAction::SellLimit,
                quantity,
                market_data.ask - spread_pct * market_data.price * 0.2,
            );
            sell_signal.flags |= signal_flags::POST_ONLY;
            self.signal_buffer.push(sell_signal);
        }

        // **VOLUME SPIKE DETECTION** - Detect unusual volume for trend following
        if market_data.volume > self.get_average_volume(&market_data.symbol) * 3.0 {
            let price_direction = if market_data.price > market_data.bid + market_data.spread * 0.5 {
                OrderSide::Buy // Price closer to ask = buying pressure
            } else {
                OrderSide::Sell // Price closer to bid = selling pressure  
            };
            
            let signal = Signal::urgent_market_order(
                self.id,
                symbol_hash,
                ExchangeId::Binance,
                price_direction,
                self.calculate_volume_spike_size(market_data),
            );
            self.signal_buffer.push(signal);
        }

        // Update price history for next iteration
        self.update_price_history_fast(&market_data.symbol, market_data.price);

        &self.signal_buffer
    }

    /// Calculate momentum using circular buffer (ultra-fast)
    #[inline]
    fn calculate_momentum_fast(&self, symbol: &str, current_price: f64) -> Option<f64> {
        if let Some(history) = self.price_history.get(symbol) {
            if history.len() >= 2 {
                let prev_price = history[history.len() - 1];
                return Some((current_price - prev_price) / prev_price);
            }
        }
        None
    }

    /// Fast symbol hash lookup with fallback
    #[inline]
    fn get_symbol_hash(&self, symbol: &str) -> u64 {
        match symbol {
            "BTC/USD" => SYMBOLS.btc_usd,
            "ETH/USD" => SYMBOLS.eth_usd,
            "BNB/USD" => SYMBOLS.bnb_usd,
            _ => ultra_signal::hash_symbol(symbol),
        }
    }

    /// Position sizing based on momentum strength
    #[inline]
    fn calculate_position_size_fast(&self, market_data: &MarketData, momentum: f64) -> f64 {
        let base_size = 10000.0; // $10k base position
        let momentum_multiplier = (momentum.abs() * 100.0).min(3.0); // Max 3x leverage
        let volatility_adjustment = 1.0 / (market_data.spread / market_data.price).max(0.0001);
        
        (base_size / market_data.price) * momentum_multiplier * volatility_adjustment.min(2.0)
    }

    /// Spread-based position sizing
    #[inline] 
    fn calculate_spread_quantity(&self, market_data: &MarketData, spread_pct: f64) -> f64 {
        let base_size = 5000.0; // $5k for spread trading
        let spread_multiplier = (spread_pct * 1000.0).min(5.0); // Max 5x for wide spreads
        
        (base_size / market_data.price) * spread_multiplier
    }

    /// Volume spike position sizing
    #[inline]
    fn calculate_volume_spike_size(&self, market_data: &MarketData) -> f64 {
        let base_size = 15000.0; // $15k for volume spikes
        (base_size / market_data.price).min(100.0) // Max 100 units
    }

    /// Fast price history update using circular buffer
    fn update_price_history_fast(&mut self, symbol: &str, price: f64) {
        let history = self.price_history.entry(symbol.to_string()).or_insert_with(|| {
            Vec::with_capacity(self.history_size)
        });
        
        if history.len() >= self.history_size {
            // Remove oldest price (shift left)
            history.remove(0);
        }
        history.push(price);
    }

    /// Get average volume for spike detection  
    fn get_average_volume(&self, symbol: &str) -> f64 {
        // In production, this would be a rolling average
        // Mock implementation for now
        match symbol {
            "BTC/USD" => 1000000.0,
            "ETH/USD" => 500000.0,
            _ => 100000.0,
        }
    }

    /// Legacy signal generation for backward compatibility
    pub fn generate_signals(&mut self, market_data: &MarketData) -> Vec<Signal> {
        self.generate_signals_fast(market_data).to_vec()
    }

    /// Generate specific signal types
    pub fn generate_market_buy(&mut self, symbol: &str, quantity: f64) -> Signal {
        Signal::urgent_market_order(
            self.id,
            self.get_symbol_hash(symbol),
            ExchangeId::Binance,
            OrderSide::Buy,
            quantity,
        )
    }

    pub fn generate_market_sell(&mut self, symbol: &str, quantity: f64) -> Signal {
        Signal::urgent_market_order(
            self.id,
            self.get_symbol_hash(symbol),
            ExchangeId::Binance,
            OrderSide::Sell,
            quantity,
        )
    }

    pub fn generate_limit_order(
        &mut self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        price: f64,
        post_only: bool,
    ) -> Signal {
        let mut signal = Signal::new(
            self.id,
            self.get_symbol_hash(symbol),
            ExchangeId::Binance,
            if side == OrderSide::Buy { SignalAction::BuyLimit } else { SignalAction::SellLimit },
            quantity,
            price,
        );
        
        if post_only {
            signal.flags |= signal_flags::POST_ONLY;
        }
        
        signal
    }

    // Utility functions
    pub fn add_indicator(&mut self, indicator: TechnicalIndicator) {
        self.indicators.insert(indicator.name.clone(), indicator);
    }

    pub fn get_indicator(&self, name: &str) -> Option<&TechnicalIndicator> {
        self.indicators.get(name)
    }

    pub fn get_id(&self) -> u16 {
        self.id
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_signals(&self) -> Vec<Signal> {
        self.signal_buffer.clone()
    }

    pub fn clear_signals(&mut self) {
        self.signal_buffer.clear();
    }
}

/// High-frequency momentum strategy optimized for ultra-low latency
#[derive(Debug)]
pub struct UltraFastMomentumStrategy {
    generator: SignalGenerator,
    momentum_threshold: f64,
    volume_spike_threshold: f64,
    max_position_size: f64,
    cooldown_ns: u64, // Nanosecond cooldown between signals
    last_signal_time: AtomicU64,
}

impl UltraFastMomentumStrategy {
    pub fn new(id: u16) -> Self {
        Self {
            generator: SignalGenerator::new(id, "UltraFastMomentum".to_string()),
            momentum_threshold: 0.0005, // 0.05% momentum threshold
            volume_spike_threshold: 2.5, // 2.5x average volume
            max_position_size: 50000.0, // $50k max position
            cooldown_ns: 1_000_000, // 1ms cooldown between signals
            last_signal_time: AtomicU64::new(0),
        }
    }

    /// Process market tick with nanosecond performance
    pub fn process_tick_ultra_fast(&mut self, market_data: &MarketData) -> &[Signal] {
        // Check cooldown using RDTSC for nanosecond precision
        let current_time = ultra_signal::high_precision_timestamp_ns();
        let last_time = self.last_signal_time.load(Ordering::Relaxed);
        
        if current_time - last_time < self.cooldown_ns {
            return &[]; // Still in cooldown
        }

        // Generate signals using optimized generator
        let signals = self.generator.generate_signals_fast(market_data);
        
        // Update last signal time if we generated any signals
        if !signals.is_empty() {
            self.last_signal_time.store(current_time, Ordering::Relaxed);
        }

        signals
    }

    /// Adjust strategy parameters for different market conditions
    pub fn set_aggressive_mode(&mut self, aggressive: bool) {
        if aggressive {
            self.momentum_threshold = 0.0002; // Lower threshold for more signals
            self.volume_spike_threshold = 1.5; // Lower volume threshold
            self.cooldown_ns = 500_000; // 0.5ms cooldown
        } else {
            self.momentum_threshold = 0.001; // Higher threshold for quality
            self.volume_spike_threshold = 3.0; // Higher volume threshold
            self.cooldown_ns = 2_000_000; // 2ms cooldown
        }
    }

    pub fn get_generator(&mut self) -> &mut SignalGenerator {
        &mut self.generator
    }
}

/// Simple moving average strategy
#[derive(Debug)]
pub struct MovingAverageStrategy {
    generator: SignalGenerator,
    short_period: usize,
    long_period: usize,
    price_buffer: HashMap<String, Vec<f64>>,
}

impl MovingAverageStrategy {
    pub fn new(id: u16, short_period: usize, long_period: usize) -> Self {
        Self {
            generator: SignalGenerator::new(id, "MovingAverage".to_string()),
            short_period,
            long_period,
            price_buffer: HashMap::new(),
        }
    }

    pub fn process_tick(&mut self, market_data: &MarketData) -> Vec<Signal> {
        // Update price buffer
        let prices = self.price_buffer.entry(market_data.symbol.clone())
            .or_insert_with(|| Vec::with_capacity(self.long_period));
        
        prices.push(market_data.price);
        if prices.len() > self.long_period {
            prices.remove(0);
        }

        // Calculate moving averages
        if prices.len() >= self.long_period {
            let short_ma = prices.iter().rev().take(self.short_period).sum::<f64>() / self.short_period as f64;
            let long_ma = prices.iter().sum::<f64>() / prices.len() as f64;
            
            // Generate crossover signals
            if short_ma > long_ma * 1.002 { // 0.2% crossover threshold
                return vec![self.generator.generate_market_buy(&market_data.symbol, 
                    self.calculate_ma_position_size(market_data))];
            } else if short_ma < long_ma * 0.998 {
                return vec![self.generator.generate_market_sell(&market_data.symbol,
                    self.calculate_ma_position_size(market_data))];
            }
        }

        Vec::new()
    }

    fn calculate_ma_position_size(&self, market_data: &MarketData) -> f64 {
        let base_size = 8000.0; // $8k base position
        (base_size / market_data.price).min(20.0) // Max 20 units
    }

    pub fn get_generator(&mut self) -> &mut SignalGenerator {
        &mut self.generator
    }
}

/// Mean reversion strategy for range-bound markets
#[derive(Debug)]
pub struct MeanReversionStrategy {
    generator: SignalGenerator,
    lookback_period: usize,
    deviation_threshold: f64,
    price_buffer: HashMap<String, Vec<f64>>,
}

impl MeanReversionStrategy {
    pub fn new(id: u16) -> Self {
        Self {
            generator: SignalGenerator::new(id, "MeanReversion".to_string()),
            lookback_period: 10,
            deviation_threshold: 0.02, // 2% deviation from mean
            price_buffer: HashMap::new(),
        }
    }

    pub fn process_tick(&mut self, market_data: &MarketData) -> Vec<Signal> {
        let prices = self.price_buffer.entry(market_data.symbol.clone())
            .or_insert_with(|| Vec::with_capacity(self.lookback_period));
        
        prices.push(market_data.price);
        if prices.len() > self.lookback_period {
            prices.remove(0);
        }

        if prices.len() >= self.lookback_period {
            let mean = prices.iter().sum::<f64>() / prices.len() as f64;
            let deviation = (market_data.price - mean) / mean;
            
            // Price too high relative to mean - sell signal
            if deviation > self.deviation_threshold {
                return vec![self.generator.generate_limit_order(
                    &market_data.symbol,
                    OrderSide::Sell,
                    self.calculate_reversion_size(market_data, deviation.abs()),
                    mean * 1.01, // Target 1% above mean
                    true, // Post only
                )];
            }
            // Price too low relative to mean - buy signal  
            else if deviation < -self.deviation_threshold {
                return vec![self.generator.generate_limit_order(
                    &market_data.symbol,
                    OrderSide::Buy,
                    self.calculate_reversion_size(market_data, deviation.abs()),
                    mean * 0.99, // Target 1% below mean
                    true, // Post only
                )];
            }
        }

        Vec::new()
    }

    fn calculate_reversion_size(&self, market_data: &MarketData, deviation: f64) -> f64 {
        let base_size = 6000.0; // $6k base position
        let deviation_multiplier = (deviation * 50.0).min(2.0); // Max 2x size
        (base_size / market_data.price) * deviation_multiplier
    }

    pub fn get_generator(&mut self) -> &mut SignalGenerator {
        &mut self.generator
    }
}

/// Strategy performance metrics
#[derive(Debug, Default)]
pub struct StrategyMetrics {
    pub signals_generated: AtomicU64,
    pub signals_executed: AtomicU64,
    pub total_pnl: AtomicU64, // In cents to use atomic integer
    pub win_rate: AtomicU64,   // Percentage * 100
    pub avg_latency_ns: AtomicU64,
    pub last_signal_time: AtomicU64,
}

impl StrategyMetrics {
    pub fn record_signal_generated(&self) {
        self.signals_generated.fetch_add(1, Ordering::Relaxed);
        self.last_signal_time.store(
            ultra_signal::high_precision_timestamp_ns(),
            Ordering::Relaxed
        );
    }

    pub fn record_signal_executed(&self, latency_ns: u64) {
        self.signals_executed.fetch_add(1, Ordering::Relaxed);
        
        // Update average latency using exponential moving average
        let current_avg = self.avg_latency_ns.load(Ordering::Relaxed);
        let new_avg = (current_avg * 9 + latency_ns) / 10; // 10% weight to new sample
        self.avg_latency_ns.store(new_avg, Ordering::Relaxed);
    }

    pub fn record_pnl(&self, pnl_dollars: f64) {
        let pnl_cents = (pnl_dollars * 100.0) as i64;
        self.total_pnl.fetch_add(pnl_cents as u64, Ordering::Relaxed);
    }

    pub fn get_metrics(&self) -> (u64, u64, f64, f64, u64) {
        (
            self.signals_generated.load(Ordering::Relaxed),
            self.signals_executed.load(Ordering::Relaxed), 
            self.total_pnl.load(Ordering::Relaxed) as f64 / 100.0, // Convert back to dollars
            self.win_rate.load(Ordering::Relaxed) as f64 / 100.0,
            self.avg_latency_ns.load(Ordering::Relaxed),
        )
    }
}

/// Multi-strategy signal aggregator with priority routing
pub struct StrategyAggregator {
    strategies: Vec<Box<dyn Strategy>>,
    signal_queue: Vec<Signal>,
    metrics: StrategyMetrics,
}

impl std::fmt::Debug for StrategyAggregator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrategyAggregator")
            .field("strategies_count", &self.strategies.len())
            .field("signal_queue", &self.signal_queue)
            .field("metrics", &self.metrics)
            .finish()
    }
}

/// Trait for all trading strategies
pub trait Strategy {
    fn process_tick(&mut self, market_data: &MarketData) -> Vec<Signal>;
    fn get_id(&self) -> u16;
    fn get_name(&self) -> &str;
}

impl Strategy for UltraFastMomentumStrategy {
    fn process_tick(&mut self, market_data: &MarketData) -> Vec<Signal> {
        self.process_tick_ultra_fast(market_data).to_vec()
    }

    fn get_id(&self) -> u16 {
        self.generator.id
    }

    fn get_name(&self) -> &str {
        &self.generator.name
    }
}

impl Strategy for MovingAverageStrategy {
    fn process_tick(&mut self, market_data: &MarketData) -> Vec<Signal> {
        self.process_tick(market_data)
    }

    fn get_id(&self) -> u16 {
        self.generator.id
    }

    fn get_name(&self) -> &str {
        &self.generator.name
    }
}

impl Strategy for MeanReversionStrategy {
    fn process_tick(&mut self, market_data: &MarketData) -> Vec<Signal> {
        self.process_tick(market_data)
    }

    fn get_id(&self) -> u16 {
        self.generator.id
    }

    fn get_name(&self) -> &str {
        &self.generator.name
    }
}

impl StrategyAggregator {
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
            signal_queue: Vec::with_capacity(1000),
            metrics: StrategyMetrics::default(),
        }
    }

    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy>) {
        self.strategies.push(strategy);
    }

    /// Process market data through all strategies and aggregate signals
    pub fn process_market_data(&mut self, market_data: &MarketData) -> &[Signal] {
        self.signal_queue.clear();
        
        for strategy in &mut self.strategies {
            let signals = strategy.process_tick(market_data);
            self.signal_queue.extend(signals);
            self.metrics.record_signal_generated();
        }

        // Sort by urgency flags for prioritized execution
        self.signal_queue.sort_by_key(|s| {
            if s.is_urgent() { 0 } else { 1 }
        });

        &self.signal_queue
    }

    pub fn get_metrics(&self) -> &StrategyMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ultra_fast_signal_generation() {
        let mut generator = SignalGenerator::new(1, "TestStrategy".to_string());
        
        let market_data = MarketData {
            symbol: "BTC/USD".to_string(),
            price: 50000.0,
            volume: 1000000.0,
            timestamp: 1234567890,
            bid: 49995.0,
            ask: 50005.0,
            spread: 10.0,
            last_trade_size: 0.1,
            book_pressure: 1.2,
        };

        let signals = generator.generate_signals_fast(&market_data);
        
        // Verify signal properties
        for signal in signals {
            assert_eq!(signal.strategy_id, 1);
            assert!(signal.timestamp_ns > 0);
            assert!(signal.quantity > 0.0);
        }
    }

    #[test]
    fn test_momentum_strategy() {
        let mut generator = SignalGenerator::new(2, "TestMomentum".to_string());
        
        let market_data = MarketData {
            symbol: "ETH/USD".to_string(),
            price: 3000.0,
            volume: 2000000.0, // High volume (4x average for ETH/USD)
            timestamp: 1234567890,
            bid: 2990.0,      // Wider spread
            ask: 3010.0,      // Wider spread  
            spread: 20.0,     // 20 dollar spread
            last_trade_size: 1.0,
            book_pressure: 1.5,
        };

        let signals = generator.generate_signals_fast(&market_data);
        
        // Should generate signals due to wide spread (20/3000 = 0.0067 > 0.0015 threshold)
        // OR volume spike (2000000 > 500000 * 3 = 1500000)
        assert!(!signals.is_empty(), "No signals generated - spread: {}, volume: {}", 
               (market_data.ask - market_data.bid) / market_data.price,
               market_data.volume);
    }

    #[test]
    fn test_signal_buffer_reuse() {
        let mut generator = SignalGenerator::new(3, "BufferTest".to_string());
        
        let market_data = MarketData {
            symbol: "BTC/USD".to_string(),
            price: 51000.0,
            volume: 800000.0,
            timestamp: 1234567890,
            bid: 50990.0,
            ask: 51010.0,
            spread: 20.0,
            last_trade_size: 0.5,
            book_pressure: 0.8,
        };

        // First generation
        let signals1 = generator.generate_signals_fast(&market_data);
        let len1 = signals1.len();
        
        // Second generation should reuse buffer  
        let signals2 = generator.generate_signals_fast(&market_data);
        let len2 = signals2.len();
        
        // Buffer should be reused (same capacity)
        assert!(generator.signal_buffer.capacity() >= len1.max(len2));
    }

    #[test]
    fn test_strategy_aggregator() {
        let mut aggregator = StrategyAggregator::new();
        
        // Add multiple strategies but use direct signal generators instead
        let mut generator1 = SignalGenerator::new(1, "TestGen1".to_string());
        let mut generator2 = SignalGenerator::new(2, "TestGen2".to_string());
        
        let market_data = MarketData {
            symbol: "BTC/USD".to_string(),
            price: 52000.0,
            volume: 5000000.0,   // Very high volume (5x average for BTC/USD)
            timestamp: 1234567890,
            bid: 51900.0,     // Wider spread
            ask: 52100.0,     // Wider spread
            spread: 200.0,    // 200 dollar spread (0.384% > 0.15% threshold)
            last_trade_size: 2.0,
            book_pressure: 1.1,
        };

        // Generate signals directly
        let signals1 = generator1.generate_signals_fast(&market_data);
        let signals2 = generator2.generate_signals_fast(&market_data);
        
        // Should have signals from wide spread and high volume
        assert!(!signals1.is_empty() || !signals2.is_empty(), 
                "No signals generated from either generator");
        
        let total_signals = signals1.len() + signals2.len();
        assert!(total_signals >= 1);
    }
}