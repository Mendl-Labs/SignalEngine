//! Fill Probability Model
//!
//! Orderbook depth-aware execution probability estimation for smarter order routing
//! and execution decisions. Models the likelihood of order fills based on:
//! - Orderbook depth and liquidity
//! - Historical fill rates
//! - Order size relative to available liquidity
//! - Time-to-fill estimates
//! - Market conditions (spread, volatility)

use std::time::{Duration, Instant};
use parking_lot::RwLock;
use dashmap::DashMap;

/// Configuration for the fill probability model
#[derive(Debug, Clone)]
pub struct FillProbabilityConfig {
    /// Minimum sample size for reliable statistics
    pub min_samples: usize,
    /// Time window for historical data (seconds)
    pub history_window_secs: u64,
    /// Decay factor for older observations (0.0-1.0)
    pub decay_factor: f64,
    /// Spread threshold for "tight" market (bps)
    pub tight_spread_bps: f64,
    /// Spread threshold for "wide" market (bps)
    pub wide_spread_bps: f64,
    /// Volatility threshold for regime detection
    pub high_volatility_threshold: f64,
    /// Enable adaptive model updates
    pub adaptive_updates: bool,
    /// Maximum observations per symbol (memory bound)
    pub max_observations_per_symbol: usize,
}

impl Default for FillProbabilityConfig {
    fn default() -> Self {
        Self {
            min_samples: 20,
            history_window_secs: 3600, // 1 hour
            decay_factor: 0.95,
            tight_spread_bps: 5.0,
            wide_spread_bps: 20.0,
            high_volatility_threshold: 0.02, // 2%
            adaptive_updates: true,
            max_observations_per_symbol: 10_000, // Memory safety bound
        }
    }
}

/// Orderbook snapshot for fill probability calculation
#[derive(Debug, Clone)]
pub struct OrderbookSnapshot {
    /// Best bid price
    pub best_bid: f64,
    /// Best ask price
    pub best_ask: f64,
    /// Bid side depth: (price, quantity) pairs
    pub bids: Vec<(f64, f64)>,
    /// Ask side depth: (price, quantity) pairs
    pub asks: Vec<(f64, f64)>,
    /// Timestamp of snapshot
    pub timestamp: u64,
}

impl OrderbookSnapshot {
    /// Calculate spread in basis points
    pub fn spread_bps(&self) -> f64 {
        if self.best_bid > 0.0 {
            ((self.best_ask - self.best_bid) / self.best_bid) * 10000.0
        } else {
            f64::MAX
        }
    }

    /// Calculate mid price
    pub fn mid_price(&self) -> f64 {
        (self.best_bid + self.best_ask) / 2.0
    }

    /// Calculate total bid depth up to price level
    pub fn bid_depth_to_price(&self, price: f64) -> f64 {
        self.bids.iter()
            .filter(|(p, _)| *p >= price)
            .map(|(_, q)| q)
            .sum()
    }

    /// Calculate total ask depth up to price level
    pub fn ask_depth_to_price(&self, price: f64) -> f64 {
        self.asks.iter()
            .filter(|(p, _)| *p <= price)
            .map(|(_, q)| q)
            .sum()
    }

    /// Calculate imbalance ratio (positive = more bids)
    pub fn imbalance_ratio(&self, depth_levels: usize) -> f64 {
        let bid_vol: f64 = self.bids.iter().take(depth_levels).map(|(_, q)| q).sum();
        let ask_vol: f64 = self.asks.iter().take(depth_levels).map(|(_, q)| q).sum();
        
        if bid_vol + ask_vol > 0.0 {
            (bid_vol - ask_vol) / (bid_vol + ask_vol)
        } else {
            0.0
        }
    }
}

/// Fill probability estimate result
#[derive(Debug, Clone)]
pub struct FillProbabilityEstimate {
    /// Probability of full fill (0.0 - 1.0)
    pub full_fill_probability: f64,
    /// Probability of partial fill (0.0 - 1.0)
    pub partial_fill_probability: f64,
    /// Expected fill percentage (0.0 - 1.0)
    pub expected_fill_pct: f64,
    /// Estimated time to fill (ms)
    pub estimated_fill_time_ms: u64,
    /// Expected slippage (bps)
    pub expected_slippage_bps: f64,
    /// Confidence level of estimate (0.0 - 1.0)
    pub confidence: f64,
    /// Market regime detected
    pub market_regime: MarketRegime,
    /// Factors contributing to estimate
    pub factors: FillFactors,
}

/// Factors affecting fill probability
#[derive(Debug, Clone, Default)]
pub struct FillFactors {
    pub liquidity_score: f64,
    pub spread_score: f64,
    pub size_score: f64,
    pub historical_score: f64,
    pub volatility_score: f64,
    pub imbalance_score: f64,
}

/// Market regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketRegime {
    /// Normal trading conditions
    Normal,
    /// Tight spreads, high liquidity
    Liquid,
    /// Wide spreads, low liquidity
    Illiquid,
    /// High volatility
    Volatile,
    /// Very wide spreads or thin books
    Stressed,
}

/// Historical fill data for learning
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct FillObservation {
    order_size: f64,
    available_depth: f64,
    spread_bps: f64,
    filled_pct: f64,
    fill_time_ms: u64,
    slippage_bps: f64,
    timestamp: Instant,
}

/// Symbol-specific fill statistics
#[derive(Debug)]
struct SymbolFillStats {
    observations: Vec<FillObservation>,
    avg_fill_rate: f64,
    avg_fill_time_ms: f64,
    avg_slippage_bps: f64,
    samples: usize,
    last_updated: Instant,
}

impl Default for SymbolFillStats {
    fn default() -> Self {
        Self {
            observations: Vec::new(),
            avg_fill_rate: 0.85, // Default assumption
            avg_fill_time_ms: 100.0,
            avg_slippage_bps: 5.0,
            samples: 0,
            last_updated: Instant::now(),
        }
    }
}

/// Fill probability model
pub struct FillProbabilityModel {
    config: FillProbabilityConfig,
    /// Per-symbol statistics
    symbol_stats: DashMap<String, SymbolFillStats>,
    /// Per-exchange statistics
    exchange_stats: DashMap<String, ExchangeFillStats>,
    /// Global volatility estimate
    volatility: RwLock<f64>,
}

/// Per-exchange fill statistics
#[derive(Debug, Default)]
struct ExchangeFillStats {
    avg_latency_ms: f64,
    fill_rate: f64,
    reject_rate: f64,
    samples: usize,
}

impl FillProbabilityModel {
    /// Create a new fill probability model
    pub fn new(config: FillProbabilityConfig) -> Self {
        Self {
            config,
            symbol_stats: DashMap::new(),
            exchange_stats: DashMap::new(),
            volatility: RwLock::new(0.01), // Default 1% volatility
        }
    }

    /// Estimate fill probability for an order
    pub fn estimate_fill_probability(
        &self,
        symbol: &str,
        exchange: &str,
        is_buy: bool,
        order_size: f64,
        limit_price: Option<f64>,
        orderbook: &OrderbookSnapshot,
    ) -> FillProbabilityEstimate {
        let mut factors = FillFactors::default();

        // 1. Calculate liquidity score
        let available_depth = if is_buy {
            orderbook.ask_depth_to_price(limit_price.unwrap_or(f64::MAX))
        } else {
            orderbook.bid_depth_to_price(limit_price.unwrap_or(0.0))
        };
        
        factors.liquidity_score = self.calculate_liquidity_score(order_size, available_depth);

        // 2. Calculate spread score
        let spread_bps = orderbook.spread_bps();
        factors.spread_score = self.calculate_spread_score(spread_bps);

        // 3. Calculate size score (order size vs typical market size)
        factors.size_score = self.calculate_size_score(symbol, order_size);

        // 4. Get historical fill rate
        factors.historical_score = self.get_historical_score(symbol, exchange);

        // 5. Calculate volatility impact
        let volatility = *self.volatility.read();
        factors.volatility_score = self.calculate_volatility_score(volatility);

        // 6. Calculate orderbook imbalance impact
        let imbalance = orderbook.imbalance_ratio(5);
        factors.imbalance_score = self.calculate_imbalance_score(is_buy, imbalance);

        // Determine market regime
        let market_regime = self.classify_market_regime(spread_bps, volatility, available_depth, order_size);

        // Calculate composite fill probability
        let (full_prob, partial_prob) = self.calculate_fill_probabilities(&factors, market_regime);

        // Estimate fill time
        let fill_time_ms = self.estimate_fill_time(symbol, exchange, order_size, available_depth, market_regime);

        // Estimate slippage
        let slippage_bps = self.estimate_slippage(
            is_buy,
            order_size,
            available_depth,
            spread_bps,
            &orderbook,
        );

        // Calculate confidence based on data quality
        let confidence = self.calculate_confidence(symbol, exchange);

        FillProbabilityEstimate {
            full_fill_probability: full_prob,
            partial_fill_probability: partial_prob,
            expected_fill_pct: full_prob + partial_prob * 0.5,
            estimated_fill_time_ms: fill_time_ms,
            expected_slippage_bps: slippage_bps,
            confidence,
            market_regime,
            factors,
        }
    }

    /// Record a fill observation for model learning
    pub fn record_fill(
        &self,
        symbol: &str,
        exchange: &str,
        order_size: f64,
        available_depth: f64,
        spread_bps: f64,
        filled_pct: f64,
        fill_time_ms: u64,
        slippage_bps: f64,
    ) {
        if !self.config.adaptive_updates {
            return;
        }

        let observation = FillObservation {
            order_size,
            available_depth,
            spread_bps,
            filled_pct,
            fill_time_ms,
            slippage_bps,
            timestamp: Instant::now(),
        };

        // Update symbol stats
        let mut stats = self.symbol_stats
            .entry(symbol.to_string())
            .or_insert_with(SymbolFillStats::default);

        stats.observations.push(observation.clone());
        stats.samples += 1;
        stats.last_updated = Instant::now();

        // Prune old observations
        let cutoff = Instant::now() - Duration::from_secs(self.config.history_window_secs);
        stats.observations.retain(|o| o.timestamp > cutoff);
        
        // Enforce max observations bound (remove oldest if over limit)
        if stats.observations.len() > self.config.max_observations_per_symbol {
            let drain_count = stats.observations.len() - self.config.max_observations_per_symbol;
            stats.observations.drain(0..drain_count);
        }

        // Update running averages with decay
        let decay = self.config.decay_factor;
        stats.avg_fill_rate = stats.avg_fill_rate * decay + filled_pct * (1.0 - decay);
        stats.avg_fill_time_ms = stats.avg_fill_time_ms * decay + fill_time_ms as f64 * (1.0 - decay);
        stats.avg_slippage_bps = stats.avg_slippage_bps * decay + slippage_bps * (1.0 - decay);

        // Update exchange stats
        let mut ex_stats = self.exchange_stats
            .entry(exchange.to_string())
            .or_insert_with(ExchangeFillStats::default);

        ex_stats.fill_rate = ex_stats.fill_rate * decay + (if filled_pct > 0.0 { 1.0 } else { 0.0 }) * (1.0 - decay);
        ex_stats.samples += 1;
    }

    /// Record a rejection for model learning
    pub fn record_rejection(&self, exchange: &str) {
        if !self.config.adaptive_updates {
            return;
        }

        let mut ex_stats = self.exchange_stats
            .entry(exchange.to_string())
            .or_insert_with(ExchangeFillStats::default);

        let decay = self.config.decay_factor;
        ex_stats.reject_rate = ex_stats.reject_rate * decay + (1.0 - decay);
        ex_stats.samples += 1;
    }

    /// Update global volatility estimate
    pub fn update_volatility(&self, volatility: f64) {
        *self.volatility.write() = volatility;
    }

    /// Get fill statistics for a symbol
    pub fn get_symbol_stats(&self, symbol: &str) -> Option<(f64, f64, f64, usize)> {
        self.symbol_stats.get(symbol).map(|stats| {
            (stats.avg_fill_rate, stats.avg_fill_time_ms, stats.avg_slippage_bps, stats.samples)
        })
    }

    /// Get exchange statistics
    pub fn get_exchange_stats(&self, exchange: &str) -> Option<(f64, f64, usize)> {
        self.exchange_stats.get(exchange).map(|stats| {
            (stats.fill_rate, stats.reject_rate, stats.samples)
        })
    }

    // Private helper methods

    fn calculate_liquidity_score(&self, order_size: f64, available_depth: f64) -> f64 {
        if available_depth <= 0.0 {
            return 0.0;
        }
        
        let ratio = order_size / available_depth;
        
        // Score decreases as order size approaches available depth
        if ratio <= 0.1 {
            1.0 // Plenty of liquidity
        } else if ratio <= 0.5 {
            1.0 - (ratio - 0.1) * 0.5 // Linear decrease
        } else if ratio <= 1.0 {
            0.8 - (ratio - 0.5) * 0.8 // Steeper decrease
        } else {
            0.4 / ratio // Asymptotic approach to 0
        }
    }

    fn calculate_spread_score(&self, spread_bps: f64) -> f64 {
        if spread_bps <= self.config.tight_spread_bps {
            1.0
        } else if spread_bps <= self.config.wide_spread_bps {
            1.0 - (spread_bps - self.config.tight_spread_bps) 
                / (self.config.wide_spread_bps - self.config.tight_spread_bps) * 0.4
        } else {
            0.6 * (self.config.wide_spread_bps / spread_bps).min(1.0)
        }
    }

    fn calculate_size_score(&self, symbol: &str, order_size: f64) -> f64 {
        // Get typical order size from history
        if let Some(stats) = self.symbol_stats.get(symbol) {
            if stats.observations.len() >= self.config.min_samples {
                let avg_size: f64 = stats.observations.iter()
                    .map(|o| o.order_size)
                    .sum::<f64>() / stats.observations.len() as f64;
                
                let ratio = order_size / avg_size.max(0.001);
                
                if ratio <= 1.0 {
                    1.0
                } else if ratio <= 5.0 {
                    1.0 - (ratio - 1.0) * 0.1
                } else {
                    0.6 / (ratio / 5.0).sqrt()
                }
            } else {
                0.8 // Default if insufficient data
            }
        } else {
            0.8 // Default for unknown symbol
        }
    }

    fn get_historical_score(&self, symbol: &str, exchange: &str) -> f64 {
        let symbol_score = self.symbol_stats
            .get(symbol)
            .map(|s| s.avg_fill_rate)
            .unwrap_or(0.85);

        let exchange_score = self.exchange_stats
            .get(exchange)
            .map(|s| s.fill_rate)
            .unwrap_or(0.85);

        // Weighted average favoring symbol-specific data
        symbol_score * 0.7 + exchange_score * 0.3
    }

    fn calculate_volatility_score(&self, volatility: f64) -> f64 {
        if volatility <= self.config.high_volatility_threshold / 2.0 {
            1.0 // Low volatility is good for fills
        } else if volatility <= self.config.high_volatility_threshold {
            1.0 - (volatility - self.config.high_volatility_threshold / 2.0) 
                / (self.config.high_volatility_threshold / 2.0) * 0.3
        } else {
            0.7 - ((volatility - self.config.high_volatility_threshold) * 5.0).min(0.4)
        }
    }

    fn calculate_imbalance_score(&self, is_buy: bool, imbalance: f64) -> f64 {
        // Imbalance is positive when more bids than asks
        // For buys, positive imbalance (more bids) means competition, negative is favorable
        // For sells, negative imbalance (more asks) means competition, positive is favorable
        
        let favorable_imbalance = if is_buy { -imbalance } else { imbalance };
        
        // Map imbalance to 0.5-1.0 range
        0.75 + favorable_imbalance * 0.25
    }

    fn classify_market_regime(
        &self,
        spread_bps: f64,
        volatility: f64,
        depth: f64,
        order_size: f64,
    ) -> MarketRegime {
        // Stressed: very wide spread or extremely thin book
        if spread_bps > 50.0 || (depth > 0.0 && order_size / depth > 2.0) {
            return MarketRegime::Stressed;
        }

        // Volatile
        if volatility > self.config.high_volatility_threshold {
            return MarketRegime::Volatile;
        }

        // Illiquid: wide spread
        if spread_bps > self.config.wide_spread_bps {
            return MarketRegime::Illiquid;
        }

        // Liquid: tight spread
        if spread_bps < self.config.tight_spread_bps {
            return MarketRegime::Liquid;
        }

        MarketRegime::Normal
    }

    fn calculate_fill_probabilities(&self, factors: &FillFactors, regime: MarketRegime) -> (f64, f64) {
        // Base probability from factor combination
        let base_prob = factors.liquidity_score * 0.35 +
            factors.spread_score * 0.15 +
            factors.size_score * 0.15 +
            factors.historical_score * 0.20 +
            factors.volatility_score * 0.10 +
            factors.imbalance_score * 0.05;

        // Regime adjustments
        let (full_mult, partial_mult) = match regime {
            MarketRegime::Liquid => (1.1, 0.95),
            MarketRegime::Normal => (1.0, 1.0),
            MarketRegime::Illiquid => (0.8, 1.2),
            MarketRegime::Volatile => (0.7, 1.3),
            MarketRegime::Stressed => (0.4, 1.5),
        };

        let full_prob = (base_prob * full_mult).min(0.99);
        let partial_prob = ((1.0 - full_prob) * 0.6 * partial_mult).min(1.0 - full_prob);

        (full_prob, partial_prob)
    }

    fn estimate_fill_time(
        &self,
        symbol: &str,
        _exchange: &str,
        order_size: f64,
        depth: f64,
        regime: MarketRegime,
    ) -> u64 {
        // Base time from history
        let base_time = self.symbol_stats
            .get(symbol)
            .map(|s| s.avg_fill_time_ms)
            .unwrap_or(100.0);

        // Size adjustment
        let size_factor = if depth > 0.0 {
            1.0 + (order_size / depth).min(2.0)
        } else {
            2.0
        };

        // Regime adjustment
        let regime_factor = match regime {
            MarketRegime::Liquid => 0.7,
            MarketRegime::Normal => 1.0,
            MarketRegime::Illiquid => 1.5,
            MarketRegime::Volatile => 1.2,
            MarketRegime::Stressed => 3.0,
        };

        (base_time * size_factor * regime_factor) as u64
    }

    fn estimate_slippage(
        &self,
        is_buy: bool,
        order_size: f64,
        depth: f64,
        spread_bps: f64,
        orderbook: &OrderbookSnapshot,
    ) -> f64 {
        // Base slippage from spread (crossing spread costs half the spread)
        let spread_slip = spread_bps / 2.0;

        // Size impact
        let size_slip = if depth > 0.0 {
            let fill_ratio = order_size / depth;
            // Impact grows quadratically with size
            fill_ratio.powi(2) * 10.0 // 10 bps at 100% of depth
        } else {
            5.0 // Default impact
        };

        // Walk the book for market orders
        let book_slip = if order_size > 0.0 {
            let prices = if is_buy { &orderbook.asks } else { &orderbook.bids };
            let reference_price = if is_buy { orderbook.best_ask } else { orderbook.best_bid };
            
            if reference_price > 0.0 {
                let mut remaining = order_size;
                let mut weighted_price = 0.0;
                let mut total_filled = 0.0;

                for (price, qty) in prices.iter() {
                    let fill = remaining.min(*qty);
                    weighted_price += price * fill;
                    total_filled += fill;
                    remaining -= fill;
                    if remaining <= 0.0 {
                        break;
                    }
                }

                if total_filled > 0.0 {
                    let avg_price = weighted_price / total_filled;
                    let slip = if is_buy {
                        (avg_price - reference_price) / reference_price * 10000.0
                    } else {
                        (reference_price - avg_price) / reference_price * 10000.0
                    };
                    slip.max(0.0)
                } else {
                    0.0
                }
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Return max of estimates (conservative)
        spread_slip.max(size_slip).max(book_slip)
    }

    fn calculate_confidence(&self, symbol: &str, exchange: &str) -> f64 {
        let symbol_samples = self.symbol_stats
            .get(symbol)
            .map(|s| s.samples)
            .unwrap_or(0);

        let exchange_samples = self.exchange_stats
            .get(exchange)
            .map(|s| s.samples)
            .unwrap_or(0);

        let sample_factor = ((symbol_samples + exchange_samples) as f64 / 
            (self.config.min_samples * 2) as f64).min(1.0);

        // Check data freshness
        let freshness = self.symbol_stats
            .get(symbol)
            .map(|s| {
                let age = s.last_updated.elapsed().as_secs();
                1.0 - (age as f64 / self.config.history_window_secs as f64).min(1.0)
            })
            .unwrap_or(0.5);

        sample_factor * 0.7 + freshness * 0.3
    }
}

/// Global fill probability model instance
use once_cell::sync::Lazy;

pub static FILL_MODEL: Lazy<FillProbabilityModel> = Lazy::new(|| {
    FillProbabilityModel::new(FillProbabilityConfig::default())
});

/// Convenience function to estimate fill probability
pub fn estimate_fill(
    symbol: &str,
    exchange: &str,
    is_buy: bool,
    order_size: f64,
    limit_price: Option<f64>,
    orderbook: &OrderbookSnapshot,
) -> FillProbabilityEstimate {
    FILL_MODEL.estimate_fill_probability(symbol, exchange, is_buy, order_size, limit_price, orderbook)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_orderbook(spread_bps: f64, depth_per_level: f64) -> OrderbookSnapshot {
        let mid = 100.0;
        let half_spread = mid * spread_bps / 20000.0;
        
        let bids: Vec<(f64, f64)> = (0..10)
            .map(|i| (mid - half_spread - i as f64 * 0.1, depth_per_level))
            .collect();
        
        let asks: Vec<(f64, f64)> = (0..10)
            .map(|i| (mid + half_spread + i as f64 * 0.1, depth_per_level))
            .collect();

        OrderbookSnapshot {
            best_bid: bids[0].0,
            best_ask: asks[0].0,
            bids,
            asks,
            timestamp: 0,
        }
    }

    #[test]
    fn test_orderbook_metrics() {
        let ob = make_orderbook(10.0, 100.0);
        
        assert!((ob.spread_bps() - 10.0).abs() < 0.5);
        assert!((ob.mid_price() - 100.0).abs() < 0.1);
    }

    #[test]
    fn test_small_order_high_probability() {
        let model = FillProbabilityModel::new(FillProbabilityConfig::default());
        let orderbook = make_orderbook(5.0, 1000.0); // Tight spread, deep book
        
        let estimate = model.estimate_fill_probability(
            "BTC-USD",
            "test",
            true,
            1.0,
            None,
            &orderbook,
        );
        
        assert!(estimate.full_fill_probability > 0.7, "Small order should have high fill prob");
        assert!(estimate.expected_slippage_bps < 10.0, "Low slippage expected");
    }

    #[test]
    fn test_large_order_lower_probability() {
        let model = FillProbabilityModel::new(FillProbabilityConfig::default());
        let orderbook = make_orderbook(50.0, 10.0); // Wide spread, very shallow book (100 total depth)
        
        let estimate = model.estimate_fill_probability(
            "BTC-USD",
            "test",
            true,
            500.0, // 5x the total depth - severely oversized
            None,
            &orderbook,
        );
        
        // With order 5x the depth and wide spread, should have significantly lower probability
        assert!(estimate.full_fill_probability < 0.7, 
            "Large order should have lower fill prob: got {}", estimate.full_fill_probability);
        assert!(estimate.expected_slippage_bps > 10.0, 
            "Higher slippage expected: got {}", estimate.expected_slippage_bps);
    }

    #[test]
    fn test_wide_spread_penalty() {
        let model = FillProbabilityModel::new(FillProbabilityConfig::default());
        
        let tight_ob = make_orderbook(3.0, 100.0);
        let wide_ob = make_orderbook(50.0, 100.0);
        
        let tight_est = model.estimate_fill_probability("BTC-USD", "test", true, 10.0, None, &tight_ob);
        let wide_est = model.estimate_fill_probability("BTC-USD", "test", true, 10.0, None, &wide_ob);
        
        assert!(tight_est.full_fill_probability > wide_est.full_fill_probability, 
            "Tight spread should have higher fill prob");
    }

    #[test]
    fn test_market_regime_detection() {
        let model = FillProbabilityModel::new(FillProbabilityConfig::default());
        
        // Liquid market
        let liquid_ob = make_orderbook(3.0, 1000.0);
        let liquid_est = model.estimate_fill_probability("BTC-USD", "test", true, 10.0, None, &liquid_ob);
        assert_eq!(liquid_est.market_regime, MarketRegime::Liquid);
        
        // Stressed market (very wide spread)
        let stressed_ob = make_orderbook(100.0, 10.0);
        let stressed_est = model.estimate_fill_probability("BTC-USD", "test", true, 10.0, None, &stressed_ob);
        assert_eq!(stressed_est.market_regime, MarketRegime::Stressed);
    }

    #[test]
    fn test_learning_from_fills() {
        let model = FillProbabilityModel::new(FillProbabilityConfig::default());
        
        // Record some fills
        for i in 0..30 {
            model.record_fill(
                "BTC-USD",
                "test",
                10.0,
                100.0,
                5.0,
                0.95, // 95% fill rate
                50,
                2.0,
            );
        }
        
        let (fill_rate, fill_time, slippage, samples) = model.get_symbol_stats("BTC-USD").unwrap();
        
        assert!(fill_rate > 0.9, "Fill rate should be high after good fills");
        assert!(samples >= 30);
    }

    #[test]
    fn test_confidence_increases_with_data() {
        let model = FillProbabilityModel::new(FillProbabilityConfig {
            min_samples: 10,
            ..Default::default()
        });
        let orderbook = make_orderbook(10.0, 100.0);
        
        // Low confidence with no data
        let est1 = model.estimate_fill_probability("NEW-USD", "new-ex", true, 10.0, None, &orderbook);
        assert!(est1.confidence < 0.5);
        
        // Add data
        for _ in 0..20 {
            model.record_fill("NEW-USD", "new-ex", 10.0, 100.0, 10.0, 1.0, 50, 3.0);
        }
        
        // Higher confidence with data
        let est2 = model.estimate_fill_probability("NEW-USD", "new-ex", true, 10.0, None, &orderbook);
        assert!(est2.confidence > est1.confidence, "Confidence should increase with data");
    }
}
