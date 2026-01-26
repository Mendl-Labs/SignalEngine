//! Generic exchange connector that works with any REST-based cryptocurrency exchange.
//!
//! This module provides a unified interface for executing trades on multiple exchanges
//! with exchange-specific configurations for authentication, endpoints, and symbol formats.

mod connector;
mod config;
mod auth;
mod symbols;

pub use connector::GenericConnector;
pub use config::{
    ExchangePreset, ExchangeDefinition, AuthMethod, EndpointConfig, 
    OrderParamsMapping, RateLimits, TradingMode, OrderLimits, SymbolFormat,
};
pub use auth::{AuthStrategy, HmacSha256Auth, HmacSha512Auth, HmacSha256PassphraseAuth, AuthHeaders, create_auth_strategy};
pub use symbols::SymbolConverter;
