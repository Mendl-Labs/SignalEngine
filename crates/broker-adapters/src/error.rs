//! Error types shared by all broker adapters.

use crate::decimal::Dec;
use crate::nonce::NonceError;
use crate::transport::TransportError;

/// Coarse classification of an exchange-reported error code. Drives retry / halt decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// The request was rejected because its nonce was not strictly increasing. Nothing was
    /// processed; a retry with a fresh nonce is safe, but the cause (another user of the key,
    /// clock/persistence fault) must be investigated.
    InvalidNonce,
    /// Bad key, bad signature, missing permission. Never retry; page a human.
    Auth,
    /// Back off and retry later.
    RateLimited,
    /// The exchange failed or is busy. For order placement the OUTCOME IS UNKNOWN.
    ServiceUnavailable,
    InsufficientFunds,
    InvalidArguments,
    /// A definite business-rule rejection of the order.
    OrderRejected,
    UnknownOrder,
    /// Unrecognised code. Treated as a definite rejection but never auto-retried.
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeError {
    pub code: String,
    pub class: ErrorClass,
}

impl ExchangeError {
    /// True when the exchange may or may not have acted on the request.
    pub fn outcome_unknown(&self) -> bool {
        self.class == ErrorClass::ServiceUnavailable
    }
}

impl std::fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.code)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("unknown symbol: {0}")]
    UnknownSymbol(String),
    #[error("invalid order request: {0}")]
    InvalidRequest(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("{symbol}: quantity {requested} rounds down to {rounded} at the pair's lot precision, which is zero")]
    QuantityRoundsToZero { symbol: String, requested: Dec, rounded: Dec },
    #[error("{symbol}: quantity {rounded} is below the pair minimum order size {min}")]
    BelowMinQuantity { symbol: String, min: Dec, rounded: Dec },
    #[error("{symbol}: order cost {cost} is below the pair minimum cost {min}")]
    BelowMinCost { symbol: String, min: Dec, cost: Dec },
    #[error("invalid price: {0}")]
    InvalidPrice(String),
    #[error("{symbol}: pair is not tradable (status {status})")]
    PairNotTradable { symbol: String, status: String },
    #[error("credentials: {0}")]
    Credentials(String),
    #[error("nonce: {0}")]
    Nonce(#[from] NonceError),
    #[error("transport: {0}")]
    Transport(#[from] TransportError),
    #[error("unexpected HTTP status {0}")]
    Http(u16),
    #[error("malformed response: {0}")]
    Malformed(String),
    #[error("exchange error: {}", join_codes(.0))]
    Exchange(Vec<ExchangeError>),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("order tag {0:?} was never assigned a userref")]
    UnknownTag(String),
    #[error("userref: {0}")]
    Userref(String),
    #[error("configuration: {0}")]
    Config(String),
    /// HTTP 429 (Alpaca). The request was refused before processing, so it is retryable after
    /// `retry_after_secs` when the broker supplied a `Retry-After` header.
    #[error("rate limited{}: {message}", fmt_retry_after(.retry_after_secs))]
    RateLimited { retry_after_secs: Option<u64>, message: String },
    /// A market order was refused locally because the market is closed. NOTHING was sent; wait
    /// until `next_open` (broker-formatted timestamp) and retry.
    #[error("market is closed (next open {next_open}, next close {next_close}): market order refused")]
    MarketClosed { next_open: String, next_close: String },
    /// The account is blocked from trading (or not active). NOTHING was sent.
    #[error("account cannot trade: {0}")]
    AccountBlocked(String),
    /// A pre-order check (account / clock read) could not be completed. NOTHING was sent.
    #[error("pre-order check failed, nothing was sent: {0}")]
    Preflight(String),
    /// `cancel_and_settle`: the broker refused the cancel because it knows no such cancelable
    /// order, AND the follow-up query could not find the order either. The order id is wrong, or
    /// the broker has forgotten it; the caller must reconcile by tag and balances.
    #[error("cancel target {0} was refused as unknown and the follow-up query found no such order")]
    CancelTargetNotFound(String),
}

fn fmt_retry_after(secs: &Option<u64>) -> String {
    match secs {
        Some(s) => format!(" (retry after {s}s)"),
        None => String::new(),
    }
}

fn join_codes(errs: &[ExchangeError]) -> String {
    errs.iter().map(|e| e.code.as_str()).collect::<Vec<_>>().join(", ")
}
