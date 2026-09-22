//! Explicit paper/live selection and the base-URL guard.
//!
//! SignalEngine's Alpaca connector hard-codes the paper host (`alpaca_paper_definition`,
//! VERIFIED-FROM-REPO-CODE), so a credential the operator believes is live can silently trade on
//! paper. Here the environment and the base URL are BOTH required, and they must agree:
//!
//! * `Environment::Paper` needs `https://paper-api.alpaca.markets` (or a loopback host, so the
//!   fake broker can be used in tests);
//! * `Environment::Live` needs `https://api.alpaca.markets` and nothing else;
//! * any other host is refused (typos, look-alike hosts, `user@host` URL tricks, proxies).
//!
//! The credentials carry their own environment mark, and the adapter refuses a mismatch.

use crate::decimal::Dec;
use crate::error::BrokerError;

pub const PAPER_BASE_URL: &str = "https://paper-api.alpaca.markets";
pub const LIVE_BASE_URL: &str = "https://api.alpaca.markets";
pub const PAPER_HOST: &str = "paper-api.alpaca.markets";
pub const LIVE_HOST: &str = "api.alpaca.markets";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Environment {
    Paper,
    Live,
}

impl Environment {
    pub fn as_str(self) -> &'static str {
        match self {
            Environment::Paper => "paper",
            Environment::Live => "live",
        }
    }
    pub fn canonical_host(self) -> &'static str {
        match self {
            Environment::Paper => PAPER_HOST,
            Environment::Live => LIVE_HOST,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AlpacaConfig {
    pub environment: Environment,
    /// REQUIRED and explicit. Use [`PAPER_BASE_URL`] / [`LIVE_BASE_URL`]. Validated against
    /// `environment` by [`AlpacaConfig::new`] and again by the adapter constructor.
    pub base_url: String,
    /// Skip the "market closed" refusal for market orders and send `extended_hours: true` on
    /// limit+day orders. Market orders are never sent with `extended_hours` (Alpaca only allows it
    /// for limit orders, FROM-MEMORY-OF-DOCS): a market order placed while closed queues for the
    /// next session.
    pub allow_extended_hours: bool,
    /// When set, only orders whose tag starts with this prefix are treated as ours: placement
    /// refuses other tags, and `OrderReport::tag` is `None` for orders whose client_order_id lacks
    /// it (foreign orders). When unset, every order's client_order_id is reported as its tag.
    pub own_tag_prefix: Option<String>,
    /// Refuse to trade a symbol whose `AssetInfo` is only a built-in (unverified) row.
    pub refuse_builtin_assets: bool,
    /// Minimum order notional in dollars (Alpaca's fractional minimum is $1, FROM-MEMORY-OF-DOCS).
    /// Only enforced when a price is known (limit price or `reference_price`).
    pub min_notional: Dec,
}

impl AlpacaConfig {
    pub fn new(environment: Environment, base_url: &str) -> Result<Self, BrokerError> {
        let cfg = Self {
            environment,
            base_url: base_url.to_string(),
            allow_extended_hours: false,
            own_tag_prefix: None,
            refuse_builtin_assets: false,
            min_notional: Dec::from_i64(1),
        };
        cfg.validated()
    }

    /// Re-check the environment/host agreement and normalise `base_url`. Called by
    /// [`AlpacaConfig::new`] and by the adapter, so mutating a public field cannot bypass it.
    pub fn validated(mut self) -> Result<Self, BrokerError> {
        self.base_url = check_base_url(self.environment, &self.base_url)?;
        if let Some(p) = &self.own_tag_prefix {
            if p.is_empty() {
                return Err(BrokerError::Config("own_tag_prefix must not be empty".into()));
            }
        }
        if self.min_notional.is_negative() {
            return Err(BrokerError::Config("min_notional must not be negative".into()));
        }
        Ok(self)
    }
}

struct Parsed {
    scheme: String,
    host: String,
    port: Option<u16>,
}

fn is_loopback(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "[::1]")
}

fn parse_base_url(url: &str) -> Result<Parsed, BrokerError> {
    let bad = |why: &str| BrokerError::Config(format!("alpaca base_url {url:?}: {why}"));
    let t = url.trim();
    let (scheme, rest) = if let Some(r) = t.strip_prefix("https://") {
        ("https", r)
    } else if let Some(r) = t.strip_prefix("http://") {
        ("http", r)
    } else {
        return Err(bad("must start with https:// (or http:// for a loopback fake broker)"));
    };
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    if !(tail.is_empty() || tail == "/") {
        return Err(bad("must not contain a path, query or fragment"));
    }
    if authority.contains('@') {
        return Err(bad("must not contain user info"));
    }
    if authority.is_empty() {
        return Err(bad("empty host"));
    }
    let (host, port) = if let Some(stripped) = authority.strip_prefix('[') {
        let close = stripped.find(']').ok_or_else(|| bad("unterminated IPv6 literal"))?;
        let host = format!("[{}]", &stripped[..close]);
        let after = &stripped[close + 1..];
        let port = match after.strip_prefix(':') {
            Some(p) => Some(p.parse::<u16>().map_err(|_| bad("bad port"))?),
            None if after.is_empty() => None,
            None => return Err(bad("bad authority")),
        };
        (host, port)
    } else {
        match authority.split_once(':') {
            Some((h, p)) => (h.to_string(), Some(p.parse::<u16>().map_err(|_| bad("bad port"))?)),
            None => (authority.to_string(), None),
        }
    };
    let host = host.to_ascii_lowercase();
    if host.is_empty() {
        return Err(bad("empty host"));
    }
    Ok(Parsed { scheme: scheme.to_string(), host, port })
}

fn check_base_url(environment: Environment, url: &str) -> Result<String, BrokerError> {
    let p = parse_base_url(url)?;
    let mismatch = |why: String| BrokerError::Config(format!("alpaca environment/host mismatch: {why}"));
    if p.host == PAPER_HOST || p.host == LIVE_HOST {
        let host_env = if p.host == LIVE_HOST { Environment::Live } else { Environment::Paper };
        if host_env != environment {
            return Err(mismatch(format!(
                "environment is {} but the base URL host {} is the {} endpoint",
                environment.as_str(),
                p.host,
                host_env.as_str()
            )));
        }
        if p.scheme != "https" || !matches!(p.port, None | Some(443)) {
            return Err(BrokerError::Config(format!(
                "alpaca base_url {url:?}: the real endpoints require plain https on port 443"
            )));
        }
        return Ok(format!("https://{}", p.host));
    }
    if is_loopback(&p.host) {
        if environment == Environment::Live {
            return Err(mismatch("a live environment must use https://api.alpaca.markets, not a local host".into()));
        }
        let port = p.port.map(|n| format!(":{n}")).unwrap_or_default();
        return Ok(format!("{}://{}{}", p.scheme, p.host, port));
    }
    Err(BrokerError::Config(format!(
        "alpaca base_url host {:?} is not {} (for {}) or a loopback address; refusing to send credentials there",
        p.host,
        environment.canonical_host(),
        environment.as_str()
    )))
}
