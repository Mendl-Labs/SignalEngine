//! Explicit practice/live selection and the base-URL guard for OANDA.
//!
//! Design (mirrors the Alpaca guard, tightened for live money):
//!
//! * The environment is NEVER defaulted. There is no `Default` for [`Environment`] or
//!   [`OandaConfig`], [`Environment::from_name`] accepts only the exact words `practice` and
//!   `live`, and nothing in this crate turns a missing or unknown value into either.
//! * A practice config can only be built with [`OandaConfig::practice`]; it accepts the practice
//!   host or a loopback host (the fake broker), and REFUSES the live host.
//! * A live config can only be built with [`OandaConfig::live`], which needs a
//!   [`LiveTradingAck`] value that only [`LiveTradingAck::confirm_real_money`] can produce, and
//!   accepts ONLY the live host over https on port 443 (never loopback). The fields of
//!   [`OandaConfig`] are private, so a live config cannot be assembled by a struct literal or by
//!   mutating a practice config.
//! * The adapter constructor re-validates the config and cross-checks the credentials' own
//!   environment mark and account-number prefix (see `auth`).
//! * Any other host is refused: typos, look-alike hosts (`api-fxtrade.oanda.com.evil.test`,
//!   `evil-api-fxtrade.oanda.com`), user-info tricks (`api-fxtrade.oanda.com@evil.test`), a
//!   trailing dot, non-ASCII, ports other than 443, plain http, paths, queries, fragments.
//!
//! FROM-MEMORY-OF-DOCS: the hosts. REST is `https://api-fxpractice.oanda.com` (practice) and
//! `https://api-fxtrade.oanda.com` (live); the streaming hosts are `stream-fxpractice.oanda.com`
//! and `stream-fxtrade.oanda.com` and are NOT used by this adapter. The legacy SignalEngine
//! connector confirms only the practice REST and stream hosts (VERIFIED-FROM-REPO-CODE,
//! `oanda_practice_definition`).

use crate::error::BrokerError;

pub const PRACTICE_BASE_URL: &str = "https://api-fxpractice.oanda.com";
pub const LIVE_BASE_URL: &str = "https://api-fxtrade.oanda.com";
pub const PRACTICE_HOST: &str = "api-fxpractice.oanda.com";
pub const LIVE_HOST: &str = "api-fxtrade.oanda.com";

/// Which OANDA environment. No default, on purpose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Environment {
    Practice,
    Live,
}

impl Environment {
    pub fn as_str(self) -> &'static str {
        match self {
            Environment::Practice => "practice",
            Environment::Live => "live",
        }
    }

    pub fn canonical_host(self) -> &'static str {
        match self {
            Environment::Practice => PRACTICE_HOST,
            Environment::Live => LIVE_HOST,
        }
    }

    /// Strict parse of an operator-supplied name: exactly `practice` or `live` (any case, no
    /// surrounding text). Anything else, including the empty string, `paper`, `demo`, `prod`, is
    /// an error: an unknown or missing value must never silently pick an environment.
    pub fn from_name(name: &str) -> Result<Environment, BrokerError> {
        match name.trim().to_ascii_lowercase().as_str() {
            "practice" => Ok(Environment::Practice),
            "live" => Ok(Environment::Live),
            _ => Err(BrokerError::Config(format!(
                "oanda environment {name:?} is not recognised: it must be exactly \"practice\" or \"live\" (there is no default)"
            ))),
        }
    }
}

/// Proof that the caller meant to trade real money. Only [`LiveTradingAck::confirm_real_money`]
/// makes one, so a live config cannot come from a default, a config-file typo or a copy-paste of
/// the practice setup.
#[derive(Debug, Clone, Copy)]
pub struct LiveTradingAck(());

impl LiveTradingAck {
    /// Call this ONLY from code whose author decided, explicitly, to send real-money orders.
    pub fn confirm_real_money() -> Self {
        LiveTradingAck(())
    }
}

#[derive(Debug, Clone)]
pub struct OandaConfig {
    environment: Environment,
    base_url: String,
    own_tag_prefix: Option<String>,
    client_tag: String,
}

/// `clientExtensions.tag` sent with every order unless overridden: a fixed, non-secret marker.
pub const DEFAULT_CLIENT_TAG: &str = "mendl-rb";

impl OandaConfig {
    /// A PRACTICE config. `base_url` must be [`PRACTICE_BASE_URL`] (the practice host) or a
    /// loopback address for the fake broker. The live host is refused here.
    pub fn practice(base_url: &str) -> Result<Self, BrokerError> {
        Self::build(Environment::Practice, base_url)
    }

    /// A LIVE config: real money. Needs the acknowledgement token and the live host. Loopback is
    /// refused.
    pub fn live(base_url: &str, _ack: LiveTradingAck) -> Result<Self, BrokerError> {
        Self::build(Environment::Live, base_url)
    }

    fn build(environment: Environment, base_url: &str) -> Result<Self, BrokerError> {
        let base_url = check_base_url(environment, base_url)?;
        Ok(Self { environment, base_url, own_tag_prefix: None, client_tag: DEFAULT_CLIENT_TAG.to_string() })
    }

    /// Only orders whose tag starts with `prefix` are treated as ours: placement refuses other
    /// tags, and reports of orders without it (foreign orders) carry `tag: None`.
    pub fn with_own_tag_prefix(mut self, prefix: &str) -> Result<Self, BrokerError> {
        if prefix.is_empty() {
            return Err(BrokerError::Config("own_tag_prefix must not be empty".into()));
        }
        self.own_tag_prefix = Some(prefix.to_string());
        Ok(self)
    }

    /// Replace the `clientExtensions.tag` marker (printable ASCII, 1..=128 characters).
    pub fn with_client_tag(mut self, tag: &str) -> Result<Self, BrokerError> {
        if tag.is_empty() || tag.len() > 128 || !tag.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
            return Err(BrokerError::Config("client tag must be 1..=128 printable ASCII characters".into()));
        }
        self.client_tag = tag.to_string();
        Ok(self)
    }

    pub fn environment(&self) -> Environment {
        self.environment
    }
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
    pub fn own_tag_prefix(&self) -> Option<&str> {
        self.own_tag_prefix.as_deref()
    }
    pub fn client_tag(&self) -> &str {
        &self.client_tag
    }

    /// Re-run the environment/host agreement check on the (private, hence unmodified) fields.
    /// Called by the adapter constructor as a second line of defence.
    pub fn validated(mut self) -> Result<Self, BrokerError> {
        self.base_url = check_base_url(self.environment, &self.base_url)?;
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
    let bad = |why: &str| BrokerError::Config(format!("oanda base_url {url:?}: {why}"));
    let t = url.trim();
    if !t.is_ascii() {
        return Err(bad("must be ASCII"));
    }
    if t.bytes().any(|b| b.is_ascii_control() || b == b' ' || b == b'\\' || b == b'%') {
        return Err(bad("must not contain spaces, control characters, backslashes or percent-escapes"));
    }
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
    let mismatch = |why: String| BrokerError::Config(format!("oanda environment/host mismatch: {why}"));
    if p.host == PRACTICE_HOST || p.host == LIVE_HOST {
        let host_env = if p.host == LIVE_HOST { Environment::Live } else { Environment::Practice };
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
                "oanda base_url {url:?}: the real endpoints require plain https on port 443"
            )));
        }
        return Ok(format!("https://{}", p.host));
    }
    if is_loopback(&p.host) {
        if environment == Environment::Live {
            return Err(mismatch(format!("a live environment must use https://{LIVE_HOST}, not a local host")));
        }
        let port = p.port.map(|n| format!(":{n}")).unwrap_or_default();
        return Ok(format!("{}://{}{}", p.scheme, p.host, port));
    }
    Err(BrokerError::Config(format!(
        "oanda base_url host {:?} is not {} (for {}) or, for practice only, a loopback address; refusing to send credentials there",
        p.host,
        environment.canonical_host(),
        environment.as_str()
    )))
}
