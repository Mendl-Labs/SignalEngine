//! Wiring for OANDA tests: a fake OANDA exchange plus a REAL `OandaAdapter` talking to it through the adapter's own
//! `HttpTransport` trait. No mocks of the adapter anywhere. Nothing here can reach a network.

use crate::clock::FakeClock;
use crate::oanda::{FakeOanda, OandaHandle, OandaTransport, DEFAULT_ACCOUNT_ID, DEFAULT_TOKEN};
use broker_adapters::oanda::{Environment, OandaAdapter, OandaConfig, OandaCredentials, PRACTICE_BASE_URL};
use broker_adapters::transport::HttpTransport;
use std::sync::Arc;

pub use crate::money::dec as d;

/// A fake OANDA exchange with a real practice-environment adapter attached. The adapter's instrument table is
/// loaded over the wire from the fake (`refresh_instruments`), exactly as a real run would.
pub struct OandaRig {
    pub fake: FakeOanda,
    pub handle: OandaHandle,
    pub transport: Arc<OandaTransport>,
    pub clock: Arc<FakeClock>,
    pub adapter: OandaAdapter,
    pub account_id: String,
    pub token: String,
    config: OandaConfig,
}

impl OandaRig {
    /// Standard exchange: EUR_USD, GBP_USD, USD_JPY, AUD_USD and 100000 USD; adapter with no tag-prefix filter.
    pub fn new() -> Self {
        Self::with_fake(FakeOanda::standard(), |c| c)
    }

    /// Standard exchange; `f` customises the adapter config (for example `with_own_tag_prefix("rb1:")`).
    pub fn with_config(f: impl FnOnce(OandaConfig) -> OandaConfig) -> Self {
        Self::with_fake(FakeOanda::standard(), f)
    }

    pub fn with_fake(fake: FakeOanda, f: impl FnOnce(OandaConfig) -> OandaConfig) -> Self {
        let handle = fake.handle();
        let transport = fake.transport();
        let clock = fake.clock();
        let (account_id, token) = (handle.account_id(), handle.token());
        let config = f(OandaConfig::practice(PRACTICE_BASE_URL).expect("the practice host is valid"));
        let adapter = build_adapter(&config, &account_id, &token, &transport);
        adapter.refresh_instruments().expect("the fake serves its instrument list");
        Self { fake, handle, transport, clock, adapter, account_id, token, config }
    }

    /// Simulate a process restart: a brand new adapter (no in-memory state) for the same account; the instrument
    /// table is re-read from the exchange. Nothing is carried over: the adapter keeps no persistent state of its own,
    /// which is the point (idempotency lives at the broker, keyed by the tag).
    pub fn restart_adapter(&mut self) {
        let a = build_adapter(&self.config, &self.account_id, &self.token, &self.transport);
        a.refresh_instruments().expect("the fake serves its instrument list");
        self.adapter = a;
    }
}

impl Default for OandaRig {
    fn default() -> Self {
        Self::new()
    }
}

fn build_adapter(config: &OandaConfig, account_id: &str, token: &str, transport: &Arc<OandaTransport>) -> OandaAdapter {
    let creds = OandaCredentials::new(Environment::Practice, token, account_id).expect("rig credentials are valid");
    let t: Arc<dyn HttpTransport> = transport.clone();
    OandaAdapter::new(config.clone(), creds, t).expect("the rig adapter is valid")
}

/// Kept so the defaults are visible from one place.
pub const RIG_ACCOUNT_ID: &str = DEFAULT_ACCOUNT_ID;
pub const RIG_TOKEN: &str = DEFAULT_TOKEN;
