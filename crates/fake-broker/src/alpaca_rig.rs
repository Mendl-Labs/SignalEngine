//! Wiring for Alpaca tests: a fake Alpaca exchange plus a REAL `AlpacaAdapter` talking to it through the adapter's
//! own `HttpTransport` trait. No mocks of the adapter anywhere. Nothing here can reach a network.

use std::sync::Arc;

use crate::alpaca::{AlpacaHandle, AlpacaTransport, FakeAlpaca};
use crate::clock::FakeClock;
use broker_adapters::alpaca::config::PAPER_BASE_URL;
use broker_adapters::alpaca::{AlpacaAdapter, AlpacaConfig, AlpacaCredentials, Environment};
use broker_adapters::transport::HttpTransport;

/// A fake Alpaca exchange with a real paper-environment adapter attached. The adapter's asset table is loaded over
/// the wire from the fake (`refresh_asset`, one call per known symbol), exactly as a real run would.
pub struct AlpacaRig {
    pub fake: FakeAlpaca,
    pub handle: AlpacaHandle,
    pub transport: Arc<AlpacaTransport>,
    pub clock: Arc<FakeClock>,
    pub adapter: AlpacaAdapter,
    pub key_id: String,
    pub secret: String,
    config: AlpacaConfig,
}

impl AlpacaRig {
    /// Standard exchange: $5,000 cash, SPY/EFA/IEF/DBC/VNQ at $100 each, adapter with no tag-prefix filter.
    pub fn new() -> Self {
        Self::with_fake(FakeAlpaca::standard(), |c| c)
    }

    /// Standard exchange; `f` customises the adapter config (for example setting `own_tag_prefix` or
    /// `refuse_builtin_assets`).
    pub fn with_config(f: impl FnOnce(AlpacaConfig) -> AlpacaConfig) -> Self {
        Self::with_fake(FakeAlpaca::standard(), f)
    }

    pub fn with_fake(fake: FakeAlpaca, f: impl FnOnce(AlpacaConfig) -> AlpacaConfig) -> Self {
        let handle = fake.handle();
        let transport = fake.transport();
        let clock = fake.clock();
        let (key_id, secret) = (handle.key_id(), handle.secret());
        let config = f(AlpacaConfig::new(Environment::Paper, PAPER_BASE_URL).expect("the paper host is valid"));
        let adapter = build_adapter(&config, &key_id, &secret, &transport);
        refresh_all(&adapter, &handle);
        Self { fake, handle, transport, clock, adapter, key_id, secret, config }
    }

    /// Simulate a process restart: a brand new adapter (no in-memory state) with the same credentials; the asset
    /// table is re-read from the exchange. Nothing is carried over: the adapter keeps no persistent state of its
    /// own, which is the point (idempotency lives at the broker, keyed by `client_order_id`).
    pub fn restart_adapter(&mut self) {
        let a = build_adapter(&self.config, &self.key_id, &self.secret, &self.transport);
        refresh_all(&a, &self.handle);
        self.adapter = a;
    }
}

impl Default for AlpacaRig {
    fn default() -> Self {
        Self::new()
    }
}

fn build_adapter(config: &AlpacaConfig, key_id: &str, secret: &str, transport: &Arc<AlpacaTransport>) -> AlpacaAdapter {
    let creds = AlpacaCredentials::new(Environment::Paper, key_id, secret).expect("rig credentials are valid");
    let t: Arc<dyn HttpTransport> = transport.clone();
    AlpacaAdapter::new(config.clone(), creds, t).expect("the rig adapter is valid")
}

fn refresh_all(adapter: &AlpacaAdapter, handle: &AlpacaHandle) {
    for symbol in handle.asset_symbols() {
        adapter.refresh_asset(&symbol).unwrap_or_else(|e| panic!("fake-broker: the rig's own fake refused to serve asset {symbol}: {e}"));
    }
}
