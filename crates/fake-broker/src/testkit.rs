//! Wiring for tests: a fake exchange plus a REAL `KrakenAdapter` talking to it through the
//! adapter's own `HttpTransport` trait. No mocks of the adapter anywhere.

use crate::broker::{default_secret_b64, FakeBroker, FakeBrokerHandle, DEFAULT_ACCOUNT, DEFAULT_API_KEY};
use crate::clock::FakeClock;
use crate::kraken::KrakenTransport;
use broker_adapters::kraken::auth::KrakenCredentials;
use broker_adapters::kraken::pairs::PairTable;
use broker_adapters::kraken::userref::UserrefMap;
use broker_adapters::kraken::{KrakenAdapter, KrakenConfig};
use broker_adapters::nonce::{InMemoryNonceStore, NonceGenerator, NonceStore};
use broker_adapters::transport::{HttpMethod, HttpRequest, HttpTransport};
use std::sync::Arc;

pub use crate::money::dec as d;

/// A fake Kraken exchange with a real adapter attached.
///
/// * `adapter` uses the fake's clock for nonces (nanoseconds, like production), an in-memory nonce
///   store that survives [`restart_adapter`](Self::restart_adapter), and the adapter's built-in
///   pair table (which the tests prove agrees with the fake's own pair rows).
/// * `handle` is the control API; `transport` is the raw wire, for tests that need to send
///   hand-built requests.
pub struct KrakenRig {
    pub broker: FakeBroker,
    pub handle: FakeBrokerHandle,
    pub transport: Arc<KrakenTransport>,
    pub clock: Arc<FakeClock>,
    pub nonce_store: Arc<InMemoryNonceStore>,
    pub adapter: KrakenAdapter,
    pub config: KrakenConfig,
    pub account: String,
    pub api_key: String,
    pub secret_b64: String,
}

impl KrakenRig {
    /// Standard exchange (`BTC/USD` 60000, `ETH/USD` 3000, account `main` with 100000 USD).
    pub fn new() -> Self {
        Self::with_broker(FakeBroker::standard())
    }

    pub fn with_broker(broker: FakeBroker) -> Self {
        Self::with_broker_and_config(broker, KrakenConfig::default())
    }

    pub fn with_config(config: KrakenConfig) -> Self {
        Self::with_broker_and_config(FakeBroker::standard(), config)
    }

    pub fn with_broker_and_config(broker: FakeBroker, config: KrakenConfig) -> Self {
        let handle = broker.handle();
        let transport = broker.kraken_transport();
        let clock = broker.clock();
        let nonce_store = Arc::new(InMemoryNonceStore::new());
        let adapter = build_adapter(
            &config,
            DEFAULT_API_KEY,
            &default_secret_b64(),
            &transport,
            nonce_store.clone(),
            &clock,
            None,
        );
        Self {
            broker,
            handle,
            transport,
            clock,
            nonce_store,
            adapter,
            config,
            account: DEFAULT_ACCOUNT.to_string(),
            api_key: DEFAULT_API_KEY.to_string(),
            secret_b64: default_secret_b64(),
        }
    }

    /// Build another adapter for the same account. `nonce_store` is where its nonces persist;
    /// `userrefs` is the persisted tag table it starts from (`None` = empty).
    pub fn adapter_with(&self, nonce_store: Arc<dyn NonceStore>, userrefs: Option<UserrefMap>) -> KrakenAdapter {
        build_adapter(&self.config, &self.api_key, &self.secret_b64, &self.transport, nonce_store, &self.clock, userrefs)
    }

    /// Simulate a process restart done correctly: new adapter, nonce store and userref table
    /// carried over (as the rebalancer persists them).
    pub fn restart_adapter(&mut self) {
        let userrefs = self.adapter.userref_snapshot();
        self.adapter = self.adapter_with(self.nonce_store.clone(), Some(userrefs));
    }

    /// A restart that loses the persisted userref table but keeps the nonce store.
    pub fn restart_adapter_losing_userrefs(&mut self) {
        self.adapter = self.adapter_with(self.nonce_store.clone(), None);
    }

    /// A restart that loses BOTH (fresh in-memory nonce store), the failure the adapter's nonce
    /// persistence exists to prevent.
    pub fn restart_adapter_losing_everything(&mut self) {
        self.nonce_store = Arc::new(InMemoryNonceStore::new());
        self.adapter = self.adapter_with(self.nonce_store.clone(), None);
    }

    /// Fetch the fake's public `AssetPairs` over the wire and build the adapter's pair table from it.
    pub fn pair_table_from_exchange(&self) -> PairTable {
        let req = HttpRequest {
            method: HttpMethod::Get,
            url: "https://api.kraken.com/0/public/AssetPairs".to_string(),
            headers: Vec::new(),
            body: None,
        };
        let resp = self.transport.execute(&req).expect("AssetPairs is served");
        PairTable::from_asset_pairs_json(&resp.body).expect("AssetPairs parses")
    }

    /// Balance of the rig's account on the exchange.
    pub fn balance(&self, asset: &str) -> broker_adapters::Dec {
        self.handle.balance(&self.account, asset)
    }
}

impl Default for KrakenRig {
    fn default() -> Self {
        Self::new()
    }
}

fn build_adapter(
    config: &KrakenConfig,
    api_key: &str,
    secret_b64: &str,
    transport: &Arc<KrakenTransport>,
    nonce_store: Arc<dyn NonceStore>,
    clock: &Arc<FakeClock>,
    userrefs: Option<UserrefMap>,
) -> KrakenAdapter {
    let creds = KrakenCredentials::new(api_key, secret_b64).expect("rig credentials are valid");
    let nonces = NonceGenerator::new(nonce_store, clock.clone());
    let t: Arc<dyn HttpTransport> = transport.clone();
    let a = KrakenAdapter::new(config.clone(), creds, t, nonces, PairTable::builtin());
    match userrefs {
        Some(m) => a.with_userref_map(m),
        None => a,
    }
}
