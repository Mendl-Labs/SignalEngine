//! The shared world (core + Kraken front-end state + fault queue + event log), the builder that
//! creates it, and [`FakeBrokerHandle`], the control API tests hold.

use crate::clock::{FakeClock, DEFAULT_START_NANOS};
use crate::exchange::policy::{FillPolicy, OrderRule};
use crate::exchange::{Core, FeeSchedule, Fill, ForeignOrder, Order, PairSpec, ReportGlitch};
use crate::fault::{Fault, FaultQueue};
use crate::kraken::{KrakenState, KrakenTransport, KeyPermissions, KeyState};
use crate::log::{self, ControlRecord, LogEntry, RequestRecord};
use crate::money::{add, dec, mul, sub};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use broker_adapters::decimal::Rounding;
use broker_adapters::transport::{HttpMethod, HttpResponse};
use broker_adapters::Dec;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

/// Default API key of the standard account. Not a real key.
pub const DEFAULT_API_KEY: &str = "FAKE-KRAKEN-API-KEY";
/// Default account name of the standard broker.
pub const DEFAULT_ACCOUNT: &str = "main";

/// Base64 secret of the standard account (the decoded bytes are an obvious test string).
pub fn default_secret_b64() -> String {
    B64.encode(b"fake-broker-secret-not-a-real-key")
}

/// Anything that can be turned into an exact decimal: a `Dec`, a `&str` literal, an integer.
pub trait IntoDec {
    fn into_dec(self) -> Dec;
}
impl IntoDec for Dec {
    fn into_dec(self) -> Dec {
        self
    }
}
impl IntoDec for &str {
    fn into_dec(self) -> Dec {
        dec(self)
    }
}
impl IntoDec for i64 {
    fn into_dec(self) -> Dec {
        Dec::from_i64(self)
    }
}

// ---------------------------------------------------------------- shared state

pub(crate) struct World {
    pub core: Core,
    pub kraken: KrakenState,
    pub faults: FaultQueue,
    pub log: Vec<LogEntry>,
    pub next_seq: u64,
}

impl World {
    pub fn seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }

    pub fn note(&mut self, now: u64, text: String) {
        let seq = self.seq();
        self.log.push(LogEntry::Control(ControlRecord { seq, time_nanos: now, note: text }));
    }

    pub fn record(&mut self, r: RequestRecord) {
        self.log.push(LogEntry::Request(r));
    }
}

pub(crate) struct Shared {
    world: Mutex<World>,
    pub clock: Arc<FakeClock>,
}

impl Shared {
    pub fn lock(&self) -> MutexGuard<'_, World> {
        self.world.lock().unwrap_or_else(|e| e.into_inner())
    }
}

// ---------------------------------------------------------------- builder

/// One trading account and its Kraken API key.
#[derive(Debug, Clone)]
pub struct AccountSpec {
    pub name: String,
    pub api_key: String,
    pub secret_b64: String,
    pub balances: Vec<(String, Dec)>,
}

impl AccountSpec {
    pub fn new(name: &str) -> Self {
        Self { name: name.to_string(), api_key: DEFAULT_API_KEY.to_string(), secret_b64: default_secret_b64(), balances: Vec::new() }
    }
    pub fn key(mut self, api_key: &str, secret_b64: &str) -> Self {
        self.api_key = api_key.to_string();
        self.secret_b64 = secret_b64.to_string();
        self
    }
    pub fn balance(mut self, asset: &str, amount: impl IntoDec) -> Self {
        self.balances.push((asset.to_string(), amount.into_dec()));
        self
    }
}

pub struct FakeBrokerBuilder {
    seed: u64,
    start_nanos: u64,
    host: String,
    fees: FeeSchedule,
    pairs: Vec<(PairSpec, Dec)>,
    accounts: Vec<AccountSpec>,
    closed_page_size: usize,
}

impl Default for FakeBrokerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeBrokerBuilder {
    /// An empty exchange: add pairs and accounts yourself.
    pub fn new() -> Self {
        Self {
            seed: 0xF4CE,
            start_nanos: DEFAULT_START_NANOS,
            host: "api.kraken.com".to_string(),
            fees: FeeSchedule::default(),
            pairs: Vec::new(),
            accounts: Vec::new(),
            closed_page_size: 50,
        }
    }

    /// BTC/USD at 60000, ETH/USD at 3000, and account `main` (default key) holding 100000 USD.
    /// The pair rows match the adapter's built-in table.
    pub fn standard() -> Self {
        Self::new()
            .pair(PairSpec::btc_usd(), "60000")
            .pair(PairSpec::eth_usd(), "3000")
            .account(AccountSpec::new(DEFAULT_ACCOUNT).balance("USD", "100000"))
    }

    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }
    pub fn start_nanos(mut self, nanos: u64) -> Self {
        self.start_nanos = nanos;
        self
    }
    /// The host the fake answers for. Requests to any other host fail with `ConnectFailed`.
    pub fn host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }
    pub fn fees(mut self, maker: impl IntoDec, taker: impl IntoDec) -> Self {
        self.fees = FeeSchedule { maker: maker.into_dec(), taker: taker.into_dec() };
        self
    }
    pub fn pair(mut self, spec: PairSpec, price: impl IntoDec) -> Self {
        self.pairs.push((spec, price.into_dec()));
        self
    }
    pub fn account(mut self, spec: AccountSpec) -> Self {
        self.accounts.push(spec);
        self
    }
    /// `ClosedOrders` page size (Kraken returns 50 per page).
    pub fn closed_page_size(mut self, n: usize) -> Self {
        assert!(n > 0);
        self.closed_page_size = n;
        self
    }

    pub fn build(self) -> FakeBroker {
        let mut core = Core::new(self.seed, self.fees);
        for (spec, price) in self.pairs {
            core.add_pair(spec, price);
        }
        let mut kraken = KrakenState::new(self.host, self.closed_page_size);
        for a in self.accounts {
            core.add_account(&a.name);
            for (asset, amount) in &a.balances {
                core.external_adjust(&a.name, asset, *amount);
            }
            let secret = B64.decode(a.secret_b64.trim()).expect("fake-broker: account secret must be base64");
            assert!(!kraken.keys.contains_key(&a.api_key), "fake-broker: duplicate api key {}", a.api_key);
            kraken.keys.insert(
                a.api_key.clone(),
                KeyState { account: a.name.clone(), secret, highest_nonce: None, perms: KeyPermissions::all(), revoked: false },
            );
        }
        let world = World { core, kraken, faults: FaultQueue::default(), log: Vec::new(), next_seq: 1 };
        FakeBroker { shared: Arc::new(Shared { world: Mutex::new(world), clock: Arc::new(FakeClock::new(self.start_nanos)) }) }
    }
}

// ---------------------------------------------------------------- the broker

/// The fake exchange. Hand `kraken_transport()` to the adapter, keep `handle()` for scripting.
#[derive(Clone)]
pub struct FakeBroker {
    shared: Arc<Shared>,
}

impl FakeBroker {
    pub fn builder() -> FakeBrokerBuilder {
        FakeBrokerBuilder::new()
    }

    /// The standard exchange, see [`FakeBrokerBuilder::standard`].
    pub fn standard() -> Self {
        FakeBrokerBuilder::standard().build()
    }

    /// The Kraken wire front end. Implements the adapter's `HttpTransport`.
    pub fn kraken_transport(&self) -> Arc<KrakenTransport> {
        Arc::new(KrakenTransport::new(self.shared.clone()))
    }

    pub fn handle(&self) -> FakeBrokerHandle {
        FakeBrokerHandle { shared: self.shared.clone() }
    }

    /// The manual clock. Also a `broker_adapters::nonce::Clock`, so it can drive the adapter's nonces.
    pub fn clock(&self) -> Arc<FakeClock> {
        self.shared.clock.clone()
    }
}

// ---------------------------------------------------------------- control API

/// Everything a test can do to the exchange besides talking to it over the wire. Cheap to
/// clone; all clones control the same exchange. Each mutating call leaves a note in the event
/// log so dumps read as a timeline.
#[derive(Clone)]
pub struct FakeBrokerHandle {
    shared: Arc<Shared>,
}

impl FakeBrokerHandle {
    fn now(&self) -> u64 {
        use broker_adapters::nonce::Clock;
        self.shared.clock.now_nanos()
    }

    /// Run a mutation and log a note about it.
    fn control<R>(&self, note: String, f: impl FnOnce(&mut World, u64) -> R) -> R {
        let now = self.now();
        let mut w = self.shared.lock();
        w.note(now, note);
        f(&mut w, now)
    }

    fn read<R>(&self, f: impl FnOnce(&World) -> R) -> R {
        f(&self.shared.lock())
    }

    // ------------------------------------------------------------ time

    pub fn clock(&self) -> Arc<FakeClock> {
        self.shared.clock.clone()
    }

    pub fn advance(&self, by: Duration) {
        self.shared.clock.advance(by);
    }

    pub fn advance_secs(&self, secs: u64) {
        self.shared.clock.advance_secs(secs);
    }

    // ------------------------------------------------------------ market

    /// Set the last price of a pair (bid = price, ask = price + spread) and let resting orders match.
    pub fn set_price(&self, pair: &str, price: impl IntoDec) {
        let p = price.into_dec();
        self.control(format!("set_price {pair} {p}"), |w, now| w.core.set_price(now, pair, p));
    }

    pub fn set_quote(&self, pair: &str, bid: impl IntoDec, ask: impl IntoDec, last: impl IntoDec) {
        let (b, a, l) = (bid.into_dec(), ask.into_dec(), last.into_dec());
        self.control(format!("set_quote {pair} bid={b} ask={a} last={l}"), |w, now| w.core.set_quote(now, pair, b, a, l));
    }

    pub fn set_spread(&self, pair: &str, spread: impl IntoDec) {
        let s = spread.into_dec();
        self.control(format!("set_spread {pair} {s}"), |w, _| w.core.set_spread(pair, s));
    }

    /// Move the last price by `ratio` (`-0.03` = down 3 percent), rounded to the pair's tick.
    /// Returns the new price.
    pub fn move_price_pct(&self, pair: &str, ratio: impl IntoDec) -> Dec {
        let r = ratio.into_dec();
        let (last, tick) = self.read(|w| {
            let m = w.core.market(pair).unwrap_or_else(|| panic!("fake-broker: unknown pair {pair}"));
            (m.last, m.spec.tick_size)
        });
        let new = mul(last, add(Dec::from_i64(1), r)).round_to_multiple(tick, Rounding::HalfUp).expect("tick is positive");
        self.set_price(pair, new);
        new
    }

    pub fn price(&self, pair: &str) -> Dec {
        self.read(|w| w.core.market(pair).unwrap_or_else(|| panic!("fake-broker: unknown pair {pair}")).last)
    }

    /// `(bid, ask, last)`.
    pub fn quote(&self, pair: &str) -> (Dec, Dec, Dec) {
        self.read(|w| {
            let m = w.core.market(pair).unwrap_or_else(|| panic!("fake-broker: unknown pair {pair}"));
            (m.bid, m.ask, m.last)
        })
    }

    /// Set the trading status of a pair: `online`, `cancel_only`, `post_only`, `limit_only`.
    pub fn set_pair_status(&self, pair: &str, status: &str) {
        self.control(format!("set_pair_status {pair} {status}"), |w, _| w.core.set_pair_status(pair, status));
    }

    pub fn set_fees(&self, maker: impl IntoDec, taker: impl IntoDec) {
        let f = FeeSchedule { maker: maker.into_dec(), taker: taker.into_dec() };
        self.control(format!("set_fees maker={} taker={}", f.maker, f.taker), |w, _| w.core.set_fee_default(f));
    }

    // ------------------------------------------------------------ accounts

    pub fn balance(&self, account: &str, asset: &str) -> Dec {
        self.read(|w| w.core.balance(account, asset))
    }

    pub fn balances(&self, account: &str) -> BTreeMap<String, Dec> {
        self.read(|w| w.core.balances(account))
    }

    /// Balance not committed to open orders.
    pub fn available(&self, account: &str, asset: &str) -> Dec {
        self.read(|w| w.core.available(account, asset))
    }

    /// Set a balance outright (an external movement, recorded in the ledger).
    pub fn set_balance(&self, account: &str, asset: &str, amount: impl IntoDec) {
        let a = amount.into_dec();
        self.control(format!("set_balance {account} {asset} {a}"), |w, _| w.core.set_balance(account, asset, a));
    }

    /// Change a balance by `delta` without any trade: unexplained drift, a deposit, a withdrawal.
    pub fn adjust_balance(&self, account: &str, asset: &str, delta: impl IntoDec) {
        let d = delta.into_dec();
        self.control(format!("adjust_balance {account} {asset} by {d}"), |w, _| w.core.external_adjust(account, asset, d));
    }

    /// Mark-to-market equity in USD (each holding at its `X/USD` last price).
    pub fn equity(&self, account: &str) -> Dec {
        self.equity_in(account, "USD")
    }

    pub fn equity_in(&self, account: &str, quote: &str) -> Dec {
        self.read(|w| w.core.equity(account, quote))
    }

    /// Make USD equity equal `target` by moving USD cash (an external deposit/withdrawal).
    pub fn set_equity(&self, account: &str, target: impl IntoDec) {
        let t = target.into_dec();
        self.control(format!("set_equity {account} {t}"), |w, _| {
            let delta = sub(t, w.core.equity(account, "USD"));
            w.core.external_adjust(account, "USD", delta);
        });
    }

    /// Per-account fee override (`None` = exchange default).
    pub fn set_account_fees(&self, account: &str, fees: Option<(Dec, Dec)>) {
        self.control(format!("set_account_fees {account} {fees:?}"), |w, _| {
            w.core.set_account_fees(account, fees.map(|(maker, taker)| FeeSchedule { maker, taker }))
        });
    }

    // ------------------------------------------------------------ orders

    pub fn orders(&self, account: &str) -> Vec<Order> {
        self.read(|w| w.core.orders_of(account).into_iter().cloned().collect())
    }

    pub fn order(&self, txid: &str) -> Option<Order> {
        self.read(|w| w.core.order(txid).cloned())
    }

    /// Orders that can still execute.
    pub fn live_orders(&self, account: &str) -> Vec<Order> {
        self.orders(account).into_iter().filter(|o| o.status.is_live()).collect()
    }

    pub fn orders_with_userref(&self, account: &str, userref: i32) -> Vec<Order> {
        self.orders(account).into_iter().filter(|o| o.userref == Some(userref)).collect()
    }

    /// Every fill of the account, oldest first.
    pub fn fills(&self, account: &str) -> Vec<Fill> {
        let mut v: Vec<Fill> = self.orders(account).into_iter().flat_map(|o| o.fills).collect();
        v.sort_by(|a, b| a.time_nanos.cmp(&b.time_nanos).then_with(|| a.id.cmp(&b.id)));
        v
    }

    /// Behaviour of every order that no rule matches. Default `Auto`.
    pub fn set_default_fill_policy(&self, policy: FillPolicy) {
        self.control(format!("set_default_fill_policy {policy:?}"), |w, _| w.core.set_default_policy(policy));
    }

    /// Script the next matching order(s), for example
    /// `handle.script_orders(OrderRule::next(FillPolicy::reject("EOrder:Insufficient funds")))`.
    pub fn script_orders(&self, rule: OrderRule) {
        self.control(format!("script_orders {rule:?}"), |w, _| w.core.push_rule(rule));
    }

    pub fn clear_order_rules(&self) {
        self.control("clear_order_rules".into(), |w, _| w.core.clear_rules());
    }

    /// Execute `qty` of a live order now (`price: None` = limit price / touch).
    pub fn script_fill(&self, txid: &str, qty: impl IntoDec, price: Option<Dec>) -> Result<(), String> {
        let q = qty.into_dec();
        self.control(format!("script_fill {txid} qty={q} price={price:?}"), |w, now| w.core.script_fill(now, txid, q, price))
    }

    /// Apply the next step of the order's `Partial` script.
    pub fn apply_next_fill(&self, txid: &str) -> Result<(), String> {
        self.control(format!("apply_next_fill {txid}"), |w, now| w.core.apply_next_fill(now, txid))
    }

    /// Move a `Pending` order to `Open`.
    pub fn activate_order(&self, txid: &str) -> Result<(), String> {
        self.control(format!("activate_order {txid}"), |w, now| w.core.activate(now, txid))
    }

    pub fn expire_order(&self, txid: &str) -> Result<(), String> {
        self.control(format!("expire_order {txid}"), |w, now| w.core.expire(now, txid, "Expired"))
    }

    /// Cancel from the exchange's side (not a user request).
    pub fn cancel_from_exchange(&self, txid: &str, reason: &str) -> Result<(), String> {
        self.control(format!("cancel_from_exchange {txid} {reason:?}"), |w, now| w.core.cancel_from_exchange(now, txid, reason))
    }

    /// Answer the next `n` cancel requests with `pending` and process them only on
    /// [`settle_pending_cancels`](Self::settle_pending_cancels). Until then the order may still fill.
    pub fn defer_next_cancels(&self, n: u32) {
        self.control(format!("defer_next_cancels {n}"), |w, _| w.core.defer_next_cancels(n));
    }

    pub fn settle_pending_cancels(&self) -> usize {
        self.control("settle_pending_cancels".into(), |w, now| w.core.settle_pending_cancels(now))
    }

    /// Someone else's order on the account (no userref of ours). Returns its txid. Panics if the
    /// order could not exist (bad pair, precision, funds).
    pub fn add_foreign_order(&self, account: &str, order: ForeignOrder) -> String {
        self.control(format!("add_foreign_order {account} {order:?}"), |w, now| {
            w.core
                .add_foreign_order(now, account, &order)
                .unwrap_or_else(|r| panic!("fake-broker: foreign order refused: {r:?}"))
        })
    }

    /// The next fill created anywhere shows up distorted in order reports.
    pub fn arm_fill_report_glitch(&self, glitch: ReportGlitch) {
        self.control(format!("arm_fill_report_glitch {glitch:?}"), |w, _| w.core.arm_glitch(glitch));
    }

    /// Make fill `fill_index` of an order vanish from order reports (balances still moved).
    pub fn drop_fill_report(&self, txid: &str, fill_index: usize) -> Result<(), String> {
        self.control(format!("drop_fill_report {txid}#{fill_index}"), |w, _| w.core.set_fill_glitch(txid, fill_index, ReportGlitch::Dropped))
    }

    /// Make fill `fill_index` count twice in order reports (balances moved once).
    pub fn duplicate_fill_report(&self, txid: &str, fill_index: usize) -> Result<(), String> {
        self.control(format!("duplicate_fill_report {txid}#{fill_index}"), |w, _| {
            w.core.set_fill_glitch(txid, fill_index, ReportGlitch::Duplicated)
        })
    }

    pub fn restore_fill_report(&self, txid: &str, fill_index: usize) -> Result<(), String> {
        self.control(format!("restore_fill_report {txid}#{fill_index}"), |w, _| w.core.set_fill_glitch(txid, fill_index, ReportGlitch::Normal))
    }

    // ------------------------------------------------------------ nonces and keys

    /// Highest nonce accepted so far for a key.
    pub fn highest_nonce(&self, api_key: &str) -> Option<u64> {
        self.read(|w| w.kraken.keys.get(api_key).and_then(|k| k.highest_nonce))
    }

    /// Force the highest-seen nonce, as if another process had used it.
    pub fn set_highest_nonce(&self, api_key: &str, nonce: u64) {
        self.control(format!("set_highest_nonce {api_key} {nonce}"), |w, _| {
            w.kraken.keys.get_mut(api_key).unwrap_or_else(|| panic!("fake-broker: unknown key {api_key}")).highest_nonce = Some(nonce);
        });
    }

    /// Play another process that shares the API key: a properly signed private request with a
    /// nonce of the caller's choosing. Goes through the real signature and nonce checks (and so
    /// advances the key's highest nonce on success) but never through injected faults. The
    /// response is returned and logged with origin `External`.
    pub fn other_process_call(&self, api_key: &str, path: &str, params: &[(&str, &str)], nonce: u64) -> HttpResponse {
        let secret = self.read(|w| w.kraken.keys.get(api_key).unwrap_or_else(|| panic!("fake-broker: unknown key {api_key}")).secret.clone());
        let nonce_text = nonce.to_string();
        let mut body = format!("nonce={nonce_text}");
        for (k, v) in params {
            body.push('&');
            body.push_str(&crate::kraken::wire::url_encode(k));
            body.push('=');
            body.push_str(&crate::kraken::wire::url_encode(v));
        }
        let sig = crate::kraken::wire::sign(&secret, path, &nonce_text, &body);
        let host = self.read(|w| w.kraken.host.clone());
        let req = broker_adapters::transport::HttpRequest {
            method: HttpMethod::Post,
            url: format!("https://{host}{path}"),
            headers: vec![("API-Key".into(), api_key.into()), ("API-Sign".into(), sig)],
            body: Some(body),
        };
        match KrakenTransport::new(self.shared.clone()).call(&req, log::Origin::External) {
            Ok(r) => r,
            Err(e) => panic!("fake-broker: external call failed at transport level: {e}"),
        }
    }

    pub fn set_key_permissions(&self, api_key: &str, perms: KeyPermissions) {
        self.control(format!("set_key_permissions {api_key} {perms:?}"), |w, _| {
            w.kraken.keys.get_mut(api_key).unwrap_or_else(|| panic!("fake-broker: unknown key {api_key}")).perms = perms;
        });
    }

    /// Make the exchange stop recognising a key (`EAPI:Invalid key`).
    pub fn revoke_key(&self, api_key: &str) {
        self.control(format!("revoke_key {api_key}"), |w, _| {
            w.kraken.keys.get_mut(api_key).unwrap_or_else(|| panic!("fake-broker: unknown key {api_key}")).revoked = true;
        });
    }

    // ------------------------------------------------------------ faults

    pub fn inject_fault(&self, fault: Fault) {
        self.control(format!("inject_fault {fault:?}"), |w, _| w.faults.push(fault));
    }

    /// Let requests held back by `Fault::..delayed()` reach the exchange now (they are
    /// authenticated and nonce-checked at this moment). Returns the exchange's answers.
    pub fn deliver_delayed(&self) -> Vec<HttpResponse> {
        self.control("deliver_delayed".into(), |_, _| ());
        KrakenTransport::new(self.shared.clone()).deliver_delayed()
    }

    /// How many requests are currently held back by delayed faults.
    pub fn delayed_count(&self) -> usize {
        self.read(|w| w.kraken.delayed.len())
    }

    pub fn clear_faults(&self) {
        self.control("clear_faults".into(), |w, _| w.faults.clear());
    }

    pub fn pending_faults(&self) -> Vec<Fault> {
        self.read(|w| w.faults.pending().to_vec())
    }

    // ------------------------------------------------------------ event log

    pub fn events(&self) -> Vec<LogEntry> {
        self.read(|w| w.log.clone())
    }

    /// Every request the exchange saw, in arrival order.
    pub fn requests(&self) -> Vec<RequestRecord> {
        self.read(|w| {
            w.log
                .iter()
                .filter_map(|e| match e {
                    LogEntry::Request(r) => Some(r.clone()),
                    LogEntry::Control(_) => None,
                })
                .collect()
        })
    }

    pub fn requests_to(&self, path: &str) -> Vec<RequestRecord> {
        self.requests().into_iter().filter(|r| r.path == path).collect()
    }

    /// Requests that reached the exchange (were applied), to `path`.
    pub fn applied_requests_to(&self, path: &str) -> Vec<RequestRecord> {
        self.requests_to(path).into_iter().filter(|r| r.reached_exchange).collect()
    }

    pub fn count_requests(&self, path: &str) -> usize {
        self.requests_to(path).len()
    }

    pub fn clear_log(&self) {
        let mut w = self.shared.lock();
        w.log.clear();
    }

    /// Add a note to the log (for example the name of the drill step).
    pub fn note(&self, text: &str) {
        let now = self.now();
        self.shared.lock().note(now, text.to_string());
    }

    /// The whole timeline as text.
    pub fn dump_log(&self) -> String {
        log::render(&self.events())
    }

    // ------------------------------------------------------------ invariants

    /// Check the exchange's internal invariants (balances = ledger + fills, no negative
    /// balances, order arithmetic).
    pub fn check_invariants(&self) -> Result<(), String> {
        self.read(|w| w.core.check_invariants())
    }

    /// Like [`check_invariants`](Self::check_invariants) but panics with the log on failure.
    pub fn assert_invariants(&self) {
        if let Err(e) = self.check_invariants() {
            panic!("fake-broker invariant violated: {e}\n--- event log ---\n{}", self.dump_log());
        }
    }
}
