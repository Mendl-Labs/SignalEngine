#![cfg(feature = "reqwest-transport")]
//! ONE env-gated smoke test of the OANDA adapter over the REAL HTTP transport against the OANDA PRACTICE host.
//!
//! It is `#[ignore]`d and it also refuses to do anything unless BOTH `OANDA_API_KEY` and `OANDA_PRACTICE_ACCOUNT_ID` are set,
//! so `cargo test` never touches the network. The owner runs it by hand:
//!
//! ```text
//! OANDA_API_KEY=... OANDA_PRACTICE_ACCOUNT_ID=101-... \
//!   cargo test -p broker-adapters --features reqwest-transport --test oanda_practice_smoke -- --ignored --nocapture
//! ```
//!
//! SAFETY, in layers:
//! * the config is `OandaConfig::practice(PRACTICE_BASE_URL)`, which refuses the live host, and the credentials are marked
//!   `Environment::Practice`, which refuses a live (`001-`) account id;
//! * the transport handed to the adapter is wrapped so that any request whose URL is not on `api-fxpractice.oanda.com` (for
//!   example a redirect) is refused before it leaves;
//! * the token is never printed (only its adapter fingerprint is, and the account id is masked);
//! * it trades ONE unit of EUR_USD, only when the account is flat in EUR_USD with no netting surprises, and a drop guard
//!   tries to close the position again if the test panics half way.
//!
//! WHAT IT DOES: read-only summary / instruments / pricing / open positions / pending orders; then a 1-unit EUR_USD market
//! buy with a unique tag; verifies the transaction scan finds it by tag (it reports, but does not assert, whether `GET /orders/@tag` finds it: that answered 404 once and 200 later);
//! closes it with the one-sided close; verifies the account is flat again. It also places a far-away 1-unit LIMIT buy,
//! finds it by the pending lookup and cancels it.
//!
//! CALLED OUT: the `reqwest` transport has NEVER been exercised for a `PUT` WITHOUT A BODY. `cancel_order` / `cancel_and_settle`
//! send exactly that (`PUT /orders/{id}/cancel`, no body, no `Content-Type`), and the recorded cancel responses came from
//! another client. The limit-order step below is the first time this adapter's real transport does it. The position close
//! (`PUT` WITH a JSON body) is exercised as well. If the cancel step fails, that is the finding, not a flaky test.
//!
//! Nothing here can be run without credentials; compile check only:
//! `cargo test -p broker-adapters --features reqwest-transport --test oanda_practice_smoke --no-run`.

use broker_adapters::oanda::{CloseOutcome, CoverageKind, Environment, OandaAdapter, OandaConfig, OandaCredentials, PRACTICE_BASE_URL};
use broker_adapters::transport::reqwest_transport::ReqwestTransport;
use broker_adapters::transport::{HttpRequest, HttpResponse, HttpResponseDetailed, HttpTransport, TransportError};
use broker_adapters::{BrokerAdapter, Dec, OrderKind, OrderRequest, OrderStatus, PlaceOutcome, Side};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const PRACTICE_HOST_PREFIX: &str = "https://api-fxpractice.oanda.com/";

/// Refuses every request that is not addressed to the practice host, whatever the adapter or a redirect might do.
struct PracticeOnly(ReqwestTransport);

impl HttpTransport for PracticeOnly {
    fn execute(&self, req: &HttpRequest) -> Result<HttpResponse, TransportError> {
        self.execute_detailed(req).map(|r| HttpResponse { status: r.status, body: r.body })
    }

    fn execute_detailed(&self, req: &HttpRequest) -> Result<HttpResponseDetailed, TransportError> {
        if !req.url.starts_with(PRACTICE_HOST_PREFIX) {
            return Err(TransportError::ConnectFailed("smoke test: refusing a request that is not on the OANDA practice host".into()));
        }
        self.0.execute_detailed(req)
    }
}

fn mask(account: &str) -> String {
    let n = account.len();
    if n <= 8 {
        return "****".into();
    }
    format!("{}...{}", &account[..4], &account[n - 3..])
}

/// Best-effort: if the test dies with EUR_USD open, close it again.
struct FlatGuard<'a> {
    adapter: &'a OandaAdapter,
    armed: bool,
    tag: String,
}

impl Drop for FlatGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            eprintln!("smoke test: cleaning up, closing EUR_USD");
            let _ = self.adapter.close_position("EUR_USD", &self.tag);
        }
    }
}

#[test]
#[ignore = "talks to the real OANDA PRACTICE host; needs OANDA_API_KEY and OANDA_PRACTICE_ACCOUNT_ID; run by hand"]
fn oanda_practice_smoke() {
    let (Some(token), Some(account)) = (std::env::var("OANDA_API_KEY").ok().filter(|s| !s.is_empty()), std::env::var("OANDA_PRACTICE_ACCOUNT_ID").ok().filter(|s| !s.is_empty()))
    else {
        eprintln!("oanda_practice_smoke: OANDA_API_KEY and OANDA_PRACTICE_ACCOUNT_ID are not both set; doing nothing");
        return;
    };

    // ---- build the adapter: practice config, practice-marked credentials, practice-only transport
    let config = OandaConfig::practice(PRACTICE_BASE_URL).expect("the practice host is valid").with_own_tag_prefix("smoke-").unwrap();
    let creds = OandaCredentials::new(Environment::Practice, &token, &account).expect("practice credentials for a practice account id");
    let transport: Arc<dyn HttpTransport> = Arc::new(PracticeOnly(ReqwestTransport::new().expect("reqwest client")));
    let adapter = OandaAdapter::new(config, creds, transport).expect("adapter");
    assert_eq!(adapter.environment(), Environment::Practice);
    assert_eq!(adapter.base_url(), PRACTICE_BASE_URL, "never any other host");
    println!("smoke: account {} on {} (token fingerprint {})", mask(&account), adapter.base_url(), adapter.token_fingerprint());

    // ---- read-only
    let summary = adapter.verify_account().expect("summary (and: netting account, id matches)");
    println!("smoke: currency {} balance {} NAV {} lastTransactionID {:?}", summary.currency, summary.balance, summary.nav, summary.last_transaction_id);
    assert!(summary.last_transaction_id.is_some(), "the summary must carry lastTransactionID: the idempotency checkpoint reads it");
    let n = adapter.refresh_instruments().expect("instruments");
    let eur = adapter.instrument("EUR_USD").expect("EUR_USD is tradable on this account");
    println!(
        "smoke: {n} instruments; EUR_USD units precision {} min {} max order {} max position {:?} (None = no cap)",
        eur.trade_units_precision, eur.minimum_trade_size, eur.maximum_order_units, eur.maximum_position_size
    );
    let quote = adapter.get_quote("EUR/USD").expect("pricing (an open market is required)");
    println!("smoke: EUR/USD bid {} ask {}", quote.bid, quote.ask);
    let pricing = adapter.get_pricing(&["EUR_USD"]).expect("pricing with home conversions");
    println!("smoke: {} price rows, {} home conversions", pricing.prices.len(), pricing.home_conversions.len());
    let positions = adapter.get_open_positions().expect("open positions");
    println!("smoke: {} open positions", positions.len());
    let pending = adapter.open_orders().expect("pending orders");
    println!("smoke: {} pending orders", pending.len());
    if adapter.get_position("EUR_USD").expect("position read").is_some() {
        eprintln!("smoke: the account already holds EUR_USD; refusing to trade on top of it. Flatten it and rerun.");
        return;
    }

    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut guard = FlatGuard { adapter: &adapter, armed: false, tag: format!("smoke-cleanup-{stamp}") };

    // ---- a resting LIMIT order: pending lookup by @tag (the one lookup by client id OANDA answers), then cancel (PUT, NO BODY)
    let limit_tag = format!("smoke-limit-{stamp}");
    let limit = OrderRequest::limit(&limit_tag, "EUR/USD", Side::Buy, Dec::parse("1").unwrap(), Dec::parse("0.5").unwrap());
    let limit_id = match adapter.place_order(&limit).expect("limit placement") {
        PlaceOutcome::Accepted { broker_order_id, .. } => broker_order_id,
        other => panic!("the limit order was not accepted: {other:?}"),
    };
    let pending_by_tag = adapter.get_pending_order_by_tag(&limit_tag).expect("pending lookup").expect("a resting order is found by its client id");
    assert_eq!((pending_by_tag.broker_order_id.as_str(), pending_by_tag.status), (limit_id.as_str(), OrderStatus::Open));
    assert!(matches!(pending_by_tag.kind, Some(OrderKind::Limit { .. })));
    let (cancelled, report) = adapter.cancel_and_settle(&limit_id).expect("cancel: THE FIRST REAL PUT WITH NO BODY through reqwest");
    println!("smoke: limit order {limit_id} cancelled ({} cancelled, now {:?})", cancelled.canceled_count, report.status);
    assert_eq!(report.status, OrderStatus::Canceled);
    // OBSERVED, NOT ASSERTED: whether OANDA answers a client-id lookup for a CANCELLED order was 404 in one run (2026-09-23 21:14Z)
    // and 200 in later runs (22:30Z and after), so lookup by client id is not a reliable signal either way. The transaction scan below is.
    let after_cancel = adapter.get_pending_order_by_tag(&limit_tag).expect("lookup after cancel");
    println!("smoke: client-id lookup of the CANCELLED order: {}", if after_cancel.is_some() { "found" } else { "not found" });
    assert_eq!(adapter.find_orders_by_tag(&limit_tag).expect("scan lookup").len(), 1, "but the transaction stream still knows it");

    // ---- a 1-unit market buy, found by the transaction scan
    guard.armed = true;
    let buy_tag = format!("smoke-buy-{stamp}");
    let checkpoint_before = adapter.get_account_summary().unwrap().last_transaction_id;
    let out = adapter.place_order(&OrderRequest::market(&buy_tag, "EUR/USD", Side::Buy, Dec::parse("1").unwrap())).expect("market buy");
    let buy_id = match out {
        PlaceOutcome::Accepted { broker_order_id, warnings, .. } => {
            println!("smoke: market buy accepted as order {broker_order_id}, warnings {warnings:?}");
            broker_order_id
        }
        other => panic!("the market buy was not accepted: {other:?}"),
    };
    let cp = adapter.tag_checkpoint(&buy_tag).expect("the checkpoint of the first attempt is registered");
    println!("smoke: checkpoint {cp} (summary before said {checkpoint_before:?})");
    let scan = adapter.scan_since(&buy_tag, cp, CoverageKind::SinceCheckpoint).expect("scan since the checkpoint");
    assert_eq!(scan.orders.len(), 1, "the transaction scan finds the order by its tag");
    assert_eq!(scan.orders[0].order_id, buy_id);
    assert_eq!(scan.orders[0].fills.len(), 1, "with its fill");
    // OBSERVED, NOT ASSERTED (see above): a client-id lookup of a FILLED market order answered 404 once and 200 later.
    let by_id = adapter.get_pending_order_by_tag(&buy_tag).expect("lookup");
    println!("smoke: client-id lookup of the FILLED order: {}", if by_id.is_some() { "found" } else { "not found" });
    let found = adapter.find_orders_by_tag(&buy_tag).expect("lookup by tag");
    assert_eq!(found.len(), 1);
    assert_eq!((found[0].status, found[0].executed_quantity), (OrderStatus::Filled, Dec::parse("1").unwrap()));
    // a second call with the same tag must adopt, not buy again
    assert!(matches!(adapter.place_order(&OrderRequest::market(&buy_tag, "EUR/USD", Side::Buy, Dec::parse("1").unwrap())).expect("replay"), PlaceOutcome::Accepted { .. }));
    let pos = adapter.get_position("EUR_USD").expect("position").expect("one unit long");
    assert_eq!(pos.net_units(), Dec::parse("1").unwrap(), "exactly one unit: the replay did not buy again");

    // ---- close it with the one-sided close (PUT WITH a JSON body)
    let close_tag = format!("smoke-close-{stamp}");
    match adapter.close_position("EUR_USD", &close_tag).expect("close") {
        CloseOutcome::Closed { broker_order_id, fill } => println!("smoke: closed by order {broker_order_id}, units {}, pl {:?}", fill.units, fill.pl),
        other => panic!("the close did not report Closed: {other:?}"),
    }
    guard.armed = false;
    // INFORMATION (UNMEASURED until now): does OANDA echo longClientExtensions onto the closeout transactions?
    let cp_close = adapter.tag_checkpoint(&close_tag).expect("close checkpoint");
    let echoed = adapter.scan_since(&close_tag, cp_close, CoverageKind::SinceCheckpoint).expect("scan").orders.len();
    println!("smoke: closeout transactions carry the client id: {}", if echoed > 0 { "YES" } else { "NO (the adapter falls back to reading the position)" });

    // ---- flat again
    assert!(adapter.get_position("EUR_USD").expect("position").is_none(), "EUR_USD must be flat again");
    assert!(adapter.get_open_positions().expect("positions").iter().all(|p| p.instrument != "EUR_USD"));
    assert!(adapter.open_orders().expect("pending").iter().all(|o| o.tag.as_deref() != Some(limit_tag.as_str())));
    println!("smoke: OK, flat again");
}
