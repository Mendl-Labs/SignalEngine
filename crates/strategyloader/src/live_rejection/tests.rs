use super::*;
use protocol::broker::messages::StrategyDeployment;
use std::sync::Mutex;
use protocol::broker::messages::MarketDataUnsubscribe;

// ---- test support --------------------------------------------------------

/// Records every ack; optionally fails.
#[derive(Default)]
pub(crate) struct RecordingSink {
    pub acks: Mutex<Vec<StrategyDeploymentAck>>,
    pub unsubs: Mutex<Vec<MarketDataUnsubscribe>>,
    pub fail: bool,
    pub fail_unsub: bool,
}

#[async_trait]
impl AckSink for RecordingSink {
    async fn publish_ack(&self, ack: StrategyDeploymentAck) -> Result<(), String> {
        if self.fail {
            return Err("broker down".to_string());
        }
        self.acks.lock().unwrap().push(ack);
        Ok(())
    }

    async fn publish_unsubscribe(&self, unsub: MarketDataUnsubscribe) -> Result<(), String> {
        if self.fail_unsub {
            return Err("unsubscribe path down".to_string());
        }
        self.unsubs.lock().unwrap().push(unsub);
        Ok(())
    }
}

pub(crate) fn deployment_msg(instance_id: Uuid, mode: &str) -> StrategyDeployment {
    StrategyDeployment {
        strategy_id: Uuid::new_v4().to_string(),
        instance_id: instance_id.to_string(),
        tenant_id: Uuid::new_v4().to_string(),
        strategy_type: "custom".to_string(),
        strategy_name: "unit".to_string(),
        target_exchanges: vec!["kraken".to_string()],
        symbols: vec!["BTC-USD".to_string()],
        mode: mode.to_string(),
        ..Default::default()
    }
}

pub(crate) fn deployed(instance_id: Uuid, mode: &str) -> Arc<DeployedStrategy> {
    Arc::new(
        DeployedStrategy::from_deployment(&deployment_msg(instance_id, mode), Default::default())
            .unwrap(),
    )
}

// ---- reason text ---------------------------------------------------------

#[test]
fn sanitize_strips_control_chars_and_collapses_whitespace() {
    assert_eq!(sanitize_reason("a\n\tb   c\u{0}d"), "a b c d");
}

#[test]
fn sanitize_redacts_key_like_tokens_but_keeps_uuids() {
    let key: &str = &["AKIAIO", "SFODNN", "7EXAMP", "LEwJal", "rXUtnF", "EMI/K7", "MDENG"].concat();
    let uuid = "11111111-2222-3333-4444-555555555555";
    let s = sanitize_reason(&format!("bad key={} tenant {} end", key, uuid));
    assert!(!s.contains("AKIAIOSFODNN7"), "{}", s);
    assert!(s.contains(uuid), "{}", s);
    assert!(s.contains("[redacted]"), "{}", s);
}

#[test]
fn sanitize_truncates() {
    let long = "word ".repeat(200);
    let s = sanitize_reason(&long);
    assert!(s.chars().count() <= MAX_REASON_LEN);
    assert!(s.ends_with("..."));
}

#[test]
fn canned_reasons_are_plain_and_name_the_fix() {
    let none = reason_no_provider();
    assert!(none.contains("live trading is disabled") && none.contains("paper"));
    let v = reason_credential_unavailable("kraken");
    assert!(v.contains("'kraken'") && v.contains("paper"));
    for r in [none, v] {
        assert_eq!(sanitize_reason(&r), r, "canned reasons must survive sanitizing unchanged");
    }
}

#[test]
fn failure_ack_is_a_failure_with_the_reason() {
    let sink = Arc::new(RecordingSink::default());
    let sender = AckSender::with_sink(sink, "node-7");
    let ack = sender.failure_ack("strat", "inst", "because");
    assert!(!ack.success);
    assert_eq!(ack.error_message, "because");
    assert_eq!(ack.signal_engine_node, "node-7");
    assert_eq!(ack.instance_id, "inst");
    assert_eq!(ack.strategy_id, "strat");
    assert!(ack.active_exchanges.is_empty());
}

// ---- reject_live_deployment (fake publisher + real map) -------------------

#[tokio::test]
async fn removes_from_map_and_sends_exactly_one_failure_ack() {
    let map: DashMap<Uuid, Arc<DeployedStrategy>> = DashMap::new();
    let id = Uuid::new_v4();
    let other = Uuid::new_v4();
    let entry = deployed(id, "live");
    map.insert(id, entry.clone());
    map.insert(other, deployed(other, "live"));
    let sink = Arc::new(RecordingSink::default());
    let sender = AckSender::with_sink(sink.clone(), "node-1");

    let out = reject_live_deployment(&map, Some(&sender), None, "sid", id, &reason_no_provider()).await;

    assert!(out.removed_from_map && out.ack_published);
    assert!(!map.contains_key(&id), "rejected instance must leave the deployed set");
    assert!(map.contains_key(&other), "other deployments must be untouched");
    assert!(!entry.is_active.load(Ordering::SeqCst));
    let acks = sink.acks.lock().unwrap();
    assert_eq!(acks.len(), 1, "exactly one ack");
    assert!(!acks[0].success);
    assert_eq!(acks[0].instance_id, id.to_string());
    assert_eq!(acks[0].strategy_id, "sid");
    assert_eq!(acks[0].error_message, reason_no_provider());
}

#[tokio::test]
async fn ack_failure_does_not_panic_and_rejection_stands() {
    let map: DashMap<Uuid, Arc<DeployedStrategy>> = DashMap::new();
    let id = Uuid::new_v4();
    map.insert(id, deployed(id, "live"));
    let sink = Arc::new(RecordingSink { fail: true, ..Default::default() });
    let sender = AckSender::with_sink(sink, "n");
    let out = reject_live_deployment(&map, Some(&sender), None, "s", id, "nope").await;
    assert!(out.removed_from_map);
    assert!(!out.ack_published);
    assert!(out.errors.iter().any(|e| e.contains("broker down")), "{:?}", out.errors);
    assert!(!map.contains_key(&id));
}

#[tokio::test]
async fn missing_publisher_is_reported_not_silent() {
    let map: DashMap<Uuid, Arc<DeployedStrategy>> = DashMap::new();
    let id = Uuid::new_v4();
    map.insert(id, deployed(id, "live"));
    let out = reject_live_deployment(&map, None, None, "s", id, "nope").await;
    assert!(out.removed_from_map && !out.ack_published);
    assert!(out.errors.iter().any(|e| e.contains("no broker publisher")), "{:?}", out.errors);
}

#[tokio::test]
async fn unknown_instance_is_harmless_and_still_acked() {
    let map: DashMap<Uuid, Arc<DeployedStrategy>> = DashMap::new();
    let sink = Arc::new(RecordingSink::default());
    let sender = AckSender::with_sink(sink.clone(), "n");
    let out = reject_live_deployment(&map, Some(&sender), None, "s", Uuid::new_v4(), "nope").await;
    assert!(!out.removed_from_map);
    assert!(out.ack_published, "still tells the Engine");
    assert_eq!(sink.acks.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn reason_is_sanitized_again_inside_the_helper() {
    let map: DashMap<Uuid, Arc<DeployedStrategy>> = DashMap::new();
    let sink = Arc::new(RecordingSink::default());
    let sender = AckSender::with_sink(sink.clone(), "n");
    let secret: &str = &["sk_liv", "e_51H8", "xYzABC", "DEFGHI", "JKLMNO", "PQRSTU", "VWXYZ0", "123456", "789"].concat();
    reject_live_deployment(
        &map,
        Some(&sender),
        None,
        "s",
        Uuid::new_v4(),
        &format!("failed with {}", secret),
    )
    .await;
    let msg = sink.acks.lock().unwrap()[0].error_message.clone();
    assert!(!msg.contains("sk_live_51H8"), "{}", msg);
}

// ---- the SQL against a REAL Postgres ---------------------------------------
//
// Needs LIVEFAIL_TEST_DATABASE_URL pointing at a scratch database that has the
// public databaseschema migrations applied. Without it these tests print
// SKIPPED and return (so the suite still builds in CI without a DB).
#[cfg(feature = "postgres")]
pub(crate) mod db {
    use super::*;
    use diesel::sql_types::Text;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    #[derive(diesel::QueryableByName)]
    struct J {
        #[diesel(sql_type = Text)]
        j: String,
    }

    pub(crate) fn url() -> Option<String> {
        match std::env::var("LIVEFAIL_TEST_DATABASE_URL") {
            Ok(u) if !u.is_empty() => Some(u),
            _ => {
                eprintln!("SKIPPED: LIVEFAIL_TEST_DATABASE_URL not set");
                None
            }
        }
    }

    /// A scratch database whose `deployed_strategies` ALSO has a nullable
    /// `tenant_id uuid` column (the private-schema shape).
    pub(crate) fn tenant_url() -> Option<String> {
        match std::env::var("FOLLOWUP_TENANT_DATABASE_URL") {
            Ok(u) if !u.is_empty() => Some(u),
            _ => {
                eprintln!("SKIPPED: FOLLOWUP_TENANT_DATABASE_URL not set");
                None
            }
        }
    }

    /// Insert a backtest_results row plus a deployed_strategies row.
    pub(crate) async fn insert_deployment(url: &str, mode: &str, metadata: Option<&str>) -> Uuid {
        insert_deployment_tenant(url, mode, metadata, None).await
    }

    /// Like [`insert_deployment`]; `tenant` = `Some(t)` also writes the
    /// `tenant_id` column (`Some(None)` writes NULL). It only works against a
    /// database whose `deployed_strategies` HAS that column.
    pub(crate) async fn insert_deployment_tenant(
        url: &str,
        mode: &str,
        metadata: Option<&str>,
        tenant: Option<Option<Uuid>>,
    ) -> Uuid {
        let mut c = AsyncPgConnection::establish(url).await.unwrap();
        let bt = Uuid::new_v4();
        let dep = Uuid::new_v4();
        diesel::sql_query(format!(
            "INSERT INTO backtest_results (id, backtest_id, strategy_name, symbol, start_date, end_date, \
             initial_capital, commission_rate, slippage_model_type) \
             VALUES ('{bt}', '{bt}', 'lf', 'BTC-USD', now(), now(), 1000, 0.001, 'fixed')"
        ))
        .execute(&mut c)
        .await
        .unwrap();
        let md = match metadata {
            Some(m) => format!("'{}'::jsonb", m),
            None => "NULL".to_string(),
        };
        let (tcol, tval) = match tenant {
            None => (String::new(), String::new()),
            Some(None) => (", tenant_id".to_string(), ", NULL".to_string()),
            Some(Some(t)) => (", tenant_id".to_string(), format!(", '{t}'")),
        };
        diesel::sql_query(format!(
            "INSERT INTO deployed_strategies (id, backtest_result_id, name, capital_allocation, mode, metadata{tcol}) \
             VALUES ('{dep}', '{bt}', 'lf', 1000, '{mode}', {md}{tval})"
        ))
        .execute(&mut c)
        .await
        .unwrap();
        dep
    }

    pub(crate) async fn row_json(url: &str, id: Uuid) -> serde_json::Value {
        let mut c = AsyncPgConnection::establish(url).await.unwrap();
        let rows: Vec<J> = diesel::sql_query(format!(
            "SELECT row_to_json(d)::text AS j FROM deployed_strategies d WHERE id = '{id}'"
        ))
        .load(&mut c)
        .await
        .unwrap();
        serde_json::from_str(&rows[0].j).unwrap()
    }

    #[tokio::test]
    async fn live_row_is_stopped_with_reason_in_metadata() {
        let Some(url) = url() else { return };
        let id = insert_deployment(&url, "live", Some(r#"{"keep":"me"}"#)).await;
        let n = mark_live_rejected_in_db(&url, id, "no provider").await.unwrap();
        assert_eq!(n, 1);
        let r = row_json(&url, id).await;
        assert_eq!(r["is_active"], false);
        assert_eq!(r["status"], "stopped");
        assert!(r["stopped_at"].is_string());
        let md = &r["metadata"];
        assert_eq!(md["keep"], "me", "existing metadata must be merged, not replaced");
        assert_eq!(md["signal_engine_ack"], false);
        assert_eq!(md["signal_engine_ack_error"], "no provider");
        assert_eq!(md["live_rejected"], true);
        let at = md["live_rejected_at"].as_str().unwrap();
        assert!(at.ends_with('Z') && at.contains('T') && at.len() == 20, "{}", at);
    }

    #[tokio::test]
    async fn null_metadata_is_handled() {
        let Some(url) = url() else { return };
        let id = insert_deployment(&url, "live", None).await;
        assert_eq!(mark_live_rejected_in_db(&url, id, "r").await.unwrap(), 1);
        assert_eq!(row_json(&url, id).await["metadata"]["live_rejected"], true);
    }

    #[tokio::test]
    async fn paper_row_with_same_shape_is_untouched() {
        let Some(url) = url() else { return };
        let id = insert_deployment(&url, "paper", Some(r#"{"a":1}"#)).await;
        let before = row_json(&url, id).await;
        let n = mark_live_rejected_in_db(&url, id, "r").await.unwrap();
        assert_eq!(n, 0, "a paper row must not match");
        let after = row_json(&url, id).await;
        assert_eq!(before, after);
        assert_eq!(after["is_active"], true);
        assert_eq!(after["status"], "active");
    }

    #[tokio::test]
    async fn a_different_deployment_id_is_untouched() {
        let Some(url) = url() else { return };
        let target = insert_deployment(&url, "live", None).await;
        let bystander = insert_deployment(&url, "live", Some(r#"{"b":2}"#)).await;
        let before = row_json(&url, bystander).await;
        assert_eq!(mark_live_rejected_in_db(&url, target, "r").await.unwrap(), 1);
        assert_eq!(before, row_json(&url, bystander).await);
    }

    #[tokio::test]
    async fn full_rejection_map_ack_and_db_together() {
        let Some(url) = url() else { return };
        let id = insert_deployment(&url, "live", None).await;
        let map: DashMap<Uuid, Arc<DeployedStrategy>> = DashMap::new();
        map.insert(id, deployed(id, "live"));
        let sink = Arc::new(RecordingSink::default());
        let sender = AckSender::with_sink(sink.clone(), "node");

        let out =
            reject_live_deployment(&map, Some(&sender), Some(&url), "sid", id, &reason_no_provider()).await;

        assert!(out.errors.is_empty(), "{:?}", out.errors);
        assert!(out.removed_from_map && out.ack_published);
        assert_eq!(out.db_rows_updated, Some(1));
        assert!(map.is_empty());
        assert_eq!(sink.acks.lock().unwrap().len(), 1);
        let r = row_json(&url, id).await;
        assert_eq!(r["status"], "stopped");
        assert_eq!(r["metadata"]["signal_engine_ack_error"], reason_no_provider());
    }

    #[tokio::test]
    async fn db_failure_does_not_panic_and_rejection_stands() {
        // Port 1 refuses connections immediately.
        let bad = "postgres://postgres@127.0.0.1:1/nope";
        let map: DashMap<Uuid, Arc<DeployedStrategy>> = DashMap::new();
        let id = Uuid::new_v4();
        map.insert(id, deployed(id, "live"));
        let sink = Arc::new(RecordingSink::default());
        let sender = AckSender::with_sink(sink.clone(), "node");
        let out = reject_live_deployment(&map, Some(&sender), Some(bad), "s", id, "r").await;
        assert!(out.removed_from_map, "rejection stands");
        assert!(out.ack_published, "ack still goes out");
        assert_eq!(out.db_rows_updated, None);
        assert!(out.errors.iter().any(|e| e.contains("database")), "{:?}", out.errors);
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn missing_database_url_is_reported() {
        let map: DashMap<Uuid, Arc<DeployedStrategy>> = DashMap::new();
        let out = reject_live_deployment(&map, None, None, "s", Uuid::new_v4(), "r").await;
        assert!(
            out.errors.iter().any(|e| e.contains("DATABASE_URL not set")),
            "{:?}",
            out.errors
        );
    }
}

// ---- unsubscribe of leaked market-data subscriptions ------------------------

fn deployed_subscribed(
    instance_id: Uuid,
    mode: &str,
    exchanges: &[&str],
    symbols: &[&str],
    subscribed: &[&str],
) -> Arc<DeployedStrategy> {
    let mut msg = deployment_msg(instance_id, mode);
    msg.target_exchanges = exchanges.iter().map(|s| s.to_string()).collect();
    msg.symbols = symbols.iter().map(|s| s.to_string()).collect();
    let d = DeployedStrategy::from_deployment(&msg, Default::default()).unwrap();
    *d.subscribed_exchanges.lock() = subscribed.iter().map(|s| s.to_string()).collect();
    Arc::new(d)
}

fn setup(entry: Arc<DeployedStrategy>) -> (DashMap<Uuid, Arc<DeployedStrategy>>, Arc<RecordingSink>, AckSender) {
    let map: DashMap<Uuid, Arc<DeployedStrategy>> = DashMap::new();
    map.insert(entry.instance_id, entry);
    let sink = Arc::new(RecordingSink::default());
    let sender = AckSender::with_sink(sink.clone(), "node");
    (map, sink, sender)
}

#[tokio::test]
async fn one_unsubscribe_per_subscribed_exchange_with_the_subscribe_ids() {
    let id = Uuid::new_v4();
    let (map, sink, sender) = setup(deployed_subscribed(
        id, "live", &["kraken", "binance"], &["BTC-USD", "ETH-USD"], &["kraken", "binance"],
    ));
    let out = reject_live_deployment(&map, Some(&sender), None, "s", id, "nope").await;
    assert_eq!(out.unsubscribes_published, 2);
    let unsubs = sink.unsubs.lock().unwrap();
    assert_eq!(unsubs.len(), 2, "exactly one per exchange");
    for (u, ex) in unsubs.iter().zip(["kraken", "binance"]) {
        assert_eq!(u.subscription_id, format!("{}_{}", id, ex), "same id handle_deployment subscribed with");
        assert_eq!(u.exchange, ex);
        assert_eq!(u.strategy_instance_id, id.to_string());
        assert_eq!(u.symbols, vec!["BTC-USD".to_string(), "ETH-USD".to_string()]);
        assert!(!u.reason.is_empty());
    }
    assert_eq!(sink.acks.lock().unwrap().len(), 1, "the failure ack still goes out once");
}

#[tokio::test]
async fn only_exchanges_actually_subscribed_are_unsubscribed() {
    let id = Uuid::new_v4();
    let (map, sink, sender) =
        setup(deployed_subscribed(id, "live", &["kraken", "binance"], &["BTC-USD"], &["kraken"]));
    reject_live_deployment(&map, Some(&sender), None, "s", id, "nope").await;
    let unsubs = sink.unsubs.lock().unwrap();
    assert_eq!(unsubs.len(), 1);
    assert_eq!(unsubs[0].exchange, "kraken");
}

#[tokio::test]
async fn no_unsubscribe_when_nothing_was_subscribed() {
    let id = Uuid::new_v4();
    let (map, sink, sender) =
        setup(deployed_subscribed(id, "live", &["kraken"], &["BTC-USD"], &[]));
    let out = reject_live_deployment(&map, Some(&sender), None, "s", id, "nope").await;
    assert_eq!(out.unsubscribes_published, 0);
    assert!(sink.unsubs.lock().unwrap().is_empty());
    assert!(out.removed_from_map);
}

#[tokio::test]
async fn no_unsubscribe_for_a_paper_entry() {
    let id = Uuid::new_v4();
    let (map, sink, sender) =
        setup(deployed_subscribed(id, "paper", &["kraken"], &["BTC-USD"], &["kraken"]));
    reject_live_deployment(&map, Some(&sender), None, "s", id, "nope").await;
    assert!(sink.unsubs.lock().unwrap().is_empty(), "the live-only path must not touch a paper subscription");
}

#[tokio::test]
async fn second_rejection_publishes_no_further_unsubscribes() {
    let id = Uuid::new_v4();
    let entry = deployed_subscribed(id, "live", &["kraken"], &["BTC-USD"], &["kraken"]);
    let (map, sink, sender) = setup(entry.clone());
    reject_live_deployment(&map, Some(&sender), None, "s", id, "nope").await;
    reject_live_deployment(&map, Some(&sender), None, "s", id, "nope again").await;
    assert_eq!(sink.unsubs.lock().unwrap().len(), 1, "unsubscribe happens once per deployment");
    // Even if the same entry were re-inserted (a second holder), the drained list stays empty.
    map.insert(id, entry);
    reject_live_deployment(&map, Some(&sender), None, "s", id, "third").await;
    assert_eq!(sink.unsubs.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn unsubscribe_failure_is_reported_but_rejection_stands() {
    let id = Uuid::new_v4();
    let (map, sink_ok, _) = setup(deployed_subscribed(id, "live", &["kraken"], &["BTC-USD"], &["kraken"]));
    drop(sink_ok);
    let sink = Arc::new(RecordingSink { fail_unsub: true, ..Default::default() });
    let sender = AckSender::with_sink(sink.clone(), "node");
    let out = reject_live_deployment(&map, Some(&sender), None, "s", id, "nope").await;
    assert_eq!(out.unsubscribes_published, 0);
    assert!(out.removed_from_map && out.ack_published, "rejection and failure ack still stand");
    assert!(out.errors.iter().any(|e| e.contains("unsubscribe") && e.contains("DataEngine")), "{:?}", out.errors);
    assert!(map.is_empty());
}

#[tokio::test]
async fn missing_publisher_reports_the_leaked_subscription() {
    let id = Uuid::new_v4();
    let (map, _sink, _sender) = setup(deployed_subscribed(id, "live", &["kraken"], &["BTC-USD"], &["kraken"]));
    let out = reject_live_deployment(&map, None, None, "s", id, "nope").await;
    assert!(out.errors.iter().any(|e| e.contains("unsubscribe") && e.contains("no broker publisher")), "{:?}", out.errors);
}

// ---- wire format ------------------------------------------------------------

#[test]
fn unsubscribe_wire_bytes_decode_the_way_dataengine_reads_them() {
    // DataEngine (SubscriptionManager): PublishRequest::decode, then act only on
    // `RawData` payloads on topic market.subscription.unsubscribe.
    let id = Uuid::new_v4();
    let unsubs = build_unsubscribes(id, &["BTC-USD".to_string()], &["kraken".to_string()], "why");
    let bytes = unsubscribe_wire_bytes(&unsubs[0]);
    let req = PublishRequest::decode(bytes.as_slice()).unwrap();
    assert_eq!(req.topic, "market.subscription.unsubscribe");
    let Some(publish_request::Payload::RawData(data)) = req.payload else { panic!("not RawData") };
    let back = MarketDataUnsubscribe::decode(data.as_slice()).unwrap();
    assert_eq!(back.subscription_id, format!("{}_kraken", id));
    assert_eq!(back.strategy_instance_id, id.to_string());
    assert_eq!(back.exchange, "kraken");
    assert_eq!(back.symbols, vec!["BTC-USD".to_string()]);
}

/// Evidence for a suspected pre-existing bug (NOT changed here): the
/// deactivation path publishes the bare `MarketDataUnsubscribe` bytes with no
/// `PublishRequest` envelope. Decoded the way DataEngine decodes, that is not a
/// `RawData` payload, so DataEngine would ignore it.
#[test]
fn bare_unsubscribe_bytes_are_not_a_rawdata_publish_request() {
    let id = Uuid::new_v4();
    let u = build_unsubscribes(id, &["BTC-USD".to_string()], &["kraken".to_string()], "why").remove(0);
    let bare = u.encode_to_vec();
    let is_rawdata = matches!(
        PublishRequest::decode(bare.as_slice()).map(|r| r.payload),
        Ok(Some(publish_request::Payload::RawData(_)))
    );
    assert!(!is_rawdata, "bare bytes must not look like the enveloped form");
}
