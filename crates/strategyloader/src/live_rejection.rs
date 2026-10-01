//! Loud, correct rejection of a live deployment.
//!
//! `DeploymentSubscriber::handle_deployment` publishes a SUCCESS
//! `StrategyDeploymentAck`, records the deployment in the shared
//! `deployed_strategies` map and forwards `DeploymentEvent::Deploy` to the
//! host BEFORE the host has checked that a live deployment can obtain an
//! exchange credential. When that check fails the host must therefore UNDO the
//! optimistic success, not merely `continue`:
//!
//! 1. remove the instance from the shared map (SignalEngine must stop listing
//!    it as deployed),
//! 2. publish a FAILURE ack on `strategy.deployment.ack`,
//! 3. (feature `postgres`) mark the shared `deployed_strategies` row stopped
//!    with the reason in `metadata`, because the Engine does not consume acks
//!    but does read that row and it is what the UI shows.
//!
//! 4. unsubscribe the market data `handle_deployment` subscribed for it (one
//!    `MarketDataUnsubscribe` per subscribed exchange, same subscription ids).
//!    DataEngine keeps a subscription until it is unsubscribed, and it counts
//!    against the tenant's tier limits, so a rejected deployment must not
//!    leave one behind.
//!
//! [`reject_live_deployment`] does all of these, never panics, never blocks for
//! more than a bounded time, and reports what it managed to do. A failure of
//! step 2 or 3 is logged at ERROR and the rejection still stands.

use crate::deployment_subscriber::{topics, DeployedStrategy};
use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use prost::Message;
use protocol::broker::messages::{
    publish_request, MarketDataUnsubscribe, PublishRequest, StrategyDeploymentAck,
};
use publisher::UltraFastPublisher;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use ultra_logger::{ultra_error, ultra_warn};
use uuid::Uuid;

/// The subscriber's shared deployed-set (DeploymentSubscriber::get_deployed_strategies).
/// A named alias so callers linking a different dashmap version still get the right type.
pub type DeployedSet = Arc<DashMap<Uuid, Arc<DeployedStrategy>>>;

/// Upper bound for each of the two external effects (ack publish, DB write).
pub const REJECTION_IO_TIMEOUT: Duration = Duration::from_secs(5);
/// Longest reason text that is ever published or stored.
pub const MAX_REASON_LEN: usize = 300;

// ---------------------------------------------------------------------------
// Reason text
// ---------------------------------------------------------------------------

/// Plain, secret-free reason for a live deployment that has no credential
/// provider at all (credential mode `none`).
pub fn reason_no_provider() -> String {
    sanitize_reason(
        "live trading is disabled on this SignalEngine: no exchange credential provider is \
         configured (credential mode: none). Deploy in paper mode.",
    )
}

/// Plain, secret-free reason for a venue whose credential could not be loaded.
/// Deliberately does NOT embed the underlying error text: that is logged
/// locally, and may name connector internals.
pub fn reason_credential_unavailable(venue: &str) -> String {
    sanitize_reason(&format!(
        "no usable production credential for exchange '{}' on this deployment's tenant \
         (missing, disabled, testnet-only, a tenant this SignalEngine does not serve, or a \
         connector owned by another tenant). Add production API keys or deploy in paper mode.",
        venue
    ))
}

/// Make `raw` safe to publish and store: control characters become spaces,
/// whitespace is collapsed, long opaque tokens (>= 32 chars of key-ish
/// characters, excluding UUIDs) are redacted, and the result is truncated to
/// [`MAX_REASON_LEN`] characters.
pub fn sanitize_reason(raw: &str) -> String {
    let cleaned: String = raw.chars().map(|c| if c.is_control() { ' ' } else { c }).collect();
    let mut out: Vec<String> = Vec::new();
    for tok in cleaned.split_whitespace() {
        out.push(redact_token(tok));
    }
    let mut s = out.join(" ");
    if s.chars().count() > MAX_REASON_LEN {
        s = s.chars().take(MAX_REASON_LEN - 3).collect::<String>() + "...";
    }
    s
}

fn redact_token(tok: &str) -> String {
    // Trim wrapping punctuation before judging, keep it in the output.
    let core = tok.trim_matches(|c: char| !c.is_ascii_alphanumeric());
    let keyish = core.len() >= 32
        && core
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '+' | '/' | '='));
    if keyish && Uuid::parse_str(core).is_err() {
        tok.replace(core, "[redacted]")
    } else {
        tok.to_string()
    }
}

// ---------------------------------------------------------------------------
// Ack channel
// ---------------------------------------------------------------------------

/// Where a deployment ack goes. The production impl wraps the broker publisher;
/// tests use a recording fake.
#[async_trait]
pub trait AckSink: Send + Sync {
    async fn publish_ack(&self, ack: StrategyDeploymentAck) -> Result<(), String>;
    /// Publish a market-data unsubscribe (topic `market.subscription.unsubscribe`).
    async fn publish_unsubscribe(&self, unsub: MarketDataUnsubscribe) -> Result<(), String>;
}

/// Wire bytes DataEngine's `SubscriptionManager` accepts for an unsubscribe:
/// a `PublishRequest` on `market.subscription.unsubscribe` carrying the
/// `MarketDataUnsubscribe` bytes as `RawData` -- the same envelope
/// `handle_deployment` uses for `MarketDataSubscribe`. (DataEngine decodes
/// `PublishRequest` first and only acts on its `RawData` payload.)
pub fn unsubscribe_wire_bytes(unsub: &MarketDataUnsubscribe) -> Vec<u8> {
    PublishRequest {
        topic: topics::MARKET_DATA_UNSUBSCRIBE.to_string(),
        payload: Some(publish_request::Payload::RawData(unsub.encode_to_vec())),
    }
    .encode_to_vec()
}

struct BrokerAckSink(Arc<UltraFastPublisher>);

#[async_trait]
impl AckSink for BrokerAckSink {
    async fn publish_ack(&self, ack: StrategyDeploymentAck) -> Result<(), String> {
        // Same envelope/topic as the subscriber's own success ack.
        let request = PublishRequest {
            topic: topics::STRATEGY_DEPLOYMENT_ACK.to_string(),
            payload: Some(publish_request::Payload::StrategyDeploymentAck(ack)),
        };
        self.0
            .publish_raw(request.encode_to_vec(), topics::STRATEGY_DEPLOYMENT_ACK)
            .await
            .map_err(|e| format!("publish: {:?}", e))?;
        self.0.flush().await.map_err(|e| format!("flush: {:?}", e))?;
        Ok(())
    }

    async fn publish_unsubscribe(&self, unsub: MarketDataUnsubscribe) -> Result<(), String> {
        self.0
            .publish_raw(unsubscribe_wire_bytes(&unsub), topics::MARKET_DATA_UNSUBSCRIBE)
            .await
            .map_err(|e| format!("publish: {:?}", e))?;
        self.0.flush().await.map_err(|e| format!("flush: {:?}", e))?;
        Ok(())
    }
}

/// Small cloneable handle for sending deployment acks with this node's id,
/// obtained from `DeploymentSubscriber::ack_sender()`.
#[derive(Clone)]
pub struct AckSender {
    sink: Arc<dyn AckSink>,
    node_id: String,
}

impl AckSender {
    /// Wrap the subscriber's real publisher.
    pub fn from_publisher(publisher: Arc<UltraFastPublisher>, node_id: &str) -> Self {
        Self { sink: Arc::new(BrokerAckSink(publisher)), node_id: node_id.to_string() }
    }

    /// Any sink (used by tests).
    pub fn with_sink(sink: Arc<dyn AckSink>, node_id: &str) -> Self {
        Self { sink, node_id: node_id.to_string() }
    }

    /// The failure ack this sender would publish. `reason` must already be safe.
    pub fn failure_ack(&self, strategy_id: &str, instance_id: &str, reason: &str) -> StrategyDeploymentAck {
        StrategyDeploymentAck {
            strategy_id: strategy_id.to_string(),
            instance_id: instance_id.to_string(),
            signal_engine_node: self.node_id.clone(),
            success: false,
            error_message: reason.to_string(),
            loaded_at: Utc::now().timestamp_millis(),
            active_exchanges: Vec::new(),
        }
    }

    /// Publish a market-data unsubscribe (bounded by [`REJECTION_IO_TIMEOUT`]).
    pub async fn send_unsubscribe(&self, unsub: MarketDataUnsubscribe) -> Result<(), String> {
        match tokio::time::timeout(REJECTION_IO_TIMEOUT, self.sink.publish_unsubscribe(unsub)).await {
            Ok(r) => r,
            Err(_) => Err(format!("unsubscribe publish timed out after {:?}", REJECTION_IO_TIMEOUT)),
        }
    }

    /// Publish a failure ack (bounded by [`REJECTION_IO_TIMEOUT`]).
    pub async fn send_failure(&self, strategy_id: &str, instance_id: &str, reason: &str) -> Result<(), String> {
        let ack = self.failure_ack(strategy_id, instance_id, reason);
        match tokio::time::timeout(REJECTION_IO_TIMEOUT, self.sink.publish_ack(ack)).await {
            Ok(r) => r,
            Err(_) => Err(format!("ack publish timed out after {:?}", REJECTION_IO_TIMEOUT)),
        }
    }
}

// ---------------------------------------------------------------------------
// Database record
// ---------------------------------------------------------------------------

/// The statement that makes the rejection visible to the Engine UI. `$1` is the
/// deployment id, `$2` the (safe) reason. It only ever touches a LIVE row.
pub const MARK_LIVE_REJECTED_SQL: &str = "UPDATE deployed_strategies SET \
    is_active = false, status = 'stopped', stopped_at = now(), updated_at = now(), \
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(\
        'signal_engine_ack', false, \
        'signal_engine_ack_error', $2::text, \
        'live_rejected', true, \
        'live_rejected_at', to_char(now() at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')) \
    WHERE id = $1 AND mode = 'live'";

/// Run [`MARK_LIVE_REJECTED_SQL`] against `database_url`. Returns rows updated.
#[cfg(feature = "postgres")]
pub async fn mark_live_rejected_in_db(
    database_url: &str,
    instance_id: Uuid,
    reason: &str,
) -> Result<usize, String> {
    use diesel::sql_types::{Text, Uuid as SqlUuid};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    let work = async {
        let mut conn = AsyncPgConnection::establish(database_url)
            .await
            .map_err(|e| format!("db connect: {}", e))?;
        diesel::sql_query(MARK_LIVE_REJECTED_SQL)
            .bind::<SqlUuid, _>(instance_id)
            .bind::<Text, _>(reason.to_string())
            .execute(&mut conn)
            .await
            .map_err(|e| format!("db update: {}", e))
    };
    match tokio::time::timeout(REJECTION_IO_TIMEOUT, work).await {
        Ok(r) => r,
        Err(_) => Err(format!("db update timed out after {:?}", REJECTION_IO_TIMEOUT)),
    }
}

// ---------------------------------------------------------------------------
// The helper
// ---------------------------------------------------------------------------

/// One `MarketDataUnsubscribe` per subscribed exchange, mirroring exactly what
/// `handle_deployment` subscribed: subscription id `{instance_id}_{exchange}`,
/// the deployment's symbols and strategy instance id. (The message has no
/// tenant field; DataEngine reads the tenant from the stored subscription.)
pub fn build_unsubscribes(
    instance_id: Uuid,
    symbols: &[String],
    subscribed_exchanges: &[String],
    reason: &str,
) -> Vec<MarketDataUnsubscribe> {
    subscribed_exchanges
        .iter()
        .map(|exchange| MarketDataUnsubscribe {
            subscription_id: format!("{}_{}", instance_id, exchange),
            strategy_instance_id: instance_id.to_string(),
            exchange: exchange.clone(),
            symbols: symbols.to_vec(),
            reason: reason.to_string(),
            timestamp: Utc::now().timestamp_millis(),
        })
        .collect()
}

/// What [`reject_live_deployment`] managed to do (for logs and tests).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LiveRejectionOutcome {
    /// The instance was present in the shared map and has been removed.
    pub removed_from_map: bool,
    /// A failure ack was published.
    pub ack_published: bool,
    /// Market-data unsubscribes published (one per subscribed exchange).
    pub unsubscribes_published: usize,
    /// Rows changed in `deployed_strategies` (None: no DB write attempted or it failed).
    pub db_rows_updated: Option<usize>,
    /// One line per external effect that failed (already logged at ERROR).
    pub errors: Vec<String>,
}

/// Reject a live deployment loudly. See the module docs.
///
/// * `deployed` - the subscriber's shared map (`DeploymentSubscriber::get_deployed_strategies`)
/// * `ack` - failure ack channel; `None` means "no broker connection" (logged)
/// * `database_url` - shared DB; `None` means no DB is configured (logged)
/// * `strategy_id` - wire `strategy_id` string of the deployment
/// * `reason` - reason text; it is passed through [`sanitize_reason`] again
///   here, so a careless caller cannot leak a token.
pub async fn reject_live_deployment(
    deployed: &DashMap<Uuid, Arc<DeployedStrategy>>,
    ack: Option<&AckSender>,
    database_url: Option<&str>,
    strategy_id: &str,
    instance_id: Uuid,
    reason: &str,
) -> LiveRejectionOutcome {
    let reason = sanitize_reason(reason);
    let mut out = LiveRejectionOutcome::default();

    // (a) SignalEngine must stop reporting it as deployed.
    // Draining `subscribed_exchanges` makes the unsubscribe below happen at most
    // once per deployment: a second rejection finds no entry (and even a second
    // holder of the same Arc finds the list empty).
    let mut unsubscribes: Vec<MarketDataUnsubscribe> = Vec::new();
    if let Some((_, entry)) = deployed.remove(&instance_id) {
        entry.is_active.store(false, Ordering::SeqCst);
        out.removed_from_map = true;
        if entry.mode == "live" {
            let exchanges = std::mem::take(&mut *entry.subscribed_exchanges.lock());
            unsubscribes = build_unsubscribes(instance_id, &entry.symbols, &exchanges, "live deployment rejected");
        }
    }

    // (a2) DataEngine must stop streaming (and stop counting it against the
    // tenant's tier limits) for what handle_deployment subscribed.
    for unsub in unsubscribes {
        let exchange = unsub.exchange.clone();
        match ack {
            Some(sender) => match sender.send_unsubscribe(unsub).await {
                Ok(()) => out.unsubscribes_published += 1,
                Err(e) => {
                    let msg = format!(
                        "market-data unsubscribe for {} on {} not published: {} (DataEngine may keep streaming)",
                        instance_id, exchange, e
                    );
                    ultra_error!(format!("❌ {}", msg));
                    out.errors.push(msg);
                }
            },
            None => {
                let msg = format!(
                    "market-data unsubscribe for {} on {} not published: no broker publisher \
                     (DataEngine may keep streaming)",
                    instance_id, exchange
                );
                ultra_error!(format!("❌ {}", msg));
                out.errors.push(msg);
            }
        }
    }

    // (b) tell the broker the truth.
    match ack {
        Some(sender) => match sender.send_failure(strategy_id, &instance_id.to_string(), &reason).await {
            Ok(()) => out.ack_published = true,
            Err(e) => {
                let msg = format!("failure ack for {} not published: {}", instance_id, e);
                ultra_error!(format!("❌ {}", msg));
                out.errors.push(msg);
            }
        },
        None => {
            let msg = format!(
                "failure ack for {} not published: no broker publisher (the Engine was NOT told; \
                 the DB row is the only record)",
                instance_id
            );
            ultra_error!(format!("❌ {}", msg));
            out.errors.push(msg);
        }
    }

    // (c) make it visible where the Engine UI looks.
    #[cfg(feature = "postgres")]
    match database_url {
        Some(url) if !url.trim().is_empty() => match mark_live_rejected_in_db(url, instance_id, &reason).await {
            Ok(n) => {
                out.db_rows_updated = Some(n);
                if n == 0 {
                    ultra_warn!(format!(
                        "⚠️ live rejection of {}: no live deployed_strategies row matched (nothing to mark stopped)",
                        instance_id
                    ));
                }
            }
            Err(e) => {
                let msg = format!("could not record live rejection of {} in the database: {}", instance_id, e);
                ultra_error!(format!("❌ {}", msg));
                out.errors.push(msg);
            }
        },
        _ => {
            let msg = format!(
                "could not record live rejection of {} in the database: DATABASE_URL not set",
                instance_id
            );
            ultra_error!(format!("❌ {}", msg));
            out.errors.push(msg);
        }
    }
    #[cfg(not(feature = "postgres"))]
    {
        let _ = database_url;
        ultra_warn!(format!(
            "⚠️ live rejection of {}: built without the postgres feature, DB row not updated",
            instance_id
        ));
    }

    ultra_error!(format!(
        "🚫 LIVE DEPLOYMENT REJECTED {}: {} (removed from deployed set: {}, failure ack: {}, unsubscribes: {}, db rows updated: {:?})",
        instance_id, reason, out.removed_from_map, out.ack_published, out.unsubscribes_published, out.db_rows_updated
    ));
    out
}

#[cfg(test)]
pub(crate) mod tests;
