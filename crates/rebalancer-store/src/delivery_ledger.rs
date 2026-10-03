//! `PgDeliveryLedger`: the Postgres-backed `rebalancer_alerts::DeliveryLedger` (W6.1), over `rebalancer_alerts`
//! (read), `alert_channels` (read) and `alert_deliveries` (insert-if-absent, then one update per row).
//!
//! The two new tables are a contract with `databaseschema-internal` (`feat/w6-1-alert-channels`), written here as
//! raw SQL in the same style as every other store in this crate; nothing is renamed and no column is invented. The
//! service role needs `SELECT` on `rebalancer_alerts` (it already has it) and `alert_channels`, and `SELECT, INSERT,
//! UPDATE` on `alert_deliveries`. Every failure, including the tables not existing yet, is `LedgerError` (printed as
//! `ALERT_STORE_UNAVAILABLE: ...`) -- fail closed, never an empty success -- and the worker reports it on every tick
//! until it is fixed, which is the honest state of "alerts are enabled but cannot be delivered".
//!
//! `tenant_id` of a delivery is the CHANNEL's (null for a platform channel); the alert's own tenant id (the
//! `rebalancer_alerts` row, denormalised there from `AccountTenants`) is what the worker matches tenant channels
//! against. The nil tenant (an alert about the platform itself, or an account the registry did not know) matches no
//! tenant channel and is surfaced to the worker as `None`.

use chrono::{DateTime, Utc};
use diesel::sql_types::{BigInt, Nullable, Text, Timestamptz};
use diesel_async::RunQueryDsl;
use rebalancer_alerts::ledger::{Channel, ChannelScope, DeliveryLedger, LedgerError, NewDelivery, PendingAlert, PendingDelivery, SOURCE_REBALANCER};
use uuid::Uuid;

use crate::pg::{Bridge, Pool};
use crate::tenants::UNKNOWN_TENANT;

pub struct PgDeliveryLedger {
    bridge: Bridge,
}

impl PgDeliveryLedger {
    pub fn new(pool: Pool) -> Result<Self, String> {
        Ok(Self { bridge: Bridge::new(pool)? })
    }
}

fn unavailable(e: impl std::fmt::Display) -> LedgerError {
    LedgerError(e.to_string())
}

fn tenant_of(text: Option<String>) -> Option<String> {
    let t = text?;
    match Uuid::parse_str(t.trim()) {
        Ok(u) if u != UNKNOWN_TENANT => Some(u.hyphenated().to_string()),
        _ => None,
    }
}

fn uuid_param(id: &str, what: &str) -> Result<Uuid, LedgerError> {
    Uuid::parse_str(id.trim()).map_err(|e| LedgerError(format!("{what} {id:?} is not a UUID: {e}")))
}

#[derive(diesel::QueryableByName)]
struct AlertRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Nullable<Text>)]
    tenant_id: Option<String>,
    #[diesel(sql_type = Text)]
    account_id: String,
    #[diesel(sql_type = Text)]
    code: String,
    #[diesel(sql_type = Text)]
    severity: String,
    #[diesel(sql_type = Text)]
    message: String,
    #[diesel(sql_type = Text)]
    run_key: String,
    #[diesel(sql_type = Text)]
    dedupe_key: String,
    #[diesel(sql_type = Timestamptz)]
    at: DateTime<Utc>,
    #[diesel(sql_type = Timestamptz)]
    created_at: DateTime<Utc>,
}

#[derive(diesel::QueryableByName)]
struct ChannelRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Text)]
    scope: String,
    #[diesel(sql_type = Nullable<Text>)]
    tenant_id: Option<String>,
    #[diesel(sql_type = Text)]
    kind: String,
    #[diesel(sql_type = Text)]
    address: String,
}

#[derive(diesel::QueryableByName)]
struct DeliveryRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Text)]
    channel_id: String,
    #[diesel(sql_type = Text)]
    kind: String,
    #[diesel(sql_type = Text)]
    address: String,
    #[diesel(sql_type = Text)]
    subject: String,
    #[diesel(sql_type = Text)]
    body: String,
}

impl DeliveryLedger for PgDeliveryLedger {
    fn alerts_since(&self, since: DateTime<Utc>, limit: usize) -> Result<Vec<PendingAlert>, LedgerError> {
        let limit = limit.min(i64::MAX as usize) as i64;
        self.bridge
            .block_on(move |mut conn| async move {
                let rows: Vec<AlertRow> = diesel::sql_query(
                    "SELECT id::text AS id, tenant_id::text AS tenant_id, account_id, code, severity, message, run_key, dedupe_key, at, created_at \
                     FROM rebalancer_alerts WHERE created_at > $1 ORDER BY created_at ASC, id ASC LIMIT $2",
                )
                .bind::<Timestamptz, _>(since)
                .bind::<BigInt, _>(limit)
                .get_results(&mut conn)
                .await
                .map_err(|e| e.to_string())?;
                Ok(rows
                    .into_iter()
                    .map(|r| PendingAlert {
                        id: r.id,
                        tenant_id: tenant_of(r.tenant_id),
                        account_id: r.account_id,
                        code: r.code,
                        severity: r.severity,
                        message: r.message,
                        run_key: r.run_key,
                        dedupe_key: r.dedupe_key,
                        at: r.at,
                        created_at: r.created_at,
                    })
                    .collect())
            })
            .map_err(unavailable)
    }

    fn channels(&self) -> Result<Vec<Channel>, LedgerError> {
        self.bridge
            .block_on(move |mut conn| async move {
                let rows: Vec<ChannelRow> = diesel::sql_query(
                    "SELECT id::text AS id, scope, tenant_id::text AS tenant_id, kind, address FROM alert_channels \
                     WHERE verified_at IS NOT NULL AND disabled_at IS NULL ORDER BY created_at ASC, id ASC",
                )
                .get_results(&mut conn)
                .await
                .map_err(|e| e.to_string())?;
                let mut out = Vec::with_capacity(rows.len());
                for r in rows {
                    let scope = ChannelScope::parse(&r.scope).ok_or_else(|| format!("alert_channels.{}: unknown scope {:?}", r.id, r.scope))?;
                    out.push(Channel { id: r.id, scope, tenant_id: tenant_of(r.tenant_id), kind: r.kind, address: r.address });
                }
                Ok(out)
            })
            .map_err(unavailable)
    }

    fn insert_if_absent(&self, d: &NewDelivery, now: DateTime<Utc>) -> Result<bool, LedgerError> {
        let channel_id = uuid_param(&d.channel_id, "channel_id")?;
        let tenant_id = match &d.tenant_id {
            Some(t) => Some(uuid_param(t, "tenant_id")?),
            None => None,
        };
        let scope = d.scope.as_str().to_string();
        let (severity, dedupe_key, subject, body) = (d.severity.clone(), d.dedupe_key.clone(), d.subject.clone(), d.body.clone());
        self.bridge
            .block_on(move |mut conn| async move {
                let n = diesel::sql_query(
                    "INSERT INTO alert_deliveries (scope, tenant_id, channel_id, source, severity, dedupe_key, subject, body, created_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                     ON CONFLICT (channel_id, dedupe_key) DO NOTHING",
                )
                .bind::<Text, _>(&scope)
                .bind::<Nullable<diesel::sql_types::Uuid>, _>(tenant_id)
                .bind::<diesel::sql_types::Uuid, _>(channel_id)
                .bind::<Text, _>(SOURCE_REBALANCER)
                .bind::<Text, _>(&severity)
                .bind::<Text, _>(&dedupe_key)
                .bind::<Text, _>(&subject)
                .bind::<Text, _>(&body)
                .bind::<Timestamptz, _>(now)
                .execute(&mut conn)
                .await
                .map_err(|e| e.to_string())?;
                Ok(n == 1)
            })
            .map_err(unavailable)
    }

    fn undelivered(&self, limit: usize) -> Result<Vec<PendingDelivery>, LedgerError> {
        let limit = limit.min(i64::MAX as usize) as i64;
        self.bridge
            .block_on(move |mut conn| async move {
                let rows: Vec<DeliveryRow> = diesel::sql_query(
                    "SELECT d.id::text AS id, d.channel_id::text AS channel_id, c.kind, c.address, d.subject, d.body \
                     FROM alert_deliveries d JOIN alert_channels c ON c.id = d.channel_id \
                     WHERE d.source = $1 AND d.sent_at IS NULL AND d.error IS NULL \
                     ORDER BY d.created_at ASC, d.id ASC LIMIT $2",
                )
                .bind::<Text, _>(SOURCE_REBALANCER)
                .bind::<BigInt, _>(limit)
                .get_results(&mut conn)
                .await
                .map_err(|e| e.to_string())?;
                Ok(rows.into_iter().map(|r| PendingDelivery { id: r.id, channel_id: r.channel_id, kind: r.kind, address: r.address, subject: r.subject, body: r.body }).collect())
            })
            .map_err(unavailable)
    }

    fn mark_sent(&self, id: &str, provider_message_id: &str, at: DateTime<Utc>) -> Result<(), LedgerError> {
        let id = uuid_param(id, "delivery id")?;
        let pmid = provider_message_id.to_string();
        self.bridge
            .block_on(move |mut conn| async move {
                diesel::sql_query("UPDATE alert_deliveries SET sent_at = $2, provider_message_id = $3 WHERE id = $1 AND sent_at IS NULL AND error IS NULL")
                    .bind::<diesel::sql_types::Uuid, _>(id)
                    .bind::<Timestamptz, _>(at)
                    .bind::<Text, _>(&pmid)
                    .execute(&mut conn)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
            .map_err(unavailable)
    }

    fn mark_error(&self, id: &str, error: &str) -> Result<(), LedgerError> {
        let id = uuid_param(id, "delivery id")?;
        let error = error.to_string();
        self.bridge
            .block_on(move |mut conn| async move {
                diesel::sql_query("UPDATE alert_deliveries SET error = $2 WHERE id = $1 AND sent_at IS NULL AND error IS NULL")
                    .bind::<diesel::sql_types::Uuid, _>(id)
                    .bind::<Text, _>(&error)
                    .execute(&mut conn)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
            .map_err(unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_nil_tenant_and_garbage_are_no_tenant_and_a_real_uuid_is_canonical() {
        assert_eq!(tenant_of(None), None);
        assert_eq!(tenant_of(Some(UNKNOWN_TENANT.to_string())), None);
        assert_eq!(tenant_of(Some("not-a-uuid".into())), None);
        assert_eq!(tenant_of(Some("AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA".into())).as_deref(), Some("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"));
        assert!(uuid_param("x", "channel_id").unwrap_err().to_string().starts_with("ALERT_STORE_UNAVAILABLE: channel_id"));
    }
}
