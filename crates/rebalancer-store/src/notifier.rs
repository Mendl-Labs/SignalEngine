//! `PgNotifier`: the Postgres-backed [`Notifier`], against `rebalancer_alerts`. `notify` is a plain
//! INSERT; a failed insert returns `Err`, which the pipeline records in
//! `RunRecord::alert_delivery_failures` and never treats as a reason to stop (see
//! `pipeline.rs::Run::alert`) -- so a Postgres outage degrades this crate's alerting to "recorded in
//! the run record only," never to "silently dropped."
//!
//! This is a PERSISTENCE sink, not a delivery channel (email/Telegram/dead-man's-switch are WP5.1/5.2,
//! not built here) -- it answers "was this alert ever raised" for an audit trail and a future
//! dashboard, not "did a human see it".

use diesel::sql_types::{Text, Timestamptz};
use diesel_async::RunQueryDsl;
use rebalancer_run::record::Alert;
use rebalancer_run::stores::Notifier;

use crate::pg::{Bridge, Pool};
use crate::tenants::AccountTenants;

pub struct PgNotifier {
    bridge: Bridge,
    tenants: std::sync::Arc<AccountTenants>,
}

impl PgNotifier {
    pub fn new(pool: Pool, tenants: std::sync::Arc<AccountTenants>) -> Result<Self, String> {
        Ok(Self { bridge: Bridge::new(pool)?, tenants })
    }
}

impl PgNotifier {
    /// Every alert already stored for `(account_id, code)` whose `dedupe_key` starts with `prefix`, as
    /// `(dedupe_key, at)`. R47 uses this as its "first seen" record: the first alert of an exclusion IS the anchor, so
    /// no new table is needed. A plain prefix compare (`left(..) = ..`), not `LIKE`, so `_` in a code is literal.
    pub fn alerts_with_key_prefix(&self, account_id: &str, code: &str, prefix: &str) -> Result<Vec<(String, chrono::DateTime<chrono::Utc>)>, String> {
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = Text)]
            dedupe_key: String,
            #[diesel(sql_type = Timestamptz)]
            at: chrono::DateTime<chrono::Utc>,
        }
        let (account_id, code, prefix) = (account_id.to_string(), code.to_string(), prefix.to_string());
        self.bridge.block_on(move |mut conn| async move {
            let rows: Vec<Row> = diesel::sql_query(
                "SELECT dedupe_key, at FROM rebalancer_alerts \
                 WHERE account_id = $1 AND code = $2 AND left(dedupe_key, char_length($3)) = $3",
            )
            .bind::<Text, _>(&account_id)
            .bind::<Text, _>(&code)
            .bind::<Text, _>(&prefix)
            .get_results(&mut conn)
            .await
            .map_err(|e| format!("NOTIFIER_UNAVAILABLE: {e}"))?;
            Ok(rows.into_iter().map(|r| (r.dedupe_key, r.at)).collect())
        })
    }

    /// Persist `alert` under an explicit tenant (an excluded account has no entry in the `AccountTenants` registry,
    /// which is filled from the due scan). Same INSERT as `notify`.
    pub fn notify_for_tenant(&self, alert: &Alert, tenant_id: uuid::Uuid) -> Result<(), String> {
        self.tenants.register(&alert.account_id, tenant_id);
        <Self as Notifier>::notify(self, alert)
    }
}

impl Notifier for PgNotifier {
    fn notify(&self, alert: &Alert) -> Result<(), String> {
        let tenant_id = self.tenants.get(&alert.account_id);
        let code = alert.code.as_str().to_string();
        let severity = alert.severity.as_str().to_string();
        let account_id = alert.account_id.clone();
        let run_key = alert.run_key.clone();
        let message = alert.message.clone();
        let dedupe_key = alert.dedupe_key.clone();
        let at = alert.at;
        self.bridge.block_on(move |mut conn| async move {
            diesel::sql_query(
                "INSERT INTO rebalancer_alerts \
                 (tenant_id, account_id, code, severity, message, run_key, dedupe_key, at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind::<diesel::sql_types::Uuid, _>(tenant_id)
            .bind::<Text, _>(&account_id)
            .bind::<Text, _>(&code)
            .bind::<Text, _>(&severity)
            .bind::<Text, _>(&message)
            .bind::<Text, _>(&run_key)
            .bind::<Text, _>(&dedupe_key)
            .bind::<Timestamptz, _>(at)
            .execute(&mut conn)
            .await
            .map(|_| ())
            .map_err(|e| format!("NOTIFIER_UNAVAILABLE: {e}"))
        })
    }
}
