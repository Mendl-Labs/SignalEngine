//! Multi-tenant exchange-credential provider over the PRIVATE schema.
//!
//! [`MultiTenantDbProvider`] answers "which key signs THIS tenant's order?" from
//! the `exchange_credentials` table of the private (SaaS) database, where every
//! row carries `tenant_id UUID NOT NULL`
//! (`databaseschema-internal/migrations/2026-01-24-500000_add_tenant_extension_columns`).
//!
//! # Why raw SQL and a runtime column check
//!
//! The public crates are built against the PUBLIC `databaseschema`, whose
//! `exchange_credentials` has NO `tenant_id` column. So this module cannot use
//! the diesel DSL against a tenant column; it uses `diesel::sql_query` and
//! checks, at construction AND on every lookup, that the table it would query
//! really has a `tenant_id uuid` column. Against the public schema
//! construction fails ([`CredentialError::SchemaNotTenantScoped`]); if the
//! column disappears later, every lookup fails. The provider therefore can
//! never degrade into an untenanted read.
//!
//! # What it guarantees
//!
//! * The tenant is always a bound parameter of the `WHERE` clause
//!   (`tenant_id = $1`); no query in this module reads exchange credentials
//!   without it.
//! * The nil UUID is never served (no query is run for it).
//! * Only `is_enabled` rows; with `live_only` also only `NOT is_testnet` rows;
//!   soft-deleted rows (`deleted_at IS NOT NULL`, private schema migration
//!   `2026-09-22-000004`) are skipped whenever that column exists.
//! * AMBIGUITY FAILS CLOSED. The table's own uniqueness constraint is
//!   `UNIQUE (tenant_id, exchange, label)` -- i.e. a tenant may legitimately hold
//!   several rows for one exchange under different labels, and `exchange` is
//!   compared case-insensitively here but case-sensitively by the constraint. If
//!   more than one row matches (tenant, exchange, enabled[, not testnet]) the
//!   provider returns [`CredentialError::Ambiguous`] instead of picking one
//!   ("the first row" is exactly what must never sign an order). With
//!   `live_only = true` a tenant's sandbox key never causes ambiguity;
//!   with `live_only = false` a sandbox key next to a production key does.
//! * A row that cannot be decrypted is an error
//!   ([`CredentialError::Backend`]) for the whole call -- never skipped, never
//!   replaced by another row, and a passphrase that cannot be decrypted is
//!   NOT silently dropped.
//! * No key, secret or passphrase (plain or encrypted) is ever logged or put in
//!   an error message.
//!
//! Encryption format: identical to the writer in BacktestingEngine
//! (`program/src/api/exchange_credentials.rs`): `aes:<base64(12-byte nonce ++
//! AES-256-GCM ciphertext)>` under `CREDENTIALS_ENCRYPTION_KEY` (64 hex chars),
//! or the legacy `enc:<base64>`. The decryption itself is
//! [`crate::database`]'s existing routine; it is not duplicated here.

use std::sync::Arc;

use async_trait::async_trait;
use diesel::sql_types::{Bool, Nullable, Text, Uuid as SqlUuid};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::credentials::{CredentialError, CredentialProvider, ExchangeCredential, TenantId};
use crate::database::{decrypt_credential_value, DbPool};

/// Environment variable holding the AES-256-GCM key (64 hex chars).
pub const ENCRYPTION_KEY_ENV: &str = "CREDENTIALS_ENCRYPTION_KEY";

/// Pure check of a candidate encryption key value: present and 32 hex-encoded
/// bytes. `Err` carries a plain reason (never the value).
pub fn check_encryption_key(value: Option<&str>) -> Result<(), String> {
    let Some(v) = value.map(str::trim).filter(|v| !v.is_empty()) else {
        return Err(format!("{} is not set", ENCRYPTION_KEY_ENV));
    };
    match hex::decode(v) {
        Ok(bytes) if bytes.len() == 32 => Ok(()),
        Ok(bytes) => Err(format!(
            "{} must be 32 bytes (64 hex chars), got {} bytes",
            ENCRYPTION_KEY_ENV,
            bytes.len()
        )),
        Err(_) => Err(format!("{} must be hex (64 chars)", ENCRYPTION_KEY_ENV)),
    }
}

/// [`check_encryption_key`] against this process's environment.
pub fn multi_tenant_encryption_key_status() -> Result<(), String> {
    check_encryption_key(std::env::var(ENCRYPTION_KEY_ENV).ok().as_deref())
}

// ---------------------------------------------------------------------------
// Schema check
// ---------------------------------------------------------------------------

/// Columns of the `exchange_credentials` table that an unqualified query would
/// hit (the one `search_path` resolves to), not merely one named that in some
/// schema.
const SHAPE_SQL: &str = "SELECT column_name::text AS column_name, data_type::text AS data_type \
     FROM information_schema.columns \
     WHERE table_name = 'exchange_credentials' \
       AND table_schema = (SELECT n.nspname::text FROM pg_class c \
                           JOIN pg_namespace n ON n.oid = c.relnamespace \
                           WHERE c.oid = to_regclass('exchange_credentials'))";

#[derive(diesel::QueryableByName)]
struct ColumnRow {
    #[diesel(sql_type = Text)]
    column_name: String,
    #[diesel(sql_type = Text)]
    data_type: String,
}

/// What the live table looks like (only the optional parts; `tenant_id` is
/// mandatory and verified before this is returned).
#[derive(Debug, Clone, Copy)]
struct TableShape {
    has_deleted_at: bool,
}

/// Fail closed unless `exchange_credentials` has a `tenant_id uuid` column.
async fn verify_shape(conn: &mut AsyncPgConnection) -> Result<TableShape, CredentialError> {
    let cols: Vec<ColumnRow> = diesel::sql_query(SHAPE_SQL)
        .load(conn)
        .await
        .map_err(|e| CredentialError::Backend(format!("could not inspect exchange_credentials: {}", e)))?;
    if cols.is_empty() {
        return Err(CredentialError::SchemaNotTenantScoped(
            "table exchange_credentials was not found in the connected database".to_string(),
        ));
    }
    match cols.iter().find(|c| c.column_name == "tenant_id") {
        None => Err(CredentialError::SchemaNotTenantScoped(
            "exchange_credentials has no tenant_id column (this is the PUBLIC schema); the \
             multi-tenant provider needs the private schema and will not read an untenanted table"
                .to_string(),
        )),
        Some(c) if c.data_type != "uuid" => Err(CredentialError::SchemaNotTenantScoped(format!(
            "exchange_credentials.tenant_id has type '{}', expected uuid",
            c.data_type
        ))),
        Some(_) => Ok(TableShape { has_deleted_at: cols.iter().any(|c| c.column_name == "deleted_at") }),
    }
}

// ---------------------------------------------------------------------------
// Queries (every one of them is bound to the tenant)
// ---------------------------------------------------------------------------

const COLUMNS: &str =
    "id, exchange, label, api_key_encrypted, api_secret_encrypted, passphrase_encrypted, is_testnet, is_enabled";

/// The enabled credential(s) of `$1` (tenant) for `$2` (exchange, any case).
fn credential_sql(shape: TableShape, live_only: bool) -> String {
    let mut sql = format!(
        "SELECT {COLUMNS} FROM exchange_credentials \
         WHERE tenant_id = $1 AND lower(exchange) = lower($2) AND is_enabled"
    );
    if live_only {
        sql.push_str(" AND NOT is_testnet");
    }
    if shape.has_deleted_at {
        sql.push_str(" AND deleted_at IS NULL");
    }
    sql.push_str(" ORDER BY exchange, label, id");
    sql
}

/// Every enabled credential of `$1` (tenant), all exchanges, testnet included.
fn all_credentials_sql(shape: TableShape) -> String {
    let mut sql = format!(
        "SELECT {COLUMNS} FROM exchange_credentials WHERE tenant_id = $1 AND is_enabled"
    );
    if shape.has_deleted_at {
        sql.push_str(" AND deleted_at IS NULL");
    }
    sql.push_str(" ORDER BY exchange, label, id");
    sql
}

/// Diagnostic only, used when [`credential_sql`] found nothing: which reason
/// (missing / disabled / testnet-only) applies to THIS tenant.
fn why_missing_sql(shape: TableShape) -> String {
    let mut sql = String::from(
        "SELECT is_enabled, is_testnet FROM exchange_credentials \
         WHERE tenant_id = $1 AND lower(exchange) = lower($2)",
    );
    if shape.has_deleted_at {
        sql.push_str(" AND deleted_at IS NULL");
    }
    sql
}

#[derive(diesel::QueryableByName)]
struct CredentialRow {
    #[diesel(sql_type = SqlUuid)]
    id: Uuid,
    #[diesel(sql_type = Text)]
    exchange: String,
    #[diesel(sql_type = Text)]
    label: String,
    #[diesel(sql_type = Text)]
    api_key_encrypted: String,
    #[diesel(sql_type = Text)]
    api_secret_encrypted: String,
    #[diesel(sql_type = Nullable<Text>)]
    passphrase_encrypted: Option<String>,
    #[diesel(sql_type = Bool)]
    is_testnet: bool,
    #[diesel(sql_type = Bool)]
    is_enabled: bool,
}

#[derive(diesel::QueryableByName)]
struct FlagsRow {
    #[diesel(sql_type = Bool)]
    is_enabled: bool,
    #[diesel(sql_type = Bool)]
    is_testnet: bool,
}

/// Decrypt one row. ANY failure is an error for the caller: no fallback.
fn decrypt_row(row: CredentialRow) -> Result<ExchangeCredential, CredentialError> {
    let fail = |field: &str| {
        CredentialError::Backend(format!(
            "exchange credential {} (exchange '{}', label '{}'): {} could not be decrypted \
             (wrong {} or a corrupt value); refusing to fall back to any other credential",
            row.id, row.exchange, row.label, field, ENCRYPTION_KEY_ENV
        ))
    };
    let api_key = decrypt_credential_value(&row.api_key_encrypted).ok_or_else(|| fail("api key"))?;
    let api_secret =
        decrypt_credential_value(&row.api_secret_encrypted).ok_or_else(|| fail("api secret"))?;
    let passphrase = match row.passphrase_encrypted.as_deref() {
        None => None,
        Some(p) => Some(decrypt_credential_value(p).ok_or_else(|| fail("passphrase"))?),
    };
    Ok(ExchangeCredential {
        id: row.id,
        exchange: row.exchange,
        label: row.label,
        api_key,
        api_secret,
        passphrase,
        is_testnet: row.is_testnet,
        is_enabled: row.is_enabled,
    })
}

// ---------------------------------------------------------------------------
// The provider
// ---------------------------------------------------------------------------

/// Credential provider for the multi-tenant (SaaS) deployment. See the module
/// docs. Build it with [`MultiTenantDbProvider::new`]; there is no constructor
/// that skips the schema and key checks.
pub struct MultiTenantDbProvider {
    pool: Arc<DbPool>,
}

impl MultiTenantDbProvider {
    /// Verify that the encryption key is usable and that the connected database
    /// has a tenant-scoped `exchange_credentials` table, then build the
    /// provider. Refuses (`Err`) otherwise.
    pub async fn new(pool: Arc<DbPool>) -> Result<Self, CredentialError> {
        Self::with_key_source(pool, std::env::var(ENCRYPTION_KEY_ENV).ok()).await
    }

    /// [`Self::new`] with the key value passed in (tests).
    pub(crate) async fn with_key_source(
        pool: Arc<DbPool>,
        key_value: Option<String>,
    ) -> Result<Self, CredentialError> {
        check_encryption_key(key_value.as_deref()).map_err(|why| {
            CredentialError::Backend(format!("multi-tenant credential provider refused: {}", why))
        })?;
        let provider = Self { pool };
        let mut conn = provider.connection().await?;
        verify_shape(&mut conn).await?;
        Ok(provider)
    }

    /// Test-only: a provider that skipped the checks (to prove that requests it
    /// must refuse never reach the database).
    #[cfg(test)]
    pub(crate) fn unverified_for_test(pool: Arc<DbPool>) -> Self {
        Self { pool }
    }

    async fn connection(
        &self,
    ) -> Result<diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>, CredentialError>
    {
        self.pool
            .get()
            .await
            .map_err(|e| CredentialError::Backend(format!("database connection: {}", e)))
    }
}

fn not_found(tenant: TenantId, exchange: &str) -> CredentialError {
    CredentialError::NotFound { tenant, exchange: exchange.to_string() }
}

#[async_trait]
impl CredentialProvider for MultiTenantDbProvider {
    async fn credentials_for(
        &self,
        tenant: TenantId,
        exchange: &str,
        live_only: bool,
    ) -> Result<ExchangeCredential, CredentialError> {
        // The nil tenant (a deployment row without a tenant) is never served,
        // and nothing is queried for it.
        if tenant.is_nil() || exchange.trim().is_empty() {
            return Err(not_found(tenant, exchange));
        }
        let mut conn = self.connection().await?;
        // Re-checked on EVERY lookup: never an untenanted read, even if the
        // schema changed underneath a running process.
        let shape = verify_shape(&mut conn).await?;

        let rows: Vec<CredentialRow> = diesel::sql_query(credential_sql(shape, live_only))
            .bind::<SqlUuid, _>(tenant)
            .bind::<Text, _>(exchange.trim().to_string())
            .load(&mut conn)
            .await
            .map_err(|e| CredentialError::Backend(format!("loading exchange credential: {}", e)))?;

        match rows.len() {
            0 => {
                let flags: Vec<FlagsRow> = diesel::sql_query(why_missing_sql(shape))
                    .bind::<SqlUuid, _>(tenant)
                    .bind::<Text, _>(exchange.trim().to_string())
                    .load(&mut conn)
                    .await
                    .map_err(|e| {
                        CredentialError::Backend(format!("loading exchange credential: {}", e))
                    })?;
                Err(if flags.is_empty() {
                    not_found(tenant, exchange)
                } else if !flags.iter().any(|f| f.is_enabled) {
                    CredentialError::Disabled { exchange: exchange.to_string() }
                } else if live_only && flags.iter().all(|f| !f.is_enabled || f.is_testnet) {
                    CredentialError::NoLiveCredential { exchange: exchange.to_string() }
                } else {
                    not_found(tenant, exchange)
                })
            }
            1 => {
                let row = rows.into_iter().next().expect("len is 1");
                debug_assert!(row.is_enabled);
                log::debug!(
                    "[CREDENTIALS] tenant {} exchange '{}' live_only={}: serving credential {}",
                    tenant, exchange, live_only, row.id
                );
                decrypt_row(row)
            }
            n => Err(CredentialError::Ambiguous {
                tenant,
                exchange: exchange.to_string(),
                matches: n,
            }),
        }
    }

    async fn all_credentials_for(
        &self,
        tenant: TenantId,
    ) -> Result<Vec<ExchangeCredential>, CredentialError> {
        if tenant.is_nil() {
            return Err(not_found(tenant, "*"));
        }
        let mut conn = self.connection().await?;
        let shape = verify_shape(&mut conn).await?;
        let rows: Vec<CredentialRow> = diesel::sql_query(all_credentials_sql(shape))
            .bind::<SqlUuid, _>(tenant)
            .load(&mut conn)
            .await
            .map_err(|e| CredentialError::Backend(format!("loading exchange credentials: {}", e)))?;
        rows.into_iter().map(decrypt_row).collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::{resolve_all_credentials, resolve_credential};
    use crate::database::create_pool;
    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::{Aes256Gcm, Key, Nonce};
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use diesel_async::AsyncConnection;
    use std::sync::{Mutex, Once};

    /// Fixed fake key (32 bytes of 0x11): the tests' "production key".
    const KEY_HEX: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    /// A different key, used to write rows this process cannot decrypt.
    const OTHER_KEY_HEX: &str = "2222222222222222222222222222222222222222222222222222222222222222";

    fn use_test_key() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| std::env::set_var(ENCRYPTION_KEY_ENV, KEY_HEX));
    }

    /// Encrypt like the BacktestingEngine writer: `aes:<b64(nonce ++ ct)>`.
    fn encrypt(key_hex: &str, plain: &str) -> String {
        let key_bytes = hex::decode(key_hex).unwrap();
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));
        let nonce_src = Uuid::new_v4();
        let nonce_bytes = &nonce_src.as_bytes()[..12];
        let ct = cipher.encrypt(Nonce::from_slice(nonce_bytes), plain.as_bytes()).unwrap();
        let mut combined = nonce_bytes.to_vec();
        combined.extend_from_slice(&ct);
        format!("aes:{}", STANDARD.encode(combined))
    }

    /// A made-up, obviously non-secret value that is unique per call.
    fn fake(tag: &str) -> String {
        format!("fake-{}-{}", tag, Uuid::new_v4().simple())
    }

    // ---- log capture (to prove nothing secret is logged) ----------------

    static CAPTURED: Mutex<Vec<String>> = Mutex::new(Vec::new());
    struct Capture;
    impl log::Log for Capture {
        fn enabled(&self, _: &log::Metadata) -> bool {
            true
        }
        fn log(&self, record: &log::Record) {
            if let Ok(mut g) = CAPTURED.lock() {
                g.push(format!("{}", record.args()));
            }
        }
        fn flush(&self) {}
    }
    static CAPTURE: Capture = Capture;
    fn capture_logs() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            let _ = log::set_logger(&CAPTURE);
            log::set_max_level(log::LevelFilter::Trace);
        });
    }

    // ---- scratch database ------------------------------------------------

    fn base_url() -> Option<String> {
        match std::env::var("TENANTCRED_TEST_DATABASE_URL") {
            Ok(u) if !u.trim().is_empty() => Some(u),
            _ => {
                eprintln!("SKIPPED: TENANTCRED_TEST_DATABASE_URL not set");
                None
            }
        }
    }

    /// One throw-away schema holding an `exchange_credentials` table, reached
    /// through a connection URL whose `search_path` is that schema.
    struct Scratch {
        base: String,
        schema: String,
        url: String,
    }

    #[derive(Clone, Copy, PartialEq)]
    enum Shape {
        /// tenant_id uuid NOT NULL + UNIQUE(tenant_id, exchange, label) (+ deleted_at).
        Private { deleted_at: bool },
        /// The public schema: no tenant_id at all.
        Public,
        /// tenant_id present but not a uuid.
        TextTenant,
    }

    impl Scratch {
        async fn new(shape: Shape) -> Option<Scratch> {
            use_test_key();
            let base = base_url()?;
            let schema = format!("tc_{}", Uuid::new_v4().simple());
            let mut admin = AsyncPgConnection::establish(&base).await.expect("connect scratch db");
            diesel::sql_query(format!("CREATE SCHEMA {schema}")).execute(&mut admin).await.unwrap();
            let (tenant_col, unique, deleted) = match shape {
                Shape::Private { deleted_at } => (
                    "tenant_id UUID NOT NULL,",
                    ", CONSTRAINT unique_tenant_exchange_label UNIQUE (tenant_id, exchange, label)",
                    if deleted_at { ", deleted_at TIMESTAMPTZ NULL" } else { "" },
                ),
                Shape::Public => ("", "", ""),
                Shape::TextTenant => ("tenant_id TEXT NOT NULL,", "", ""),
            };
            diesel::sql_query(format!(
                "CREATE TABLE {schema}.exchange_credentials (\
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), {tenant_col} \
                    exchange VARCHAR(50) NOT NULL, label VARCHAR(255) NOT NULL, \
                    api_key_encrypted TEXT NOT NULL, api_secret_encrypted TEXT NOT NULL, \
                    passphrase_encrypted TEXT, \
                    is_testnet BOOLEAN NOT NULL DEFAULT false, is_enabled BOOLEAN NOT NULL DEFAULT true, \
                    permissions JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), \
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW() {deleted} {unique})"
            ))
            .execute(&mut admin)
            .await
            .unwrap();
            let sep = if base.contains('?') { '&' } else { '?' };
            let url = format!("{base}{sep}options=-c%20search_path%3D{schema}");
            Some(Scratch { base, schema, url })
        }

        async fn conn(&self) -> AsyncPgConnection {
            AsyncPgConnection::establish(&self.url).await.expect("connect scratch schema")
        }

        async fn provider(&self) -> Result<MultiTenantDbProvider, CredentialError> {
            let pool = create_pool(&self.url).await.unwrap();
            MultiTenantDbProvider::new(Arc::new(pool)).await
        }

        async fn ok_provider(&self) -> MultiTenantDbProvider {
            self.provider().await.expect("provider builds on a private-shaped table")
        }

        /// Insert a row encrypted with `KEY_HEX`; returns (id, api_key, api_secret).
        #[allow(clippy::too_many_arguments)]
        async fn insert(
            &self,
            tenant: Uuid,
            exchange: &str,
            label: &str,
            passphrase: bool,
            testnet: bool,
            enabled: bool,
        ) -> Row {
            let key = fake("key");
            let secret = fake("secret");
            let pass = passphrase.then(|| fake("pass"));
            let id = self
                .insert_raw(
                    tenant,
                    exchange,
                    label,
                    &encrypt(KEY_HEX, &key),
                    &encrypt(KEY_HEX, &secret),
                    pass.as_deref().map(|p| encrypt(KEY_HEX, p)).as_deref(),
                    testnet,
                    enabled,
                )
                .await;
            Row { id, key, secret, pass }
        }

        #[allow(clippy::too_many_arguments)]
        async fn insert_raw(
            &self,
            tenant: Uuid,
            exchange: &str,
            label: &str,
            key_enc: &str,
            secret_enc: &str,
            pass_enc: Option<&str>,
            testnet: bool,
            enabled: bool,
        ) -> Uuid {
            let mut c = self.conn().await;
            let id = Uuid::new_v4();
            diesel::sql_query(
                "INSERT INTO exchange_credentials \
                 (id, tenant_id, exchange, label, api_key_encrypted, api_secret_encrypted, \
                  passphrase_encrypted, is_testnet, is_enabled) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            )
            .bind::<SqlUuid, _>(id)
            .bind::<SqlUuid, _>(tenant)
            .bind::<Text, _>(exchange.to_string())
            .bind::<Text, _>(label.to_string())
            .bind::<Text, _>(key_enc.to_string())
            .bind::<Text, _>(secret_enc.to_string())
            .bind::<Nullable<Text>, _>(pass_enc.map(str::to_string))
            .bind::<Bool, _>(testnet)
            .bind::<Bool, _>(enabled)
            .execute(&mut c)
            .await
            .expect("insert exchange_credentials row");
            id
        }

        async fn exec(&self, sql: &str) {
            let mut c = self.conn().await;
            diesel::sql_query(sql).execute(&mut c).await.unwrap();
        }

        async fn cleanup(self) {
            let mut admin = AsyncPgConnection::establish(&self.base).await.unwrap();
            let _ = diesel::sql_query(format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema))
                .execute(&mut admin)
                .await;
        }
    }

    struct Row {
        id: Uuid,
        key: String,
        secret: String,
        pass: Option<String>,
    }

    macro_rules! scratch {
        ($shape:expr) => {
            match Scratch::new($shape).await {
                Some(s) => s,
                None => return,
            }
        };
    }
    const PRIVATE: Shape = Shape::Private { deleted_at: false };

    // ---- tests that need no database --------------------------------------

    #[test]
    fn encryption_key_check_is_strict() {
        assert!(check_encryption_key(None).unwrap_err().contains("not set"));
        assert!(check_encryption_key(Some("")).is_err());
        assert!(check_encryption_key(Some("   ")).is_err());
        assert!(check_encryption_key(Some("zz")).unwrap_err().contains("hex"));
        assert!(check_encryption_key(Some("abcd")).unwrap_err().contains("32 bytes"));
        assert!(check_encryption_key(Some(&"ab".repeat(31))).is_err());
        assert!(check_encryption_key(Some(&"ab".repeat(33))).is_err());
        assert!(check_encryption_key(Some(&"ab".repeat(32))).is_ok());
        assert!(check_encryption_key(Some(&format!(" {} \n", "ab".repeat(32)))).is_ok());
        // The reason never echoes the value.
        let secretish = "not-a-real-key-but-treat-it-as-one";
        assert!(!check_encryption_key(Some(secretish)).unwrap_err().contains(secretish));
    }

    #[test]
    fn queries_are_always_bound_to_the_tenant() {
        for deleted_at in [false, true] {
            let shape = TableShape { has_deleted_at: deleted_at };
            for live_only in [false, true] {
                let sql = credential_sql(shape, live_only);
                assert!(sql.contains("tenant_id = $1"), "{sql}");
                assert!(sql.contains("AND is_enabled"), "{sql}");
                assert_eq!(sql.contains("NOT is_testnet"), live_only, "{sql}");
                assert_eq!(sql.contains("deleted_at IS NULL"), deleted_at, "{sql}");
            }
            for sql in [all_credentials_sql(shape), why_missing_sql(shape)] {
                assert!(sql.contains("tenant_id = $1"), "{sql}");
            }
        }
    }

    /// Without the key the provider refuses to exist, before any connection.
    #[tokio::test]
    async fn construction_refuses_a_missing_or_bad_key() {
        let pool = Arc::new(create_pool("postgres://nobody:nothing@127.0.0.1:1/none").await.unwrap());
        for bad in [None, Some(String::new()), Some("xyz".to_string()), Some("ab".repeat(16))] {
            let err = match MultiTenantDbProvider::with_key_source(pool.clone(), bad).await {
                Ok(_) => panic!("provider must not be built without a valid key"),
                Err(e) => e,
            };
            assert!(
                matches!(&err, CredentialError::Backend(m) if m.contains(ENCRYPTION_KEY_ENV)),
                "{:?}",
                err
            );
        }
    }

    /// The nil tenant is refused before the database is touched (the pool points
    /// at a closed port: reaching it would be `Backend`, not `NotFound`).
    #[tokio::test]
    async fn nil_tenant_is_refused_without_touching_the_database() {
        let pool = Arc::new(create_pool("postgres://nobody:nothing@127.0.0.1:1/none").await.unwrap());
        let p = MultiTenantDbProvider::unverified_for_test(pool);
        let nil = Uuid::nil();
        assert_eq!(
            p.credentials_for(nil, "kraken", true).await.unwrap_err(),
            CredentialError::NotFound { tenant: nil, exchange: "kraken".to_string() }
        );
        assert!(matches!(
            p.credentials_for(nil, "kraken", false).await.unwrap_err(),
            CredentialError::NotFound { .. }
        ));
        assert!(matches!(
            p.all_credentials_for(nil).await.unwrap_err(),
            CredentialError::NotFound { .. }
        ));
        // A real tenant does reach the (unreachable) database and fails closed.
        assert!(matches!(
            p.credentials_for(Uuid::new_v4(), "kraken", true).await.unwrap_err(),
            CredentialError::Backend(_)
        ));
    }

    // ---- tests against the scratch database ---------------------------------

    #[tokio::test]
    async fn tenant_a_gets_only_its_own_credential_even_when_b_has_one() {
        let s = scratch!(PRIVATE);
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let ra = s.insert(a, "kraken", "main", true, false, true).await;
        let rb = s.insert(b, "kraken", "main", true, false, true).await;
        let p = s.ok_provider().await;

        let ca = resolve_credential(&p, a, "kraken", true).await.unwrap();
        assert_eq!((ca.id, &ca.api_key, &ca.api_secret), (ra.id, &ra.key, &ra.secret));
        assert_eq!(ca.passphrase, ra.pass);
        let cb = resolve_credential(&p, b, "KRAKEN", true).await.unwrap();
        assert_eq!((cb.id, &cb.api_key, &cb.api_secret), (rb.id, &rb.key, &rb.secret));
        assert_ne!(ca.api_key, cb.api_key);
        assert!(!ca.is_testnet && ca.is_enabled);

        // A holds nothing on coinbase even though B might: never B's row.
        s.insert(b, "coinbase", "main", false, false, true).await;
        let err = resolve_credential(&p, a, "coinbase", true).await.unwrap_err();
        assert!(matches!(err, CredentialError::NotFound { tenant, .. } if tenant == a), "{err:?}");
        s.cleanup().await;
    }

    #[tokio::test]
    async fn unknown_tenant_and_unknown_exchange_are_not_found() {
        let s = scratch!(PRIVATE);
        let a = Uuid::new_v4();
        s.insert(a, "kraken", "main", false, false, true).await;
        let p = s.ok_provider().await;
        let stranger = Uuid::new_v4();
        let err = resolve_credential(&p, stranger, "kraken", true).await.unwrap_err();
        assert_eq!(err, CredentialError::NotFound { tenant: stranger, exchange: "kraken".into() });
        assert!(matches!(
            resolve_credential(&p, a, "binance", true).await.unwrap_err(),
            CredentialError::NotFound { .. }
        ));
        assert!(matches!(
            resolve_credential(&p, a, "   ", true).await.unwrap_err(),
            CredentialError::NotFound { .. }
        ));
        assert!(resolve_all_credentials(&p, stranger).await.unwrap().is_empty());
        s.cleanup().await;
    }

    /// A row stored with the nil tenant (e.g. a broken import) is still never
    /// served: the nil tenant is refused before any query, and no real tenant
    /// can see it.
    #[tokio::test]
    async fn nil_tenant_is_refused_even_when_a_nil_tenant_row_exists() {
        let s = scratch!(PRIVATE);
        let real = Uuid::new_v4();
        s.insert(Uuid::nil(), "kraken", "orphan", false, false, true).await;
        s.insert(real, "kraken", "main", false, false, true).await;
        let p = s.ok_provider().await;
        let nil = Uuid::nil();
        assert!(matches!(
            resolve_credential(&p, nil, "kraken", true).await.unwrap_err(),
            CredentialError::NotFound { .. }
        ));
        assert!(matches!(p.all_credentials_for(nil).await.unwrap_err(), CredentialError::NotFound { .. }));
        // The real tenant gets its own row, not the orphan.
        let c = resolve_credential(&p, real, "kraken", true).await.unwrap();
        assert_eq!(c.label, "main");
        s.cleanup().await;
    }

    #[tokio::test]
    async fn disabled_rows_are_not_served() {
        let s = scratch!(PRIVATE);
        let a = Uuid::new_v4();
        s.insert(a, "kraken", "off", false, false, false).await;
        s.insert(a, "coinbase", "off", false, false, false).await;
        let on = s.insert(a, "coinbase", "on", false, false, true).await;
        let p = s.ok_provider().await;
        assert!(matches!(
            resolve_credential(&p, a, "kraken", true).await.unwrap_err(),
            CredentialError::Disabled { .. }
        ));
        assert!(matches!(
            resolve_credential(&p, a, "kraken", false).await.unwrap_err(),
            CredentialError::Disabled { .. }
        ));
        // A disabled duplicate neither wins nor makes the enabled row ambiguous.
        assert_eq!(resolve_credential(&p, a, "coinbase", true).await.unwrap().id, on.id);
        // all_credentials_for lists enabled rows only.
        let all = p.all_credentials_for(a).await.unwrap();
        assert_eq!(all.iter().map(|c| c.id).collect::<Vec<_>>(), vec![on.id]);
        s.cleanup().await;
    }

    #[tokio::test]
    async fn live_only_excludes_testnet_rows() {
        let s = scratch!(PRIVATE);
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let only_test = s.insert(a, "kraken", "sandbox", false, true, true).await;
        let live_b = s.insert(b, "kraken", "live", false, false, true).await;
        s.insert(b, "kraken", "sandbox", false, true, true).await;
        let p = s.ok_provider().await;

        // Tenant with only a sandbox key: fine for paper, refused for live.
        assert!(matches!(
            resolve_credential(&p, a, "kraken", true).await.unwrap_err(),
            CredentialError::NoLiveCredential { .. }
        ));
        let paper = resolve_credential(&p, a, "kraken", false).await.unwrap();
        assert_eq!(paper.id, only_test.id);
        assert!(paper.is_testnet);
        // Tenant with both: live_only picks the production key, never the sandbox one.
        let live = resolve_credential(&p, b, "kraken", true).await.unwrap();
        assert_eq!(live.id, live_b.id);
        assert!(!live.is_testnet);
        s.cleanup().await;
    }

    #[tokio::test]
    async fn ambiguity_fails_closed_instead_of_picking_a_row() {
        let s = scratch!(PRIVATE);
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        // A: two enabled production rows for one exchange (legal under
        // UNIQUE(tenant_id, exchange, label)), plus a case variant of the name.
        s.insert(a, "kraken", "one", false, false, true).await;
        s.insert(a, "kraken", "two", false, false, true).await;
        s.insert(a, "Coinbase", "x", false, false, true).await;
        s.insert(a, "coinbase", "x", false, false, true).await;
        // B: one production + one sandbox row.
        let b_live = s.insert(b, "kraken", "live", false, false, true).await;
        s.insert(b, "kraken", "sandbox", false, true, true).await;
        // C: exactly one row.
        let c_row = s.insert(c, "kraken", "only", false, false, true).await;
        let p = s.ok_provider().await;

        for live_only in [true, false] {
            let err = p.credentials_for(a, "kraken", live_only).await.unwrap_err();
            assert_eq!(
                err,
                CredentialError::Ambiguous { tenant: a, exchange: "kraken".into(), matches: 2 },
                "live_only={live_only}"
            );
            // And the choke point passes the error through untouched.
            assert!(matches!(
                resolve_credential(&p, a, "kraken", live_only).await.unwrap_err(),
                CredentialError::Ambiguous { .. }
            ));
        }
        assert!(matches!(
            p.credentials_for(a, "coinbase", true).await.unwrap_err(),
            CredentialError::Ambiguous { matches: 2, .. }
        ));
        // B: live_only resolves cleanly to the production row; not ambiguous.
        assert_eq!(p.credentials_for(b, "kraken", true).await.unwrap().id, b_live.id);
        // ... but "any mode" cannot tell which one the caller wants.
        assert!(matches!(
            p.credentials_for(b, "kraken", false).await.unwrap_err(),
            CredentialError::Ambiguous { matches: 2, .. }
        ));
        // A's ambiguity does not spill onto C.
        assert_eq!(p.credentials_for(c, "kraken", true).await.unwrap().id, c_row.id);
        s.cleanup().await;
    }

    #[tokio::test]
    async fn a_table_without_tenant_id_makes_construction_fail() {
        // The PUBLIC schema shape.
        let s = scratch!(Shape::Public);
        s.insert_public().await;
        let err = match s.provider().await {
            Ok(_) => panic!("must not build over an untenanted table"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, CredentialError::SchemaNotTenantScoped(m) if m.contains("tenant_id")),
            "{err:?}"
        );
        s.cleanup().await;

        // tenant_id of the wrong type is refused too.
        let s = scratch!(Shape::TextTenant);
        assert!(matches!(
            s.provider().await.map(|_| ()).unwrap_err(),
            CredentialError::SchemaNotTenantScoped(m) if m.contains("uuid")
        ));
        s.cleanup().await;

        // No table at all (empty schema on the search_path).
        let Some(base) = base_url() else { return };
        use_test_key();
        let schema = format!("tc_{}", Uuid::new_v4().simple());
        let mut admin = AsyncPgConnection::establish(&base).await.unwrap();
        diesel::sql_query(format!("CREATE SCHEMA {schema}")).execute(&mut admin).await.unwrap();
        let sep = if base.contains('?') { '&' } else { '?' };
        let pool = create_pool(&format!("{base}{sep}options=-c%20search_path%3D{schema}")).await.unwrap();
        assert!(matches!(
            MultiTenantDbProvider::new(Arc::new(pool)).await.map(|_| ()).unwrap_err(),
            CredentialError::SchemaNotTenantScoped(_)
        ));
        diesel::sql_query(format!("DROP SCHEMA {schema} CASCADE")).execute(&mut admin).await.unwrap();
    }

    /// The schema is re-checked on every lookup: if the tenant column goes away
    /// under a running provider, nothing is read.
    #[tokio::test]
    async fn losing_the_tenant_column_after_construction_stops_all_reads() {
        let s = scratch!(PRIVATE);
        let a = Uuid::new_v4();
        s.insert(a, "kraken", "main", false, false, true).await;
        let p = s.ok_provider().await;
        assert!(p.credentials_for(a, "kraken", true).await.is_ok());
        s.exec("ALTER TABLE exchange_credentials DROP CONSTRAINT unique_tenant_exchange_label").await;
        s.exec("ALTER TABLE exchange_credentials DROP COLUMN tenant_id").await;
        assert!(matches!(
            p.credentials_for(a, "kraken", true).await.unwrap_err(),
            CredentialError::SchemaNotTenantScoped(_)
        ));
        assert!(matches!(
            p.all_credentials_for(a).await.unwrap_err(),
            CredentialError::SchemaNotTenantScoped(_)
        ));
        s.cleanup().await;
    }

    #[tokio::test]
    async fn decrypt_failure_is_a_backend_error_and_never_falls_back() {
        let s = scratch!(PRIVATE);
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        // A's only kraken row was encrypted with a key this process does not have.
        let foreign = fake("foreign-key");
        s.insert_raw(a, "kraken", "foreign", &encrypt(OTHER_KEY_HEX, &foreign), &encrypt(KEY_HEX, "x"), None, false, true)
            .await;
        // B has a perfectly good kraken row: it must NOT be used for A.
        let rb = s.insert(b, "kraken", "good", false, false, true).await;
        // C: plaintext in the column (neither aes: nor enc:).
        s.insert_raw(c, "kraken", "plain", "not-encrypted", &encrypt(KEY_HEX, "x"), None, false, true).await;
        // D: good key/secret but an undecryptable passphrase must not be dropped silently.
        let d = Uuid::new_v4();
        s.insert_raw(d, "kraken", "badpass", &encrypt(KEY_HEX, "k"), &encrypt(KEY_HEX, "s"), Some("aes:AAAA"), false, true)
            .await;
        // E: corrupt base64 after the prefix.
        let e = Uuid::new_v4();
        s.insert_raw(e, "kraken", "corrupt", "aes:!!!not-base64!!!", &encrypt(KEY_HEX, "s"), None, false, true).await;
        // F: legacy enc: rows keep working (no key needed).
        let f = Uuid::new_v4();
        let legacy_key = fake("legacy");
        s.insert_raw(f, "kraken", "legacy", &format!("enc:{}", STANDARD.encode(&legacy_key)), &format!("enc:{}", STANDARD.encode("s")), None, false, true)
            .await;
        let p = s.ok_provider().await;

        for who in [a, c, d, e] {
            let err = resolve_credential(&p, who, "kraken", true).await.unwrap_err();
            assert!(
                matches!(&err, CredentialError::Backend(m) if m.contains("could not be decrypted")),
                "{err:?}"
            );
            let all = p.all_credentials_for(who).await.unwrap_err();
            assert!(matches!(all, CredentialError::Backend(_)), "{all:?}");
        }
        // The message names the row, never the value.
        let msg = resolve_credential(&p, a, "kraken", true).await.unwrap_err().to_string();
        assert!(!msg.contains(&foreign) && !msg.contains("aes:"), "{msg}");
        // B is untouched by A's failure and gets only B's credential.
        assert_eq!(resolve_credential(&p, b, "kraken", true).await.unwrap().api_key, rb.key);
        assert_eq!(resolve_credential(&p, f, "kraken", true).await.unwrap().api_key, legacy_key);
        s.cleanup().await;
    }

    #[tokio::test]
    async fn secrets_never_appear_in_debug_errors_or_logs() {
        capture_logs();
        let s = scratch!(PRIVATE);
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let ra = s.insert(a, "kraken", "main", true, false, true).await;
        let bad_secret = fake("undecryptable");
        let bad_enc = encrypt(OTHER_KEY_HEX, &bad_secret);
        s.insert_raw(b, "kraken", "bad", &bad_enc, &bad_enc, None, false, true).await;
        let p = s.ok_provider().await;

        let cred = resolve_credential(&p, a, "kraken", true).await.unwrap();
        let err = resolve_credential(&p, b, "kraken", true).await.unwrap_err();
        let _ = p.all_credentials_for(a).await.unwrap();
        let _ = p.all_credentials_for(b).await;

        let mut surfaces = vec![format!("{:?}", cred), format!("{:#?}", cred), format!("{:?}", err), err.to_string()];
        surfaces.extend(CAPTURED.lock().unwrap().iter().cloned());
        let secrets = [
            ra.key.as_str(),
            ra.secret.as_str(),
            ra.pass.as_deref().unwrap(),
            bad_secret.as_str(),
            bad_enc.as_str(),
            KEY_HEX,
            OTHER_KEY_HEX,
        ];
        for text in &surfaces {
            for secret in secrets {
                assert!(!text.contains(secret), "secret leaked into: {text}");
            }
        }
        // ... and the redaction is in place, not merely absent.
        assert!(format!("{:?}", cred).contains("<redacted>"));
        s.cleanup().await;
    }

    #[tokio::test]
    async fn soft_deleted_rows_are_not_served_when_the_column_exists() {
        let s = scratch!(Shape::Private { deleted_at: true });
        let a = Uuid::new_v4();
        let dead = s.insert(a, "kraken", "dead", false, false, true).await;
        let live = s.insert(a, "coinbase", "live", false, false, true).await;
        s.exec(&format!("UPDATE exchange_credentials SET deleted_at = now() WHERE id = '{}'", dead.id)).await;
        let p = s.ok_provider().await;
        assert!(matches!(
            resolve_credential(&p, a, "kraken", true).await.unwrap_err(),
            CredentialError::NotFound { .. }
        ));
        assert_eq!(resolve_credential(&p, a, "coinbase", true).await.unwrap().id, live.id);
        assert_eq!(p.all_credentials_for(a).await.unwrap().len(), 1);
        // A deleted duplicate does not make the live row ambiguous.
        s.insert(a, "coinbase", "old", false, false, true).await;
        s.exec("UPDATE exchange_credentials SET deleted_at = now() WHERE label = 'old'").await;
        assert_eq!(resolve_credential(&p, a, "coinbase", true).await.unwrap().id, live.id);
        s.cleanup().await;
    }

    #[tokio::test]
    async fn all_credentials_for_is_scoped_ordered_and_includes_testnet() {
        let s = scratch!(PRIVATE);
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let k1 = s.insert(a, "kraken", "b-second", false, false, true).await;
        let k0 = s.insert(a, "kraken", "a-first", false, true, true).await;
        let c1 = s.insert(a, "coinbase", "main", true, false, true).await;
        s.insert(a, "binance", "off", false, false, false).await;
        s.insert(b, "kraken", "theirs", false, false, true).await;
        let p = s.ok_provider().await;
        let ids: Vec<Uuid> = resolve_all_credentials(&p, a).await.unwrap().iter().map(|c| c.id).collect();
        // ORDER BY exchange, label, id
        assert_eq!(ids, vec![c1.id, k0.id, k1.id]);
        assert_eq!(p.all_credentials_for(b).await.unwrap().len(), 1);
        assert!(p.all_credentials_for(Uuid::new_v4()).await.unwrap().is_empty());
        s.cleanup().await;
    }

    /// The tenant and exchange are bind parameters, never spliced into SQL.
    #[tokio::test]
    async fn hostile_exchange_names_are_just_names() {
        let s = scratch!(PRIVATE);
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        s.insert(b, "kraken", "theirs", false, false, true).await;
        let p = s.ok_provider().await;
        for hostile in ["kraken' OR '1'='1", "kraken'; DROP TABLE exchange_credentials; --", "%", "k%"] {
            let err = p.credentials_for(a, hostile, true).await.unwrap_err();
            assert!(matches!(err, CredentialError::NotFound { .. }), "{hostile}: {err:?}");
        }
        assert!(p.credentials_for(b, "kraken", true).await.is_ok(), "table still there");
        s.cleanup().await;
    }

    impl Scratch {
        /// Insert into the public-shaped table (no tenant column).
        async fn insert_public(&self) {
            let mut c = self.conn().await;
            diesel::sql_query(
                "INSERT INTO exchange_credentials (exchange, label, api_key_encrypted, api_secret_encrypted) \
                 VALUES ('kraken', 'x', 'enc:AA==', 'enc:AA==')",
            )
            .execute(&mut c)
            .await
            .unwrap();
        }
    }
}
