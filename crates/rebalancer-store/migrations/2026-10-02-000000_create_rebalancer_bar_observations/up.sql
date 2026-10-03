-- The first-seen-latency / revision recorder's tables (SignalEngine W9.1; COUNCIL_DATA_GATE_2026_09_26.md R20,
-- R25 precondition (6) and work item G0, R26 item 4; COUNCIL_ETF_TIMING_AND_CADENCE.md Rulings 7 and 8(6)).
--
-- WHERE THIS FILE LIVES AND HOW IT IS APPLIED. Every other rebalancer_* table is created by a migration in the
-- databaseschema-internal repository (`migrations/2026-09-22-010000_create_rebalancer_service_tables`,
-- `2026-09-27-000000_create_rebalancer_pilot_ledger`), and merging a migration there to `main` applies it to the
-- live cluster after the owner's dry run. This file is written in that repository's Diesel layout (a dated
-- directory with up.sql / down.sql) so the owner can copy the directory into databaseschema-internal/migrations
-- VERBATIM; it is kept here, beside the Rust that depends on it (`crates/rebalancer-store/src/latency_store.rs`),
-- because this change is a SignalEngine-only slice. Until it is applied, `PgLatencyStore` fails every call with
-- `LATENCY_STORE_UNAVAILABLE` naming this directory, the recorder logs it and raises one Warning alert, and no run is
-- affected (the recorder is an observer). The env-gated test `tests/latency_store_db.rs` applies this file to a
-- scratch database on top of the real migrations.
--
-- What is here:
--   1. rebalancer_bar_first_seen -- the FIRST sighting of each (source, instrument, bar_date): when the driver tick
--      first saw the bar, the venue's nominal close of that date, the latency between them, the values as first
--      shown, the tick, the response hash and the policy version. One row per key (UNIQUE), never updated.
--   2. rebalancer_bar_revisions -- APPENDED whenever a bar already on file comes back with different values: the
--      old and the new values, which fields changed, when. The first-sighting row is never edited.
--
-- Conventions, and the one deliberate deviation from the other rebalancer_* tables:
--   * APPEND-ONLY: UPDATE, DELETE and TRUNCATE are refused by reject_update_or_delete() (defined by
--     databaseschema-internal `2026-09-22-000004_create_mandate_tables`, as the decision ledger uses it).
--   * NO tenant_id. Vendor bars are tenant-independent (`DataSource::sleeve_data` has no tenant argument; R26 item 5
--     "provenance rows are shared"), and storing a copy of vendor bars per tenant is exactly what R21(c) forbids
--     (licensing: "no raw vendor bars per tenant"). Like rebalancer_kill_flags, this is a platform-level table, and
--     nothing tenant-facing reads it: it feeds the paper-cycle gate's evidence summary only.
--   * The values are DOUBLE PRECISION because the recorder's revision rule is EXACT IEEE-754 equality on the f64 the
--     vendor parser produced; a NUMERIC round-trip could manufacture or hide a difference.
--   * The policy (what is sampled, what counts as a revision, the 20-session threshold) is NOT in the database: it
--     is `rebalancer_run::latency::policy`, pre-registered 2026-10-02 and stamped on every row as policy_version.
--
-- The role that runs the service needs SELECT and INSERT on both tables, nothing else:
--   GRANT SELECT, INSERT ON rebalancer_bar_first_seen, rebalancer_bar_revisions TO <service role>;

-- ---------------------------------------------------------------------------------------
-- 1. rebalancer_bar_first_seen
-- ---------------------------------------------------------------------------------------
CREATE TABLE rebalancer_bar_first_seen (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    seq BIGINT GENERATED ALWAYS AS IDENTITY,
    -- The vendor source id (`massive`); a second source gets its own rows.
    source VARCHAR(32) NOT NULL CHECK (char_length(btrim(source)) > 0),
    -- The canonical symbol of the panel (`SPY`, `BTC`), never the vendor ticker.
    instrument VARCHAR(32) NOT NULL CHECK (char_length(btrim(instrument)) > 0),
    bar_date DATE NOT NULL,
    -- The venue convention the latency is measured from (ETF: 16:00 America/New_York of bar_date; crypto: 00:00
    -- UTC of bar_date + 1), supplied by the source, see market-data `time::nominal_close_at`.
    nominal_close_at TIMESTAMPTZ NOT NULL,
    -- The clock of the driver tick that first saw the bar (resolution = the tick interval).
    first_seen_at TIMESTAMPTZ NOT NULL,
    -- first_seen_at - nominal_close_at, whole seconds, as the recorder computed it (checked below).
    latency_secs BIGINT NOT NULL,
    -- The values as first shown. close is mandatory (the parser refuses a bar without one); the others are recorded
    -- when the vendor sent them.
    open DOUBLE PRECISION CHECK (open IS NULL OR open > 0),
    high DOUBLE PRECISION CHECK (high IS NULL OR high > 0),
    low DOUBLE PRECISION CHECK (low IS NULL OR low > 0),
    close DOUBLE PRECISION NOT NULL CHECK (close > 0),
    volume DOUBLE PRECISION CHECK (volume IS NULL OR volume >= 0),
    -- The driver tick (its clock, RFC 3339); there is no per-tick run key.
    run_id VARCHAR(64) NOT NULL CHECK (char_length(btrim(run_id)) > 0),
    -- SHA-256 of the raw vendor response the sighting came from (R26 item 6), when the source has one.
    response_sha256 VARCHAR(64) CHECK (response_sha256 IS NULL OR response_sha256 ~ '^[0-9a-f]{64}$'),
    policy_version VARCHAR(64) NOT NULL CHECK (char_length(btrim(policy_version)) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT rebalancer_bar_first_seen_uniq UNIQUE (source, instrument, bar_date),
    -- The stored latency is the stored instants' difference (whole seconds, truncated toward zero).
    CONSTRAINT rebalancer_bar_first_seen_latency_consistent
        CHECK (abs(latency_secs - EXTRACT(EPOCH FROM (first_seen_at - nominal_close_at))) < 1)
);

CREATE INDEX rebalancer_bar_first_seen_source_idx ON rebalancer_bar_first_seen (source, instrument, bar_date);

CREATE TRIGGER rebalancer_bar_first_seen_append_only
    BEFORE UPDATE OR DELETE ON rebalancer_bar_first_seen
    FOR EACH ROW
    EXECUTE FUNCTION reject_update_or_delete();
CREATE TRIGGER rebalancer_bar_first_seen_no_truncate
    BEFORE TRUNCATE ON rebalancer_bar_first_seen
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_update_or_delete();

-- ---------------------------------------------------------------------------------------
-- 2. rebalancer_bar_revisions
-- ---------------------------------------------------------------------------------------
CREATE TABLE rebalancer_bar_revisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Total order of the revisions of one bar: the newest row (highest seq) holds the values now on file.
    seq BIGINT GENERATED ALWAYS AS IDENTITY,
    source VARCHAR(32) NOT NULL,
    instrument VARCHAR(32) NOT NULL,
    bar_date DATE NOT NULL,
    -- The clock of the tick that saw the changed values.
    seen_at TIMESTAMPTZ NOT NULL,
    -- The values on file before this sighting (the first sighting's, or the previous revision's new values) ...
    old_open DOUBLE PRECISION,
    old_high DOUBLE PRECISION,
    old_low DOUBLE PRECISION,
    old_close DOUBLE PRECISION NOT NULL,
    old_volume DOUBLE PRECISION,
    -- ... and the values seen.
    new_open DOUBLE PRECISION CHECK (new_open IS NULL OR new_open > 0),
    new_high DOUBLE PRECISION CHECK (new_high IS NULL OR new_high > 0),
    new_low DOUBLE PRECISION CHECK (new_low IS NULL OR new_low > 0),
    new_close DOUBLE PRECISION NOT NULL CHECK (new_close > 0),
    new_volume DOUBLE PRECISION CHECK (new_volume IS NULL OR new_volume >= 0),
    -- Which of open/high/low/close/volume differed (exact equality), at least one.
    changed_fields TEXT[] NOT NULL CHECK (cardinality(changed_fields) >= 1),
    run_id VARCHAR(64) NOT NULL CHECK (char_length(btrim(run_id)) > 0),
    response_sha256 VARCHAR(64) CHECK (response_sha256 IS NULL OR response_sha256 ~ '^[0-9a-f]{64}$'),
    policy_version VARCHAR(64) NOT NULL CHECK (char_length(btrim(policy_version)) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- A revision is of a bar that was first seen (RESTRICT: a first sighting with revisions can never be deleted,
    -- not that the trigger would let it be).
    CONSTRAINT rebalancer_bar_revisions_first_seen_fk FOREIGN KEY (source, instrument, bar_date)
        REFERENCES rebalancer_bar_first_seen (source, instrument, bar_date) ON DELETE RESTRICT
);

CREATE INDEX rebalancer_bar_revisions_bar_idx ON rebalancer_bar_revisions (source, instrument, bar_date, seq DESC);

CREATE TRIGGER rebalancer_bar_revisions_append_only
    BEFORE UPDATE OR DELETE ON rebalancer_bar_revisions
    FOR EACH ROW
    EXECUTE FUNCTION reject_update_or_delete();
CREATE TRIGGER rebalancer_bar_revisions_no_truncate
    BEFORE TRUNCATE ON rebalancer_bar_revisions
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_update_or_delete();
