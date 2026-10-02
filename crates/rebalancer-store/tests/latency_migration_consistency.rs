//! The latency recorder's migration (kept in this crate for the owner to copy into databaseschema-internal) and the
//! Rust store that depends on it must agree, without a database: the table names, every column the store writes,
//! the append-only triggers, the uniqueness the store's idempotent insert relies on, and the down file.

use std::path::{Path, PathBuf};

use rebalancer_store::latency_store::{FIRST_SEEN_TABLE, MIGRATION, REVISIONS_TABLE};

fn migration_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations")
        .join(MIGRATION)
}

fn up() -> String {
    std::fs::read_to_string(migration_dir().join("up.sql")).expect("up.sql exists beside the store")
}

fn code_lines(sql: &str) -> String {
    sql.lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn the_migration_creates_exactly_the_tables_the_store_names_and_both_are_append_only() {
    let sql = code_lines(&up());
    assert!(
        sql.contains(&format!("CREATE TABLE {FIRST_SEEN_TABLE} (")),
        "{FIRST_SEEN_TABLE} is created"
    );
    assert!(
        sql.contains(&format!("CREATE TABLE {REVISIONS_TABLE} (")),
        "{REVISIONS_TABLE} is created"
    );
    assert_eq!(sql.matches("CREATE TABLE ").count(), 2, "no other table");
    for table in [FIRST_SEEN_TABLE, REVISIONS_TABLE] {
        assert!(
            sql.contains(&format!("BEFORE UPDATE OR DELETE ON {table}")),
            "{table}: append-only trigger"
        );
        assert!(
            sql.contains(&format!("BEFORE TRUNCATE ON {table}")),
            "{table}: no truncate"
        );
    }
    assert!(
        sql.contains("UNIQUE (source, instrument, bar_date)"),
        "the first-sighting key the ON CONFLICT clause targets"
    );
    assert!(
        sql.contains("REFERENCES rebalancer_bar_first_seen (source, instrument, bar_date)"),
        "a revision is of a first-seen bar"
    );
    assert!(
        !sql.contains("tenant_id"),
        "platform-level by design (see the header): no tenant column"
    );
}

#[test]
fn every_column_the_store_writes_exists_in_the_migration() {
    let sql = code_lines(&up());
    let first_seen_block = sql
        .split(&format!("CREATE TABLE {FIRST_SEEN_TABLE} ("))
        .nth(1)
        .unwrap()
        .split(");")
        .next()
        .unwrap();
    for col in [
        "source",
        "instrument",
        "bar_date",
        "nominal_close_at",
        "first_seen_at",
        "latency_secs",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "run_id",
        "response_sha256",
        "policy_version",
    ] {
        assert!(
            first_seen_block
                .lines()
                .any(|l| l.trim_start().starts_with(&format!("{col} "))),
            "{FIRST_SEEN_TABLE}.{col}"
        );
    }
    let revisions_block = sql
        .split(&format!("CREATE TABLE {REVISIONS_TABLE} ("))
        .nth(1)
        .unwrap()
        .split(");")
        .next()
        .unwrap();
    for col in [
        "source",
        "instrument",
        "bar_date",
        "seen_at",
        "old_open",
        "old_high",
        "old_low",
        "old_close",
        "old_volume",
        "new_open",
        "new_high",
        "new_low",
        "new_close",
        "new_volume",
        "changed_fields",
        "run_id",
        "response_sha256",
        "policy_version",
    ] {
        assert!(
            revisions_block
                .lines()
                .any(|l| l.trim_start().starts_with(&format!("{col} "))),
            "{REVISIONS_TABLE}.{col}"
        );
    }
    assert!(
        revisions_block.contains("new_close DOUBLE PRECISION NOT NULL"),
        "the store reads a NULL new_close as 'no revision'"
    );
}

#[test]
fn the_down_file_drops_both_tables_in_dependency_order() {
    let down =
        code_lines(&std::fs::read_to_string(migration_dir().join("down.sql")).expect("down.sql"));
    let rev = down
        .find(&format!("DROP TABLE IF EXISTS {REVISIONS_TABLE}"))
        .expect("drops the revisions");
    let first = down
        .find(&format!("DROP TABLE IF EXISTS {FIRST_SEEN_TABLE}"))
        .expect("drops the first sightings");
    assert!(rev < first, "the referencing table goes first");
}

#[test]
fn the_migration_header_says_where_it_must_be_applied_from() {
    let header = up();
    assert!(
        header.contains("databaseschema-internal"),
        "names the repository whose migrations create the other rebalancer tables"
    );
    assert!(
        header.contains(
            "GRANT SELECT, INSERT ON rebalancer_bar_first_seen, rebalancer_bar_revisions"
        ),
        "states the service role's grants"
    );
}
