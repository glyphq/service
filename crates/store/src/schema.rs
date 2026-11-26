//! ClickHouse schema. Kept as SQL so migrations are reviewable and replayable.
//!
//! Every table is a `ReplacingMergeTree` with an explicit `version` column so
//! that re-indexing the same logical row is idempotent. The projections are
//! read paths used by the API and are added with `IF NOT EXISTS`, which makes
//! [`statements`] safe to run on every startup.

pub const DATABASE: &str = "glyph";

/// Bumped whenever the DDL changes in a backwards-incompatible way.
pub const VERSION: &str = "1";

pub const CREATE_DATABASE: &str = r#"
CREATE DATABASE IF NOT EXISTS {database}
"#;

pub const CREATE_TICKS: &str = r#"
CREATE TABLE IF NOT EXISTS {database}.ticks
(
    epoch UInt16,
    tick UInt32,
    timestamp DateTime64(3, 'UTC'),
    computor_index UInt16,
    transaction_count UInt32,
    event_count UInt32,
    signature FixedString(64) DEFAULT '',
    tick_digest FixedString(32) DEFAULT '',
    verification LowCardinality(String),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY epoch
ORDER BY (epoch, tick)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
"#;

/// Projections on `ReplacingMergeTree` require a non-default dedup mode.
/// Kept separate from `CREATE_TICKS` so pre-existing tables are migrated too.
pub const ALTER_TICKS_SETTINGS: &str = r#"
ALTER TABLE {database}.ticks
MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild'
"#;

pub const CREATE_TICKS_BY_TICK: &str = r#"
ALTER TABLE {database}.ticks
ADD PROJECTION IF NOT EXISTS by_tick
(
    SELECT * ORDER BY tick
)
"#;

pub const CREATE_TRANSACTIONS: &str = r#"
CREATE TABLE IF NOT EXISTS {database}.transactions
(
    epoch UInt16,
    tick UInt32,
    tx_index UInt16,
    hash String,
    source String,
    destination String,
    amount Int64,
    input_type UInt16,
    input_data String CODEC(ZSTD(3)),
    status LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY epoch
ORDER BY (tick, tx_index)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
"#;

pub const ALTER_TRANSACTIONS_SETTINGS: &str = r#"
ALTER TABLE {database}.transactions
MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild'
"#;

pub const CREATE_TRANSACTIONS_BY_HASH: &str = r#"
ALTER TABLE {database}.transactions
ADD PROJECTION IF NOT EXISTS by_hash
(
    SELECT * ORDER BY hash
)
"#;

/// Identity history matches either side of a transfer. Two projections keep
/// both branches of the `source = ? OR destination = ?` union indexable; the
/// deduplication and final ordering remain in the query.
pub const CREATE_TRANSACTIONS_BY_SOURCE: &str = r#"
ALTER TABLE {database}.transactions
ADD PROJECTION IF NOT EXISTS by_source
(
    SELECT * ORDER BY (source, tick, tx_index)
)
"#;

pub const CREATE_TRANSACTIONS_BY_DESTINATION: &str = r#"
ALTER TABLE {database}.transactions
ADD PROJECTION IF NOT EXISTS by_destination
(
    SELECT * ORDER BY (destination, tick, tx_index)
)
"#;

pub const CREATE_EVENTS: &str = r#"
CREATE TABLE IF NOT EXISTS {database}.events
(
    epoch UInt16,
    tick UInt32,
    log_id UInt64,
    log_type UInt16,
    type_name LowCardinality(String),
    tx_hash String,
    tx_index Int16,
    body String CODEC(ZSTD(3)),
    log_digest UInt64,
    timestamp DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY epoch
ORDER BY (epoch, log_id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
"#;

pub const ALTER_EVENTS_SETTINGS: &str = r#"
ALTER TABLE {database}.events
MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild'
"#;

pub const CREATE_EVENTS_BY_TICK: &str = r#"
ALTER TABLE {database}.events
ADD PROJECTION IF NOT EXISTS by_tick
(
    SELECT * ORDER BY (epoch, tick, log_id)
)
"#;

/// Global recency does not have a useful primary key because events are
/// ordered by `(epoch, log_id)`. This projection provides `tick DESC,
/// log_id DESC` scans in reverse order.
pub const CREATE_EVENTS_RECENT: &str = r#"
ALTER TABLE {database}.events
ADD PROJECTION IF NOT EXISTS by_recent
(
    SELECT * ORDER BY (tick, log_id)
)
"#;

/// `event_type_counts` groups by `type_name`. The projection lets ClickHouse
/// scan only that column in type order instead of the natural tick order.
pub const CREATE_EVENTS_BY_TYPE: &str = r#"
ALTER TABLE {database}.events
ADD PROJECTION IF NOT EXISTS by_type
(
    SELECT * ORDER BY (type_name, epoch, log_id)
)
"#;

/// Events produced by one transaction are looked up by hash from the explorer
/// transaction page. The natural key is `(epoch, log_id)`, so this projection
/// turns the lookup into a bounded primary-key read.
pub const CREATE_EVENTS_BY_TX_HASH: &str = r#"
ALTER TABLE {database}.events
ADD PROJECTION IF NOT EXISTS by_tx_hash
(
    SELECT * ORDER BY (tx_hash, epoch, log_id)
)
"#;

/// Normalized `(identity, role)` index over event logs.
///
/// The events table keeps participants inside the JSON `body`, which cannot be
/// used as a primary key. This table duplicates each participant pair so that
/// per-identity history can be answered with a bounded primary-key read instead
/// of scanning every event. `role` is not part of the sort key, so a merge keeps
/// the highest `version` for one `(identity, tick, log_id)` triple.
pub const CREATE_EVENT_PARTICIPANTS: &str = r#"
CREATE TABLE IF NOT EXISTS {database}.event_participants
(
    epoch UInt16,
    tick UInt32,
    log_id UInt64,
    identity String,
    role LowCardinality(String),
    tx_hash String,
    tx_index Int16,
    timestamp DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY epoch
ORDER BY (identity, tick, log_id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild'
"#;

pub const ALTER_EVENT_PARTICIPANTS_SETTINGS: &str = r#"
ALTER TABLE {database}.event_participants
MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild'
"#;

/// Tick-scoped reads (`epoch, tick, log_id`) in tick order. The natural key
/// starts with `identity`, so without this projection a tick lookup scans the
/// whole partition.
pub const CREATE_EVENT_PARTICIPANTS_BY_TICK: &str = r#"
ALTER TABLE {database}.event_participants
ADD PROJECTION IF NOT EXISTS by_tick
(
    SELECT * ORDER BY (epoch, tick, log_id)
)
"#;

pub const CREATE_IDENTITIES: &str = r#"
CREATE TABLE IF NOT EXISTS {database}.identities
(
    identity String,
    public_key String,
    balance Int64,
    incoming_amount Int64,
    outgoing_amount Int64,
    transfer_count UInt64,
    first_seen_tick UInt32,
    last_seen_tick UInt32,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY identity
"#;

pub const ALTER_IDENTITIES_SETTINGS: &str = r#"
ALTER TABLE {database}.identities
MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild'
"#;

/// Last observed peer spectrum entity for one identity. This is deliberately
/// separate from `identities`: the indexed identity row is a net-flow
/// aggregate, while this table records the authoritative live balance and
/// spectrum metadata returned by a peer.
pub const CREATE_ENTITY_SNAPSHOTS: &str = r#"
CREATE TABLE IF NOT EXISTS {database}.entity_snapshots
(
    identity String,
    public_key String,
    spectrum_index Int32,
    tick UInt32,
    incoming_amount Int64,
    outgoing_amount Int64,
    incoming_transfers UInt64,
    outgoing_transfers UInt64,
    latest_incoming_tick UInt32,
    latest_outgoing_tick UInt32,
    observed_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY identity
"#;

/// `top_identities` orders by transfer count, so keep an activity-sorted copy
/// that can be scanned from the high end for small limits.
pub const CREATE_IDENTITIES_BY_ACTIVITY: &str = r#"
ALTER TABLE {database}.identities
ADD PROJECTION IF NOT EXISTS by_activity
(
    SELECT * ORDER BY (transfer_count, identity)
)
"#;

/// Generic key/value cursor storage. Cursors are read with `ORDER BY version
/// DESC LIMIT 1 BY key`, so the latest write always wins without `FINAL`.
pub const CREATE_INGEST_STATE: &str = r#"
CREATE TABLE IF NOT EXISTS {database}.ingest_state
(
    key String,
    value String,
    updated_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY key
"#;

/// The elected computor set for one epoch. Rows are keyed by
/// `(epoch, computor_index)` so replaying an epoch's computor message replaces
/// the previous set instead of appending duplicates.
pub const CREATE_COMPUTORS: &str = r#"
CREATE TABLE IF NOT EXISTS {database}.computors
(
    epoch UInt16,
    computor_index UInt16,
    public_key String,
    identity String,
    version UInt64,
    timestamp DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (epoch, computor_index)
"#;

/// Observed epoch lifecycle. Rows are keyed by `epoch`; a later `version`
/// replaces the earlier observation, so recording a start and closing an epoch
/// are both idempotent upserts.
///
/// Boundaries are observed from peer heads rather than assumed. `initial_tick`
/// is the first tick the network reported for the epoch, `first_tick` and
/// `last_tick` are the tick range the indexer actually persisted, and `closed`
/// flips once a later epoch has been observed.
pub const CREATE_EPOCHS: &str = r#"
CREATE TABLE IF NOT EXISTS {database}.epochs
(
    epoch UInt16,
    initial_tick UInt32,
    first_tick UInt32,
    last_tick UInt32,
    closed UInt8,
    updated_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY epoch
"#;

/// Tables partitioned by `epoch`. Retention drops whole partitions for these.
///
/// `identities` and `price_points` are deliberately absent: the first is
/// global state and the second is keyed by hour.
pub const EPOCH_PARTITIONED_TABLES: &[&str] = &[
    "ticks",
    "transactions",
    "events",
    "event_participants",
    "quorum_evidence",
];

/// Epoch-keyed metadata tables that are not partitioned. Retention deletes
/// their rows instead of dropping a partition.
pub const EPOCH_METADATA_TABLES: &[&str] = &["computors", "epochs"];

/// Vote evidence behind a stored tick. The digests are raw K12 values; vote
/// counts describe how the winning quorum was assembled. `warnings` is the
/// producer's serialized warning list, kept as one string so the row stays
/// flat for ClickHouse.
pub const CREATE_QUORUM_EVIDENCE: &str = r#"
CREATE TABLE IF NOT EXISTS {database}.quorum_evidence
(
    epoch UInt16,
    tick UInt32,
    quorum_digest FixedString(32),
    tick_data_digest FixedString(32),
    required_votes UInt32,
    aligned_votes UInt32,
    fetched_votes UInt32,
    unique_votes UInt32,
    verified_votes UInt32,
    verification LowCardinality(String),
    warning_count UInt32,
    warnings String,
    timestamp DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY epoch
ORDER BY (epoch, tick)
"#;

/// Hourly QUBIC/USDT oracle price points. Each `hour` is a logical row, so the
/// `ReplacingMergeTree(version)` sort key is the hour itself and reads use
/// `ORDER BY version DESC LIMIT 1 BY hour` to see the newest revision.
pub const CREATE_PRICE_POINTS: &str = r#"
CREATE TABLE IF NOT EXISTS {database}.price_points
(
    hour UInt32,
    append_tick UInt32,
    source_tick UInt32,
    epoch UInt16,
    flags UInt32,
    price_e8 Int64,
    source UInt32,
    synced UInt8,
    bound UInt8,
    series_digest FixedString(32) DEFAULT '',
    observed_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY hour
"#;

pub const ALL: &[&str] = &[
    CREATE_DATABASE,
    CREATE_TICKS,
    ALTER_TICKS_SETTINGS,
    CREATE_TICKS_BY_TICK,
    CREATE_TRANSACTIONS,
    ALTER_TRANSACTIONS_SETTINGS,
    CREATE_TRANSACTIONS_BY_HASH,
    CREATE_TRANSACTIONS_BY_SOURCE,
    CREATE_TRANSACTIONS_BY_DESTINATION,
    CREATE_EVENTS,
    ALTER_EVENTS_SETTINGS,
    CREATE_EVENTS_BY_TICK,
    CREATE_EVENTS_RECENT,
    CREATE_EVENTS_BY_TYPE,
    CREATE_EVENTS_BY_TX_HASH,
    CREATE_EVENT_PARTICIPANTS,
    ALTER_EVENT_PARTICIPANTS_SETTINGS,
    CREATE_EVENT_PARTICIPANTS_BY_TICK,
    CREATE_IDENTITIES,
    ALTER_IDENTITIES_SETTINGS,
    CREATE_ENTITY_SNAPSHOTS,
    CREATE_IDENTITIES_BY_ACTIVITY,
    CREATE_INGEST_STATE,
    CREATE_COMPUTORS,
    CREATE_EPOCHS,
    CREATE_QUORUM_EVIDENCE,
    CREATE_PRICE_POINTS,
];

/// Validates a database name before it is interpolated into DDL.
pub fn validate_database(database: &str) -> std::result::Result<(), String> {
    if database.is_empty() {
        return Err("database name is empty".into());
    }
    let mut chars = database.chars();
    let first = chars.next().expect("non-empty");
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(format!(
            "database name must start with a letter or underscore: {database}"
        ));
    }
    if let Some(bad) = chars.find(|c| !(c.is_ascii_alphanumeric() || *c == '_')) {
        return Err(format!(
            "database name contains an invalid character {bad:?}: {database}"
        ));
    }
    if database.len() > 128 {
        return Err("database name is longer than 128 bytes".into());
    }
    Ok(())
}

/// Renders every schema statement for `database`, in execution order.
pub fn statements(database: &str) -> std::result::Result<Vec<String>, String> {
    validate_database(database)?;
    Ok(ALL
        .iter()
        .map(|sql| sql.replace("{database}", database))
        .collect())
}

pub fn create_database(database: &str) -> String {
    CREATE_DATABASE.replace("{database}", database)
}

pub fn drop_database(database: &str) -> String {
    format!("DROP DATABASE IF EXISTS {database}")
}

/// `ALTER TABLE ... DROP PARTITION` for one epoch.
///
/// ClickHouse does not accept `IF EXISTS` on `DROP PARTITION`, so callers must
/// check [`crate::schema::EPOCH_PARTITIONED_TABLES`] partitions with
/// `system.parts` first. That check is what keeps retention idempotent when an
/// epoch had no rows in a table.
pub fn drop_epoch_partition(database: &str, table: &str, epoch: u16) -> String {
    format!("ALTER TABLE {database}.{table} DROP PARTITION {epoch}")
}

/// Removes the rows for one epoch from a non-partitioned metadata table.
pub fn delete_epoch_rows(database: &str, table: &str, epoch: u16) -> String {
    format!("ALTER TABLE {database}.{table} DELETE WHERE epoch = {epoch}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_all_statements() {
        let statements = statements("glyph").expect("valid database");
        assert_eq!(statements.len(), ALL.len());
        assert!(statements.iter().all(|sql| !sql.contains("{database}")));
        assert!(statements[0].contains("glyph"));
    }

    #[test]
    fn rejects_injection_attempts() {
        assert!(validate_database("glyph; DROP DATABASE default").is_err());
        assert!(validate_database("1glyph").is_err());
        assert!(validate_database("").is_err());
        assert!(validate_database("glyph_2").is_ok());
    }

    #[test]
    fn epochs_table_is_created_once() {
        assert_eq!(ALL.iter().filter(|sql| sql.contains(".epochs")).count(), 1);
        assert!(CREATE_EPOCHS.contains("initial_tick UInt32"));
        assert!(CREATE_EPOCHS.contains("ReplacingMergeTree(version)"));
    }

    #[test]
    fn retention_helpers_target_only_epoch_data() {
        assert_eq!(
            drop_epoch_partition("glyph", "ticks", 7),
            "ALTER TABLE glyph.ticks DROP PARTITION 7"
        );
        assert_eq!(
            delete_epoch_rows("glyph", "computors", 7),
            "ALTER TABLE glyph.computors DELETE WHERE epoch = 7"
        );
        assert!(!EPOCH_PARTITIONED_TABLES.contains(&"identities"));
        assert!(!EPOCH_PARTITIONED_TABLES.contains(&"price_points"));
        assert!(!EPOCH_METADATA_TABLES.contains(&"identities"));
        assert!(!EPOCH_METADATA_TABLES.contains(&"price_points"));
    }
}
