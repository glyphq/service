//! ClickHouse schema. Kept as SQL so migrations are reviewable and replayable.

pub const DATABASE: &str = "glyph";

pub const CREATE_DATABASE: &str = r#"
CREATE DATABASE IF NOT EXISTS glyph
"#;

pub const CREATE_TICKS: &str = r#"
CREATE TABLE IF NOT EXISTS glyph.ticks
(
    epoch UInt16,
    tick UInt32,
    timestamp DateTime64(3, 'UTC'),
    computor_index UInt16,
    transaction_count UInt32,
    event_count UInt32,
    signature FixedString(64),
    tick_digest FixedString(32),
    verification LowCardinality(String),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY epoch
ORDER BY (epoch, tick)
"#;

pub const CREATE_TRANSACTIONS: &str = r#"
CREATE TABLE IF NOT EXISTS glyph.transactions
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
"#;

pub const CREATE_TRANSACTIONS_BY_HASH: &str = r#"
ALTER TABLE glyph.transactions
ADD PROJECTION IF NOT EXISTS by_hash
(
    SELECT * ORDER BY hash
)
"#;

pub const CREATE_EVENTS: &str = r#"
CREATE TABLE IF NOT EXISTS glyph.events
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
"#;

pub const CREATE_EVENTS_BY_TICK: &str = r#"
ALTER TABLE glyph.events
ADD PROJECTION IF NOT EXISTS by_tick
(
    SELECT * ORDER BY (epoch, tick, log_id)
)
"#;

pub const CREATE_IDENTITIES: &str = r#"
CREATE TABLE IF NOT EXISTS glyph.identities
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

pub const CREATE_INGEST_STATE: &str = r#"
CREATE TABLE IF NOT EXISTS glyph.ingest_state
(
    key String,
    value String,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY key
"#;

pub const ALL: &[&str] = &[
    CREATE_DATABASE,
    CREATE_TICKS,
    CREATE_TRANSACTIONS,
    CREATE_TRANSACTIONS_BY_HASH,
    CREATE_EVENTS,
    CREATE_EVENTS_BY_TICK,
    CREATE_IDENTITIES,
    CREATE_INGEST_STATE,
];
