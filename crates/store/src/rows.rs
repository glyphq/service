//! RowBinary row shapes.
//!
//! These are deliberately separate from the public API rows: ClickHouse
//! `DateTime64(3)` needs a millisecond serde adapter, while the API keeps
//! RFC 3339 timestamps.

use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

use crate::{EventRow, IdentityRow, Result, TickRow, TransactionRow};

/// RowBinary encodes `FixedString(N)` as N raw bytes. Serde's array impl only
/// covers lengths up to 32, so serialize byte arrays as fixed-length tuples.
pub(crate) mod fixed_bytes {
    use serde::Serializer;
    use serde::ser::SerializeTuple;

    pub fn serialize<const N: usize, S>(bytes: &[u8; N], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut tuple = serializer.serialize_tuple(N)?;
        for byte in bytes {
            tuple.serialize_element(byte)?;
        }
        tuple.end()
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct TickInsert {
    pub epoch: u16,
    pub tick: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub computor_index: u16,
    pub transaction_count: u32,
    pub event_count: u32,
    #[serde(with = "fixed_bytes")]
    pub signature: [u8; 64],
    #[serde(with = "fixed_bytes")]
    pub tick_digest: [u8; 32],
    pub verification: String,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct TickSelect {
    pub epoch: u16,
    pub tick: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub computor_index: u16,
    pub transaction_count: u32,
    pub event_count: u32,
    pub verification: String,
}

impl From<TickSelect> for TickRow {
    fn from(row: TickSelect) -> Self {
        Self {
            epoch: row.epoch,
            tick: row.tick,
            timestamp: row.timestamp,
            computor_index: row.computor_index,
            transaction_count: row.transaction_count,
            event_count: row.event_count,
            verification: row.verification,
        }
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct TransactionInsert {
    pub epoch: u16,
    pub tick: u32,
    pub tx_index: u16,
    pub hash: String,
    pub source: String,
    pub destination: String,
    pub amount: i64,
    pub input_type: u16,
    pub input_data: String,
    pub status: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct TransactionSelect {
    pub epoch: u16,
    pub tick: u32,
    pub tx_index: u16,
    pub hash: String,
    pub source: String,
    pub destination: String,
    pub amount: i64,
    pub input_type: u16,
    pub input_data: String,
    pub status: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl From<TransactionSelect> for TransactionRow {
    fn from(row: TransactionSelect) -> Self {
        Self {
            epoch: row.epoch,
            tick: row.tick,
            tx_index: row.tx_index,
            hash: row.hash,
            source: row.source,
            destination: row.destination,
            amount: row.amount,
            input_type: row.input_type,
            input_data: row.input_data,
            status: row.status,
            timestamp: row.timestamp,
        }
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct EventInsert {
    pub epoch: u16,
    pub tick: u32,
    pub log_id: u64,
    pub log_type: u16,
    pub type_name: String,
    pub tx_hash: String,
    pub tx_index: i16,
    pub body: String,
    pub log_digest: u64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct EventSelect {
    pub epoch: u16,
    pub tick: u32,
    pub log_id: u64,
    pub log_type: u16,
    pub type_name: String,
    pub tx_hash: String,
    pub tx_index: i16,
    pub body: String,
    pub log_digest: u64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl From<EventSelect> for EventRow {
    fn from(row: EventSelect) -> Self {
        Self {
            epoch: row.epoch,
            tick: row.tick,
            log_id: row.log_id,
            log_type: row.log_type,
            type_name: row.type_name,
            tx_hash: row.tx_hash,
            tx_index: row.tx_index,
            body: row.body,
            log_digest: row.log_digest,
            timestamp: row.timestamp,
        }
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct IdentityInsert {
    pub identity: String,
    pub public_key: String,
    pub balance: i64,
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub transfer_count: u64,
    pub first_seen_tick: u32,
    pub last_seen_tick: u32,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct IdentitySelect {
    pub identity: String,
    pub public_key: String,
    pub balance: i64,
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub transfer_count: u64,
    pub first_seen_tick: u32,
    pub last_seen_tick: u32,
}

impl From<IdentitySelect> for IdentityRow {
    fn from(row: IdentitySelect) -> Self {
        Self {
            identity: row.identity,
            public_key: row.public_key,
            balance: row.balance,
            incoming_amount: row.incoming_amount,
            outgoing_amount: row.outgoing_amount,
            transfer_count: row.transfer_count,
            first_seen_tick: row.first_seen_tick,
            last_seen_tick: row.last_seen_tick,
        }
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct CursorInsert {
    pub key: String,
    pub value: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub updated_at: DateTime<Utc>,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct CursorSelect {
    pub value: String,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct CountRow {
    pub n: u64,
}

pub(crate) async fn insert_cursor(
    client: &clickhouse::Client,
    key: &str,
    value: &str,
    updated_at: DateTime<Utc>,
) -> Result<()> {
    let mut insert = client.insert::<CursorInsert>("ingest_state")?;
    insert
        .write(&CursorInsert {
            key: key.to_owned(),
            value: value.to_owned(),
            updated_at,
            version: next_cursor_version(updated_at),
        })
        .await?;
    insert.end().await?;
    Ok(())
}

/// Cursors use `ReplacingMergeTree(version)`. A process-local monotonic
/// counter keeps writes ordered even when they land in the same millisecond.
fn next_cursor_version(now: DateTime<Utc>) -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};

    static LAST: AtomicU64 = AtomicU64::new(0);

    let mut previous = LAST.load(Ordering::Relaxed);
    loop {
        let base = u64::try_from(now.timestamp_micros()).unwrap_or(0);
        let next = base.max(previous.saturating_add(1));
        match LAST.compare_exchange_weak(previous, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(actual) => previous = actual,
        }
    }
}
