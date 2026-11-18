//! ClickHouse-backed storage for the glyph indexer and API.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub mod rows;
pub mod schema;
pub mod writer;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("clickhouse: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),
    #[error("configuration: {0}")]
    Config(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid batch: {0}")]
    InvalidBatch(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Cursor key advanced after a tick has been fully persisted.
pub const CURSOR_HEAD: &str = "head";
/// Cursor key holding the typed [`IngestCursor`] JSON document.
pub const CURSOR_INGEST: &str = "ingest";

/// JSON value stored under [`CURSOR_HEAD`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadCursor {
    pub epoch: u16,
    pub tick: u32,
    pub timestamp: DateTime<Utc>,
}

/// Typed ingest progress stored under [`CURSOR_INGEST`].
///
/// `last_indexed_tick` is the highest fully persisted tick. Writers never move
/// it backwards, so replaying an older tick cannot corrupt the cursor.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestCursor {
    pub last_indexed_tick: Option<u32>,
    pub epoch: Option<u16>,
    pub verification: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickRow {
    pub epoch: u16,
    pub tick: u32,
    pub timestamp: DateTime<Utc>,
    pub computor_index: u16,
    pub transaction_count: u32,
    pub event_count: u32,
    pub verification: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionRow {
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
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRow {
    pub epoch: u16,
    pub tick: u32,
    pub log_id: u64,
    pub log_type: u16,
    pub type_name: String,
    pub tx_hash: String,
    pub tx_index: i16,
    pub body: String,
    pub log_digest: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityRow {
    pub identity: String,
    pub public_key: String,
    pub balance: i64,
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub transfer_count: u64,
    pub first_seen_tick: u32,
    pub last_seen_tick: u32,
}

/// A verified tick and everything derived from it.
///
/// Identity rows are absolute snapshots as of `tick`, not deltas, so replaying
/// the same batch is a no-op. Transaction and event rows must carry the same
/// tick as the batch; the writer uses the batch version for all of them.
#[derive(Debug, Clone)]
pub struct TickBatch {
    pub epoch: u16,
    pub tick: u32,
    pub timestamp: DateTime<Utc>,
    pub computor_index: u16,
    pub verification: String,
    /// ReplacingMergeTree version. Defaults to the tick number; bump it when
    /// replacing a tick whose contents changed (for example after a reorg).
    pub version: u64,
    /// Raw SchnorrQ signature over the tick.
    pub signature: [u8; 64],
    /// K12 digest of the tick.
    pub tick_digest: [u8; 32],
    pub transactions: Vec<TransactionRow>,
    pub events: Vec<EventRow>,
    pub identities: Vec<IdentityRow>,
}

impl TickBatch {
    pub fn new(
        epoch: u16,
        tick: u32,
        timestamp: DateTime<Utc>,
        computor_index: u16,
        verification: impl Into<String>,
    ) -> Self {
        Self {
            epoch,
            tick,
            timestamp,
            computor_index,
            verification: verification.into(),
            version: u64::from(tick),
            signature: [0; 64],
            tick_digest: [0; 32],
            transactions: Vec::new(),
            events: Vec::new(),
            identities: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOutcome {
    pub tick: u32,
    pub version: u64,
    pub transactions: usize,
    pub events: usize,
    pub identities: usize,
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn init(&self) -> Result<()>;
    async fn drop_database(&self) -> Result<()>;
    async fn head(&self) -> Result<Option<TickRow>>;
    async fn tick(&self, tick: u32) -> Result<Option<TickRow>>;
    async fn transactions_for_tick(
        &self,
        tick: u32,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<TransactionRow>>;
    async fn transaction(&self, hash: &str) -> Result<Option<TransactionRow>>;
    async fn events_for_tick(&self, tick: u32, limit: u32, offset: u64) -> Result<Vec<EventRow>>;
    async fn identity(&self, identity: &str) -> Result<Option<IdentityRow>>;
    async fn stats(&self) -> Result<Value>;

    /// Persist a fully verified tick and advance the head cursor.
    async fn write_tick(&self, batch: &TickBatch) -> Result<WriteOutcome>;
    /// Read an ingest cursor. Values are opaque strings owned by the caller.
    async fn cursor(&self, key: &str) -> Result<Option<String>>;
    async fn set_cursor(&self, key: &str, value: &str) -> Result<()>;
    /// Typed getter for the last indexed tick, epoch, and verification level.
    async fn ingest_cursor(&self) -> Result<IngestCursor>;
    async fn set_ingest_cursor(&self, cursor: &IngestCursor) -> Result<()>;
}

pub struct ClickHouseStore {
    pub client: clickhouse::Client,
    bootstrap: clickhouse::Client,
    database: String,
}

impl ClickHouseStore {
    pub fn new(url: &str, database: &str) -> Result<Self> {
        if url.is_empty() {
            return Err(Error::Config("clickhouse url is empty".into()));
        }
        schema::validate_database(database).map_err(Error::Config)?;
        // ClickHouse 26 answers `compress=1` with ZSTD while clickhouse-rs 0.13
        // can only decode LZ4. Leave the wire uncompressed until the client
        // supports ZSTD responses.
        let client = clickhouse::Client::default()
            .with_url(url)
            .with_database(database)
            .with_compression(clickhouse::Compression::None);
        // DDL that creates the database cannot be scoped to a database that
        // does not exist yet.
        let bootstrap = clickhouse::Client::default()
            .with_url(url)
            .with_compression(clickhouse::Compression::None);
        Ok(Self {
            client,
            bootstrap,
            database: database.to_owned(),
        })
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    async fn count(&self, query: &str) -> Result<u64> {
        let row = self
            .client
            .query(query)
            .fetch_one::<rows::CountRow>()
            .await?;
        Ok(row.n)
    }
}

#[async_trait]
impl Store for ClickHouseStore {
    async fn init(&self) -> Result<()> {
        self.bootstrap
            .query(&schema::create_database(&self.database))
            .execute()
            .await?;
        let statements = schema::statements(&self.database).map_err(Error::Config)?;
        for statement in statements {
            self.client.query(&statement).execute().await?;
        }
        Ok(())
    }

    async fn drop_database(&self) -> Result<()> {
        self.bootstrap
            .query(&schema::drop_database(&self.database))
            .execute()
            .await?;
        Ok(())
    }

    async fn head(&self) -> Result<Option<TickRow>> {
        let row = self
            .client
            .query("SELECT ?fields FROM ticks ORDER BY tick DESC, version DESC LIMIT 1")
            .fetch_optional::<rows::TickSelect>()
            .await?;
        Ok(row.map(Into::into))
    }

    async fn tick(&self, tick: u32) -> Result<Option<TickRow>> {
        let row = self
            .client
            .query("SELECT ?fields FROM ticks WHERE tick = ? ORDER BY version DESC LIMIT 1")
            .bind(tick)
            .fetch_optional::<rows::TickSelect>()
            .await?;
        Ok(row.map(Into::into))
    }

    async fn transactions_for_tick(
        &self,
        tick: u32,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<TransactionRow>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM transactions WHERE tick = ? \
                     ORDER BY version DESC LIMIT 1 BY tick, tx_index\
                 ) ORDER BY tx_index LIMIT ? OFFSET ?",
            )
            .bind(tick)
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::TransactionSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn transaction(&self, hash: &str) -> Result<Option<TransactionRow>> {
        let row = self
            .client
            .query("SELECT ?fields FROM transactions WHERE hash = ? ORDER BY version DESC LIMIT 1")
            .bind(hash)
            .fetch_optional::<rows::TransactionSelect>()
            .await?;
        Ok(row.map(Into::into))
    }

    async fn events_for_tick(&self, tick: u32, limit: u32, offset: u64) -> Result<Vec<EventRow>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM events WHERE tick = ? \
                     ORDER BY version DESC LIMIT 1 BY epoch, log_id\
                 ) ORDER BY log_id LIMIT ? OFFSET ?",
            )
            .bind(tick)
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::EventSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn identity(&self, identity: &str) -> Result<Option<IdentityRow>> {
        let row = self
            .client
            .query(
                "SELECT ?fields FROM identities WHERE identity = ? ORDER BY version DESC LIMIT 1",
            )
            .bind(identity)
            .fetch_optional::<rows::IdentitySelect>()
            .await?;
        Ok(row.map(Into::into))
    }

    async fn stats(&self) -> Result<Value> {
        let head = self.head().await?;
        // uniqExact on the sorting keys is exact and still avoids FINAL, so
        // replayed ticks are not double counted before merges run.
        let ticks = self.count("SELECT uniqExact(tick) AS n FROM ticks").await?;
        let transactions = self
            .count("SELECT uniqExact(tick, tx_index) AS n FROM transactions")
            .await?;
        let events = self
            .count("SELECT uniqExact(log_id) AS n FROM events")
            .await?;
        let identities = self
            .count("SELECT uniqExact(identity) AS n FROM identities")
            .await?;
        let cursor = match self.cursor(CURSOR_HEAD).await? {
            Some(value) => serde_json::from_str(&value).unwrap_or_else(|_| json!(value)),
            None => Value::Null,
        };
        Ok(json!({
            "head": head,
            "counts": {
                "ticks": ticks,
                "transactions": transactions,
                "events": events,
                "identities": identities,
            },
            "cursor": cursor,
        }))
    }

    async fn write_tick(&self, batch: &TickBatch) -> Result<WriteOutcome> {
        writer::write_tick(self, batch).await
    }

    async fn cursor(&self, key: &str) -> Result<Option<String>> {
        let row = self
            .client
            .query("SELECT value FROM ingest_state WHERE key = ? ORDER BY version DESC LIMIT 1")
            .bind(key)
            .fetch_optional::<rows::CursorSelect>()
            .await?;
        Ok(row.map(|row| row.value))
    }

    async fn set_cursor(&self, key: &str, value: &str) -> Result<()> {
        rows::insert_cursor(&self.client, key, value, Utc::now()).await
    }

    async fn ingest_cursor(&self) -> Result<IngestCursor> {
        match self.cursor(CURSOR_INGEST).await? {
            Some(value) => serde_json::from_str(&value)
                .map_err(|err| Error::Config(format!("invalid ingest cursor: {err}"))),
            None => Ok(IngestCursor::default()),
        }
    }

    async fn set_ingest_cursor(&self, cursor: &IngestCursor) -> Result<()> {
        let mut cursor = cursor.clone();
        if cursor.updated_at.is_none() {
            cursor.updated_at = Some(Utc::now());
        }
        let value = serde_json::to_string(&cursor)
            .map_err(|err| Error::Config(format!("serialize ingest cursor: {err}")))?;
        self.set_cursor(CURSOR_INGEST, &value).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_url_and_unsafe_database() {
        assert!(ClickHouseStore::new("", "glyph").is_err());
        assert!(ClickHouseStore::new("http://127.0.0.1:8123", "glyph; DROP").is_err());
        assert!(ClickHouseStore::new("http://127.0.0.1:8123", "glyph").is_ok());
    }

    #[test]
    fn version_tracks_tick() {
        let batch = TickBatch::new(1, 42, Utc::now(), 3, "quorum");
        assert_eq!(batch.version, 42);
    }
}
