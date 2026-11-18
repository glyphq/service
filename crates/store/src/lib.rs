use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("clickhouse: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),
    #[error("configuration: {0}")]
    Config(String),
    #[error("not found: {0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickRow {
    pub epoch: u16,
    pub tick: u32,
    pub timestamp: DateTime<Utc>,
    pub computor_index: u16,
    pub transaction_count: u32,
    pub event_count: u32,
    pub verification: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRow {
    pub epoch: u16,
    pub tick: u32,
    pub log_id: u64,
    pub log_type: u16,
    pub tx_hash: String,
    pub body: String,
    pub log_digest: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[async_trait]
pub trait Store: Send + Sync {
    async fn init(&self) -> Result<()>;
    async fn head(&self) -> Result<Option<TickRow>>;
    async fn tick(&self, tick: u32) -> Result<Option<TickRow>>;
    async fn transactions_for_tick(&self, tick: u32, limit: u32, offset: u64) -> Result<Vec<TransactionRow>>;
    async fn transaction(&self, hash: &str) -> Result<Option<TransactionRow>>;
    async fn events_for_tick(&self, tick: u32, limit: u32, offset: u64) -> Result<Vec<EventRow>>;
    async fn identity(&self, identity: &str) -> Result<Option<IdentityRow>>;
    async fn stats(&self) -> Result<serde_json::Value>;
}

pub struct ClickHouseStore {
    pub client: clickhouse::Client,
}

impl ClickHouseStore {
    pub fn new(url: &str, database: &str) -> Result<Self> {
        if url.is_empty() {
            return Err(Error::Config("clickhouse url is empty".into()));
        }
        let client = clickhouse::Client::default()
            .with_url(url)
            .with_database(database);
        Ok(Self { client })
    }
}

#[async_trait]
impl Store for ClickHouseStore {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn head(&self) -> Result<Option<TickRow>> {
        Ok(None)
    }

    async fn tick(&self, _tick: u32) -> Result<Option<TickRow>> {
        Ok(None)
    }

    async fn transactions_for_tick(&self, _tick: u32, _limit: u32, _offset: u64) -> Result<Vec<TransactionRow>> {
        Ok(Vec::new())
    }

    async fn transaction(&self, _hash: &str) -> Result<Option<TransactionRow>> {
        Ok(None)
    }

    async fn events_for_tick(&self, _tick: u32, _limit: u32, _offset: u64) -> Result<Vec<EventRow>> {
        Ok(Vec::new())
    }

    async fn identity(&self, _identity: &str) -> Result<Option<IdentityRow>> {
        Ok(None)
    }

    async fn stats(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({}))
    }
}

pub mod schema;
