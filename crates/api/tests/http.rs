use std::sync::Arc;

use api::{AppState, router};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use serde_json::Value;
use store::{
    EventRow, IdentityRow, IngestCursor, Result as StoreResult, Store, TickBatch, TickRow,
    TransactionRow, WriteOutcome,
};
use tower::ServiceExt;

const IDENTITY: &str = "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK";
const HASH: &str = "yqcvscbbnhotdgqfpvcaniwcppcenlphwdxqqrewmbsfvvbpojfsnvwazgvb";

struct MockStore {
    tick: TickRow,
    transactions: Vec<TransactionRow>,
    events: Vec<EventRow>,
}

impl MockStore {
    fn new() -> Self {
        let now = Utc::now();
        let tick = TickRow {
            epoch: 1,
            tick: 7,
            timestamp: now,
            computor_index: 3,
            transaction_count: 2,
            event_count: 1,
            verification: "data".into(),
        };
        let transactions = (0..2)
            .map(|index| TransactionRow {
                epoch: 1,
                tick: 7,
                tx_index: index,
                hash: format!("{HASH}{index}"),
                source: IDENTITY.into(),
                destination: IDENTITY.into(),
                amount: 42,
                input_type: 0,
                input_data: String::new(),
                status: "success".into(),
                timestamp: now,
            })
            .collect();
        let events = vec![EventRow {
            epoch: 1,
            tick: 7,
            log_id: 9,
            log_type: 0,
            type_name: "qu_transfer".into(),
            tx_hash: format!("{HASH}0"),
            tx_index: 0,
            body: r#"{"amount":"42"}"#.into(),
            log_digest: 1,
            timestamp: now,
        }];
        Self {
            tick,
            transactions,
            events,
        }
    }
}

#[async_trait]
impl Store for MockStore {
    async fn init(&self) -> StoreResult<()> {
        Ok(())
    }

    async fn drop_database(&self) -> StoreResult<()> {
        Ok(())
    }

    async fn head(&self) -> StoreResult<Option<TickRow>> {
        Ok(Some(self.tick.clone()))
    }

    async fn tick(&self, tick: u32) -> StoreResult<Option<TickRow>> {
        Ok((tick == self.tick.tick).then(|| self.tick.clone()))
    }

    async fn transactions_for_tick(
        &self,
        _tick: u32,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        Ok(page(&self.transactions, limit, offset))
    }

    async fn transaction(&self, hash: &str) -> StoreResult<Option<TransactionRow>> {
        Ok(self
            .transactions
            .iter()
            .find(|row| row.hash == hash)
            .cloned())
    }

    async fn events_for_tick(
        &self,
        _tick: u32,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<EventRow>> {
        Ok(page(&self.events, limit, offset))
    }

    async fn identity(&self, identity: &str) -> StoreResult<Option<IdentityRow>> {
        Ok((identity == IDENTITY).then(|| IdentityRow {
            identity: IDENTITY.into(),
            public_key: "0x00".into(),
            balance: 0,
            incoming_amount: 42,
            outgoing_amount: 42,
            transfer_count: 4,
            first_seen_tick: 7,
            last_seen_tick: 7,
        }))
    }

    async fn stats(&self) -> StoreResult<Value> {
        Ok(serde_json::json!({ "counts": { "ticks": 1 } }))
    }

    async fn write_tick(&self, batch: &TickBatch) -> StoreResult<WriteOutcome> {
        Ok(WriteOutcome {
            tick: batch.tick,
            version: batch.version,
            transactions: batch.transactions.len(),
            events: batch.events.len(),
            identities: batch.identities.len(),
        })
    }

    async fn cursor(&self, _key: &str) -> StoreResult<Option<String>> {
        Ok(None)
    }

    async fn set_cursor(&self, _key: &str, _value: &str) -> StoreResult<()> {
        Ok(())
    }

    async fn ingest_cursor(&self) -> StoreResult<IngestCursor> {
        Ok(IngestCursor::default())
    }

    async fn set_ingest_cursor(&self, _cursor: &IngestCursor) -> StoreResult<()> {
        Ok(())
    }

    async fn transactions_for_identity(
        &self,
        _identity: &str,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        Ok(page(&self.transactions, limit, offset))
    }

    async fn recent_events(&self, limit: u32, offset: u64) -> StoreResult<Vec<EventRow>> {
        Ok(page(&self.events, limit, offset))
    }

    async fn recent_transactions(
        &self,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        Ok(page(&self.transactions, limit, offset))
    }
}

fn page<T: Clone>(rows: &[T], limit: u32, offset: u64) -> Vec<T> {
    rows.iter()
        .skip(offset as usize)
        .take(limit as usize)
        .cloned()
        .collect()
}

async fn get(uri: &str) -> (StatusCode, Value) {
    let app = router(AppState {
        store: Arc::new(MockStore::new()),
    });
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

#[tokio::test]
async fn tick_and_transaction_resources() {
    let (status, body) = get("/v1/ticks/7").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["tick"], 7);
    assert_eq!(body["data"]["verification"], "data");

    let (status, body) = get("/v1/transactions?limit=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["meta"]["hasMore"], true);

    let (status, body) = get("/v1/ticks/7/events").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["type_name"], "qu_transfer");
    assert_eq!(body["data"][0]["body"]["amount"], "42");

    let (status, body) = get("/v1/ticks/999/events").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "not_found");
}

#[tokio::test]
async fn identity_accepts_case_insensitive_input() {
    let uri = format!("/v1/identities/{}", IDENTITY.to_ascii_lowercase());
    let (status, body) = get(&uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["identity"], IDENTITY);

    let uri = format!("/v1/identities/{IDENTITY}/transactions?limit=1");
    let (status, body) = get(&uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["meta"]["limit"], 1);
}

#[tokio::test]
async fn errors_use_problem_details() {
    let (status, body) = get("/v1/identities/NOT-AN-IDENTITY").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_identity");

    let (status, body) = get("/v1/transactions/zzzz").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_transaction_hash");

    let (status, body) = get("/v1/ticks/999").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "not_found");

    let (status, body) = get("/does-not-exist").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "not_found");
}
