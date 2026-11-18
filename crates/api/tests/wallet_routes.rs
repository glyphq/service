//! Contract tests for the peer-backed wallet and asset-holder routes.
//!
//! The routes validate identifiers before they open a peer connection, so a
//! deployment without `QUBIC_PEERS` still exercises every rejection path and
//! proves the routes are wired. Success paths need a live peer and are covered
//! by unit tests in `api::wallet` plus the live smoke suite.

use std::sync::Arc;

use api::{AppState, router};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use store::{
    ActivityBucket, AssetRow, EpochRow, EventFilter, EventRow, EventTypeCount, IdentityRow,
    IngestCursor, NetworkStats, Result as StoreResult, Store, TickBatch, TickRow,
    TransactionFilter, TransactionRow, WriteOutcome,
};
use tower::ServiceExt;

const IDENTITY: &str = "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK";

struct EmptyStore;

#[async_trait]
impl Store for EmptyStore {
    async fn init(&self) -> StoreResult<()> {
        Ok(())
    }
    async fn drop_database(&self) -> StoreResult<()> {
        Ok(())
    }
    async fn head(&self) -> StoreResult<Option<TickRow>> {
        Ok(None)
    }
    async fn tick(&self, _tick: u32) -> StoreResult<Option<TickRow>> {
        Ok(None)
    }
    async fn transactions_for_tick(
        &self,
        _tick: u32,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        Ok(Vec::new())
    }
    async fn transaction(&self, _hash: &str) -> StoreResult<Option<TransactionRow>> {
        Ok(None)
    }
    async fn transactions_for_identity(
        &self,
        _identity: &str,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        Ok(Vec::new())
    }
    async fn recent_events(&self, _limit: u32, _offset: u64) -> StoreResult<Vec<EventRow>> {
        Ok(Vec::new())
    }
    async fn recent_transactions(
        &self,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        Ok(Vec::new())
    }
    async fn events_for_tick(
        &self,
        _tick: u32,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<EventRow>> {
        Ok(Vec::new())
    }
    async fn identity(&self, _identity: &str) -> StoreResult<Option<IdentityRow>> {
        Ok(None)
    }
    async fn stats(&self) -> StoreResult<Value> {
        Ok(serde_json::json!({}))
    }
    async fn ticks(
        &self,
        _epoch: Option<u16>,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<TickRow>> {
        Ok(Vec::new())
    }
    async fn epochs(&self, _limit: u32, _offset: u64) -> StoreResult<Vec<EpochRow>> {
        Ok(Vec::new())
    }
    async fn epoch(&self, _epoch: u16) -> StoreResult<Option<EpochRow>> {
        Ok(None)
    }
    async fn identities(&self, _limit: u32, _offset: u64) -> StoreResult<Vec<IdentityRow>> {
        Ok(Vec::new())
    }
    async fn assets(
        &self,
        _issuer: Option<&str>,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<AssetRow>> {
        Ok(Vec::new())
    }
    async fn asset(&self, _issuer: &str, _name: &str) -> StoreResult<Option<AssetRow>> {
        Ok(None)
    }
    async fn transactions_page(
        &self,
        _filter: &TransactionFilter,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        Ok(Vec::new())
    }
    async fn events_page(
        &self,
        _filter: &EventFilter,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<EventRow>> {
        Ok(Vec::new())
    }
    async fn event_type_counts(&self, _limit: u32) -> StoreResult<Vec<EventTypeCount>> {
        Ok(Vec::new())
    }
    async fn activity_analytics(
        &self,
        _interval_seconds: u32,
        _limit: u32,
    ) -> StoreResult<Vec<ActivityBucket>> {
        Ok(Vec::new())
    }
    async fn network_stats(&self) -> StoreResult<NetworkStats> {
        Ok(NetworkStats {
            head: None,
            epochs: 0,
            ticks: 0,
            transactions: 0,
            events: 0,
            identities: 0,
            latest_timestamp: None,
            ingest: IngestCursor::default(),
        })
    }
    async fn write_tick(&self, _batch: &TickBatch) -> StoreResult<WriteOutcome> {
        unreachable!()
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
}

async fn get(uri: &str) -> (StatusCode, Value) {
    let app = router(AppState::new(Arc::new(EmptyStore)));
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn wallet_rejects_invalid_identity() {
    let (status, body) = get("/v1/wallets/not-an-identity").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_identity");
}

#[tokio::test]
async fn wallet_requires_a_peer() {
    let (status, body) = get(&format!("/v1/wallets/{IDENTITY}")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "unavailable");
}

#[tokio::test]
async fn wallet_assets_rejects_invalid_identity_and_kind() {
    let (status, body) = get("/v1/wallets/nope/assets").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_identity");

    let (status, body) = get(&format!("/v1/wallets/{IDENTITY}/assets?kind=loaned")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "bad_request");

    let (status, body) = get(&format!("/v1/wallets/{IDENTITY}/assets?kind=owned")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "unavailable");
}

#[tokio::test]
async fn wallet_snapshot_rejects_invalid_identity() {
    let (status, body) = get("/v1/wallets/nope/snapshot").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_identity");

    let (status, body) = get(&format!("/v1/wallets/{IDENTITY}/snapshot")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "unavailable");
}

#[tokio::test]
async fn holders_validates_issuer_and_name() {
    let (status, body) = get("/v1/assets/not-an-issuer/QX/holders").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_identity");

    for name in ["BAD-NAME", "TOOLONG1"] {
        let (status, body) = get(&format!("/v1/assets/{IDENTITY}/{name}/holders")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "name {name:?}");
        assert_eq!(body["code"], "bad_request");
    }

    let (status, body) = get(&format!("/v1/assets/{IDENTITY}/qx/holders")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "unavailable");
}

#[tokio::test]
async fn openapi_lists_wallet_surface() {
    let spec = api::openapi::spec();
    let paths = spec["paths"].as_object().expect("paths");
    for path in [
        "/v1/wallets/{identity}",
        "/v1/wallets/{identity}/assets",
        "/v1/wallets/{identity}/snapshot",
        "/v1/assets/{issuer}/{name}/holders",
    ] {
        assert!(paths.contains_key(path), "missing {path}");
    }
    let schemas = spec["components"]["schemas"].as_object().expect("schemas");
    for schema in [
        "WalletOverview",
        "WalletAssets",
        "WalletSnapshot",
        "AssetHolder",
    ] {
        assert!(schemas.contains_key(schema), "missing schema {schema}");
    }
}
