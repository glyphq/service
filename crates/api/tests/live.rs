//! Live API smoke test against a local ClickHouse.
//!
//! Creates a uniquely named database, writes two ticks, and exercises every
//! v1 collection through the real router. If ClickHouse is unreachable the
//! test prints a skip message and succeeds, matching the store test policy.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use api::{AppState, router};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{TimeZone, Utc};
use serde_json::Value;
use store::{ClickHouseStore, EventRow, IdentityRow, Store, TickBatch, TransactionRow};
use tower::ServiceExt;

const IDENTITY: &str = "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK";
const OTHER: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const HASH: &str = "yqcvscbbnhotdgqfpvcaniwcppcenlphwdxqqrewmbsfvvbpojfsnvwazgvb";

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn clickhouse_url() -> String {
    std::env::var("CLICKHOUSE_TEST_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into())
}

fn unique_database() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "glyph_api_test_{}_{}_{}",
        std::process::id(),
        nanos,
        counter
    )
}

fn timestamp() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 18, 12, 0, 0)
        .single()
        .expect("valid timestamp")
}

fn batch(tick: u32) -> TickBatch {
    let mut batch = TickBatch::new(240, tick, timestamp(), 7, "data");
    batch.transactions = vec![
        TransactionRow {
            epoch: 240,
            tick,
            tx_index: 0,
            hash: format!("{HASH}{tick}"),
            source: IDENTITY.into(),
            destination: OTHER.into(),
            amount: 1000,
            input_type: 0,
            input_data: String::new(),
            status: "success".into(),
            timestamp: timestamp(),
        },
        TransactionRow {
            epoch: 240,
            tick,
            tx_index: 1,
            hash: format!("{HASH}{tick}1"),
            source: OTHER.into(),
            destination: IDENTITY.into(),
            amount: 250,
            input_type: 0,
            input_data: String::new(),
            status: "success".into(),
            timestamp: timestamp(),
        },
    ];
    batch.events = vec![EventRow {
        epoch: 240,
        tick,
        log_id: u64::from(tick),
        log_type: 1,
        type_name: "asset_issuance".into(),
        tx_hash: format!("{HASH}{tick}"),
        tx_index: 0,
        body: format!(
            r#"{{"issuer":"{IDENTITY}","name":"QX","decimals":0,"unit":"","managingContract":1,"shares":"1000"}}"#
        ),
        log_digest: u64::from(tick),
        timestamp: timestamp(),
    }];
    batch.identities = vec![IdentityRow {
        identity: IDENTITY.into(),
        public_key: format!("0x{}", "00".repeat(32)),
        balance: 750,
        incoming_amount: 250,
        outgoing_amount: 1000,
        transfer_count: 2,
        first_seen_tick: tick,
        last_seen_tick: tick,
    }];
    batch
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
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
async fn live_clickhouse_serves_every_collection() {
    let database = unique_database();
    let store = ClickHouseStore::new(&clickhouse_url(), &database).expect("store config");
    if let Err(error) = store.init().await {
        eprintln!(
            "skipping API ClickHouse smoke test at {}: {error}",
            clickhouse_url()
        );
        return;
    }
    store.write_tick(&batch(100)).await.expect("write 100");
    store.write_tick(&batch(101)).await.expect("write 101");

    let app = router(AppState {
        store: Arc::new(store),
    });

    let (status, body) = get(&app, "/v1/status").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["head"]["tick"], 101);

    let (status, body) = get(&app, "/v1/network/stats").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["ticks"], 2);
    assert_eq!(body["data"]["transactions"], 4);
    assert_eq!(body["data"]["events"], 2);

    let (status, body) = get(&app, "/v1/epochs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["epoch"], 240);
    assert_eq!(body["data"][0]["tick_count"], 2);

    let (status, body) = get(&app, "/v1/epochs/240").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["first_tick"], 100);
    assert_eq!(body["data"]["last_tick"], 101);

    let (status, body) = get(&app, "/v1/ticks?epoch=240&limit=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["tick"], 101);
    let cursor = body["meta"]["nextCursor"].as_str().unwrap().to_owned();

    let (status, body) = get(
        &app,
        &format!("/v1/ticks?epoch=240&limit=1&cursor={cursor}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["tick"], 100);

    let (status, body) = get(&app, "/v1/ticks/101/transactions").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 2);

    let (status, body) = get(&app, "/v1/ticks/101/events").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["type_name"], "asset_issuance");

    let (status, body) = get(&app, "/v1/transactions?tick=101&limit=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);

    let (status, body) = get(&app, &format!("/v1/transactions?identity={IDENTITY}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 4);

    let (status, body) = get(&app, "/v1/events?type=asset_issuance").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 2);

    let (status, body) = get(&app, "/v1/identities").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["identity"], IDENTITY);

    let (status, body) = get(&app, &format!("/v1/identities/{IDENTITY}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["transfer_count"], 2);

    let (status, body) = get(&app, "/v1/assets").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["name"], "QX");
    assert_eq!(body["data"][0]["shares"], 1000);

    let (status, body) = get(&app, &format!("/v1/assets/{IDENTITY}/QX")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["issued_tick"], 101);

    let (status, body) = get(&app, "/openapi.json").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["openapi"], "3.1.0");
}
