//! Integration tests for the ClickHouse store.
//!
//! They run against a local ClickHouse (`CLICKHOUSE_TEST_URL`, default
//! `http://127.0.0.1:8123`) and create a uniquely named database per test. If
//! the server is unreachable the test prints a skip message and succeeds, so
//! the workspace test suite still passes on machines without ClickHouse.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use store::{
    CURSOR_HEAD, ClickHouseStore, Error, EventRow, IdentityRow, Store, TickBatch, TickRow,
    TransactionRow,
};

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

fn clickhouse_url() -> String {
    std::env::var("CLICKHOUSE_TEST_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into())
}

fn unique_database() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let counter = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("glyph_test_{}_{}_{}", std::process::id(), nanos, counter)
}

fn timestamp() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 18, 12, 34, 56)
        .single()
        .expect("valid timestamp")
}

async fn test_store() -> Option<ClickHouseStore> {
    let store = ClickHouseStore::new(&clickhouse_url(), &unique_database())
        .expect("valid store configuration");
    match store.init().await {
        Ok(()) => Some(store),
        Err(err) => {
            eprintln!(
                "skipping ClickHouse integration test at {}: {err}",
                clickhouse_url()
            );
            None
        }
    }
}

fn transaction(epoch: u16, tick: u32, index: u16, hash: &str, amount: i64) -> TransactionRow {
    TransactionRow {
        epoch,
        tick,
        tx_index: index,
        hash: hash.to_string(),
        source: "SRC".into(),
        destination: "DST".into(),
        amount,
        input_type: 0,
        input_data: String::new(),
        status: "success".into(),
        timestamp: timestamp(),
    }
}

fn event(epoch: u16, tick: u32, log_id: u64, body: &str) -> EventRow {
    EventRow {
        epoch,
        tick,
        log_id,
        log_type: 0,
        type_name: "qu_transfer".into(),
        tx_hash: format!("hash-{log_id}"),
        tx_index: log_id as i16,
        body: body.into(),
        log_digest: log_id,
        timestamp: timestamp(),
    }
}

fn identity(identity: &str, balance: i64, last_seen_tick: u32) -> IdentityRow {
    IdentityRow {
        identity: identity.into(),
        public_key: format!("0x{}", "ab".repeat(32)),
        balance,
        incoming_amount: balance.max(0),
        outgoing_amount: (-balance).max(0),
        transfer_count: 1,
        first_seen_tick: 1,
        last_seen_tick,
    }
}

fn batch(epoch: u16, tick: u32) -> TickBatch {
    let mut batch = TickBatch::new(epoch, tick, timestamp(), 7, "quorum");
    batch.signature = [0x11; 64];
    batch.tick_digest = [0x22; 32];
    batch.transactions = vec![
        transaction(epoch, tick, 1, "hash-a", 10),
        transaction(epoch, tick, 0, "hash-b", 20),
    ];
    batch.events = vec![
        event(epoch, tick, 10, "event-a"),
        event(epoch, tick, 11, "event-b"),
    ];
    batch.identities = vec![identity("AAAA", 100, tick), identity("BBBB", 200, tick)];
    batch
}

#[derive(Debug, clickhouse::Row, Deserialize)]
struct Count {
    n: u64,
}

async fn scalar_count(store: &ClickHouseStore, query: &str) -> u64 {
    store
        .client
        .query(query)
        .fetch_one::<Count>()
        .await
        .expect("count query")
        .n
}

#[derive(Debug, clickhouse::Row, Deserialize)]
struct DdlRow {
    statement: String,
}

async fn show_create(store: &ClickHouseStore, table: &str) -> String {
    store
        .client
        .query(&format!("SHOW CREATE TABLE {table}"))
        .fetch_one::<DdlRow>()
        .await
        .expect("show create table")
        .statement
}

#[tokio::test]
async fn round_trip_is_idempotent_and_cursor_never_regresses() {
    let Some(store) = test_store().await else {
        return;
    };

    // Schema initialization is safe to repeat.
    store.init().await.expect("re-init");

    let first = batch(1, 100);
    let outcome = store.write_tick(&first).await.expect("write first tick");
    assert_eq!(outcome.tick, 100);
    assert_eq!(outcome.version, 100);
    assert_eq!(outcome.transactions, 2);
    assert_eq!(outcome.events, 2);
    assert_eq!(outcome.identities, 2);

    let head: TickRow = store.head().await.unwrap().expect("head");
    assert_eq!(head.tick, 100);
    assert_eq!(head.epoch, 1);
    assert_eq!(head.transaction_count, 2);
    assert_eq!(head.event_count, 2);
    assert_eq!(head.verification, "quorum");
    assert_eq!(head.timestamp, timestamp());

    let tick: TickRow = store.tick(100).await.unwrap().expect("tick");
    assert_eq!(tick, head);

    let transactions = store.transactions_for_tick(100, 10, 0).await.unwrap();
    assert_eq!(transactions.len(), 2);
    assert_eq!(transactions[0].tx_index, 0);
    assert_eq!(transactions[1].tx_index, 1);
    assert_eq!(transactions[0].hash, "hash-b");

    let events = store.events_for_tick(100, 10, 0).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].log_id, 10);
    assert_eq!(events[1].log_id, 11);

    let found = store
        .transaction("hash-a")
        .await
        .unwrap()
        .expect("transaction");
    assert_eq!(found.tick, 100);
    assert_eq!(found.amount, 10);

    let account = store.identity("AAAA").await.unwrap().expect("identity");
    assert_eq!(account.balance, 100);

    let stats = store.stats().await.unwrap();
    assert_eq!(stats["counts"]["ticks"], 1);
    assert_eq!(stats["counts"]["transactions"], 2);
    assert_eq!(stats["counts"]["events"], 2);
    assert_eq!(stats["counts"]["identities"], 2);
    assert_eq!(stats["cursor"]["tick"], 100);

    // Replaying the exact same batch must not duplicate reads or counts.
    store.write_tick(&first).await.expect("replay first tick");
    assert_eq!(
        store.transactions_for_tick(100, 10, 0).await.unwrap().len(),
        2
    );
    assert_eq!(store.events_for_tick(100, 10, 0).await.unwrap().len(), 2);
    assert_eq!(store.head().await.unwrap().unwrap().tick, 100);
    assert_eq!(
        scalar_count(&store, "SELECT uniqExact(tick) AS n FROM ticks").await,
        1
    );
    assert_eq!(
        scalar_count(
            &store,
            "SELECT uniqExact(tick, tx_index) AS n FROM transactions"
        )
        .await,
        2
    );
    assert_eq!(
        scalar_count(&store, "SELECT uniqExact(log_id) AS n FROM events").await,
        2
    );
    assert_eq!(
        scalar_count(&store, "SELECT uniqExact(identity) AS n FROM identities").await,
        2
    );

    // A newer tick updates the identity snapshot and the head.
    let mut second = batch(1, 101);
    second.transactions.truncate(1);
    second.events.clear();
    second.identities = vec![identity("AAAA", 150, 101)];
    store.write_tick(&second).await.expect("write second tick");
    assert_eq!(store.head().await.unwrap().unwrap().tick, 101);
    assert_eq!(store.identity("AAAA").await.unwrap().unwrap().balance, 150);

    // Replaying an older tick must not move the head or the identity back.
    store
        .write_tick(&batch(1, 99))
        .await
        .expect("write older tick");
    assert_eq!(store.head().await.unwrap().unwrap().tick, 101);
    assert_eq!(store.identity("AAAA").await.unwrap().unwrap().balance, 150);
    let cursor: serde_json::Value =
        serde_json::from_str(&store.cursor(CURSOR_HEAD).await.unwrap().unwrap()).unwrap();
    assert_eq!(cursor["tick"], 101);

    // Pagination walks the deduplicated rows.
    let page = store.transactions_for_tick(100, 1, 1).await.unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].tx_index, 1);

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn duplicate_keys_within_a_batch_are_collapsed() {
    let Some(store) = test_store().await else {
        return;
    };

    let mut duplicated = batch(1, 200);
    // Same transaction index twice, same event log id twice, same identity
    // twice. The writer keeps the last occurrence of each natural key.
    duplicated
        .transactions
        .push(transaction(1, 200, 1, "hash-a-updated", 999));
    duplicated.events.push(event(1, 200, 10, "event-a-updated"));
    duplicated.identities.push(identity("AAAA", 1234, 200));

    let outcome = store
        .write_tick(&duplicated)
        .await
        .expect("write duplicates");
    assert_eq!(outcome.transactions, 2);
    assert_eq!(outcome.events, 2);
    assert_eq!(outcome.identities, 2);

    let transactions = store.transactions_for_tick(200, 10, 0).await.unwrap();
    assert_eq!(transactions.len(), 2);
    let updated = transactions.iter().find(|tx| tx.tx_index == 1).unwrap();
    assert_eq!(updated.hash, "hash-a-updated");

    let events = store.events_for_tick(200, 10, 0).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].body, "event-a-updated");
    assert_eq!(store.identity("AAAA").await.unwrap().unwrap().balance, 1234);

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn rejecting_mismatched_rows_writes_nothing() {
    let Some(store) = test_store().await else {
        return;
    };

    let mut bad = batch(1, 300);
    bad.transactions[0].tick = 299;
    let err = store.write_tick(&bad).await.expect_err("mismatched tick");
    assert!(
        matches!(err, Error::InvalidBatch(_)),
        "unexpected error: {err:?}"
    );
    assert!(store.head().await.unwrap().is_none());

    let mut bad_event = batch(1, 300);
    bad_event.events[0].epoch = 2;
    let err = store
        .write_tick(&bad_event)
        .await
        .expect_err("mismatched epoch");
    assert!(
        matches!(err, Error::InvalidBatch(_)),
        "unexpected error: {err:?}"
    );
    assert!(store.head().await.unwrap().is_none());

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn schema_statements_do_not_contain_placeholders() {
    let statements = store::schema::statements("glyph_unit").expect("valid database");
    assert!(!statements.is_empty());
    for statement in statements {
        assert!(!statement.contains("{database}"), "{statement}");
    }
    assert!(store::schema::statements("glyph; DROP TABLE ticks").is_err());
}

#[tokio::test]
async fn schema_creates_named_read_projections() {
    let Some(store) = test_store().await else {
        return;
    };

    // The projections make hash lookups and tick lookups use an index instead
    // of scanning the table's natural sort order.
    assert!(show_create(&store, "ticks").await.contains("by_tick"));
    assert!(
        show_create(&store, "transactions")
            .await
            .contains("by_hash")
    );
    assert!(show_create(&store, "events").await.contains("by_tick"));

    store.drop_database().await.expect("drop test database");
}
