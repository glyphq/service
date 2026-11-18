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
    CURSOR_HEAD, CURSOR_INGEST, ClickHouseStore, Error, EventRow, IdentityRow, IngestCursor, Store,
    TickBatch, TickRow, TransactionRow,
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

fn timestamp_at(hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 18, hour, 0, 0)
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

fn transfer(tick: u32, index: u16, hash: &str, source: &str, destination: &str) -> TransactionRow {
    let mut row = transaction(1, tick, index, hash, 1);
    row.source = source.into();
    row.destination = destination.into();
    row
}

fn event(epoch: u16, tick: u32, log_id: u64, body: &str) -> EventRow {
    event_typed(epoch, tick, log_id, "qu_transfer", body)
}

fn event_typed(epoch: u16, tick: u32, log_id: u64, type_name: &str, body: &str) -> EventRow {
    EventRow {
        epoch,
        tick,
        log_id,
        log_type: 0,
        type_name: type_name.into(),
        tx_hash: format!("hash-{log_id}"),
        tx_index: log_id as i16,
        body: body.into(),
        log_digest: log_id,
        timestamp: timestamp(),
    }
}

fn identity(identity: &str, balance: i64, last_seen_tick: u32) -> IdentityRow {
    identity_with_transfers(identity, balance, 1, last_seen_tick)
}

fn identity_with_transfers(
    identity: &str,
    balance: i64,
    transfer_count: u64,
    last_seen_tick: u32,
) -> IdentityRow {
    IdentityRow {
        identity: identity.into(),
        public_key: format!("0x{}", "ab".repeat(32)),
        balance,
        incoming_amount: balance.max(0),
        outgoing_amount: (-balance).max(0),
        transfer_count,
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

    let ingest = store.ingest_cursor().await.unwrap();
    assert_eq!(ingest.last_indexed_tick, Some(100));
    assert_eq!(ingest.epoch, Some(1));
    assert_eq!(ingest.verification.as_deref(), Some("quorum"));
    assert!(ingest.updated_at.is_some());
    assert_eq!(stats["ingest"]["last_indexed_tick"], 100);

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
    let ingest = store.ingest_cursor().await.unwrap();
    assert_eq!(ingest.last_indexed_tick, Some(101));
    assert_eq!(ingest.verification.as_deref(), Some("quorum"));

    // Pagination walks the deduplicated rows.
    let page = store.transactions_for_tick(100, 1, 1).await.unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].tx_index, 1);

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn ingest_cursor_round_trips_through_clickhouse() {
    let Some(store) = test_store().await else {
        return;
    };

    assert_eq!(
        store.ingest_cursor().await.unwrap(),
        IngestCursor::default()
    );

    let cursor = IngestCursor {
        last_indexed_tick: Some(4242),
        epoch: Some(7),
        verification: Some("state".into()),
        updated_at: None,
    };
    store.set_ingest_cursor(&cursor).await.expect("set cursor");

    let loaded = store.ingest_cursor().await.unwrap();
    assert_eq!(loaded.last_indexed_tick, Some(4242));
    assert_eq!(loaded.epoch, Some(7));
    assert_eq!(loaded.verification.as_deref(), Some("state"));
    assert!(loaded.updated_at.is_some());

    let raw = store
        .cursor(CURSOR_INGEST)
        .await
        .unwrap()
        .expect("raw cursor");
    assert!(raw.contains("\"last_indexed_tick\":4242"), "{raw}");

    // A later write with a lower tick still wins for explicit cursor sets;
    // only the tick writer applies the no-regression rule.
    store
        .set_ingest_cursor(&IngestCursor {
            last_indexed_tick: Some(1),
            epoch: Some(1),
            verification: Some("quorum".into()),
            updated_at: None,
        })
        .await
        .expect("overwrite cursor");
    assert_eq!(
        store.ingest_cursor().await.unwrap().last_indexed_tick,
        Some(1)
    );

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn identity_history_and_recent_activity_are_ordered_and_deduped() {
    let Some(store) = test_store().await else {
        return;
    };

    let mut tick_100 = TickBatch::new(1, 100, timestamp(), 7, "quorum");
    tick_100.transactions = vec![
        transfer(100, 1, "tx-100-1", "LOOKUP", "B"),
        transfer(100, 2, "tx-100-2", "C", "LOOKUP"),
    ];
    tick_100.events = vec![event(1, 100, 10, "saw-100")];
    store.write_tick(&tick_100).await.expect("write tick 100");

    let mut tick_101 = TickBatch::new(1, 101, timestamp(), 7, "quorum");
    tick_101.transactions = vec![transfer(101, 0, "tx-101-0", "LOOKUP", "LOOKUP")];
    tick_101.events = vec![event(1, 101, 5, "saw-101")];
    store.write_tick(&tick_101).await.expect("write tick 101");

    let mut tick_99 = TickBatch::new(1, 99, timestamp(), 7, "quorum");
    tick_99.transactions = vec![transfer(99, 5, "tx-99-5", "B", "LOOKUP")];
    tick_99.events = vec![event(1, 99, 20, "saw-99")];
    store.write_tick(&tick_99).await.expect("write tick 99");

    let history = store
        .transactions_for_identity("LOOKUP", 10, 0)
        .await
        .expect("identity history");
    let hashes = history
        .iter()
        .map(|tx| tx.hash.as_str())
        .collect::<Vec<_>>();
    assert_eq!(hashes, vec!["tx-101-0", "tx-100-1", "tx-100-2", "tx-99-5"]);
    assert_eq!(history[1].tx_index, 1);
    assert_eq!(history[2].tx_index, 2);

    let page = store
        .transactions_for_identity("LOOKUP", 2, 1)
        .await
        .expect("identity history page");
    let hashes = page.iter().map(|tx| tx.hash.as_str()).collect::<Vec<_>>();
    assert_eq!(hashes, vec!["tx-100-1", "tx-100-2"]);

    let recent_transactions = store
        .recent_transactions(3, 0)
        .await
        .expect("recent transactions");
    let hashes = recent_transactions
        .iter()
        .map(|tx| tx.hash.as_str())
        .collect::<Vec<_>>();
    assert_eq!(hashes, vec!["tx-101-0", "tx-100-1", "tx-100-2"]);

    let recent_events = store.recent_events(10, 0).await.expect("recent events");
    let log_ids = recent_events
        .iter()
        .map(|event| event.log_id)
        .collect::<Vec<_>>();
    assert_eq!(log_ids, vec![5, 10, 20]);

    // Replaying an existing tick must not duplicate recent or identity views.
    store.write_tick(&tick_100).await.expect("replay tick 100");
    assert_eq!(
        store
            .transactions_for_identity("LOOKUP", 10, 0)
            .await
            .unwrap()
            .len(),
        4
    );
    assert_eq!(store.recent_transactions(10, 0).await.unwrap().len(), 4);
    assert_eq!(store.recent_events(10, 0).await.unwrap().len(), 3);
    assert!(
        store
            .transactions_for_identity("NOBODY", 10, 0)
            .await
            .unwrap()
            .is_empty()
    );

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
async fn advanced_reads_are_ordered_deduped_and_bucketed() {
    let Some(store) = test_store().await else {
        return;
    };

    let mut tick_99 = TickBatch::new(1, 99, timestamp_at(11), 7, "quorum");
    tick_99.transactions = vec![transfer(99, 0, "hash-d", "D", "E")];
    tick_99.transactions[0].amount = 5;
    tick_99.events = vec![event_typed(1, 99, 30, "oracle", "saw-99")];
    store.write_tick(&tick_99).await.expect("write tick 99");

    let mut tick_100 = TickBatch::new(1, 100, timestamp_at(12), 7, "quorum");
    tick_100.transactions = vec![
        transfer(100, 0, "hash-a", "AAAA", "DST"),
        transfer(100, 1, "hash-b", "BBBB", "DST"),
    ];
    tick_100.transactions[0].amount = 10;
    tick_100.transactions[1].amount = 20;
    tick_100.events = vec![
        event_typed(1, 100, 10, "qu_transfer", "saw-100"),
        event_typed(1, 100, 11, "asset_issuance", "issued-100"),
    ];
    tick_100.identities = vec![
        identity_with_transfers("AAAA", 100, 5, 100),
        identity_with_transfers("BBBB", 200, 2, 100),
    ];
    store.write_tick(&tick_100).await.expect("write tick 100");

    let mut tick_101 = TickBatch::new(1, 101, timestamp_at(13), 7, "quorum");
    tick_101.transactions = vec![transfer(101, 0, "hash-c", "AAAA", "DST")];
    tick_101.transactions[0].amount = 30;
    tick_101.events = vec![event_typed(1, 101, 20, "qu_transfer", "saw-101")];
    tick_101.identities = vec![
        identity_with_transfers("AAAA", 150, 7, 101),
        identity_with_transfers("CCCC", 300, 9, 101),
    ];
    store.write_tick(&tick_101).await.expect("write tick 101");

    // Batch hash lookup collapses duplicate hashes and sorts newest first.
    let hashes = vec![
        "hash-a".to_string(),
        "hash-c".to_string(),
        "hash-c".to_string(),
        "missing".to_string(),
    ];
    let found = store
        .transactions_by_hashes(&hashes)
        .await
        .expect("batch hashes");
    let order = found.iter().map(|tx| tx.hash.as_str()).collect::<Vec<_>>();
    assert_eq!(order, vec!["hash-c", "hash-a"]);
    assert_eq!(found[0].amount, 30);
    assert!(store.transactions_by_hashes(&[]).await.unwrap().is_empty());

    // Streaming resumes strictly after the last delivered tick, oldest first.
    let after = store.ticks_after(100, 10).await.expect("ticks after");
    assert_eq!(
        after.iter().map(|row| row.tick).collect::<Vec<_>>(),
        vec![101]
    );
    let page = store.ticks_after(98, 2).await.expect("ticks after page");
    assert_eq!(
        page.iter().map(|row| row.tick).collect::<Vec<_>>(),
        vec![99, 100]
    );
    assert!(store.ticks_after(101, 10).await.unwrap().is_empty());

    // Top identities use the latest snapshot and rank by transfer count.
    let top = store.top_identities(2).await.expect("top identities");
    assert_eq!(
        top.iter()
            .map(|row| (row.identity.as_str(), row.transfer_count))
            .collect::<Vec<_>>(),
        vec![("CCCC", 9), ("AAAA", 7)]
    );
    assert_eq!(top[1].balance, 150);

    let types = store.event_type_counts(10).await.expect("event types");
    assert_eq!(
        types
            .iter()
            .map(|row| (row.type_name.as_str(), row.count))
            .collect::<Vec<_>>(),
        vec![("qu_transfer", 2), ("asset_issuance", 1), ("oracle", 1)]
    );
    assert_eq!(store.event_type_counts(1).await.unwrap().len(), 1);

    let hourly = store
        .activity_analytics(3600, 10)
        .await
        .expect("hourly analytics");
    assert_eq!(hourly.len(), 3);
    assert_eq!(hourly[0].bucket, timestamp_at(13));
    assert_eq!(
        (
            hourly[0].ticks,
            hourly[0].transactions,
            hourly[0].events,
            hourly[0].volume
        ),
        (1, 1, 1, 30)
    );
    assert_eq!(
        (
            hourly[1].ticks,
            hourly[1].transactions,
            hourly[1].events,
            hourly[1].volume
        ),
        (1, 2, 2, 30)
    );
    assert_eq!((hourly[2].ticks, hourly[2].volume), (1, 5));

    let limited = store.activity_analytics(3600, 2).await.unwrap();
    assert_eq!(limited.len(), 2);
    assert_eq!(limited[0].bucket, timestamp_at(13));
    assert_eq!(limited[1].bucket, timestamp_at(12));

    let daily = store.activity_analytics(86_400, 10).await.unwrap();
    assert_eq!(daily.len(), 1);
    assert_eq!(
        (
            daily[0].ticks,
            daily[0].transactions,
            daily[0].events,
            daily[0].volume
        ),
        (3, 4, 4, 65)
    );

    // A zero interval is clamped to one second instead of failing.
    assert_eq!(store.activity_analytics(0, 10).await.unwrap().len(), 3);

    // Replaying a tick must not inflate any aggregate.
    store.write_tick(&tick_100).await.expect("replay tick 100");
    let replayed = store.activity_analytics(86_400, 10).await.unwrap();
    assert_eq!(replayed[0].ticks, 3);
    assert_eq!(replayed[0].transactions, 4);
    assert_eq!(replayed[0].events, 4);
    assert_eq!(
        store
            .event_type_counts(10)
            .await
            .unwrap()
            .iter()
            .map(|row| row.count)
            .sum::<u64>(),
        4
    );

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn initializes_the_real_glyph_database() {
    let store =
        ClickHouseStore::new(&clickhouse_url(), "glyph").expect("valid store configuration");
    if let Err(err) = store.init().await {
        eprintln!(
            "skipping ClickHouse integration test at {}: {err}",
            clickhouse_url()
        );
        return;
    }
    // `init` must be replayable against the already-created database.
    store.init().await.expect("re-init glyph");

    let tables: Vec<String> = store
        .client
        .query("SELECT name FROM system.tables WHERE database = ? ORDER BY name")
        .bind("glyph")
        .fetch_all()
        .await
        .expect("system.tables");
    assert_eq!(
        tables,
        vec![
            "events",
            "identities",
            "ingest_state",
            "ticks",
            "transactions"
        ]
    );

    let projections: Vec<String> = store
        .client
        .query("SELECT name FROM system.projections WHERE database = ? ORDER BY name")
        .bind("glyph")
        .fetch_all()
        .await
        .expect("system.projections");
    assert!(projections.contains(&"by_hash".to_string()));
    assert!(projections.contains(&"by_tick".to_string()));
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

    // The projections make hash lookups, identity history, and recent activity
    // use an index instead of scanning the table's natural sort order.
    assert!(show_create(&store, "ticks").await.contains("by_tick"));
    let transactions = show_create(&store, "transactions").await;
    assert!(transactions.contains("by_hash"));
    assert!(transactions.contains("by_source"));
    assert!(transactions.contains("by_destination"));
    let events = show_create(&store, "events").await;
    assert!(events.contains("by_tick"));
    assert!(events.contains("by_recent"));
    assert!(events.contains("by_type"));
    assert!(
        show_create(&store, "identities")
            .await
            .contains("by_activity")
    );

    store.drop_database().await.expect("drop test database");
}
