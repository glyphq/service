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
    CURSOR_HEAD, CURSOR_INGEST, ClickHouseStore, ComputorRow, Error, EventFilter,
    EventParticipantRow, EventRow, IdentityActivityRow, IdentityRow, IngestCursor, PricePointRow,
    QuorumEvidenceRow, Store, TickBatch, TickRow, TransactionFilter, TransactionRow,
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

fn participant(
    epoch: u16,
    tick: u32,
    log_id: u64,
    identity: &str,
    role: &str,
) -> EventParticipantRow {
    EventParticipantRow {
        epoch,
        tick,
        log_id,
        identity: identity.into(),
        role: role.into(),
        tx_hash: format!("hash-{log_id}"),
        tx_index: log_id as i16,
        timestamp: timestamp(),
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
struct ParticipantDbRow {
    role: String,
    log_id: u64,
}

fn price_point(hour: u32, price_e8: i64, version: u64) -> PricePointRow {
    PricePointRow {
        hour,
        append_tick: 1000 + hour,
        source_tick: 900 + hour,
        epoch: 3,
        flags: 0b101,
        price_e8,
        source: 7,
        synced: 1,
        bound: 1,
        series_digest: [0x33; 32],
        observed_at: timestamp(),
        version,
    }
}

fn computor_message(epoch: u16, seed: u8) -> qubic::messages::Computors {
    let public_keys = (0..qubic::messages::COMPUTOR_COUNT)
        .map(|index| {
            let bytes = u32::try_from(index)
                .expect("computor index fits u32")
                .to_le_bytes();
            let mut key = [0u8; 32];
            key[..4].copy_from_slice(&bytes);
            key[4] = seed;
            key
        })
        .collect();
    qubic::messages::Computors {
        epoch,
        public_keys,
        signature: [0; 64],
    }
}

fn computor_row(epoch: u16, index: u16, seed: u8) -> ComputorRow {
    let bytes = u32::from(index).to_le_bytes();
    let mut key = [0u8; 32];
    key[..4].copy_from_slice(&bytes);
    key[4] = seed;
    let public_key = qubic::PublicKey::new(key);
    ComputorRow {
        epoch,
        computor_index: index,
        public_key: public_key.to_hex(),
        identity: public_key.to_identity(),
        version: 0,
        timestamp: timestamp(),
    }
}

fn quorum_evidence(epoch: u16, tick: u32, version: u64) -> QuorumEvidenceRow {
    QuorumEvidenceRow {
        epoch,
        tick,
        quorum_digest: [0xAA; 32],
        tick_data_digest: [0xBB; 32],
        required_votes: 451,
        aligned_votes: 500,
        fetched_votes: 600,
        unique_votes: 550,
        verified_votes: 480,
        verification: "data".into(),
        warning_count: 1,
        warnings: r#"["lagging peer"]"#.into(),
        timestamp: timestamp(),
        version,
    }
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
async fn batched_ticks_write_once_and_advance_to_the_newest() {
    let Some(store) = test_store().await else {
        return;
    };

    let mut first = batch(1, 100);
    first.transactions.truncate(1);
    first.identities = vec![identity("AAAA", 100, 100)];
    let mut second = batch(1, 101);
    second.transactions.clear();
    second.events.clear();
    second.identities = vec![identity("AAAA", 150, 101)];
    let mut third = batch(1, 102);
    third.identities = vec![identity("AAAA", 200, 102)];

    let outcomes = store
        .write_ticks(&[first, second, third])
        .await
        .expect("batched write");
    assert_eq!(outcomes.len(), 3);
    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| outcome.tick)
            .collect::<Vec<_>>(),
        [100, 101, 102]
    );
    assert_eq!(outcomes[0].transactions, 1);
    assert_eq!(outcomes[1].transactions, 0);
    assert_eq!(outcomes[2].transactions, 2);

    assert_eq!(
        scalar_count(&store, "SELECT count() AS n FROM ticks FINAL").await,
        3
    );
    assert_eq!(
        scalar_count(&store, "SELECT count() AS n FROM transactions FINAL").await,
        3
    );
    assert_eq!(
        store.head().await.unwrap().expect("head").tick,
        102,
        "head is the newest batch"
    );
    assert_eq!(
        store.identity("AAAA").await.unwrap().unwrap().balance,
        200,
        "the newest identity snapshot wins within one insert"
    );
    assert_eq!(
        store.ingest_cursor().await.unwrap().last_indexed_tick,
        Some(102)
    );

    // Empty batches are a no-op, and replaying the range is idempotent.
    assert!(store.write_ticks(&[]).await.unwrap().is_empty());
    store
        .write_ticks(&[batch(1, 100), batch(1, 101), batch(1, 102)])
        .await
        .expect("replay");
    assert_eq!(
        scalar_count(&store, "SELECT count() AS n FROM ticks FINAL").await,
        3
    );
    assert_eq!(store.head().await.unwrap().unwrap().tick, 102);

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
async fn last_tick_for_epoch_returns_the_partition_maximum() {
    let Some(store) = test_store().await else {
        return;
    };
    store
        .write_ticks(&[batch(1, 100), batch(1, 102), batch(2, 200)])
        .await
        .expect("write ticks");
    assert_eq!(store.last_tick_for_epoch(1).await.unwrap(), Some(102));
    assert_eq!(store.last_tick_for_epoch(2).await.unwrap(), Some(200));
    assert_eq!(store.last_tick_for_epoch(3).await.unwrap(), None);
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
            "btc_price_points",
            "computors",
            "epochs",
            "event_participants",
            "events",
            "identities",
            "ingest_state",
            "price_points",
            "quorum_evidence",
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
async fn price_points_dedupe_order_limit_and_report_snapshot() {
    let Some(store) = test_store().await else {
        return;
    };

    // An empty series has no latest point and no snapshot.
    assert!(store.price_points(0, 10).await.unwrap().is_empty());
    assert!(store.latest_price().await.unwrap().is_none());
    assert!(store.price_snapshot().await.unwrap().is_none());
    store.write_price_points(&[]).await.expect("empty write");

    // Duplicate hours inside one batch collapse to the highest version. The
    // stale lower-version row for hour 100 is ignored.
    store
        .write_price_points(&[
            price_point(100, 100_00000000, 1),
            price_point(100, 1, 0),
            price_point(101, 200_00000000, 1),
            price_point(101, 250_00000000, 2),
            price_point(102, 300_00000000, 1),
        ])
        .await
        .expect("write first batch");

    let points = store.price_points(0, 10).await.expect("all points");
    assert_eq!(
        points.iter().map(|row| row.hour).collect::<Vec<_>>(),
        vec![100, 101, 102]
    );
    assert_eq!(
        points.iter().map(|row| row.price_e8).collect::<Vec<_>>(),
        vec![100_00000000, 250_00000000, 300_00000000]
    );
    assert_eq!(points[0].series_digest, [0x33; 32]);
    assert_eq!(points[0].observed_at, timestamp());
    assert_eq!(points[0].synced, 1);
    assert_eq!(points[0].bound, 1);

    // Replaying the first batch cannot overwrite the newer hour 101 revision,
    // and a later write for hour 103 becomes the latest point.
    store
        .write_price_points(&[
            price_point(101, 250_00000000, 2),
            price_point(102, 300_00000000, 1),
            price_point(100, 100_00000000, 1),
        ])
        .await
        .expect("replay first batch");
    store
        .write_price_points(&[
            price_point(101, 260_00000000, 3),
            price_point(103, 400_00000000, 1),
        ])
        .await
        .expect("write newer revisions");

    let latest = store.latest_price().await.unwrap().expect("latest price");
    assert_eq!(latest.hour, 103);
    assert_eq!(latest.price_e8, 400_00000000);

    let all = store.price_points(0, 10).await.unwrap();
    assert_eq!(
        all.iter()
            .map(|row| (row.hour, row.price_e8))
            .collect::<Vec<_>>(),
        vec![
            (100, 100_00000000),
            (101, 260_00000000),
            (102, 300_00000000),
            (103, 400_00000000),
        ]
    );

    // `from_hour` is inclusive and the limit is clamped to 1..=1000.
    assert_eq!(
        store
            .price_points(102, 10)
            .await
            .unwrap()
            .iter()
            .map(|row| row.hour)
            .collect::<Vec<_>>(),
        vec![102, 103]
    );
    assert_eq!(store.price_points(0, 0).await.unwrap().len(), 1);
    assert_eq!(store.price_points(0, 2).await.unwrap().len(), 2);
    assert_eq!(store.price_points(0, 10_000).await.unwrap().len(), 4);

    let snapshot = store
        .price_snapshot()
        .await
        .expect("snapshot")
        .expect("non-empty snapshot");
    assert_eq!(snapshot.points, 4);
    assert!(snapshot.synced);
    assert!(snapshot.bound);
    assert_eq!(snapshot.source, 7);
    assert_eq!(snapshot.series_digest, [0x33; 32]);
    assert_eq!(snapshot.epoch, 3);
    assert_eq!(snapshot.append_tick, 1000 + 103);
    assert_eq!(snapshot.source_tick, 900 + 103);
    assert_eq!(snapshot.observed_at, timestamp());
    assert_eq!(snapshot.latest.as_ref().map(|row| row.hour), Some(103));
    assert_eq!(
        snapshot.latest.as_ref().map(|row| row.price_e8),
        Some(400_00000000)
    );

    let json = serde_json::to_value(&snapshot).expect("serialize snapshot");
    assert_eq!(json["series_digest"].as_array().map(Vec::len), Some(32));
    assert_eq!(json["latest"]["price_e8"], 400_00000000_i64);
    assert!(json["observed_at"].is_string(), "{json}");

    assert!(store.price_points(104, 10).await.unwrap().is_empty());

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn computors_round_trip_and_keep_the_newest_version() {
    let Some(store) = test_store().await else {
        return;
    };

    assert!(
        store
            .computors(1)
            .await
            .expect("empty computors")
            .is_empty()
    );
    store
        .write_computors(
            1,
            &qubic::messages::Computors {
                epoch: 1,
                public_keys: Vec::new(),
                signature: [0; 64],
            },
            timestamp(),
        )
        .await
        .expect("empty computor write");
    assert!(
        store
            .computors(1)
            .await
            .expect("empty computors")
            .is_empty()
    );

    let first_timestamp = timestamp();
    store
        .write_computors(1, &computor_message(1, 7), first_timestamp)
        .await
        .expect("write computors");

    let rows = store.computors(1).await.expect("computors");
    assert_eq!(rows.len(), qubic::messages::COMPUTOR_COUNT);
    let expected_first = computor_row(1, 0, 7);
    assert_eq!(rows[0].epoch, 1);
    assert_eq!(rows[0].computor_index, 0);
    assert_eq!(rows[0].public_key, expected_first.public_key);
    assert_eq!(rows[0].identity, expected_first.identity);
    assert_eq!(rows[0].timestamp, first_timestamp);
    let expected_last = computor_row(1, 675, 7);
    assert_eq!(rows[675].computor_index, 675);
    assert_eq!(rows[675].identity, expected_last.identity);

    // A later timestamp replaces the same epoch's set in place.
    let later = first_timestamp + chrono::Duration::seconds(1);
    store
        .write_computors(1, &computor_message(1, 9), later)
        .await
        .expect("replace computors");
    let replaced = store.computors(1).await.expect("replaced computors");
    assert_eq!(replaced.len(), qubic::messages::COMPUTOR_COUNT);
    assert_eq!(replaced[0].identity, computor_row(1, 0, 9).identity);
    assert_eq!(replaced[0].timestamp, later);

    // A stale replay never wins over the newer set.
    store
        .write_computors(
            1,
            &computor_message(1, 7),
            first_timestamp - chrono::Duration::seconds(1),
        )
        .await
        .expect("stale computor replay");
    let stable = store.computors(1).await.expect("stable computors");
    assert_eq!(stable[0].identity, computor_row(1, 0, 9).identity);
    assert!(store.computors(2).await.expect("unknown epoch").is_empty());

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn evidence_is_written_with_the_tick_and_round_trips() {
    let Some(store) = test_store().await else {
        return;
    };

    assert!(
        store
            .evidence_for_tick(100)
            .await
            .expect("missing evidence")
            .is_none()
    );

    let mut first = TickBatch::new(1, 100, timestamp(), 7, "data");
    first.evidence = Some(quorum_evidence(1, 100, 1));
    store.write_tick(&first).await.expect("write tick");

    let evidence = store
        .evidence_for_tick(100)
        .await
        .expect("evidence lookup")
        .expect("evidence row");
    assert_eq!(evidence.epoch, 1);
    assert_eq!(evidence.tick, 100);
    assert_eq!(evidence.quorum_digest, [0xAA; 32]);
    assert_eq!(evidence.tick_data_digest, [0xBB; 32]);
    assert_eq!(evidence.required_votes, 451);
    assert_eq!(evidence.aligned_votes, 500);
    assert_eq!(evidence.fetched_votes, 600);
    assert_eq!(evidence.unique_votes, 550);
    assert_eq!(evidence.verified_votes, 480);
    assert_eq!(evidence.verification, "data");
    assert_eq!(evidence.warning_count, 1);
    assert_eq!(evidence.warnings, r#"["lagging peer"]"#);
    // An explicit evidence version is preserved.
    assert_eq!(evidence.version, 1);

    // The batched path writes one evidence row per batch, before the ticks.
    let mut second = TickBatch::new(1, 101, timestamp(), 7, "quorum");
    second.evidence = Some(quorum_evidence(1, 101, 0));
    let mut third = TickBatch::new(1, 102, timestamp(), 7, "quorum");
    third.evidence = Some(quorum_evidence(1, 102, 0));
    let outcomes = store
        .write_ticks(&[second, third])
        .await
        .expect("batched write");
    assert_eq!(outcomes.len(), 2);
    assert!(
        store
            .evidence_for_tick(101)
            .await
            .expect("evidence 101")
            .is_some()
    );
    assert!(
        store
            .evidence_for_tick(102)
            .await
            .expect("evidence 102")
            .is_some()
    );

    // Replaying a tick with a newer version replaces the evidence.
    first.version = 150;
    let mut updated = quorum_evidence(1, 100, 0);
    updated.verification = "quorum".into();
    updated.warnings = "[]".into();
    updated.warning_count = 0;
    updated.version = 150;
    first.evidence = Some(updated);
    store.write_tick(&first).await.expect("replace evidence");
    let replaced = store
        .evidence_for_tick(100)
        .await
        .expect("replaced evidence")
        .expect("evidence row");
    assert_eq!(replaced.version, 150);
    assert_eq!(replaced.verification, "quorum");
    assert_eq!(replaced.warnings, "[]");
    assert_eq!(replaced.warning_count, 0);

    // Evidence for another tick rejects the batch before anything is written.
    let mut bad = TickBatch::new(1, 110, timestamp(), 7, "data");
    bad.evidence = Some(quorum_evidence(1, 111, 0));
    let err = store
        .write_tick(&bad)
        .await
        .expect_err("mismatched evidence");
    assert!(
        matches!(err, Error::InvalidBatch(_)),
        "unexpected error: {err:?}"
    );
    assert!(
        store
            .evidence_for_tick(110)
            .await
            .expect("missing evidence")
            .is_none()
    );

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
        show_create(&store, "event_participants")
            .await
            .contains("by_tick")
    );
    assert!(
        show_create(&store, "identities")
            .await
            .contains("by_activity")
    );

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn event_participants_collapse_duplicates_and_keep_roles() {
    let Some(store) = test_store().await else {
        return;
    };

    // Empty writes are a no-op rather than an error.
    store
        .write_event_participants(&[])
        .await
        .expect("empty participant write");

    let rows = vec![
        participant(1, 100, 10, "AAAA", "from"),
        participant(1, 100, 10, "AAAA", "from"),
        participant(1, 100, 12, "AAAA", "to"),
        participant(1, 100, 11, "BBBB", "to"),
    ];
    store
        .write_event_participants(&rows)
        .await
        .expect("write participants");
    assert_eq!(
        scalar_count(
            &store,
            "SELECT uniqExact(epoch, log_id, identity, role) AS n FROM event_participants"
        )
        .await,
        3
    );

    // Replaying the same participant rows is idempotent.
    store
        .write_event_participants(&rows)
        .await
        .expect("replay participants");
    assert_eq!(
        scalar_count(
            &store,
            "SELECT uniqExact(epoch, log_id, identity, role) AS n FROM event_participants"
        )
        .await,
        3
    );

    let stored: Vec<ParticipantDbRow> = store
        .client
        .query(
            "SELECT role, log_id FROM ( \
                 SELECT * FROM event_participants WHERE identity = ? \
                 ORDER BY version DESC LIMIT 1 BY epoch, log_id, identity, role \
             ) ORDER BY log_id ASC, role ASC",
        )
        .bind("AAAA")
        .fetch_all()
        .await
        .expect("participant rows");
    assert_eq!(
        stored
            .iter()
            .map(|row| (row.log_id, row.role.as_str()))
            .collect::<Vec<_>>(),
        vec![(10, "from"), (12, "to")]
    );

    // Only AAAA and BBBB are indexed.
    let unknown: Vec<ParticipantDbRow> = store
        .client
        .query("SELECT role, log_id FROM event_participants WHERE identity = ?")
        .bind("NOBODY")
        .fetch_all()
        .await
        .expect("unknown participant rows");
    assert!(unknown.is_empty());

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn events_for_identity_joins_participants_newest_first() {
    let Some(store) = test_store().await else {
        return;
    };

    let mut tick = TickBatch::new(1, 100, timestamp(), 7, "quorum");
    tick.events = vec![
        event(1, 100, 10, "event-a"),
        event(1, 100, 11, "event-b"),
        event(1, 100, 12, "event-c"),
    ];
    tick.event_participants = vec![
        participant(1, 100, 10, "LOOKUP", "from"),
        participant(1, 100, 11, "LOOKUP", "to"),
        participant(1, 100, 12, "LOOKUP", "to"),
        participant(1, 100, 10, "OTHER", "to"),
        // Duplicate inside the batch is collapsed by the writer.
        participant(1, 100, 10, "LOOKUP", "from"),
    ];
    store.write_tick(&tick).await.expect("write tick");

    let events = store
        .events_for_identity("LOOKUP", 10, 0)
        .await
        .expect("events by identity");
    assert_eq!(
        events.iter().map(|event| event.log_id).collect::<Vec<_>>(),
        vec![12, 11, 10]
    );
    assert_eq!(events[0].type_name, "qu_transfer");
    assert_eq!(events[0].body, "event-c");

    let events = store
        .events_for_identity("OTHER", 10, 0)
        .await
        .expect("events by other identity");
    assert_eq!(
        events.iter().map(|event| event.log_id).collect::<Vec<_>>(),
        vec![10]
    );

    // The limit is clamped to at least one row.
    assert_eq!(
        store
            .events_for_identity("LOOKUP", 0, 0)
            .await
            .unwrap()
            .len(),
        1
    );
    let page = store
        .events_for_identity("LOOKUP", 1, 1)
        .await
        .expect("events page");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].log_id, 11);

    assert!(
        store
            .events_for_identity("NOBODY", 10, 0)
            .await
            .unwrap()
            .is_empty()
    );

    // Roles are preserved in the index and queryable directly.
    let roles = store
        .client
        .query(
            "SELECT role, log_id FROM ( \
                 SELECT * FROM event_participants WHERE identity = ? \
                 ORDER BY version DESC LIMIT 1 BY epoch, log_id, identity, role \
             ) ORDER BY log_id ASC",
        )
        .bind("LOOKUP")
        .fetch_all::<ParticipantDbRow>()
        .await
        .expect("participant roles");
    assert_eq!(
        roles
            .iter()
            .map(|row| (row.log_id, row.role.as_str()))
            .collect::<Vec<_>>(),
        vec![(10, "from"), (11, "to"), (12, "to")]
    );

    // A replayed tick must not duplicate the identity event view.
    store.write_tick(&tick).await.expect("replay tick");
    assert_eq!(
        store
            .events_for_identity("LOOKUP", 10, 0)
            .await
            .unwrap()
            .len(),
        3
    );

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn activity_merges_direct_and_contract_originated_rows() {
    let Some(store) = test_store().await else {
        return;
    };

    let mut direct = TickBatch::new(1, 100, timestamp(), 7, "quorum");
    direct.transactions = vec![transfer(100, 0, "tx-100", "LOOKUP", "DST")];
    direct.transactions[0].amount = 7;
    store.write_tick(&direct).await.expect("write direct tick");

    let mut contract = TickBatch::new(1, 101, timestamp(), 7, "quorum");
    contract.events = vec![event(
        1,
        101,
        20,
        r#"{"from":"CONTRACT","to":"LOOKUP","amount":"500"}"#,
    )];
    contract.event_participants = vec![
        participant(1, 101, 20, "CONTRACT", "from"),
        participant(1, 101, 20, "LOOKUP", "to"),
    ];
    store
        .write_tick(&contract)
        .await
        .expect("write contract tick");

    let activity = store
        .activity_for_identity("LOOKUP", 10, 0)
        .await
        .expect("identity activity");
    assert_eq!(activity.len(), 2);

    // Contract-originated transfer event first: newer tick, populated role and
    // transfer fields read from the event body.
    assert_eq!(activity[0].kind, "event");
    assert_eq!(activity[0].epoch, 1);
    assert_eq!(activity[0].tick, 101);
    assert_eq!(activity[0].log_id, Some(20));
    assert_eq!(activity[0].tx_index, Some(20));
    assert_eq!(activity[0].hash, "hash-20");
    assert_eq!(activity[0].source, "CONTRACT");
    assert_eq!(activity[0].destination, "LOOKUP");
    assert_eq!(activity[0].amount, 500);
    assert_eq!(activity[0].type_name, "qu_transfer");
    assert_eq!(activity[0].role, "to");
    assert_eq!(activity[0].status, "");
    assert!(activity[0].body.contains("\"amount\":\"500\""));
    let event_json = serde_json::to_value(&activity[0]).expect("serialize event");
    assert_eq!(event_json["kind"], "event");
    assert!(event_json.get("status").is_none());
    assert!(event_json.get("input_data").is_none());
    assert!(event_json.get("input_type").is_none());
    assert_eq!(event_json["role"], "to");
    assert_eq!(event_json["type_name"], "qu_transfer");

    // Direct transaction second, with transaction-only fields populated.
    assert_eq!(activity[1].kind, "transaction");
    assert_eq!(activity[1].tick, 100);
    assert_eq!(activity[1].log_id, None);
    assert_eq!(activity[1].tx_index, Some(0));
    assert_eq!(activity[1].hash, "tx-100");
    assert_eq!(activity[1].source, "LOOKUP");
    assert_eq!(activity[1].destination, "DST");
    assert_eq!(activity[1].amount, 7);
    assert_eq!(activity[1].status, "success");
    let transaction_json = serde_json::to_value(&activity[1]).expect("serialize transaction");
    assert_eq!(transaction_json["kind"], "transaction");
    assert_eq!(transaction_json["input_type"], 0);
    assert_eq!(transaction_json["status"], "success");
    assert!(transaction_json.get("log_id").is_none());
    assert!(transaction_json.get("type_name").is_none());
    assert!(transaction_json.get("body").is_none());
    assert!(transaction_json.get("role").is_none());
    assert_eq!(activity[1].role, "");
    assert_eq!(activity[1].timestamp, timestamp());

    // The event view sees the same contract log.
    let events = store
        .events_for_identity("LOOKUP", 10, 0)
        .await
        .expect("events by identity");
    assert_eq!(
        events.iter().map(|event| event.log_id).collect::<Vec<_>>(),
        vec![20]
    );

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn activity_pagination_is_stable_and_has_no_duplicates() {
    let Some(store) = test_store().await else {
        return;
    };

    let mut tick_100 = TickBatch::new(1, 100, timestamp(), 7, "quorum");
    tick_100.transactions = vec![transfer(100, 0, "tx-100", "LOOKUP", "B")];
    store.write_tick(&tick_100).await.expect("write tick 100");

    let mut tick_101 = TickBatch::new(1, 101, timestamp(), 7, "quorum");
    tick_101.transactions = vec![transfer(101, 0, "tx-101", "A", "LOOKUP")];
    tick_101.events = vec![
        event(1, 101, 5, r#"{"from":"LOOKUP","to":"C","amount":"1"}"#),
        event(
            1,
            101,
            6,
            r#"{"from":"CONTRACT","to":"LOOKUP","amount":"2"}"#,
        ),
    ];
    tick_101.event_participants = vec![
        participant(1, 101, 5, "LOOKUP", "from"),
        participant(1, 101, 6, "LOOKUP", "to"),
    ];
    store.write_tick(&tick_101).await.expect("write tick 101");

    let mut tick_102 = TickBatch::new(1, 102, timestamp(), 7, "quorum");
    tick_102.transactions = vec![transfer(102, 0, "tx-102", "LOOKUP", "D")];
    store.write_tick(&tick_102).await.expect("write tick 102");

    let key = |row: &IdentityActivityRow| {
        (
            row.kind.clone(),
            row.tick,
            row.log_id,
            row.tx_index,
            row.hash.clone(),
        )
    };
    let all = store
        .activity_for_identity("LOOKUP", 100, 0)
        .await
        .expect("all activity");
    assert_eq!(all.len(), 5);
    // tick DESC, then log_id DESC with nulls last (transactions have no log
    // and sort after events on the same tick), then tx_index DESC.
    assert_eq!(
        all.iter()
            .map(|row| (row.tick, row.log_id, row.kind.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (102, None, "transaction"),
            (101, Some(6), "event"),
            (101, Some(5), "event"),
            (101, None, "transaction"),
            (100, None, "transaction"),
        ]
    );

    let mut paged = Vec::new();
    for offset in [0_u64, 2, 4] {
        let page = store
            .activity_for_identity("LOOKUP", 2, offset)
            .await
            .expect("activity page");
        assert!(!page.is_empty() && page.len() <= 2);
        paged.extend(page);
    }
    assert_eq!(
        paged.iter().map(key).collect::<Vec<_>>(),
        all.iter().map(key).collect::<Vec<_>>()
    );

    // The limit clamp still yields at least one row.
    assert_eq!(
        store
            .activity_for_identity("LOOKUP", 0, 0)
            .await
            .unwrap()
            .len(),
        1
    );

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn activity_for_empty_and_unknown_identity_is_empty() {
    let Some(store) = test_store().await else {
        return;
    };

    let mut tick = TickBatch::new(1, 100, timestamp(), 7, "quorum");
    tick.transactions = vec![transfer(100, 0, "tx-100", "LOOKUP", "DST")];
    tick.events = vec![event(1, 100, 10, "event-a")];
    tick.event_participants = vec![participant(1, 100, 10, "LOOKUP", "from")];
    store.write_tick(&tick).await.expect("write tick");

    assert!(
        store
            .activity_for_identity("", 10, 0)
            .await
            .expect("empty identity activity")
            .is_empty()
    );
    assert!(
        store
            .activity_for_identity("NOBODY", 10, 0)
            .await
            .expect("unknown identity activity")
            .is_empty()
    );
    assert!(
        store
            .events_for_identity("", 10, 0)
            .await
            .expect("empty identity events")
            .is_empty()
    );
    assert!(
        store
            .events_for_identity("NOBODY", 10, 0)
            .await
            .expect("unknown identity events")
            .is_empty()
    );

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn tick_writer_persists_event_participants_before_the_tick() {
    let Some(store) = test_store().await else {
        return;
    };

    let mut tick = TickBatch::new(1, 400, timestamp(), 7, "quorum");
    tick.events = vec![event(1, 400, 10, "event-a"), event(1, 400, 11, "event-b")];
    tick.event_participants = vec![
        participant(1, 400, 10, "WRITER", "from"),
        participant(1, 400, 11, "WRITER", "to"),
        participant(1, 400, 11, "WRITER", "to"),
        participant(1, 400, 11, "OTHER", "from"),
    ];
    store.write_tick(&tick).await.expect("write tick");

    // The head is only written after the participant insert succeeded.
    assert_eq!(store.head().await.unwrap().expect("head").tick, 400);
    assert_eq!(
        show_create(&store, "event_participants")
            .await
            .matches("by_tick")
            .count(),
        1
    );
    assert_eq!(
        scalar_count(
            &store,
            "SELECT uniqExact(epoch, log_id, identity, role) AS n FROM event_participants"
        )
        .await,
        3
    );
    let events = store
        .events_for_identity("WRITER", 10, 0)
        .await
        .expect("writer events");
    assert_eq!(
        events.iter().map(|event| event.log_id).collect::<Vec<_>>(),
        vec![11, 10]
    );

    // A participant for another tick rejects the whole batch.
    let mut bad = TickBatch::new(1, 401, timestamp(), 7, "quorum");
    bad.event_participants = vec![participant(1, 402, 12, "WRITER", "to")];
    let err = store.write_tick(&bad).await.expect_err("mismatched tick");
    assert!(
        matches!(err, Error::InvalidBatch(_)),
        "unexpected error: {err:?}"
    );

    // An empty identity is rejected before anything is inserted.
    let mut blank = TickBatch::new(1, 401, timestamp(), 7, "quorum");
    blank.event_participants = vec![participant(1, 401, 13, "", "to")];
    let err = store.write_tick(&blank).await.expect_err("blank identity");
    assert!(
        matches!(err, Error::InvalidBatch(_)),
        "unexpected error: {err:?}"
    );

    store.drop_database().await.expect("drop test database");
}

#[tokio::test]
async fn activity_rows_serialize_to_a_stable_json_shape() {
    let participant = participant(1, 100, 10, "AAAA", "from");
    let json = serde_json::to_value(&participant).expect("serialize participant");
    assert_eq!(
        json,
        serde_json::json!({
            "epoch": 1,
            "tick": 100,
            "log_id": 10,
            "identity": "AAAA",
            "role": "from",
            "tx_hash": "hash-10",
            "tx_index": 10,
            "timestamp": "2026-11-18T12:34:56Z",
        })
    );
    let decoded: EventParticipantRow =
        serde_json::from_value(json).expect("deserialize participant");
    assert_eq!(decoded, participant);

    let activity = IdentityActivityRow {
        kind: "event".into(),
        epoch: 1,
        tick: 101,
        timestamp: timestamp(),
        tx_index: Some(3),
        log_id: Some(20),
        hash: "hash-20".into(),
        source: "CONTRACT".into(),
        destination: "AAAA".into(),
        amount: 500,
        input_type: None,
        input_data: String::new(),
        status: String::new(),
        type_name: "qu_transfer".into(),
        body: "{\"amount\":\"500\"}".into(),
        role: "to".into(),
    };
    let json = serde_json::to_value(&activity).expect("serialize activity");
    assert_eq!(json["kind"], "event");
    assert_eq!(json["epoch"], 1);
    assert_eq!(json["tick"], 101);
    assert_eq!(json["timestamp"], "2026-11-18T12:34:56Z");
    assert_eq!(json["tx_index"], 3);
    assert_eq!(json["log_id"], 20);
    assert_eq!(json["hash"], "hash-20");
    assert_eq!(json["source"], "CONTRACT");
    assert_eq!(json["destination"], "AAAA");
    assert_eq!(json["amount"], 500);
    assert!(json.get("input_type").is_none());
    assert!(json.get("input_data").is_none());
    assert!(json.get("status").is_none());
    assert_eq!(json["type_name"], "qu_transfer");
    assert_eq!(json["body"], "{\"amount\":\"500\"}");
    assert_eq!(json["role"], "to");
    let decoded: IdentityActivityRow = serde_json::from_value(json).expect("deserialize activity");
    assert_eq!(decoded, activity);

    // A transaction row leaves event-only fields empty and null.
    let transaction = IdentityActivityRow {
        kind: "transaction".into(),
        log_id: None,
        input_type: Some(0),
        status: "success".into(),
        ..activity
    };
    let json = serde_json::to_value(&transaction).expect("serialize transaction");
    assert_eq!(json["kind"], "transaction");
    assert!(json.get("log_id").is_none());
    assert_eq!(json["input_type"], 0);
    assert_eq!(json["status"], "success");
}

#[tokio::test]
async fn explorer_history_queries_return_deduped_rows() {
    let Some(store) = test_store().await else {
        return;
    };
    let mut batch = batch(1, 100);
    batch.transactions = vec![
        transfer(100, 0, "hash-a", "ALICE", "BOB"),
        transfer(100, 1, "hash-b", "BOB", "ALICE"),
    ];
    let mut transfer_event = event_typed(
        1,
        100,
        20,
        "qu_transfer",
        r#"{"from":"ALICE","to":"BOB","amount":10}"#,
    );
    transfer_event.tx_hash = "hash-a".into();
    let mut asset_event = event_typed(
        1,
        100,
        21,
        "asset_ownership_change",
        r#"{"from":"ALICE","to":"BOB","issuer":"ALICE","name":"QX","managingContract":1,"shares":5}"#,
    );
    asset_event.tx_hash = "hash-a".into();
    batch.events = vec![transfer_event, asset_event];
    store.write_tick(&batch).await.expect("write batch");

    let events = store
        .events_for_transaction("hash-a")
        .await
        .expect("events for transaction");
    assert_eq!(events.len(), 2);

    let counterparties = store
        .counterparties("ALICE", None, 10)
        .await
        .expect("counterparties");
    assert_eq!(counterparties.len(), 1);
    assert_eq!(counterparties[0].identity, "BOB");
    assert_eq!(counterparties[0].incoming_amount, 1);
    assert_eq!(counterparties[0].outgoing_amount, 1);
    assert_eq!(counterparties[0].transaction_count, 2);

    let flow = store
        .identity_flow("ALICE", 3600, timestamp() - chrono::Duration::hours(2), 10)
        .await
        .expect("identity flow");
    assert_eq!(flow.len(), 1);
    assert_eq!(flow[0].incoming_amount, 1);
    assert_eq!(flow[0].outgoing_amount, 1);
    assert_eq!(flow[0].transaction_count, 2);

    let transfers = store
        .asset_transfers("ALICE", "QX", None, 10, 0)
        .await
        .expect("asset transfers");
    assert_eq!(transfers.len(), 1);
    assert_eq!(transfers[0].kind, "ownership");
    assert_eq!(transfers[0].from, "ALICE");
    assert_eq!(transfers[0].to, "BOB");
    assert_eq!(transfers[0].shares, 5);
}

#[tokio::test]
async fn paged_reads_bound_by_tick_and_union_identity_filters() {
    let Some(store) = test_store().await else {
        return;
    };

    let mut first = batch(1, 100);
    first.transactions = vec![
        transfer(100, 0, "hash-a", "ALICE", "BOB"),
        transfer(100, 1, "hash-b", "BOB", "ALICE"),
    ];
    first.events = vec![event_typed(
        1,
        100,
        20,
        "qu_transfer",
        r#"{"from":"ALICE","to":"BOB","amount":10}"#,
    )];
    first.event_participants = vec![
        participant(1, 100, 20, "ALICE", "from"),
        participant(1, 100, 20, "BOB", "to"),
    ];
    store.write_tick(&first).await.expect("write first tick");

    let mut second = batch(1, 101);
    second.transactions = vec![transfer(101, 0, "hash-c", "ALICE", "CAROL")];
    second.events = vec![event_typed(
        1,
        101,
        30,
        "qu_transfer",
        r#"{"from":"ALICE","to":"CAROL","amount":11}"#,
    )];
    second.event_participants = vec![
        participant(1, 101, 30, "ALICE", "from"),
        participant(1, 101, 30, "CAROL", "to"),
    ];
    store.write_tick(&second).await.expect("write second tick");

    let recent = TransactionFilter {
        epoch: Some(1),
        tick: None,
        min_tick: Some(101),
        identity: Some("ALICE".into()),
    };
    let rows = store
        .transactions_page(&recent, 10, 0)
        .await
        .expect("recent identity page");
    assert_eq!(
        rows.iter().map(|row| row.hash.as_str()).collect::<Vec<_>>(),
        vec!["hash-c"]
    );

    let all = TransactionFilter {
        min_tick: Some(100),
        ..recent
    };
    let rows = store
        .transactions_page(&all, 10, 0)
        .await
        .expect("full identity page");
    assert_eq!(
        rows.iter().map(|row| row.hash.as_str()).collect::<Vec<_>>(),
        vec!["hash-c", "hash-a", "hash-b"]
    );

    let recent_events = EventFilter {
        epoch: Some(1),
        tick: None,
        min_tick: Some(101),
        type_name: Some("qu_transfer".into()),
    };
    let events = store
        .events_page(&recent_events, 10, 0)
        .await
        .expect("recent typed events");
    assert_eq!(
        events.iter().map(|event| event.log_id).collect::<Vec<_>>(),
        vec![30]
    );

    let all_events = EventFilter {
        min_tick: Some(100),
        ..recent_events
    };
    let events = store
        .events_page(&all_events, 10, 0)
        .await
        .expect("all typed events");
    assert_eq!(
        events.iter().map(|event| event.log_id).collect::<Vec<_>>(),
        vec![30, 20]
    );
}
