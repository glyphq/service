//! Batch writes for verified ticks.
//!
//! A batch is written entity-first and the tick row last, followed by the head
//! cursor. That ordering means the API never reports a head whose transactions
//! or events have not been inserted yet. ClickHouse has no multi-table
//! transaction, so a process crash between inserts is possible; replaying the
//! same batch is safe because every row is versioned by the tick.

use std::collections::{BTreeMap, HashMap};

use serde::Serialize;

use crate::rows::{self, EventInsert, IdentityInsert, TickInsert, TransactionInsert};
use crate::{
    CURSOR_HEAD, ClickHouseStore, Error, HeadCursor, Result, Store, TickBatch, WriteOutcome,
};

pub async fn write_tick(store: &ClickHouseStore, batch: &TickBatch) -> Result<WriteOutcome> {
    // Deduplicate by natural key. A single tick can mention the same identity
    // more than once and a retry can redeliver the same rows.
    let mut transactions = BTreeMap::new();
    for tx in &batch.transactions {
        validate_row(batch, tx.epoch, tx.tick, "transaction")?;
        transactions.insert(tx.tx_index, tx);
    }

    let mut events = BTreeMap::new();
    for event in &batch.events {
        validate_row(batch, event.epoch, event.tick, "event")?;
        events.insert(event.log_id, event);
    }

    let mut identities: HashMap<&str, &crate::IdentityRow> = HashMap::new();
    for identity in &batch.identities {
        if identity.identity.is_empty() {
            return Err(Error::InvalidBatch("identity is empty".into()));
        }
        identities.insert(identity.identity.as_str(), identity);
    }

    let version = batch.version;
    let transaction_rows = transactions
        .values()
        .map(|tx| TransactionInsert {
            epoch: batch.epoch,
            tick: batch.tick,
            tx_index: tx.tx_index,
            hash: tx.hash.clone(),
            source: tx.source.clone(),
            destination: tx.destination.clone(),
            amount: tx.amount,
            input_type: tx.input_type,
            input_data: tx.input_data.clone(),
            status: tx.status.clone(),
            timestamp: batch.timestamp,
            version,
        })
        .collect::<Vec<_>>();

    let event_rows = events
        .values()
        .map(|event| EventInsert {
            epoch: batch.epoch,
            tick: batch.tick,
            log_id: event.log_id,
            log_type: event.log_type,
            type_name: event.type_name.clone(),
            tx_hash: event.tx_hash.clone(),
            tx_index: event.tx_index,
            body: event.body.clone(),
            log_digest: event.log_digest,
            timestamp: batch.timestamp,
            version,
        })
        .collect::<Vec<_>>();

    let identity_rows = identities
        .values()
        .map(|identity| IdentityInsert {
            identity: identity.identity.clone(),
            public_key: identity.public_key.clone(),
            balance: identity.balance,
            incoming_amount: identity.incoming_amount,
            outgoing_amount: identity.outgoing_amount,
            transfer_count: identity.transfer_count,
            first_seen_tick: identity.first_seen_tick,
            last_seen_tick: identity.last_seen_tick,
            version,
        })
        .collect::<Vec<_>>();

    insert_all(&store.client, "transactions", &transaction_rows).await?;
    insert_all(&store.client, "events", &event_rows).await?;
    insert_all(&store.client, "identities", &identity_rows).await?;

    insert_all(
        &store.client,
        "ticks",
        &[TickInsert {
            epoch: batch.epoch,
            tick: batch.tick,
            timestamp: batch.timestamp,
            computor_index: batch.computor_index,
            transaction_count: u32::try_from(transaction_rows.len()).unwrap_or(u32::MAX),
            event_count: u32::try_from(event_rows.len()).unwrap_or(u32::MAX),
            signature: batch.signature,
            tick_digest: batch.tick_digest,
            verification: batch.verification.clone(),
            version,
        }],
    )
    .await?;

    advance_head(store, batch).await?;
    advance_ingest(store, batch).await?;

    Ok(WriteOutcome {
        tick: batch.tick,
        version,
        transactions: transaction_rows.len(),
        events: event_rows.len(),
        identities: identity_rows.len(),
    })
}

fn validate_row(batch: &TickBatch, epoch: u16, tick: u32, kind: &str) -> Result<()> {
    if epoch != batch.epoch || tick != batch.tick {
        return Err(Error::InvalidBatch(format!(
            "{kind} belongs to epoch {epoch} tick {tick}, batch is epoch {} tick {}",
            batch.epoch, batch.tick
        )));
    }
    Ok(())
}

/// Advance the head cursor, but never move it backwards when an older tick is
/// replayed after a newer one.
async fn advance_head(store: &ClickHouseStore, batch: &TickBatch) -> Result<()> {
    let cursor = HeadCursor {
        epoch: batch.epoch,
        tick: batch.tick,
        timestamp: batch.timestamp,
    };
    if let Some(existing) = store.cursor(CURSOR_HEAD).await? {
        if let Ok(existing) = serde_json::from_str::<HeadCursor>(&existing) {
            if existing.tick > cursor.tick {
                return Ok(());
            }
            if existing.tick == cursor.tick {
                return Ok(());
            }
        }
    }
    let value = serde_json::to_string(&cursor)
        .map_err(|err| Error::InvalidBatch(format!("serialize head cursor: {err}")))?;
    rows::insert_cursor(&store.client, CURSOR_HEAD, &value, chrono::Utc::now()).await
}

/// Keep the typed ingest cursor in sync with the newest persisted tick. Like
/// the head cursor it never regresses when an older tick is replayed.
async fn advance_ingest(store: &ClickHouseStore, batch: &TickBatch) -> Result<()> {
    let mut cursor = store.ingest_cursor().await?;
    if cursor
        .last_indexed_tick
        .is_some_and(|existing| existing > batch.tick)
    {
        return Ok(());
    }
    cursor.last_indexed_tick = Some(batch.tick);
    cursor.epoch = Some(batch.epoch);
    cursor.verification = Some(batch.verification.clone());
    cursor.updated_at = Some(chrono::Utc::now());
    store.set_ingest_cursor(&cursor).await
}

async fn insert_all<T>(client: &clickhouse::Client, table: &str, rows: &[T]) -> Result<()>
where
    T: clickhouse::Row + Serialize,
{
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert::<T>(table)?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}
