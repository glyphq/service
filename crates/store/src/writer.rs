//! Batch writes for verified ticks.
//!
//! A batch is written entity-first and the tick row last, followed by the head
//! cursor. That ordering means the API never reports a head whose transactions
//! or events have not been inserted yet. ClickHouse has no multi-table
//! transaction, so a process crash between inserts is possible; replaying the
//! same batch is safe because every row is versioned by the tick.

use std::collections::{BTreeMap, HashMap};

use serde::Serialize;

use crate::rows::{
    self, ContractDividendInsert, EventInsert, IdentityInsert, QuorumEvidenceInsert, TickInsert,
    TransactionInsert,
};
use crate::{
    CURSOR_HEAD, ClickHouseStore, Error, EventParticipantRow, HeadCursor, Result, Store, TickBatch,
    WriteOutcome,
};

struct PreparedTick {
    tick: TickInsert,
    transactions: Vec<TransactionInsert>,
    events: Vec<EventInsert>,
    participants: Vec<EventParticipantRow>,
    identities: Vec<IdentityInsert>,
    dividends: Vec<ContractDividendInsert>,
    evidence: Option<QuorumEvidenceInsert>,
    outcome: WriteOutcome,
}

pub async fn write_tick(store: &ClickHouseStore, batch: &TickBatch) -> Result<WriteOutcome> {
    let mut outcomes = write_ticks(store, std::slice::from_ref(batch)).await?;
    outcomes
        .pop()
        .ok_or_else(|| Error::InvalidBatch("tick batch produced no outcome".into()))
}

/// Persists many ordered ticks with one insert per table and one cursor
/// advance. This is the backfill path: per-tick inserts and cursor reads are
/// the dominant cost when catching up an epoch.
pub async fn write_ticks(
    store: &ClickHouseStore,
    batches: &[TickBatch],
) -> Result<Vec<WriteOutcome>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let mut transactions = Vec::new();
    let mut events = Vec::new();
    let mut participants = Vec::new();
    let mut identities = Vec::new();
    let mut dividends = Vec::new();
    let mut evidence = Vec::new();
    let mut ticks = Vec::with_capacity(batches.len());
    let mut outcomes = Vec::with_capacity(batches.len());

    for batch in batches {
        let prepared = prepare_tick(batch)?;
        transactions.extend(prepared.transactions);
        events.extend(prepared.events);
        participants.extend(prepared.participants);
        identities.extend(prepared.identities);
        dividends.extend(prepared.dividends);
        if let Some(row) = prepared.evidence {
            evidence.push(row);
        }
        ticks.push(prepared.tick);
        outcomes.push(prepared.outcome);
    }

    insert_all(&store.client, "transactions", &transactions).await?;
    insert_all(&store.client, "events", &events).await?;
    // Participants are written before the tick rows so a visible head always
    // has a complete identity index.
    store.write_event_participants(&participants).await?;
    insert_all(&store.client, "identities", &identities).await?;
    insert_all(&store.client, "contract_dividends", &dividends).await?;
    // Evidence is written before the tick row so a visible head always has its
    // audit row. It shares the batch version, so a replay replaces it.
    insert_all(&store.client, "quorum_evidence", &evidence).await?;
    insert_all(&store.client, "ticks", &ticks).await?;

    // Batches are ordered by the caller. The cursor only advances to the last
    // one, so a crash before this point replays the range idempotently.
    let newest = batches
        .last()
        .expect("non-empty batches checked at function entry");
    advance_head(store, newest).await?;
    advance_ingest(store, newest).await?;

    Ok(outcomes)
}

fn prepare_tick(batch: &TickBatch) -> Result<PreparedTick> {
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

    // The participant index is keyed by the event key plus identity and role.
    // Collapsing here keeps the pre-insert batch small; the store also
    // deduplicates defensively.
    let mut participant_map = BTreeMap::new();
    for participant in &batch.event_participants {
        validate_row(
            batch,
            participant.epoch,
            participant.tick,
            "event participant",
        )?;
        if participant.identity.is_empty() {
            return Err(Error::InvalidBatch("participant identity is empty".into()));
        }
        participant_map.insert(
            (
                participant.log_id,
                participant.identity.as_str(),
                participant.role.as_str(),
            ),
            participant,
        );
    }
    let participants = participant_map
        .into_values()
        .cloned()
        .collect::<Vec<EventParticipantRow>>();

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

    let evidence = match &batch.evidence {
        Some(row) => {
            validate_row(batch, row.epoch, row.tick, "quorum evidence")?;
            Some(QuorumEvidenceInsert {
                epoch: batch.epoch,
                tick: batch.tick,
                quorum_digest: row.quorum_digest,
                tick_data_digest: row.tick_data_digest,
                required_votes: row.required_votes,
                aligned_votes: row.aligned_votes,
                fetched_votes: row.fetched_votes,
                unique_votes: row.unique_votes,
                verified_votes: row.verified_votes,
                verification: row.verification.clone(),
                warning_count: row.warning_count,
                warnings: row.warnings.clone(),
                timestamp: row.timestamp,
                // Callers that construct evidence usually leave `version` at
                // its default; those rows inherit the tick version. An explicit
                // version is preserved so a caller can rewrite evidence
                // independently.
                version: if row.version == 0 {
                    batch.version
                } else {
                    row.version
                },
            })
        }
        None => None,
    };

    let outcome = WriteOutcome {
        tick: batch.tick,
        version,
        transactions: transaction_rows.len(),
        events: event_rows.len(),
        identities: identity_rows.len(),
    };
    let tick = TickInsert {
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
    };

    let dividends = batch
        .dividends
        .iter()
        .map(|row| {
            validate_row(batch, row.epoch, row.tick, "contract dividend")?;
            let mut row = ContractDividendInsert::from(row);
            if row.version == 0 {
                row.version = batch.version;
            }
            Ok(row)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(PreparedTick {
        tick,
        transactions: transaction_rows,
        events: event_rows,
        participants,
        identities: identity_rows,
        dividends,
        evidence,
        outcome,
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

pub(crate) async fn insert_all<T>(
    client: &clickhouse::Client,
    table: &str,
    rows: &[T],
) -> Result<()>
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
