//! RowBinary row shapes.
//!
//! These are deliberately separate from the public API rows: ClickHouse
//! `DateTime64(3)` needs a millisecond serde adapter, while the API keeps
//! RFC 3339 timestamps.

use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

use crate::{
    ActivityBucket, AssetRow, AssetTransferRow, ComputorRow, CounterpartyRow, EntitySnapshot,
    EpochMetadata, EpochRow, EventParticipantRow, EventRow, EventTypeCount, FlowBucket,
    IdentityActivityRow, IdentityRow, IdentityTotals, PricePointRow, QuorumEvidenceRow, Result,
    TickRow, TransactionRow,
};

/// RowBinary encodes `FixedString(N)` as N raw bytes. Serde's array impl only
/// covers lengths up to 32, so serialize byte arrays as fixed-length tuples.
pub(crate) mod fixed_bytes {
    use std::marker::PhantomData;

    use serde::de::{SeqAccess, Visitor};
    use serde::ser::SerializeTuple;
    use serde::{Deserializer, Serializer};

    pub fn serialize<const N: usize, S>(bytes: &[u8; N], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut tuple = serializer.serialize_tuple(N)?;
        for byte in bytes {
            tuple.serialize_element(byte)?;
        }
        tuple.end()
    }

    pub fn deserialize<'de, const N: usize, D>(deserializer: D) -> Result<[u8; N], D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FixedBytesVisitor<const N: usize>(PhantomData<[u8; N]>);

        impl<'de, const N: usize> Visitor<'de> for FixedBytesVisitor<N> {
            type Value = [u8; N];

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "{N} bytes")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut bytes = [0u8; N];
                for (index, byte) in bytes.iter_mut().enumerate() {
                    *byte = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(index, &self))?;
                }
                Ok(bytes)
            }
        }

        deserializer.deserialize_tuple(N, FixedBytesVisitor(PhantomData))
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct TickInsert {
    pub epoch: u16,
    pub tick: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub computor_index: u16,
    pub transaction_count: u32,
    pub event_count: u32,
    #[serde(with = "fixed_bytes")]
    pub signature: [u8; 64],
    #[serde(with = "fixed_bytes")]
    pub tick_digest: [u8; 32],
    pub verification: String,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct TickSelect {
    pub epoch: u16,
    pub tick: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub computor_index: u16,
    pub transaction_count: u32,
    pub event_count: u32,
    pub verification: String,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct TickFullSelect {
    pub epoch: u16,
    pub tick: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub computor_index: u16,
    pub transaction_count: u32,
    pub event_count: u32,
    #[serde(with = "fixed_bytes")]
    pub signature: [u8; 64],
    #[serde(with = "fixed_bytes")]
    pub tick_digest: [u8; 32],
    pub verification: String,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct TickNumberSelect {
    pub tick: u32,
}

impl From<TickSelect> for TickRow {
    fn from(row: TickSelect) -> Self {
        Self {
            epoch: row.epoch,
            tick: row.tick,
            timestamp: row.timestamp,
            computor_index: row.computor_index,
            transaction_count: row.transaction_count,
            event_count: row.event_count,
            verification: row.verification,
        }
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct TransactionInsert {
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
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct TransactionSelect {
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
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl From<TransactionSelect> for TransactionRow {
    fn from(row: TransactionSelect) -> Self {
        Self {
            epoch: row.epoch,
            tick: row.tick,
            tx_index: row.tx_index,
            hash: row.hash,
            source: row.source,
            destination: row.destination,
            amount: row.amount,
            input_type: row.input_type,
            input_data: row.input_data,
            status: row.status,
            timestamp: row.timestamp,
        }
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct EventInsert {
    pub epoch: u16,
    pub tick: u32,
    pub log_id: u64,
    pub log_type: u16,
    pub type_name: String,
    pub tx_hash: String,
    pub tx_index: i16,
    pub body: String,
    pub log_digest: u64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct EventSelect {
    pub epoch: u16,
    pub tick: u32,
    pub log_id: u64,
    pub log_type: u16,
    pub type_name: String,
    pub tx_hash: String,
    pub tx_index: i16,
    pub body: String,
    pub log_digest: u64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl From<EventSelect> for EventRow {
    fn from(row: EventSelect) -> Self {
        Self {
            epoch: row.epoch,
            tick: row.tick,
            log_id: row.log_id,
            log_type: row.log_type,
            type_name: row.type_name,
            tx_hash: row.tx_hash,
            tx_index: row.tx_index,
            body: row.body,
            log_digest: row.log_digest,
            timestamp: row.timestamp,
        }
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct EventParticipantInsert {
    pub epoch: u16,
    pub tick: u32,
    pub log_id: u64,
    pub identity: String,
    pub role: String,
    pub tx_hash: String,
    pub tx_index: i16,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct EventParticipantSelect {
    pub epoch: u16,
    pub tick: u32,
    pub log_id: u64,
    pub identity: String,
    pub role: String,
    pub tx_hash: String,
    pub tx_index: i16,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl From<EventParticipantSelect> for EventParticipantRow {
    fn from(row: EventParticipantSelect) -> Self {
        Self {
            epoch: row.epoch,
            tick: row.tick,
            log_id: row.log_id,
            identity: row.identity,
            role: row.role,
            tx_hash: row.tx_hash,
            tx_index: row.tx_index,
            timestamp: row.timestamp,
        }
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct ComputorInsert {
    pub epoch: u16,
    pub computor_index: u16,
    pub public_key: String,
    pub identity: String,
    pub version: u64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct ComputorSelect {
    pub epoch: u16,
    pub computor_index: u16,
    pub public_key: String,
    pub identity: String,
    pub version: u64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl From<ComputorSelect> for ComputorRow {
    fn from(row: ComputorSelect) -> Self {
        Self {
            epoch: row.epoch,
            computor_index: row.computor_index,
            public_key: row.public_key,
            identity: row.identity,
            version: row.version,
            timestamp: row.timestamp,
        }
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct QuorumEvidenceInsert {
    pub epoch: u16,
    pub tick: u32,
    #[serde(with = "fixed_bytes")]
    pub quorum_digest: [u8; 32],
    #[serde(with = "fixed_bytes")]
    pub tick_data_digest: [u8; 32],
    pub required_votes: u32,
    pub aligned_votes: u32,
    pub fetched_votes: u32,
    pub unique_votes: u32,
    pub verified_votes: u32,
    pub verification: String,
    pub warning_count: u32,
    pub warnings: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub version: u64,
}

impl From<&QuorumEvidenceRow> for QuorumEvidenceInsert {
    fn from(row: &QuorumEvidenceRow) -> Self {
        Self {
            epoch: row.epoch,
            tick: row.tick,
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
            version: row.version,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct QuorumEvidenceSelect {
    pub epoch: u16,
    pub tick: u32,
    pub quorum_digest: [u8; 32],
    pub tick_data_digest: [u8; 32],
    pub required_votes: u32,
    pub aligned_votes: u32,
    pub fetched_votes: u32,
    pub unique_votes: u32,
    pub verified_votes: u32,
    pub verification: String,
    pub warning_count: u32,
    pub warnings: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub version: u64,
}

impl From<QuorumEvidenceSelect> for QuorumEvidenceRow {
    fn from(row: QuorumEvidenceSelect) -> Self {
        Self {
            epoch: row.epoch,
            tick: row.tick,
            quorum_digest: row.quorum_digest,
            tick_data_digest: row.tick_data_digest,
            required_votes: row.required_votes,
            aligned_votes: row.aligned_votes,
            fetched_votes: row.fetched_votes,
            unique_votes: row.unique_votes,
            verified_votes: row.verified_votes,
            verification: row.verification,
            warning_count: row.warning_count,
            warnings: row.warnings,
            timestamp: row.timestamp,
            version: row.version,
        }
    }
}

/// Union shape of a direct transaction and an event-derived activity row.
///
/// `kind` selects which side produced the row. Transaction rows leave
/// `log_id`, `type_name`, `body`, and `role` empty; event rows leave
/// `input_*` and `status` empty. `source`, `destination`, and `amount` are
/// populated from the event body when it carries transfer fields.
#[derive(Debug, Row, Deserialize)]
pub(crate) struct IdentityActivitySelect {
    pub kind: String,
    pub epoch: u16,
    pub tick: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub tx_index: Option<i16>,
    pub log_id: Option<u64>,
    pub hash: String,
    pub source: String,
    pub destination: String,
    pub amount: i64,
    pub input_type: u16,
    pub input_data: String,
    pub status: String,
    pub type_name: String,
    pub body: String,
    pub role: String,
}

impl From<IdentityActivitySelect> for IdentityActivityRow {
    fn from(row: IdentityActivitySelect) -> Self {
        let is_transaction = row.kind == "transaction";
        Self {
            kind: row.kind,
            epoch: row.epoch,
            tick: row.tick,
            timestamp: row.timestamp,
            tx_index: row.tx_index,
            log_id: row.log_id,
            hash: row.hash,
            source: row.source,
            destination: row.destination,
            amount: row.amount,
            input_type: is_transaction.then_some(row.input_type),
            input_data: row.input_data,
            status: row.status,
            type_name: row.type_name,
            body: row.body,
            role: row.role,
        }
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct IdentityInsert {
    pub identity: String,
    pub public_key: String,
    pub balance: i64,
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub transfer_count: u64,
    pub first_seen_tick: u32,
    pub last_seen_tick: u32,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct IdentitySelect {
    pub identity: String,
    pub public_key: String,
    pub balance: i64,
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub transfer_count: u64,
    pub first_seen_tick: u32,
    pub last_seen_tick: u32,
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct EntitySnapshotInsert {
    pub identity: String,
    pub public_key: String,
    pub spectrum_index: i32,
    pub tick: u32,
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub incoming_transfers: u64,
    pub outgoing_transfers: u64,
    pub latest_incoming_tick: u32,
    pub latest_outgoing_tick: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub observed_at: DateTime<Utc>,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct EntitySnapshotSelect {
    pub identity: String,
    pub public_key: String,
    pub spectrum_index: i32,
    pub tick: u32,
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub incoming_transfers: u64,
    pub outgoing_transfers: u64,
    pub latest_incoming_tick: u32,
    pub latest_outgoing_tick: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub observed_at: DateTime<Utc>,
}

impl From<EntitySnapshotSelect> for EntitySnapshot {
    fn from(row: EntitySnapshotSelect) -> Self {
        Self {
            identity: row.identity,
            public_key: row.public_key,
            spectrum_index: row.spectrum_index,
            tick: row.tick,
            incoming_amount: row.incoming_amount,
            outgoing_amount: row.outgoing_amount,
            incoming_transfers: row.incoming_transfers,
            outgoing_transfers: row.outgoing_transfers,
            latest_incoming_tick: row.latest_incoming_tick,
            latest_outgoing_tick: row.latest_outgoing_tick,
            observed_at: row.observed_at,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct IdentityTotalsSelect {
    pub identity: String,
    pub incoming: i64,
    pub outgoing: i64,
    pub transfer_count: u64,
    pub first_seen_tick: u32,
    pub last_seen_tick: u32,
}

impl From<IdentityTotalsSelect> for IdentityTotals {
    fn from(row: IdentityTotalsSelect) -> Self {
        Self {
            identity: row.identity,
            incoming_amount: row.incoming,
            outgoing_amount: row.outgoing,
            transfer_count: row.transfer_count,
            first_seen_tick: row.first_seen_tick,
            last_seen_tick: row.last_seen_tick,
        }
    }
}

impl From<IdentitySelect> for IdentityRow {
    fn from(row: IdentitySelect) -> Self {
        Self {
            identity: row.identity,
            public_key: row.public_key,
            balance: row.balance,
            incoming_amount: row.incoming_amount,
            outgoing_amount: row.outgoing_amount,
            transfer_count: row.transfer_count,
            first_seen_tick: row.first_seen_tick,
            last_seen_tick: row.last_seen_tick,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct EpochSelect {
    pub epoch: u16,
    pub first_tick: u32,
    pub last_tick: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub first_timestamp: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub last_timestamp: DateTime<Utc>,
    pub tick_count: u64,
    pub transaction_count: u64,
    pub event_count: u64,
}

impl EpochSelect {
    pub(crate) fn into_row(
        self,
        head_epoch: Option<u16>,
        metadata: Option<&EpochMetadata>,
    ) -> EpochRow {
        EpochRow {
            // A later indexed epoch proves this one ended, even when metadata
            // was left open because the boundary successor was never observed.
            closed: metadata.is_some_and(|metadata| metadata.closed)
                || head_epoch.is_some_and(|head| self.epoch < head),
            initial_tick: metadata
                .map(|metadata| metadata.initial_tick)
                .unwrap_or(self.first_tick),
            epoch: self.epoch,
            first_tick: self.first_tick,
            last_tick: self.last_tick,
            first_timestamp: self.first_timestamp,
            last_timestamp: self.last_timestamp,
            tick_count: self.tick_count,
            transaction_count: self.transaction_count,
            event_count: self.event_count,
        }
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct EpochInsert {
    pub epoch: u16,
    pub initial_tick: u32,
    pub first_tick: u32,
    pub last_tick: u32,
    pub closed: u8,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub updated_at: DateTime<Utc>,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct EpochBoundarySelect {
    pub epoch: u16,
    pub initial_tick: u32,
}

impl From<EpochBoundarySelect> for crate::EpochBoundary {
    fn from(row: EpochBoundarySelect) -> Self {
        Self {
            epoch: row.epoch,
            initial_tick: row.initial_tick,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct EpochMetadataSelect {
    pub epoch: u16,
    pub initial_tick: u32,
    pub first_tick: u32,
    pub last_tick: u32,
    pub closed: u8,
}

impl From<EpochMetadataSelect> for EpochMetadata {
    fn from(row: EpochMetadataSelect) -> Self {
        Self {
            epoch: row.epoch,
            initial_tick: row.initial_tick,
            first_tick: row.first_tick,
            last_tick: row.last_tick,
            closed: row.closed != 0,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct AssetSelect {
    pub issuer: String,
    pub name: String,
    pub decimals: u8,
    pub unit: String,
    pub managing_contract: i64,
    pub shares: i64,
    pub issued_tick: u32,
    pub log_id: u64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl From<AssetSelect> for AssetRow {
    fn from(row: AssetSelect) -> Self {
        Self {
            issuer: row.issuer,
            name: row.name,
            decimals: row.decimals,
            unit: row.unit,
            managing_contract: row.managing_contract,
            shares: row.shares,
            issued_tick: row.issued_tick,
            log_id: row.log_id,
            timestamp: row.timestamp,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct ActivityBucketSelect {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub bucket: DateTime<Utc>,
    pub ticks: u64,
    pub transactions: u64,
    pub events: u64,
    pub volume: i64,
}

impl From<ActivityBucketSelect> for ActivityBucket {
    fn from(row: ActivityBucketSelect) -> Self {
        Self {
            bucket: row.bucket,
            ticks: row.ticks,
            transactions: row.transactions,
            events: row.events,
            volume: row.volume,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct EventTypeCountSelect {
    pub type_name: String,
    pub n: u64,
}

impl From<EventTypeCountSelect> for EventTypeCount {
    fn from(row: EventTypeCountSelect) -> Self {
        Self {
            type_name: row.type_name,
            count: row.n,
        }
    }
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct CursorInsert {
    pub key: String,
    pub value: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub updated_at: DateTime<Utc>,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct CursorSelect {
    pub value: String,
}

#[derive(Debug, Row, Serialize)]
pub(crate) struct PricePointInsert {
    pub hour: u32,
    pub append_tick: u32,
    pub source_tick: u32,
    pub epoch: u16,
    pub flags: u32,
    pub price_e8: i64,
    pub source: u32,
    pub synced: u8,
    pub bound: u8,
    /// `[u8; 32]` serializes as a fixed tuple of 32 bytes, matching
    /// `FixedString(32)` in RowBinary.
    pub series_digest: [u8; 32],
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub observed_at: DateTime<Utc>,
    pub version: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct PricePointSelect {
    pub hour: u32,
    pub append_tick: u32,
    pub source_tick: u32,
    pub epoch: u16,
    pub flags: u32,
    pub price_e8: i64,
    pub source: u32,
    pub synced: u8,
    pub bound: u8,
    pub series_digest: [u8; 32],
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub observed_at: DateTime<Utc>,
    pub version: u64,
}

impl From<PricePointSelect> for PricePointRow {
    fn from(row: PricePointSelect) -> Self {
        Self {
            hour: row.hour,
            append_tick: row.append_tick,
            source_tick: row.source_tick,
            epoch: row.epoch,
            flags: row.flags,
            price_e8: row.price_e8,
            source: row.source,
            synced: row.synced,
            bound: row.bound,
            series_digest: row.series_digest,
            observed_at: row.observed_at,
            version: row.version,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct CounterpartySelect {
    pub counterparty: String,
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub net_amount: i64,
    pub transaction_count: u64,
    pub first_tick: u32,
    pub last_tick: u32,
}

impl From<CounterpartySelect> for CounterpartyRow {
    fn from(row: CounterpartySelect) -> Self {
        Self {
            identity: row.counterparty,
            incoming_amount: row.incoming_amount,
            outgoing_amount: row.outgoing_amount,
            net_amount: row.net_amount,
            transaction_count: row.transaction_count,
            first_tick: row.first_tick,
            last_tick: row.last_tick,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct FlowBucketSelect {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub bucket: DateTime<Utc>,
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub net_amount: i64,
    pub transaction_count: u64,
    pub counterparty_count: u64,
}

impl From<FlowBucketSelect> for FlowBucket {
    fn from(row: FlowBucketSelect) -> Self {
        Self {
            bucket: row.bucket,
            incoming_amount: row.incoming_amount,
            outgoing_amount: row.outgoing_amount,
            net_amount: row.net_amount,
            transaction_count: row.transaction_count,
            counterparty_count: row.counterparty_count,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct AssetTransferSelect {
    pub epoch: u16,
    pub tick: u32,
    pub log_id: u64,
    pub kind: String,
    pub hash: String,
    pub tx_index: i16,
    pub issuer: String,
    pub name: String,
    pub from_identity: String,
    pub to_identity: String,
    pub shares: i64,
    pub managing_contract: i64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
}

impl From<AssetTransferSelect> for AssetTransferRow {
    fn from(row: AssetTransferSelect) -> Self {
        Self {
            epoch: row.epoch,
            tick: row.tick,
            log_id: row.log_id,
            kind: row.kind,
            hash: row.hash,
            tx_index: row.tx_index,
            issuer: row.issuer,
            name: row.name,
            from: row.from_identity,
            to: row.to_identity,
            shares: row.shares,
            managing_contract: row.managing_contract,
            timestamp: row.timestamp,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct CountRow {
    pub n: u64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct EpochNumberRow {
    pub epoch: u16,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct EarliestTickRow {
    pub tick: u32,
    pub n: u64,
}

pub(crate) async fn insert_cursor(
    client: &clickhouse::Client,
    key: &str,
    value: &str,
    updated_at: DateTime<Utc>,
) -> Result<()> {
    let mut insert = client.insert::<CursorInsert>("ingest_state")?;
    insert
        .write(&CursorInsert {
            key: key.to_owned(),
            value: value.to_owned(),
            updated_at,
            version: next_cursor_version(updated_at),
        })
        .await?;
    insert.end().await?;
    Ok(())
}

/// Cursors use `ReplacingMergeTree(version)`. A process-local monotonic
/// counter keeps writes ordered even when they land in the same millisecond.
pub(crate) fn next_cursor_version(now: DateTime<Utc>) -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};

    static LAST: AtomicU64 = AtomicU64::new(0);

    let mut previous = LAST.load(Ordering::Relaxed);
    loop {
        let base = u64::try_from(now.timestamp_micros()).unwrap_or(0);
        let next = base.max(previous.saturating_add(1));
        match LAST.compare_exchange_weak(previous, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(actual) => previous = actual,
        }
    }
}
