use crate::crypto::k12::k12_32;
use crate::error::{Error, Result};
use crate::wire::{MessageType, Reader};

pub const COMPUTOR_COUNT: usize = 676;
pub const QUORUM: usize = 451;
pub const EMPTY_TICK_QUORUM: usize = 226;
pub const TX_PER_TICK: usize = 4096;
pub const SPECIAL_EVENT_COUNT: usize = 6;
pub const LOG_TX_PER_TICK: usize = TX_PER_TICK + SPECIAL_EVENT_COUNT;
pub const MAX_CONTRACTS: usize = 1024;
pub const TICK_SIZE: usize = 352;
pub const TICK_DATA_SIZE: usize = 139_376;
pub const COMPUTORS_SIZE: usize = 21_698;
pub const CURRENT_TICK_INFO_SIZE: usize = 16;
pub const LOG_RANGES_SIZE: usize = LOG_TX_PER_TICK * 16;
pub const REQUEST_QUORUM_TICK_SIZE: usize = 92;
pub const REQUEST_TICK_TRANSACTIONS_SIZE: usize = 516;
pub const MAX_INPUT_SIZE: usize = 1024;
/// Largest amount a transaction may carry (`ISSUANCE_RATE * 1000`).
pub const MAX_AMOUNT: i64 = 1_000_000_000_000_000;
pub const ASSET_RECORD_SIZE: usize = 48;
pub const ISSUANCE_RECORD_SIZE: usize = 48;
pub const OWNERSHIP_RECORD_SIZE: usize = 48;
pub const POSSESSION_RECORD_SIZE: usize = 48;
pub const REQUEST_ASSETS_SIZE: usize = 112;
pub const RESPOND_ASSETS_SIZE: usize = 56;
pub const RESPOND_ASSETS_WITH_SIBLINGS_SIZE: usize = 824;
pub const SPECTRUM_DEPTH: usize = 24;
pub const ASSETS_DEPTH: usize = 24;
pub const ENTITY_RECORD_SIZE: usize = 64;
pub const RESPOND_ENTITY_SIZE: usize = ENTITY_RECORD_SIZE + 8 + 32 * SPECTRUM_DEPTH;
pub const RESPOND_SYSTEM_INFO_SIZE: usize = 128;
pub const REQUEST_CONTRACT_FUNCTION_SIZE: usize = 8;
pub const RESPOND_CONTRACT_IPO_SIZE: usize = 8 + 32 * COMPUTOR_COUNT + 8 * COMPUTOR_COUNT;
pub const RESPOND_ISSUED_ASSETS_SIZE: usize = ASSET_RECORD_SIZE + 8 + 32 * ASSETS_DEPTH;
pub const RESPOND_OWNED_ASSETS_SIZE: usize = 2 * ASSET_RECORD_SIZE + 8 + 32 * ASSETS_DEPTH;
pub const RESPOND_POSSESSED_ASSETS_SIZE: usize = 3 * ASSET_RECORD_SIZE + 8 + 32 * ASSETS_DEPTH;
pub const REQUEST_ENTITY_SIZE: usize = 32;
pub const REQUEST_TRANSACTION_INFO_SIZE: usize = 32;
pub const REQUEST_TX_STATUS_SIZE: usize = 4;
/// Fixed part of `RespondTxStatus`: three `u32` fields and the money-flew bitmap.
pub const RESPOND_TX_STATUS_HEADER_SIZE: usize = 4 + 4 + 4 + TX_PER_TICK.div_ceil(8);
pub const REQUEST_LOG_SIZE: usize = 48;
pub const REQUEST_ALL_LOG_RANGES_SIZE: usize = 36;
pub const REQUEST_LOG_ID_RANGE_FROM_TX_SIZE: usize = 40;
pub const RESPOND_LOG_ID_RANGE_FROM_TX_SIZE: usize = 16;
pub const REQUEST_PRUNING_LOG_SIZE: usize = 48;
pub const RESPOND_PRUNING_LOG_SIZE: usize = 8;
pub const REQUEST_LOG_STATE_DIGEST_SIZE: usize = 36;
pub const RESPOND_LOG_STATE_DIGEST_SIZE: usize = 32;
pub const ASSET_NAME_LEN: usize = 7;
pub const ASSET_UNIT_LEN: usize = 7;
pub const ZERO_DIGEST: [u8; 32] = [0u8; 32];

pub const ASSET_TYPE_EMPTY: u8 = 0;
pub const ASSET_TYPE_ISSUANCE: u8 = 1;
pub const ASSET_TYPE_OWNERSHIP: u8 = 2;
pub const ASSET_TYPE_POSSESSION: u8 = 3;

pub const ASSET_REQUEST_ISSUANCE: u16 = 0;
pub const ASSET_REQUEST_OWNERSHIP: u16 = 1;
pub const ASSET_REQUEST_POSSESSION: u16 = 2;
pub const ASSET_REQUEST_BY_UNIVERSE_INDEX: u16 = 3;
pub const ASSET_REQUEST_GET_SIBLINGS: u16 = 0b1;
pub const ASSET_REQUEST_ANY_ISSUER: u16 = 0b10;
pub const ASSET_REQUEST_ANY_ASSET_NAME: u16 = 0b100;
pub const ASSET_REQUEST_ANY_OWNER: u16 = 0b1000;
pub const ASSET_REQUEST_ANY_OWNERSHIP_MANAGING_CONTRACT: u16 = 0b10000;
pub const ASSET_REQUEST_ANY_POSSESSOR: u16 = 0b100000;
pub const ASSET_REQUEST_ANY_POSSESSION_MANAGING_CONTRACT: u16 = 0b1000000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tick {
    pub computor_index: u16,
    pub epoch: u16,
    pub tick: u32,
    pub millisecond: u16,
    pub second: u8,
    pub minute: u8,
    pub hour: u8,
    pub day: u8,
    pub month: u8,
    pub year: u8,
    pub prev_resource_testing_digest: u32,
    pub salted_resource_testing_digest: u32,
    pub prev_transaction_body_digest: u32,
    pub salted_transaction_body_digest: u32,
    pub prev_spectrum_digest: [u8; 32],
    pub prev_universe_digest: [u8; 32],
    pub prev_computer_digest: [u8; 32],
    pub salted_spectrum_digest: [u8; 32],
    pub salted_universe_digest: [u8; 32],
    pub salted_computer_digest: [u8; 32],
    pub transaction_digest: [u8; 32],
    pub expected_next_tick_transaction_digest: [u8; 32],
    pub signature: [u8; 64],
}

impl Tick {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != TICK_SIZE {
            return Err(Error::InvalidPacket(format!(
                "tick needs {TICK_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        Ok(Self {
            computor_index: r.u16()?,
            epoch: r.u16()?,
            tick: r.u32()?,
            millisecond: r.u16()?,
            second: r.u8()?,
            minute: r.u8()?,
            hour: r.u8()?,
            day: r.u8()?,
            month: r.u8()?,
            year: r.u8()?,
            prev_resource_testing_digest: r.u32()?,
            salted_resource_testing_digest: r.u32()?,
            prev_transaction_body_digest: r.u32()?,
            salted_transaction_body_digest: r.u32()?,
            prev_spectrum_digest: r.array()?,
            prev_universe_digest: r.array()?,
            prev_computer_digest: r.array()?,
            salted_spectrum_digest: r.array()?,
            salted_universe_digest: r.array()?,
            salted_computer_digest: r.array()?,
            transaction_digest: r.array()?,
            expected_next_tick_transaction_digest: r.array()?,
            signature: r.array()?,
        })
    }

    pub fn encode(&self) -> [u8; TICK_SIZE] {
        let mut out = [0u8; TICK_SIZE];
        let mut o = 0;
        let mut put = |bytes: &[u8]| {
            out[o..o + bytes.len()].copy_from_slice(bytes);
            o += bytes.len();
        };
        put(&self.computor_index.to_le_bytes());
        put(&self.epoch.to_le_bytes());
        put(&self.tick.to_le_bytes());
        put(&self.millisecond.to_le_bytes());
        put(&[
            self.second,
            self.minute,
            self.hour,
            self.day,
            self.month,
            self.year,
        ]);
        put(&self.prev_resource_testing_digest.to_le_bytes());
        put(&self.salted_resource_testing_digest.to_le_bytes());
        put(&self.prev_transaction_body_digest.to_le_bytes());
        put(&self.salted_transaction_body_digest.to_le_bytes());
        put(&self.prev_spectrum_digest);
        put(&self.prev_universe_digest);
        put(&self.prev_computer_digest);
        put(&self.salted_spectrum_digest);
        put(&self.salted_universe_digest);
        put(&self.salted_computer_digest);
        put(&self.transaction_digest);
        put(&self.expected_next_tick_transaction_digest);
        put(&self.signature);
        debug_assert_eq!(o, TICK_SIZE);
        out
    }

    pub fn digest(&self) -> [u8; 32] {
        k12_32(&self.encode())
    }

    pub fn is_empty_vote(&self) -> bool {
        self.transaction_digest == ZERO_DIGEST
    }

    /// Timestamp encoded by the tick as a Unix timestamp in milliseconds.
    pub fn timestamp_millis(&self) -> i64 {
        let year = 2000i32 + self.year as i32;
        let month = self.month.max(1) as u32;
        let day = self.day.max(1) as u32;
        let days = days_from_civil(year, month, day);
        let seconds =
            days * 86_400 + self.hour as i64 * 3_600 + self.minute as i64 * 60 + self.second as i64;
        seconds * 1_000 + self.millisecond as i64
    }

    pub fn display_time(&self) -> String {
        format!(
            "{:02}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
            self.year, self.month, self.day, self.hour, self.minute, self.second, self.millisecond
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickData {
    pub computor_index: u16,
    pub epoch: u16,
    pub tick: u32,
    pub millisecond: u16,
    pub second: u8,
    pub minute: u8,
    pub hour: u8,
    pub day: u8,
    pub month: u8,
    pub year: u8,
    pub timelock: [u8; 32],
    pub transaction_digests: Vec<[u8; 32]>,
    pub contract_fees: Vec<i64>,
    pub signature: [u8; 64],
}

impl TickData {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != TICK_DATA_SIZE {
            return Err(Error::InvalidPacket(format!(
                "tick data needs {TICK_DATA_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        let computor_index = r.u16()?;
        let epoch = r.u16()?;
        let tick = r.u32()?;
        let millisecond = r.u16()?;
        let second = r.u8()?;
        let minute = r.u8()?;
        let hour = r.u8()?;
        let day = r.u8()?;
        let month = r.u8()?;
        let year = r.u8()?;
        let timelock = r.array()?;
        let mut transaction_digests = Vec::with_capacity(TX_PER_TICK);
        for _ in 0..TX_PER_TICK {
            transaction_digests.push(r.array()?);
        }
        let mut contract_fees = Vec::with_capacity(MAX_CONTRACTS);
        for _ in 0..MAX_CONTRACTS {
            contract_fees.push(r.i64()?);
        }
        let signature = r.array()?;
        Ok(Self {
            computor_index,
            epoch,
            tick,
            millisecond,
            second,
            minute,
            hour,
            day,
            month,
            year,
            timelock,
            transaction_digests,
            contract_fees,
            signature,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(TICK_DATA_SIZE);
        out.extend_from_slice(&self.computor_index.to_le_bytes());
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.tick.to_le_bytes());
        out.extend_from_slice(&self.millisecond.to_le_bytes());
        out.extend_from_slice(&[
            self.second,
            self.minute,
            self.hour,
            self.day,
            self.month,
            self.year,
        ]);
        out.extend_from_slice(&self.timelock);
        for digest in &self.transaction_digests {
            out.extend_from_slice(digest);
        }
        for fee in &self.contract_fees {
            out.extend_from_slice(&fee.to_le_bytes());
        }
        out.extend_from_slice(&self.signature);
        out
    }

    pub fn digest(&self) -> [u8; 32] {
        k12_32(&self.encode())
    }

    pub fn non_empty_transactions(&self) -> impl Iterator<Item = (usize, &[u8; 32])> {
        self.transaction_digests
            .iter()
            .enumerate()
            .filter(|(_, digest)| **digest != ZERO_DIGEST)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    pub source: [u8; 32],
    pub destination: [u8; 32],
    pub amount: i64,
    pub tick: u32,
    pub input_type: u16,
    pub input: Vec<u8>,
    pub signature: [u8; 64],
}

impl Transaction {
    pub const BASE_SIZE: usize = 80;

    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() < Self::BASE_SIZE + 64 {
            return Err(Error::InvalidPacket(format!(
                "transaction needs at least {} bytes, got {}",
                Self::BASE_SIZE + 64,
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        let source = r.array()?;
        let destination = r.array()?;
        let amount = r.i64()?;
        let tick = r.u32()?;
        let input_type = r.u16()?;
        let input_size = r.u16()? as usize;
        if payload.len() != Self::BASE_SIZE + input_size + 64 {
            return Err(Error::InvalidPacket(format!(
                "transaction size {} does not match input size {input_size}",
                payload.len()
            )));
        }
        let input = r.bytes(input_size)?.to_vec();
        let signature = r.array()?;
        Ok(Self {
            source,
            destination,
            amount,
            tick,
            input_type,
            input,
            signature,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::BASE_SIZE + self.input.len() + 64);
        out.extend_from_slice(&self.source);
        out.extend_from_slice(&self.destination);
        out.extend_from_slice(&self.amount.to_le_bytes());
        out.extend_from_slice(&self.tick.to_le_bytes());
        out.extend_from_slice(&self.input_type.to_le_bytes());
        out.extend_from_slice(&(self.input.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.input);
        out.extend_from_slice(&self.signature);
        out
    }

    /// Transaction hashes in Qubic are K12 over the full signed transaction.
    pub fn hash(&self) -> [u8; 32] {
        k12_32(&self.encode())
    }

    pub fn total_size(&self) -> usize {
        Self::BASE_SIZE + self.input.len() + 64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Computors {
    pub epoch: u16,
    pub public_keys: Vec<[u8; 32]>,
    pub signature: [u8; 64],
}

impl Computors {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != COMPUTORS_SIZE {
            return Err(Error::InvalidPacket(format!(
                "computors need {COMPUTORS_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        let epoch = r.u16()?;
        let mut public_keys = Vec::with_capacity(COMPUTOR_COUNT);
        for _ in 0..COMPUTOR_COUNT {
            public_keys.push(r.array()?);
        }
        let signature = r.array()?;
        Ok(Self {
            epoch,
            public_keys,
            signature,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(COMPUTORS_SIZE);
        out.extend_from_slice(&self.epoch.to_le_bytes());
        for key in &self.public_keys {
            out.extend_from_slice(key);
        }
        out.extend_from_slice(&self.signature);
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CurrentTickInfo {
    pub tick_duration: u16,
    pub epoch: u16,
    pub tick: u32,
    pub aligned_votes: u16,
    pub misaligned_votes: u16,
    pub initial_tick: u32,
}

impl CurrentTickInfo {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != CURRENT_TICK_INFO_SIZE {
            return Err(Error::InvalidPacket(format!(
                "current tick info needs {CURRENT_TICK_INFO_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        Ok(Self {
            tick_duration: r.u16()?,
            epoch: r.u16()?,
            tick: r.u32()?,
            aligned_votes: r.u16()?,
            misaligned_votes: r.u16()?,
            initial_tick: r.u32()?,
        })
    }

    pub fn encode(&self) -> [u8; CURRENT_TICK_INFO_SIZE] {
        let mut out = [0u8; CURRENT_TICK_INFO_SIZE];
        out[0..2].copy_from_slice(&self.tick_duration.to_le_bytes());
        out[2..4].copy_from_slice(&self.epoch.to_le_bytes());
        out[4..8].copy_from_slice(&self.tick.to_le_bytes());
        out[8..10].copy_from_slice(&self.aligned_votes.to_le_bytes());
        out[10..12].copy_from_slice(&self.misaligned_votes.to_le_bytes());
        out[12..16].copy_from_slice(&self.initial_tick.to_le_bytes());
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRanges {
    pub from_log_id: Vec<i64>,
    pub length: Vec<i64>,
}

impl LogRanges {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != LOG_RANGES_SIZE {
            return Err(Error::InvalidPacket(format!(
                "log ranges need {LOG_RANGES_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        let mut from_log_id = Vec::with_capacity(LOG_TX_PER_TICK);
        let mut length = Vec::with_capacity(LOG_TX_PER_TICK);
        for _ in 0..LOG_TX_PER_TICK {
            from_log_id.push(r.i64()?);
        }
        for _ in 0..LOG_TX_PER_TICK {
            length.push(r.i64()?);
        }
        Ok(Self {
            from_log_id,
            length,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(LOG_RANGES_SIZE);
        for value in &self.from_log_id {
            out.extend_from_slice(&value.to_le_bytes());
        }
        for value in &self.length {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    pub fn range(&self) -> Option<(i64, i64)> {
        let mut min = i64::MAX;
        let mut max = -1i64;
        for (from, len) in self.from_log_id.iter().zip(self.length.iter()) {
            if *from < 0 || *len < 0 {
                continue;
            }
            min = min.min(*from);
            max = max.max(*from + *len);
        }
        if min == i64::MAX {
            None
        } else {
            Some((min, max - 1))
        }
    }

    pub fn is_pending(&self) -> bool {
        self.from_log_id.contains(&-3) || self.length.contains(&-3)
    }

    pub fn count(&self) -> i64 {
        self.length
            .iter()
            .zip(self.from_log_id.iter())
            .filter(|(len, from)| **from >= 0 && **len > 0)
            .map(|(len, _)| *len)
            .sum()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerAddress {
    pub ip: [u8; 4],
}

impl PeerAddress {
    pub fn decode(payload: &[u8]) -> Result<Vec<Self>> {
        if payload.len() % 4 != 0 {
            return Err(Error::InvalidPacket(
                "peer exchange payload is not 4-byte aligned".into(),
            ));
        }
        Ok(payload
            .chunks_exact(4)
            .map(|chunk| Self {
                ip: [chunk[0], chunk[1], chunk[2], chunk[3]],
            })
            .filter(|p| p.ip != [0, 0, 0, 0])
            .collect())
    }
}

impl std::fmt::Display for PeerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}.{}",
            self.ip[0], self.ip[1], self.ip[2], self.ip[3]
        )
    }
}

/// Decodes a fixed-size asset name (up to 7 ASCII letters/digits).
pub fn asset_name_string(name: &[u8]) -> String {
    name.iter()
        .copied()
        .take_while(|byte| *byte != 0)
        .map(|byte| byte as char)
        .collect()
}

/// Decodes the 7-byte SI unit-of-measurement field.
///
/// Each byte stores a (possibly negative) power of the corresponding SI base
/// unit and is rendered by adding 48, matching core and bob. Unlike the asset
/// name, zeros are meaningful digits ("0"), so all seven bytes are kept.
pub fn asset_unit_string(unit: &[u8]) -> String {
    unit.iter()
        .copied()
        .map(|byte| char::from(byte.wrapping_add(48)))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssetIssuanceRecord {
    pub public_key: [u8; 32],
    pub name: [u8; ASSET_NAME_LEN],
    pub decimals: u8,
    pub unit: [u8; ASSET_UNIT_LEN],
}

impl AssetIssuanceRecord {
    pub fn name(&self) -> String {
        asset_name_string(&self.name)
    }

    pub fn unit(&self) -> String {
        asset_unit_string(&self.unit)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssetOwnershipRecord {
    pub public_key: [u8; 32],
    pub managing_contract_index: u16,
    pub issuance_index: u32,
    pub shares: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssetPossessionRecord {
    pub public_key: [u8; 32],
    pub managing_contract_index: u16,
    pub ownership_index: u32,
    pub shares: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetRecordKind {
    Empty,
    Issuance(AssetIssuanceRecord),
    Ownership(AssetOwnershipRecord),
    Possession(AssetPossessionRecord),
    /// Unrecognised asset record type, preserved verbatim for forward
    /// compatibility.
    Unknown {
        record_type: u8,
        raw: [u8; ASSET_RECORD_SIZE],
    },
}

/// The 48-byte `AssetRecord` union from `network_messages/assets.h`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetRecord {
    pub public_key: [u8; 32],
    pub kind: AssetRecordKind,
}

impl AssetRecord {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != ASSET_RECORD_SIZE {
            return Err(Error::InvalidPacket(format!(
                "asset record needs {ASSET_RECORD_SIZE} bytes, got {}",
                bytes.len()
            )));
        }
        let mut raw = [0u8; ASSET_RECORD_SIZE];
        raw.copy_from_slice(bytes);
        let public_key: [u8; 32] = raw[0..32].try_into().expect("32 bytes");
        let record_type = raw[32];
        let kind = match record_type {
            ASSET_TYPE_EMPTY => AssetRecordKind::Empty,
            ASSET_TYPE_ISSUANCE => AssetRecordKind::Issuance(AssetIssuanceRecord {
                public_key,
                name: raw[33..40].try_into().expect("7 bytes"),
                decimals: raw[40],
                unit: raw[41..48].try_into().expect("7 bytes"),
            }),
            ASSET_TYPE_OWNERSHIP => AssetRecordKind::Ownership(AssetOwnershipRecord {
                public_key,
                managing_contract_index: u16::from_le_bytes([raw[34], raw[35]]),
                issuance_index: u32::from_le_bytes([raw[36], raw[37], raw[38], raw[39]]),
                shares: i64::from_le_bytes(raw[40..48].try_into().expect("8 bytes")),
            }),
            ASSET_TYPE_POSSESSION => AssetRecordKind::Possession(AssetPossessionRecord {
                public_key,
                managing_contract_index: u16::from_le_bytes([raw[34], raw[35]]),
                ownership_index: u32::from_le_bytes([raw[36], raw[37], raw[38], raw[39]]),
                shares: i64::from_le_bytes(raw[40..48].try_into().expect("8 bytes")),
            }),
            _ => AssetRecordKind::Unknown { record_type, raw },
        };
        Ok(Self { public_key, kind })
    }

    pub fn encode(&self) -> [u8; ASSET_RECORD_SIZE] {
        let mut out = [0u8; ASSET_RECORD_SIZE];
        match &self.kind {
            AssetRecordKind::Empty => {
                out[0..32].copy_from_slice(&self.public_key);
                out[32] = ASSET_TYPE_EMPTY;
            }
            AssetRecordKind::Issuance(record) => {
                out[0..32].copy_from_slice(&record.public_key);
                out[32] = ASSET_TYPE_ISSUANCE;
                out[33..40].copy_from_slice(&record.name);
                out[40] = record.decimals;
                out[41..48].copy_from_slice(&record.unit);
            }
            AssetRecordKind::Ownership(record) => {
                out[0..32].copy_from_slice(&record.public_key);
                out[32] = ASSET_TYPE_OWNERSHIP;
                out[34..36].copy_from_slice(&record.managing_contract_index.to_le_bytes());
                out[36..40].copy_from_slice(&record.issuance_index.to_le_bytes());
                out[40..48].copy_from_slice(&record.shares.to_le_bytes());
            }
            AssetRecordKind::Possession(record) => {
                out[0..32].copy_from_slice(&record.public_key);
                out[32] = ASSET_TYPE_POSSESSION;
                out[34..36].copy_from_slice(&record.managing_contract_index.to_le_bytes());
                out[36..40].copy_from_slice(&record.ownership_index.to_le_bytes());
                out[40..48].copy_from_slice(&record.shares.to_le_bytes());
            }
            AssetRecordKind::Unknown { raw, .. } => out.copy_from_slice(raw),
        }
        out
    }
}

/// `RequestAssets` (`network_messages/assets.h`, 112 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestAssets {
    ByFilter {
        request_type: u16,
        flags: u16,
        ownership_managing_contract: u16,
        possession_managing_contract: u16,
        issuer: [u8; 32],
        asset_name: u64,
        owner: [u8; 32],
        possessor: [u8; 32],
    },
    ByUniverseIndex {
        flags: u16,
        universe_index: u32,
    },
}

impl RequestAssets {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != REQUEST_ASSETS_SIZE {
            return Err(Error::InvalidPacket(format!(
                "request assets needs {REQUEST_ASSETS_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let request_type = u16::from_le_bytes([payload[0], payload[1]]);
        let flags = u16::from_le_bytes([payload[2], payload[3]]);
        if request_type == ASSET_REQUEST_BY_UNIVERSE_INDEX {
            return Ok(Self::ByUniverseIndex {
                flags,
                universe_index: u32::from_le_bytes([
                    payload[4], payload[5], payload[6], payload[7],
                ]),
            });
        }
        let mut issuer = [0u8; 32];
        issuer.copy_from_slice(&payload[8..40]);
        let mut owner = [0u8; 32];
        owner.copy_from_slice(&payload[48..80]);
        let mut possessor = [0u8; 32];
        possessor.copy_from_slice(&payload[80..112]);
        Ok(Self::ByFilter {
            request_type,
            flags,
            ownership_managing_contract: u16::from_le_bytes([payload[4], payload[5]]),
            possession_managing_contract: u16::from_le_bytes([payload[6], payload[7]]),
            issuer,
            asset_name: u64::from_le_bytes(payload[40..48].try_into().expect("8 bytes")),
            owner,
            possessor,
        })
    }

    pub fn request_type(&self) -> u16 {
        match self {
            Self::ByFilter { request_type, .. } => *request_type,
            Self::ByUniverseIndex { .. } => ASSET_REQUEST_BY_UNIVERSE_INDEX,
        }
    }

    pub fn encode(&self) -> [u8; REQUEST_ASSETS_SIZE] {
        let mut out = [0u8; REQUEST_ASSETS_SIZE];
        match self {
            Self::ByFilter {
                request_type,
                flags,
                ownership_managing_contract,
                possession_managing_contract,
                issuer,
                asset_name,
                owner,
                possessor,
            } => {
                out[0..2].copy_from_slice(&request_type.to_le_bytes());
                out[2..4].copy_from_slice(&flags.to_le_bytes());
                out[4..6].copy_from_slice(&ownership_managing_contract.to_le_bytes());
                out[6..8].copy_from_slice(&possession_managing_contract.to_le_bytes());
                out[8..40].copy_from_slice(issuer);
                out[40..48].copy_from_slice(&asset_name.to_le_bytes());
                out[48..80].copy_from_slice(owner);
                out[80..112].copy_from_slice(possessor);
            }
            Self::ByUniverseIndex {
                flags,
                universe_index,
            } => {
                out[0..2].copy_from_slice(&ASSET_REQUEST_BY_UNIVERSE_INDEX.to_le_bytes());
                out[2..4].copy_from_slice(&flags.to_le_bytes());
                out[4..8].copy_from_slice(&universe_index.to_le_bytes());
            }
        }
        out
    }
}

/// `RespondAssets` (`network_messages/assets.h`, 56 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespondAssets {
    pub asset: AssetRecord,
    pub tick: u32,
    pub universe_index: u32,
}

impl RespondAssets {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != RESPOND_ASSETS_SIZE {
            return Err(Error::InvalidPacket(format!(
                "respond assets needs {RESPOND_ASSETS_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        Ok(Self {
            asset: AssetRecord::decode(&payload[0..ASSET_RECORD_SIZE])?,
            tick: u32::from_le_bytes(payload[48..52].try_into().expect("4 bytes")),
            universe_index: u32::from_le_bytes(payload[52..56].try_into().expect("4 bytes")),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(RESPOND_ASSETS_SIZE);
        out.extend_from_slice(&self.asset.encode());
        out.extend_from_slice(&self.tick.to_le_bytes());
        out.extend_from_slice(&self.universe_index.to_le_bytes());
        out
    }
}

/// `RespondAssetsWithSiblings` (`network_messages/assets.h`, 824 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespondAssetsWithSiblings {
    pub asset: AssetRecord,
    pub tick: u32,
    pub universe_index: u32,
    pub siblings: Vec<[u8; 32]>,
}

impl RespondAssetsWithSiblings {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != RESPOND_ASSETS_WITH_SIBLINGS_SIZE {
            return Err(Error::InvalidPacket(format!(
                "respond assets with siblings needs {RESPOND_ASSETS_WITH_SIBLINGS_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(&payload[RESPOND_ASSETS_SIZE..]);
        let mut siblings = Vec::with_capacity(ASSETS_DEPTH);
        for _ in 0..ASSETS_DEPTH {
            siblings.push(r.array()?);
        }
        Ok(Self {
            asset: AssetRecord::decode(&payload[0..ASSET_RECORD_SIZE])?,
            tick: u32::from_le_bytes(payload[48..52].try_into().expect("4 bytes")),
            universe_index: u32::from_le_bytes(payload[52..56].try_into().expect("4 bytes")),
            siblings,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(RESPOND_ASSETS_WITH_SIBLINGS_SIZE);
        out.extend_from_slice(&self.asset.encode());
        out.extend_from_slice(&self.tick.to_le_bytes());
        out.extend_from_slice(&self.universe_index.to_le_bytes());
        for sibling in &self.siblings {
            out.extend_from_slice(sibling);
        }
        out
    }
}

/// `EntityRecord` (`network_messages/entity.h`, 64 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EntityRecord {
    pub public_key: [u8; 32],
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub number_of_incoming_transfers: u32,
    pub number_of_outgoing_transfers: u32,
    pub latest_incoming_transfer_tick: u32,
    pub latest_outgoing_transfer_tick: u32,
}

impl EntityRecord {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != ENTITY_RECORD_SIZE {
            return Err(Error::InvalidPacket(format!(
                "entity record needs {ENTITY_RECORD_SIZE} bytes, got {}",
                bytes.len()
            )));
        }
        let mut r = Reader::new(bytes);
        Ok(Self {
            public_key: r.array()?,
            incoming_amount: r.i64()?,
            outgoing_amount: r.i64()?,
            number_of_incoming_transfers: r.u32()?,
            number_of_outgoing_transfers: r.u32()?,
            latest_incoming_transfer_tick: r.u32()?,
            latest_outgoing_transfer_tick: r.u32()?,
        })
    }

    pub fn encode(&self) -> [u8; ENTITY_RECORD_SIZE] {
        let mut out = [0u8; ENTITY_RECORD_SIZE];
        out[0..32].copy_from_slice(&self.public_key);
        out[32..40].copy_from_slice(&self.incoming_amount.to_le_bytes());
        out[40..48].copy_from_slice(&self.outgoing_amount.to_le_bytes());
        out[48..52].copy_from_slice(&self.number_of_incoming_transfers.to_le_bytes());
        out[52..56].copy_from_slice(&self.number_of_outgoing_transfers.to_le_bytes());
        out[56..60].copy_from_slice(&self.latest_incoming_transfer_tick.to_le_bytes());
        out[60..64].copy_from_slice(&self.latest_outgoing_transfer_tick.to_le_bytes());
        out
    }
}

/// `RespondEntity` (`network_messages/entity.h`, 840 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespondEntity {
    pub entity: EntityRecord,
    pub tick: u32,
    pub spectrum_index: i32,
    pub siblings: Vec<[u8; 32]>,
}

impl RespondEntity {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != RESPOND_ENTITY_SIZE {
            return Err(Error::InvalidPacket(format!(
                "respond entity needs {RESPOND_ENTITY_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let entity = EntityRecord::decode(&payload[0..ENTITY_RECORD_SIZE])?;
        let tick = u32::from_le_bytes(payload[64..68].try_into().expect("4 bytes"));
        let spectrum_index = i32::from_le_bytes(payload[68..72].try_into().expect("4 bytes"));
        let mut r = Reader::new(&payload[72..]);
        let mut siblings = Vec::with_capacity(SPECTRUM_DEPTH);
        for _ in 0..SPECTRUM_DEPTH {
            siblings.push(r.array()?);
        }
        Ok(Self {
            entity,
            tick,
            spectrum_index,
            siblings,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(RESPOND_ENTITY_SIZE);
        out.extend_from_slice(&self.entity.encode());
        out.extend_from_slice(&self.tick.to_le_bytes());
        out.extend_from_slice(&self.spectrum_index.to_le_bytes());
        for sibling in &self.siblings {
            out.extend_from_slice(sibling);
        }
        out
    }
}

/// `RespondSystemInfo` (`network_messages/system_info.h`, packed, 128 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SystemInfo {
    pub version: i16,
    pub epoch: u16,
    pub tick: u32,
    pub initial_tick: u32,
    pub latest_created_tick: u32,
    pub initial_millisecond: u16,
    pub initial_second: u8,
    pub initial_minute: u8,
    pub initial_hour: u8,
    pub initial_day: u8,
    pub initial_month: u8,
    pub initial_year: u8,
    pub number_of_entities: u32,
    pub number_of_transactions: u32,
    pub random_mining_seed: [u8; 32],
    pub solution_threshold: i32,
    pub total_spectrum_amount: u64,
    pub current_entity_balance_dust_threshold: u64,
    pub target_tick_vote_signature: u32,
    pub computor_packet_signature: u64,
    pub solution_additional_threshold: u64,
    pub reserve2: u64,
    pub reserve3: u64,
    pub reserve4: u64,
}

impl SystemInfo {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != RESPOND_SYSTEM_INFO_SIZE {
            return Err(Error::InvalidPacket(format!(
                "system info needs {RESPOND_SYSTEM_INFO_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        Ok(Self {
            version: r.u16()? as i16,
            epoch: r.u16()?,
            tick: r.u32()?,
            initial_tick: r.u32()?,
            latest_created_tick: r.u32()?,
            initial_millisecond: r.u16()?,
            initial_second: r.u8()?,
            initial_minute: r.u8()?,
            initial_hour: r.u8()?,
            initial_day: r.u8()?,
            initial_month: r.u8()?,
            initial_year: r.u8()?,
            number_of_entities: r.u32()?,
            number_of_transactions: r.u32()?,
            random_mining_seed: r.array()?,
            solution_threshold: r.u32()? as i32,
            total_spectrum_amount: r.u64()?,
            current_entity_balance_dust_threshold: r.u64()?,
            target_tick_vote_signature: r.u32()?,
            computor_packet_signature: r.u64()?,
            solution_additional_threshold: r.u64()?,
            reserve2: r.u64()?,
            reserve3: r.u64()?,
            reserve4: r.u64()?,
        })
    }

    pub fn encode(&self) -> [u8; RESPOND_SYSTEM_INFO_SIZE] {
        let mut out = [0u8; RESPOND_SYSTEM_INFO_SIZE];
        let mut o = 0;
        let mut put = |bytes: &[u8]| {
            out[o..o + bytes.len()].copy_from_slice(bytes);
            o += bytes.len();
        };
        put(&(self.version as u16).to_le_bytes());
        put(&self.epoch.to_le_bytes());
        put(&self.tick.to_le_bytes());
        put(&self.initial_tick.to_le_bytes());
        put(&self.latest_created_tick.to_le_bytes());
        put(&self.initial_millisecond.to_le_bytes());
        put(&[
            self.initial_second,
            self.initial_minute,
            self.initial_hour,
            self.initial_day,
            self.initial_month,
            self.initial_year,
        ]);
        put(&self.number_of_entities.to_le_bytes());
        put(&self.number_of_transactions.to_le_bytes());
        put(&self.random_mining_seed);
        put(&(self.solution_threshold as u32).to_le_bytes());
        put(&self.total_spectrum_amount.to_le_bytes());
        put(&self.current_entity_balance_dust_threshold.to_le_bytes());
        put(&self.target_tick_vote_signature.to_le_bytes());
        put(&self.computor_packet_signature.to_le_bytes());
        put(&self.solution_additional_threshold.to_le_bytes());
        put(&self.reserve2.to_le_bytes());
        put(&self.reserve3.to_le_bytes());
        put(&self.reserve4.to_le_bytes());
        debug_assert_eq!(o, RESPOND_SYSTEM_INFO_SIZE);
        out
    }
}

/// `RequestContractFunction` header plus its variable-size input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContractFunction {
    pub contract_index: u32,
    pub input_type: u16,
    pub input: Vec<u8>,
}

impl RequestContractFunction {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() < REQUEST_CONTRACT_FUNCTION_SIZE {
            return Err(Error::InvalidPacket(format!(
                "request contract function needs at least {REQUEST_CONTRACT_FUNCTION_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let contract_index = u32::from_le_bytes(payload[0..4].try_into().expect("4 bytes"));
        let input_type = u16::from_le_bytes(payload[4..6].try_into().expect("2 bytes"));
        let input_size = u16::from_le_bytes(payload[6..8].try_into().expect("2 bytes")) as usize;
        if input_size > MAX_INPUT_SIZE
            || payload.len() != REQUEST_CONTRACT_FUNCTION_SIZE + input_size
        {
            return Err(Error::InvalidPacket(format!(
                "request contract function input size {input_size} does not match payload length {}",
                payload.len()
            )));
        }
        Ok(Self {
            contract_index,
            input_type,
            input: payload[REQUEST_CONTRACT_FUNCTION_SIZE..].to_vec(),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(REQUEST_CONTRACT_FUNCTION_SIZE + self.input.len());
        out.extend_from_slice(&self.contract_index.to_le_bytes());
        out.extend_from_slice(&self.input_type.to_le_bytes());
        out.extend_from_slice(&(self.input.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.input);
        out
    }
}

/// `RespondContractFunction`: variable-size raw contract output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespondContractFunction {
    pub data: Vec<u8>,
}

impl RespondContractFunction {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        Ok(Self {
            data: payload.to_vec(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestTransactionInfo {
    pub digest: [u8; 32],
}

impl RequestTransactionInfo {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != 32 {
            return Err(Error::InvalidPacket(format!(
                "request transaction info needs 32 bytes, got {}",
                payload.len()
            )));
        }
        Ok(Self {
            digest: payload.try_into().expect("32 bytes"),
        })
    }

    pub fn encode(&self) -> [u8; 32] {
        self.digest
    }
}

/// `RequestTxStatus` from the qli tx-status add-on (`addons/tx_status_request.h`, 4 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestTxStatus {
    pub tick: u32,
}

impl RequestTxStatus {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != REQUEST_TX_STATUS_SIZE {
            return Err(Error::InvalidPacket(format!(
                "request tx status needs {REQUEST_TX_STATUS_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        Ok(Self {
            tick: u32::from_le_bytes(payload.try_into().expect("4 bytes")),
        })
    }

    pub fn encode(&self) -> [u8; REQUEST_TX_STATUS_SIZE] {
        self.tick.to_le_bytes()
    }
}

/// `RespondTxStatus` from the qli tx-status add-on: confirmed digests for one
/// tick plus a `moneyFlew` bit per transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespondTxStatus {
    pub current_tick: u32,
    pub tick: u32,
    pub digests: Vec<[u8; 32]>,
    /// One flag per transaction, in the same order as [`Self::digests`].
    pub money_flew: Vec<bool>,
}

impl RespondTxStatus {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() < RESPOND_TX_STATUS_HEADER_SIZE {
            return Err(Error::InvalidPacket(format!(
                "respond tx status needs at least {RESPOND_TX_STATUS_HEADER_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let current_tick = u32::from_le_bytes(payload[0..4].try_into().expect("4 bytes"));
        let tick = u32::from_le_bytes(payload[4..8].try_into().expect("4 bytes"));
        let tx_count = u32::from_le_bytes(payload[8..12].try_into().expect("4 bytes")) as usize;
        if tx_count > TX_PER_TICK {
            return Err(Error::InvalidPacket(format!(
                "respond tx status count {tx_count} exceeds {TX_PER_TICK}"
            )));
        }
        let bitmap_len = TX_PER_TICK.div_ceil(8);
        let expected = RESPOND_TX_STATUS_HEADER_SIZE + tx_count * 32;
        if payload.len() != expected {
            return Err(Error::InvalidPacket(format!(
                "respond tx status needs {expected} bytes for {tx_count} transactions, got {}",
                payload.len()
            )));
        }
        let bitmap = &payload[12..12 + bitmap_len];
        let money_flew = (0..tx_count)
            .map(|index| bitmap[index / 8] & (1 << (index % 8)) != 0)
            .collect();
        let digest_bytes = &payload[12 + bitmap_len..];
        let digests = digest_bytes
            .chunks_exact(32)
            .map(|chunk| chunk.try_into().expect("32 bytes"))
            .collect();
        Ok(Self {
            current_tick,
            tick,
            digests,
            money_flew,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let digests = &self.digests[..self.digests.len().min(TX_PER_TICK)];
        let tx_count = digests.len();
        let mut out = Vec::with_capacity(RESPOND_TX_STATUS_HEADER_SIZE + tx_count * 32);
        out.extend_from_slice(&self.current_tick.to_le_bytes());
        out.extend_from_slice(&self.tick.to_le_bytes());
        out.extend_from_slice(&(tx_count as u32).to_le_bytes());
        let mut bitmap = vec![0u8; TX_PER_TICK.div_ceil(8)];
        for (index, flew) in self.money_flew.iter().take(tx_count).enumerate() {
            if *flew {
                bitmap[index / 8] |= 1 << (index % 8);
            }
        }
        out.extend_from_slice(&bitmap);
        for digest in digests {
            out.extend_from_slice(digest);
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestLog {
    pub passcode: [u64; 4],
    pub from_id: u64,
    pub to_id: u64,
}

impl RequestLog {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != 48 {
            return Err(Error::InvalidPacket(format!(
                "request log needs 48 bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        Ok(Self {
            passcode: [r.u64()?, r.u64()?, r.u64()?, r.u64()?],
            from_id: r.u64()?,
            to_id: r.u64()?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(48);
        for part in self.passcode {
            out.extend_from_slice(&part.to_le_bytes());
        }
        out.extend_from_slice(&self.from_id.to_le_bytes());
        out.extend_from_slice(&self.to_id.to_le_bytes());
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestAllLogRanges {
    pub passcode: [u64; 4],
    pub tick: u32,
}

impl RequestAllLogRanges {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != 36 {
            return Err(Error::InvalidPacket(format!(
                "request all log ranges needs 36 bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        Ok(Self {
            passcode: [r.u64()?, r.u64()?, r.u64()?, r.u64()?],
            tick: r.u32()?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(36);
        for part in self.passcode {
            out.extend_from_slice(&part.to_le_bytes());
        }
        out.extend_from_slice(&self.tick.to_le_bytes());
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogIdRange {
    pub from_log_id: i64,
    pub length: i64,
}

impl LogIdRange {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != 16 {
            return Err(Error::InvalidPacket(format!(
                "log id range needs 16 bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        Ok(Self {
            from_log_id: r.i64()?,
            length: r.i64()?,
        })
    }

    pub fn encode(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[0..8].copy_from_slice(&self.from_log_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.length.to_le_bytes());
        out
    }
}

/// `RequestLogIdRangeFromTx` (`network_messages/logging.h`, 40 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLogIdRangeFromTx {
    pub passcode: [u64; 4],
    pub tick: u32,
    pub tx_id: u32,
}

impl RequestLogIdRangeFromTx {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != REQUEST_LOG_ID_RANGE_FROM_TX_SIZE {
            return Err(Error::InvalidPacket(format!(
                "request log id range from tx needs {REQUEST_LOG_ID_RANGE_FROM_TX_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        Ok(Self {
            passcode: [r.u64()?, r.u64()?, r.u64()?, r.u64()?],
            tick: r.u32()?,
            tx_id: r.u32()?,
        })
    }

    pub fn encode(&self) -> [u8; REQUEST_LOG_ID_RANGE_FROM_TX_SIZE] {
        let mut out = [0u8; REQUEST_LOG_ID_RANGE_FROM_TX_SIZE];
        for (index, part) in self.passcode.iter().enumerate() {
            out[index * 8..index * 8 + 8].copy_from_slice(&part.to_le_bytes());
        }
        out[32..36].copy_from_slice(&self.tick.to_le_bytes());
        out[36..40].copy_from_slice(&self.tx_id.to_le_bytes());
        out
    }
}

/// `RespondLogIdRangeFromTx` (`network_messages/logging.h`, 16 bytes).
pub type RespondLogIdRangeFromTx = LogIdRange;

/// `RequestPruningLog` (`network_messages/logging.h`, 48 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestPruningLog {
    pub passcode: [u64; 4],
    pub from_log_id: u64,
    pub to_log_id: u64,
}

impl RequestPruningLog {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != REQUEST_PRUNING_LOG_SIZE {
            return Err(Error::InvalidPacket(format!(
                "request pruning log needs {REQUEST_PRUNING_LOG_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        Ok(Self {
            passcode: [r.u64()?, r.u64()?, r.u64()?, r.u64()?],
            from_log_id: r.u64()?,
            to_log_id: r.u64()?,
        })
    }

    pub fn encode(&self) -> [u8; REQUEST_PRUNING_LOG_SIZE] {
        let mut out = [0u8; REQUEST_PRUNING_LOG_SIZE];
        for (index, part) in self.passcode.iter().enumerate() {
            out[index * 8..index * 8 + 8].copy_from_slice(&part.to_le_bytes());
        }
        out[32..40].copy_from_slice(&self.from_log_id.to_le_bytes());
        out[40..48].copy_from_slice(&self.to_log_id.to_le_bytes());
        out
    }
}

/// `RespondPruningLog` (`network_messages/logging.h`, 8 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RespondPruningLog {
    pub success: i64,
}

impl RespondPruningLog {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != RESPOND_PRUNING_LOG_SIZE {
            return Err(Error::InvalidPacket(format!(
                "respond pruning log needs {RESPOND_PRUNING_LOG_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        Ok(Self {
            success: i64::from_le_bytes(payload.try_into().expect("8 bytes")),
        })
    }

    pub fn encode(&self) -> [u8; RESPOND_PRUNING_LOG_SIZE] {
        self.success.to_le_bytes()
    }
}

/// `RequestLogStateDigest` (`network_messages/logging.h`, 36 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLogStateDigest {
    pub passcode: [u64; 4],
    pub requested_tick: u32,
}

impl RequestLogStateDigest {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != REQUEST_LOG_STATE_DIGEST_SIZE {
            return Err(Error::InvalidPacket(format!(
                "request log state digest needs {REQUEST_LOG_STATE_DIGEST_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let mut r = Reader::new(payload);
        Ok(Self {
            passcode: [r.u64()?, r.u64()?, r.u64()?, r.u64()?],
            requested_tick: r.u32()?,
        })
    }

    pub fn encode(&self) -> [u8; REQUEST_LOG_STATE_DIGEST_SIZE] {
        let mut out = [0u8; REQUEST_LOG_STATE_DIGEST_SIZE];
        for (index, part) in self.passcode.iter().enumerate() {
            out[index * 8..index * 8 + 8].copy_from_slice(&part.to_le_bytes());
        }
        out[32..36].copy_from_slice(&self.requested_tick.to_le_bytes());
        out
    }
}

/// `RespondLogStateDigest` (`network_messages/logging.h`, 32 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RespondLogStateDigest {
    pub digest: [u8; 32],
}

impl RespondLogStateDigest {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != RESPOND_LOG_STATE_DIGEST_SIZE {
            return Err(Error::InvalidPacket(format!(
                "respond log state digest needs {RESPOND_LOG_STATE_DIGEST_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        Ok(Self {
            digest: payload.try_into().expect("32 bytes"),
        })
    }

    pub fn encode(&self) -> [u8; RESPOND_LOG_STATE_DIGEST_SIZE] {
        self.digest
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestContractIpo {
    pub contract_index: u32,
}

impl RequestContractIpo {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != 4 {
            return Err(Error::InvalidPacket(format!(
                "request contract ipo needs 4 bytes, got {}",
                payload.len()
            )));
        }
        Ok(Self {
            contract_index: u32::from_le_bytes(payload[0..4].try_into().expect("4 bytes")),
        })
    }

    pub fn encode(&self) -> [u8; 4] {
        self.contract_index.to_le_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespondContractIpo {
    pub contract_index: u32,
    pub tick: u32,
    pub public_keys: Vec<[u8; 32]>,
    pub prices: Vec<i64>,
}

impl RespondContractIpo {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != RESPOND_CONTRACT_IPO_SIZE {
            return Err(Error::InvalidPacket(format!(
                "respond contract ipo needs {RESPOND_CONTRACT_IPO_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        let contract_index = u32::from_le_bytes(payload[0..4].try_into().expect("4 bytes"));
        let tick = u32::from_le_bytes(payload[4..8].try_into().expect("4 bytes"));
        let mut r = Reader::new(&payload[8..]);
        let mut public_keys = Vec::with_capacity(COMPUTOR_COUNT);
        for _ in 0..COMPUTOR_COUNT {
            public_keys.push(r.array()?);
        }
        let mut prices = Vec::with_capacity(COMPUTOR_COUNT);
        for _ in 0..COMPUTOR_COUNT {
            prices.push(r.i64()?);
        }
        Ok(Self {
            contract_index,
            tick,
            public_keys,
            prices,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(RESPOND_CONTRACT_IPO_SIZE);
        out.extend_from_slice(&self.contract_index.to_le_bytes());
        out.extend_from_slice(&self.tick.to_le_bytes());
        for key in &self.public_keys {
            out.extend_from_slice(key);
        }
        for price in &self.prices {
            out.extend_from_slice(&price.to_le_bytes());
        }
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RespondActiveIpo {
    pub contract_index: u32,
    pub asset_name: [u8; 8],
}

impl RespondActiveIpo {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != 12 {
            return Err(Error::InvalidPacket(format!(
                "respond active ipo needs 12 bytes, got {}",
                payload.len()
            )));
        }
        Ok(Self {
            contract_index: u32::from_le_bytes(payload[0..4].try_into().expect("4 bytes")),
            asset_name: payload[4..12].try_into().expect("8 bytes"),
        })
    }

    pub fn asset_name(&self) -> String {
        asset_name_string(&self.asset_name)
    }

    pub fn encode(&self) -> [u8; 12] {
        let mut out = [0u8; 12];
        out[0..4].copy_from_slice(&self.contract_index.to_le_bytes());
        out[4..12].copy_from_slice(&self.asset_name);
        out
    }
}

/// `RequestEntity` (`network_messages/entity.h`, 32 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestEntity {
    pub public_key: [u8; 32],
}

impl RequestEntity {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != REQUEST_ENTITY_SIZE {
            return Err(Error::InvalidPacket(format!(
                "request entity needs {REQUEST_ENTITY_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        Ok(Self {
            public_key: payload.try_into().expect("32 bytes"),
        })
    }

    pub fn encode(&self) -> [u8; REQUEST_ENTITY_SIZE] {
        self.public_key
    }
}

/// `RequestIssuedAssets`: a 32-byte issuer public key.
pub type RequestIssuedAssets = RequestEntity;
/// `RequestOwnedAssets`: a 32-byte owner public key.
pub type RequestOwnedAssets = RequestEntity;
/// `RequestPossessedAssets`: a 32-byte possessor public key.
pub type RequestPossessedAssets = RequestEntity;

/// `RespondIssuedAssets` (`network_messages/assets.h`, 824 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespondIssuedAssets {
    pub asset: AssetRecord,
    pub tick: u32,
    pub universe_index: u32,
    pub siblings: Vec<[u8; 32]>,
}

impl RespondIssuedAssets {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != RESPOND_ISSUED_ASSETS_SIZE {
            return Err(Error::InvalidPacket(format!(
                "respond issued assets needs {RESPOND_ISSUED_ASSETS_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        Ok(Self {
            asset: AssetRecord::decode(&payload[0..ASSET_RECORD_SIZE])?,
            tick: u32::from_le_bytes(payload[48..52].try_into().expect("4 bytes")),
            universe_index: u32::from_le_bytes(payload[52..56].try_into().expect("4 bytes")),
            siblings: decode_siblings(&payload[56..])?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(RESPOND_ISSUED_ASSETS_SIZE);
        out.extend_from_slice(&self.asset.encode());
        out.extend_from_slice(&self.tick.to_le_bytes());
        out.extend_from_slice(&self.universe_index.to_le_bytes());
        encode_siblings(&mut out, &self.siblings);
        out
    }
}

/// `RespondOwnedAssets` (`network_messages/assets.h`, 872 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespondOwnedAssets {
    pub asset: AssetRecord,
    pub issuance_asset: AssetRecord,
    pub tick: u32,
    pub universe_index: u32,
    pub siblings: Vec<[u8; 32]>,
}

impl RespondOwnedAssets {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != RESPOND_OWNED_ASSETS_SIZE {
            return Err(Error::InvalidPacket(format!(
                "respond owned assets needs {RESPOND_OWNED_ASSETS_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        Ok(Self {
            asset: AssetRecord::decode(&payload[0..ASSET_RECORD_SIZE])?,
            issuance_asset: AssetRecord::decode(
                &payload[ASSET_RECORD_SIZE..2 * ASSET_RECORD_SIZE],
            )?,
            tick: u32::from_le_bytes(payload[96..100].try_into().expect("4 bytes")),
            universe_index: u32::from_le_bytes(payload[100..104].try_into().expect("4 bytes")),
            siblings: decode_siblings(&payload[104..])?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(RESPOND_OWNED_ASSETS_SIZE);
        out.extend_from_slice(&self.asset.encode());
        out.extend_from_slice(&self.issuance_asset.encode());
        out.extend_from_slice(&self.tick.to_le_bytes());
        out.extend_from_slice(&self.universe_index.to_le_bytes());
        encode_siblings(&mut out, &self.siblings);
        out
    }
}

/// `RespondPossessedAssets` (`network_messages/assets.h`, 920 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespondPossessedAssets {
    pub asset: AssetRecord,
    pub ownership_asset: AssetRecord,
    pub issuance_asset: AssetRecord,
    pub tick: u32,
    pub universe_index: u32,
    pub siblings: Vec<[u8; 32]>,
}

impl RespondPossessedAssets {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() != RESPOND_POSSESSED_ASSETS_SIZE {
            return Err(Error::InvalidPacket(format!(
                "respond possessed assets needs {RESPOND_POSSESSED_ASSETS_SIZE} bytes, got {}",
                payload.len()
            )));
        }
        Ok(Self {
            asset: AssetRecord::decode(&payload[0..ASSET_RECORD_SIZE])?,
            ownership_asset: AssetRecord::decode(
                &payload[ASSET_RECORD_SIZE..2 * ASSET_RECORD_SIZE],
            )?,
            issuance_asset: AssetRecord::decode(
                &payload[2 * ASSET_RECORD_SIZE..3 * ASSET_RECORD_SIZE],
            )?,
            tick: u32::from_le_bytes(payload[144..148].try_into().expect("4 bytes")),
            universe_index: u32::from_le_bytes(payload[148..152].try_into().expect("4 bytes")),
            siblings: decode_siblings(&payload[152..])?,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(RESPOND_POSSESSED_ASSETS_SIZE);
        out.extend_from_slice(&self.asset.encode());
        out.extend_from_slice(&self.ownership_asset.encode());
        out.extend_from_slice(&self.issuance_asset.encode());
        out.extend_from_slice(&self.tick.to_le_bytes());
        out.extend_from_slice(&self.universe_index.to_le_bytes());
        encode_siblings(&mut out, &self.siblings);
        out
    }
}

fn decode_siblings(payload: &[u8]) -> Result<Vec<[u8; 32]>> {
    if payload.len() != 32 * ASSETS_DEPTH {
        return Err(Error::InvalidPacket(format!(
            "sibling list needs {} bytes, got {}",
            32 * ASSETS_DEPTH,
            payload.len()
        )));
    }
    let mut r = Reader::new(payload);
    let mut siblings = Vec::with_capacity(ASSETS_DEPTH);
    for _ in 0..ASSETS_DEPTH {
        siblings.push(r.array()?);
    }
    Ok(siblings)
}

fn encode_siblings(out: &mut Vec<u8>, siblings: &[[u8; 32]]) {
    for sibling in siblings.iter().take(ASSETS_DEPTH) {
        out.extend_from_slice(sibling);
    }
    for _ in siblings.len()..ASSETS_DEPTH {
        out.extend_from_slice(&ZERO_DIGEST);
    }
}

pub fn exchange_public_peers(peers: &[PeerAddress; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    for peer in peers {
        out.extend_from_slice(&peer.ip);
    }
    out
}

pub fn request_current_tick_info() -> Vec<u8> {
    Vec::new()
}

pub fn request_computors() -> Vec<u8> {
    Vec::new()
}

pub fn request_tick_data(tick: u32) -> Vec<u8> {
    tick.to_le_bytes().to_vec()
}

/// Builds a `RequestQuorumTick` payload.
///
/// `known[i]` marks computor votes that the caller already has. The peer only
/// sends ticks for computors whose flag is **not** set. The 85 flag bytes are
/// followed by 3 bytes of C struct padding, giving the 92-byte payload size.
pub fn request_quorum_tick(tick: u32, known: &[bool; COMPUTOR_COUNT]) -> Vec<u8> {
    let mut out = Vec::with_capacity(92);
    out.extend_from_slice(&tick.to_le_bytes());
    let mut flags = [0u8; 85];
    for (index, known) in known.iter().enumerate() {
        if *known {
            flags[index / 8] |= 1 << (index % 8);
        }
    }
    out.extend_from_slice(&flags);
    out.extend_from_slice(&[0, 0, 0]); // C struct padding to 92 bytes
    out
}

/// Builds a `RequestTickTransactions` payload.
///
/// `known[i]` marks transactions the caller already has. The peer only sends
/// transactions whose flag is **not** set (flags are "not interested" bits).
pub fn request_tick_transactions(tick: u32, known: &[bool; TX_PER_TICK]) -> Vec<u8> {
    let mut out = Vec::with_capacity(516);
    out.extend_from_slice(&tick.to_le_bytes());
    let mut flags = [0u8; TX_PER_TICK / 8];
    for (index, known) in known.iter().enumerate() {
        if *known {
            flags[index / 8] |= 1 << (index % 8);
        }
    }
    out.extend_from_slice(&flags);
    out
}

pub fn request_all_log_ranges(tick: u32, passcode: [u64; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(36);
    for part in passcode {
        out.extend_from_slice(&part.to_le_bytes());
    }
    out.extend_from_slice(&tick.to_le_bytes());
    out
}

pub fn request_logs(from_log_id: u64, to_log_id: u64, passcode: [u64; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(48);
    for part in passcode {
        out.extend_from_slice(&part.to_le_bytes());
    }
    out.extend_from_slice(&from_log_id.to_le_bytes());
    out.extend_from_slice(&to_log_id.to_le_bytes());
    out
}

pub fn request_system_info() -> Vec<u8> {
    Vec::new()
}

pub fn request_entity(public_key: [u8; 32]) -> Vec<u8> {
    public_key.to_vec()
}

pub fn request_issued_assets(public_key: [u8; 32]) -> Vec<u8> {
    public_key.to_vec()
}

pub fn request_owned_assets(public_key: [u8; 32]) -> Vec<u8> {
    public_key.to_vec()
}

pub fn request_possessed_assets(public_key: [u8; 32]) -> Vec<u8> {
    public_key.to_vec()
}

pub fn request_assets(request: &RequestAssets) -> Vec<u8> {
    request.encode().to_vec()
}

pub fn request_contract_ipo(contract_index: u32) -> Vec<u8> {
    contract_index.to_le_bytes().to_vec()
}

pub fn request_active_ipos() -> Vec<u8> {
    Vec::new()
}

pub fn request_contract_function(contract_index: u32, input_type: u16, input: &[u8]) -> Vec<u8> {
    RequestContractFunction {
        contract_index,
        input_type,
        input: input.to_vec(),
    }
    .encode()
}

pub fn request_transaction_info(digest: [u8; 32]) -> Vec<u8> {
    digest.to_vec()
}

pub fn request_tx_status(tick: u32) -> Vec<u8> {
    RequestTxStatus { tick }.encode().to_vec()
}

pub fn request_log_id_range_from_tx(tick: u32, tx_id: u32, passcode: [u64; 4]) -> Vec<u8> {
    RequestLogIdRangeFromTx {
        passcode,
        tick,
        tx_id,
    }
    .encode()
    .to_vec()
}

pub fn request_log_state_digest(tick: u32, passcode: [u64; 4]) -> Vec<u8> {
    RequestLogStateDigest {
        passcode,
        requested_tick: tick,
    }
    .encode()
    .to_vec()
}

pub fn request_pruning_log(from_log_id: u64, to_log_id: u64, passcode: [u64; 4]) -> Vec<u8> {
    RequestPruningLog {
        passcode,
        from_log_id,
        to_log_id,
    }
    .encode()
    .to_vec()
}

pub fn request_type(kind: MessageType) -> u8 {
    kind as u8
}

/// Converts a civil date to days since 1970-01-01.
fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month_p = month as i32;
    let doy = (153 * (month_p + if month_p > 2 { -3 } else { 9 }) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era as i64) * 146_097 + doe as i64 - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_round_trip_and_digest() {
        let tick = Tick {
            computor_index: 7,
            epoch: 240,
            tick: 91_915_000,
            millisecond: 123,
            second: 45,
            minute: 12,
            hour: 9,
            day: 18,
            month: 11,
            year: 26,
            prev_resource_testing_digest: 1,
            salted_resource_testing_digest: 2,
            prev_transaction_body_digest: 3,
            salted_transaction_body_digest: 4,
            prev_spectrum_digest: [5; 32],
            prev_universe_digest: [6; 32],
            prev_computer_digest: [7; 32],
            salted_spectrum_digest: [8; 32],
            salted_universe_digest: [9; 32],
            salted_computer_digest: [10; 32],
            transaction_digest: [11; 32],
            expected_next_tick_transaction_digest: [12; 32],
            signature: [13; 64],
        };
        let bytes = tick.encode();
        assert_eq!(bytes.len(), TICK_SIZE);
        assert_eq!(Tick::decode(&bytes).unwrap(), tick);
        assert_eq!(tick.digest(), k12_32(&bytes));
        assert_eq!(tick.timestamp_millis(), 1_794_993_165_123);
    }

    #[test]
    fn request_quorum_tick_layout() {
        let mut known = [false; COMPUTOR_COUNT];
        known[0] = true;
        known[675] = true;
        let payload = request_quorum_tick(42, &known);
        assert_eq!(payload.len(), 92);
        assert_eq!(&payload[..4], &42u32.to_le_bytes());
        assert_eq!(payload[4] & 1, 1);
        assert_eq!(payload[4 + 84] & (1 << 3), 1 << 3);
    }

    #[test]
    fn transaction_round_trip() {
        let tx = Transaction {
            source: [1; 32],
            destination: [2; 32],
            amount: 123,
            tick: 9,
            input_type: 1,
            input: vec![3, 4, 5],
            signature: [6; 64],
        };
        let bytes = tx.encode();
        assert_eq!(bytes.len(), tx.total_size());
        assert_eq!(Transaction::decode(&bytes).unwrap(), tx);
    }

    #[test]
    fn tx_status_round_trip() {
        let status = RespondTxStatus {
            current_tick: 10,
            tick: 9,
            digests: vec![[1; 32], [2; 32], [3; 32]],
            money_flew: vec![true, false, true],
        };
        let bytes = status.encode();
        assert_eq!(bytes.len(), RESPOND_TX_STATUS_HEADER_SIZE + 3 * 32);
        assert_eq!(RespondTxStatus::decode(&bytes).unwrap(), status);
        assert_eq!(request_tx_status(9), 9u32.to_le_bytes());
        assert_eq!(
            RequestTxStatus::decode(&request_tx_status(9)).unwrap().tick,
            9
        );
    }

    #[test]
    fn tx_status_rejects_inconsistent_lengths() {
        let status = RespondTxStatus {
            current_tick: 10,
            tick: 9,
            digests: vec![[1; 32]],
            money_flew: vec![true],
        };
        let mut bytes = status.encode();
        bytes.pop();
        assert!(RespondTxStatus::decode(&bytes).is_err());

        let mut too_many = status.encode();
        too_many[8..12].copy_from_slice(&(TX_PER_TICK as u32 + 1).to_le_bytes());
        assert!(RespondTxStatus::decode(&too_many).is_err());
    }
}
