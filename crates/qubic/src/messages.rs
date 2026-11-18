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
pub const ZERO_DIGEST: [u8; 32] = [0u8; 32];

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
        put(&[self.second, self.minute, self.hour, self.day, self.month, self.year]);
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
        let seconds = days * 86_400
            + self.hour as i64 * 3_600
            + self.minute as i64 * 60
            + self.second as i64;
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
        out.extend_from_slice(&[self.second, self.minute, self.hour, self.day, self.month, self.year]);
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
        Ok(Self { from_log_id, length })
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
        self.from_log_id.iter().any(|v| *v == -3) || self.length.iter().any(|v| *v == -3)
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
            return Err(Error::InvalidPacket("peer exchange payload is not 4-byte aligned".into()));
        }
        Ok(payload
            .chunks_exact(4)
            .map(|chunk| Self {
                ip: [chunk[0], chunk[1], chunk[2], chunk[3]],
            })
            .filter(|p| p.ip != [0, 0, 0, 0])
            .collect())
    }

    pub fn to_string(&self) -> String {
        format!("{}.{}.{}.{}", self.ip[0], self.ip[1], self.ip[2], self.ip[3])
    }
}

impl std::fmt::Display for PeerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_string())
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
        assert_eq!(tick.timestamp_millis(), 1_795_027_925_123);
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
}
