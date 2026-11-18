use crate::error::{Error, Result};

pub const HEADER_SIZE: usize = 8;
pub const MAX_PAYLOAD_SIZE: usize = 0x00FF_FFFF - HEADER_SIZE;

/// Qubic network message types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MessageType {
    ExchangePublicPeers = 0,
    BroadcastMessage = 1,
    BroadcastComputors = 2,
    BroadcastTick = 3,
    BroadcastFutureTickData = 8,
    RequestComputors = 11,
    RequestQuorumTick = 14,
    RequestTickData = 16,
    BroadcastTransaction = 24,
    RequestTransactionInfo = 26,
    RequestCurrentTickInfo = 27,
    RespondCurrentTickInfo = 28,
    RequestTickTransactions = 29,
    RequestEntity = 31,
    RespondEntity = 32,
    RequestContractIpo = 33,
    RespondContractIpo = 34,
    EndResponse = 35,
    RequestIssuedAssets = 36,
    RespondIssuedAssets = 37,
    RequestOwnedAssets = 38,
    RespondOwnedAssets = 39,
    RequestPossessedAssets = 40,
    RespondPossessedAssets = 41,
    RequestContractFunction = 42,
    RespondContractFunction = 43,
    RequestLog = 44,
    RespondLog = 45,
    RequestSystemInfo = 46,
    RespondSystemInfo = 47,
    RequestLogIdRangeFromTx = 48,
    RespondLogIdRangeFromTx = 49,
    RequestAllLogIdRangesFromTx = 50,
    RespondAllLogIdRangesFromTx = 51,
    RequestAssets = 52,
    RespondAssets = 53,
    TryAgain = 54,
    RequestPruningLog = 56,
    RespondPruningLog = 57,
    RequestLogStateDigest = 58,
    RespondLogStateDigest = 59,
    RequestActiveIpos = 64,
    RespondActiveIpo = 65,
    RequestOracleData = 66,
    RespondOracleData = 67,
    RequestRevenueData = 70,
    RespondRevenueData = 71,
    RequestPriceSeries = 78,
    RespondPriceSeries = 79,
    SpecialCommand = 255,
}

impl MessageType {
    pub fn from_u8(value: u8) -> Result<Self> {
        Ok(match value {
            0 => Self::ExchangePublicPeers,
            1 => Self::BroadcastMessage,
            2 => Self::BroadcastComputors,
            3 => Self::BroadcastTick,
            8 => Self::BroadcastFutureTickData,
            11 => Self::RequestComputors,
            14 => Self::RequestQuorumTick,
            16 => Self::RequestTickData,
            24 => Self::BroadcastTransaction,
            26 => Self::RequestTransactionInfo,
            27 => Self::RequestCurrentTickInfo,
            28 => Self::RespondCurrentTickInfo,
            29 => Self::RequestTickTransactions,
            31 => Self::RequestEntity,
            32 => Self::RespondEntity,
            33 => Self::RequestContractIpo,
            34 => Self::RespondContractIpo,
            35 => Self::EndResponse,
            36 => Self::RequestIssuedAssets,
            37 => Self::RespondIssuedAssets,
            38 => Self::RequestOwnedAssets,
            39 => Self::RespondOwnedAssets,
            40 => Self::RequestPossessedAssets,
            41 => Self::RespondPossessedAssets,
            42 => Self::RequestContractFunction,
            43 => Self::RespondContractFunction,
            44 => Self::RequestLog,
            45 => Self::RespondLog,
            46 => Self::RequestSystemInfo,
            47 => Self::RespondSystemInfo,
            48 => Self::RequestLogIdRangeFromTx,
            49 => Self::RespondLogIdRangeFromTx,
            50 => Self::RequestAllLogIdRangesFromTx,
            51 => Self::RespondAllLogIdRangesFromTx,
            52 => Self::RequestAssets,
            53 => Self::RespondAssets,
            54 => Self::TryAgain,
            56 => Self::RequestPruningLog,
            57 => Self::RespondPruningLog,
            58 => Self::RequestLogStateDigest,
            59 => Self::RespondLogStateDigest,
            64 => Self::RequestActiveIpos,
            65 => Self::RespondActiveIpo,
            66 => Self::RequestOracleData,
            67 => Self::RespondOracleData,
            70 => Self::RequestRevenueData,
            71 => Self::RespondRevenueData,
            78 => Self::RequestPriceSeries,
            79 => Self::RespondPriceSeries,
            255 => Self::SpecialCommand,
            other => return Err(Error::InvalidMessageType(other)),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub size: u32,
    pub kind: MessageType,
    pub dejavu: u32,
}

impl Header {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(Error::InvalidPacket(format!(
                "header needs {HEADER_SIZE} bytes, got {}",
                bytes.len()
            )));
        }
        let size = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]);
        if (size as usize) < HEADER_SIZE {
            return Err(Error::InvalidPacket(format!("packet size {size} is too small")));
        }
        Ok(Self {
            size,
            kind: MessageType::from_u8(bytes[3])?,
            dejavu: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        })
    }

    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let size = self.size.to_le_bytes();
        let dejavu = self.dejavu.to_le_bytes();
        [size[0], size[1], size[2], self.kind as u8, dejavu[0], dejavu[1], dejavu[2], dejavu[3]]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub kind: MessageType,
    pub dejavu: u32,
    pub payload: Vec<u8>,
}

impl Packet {
    pub fn new(kind: MessageType, dejavu: u32, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind,
            dejavu,
            payload: payload.into(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let size = HEADER_SIZE + self.payload.len();
        let mut out = Vec::with_capacity(size);
        out.extend_from_slice(&Header {
            size: size as u32,
            kind: self.kind,
            dejavu: self.dejavu,
        }.encode());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = Header::decode(bytes)?;
        let size = header.size as usize;
        if size > bytes.len() {
            return Err(Error::InvalidPacket(format!(
                "packet declares {size} bytes, only {} available",
                bytes.len()
            )));
        }
        if size < HEADER_SIZE {
            return Err(Error::InvalidPacket("packet size below header size".into()));
        }
        Ok(Self {
            kind: header.kind,
            dejavu: header.dejavu,
            payload: bytes[HEADER_SIZE..size].to_vec(),
        })
    }
}

/// Little-endian cursor used by the wire decoders.
pub(crate) struct Reader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.offset)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        let value = *self
            .data
            .get(self.offset)
            .ok_or_else(|| Error::InvalidPacket("unexpected end of packet".into()))?;
        self.offset += 1;
        Ok(value)
    }

    pub(crate) fn u16(&mut self) -> Result<u16> {
        let bytes = self.bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        let bytes = self.bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        let bytes = self.bytes(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().expect("8 bytes")))
    }

    pub(crate) fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let bytes = self.bytes(N)?;
        Ok(bytes.try_into().expect("fixed array"))
    }

    pub(crate) fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| Error::InvalidPacket("offset overflow".into()))?;
        let bytes = self
            .data
            .get(self.offset..end)
            .ok_or_else(|| Error::InvalidPacket(format!("need {len} bytes, have {}", self.remaining())))?;
        self.offset = end;
        Ok(bytes)
    }

    pub(crate) fn skip(&mut self, len: usize) -> Result<()> {
        self.bytes(len)?;
        Ok(())
    }
}
