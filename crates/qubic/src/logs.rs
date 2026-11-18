use crate::crypto::k12::k12_8;
use crate::error::{Error, Result};

pub const LOG_HEADER_SIZE: usize = 26;

pub const QU_TRANSFER: u8 = 0;
pub const ASSET_ISSUANCE: u8 = 1;
pub const ASSET_OWNERSHIP_CHANGE: u8 = 2;
pub const ASSET_POSSESSION_CHANGE: u8 = 3;
pub const CONTRACT_ERROR_MESSAGE: u8 = 4;
pub const CONTRACT_WARNING_MESSAGE: u8 = 5;
pub const CONTRACT_INFORMATION_MESSAGE: u8 = 6;
pub const CONTRACT_DEBUG_MESSAGE: u8 = 7;
pub const BURNING: u8 = 8;
pub const DUST_BURNING: u8 = 9;
pub const SPECTRUM_STATS: u8 = 10;
pub const ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE: u8 = 11;
pub const ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE: u8 = 12;
pub const CONTRACT_RESERVE_DEDUCTION: u8 = 13;
pub const ORACLE_QUERY_STATUS_CHANGE: u8 = 14;
pub const ORACLE_SUBSCRIBER_MESSAGE: u8 = 15;
pub const OC_INVOCATION_STATUS_CHANGE: u8 = 16;
pub const CUSTOM_MESSAGE: u8 = 255;

pub const CUSTOM_OP_START_DISTRIBUTE_DIVIDENDS: u64 = 6_217_575_821_008_262_227;
pub const CUSTOM_OP_END_DISTRIBUTE_DIVIDENDS: u64 = 6_217_575_821_008_457_285;
pub const CUSTOM_OP_START_EPOCH: u64 = 4_850_183_582_582_395_987;
pub const CUSTOM_OP_END_EPOCH: u64 = 4_850_183_582_582_591_045;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEvent {
    pub epoch: u16,
    pub tick: u32,
    pub log_type: u8,
    pub log_id: u64,
    pub log_digest: u64,
    pub body: Vec<u8>,
}

impl LogEvent {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() < LOG_HEADER_SIZE + 8 {
            return Err(Error::InvalidLog(format!(
                "log record needs at least {} bytes, got {}",
                LOG_HEADER_SIZE + 8,
                payload.len()
            )));
        }
        let epoch = u16::from_le_bytes([payload[0], payload[1]]);
        let tick = u32::from_le_bytes([payload[2], payload[3], payload[4], payload[5]]);
        let combo = u32::from_le_bytes([payload[6], payload[7], payload[8], payload[9]]);
        let size = (combo & 0x00FF_FFFF) as usize;
        let log_type = ((combo >> 24) & 0xFF) as u8;
        let log_id = u64::from_le_bytes(payload[10..18].try_into().expect("8 bytes"));
        let log_digest = u64::from_le_bytes(payload[18..26].try_into().expect("8 bytes"));
        if payload.len() < LOG_HEADER_SIZE + size {
            return Err(Error::InvalidLog(format!(
                "log record declares {size} body bytes, only {} available",
                payload.len() - LOG_HEADER_SIZE
            )));
        }
        Ok(Self {
            epoch,
            tick,
            log_type,
            log_id,
            log_digest,
            body: payload[LOG_HEADER_SIZE..LOG_HEADER_SIZE + size].to_vec(),
        })
    }

    pub fn encoded_len(&self) -> usize {
        LOG_HEADER_SIZE + self.body.len()
    }

    pub fn verify_digest(&self) -> bool {
        u64::from_le_bytes(k12_8(&self.body)) == self.log_digest
    }

    pub fn decode_many(payload: &[u8]) -> Result<Vec<Self>> {
        let mut out = Vec::new();
        let mut offset = 0;
        while offset < payload.len() {
            if offset + LOG_HEADER_SIZE > payload.len() {
                return Err(Error::InvalidLog("truncated log header".into()));
            }
            let combo = u32::from_le_bytes([
                payload[offset + 6],
                payload[offset + 7],
                payload[offset + 8],
                payload[offset + 9],
            ]);
            let size = (combo & 0x00FF_FFFF) as usize;
            let end = offset + LOG_HEADER_SIZE + size;
            if end > payload.len() {
                return Err(Error::InvalidLog("truncated log body".into()));
            }
            out.push(Self::decode(&payload[offset..end])?);
            offset = end;
        }
        Ok(out)
    }

    pub fn body(&self) -> EventBody {
        EventBody::decode(self.log_type, &self.body)
    }

    pub fn type_name(&self) -> &'static str {
        type_name(self.log_type)
    }

    pub fn custom_op_code(&self) -> Option<u64> {
        if self.log_type == CUSTOM_MESSAGE && self.body.len() >= 8 {
            Some(u64::from_le_bytes(self.body[..8].try_into().expect("8 bytes")))
        } else {
            None
        }
    }
}

pub fn type_name(log_type: u8) -> &'static str {
    match log_type {
        QU_TRANSFER => "qu_transfer",
        ASSET_ISSUANCE => "asset_issuance",
        ASSET_OWNERSHIP_CHANGE => "asset_ownership_change",
        ASSET_POSSESSION_CHANGE => "asset_possession_change",
        CONTRACT_ERROR_MESSAGE => "contract_error",
        CONTRACT_WARNING_MESSAGE => "contract_warning",
        CONTRACT_INFORMATION_MESSAGE => "contract_information",
        CONTRACT_DEBUG_MESSAGE => "contract_debug",
        BURNING => "burning",
        DUST_BURNING => "dust_burning",
        SPECTRUM_STATS => "spectrum_stats",
        ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE => "asset_ownership_managing_contract_change",
        ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE => "asset_possession_managing_contract_change",
        CONTRACT_RESERVE_DEDUCTION => "contract_reserve_deduction",
        ORACLE_QUERY_STATUS_CHANGE => "oracle_query_status_change",
        ORACLE_SUBSCRIBER_MESSAGE => "oracle_subscriber_message",
        OC_INVOCATION_STATUS_CHANGE => "oc_invocation_status_change",
        CUSTOM_MESSAGE => "custom_message",
        _ => "unknown",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetRef {
    pub issuer: [u8; 32],
    pub name: String,
    pub decimals: u8,
    pub unit: String,
    pub managing_contract: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventBody {
    QuTransfer {
        from: [u8; 32],
        to: [u8; 32],
        amount: i64,
    },
    AssetIssuance {
        asset: AssetRef,
        shares: i64,
    },
    AssetOwnershipChange {
        from: [u8; 32],
        to: [u8; 32],
        asset: AssetRef,
        shares: i64,
    },
    AssetPossessionChange {
        from: [u8; 32],
        to: [u8; 32],
        asset: AssetRef,
        shares: i64,
    },
    Burning {
        from: [u8; 32],
        amount: i64,
        contract_index: u32,
    },
    DustBurning {
        burns: Vec<([u8; 32], i64)>,
    },
    ContractMessage {
        contract_index: u32,
        message_type: u32,
        data: Vec<u8>,
    },
    Custom {
        op_code: u64,
        data: Vec<u8>,
    },
    Unknown {
        raw: Vec<u8>,
    },
}

impl EventBody {
    pub fn decode(log_type: u8, body: &[u8]) -> Self {
        match log_type {
            QU_TRANSFER if body.len() >= 72 => Self::QuTransfer {
                from: key(body, 0),
                to: key(body, 32),
                amount: i64::from_le_bytes(body[64..72].try_into().expect("8 bytes")),
            },
            ASSET_ISSUANCE if body.len() >= 63 => Self::AssetIssuance {
                asset: AssetRef {
                    issuer: key(body, 0),
                    name: asset_name(&body[48..55]),
                    decimals: body[55],
                    unit: unit_name(&body[56..63]),
                    managing_contract: i64::from_le_bytes(body[40..48].try_into().expect("8 bytes")),
                },
                shares: i64::from_le_bytes(body[32..40].try_into().expect("8 bytes")),
            },
            ASSET_OWNERSHIP_CHANGE if body.len() >= 119 => Self::AssetOwnershipChange {
                from: key(body, 0),
                to: key(body, 32),
                asset: AssetRef {
                    issuer: key(body, 64),
                    name: asset_name(&body[104..111]),
                    decimals: body[111],
                    unit: unit_name(&body[112..119]),
                    managing_contract: i64::from_le_bytes(body[96..104].try_into().expect("8 bytes")),
                },
                shares: i64::from_le_bytes(body[88..96].try_into().expect("8 bytes")),
            },
            ASSET_POSSESSION_CHANGE if body.len() >= 119 => Self::AssetPossessionChange {
                from: key(body, 0),
                to: key(body, 32),
                asset: AssetRef {
                    issuer: key(body, 64),
                    name: asset_name(&body[104..111]),
                    decimals: body[111],
                    unit: unit_name(&body[112..119]),
                    managing_contract: i64::from_le_bytes(body[96..104].try_into().expect("8 bytes")),
                },
                shares: i64::from_le_bytes(body[88..96].try_into().expect("8 bytes")),
            },
            BURNING if body.len() >= 44 => Self::Burning {
                from: key(body, 0),
                amount: i64::from_le_bytes(body[32..40].try_into().expect("8 bytes")),
                contract_index: u32::from_le_bytes(body[40..44].try_into().expect("4 bytes")),
            },
            DUST_BURNING if body.len() >= 2 => {
                let count = u16::from_le_bytes([body[0], body[1]]) as usize;
                let mut burns = Vec::with_capacity(count);
                let mut offset = 2;
                for _ in 0..count {
                    if offset + 40 > body.len() {
                        break;
                    }
                    burns.push((
                        key(body, offset),
                        i64::from_le_bytes(body[offset + 32..offset + 40].try_into().expect("8 bytes")),
                    ));
                    offset += 40;
                }
                Self::DustBurning { burns }
            }
            CONTRACT_ERROR_MESSAGE | CONTRACT_WARNING_MESSAGE | CONTRACT_INFORMATION_MESSAGE
            | CONTRACT_DEBUG_MESSAGE if body.len() >= 8 => Self::ContractMessage {
                contract_index: u32::from_le_bytes(body[0..4].try_into().expect("4 bytes")),
                message_type: u32::from_le_bytes(body[4..8].try_into().expect("4 bytes")),
                data: body[8..].to_vec(),
            },
            CUSTOM_MESSAGE if body.len() >= 8 => Self::Custom {
                op_code: u64::from_le_bytes(body[..8].try_into().expect("8 bytes")),
                data: body[8..].to_vec(),
            },
            _ => Self::Unknown { raw: body.to_vec() },
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::QuTransfer { .. } => "qu_transfer",
            Self::AssetIssuance { .. } => "asset_issuance",
            Self::AssetOwnershipChange { .. } => "asset_ownership_change",
            Self::AssetPossessionChange { .. } => "asset_possession_change",
            Self::Burning { .. } => "burning",
            Self::DustBurning { .. } => "dust_burning",
            Self::ContractMessage { .. } => "contract_message",
            Self::Custom { .. } => "custom",
            Self::Unknown { .. } => "unknown",
        }
    }
}

fn key(body: &[u8], offset: usize) -> [u8; 32] {
    body[offset..offset + 32]
        .try_into()
        .expect("32-byte key slice")
}

fn asset_name(bytes: &[u8]) -> String {
    bytes
        .iter()
        .copied()
        .take_while(|b| *b != 0)
        .map(|b| b as char)
        .collect()
}

fn unit_name(bytes: &[u8]) -> String {
    bytes
        .iter()
        .copied()
        .take_while(|b| *b != 0)
        .map(|b| (b + 48) as char)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_log(log_type: u8, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&240u16.to_le_bytes());
        out.extend_from_slice(&91_915_000u32.to_le_bytes());
        let combo = (log_type as u32) << 24 | body.len() as u32;
        out.extend_from_slice(&combo.to_le_bytes());
        out.extend_from_slice(&7u64.to_le_bytes());
        out.extend_from_slice(&u64::from_le_bytes(crate::crypto::k12::k12_8(body)).to_le_bytes());
        out.extend_from_slice(body);
        out
    }

    #[test]
    fn decodes_qu_transfer() {
        let mut body = Vec::new();
        body.extend_from_slice(&[1u8; 32]);
        body.extend_from_slice(&[2u8; 32]);
        body.extend_from_slice(&123i64.to_le_bytes());
        let bytes = encode_log(QU_TRANSFER, &body);
        let event = LogEvent::decode(&bytes).unwrap();
        assert!(event.verify_digest());
        assert_eq!(event.log_type, QU_TRANSFER);
        assert_eq!(
            event.body(),
            EventBody::QuTransfer {
                from: [1u8; 32],
                to: [2u8; 32],
                amount: 123
            }
        );
    }

    #[test]
    fn decodes_custom_message() {
        let mut body = Vec::new();
        body.extend_from_slice(&CUSTOM_OP_END_EPOCH.to_le_bytes());
        let bytes = encode_log(CUSTOM_MESSAGE, &body);
        let event = LogEvent::decode(&bytes).unwrap();
        assert_eq!(event.custom_op_code(), Some(CUSTOM_OP_END_EPOCH));
    }
}
