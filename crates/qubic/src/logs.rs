use crate::crypto::k12::k12_8;
use crate::error::{Error, Result};
use crate::wire::Reader;

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
pub const CUSTOM_OP_ANT_SOLUTION: u64 = 6_146_374_810_954_124_865;

pub const ORACLE_QUERY_TYPE_CONTRACT_QUERY: u8 = 0;
pub const ORACLE_QUERY_TYPE_CONTRACT_SUBSCRIPTION: u8 = 1;
pub const ORACLE_QUERY_TYPE_USER_QUERY: u8 = 2;

pub const ORACLE_QUERY_STATUS_UNKNOWN: u8 = 0;
pub const ORACLE_QUERY_STATUS_PENDING: u8 = 1;
pub const ORACLE_QUERY_STATUS_COMMITTED: u8 = 2;
pub const ORACLE_QUERY_STATUS_SUCCESS: u8 = 3;
pub const ORACLE_QUERY_STATUS_TIMEOUT: u8 = 4;
pub const ORACLE_QUERY_STATUS_UNRESOLVABLE: u8 = 5;

pub const OC_INVOCATION_STATUS_UNKNOWN: u8 = 0;
pub const OC_INVOCATION_STATUS_PENDING_AUTH: u8 = 1;
pub const OC_INVOCATION_STATUS_AUTHORIZED: u8 = 2;
pub const OC_INVOCATION_STATUS_TIMEOUT: u8 = 3;

pub const SPECTRUM_ENTITY_CATEGORIES: usize = 48;

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
        if payload.len() < LOG_HEADER_SIZE {
            return Err(Error::InvalidLog(format!(
                "log record needs at least {LOG_HEADER_SIZE} bytes, got {}",
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

    /// Encodes the 26-byte core log header followed by the raw body.
    pub fn encode(&self) -> Vec<u8> {
        let size = self.body.len();
        debug_assert!(size <= 0x00FF_FFFF, "log body exceeds 24-bit size field");
        let combo = (self.log_type as u32) << 24 | size as u32;
        let mut out = Vec::with_capacity(LOG_HEADER_SIZE + size);
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.tick.to_le_bytes());
        out.extend_from_slice(&combo.to_le_bytes());
        out.extend_from_slice(&self.log_id.to_le_bytes());
        out.extend_from_slice(&self.log_digest.to_le_bytes());
        out.extend_from_slice(&self.body);
        out
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
            Some(u64::from_le_bytes(
                self.body[..8].try_into().expect("8 bytes"),
            ))
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

pub fn oracle_query_status_name(status: u8) -> &'static str {
    match status {
        ORACLE_QUERY_STATUS_UNKNOWN => "unknown",
        ORACLE_QUERY_STATUS_PENDING => "pending",
        ORACLE_QUERY_STATUS_COMMITTED => "committed",
        ORACLE_QUERY_STATUS_SUCCESS => "success",
        ORACLE_QUERY_STATUS_TIMEOUT => "timeout",
        ORACLE_QUERY_STATUS_UNRESOLVABLE => "unresolvable",
        _ => "unknown",
    }
}

pub fn oracle_query_type_name(query_type: u8) -> &'static str {
    match query_type {
        ORACLE_QUERY_TYPE_CONTRACT_QUERY => "contract_query",
        ORACLE_QUERY_TYPE_CONTRACT_SUBSCRIPTION => "contract_subscription",
        ORACLE_QUERY_TYPE_USER_QUERY => "user_query",
        _ => "unknown",
    }
}

pub fn oc_invocation_status_name(status: u8) -> &'static str {
    match status {
        OC_INVOCATION_STATUS_UNKNOWN => "unknown",
        OC_INVOCATION_STATUS_PENDING_AUTH => "pending_authorization",
        OC_INVOCATION_STATUS_AUTHORIZED => "authorized",
        OC_INVOCATION_STATUS_TIMEOUT => "timeout",
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
    AssetOwnershipManagingContractChange {
        ownership: [u8; 32],
        issuer: [u8; 32],
        source_contract_index: u32,
        destination_contract_index: u32,
        shares: i64,
        asset_name: String,
    },
    AssetPossessionManagingContractChange {
        possession: [u8; 32],
        ownership: [u8; 32],
        issuer: [u8; 32],
        source_contract_index: u32,
        destination_contract_index: u32,
        shares: i64,
        asset_name: String,
    },
    Burning {
        from: [u8; 32],
        amount: i64,
        contract_index: u32,
    },
    DustBurning {
        burns: Vec<([u8; 32], i64)>,
    },
    SpectrumStats {
        total_amount: u64,
        dust_threshold_burn_all: u64,
        dust_threshold_burn_half: u64,
        number_of_entities: u32,
        entity_category_populations: Vec<u32>,
    },
    ContractMessage {
        contract_index: u32,
        message_type: u32,
        data: Vec<u8>,
    },
    ContractReserveDeduction {
        deducted_amount: u64,
        remaining_amount: i64,
        contract_index: u32,
    },
    OracleQueryStatusChange {
        querying_entity: [u8; 32],
        query_id: i64,
        interface_index: u32,
        query_type: u8,
        status: u8,
    },
    OracleSubscriberMessage {
        subscription_id: i32,
        interface_index: u32,
        contract_index: u32,
        period_in_milliseconds: u32,
        first_query_date_and_time: u64,
    },
    OcInvocationStatusChange {
        invocation_id: i64,
        contract_index: u32,
        interface_index: u32,
        status: u8,
    },
    Custom {
        op_code: u64,
        data: Vec<u8>,
    },
    AntSolution {
        source: [u8; 32],
        nonce: [u8; 32],
        parent_tick: u32,
        parent_solution_index: u32,
        anchor_tick: u32,
        score: u32,
        result: u32,
    },
    /// Preserved verbatim for unknown event types and for known types whose
    /// body is too short to decode safely.
    Unknown {
        raw: Vec<u8>,
    },
}

impl EventBody {
    pub fn decode(log_type: u8, body: &[u8]) -> Self {
        let decoded = match log_type {
            QU_TRANSFER => decode_qu_transfer(body),
            ASSET_ISSUANCE => decode_asset_issuance(body),
            ASSET_OWNERSHIP_CHANGE => decode_asset_change(body, false),
            ASSET_POSSESSION_CHANGE => decode_asset_change(body, true),
            ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE => {
                decode_asset_ownership_managing_contract_change(body)
            }
            ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE => {
                decode_asset_possession_managing_contract_change(body)
            }
            BURNING => decode_burning(body),
            DUST_BURNING => decode_dust_burning(body),
            SPECTRUM_STATS => decode_spectrum_stats(body),
            CONTRACT_ERROR_MESSAGE
            | CONTRACT_WARNING_MESSAGE
            | CONTRACT_INFORMATION_MESSAGE
            | CONTRACT_DEBUG_MESSAGE => decode_contract_message(body),
            CONTRACT_RESERVE_DEDUCTION => decode_contract_reserve_deduction(body),
            ORACLE_QUERY_STATUS_CHANGE => decode_oracle_query_status_change(body),
            ORACLE_SUBSCRIBER_MESSAGE => decode_oracle_subscriber_message(body),
            OC_INVOCATION_STATUS_CHANGE => decode_oc_invocation_status_change(body),
            CUSTOM_MESSAGE => decode_custom_message(body),
            _ => None,
        };
        decoded.unwrap_or_else(|| Self::Unknown { raw: body.to_vec() })
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::QuTransfer { .. } => "qu_transfer",
            Self::AssetIssuance { .. } => "asset_issuance",
            Self::AssetOwnershipChange { .. } => "asset_ownership_change",
            Self::AssetPossessionChange { .. } => "asset_possession_change",
            Self::AssetOwnershipManagingContractChange { .. } => {
                "asset_ownership_managing_contract_change"
            }
            Self::AssetPossessionManagingContractChange { .. } => {
                "asset_possession_managing_contract_change"
            }
            Self::Burning { .. } => "burning",
            Self::DustBurning { .. } => "dust_burning",
            Self::SpectrumStats { .. } => "spectrum_stats",
            Self::ContractMessage { .. } => "contract_message",
            Self::ContractReserveDeduction { .. } => "contract_reserve_deduction",
            Self::OracleQueryStatusChange { .. } => "oracle_query_status_change",
            Self::OracleSubscriberMessage { .. } => "oracle_subscriber_message",
            Self::OcInvocationStatusChange { .. } => "oc_invocation_status_change",
            Self::Custom { .. } => "custom",
            Self::AntSolution { .. } => "ant_solution",
            Self::Unknown { .. } => "unknown",
        }
    }
}

fn decode_qu_transfer(body: &[u8]) -> Option<EventBody> {
    if body.len() < 72 {
        return None;
    }
    Some(EventBody::QuTransfer {
        from: key(body, 0),
        to: key(body, 32),
        amount: i64::from_le_bytes(body[64..72].try_into().expect("8 bytes")),
    })
}

fn decode_asset_issuance(body: &[u8]) -> Option<EventBody> {
    if body.len() < 63 {
        return None;
    }
    Some(EventBody::AssetIssuance {
        asset: AssetRef {
            issuer: key(body, 0),
            managing_contract: i64::from_le_bytes(body[40..48].try_into().expect("8 bytes")),
            name: asset_name(&body[48..55]),
            decimals: body[55],
            unit: unit_name(&body[56..63]),
        },
        shares: i64::from_le_bytes(body[32..40].try_into().expect("8 bytes")),
    })
}

/// ASSET_OWNERSHIP_CHANGE and ASSET_POSSESSION_CHANGE share this layout:
/// source(32) destination(32) issuer(32) shares(8) managingContract(8)
/// name(7) decimals(1) unit(7).
fn decode_asset_change(body: &[u8], possession: bool) -> Option<EventBody> {
    if body.len() < 127 {
        return None;
    }
    let from = key(body, 0);
    let to = key(body, 32);
    let shares = i64::from_le_bytes(body[96..104].try_into().expect("8 bytes"));
    let asset = AssetRef {
        issuer: key(body, 64),
        managing_contract: i64::from_le_bytes(body[104..112].try_into().expect("8 bytes")),
        name: asset_name(&body[112..119]),
        decimals: body[119],
        unit: unit_name(&body[120..127]),
    };
    Some(if possession {
        EventBody::AssetPossessionChange {
            from,
            to,
            asset,
            shares,
        }
    } else {
        EventBody::AssetOwnershipChange {
            from,
            to,
            asset,
            shares,
        }
    })
}

fn decode_asset_ownership_managing_contract_change(body: &[u8]) -> Option<EventBody> {
    if body.len() < 87 {
        return None;
    }
    Some(EventBody::AssetOwnershipManagingContractChange {
        ownership: key(body, 0),
        issuer: key(body, 32),
        source_contract_index: u32::from_le_bytes(body[64..68].try_into().expect("4 bytes")),
        destination_contract_index: u32::from_le_bytes(body[68..72].try_into().expect("4 bytes")),
        shares: i64::from_le_bytes(body[72..80].try_into().expect("8 bytes")),
        asset_name: asset_name(&body[80..87]),
    })
}

fn decode_asset_possession_managing_contract_change(body: &[u8]) -> Option<EventBody> {
    if body.len() < 119 {
        return None;
    }
    Some(EventBody::AssetPossessionManagingContractChange {
        possession: key(body, 0),
        ownership: key(body, 32),
        issuer: key(body, 64),
        source_contract_index: u32::from_le_bytes(body[96..100].try_into().expect("4 bytes")),
        destination_contract_index: u32::from_le_bytes(body[100..104].try_into().expect("4 bytes")),
        shares: i64::from_le_bytes(body[104..112].try_into().expect("8 bytes")),
        asset_name: asset_name(&body[112..119]),
    })
}

fn decode_burning(body: &[u8]) -> Option<EventBody> {
    if body.len() < 44 {
        return None;
    }
    Some(EventBody::Burning {
        from: key(body, 0),
        amount: i64::from_le_bytes(body[32..40].try_into().expect("8 bytes")),
        contract_index: u32::from_le_bytes(body[40..44].try_into().expect("4 bytes")),
    })
}

fn decode_dust_burning(body: &[u8]) -> Option<EventBody> {
    if body.len() < 2 {
        return None;
    }
    let count = u16::from_le_bytes([body[0], body[1]]) as usize;
    let required = 2 + count * 40;
    if body.len() < required {
        return None;
    }
    let mut burns = Vec::with_capacity(count);
    let mut offset = 2;
    for _ in 0..count {
        burns.push((
            key(body, offset),
            i64::from_le_bytes(body[offset + 32..offset + 40].try_into().expect("8 bytes")),
        ));
        offset += 40;
    }
    Some(EventBody::DustBurning { burns })
}

fn decode_spectrum_stats(body: &[u8]) -> Option<EventBody> {
    if body.len() < 224 {
        return None;
    }
    let mut r = Reader::new(body);
    let total_amount = r.u64().ok()?;
    let dust_threshold_burn_all = r.u64().ok()?;
    let dust_threshold_burn_half = r.u64().ok()?;
    let number_of_entities = r.u32().ok()?;
    let mut entity_category_populations = Vec::with_capacity(SPECTRUM_ENTITY_CATEGORIES);
    for _ in 0..SPECTRUM_ENTITY_CATEGORIES {
        entity_category_populations.push(r.u32().ok()?);
    }
    Some(EventBody::SpectrumStats {
        total_amount,
        dust_threshold_burn_all,
        dust_threshold_burn_half,
        number_of_entities,
        entity_category_populations,
    })
}

fn decode_contract_message(body: &[u8]) -> Option<EventBody> {
    if body.len() < 8 {
        return None;
    }
    Some(EventBody::ContractMessage {
        contract_index: u32::from_le_bytes(body[0..4].try_into().expect("4 bytes")),
        message_type: u32::from_le_bytes(body[4..8].try_into().expect("4 bytes")),
        data: body[8..].to_vec(),
    })
}

fn decode_contract_reserve_deduction(body: &[u8]) -> Option<EventBody> {
    if body.len() < 24 {
        return None;
    }
    Some(EventBody::ContractReserveDeduction {
        deducted_amount: u64::from_le_bytes(body[0..8].try_into().expect("8 bytes")),
        remaining_amount: i64::from_le_bytes(body[8..16].try_into().expect("8 bytes")),
        contract_index: u32::from_le_bytes(body[16..20].try_into().expect("4 bytes")),
    })
}

fn decode_oracle_query_status_change(body: &[u8]) -> Option<EventBody> {
    if body.len() < 46 {
        return None;
    }
    Some(EventBody::OracleQueryStatusChange {
        querying_entity: key(body, 0),
        query_id: i64::from_le_bytes(body[32..40].try_into().expect("8 bytes")),
        interface_index: u32::from_le_bytes(body[40..44].try_into().expect("4 bytes")),
        query_type: body[44],
        status: body[45],
    })
}

fn decode_oracle_subscriber_message(body: &[u8]) -> Option<EventBody> {
    if body.len() < 24 {
        return None;
    }
    Some(EventBody::OracleSubscriberMessage {
        subscription_id: i32::from_le_bytes(body[0..4].try_into().expect("4 bytes")),
        interface_index: u32::from_le_bytes(body[4..8].try_into().expect("4 bytes")),
        contract_index: u32::from_le_bytes(body[8..12].try_into().expect("4 bytes")),
        period_in_milliseconds: u32::from_le_bytes(body[12..16].try_into().expect("4 bytes")),
        first_query_date_and_time: u64::from_le_bytes(body[16..24].try_into().expect("8 bytes")),
    })
}

fn decode_oc_invocation_status_change(body: &[u8]) -> Option<EventBody> {
    if body.len() < 17 {
        return None;
    }
    Some(EventBody::OcInvocationStatusChange {
        invocation_id: i64::from_le_bytes(body[0..8].try_into().expect("8 bytes")),
        contract_index: u32::from_le_bytes(body[8..12].try_into().expect("4 bytes")),
        interface_index: u32::from_le_bytes(body[12..16].try_into().expect("4 bytes")),
        status: body[16],
    })
}

fn decode_custom_message(body: &[u8]) -> Option<EventBody> {
    if body.len() < 8 {
        return None;
    }
    let op_code = u64::from_le_bytes(body[..8].try_into().expect("8 bytes"));
    if op_code == CUSTOM_OP_ANT_SOLUTION {
        if body.len() < 92 {
            return None;
        }
        return Some(EventBody::AntSolution {
            source: key(body, 8),
            nonce: key(body, 40),
            parent_tick: u32::from_le_bytes(body[72..76].try_into().expect("4 bytes")),
            parent_solution_index: u32::from_le_bytes(body[76..80].try_into().expect("4 bytes")),
            anchor_tick: u32::from_le_bytes(body[80..84].try_into().expect("4 bytes")),
            score: u32::from_le_bytes(body[84..88].try_into().expect("4 bytes")),
            result: u32::from_le_bytes(body[88..92].try_into().expect("4 bytes")),
        });
    }
    Some(EventBody::Custom {
        op_code,
        data: body[8..].to_vec(),
    })
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
        .map(|b| char::from(b.wrapping_add(48)))
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

    #[test]
    fn short_known_body_stays_raw() {
        let body = [0xAB, 0xCD, 0xEF];
        assert_eq!(
            EventBody::decode(QU_TRANSFER, &body),
            EventBody::Unknown { raw: body.to_vec() }
        );
    }
}
