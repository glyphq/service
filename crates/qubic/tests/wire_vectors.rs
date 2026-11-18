//! Wire-format vectors derived from `qubic/core` `src/network_messages/*.h`.
//!
//! These tests are intentionally independent from the K12 implementation: they
//! only exercise layout, endianness, and truncation handling.

use qubic::logs::{
    self, ASSET_ISSUANCE, ASSET_OWNERSHIP_CHANGE, ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE,
    ASSET_POSSESSION_CHANGE, ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE, BURNING,
    CONTRACT_DEBUG_MESSAGE, CONTRACT_ERROR_MESSAGE, CONTRACT_INFORMATION_MESSAGE,
    CONTRACT_RESERVE_DEDUCTION, CONTRACT_WARNING_MESSAGE, CUSTOM_MESSAGE, CUSTOM_OP_END_EPOCH,
    DUST_BURNING, EventBody, LogEvent, OC_INVOCATION_STATUS_CHANGE, ORACLE_QUERY_STATUS_CHANGE,
    ORACLE_QUERY_TYPE_USER_QUERY, ORACLE_SUBSCRIBER_MESSAGE, QU_TRANSFER, SPECTRUM_STATS,
};
use qubic::messages::*;
use qubic::wire::{HEADER_SIZE, Header, MessageType, Packet};

#[test]
fn core_sizes_match_headers() {
    assert_eq!(TICK_SIZE, 352);
    assert_eq!(TICK_DATA_SIZE, 139_376);
    assert_eq!(COMPUTORS_SIZE, 21_698);
    assert_eq!(CURRENT_TICK_INFO_SIZE, 16);
    assert_eq!(LOG_RANGES_SIZE, 65_632);
    assert_eq!(REQUEST_QUORUM_TICK_SIZE, 92);
    assert_eq!(REQUEST_TICK_TRANSACTIONS_SIZE, 516);
    assert_eq!(Transaction::BASE_SIZE, 80);
    assert_eq!(ASSET_RECORD_SIZE, 48);
    assert_eq!(REQUEST_ASSETS_SIZE, 112);
    assert_eq!(RESPOND_ASSETS_SIZE, 56);
    assert_eq!(RESPOND_ASSETS_WITH_SIBLINGS_SIZE, 824);
    assert_eq!(RESPOND_ISSUED_ASSETS_SIZE, 824);
    assert_eq!(RESPOND_OWNED_ASSETS_SIZE, 872);
    assert_eq!(RESPOND_POSSESSED_ASSETS_SIZE, 920);
    assert_eq!(ENTITY_RECORD_SIZE, 64);
    assert_eq!(RESPOND_ENTITY_SIZE, 840);
    assert_eq!(RESPOND_SYSTEM_INFO_SIZE, 128);
    assert_eq!(REQUEST_CONTRACT_FUNCTION_SIZE, 8);
    assert_eq!(RESPOND_CONTRACT_IPO_SIZE, 27_048);
    assert_eq!(LOG_TX_PER_TICK, 4_102);
    assert_eq!(REQUEST_ENTITY_SIZE, 32);
    assert_eq!(REQUEST_LOG_ID_RANGE_FROM_TX_SIZE, 40);
    assert_eq!(REQUEST_PRUNING_LOG_SIZE, 48);
    assert_eq!(REQUEST_LOG_STATE_DIGEST_SIZE, 36);
    assert_eq!(RESPOND_LOG_STATE_DIGEST_SIZE, 32);
}

#[test]
fn tick_field_offsets_and_endianness() {
    let tick = Tick {
        computor_index: 0x0102,
        epoch: 0x0304,
        tick: 0x0506_0708,
        millisecond: 0x090A,
        second: 0x0B,
        minute: 0x0C,
        hour: 0x0D,
        day: 0x0E,
        month: 0x0F,
        year: 0x10,
        prev_resource_testing_digest: 0x1112_1314,
        salted_resource_testing_digest: 0x1516_1718,
        prev_transaction_body_digest: 0x191A_1B1C,
        salted_transaction_body_digest: 0x1D1E_1F20,
        prev_spectrum_digest: [0x21; 32],
        prev_universe_digest: [0x22; 32],
        prev_computer_digest: [0x23; 32],
        salted_spectrum_digest: [0x24; 32],
        salted_universe_digest: [0x25; 32],
        salted_computer_digest: [0x26; 32],
        transaction_digest: [0x27; 32],
        expected_next_tick_transaction_digest: [0x28; 32],
        signature: [0x29; 64],
    };
    let bytes = tick.encode();
    assert_eq!(&bytes[0..2], &[0x02, 0x01]);
    assert_eq!(&bytes[2..4], &[0x04, 0x03]);
    assert_eq!(&bytes[4..8], &[0x08, 0x07, 0x06, 0x05]);
    assert_eq!(&bytes[8..10], &[0x0A, 0x09]);
    assert_eq!(&bytes[10..16], &[0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10]);
    assert_eq!(&bytes[16..20], &[0x14, 0x13, 0x12, 0x11]);
    assert_eq!(&bytes[20..24], &[0x18, 0x17, 0x16, 0x15]);
    assert_eq!(&bytes[24..28], &[0x1C, 0x1B, 0x1A, 0x19]);
    assert_eq!(&bytes[28..32], &[0x20, 0x1F, 0x1E, 0x1D]);
    assert_eq!(&bytes[32..64], &[0x21; 32]);
    assert_eq!(&bytes[64..96], &[0x22; 32]);
    assert_eq!(&bytes[96..128], &[0x23; 32]);
    assert_eq!(&bytes[128..160], &[0x24; 32]);
    assert_eq!(&bytes[160..192], &[0x25; 32]);
    assert_eq!(&bytes[192..224], &[0x26; 32]);
    assert_eq!(&bytes[224..256], &[0x27; 32]);
    assert_eq!(&bytes[256..288], &[0x28; 32]);
    assert_eq!(&bytes[288..352], &[0x29; 64]);
    assert_eq!(Tick::decode(&bytes).unwrap(), tick);
}

#[test]
fn tick_data_offsets() {
    let mut payload = vec![0u8; TICK_DATA_SIZE];
    payload[0..2].copy_from_slice(&7u16.to_le_bytes());
    payload[2..4].copy_from_slice(&240u16.to_le_bytes());
    payload[4..8].copy_from_slice(&91_915_000u32.to_le_bytes());
    payload[8..10].copy_from_slice(&123u16.to_le_bytes());
    payload[10..16].copy_from_slice(&[1, 2, 3, 4, 5, 6]);
    payload[16..48].copy_from_slice(&[0xAA; 32]);
    payload[48..80].copy_from_slice(&[0xBB; 32]);
    // Second transaction digest starts at 48 + 32.
    payload[80..112].copy_from_slice(&[0xCC; 32]);
    let fees_offset = 48 + TX_PER_TICK * 32;
    assert_eq!(fees_offset, 131_120);
    payload[fees_offset..fees_offset + 8].copy_from_slice(&(-42i64).to_le_bytes());
    let signature_offset = fees_offset + MAX_CONTRACTS * 8;
    assert_eq!(signature_offset, 139_312);
    payload[signature_offset..signature_offset + 64].copy_from_slice(&[0xDD; 64]);

    let data = TickData::decode(&payload).unwrap();
    assert_eq!(data.tick, 91_915_000);
    assert_eq!(data.timelock, [0xAA; 32]);
    assert_eq!(data.transaction_digests[0], [0xBB; 32]);
    assert_eq!(data.transaction_digests[1], [0xCC; 32]);
    assert_eq!(data.contract_fees[0], -42);
    assert_eq!(data.signature, [0xDD; 64]);
    assert_eq!(data.encode(), payload);
    assert!(TickData::decode(&payload[..TICK_DATA_SIZE - 1]).is_err());
}

#[test]
fn transaction_layout_and_input_size_validation() {
    let tx = Transaction {
        source: [1; 32],
        destination: [2; 32],
        amount: -5,
        tick: 0x0A0B_0C0D,
        input_type: 0x1234,
        input: vec![9, 8, 7],
        signature: [6; 64],
    };
    let bytes = tx.encode();
    assert_eq!(bytes.len(), 80 + 3 + 64);
    assert_eq!(&bytes[64..72], &(-5i64).to_le_bytes());
    assert_eq!(&bytes[72..76], &[0x0D, 0x0C, 0x0B, 0x0A]);
    assert_eq!(&bytes[76..78], &[0x34, 0x12]);
    assert_eq!(&bytes[78..80], &[3, 0]);
    assert_eq!(&bytes[80..83], &[9, 8, 7]);

    let mut bad = bytes.clone();
    bad[78..80].copy_from_slice(&4u16.to_le_bytes());
    assert!(Transaction::decode(&bad).is_err());
    assert!(Transaction::decode(&bytes[..80]).is_err());
}

#[test]
fn computors_layout() {
    let mut payload = vec![0u8; COMPUTORS_SIZE];
    payload[0..2].copy_from_slice(&240u16.to_le_bytes());
    payload[2..34].copy_from_slice(&[0x11; 32]);
    let last_key = 2 + (COMPUTOR_COUNT - 1) * 32;
    payload[last_key..last_key + 32].copy_from_slice(&[0x22; 32]);
    let signature = 2 + COMPUTOR_COUNT * 32;
    assert_eq!(signature, 21_634);
    payload[signature..signature + 64].copy_from_slice(&[0x33; 64]);

    let computors = Computors::decode(&payload).unwrap();
    assert_eq!(computors.epoch, 240);
    assert_eq!(computors.public_keys[0], [0x11; 32]);
    assert_eq!(computors.public_keys[COMPUTOR_COUNT - 1], [0x22; 32]);
    assert_eq!(computors.signature, [0x33; 64]);
    assert_eq!(computors.encode(), payload);
    assert!(Computors::decode(&payload[..COMPUTORS_SIZE - 1]).is_err());
}

#[test]
fn current_tick_info_layout() {
    let payload = [
        0x34, 0x12, // tickDuration
        0xF0, 0x00, // epoch
        0x78, 0x56, 0x34, 0x12, // tick
        0x03, 0x00, // aligned votes
        0x02, 0x00, // misaligned votes
        0x01, 0x00, 0x00, 0x00, // initial tick
    ];
    let info = CurrentTickInfo::decode(&payload).unwrap();
    assert_eq!(info.tick_duration, 0x1234);
    assert_eq!(info.epoch, 240);
    assert_eq!(info.tick, 0x1234_5678);
    assert_eq!(info.aligned_votes, 3);
    assert_eq!(info.misaligned_votes, 2);
    assert_eq!(info.initial_tick, 1);
    assert_eq!(info.encode(), payload);
    assert!(CurrentTickInfo::decode(&payload[..15]).is_err());
}

#[test]
fn log_ranges_layout() {
    let mut ranges = LogRanges {
        from_log_id: vec![0; LOG_TX_PER_TICK],
        length: vec![0; LOG_TX_PER_TICK],
    };
    ranges.from_log_id[0] = 10;
    ranges.from_log_id[LOG_TX_PER_TICK - 1] = 20;
    ranges.length[0] = 1;
    ranges.length[LOG_TX_PER_TICK - 1] = 2;
    let bytes = ranges.encode();
    assert_eq!(bytes.len(), LOG_RANGES_SIZE);
    assert_eq!(&bytes[0..8], &10i64.to_le_bytes());
    let length_offset = LOG_TX_PER_TICK * 8;
    assert_eq!(
        &bytes[length_offset..length_offset + 8],
        &1i64.to_le_bytes()
    );
    assert_eq!(LogRanges::decode(&bytes).unwrap(), ranges);
    assert!(LogRanges::decode(&bytes[..LOG_RANGES_SIZE - 1]).is_err());
}

#[test]
fn vote_requests_match_reference_sizes() {
    let mut known = [false; COMPUTOR_COUNT];
    known[0] = true;
    known[675] = true;
    let quorum = request_quorum_tick(42, &known);
    assert_eq!(quorum.len(), REQUEST_QUORUM_TICK_SIZE);
    assert_eq!(&quorum[..4], &42u32.to_le_bytes());
    assert_eq!(quorum[4] & 1, 1);
    assert_eq!(quorum[4 + 84] & (1 << 3), 1 << 3);
    // Vote flags are 85 bytes followed by C struct padding to 92.
    assert_eq!(&quorum[4 + 85..], &[0, 0, 0]);

    let mut requested = [false; TX_PER_TICK];
    requested[0] = true;
    requested[TX_PER_TICK - 1] = true;
    let transactions = request_tick_transactions(7, &requested);
    assert_eq!(transactions.len(), REQUEST_TICK_TRANSACTIONS_SIZE);
    assert_eq!(&transactions[..4], &7u32.to_le_bytes());
    assert_eq!(transactions[4] & 1, 1);
    assert_eq!(transactions[4 + 511] & (1 << 7), 1 << 7);
}

#[test]
fn asset_records_and_units() {
    let name = *b"MYTKN\0\0";
    let unit = [0u8, 1, 0xFF, 2, 3, 4, 5]; // 0 -> '0', -1 -> '/'
    let mut record = [0u8; ASSET_RECORD_SIZE];
    record[0..32].copy_from_slice(&[0xAB; 32]);
    record[32] = ASSET_TYPE_ISSUANCE;
    record[33..40].copy_from_slice(&name);
    record[40] = 6;
    record[41..48].copy_from_slice(&unit);

    let asset = AssetRecord::decode(&record).unwrap();
    assert_eq!(asset.public_key, [0xAB; 32]);
    match &asset.kind {
        AssetRecordKind::Issuance(issuance) => {
            assert_eq!(issuance.name(), "MYTKN");
            assert_eq!(issuance.decimals, 6);
            assert_eq!(issuance.unit(), "01/2345");
        }
        other => panic!("expected issuance, got {other:?}"),
    }
    assert_eq!(asset.encode(), record);

    // Ownership and possession layouts share offsets 32..48.
    let mut ownership = [0u8; ASSET_RECORD_SIZE];
    ownership[0..32].copy_from_slice(&[1; 32]);
    ownership[32] = ASSET_TYPE_OWNERSHIP;
    ownership[34..36].copy_from_slice(&9u16.to_le_bytes());
    ownership[36..40].copy_from_slice(&123u32.to_le_bytes());
    ownership[40..48].copy_from_slice(&456i64.to_le_bytes());
    let asset = AssetRecord::decode(&ownership).unwrap();
    match asset.kind {
        AssetRecordKind::Ownership(record) => {
            assert_eq!(record.managing_contract_index, 9);
            assert_eq!(record.issuance_index, 123);
            assert_eq!(record.shares, 456);
        }
        other => panic!("expected ownership, got {other:?}"),
    }
    assert!(
        AssetRecord::decode(&record[..47])
            .unwrap_err()
            .to_string()
            .contains("48 bytes")
    );
}

#[test]
fn request_and_respond_assets_layout() {
    let request = RequestAssets::ByFilter {
        request_type: ASSET_REQUEST_OWNERSHIP,
        flags: ASSET_REQUEST_GET_SIBLINGS,
        ownership_managing_contract: 1,
        possession_managing_contract: 2,
        issuer: [3; 32],
        asset_name: 0x0000_474E_4B54_594D,
        owner: [4; 32],
        possessor: [5; 32],
    };
    let bytes = request.encode();
    assert_eq!(bytes.len(), 112);
    assert_eq!(&bytes[0..2], &1u16.to_le_bytes());
    assert_eq!(&bytes[2..4], &1u16.to_le_bytes());
    assert_eq!(&bytes[8..40], &[3; 32]);
    assert_eq!(&bytes[40..48], &0x0000_474E_4B54_594Du64.to_le_bytes());
    assert_eq!(&bytes[48..80], &[4; 32]);
    assert_eq!(&bytes[80..112], &[5; 32]);
    assert_eq!(RequestAssets::decode(&bytes).unwrap(), request);

    let by_index = RequestAssets::ByUniverseIndex {
        flags: 1,
        universe_index: 0xDEAD_BEEF,
    };
    let bytes = by_index.encode();
    assert_eq!(&bytes[0..2], &3u16.to_le_bytes());
    assert_eq!(&bytes[4..8], &0xDEAD_BEEFu32.to_le_bytes());
    assert_eq!(RequestAssets::decode(&bytes).unwrap(), by_index);
    assert!(RequestAssets::decode(&bytes[..111]).is_err());

    let mut record = [0u8; ASSET_RECORD_SIZE];
    record[32] = ASSET_TYPE_ISSUANCE;
    record[33..40].copy_from_slice(b"QX\0\0\0\0\0");
    let response = RespondAssets {
        asset: AssetRecord::decode(&record).unwrap(),
        tick: 100,
        universe_index: 200,
    };
    let bytes = response.encode();
    assert_eq!(bytes.len(), RESPOND_ASSETS_SIZE);
    assert_eq!(&bytes[48..52], &100u32.to_le_bytes());
    assert_eq!(&bytes[52..56], &200u32.to_le_bytes());
    assert_eq!(RespondAssets::decode(&bytes).unwrap(), response);

    let siblings = RespondAssetsWithSiblings {
        asset: response.asset,
        tick: 1,
        universe_index: 2,
        siblings: vec![[0x5A; 32]; ASSETS_DEPTH],
    };
    let bytes = siblings.encode();
    assert_eq!(bytes.len(), RESPOND_ASSETS_WITH_SIBLINGS_SIZE);
    assert_eq!(RespondAssetsWithSiblings::decode(&bytes).unwrap(), siblings);

    let record = AssetRecord::decode(&record).unwrap();
    let issued = RespondIssuedAssets {
        asset: record.clone(),
        tick: 3,
        universe_index: 4,
        siblings: vec![[0; 32]; ASSETS_DEPTH],
    };
    let bytes = issued.encode();
    assert_eq!(bytes.len(), RESPOND_ISSUED_ASSETS_SIZE);
    assert_eq!(RespondIssuedAssets::decode(&bytes).unwrap(), issued);

    let owned = RespondOwnedAssets {
        asset: record.clone(),
        issuance_asset: record.clone(),
        tick: 5,
        universe_index: 6,
        siblings: vec![[1; 32]; ASSETS_DEPTH],
    };
    let bytes = owned.encode();
    assert_eq!(bytes.len(), RESPOND_OWNED_ASSETS_SIZE);
    assert_eq!(RespondOwnedAssets::decode(&bytes).unwrap(), owned);

    let possessed = RespondPossessedAssets {
        asset: record.clone(),
        ownership_asset: record.clone(),
        issuance_asset: record,
        tick: 7,
        universe_index: 8,
        siblings: vec![[2; 32]; ASSETS_DEPTH],
    };
    let bytes = possessed.encode();
    assert_eq!(bytes.len(), RESPOND_POSSESSED_ASSETS_SIZE);
    assert_eq!(RespondPossessedAssets::decode(&bytes).unwrap(), possessed);
}

#[test]
fn entity_and_system_info_layout() {
    let entity = EntityRecord {
        public_key: [7; 32],
        incoming_amount: -1,
        outgoing_amount: 2,
        number_of_incoming_transfers: 3,
        number_of_outgoing_transfers: 4,
        latest_incoming_transfer_tick: 5,
        latest_outgoing_transfer_tick: 6,
    };
    let bytes = entity.encode();
    assert_eq!(bytes.len(), ENTITY_RECORD_SIZE);
    assert_eq!(&bytes[32..40], &(-1i64).to_le_bytes());
    assert_eq!(&bytes[40..48], &2i64.to_le_bytes());
    assert_eq!(&bytes[48..52], &3u32.to_le_bytes());
    assert_eq!(&bytes[52..56], &4u32.to_le_bytes());
    assert_eq!(&bytes[56..60], &5u32.to_le_bytes());
    assert_eq!(&bytes[60..64], &6u32.to_le_bytes());
    assert_eq!(EntityRecord::decode(&bytes).unwrap(), entity);

    let response = RespondEntity {
        entity,
        tick: 9,
        spectrum_index: -3,
        siblings: vec![[0x11; 32]; SPECTRUM_DEPTH],
    };
    let bytes = response.encode();
    assert_eq!(bytes.len(), RESPOND_ENTITY_SIZE);
    assert_eq!(&bytes[64..68], &9u32.to_le_bytes());
    assert_eq!(&bytes[68..72], &(-3i32).to_le_bytes());
    assert_eq!(RespondEntity::decode(&bytes).unwrap(), response);
    assert!(RespondEntity::decode(&bytes[..839]).is_err());

    let info = SystemInfo {
        version: 1,
        epoch: 240,
        tick: 91_915_000,
        initial_tick: 91_000_000,
        latest_created_tick: 91_914_000,
        initial_millisecond: 1,
        initial_second: 2,
        initial_minute: 3,
        initial_hour: 4,
        initial_day: 5,
        initial_month: 6,
        initial_year: 26,
        number_of_entities: 7,
        number_of_transactions: 8,
        random_mining_seed: [0x42; 32],
        solution_threshold: -9,
        total_spectrum_amount: 10,
        current_entity_balance_dust_threshold: 11,
        target_tick_vote_signature: 12,
        computor_packet_signature: 13,
        solution_additional_threshold: 14,
        reserve2: 15,
        reserve3: 16,
        reserve4: 17,
    };
    let bytes = info.encode();
    assert_eq!(bytes.len(), RESPOND_SYSTEM_INFO_SIZE);
    assert_eq!(&bytes[0..2], &1i16.to_le_bytes());
    assert_eq!(&bytes[2..4], &240u16.to_le_bytes());
    assert_eq!(&bytes[4..8], &91_915_000u32.to_le_bytes());
    assert_eq!(&bytes[16..18], &1u16.to_le_bytes());
    assert_eq!(&bytes[18..24], &[2, 3, 4, 5, 6, 26]);
    assert_eq!(&bytes[24..28], &7u32.to_le_bytes());
    assert_eq!(&bytes[28..32], &8u32.to_le_bytes());
    assert_eq!(&bytes[32..64], &[0x42; 32]);
    assert_eq!(&bytes[64..68], &(-9i32).to_le_bytes());
    assert_eq!(&bytes[68..76], &10u64.to_le_bytes());
    assert_eq!(&bytes[76..84], &11u64.to_le_bytes());
    assert_eq!(&bytes[84..88], &12u32.to_le_bytes());
    assert_eq!(&bytes[88..96], &13u64.to_le_bytes());
    assert_eq!(&bytes[96..104], &14u64.to_le_bytes());
    assert_eq!(&bytes[104..112], &15u64.to_le_bytes());
    assert_eq!(&bytes[112..120], &16u64.to_le_bytes());
    assert_eq!(&bytes[120..128], &17u64.to_le_bytes());
    assert_eq!(SystemInfo::decode(&bytes).unwrap(), info);
    assert!(SystemInfo::decode(&bytes[..127]).is_err());
}

#[test]
fn contract_function_and_ipo_messages() {
    let request = RequestContractFunction {
        contract_index: 13,
        input_type: 1,
        input: vec![1, 2, 3, 4],
    };
    let bytes = request.encode();
    assert_eq!(bytes.len(), 12);
    assert_eq!(&bytes[0..4], &13u32.to_le_bytes());
    assert_eq!(&bytes[4..6], &1u16.to_le_bytes());
    assert_eq!(&bytes[6..8], &4u16.to_le_bytes());
    assert_eq!(RequestContractFunction::decode(&bytes).unwrap(), request);
    assert!(RequestContractFunction::decode(&bytes[..7]).is_err());
    let mut mismatch = bytes.clone();
    mismatch[6..8].copy_from_slice(&9u16.to_le_bytes());
    assert!(RequestContractFunction::decode(&mismatch).is_err());

    let ipo = RespondContractIpo {
        contract_index: 2,
        tick: 3,
        public_keys: vec![[0x33; 32]; COMPUTOR_COUNT],
        prices: vec![-1; COMPUTOR_COUNT],
    };
    let bytes = ipo.encode();
    assert_eq!(bytes.len(), RESPOND_CONTRACT_IPO_SIZE);
    assert_eq!(&bytes[0..4], &2u32.to_le_bytes());
    assert_eq!(&bytes[4..8], &3u32.to_le_bytes());
    let first_price = 8 + COMPUTOR_COUNT * 32;
    assert_eq!(&bytes[first_price..first_price + 8], &(-1i64).to_le_bytes());
    assert_eq!(RespondContractIpo::decode(&bytes).unwrap(), ipo);

    let active = RespondActiveIpo {
        contract_index: 5,
        asset_name: *b"QX\0\0\0\0\0\0",
    };
    assert_eq!(active.asset_name(), "QX");
    assert_eq!(RespondActiveIpo::decode(&active.encode()).unwrap(), active);
    assert_eq!(
        RequestContractIpo { contract_index: 9 }.encode(),
        9u32.to_le_bytes()
    );
}

#[test]
fn auxiliary_request_structs_round_trip() {
    let entity = RequestEntity {
        public_key: [9; 32],
    };
    assert_eq!(RequestEntity::decode(&entity.encode()).unwrap(), entity);

    let range = RequestLogIdRangeFromTx {
        passcode: [1, 2, 3, 4],
        tick: 5,
        tx_id: 6,
    };
    let bytes = range.encode();
    assert_eq!(bytes.len(), 40);
    assert_eq!(&bytes[0..8], &1u64.to_le_bytes());
    assert_eq!(&bytes[32..36], &5u32.to_le_bytes());
    assert_eq!(&bytes[36..40], &6u32.to_le_bytes());
    assert_eq!(RequestLogIdRangeFromTx::decode(&bytes).unwrap(), range);

    let pruning = RequestPruningLog {
        passcode: [7; 4],
        from_log_id: 8,
        to_log_id: 9,
    };
    assert_eq!(
        RequestPruningLog::decode(&pruning.encode()).unwrap(),
        pruning
    );
    assert_eq!(
        RespondPruningLog::decode(&(-1i64).to_le_bytes())
            .unwrap()
            .success,
        -1
    );

    let digest_request = RequestLogStateDigest {
        passcode: [10; 4],
        requested_tick: 11,
    };
    assert_eq!(
        RequestLogStateDigest::decode(&digest_request.encode()).unwrap(),
        digest_request
    );
    let digest = RespondLogStateDigest { digest: [12; 32] };
    assert_eq!(
        RespondLogStateDigest::decode(&digest.encode()).unwrap(),
        digest
    );
}

#[test]
fn packet_framing_rejects_truncated_and_invalid_headers() {
    let packet = Packet::new(MessageType::RequestCurrentTickInfo, 0xDEAD_BEEF, Vec::new());
    let bytes = packet.encode();
    assert_eq!(bytes.len(), HEADER_SIZE);
    assert_eq!(&bytes[..3], &[8, 0, 0]);
    assert_eq!(bytes[3], 27);
    assert_eq!(&bytes[4..8], &0xDEAD_BEEFu32.to_le_bytes());
    assert_eq!(Packet::decode(&bytes).unwrap(), packet);
    assert!(Header::decode(&bytes[..7]).is_err());
    let mut too_small = bytes.clone();
    too_small[0] = 4;
    assert!(Header::decode(&too_small).is_err());
    let mut unknown = bytes.clone();
    unknown[3] = 200;
    assert!(Header::decode(&unknown).is_err());

    let packet = Packet::new(MessageType::RequestLog, 1, vec![0; 48]);
    let bytes = packet.encode();
    assert_eq!(bytes.len(), 56);
    assert!(Packet::decode(&bytes[..55]).is_err());
    // Extra trailing bytes beyond the declared size are ignored.
    let mut padded = bytes.clone();
    padded.extend_from_slice(&[0xAA; 4]);
    assert_eq!(Packet::decode(&padded).unwrap(), packet);
}

fn log_record(log_type: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&240u16.to_le_bytes());
    out.extend_from_slice(&91_915_000u32.to_le_bytes());
    let combo = (log_type as u32) << 24 | body.len() as u32;
    out.extend_from_slice(&combo.to_le_bytes());
    out.extend_from_slice(&1u64.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(body);
    out
}

fn decode_body(log_type: u8, body: &[u8]) -> EventBody {
    LogEvent::decode(&log_record(log_type, body))
        .unwrap()
        .body()
}

#[test]
fn log_body_offsets_match_core_structs() {
    // QU_TRANSFER: source(32) destination(32) amount(8)
    let mut body = vec![0u8; 72];
    body[0..32].copy_from_slice(&[1; 32]);
    body[32..64].copy_from_slice(&[2; 32]);
    body[64..72].copy_from_slice(&(-7i64).to_le_bytes());
    assert_eq!(
        decode_body(QU_TRANSFER, &body),
        EventBody::QuTransfer {
            from: [1; 32],
            to: [2; 32],
            amount: -7,
        }
    );

    // ASSET_ISSUANCE: issuer(32) shares(8) contract(8) name(7) decimals(1) unit(7)
    let mut body = vec![0u8; 63];
    body[0..32].copy_from_slice(&[3; 32]);
    body[32..40].copy_from_slice(&100i64.to_le_bytes());
    body[40..48].copy_from_slice(&4i64.to_le_bytes());
    body[48..55].copy_from_slice(b"MYTKN\0\0");
    body[55] = 8;
    body[56..63].copy_from_slice(&[0, 0, 0, 0, 0, 0, 0]);
    match decode_body(ASSET_ISSUANCE, &body) {
        EventBody::AssetIssuance { asset, shares } => {
            assert_eq!(asset.issuer, [3; 32]);
            assert_eq!(asset.managing_contract, 4);
            assert_eq!(asset.name, "MYTKN");
            assert_eq!(asset.decimals, 8);
            assert_eq!(asset.unit, "0000000");
            assert_eq!(shares, 100);
        }
        other => panic!("unexpected {other:?}"),
    }

    // ASSET_OWNERSHIP_CHANGE: source, dest, issuer, shares, contract, name, decimals, unit
    for log_type in [ASSET_OWNERSHIP_CHANGE, ASSET_POSSESSION_CHANGE] {
        let mut body = vec![0u8; 127];
        body[0..32].copy_from_slice(&[10; 32]);
        body[32..64].copy_from_slice(&[11; 32]);
        body[64..96].copy_from_slice(&[12; 32]);
        body[96..104].copy_from_slice(&55i64.to_le_bytes());
        body[104..112].copy_from_slice(&6i64.to_le_bytes());
        body[112..119].copy_from_slice(b"ASSET\0\0");
        body[119] = 2;
        body[120..127].copy_from_slice(&[1, 0, 0, 0, 0, 0, 0]);
        let event = decode_body(log_type, &body);
        let asset = match &event {
            EventBody::AssetOwnershipChange { asset, shares, .. }
            | EventBody::AssetPossessionChange { asset, shares, .. } => {
                assert_eq!(*shares, 55);
                asset
            }
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(asset.issuer, [12; 32]);
        assert_eq!(asset.managing_contract, 6);
        assert_eq!(asset.name, "ASSET");
        assert_eq!(asset.decimals, 2);
        assert_eq!(asset.unit, "1000000");
    }

    // BURNING: source(32) amount(8) contractIndex(4)
    let mut body = vec![0u8; 44];
    body[0..32].copy_from_slice(&[13; 32]);
    body[32..40].copy_from_slice(&9i64.to_le_bytes());
    body[40..44].copy_from_slice(&17u32.to_le_bytes());
    assert_eq!(
        decode_body(BURNING, &body),
        EventBody::Burning {
            from: [13; 32],
            amount: 9,
            contract_index: 17,
        }
    );

    // DUST_BURNING: count(2) then (publicKey(32), amount(8)) records.
    let mut body = vec![0u8; 2 + 2 * 40];
    body[0..2].copy_from_slice(&2u16.to_le_bytes());
    body[2..34].copy_from_slice(&[20; 32]);
    body[34..42].copy_from_slice(&100i64.to_le_bytes());
    body[42..74].copy_from_slice(&[21; 32]);
    body[74..82].copy_from_slice(&200i64.to_le_bytes());
    assert_eq!(
        decode_body(DUST_BURNING, &body),
        EventBody::DustBurning {
            burns: vec![([20; 32], 100), ([21; 32], 200)],
        }
    );
}

#[test]
fn log_body_asset_management_and_contract_events() {
    // ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE: ownership, issuer, source, destination, shares, name
    let mut body = vec![0u8; 87];
    body[0..32].copy_from_slice(&[1; 32]);
    body[32..64].copy_from_slice(&[2; 32]);
    body[64..68].copy_from_slice(&3u32.to_le_bytes());
    body[68..72].copy_from_slice(&4u32.to_le_bytes());
    body[72..80].copy_from_slice(&5i64.to_le_bytes());
    body[80..87].copy_from_slice(b"TOKEN\0\0");
    match decode_body(ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE, &body) {
        EventBody::AssetOwnershipManagingContractChange {
            ownership,
            issuer,
            source_contract_index,
            destination_contract_index,
            shares,
            asset_name,
        } => {
            assert_eq!(ownership, [1; 32]);
            assert_eq!(issuer, [2; 32]);
            assert_eq!(source_contract_index, 3);
            assert_eq!(destination_contract_index, 4);
            assert_eq!(shares, 5);
            assert_eq!(asset_name, "TOKEN");
        }
        other => panic!("unexpected {other:?}"),
    }

    // ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE: possession, ownership, issuer, source, destination, shares, name
    let mut body = vec![0u8; 119];
    body[0..32].copy_from_slice(&[1; 32]);
    body[32..64].copy_from_slice(&[2; 32]);
    body[64..96].copy_from_slice(&[3; 32]);
    body[96..100].copy_from_slice(&4u32.to_le_bytes());
    body[100..104].copy_from_slice(&5u32.to_le_bytes());
    body[104..112].copy_from_slice(&6i64.to_le_bytes());
    body[112..119].copy_from_slice(b"TOKEN\0\0");
    match decode_body(ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE, &body) {
        EventBody::AssetPossessionManagingContractChange {
            possession,
            ownership,
            issuer,
            source_contract_index,
            destination_contract_index,
            shares,
            asset_name,
        } => {
            assert_eq!(possession, [1; 32]);
            assert_eq!(ownership, [2; 32]);
            assert_eq!(issuer, [3; 32]);
            assert_eq!(source_contract_index, 4);
            assert_eq!(destination_contract_index, 5);
            assert_eq!(shares, 6);
            assert_eq!(asset_name, "TOKEN");
        }
        other => panic!("unexpected {other:?}"),
    }

    // CONTRACT_RESERVE_DEDUCTION
    let mut body = vec![0u8; 24];
    body[0..8].copy_from_slice(&77u64.to_le_bytes());
    body[8..16].copy_from_slice(&(-88i64).to_le_bytes());
    body[16..20].copy_from_slice(&9u32.to_le_bytes());
    assert_eq!(
        decode_body(CONTRACT_RESERVE_DEDUCTION, &body),
        EventBody::ContractReserveDeduction {
            deducted_amount: 77,
            remaining_amount: -88,
            contract_index: 9,
        }
    );

    // Contract error/warning/info/debug share the same header.
    for log_type in [
        CONTRACT_ERROR_MESSAGE,
        CONTRACT_WARNING_MESSAGE,
        CONTRACT_INFORMATION_MESSAGE,
        CONTRACT_DEBUG_MESSAGE,
    ] {
        let body = [1, 0, 0, 0, 2, 0, 0, 0, 0xAA, 0xBB];
        assert_eq!(
            decode_body(log_type, &body),
            EventBody::ContractMessage {
                contract_index: 1,
                message_type: 2,
                data: vec![0xAA, 0xBB],
            }
        );
    }

    // CUSTOM_MESSAGE
    let mut body = CUSTOM_OP_END_EPOCH.to_le_bytes().to_vec();
    body.extend_from_slice(&[1, 2, 3]);
    assert_eq!(
        decode_body(CUSTOM_MESSAGE, &body),
        EventBody::Custom {
            op_code: CUSTOM_OP_END_EPOCH,
            data: vec![1, 2, 3],
        }
    );
}

#[test]
fn log_body_oracle_oc_and_spectrum_events() {
    let mut body = vec![0u8; 224];
    body[0..8].copy_from_slice(&1u64.to_le_bytes());
    body[8..16].copy_from_slice(&2u64.to_le_bytes());
    body[16..24].copy_from_slice(&3u64.to_le_bytes());
    body[24..28].copy_from_slice(&4u32.to_le_bytes());
    body[28..32].copy_from_slice(&5u32.to_le_bytes());
    body[28 + 47 * 4..28 + 48 * 4].copy_from_slice(&6u32.to_le_bytes());
    match decode_body(SPECTRUM_STATS, &body) {
        EventBody::SpectrumStats {
            total_amount,
            dust_threshold_burn_all,
            dust_threshold_burn_half,
            number_of_entities,
            entity_category_populations,
        } => {
            assert_eq!(total_amount, 1);
            assert_eq!(dust_threshold_burn_all, 2);
            assert_eq!(dust_threshold_burn_half, 3);
            assert_eq!(number_of_entities, 4);
            assert_eq!(entity_category_populations.len(), 48);
            assert_eq!(entity_category_populations[0], 5);
            assert_eq!(entity_category_populations[47], 6);
        }
        other => panic!("unexpected {other:?}"),
    }

    let mut body = vec![0u8; 46];
    body[0..32].copy_from_slice(&[9; 32]);
    body[32..40].copy_from_slice(&10i64.to_le_bytes());
    body[40..44].copy_from_slice(&11u32.to_le_bytes());
    body[44] = ORACLE_QUERY_TYPE_USER_QUERY;
    body[45] = logs::ORACLE_QUERY_STATUS_SUCCESS;
    assert_eq!(
        decode_body(ORACLE_QUERY_STATUS_CHANGE, &body),
        EventBody::OracleQueryStatusChange {
            querying_entity: [9; 32],
            query_id: 10,
            interface_index: 11,
            query_type: ORACLE_QUERY_TYPE_USER_QUERY,
            status: logs::ORACLE_QUERY_STATUS_SUCCESS,
        }
    );

    let mut body = vec![0u8; 24];
    body[0..4].copy_from_slice(&12i32.to_le_bytes());
    body[4..8].copy_from_slice(&13u32.to_le_bytes());
    body[8..12].copy_from_slice(&14u32.to_le_bytes());
    body[12..16].copy_from_slice(&15u32.to_le_bytes());
    body[16..24].copy_from_slice(&16u64.to_le_bytes());
    assert_eq!(
        decode_body(ORACLE_SUBSCRIBER_MESSAGE, &body),
        EventBody::OracleSubscriberMessage {
            subscription_id: 12,
            interface_index: 13,
            contract_index: 14,
            period_in_milliseconds: 15,
            first_query_date_and_time: 16,
        }
    );

    let mut body = vec![0u8; 17];
    body[0..8].copy_from_slice(&17i64.to_le_bytes());
    body[8..12].copy_from_slice(&18u32.to_le_bytes());
    body[12..16].copy_from_slice(&19u32.to_le_bytes());
    body[16] = logs::OC_INVOCATION_STATUS_AUTHORIZED;
    assert_eq!(
        decode_body(OC_INVOCATION_STATUS_CHANGE, &body),
        EventBody::OcInvocationStatusChange {
            invocation_id: 17,
            contract_index: 18,
            interface_index: 19,
            status: logs::OC_INVOCATION_STATUS_AUTHORIZED,
        }
    );
}

#[test]
fn short_log_bodies_never_panic_and_keep_raw_bytes() {
    for log_type in [
        QU_TRANSFER,
        ASSET_ISSUANCE,
        ASSET_OWNERSHIP_CHANGE,
        ASSET_POSSESSION_CHANGE,
        ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE,
        ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE,
        BURNING,
        SPECTRUM_STATS,
        CONTRACT_RESERVE_DEDUCTION,
        ORACLE_QUERY_STATUS_CHANGE,
        ORACLE_SUBSCRIBER_MESSAGE,
        OC_INVOCATION_STATUS_CHANGE,
        CUSTOM_MESSAGE,
    ] {
        let body = vec![0xAB; 3];
        match decode_body(log_type, &body) {
            EventBody::Unknown { raw } => assert_eq!(raw, body),
            other => panic!("short body for type {log_type} did not stay raw: {other:?}"),
        }
    }

    // A dust-burning count larger than the body must not index out of bounds.
    let body = [0xFF, 0xFF, 0x01, 0x02];
    assert_eq!(
        decode_body(DUST_BURNING, &body),
        EventBody::Unknown { raw: body.to_vec() }
    );

    // Truncated log framing is an error rather than a panic.
    let mut record = log_record(QU_TRANSFER, &[0u8; 72]);
    record.truncate(record.len() - 1);
    assert!(LogEvent::decode(&record).is_err());
}

#[test]
fn zero_length_log_bodies_and_decode_many() {
    let empty = log_record(CONTRACT_INFORMATION_MESSAGE, &[]);
    let event = LogEvent::decode(&empty).unwrap();
    assert_eq!(event.log_type, CONTRACT_INFORMATION_MESSAGE);
    assert!(event.body.is_empty());
    assert!(matches!(event.body(), EventBody::Unknown { raw } if raw.is_empty()));

    let first = log_record(QU_TRANSFER, &[0u8; 72]);
    let second = log_record(BURNING, &[0u8; 44]);
    let mut stream = first.clone();
    stream.extend_from_slice(&second);
    let events = LogEvent::decode_many(&stream).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].log_type, QU_TRANSFER);
    assert_eq!(events[1].log_type, BURNING);

    // A trailing partial record is an error, not a panic.
    stream.extend_from_slice(&[0u8; 10]);
    assert!(LogEvent::decode_many(&stream).is_err());
}

#[test]
fn type_names_cover_core_event_types() {
    let cases = [
        (QU_TRANSFER, "qu_transfer"),
        (ASSET_ISSUANCE, "asset_issuance"),
        (ASSET_OWNERSHIP_CHANGE, "asset_ownership_change"),
        (ASSET_POSSESSION_CHANGE, "asset_possession_change"),
        (BURNING, "burning"),
        (DUST_BURNING, "dust_burning"),
        (SPECTRUM_STATS, "spectrum_stats"),
        (
            ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE,
            "asset_ownership_managing_contract_change",
        ),
        (
            ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE,
            "asset_possession_managing_contract_change",
        ),
        (CONTRACT_RESERVE_DEDUCTION, "contract_reserve_deduction"),
        (ORACLE_QUERY_STATUS_CHANGE, "oracle_query_status_change"),
        (ORACLE_SUBSCRIBER_MESSAGE, "oracle_subscriber_message"),
        (OC_INVOCATION_STATUS_CHANGE, "oc_invocation_status_change"),
        (CUSTOM_MESSAGE, "custom_message"),
    ];
    for (log_type, name) in cases {
        assert_eq!(logs::type_name(log_type), name);
    }
}

#[test]
fn golden_peer_fixtures_decode() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures");
    let cases = [
        ("current_tick_info.bin", MessageType::RespondCurrentTickInfo),
        ("system_info.bin", MessageType::RespondSystemInfo),
        ("computors.bin", MessageType::BroadcastComputors),
    ];
    for (file, expected) in cases {
        let path = root.join(file);
        let Ok(bytes) = std::fs::read(&path) else {
            eprintln!("skipping {file}: not captured");
            continue;
        };
        let packet = Packet::decode(&bytes).expect("fixture packet decodes");
        assert_eq!(packet.kind, expected);
        assert_eq!(packet.dejavu, Header::decode(&bytes).unwrap().dejavu);
        match packet.kind {
            MessageType::RespondCurrentTickInfo => {
                let info = CurrentTickInfo::decode(&packet.payload).unwrap();
                assert_eq!(info.encode().as_slice(), packet.payload.as_slice());
            }
            MessageType::RespondSystemInfo => {
                let info = SystemInfo::decode(&packet.payload).unwrap();
                assert_eq!(info.encode().as_slice(), packet.payload.as_slice());
            }
            MessageType::BroadcastComputors => {
                let computors = Computors::decode(&packet.payload).unwrap();
                assert_eq!(computors.encode(), packet.payload);
            }
            other => panic!("unexpected fixture type {other:?}"),
        }
    }
}

fn live_fixture(name: &str) -> Option<Vec<u8>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(name);
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(_) => {
            eprintln!("skipping {name}: not captured");
            None
        }
    }
}

#[test]
fn golden_live_tick_packets_round_trip() {
    let Some(bytes) = live_fixture("tick_data.bin") else {
        return;
    };
    let packet = Packet::decode(&bytes).unwrap();
    assert_eq!(packet.kind, MessageType::BroadcastFutureTickData);
    assert_eq!(packet.payload.len(), TICK_DATA_SIZE);
    let tick_data = TickData::decode(&packet.payload).unwrap();
    assert_eq!(tick_data.encode(), packet.payload);
    assert_eq!(tick_data.epoch, 240);
    assert_eq!(tick_data.tick, 91_916_246);

    let Some(bytes) = live_fixture("quorum_tick.bin") else {
        return;
    };
    let packet = Packet::decode(&bytes).unwrap();
    assert_eq!(packet.kind, MessageType::BroadcastTick);
    let vote = Tick::decode(&packet.payload).unwrap();
    assert_eq!(vote.encode().as_slice(), packet.payload.as_slice());
    assert_eq!(vote.epoch, 240);
    assert_eq!(vote.tick, 91_916_246);

    let Some(bytes) = live_fixture("tick_transactions.bin") else {
        return;
    };
    let packet = Packet::decode(&bytes).unwrap();
    assert_eq!(packet.kind, MessageType::BroadcastTransaction);
    let transaction = Transaction::decode(&packet.payload).unwrap();
    assert_eq!(transaction.encode(), packet.payload);
    assert_eq!(transaction.tick, 91_916_246);
    assert_eq!(transaction.input_type, 12);
    assert_eq!(transaction.input.len(), 48);
}

#[test]
fn golden_live_query_packets_round_trip() {
    let Some(bytes) = live_fixture("entity.bin") else {
        return;
    };
    let packet = Packet::decode(&bytes).unwrap();
    assert_eq!(packet.kind, MessageType::RespondEntity);
    let entity = RespondEntity::decode(&packet.payload).unwrap();
    assert_eq!(entity.encode(), packet.payload);
    assert_eq!(entity.siblings.len(), SPECTRUM_DEPTH);

    let Some(bytes) = live_fixture("assets_issuance.bin") else {
        return;
    };
    let packet = Packet::decode(&bytes).unwrap();
    assert_eq!(packet.kind, MessageType::RespondAssets);
    let asset = RespondAssetsWithSiblings::decode(&packet.payload).unwrap();
    assert_eq!(asset.encode(), packet.payload);
    assert!(matches!(
        asset.asset.kind,
        AssetRecordKind::Issuance(_) | AssetRecordKind::Empty
    ));

    let Some(bytes) = live_fixture("contract_ipo.bin") else {
        return;
    };
    let packet = Packet::decode(&bytes).unwrap();
    assert_eq!(packet.kind, MessageType::RespondContractIpo);
    let ipo = RespondContractIpo::decode(&packet.payload).unwrap();
    assert_eq!(ipo.encode(), packet.payload);
    assert_eq!(ipo.public_keys.len(), COMPUTOR_COUNT);
    assert_eq!(ipo.prices.len(), COMPUTOR_COUNT);

    let Some(bytes) = live_fixture("contract_function_empty.bin") else {
        return;
    };
    let packet = Packet::decode(&bytes).unwrap();
    assert_eq!(packet.kind, MessageType::RespondContractFunction);
    assert_eq!(
        RespondContractFunction::decode(&packet.payload)
            .unwrap()
            .data,
        Vec::<u8>::new()
    );

    let Some(bytes) = live_fixture("exchange_public_peers.bin") else {
        return;
    };
    let packet = Packet::decode(&bytes).unwrap();
    assert_eq!(packet.kind, MessageType::ExchangePublicPeers);
    assert_eq!(packet.payload.len(), 16);
    assert!(!PeerAddress::decode(&packet.payload).unwrap().is_empty());
}

#[test]
fn golden_live_logs_decode_and_reencode() {
    let Some(bytes) = live_fixture("logs.bin") else {
        return;
    };
    let packet = Packet::decode(&bytes).unwrap();
    assert_eq!(packet.kind, MessageType::RespondLog);
    let events = LogEvent::decode_many(&packet.payload).unwrap();
    assert_eq!(events.len(), 11);
    assert_eq!(events[0].log_type, CUSTOM_MESSAGE);
    assert_eq!(events[0].log_id, 0);
    assert_eq!(events[1].log_type, BURNING);
    assert_eq!(events[5].log_type, ASSET_OWNERSHIP_CHANGE);
    assert_eq!(events[6].log_type, ASSET_POSSESSION_CHANGE);

    let mut reencoded = Vec::new();
    for event in &events {
        assert_eq!(event.encoded_len(), event.encode().len());
        reencoded.extend_from_slice(&event.encode());
    }
    assert_eq!(reencoded, packet.payload);

    // Every live body must decode without a panic; asset changes must hit the
    // fixed 127-byte layout from core/logging/logging.h.
    for event in &events {
        let body = event.body();
        if event.log_type == ASSET_OWNERSHIP_CHANGE || event.log_type == ASSET_POSSESSION_CHANGE {
            assert!(matches!(
                body,
                EventBody::AssetOwnershipChange { .. } | EventBody::AssetPossessionChange { .. }
            ));
        }
    }
}
