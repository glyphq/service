//! Contract tests for the peer-backed wallet, holder, contract, and
//! transaction routes.
//!
//! Each test starts a local fake Qubic peer that speaks the wire protocol and
//! a mock store, then exercises the real router. This covers the success paths
//! that the no-peer contract tests cannot reach.

use std::sync::Arc;
use std::time::Duration;

use api::{AppState, router};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode, header};
use node::{NodePool, PeerConfig};
use qubic::messages::{
    ASSET_REQUEST_ANY_OWNER, ASSET_REQUEST_ANY_OWNERSHIP_MANAGING_CONTRACT,
    ASSET_REQUEST_GET_SIBLINGS, ASSET_REQUEST_OWNERSHIP, ASSETS_DEPTH, AssetIssuanceRecord,
    AssetOwnershipRecord, AssetPossessionRecord, AssetRecord, AssetRecordKind, CurrentTickInfo,
    EntityRecord, MAX_INPUT_SIZE, ORACLE_QUERY_STATUS_PENDING,
    ORACLE_REQUEST_ACTIVE_CONTRACT_SUBSCRIPTIONS, ORACLE_REQUEST_ACTIVE_SUBSCRIPTIONS,
    ORACLE_REQUEST_PENDING_QUERY_IDS, ORACLE_REQUEST_QUERY_AND_RESPONSE,
    ORACLE_REQUEST_QUERY_STATISTICS, ORACLE_REQUEST_REVENUE_POINTS, ORACLE_REQUEST_SUBSCRIPTION,
    OracleQueryMetadata, OracleQueryStatistics, OracleSubscriber, OracleSubscription,
    PriceOracleQuery, RequestAssets, RequestOracleData, RespondAssets, RespondAssetsWithSiblings,
    RespondContractFunction, RespondEntity, RespondIssuedAssets, RespondOwnedAssets,
    RespondPossessedAssets, RespondRevenueData, RespondTxStatus, SPECTRUM_DEPTH, SystemInfo,
    Transaction,
};
use qubic::wire::{HEADER_SIZE, MessageType, Packet};
use serde_json::{Value, json};
use store::{
    ActivityBucket, AssetRow, EpochRow, EventFilter, EventRow, EventTypeCount, IdentityRow,
    IngestCursor, NetworkStats, Result as StoreResult, Store, TickBatch, TickRow,
    TransactionFilter, TransactionRow, WriteOutcome,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tower::ServiceExt;

const TICK: u32 = 1000;
const WALLET_KEY: [u8; 32] = [0x11; 32];
const MISSING_KEY: [u8; 32] = [0; 32];
const ISSUER: [u8; 32] = [1; 32];
const OWNER: [u8; 32] = [2; 32];
const POSSESSOR: [u8; 32] = [3; 32];
const OTHER_OWNER: [u8; 32] = [4; 32];
const THIRD_OWNER: [u8; 32] = [5; 32];
const ERROR_DIGEST: [u8; 32] = [0xEE; 32];

fn identity(key: &[u8; 32]) -> String {
    qubic::PublicKey(*key).to_identity()
}

fn currency_id(name: &str) -> [u8; 32] {
    let mut id = [0u8; 32];
    id[..name.len()].copy_from_slice(name.as_bytes());
    id
}

fn fixture_transaction() -> Transaction {
    Transaction::decode(include_bytes!("../../node/fixtures/tx-91916364-1.bin"))
        .expect("fixture transaction decodes")
}

fn error_hash() -> String {
    qubic::encode_tx_hash(&ERROR_DIGEST)
}

fn indexed_hash() -> String {
    qubic::encode_tx_hash(&WALLET_KEY)
}

// ---- mock store ---------------------------------------------------------

struct MockStore {
    transactions: Vec<TransactionRow>,
    identity_rows: Vec<IdentityRow>,
}

impl MockStore {
    fn new() -> Self {
        let wallet_identity = identity(&WALLET_KEY);
        Self {
            transactions: vec![TransactionRow {
                epoch: 240,
                tick: 7,
                tx_index: 0,
                hash: indexed_hash(),
                source: wallet_identity,
                destination: identity(&OTHER_OWNER),
                amount: 42,
                input_type: 0,
                input_data: String::new(),
                status: "success".into(),
                timestamp: chrono::DateTime::from_timestamp(1_700_000_000, 0)
                    .expect("valid timestamp"),
            }],
            identity_rows: Vec::new(),
        }
    }

    fn with_indexed_transaction(mut self, hash: &str, tick: u32) -> Self {
        self.transactions.push(TransactionRow {
            epoch: 240,
            tick,
            tx_index: 1,
            hash: hash.to_owned(),
            source: identity(&WALLET_KEY),
            destination: identity(&OTHER_OWNER),
            amount: 7,
            input_type: 0,
            input_data: String::new(),
            status: "success".into(),
            timestamp: chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp"),
        });
        self
    }

    fn with_identity(mut self, identity: &str) -> Self {
        self.identity_rows.push(IdentityRow {
            identity: identity.to_owned(),
            public_key: "0x00".into(),
            balance: 1,
            incoming_amount: 1,
            outgoing_amount: 0,
            transfer_count: 1,
            first_seen_tick: 5,
            last_seen_tick: 7,
        });
        self
    }
}

#[async_trait]
impl Store for MockStore {
    async fn init(&self) -> StoreResult<()> {
        Ok(())
    }
    async fn drop_database(&self) -> StoreResult<()> {
        Ok(())
    }
    async fn head(&self) -> StoreResult<Option<TickRow>> {
        Ok(None)
    }
    async fn tick(&self, _tick: u32) -> StoreResult<Option<TickRow>> {
        Ok(None)
    }
    async fn transactions_for_tick(
        &self,
        _tick: u32,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        Ok(Vec::new())
    }
    async fn transaction(&self, hash: &str) -> StoreResult<Option<TransactionRow>> {
        Ok(self
            .transactions
            .iter()
            .find(|row| row.hash == hash)
            .cloned())
    }
    async fn transactions_for_identity(
        &self,
        identity: &str,
        limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        Ok(self
            .transactions
            .iter()
            .filter(|row| row.source == identity || row.destination == identity)
            .take(limit as usize)
            .cloned()
            .collect())
    }
    async fn recent_events(&self, _limit: u32, _offset: u64) -> StoreResult<Vec<EventRow>> {
        Ok(Vec::new())
    }
    async fn recent_transactions(
        &self,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        Ok(Vec::new())
    }
    async fn events_for_tick(
        &self,
        _tick: u32,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<EventRow>> {
        Ok(Vec::new())
    }
    async fn identity(&self, identity: &str) -> StoreResult<Option<IdentityRow>> {
        Ok(self
            .identity_rows
            .iter()
            .find(|row| row.identity == identity)
            .cloned())
    }
    async fn stats(&self) -> StoreResult<Value> {
        Ok(json!({}))
    }
    async fn ticks(
        &self,
        _epoch: Option<u16>,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<TickRow>> {
        Ok(Vec::new())
    }
    async fn epochs(&self, _limit: u32, _offset: u64) -> StoreResult<Vec<EpochRow>> {
        Ok(Vec::new())
    }
    async fn epoch(&self, _epoch: u16) -> StoreResult<Option<EpochRow>> {
        Ok(None)
    }
    async fn identities(&self, _limit: u32, _offset: u64) -> StoreResult<Vec<IdentityRow>> {
        Ok(Vec::new())
    }
    async fn assets(
        &self,
        _issuer: Option<&str>,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<AssetRow>> {
        Ok(Vec::new())
    }
    async fn asset(&self, _issuer: &str, _name: &str) -> StoreResult<Option<AssetRow>> {
        Ok(None)
    }
    async fn transactions_page(
        &self,
        _filter: &TransactionFilter,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        Ok(Vec::new())
    }
    async fn events_page(
        &self,
        _filter: &EventFilter,
        _limit: u32,
        _offset: u64,
    ) -> StoreResult<Vec<EventRow>> {
        Ok(Vec::new())
    }
    async fn event_type_counts(&self, _limit: u32) -> StoreResult<Vec<EventTypeCount>> {
        Ok(Vec::new())
    }
    async fn activity_analytics(
        &self,
        _interval_seconds: u32,
        _limit: u32,
    ) -> StoreResult<Vec<ActivityBucket>> {
        Ok(Vec::new())
    }
    async fn network_stats(&self) -> StoreResult<NetworkStats> {
        Ok(NetworkStats {
            head: None,
            epochs: 0,
            ticks: 0,
            transactions: 0,
            events: 0,
            identities: 0,
            latest_timestamp: None,
            ingest: IngestCursor::default(),
        })
    }
    async fn write_tick(&self, _batch: &TickBatch) -> StoreResult<WriteOutcome> {
        unreachable!()
    }
    async fn cursor(&self, _key: &str) -> StoreResult<Option<String>> {
        Ok(None)
    }
    async fn set_cursor(&self, _key: &str, _value: &str) -> StoreResult<()> {
        Ok(())
    }
    async fn ingest_cursor(&self) -> StoreResult<IngestCursor> {
        Ok(IngestCursor::default())
    }
    async fn set_ingest_cursor(&self, _cursor: &IngestCursor) -> StoreResult<()> {
        Ok(())
    }
}

// ---- mock peer ----------------------------------------------------------

fn issuance_record(key: u8, name: &[u8; 7]) -> AssetRecord {
    AssetRecord {
        public_key: [key; 32],
        kind: AssetRecordKind::Issuance(AssetIssuanceRecord {
            public_key: [key; 32],
            name: *name,
            decimals: 2,
            unit: *b"0000000",
        }),
    }
}

fn ownership_record(key: [u8; 32], shares: i64) -> AssetRecord {
    AssetRecord {
        public_key: key,
        kind: AssetRecordKind::Ownership(AssetOwnershipRecord {
            public_key: key,
            managing_contract_index: 1,
            issuance_index: 10,
            shares,
        }),
    }
}

fn possession_record(key: [u8; 32], shares: i64) -> AssetRecord {
    AssetRecord {
        public_key: key,
        kind: AssetRecordKind::Possession(AssetPossessionRecord {
            public_key: key,
            managing_contract_index: 2,
            ownership_index: 5,
            shares,
        }),
    }
}

fn siblings() -> Vec<[u8; 32]> {
    vec![[0u8; 32]; ASSETS_DEPTH]
}

async fn read_request(stream: &mut TcpStream) -> Option<(MessageType, u32, Vec<u8>)> {
    let mut header = [0u8; HEADER_SIZE];
    stream.read_exact(&mut header).await.ok()?;
    let header = qubic::Header::decode(&header).ok()?;
    let mut payload = vec![0u8; header.size as usize - HEADER_SIZE];
    stream.read_exact(&mut payload).await.ok()?;
    Some((header.kind, header.dejavu, payload))
}

async fn send(stream: &mut TcpStream, kind: MessageType, dejavu: u32, payload: Vec<u8>) {
    stream
        .write_all(&Packet::new(kind, dejavu, payload).encode())
        .await
        .expect("mock peer write");
    stream.flush().await.expect("mock peer flush");
}

async fn send_oracle(stream: &mut TcpStream, dejavu: u32, res_type: u32, data: Vec<u8>) {
    let mut payload = res_type.to_le_bytes().to_vec();
    payload.extend_from_slice(&data);
    send(stream, MessageType::RespondOracleData, dejavu, payload).await;
}

async fn serve(stream: TcpStream) {
    let mut stream = stream;
    send(
        &mut stream,
        MessageType::ExchangePublicPeers,
        77,
        vec![0u8; 16],
    )
    .await;
    while let Some((kind, dejavu, payload)) = read_request(&mut stream).await {
        match kind {
            MessageType::ExchangePublicPeers => {}
            MessageType::RequestCurrentTickInfo => {
                let info = CurrentTickInfo {
                    tick_duration: 1000,
                    epoch: 240,
                    tick: TICK,
                    aligned_votes: 451,
                    misaligned_votes: 0,
                    initial_tick: 900,
                };
                send(
                    &mut stream,
                    MessageType::RespondCurrentTickInfo,
                    dejavu,
                    info.encode().to_vec(),
                )
                .await;
            }
            MessageType::RequestEntity => {
                let public_key: [u8; 32] = payload.try_into().expect("entity key");
                let (entity, spectrum_index) = if public_key == MISSING_KEY {
                    (
                        EntityRecord {
                            public_key,
                            ..EntityRecord::default()
                        },
                        -1,
                    )
                } else {
                    (
                        EntityRecord {
                            public_key,
                            incoming_amount: 500,
                            outgoing_amount: 250,
                            number_of_incoming_transfers: 3,
                            number_of_outgoing_transfers: 2,
                            latest_incoming_transfer_tick: TICK,
                            latest_outgoing_transfer_tick: TICK - 1,
                        },
                        42,
                    )
                };
                let response = RespondEntity {
                    entity,
                    tick: TICK,
                    spectrum_index,
                    siblings: vec![[0u8; 32]; SPECTRUM_DEPTH],
                };
                send(
                    &mut stream,
                    MessageType::RespondEntity,
                    dejavu,
                    response.encode(),
                )
                .await;
            }
            MessageType::RequestIssuedAssets => {
                let public_key: [u8; 32] = payload.try_into().expect("issued key");
                if public_key == WALLET_KEY {
                    for (name, universe_index) in [(b"QX\0\0\0\0\0", 10u32), (b"QY\0\0\0\0\0", 11)]
                    {
                        let response = RespondIssuedAssets {
                            asset: issuance_record(ISSUER[0], name),
                            tick: TICK,
                            universe_index,
                            siblings: siblings(),
                        };
                        send(
                            &mut stream,
                            MessageType::RespondIssuedAssets,
                            dejavu,
                            response.encode(),
                        )
                        .await;
                    }
                }
                send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
            }
            MessageType::RequestOwnedAssets => {
                let public_key: [u8; 32] = payload.try_into().expect("owned key");
                if public_key == WALLET_KEY {
                    for (index, universe_index) in [20u32, 21].into_iter().enumerate() {
                        let response = RespondOwnedAssets {
                            asset: ownership_record(OWNER, 100 + index as i64),
                            issuance_asset: issuance_record(ISSUER[0], b"QX\0\0\0\0\0"),
                            tick: TICK,
                            universe_index,
                            siblings: siblings(),
                        };
                        send(
                            &mut stream,
                            MessageType::RespondOwnedAssets,
                            dejavu,
                            response.encode(),
                        )
                        .await;
                    }
                }
                send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
            }
            MessageType::RequestPossessedAssets => {
                let public_key: [u8; 32] = payload.try_into().expect("possessed key");
                if public_key == WALLET_KEY {
                    for (index, universe_index) in [30u32, 31].into_iter().enumerate() {
                        let response = RespondPossessedAssets {
                            asset: possession_record(POSSESSOR, 200 + index as i64),
                            ownership_asset: ownership_record(OWNER, 300),
                            issuance_asset: issuance_record(ISSUER[0], b"QX\0\0\0\0\0"),
                            tick: TICK,
                            universe_index,
                            siblings: siblings(),
                        };
                        send(
                            &mut stream,
                            MessageType::RespondPossessedAssets,
                            dejavu,
                            response.encode(),
                        )
                        .await;
                    }
                }
                send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
            }
            MessageType::RequestAssets => {
                let request = RequestAssets::decode(&payload).expect("asset request");
                // Only answer the exact filter the holders route should send.
                // In particular the asset name is packed as raw ASCII bytes
                // ("QX"), so an off-by-offset encoding requests nothing and
                // the holder assertions fail instead of silently passing.
                let matches = matches!(
                    request,
                    RequestAssets::ByFilter {
                        request_type,
                        flags,
                        issuer,
                        asset_name,
                        ..
                    } if request_type == ASSET_REQUEST_OWNERSHIP
                        && flags & ASSET_REQUEST_ANY_OWNER != 0
                        && flags & ASSET_REQUEST_ANY_OWNERSHIP_MANAGING_CONTRACT != 0
                        && issuer == ISSUER
                        && asset_name == u64::from_le_bytes(*b"QX\0\0\0\0\0\0")
                );
                let records = if matches {
                    vec![
                        ownership_record(OWNER, 100),
                        ownership_record(OWNER, 50),
                        ownership_record(OTHER_OWNER, 200),
                        ownership_record(THIRD_OWNER, 1),
                        issuance_record(ISSUER[0], b"QX\0\0\0\0\0"),
                    ]
                } else {
                    Vec::new()
                };
                for (universe_index, record) in records.into_iter().enumerate() {
                    let tick = TICK;
                    let universe_index = 40 + universe_index as u32;
                    let payload = if request.flags() & ASSET_REQUEST_GET_SIBLINGS != 0 {
                        RespondAssetsWithSiblings {
                            asset: record,
                            tick,
                            universe_index,
                            siblings: siblings(),
                        }
                        .encode()
                    } else {
                        RespondAssets {
                            asset: record,
                            tick,
                            universe_index,
                        }
                        .encode()
                    };
                    send(&mut stream, MessageType::RespondAssets, dejavu, payload).await;
                }
                send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
            }
            MessageType::RequestContractFunction => {
                let request =
                    qubic::messages::RequestContractFunction::decode(&payload).expect("contract");
                let mut output = request.input;
                output.reverse();
                if request.input_type == 0 {
                    output.clear();
                }
                send(
                    &mut stream,
                    MessageType::RespondContractFunction,
                    dejavu,
                    RespondContractFunction { data: output }.data,
                )
                .await;
            }
            MessageType::RequestTransactionInfo => {
                let digest: [u8; 32] = payload.try_into().expect("transaction digest");
                let transaction = fixture_transaction();
                if digest == transaction.hash() {
                    send(
                        &mut stream,
                        MessageType::BroadcastTransaction,
                        dejavu,
                        transaction.encode(),
                    )
                    .await;
                } else if digest == ERROR_DIGEST {
                    // Deliberately unexpected: the client must not treat this
                    // as a confirmed transaction.
                    send(
                        &mut stream,
                        MessageType::RespondCurrentTickInfo,
                        dejavu,
                        CurrentTickInfo {
                            tick_duration: 1000,
                            epoch: 240,
                            tick: TICK,
                            aligned_votes: 451,
                            misaligned_votes: 0,
                            initial_tick: 900,
                        }
                        .encode()
                        .to_vec(),
                    )
                    .await;
                } else {
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
            }
            MessageType::RequestTxStatus => {
                let tick = u32::from_le_bytes(payload.try_into().expect("tx status tick"));
                let current_tick = fixture_transaction().tick + 5;
                let status = RespondTxStatus {
                    current_tick,
                    tick,
                    digests: vec![fixture_transaction().hash(), [0x22; 32]],
                    money_flew: vec![true, false],
                };
                send(
                    &mut stream,
                    MessageType::RespondTxStatus,
                    dejavu,
                    status.encode(),
                )
                .await;
            }
            MessageType::RequestSystemInfo => {
                let info = SystemInfo {
                    version: 300,
                    epoch: 240,
                    tick: TICK,
                    initial_tick: 900,
                    latest_created_tick: TICK + 1,
                    initial_millisecond: 789,
                    initial_second: 56,
                    initial_minute: 34,
                    initial_hour: 12,
                    initial_day: 22,
                    initial_month: 11,
                    initial_year: 26,
                    number_of_entities: 1234,
                    number_of_transactions: 5678,
                    random_mining_seed: [7; 32],
                    solution_threshold: 100,
                    total_spectrum_amount: 1_000_000_000_000,
                    current_entity_balance_dust_threshold: 100,
                    target_tick_vote_signature: 451,
                    computor_packet_signature: 9,
                    solution_additional_threshold: 50,
                    reserve2: 0,
                    reserve3: 0,
                    reserve4: 0,
                };
                send(
                    &mut stream,
                    MessageType::RespondSystemInfo,
                    dejavu,
                    info.encode().to_vec(),
                )
                .await;
            }
            MessageType::RequestRevenueData => {
                let revenue = RespondRevenueData {
                    tick: TICK,
                    doge_k: 2,
                    ipc: 1_479_289_940,
                    tx_score: (0..676u64).collect(),
                    oracle_score: (0..676u64).map(|value| value % 32).collect(),
                    doge_score: vec![0; 676],
                };
                send(
                    &mut stream,
                    MessageType::RespondRevenueData,
                    dejavu,
                    revenue.encode(),
                )
                .await;
            }
            MessageType::RequestOracleData => {
                let request = RequestOracleData::decode(&payload).expect("oracle request");
                match request.req_type {
                    ORACLE_REQUEST_QUERY_STATISTICS => {
                        let stats = OracleQueryStatistics {
                            pending_count: 2,
                            successful_count: 10,
                            timeout_count: 1,
                            user_queries: 4,
                            contract_queries: 5,
                            subscription_queries: 6,
                            ..OracleQueryStatistics::default()
                        };
                        send_oracle(
                            &mut stream,
                            dejavu,
                            qubic::messages::ORACLE_RESPOND_QUERY_STATISTICS,
                            stats.encode().to_vec(),
                        )
                        .await;
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                    ORACLE_REQUEST_PENDING_QUERY_IDS => {
                        for id in [1001i64, 1002] {
                            send_oracle(
                                &mut stream,
                                dejavu,
                                qubic::messages::ORACLE_RESPOND_QUERY_IDS,
                                id.to_le_bytes().to_vec(),
                            )
                            .await;
                        }
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                    ORACLE_REQUEST_ACTIVE_SUBSCRIPTIONS => {
                        send_oracle(
                            &mut stream,
                            dejavu,
                            qubic::messages::ORACLE_RESPOND_SUBSCRIPTION_IDS,
                            7i32.to_le_bytes().to_vec(),
                        )
                        .await;
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                    ORACLE_REQUEST_QUERY_AND_RESPONSE => {
                        let query_id = request.tick_or_id;
                        let metadata = OracleQueryMetadata {
                            query_id,
                            query_type: 2,
                            status: ORACLE_QUERY_STATUS_PENDING,
                            status_flags: 0,
                            query_tick: TICK,
                            querying_entity: WALLET_KEY,
                            timeout: (2026u64 << 46)
                                | (11u64 << 42)
                                | (22u64 << 37)
                                | (12u64 << 32)
                                | (34u64 << 26)
                                | (56u64 << 20)
                                | (789u64 << 10),
                            interface_index: 0,
                            subscription_id: -1,
                            reveal_tick: 0,
                            total_commits: 0,
                            agreeing_commits: 0,
                        };
                        let query = PriceOracleQuery {
                            oracle: [0; 32],
                            timestamp: 1_700_000_000,
                            currency1: currency_id("QUBIC"),
                            currency2: currency_id("USDT"),
                        };
                        send_oracle(
                            &mut stream,
                            dejavu,
                            qubic::messages::ORACLE_RESPOND_QUERY_METADATA,
                            metadata.encode().to_vec(),
                        )
                        .await;
                        send_oracle(
                            &mut stream,
                            dejavu,
                            qubic::messages::ORACLE_RESPOND_QUERY_DATA,
                            query.encode().to_vec(),
                        )
                        .await;
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                    ORACLE_REQUEST_SUBSCRIPTION => {
                        let subscription = OracleSubscription {
                            subscription_id: 7,
                            interface_index: 0,
                            last_pending_query_id: 1001,
                            last_revealed_query_id: 1000,
                            generated_queries_count: 10,
                        };
                        let query = PriceOracleQuery {
                            oracle: [0; 32],
                            timestamp: 1_700_000_000,
                            currency1: currency_id("QUBIC"),
                            currency2: currency_id("USDT"),
                        };
                        let subscriber = OracleSubscriber {
                            subscription_id: 7,
                            contract_index: 13,
                            notification_period_minutes: 60,
                            next_query_timestamp: 0,
                        };
                        send_oracle(
                            &mut stream,
                            dejavu,
                            qubic::messages::ORACLE_RESPOND_SUBSCRIPTION,
                            subscription.encode(),
                        )
                        .await;
                        send_oracle(
                            &mut stream,
                            dejavu,
                            qubic::messages::ORACLE_RESPOND_QUERY_DATA,
                            query.encode().to_vec(),
                        )
                        .await;
                        send_oracle(
                            &mut stream,
                            dejavu,
                            qubic::messages::ORACLE_RESPOND_SUBSCRIBER,
                            subscriber.encode().to_vec(),
                        )
                        .await;
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                    ORACLE_REQUEST_REVENUE_POINTS => {
                        let mut data = Vec::with_capacity(676 * 8);
                        for point in 1..=676u64 {
                            data.extend_from_slice(&point.to_le_bytes());
                        }
                        send_oracle(
                            &mut stream,
                            dejavu,
                            qubic::messages::ORACLE_RESPOND_REVENUE_POINTS,
                            data,
                        )
                        .await;
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                    ORACLE_REQUEST_ACTIVE_CONTRACT_SUBSCRIPTIONS => {
                        for id in [7i32, 8] {
                            send_oracle(
                                &mut stream,
                                dejavu,
                                qubic::messages::ORACLE_RESPOND_SUBSCRIPTION_IDS,
                                id.to_le_bytes().to_vec(),
                            )
                            .await;
                        }
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                    _ => {
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                }
            }
            MessageType::BroadcastTransaction => {}
            _ => {}
        }
    }
}

async fn spawn_mock_peer() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind peer");
    let addr = listener.local_addr().expect("peer address");
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(serve(stream));
        }
    });
    addr
}

fn state_with_peer(store: Arc<dyn Store>, addr: std::net::SocketAddr) -> AppState {
    let config = PeerConfig::new(addr)
        .with_timeout(Duration::from_secs(2))
        .with_connect_timeout(Duration::from_secs(2))
        .with_retries(0);
    let mut state = AppState::new(store);
    state.peers = vec![config.clone()];
    state.pool = NodePool::new(vec![config]);
    state
}

async fn request(
    state: AppState,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value, HeaderMap) {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(value) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    let response = router(state)
        .oneshot(builder.body(body).expect("request"))
        .await
        .expect("router response");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body, headers)
}

async fn get(state: AppState, uri: &str) -> (StatusCode, Value, HeaderMap) {
    request(state, "GET", uri, None).await
}

async fn post(state: AppState, uri: &str, body: Value) -> (StatusCode, Value, HeaderMap) {
    request(state, "POST", uri, Some(body)).await
}

// ---- wallet and holder routes ------------------------------------------

#[tokio::test]
async fn wallet_overview_joins_peer_entity_assets_and_index() {
    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let (status, body, headers) =
        get(state, &format!("/v1/wallets/{}", identity(&WALLET_KEY))).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(body["data"]["identity"], identity(&WALLET_KEY));
    assert_eq!(
        body["data"]["public_key"],
        qubic::PublicKey(WALLET_KEY).to_hex()
    );
    assert_eq!(body["data"]["entity"]["spectrum_index"], 42);
    assert_eq!(body["data"]["entity"]["incoming_amount"], 500);
    assert_eq!(body["data"]["entity"]["outgoing_amount"], 250);
    assert_eq!(body["data"]["entity"]["balance"], 250);
    assert_eq!(
        body["data"]["assets"]["issued"].as_array().unwrap().len(),
        2
    );
    assert_eq!(body["data"]["assets"]["owned"].as_array().unwrap().len(), 2);
    assert_eq!(
        body["data"]["assets"]["possessed"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(body["data"]["assets"]["issued"][0]["name"], "QX");
    assert_eq!(body["data"]["assets"]["issued"][0]["decimals"], 2);
    assert_eq!(
        body["data"]["assets"]["owned"][0]["owner"],
        identity(&OWNER)
    );
    assert_eq!(body["data"]["assets"]["owned"][0]["shares"], 100);
    assert_eq!(
        body["data"]["assets"]["possessed"][0]["possessor"],
        identity(&POSSESSOR)
    );
    assert_eq!(body["data"]["recent"].as_array().unwrap().len(), 1);
    assert_eq!(body["meta"]["peer_tick"], TICK);
    assert_eq!(body["meta"]["warning"], Value::Null);
}

#[tokio::test]
async fn wallet_without_spectrum_entry_is_not_found_unless_indexed() {
    let peer = spawn_mock_peer().await;
    let missing = identity(&MISSING_KEY);

    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let (status, body, _) = get(state, &format!("/v1/wallets/{missing}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], "not_found");

    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new().with_identity(&missing)), peer);
    let (status, body, _) = get(state, &format!("/v1/wallets/{missing}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["entity"]["balance"], 0);
    assert!(
        body["meta"]["warning"]
            .as_str()
            .unwrap()
            .contains("spectrum")
    );
}

#[tokio::test]
async fn wallet_assets_filters_by_kind() {
    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let (status, body, headers) = get(
        state,
        &format!("/v1/wallets/{}/assets?kind=issued", identity(&WALLET_KEY)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(body["data"]["issued"].as_array().unwrap().len(), 2);
    assert_eq!(body["data"]["owned"].as_array().unwrap().len(), 0);
    assert_eq!(body["data"]["possessed"].as_array().unwrap().len(), 0);
    assert_eq!(body["meta"]["kind"], "issued");
    assert_eq!(body["meta"]["counts"]["issued"], 2);
}

#[tokio::test]
async fn wallet_snapshot_reports_peer_head_and_counts() {
    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let (status, body, headers) = get(
        state,
        &format!("/v1/live/wallets/{}", identity(&WALLET_KEY)),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(body["data"]["peer_tick"], TICK);
    assert_eq!(body["data"]["epoch"], 240);
    assert_eq!(body["data"]["counts"]["issued"], 2);
    assert_eq!(body["data"]["counts"]["owned"], 2);
    assert_eq!(body["data"]["counts"]["possessed"], 2);
    assert_eq!(body["meta"]["source"], "peer");
}

#[tokio::test]
async fn asset_holders_dedupes_ranks_and_truncates() {
    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let issuer = identity(&ISSUER);
    let (status, body, _) = get(state, &format!("/v1/assets/{issuer}/qx/holders?limit=2")).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    let holders = body["data"].as_array().unwrap();
    assert_eq!(holders.len(), 2);
    assert_eq!(holders[0]["identity"], identity(&OTHER_OWNER));
    assert_eq!(holders[0]["shares"], 200);
    assert_eq!(holders[1]["identity"], identity(&OWNER));
    assert_eq!(holders[1]["shares"], 150);
    assert_eq!(body["meta"]["name"], "QX");
    assert_eq!(body["meta"]["count"], 2);
    assert_eq!(body["meta"]["truncated"], true);
}

// ---- contract queries ---------------------------------------------------

#[tokio::test]
async fn contract_query_forwards_input_and_returns_hex() {
    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let (status, body, _) = post(
        state,
        "/v1/live/contracts/1/query",
        json!({ "input_type": 1, "input": "0Xdeadbeef" }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["contract_index"], 1);
    assert_eq!(body["data"]["input_type"], 1);
    assert_eq!(body["data"]["output"], "efbeadde");
    assert_eq!(body["data"]["bytes"], 4);
    assert_eq!(body["meta"]["empty"], false);
}

#[tokio::test]
async fn contract_query_defaults_and_rejects_oversized_input() {
    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let (status, body, _) = post(
        state,
        "/v1/live/contracts/1/query",
        json!({ "input": " 0x0102 " }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["input_type"], 0);
    assert_eq!(body["data"]["output"], "");
    assert_eq!(body["meta"]["empty"], true);

    let oversized = "00".repeat(MAX_INPUT_SIZE + 1);
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let (status, body, _) = post(
        state,
        "/v1/live/contracts/1/query",
        json!({ "input": oversized }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "bad_request");
}

#[tokio::test]
async fn contract_query_rejects_malformed_hex() {
    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let (status, body, _) = post(
        state,
        "/v1/live/contracts/1/query",
        json!({ "input": "not hex" }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "bad_request");
}

// ---- transaction routes -------------------------------------------------

#[tokio::test]
async fn network_transaction_reads_peer_and_sets_cache_policy() {
    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let transaction = fixture_transaction();
    let hash = qubic::encode_tx_hash(&transaction.hash());
    let (status, body, headers) = get(state, &format!("/v1/live/transactions/{hash}")).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        headers[header::CACHE_CONTROL],
        "public, max-age=2, stale-while-revalidate=5"
    );
    assert_eq!(body["data"]["hash"], hash);
    assert_eq!(
        body["data"]["source"],
        qubic::PublicKey(transaction.source).to_identity()
    );
    assert_eq!(body["data"]["amount"], transaction.amount);
    assert_eq!(body["data"]["tick"], transaction.tick);
    assert_eq!(body["meta"]["source"], "peer");
}

#[tokio::test]
async fn network_transaction_reports_unknown_hash() {
    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let hash = qubic::encode_tx_hash(&[0x12; 32]);
    let (status, body, _) = get(state, &format!("/v1/live/transactions/{hash}")).await;

    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], "not_found");
}

#[tokio::test]
async fn network_transaction_status_reports_money_flew() {
    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let transaction = fixture_transaction();
    let hash = qubic::encode_tx_hash(&transaction.hash());
    let (status, body, headers) = get(state, &format!("/v1/live/transactions/{hash}/status")).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        headers[header::CACHE_CONTROL],
        "public, max-age=2, stale-while-revalidate=5"
    );
    assert_eq!(body["data"]["confirmed"], true);
    assert_eq!(body["data"]["money_flew"], true);
    assert_eq!(body["data"]["execution"], "transferred");
    assert_eq!(body["data"]["current_tick"], transaction.tick + 5);
    assert_eq!(body["data"]["confirmations"], 5);
    assert_eq!(body["meta"]["status_source"], "tx-status");
}

#[tokio::test]
async fn tick_transaction_status_lists_every_entry() {
    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let (status, body, _) = get(state, &format!("/v1/live/ticks/{TICK}/transactions")).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["current_tick"], fixture_transaction().tick + 5);
    let entries = body["data"]["transactions"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["money_flew"], true);
    assert_eq!(
        entries[0]["hash"],
        qubic::encode_tx_hash(&fixture_transaction().hash())
    );
}

#[tokio::test]
async fn indexed_transaction_status_degrades_when_peer_errors() {
    let peer = spawn_mock_peer().await;
    let hash = error_hash();
    let store = MockStore::new().with_indexed_transaction(&hash, 7);
    let state = state_with_peer(Arc::new(store), peer);
    let (status, body, headers) = get(state, &format!("/v1/transactions/{hash}/status")).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(body["data"]["indexed"]["hash"], hash);
    assert_eq!(body["data"]["network"]["found"], false);
    assert_eq!(body["meta"]["source"], "index");
    assert!(body["meta"]["peer_error"].as_str().is_some());
}

#[tokio::test]
async fn transaction_status_unknown_to_both_is_not_found() {
    let peer = spawn_mock_peer().await;
    let state = state_with_peer(Arc::new(MockStore::new()), peer);
    let hash = qubic::encode_tx_hash(&[0x12; 32]);
    let (status, body, _) = get(state, &format!("/v1/transactions/{hash}/status")).await;

    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["code"], "not_found");
}

#[tokio::test]
async fn batch_counts_unique_hashes_and_collapses_duplicates() {
    let state = state_with_peer(Arc::new(MockStore::new()), spawn_mock_peer().await);
    let hashes: Vec<String> = (0..100u8)
        .map(|index| qubic::encode_tx_hash(&[index; 32]))
        .collect();
    let duplicated: Vec<String> = (0..150).map(|index| hashes[index % 100].clone()).collect();
    let (status, body, _) = post(
        state,
        "/v1/transactions/batch",
        json!({ "hashes": duplicated }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["meta"]["requested"], 100);
    // The mock store indexes the hash for `WALLET_KEY`, which is one of the
    // requested keys.
    assert_eq!(body["meta"]["found"], 1);
    assert_eq!(body["meta"]["missing"].as_array().unwrap().len(), 99);

    let state = state_with_peer(Arc::new(MockStore::new()), spawn_mock_peer().await);
    let too_many: Vec<String> = (0..=100u8)
        .map(|index| qubic::encode_tx_hash(&[index; 32]))
        .collect();
    let (status, body, _) = post(
        state,
        "/v1/transactions/batch",
        json!({ "hashes": too_many }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "bad_request");
}

#[tokio::test]
async fn network_system_revenue_and_peers_are_typed() {
    let state = state_with_peer(Arc::new(MockStore::new()), spawn_mock_peer().await);
    let (status, body, headers) = get(state.clone(), "/v1/network/system").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        headers[header::CACHE_CONTROL],
        "public, max-age=2, stale-while-revalidate=5"
    );
    assert_eq!(body["data"]["version"], 300);
    assert_eq!(body["data"]["epoch"], 240);
    assert_eq!(body["data"]["tick"], TICK);
    assert_eq!(body["data"]["initial_tick"], 900);
    assert_eq!(body["data"]["entity_count"], 1234);
    assert_eq!(body["data"]["started_at"], "2026-11-22T12:34:56.789Z");
    assert_eq!(body["meta"]["source"], "peer");

    let (status, body, _) = get(state.clone(), "/v1/network/revenue?limit=3&sort=tx_score").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["epoch"], 240);
    assert_eq!(body["data"]["computors"].as_array().unwrap().len(), 3);
    assert_eq!(body["data"]["computors"][0]["tx_score"], 675);
    assert!(body["data"]["totals"]["estimated_reward"].as_u64().unwrap() > 0);
    assert_eq!(body["meta"]["approximate"], true);

    let (status, body, _) = get(state, "/v1/network/peers").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["meta"]["configured"], 1);
    assert_eq!(body["data"][0]["status"], "healthy");
}

#[tokio::test]
async fn oracle_routes_expose_queries_subscriptions_and_revenue() {
    let state = state_with_peer(Arc::new(MockStore::new()), spawn_mock_peer().await);

    let (status, body, _) = get(state.clone(), "/v1/network/oracles").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["pending_queries"], 2);
    assert_eq!(body["data"]["active_subscriptions"], 1);
    assert_eq!(body["data"]["stats"]["pending_count"], 2);

    let (status, body, _) = get(state.clone(), "/v1/network/oracles/stats").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["successful_count"], 10);
    assert_eq!(body["data"]["user_queries"], 4);

    let (status, body, _) = get(
        state.clone(),
        "/v1/network/oracles/queries?status=pending&limit=2",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["data"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["query_id"], 1001);
    assert_eq!(rows[0]["status_name"], "pending");
    assert_eq!(rows[0]["type_name"], "user_query");
    assert_eq!(
        rows[0]["price"]["currency1"],
        identity(&currency_id("QUBIC"))
    );
    assert_eq!(
        rows[0]["price"]["currency2"],
        identity(&currency_id("USDT"))
    );

    let (status, body, _) = get(state.clone(), "/v1/network/oracles/queries/1001").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["query_id"], 1001);
    assert_eq!(body["data"]["timeout_at"], "2026-11-22T12:34:56.789Z");
    assert_eq!(body["data"]["interface_name"], "price");

    let (status, body, _) = get(state.clone(), "/v1/network/oracles/subscriptions?limit=1").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["data"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["subscription_id"], 7);
    assert_eq!(rows[0]["subscribers"][0]["contract_index"], 13);

    let (status, body, _) = get(state.clone(), "/v1/network/oracles/subscriptions/7").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["subscription_id"], 7);
    assert_eq!(body["data"]["generated_queries_count"], 10);

    let (status, body, _) = get(state, "/v1/network/oracles/revenue").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["total_points"], 228_826);
    assert_eq!(body["data"]["computors"].as_array().unwrap().len(), 676);
}
