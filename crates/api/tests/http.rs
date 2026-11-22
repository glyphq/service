use std::sync::Arc;

use api::{AppState, router};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::response::IntoResponse;
use chrono::{TimeZone, Utc};
use serde_json::Value;
use store::{
    ActivityBucket, AssetRow, AssetTransferRow, ComputorRow, CounterpartyRow, EpochRow,
    EventFilter, EventRow, EventTypeCount, FlowBucket, IdentityRow, IngestCursor, NetworkStats,
    QuorumEvidenceRow, Result as StoreResult, Store, TickBatch, TickRow, TransactionFilter,
    TransactionRow, WriteOutcome,
};
use tower::ServiceExt;

const IDENTITY: &str = "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK";
const OTHER: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
fn hash_for(tick: u32, index: u16) -> String {
    let mut key = [0u8; 32];
    key[..4].copy_from_slice(&tick.to_le_bytes());
    key[4] = index as u8;
    qubic::encode_tx_hash(&key)
}

fn timestamp() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 18, 12, 0, 0)
        .single()
        .expect("valid timestamp")
}

struct MockStore {
    ticks: Vec<TickRow>,
    transactions: Vec<TransactionRow>,
    events: Vec<EventRow>,
    identities: Vec<IdentityRow>,
    assets: Vec<AssetRow>,
    computors: Vec<ComputorRow>,
    evidence: Vec<QuorumEvidenceRow>,
}

impl MockStore {
    fn new() -> Self {
        let ticks = [7u32, 6, 5]
            .into_iter()
            .map(|tick| TickRow {
                epoch: if tick == 5 { 2 } else { 1 },
                tick,
                timestamp: timestamp(),
                computor_index: 3,
                transaction_count: 2,
                event_count: 2,
                verification: "data".into(),
            })
            .collect();
        let transactions = [7u32, 6, 5]
            .into_iter()
            .flat_map(|tick| {
                (0..2).map(move |index| TransactionRow {
                    epoch: if tick == 5 { 2 } else { 1 },
                    tick,
                    tx_index: index,
                    hash: hash_for(tick, index),
                    source: if index == 0 { IDENTITY } else { OTHER }.into(),
                    destination: if index == 0 { OTHER } else { IDENTITY }.into(),
                    amount: 42 + i64::from(tick),
                    input_type: 0,
                    input_data: String::new(),
                    status: "success".into(),
                    timestamp: timestamp(),
                })
            })
            .collect();
        let mut events: Vec<EventRow> = [7u32, 6, 5]
            .into_iter()
            .map(|tick| EventRow {
                epoch: if tick == 5 { 2 } else { 1 },
                tick,
                log_id: u64::from(tick) * 10,
                log_type: 1,
                type_name: "asset_issuance".into(),
                tx_hash: hash_for(tick, 0),
                tx_index: 0,
                body: r#"{"issuer":"BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK","name":"QX","decimals":0,"unit":"","managingContract":1,"shares":"1000"}"#.into(),
                log_digest: u64::from(tick),
                timestamp: timestamp(),
            })
            .collect();
        events.push(EventRow {
            epoch: 1,
            tick: 7,
            log_id: 71,
            log_type: 0,
            type_name: "qu_transfer".into(),
            tx_hash: hash_for(7, 0),
            tx_index: 0,
            body: format!(r#"{{"from":"{IDENTITY}","to":"{OTHER}","amount":"5"}}"#),
            log_digest: 71,
            timestamp: timestamp(),
        });
        events.push(EventRow {
            epoch: 1,
            tick: 7,
            log_id: 72,
            log_type: 2,
            type_name: "asset_ownership_change".into(),
            tx_hash: hash_for(7, 0),
            tx_index: 0,
            body: format!(
                r#"{{"from":"{IDENTITY}","to":"{OTHER}","issuer":"{IDENTITY}","name":"QX","managingContract":1,"shares":"10"}}"#
            ),
            log_digest: 72,
            timestamp: timestamp(),
        });
        let identities = vec![
            IdentityRow {
                identity: IDENTITY.into(),
                public_key: "0x00".into(),
                balance: 0,
                incoming_amount: 42,
                outgoing_amount: 42,
                transfer_count: 4,
                first_seen_tick: 5,
                last_seen_tick: 7,
            },
            IdentityRow {
                identity: OTHER.into(),
                public_key: "0x01".into(),
                balance: 10,
                incoming_amount: 10,
                outgoing_amount: 0,
                transfer_count: 1,
                first_seen_tick: 5,
                last_seen_tick: 6,
            },
        ];
        let assets = vec![AssetRow {
            issuer: IDENTITY.into(),
            name: "QX".into(),
            decimals: 0,
            unit: String::new(),
            managing_contract: 1,
            shares: 1000,
            issued_tick: 7,
            log_id: 70,
            timestamp: timestamp(),
        }];
        let computors = (0..3u16)
            .map(|index| {
                let mut key = [0u8; 32];
                key[0] = u8::try_from(index + 1).expect("small index");
                let public_key = qubic::PublicKey::new(key);
                ComputorRow {
                    epoch: 1,
                    computor_index: index,
                    public_key: public_key.to_hex(),
                    identity: public_key.to_identity(),
                    version: 1,
                    timestamp: timestamp(),
                }
            })
            .collect();
        let evidence = vec![QuorumEvidenceRow {
            epoch: 1,
            tick: 7,
            quorum_digest: [0xAA; 32],
            tick_data_digest: [0xBB; 32],
            required_votes: 451,
            aligned_votes: 500,
            fetched_votes: 600,
            unique_votes: 550,
            verified_votes: 480,
            verification: "data".into(),
            warning_count: 1,
            warnings: r#"["lagging peer"]"#.into(),
            timestamp: timestamp(),
            version: 7,
        }];
        Self {
            ticks,
            transactions,
            events,
            identities,
            assets,
            computors,
            evidence,
        }
    }
}

fn page<T: Clone>(rows: &[T], limit: u32, offset: u64) -> Vec<T> {
    rows.iter()
        .skip(offset as usize)
        .take(limit as usize)
        .cloned()
        .collect()
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
        Ok(self.ticks.first().cloned())
    }

    async fn tick(&self, tick: u32) -> StoreResult<Option<TickRow>> {
        Ok(self.ticks.iter().find(|row| row.tick == tick).cloned())
    }

    async fn transactions_for_tick(
        &self,
        tick: u32,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        let rows: Vec<_> = self
            .transactions
            .iter()
            .filter(|row| row.tick == tick)
            .cloned()
            .collect();
        Ok(page(&rows, limit, offset))
    }

    async fn transaction(&self, hash: &str) -> StoreResult<Option<TransactionRow>> {
        Ok(self
            .transactions
            .iter()
            .find(|row| row.hash == hash)
            .cloned())
    }

    async fn transactions_by_hashes(&self, hashes: &[String]) -> StoreResult<Vec<TransactionRow>> {
        Ok(self
            .transactions
            .iter()
            .filter(|row| hashes.contains(&row.hash))
            .cloned()
            .collect())
    }

    async fn events_for_tick(
        &self,
        tick: u32,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<EventRow>> {
        let rows: Vec<_> = self
            .events
            .iter()
            .filter(|row| row.tick == tick)
            .cloned()
            .collect();
        Ok(page(&rows, limit, offset))
    }

    async fn events_for_transaction(&self, hash: &str) -> StoreResult<Vec<EventRow>> {
        Ok(self
            .events
            .iter()
            .filter(|row| row.tx_hash == hash)
            .cloned()
            .collect())
    }

    async fn counterparties(
        &self,
        identity: &str,
        direction: Option<&str>,
        limit: u32,
    ) -> StoreResult<Vec<CounterpartyRow>> {
        let mut totals: std::collections::HashMap<String, CounterpartyRow> =
            std::collections::HashMap::new();
        for row in &self.transactions {
            let (counterparty, incoming) = if row.source == identity {
                (row.destination.clone(), false)
            } else if row.destination == identity {
                (row.source.clone(), true)
            } else {
                continue;
            };
            if counterparty == identity {
                continue;
            }
            if direction == Some("incoming") && !incoming {
                continue;
            }
            if direction == Some("outgoing") && incoming {
                continue;
            }
            let entry = totals
                .entry(counterparty.clone())
                .or_insert(CounterpartyRow {
                    identity: counterparty,
                    incoming_amount: 0,
                    outgoing_amount: 0,
                    net_amount: 0,
                    transaction_count: 0,
                    first_tick: row.tick,
                    last_tick: row.tick,
                });
            if incoming {
                entry.incoming_amount = entry.incoming_amount.saturating_add(row.amount);
            } else {
                entry.outgoing_amount = entry.outgoing_amount.saturating_add(row.amount);
            }
            entry.net_amount = entry.incoming_amount - entry.outgoing_amount;
            entry.transaction_count += 1;
            entry.first_tick = entry.first_tick.min(row.tick);
            entry.last_tick = entry.last_tick.max(row.tick);
        }
        let mut rows: Vec<_> = totals.into_values().collect();
        rows.sort_by(|a, b| {
            (b.incoming_amount + b.outgoing_amount)
                .cmp(&(a.incoming_amount + a.outgoing_amount))
                .then_with(|| b.transaction_count.cmp(&a.transaction_count))
        });
        rows.truncate(limit as usize);
        Ok(rows)
    }

    async fn identity_flow(
        &self,
        identity: &str,
        _interval_seconds: u32,
        _from: chrono::DateTime<Utc>,
        limit: u32,
    ) -> StoreResult<Vec<FlowBucket>> {
        let mut incoming = 0i64;
        let mut outgoing = 0i64;
        let mut count = 0u64;
        for row in &self.transactions {
            if row.source == identity {
                outgoing = outgoing.saturating_add(row.amount);
                count += 1;
            } else if row.destination == identity {
                incoming = incoming.saturating_add(row.amount);
                count += 1;
            }
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        Ok(vec![FlowBucket {
            bucket: timestamp(),
            incoming_amount: incoming,
            outgoing_amount: outgoing,
            net_amount: incoming - outgoing,
            transaction_count: count,
            counterparty_count: 1,
        }])
    }

    async fn asset_transfers(
        &self,
        issuer: &str,
        name: &str,
        kind: Option<&str>,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<AssetTransferRow>> {
        let rows: Vec<_> = self
            .events
            .iter()
            .filter(|row| {
                matches!(
                    row.type_name.as_str(),
                    "asset_ownership_change" | "asset_possession_change"
                )
            })
            .filter(|row| match kind {
                Some("ownership") => row.type_name == "asset_ownership_change",
                Some("possession") => row.type_name == "asset_possession_change",
                _ => true,
            })
            .filter_map(|row| {
                let body: Value = serde_json::from_str(&row.body).ok()?;
                if body.get("issuer")?.as_str()? != issuer || body.get("name")?.as_str()? != name {
                    return None;
                }
                Some(AssetTransferRow {
                    epoch: row.epoch,
                    tick: row.tick,
                    log_id: row.log_id,
                    kind: if row.type_name == "asset_ownership_change" {
                        "ownership".into()
                    } else {
                        "possession".into()
                    },
                    hash: row.tx_hash.clone(),
                    tx_index: row.tx_index,
                    issuer: issuer.into(),
                    name: name.into(),
                    from: body.get("from")?.as_str()?.into(),
                    to: body.get("to")?.as_str()?.into(),
                    shares: body.get("shares")?.as_str()?.parse().ok()?,
                    managing_contract: body
                        .get("managingContract")
                        .and_then(Value::as_i64)
                        .unwrap_or(0),
                    timestamp: row.timestamp,
                })
            })
            .collect();
        Ok(page(&rows, limit, offset))
    }

    async fn identity(&self, identity: &str) -> StoreResult<Option<IdentityRow>> {
        Ok(self
            .identities
            .iter()
            .find(|row| row.identity == identity)
            .cloned())
    }

    async fn top_identities(&self, limit: u32) -> StoreResult<Vec<IdentityRow>> {
        Ok(page(&self.identities, limit, 0))
    }

    async fn stats(&self) -> StoreResult<Value> {
        Ok(serde_json::json!({ "counts": { "ticks": self.ticks.len() }, "ingest": {} }))
    }

    async fn ticks(
        &self,
        epoch: Option<u16>,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<TickRow>> {
        let rows: Vec<_> = self
            .ticks
            .iter()
            .filter(|row| epoch.is_none_or(|epoch| row.epoch == epoch))
            .cloned()
            .collect();
        Ok(page(&rows, limit, offset))
    }

    async fn ticks_after(&self, tick: u32, limit: u32) -> StoreResult<Vec<TickRow>> {
        let rows: Vec<_> = self
            .ticks
            .iter()
            .filter(|row| row.tick > tick)
            .cloned()
            .collect();
        Ok(page(&rows, limit, 0))
    }

    async fn epochs(&self, limit: u32, offset: u64) -> StoreResult<Vec<EpochRow>> {
        let rows = vec![
            EpochRow {
                epoch: 2,
                initial_tick: 5,
                first_tick: 5,
                last_tick: 5,
                first_timestamp: timestamp(),
                last_timestamp: timestamp(),
                tick_count: 1,
                transaction_count: 2,
                event_count: 1,
                closed: false,
            },
            EpochRow {
                epoch: 1,
                initial_tick: 1,
                first_tick: 6,
                last_tick: 7,
                first_timestamp: timestamp(),
                last_timestamp: timestamp(),
                tick_count: 2,
                transaction_count: 4,
                event_count: 2,
                closed: true,
            },
        ];
        Ok(page(&rows, limit, offset))
    }

    async fn epoch(&self, epoch: u16) -> StoreResult<Option<EpochRow>> {
        Ok(self
            .epochs(10, 0)
            .await?
            .into_iter()
            .find(|row| row.epoch == epoch))
    }

    async fn identities(&self, limit: u32, offset: u64) -> StoreResult<Vec<IdentityRow>> {
        Ok(page(&self.identities, limit, offset))
    }

    async fn assets(
        &self,
        issuer: Option<&str>,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<AssetRow>> {
        let rows: Vec<_> = self
            .assets
            .iter()
            .filter(|row| issuer.is_none_or(|issuer| row.issuer == issuer))
            .cloned()
            .collect();
        Ok(page(&rows, limit, offset))
    }

    async fn asset(&self, issuer: &str, name: &str) -> StoreResult<Option<AssetRow>> {
        Ok(self
            .assets
            .iter()
            .find(|row| row.issuer == issuer && row.name == name)
            .cloned())
    }

    async fn computors(&self, epoch: u16) -> StoreResult<Vec<ComputorRow>> {
        Ok(self
            .computors
            .iter()
            .filter(|row| row.epoch == epoch)
            .cloned()
            .collect())
    }

    async fn evidence_for_tick(&self, tick: u32) -> StoreResult<Option<QuorumEvidenceRow>> {
        Ok(self.evidence.iter().find(|row| row.tick == tick).cloned())
    }

    async fn transactions_page(
        &self,
        filter: &TransactionFilter,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        let rows: Vec<_> =
            self.transactions
                .iter()
                .filter(|row| filter.epoch.is_none_or(|epoch| row.epoch == epoch))
                .filter(|row| filter.tick.is_none_or(|tick| row.tick == tick))
                .filter(|row| {
                    filter.identity.as_ref().is_none_or(|identity| {
                        row.source == *identity || row.destination == *identity
                    })
                })
                .cloned()
                .collect();
        Ok(page(&rows, limit, offset))
    }

    async fn events_page(
        &self,
        filter: &EventFilter,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<EventRow>> {
        let rows: Vec<_> = self
            .events
            .iter()
            .filter(|row| filter.epoch.is_none_or(|epoch| row.epoch == epoch))
            .filter(|row| filter.tick.is_none_or(|tick| row.tick == tick))
            .filter(|row| {
                filter
                    .type_name
                    .as_ref()
                    .is_none_or(|name| row.type_name == *name)
            })
            .cloned()
            .collect();
        Ok(page(&rows, limit, offset))
    }

    async fn event_type_counts(&self, limit: u32) -> StoreResult<Vec<EventTypeCount>> {
        Ok(vec![EventTypeCount {
            type_name: "asset_issuance".into(),
            count: self.events.len() as u64,
        }]
        .into_iter()
        .take(limit as usize)
        .collect())
    }

    async fn activity_analytics(
        &self,
        _interval_seconds: u32,
        limit: u32,
    ) -> StoreResult<Vec<ActivityBucket>> {
        Ok(vec![ActivityBucket {
            bucket: timestamp(),
            ticks: self.ticks.len() as u64,
            transactions: self.transactions.len() as u64,
            events: self.events.len() as u64,
            volume: 42,
        }]
        .into_iter()
        .take(limit as usize)
        .collect())
    }

    async fn network_stats(&self) -> StoreResult<NetworkStats> {
        Ok(NetworkStats {
            head: self.head().await?,
            epochs: 2,
            ticks: self.ticks.len() as u64,
            transactions: self.transactions.len() as u64,
            events: self.events.len() as u64,
            identities: self.identities.len() as u64,
            latest_timestamp: Some(timestamp()),
            ingest: IngestCursor::default(),
        })
    }

    async fn write_tick(&self, batch: &TickBatch) -> StoreResult<WriteOutcome> {
        Ok(WriteOutcome {
            tick: batch.tick,
            version: batch.version,
            transactions: batch.transactions.len(),
            events: batch.events.len(),
            identities: batch.identities.len(),
        })
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

    async fn transactions_for_identity(
        &self,
        identity: &str,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        let rows: Vec<_> = self
            .transactions
            .iter()
            .filter(|row| row.source == identity || row.destination == identity)
            .cloned()
            .collect();
        Ok(page(&rows, limit, offset))
    }

    async fn recent_events(&self, limit: u32, offset: u64) -> StoreResult<Vec<EventRow>> {
        Ok(page(&self.events, limit, offset))
    }

    async fn recent_transactions(
        &self,
        limit: u32,
        offset: u64,
    ) -> StoreResult<Vec<TransactionRow>> {
        Ok(page(&self.transactions, limit, offset))
    }
}

async fn request(
    uri: &str,
    headers: &[(header::HeaderName, &str)],
) -> (StatusCode, Value, axum::http::HeaderMap) {
    let app = router(AppState::new(Arc::new(MockStore::new())));
    let mut builder = Request::builder().uri(uri);
    for (name, value) in headers {
        builder = builder.header(name, *value);
    }
    let response = app
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value, headers)
}

async fn get(uri: &str) -> (StatusCode, Value) {
    let (status, body, _) = request(uri, &[]).await;
    (status, body)
}

async fn post(uri: &str, body: Value) -> (StatusCode, Value) {
    let app = router(AppState::new(Arc::new(MockStore::new())));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn tick_and_transaction_resources() {
    let (status, body) = get("/v1/ticks/7").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["tick"], 7);
    assert_eq!(body["data"]["verification"], "data");

    let (status, body) = get("/v1/transactions?limit=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["data"][0]["target_tick"], body["data"][0]["tick"],
        "indexed transactions expose the executed tick as target_tick"
    );
    assert_eq!(body["meta"]["has_more"], true);
    assert!(body["meta"]["next_cursor"].is_string());

    let (status, body) = get("/v1/ticks/7/events").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["type_name"], "asset_issuance");
    assert_eq!(body["data"][0]["body"]["issuer"], IDENTITY);

    let (status, body) = get("/v1/ticks/999/events").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "not_found");
}

#[tokio::test]
async fn public_mempool_routes_are_retired() {
    for uri in [
        "/v1/mempool",
        "/v1/mempool/abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
    ] {
        let (status, body) = get(uri).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}: {body}");
    }
}

#[tokio::test]
async fn collection_resources_are_typed_and_enveloped() {
    let (status, body) = get("/v1/epochs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["epoch"], 2);
    assert_eq!(body["data"][1]["closed"], true);
    assert_eq!(body["meta"]["count"], 2);

    let (status, body) = get("/v1/ticks?epoch=1&limit=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["tick"], 7);

    let (status, body) = get("/v1/epochs/1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["epoch"], 1);

    let (status, body) = get("/v1/identities").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["identity"], IDENTITY);

    let (status, body) = get("/v1/assets").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["name"], "QX");

    let (status, body) = get(&format!("/v1/assets/{IDENTITY}/QX")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["issuer"], IDENTITY);

    let (status, body) = get("/v1/network/index").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["ticks"], 3);
    assert_eq!(body["data"]["head"]["tick"], 7);

    let (status, body) = get("/v1/network").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["indexed"]["ticks"], 3);
    assert_eq!(body["data"]["system"], Value::Null);
    assert_eq!(body["data"]["peers"]["configured"], 0);
}

#[tokio::test]
async fn computors_and_evidence_are_typed_and_report_not_found() {
    let (status, body, headers) = request("/v1/epochs/1/computors", &[]).await;
    assert_eq!(status, StatusCode::OK);
    let rows = body["data"].as_array().expect("computor list");
    assert_eq!(rows.len(), 3);
    let mut key = [0u8; 32];
    key[0] = 1;
    let public_key = qubic::PublicKey::new(key);
    assert_eq!(rows[0]["epoch"], 1);
    assert_eq!(rows[0]["computor_index"], 0);
    assert_eq!(rows[0]["public_key"], public_key.to_hex());
    assert_eq!(rows[0]["identity"], public_key.to_identity());
    assert_eq!(body["meta"]["epoch"], 1);
    assert_eq!(body["meta"]["count"], 3);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=30, stale-while-revalidate=60"
    );

    let (status, body) = get("/v1/epochs/999/computors").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "not_found");

    let (status, body, headers) = request("/v1/ticks/7/evidence", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["epoch"], 1);
    assert_eq!(body["data"]["tick"], 7);
    assert_eq!(body["data"]["quorum_digest"], "aa".repeat(32));
    assert_eq!(body["data"]["tick_data_digest"], "bb".repeat(32));
    assert_eq!(body["data"]["required_votes"], 451);
    assert_eq!(body["data"]["aligned_votes"], 500);
    assert_eq!(body["data"]["fetched_votes"], 600);
    assert_eq!(body["data"]["unique_votes"], 550);
    assert_eq!(body["data"]["verified_votes"], 480);
    assert_eq!(body["data"]["verification"], "data");
    assert_eq!(body["data"]["warning_count"], 1);
    assert_eq!(body["data"]["warnings"], r#"["lagging peer"]"#);
    assert!(body["data"]["timestamp"].is_string());
    assert_eq!(
        headers.get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=31536000, immutable"
    );

    let (status, body) = get("/v1/ticks/999/evidence").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "not_found");
}

#[tokio::test]
async fn filters_narrow_transactions_and_events() {
    let (status, body) = get(&format!("/v1/transactions?identity={IDENTITY}&limit=2")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    for row in body["data"].as_array().unwrap() {
        assert!(
            row["source"] == IDENTITY || row["destination"] == IDENTITY,
            "{row}"
        );
    }

    let (status, body) = get("/v1/events?type=asset_issuance&tick=7").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);

    let (status, body) = get("/v1/transactions?tick=5&epoch=2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn cursors_page_without_duplicates_and_reject_other_filters() {
    let (status, body) = get("/v1/ticks?limit=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["tick"], 7);
    let cursor = body["meta"]["next_cursor"].as_str().unwrap().to_owned();

    let (status, body) = get(&format!("/v1/ticks?limit=1&cursor={cursor}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["tick"], 6);
    assert_eq!(body["meta"]["offset"], 1);

    let (status, body) = get(&format!("/v1/ticks?limit=1&epoch=1&cursor={cursor}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_cursor");

    let (status, body) = get("/v1/ticks?cursor=not-a-cursor").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_cursor");
}

#[tokio::test]
async fn identity_accepts_case_insensitive_input() {
    let uri = format!("/v1/identities/{}", IDENTITY.to_ascii_lowercase());
    let (status, body) = get(&uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["identity"], IDENTITY);

    let uri = format!("/v1/identities/{IDENTITY}/transactions?limit=1");
    let (status, body) = get(&uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["meta"]["limit"], 1);
}

#[tokio::test]
async fn errors_use_problem_json_and_stable_codes() {
    let (status, body, headers) = request("/v1/identities/NOT-AN-IDENTITY", &[]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_identity");
    assert_eq!(
        headers.get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");

    let (status, body) = get("/v1/transactions/zzzz").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_transaction_hash");

    let (status, body) = get("/v1/ticks/999").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "not_found");

    let (status, body) = get("/does-not-exist").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "not_found");
}

#[tokio::test]
async fn middleware_sets_request_id_cache_and_cors_headers() {
    let (status, body, headers) =
        request("/v1/ticks/7", &[(header::ORIGIN, "https://example.test")]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["tick"], 7);
    assert!(headers.get("x-request-id").is_some());
    assert_eq!(
        headers.get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=31536000, immutable"
    );
    assert!(headers.contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN));
}

#[tokio::test]
async fn openapi_and_docs_are_served() {
    let (status, body) = get("/openapi.json").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["openapi"], "3.1.0");
    assert!(body["paths"]["/v1/epochs"].is_object());
    assert!(body["paths"]["/v1/epochs/{epoch}/computors"].is_object());
    assert!(body["paths"]["/v1/ticks/{tick}/evidence"].is_object());
    assert!(body["paths"]["/v1/assets/{issuer}/{name}"].is_object());
    assert!(body["components"]["schemas"]["Computor"].is_object());
    assert!(body["components"]["schemas"]["QuorumEvidence"].is_object());

    let app = router(AppState::new(Arc::new(MockStore::new())));
    let response = app
        .oneshot(Request::builder().uri("/docs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/html; charset=utf-8"
    );
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8_lossy(&bytes);
    assert!(html.contains("Scalar.createApiReference"));
    assert!(html.contains("/scalar.js"));
    assert!(html.contains("/openapi.json"));
    assert!(html.contains("Space Grotesk"));
    assert!(html.contains("/fonts/space-grotesk.woff2"));
    assert!(html.contains(r#"rel="preload""#));

    let app = router(AppState::new(Arc::new(MockStore::new())));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/scalar.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/javascript"
    );

    let app = router(AppState::new(Arc::new(MockStore::new())));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/fonts/space-grotesk.woff2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "font/woff2"
    );
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=31536000, immutable"
    );
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(bytes.starts_with(b"wOF2"));

    let app = router(AppState::new(Arc::new(MockStore::new())));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/fonts/space-grotesk-ofl.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("SIL OPEN FONT LICENSE"));
}

#[tokio::test]
async fn large_responses_are_compressed_when_requested() {
    let app = router(AppState::new(Arc::new(MockStore::new())));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/openapi.json")
                .header(header::ACCEPT_ENCODING, "gzip")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers().clone();
    assert_eq!(
        headers.get(header::CONTENT_ENCODING).map(|v| v.as_bytes()),
        Some(b"gzip".as_slice())
    );
}

#[tokio::test]
async fn rate_limit_problem_uses_429_and_retry_after() {
    let response = api::ApiError::too_many_requests("slow down").into_response();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn batch_lookup_reports_found_and_missing() {
    let found = hash_for(7, 0);
    let missing = hash_for(99, 9);
    let (status, body) = post(
        "/v1/transactions/batch",
        serde_json::json!({ "hashes": [found.clone(), missing.clone()] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0]["hash"], found);
    assert_eq!(body["meta"]["requested"], 2);
    assert_eq!(body["meta"]["found"], 1);
    assert_eq!(body["meta"]["missing"], serde_json::json!([missing]));

    let (status, body) = post(
        "/v1/transactions/batch",
        serde_json::json!({ "hashes": [] }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "bad_request");
}

#[tokio::test]
async fn analytics_routes_have_effective_defaults() {
    let (status, body) = get("/v1/analytics/activity").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["meta"]["interval_seconds"], 3600);
    assert_eq!(body["meta"]["limit"], 24);
    assert_eq!(body["data"][0]["ticks"], 3);

    let (status, body) = get("/v1/analytics/top-identities").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["meta"]["limit"], 25);

    let (status, body) = get("/v1/analytics/event-types").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["type_name"], "asset_issuance");
}

#[tokio::test]
async fn optional_network_features_report_unavailable_or_forbidden() {
    let (status, body) = get("/v1/stream").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "unavailable");

    let (status, body) = get("/v1/ticks/7/raw").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "unavailable");

    let (status, body) = post("/v1/transactions", serde_json::json!({ "raw": "00" })).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "forbidden");

    let (status, body) = get("/v1/live/identities/NOT-AN-IDENTITY").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_identity");

    let (status, body) = post(
        "/v1/live/contracts/1/query",
        serde_json::json!({ "input": "not hex" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "bad_request");

    for uri in [
        "/v1/network/system",
        "/v1/network/revenue",
        "/v1/network/oracles",
        "/v1/network/oracles/stats",
    ] {
        let (status, body) = get(uri).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{uri}: {body}");
        assert_eq!(body["code"], "unavailable", "{uri}");
    }
}

#[tokio::test]
async fn transaction_validation_reports_local_checks() {
    let raw = include_bytes!("../../node/fixtures/tx-91916364-1.bin");
    let transaction = qubic::Transaction::decode(raw).expect("fixture transaction decodes");

    let (status, body, headers) = {
        let app = router(AppState::new(Arc::new(MockStore::new())));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/transactions/validate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({ "raw": format!("0x{}", hex::encode(raw)) }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice::<Value>(&bytes).expect("json body"),
            headers,
        )
    };

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
    assert_eq!(body["data"]["valid"], true);
    assert_eq!(
        body["data"]["hash"],
        qubic::encode_tx_hash(&transaction.hash())
    );
    assert_eq!(
        body["data"]["source"],
        qubic::PublicKey(transaction.source).to_identity()
    );
    assert_eq!(body["data"]["tick"], transaction.tick);
    assert_eq!(body["data"]["amount"], transaction.amount);
    assert_eq!(body["data"]["input_bytes"], transaction.input.len());
    assert_eq!(
        body["data"]["total_bytes"],
        qubic::Transaction::BASE_SIZE + transaction.input.len() + 64
    );
    let checks = body["data"]["checks"].as_array().expect("checks array");
    assert_eq!(checks.len(), 4);
    assert!(checks.iter().all(|check| check["passed"] == true));
    assert_eq!(body["meta"]["source"], "local");
    assert_eq!(body["meta"]["broadcast"], false);
}

#[tokio::test]
async fn transaction_validation_reports_failed_checks() {
    let raw = include_bytes!("../../node/fixtures/tx-91916364-1.bin");
    let mut tampered = raw.to_vec();
    let last = tampered.len() - 1;
    tampered[last] ^= 0xFF;
    let (status, body) = post(
        "/v1/transactions/validate",
        serde_json::json!({ "raw": hex::encode(&tampered) }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["valid"], false);
    let checks = body["data"]["checks"].as_array().expect("checks array");
    let signature = checks
        .iter()
        .find(|check| check["name"] == "signature")
        .expect("signature check");
    assert_eq!(signature["passed"], false);
    assert!(signature["detail"].is_string());

    let oversized = qubic::Transaction {
        source: [1; 32],
        destination: [2; 32],
        amount: qubic::messages::MAX_AMOUNT + 1,
        tick: 7,
        input_type: 0,
        input: vec![0; qubic::messages::MAX_INPUT_SIZE + 1],
        signature: [0; 64],
    };
    let (status, body) = post(
        "/v1/transactions/validate",
        serde_json::json!({ "raw": hex::encode(oversized.encode()) }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["valid"], false);
    let checks = body["data"]["checks"].as_array().expect("checks array");
    assert_eq!(checks[0]["name"], "amount");
    assert_eq!(checks[0]["passed"], false);
    assert_eq!(checks[1]["name"], "input_size");
    assert_eq!(checks[1]["passed"], false);
    assert_eq!(checks[2]["name"], "encoding");
    assert_eq!(checks[2]["passed"], true);
}

#[tokio::test]
async fn transaction_validation_rejects_malformed_bytes() {
    let (status, body) = post(
        "/v1/transactions/validate",
        serde_json::json!({ "raw": "not hex" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "bad_request");

    let raw = include_bytes!("../../node/fixtures/tx-91916364-1.bin");
    let (status, body) = post(
        "/v1/transactions/validate",
        serde_json::json!({ "raw": hex::encode(&raw[..raw.len() - 1]) }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "bad_request");

    let (status, body) = post(
        "/v1/transactions/validate",
        serde_json::json!({ "raw": "00" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "bad_request");
}

#[tokio::test]
async fn transaction_network_status_reports_errors() {
    let (status, body) = get("/v1/live/transactions/not-a-hash/status").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_transaction_hash");

    let hash = hash_for(7, 0);
    let (status, body) = get(&format!("/v1/live/transactions/{hash}/status")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "unavailable");

    let (status, body) = get(&format!("/v1/live/transactions/{hash}")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "unavailable");
}

#[tokio::test]
async fn raw_archive_serves_a_section() {
    let dir = std::env::temp_dir().join(format!("glyph-api-raw-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let archive = archive::FileArchive::new(&dir);
    let mut bundle = archive::RawBundle::new(1, 7);
    bundle.set_section(archive::Section::Transactions, vec![1, 2, 3, 4]);
    archive::put_bundle(&archive, &bundle).await.unwrap();

    let mut state = AppState::new(Arc::new(MockStore::new()));
    state.archive = Some(Arc::new(archive));
    let response = router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/ticks/7/raw?section=transactions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/octet-stream"
    );
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), &[1, 2, 3, 4]);

    let mut state = AppState::new(Arc::new(MockStore::new()));
    state.archive = Some(Arc::new(archive::FileArchive::new(&dir)));
    let app = router(state);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/ticks/7/raw?section=bogus")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn transaction_effects_combine_events_and_summary() {
    let hash = hash_for(7, 0);
    let (status, body) = get(&format!("/v1/transactions/{hash}/effects")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["transaction"]["hash"], hash);
    assert_eq!(body["data"]["tick"]["tick"], 7);
    assert_eq!(body["data"]["summary"]["qu_transfer_count"], 1);
    assert_eq!(body["data"]["summary"]["qu_transfer_amount"], 5);
    assert_eq!(body["data"]["summary"]["asset_ownership_change_count"], 1);
    assert_eq!(body["data"]["effects"][0]["kind"], "qu_transfer");
    assert_eq!(body["data"]["effects"][0]["amount"], 5);
}

#[tokio::test]
async fn identity_counterparties_and_flow_are_aggregated() {
    let (status, body) = get(&format!("/v1/identities/{IDENTITY}/counterparties?limit=5")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["meta"]["count"], 1);
    assert_eq!(body["data"][0]["identity"], OTHER);
    assert_eq!(body["data"][0]["incoming_amount"], 144);
    assert_eq!(body["data"][0]["outgoing_amount"], 144);
    assert_eq!(body["data"][0]["net_amount"], 0);
    assert_eq!(body["data"][0]["transaction_count"], 6);

    let (status, body) = get(&format!(
        "/v1/identities/{IDENTITY}/flow?interval=3600&limit=3"
    ))
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 3);
    assert_eq!(body["meta"]["totals"]["incoming_amount"], 144);
    assert_eq!(body["meta"]["totals"]["outgoing_amount"], 144);
    assert_eq!(body["meta"]["totals"]["net_amount"], 0);
    assert_eq!(body["meta"]["totals"]["transaction_count"], 6);
}

#[tokio::test]
async fn asset_transfers_are_paged_locally() {
    let (status, body) = get(&format!("/v1/assets/{IDENTITY}/QX/transfers?limit=1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0]["kind"], "ownership");
    assert_eq!(body["data"][0]["from"], IDENTITY);
    assert_eq!(body["data"][0]["to"], OTHER);
    assert_eq!(body["data"][0]["shares"], 10);
    assert_eq!(body["meta"]["issuer"], IDENTITY);
    assert_eq!(body["meta"]["name"], "QX");
}

#[tokio::test]
async fn search_returns_every_matching_resource_type() {
    let (status, body) = get("/v1/search?q=7").await;
    assert_eq!(status, StatusCode::OK);
    let types: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|item| item["type"].as_str())
        .collect();
    assert!(types.contains(&"tick"));

    let (status, body) = get("/v1/search?q=1").await;
    assert_eq!(status, StatusCode::OK);
    let types: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|item| item["type"].as_str())
        .collect();
    assert!(types.contains(&"epoch"));

    let (status, body) = get(&format!("/v1/search?q={IDENTITY}/QX")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["type"], "asset");
    assert_eq!(body["data"][0]["value"]["name"], "QX");
}

#[tokio::test]
async fn transaction_lifecycle_reports_inclusion_and_execution() {
    let hash = hash_for(7, 0);
    let (status, body) = get(&format!("/v1/transactions/{hash}/lifecycle")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["state"], "confirmed");
    assert_eq!(body["data"]["target_tick"], 7);
    assert_eq!(body["data"]["included_tick"], 7);
    assert_eq!(body["data"]["transaction"]["hash"], hash);
    assert_eq!(body["data"]["money_flew"], Value::Null);
    assert_eq!(body["meta"]["pending_pool_public"], false);
    assert_eq!(body["data"]["timeline"][0]["stage"], "included");
    assert!(
        body["data"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["type_name"] == "qu_transfer")
    );

    let (status, body) = get("/v1/transactions/unknown/lifecycle").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_transaction_hash");
}

#[tokio::test]
async fn inclusion_risk_uses_recent_fill_and_target_distance() {
    let (status, body) = get(&format!(
        "/v1/transactions/inclusion-risk?source={IDENTITY}&target_tick=8"
    ))
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["current_tick"], 7);
    assert_eq!(body["data"]["target_tick"], 8);
    assert_eq!(body["data"]["ticks_until_target"], 1);
    assert!(body["data"]["score"].as_f64().unwrap() < 0.25);
    assert_eq!(body["data"]["level"], "low");
    assert_eq!(body["meta"]["model"], "heuristic");

    let (status, body) = get("/v1/transactions/inclusion-risk?target_tick=1").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["level"], "very_high");
    assert_eq!(
        body["data"]["recommendation"],
        "rebuild with a future target tick"
    );
}

#[tokio::test]
async fn tick_capacity_reports_fill_and_top_senders() {
    let (status, body) = get("/v1/ticks/7/capacity").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["transaction_count"], 2);
    assert_eq!(body["data"]["transaction_slots"], 4096);
    assert_eq!(body["data"]["remaining_slots"], 4094);
    assert_eq!(body["data"]["top_senders"].as_array().unwrap().len(), 2);
    assert_eq!(body["data"]["money_flew"]["available"], false);
}
