//! Combined indexed and direct-peer transaction status.
//!
//! `GET /v1/transactions/{hash}/status` reports what the local index knows
//! about a transaction and, when peers are configured, asks a peer for the
//! transaction and its tick confirmation status. The peer lookup is
//! best-effort: an index hit still returns `200` when a peer has already
//! pruned the transaction.

use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use node::Peer;
use qubic::messages::Transaction;
use serde_json::{Value, json};

use crate::dto::event_value;
use crate::{ApiError, AppState};

/// Cap on the primary peer transaction lookup. A peer answers an unknown hash
/// with `EndResponse` immediately; this only bounds a peer that stops
/// responding, which would otherwise spend the node's full retry budget
/// (three eight-second attempts) before the index fallback is used.
const PEER_LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
/// Cap on the optional tx-status add-on probe. Peers without the add-on drop
/// the request silently, so this keeps a status request responsive.
const PEER_STATUS_TIMEOUT: Duration = Duration::from_secs(2);

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/transactions/{hash}/status", get(transaction_status))
        .route("/v1/transactions/{hash}/effects", get(transaction_effects))
}

/// One request for an explorer transaction page: the indexed transaction, the
/// tick header, every decoded event it produced, and a compact effect summary.
/// This route never opens a peer connection.
async fn transaction_effects(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let hash = hash.trim().to_ascii_lowercase();
    qubic::decode_tx_hash(&hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;
    let transaction = state
        .store
        .transaction(&hash)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("transaction {hash} is not indexed")))?;
    let tick = state.store.tick(transaction.tick).await?;
    let events: Vec<Value> = state
        .store
        .events_for_transaction(&hash)
        .await?
        .into_iter()
        .map(event_value)
        .collect();
    let effects: Vec<Value> = events.iter().filter_map(effect_json).collect();
    let summary = effect_summary(&events);

    Ok((
        [(
            header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("public, max-age=31536000, immutable"),
        )],
        Json(json!({
            "data": {
                "transaction": transaction,
                "tick": tick,
                "events": events,
                "effects": effects,
                "summary": summary,
            },
            "meta": {
                "source": "index",
                "event_count": events.len(),
                "effect_count": effects.len(),
            }
        })),
    ))
}

pub(crate) fn effect_json(event: &Value) -> Option<Value> {
    let body = event.get("body")?;
    let kind = event.get("type_name")?.as_str()?;
    let common = json!({
        "kind": kind,
        "tick": event.get("tick").cloned().unwrap_or(Value::Null),
        "log_id": event.get("log_id").cloned().unwrap_or(Value::Null),
        "tx_hash": event.get("tx_hash").cloned().unwrap_or(Value::Null),
    });
    let effect = match kind {
        "qu_transfer" => json!({
            "kind": "qu_transfer",
            "from": body.get("from").cloned().unwrap_or(Value::Null),
            "to": body.get("to").cloned().unwrap_or(Value::Null),
            "amount": body.get("amount").cloned().unwrap_or(Value::Null),
        }),
        "asset_ownership_change" | "asset_possession_change" => json!({
            "kind": kind,
            "issuer": body.get("issuer").cloned().unwrap_or(Value::Null),
            "name": body.get("name").cloned().unwrap_or(Value::Null),
            "from": body.get("from").cloned().unwrap_or(Value::Null),
            "to": body.get("to").cloned().unwrap_or(Value::Null),
            "shares": body.get("shares").cloned().unwrap_or(Value::Null),
            "managing_contract": body
                .get("managing_contract")
                .cloned()
                .unwrap_or(Value::Null),
        }),
        "burning" => json!({
            "kind": "burning",
            "from": body.get("from").cloned().unwrap_or(Value::Null),
            "amount": body.get("amount").cloned().unwrap_or(Value::Null),
            "contract_index": body.get("contract_index").cloned().unwrap_or(Value::Null),
        }),
        "contract_error" | "contract_warning" | "contract_information" | "contract_debug" => {
            json!({
                "kind": kind,
                "contract_index": body.get("contract_index").cloned().unwrap_or(Value::Null),
                "message_type": body.get("message_type").cloned().unwrap_or(Value::Null),
                "data": body.get("data").cloned().unwrap_or(Value::Null),
            })
        }
        "custom_message" => json!({
            "kind": "custom_message",
            "op_code": body.get("op_code").cloned().unwrap_or(Value::Null),
            "data": body.get("data").cloned().unwrap_or(Value::Null),
        }),
        _ => return None,
    };
    let mut effect = effect;
    if let (Some(effect), Some(common)) = (effect.as_object_mut(), common.as_object()) {
        for (key, value) in common {
            effect.insert(key.clone(), value.clone());
        }
    }
    Some(effect)
}

pub(crate) fn effect_summary(events: &[Value]) -> Value {
    let mut qu_transfer_count = 0u64;
    let mut qu_transfer_amount = 0i64;
    let mut asset_ownership_change_count = 0u64;
    let mut asset_possession_change_count = 0u64;
    let mut burn_count = 0u64;
    let mut contract_error_count = 0u64;
    let mut custom_message_count = 0u64;
    for event in events {
        match event.get("type_name").and_then(Value::as_str).unwrap_or("") {
            "qu_transfer" => {
                qu_transfer_count += 1;
                qu_transfer_amount = qu_transfer_amount.saturating_add(
                    event
                        .pointer("/body/amount")
                        .and_then(Value::as_i64)
                        .unwrap_or(0),
                );
            }
            "asset_ownership_change" => asset_ownership_change_count += 1,
            "asset_possession_change" => asset_possession_change_count += 1,
            "burning" | "dust_burning" => burn_count += 1,
            "contract_error" => contract_error_count += 1,
            "custom_message" => custom_message_count += 1,
            _ => {}
        }
    }
    json!({
        "event_count": events.len(),
        "qu_transfer_count": qu_transfer_count,
        "qu_transfer_amount": qu_transfer_amount,
        "asset_ownership_change_count": asset_ownership_change_count,
        "asset_possession_change_count": asset_possession_change_count,
        "burn_count": burn_count,
        "contract_error_count": contract_error_count,
        "custom_message_count": custom_message_count,
    })
}

async fn transaction_status(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let hash = hash.trim().to_ascii_lowercase();
    let digest =
        qubic::decode_tx_hash(&hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;

    let indexed = state.store.transaction(&hash).await?;
    let (mut peer, mut peer_error) = match connect(&state).await {
        Ok(peer) => (Some(peer), None),
        Err(error) if indexed.is_some() => {
            tracing::warn!(%hash, %error.detail, "transaction status peer lookup degraded");
            (None, Some(error.detail))
        }
        Err(error) => return Err(error),
    };
    let lookup = match peer.as_mut() {
        Some(peer) => Some(
            tokio::time::timeout(
                PEER_LOOKUP_TIMEOUT,
                peer_transaction(peer, digest, indexed.as_ref().map(|row| row.tick)),
            )
            .await,
        ),
        None => None,
    };
    let network = match lookup {
        Some(Ok(Ok(transaction))) => transaction,
        Some(Ok(Err(error))) if indexed.is_some() => {
            tracing::warn!(%hash, %error.detail, "transaction status peer lookup degraded");
            peer_error = Some(error.detail);
            peer = None;
            None
        }
        Some(Ok(Err(error))) => return Err(error),
        Some(Err(_)) if indexed.is_some() => {
            tracing::warn!(
                %hash,
                timeout_seconds = PEER_LOOKUP_TIMEOUT.as_secs(),
                "transaction status peer lookup timed out; using index"
            );
            peer_error = Some(format!(
                "peer transaction lookup timed out after {}s",
                PEER_LOOKUP_TIMEOUT.as_secs()
            ));
            peer = None;
            None
        }
        Some(Err(_)) => {
            return Err(ApiError::unavailable(format!(
                "peer transaction lookup timed out after {}s",
                PEER_LOOKUP_TIMEOUT.as_secs()
            )));
        }
        None => None,
    };

    if indexed.is_none() && network.is_none() {
        return Err(ApiError::not_found(format!(
            "transaction {hash} is not indexed and is unknown to the peer"
        )));
    }

    let tick = indexed
        .as_ref()
        .map(|row| row.tick)
        .or_else(|| network.as_ref().map(|transaction| transaction.tick));
    let confirmation = match (tick, peer.as_mut()) {
        (Some(tick), Some(peer)) => {
            match tokio::time::timeout(PEER_STATUS_TIMEOUT, peer_status(peer, tick)).await {
                Ok(value) => value,
                Err(_) => {
                    tracing::debug!(
                        %hash, tick,
                        timeout_seconds = PEER_STATUS_TIMEOUT.as_secs(),
                        "peer tx-status add-on did not answer; reporting without confirmation"
                    );
                    None
                }
            }
        }
        _ => None,
    };

    let mut network_body = json!({ "found": network.is_some() });
    if let Some(transaction) = &network {
        network_body["source"] = json!(qubic::PublicKey(transaction.source).to_identity());
        network_body["destination"] =
            json!(qubic::PublicKey(transaction.destination).to_identity());
        network_body["amount"] = json!(transaction.amount);
        network_body["tick"] = json!(transaction.tick);
        network_body["input_type"] = json!(transaction.input_type);
        network_body["input_data"] = json!(hex::encode(&transaction.input));
    }
    if let Some(confirmation) = confirmation {
        network_body["confirmation"] = confirmation;
    }

    let mut data = json!({ "hash": hash, "network": network_body });
    if let Some(row) = &indexed {
        data["indexed"] = json!(row);
    }
    if let Some(tick) = tick {
        data["tick"] = json!(tick);
    }

    let mut meta = json!({ "source": "index+peer" });
    if let Some(error) = peer_error {
        meta["peer_error"] = json!(error);
        meta["source"] = json!("index");
    }
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({
            "data": data,
            "meta": meta
        })),
    ))
}

async fn connect(state: &AppState) -> Result<Peer, ApiError> {
    if state.peers.is_empty() {
        return Err(ApiError::unavailable(
            "no QUBIC_PEERS configured for peer-backed routes",
        ));
    }
    state
        .pool
        .connect_best()
        .await
        .map_err(|error| ApiError::unavailable(format!("peer connection failed: {error}")))
}

/// Fetches a transaction by digest.
///
/// The index and tick-data digests hash the transaction bytes without the
/// trailing signature, while some peers validate `Transaction::hash()` over the
/// full packet. When the indexed tick is known, fall back to fetching that
/// tick's transactions and matching the canonical digest locally.
async fn peer_transaction(
    peer: &mut Peer,
    digest: [u8; 32],
    indexed_tick: Option<u32>,
) -> Result<Option<Transaction>, ApiError> {
    let error = match peer.transaction(digest).await {
        Ok(transaction) => return Ok(transaction),
        Err(error) => error,
    };
    let Some(tick) = indexed_tick else {
        return Err(ApiError::unavailable(format!(
            "transaction lookup failed: {error}"
        )));
    };
    if !matches!(&error, node::Error::Verification(_)) {
        return Err(ApiError::unavailable(format!(
            "transaction lookup failed: {error}"
        )));
    }
    let known = [false; qubic::messages::TX_PER_TICK];
    let transactions = peer.transactions(tick, &known).await.map_err(|error| {
        ApiError::unavailable(format!("tick {tick} transaction lookup failed: {error}"))
    })?;
    Ok(transactions
        .into_iter()
        .find(|transaction| canonical_hash(transaction) == digest))
}

async fn peer_status(peer: &mut Peer, tick: u32) -> Option<Value> {
    let status = peer.tx_status(tick).await.ok()?;
    let entries: Vec<Value> = status
        .transactions
        .iter()
        .map(|entry| {
            json!({
                "hash": qubic::encode_tx_hash(&entry.digest),
                "money_flew": entry.money_flew,
            })
        })
        .collect();
    Some(json!({
        "current_tick": status.current_tick,
        "tick": status.tick,
        "entries": entries,
    }))
}

fn canonical_hash(transaction: &Transaction) -> [u8; 32] {
    let bytes = transaction.encode();
    qubic::crypto::k12::k12_32(&bytes[..bytes.len() - 64])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use store::{
        ActivityBucket, AssetRow, EpochRow, EventFilter, EventRow, EventTypeCount, IdentityRow,
        IngestCursor, NetworkStats, Result as StoreResult, Store, TickBatch, TickRow,
        TransactionFilter, TransactionRow, WriteOutcome,
    };
    use tower::ServiceExt;

    use super::*;

    struct EmptyStore;

    #[async_trait]
    impl Store for EmptyStore {
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
        async fn transaction(&self, _hash: &str) -> StoreResult<Option<TransactionRow>> {
            Ok(None)
        }
        async fn transactions_for_identity(
            &self,
            _identity: &str,
            _limit: u32,
            _offset: u64,
        ) -> StoreResult<Vec<TransactionRow>> {
            Ok(Vec::new())
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
        async fn identity(&self, _identity: &str) -> StoreResult<Option<IdentityRow>> {
            Ok(None)
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

    async fn get_status(uri: &str) -> (StatusCode, Value) {
        let app = router().with_state(AppState::new(Arc::new(EmptyStore)));
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn rejects_invalid_hash() {
        let (status, body) = get_status("/v1/transactions/not-a-hash/status").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_transaction_hash");
    }

    #[tokio::test]
    async fn valid_hash_without_peers_is_unavailable() {
        let hash = qubic::encode_tx_hash(&[7u8; 32]);
        let (status, body) = get_status(&format!("/v1/transactions/{hash}/status")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "unavailable");
    }
}
