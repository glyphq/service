//! Peer-backed operations that are not part of the immutable index.
//!
//! These routes connect directly to a configured Qubic peer per request. They
//! are intentionally separate from the ClickHouse read path so a peer outage
//! cannot affect indexed history.

use axum::extract::Query;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use node::{NodePool, Peer};
use qubic::messages::Transaction;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{ApiError, AppState};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/identities/{identity}/entity", get(entity))
        .route("/v1/transactions/{hash}/network", get(network_transaction))
        .route("/v1/ticks/{tick}/tx-status", get(tx_status))
        .route("/v1/ticks/{tick}/raw", get(raw_bundle))
        .route("/v1/contracts/{contract_index}/query", post(contract_query))
}

#[derive(Debug, Deserialize)]
struct RawQuery {
    #[serde(default)]
    section: Option<String>,
}

async fn raw_bundle(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
    Query(params): Query<RawQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let Some(archive) = state.archive.clone() else {
        return Err(ApiError::unavailable(
            "raw archive is not configured; set S3_BUCKET or ARCHIVE_DIR",
        ));
    };
    let row = state
        .store
        .tick(tick)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("tick {tick} is not indexed")))?;
    let bundle = archive::get_bundle(archive.as_ref(), row.epoch, tick)
        .await
        .map_err(|error| ApiError::unavailable(format!("raw archive read failed: {error}")))?
        .ok_or_else(|| {
            ApiError::not_found(format!("raw bundle for tick {tick} is not archived"))
        })?;
    let section = match params.section.as_deref() {
        None => None,
        Some("tick") | Some("header") => Some(archive::Section::TickHeader),
        Some("tick_data") | Some("tick-data") => Some(archive::Section::TickData),
        Some("votes") => Some(archive::Section::Votes),
        Some("transactions") => Some(archive::Section::Transactions),
        Some("log_ranges") | Some("log-ranges") => Some(archive::Section::LogRanges),
        Some("logs") => Some(archive::Section::Logs),
        Some("computors") => Some(archive::Section::Computors),
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "unknown raw section {other:?}"
            )));
        }
    };
    let (payload, name) = match section {
        Some(section) => (bundle.section(section).to_vec(), section.name()),
        None => (
            bundle
                .encode()
                .map_err(|error| ApiError::internal(format!("encode raw bundle: {error}")))?,
            "bundle",
        ),
    };
    let disposition = format!("attachment; filename=\"tick-{tick}-{name}.bin\"");
    Ok((
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            ),
            (
                header::CONTENT_DISPOSITION,
                HeaderValue::from_str(&disposition)
                    .unwrap_or_else(|_| HeaderValue::from_static("attachment")),
            ),
            (
                header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=31536000, immutable"),
            ),
        ],
        payload,
    ))
}

async fn connect(state: &AppState) -> Result<Peer, ApiError> {
    if state.peers.is_empty() {
        return Err(ApiError::unavailable(
            "no QUBIC_PEERS configured for peer-backed routes",
        ));
    }
    NodePool::new(state.peers.clone())
        .connect_best()
        .await
        .map_err(|error| ApiError::unavailable(format!("peer connection failed: {error}")))
}

async fn entity(
    State(state): State<AppState>,
    Path(identity): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = identity.to_ascii_uppercase();
    let key = qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let mut peer = connect(&state).await?;
    let response = peer
        .entity(key.0)
        .await
        .map_err(|error| ApiError::unavailable(format!("entity lookup failed: {error}")))?;
    if response.spectrum_index < 0 {
        return Err(ApiError::not_found(format!(
            "identity {identity} is not in the current spectrum"
        )));
    }
    let entity = response.entity;
    Ok((
        [(
            axum::http::header::CACHE_CONTROL,
            "public, max-age=2, stale-while-revalidate=5",
        )],
        Json(json!({
            "data": {
                "identity": identity,
                "publicKey": qubic::PublicKey(entity.public_key).to_hex(),
                "spectrumIndex": response.spectrum_index,
                "tick": response.tick,
                "incomingAmount": entity.incoming_amount,
                "outgoingAmount": entity.outgoing_amount,
                "balance": entity.incoming_amount.saturating_sub(entity.outgoing_amount),
                "incomingTransfers": entity.number_of_incoming_transfers,
                "outgoingTransfers": entity.number_of_outgoing_transfers,
                "latestIncomingTick": entity.latest_incoming_transfer_tick,
                "latestOutgoingTick": entity.latest_outgoing_transfer_tick,
            },
            "meta": { "source": "peer" }
        })),
    ))
}

async fn network_transaction(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let hash = hash.to_ascii_lowercase();
    let digest =
        qubic::decode_tx_hash(&hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;
    let mut peer = connect(&state).await?;
    let transaction = peer
        .transaction(digest)
        .await
        .map_err(|error| ApiError::unavailable(format!("transaction lookup failed: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("peer does not retain transaction {hash}")))?;
    Ok((Json(json!({
        "data": {
            "hash": hash,
            "source": qubic::PublicKey(transaction.source).to_identity(),
            "destination": qubic::PublicKey(transaction.destination).to_identity(),
            "amount": transaction.amount,
            "tick": transaction.tick,
            "inputType": transaction.input_type,
            "inputData": hex::encode(&transaction.input),
        },
        "meta": { "source": "peer" }
    })),))
}

async fn tx_status(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
) -> Result<impl IntoResponse, ApiError> {
    let mut peer = connect(&state).await?;
    let status = peer
        .tx_status(tick)
        .await
        .map_err(|error| ApiError::unavailable(format!("tx status unavailable: {error}")))?;
    let transactions: Vec<Value> = status
        .transactions
        .iter()
        .map(|entry| {
            json!({
                "hash": qubic::encode_tx_hash(&entry.digest),
                "moneyFlew": entry.money_flew,
            })
        })
        .collect();
    Ok((
        [(
            axum::http::header::CACHE_CONTROL,
            "public, max-age=2, stale-while-revalidate=5",
        )],
        Json(json!({
            "data": {
                "tick": status.tick,
                "currentTick": status.current_tick,
                "transactions": transactions,
            },
            "meta": { "source": "peer" }
        })),
    ))
}

#[derive(Debug, Deserialize)]
struct ContractQuery {
    #[serde(default, rename = "inputType")]
    input_type: Option<u16>,
    #[serde(default)]
    input: Option<String>,
}

async fn contract_query(
    State(state): State<AppState>,
    Path(contract_index): Path<u32>,
    Json(body): Json<ContractQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let input_type = body.input_type.unwrap_or(0);
    let input = match body.input {
        Some(value) => hex::decode(value.trim_start_matches("0x"))
            .map_err(|error| ApiError::bad_request(format!("input is not hex: {error}")))?,
        None => Vec::new(),
    };
    if input.len() > qubic::messages::MAX_INPUT_SIZE {
        return Err(ApiError::bad_request(format!(
            "input is {} bytes, maximum is {}",
            input.len(),
            qubic::messages::MAX_INPUT_SIZE
        )));
    }
    let mut peer = connect(&state).await?;
    let output = peer
        .contract_function(contract_index, input_type, &input)
        .await
        .map_err(|error| ApiError::unavailable(format!("contract query failed: {error}")))?;
    Ok((
        [(
            axum::http::header::CACHE_CONTROL,
            "public, max-age=2, stale-while-revalidate=5",
        )],
        Json(json!({
            "data": {
                "contractIndex": contract_index,
                "inputType": input_type,
                "output": hex::encode(&output),
                "bytes": output.len(),
            },
            "meta": { "source": "peer", "empty": output.is_empty() }
        })),
    ))
}

#[derive(Debug, Deserialize)]
pub struct BroadcastBody {
    /// Full signed transaction as lowercase or uppercase hex.
    pub raw: String,
}

pub async fn broadcast_transaction(
    State(state): State<AppState>,
    Json(body): Json<BroadcastBody>,
) -> Result<impl IntoResponse, ApiError> {
    if !state.allow_broadcast {
        return Err(ApiError::forbidden(
            "transaction broadcast is disabled; set ALLOW_BROADCAST=true",
        ));
    }
    let bytes = hex::decode(body.raw.trim_start_matches("0x"))
        .map_err(|error| ApiError::bad_request(format!("raw is not hex: {error}")))?;
    let transaction = Transaction::decode(&bytes)
        .map_err(|error| ApiError::bad_request(format!("invalid transaction: {error}")))?;
    if transaction.input.len() > qubic::messages::MAX_INPUT_SIZE {
        return Err(ApiError::bad_request("transaction input is too large"));
    }
    let mut peer = connect(&state).await?;
    let receipt = peer
        .broadcast_transaction(&transaction)
        .await
        .map_err(|error| ApiError::bad_request(format!("broadcast rejected: {error}")))?;
    Ok((Json(json!({
        "data": {
            "hash": qubic::encode_tx_hash(&receipt.digest),
            "tick": receipt.tick,
            "peer": receipt.peer.to_string(),
            "bytesSent": Transaction::BASE_SIZE + transaction.input.len(),
        }
    })),))
}
