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
use node::Peer;
use qubic::messages::Transaction;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{ApiError, AppState};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/identities/{identity}/entity", get(entity))
        .route("/v1/transactions/{hash}/network", get(network_transaction))
        .route(
            "/v1/transactions/{hash}/network/status",
            get(network_transaction_status),
        )
        .route("/v1/transactions/validate", post(validate))
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
    state
        .pool
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
                "public_key": qubic::PublicKey(entity.public_key).to_hex(),
                "spectrum_index": response.spectrum_index,
                "tick": response.tick,
                "incoming_amount": entity.incoming_amount,
                "outgoing_amount": entity.outgoing_amount,
                "balance": entity.incoming_amount.saturating_sub(entity.outgoing_amount),
                "incoming_transfers": entity.number_of_incoming_transfers,
                "outgoing_transfers": entity.number_of_outgoing_transfers,
                "latest_incoming_tick": entity.latest_incoming_transfer_tick,
                "latest_outgoing_tick": entity.latest_outgoing_transfer_tick,
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
            "input_type": transaction.input_type,
            "input_data": hex::encode(&transaction.input),
        },
        "meta": { "source": "peer" }
    })),))
}

async fn network_transaction_status(
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
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "peer has no confirmed transaction {hash}; it may be unknown or still pending"
            ))
        })?;
    let status = match peer.tx_status(transaction.tick).await {
        Ok(status) => Some(status),
        Err(error) => {
            tracing::warn!(%hash, %error, "peer transaction status add-on unavailable");
            None
        }
    };
    let money_flew = status
        .as_ref()
        .and_then(|status| status.money_flew_for(&digest));
    let current_tick = status.as_ref().map(|status| status.current_tick);
    let execution = match money_flew {
        Some(true) => "transferred",
        Some(false) => "no_effect",
        None => "unknown",
    };
    Ok((
        [(
            axum::http::header::CACHE_CONTROL,
            "public, max-age=2, stale-while-revalidate=5",
        )],
        Json(json!({
            "data": {
                "hash": hash,
                "tick": transaction.tick,
                "source": qubic::PublicKey(transaction.source).to_identity(),
                "destination": qubic::PublicKey(transaction.destination).to_identity(),
                "amount": transaction.amount,
                "input_type": transaction.input_type,
                "input_data": hex::encode(&transaction.input),
                "confirmed": true,
                "money_flew": money_flew,
                "execution": execution,
                "current_tick": current_tick,
                "confirmations": current_tick.map(|current| current.saturating_sub(transaction.tick)),
            },
            "meta": {
                "source": "peer",
                "status_source": if status.is_some() { "tx-status" } else { "unavailable" },
            }
        })),
    ))
}

#[derive(Debug, Deserialize)]
pub struct ValidateBody {
    /// Full signed transaction as lowercase or uppercase hex.
    pub raw: String,
}

#[derive(Debug, Serialize)]
struct ValidationCheck {
    name: &'static str,
    passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

impl ValidationCheck {
    fn new(name: &'static str, passed: bool, detail: impl Into<Option<String>>) -> Self {
        Self {
            name,
            passed,
            detail: detail.into(),
        }
    }
}

/// Local preflight checks for a signed transaction. This never opens a peer
/// connection and never broadcasts, so it is safe to enable unconditionally.
async fn validate(Json(body): Json<ValidateBody>) -> Result<impl IntoResponse, ApiError> {
    let raw = body.raw.trim();
    let hex_body = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .unwrap_or(raw);
    let bytes = hex::decode(hex_body)
        .map_err(|error| ApiError::bad_request(format!("raw is not hex: {error}")))?;
    let transaction = Transaction::decode(&bytes)
        .map_err(|error| ApiError::bad_request(format!("invalid transaction: {error}")))?;

    let signable_len = Transaction::BASE_SIZE + transaction.input.len();
    let amount_ok = (0..=qubic::messages::MAX_AMOUNT).contains(&transaction.amount);
    let input_ok = transaction.input.len() <= qubic::messages::MAX_INPUT_SIZE;
    let signature_ok = qubic::crypto::schnorrq::verify(
        &transaction.source,
        &bytes[..signable_len],
        &transaction.signature,
    );

    let checks = vec![
        ValidationCheck::new(
            "amount",
            amount_ok,
            (!amount_ok).then(|| {
                format!(
                    "amount {} is outside 0..={}",
                    transaction.amount,
                    qubic::messages::MAX_AMOUNT
                )
            }),
        ),
        ValidationCheck::new(
            "input_size",
            input_ok,
            (!input_ok).then(|| {
                format!(
                    "input is {} bytes, limit is {}",
                    transaction.input.len(),
                    qubic::messages::MAX_INPUT_SIZE
                )
            }),
        ),
        ValidationCheck::new(
            "signature",
            signature_ok,
            (!signature_ok)
                .then(|| "signature does not verify against the source public key".to_owned()),
        ),
    ];
    let valid = checks.iter().all(|check| check.passed);

    Ok((
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(json!({
            "data": {
                "valid": valid,
                "hash": qubic::encode_tx_hash(&transaction.hash()),
                "source": qubic::PublicKey(transaction.source).to_identity(),
                "destination": qubic::PublicKey(transaction.destination).to_identity(),
                "amount": transaction.amount,
                "tick": transaction.tick,
                "input_type": transaction.input_type,
                "input_data": hex::encode(&transaction.input),
                "input_bytes": transaction.input.len(),
                "total_bytes": transaction.total_size(),
                "checks": checks,
            },
            "meta": { "source": "local", "broadcast": false }
        })),
    ))
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
                "money_flew": entry.money_flew,
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
                "current_tick": status.current_tick,
                "transactions": transactions,
            },
            "meta": { "source": "peer" }
        })),
    ))
}

#[derive(Debug, Deserialize)]
struct ContractQuery {
    #[serde(default, alias = "inputType")]
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
                "contract_index": contract_index,
                "input_type": input_type,
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
            "bytes_sent": Transaction::BASE_SIZE + transaction.input.len(),
        }
    })),))
}
