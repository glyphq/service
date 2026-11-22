//! Peer-backed operations that are not part of the immutable index.
//!
//! These routes connect directly to a configured Qubic peer per request. They
//! are intentionally separate from the ClickHouse read path so a peer outage
//! cannot affect indexed history.

use std::time::Duration;

use axum::extract::Query;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use node::Peer;
use qubic::messages::{
    PENDING_TXS_POOL_TICKS, RECOMMENDED_TARGET_TICK_LEAD, TICK_TRANSACTIONS_PUBLICATION_OFFSET,
    Transaction,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{ApiError, AppState};

/// The qli tx-status request is an optional peer add-on. Peers that do not
/// run it silently drop the request, so cap how long an optional status probe
/// can hold a request open instead of paying the full peer retry budget.
const TX_STATUS_TIMEOUT: Duration = Duration::from_secs(3);

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/live/identities/{identity}", get(entity))
        .route("/v1/live/transactions/{hash}", get(network_transaction))
        .route(
            "/v1/live/transactions/{hash}/status",
            get(network_transaction_status),
        )
        .route("/v1/transactions/validate", post(validate))
        .route("/v1/live/ticks/{tick}/transactions", get(tx_status))
        .route("/v1/ticks/{tick}/raw", get(raw_bundle))
        .route(
            "/v1/live/contracts/{contract_index}/query",
            post(contract_query),
        )
}

#[derive(Debug, Deserialize)]
struct RawQuery {
    #[serde(default)]
    section: Option<String>,
}

fn parse_tick(raw: &str) -> Result<u32, ApiError> {
    raw.trim().parse::<u32>().map_err(|_| {
        ApiError::bad_request(format!(
            "invalid tick {raw:?}: expected an unsigned 32-bit integer"
        ))
    })
}

/// Maps axum body rejections onto the problem+json error envelope so a
/// malformed request body never falls back to axum's plain-text rejection.
pub(crate) fn json_body<T>(payload: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    payload.map(|Json(value)| value).map_err(|rejection| {
        let status = rejection.status();
        let (status, code) = if status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
            (status, "unsupported_media_type")
        } else {
            (StatusCode::BAD_REQUEST, "bad_request")
        };
        ApiError {
            status,
            code,
            detail: rejection.body_text(),
        }
    })
}

async fn raw_bundle(
    State(state): State<AppState>,
    Path(tick): Path<String>,
    Query(params): Query<RawQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tick = parse_tick(&tick)?;
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
    let mut bundle = archive::get_bundle(archive.as_ref(), row.epoch, tick)
        .await
        .map_err(|error| ApiError::unavailable(format!("raw archive read failed: {error}")))?
        .ok_or_else(|| {
            ApiError::not_found(format!("raw bundle for tick {tick} is not archived"))
        })?;
    // New bundles omit the computor set because it is identical for every tick
    // in an epoch. Older bundles still carry it inline, so only read the epoch
    // sidecar when the bundle itself has nothing.
    if bundle.computors.is_empty() {
        match archive::get_computors(archive.as_ref(), row.epoch).await {
            Ok(Some(bytes)) => bundle.set_section(archive::Section::Computors, bytes.to_vec()),
            Ok(None) => {}
            Err(error) => {
                return Err(ApiError::unavailable(format!(
                    "raw archive computor read failed: {error}"
                )));
            }
        }
    }
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
        Some(archive::Section::Votes) if bundle.votes.is_empty() => {
            return Err(ApiError::not_found(format!(
                "votes for tick {tick} are not archived; set ARCHIVE_VOTES=true to store votes for new ticks"
            )));
        }
        Some(archive::Section::Computors) if bundle.computors.is_empty() => {
            return Err(ApiError::not_found(format!(
                "computor set for epoch {} is not archived",
                row.epoch
            )));
        }
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
    let hash = hash.trim().to_ascii_lowercase();
    let digest =
        qubic::decode_tx_hash(&hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;
    let mut peer = connect(&state).await?;
    let transaction = peer
        .transaction(digest)
        .await
        .map_err(|error| ApiError::unavailable(format!("transaction lookup failed: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("peer does not retain transaction {hash}")))?;
    Ok((
        [(
            axum::http::header::CACHE_CONTROL,
            "public, max-age=2, stale-while-revalidate=5",
        )],
        Json(json!({
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
        })),
    ))
}

async fn network_transaction_status(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let hash = hash.trim().to_ascii_lowercase();
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
pub(crate) struct ValidationCheck {
    pub(crate) name: &'static str,
    pub(crate) passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) detail: Option<String>,
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

/// A signed transaction decoded from a request body, together with the local
/// preflight checks. Broadcast and mempool submission share this path so both
/// reject the same inputs before opening a peer socket.
pub(crate) struct DecodedTransaction {
    pub transaction: Transaction,
    pub bytes: Vec<u8>,
    pub checks: Vec<ValidationCheck>,
    pub hash: String,
    pub valid: bool,
}

impl DecodedTransaction {
    /// Human-readable list of the checks that failed. Broadcast and mempool
    /// submission share this so both report the same reason.
    pub(crate) fn failure_detail(&self) -> String {
        let detail = self
            .checks
            .iter()
            .filter(|check| !check.passed)
            .filter_map(|check| check.detail.as_deref())
            .collect::<Vec<_>>()
            .join("; ");
        if detail.is_empty() {
            "one or more local checks failed".to_owned()
        } else {
            detail
        }
    }
}

pub(crate) fn decode_transaction(raw: &str) -> Result<DecodedTransaction, ApiError> {
    let raw = raw.trim();
    let hex_body = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .unwrap_or(raw);
    let bytes = hex::decode(hex_body)
        .map_err(|error| ApiError::bad_request(format!("raw is not hex: {error}")))?;
    let transaction = Transaction::decode(&bytes)
        .map_err(|error| ApiError::bad_request(format!("invalid transaction: {error}")))?;
    let checks = transaction_checks(&transaction, &bytes);
    let valid = checks.iter().all(|check| check.passed);
    let hash = qubic::encode_tx_hash(&transaction.hash());
    Ok(DecodedTransaction {
        transaction,
        bytes,
        checks,
        hash,
        valid,
    })
}

/// Local preflight checks for a signed transaction. This never opens a peer
/// connection and never broadcasts, so it is safe to enable unconditionally.
async fn validate(
    State(state): State<AppState>,
    payload: Result<Json<ValidateBody>, JsonRejection>,
) -> Result<impl IntoResponse, ApiError> {
    let body = json_body(payload)?;
    let raw = body.raw.trim();
    let hex_body = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .unwrap_or(raw);
    let bytes = hex::decode(hex_body)
        .map_err(|error| ApiError::bad_request(format!("raw is not hex: {error}")))?;
    let transaction = Transaction::decode(&bytes)
        .map_err(|error| ApiError::bad_request(format!("invalid transaction: {error}")))?;

    let mut checks = transaction_checks(&transaction, &bytes);
    let TickAssessment {
        head_tick,
        checks: tick_checks,
        warnings,
    } = target_tick_assessment(&state, transaction.tick).await;
    checks.extend(tick_checks);
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
                "warnings": warnings,
                "head_tick": head_tick,
            },
            "meta": { "source": "local", "broadcast": false }
        })),
    ))
}

struct TickAssessment {
    head_tick: Option<u32>,
    checks: Vec<ValidationCheck>,
    warnings: Vec<String>,
}

/// Checks the target tick against the indexed head. Qubic only accepts target
/// ticks inside the node pending window, and tick data for `T + 2` is built
/// during tick `T`, so a target at or before `head + 1` cannot make it.
async fn target_tick_assessment(state: &AppState, target_tick: u32) -> TickAssessment {
    let head_tick = match state.store.head().await {
        Ok(Some(row)) => Some(row.tick),
        _ => None,
    };
    let Some(head) = head_tick else {
        return TickAssessment {
            head_tick: None,
            checks: Vec::new(),
            warnings: Vec::new(),
        };
    };
    let (check, warnings) = target_tick_check(head, target_tick);
    TickAssessment {
        head_tick: Some(head),
        checks: vec![check],
        warnings,
    }
}

fn target_tick_check(head: u32, target_tick: u32) -> (ValidationCheck, Vec<String>) {
    let minimum = head.saturating_add(TICK_TRANSACTIONS_PUBLICATION_OFFSET);
    let maximum = head.saturating_add(PENDING_TXS_POOL_TICKS);
    let too_close = target_tick < minimum;
    let too_far = target_tick > maximum;
    let passed = !too_close && !too_far;
    let detail = if too_close {
        Some(format!(
            "target tick {target_tick} is too close to indexed head {head}; Qubic publishes tick data {} ticks ahead. Use at least {minimum}, preferably {}.",
            TICK_TRANSACTIONS_PUBLICATION_OFFSET,
            head.saturating_add(RECOMMENDED_TARGET_TICK_LEAD)
        ))
    } else if too_far {
        Some(format!(
            "target tick {target_tick} is more than {PENDING_TXS_POOL_TICKS} ticks beyond indexed head {head}; the node pending pool will not accept it."
        ))
    } else {
        None
    };
    let mut warnings = Vec::new();
    if passed
        && target_tick < head.saturating_add(TICK_TRANSACTIONS_PUBLICATION_OFFSET.saturating_add(2))
    {
        warnings.push(format!(
            "target tick {target_tick} leaves little propagation time; prefer at least {} or the common +{} lead.",
            head.saturating_add(TICK_TRANSACTIONS_PUBLICATION_OFFSET.saturating_add(2)),
            RECOMMENDED_TARGET_TICK_LEAD
        ));
    }
    (
        ValidationCheck::new("target_tick", passed, detail),
        warnings,
    )
}

async fn tx_status(
    State(state): State<AppState>,
    Path(tick): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let tick = parse_tick(&tick)?;
    let mut peer = connect(&state).await?;
    let status = match tokio::time::timeout(TX_STATUS_TIMEOUT, peer.tx_status(tick)).await {
        Ok(result) => result
            .map_err(|error| ApiError::unavailable(format!("tx status unavailable: {error}")))?,
        Err(_) => {
            return Err(ApiError::unavailable(format!(
                "peer did not answer tx status for tick {tick} within {}s; \
                 the peer may not run the tx-status add-on or may have pruned the tick",
                TX_STATUS_TIMEOUT.as_secs()
            )));
        }
    };
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
    payload: Result<Json<ContractQuery>, JsonRejection>,
) -> Result<impl IntoResponse, ApiError> {
    let body = json_body(payload)?;
    let input_type = body.input_type.unwrap_or(0);
    let input = match body.input {
        Some(value) => {
            let value = value.trim();
            let value = value
                .strip_prefix("0x")
                .or_else(|| value.strip_prefix("0X"))
                .unwrap_or(value);
            hex::decode(value)
                .map_err(|error| ApiError::bad_request(format!("input is not hex: {error}")))?
        }
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
    #[serde(
        alias = "transaction",
        alias = "raw_transaction",
        alias = "signed_transaction"
    )]
    pub raw: String,
    /// Validate and describe the transaction without opening a peer connection.
    #[serde(default)]
    pub dry_run: bool,
    /// Number of peers to fan out to. Capped by BROADCAST_PEERS and 8.
    #[serde(default, alias = "max_peers")]
    pub peers: Option<usize>,
    /// Skip the mempool and fan out directly to peers.
    #[serde(default)]
    pub direct: bool,
    /// Optional webhook registered for this transaction's mempool changes.
    #[serde(default)]
    pub webhook_url: Option<String>,
    #[serde(default)]
    pub webhook_secret: Option<String>,
}

pub async fn broadcast_transaction(
    State(state): State<AppState>,
    Json(body): Json<BroadcastBody>,
) -> Result<axum::response::Response, ApiError> {
    if !state.allow_broadcast {
        return Err(ApiError::forbidden(
            "transaction broadcast is disabled; set ALLOW_BROADCAST=true",
        ));
    }
    let mut decoded = decode_transaction(&body.raw)?;
    if !body.direct && !body.dry_run {
        if let Some(mempool) = state.mempool.clone() {
            if let Some(entry) = mempool.status(&decoded.hash).await? {
                return Ok(crate::mempool::accepted(
                    entry,
                    mempool.config().expiry_ticks,
                    None,
                    Vec::new(),
                ));
            }
        }
    }
    let TickAssessment {
        head_tick,
        checks: tick_checks,
        mut warnings,
    } = target_tick_assessment(&state, decoded.transaction.tick).await;
    decoded.checks.extend(tick_checks);
    decoded.valid = decoded.checks.iter().all(|check| check.passed);
    let hash = decoded.hash.clone();
    let peers_requested = body
        .peers
        .unwrap_or(state.broadcast_peers)
        .clamp(1, state.broadcast_peers.min(8));
    let target = state.peers.len().min(peers_requested);
    let transaction_json = json!({
        "hash": hash,
        "source": qubic::PublicKey(decoded.transaction.source).to_identity(),
        "destination": qubic::PublicKey(decoded.transaction.destination).to_identity(),
        "amount": decoded.transaction.amount,
        "tick": decoded.transaction.tick,
        "target_tick": decoded.transaction.tick,
        "input_type": decoded.transaction.input_type,
        "input_data": hex::encode(&decoded.transaction.input),
        "input_bytes": decoded.transaction.input.len(),
        "total_bytes": decoded.transaction.total_size(),
    });

    if body.dry_run {
        return Ok((
            [(axum::http::header::CACHE_CONTROL, "no-store")],
            Json(json!({
                "data": {
                    "valid": decoded.valid,
                    "dry_run": true,
                    "target_peers": target,
                    "transaction": transaction_json,
                    "checks": decoded.checks,
                    "warnings": warnings,
                    "head_tick": head_tick,
                },
                "meta": {
                    "source": "local",
                    "broadcast": false,
                    "confirmed": false,
                }
            })),
        )
            .into_response());
    }

    // Submit to the mempool by default when it is enabled. `direct: true`
    // keeps the original fan-out-and-return behavior.
    if !body.direct {
        if let Some(mempool) = state.mempool.clone() {
            let inline = match (body.webhook_url.clone(), body.webhook_secret.clone()) {
                (None, None) => None,
                (Some(url), Some(secret)) => Some(crate::mempool::InlineWebhook { url, secret }),
                _ => {
                    return Err(ApiError::bad_request(
                        "webhook_url and webhook_secret must be provided together",
                    ));
                }
            };
            let entry = mempool.submit(decoded, peers_requested, inline).await?;
            return Ok(crate::mempool::accepted(
                entry,
                mempool.config().expiry_ticks,
                head_tick,
                warnings,
            ));
        }
    }

    if !decoded.valid {
        let failed = decoded.failure_detail();
        let target_tick_failed = decoded
            .checks
            .iter()
            .any(|check| check.name == "target_tick" && !check.passed);
        if target_tick_failed {
            return Err(ApiError::invalid_target_tick(format!(
                "transaction has an invalid target tick: {failed}"
            )));
        }
        return Err(ApiError::bad_request(format!(
            "transaction failed local validation: {failed}"
        )));
    }
    if state.peers.is_empty() {
        return Err(ApiError::unavailable(
            "no QUBIC_PEERS configured for transaction broadcast",
        ));
    }

    let attempts = state
        .pool
        .broadcast_transaction_fanout(&decoded.transaction, peers_requested)
        .await;
    let sent = attempts.iter().filter(|attempt| attempt.sent).count();
    let peers: Vec<Value> = attempts
        .iter()
        .map(|attempt| {
            json!({
                "peer": attempt.peer.to_string(),
                "sent": attempt.sent,
                "latency_ms": attempt.elapsed.as_millis() as u64,
                "error": attempt.error.clone(),
            })
        })
        .collect();
    if sent == 0 {
        let errors = attempts
            .iter()
            .filter_map(|attempt| attempt.error.as_deref())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(ApiError::unavailable(format!(
            "broadcast reached no peers: {errors}"
        )));
    }
    if sent < target {
        warnings.push(format!(
            "transaction reached {sent} of {target} peers; propagation may be slower"
        ));
    }
    Ok((
        StatusCode::ACCEPTED,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(json!({
            "data": {
                "sent": sent,
                "targeted": target,
                "transaction": transaction_json,
                "peers": peers,
                "warnings": warnings,
            },
            "meta": {
                "source": "direct-qubic-peers",
                "broadcast": true,
                "confirmed": false,
                "head_tick": head_tick,
                "links": {
                    "transaction": format!("/v1/transactions/{hash}"),
                    "tick": format!("/v1/ticks/{}", decoded.transaction.tick),
                },
                "hint": "A sent peer accepted the packet at the socket. Qubic does not acknowledge broadcasts; poll the transaction link or stream new ticks to observe inclusion."
            }
        })),
    )
        .into_response())
}

fn transaction_checks(transaction: &Transaction, bytes: &[u8]) -> Vec<ValidationCheck> {
    let amount_ok = (0..=qubic::messages::MAX_AMOUNT).contains(&transaction.amount);
    let input_ok = transaction.input.len() <= qubic::messages::MAX_INPUT_SIZE;
    let size_ok = bytes.len() == transaction.total_size();
    let signable_len = Transaction::BASE_SIZE + transaction.input.len();
    let signature_ok = bytes.len() >= signable_len + 64
        && qubic::crypto::schnorrq::verify(
            &transaction.source,
            &bytes[..signable_len],
            &transaction.signature,
        );
    vec![
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
            "encoding",
            size_ok,
            (!size_ok).then(|| {
                format!(
                    "raw is {} bytes, transaction declares {}",
                    bytes.len(),
                    transaction.total_size()
                )
            }),
        ),
        ValidationCheck::new(
            "signature",
            signature_ok,
            (!signature_ok)
                .then(|| "signature does not verify against the source public key".to_owned()),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_tick_rejects_too_close_and_too_far() {
        let head = 1000;
        assert!(!target_tick_check(head, head).0.passed);
        assert!(!target_tick_check(head, head + 1).0.passed);
        assert!(target_tick_check(head, head + 2).0.passed);
        assert!(target_tick_check(head, head + 3).0.passed);
        assert!(target_tick_check(head, head + 4).0.passed);
        assert!(
            target_tick_check(head, head + PENDING_TXS_POOL_TICKS)
                .0
                .passed
        );
        assert!(
            !target_tick_check(head, head + PENDING_TXS_POOL_TICKS + 1)
                .0
                .passed
        );
    }

    #[test]
    fn target_tick_warns_when_propagation_time_is_short() {
        let head = 1000;
        assert!(!target_tick_check(head, head + 2).1.is_empty());
        assert!(!target_tick_check(head, head + 3).1.is_empty());
        assert!(target_tick_check(head, head + 4).1.is_empty());
    }

    #[test]
    fn protocol_scheduling_constants_match_qubic_core() {
        assert_eq!(PENDING_TXS_POOL_TICKS, 2_666);
        assert_eq!(TICK_TRANSACTIONS_PUBLICATION_OFFSET, 2);
    }
}
