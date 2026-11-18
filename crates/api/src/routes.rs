use std::collections::{HashMap, HashSet};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use store::{EventFilter, TransactionFilter};

use crate::dto::{PageParams, event_value};
use crate::{ApiError, AppState};

const IMMUTABLE: &str = "public, max-age=31536000, immutable";
const RECENT: &str = "public, max-age=2, stale-while-revalidate=5";
const AGGREGATE: &str = "public, max-age=30, stale-while-revalidate=60";

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/status", get(status))
        .route("/v1/network/stats", get(network_stats))
        .route("/v1/epochs", get(epochs))
        .route("/v1/epochs/{epoch}", get(epoch))
        .route("/v1/epochs/{epoch}/computors", get(epoch_computors))
        .route("/v1/ticks", get(ticks))
        .route("/v1/ticks/{tick}", get(tick))
        .route("/v1/ticks/{tick}/evidence", get(tick_evidence))
        .route("/v1/ticks/{tick}/transactions", get(tick_transactions))
        .route("/v1/ticks/{tick}/events", get(tick_events))
        .route(
            "/v1/transactions",
            get(transactions).post(crate::advanced::broadcast_transaction),
        )
        .route("/v1/transactions/batch", post(transactions_batch))
        .route("/v1/transactions/{hash}", get(transaction))
        .route("/v1/events", get(events))
        .route("/v1/identities", get(identities))
        .route(
            "/v1/identities/{identity}/transactions",
            get(identity_transactions),
        )
        .route("/v1/identities/{identity}/activity", get(identity_activity))
        .route("/v1/identities/{identity}/events", get(identity_events))
        .route("/v1/identities/{identity}", get(identity))
        .route("/v1/assets", get(assets))
        .route("/v1/assets/{issuer}/{name}", get(asset))
        .route("/v1/analytics/activity", get(analytics_activity))
        .route(
            "/v1/analytics/top-identities",
            get(analytics_top_identities),
        )
        .route("/v1/analytics/event-types", get(analytics_event_types))
        .route("/v1/search", get(search))
}

#[derive(Debug, Default, Deserialize)]
struct AnalyticsQuery {
    #[serde(default)]
    interval: Option<u32>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct BatchTransactions {
    hashes: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct TickListQuery {
    #[serde(default)]
    epoch: Option<u16>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct TransactionQuery {
    #[serde(default)]
    epoch: Option<u16>,
    #[serde(default)]
    tick: Option<u32>,
    #[serde(default)]
    identity: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct EventQuery {
    #[serde(default)]
    epoch: Option<u16>,
    #[serde(default)]
    tick: Option<u32>,
    #[serde(default, rename = "type")]
    event_type: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct AssetQuery {
    #[serde(default)]
    issuer: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    offset: Option<u64>,
}

impl TickListQuery {
    fn page(&self) -> PageParams {
        PageParams {
            limit: self.limit,
            cursor: self.cursor.clone(),
            offset: self.offset,
        }
    }
}

impl TransactionQuery {
    fn page(&self) -> PageParams {
        PageParams {
            limit: self.limit,
            cursor: self.cursor.clone(),
            offset: self.offset,
        }
    }
}

impl EventQuery {
    fn page(&self) -> PageParams {
        PageParams {
            limit: self.limit,
            cursor: self.cursor.clone(),
            offset: self.offset,
        }
    }
}

impl AssetQuery {
    fn page(&self) -> PageParams {
        PageParams {
            limit: self.limit,
            cursor: self.cursor.clone(),
            offset: self.offset,
        }
    }
}

async fn healthz() -> Json<Value> {
    Json(json!({ "ok": true }))
}

async fn status(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let (head, stats, earliest, network) = tokio::join!(
        state.store.head(),
        state.store.stats(),
        state.store.earliest_tick(),
        tokio::time::timeout(Duration::from_secs(2), state.pool.head()),
    );
    let head = head?;
    let stats = stats?;
    let earliest = earliest?;
    let (network, network_error) = match network {
        Ok(Ok(network)) => (Some(network), None),
        Ok(Err(error)) => (None, Some(error.to_string())),
        Err(_) => (None, Some("peer head request timed out".to_string())),
    };
    let last_indexed = stats
        .get("ingest")
        .and_then(|ingest| ingest.get("last_indexed_tick"))
        .and_then(Value::as_u64)
        .and_then(|tick| u32::try_from(tick).ok());
    let indexed_ticks = stats
        .get("counts")
        .and_then(|counts| counts.get("ticks"))
        .and_then(Value::as_u64);
    let network_head = network.as_ref().map(|head| head.tick);
    let rates = state.progress.observe(last_indexed, network_head);
    let lag_ticks = network_head
        .zip(last_indexed)
        .map(|(network, indexed)| network.saturating_sub(indexed));
    let eta_seconds = lag_ticks
        .zip(rates.ingest)
        .filter(|(_, rate)| *rate > 0.0)
        .map(|(lag, rate)| (f64::from(lag) / rate).round() as u64);
    let backfill = backfill_json(
        network.as_ref().map(|head| head.initial_tick),
        network_head,
        earliest,
        last_indexed,
    );
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({
            "data": {
                "head": head,
                "ingest": stats.get("ingest").cloned().unwrap_or(Value::Null),
                "ingest_status": {
                    "last_indexed_tick": last_indexed,
                    "indexed_ticks": indexed_ticks,
                    "ticks_per_second": rates.ingest,
                    "lag_ticks": lag_ticks,
                    "eta_seconds": eta_seconds,
                },
                "network": {
                    "reachable": network.is_some(),
                    "head": network_head,
                    "epoch": network.as_ref().map(|head| head.epoch),
                    "initial_tick": network.as_ref().map(|head| head.initial_tick),
                    "tick_duration": network.as_ref().map(|head| head.tick_duration),
                    "ticks_per_second": rates.network,
                    "error": network_error,
                },
                "backfill": backfill,
                "stats": stats,
            },
            "meta": {
                "source": "direct-qubic-peers",
                "version": env!("CARGO_PKG_VERSION"),
                "uptime_seconds": state.uptime_seconds(),
                "streaming": state.hub.is_some(),
                "broadcast": state.allow_broadcast,
                "archive": state.archive.is_some(),
                "peers_configured": state.peers.len(),
            }
        })),
    ))
}

async fn network_stats(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let stats = state.store.network_stats().await?;
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": stats,
            "meta": { "source": "direct-qubic-peers" },
        })),
    ))
}

fn backfill_json(
    initial_tick: Option<u32>,
    network_head: Option<u32>,
    earliest_tick: Option<u32>,
    last_indexed_tick: Option<u32>,
) -> Value {
    let Some(initial_tick) = initial_tick else {
        return json!({ "available": false, "reason": "network head is unavailable" });
    };
    let epoch_ticks = network_head.map(|head| u64::from(head.saturating_sub(initial_tick)) + 1);
    let missing_before =
        earliest_tick.map(|earliest| u64::from(earliest.saturating_sub(initial_tick)));
    let complete = earliest_tick.is_some_and(|earliest| earliest <= initial_tick);
    let remaining = network_head
        .zip(last_indexed_tick)
        .map(|(head, last)| u64::from(head.saturating_sub(last)));
    // Count only ticks that belong to the current epoch so the coverage value does not
    // drift when a previous epoch remains indexed after the epoch boundary.
    let indexed_in_epoch = match (network_head, earliest_tick, last_indexed_tick) {
        (Some(head), Some(earliest), Some(last)) => {
            let start = earliest.max(initial_tick);
            let end = last.min(head);
            (end >= start).then(|| u64::from(end - start) + 1)
        }
        _ => None,
    };
    let coverage = match (indexed_in_epoch, epoch_ticks) {
        (Some(indexed), Some(total)) if total > 0 => {
            Some((indexed as f64 / total as f64).clamp(0.0, 1.0))
        }
        _ => None,
    };
    json!({
        "available": true,
        "complete": complete,
        "initial_tick": initial_tick,
        "first_indexed_tick": earliest_tick,
        "last_indexed_tick": last_indexed_tick,
        "missing_before": missing_before,
        "indexed_ticks": indexed_in_epoch,
        "epoch_ticks": epoch_ticks,
        "remaining_ticks": remaining,
        "coverage": coverage,
        "coverage_percent": coverage.map(|value| (value * 100.0 * 100.0).round() / 100.0),
    })
}

async fn epochs(
    State(state): State<AppState>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    let page = params.resolve("epochs")?;
    let mut rows = state.store.epochs(page.fetch_limit(), page.offset).await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn epoch(
    State(state): State<AppState>,
    Path(epoch): Path<u16>,
) -> Result<impl IntoResponse, ApiError> {
    let row = state
        .store
        .epoch(epoch)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("epoch {epoch} is not indexed")))?;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({ "data": row })),
    ))
}

async fn epoch_computors(
    State(state): State<AppState>,
    Path(epoch): Path<u16>,
) -> Result<impl IntoResponse, ApiError> {
    let rows = state.store.computors(epoch).await?;
    if rows.is_empty() {
        return Err(ApiError::not_found(format!(
            "computors for epoch {epoch} are not indexed"
        )));
    }
    let count = rows.len();
    let rows: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            json!({
                "epoch": row.epoch,
                "computor_index": row.computor_index,
                "public_key": row.public_key,
                "identity": row.identity,
            })
        })
        .collect();
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": rows,
            "meta": { "epoch": epoch, "count": count },
        })),
    ))
}

async fn ticks(
    State(state): State<AppState>,
    Query(params): Query<TickListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let fingerprint = format!("ticks?epoch={:?}", params.epoch);
    let page = params.page().resolve(&fingerprint)?;
    let mut rows = state
        .store
        .ticks(params.epoch, page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

pub async fn not_found() -> ApiError {
    ApiError::not_found("route not found")
}

async fn tick(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
) -> Result<impl IntoResponse, ApiError> {
    let row = state
        .store
        .tick(tick)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("tick {tick} is not indexed")))?;
    Ok((
        [(header::CACHE_CONTROL, IMMUTABLE)],
        Json(json!({ "data": row })),
    ))
}

async fn tick_evidence(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
) -> Result<impl IntoResponse, ApiError> {
    let row =
        state.store.evidence_for_tick(tick).await?.ok_or_else(|| {
            ApiError::not_found(format!("evidence for tick {tick} is not indexed"))
        })?;
    Ok((
        [(header::CACHE_CONTROL, IMMUTABLE)],
        Json(json!({
            "data": {
                "epoch": row.epoch,
                "tick": row.tick,
                "quorum_digest": hex::encode(row.quorum_digest),
                "tick_data_digest": hex::encode(row.tick_data_digest),
                "required_votes": row.required_votes,
                "aligned_votes": row.aligned_votes,
                "fetched_votes": row.fetched_votes,
                "unique_votes": row.unique_votes,
                "verified_votes": row.verified_votes,
                "verification": row.verification,
                "warning_count": row.warning_count,
                "warnings": row.warnings,
                "timestamp": row.timestamp,
            }
        })),
    ))
}

async fn tick_transactions(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    if state.store.tick(tick).await?.is_none() {
        return Err(ApiError::not_found(format!("tick {tick} is not indexed")));
    }
    let page = params.resolve(&format!("ticks/{tick}/transactions"))?;
    let mut rows = state
        .store
        .transactions_for_tick(tick, page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, IMMUTABLE)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn tick_events(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    if state.store.tick(tick).await?.is_none() {
        return Err(ApiError::not_found(format!("tick {tick} is not indexed")));
    }
    let page = params.resolve(&format!("ticks/{tick}/events"))?;
    let mut rows = state
        .store
        .events_for_tick(tick, page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    let rows: Vec<Value> = rows.into_iter().map(event_value).collect();
    Ok((
        [(header::CACHE_CONTROL, IMMUTABLE)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn transaction(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let hash = hash.to_ascii_lowercase();
    let _ =
        qubic::decode_tx_hash(&hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;
    let row = state
        .store
        .transaction(&hash)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("transaction {hash} is not indexed")))?;
    Ok((
        [(header::CACHE_CONTROL, IMMUTABLE)],
        Json(json!({ "data": row })),
    ))
}

async fn transactions_batch(
    State(state): State<AppState>,
    Json(body): Json<BatchTransactions>,
) -> Result<impl IntoResponse, ApiError> {
    if body.hashes.is_empty() {
        return Err(ApiError::bad_request("hashes must not be empty"));
    }
    if body.hashes.len() > 100 {
        return Err(ApiError::bad_request("at most 100 hashes per request"));
    }
    let mut hashes = Vec::with_capacity(body.hashes.len());
    let mut seen = HashSet::new();
    for value in body.hashes {
        let hash = value.trim().to_ascii_lowercase();
        qubic::decode_tx_hash(&hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;
        if seen.insert(hash.clone()) {
            hashes.push(hash);
        }
    }
    let rows = state.store.transactions_by_hashes(&hashes).await?;
    let found: HashMap<String, store::TransactionRow> = rows
        .into_iter()
        .map(|row| (row.hash.clone(), row))
        .collect();
    let data: Vec<Value> = hashes
        .iter()
        .filter_map(|hash| {
            found
                .get(hash)
                .map(|row| serde_json::to_value(row).unwrap_or(Value::Null))
        })
        .collect();
    let missing: Vec<&String> = hashes
        .iter()
        .filter(|hash| !found.contains_key(*hash))
        .collect();
    Ok((
        [(header::CACHE_CONTROL, IMMUTABLE)],
        Json(json!({
            "data": data,
            "meta": { "requested": hashes.len(), "found": data.len(), "missing": missing },
        })),
    ))
}

async fn transactions(
    State(state): State<AppState>,
    Query(params): Query<TransactionQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = match params.identity.as_deref() {
        Some(value) => {
            let value = value.trim().to_ascii_uppercase();
            qubic::PublicKey::from_identity(&value)
                .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
            Some(value)
        }
        None => None,
    };
    let filter = TransactionFilter {
        epoch: params.epoch,
        tick: params.tick,
        identity: identity.clone(),
    };
    let fingerprint = format!(
        "transactions?epoch={:?}&tick={:?}&identity={:?}",
        params.epoch, params.tick, identity
    );
    let page = params.page().resolve(&fingerprint)?;
    let mut rows = state
        .store
        .transactions_page(&filter, page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn events(
    State(state): State<AppState>,
    Query(params): Query<EventQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let filter = EventFilter {
        epoch: params.epoch,
        tick: params.tick,
        type_name: params.event_type.clone(),
    };
    let fingerprint = format!(
        "events?epoch={:?}&tick={:?}&type={:?}",
        params.epoch, params.tick, params.event_type
    );
    let page = params.page().resolve(&fingerprint)?;
    let mut rows = state
        .store
        .events_page(&filter, page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    let rows: Vec<Value> = rows.into_iter().map(event_value).collect();
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn identities(
    State(state): State<AppState>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    let page = params.resolve("identities")?;
    let mut rows = state
        .store
        .identities(page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn identity_transactions(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = identity.to_ascii_uppercase();
    let _ = qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let page = params.resolve(&format!("identities/{identity}/transactions"))?;
    let mut rows = state
        .store
        .transactions_for_identity(&identity, page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn identity_activity(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = identity.trim().to_ascii_uppercase();
    qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let page = params.resolve(&format!("identities/{identity}/activity"))?;
    let mut rows = state
        .store
        .activity_for_identity(&identity, page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn identity_events(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = identity.trim().to_ascii_uppercase();
    qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let page = params.resolve(&format!("identities/{identity}/events"))?;
    let mut rows = state
        .store
        .events_for_identity(&identity, page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn identity(
    State(state): State<AppState>,
    Path(identity): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = identity.to_ascii_uppercase();
    let _ = qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let row = state
        .store
        .identity(&identity)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("identity {identity} is not indexed")))?;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": row,
            "meta": {
                "notice": "balance is the net QU flow observed in the indexed epoch range"
            }
        })),
    ))
}

async fn assets(
    State(state): State<AppState>,
    Query(params): Query<AssetQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let issuer = match params.issuer.as_deref() {
        Some(value) => {
            let value = value.trim().to_ascii_uppercase();
            qubic::PublicKey::from_identity(&value)
                .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
            Some(value)
        }
        None => None,
    };
    let fingerprint = format!("assets?issuer={issuer:?}");
    let page = params.page().resolve(&fingerprint)?;
    let mut rows = state
        .store
        .assets(issuer.as_deref(), page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn asset(
    State(state): State<AppState>,
    Path((issuer, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let issuer = issuer.to_ascii_uppercase();
    let _ = qubic::PublicKey::from_identity(&issuer)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let row = state
        .store
        .asset(&issuer, &name)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("asset {issuer}/{name} is not indexed")))?;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({ "data": row })),
    ))
}

async fn search(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ApiError> {
    let query = params
        .get("q")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("query parameter q is required"))?;

    if query.chars().all(|c| c.is_ascii_digit()) {
        if let Ok(tick) = query.parse::<u32>() {
            if let Some(row) = state.store.tick(tick).await? {
                return Ok(Json(json!({
                    "data": [{ "type": "tick", "tick": tick, "value": row }]
                })));
            }
        }
    }

    let lower = query.to_ascii_lowercase();
    if qubic::decode_tx_hash(&lower).is_ok() {
        if let Some(row) = state.store.transaction(&lower).await? {
            return Ok(Json(json!({
                "data": [{ "type": "transaction", "value": row }]
            })));
        }
    }

    let upper = query.to_ascii_uppercase();
    if qubic::PublicKey::from_identity(&upper).is_ok() {
        if let Some(row) = state.store.identity(&upper).await? {
            return Ok(Json(json!({
                "data": [{ "type": "identity", "value": row }]
            })));
        }
    }

    Ok(Json(json!({ "data": [] })))
}

async fn analytics_activity(
    State(state): State<AppState>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let interval = params.interval.unwrap_or(3600).clamp(1, 86_400);
    let limit = params.limit.unwrap_or(24).clamp(1, 500);
    let rows = state.store.activity_analytics(interval, limit).await?;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": rows,
            "meta": { "interval_seconds": interval, "limit": limit },
        })),
    ))
}

async fn analytics_top_identities(
    State(state): State<AppState>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = params.limit.unwrap_or(25).clamp(1, 200);
    let rows = state.store.top_identities(limit).await?;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({ "data": rows, "meta": { "limit": limit } })),
    ))
}

async fn analytics_event_types(
    State(state): State<AppState>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = params.limit.unwrap_or(50).clamp(1, 200);
    let rows = state.store.event_type_counts(limit).await?;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({ "data": rows, "meta": { "limit": limit } })),
    ))
}

pub async fn docs() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html><head><meta charset="utf-8"><title>Glyph Qubic API</title>
<style>body{font-family:ui-sans-serif,system-ui;max-width:52rem;margin:3rem auto;padding:0 1rem;line-height:1.5}code{background:#f4f4f5;padding:.15rem .35rem;border-radius:.25rem}</style>
</head><body>
<h1>Glyph Qubic API</h1>
<p>Direct-peer Qubic index. The OpenAPI 3.1 document is at <a href="/openapi.json"><code>/openapi.json</code></a>.</p>
<ul>
<li><code>GET /v1/status</code></li>
<li><code>GET /v1/network/stats</code></li>
<li><code>GET /v1/epochs?limit=&amp;cursor=</code></li>
<li><code>GET /v1/epochs/{epoch}</code></li>
<li><code>GET /v1/epochs/{epoch}/computors</code></li>
<li><code>GET /v1/ticks?epoch=&amp;limit=&amp;cursor=</code></li>
<li><code>GET /v1/ticks/{tick}</code></li>
<li><code>GET /v1/ticks/{tick}/evidence</code></li>
<li><code>GET /v1/ticks/{tick}/transactions?limit=&amp;cursor=</code></li>
<li><code>GET /v1/ticks/{tick}/events?limit=&amp;cursor=</code></li>
<li><code>GET /v1/transactions?epoch=&amp;tick=&amp;identity=&amp;limit=&amp;cursor=</code></li>
<li><code>GET /v1/transactions/{hash}</code></li>
<li><code>GET /v1/events?epoch=&amp;tick=&amp;type=&amp;limit=&amp;cursor=</code></li>
<li><code>GET /v1/identities?limit=&amp;cursor=</code></li>
<li><code>GET /v1/identities/{identity}</code></li>
<li><code>GET /v1/identities/{identity}/transactions?limit=&amp;cursor=</code></li>
<li><code>GET /v1/assets?issuer=&amp;limit=&amp;cursor=</code></li>
<li><code>GET /v1/assets/{issuer}/{name}</code></li>
<li><code>GET /v1/assets/{issuer}/{name}/holders?limit=</code></li>
<li><code>GET /v1/wallets/{identity}</code></li>
<li><code>GET /v1/wallets/{identity}/assets?kind=</code></li>
<li><code>GET /v1/wallets/{identity}/snapshot</code></li>
<li><code>GET /v1/prices?from_hour=&amp;limit=</code></li>
<li><code>GET /v1/prices/history?hours=</code></li>
<li><code>GET /v1/prices/latest</code></li>
<li><code>GET /v1/prices/snapshot</code></li>
<li><code>POST /v1/transactions/batch</code></li>
<li><code>POST /v1/transactions</code> (requires <code>ALLOW_BROADCAST=true</code>)</li>
<li><code>GET /v1/analytics/activity?interval=&amp;limit=</code></li>
<li><code>GET /v1/analytics/top-identities?limit=</code></li>
<li><code>GET /v1/analytics/event-types?limit=</code></li>
<li><code>GET /v1/stream?types=</code></li>
<li><code>GET /v1/stream/{ticks|transactions|events}</code></li>
<li><code>GET /v1/ws</code></li>
<li><code>GET /v1/identities/{identity}/entity</code></li>
<li><code>GET /v1/transactions/{hash}/network</code></li>
<li><code>GET /v1/transactions/{hash}/network/status</code></li>
<li><code>POST /v1/transactions/validate</code></li>
<li><code>GET /v1/ticks/{tick}/tx-status</code></li>
<li><code>GET /v1/ticks/{tick}/raw?section=</code></li>
<li><code>POST /v1/contracts/{contract_index}/query</code></li>
<li><code>GET /v1/search?q=</code></li>
</ul>
</body></html>"#,
    )
}

/// Scalar API reference backed by the embedded bundle, so the docs work
/// without a CDN and without a network.
pub async fn scalar_docs() -> Html<String> {
    let config = json!({
        "url": "/openapi.json",
        "layout": "modern",
        "theme": "purple",
        "darkMode": true,
        "agent": { "disabled": true },
    });
    Html(scalar_api_reference::scalar_html(
        &config,
        Some("/scalar.js"),
    ))
}

pub async fn scalar_js() -> Response {
    match scalar_api_reference::get_asset_with_mime("scalar.js") {
        Some((mime, bytes)) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, mime),
                (
                    header::CACHE_CONTROL,
                    "public, max-age=31536000, immutable".to_owned(),
                ),
            ],
            bytes,
        )
            .into_response(),
        None => ApiError::internal("Scalar JavaScript asset is missing").into_response(),
    }
}
