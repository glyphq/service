use std::collections::{HashMap, HashSet};

use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::response::{Html, IntoResponse};
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
        .route("/v1/ticks", get(ticks))
        .route("/v1/ticks/{tick}", get(tick))
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
    let head = state.store.head().await?;
    let stats = state.store.stats().await?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({
            "data": {
                "head": head,
                "ingest": stats.get("ingest").cloned().unwrap_or(Value::Null),
                "stats": stats,
            },
            "meta": { "source": "direct-qubic-peers" }
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
            "meta": { "intervalSeconds": interval, "limit": limit },
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
<li><code>GET /v1/ticks?epoch=&amp;limit=&amp;cursor=</code></li>
<li><code>GET /v1/ticks/{tick}</code></li>
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
<li><code>GET /v1/ticks/{tick}/tx-status</code></li>
<li><code>GET /v1/ticks/{tick}/raw?section=</code></li>
<li><code>POST /v1/contracts/{contract_index}/query</code></li>
<li><code>GET /v1/search?q=</code></li>
</ul>
</body></html>"#,
    )
}
