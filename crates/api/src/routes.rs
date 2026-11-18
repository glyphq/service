use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};

use crate::dto::{PageParams, event_value, page_meta};
use crate::{ApiError, AppState};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/status", get(status))
        .route("/v1/ticks/{tick}", get(tick))
        .route("/v1/ticks/{tick}/transactions", get(tick_transactions))
        .route("/v1/ticks/{tick}/events", get(tick_events))
        .route("/v1/transactions", get(recent_transactions))
        .route("/v1/transactions/{hash}", get(transaction))
        .route("/v1/events", get(recent_events))
        .route(
            "/v1/identities/{identity}/transactions",
            get(identity_transactions),
        )
        .route("/v1/identities/{identity}", get(identity))
        .route("/v1/search", get(search))
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
        [(header::CACHE_CONTROL, "public, max-age=31536000, immutable")],
        Json(json!({ "data": row })),
    ))
}

async fn tick_transactions(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
    Query(page): Query<PageParams>,
) -> Result<Json<Value>, ApiError> {
    if state.store.tick(tick).await?.is_none() {
        return Err(ApiError::not_found(format!("tick {tick} is not indexed")));
    }
    let limit = page.limit();
    let offset = page.offset();
    let mut rows = state
        .store
        .transactions_for_tick(tick, limit.saturating_add(1), offset)
        .await?;
    let has_more = rows.len() > limit as usize;
    if has_more {
        rows.truncate(limit as usize);
    }
    Ok(Json(json!({
        "data": rows,
        "meta": page_meta(limit, offset, rows.len(), has_more),
    })))
}

async fn tick_events(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
    Query(page): Query<PageParams>,
) -> Result<Json<Value>, ApiError> {
    if state.store.tick(tick).await?.is_none() {
        return Err(ApiError::not_found(format!("tick {tick} is not indexed")));
    }
    let limit = page.limit();
    let offset = page.offset();
    let mut rows = state
        .store
        .events_for_tick(tick, limit.saturating_add(1), offset)
        .await?;
    let has_more = rows.len() > limit as usize;
    if has_more {
        rows.truncate(limit as usize);
    }
    let rows: Vec<Value> = rows.into_iter().map(event_value).collect();
    Ok(Json(json!({
        "data": rows,
        "meta": page_meta(limit, offset, rows.len(), has_more),
    })))
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
        [(header::CACHE_CONTROL, "public, max-age=31536000, immutable")],
        Json(json!({ "data": row })),
    ))
}

async fn recent_transactions(
    State(state): State<AppState>,
    Query(page): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = page.limit();
    let offset = page.offset();
    let mut rows = state
        .store
        .recent_transactions(limit.saturating_add(1), offset)
        .await?;
    let has_more = rows.len() > limit as usize;
    if has_more {
        rows.truncate(limit as usize);
    }
    Ok((
        [(
            header::CACHE_CONTROL,
            "public, max-age=2, stale-while-revalidate=5",
        )],
        Json(json!({
            "data": rows,
            "meta": page_meta(limit, offset, rows.len(), has_more),
        })),
    ))
}

async fn recent_events(
    State(state): State<AppState>,
    Query(page): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = page.limit();
    let offset = page.offset();
    let mut rows = state
        .store
        .recent_events(limit.saturating_add(1), offset)
        .await?;
    let has_more = rows.len() > limit as usize;
    if has_more {
        rows.truncate(limit as usize);
    }
    let rows: Vec<Value> = rows.into_iter().map(event_value).collect();
    Ok((
        [(
            header::CACHE_CONTROL,
            "public, max-age=2, stale-while-revalidate=5",
        )],
        Json(json!({
            "data": rows,
            "meta": page_meta(limit, offset, rows.len(), has_more),
        })),
    ))
}

async fn identity_transactions(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(page): Query<PageParams>,
) -> Result<Json<Value>, ApiError> {
    let identity = identity.to_ascii_uppercase();
    let _ = qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let limit = page.limit();
    let offset = page.offset();
    let mut rows = state
        .store
        .transactions_for_identity(&identity, limit.saturating_add(1), offset)
        .await?;
    let has_more = rows.len() > limit as usize;
    if has_more {
        rows.truncate(limit as usize);
    }
    Ok(Json(json!({
        "data": rows,
        "meta": page_meta(limit, offset, rows.len(), has_more),
    })))
}

async fn identity(
    State(state): State<AppState>,
    Path(identity): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let identity = identity.to_ascii_uppercase();
    let _ = qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let row = state
        .store
        .identity(&identity)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("identity {identity} is not indexed")))?;
    Ok(Json(json!({
        "data": row,
        "meta": {
            "notice": "balance is the net QU flow observed in the indexed epoch range"
        }
    })))
}

async fn search(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
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
<li><code>GET /v1/ticks/{tick}</code></li>
<li><code>GET /v1/ticks/{tick}/transactions</code></li>
<li><code>GET /v1/ticks/{tick}/events</code></li>
<li><code>GET /v1/transactions?limit=&amp;offset=</code></li>
<li><code>GET /v1/transactions/{hash}</code></li>
<li><code>GET /v1/events?limit=&amp;offset=</code></li>
<li><code>GET /v1/identities/{identity}</code></li>
<li><code>GET /v1/identities/{identity}/transactions</code></li>
<li><code>GET /v1/search?q=</code></li>
</ul>
</body></html>"#,
    )
}
