use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::{ApiError, AppState};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/status", get(status))
        .route("/v1/ticks/{tick}", get(tick))
        .route("/v1/transactions/{hash}", get(transaction))
        .route("/v1/identities/{identity}", get(identity))
        .with_state(state)
}

async fn healthz() -> Json<Value> {
    Json(json!({ "ok": true }))
}

async fn status(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let head = state.store.head().await?;
    let stats = state.store.stats().await?;
    Ok(Json(json!({
        "data": {
            "head": head,
            "stats": stats
        }
    })))
}

async fn tick(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
) -> Result<Json<Value>, ApiError> {
    let row = state
        .store
        .tick(tick)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("tick {tick} is not indexed")))?;
    Ok(Json(json!({ "data": row })))
}

async fn transaction(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let row = state
        .store
        .transaction(&hash)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("transaction {hash} is not indexed")))?;
    Ok(Json(json!({ "data": row })))
}

async fn identity(
    State(state): State<AppState>,
    Path(identity): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let row = state
        .store
        .identity(&identity)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("identity {identity} is not indexed")))?;
    Ok(Json(json!({ "data": row })))
}
