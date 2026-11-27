//! Generic read batching.
//!
//! One POST can resolve transactions, ticks, epochs, identities, assets, and
//! contracts. Requests are independent and capped; each result carries its own
//! status so a missing item does not fail the whole batch.

use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::routing::post;
use axum::{Json, Router};
use futures_util::future::join_all;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::dto::transaction_value;
use crate::{ApiError, AppState, tracked, wallet};

const MAX_REQUESTS: usize = 100;

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/batch", post(batch))
}

#[derive(Debug, Deserialize)]
struct BatchBody {
    requests: Vec<BatchRequest>,
}

#[derive(Debug, Deserialize)]
struct BatchRequest {
    #[serde(default)]
    id: Option<String>,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    tick: Option<u32>,
    #[serde(default)]
    epoch: Option<u16>,
    #[serde(default)]
    identity: Option<String>,
    #[serde(default)]
    issuer: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    contract_index: Option<u32>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    offset: Option<u64>,
}

async fn batch(
    State(state): State<AppState>,
    payload: Result<Json<BatchBody>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    let body = crate::advanced::json_body(payload)?;
    if body.requests.is_empty() {
        return Err(ApiError::bad_request("requests must not be empty"));
    }
    if body.requests.len() > MAX_REQUESTS {
        return Err(ApiError::bad_request(format!(
            "at most {MAX_REQUESTS} requests are allowed per batch"
        )));
    }
    let futures = body
        .requests
        .into_iter()
        .map(|request| resolve_one(&state, request));
    let data = join_all(futures).await;
    Ok(Json(json!({
        "data": data,
        "meta": { "count": data.len(), "max_requests": MAX_REQUESTS },
    })))
}

async fn resolve_one(state: &AppState, request: BatchRequest) -> Value {
    let id = request.id.clone();
    let kind = request.kind.clone();
    match resolve_value(state, &request).await {
        Ok((value, meta)) => json!({
            "id": id,
            "type": kind,
            "status": 200,
            "data": value,
            "meta": meta,
        }),
        Err(error) => json!({
            "id": id,
            "type": kind,
            "status": error.status.as_u16(),
            "error": {
                "code": error.code,
                "detail": error.detail,
            },
        }),
    }
}

async fn resolve_value(
    state: &AppState,
    request: &BatchRequest,
) -> Result<(Value, Value), ApiError> {
    match request.kind.as_str() {
        "transaction" => {
            let hash = request
                .hash
                .as_deref()
                .ok_or_else(|| ApiError::bad_request("transaction requires hash"))?;
            let resolved = tracked::resolve(state, hash).await?.ok_or_else(|| {
                ApiError::not_found(format!("transaction {hash} is not indexed or tracked"))
            })?;
            let view = tracked::view(&resolved);
            Ok((view.data, view.meta))
        }
        "tick" => {
            let tick = request
                .tick
                .ok_or_else(|| ApiError::bad_request("tick requires tick"))?;
            let row = state
                .store
                .tick(tick)
                .await?
                .ok_or_else(|| ApiError::not_found(format!("tick {tick} is not indexed")))?;
            Ok((serde_json::to_value(row).unwrap_or(Value::Null), json!({})))
        }
        "epoch" => {
            let epoch = request
                .epoch
                .ok_or_else(|| ApiError::bad_request("epoch requires epoch"))?;
            let row = state
                .store
                .epoch(epoch)
                .await?
                .ok_or_else(|| ApiError::not_found(format!("epoch {epoch} is not indexed")))?;
            Ok((serde_json::to_value(row).unwrap_or(Value::Null), json!({})))
        }
        "identity" => {
            let identity = required_identity(request)?;
            let row = state.store.identity(&identity).await?.ok_or_else(|| {
                ApiError::not_found(format!("identity {identity} is not indexed"))
            })?;
            let mut value = serde_json::to_value(row).unwrap_or(Value::Null);
            state.labels.enrich_identity(&mut value, &identity);
            Ok((value, json!({ "identity": identity })))
        }
        "asset" => {
            let (issuer, name) = asset_key(request)?;
            let row = state.store.asset(&issuer, &name).await?.ok_or_else(|| {
                ApiError::not_found(format!("asset {issuer}/{name} is not indexed"))
            })?;
            let mut value = serde_json::to_value(row).unwrap_or(Value::Null);
            state.labels.enrich_asset(&mut value, &issuer, &name);
            Ok((value, json!({ "issuer": issuer, "name": name })))
        }
        "contract" => {
            let index = required_contract(request)?;
            let row =
                state.store.contract(index).await?.ok_or_else(|| {
                    ApiError::not_found(format!("contract {index} is not indexed"))
                })?;
            let mut value = serde_json::to_value(row).unwrap_or(Value::Null);
            state.labels.enrich_contract(&mut value, index);
            Ok((value, json!({ "contract_index": index })))
        }
        "contract_events" => {
            let index = required_contract(request)?;
            let limit = request.limit.unwrap_or(20).clamp(1, 200);
            let offset = request.offset.unwrap_or(0);
            let rows = state.store.contract_events(index, limit, offset).await?;
            let values: Vec<Value> = rows.into_iter().map(crate::dto::event_value).collect();
            Ok((
                Value::Array(values),
                json!({ "contract_index": index, "limit": limit, "offset": offset }),
            ))
        }
        "contract_dividends" => {
            let index = required_contract(request)?;
            let limit = request.limit.unwrap_or(20).clamp(1, 200);
            let rows = state
                .store
                .contract_dividends(
                    Some(index),
                    request.epoch,
                    limit,
                    request.offset.unwrap_or(0),
                )
                .await?;
            let values: Vec<Value> = rows
                .into_iter()
                .map(|row| {
                    json!({
                        "epoch": row.epoch,
                        "tick": row.tick,
                        "start_log_id": row.start_log_id,
                        "contract_index": row.contract_index,
                        "contract": row.contract,
                        "total_amount": row.total_amount,
                        "transfer_count": row.transfer_count,
                        "recipient_count": row.recipient_count,
                        "share_count": row.share_count,
                        "amount_per_share": row.amount_per_share,
                        "fingerprint": hex::encode(row.fingerprint),
                        "timestamp": row.timestamp,
                    })
                })
                .collect();
            Ok((
                Value::Array(values),
                json!({ "contract_index": index, "limit": limit }),
            ))
        }
        "transactions" => {
            let identity = required_identity(request)?;
            let limit = request.limit.unwrap_or(20).clamp(1, 100);
            let rows = state
                .store
                .transactions_for_identity(&identity, limit, request.offset.unwrap_or(0))
                .await?;
            let values: Vec<Value> = rows.iter().map(transaction_value).collect();
            Ok((
                Value::Array(values),
                json!({ "identity": identity, "limit": limit }),
            ))
        }
        other => Err(ApiError::bad_request(format!(
            "unknown batch request type {other:?}"
        ))),
    }
}

fn required_identity(request: &BatchRequest) -> Result<String, ApiError> {
    let identity = request
        .identity
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("request requires identity"))?;
    Ok(wallet::parse_identity(identity)?.0)
}

fn asset_key(request: &BatchRequest) -> Result<(String, String), ApiError> {
    let issuer = request
        .issuer
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("asset request requires issuer"))?;
    let name = request
        .name
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("asset request requires name"))?;
    let (issuer, _) = wallet::parse_identity(issuer)?;
    let name = wallet::normalize_asset_name(name)?;
    Ok((issuer, name))
}

fn required_contract(request: &BatchRequest) -> Result<u32, ApiError> {
    request
        .contract_index
        .ok_or_else(|| ApiError::bad_request("request requires contract_index"))
}
