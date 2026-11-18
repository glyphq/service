//! Identity intelligence derived from the local transaction index.
//!
//! These routes do not connect to a peer. They answer the two questions an
//! explorer or wallet shows next to an address: who does this identity transact
//! with, and how did its QU flow change over time.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, header};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::json;

use crate::{ApiError, AppState};

const SHORT_CACHE: &str = "public, max-age=5, stale-while-revalidate=10";
const DEFAULT_INTERVAL_SECONDS: u32 = 3600;
const DEFAULT_BUCKETS: u32 = 168;

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/identities/{identity}/counterparties",
            get(counterparties),
        )
        .route("/v1/identities/{identity}/flow", get(flow))
}

#[derive(Debug, Default, Deserialize)]
struct CounterpartiesQuery {
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    direction: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct FlowQuery {
    #[serde(default)]
    interval: Option<u32>,
    #[serde(default)]
    limit: Option<u32>,
}

fn parse_identity(input: &str) -> Result<String, ApiError> {
    let identity = input.trim().to_ascii_uppercase();
    qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    Ok(identity)
}

async fn counterparties(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(params): Query<CounterpartiesQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = parse_identity(&identity)?;
    let direction = match params.direction.as_deref() {
        None | Some("") | Some("all") => None,
        Some("incoming") | Some("in") => Some("incoming"),
        Some("outgoing") | Some("out") => Some("outgoing"),
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "unknown direction {other:?}; expected incoming, outgoing, or all"
            )));
        }
    };
    let limit = params.limit.unwrap_or(20).clamp(1, 100);
    let rows = state
        .store
        .counterparties(&identity, direction, limit)
        .await?;
    Ok((
        [(header::CACHE_CONTROL, HeaderValue::from_static(SHORT_CACHE))],
        Json(json!({
            "data": rows,
            "meta": {
                "identity": identity,
                "direction": direction.unwrap_or("all"),
                "limit": limit,
                "count": rows.len(),
            }
        })),
    ))
}

async fn flow(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(params): Query<FlowQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = parse_identity(&identity)?;
    let interval = params
        .interval
        .unwrap_or(DEFAULT_INTERVAL_SECONDS)
        .clamp(60, 86_400);
    let limit = params.limit.unwrap_or(DEFAULT_BUCKETS).clamp(1, 1000);
    let head = state
        .store
        .head()
        .await?
        .ok_or_else(|| ApiError::not_found("the index has no ticks yet"))?;

    let interval_seconds = i64::from(interval);
    let head_seconds = head.timestamp.timestamp();
    let latest_bucket = head_seconds - head_seconds.rem_euclid(interval_seconds);
    let start_seconds = latest_bucket - interval_seconds * i64::from(limit - 1);
    let from = DateTime::from_timestamp(start_seconds, 0)
        .ok_or_else(|| ApiError::internal("flow window is outside the timestamp range"))?;
    let rows = state
        .store
        .identity_flow(&identity, interval, from, limit)
        .await?;
    let by_bucket: HashMap<i64, store::FlowBucket> = rows
        .into_iter()
        .map(|row| (row.bucket.timestamp(), row))
        .collect();

    let mut data = Vec::with_capacity(limit as usize);
    let mut total_incoming = 0i64;
    let mut total_outgoing = 0i64;
    let mut total_transactions = 0u64;
    for index in 0..limit {
        let seconds = latest_bucket - interval_seconds * i64::from(index);
        let bucket = Utc
            .timestamp_opt(seconds, 0)
            .single()
            .ok_or_else(|| ApiError::internal("flow bucket is outside the timestamp range"))?;
        let row = by_bucket.get(&seconds);
        total_incoming = total_incoming.saturating_add(row.map_or(0, |row| row.incoming_amount));
        total_outgoing = total_outgoing.saturating_add(row.map_or(0, |row| row.outgoing_amount));
        total_transactions =
            total_transactions.saturating_add(row.map_or(0, |row| row.transaction_count));
        data.push(match row {
            Some(row) => json!(row),
            None => json!({
                "bucket": bucket,
                "incoming_amount": 0,
                "outgoing_amount": 0,
                "net_amount": 0,
                "transaction_count": 0,
                "counterparty_count": 0,
            }),
        });
    }

    let end = Utc
        .timestamp_opt(latest_bucket + interval_seconds, 0)
        .single()
        .ok_or_else(|| ApiError::internal("flow window is outside the timestamp range"))?;
    Ok((
        [(header::CACHE_CONTROL, HeaderValue::from_static(SHORT_CACHE))],
        Json(json!({
            "data": data,
            "meta": {
                "identity": identity,
                "interval_seconds": interval,
                "limit": limit,
                "count": data.len(),
                "from": from,
                "to": end,
                "totals": {
                    "incoming_amount": total_incoming,
                    "outgoing_amount": total_outgoing,
                    "net_amount": total_incoming.saturating_sub(total_outgoing),
                    "transaction_count": total_transactions,
                },
            }
        })),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_validation_is_case_insensitive() {
        let identity = "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK";
        assert_eq!(
            parse_identity(&identity.to_ascii_lowercase()).unwrap(),
            identity
        );
        assert!(parse_identity("not-an-identity").is_err());
    }
}
