//! Contract discovery, events, and dividend distributions.
//!
//! Contract indices come from decoded event bodies and dividend rows. The API
//! never carries a contract registry: names and descriptions are optional
//! operator labels, and an unlabelled contract is returned by index only.

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use store::ContractDividendRow;

use crate::dto::event_value;
use crate::{ApiError, AppState};

const CACHE_HEADER: &str = "public, max-age=15, stale-while-revalidate=30";

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/contracts", get(list_contracts))
        .route("/v1/contracts/dividends", get(all_dividends))
        .route("/v1/contracts/{contract_index}", get(contract_overview))
        .route(
            "/v1/contracts/{contract_index}/events",
            get(contract_events),
        )
        .route(
            "/v1/contracts/{contract_index}/dividends",
            get(contract_dividends),
        )
}

#[derive(Debug, Default, Deserialize)]
struct ListQuery {
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct DividendsQuery {
    #[serde(default)]
    epoch: Option<u16>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    include: Option<String>,
}

fn cache_header() -> [(header::HeaderName, HeaderValue); 1] {
    [(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_HEADER),
    )]
}

fn parse_index(input: &str) -> Result<u32, ApiError> {
    input
        .trim()
        .parse::<u32>()
        .map_err(|_| ApiError::bad_request(format!("invalid contract index {input:?}")))
}

async fn list_contracts(
    State(state): State<AppState>,
    Query(params): Query<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = params.limit.unwrap_or(50).clamp(1, 200);
    let offset = params.offset.unwrap_or(0);
    let rows = state.store.contracts(limit, offset).await?;
    let data: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            let index = row.contract_index;
            let mut value = serde_json::to_value(row).unwrap_or(Value::Null);
            state.labels.enrich_contract(&mut value, index);
            value
        })
        .collect();
    Ok((
        cache_header(),
        Json(json!({
            "data": data,
            "meta": {
                "limit": limit,
                "offset": offset,
                "count": data.len(),
                "source": "index",
                "discovery": "decoded_event_bodies_and_dividend_rows",
            }
        })),
    ))
}

async fn contract_overview(
    State(state): State<AppState>,
    Path(contract_index): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let index = parse_index(&contract_index)?;
    let (summary, events, dividends) = tokio::join!(
        state.store.contract(index),
        state.store.contract_events(index, 10, 0),
        state.store.contract_dividends(Some(index), None, 50, 0),
    );
    let summary =
        summary?.ok_or_else(|| ApiError::not_found(format!("contract {index} is not indexed")))?;
    let mut summary_value = serde_json::to_value(summary).unwrap_or(Value::Null);
    state.labels.enrich_contract(&mut summary_value, index);
    let data = json!({
        "contract": summary_value,
        "events": events?.into_iter().map(event_value).collect::<Vec<_>>(),
        "dividends": group_dividends(dividends?),
    });
    Ok((
        cache_header(),
        Json(json!({
            "data": data,
            "meta": {
                "contract_index": index,
                "source": "index",
                "events_limit": 10,
                "dividend_rows_limit": 50,
            }
        })),
    ))
}

async fn contract_events(
    State(state): State<AppState>,
    Path(contract_index): Path<String>,
    Query(params): Query<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let index = parse_index(&contract_index)?;
    let limit = params.limit.unwrap_or(50).clamp(1, 200);
    let offset = params.offset.unwrap_or(0);
    let rows = state.store.contract_events(index, limit, offset).await?;
    let data: Vec<Value> = rows.into_iter().map(event_value).collect();
    Ok((
        cache_header(),
        Json(json!({
            "data": data,
            "meta": {
                "contract_index": index,
                "limit": limit,
                "offset": offset,
                "count": data.len(),
                "source": "index",
            }
        })),
    ))
}

async fn contract_dividends(
    State(state): State<AppState>,
    Path(contract_index): Path<String>,
    Query(params): Query<DividendsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let index = parse_index(&contract_index)?;
    dividends_response(&state, Some(index), params).await
}

async fn all_dividends(
    State(state): State<AppState>,
    Query(params): Query<DividendsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    dividends_response(&state, None, params).await
}

async fn dividends_response(
    state: &AppState,
    contract_index: Option<u32>,
    params: DividendsQuery,
) -> Result<Response, ApiError> {
    let limit = params.limit.unwrap_or(100).clamp(1, 500);
    let offset = params.offset.unwrap_or(0);
    let include = params.include.as_deref().unwrap_or("");
    let include_distributions = include
        .split(',')
        .any(|part| part.trim() == "distributions");
    let rows = state
        .store
        .contract_dividends(contract_index, params.epoch, limit, offset)
        .await?;
    let count = rows.len();
    let data = group_dividends(rows);
    if include_distributions {
        return Ok((
            cache_header(),
            Json(json!({
                "data": data,
                "meta": {
                    "contract_index": contract_index,
                    "epoch": params.epoch,
                    "limit": limit,
                    "offset": offset,
                    "count": count,
                    "source": "index",
                    "group_by": "epoch+contract",
                }
            })),
        )
            .into_response());
    }
    let compact: Vec<Value> = data
        .into_iter()
        .map(|mut row| {
            if let Some(object) = row.as_object_mut() {
                object.remove("distributions");
            }
            row
        })
        .collect();
    Ok((
        cache_header(),
        Json(json!({
            "data": compact,
            "meta": {
                "contract_index": contract_index,
                "epoch": params.epoch,
                "limit": limit,
                "offset": offset,
                "count": count,
                "source": "index",
                "group_by": "epoch+contract",
                "include": "epochs",
                "note": "Pass include=distributions to expand each tick-level distribution.",
            }
        })),
    )
        .into_response())
}

#[derive(Default)]
struct DividendGroup {
    epoch: u16,
    contract_index: u32,
    contract: String,
    distributions: Vec<Value>,
    total_amount: i64,
    transfer_count: u64,
    recipient_count: u32,
    share_count: u32,
    amount_per_share: i64,
    first_tick: u32,
    last_tick: u32,
}

/// Groups tick-level distributions into one row per `(epoch, contract)`.
fn group_dividends(rows: Vec<ContractDividendRow>) -> Vec<Value> {
    let mut groups: BTreeMap<(u16, u32), DividendGroup> = BTreeMap::new();
    for row in rows {
        let group = groups
            .entry((row.epoch, row.contract_index))
            .or_insert_with(|| DividendGroup {
                epoch: row.epoch,
                contract_index: row.contract_index,
                contract: row.contract.clone(),
                first_tick: row.tick,
                last_tick: row.tick,
                ..DividendGroup::default()
            });
        group.first_tick = group.first_tick.min(row.tick);
        group.last_tick = group.last_tick.max(row.tick);
        group.total_amount = group.total_amount.saturating_add(row.total_amount);
        group.transfer_count = group
            .transfer_count
            .saturating_add(u64::from(row.transfer_count));
        group.recipient_count = group.recipient_count.max(row.recipient_count);
        group.share_count = group.share_count.max(row.share_count);
        group.amount_per_share = group.amount_per_share.saturating_add(row.amount_per_share);
        group.distributions.push(json!({
            "tick": row.tick,
            "start_log_id": row.start_log_id,
            "total_amount": row.total_amount,
            "transfer_count": row.transfer_count,
            "recipient_count": row.recipient_count,
            "share_count": row.share_count,
            "amount_per_share": row.amount_per_share,
            "fingerprint": hex::encode(row.fingerprint),
            "timestamp": row.timestamp,
        }));
    }
    groups
        .into_values()
        .map(|mut group| {
            group
                .distributions
                .sort_by_key(|value| value["start_log_id"].as_u64());
            let mut fingerprint_input = Vec::new();
            for distribution in &group.distributions {
                if let Some(fingerprint) = distribution["fingerprint"].as_str() {
                    if let Ok(bytes) = hex::decode(fingerprint) {
                        fingerprint_input.extend_from_slice(&bytes);
                    }
                }
            }
            json!({
                "epoch": group.epoch,
                "contract_index": group.contract_index,
                "contract": group.contract,
                "distribution_count": group.distributions.len(),
                "distributions": group.distributions,
                "first_tick": group.first_tick,
                "last_tick": group.last_tick,
                "total_amount": group.total_amount,
                "transfer_count": group.transfer_count,
                "recipient_count": group.recipient_count,
                "share_count": group.share_count,
                "amount_per_share": group.amount_per_share,
                "fingerprint": hex::encode(qubic::crypto::k12::k12_32(&fingerprint_input)),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn row(tick: u32, start_log_id: u64, total: i64, per_share: i64) -> ContractDividendRow {
        ContractDividendRow {
            epoch: 240,
            tick,
            start_log_id,
            contract_index: 13,
            contract: "CONTRACT".into(),
            total_amount: total,
            transfer_count: 2,
            recipient_count: 2,
            share_count: 676,
            amount_per_share: per_share,
            fingerprint: [7u8; 32],
            timestamp: Utc::now(),
            version: 0,
        }
    }

    #[test]
    fn groups_tick_distributions_into_one_epoch_row() {
        let data = group_dividends(vec![row(100, 9, 676, 1), row(101, 20, 1352, 2)]);
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["epoch"], 240);
        assert_eq!(data[0]["distribution_count"], 2);
        assert_eq!(data[0]["distributions"].as_array().unwrap().len(), 2);
        assert_eq!(data[0]["first_tick"], 100);
        assert_eq!(data[0]["last_tick"], 101);
        assert_eq!(data[0]["total_amount"], 2028);
        assert_eq!(data[0]["transfer_count"], 4);
        assert_eq!(data[0]["amount_per_share"], 3);
        assert_eq!(data[0]["fingerprint"].as_str().unwrap().len(), 64);
    }
}
