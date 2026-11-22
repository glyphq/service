//! Live network, revenue, and oracle routes.
//!
//! Everything here comes from a peer request message rather than the indexed
//! ClickHouse history. Routes are grouped under `/v1/network` so a client can
//! tell live data from immutable index reads without reading the docs.

use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{TimeZone, Timelike, Utc};
use node::OracleSubscriptionDetail;
use qubic::messages::{
    COMPUTOR_COUNT, ORACLE_QUERY_STATUS_COMMITTED, ORACLE_QUERY_STATUS_PENDING,
    ORACLE_QUERY_STATUS_SUCCESS, ORACLE_QUERY_STATUS_TIMEOUT, ORACLE_QUERY_STATUS_UNKNOWN,
    ORACLE_QUERY_STATUS_UNRESOLVABLE, ORACLE_QUERY_TYPE_CONTRACT_QUERY,
    ORACLE_QUERY_TYPE_CONTRACT_SUBSCRIPTION, ORACLE_QUERY_TYPE_USER_QUERY,
    ORACLE_REQUEST_ALL_QUERY_IDS_BY_TICK, ORACLE_REQUEST_CONTRACT_DIRECT_QUERY_IDS_BY_TICK,
    ORACLE_REQUEST_CONTRACT_SUBSCRIPTION_QUERY_IDS_BY_TICK, ORACLE_REQUEST_PENDING_QUERY_IDS,
    ORACLE_REQUEST_USER_QUERY_IDS_BY_TICK, OracleQueryStatistics, RespondRevenueData, SystemInfo,
};
use serde_json::{Value, json};

use crate::peer;
use crate::{ApiError, AppState};

const RECENT: &str = "public, max-age=2, stale-while-revalidate=5";
const AGGREGATE: &str = "public, max-age=10, stale-while-revalidate=30";
const SYSTEM_TTL: Duration = Duration::from_secs(1);
const REVENUE_TTL: Duration = Duration::from_secs(5);
const ORACLE_TTL: Duration = Duration::from_secs(5);
const PEER_TIMEOUT: Duration = Duration::from_secs(4);
/// Total time allowed to materialize a list of oracle queries or subscriptions.
const ORACLE_LIST_BUDGET: Duration = Duration::from_secs(8);
/// Some epochs begin with ticks carrying the genesis date, so probe a few rows.
const EPOCH_START_PROBE_TICKS: u32 = 32;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/network", get(overview))
        .route("/v1/network/index", get(indexed))
        .route("/v1/network/system", get(system))
        .route("/v1/network/peers", get(peers))
        .route("/v1/network/revenue", get(revenue))
        .route("/v1/network/revenue/{identity}", get(revenue_one))
        .route("/v1/network/oracles", get(oracle_overview))
        .route("/v1/network/oracles/stats", get(oracle_stats))
        .route("/v1/network/oracles/queries", get(oracle_queries))
        .route("/v1/network/oracles/queries/{id}", get(oracle_query))
        .route(
            "/v1/network/oracles/subscriptions",
            get(oracle_subscriptions),
        )
        .route(
            "/v1/network/oracles/subscriptions/{id}",
            get(oracle_subscription),
        )
        .route("/v1/network/oracles/revenue", get(oracle_revenue))
}

#[derive(Debug, Default, serde::Deserialize)]
struct RevenueQuery {
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    sort: Option<String>,
    #[serde(default)]
    identity: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct OracleQueryListParams {
    #[serde(default)]
    tick: Option<u32>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct OracleSubscriptionParams {
    #[serde(default)]
    contract: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

async fn overview(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let (indexed, head, system) = tokio::join!(
        state.store.network_stats(),
        state.store.head(),
        peer::request("peer system info", PEER_TIMEOUT, state.pool.system_info()),
    );
    let indexed = indexed?;
    let head = head?;
    let (system, system_error) = match system {
        Ok(system) => (Some(system), None),
        Err(error) => (None, Some(error.detail)),
    };
    let started_at = match system.as_ref() {
        Some(info) => epoch_started_at(&state, info).await,
        None => None,
    };
    let health = state.pool.health();
    let healthy = health
        .iter()
        .filter(|peer| peer.quarantine_remaining_ms.is_none() && peer.successes > 0)
        .count();
    let quarantined = health
        .iter()
        .filter(|peer| peer.quarantine_remaining_ms.is_some())
        .count();
    let lag_ticks = system
        .as_ref()
        .zip(head.as_ref())
        .map(|(system, head)| system.tick.saturating_sub(head.tick));
    let data = json!({
        "indexed": indexed_json(&indexed),
        "system": system
            .as_ref()
            .map(|info| system_json(info, head.as_ref(), started_at)),
        "peers": {
            "configured": state.peers.len(),
            "healthy": healthy,
            "unknown": health
                .len()
                .saturating_sub(healthy)
                .saturating_sub(quarantined),
            "quarantined": quarantined,
        },
        "sync": {
            "lag_ticks": lag_ticks,
            "indexed_tick": head.as_ref().map(|row| row.tick),
            "network_tick": system.as_ref().map(|info| info.tick),
        },
    });
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": data,
            "meta": {
                "source": "index+peer",
                "peer_error": system_error,
            }
        })),
    ))
}

async fn indexed(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let stats = state.store.network_stats().await?;
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": indexed_json(&stats),
            "meta": { "source": "index" }
        })),
    ))
}

fn indexed_json(stats: &store::NetworkStats) -> Value {
    json!({
        "head": stats.head,
        "epochs": stats.epochs,
        "ticks": stats.ticks,
        "transactions": stats.transactions,
        "events": stats.events,
        "identities": stats.identities,
        "latest_timestamp": stats.latest_timestamp,
        "ingest": stats.ingest,
    })
}

async fn system(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let key = "network:system";
    if let Some(value) = state.live_cache.get(key, SYSTEM_TTL) {
        return Ok(cached_response(value));
    }
    let head = state.store.head().await?;
    let info = peer::request("peer system info", PEER_TIMEOUT, state.pool.system_info()).await?;
    let started_at = epoch_started_at(&state, &info).await;
    let value = system_json(&info, head.as_ref(), started_at);
    state.live_cache.put(key, value.clone());
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": value,
            "meta": { "source": "peer", "cached": false }
        })),
    ))
}

fn cached_response(value: Value) -> ([(axum::http::HeaderName, &'static str); 1], Json<Value>) {
    (
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": value,
            "meta": { "source": "peer", "cached": true }
        })),
    )
}

fn system_json(
    info: &SystemInfo,
    indexed_head: Option<&store::TickRow>,
    started_at: Option<String>,
) -> Value {
    json!({
        "version": info.version,
        "epoch": info.epoch,
        "tick": info.tick,
        "initial_tick": info.initial_tick,
        "latest_created_tick": info.latest_created_tick,
        "epoch_age_ticks": info.tick.saturating_sub(info.initial_tick),
        "started_at": started_at,
        "entity_count": info.number_of_entities,
        "transaction_count": info.number_of_transactions,
        "total_spectrum_amount": info.total_spectrum_amount,
        "dust_threshold": info.current_entity_balance_dust_threshold,
        "solution_threshold": info.solution_threshold,
        "solution_additional_threshold": info.solution_additional_threshold,
        "target_tick_vote_signature": info.target_tick_vote_signature,
        "computor_packet_signature": info.computor_packet_signature,
        "random_mining_seed": hex::encode(info.random_mining_seed),
        "reserves": [info.reserve2, info.reserve3, info.reserve4],
        "indexed": indexed_head.map(|head| json!({
            "tick": head.tick,
            "epoch": head.epoch,
            "lag_ticks": info.tick.saturating_sub(head.tick),
            "timestamp": head.timestamp,
        })),
    })
}

async fn epoch_started_at(state: &AppState, info: &SystemInfo) -> Option<String> {
    let rows = state
        .store
        .ticks_after(info.initial_tick.saturating_sub(1), EPOCH_START_PROBE_TICKS)
        .await
        .unwrap_or_default();
    if let Some(started) = epoch_start_from_rows(&rows) {
        return Some(started);
    }
    let row = state.store.tick(info.initial_tick).await.ok().flatten()?;
    Some(
        row.timestamp
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    )
}

fn epoch_start_from_rows(rows: &[store::TickRow]) -> Option<String> {
    let latest = rows.iter().map(|row| row.timestamp).max()?;
    let cutoff = latest - chrono::TimeDelta::hours(1);
    rows.iter()
        .filter(|row| row.timestamp >= cutoff)
        .min_by_key(|row| row.tick)
        .map(|row| {
            row.timestamp
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        })
}

async fn peers(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let health = state.pool.health();
    let data: Vec<Value> = health
        .into_iter()
        .map(|peer| {
            let label = state
                .peers
                .iter()
                .find(|config| config.addr == peer.addr)
                .and_then(|config| config.label.clone());
            let status = if peer.quarantine_remaining_ms.is_some() {
                "quarantined"
            } else if peer.successes == 0 {
                "unknown"
            } else {
                "healthy"
            };
            json!({
                "address": peer.addr,
                "label": label,
                "status": status,
                "score": peer.score,
                "successes": peer.successes,
                "failures": peer.failures,
                "consecutive_failures": peer.consecutive_failures,
                "last_latency_ms": peer.last_latency_ms,
                "quarantine_remaining_ms": peer.quarantine_remaining_ms,
            })
        })
        .collect();
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": data,
            "meta": { "configured": state.peers.len(), "count": data.len() }
        })),
    ))
}

async fn revenue(
    State(state): State<AppState>,
    Query(mut params): Query<RevenueQuery>,
) -> Result<impl IntoResponse, ApiError> {
    if let Some(identity) = params.identity.as_mut() {
        *identity = identity.trim().to_ascii_uppercase();
        qubic::PublicKey::from_identity(identity)
            .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    }
    let sort = params.sort.clone().unwrap_or_else(|| "reward".to_owned());
    params.sort = Some(
        match sort.as_str() {
            "reward" | "tx_score" | "oracle_score" | "doge_score" | "index" => sort.as_str(),
            "tx" => "tx_score",
            "oracle" => "oracle_score",
            "doge" => "doge_score",
            other => {
                return Err(ApiError::bad_request(format!(
                    "unknown revenue sort {other:?}; use reward, tx_score, oracle_score, doge_score, or index"
                )));
            }
        }
        .to_owned(),
    );
    let key = format!(
        "network:revenue:{:?}:{:?}:{:?}:{:?}",
        params.limit, params.offset, params.sort, params.identity
    );
    let cacheable = params.identity.is_none();
    if cacheable && let Some(value) = state.live_cache.get(&key, REVENUE_TTL) {
        return Ok((
            [(header::CACHE_CONTROL, RECENT)],
            Json(json!({ "data": value, "meta": { "source": "peer", "cached": true } })),
        ));
    }
    let value = build_revenue(&state, &params).await?;
    if cacheable {
        state.live_cache.put(key, value.clone());
    }
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": value,
            "meta": {
                "source": "peer",
                "cached": false,
                "approximate": true,
                "note": "Scores are live node state. They are exact only after the epoch ends."
            }
        })),
    ))
}

async fn revenue_one(
    State(state): State<AppState>,
    Path(identity): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = identity.trim().to_ascii_uppercase();
    qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let params = RevenueQuery {
        identity: Some(identity),
        limit: Some(1),
        ..RevenueQuery::default()
    };
    let value = build_revenue(&state, &params).await?;
    let row = value
        .get("computors")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .cloned()
        .ok_or_else(|| ApiError::not_found("identity is not a computor in the current epoch"))?;
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": row,
            "meta": {
                "source": "peer",
                "approximate": true,
                "epoch": value.get("epoch").cloned().unwrap_or(Value::Null),
                "tick": value.get("tick").cloned().unwrap_or(Value::Null),
            }
        })),
    ))
}

async fn build_revenue(state: &AppState, params: &RevenueQuery) -> Result<Value, ApiError> {
    let system = peer::request("peer system info", PEER_TIMEOUT, state.pool.system_info()).await?;
    let data = peer::request("peer revenue", PEER_TIMEOUT, state.pool.revenue_data()).await?;
    let computors = state
        .store
        .computors(system.epoch)
        .await
        .unwrap_or_default();
    Ok(revenue_json(&system, &data, &computors, params))
}

fn revenue_json(
    system: &SystemInfo,
    data: &RespondRevenueData,
    computors: &[store::ComputorRow],
    params: &RevenueQuery,
) -> Value {
    let tx_quorum = quorum_score(&data.tx_score);
    let oracle_any = data.oracle_score.iter().any(|score| *score != 0);
    let oracle_quorum = oracle_any.then(|| quorum_score(&data.oracle_score));
    let ipc = data.ipc.max(0) as u128;
    let mut rows: Vec<Value> = (0..COMPUTOR_COUNT)
        .map(|index| {
            let tx_score = data.tx_score.get(index).copied().unwrap_or(0);
            let oracle_score = data.oracle_score.get(index).copied().unwrap_or(0);
            let doge_score = data.doge_score.get(index).copied().unwrap_or(0);
            let tx_factor = revenue_factor(tx_score, tx_quorum);
            let oracle_factor = match oracle_quorum {
                Some(quorum) => revenue_factor(oracle_score, quorum),
                None => REVENUE_SCALE,
            };
            // Qubic core currently has REVENUE_DOGE_DISABLED enabled, so the
            // doge root is the scale value. Raw doge scores stay visible for
            // clients that want to model a future where that changes.
            let reward = ipc
                .saturating_mul(u128::from(tx_factor))
                .saturating_mul(u128::from(oracle_factor))
                / (u128::from(REVENUE_SCALE) * u128::from(REVENUE_SCALE));
            let computor = computors.get(index);
            json!({
                "computor_index": index,
                "identity": computor.map(|row| row.identity.clone()),
                "public_key": computor.map(|row| row.public_key.clone()),
                "tx_score": tx_score,
                "oracle_score": oracle_score,
                "doge_score": doge_score,
                "tx_factor": tx_factor,
                "oracle_factor": oracle_factor,
                "estimated_reward": u64::try_from(reward).unwrap_or(u64::MAX),
            })
        })
        .collect();

    let total_reward: u64 = rows
        .iter()
        .filter_map(|row| row.get("estimated_reward").and_then(Value::as_u64))
        .sum();
    for row in &mut rows {
        let reward = row
            .get("estimated_reward")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if let Some(object) = row.as_object_mut() {
            object.insert(
                "share".into(),
                json!(if total_reward == 0 {
                    0.0
                } else {
                    reward as f64 / total_reward as f64
                }),
            );
        }
    }

    if let Some(identity) = &params.identity {
        let identity = identity.to_ascii_uppercase();
        rows.retain(|row| {
            row.get("identity")
                .and_then(Value::as_str)
                .is_some_and(|value| value == identity)
        });
    }

    let sort = params.sort.as_deref().unwrap_or("reward");
    rows.sort_by(|left, right| {
        if sort == "index" {
            let left = left
                .get("computor_index")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let right = right
                .get("computor_index")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            return left.cmp(&right);
        }
        let key = match sort {
            "tx_score" | "tx" => "tx_score",
            "oracle_score" | "oracle" => "oracle_score",
            "doge_score" | "doge" => "doge_score",
            _ => "estimated_reward",
        };
        let left = left.get(key).and_then(Value::as_u64).unwrap_or(0);
        let right = right.get(key).and_then(Value::as_u64).unwrap_or(0);
        right.cmp(&left)
    });

    let total = rows.len();
    let offset = params.offset.unwrap_or(0) as usize;
    let limit = params.limit.unwrap_or(COMPUTOR_COUNT as u32).clamp(1, 676) as usize;
    let page: Vec<Value> = rows.into_iter().skip(offset).take(limit).collect();

    json!({
        "epoch": system.epoch,
        "tick": data.tick,
        "ipc": data.ipc,
        "doge_k": data.doge_k,
        "doge_included": false,
        "quorum_score": {
            "tx": tx_quorum,
            "oracle": oracle_quorum,
        },
        "totals": {
            "estimated_reward": total_reward,
            "computors": total,
            "with_tx_score": data.tx_score.iter().filter(|score| **score != 0).count(),
            "with_oracle_score": data.oracle_score.iter().filter(|score| **score != 0).count(),
            "with_doge_score": data.doge_score.iter().filter(|score| **score != 0).count(),
        },
        "computors": page,
        "pagination": {
            "limit": limit,
            "offset": offset,
            "count": page.len(),
            "has_more": offset + page.len() < total,
        }
    })
}

const REVENUE_SCALE: u64 = 1024;

fn quorum_score(scores: &[u64]) -> u64 {
    let mut sorted = scores.to_vec();
    sorted.sort_unstable_by(|left, right| right.cmp(left));
    sorted
        .get(qubic::messages::QUORUM - 1)
        .copied()
        .unwrap_or(1)
        .max(1)
}

fn revenue_factor(score: u64, quorum: u64) -> u64 {
    if score == 0 {
        return 0;
    }
    if score >= quorum {
        return REVENUE_SCALE;
    }
    ((u128::from(REVENUE_SCALE) * u128::from(score)) / u128::from(quorum))
        .min(u128::from(REVENUE_SCALE)) as u64
}

async fn oracle_overview(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let key = "network:oracles:overview";
    if let Some(value) = state.live_cache.get(key, ORACLE_TTL) {
        return Ok((
            [(header::CACHE_CONTROL, RECENT)],
            Json(json!({ "data": value, "meta": { "source": "peer", "cached": true } })),
        ));
    }
    let mut peer = peer::connect(&state).await?;
    let stats = peer::request(
        "oracle statistics",
        PEER_TIMEOUT,
        peer.oracle_query_statistics(),
    )
    .await?;
    let pending = peer::request(
        "pending oracle queries",
        PEER_TIMEOUT,
        peer.oracle_query_ids(ORACLE_REQUEST_PENDING_QUERY_IDS, 0),
    )
    .await?;
    let subscriptions = peer::request(
        "oracle subscription list",
        PEER_TIMEOUT,
        peer.oracle_subscription_ids(),
    )
    .await?;
    let value = json!({
        "stats": stats_json(&stats),
        "pending_queries": pending.len(),
        "active_subscriptions": subscriptions.len(),
        "queries": stats.user_queries + stats.contract_queries + stats.subscription_queries,
    });
    state.live_cache.put(key, value.clone());
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({ "data": value, "meta": { "source": "peer" } })),
    ))
}

async fn oracle_stats(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let key = "network:oracles:stats";
    if let Some(value) = state.live_cache.get(key, ORACLE_TTL) {
        return Ok((
            [(header::CACHE_CONTROL, RECENT)],
            Json(json!({ "data": value, "meta": { "source": "peer", "cached": true } })),
        ));
    }
    let stats = peer::request(
        "oracle statistics",
        PEER_TIMEOUT,
        state.pool.oracle_query_statistics(),
    )
    .await?;
    let value = stats_json(&stats);
    state.live_cache.put(key, value.clone());
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({ "data": value, "meta": { "source": "peer" } })),
    ))
}

fn stats_json(stats: &OracleQueryStatistics) -> Value {
    json!({
        "pending_count": stats.pending_count,
        "pending_oracle_machine_count": stats.pending_oracle_machine_count,
        "pending_commit_count": stats.pending_commit_count,
        "pending_reveal_count": stats.pending_reveal_count,
        "successful_count": stats.successful_count,
        "reveal_tx_count": stats.reveal_tx_count,
        "unresolvable_count": stats.unresolvable_count,
        "timeout_count": stats.timeout_count,
        "timeout_no_reply_count": stats.timeout_no_reply_count,
        "timeout_no_commit_count": stats.timeout_no_commit_count,
        "timeout_no_reveal_count": stats.timeout_no_reveal_count,
        "oracle_machine_replies_disagree_count": stats.oracle_machine_replies_disagree_count,
        "wrong_knowledge_proof_count": stats.wrong_knowledge_proof_count,
        "user_queries": stats.user_queries,
        "contract_queries": stats.contract_queries,
        "subscription_queries": stats.subscription_queries,
        "averages_milli_ticks": {
            "oracle_machine_reply": stats.oracle_machine_reply_avg_milli_ticks_per_query,
            "commit": stats.commit_avg_milli_ticks_per_query,
            "success": stats.success_avg_milli_ticks_per_query,
            "timeout": stats.timeout_avg_milli_ticks_per_query,
        }
    })
}

async fn oracle_queries(
    State(state): State<AppState>,
    Query(params): Query<OracleQueryListParams>,
) -> Result<impl IntoResponse, ApiError> {
    let (req_type, tick) = oracle_selector(&state, &params).await?;
    let mut peer = peer::connect(&state).await?;
    let ids = peer::request(
        "oracle query IDs",
        PEER_TIMEOUT,
        peer.oracle_query_ids(req_type, i64::from(tick)),
    )
    .await?;
    let offset = params.offset.unwrap_or(0) as usize;
    let limit = params.limit.unwrap_or(25).clamp(1, 100) as usize;
    let total = ids.len();
    let page = ids.into_iter().skip(offset).take(limit);
    let mut rows = Vec::new();
    let started = std::time::Instant::now();
    let mut truncated = false;
    let tick_meta = (req_type != ORACLE_REQUEST_PENDING_QUERY_IDS).then_some(tick);
    for id in page {
        let remaining = ORACLE_LIST_BUDGET.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            truncated = true;
            break;
        }
        match peer::request(
            "oracle query",
            remaining.min(PEER_TIMEOUT),
            peer.oracle_query(id),
        )
        .await
        {
            Ok(response) => rows.push(oracle_query_json(&response)),
            Err(error) => rows.push(json!({
                "query_id": id,
                "error": error.detail,
            })),
        }
    }
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": rows,
            "meta": {
                "source": "peer",
                "tick": tick_meta,
                "limit": limit,
                "offset": offset,
                "count": rows.len(),
                "total": total,
                "has_more": truncated || offset + rows.len() < total,
                "truncated": truncated,
            }
        })),
    ))
}

async fn oracle_selector(
    state: &AppState,
    params: &OracleQueryListParams,
) -> Result<(u32, u32), ApiError> {
    let status = params.status.as_deref().unwrap_or("pending");
    if status == "pending" {
        return Ok((ORACLE_REQUEST_PENDING_QUERY_IDS, 0));
    }
    let tick = match params.tick {
        Some(tick) => tick,
        None => {
            let head = state.store.head().await?;
            head.map(|row| row.tick).ok_or_else(|| {
                ApiError::bad_request("tick is required when the index has no head")
            })?
        }
    };
    let req_type = match status {
        "all" => ORACLE_REQUEST_ALL_QUERY_IDS_BY_TICK,
        "user" => ORACLE_REQUEST_USER_QUERY_IDS_BY_TICK,
        "contract" => ORACLE_REQUEST_CONTRACT_DIRECT_QUERY_IDS_BY_TICK,
        "subscription" => ORACLE_REQUEST_CONTRACT_SUBSCRIPTION_QUERY_IDS_BY_TICK,
        other => {
            return Err(ApiError::bad_request(format!(
                "unknown oracle status {other:?}; use pending, all, user, contract, or subscription"
            )));
        }
    };
    Ok((req_type, tick))
}

async fn oracle_query(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<impl IntoResponse, ApiError> {
    let response = peer::request("oracle query", PEER_TIMEOUT, state.pool.oracle_query(id)).await?;
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": oracle_query_json(&response),
            "meta": { "source": "peer" }
        })),
    ))
}

fn oracle_query_json(response: &node::OracleQueryResponse) -> Value {
    let metadata = &response.metadata;
    json!({
        "query_id": metadata.query_id,
        "type": metadata.query_type,
        "type_name": oracle_query_type_name(metadata.query_type),
        "status": metadata.status,
        "status_name": oracle_status_name(metadata.status),
        "status_flags": metadata.status_flags,
        "status_flag_names": oracle_flag_names(metadata.status_flags),
        "query_tick": metadata.query_tick,
        "querying_entity": qubic::PublicKey(metadata.querying_entity).to_identity(),
        "timeout": metadata.timeout,
        "timeout_at": qpi_timestamp(metadata.timeout),
        "interface_index": metadata.interface_index,
        "interface_name": oracle_interface_name(metadata.interface_index),
        "subscription_id": metadata.subscription_id,
        "reveal_tick": metadata.reveal_tick,
        "total_commits": metadata.total_commits,
        "agreeing_commits": metadata.agreeing_commits,
        "query": response.query.as_ref().map(hex::encode),
        "reply": response.reply.as_ref().map(hex::encode),
        "price": decode_price_oracle(metadata.interface_index, &response.query, &response.reply),
    })
}

fn decode_price_oracle(
    interface_index: u32,
    query: &Option<Vec<u8>>,
    reply: &Option<Vec<u8>>,
) -> Option<Value> {
    if interface_index != 0 {
        return None;
    }
    let query = query.as_ref()?;
    let query = qubic::messages::PriceOracleQuery::decode(query).ok()?;
    let reply = reply
        .as_ref()
        .and_then(|bytes| qubic::messages::PriceOracleReply::decode(bytes).ok());
    Some(json!({
        "oracle": qubic::PublicKey(query.oracle).to_identity(),
        "timestamp": query.timestamp,
        "currency1": qubic::PublicKey(query.currency1).to_identity(),
        "currency2": qubic::PublicKey(query.currency2).to_identity(),
        "numerator": reply.map(|reply| reply.numerator),
        "denominator": reply.map(|reply| reply.denominator),
        "price": reply.and_then(|reply| {
            (reply.denominator != 0).then(|| reply.numerator as f64 / reply.denominator as f64)
        }),
    }))
}

async fn oracle_subscriptions(
    State(state): State<AppState>,
    Query(params): Query<OracleSubscriptionParams>,
) -> Result<impl IntoResponse, ApiError> {
    let mut peer = peer::connect(&state).await?;
    let ids = if let Some(contract) = &params.contract {
        let contract = contract.trim().parse::<u32>().map_err(|_| {
            ApiError::bad_request(format!(
                "invalid contract index {contract:?}: expected an unsigned 32-bit integer"
            ))
        })?;
        peer::request(
            "oracle contract subscriptions",
            PEER_TIMEOUT,
            peer.oracle_active_contract_subscriptions(contract),
        )
        .await?
    } else {
        peer::request(
            "oracle subscription list",
            PEER_TIMEOUT,
            peer.oracle_subscription_ids(),
        )
        .await?
    };
    let limit = params.limit.unwrap_or(25).clamp(1, 100) as usize;
    let total = ids.len();
    let mut rows = Vec::new();
    let started = std::time::Instant::now();
    let mut truncated = false;
    for id in ids.into_iter().take(limit) {
        let remaining = ORACLE_LIST_BUDGET.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            truncated = true;
            break;
        }
        match peer::request(
            "oracle subscription",
            remaining.min(PEER_TIMEOUT),
            peer.oracle_subscription_detail(id),
        )
        .await
        {
            Ok(detail) => rows.push(subscription_json(&detail)),
            Err(error) => rows.push(json!({
                "subscription_id": id,
                "error": error.detail,
            })),
        }
    }
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": rows,
            "meta": {
                "source": "peer",
                "count": rows.len(),
                "total": total,
                "has_more": truncated || total > rows.len(),
                "truncated": truncated,
            }
        })),
    ))
}

async fn oracle_subscription(
    State(state): State<AppState>,
    Path(id): Path<i32>,
) -> Result<impl IntoResponse, ApiError> {
    let detail = peer::request(
        "oracle subscription",
        PEER_TIMEOUT,
        state.pool.oracle_subscription_detail(id),
    )
    .await?;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": subscription_json(&detail),
            "meta": { "source": "peer" }
        })),
    ))
}

fn subscription_json(detail: &OracleSubscriptionDetail) -> Value {
    let subscription = &detail.subscription;
    json!({
        "subscription_id": subscription.subscription_id,
        "interface_index": subscription.interface_index,
        "interface_name": oracle_interface_name(subscription.interface_index),
        "last_pending_query_id": subscription.last_pending_query_id,
        "last_revealed_query_id": subscription.last_revealed_query_id,
        "generated_queries_count": subscription.generated_queries_count,
        "query": hex::encode(&detail.query),
        "price": decode_price_oracle(subscription.interface_index, &Some(detail.query.clone()), &None),
        "subscribers": detail.subscribers.iter().map(|subscriber| {
            json!({
                "subscription_id": subscriber.subscription_id,
                "contract_index": subscriber.contract_index,
                "notification_period_minutes": subscriber.notification_period_minutes,
                "next_query_timestamp": subscriber.next_query_timestamp,
                "next_query_at": qpi_timestamp(subscriber.next_query_timestamp),
            })
        }).collect::<Vec<_>>(),
    })
}

async fn oracle_revenue(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let key = "network:oracles:revenue";
    if let Some(value) = state.live_cache.get(key, ORACLE_TTL) {
        return Ok((
            [(header::CACHE_CONTROL, RECENT)],
            Json(json!({ "data": value, "meta": { "source": "peer", "cached": true } })),
        ));
    }
    let system = peer::request("peer system info", PEER_TIMEOUT, state.pool.system_info()).await?;
    let points = peer::request(
        "oracle revenue",
        PEER_TIMEOUT,
        state.pool.oracle_revenue_points(),
    )
    .await?;
    let computors = state
        .store
        .computors(system.epoch)
        .await
        .unwrap_or_default();
    let total: u64 = points.iter().sum();
    let rows: Vec<Value> = points
        .iter()
        .enumerate()
        .map(|(index, point)| {
            let computor = computors.get(index);
            json!({
                "computor_index": index,
                "identity": computor.map(|row| row.identity.clone()),
                "public_key": computor.map(|row| row.public_key.clone()),
                "points": point,
                "share": if total == 0 { 0.0 } else { *point as f64 / total as f64 },
            })
        })
        .collect();
    let value = json!({
        "epoch": system.epoch,
        "tick": system.tick,
        "total_points": total,
        "computors": rows,
    });
    state.live_cache.put(key, value.clone());
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({ "data": value, "meta": { "source": "peer" } })),
    ))
}

fn oracle_query_type_name(value: u8) -> &'static str {
    match value {
        ORACLE_QUERY_TYPE_CONTRACT_QUERY => "contract_query",
        ORACLE_QUERY_TYPE_CONTRACT_SUBSCRIPTION => "contract_subscription",
        ORACLE_QUERY_TYPE_USER_QUERY => "user_query",
        _ => "unknown",
    }
}

fn oracle_status_name(value: u8) -> &'static str {
    match value {
        ORACLE_QUERY_STATUS_UNKNOWN => "unknown",
        ORACLE_QUERY_STATUS_PENDING => "pending",
        ORACLE_QUERY_STATUS_COMMITTED => "committed",
        ORACLE_QUERY_STATUS_SUCCESS => "success",
        ORACLE_QUERY_STATUS_TIMEOUT => "timeout",
        ORACLE_QUERY_STATUS_UNRESOLVABLE => "unresolvable",
        _ => "unknown",
    }
}

fn oracle_flag_names(flags: u16) -> Vec<&'static str> {
    let mut names = Vec::new();
    if flags & 0x1 != 0 {
        names.push("invalid_oracle");
    }
    if flags & 0x2 != 0 {
        names.push("oracle_unavailable");
    }
    if flags & 0x4 != 0 {
        names.push("invalid_time");
    }
    if flags & 0x8 != 0 {
        names.push("invalid_place");
    }
    if flags & 0x10 != 0 {
        names.push("invalid_argument");
    }
    names
}

fn oracle_interface_name(index: u32) -> &'static str {
    match index {
        0 => "price",
        _ => "unknown",
    }
}

fn qpi_timestamp(value: u64) -> Option<String> {
    if value == 0 {
        return None;
    }
    let year = (value >> 46) as i32;
    let month = ((value >> 42) & 0xF) as u32;
    let day = ((value >> 37) & 0x1F) as u32;
    let hour = ((value >> 32) & 0x1F) as u32;
    let minute = ((value >> 26) & 0x3F) as u32;
    let second = ((value >> 20) & 0x3F) as u32;
    let millisecond = ((value >> 10) & 0x3FF) as u32;
    if year == 0 || month == 0 || day == 0 {
        return None;
    }
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()?
        .with_nanosecond(millisecond * 1_000_000)
        .map(|timestamp| timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_score_uses_the_nineteenth_percentile() {
        let mut scores = vec![1; COMPUTOR_COUNT];
        scores[0] = 1000;
        assert_eq!(quorum_score(&scores), 1);
        let mut scores = vec![0; COMPUTOR_COUNT];
        scores[0] = 5;
        assert_eq!(quorum_score(&scores), 1);
    }

    #[test]
    fn revenue_factor_scales_below_quorum() {
        assert_eq!(revenue_factor(0, 10), 0);
        assert_eq!(revenue_factor(10, 10), 1024);
        assert_eq!(revenue_factor(5, 10), 512);
    }

    #[test]
    fn epoch_start_skips_genesis_dated_ticks() {
        let row = |tick, timestamp| store::TickRow {
            epoch: 240,
            tick,
            timestamp,
            computor_index: 0,
            transaction_count: 0,
            event_count: 0,
            verification: "data".into(),
        };
        let parse = |value| {
            chrono::DateTime::parse_from_rfc3339(value)
                .expect("valid timestamp")
                .with_timezone(&Utc)
        };
        let rows = vec![
            row(100, parse("2022-04-13T12:00:00Z")),
            row(101, parse("2022-04-13T12:00:01Z")),
            row(110, parse("2026-11-18T12:11:18Z")),
            row(111, parse("2026-11-18T12:11:19Z")),
        ];
        assert_eq!(
            epoch_start_from_rows(&rows).as_deref(),
            Some("2026-11-18T12:11:18.000Z")
        );
        assert_eq!(epoch_start_from_rows(&[]), None);
    }

    #[test]
    fn qpi_timestamp_decodes_date_and_time() {
        let value = (2026u64 << 46)
            | (11u64 << 42)
            | (22u64 << 37)
            | (12u64 << 32)
            | (34u64 << 26)
            | (56u64 << 20)
            | (789u64 << 10);
        assert_eq!(
            qpi_timestamp(value).as_deref(),
            Some("2026-11-22T12:34:56.789Z")
        );
    }
}
