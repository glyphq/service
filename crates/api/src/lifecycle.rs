//! Transaction lifecycle, inclusion risk, and tick capacity.
//!
//! Qubic has no public pending-transaction pool. A transaction names a target
//! tick and is either included in that tick or not. These routes combine the
//! indexed transaction, Glyph's own mempool, peer confirmation, and recent
//! tick fill to answer the questions an explorer normally cannot answer:
//! "did it land?", "did it move money?", and "is this target tick realistic?".

use std::collections::HashMap;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use qubic::messages::TX_PER_TICK;
use serde_json::{Value, json};

use crate::dto::transaction_value;
use crate::transactions_ext::{effect_json, effect_summary};
use crate::{ApiError, AppState, peer};

const RECENT: &str = "public, max-age=2, stale-while-revalidate=5";
const AGGREGATE: &str = "public, max-age=30, stale-while-revalidate=60";
const PEER_TIMEOUT: Duration = Duration::from_secs(2);
/// A transaction lives in the node's pending pool for about ten minutes. At a
/// one-second tick, a target further away than this is likely to be dropped.
const PENDING_HORIZON_TICKS: u32 = 600;
const RISK_SAMPLE_TICKS: u32 = 120;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/transactions/inclusion-risk", get(inclusion_risk))
        .route("/v1/transactions/{hash}/lifecycle", get(lifecycle))
        .route("/v1/ticks/{tick}/capacity", get(tick_capacity))
}

#[derive(Debug, Default, serde::Deserialize)]
struct RiskQuery {
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    target_tick: Option<u32>,
    #[serde(default)]
    amount: Option<i64>,
}

async fn lifecycle(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let hash = hash.trim().to_ascii_lowercase();
    let digest =
        qubic::decode_tx_hash(&hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;

    let indexed = state.store.transaction(&hash).await?;
    let mempool = match &state.mempool {
        Some(mempool) => mempool.status(&hash).await.unwrap_or(None),
        None => None,
    };
    let mut network_error = None;
    let network = if indexed.is_none() && mempool.is_none() {
        match peer::request(
            "peer transaction lookup",
            PEER_TIMEOUT,
            state.pool.transaction(digest),
        )
        .await
        {
            Ok(transaction) => transaction,
            Err(error) => {
                network_error = Some(error.detail);
                None
            }
        }
    } else {
        None
    };

    if indexed.is_none() && mempool.is_none() && network.is_none() {
        return Err(ApiError::not_found(format!(
            "transaction {hash} is not indexed, queued, or known to a peer"
        )));
    }

    let target_tick = indexed
        .as_ref()
        .map(|row| row.tick)
        .or_else(|| mempool.as_ref().map(|entry| entry.tick))
        .or_else(|| network.as_ref().map(|transaction| transaction.tick));
    let included_tick = indexed
        .as_ref()
        .map(|row| row.tick)
        .or_else(|| mempool.as_ref().and_then(|entry| entry.confirmed_tick))
        .or_else(|| network.as_ref().map(|transaction| transaction.tick));
    let current = current_tick(&state).await;
    let money_flew = match included_tick {
        Some(tick) => peer_money_flew(&state, tick, &digest).await,
        None => None,
    };

    let events = if indexed.is_some() {
        state
            .store
            .events_for_transaction(&hash)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(crate::dto::event_value)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let effects: Vec<Value> = events.iter().filter_map(effect_json).collect();
    let summary = effect_summary(&events);
    let errors: Vec<Value> = events
        .iter()
        .filter(|event| event.get("type_name").and_then(Value::as_str) == Some("contract_error"))
        .cloned()
        .collect();

    let execution = execution_json(&events, money_flew);
    let state_name = lifecycle_state(
        indexed.is_some(),
        mempool.as_ref(),
        included_tick,
        target_tick,
        current,
    );
    let confirmations = included_tick
        .zip(current)
        .map(|(included, current)| current.saturating_sub(included));

    let data = json!({
        "hash": hash,
        "state": state_name,
        "target_tick": target_tick,
        "included_tick": included_tick,
        "current_tick": current,
        "confirmations": confirmations,
        "money_flew": money_flew,
        "transaction": indexed.as_ref().map(transaction_value),
        "execution": execution,
        "events": events,
        "effects": effects,
        "summary": summary,
        "errors": errors,
        "tracking": mempool.as_ref().map(crate::mempool::entry_json),
        "network": network.as_ref().map(network_transaction_json),
        "timeline": timeline_json(
            indexed.as_ref(),
            mempool.as_ref(),
            target_tick,
            included_tick,
            current,
        ),
    });
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": data,
            "meta": {
                "source": if indexed.is_some() { "index" } else if mempool.is_some() { "tracking" } else { "peer" },
                "peer_error": network_error,
                "pending_pool_public": false,
                "note": "money_flew is only known when a peer running the tx-status add-on answers. It is null otherwise, not false."
            }
        })),
    ))
}

fn lifecycle_state(
    indexed: bool,
    mempool: Option<&store::gateway::MempoolEntry>,
    included_tick: Option<u32>,
    target_tick: Option<u32>,
    current_tick: Option<u32>,
) -> &'static str {
    if indexed || included_tick.is_some() {
        return "confirmed";
    }
    if let Some(entry) = mempool {
        return match entry.status {
            store::gateway::MempoolStatus::Queued => "queued",
            store::gateway::MempoolStatus::Broadcast => "broadcast",
            store::gateway::MempoolStatus::Confirmed => "confirmed",
            store::gateway::MempoolStatus::Expired => "expired",
            store::gateway::MempoolStatus::Failed => "failed",
            store::gateway::MempoolStatus::Cancelled => "cancelled",
        };
    }
    if let (Some(target), Some(current)) = (target_tick, current_tick) {
        if target <= current {
            return "missed";
        }
    }
    if target_tick.is_some() {
        return "pending";
    }
    "unknown"
}

fn execution_json(events: &[Value], money_flew: Option<bool>) -> Value {
    let error_count = events
        .iter()
        .filter(|event| event.get("type_name").and_then(Value::as_str) == Some("contract_error"))
        .count();
    let status = if error_count > 0 {
        "failed"
    } else if money_flew == Some(false) {
        "no_effect"
    } else if money_flew == Some(true) {
        "success"
    } else if events.is_empty() {
        "unknown"
    } else {
        "executed"
    };
    json!({
        "status": status,
        "error_count": error_count,
        "event_count": events.len(),
        "money_flew": money_flew,
    })
}

fn network_transaction_json(transaction: &qubic::messages::Transaction) -> Value {
    json!({
        "hash": qubic::encode_tx_hash(&transaction.hash()),
        "source": qubic::PublicKey(transaction.source).to_identity(),
        "destination": qubic::PublicKey(transaction.destination).to_identity(),
        "amount": transaction.amount,
        "target_tick": transaction.tick,
        "tick": transaction.tick,
        "input_type": transaction.input_type,
        "input_data": hex::encode(&transaction.input),
    })
}

fn timeline_json(
    indexed: Option<&store::TransactionRow>,
    mempool: Option<&store::gateway::MempoolEntry>,
    target_tick: Option<u32>,
    included_tick: Option<u32>,
    current_tick: Option<u32>,
) -> Vec<Value> {
    let mut timeline = Vec::new();
    if let Some(entry) = mempool {
        timeline.push(json!({
            "stage": "queued",
            "tick": entry.tick,
            "timestamp": entry.created_at,
        }));
        if let Some(at) = entry.broadcast_at {
            timeline.push(json!({
                "stage": "broadcast",
                "tick": entry.tick,
                "timestamp": at,
                "peers": entry.broadcast_peers,
            }));
        }
        if let Some(tick) = entry.confirmed_tick {
            timeline.push(json!({
                "stage": "included",
                "tick": tick,
                "timestamp": entry.confirmed_at,
            }));
        }
    }
    if let Some(row) = indexed {
        timeline.push(json!({
            "stage": "included",
            "tick": row.tick,
            "timestamp": row.timestamp,
        }));
    }
    let already_included =
        indexed.is_some() || mempool.and_then(|entry| entry.confirmed_tick).is_some();
    if !already_included {
        if let Some(tick) = included_tick {
            timeline.push(json!({
                "stage": "included",
                "tick": tick,
            }));
        }
    }
    if let Some(tick) = target_tick {
        if included_tick.is_none() {
            timeline.push(json!({
                "stage": "target_tick",
                "tick": tick,
            }));
        }
    }
    if let (Some(included), Some(current)) = (included_tick, current_tick) {
        if current > included {
            timeline.push(json!({
                "stage": "confirmed",
                "tick": current,
                "confirmations": current - included,
            }));
        }
    }
    timeline
}

async fn peer_money_flew(state: &AppState, tick: u32, digest: &[u8; 32]) -> Option<bool> {
    let status = peer::request(
        "peer transaction status",
        PEER_TIMEOUT,
        state.pool.tx_status(tick),
    )
    .await
    .ok()?;
    status.money_flew_for(digest)
}

async fn current_tick(state: &AppState) -> Option<u32> {
    match peer::request("peer system info", PEER_TIMEOUT, state.pool.system_info()).await {
        Ok(info) => Some(info.tick),
        _ => state.store.head().await.ok().flatten().map(|row| row.tick),
    }
}

async fn inclusion_risk(
    State(state): State<AppState>,
    Query(params): Query<RiskQuery>,
) -> Result<impl IntoResponse, ApiError> {
    if let Some(amount) = params.amount {
        if amount < 0 {
            return Err(ApiError::bad_request("amount must be zero or positive"));
        }
    }
    let source = match params.source.as_deref() {
        Some(value) => {
            let value = value.trim().to_ascii_uppercase();
            qubic::PublicKey::from_identity(&value)
                .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
            Some(value)
        }
        None => None,
    };

    let head = state.store.head().await?;
    let current = current_tick(&state).await;
    if current.is_none() && head.is_none() {
        return Err(ApiError::unavailable(
            "current tick is unavailable: no peer answered and the index is empty",
        ));
    }
    let target_tick = params
        .target_tick
        .or_else(|| current.map(|tick| tick.saturating_add(1)));
    let ticks_until_target = target_tick.zip(current).map(|(target, current)| {
        if target >= current {
            i64::from(target - current)
        } else {
            -i64::from(current - target)
        }
    });

    let sample = recent_ticks(&state, head.as_ref().map(|row| row.epoch)).await;
    let average_transactions = if sample.is_empty() {
        0.0
    } else {
        sample
            .iter()
            .map(|tick| f64::from(tick.transaction_count))
            .sum::<f64>()
            / sample.len() as f64
    };
    let average_fill = average_transactions / TX_PER_TICK as f64;
    let max_transactions = sample
        .iter()
        .map(|tick| tick.transaction_count)
        .max()
        .unwrap_or(0);
    let max_fill = f64::from(max_transactions) / TX_PER_TICK as f64;
    let mut sorted_counts: Vec<u32> = sample.iter().map(|tick| tick.transaction_count).collect();
    sorted_counts.sort_unstable();
    let p95 = percentile(&sorted_counts, 0.95);
    let p95_fill = f64::from(p95) / TX_PER_TICK as f64;

    let indexed_identity = match &source {
        Some(identity) => state.store.identity(identity).await?,
        None => None,
    };
    let mut live_balance = None;
    let mut peer_error = None;
    if let Some(identity) = &source {
        match peer::request(
            "peer entity lookup",
            PEER_TIMEOUT,
            state.pool.entity_by_identity(identity),
        )
        .await
        {
            Ok(response) if response.spectrum_index >= 0 => {
                live_balance = Some(
                    response
                        .entity
                        .incoming_amount
                        .saturating_sub(response.entity.outgoing_amount),
                );
            }
            Ok(_) => {}
            Err(error) => peer_error = Some(error.detail),
        }
    }
    let balance = live_balance.or_else(|| indexed_identity.as_ref().map(|row| row.balance));
    let recent_activity = match &source {
        Some(identity) => {
            let rows = state
                .store
                .transactions_for_identity(identity, 200, 0)
                .await?;
            let floor = current.map(|tick| tick.saturating_sub(PENDING_HORIZON_TICKS));
            rows.iter()
                .filter(|row| floor.is_none_or(|floor| row.tick >= floor))
                .count()
        }
        None => 0,
    };

    let mut factors = Vec::new();
    let mut score = 0.08f64;
    factors.push(json!({
        "name": "baseline",
        "weight": 0.08,
        "value": 0.08,
        "detail": "Unavoidable inclusion uncertainty.",
    }));

    let fill_weight = 0.55;
    let fill_component = (average_fill * 0.6 + p95_fill * 0.4).clamp(0.0, 1.0) * fill_weight;
    score += fill_component;
    factors.push(json!({
        "name": "tick_capacity",
        "weight": fill_weight,
        "value": fill_component,
        "detail": format!(
            "Recent ticks average {:.1} of {} transaction slots ({:.1}% fill). p95 is {} ({:.1}%).",
            average_transactions,
            TX_PER_TICK,
            average_fill * 100.0,
            p95,
            p95_fill * 100.0
        ),
        "observed": {
            "sample_ticks": sample.len(),
            "average_transactions": average_transactions,
            "average_fill": average_fill,
            "p95_transactions": p95,
            "p95_fill": p95_fill,
            "max_transactions": max_transactions,
            "max_fill": max_fill,
        }
    }));

    if let Some(distance) = ticks_until_target {
        if distance <= 0 {
            score = 1.0;
            factors.push(json!({
                "name": "target_tick",
                "weight": 1.0,
                "value": 1.0,
                "detail": "The target tick has already passed. A transaction cannot be included in a past tick.",
            }));
        } else if distance > i64::from(PENDING_HORIZON_TICKS) {
            score += 0.45;
            factors.push(json!({
                "name": "target_tick",
                "weight": 0.45,
                "value": 0.45,
                "detail": format!(
                    "Target is {distance} ticks away. The pending pool keeps roughly the next {PENDING_HORIZON_TICKS} ticks."
                ),
            }));
        } else if distance > 60 {
            score += 0.08;
            factors.push(json!({
                "name": "target_tick",
                "weight": 0.08,
                "value": 0.08,
                "detail": format!("Target is {distance} ticks away."),
            }));
        } else {
            factors.push(json!({
                "name": "target_tick",
                "weight": 0.0,
                "value": 0.0,
                "detail": format!("Target is {distance} ticks away."),
            }));
        }
    }

    if let Some(amount) = params.amount {
        match balance {
            Some(balance) if amount > balance => {
                score = 1.0;
                factors.push(json!({
                    "name": "balance",
                    "weight": 1.0,
                    "value": 1.0,
                    "detail": format!(
                        "Amount {amount} exceeds the available balance {balance}."
                    ),
                    "observed": { "balance": balance, "amount": amount }
                }));
            }
            Some(balance) if amount > 0 => {
                let usage = amount as f64 / balance.max(1) as f64;
                if usage > 0.9 {
                    score += 0.12;
                }
                factors.push(json!({
                    "name": "balance",
                    "weight": 0.12,
                    "value": if usage > 0.9 { 0.12 } else { 0.0 },
                    "detail": format!("Amount uses {:.2}% of the available balance.", usage * 100.0),
                    "observed": { "balance": balance, "amount": amount }
                }));
            }
            None => {
                score += 0.08;
                factors.push(json!({
                    "name": "balance",
                    "weight": 0.08,
                    "value": 0.08,
                    "detail": "Balance is unknown because no peer or index row answered.",
                }));
            }
            _ => {}
        }
    }

    if let Some(source) = &source {
        factors.push(json!({
            "name": "source_activity",
            "weight": 0.0,
            "value": 0.0,
            "detail": format!(
                "{recent_activity} matching transactions in the last {PENDING_HORIZON_TICKS} ticks."
            ),
            "observed": {
                "identity": source,
                "balance": balance,
                "recent_transactions": recent_activity,
            }
        }));
    }

    let score = score.clamp(0.0, 1.0);
    let level = risk_level(score);
    let recommendation = risk_recommendation(score, ticks_until_target, params.amount, balance);
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": {
                "score": score,
                "level": level,
                "recommendation": recommendation,
                "current_tick": current,
                "target_tick": target_tick,
                "ticks_until_target": ticks_until_target,
                "source": source,
                "balance": balance,
                "recent_activity": recent_activity,
                "factors": factors,
                "capacity": {
                    "transaction_slots_per_tick": TX_PER_TICK,
                    "sample_ticks": sample.len(),
                    "average_transactions": average_transactions,
                    "average_fill": average_fill,
                    "p95_transactions": p95,
                    "p95_fill": p95_fill,
                    "max_transactions": max_transactions,
                    "max_fill": max_fill,
                    "pending_horizon_ticks": PENDING_HORIZON_TICKS,
                }
            },
            "meta": {
                "source": "index+peer",
                "peer_error": peer_error,
                "model": "heuristic",
                "note": "Qubic does not expose the computor pending pool. This score is an estimate from recent tick fill and target-tick distance, not a consensus guarantee."
            }
        })),
    ))
}

fn risk_level(score: f64) -> &'static str {
    if score < 0.25 {
        "low"
    } else if score < 0.5 {
        "medium"
    } else if score < 0.75 {
        "high"
    } else {
        "very_high"
    }
}

fn risk_recommendation(
    score: f64,
    ticks_until_target: Option<i64>,
    amount: Option<i64>,
    balance: Option<i64>,
) -> &'static str {
    if ticks_until_target.is_some_and(|distance| distance <= 0) {
        return "rebuild with a future target tick";
    }
    if let (Some(amount), Some(balance)) = (amount, balance) {
        if amount > balance {
            return "reduce the amount or add funds";
        }
    }
    if score >= 0.75 {
        "choose a later target tick"
    } else if score >= 0.5 {
        "submit soon and verify inclusion"
    } else {
        "target tick looks workable"
    }
}

fn percentile(sorted: &[u32], fraction: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

async fn recent_ticks(state: &AppState, epoch: Option<u16>) -> Vec<store::TickRow> {
    state
        .store
        .ticks(epoch, RISK_SAMPLE_TICKS, 0)
        .await
        .unwrap_or_default()
}

async fn tick_capacity(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
) -> Result<impl IntoResponse, ApiError> {
    let row = state
        .store
        .tick(tick)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("tick {tick} is not indexed")))?;
    let transactions = state
        .store
        .transactions_for_tick(tick, TX_PER_TICK as u32, 0)
        .await?;
    let mut senders: HashMap<&str, (u32, i64)> = HashMap::new();
    let mut contract_calls = 0u32;
    let mut transfer_count = 0u32;
    let mut volume = 0i64;
    for transaction in &transactions {
        let entry = senders.entry(transaction.source.as_str()).or_insert((0, 0));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.saturating_add(transaction.amount);
        volume = volume.saturating_add(transaction.amount);
        if transaction.input_type == 0 {
            transfer_count = transfer_count.saturating_add(1);
        } else {
            contract_calls = contract_calls.saturating_add(1);
        }
    }
    let mut top_senders: Vec<Value> = senders
        .into_iter()
        .map(|(identity, (count, amount))| {
            json!({
                "identity": identity,
                "transactions": count,
                "amount": amount,
            })
        })
        .collect();
    top_senders.sort_by(|left, right| {
        right
            .get("transactions")
            .and_then(Value::as_u64)
            .cmp(&left.get("transactions").and_then(Value::as_u64))
    });
    top_senders.truncate(20);

    let peer_status = peer::request(
        "peer transaction status",
        PEER_TIMEOUT,
        state.pool.tx_status(tick),
    )
    .await
    .ok();
    let money_flew_true = peer_status.as_ref().map(|status| {
        status
            .transactions
            .iter()
            .filter(|entry| entry.money_flew)
            .count()
    });
    let money_flew_false = peer_status.as_ref().map(|status| {
        status
            .transactions
            .iter()
            .filter(|entry| !entry.money_flew)
            .count()
    });

    let fill = f64::from(row.transaction_count) / TX_PER_TICK as f64;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": {
                "tick": tick,
                "epoch": row.epoch,
                "timestamp": row.timestamp,
                "transaction_count": row.transaction_count,
                "event_count": row.event_count,
                "transaction_slots": TX_PER_TICK,
                "remaining_slots": TX_PER_TICK.saturating_sub(row.transaction_count as usize),
                "fill_ratio": fill,
                "fill_percent": fill * 100.0,
                "volume": volume,
                "transfer_count": transfer_count,
                "contract_call_count": contract_calls,
                "top_senders": top_senders,
                "money_flew": {
                    "available": peer_status.is_some(),
                    "true_count": money_flew_true,
                    "false_count": money_flew_false,
                },
            },
            "meta": {
                "source": "index+peer",
                "note": "tx_count is the stored transaction count for this tick. money_flew is null unless a peer running the tx-status add-on answered."
            }
        })),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_bounded() {
        assert_eq!(percentile(&[], 0.95), 0);
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 0.0), 1);
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 0.95), 5);
    }

    #[test]
    fn risk_levels_cover_the_range() {
        assert_eq!(risk_level(0.0), "low");
        assert_eq!(risk_level(0.3), "medium");
        assert_eq!(risk_level(0.6), "high");
        assert_eq!(risk_level(0.9), "very_high");
    }

    #[test]
    fn execution_reports_no_effect_when_money_did_not_fly() {
        assert_eq!(execution_json(&[], Some(false))["status"], "no_effect");
        assert_eq!(execution_json(&[], Some(true))["status"], "success");
        assert_eq!(execution_json(&[], None)["status"], "unknown");
    }

    #[test]
    fn target_tick_in_the_past_is_always_high_risk() {
        let recommendation = risk_recommendation(1.0, Some(-1), None, None);
        assert_eq!(recommendation, "rebuild with a future target tick");
    }
}
