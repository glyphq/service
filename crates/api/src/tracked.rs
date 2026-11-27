//! Unified transaction view for indexed and API-tracked transactions.
//!
//! A transaction submitted through Glyph may not be indexed yet, or may never
//! reach a tick. The explorer still needs a stable page for it, so reads fall
//! back to the durable tracking record instead of returning 404 after the
//! operational mempool entry is gone.

use serde_json::{Value, json};
use store::TransactionRow;
use store::gateway::{MempoolStatus, TrackedTransaction};

use crate::dto::transaction_value;
use crate::{ApiError, AppState};

pub enum ResolvedTransaction {
    Indexed(TransactionRow),
    Tracked(TrackedTransaction),
}

pub struct TransactionView {
    pub data: Value,
    pub meta: Value,
    pub indexed: bool,
    pub state: &'static str,
}

/// Resolves a hash from the index first, then from API tracking state.
pub async fn resolve(
    state: &AppState,
    hash: &str,
) -> Result<Option<ResolvedTransaction>, ApiError> {
    let hash = normalize_hash(hash)?;
    if let Some(row) = state.store.transaction(&hash).await? {
        return Ok(Some(ResolvedTransaction::Indexed(row)));
    }
    if let Some(gateway) = state.gateway.as_ref() {
        if let Some(transaction) = gateway.tracked_transaction(&hash).await? {
            return Ok(Some(ResolvedTransaction::Tracked(transaction)));
        }
    }
    Ok(None)
}

pub fn view(resolved: &ResolvedTransaction) -> TransactionView {
    match resolved {
        ResolvedTransaction::Indexed(row) => TransactionView {
            data: transaction_value(row),
            meta: json!({
                "source": "index",
                "indexed": true,
                "state": "confirmed",
            }),
            indexed: true,
            state: "confirmed",
        },
        ResolvedTransaction::Tracked(entry) => {
            let state_name = tracked_state(entry.status);
            TransactionView {
                data: tracked_value(entry, state_name),
                meta: json!({
                    "source": "tracking",
                    "indexed": false,
                    "state": state_name,
                    "tracking_status": entry.status.as_str(),
                    "attempts": entry.attempts,
                    "broadcast_peers": entry.broadcast_peers,
                }),
                indexed: false,
                state: state_name,
            }
        }
    }
}

pub fn normalize_hash(hash: &str) -> Result<String, ApiError> {
    let hash = hash.trim().to_ascii_lowercase();
    qubic::decode_tx_hash(&hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;
    Ok(hash)
}

fn tracked_state(status: MempoolStatus) -> &'static str {
    match status {
        MempoolStatus::Queued | MempoolStatus::Broadcast => "pending",
        MempoolStatus::Confirmed => "confirmed",
        MempoolStatus::Expired => "expired",
        MempoolStatus::Failed => "failed",
        MempoolStatus::Cancelled => "cancelled",
    }
}

fn tracked_value(entry: &TrackedTransaction, state_name: &'static str) -> Value {
    json!({
        "hash": entry.hash,
        "epoch": Value::Null,
        "tick": Value::Null,
        "target_tick": entry.target_tick,
        "tx_index": Value::Null,
        "source": entry.source,
        "destination": entry.destination,
        "amount": entry.amount,
        "input_type": entry.input_type,
        "input_data": entry.input_data,
        "status": state_name,
        "timestamp": entry.broadcast_at.unwrap_or(entry.created_at),
        "tracking_status": entry.status.as_str(),
        "attempts": entry.attempts,
        "broadcast_peers": entry.broadcast_peers,
        "broadcast_at": entry.broadcast_at,
        "confirmed_tick": entry.confirmed_tick,
        "confirmed_at": entry.confirmed_at,
        "expiry_tick": entry.expiry_tick,
        "error": entry.error,
    })
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    fn entry(status: MempoolStatus) -> TrackedTransaction {
        TrackedTransaction {
            hash: "hash".into(),
            source: "SRC".into(),
            destination: "DST".into(),
            amount: 1,
            target_tick: 7,
            input_type: 1,
            input_data: "deadbeef".into(),
            status,
            attempts: 2,
            broadcast_peers: 1,
            broadcast_at: Some(Utc::now()),
            confirmed_at: None,
            confirmed_tick: None,
            expiry_tick: 17,
            error: Some("broadcast failed".into()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn tracked_states_are_terminal_and_explicit() {
        assert_eq!(tracked_state(MempoolStatus::Queued), "pending");
        assert_eq!(tracked_state(MempoolStatus::Broadcast), "pending");
        assert_eq!(tracked_state(MempoolStatus::Failed), "failed");
        assert_eq!(tracked_state(MempoolStatus::Expired), "expired");
        assert_eq!(tracked_state(MempoolStatus::Confirmed), "confirmed");
    }

    #[test]
    fn tracked_value_never_exposes_raw_signed_bytes() {
        let value = tracked_value(&entry(MempoolStatus::Failed), "failed");
        assert_eq!(value["status"], "failed");
        assert_eq!(value["expiry_tick"], 17);
        assert!(value.get("raw").is_none());
        assert_eq!(value["input_data"], "deadbeef");
    }

    #[test]
    fn tracked_view_marks_pending_rows_as_unindexed() {
        let view = view(&ResolvedTransaction::Tracked(entry(
            MempoolStatus::Broadcast,
        )));
        assert!(!view.indexed);
        assert_eq!(view.state, "pending");
        assert_eq!(view.data["status"], "pending");
        assert_eq!(view.data["target_tick"], 7);
        assert!(view.data["tick"].is_null());
        assert_eq!(view.meta["source"], "tracking");
    }
}
