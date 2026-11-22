//! API mempool for broadcast transactions.
//!
//! Qubic has no protocol-level mempool. This module tracks a signed
//! transaction from submission until it appears in the index, expires past its
//! target tick, or fails. A `broadcast` status only means at least one peer
//! accepted the packet at the socket; it is not an inclusion guarantee.
//!
//! Entries live in ClickHouse (`store::gateway`). The background task polls the
//! index for confirmation, expires stale targets, and rebroadcasts at
//! `MEMPOOL_RETRY_MS` until `MEMPOOL_MAX_ATTEMPTS`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use node::NodePool;
use serde::Deserialize;
use serde_json::json;
use store::gateway::{GatewayStore, MempoolEntry, MempoolStatus};
use tokio_util::sync::CancellationToken;

use crate::advanced::{decode_transaction, json_body};
use crate::webhook::Webhooks;
use crate::{ApiError, AppState};

/// Runtime limits for the mempool. All values are configurable by environment.
#[derive(Debug, Clone)]
pub struct MempoolConfig {
    pub enabled: bool,
    pub poll: Duration,
    pub retry: Duration,
    pub max_attempts: u8,
    pub expiry_ticks: u32,
    pub max_entries: usize,
}

impl MempoolConfig {
    pub fn from_env() -> Self {
        let enabled = !std::env::var("MEMPOOL_ENABLED")
            .is_ok_and(|value| value.trim().eq_ignore_ascii_case("false"));
        Self {
            enabled,
            poll: Duration::from_millis(env_u64("MEMPOOL_POLL_MS", 500)),
            retry: Duration::from_millis(env_u64("MEMPOOL_RETRY_MS", 1_000)),
            max_attempts: env_u64("MEMPOOL_MAX_ATTEMPTS", 10).clamp(1, 255) as u8,
            expiry_ticks: env_u64("MEMPOOL_EXPIRY_TICKS", 10).min(1_000_000) as u32,
            max_entries: env_u64("MEMPOOL_MAX_ENTRIES", 10_000).max(1) as usize,
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

/// Inline webhook supplied with a mempool submission.
#[derive(Debug, Clone)]
pub struct InlineWebhook {
    pub url: String,
    pub secret: String,
}

/// Shared mempool service held in [`AppState`].
#[derive(Clone)]
pub struct Mempool {
    store: Arc<dyn store::Store>,
    pool: NodePool,
    gateway: Arc<GatewayStore>,
    webhooks: Arc<Webhooks>,
    config: Arc<MempoolConfig>,
    cancel: CancellationToken,
}

impl Mempool {
    pub fn new(
        store: Arc<dyn store::Store>,
        pool: NodePool,
        gateway: Arc<GatewayStore>,
        webhooks: Arc<Webhooks>,
        config: MempoolConfig,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            store,
            pool,
            gateway,
            webhooks,
            config: Arc::new(config),
            cancel,
        }
    }

    pub fn config(&self) -> &MempoolConfig {
        &self.config
    }

    /// Validates, deduplicates, and broadcasts a signed transaction.
    ///
    /// A duplicate hash returns the existing entry unchanged, which makes the
    /// endpoint idempotent. Invalid transactions are rejected before any peer
    /// socket is opened.
    pub async fn submit(
        &self,
        raw: &str,
        peers: usize,
        webhook: Option<InlineWebhook>,
    ) -> Result<MempoolEntry, ApiError> {
        let decoded = decode_transaction(raw)?;
        if !decoded.valid {
            return Err(ApiError::bad_request(format!(
                "transaction failed local validation: {}",
                decoded.failure_detail()
            )));
        }
        let hash = decoded.hash.clone();

        if let Some(existing) = self.gateway.mempool_entry(&hash).await? {
            return Ok(existing);
        }
        if self
            .gateway
            .open_mempool_count(self.config.max_attempts)
            .await?
            >= self.config.max_entries as u64
        {
            return Err(ApiError {
                status: StatusCode::TOO_MANY_REQUESTS,
                code: "mempool_full",
                detail: format!("mempool is full ({} open entries)", self.config.max_entries),
            });
        }

        if let Some(webhook) = webhook {
            self.webhooks
                .register_inline(webhook.url, webhook.secret, hash.clone())
                .await?;
        }

        let now = Utc::now();
        let mut entry = MempoolEntry {
            hash,
            raw: hex::encode(&decoded.bytes),
            source: qubic::PublicKey(decoded.transaction.source).to_identity(),
            destination: qubic::PublicKey(decoded.transaction.destination).to_identity(),
            amount: decoded.transaction.amount,
            tick: decoded.transaction.tick,
            peers: u8::try_from(peers.clamp(1, 8)).unwrap_or(3),
            status: MempoolStatus::Queued,
            attempts: 0,
            broadcast_peers: 0,
            broadcast_at: None,
            confirmed_at: None,
            confirmed_tick: None,
            error: None,
            created_at: now,
            updated_at: now,
        };
        self.gateway.insert_mempool(&entry).await?;
        self.webhooks
            .dispatch_event("transaction.queued", &entry)
            .await?;

        let attempts = self
            .pool
            .broadcast_transaction_fanout(&decoded.transaction, peers.max(1))
            .await;
        let sent = attempts.iter().filter(|attempt| attempt.sent).count();
        entry.attempts = 1;
        entry.broadcast_at = Some(now);
        entry.updated_at = Utc::now();
        let event = if sent > 0 {
            entry.status = MempoolStatus::Broadcast;
            entry.broadcast_peers = u8::try_from(sent).unwrap_or(u8::MAX);
            entry.error = None;
            Some("transaction.broadcast")
        } else {
            entry.status = MempoolStatus::Failed;
            entry.broadcast_peers = 0;
            entry.error = Some(broadcast_error(&attempts));
            None
        };
        self.gateway.update_mempool(&entry).await?;
        if let Some(event) = event {
            self.webhooks.dispatch_event(event, &entry).await?;
        }
        Ok(entry)
    }

    /// One entry with indexed status merged in. Confirmation is visible here
    /// before the next background poll persists it.
    pub async fn status(&self, hash: &str) -> Result<Option<MempoolEntry>, ApiError> {
        let hash = hash.trim().to_ascii_lowercase();
        let Some(mut entry) = self.gateway.mempool_entry(&hash).await? else {
            return Ok(None);
        };
        if entry.status != MempoolStatus::Confirmed && entry.status != MempoolStatus::Cancelled {
            if let Some(row) = self.store.transaction(&hash).await? {
                entry.status = MempoolStatus::Confirmed;
                entry.confirmed_tick = Some(row.tick);
                entry.confirmed_at = Some(Utc::now());
                entry.updated_at = Utc::now();
                // If this write races the indexer, the background poll
                // persists the same state on its next pass.
                if self.gateway.update_mempool(&entry).await.is_ok() {
                    self.webhooks
                        .dispatch_event("transaction.confirmed", &entry)
                        .await?;
                }
            }
        }
        Ok(Some(entry))
    }

    pub async fn list(
        &self,
        status: Option<MempoolStatus>,
        limit: u32,
    ) -> Result<Vec<MempoolEntry>, ApiError> {
        Ok(self.gateway.mempool_entries(status, limit).await?)
    }

    /// Cancels an entry that has not confirmed. A confirmed transaction is
    /// immutable chain history, so cancelling it is rejected.
    pub async fn cancel(&self, hash: &str) -> Result<MempoolEntry, ApiError> {
        let hash = hash.trim().to_ascii_lowercase();
        let Some(mut entry) = self.gateway.mempool_entry(&hash).await? else {
            return Err(ApiError::not_found(format!(
                "mempool entry {hash} does not exist"
            )));
        };
        match entry.status {
            MempoolStatus::Confirmed => {
                return Err(ApiError {
                    status: StatusCode::CONFLICT,
                    code: "already_confirmed",
                    detail: format!("transaction {hash} is already confirmed"),
                });
            }
            MempoolStatus::Cancelled => return Ok(entry),
            _ => {}
        }
        entry.status = MempoolStatus::Cancelled;
        entry.updated_at = Utc::now();
        self.gateway.update_mempool(&entry).await?;
        self.webhooks
            .dispatch_event("transaction.cancelled", &entry)
            .await?;
        Ok(entry)
    }

    /// Background tracker. Polls until the cancellation token fires.
    pub async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(self.config.poll);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(error) = self.tick().await {
                        tracing::warn!(?error, "mempool tracker iteration failed");
                    }
                }
            }
        }
    }

    async fn tick(&self) -> Result<(), ApiError> {
        let entries = self
            .gateway
            .open_mempool_entries(500, self.config.max_attempts)
            .await?;
        if entries.is_empty() {
            return Ok(());
        }
        let hashes: Vec<String> = entries.iter().map(|entry| entry.hash.clone()).collect();
        let indexed = self.store.transactions_by_hashes(&hashes).await?;
        let indexed: HashMap<&str, u32> = indexed
            .iter()
            .map(|row| (row.hash.as_str(), row.tick))
            .collect();
        let head = self.store.head().await?.map(|row| row.tick);
        let now = Utc::now();

        for mut entry in entries {
            if let Some(tick) = indexed.get(entry.hash.as_str()) {
                entry.status = MempoolStatus::Confirmed;
                entry.confirmed_tick = Some(*tick);
                entry.confirmed_at = Some(now);
                entry.updated_at = now;
                entry.error = None;
                self.gateway.update_mempool(&entry).await?;
                self.webhooks
                    .dispatch_event("transaction.confirmed", &entry)
                    .await?;
                continue;
            }

            if let Some(head) = head {
                if head > entry.tick.saturating_add(self.config.expiry_ticks) {
                    entry.status = MempoolStatus::Expired;
                    entry.updated_at = now;
                    self.gateway.update_mempool(&entry).await?;
                    self.webhooks
                        .dispatch_event("transaction.expired", &entry)
                        .await?;
                    continue;
                }
            }

            if entry.attempts >= self.config.max_attempts {
                continue;
            }
            let last = entry.broadcast_at.unwrap_or(entry.created_at);
            let due = now
                .signed_duration_since(last)
                .to_std()
                .is_ok_and(|elapsed| elapsed >= self.config.retry);
            if !due {
                continue;
            }

            let bytes = match hex::decode(&entry.raw) {
                Ok(bytes) => bytes,
                Err(error) => {
                    entry.status = MempoolStatus::Failed;
                    entry.error = Some(format!("stored transaction is not hex: {error}"));
                    entry.updated_at = now;
                    self.gateway.update_mempool(&entry).await?;
                    self.webhooks
                        .dispatch_event("transaction.failed", &entry)
                        .await?;
                    continue;
                }
            };
            let transaction = match qubic::messages::Transaction::decode(&bytes) {
                Ok(transaction) => transaction,
                Err(error) => {
                    entry.status = MempoolStatus::Failed;
                    entry.error = Some(format!("stored transaction is invalid: {error}"));
                    entry.updated_at = now;
                    self.gateway.update_mempool(&entry).await?;
                    self.webhooks
                        .dispatch_event("transaction.failed", &entry)
                        .await?;
                    continue;
                }
            };

            let attempts = self
                .pool
                .broadcast_transaction_fanout(&transaction, usize::from(entry.peers.max(1)))
                .await;
            let sent = attempts.iter().filter(|attempt| attempt.sent).count();
            entry.attempts = entry.attempts.saturating_add(1);
            entry.broadcast_at = Some(now);
            entry.updated_at = now;
            let event = if sent > 0 {
                entry.status = MempoolStatus::Broadcast;
                entry.broadcast_peers = u8::try_from(sent).unwrap_or(u8::MAX);
                entry.error = None;
                Some("transaction.broadcast")
            } else if entry.attempts >= self.config.max_attempts {
                entry.status = MempoolStatus::Failed;
                entry.broadcast_peers = 0;
                entry.error = Some(broadcast_error(&attempts));
                Some("transaction.failed")
            } else {
                entry.broadcast_peers = 0;
                entry.error = Some(broadcast_error(&attempts));
                None
            };
            self.gateway.update_mempool(&entry).await?;
            if let Some(event) = event {
                self.webhooks.dispatch_event(event, &entry).await?;
            }
        }
        Ok(())
    }
}

fn broadcast_error(attempts: &[node::BroadcastAttempt]) -> String {
    let errors = attempts
        .iter()
        .filter_map(|attempt| attempt.error.as_deref())
        .collect::<Vec<_>>()
        .join("; ");
    if errors.is_empty() {
        "no peer accepted the transaction".to_owned()
    } else {
        errors
    }
}

/// `202 Accepted` body shared by `/v1/mempool` and `/v1/transactions`.
pub fn accepted(entry: MempoolEntry) -> Response {
    let hash = entry.hash.clone();
    let broadcast = entry.broadcast_peers > 0;
    let broadcast_peers = entry.broadcast_peers;
    let status = entry.status.as_str();
    (
        StatusCode::ACCEPTED,
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({
            "data": entry_json(&entry),
            "meta": {
                "source": "mempool",
                "status": status,
                "broadcast": broadcast,
                "broadcast_peers": broadcast_peers,
                "confirmed": false,
                "links": {
                    "status": format!("/v1/mempool/{hash}"),
                    "transaction": format!("/v1/transactions/{hash}"),
                },
                "hint": "A broadcast status means at least one peer accepted the packet at the socket. Qubic does not acknowledge broadcasts; poll the status link or the indexed transaction."
            }
        })),
    )
        .into_response()
}

/// API shape for a mempool entry, with the scheduled tick made explicit.
pub fn entry_json(entry: &MempoolEntry) -> serde_json::Value {
    let mut value = serde_json::to_value(entry).unwrap_or(serde_json::Value::Null);
    if let Some(object) = value.as_object_mut() {
        object.insert("target_tick".into(), json!(entry.tick));
    }
    value
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/mempool", post(submit).get(list))
        .route("/v1/mempool/{hash}", get(status).delete(cancel))
}

fn service(state: &AppState) -> Result<Arc<Mempool>, ApiError> {
    state.mempool.clone().ok_or_else(|| {
        ApiError::unavailable("mempool is disabled; set MEMPOOL_ENABLED=true to enable it")
    })
}

#[derive(Debug, Default, Deserialize)]
struct SubmitBody {
    #[serde(alias = "transaction", alias = "raw_transaction")]
    raw: String,
    #[serde(default)]
    peers: Option<usize>,
    #[serde(default)]
    webhook_url: Option<String>,
    #[serde(default)]
    webhook_secret: Option<String>,
}

async fn submit(
    State(state): State<AppState>,
    payload: Result<Json<SubmitBody>, JsonRejection>,
) -> Result<Response, ApiError> {
    if !state.allow_broadcast {
        return Err(ApiError::forbidden(
            "transaction broadcast is disabled; set ALLOW_BROADCAST=true",
        ));
    }
    let body = json_body(payload)?;
    let inline = match (body.webhook_url, body.webhook_secret) {
        (None, None) => None,
        (Some(url), Some(secret)) => Some(InlineWebhook { url, secret }),
        _ => {
            return Err(ApiError::bad_request(
                "webhook_url and webhook_secret must be provided together",
            ));
        }
    };
    let peers = body
        .peers
        .unwrap_or(state.broadcast_peers)
        .clamp(1, state.broadcast_peers.min(8));
    let entry = service(&state)?.submit(&body.raw, peers, inline).await?;
    Ok(accepted(entry))
}

#[derive(Debug, Default, Deserialize)]
struct ListQuery {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

async fn list(
    State(state): State<AppState>,
    Query(params): Query<ListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let status = match params.status.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(value) => Some(MempoolStatus::parse(value).ok_or_else(|| {
            ApiError::bad_request(format!(
                "unknown mempool status {value:?}; expected queued, broadcast, confirmed, expired, failed, or cancelled"
            ))
        })?),
    };
    let limit = params.limit.unwrap_or(100).clamp(1, 500);
    let entries = service(&state)?.list(status, limit).await?;
    let entries: Vec<serde_json::Value> = entries.iter().map(entry_json).collect();
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({
            "data": entries,
            "meta": {
                "limit": limit,
                "status": status.map(MempoolStatus::as_str),
            }
        })),
    ))
}

async fn status(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let hash = hash.trim().to_ascii_lowercase();
    qubic::decode_tx_hash(&hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;
    let entry = service(&state)?
        .status(&hash)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("mempool entry {hash} does not exist")))?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({
            "data": entry,
            "meta": {
                "links": {
                    "transaction": format!("/v1/transactions/{hash}"),
                }
            }
        })),
    ))
}

async fn cancel(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let hash = hash.trim().to_ascii_lowercase();
    qubic::decode_tx_hash(&hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;
    let entry = service(&state)?.cancel(&hash).await?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({ "data": entry_json(&entry) })),
    ))
}

/// Expiry predicate shared with tests: a target tick is expired once the head
/// is strictly greater than `target + window`.
pub fn is_expired(head: u32, target_tick: u32, expiry_ticks: u32) -> bool {
    head > target_tick.saturating_add(expiry_ticks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_are_sane() {
        // Environment may be set by the caller running the suite, so assert
        // only invariants that hold for any accepted value.
        let config = MempoolConfig::from_env();
        assert!(config.max_attempts >= 1);
        assert!(config.max_entries >= 1);
        assert!(config.poll >= Duration::from_millis(1));
        assert!(config.retry >= Duration::from_millis(1));
    }

    #[test]
    fn expiry_uses_strictly_greater_head() {
        assert!(!is_expired(109, 100, 10));
        assert!(!is_expired(110, 100, 10));
        assert!(is_expired(111, 100, 10));
        assert!(!is_expired(10, 0, 10));
        assert!(is_expired(11, 0, 10));
    }

    #[test]
    fn expiry_is_saturating_at_the_tick_ceiling() {
        assert!(!is_expired(u32::MAX, u32::MAX, 10));
    }

    #[test]
    fn broadcast_error_prefers_peer_messages() {
        let attempts = vec![
            node::BroadcastAttempt {
                peer: "127.0.0.1:1".parse().unwrap(),
                sent: false,
                error: Some("connection refused".into()),
                elapsed: Duration::from_millis(1),
            },
            node::BroadcastAttempt {
                peer: "127.0.0.1:2".parse().unwrap(),
                sent: false,
                error: None,
                elapsed: Duration::from_millis(1),
            },
        ];
        assert_eq!(broadcast_error(&attempts), "connection refused");
        assert_eq!(broadcast_error(&[]), "no peer accepted the transaction");
    }

    #[test]
    fn accepted_response_points_at_status_and_index() {
        let entry = MempoolEntry {
            hash: "ab".into(),
            raw: "00".into(),
            source: "SRC".into(),
            destination: "DST".into(),
            amount: 1,
            tick: 7,
            peers: 3,
            status: MempoolStatus::Queued,
            attempts: 0,
            broadcast_peers: 0,
            broadcast_at: None,
            confirmed_at: None,
            confirmed_tick: None,
            error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let response = accepted(entry);
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[test]
    fn entry_json_exposes_the_target_tick() {
        let entry = MempoolEntry {
            hash: "ab".into(),
            raw: "00".into(),
            source: "SRC".into(),
            destination: "DST".into(),
            amount: 1,
            tick: 42,
            peers: 8,
            status: MempoolStatus::Broadcast,
            attempts: 1,
            broadcast_peers: 3,
            broadcast_at: None,
            confirmed_at: None,
            confirmed_tick: None,
            error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let value = entry_json(&entry);
        assert_eq!(value["target_tick"], 42);
        assert_eq!(value["tick"], 42);
    }
}
