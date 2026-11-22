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

use axum::Json;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use chrono::{TimeDelta, Utc};
use node::NodePool;
use serde_json::json;
use store::gateway::{GatewayStore, MempoolEntry, MempoolStatus};
use tokio_util::sync::CancellationToken;

use crate::ApiError;
use crate::advanced::DecodedTransaction;
use crate::webhook::Webhooks;

/// The request path performs the first broadcast after inserting the entry.
/// Give it time to finish before the tracker can rebroadcast the same entry.
const SUBMIT_GRACE: TimeDelta = TimeDelta::seconds(30);

/// Runtime limits for the mempool. All values are configurable by environment.
#[derive(Debug, Clone)]
pub struct MempoolConfig {
    pub enabled: bool,
    pub poll: Duration,
    pub retry: Duration,
    pub max_attempts: u8,
    pub expiry_ticks: u32,
    pub max_entries: usize,
    pub max_per_source: usize,
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
            max_per_source: env_u64("MEMPOOL_MAX_PER_SOURCE", 100).max(1) as usize,
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
    pub(crate) async fn submit(
        &self,
        decoded: DecodedTransaction,
        peers: usize,
        webhook: Option<InlineWebhook>,
    ) -> Result<MempoolEntry, ApiError> {
        if !decoded.valid {
            return Err(ApiError::bad_request(format!(
                "transaction failed local validation: {}",
                decoded.failure_detail()
            )));
        }
        let DecodedTransaction {
            transaction,
            bytes,
            hash,
            ..
        } = decoded;
        let source = qubic::PublicKey(transaction.source).to_identity();

        if let Some(existing) = self.gateway.mempool_entry(&hash).await? {
            return Ok(existing);
        }
        let (source_open, total_open) = self
            .gateway
            .open_mempool_counts(&source, self.config.max_attempts)
            .await?;
        if total_open >= self.config.max_entries as u64 {
            return Err(ApiError {
                status: StatusCode::TOO_MANY_REQUESTS,
                code: "tracking_full",
                detail: format!(
                    "transaction tracking is full ({} open entries)",
                    self.config.max_entries
                ),
            });
        }
        if source_open >= self.config.max_per_source as u64 {
            return Err(ApiError {
                status: StatusCode::TOO_MANY_REQUESTS,
                code: "source_tracking_full",
                detail: format!(
                    "source {source} already has {} open transactions; limit is {}",
                    source_open, self.config.max_per_source
                ),
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
            raw: hex::encode(&bytes),
            source,
            destination: qubic::PublicKey(transaction.destination).to_identity(),
            amount: transaction.amount,
            tick: transaction.tick,
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
            .broadcast_transaction_fanout(&transaction, peers.max(1))
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
            if submission_in_progress(entry.attempts, entry.created_at, now) {
                continue;
            }
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
            let was_broadcast = entry.status == MempoolStatus::Broadcast;
            entry.attempts = entry.attempts.saturating_add(1);
            entry.broadcast_at = Some(now);
            entry.updated_at = now;
            let event = retry_event(
                was_broadcast,
                sent,
                entry.attempts,
                self.config.max_attempts,
            );
            let event = if sent > 0 {
                entry.status = MempoolStatus::Broadcast;
                entry.broadcast_peers = u8::try_from(sent).unwrap_or(u8::MAX);
                entry.error = None;
                event
            } else if entry.attempts >= self.config.max_attempts {
                entry.error = Some(broadcast_error(&attempts));
                if !was_broadcast {
                    entry.status = MempoolStatus::Failed;
                    entry.broadcast_peers = 0;
                }
                event
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

/// Webhook event emitted by a tracked rebroadcast. Successful retries do not
/// repeat `transaction.broadcast` once the entry is already broadcast.
fn retry_event(
    was_broadcast: bool,
    sent: usize,
    attempts: u8,
    max_attempts: u8,
) -> Option<&'static str> {
    if sent > 0 {
        (!was_broadcast).then_some("transaction.broadcast")
    } else if attempts >= max_attempts && !was_broadcast {
        Some("transaction.failed")
    } else {
        None
    }
}

fn submission_in_progress(
    attempts: u8,
    created_at: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
) -> bool {
    attempts == 0 && now.signed_duration_since(created_at) < SUBMIT_GRACE
}

/// `202 Accepted` body for `/v1/transactions`.
pub fn accepted(
    entry: MempoolEntry,
    expiry_ticks: u32,
    head_tick: Option<u32>,
    warnings: Vec<String>,
) -> Response {
    let hash = entry.hash.clone();
    let broadcast = entry.broadcast_peers > 0;
    let broadcast_peers = entry.broadcast_peers;
    let status = entry.status.as_str();
    (
        StatusCode::ACCEPTED,
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({
            "data": entry_json(&entry, expiry_ticks),
            "meta": {
                "source": "tracking",
                "status": status,
                "broadcast": broadcast,
                "broadcast_peers": broadcast_peers,
                "confirmed": false,
                "head_tick": head_tick,
                "warnings": warnings,
                "links": {
                    "status": format!("/v1/transactions/{hash}/lifecycle"),
                    "transaction": format!("/v1/transactions/{hash}"),
                },
                "hint": "A broadcast status means at least one peer accepted the packet at the socket. Qubic does not acknowledge broadcasts; register a webhook or poll the lifecycle link."
            }
        })),
    )
        .into_response()
}

/// API shape for a mempool entry, with the scheduled tick made explicit.
pub fn entry_json(entry: &MempoolEntry, expiry_ticks: u32) -> serde_json::Value {
    let mut value = serde_json::to_value(entry).unwrap_or(serde_json::Value::Null);
    if let Some(object) = value.as_object_mut() {
        object.insert("target_tick".into(), json!(entry.tick));
        object.insert(
            "expiry_tick".into(),
            json!(entry.tick.saturating_add(expiry_ticks)),
        );
    }
    value
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
        assert!(config.max_per_source >= 1);
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
    fn successful_retries_do_not_repeat_the_broadcast_event() {
        assert_eq!(retry_event(false, 3, 1, 10), Some("transaction.broadcast"));
        assert_eq!(retry_event(true, 3, 2, 10), None);
        assert_eq!(retry_event(false, 0, 9, 10), None);
        assert_eq!(retry_event(false, 0, 10, 10), Some("transaction.failed"));
        assert_eq!(retry_event(true, 0, 10, 10), None);
    }

    #[test]
    fn tracker_waits_for_the_initial_broadcast() {
        let created = Utc::now();
        assert!(submission_in_progress(0, created, created));
        assert!(!submission_in_progress(0, created, created + SUBMIT_GRACE));
        assert!(!submission_in_progress(
            1,
            created,
            created + TimeDelta::seconds(1)
        ));
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
        let response = accepted(entry, 10, Some(100), Vec::new());
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[test]
    fn entry_json_exposes_the_target_and_expiry_ticks() {
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
        let value = entry_json(&entry, 10);
        assert_eq!(value["target_tick"], 42);
        assert_eq!(value["expiry_tick"], 52);
        assert_eq!(value["tick"], 42);
    }
}
