//! Shared peer access and short-lived cache for live Qubic reads.
//!
//! Indexed routes never touch this module. Live routes use it to avoid opening
//! a new TCP connection per item when one request needs several peer messages,
//! and to keep dashboards from turning every refresh into a burst of peer
//! traffic.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use node::Peer;
use serde_json::Value;

use crate::{ApiError, AppState};

/// Connect to the best currently reachable peer.
pub async fn connect(state: &AppState) -> Result<Peer, ApiError> {
    if state.peers.is_empty() {
        return Err(ApiError::unavailable(
            "no QUBIC_PEERS configured for peer-backed routes",
        ));
    }
    state
        .pool
        .connect_best()
        .await
        .map_err(|error| ApiError::unavailable(format!("peer connection failed: {error}")))
}

/// Runs one peer request with a bounded wait and a consistent error message.
pub async fn request<T, F>(label: &str, timeout: Duration, future: F) -> Result<T, ApiError>
where
    F: Future<Output = Result<T, node::Error>>,
{
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(ApiError::unavailable(format!("{label} failed: {error}"))),
        Err(_) => Err(ApiError::unavailable(format!("{label} timed out"))),
    }
}

const LIVE_CACHE_MAX_ENTRIES: usize = 1_024;

/// Small in-process TTL cache for JSON values derived from peer responses.
///
/// Entries are copied on read and write. Per-entity keys are bounded by an
/// oldest-entry eviction pass so a long-lived process cannot grow without
/// limit as wallets and transactions are queried.
#[derive(Debug)]
pub struct LiveCache {
    entries: Mutex<HashMap<String, Entry>>,
    max_entries: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl Default for LiveCache {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            max_entries: LIVE_CACHE_MAX_ENTRIES,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
struct Entry {
    stored_at: Instant,
    value: Value,
}

impl LiveCache {
    pub fn get(&self, key: &str, ttl: Duration) -> Option<Value> {
        let entries = self.entries.lock().expect("live cache lock");
        match entries.get(key) {
            Some(entry) if entry.stored_at.elapsed() < ttl => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.value.clone())
            }
            _ => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn put(&self, key: impl Into<String>, value: Value) {
        let mut entries = self.entries.lock().expect("live cache lock");
        let key = key.into();
        if entries.len() >= self.max_entries && !entries.contains_key(&key) {
            if let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.stored_at)
                .map(|(key, _)| key.clone())
            {
                entries.remove(&oldest);
            }
        }
        entries.insert(
            key,
            Entry {
                stored_at: Instant::now(),
                value,
            },
        );
    }

    /// Lifetime cache lookups, split into hits and misses.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }
}
