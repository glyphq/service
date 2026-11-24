//! Shared peer access and short-lived cache for live Qubic reads.
//!
//! Indexed routes never touch this module. Live routes use it to avoid opening
//! a new TCP connection per item when one request needs several peer messages,
//! and to keep dashboards from turning every refresh into a burst of peer
//! traffic.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
    flights: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    max_entries: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl Default for LiveCache {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            flights: Mutex::new(HashMap::new()),
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

    /// Returns a cached value, or runs `build` once for concurrent callers of
    /// the same key.
    pub async fn get_or_insert<F, Fut, E>(
        &self,
        key: impl Into<String>,
        ttl: Duration,
        build: F,
    ) -> Result<Value, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Value, E>>,
    {
        let key = key.into();
        if let Some(value) = self.get(&key, ttl) {
            return Ok(value);
        }
        let flight = {
            let mut flights = self.flights.lock().expect("live cache flights lock");
            flights
                .entry(key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let guard = flight.lock().await;
        if let Some(value) = self.get(&key, ttl) {
            drop(guard);
            self.finish_flight(&key, &flight);
            return Ok(value);
        }
        let result = build().await;
        if let Ok(value) = &result {
            self.put(key.clone(), value.clone());
        }
        drop(guard);
        self.finish_flight(&key, &flight);
        result
    }

    /// Lifetime cache lookups, split into hits and misses.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }

    fn finish_flight(&self, key: &str, flight: &Arc<tokio::sync::Mutex<()>>) {
        let mut flights = self.flights.lock().expect("live cache flights lock");
        if Arc::strong_count(flight) == 1 {
            flights.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn get_or_insert_runs_one_build_for_concurrent_callers() {
        let cache = Arc::new(LiveCache::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..12 {
            let cache = cache.clone();
            let calls = calls.clone();
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_insert("shared", Duration::from_secs(1), || async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        Ok::<Value, ()>(serde_json::json!({ "ok": true }))
                    })
                    .await
                    .expect("cache build")
            }));
        }
        for task in tasks {
            assert_eq!(task.await.expect("task"), serde_json::json!({ "ok": true }));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
