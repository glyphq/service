//! Shared peer access and short-lived cache for live Qubic reads.
//!
//! Indexed routes never touch this module. Live routes use it to avoid opening
//! a new TCP connection per item when one request needs several peer messages,
//! and to keep dashboards from turning every refresh into a burst of peer
//! traffic.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
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

/// Small in-process TTL cache for JSON values derived from peer responses.
///
/// Entries are copied on read and write. The cache intentionally has no
/// eviction pass: the number of distinct keys is bounded by the route surface
/// and the current epoch, so a process restart is an acceptable reset.
#[derive(Debug, Default)]
pub struct LiveCache {
    entries: Mutex<HashMap<String, Entry>>,
}

#[derive(Debug, Clone)]
struct Entry {
    stored_at: Instant,
    value: Value,
}

impl LiveCache {
    pub fn get(&self, key: &str, ttl: Duration) -> Option<Value> {
        let entries = self.entries.lock().expect("live cache lock");
        let entry = entries.get(key)?;
        (entry.stored_at.elapsed() < ttl).then(|| entry.value.clone())
    }

    pub fn put(&self, key: impl Into<String>, value: Value) {
        let mut entries = self.entries.lock().expect("live cache lock");
        entries.insert(
            key.into(),
            Entry {
                stored_at: Instant::now(),
                value,
            },
        );
    }
}
