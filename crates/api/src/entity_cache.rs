//! Cached peer spectrum entities with persisted last-known values.
//!
//! Callers get an immediate response from memory or ClickHouse and a peer
//! refresh happens in the background. Only a completely unknown identity waits
//! for the peer, and that wait is bounded.

use std::time::Duration;

use chrono::{DateTime, Utc};
use qubic::PublicKey;
use qubic::messages::RespondEntity;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{ApiError, AppState};

/// How long a peer observation is considered fresh for online reads.
pub const ENTITY_FRESH: Duration = Duration::from_secs(60);
/// Maximum wait when no cached or persisted entity exists yet.
const FIRST_FETCH_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedEntity {
    pub identity: String,
    pub public_key: String,
    pub spectrum_index: i32,
    pub tick: u32,
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub balance: i64,
    pub incoming_transfers: u64,
    pub outgoing_transfers: u64,
    pub latest_incoming_tick: u32,
    pub latest_outgoing_tick: u32,
    pub observed_at: DateTime<Utc>,
}

impl CachedEntity {
    pub fn from_response(identity: &str, response: &RespondEntity) -> Self {
        let entity = &response.entity;
        let incoming_amount = entity.incoming_amount;
        let outgoing_amount = entity.outgoing_amount;
        Self {
            identity: identity.to_string(),
            public_key: PublicKey(entity.public_key).to_hex(),
            spectrum_index: response.spectrum_index,
            tick: response.tick,
            incoming_amount,
            outgoing_amount,
            balance: incoming_amount.saturating_sub(outgoing_amount),
            incoming_transfers: u64::from(entity.number_of_incoming_transfers),
            outgoing_transfers: u64::from(entity.number_of_outgoing_transfers),
            latest_incoming_tick: entity.latest_incoming_transfer_tick,
            latest_outgoing_tick: entity.latest_outgoing_transfer_tick,
            observed_at: Utc::now(),
        }
    }

    pub fn from_snapshot(snapshot: &store::EntitySnapshot) -> Self {
        Self {
            identity: snapshot.identity.clone(),
            public_key: snapshot.public_key.clone(),
            spectrum_index: snapshot.spectrum_index,
            tick: snapshot.tick,
            incoming_amount: snapshot.incoming_amount,
            outgoing_amount: snapshot.outgoing_amount,
            balance: snapshot.balance(),
            incoming_transfers: snapshot.incoming_transfers,
            outgoing_transfers: snapshot.outgoing_transfers,
            latest_incoming_tick: snapshot.latest_incoming_tick,
            latest_outgoing_tick: snapshot.latest_outgoing_tick,
            observed_at: snapshot.observed_at,
        }
    }

    pub fn snapshot(&self) -> store::EntitySnapshot {
        store::EntitySnapshot {
            identity: self.identity.clone(),
            public_key: self.public_key.clone(),
            spectrum_index: self.spectrum_index,
            tick: self.tick,
            incoming_amount: self.incoming_amount,
            outgoing_amount: self.outgoing_amount,
            incoming_transfers: self.incoming_transfers,
            outgoing_transfers: self.outgoing_transfers,
            latest_incoming_tick: self.latest_incoming_tick,
            latest_outgoing_tick: self.latest_outgoing_tick,
            observed_at: self.observed_at,
        }
    }

    pub fn age_seconds(&self) -> u64 {
        (Utc::now() - self.observed_at).num_seconds().max(0) as u64
    }

    pub fn balance_source(&self) -> &'static str {
        if self.spectrum_index < 0 {
            "unavailable"
        } else {
            "spectrum"
        }
    }

    pub fn data(&self) -> Value {
        json!({
            "identity": self.identity,
            "public_key": self.public_key,
            "spectrum_index": self.spectrum_index,
            "tick": self.tick,
            "incoming_amount": self.incoming_amount,
            "outgoing_amount": self.outgoing_amount,
            "balance": self.balance,
            "incoming_transfers": self.incoming_transfers,
            "outgoing_transfers": self.outgoing_transfers,
            "latest_incoming_tick": self.latest_incoming_tick,
            "latest_outgoing_tick": self.latest_outgoing_tick,
            "observed_at": self.observed_at,
            "balance_source": self.balance_source(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct EntityMeta {
    pub source: &'static str,
    pub stale: bool,
    pub age_seconds: u64,
    pub refreshing: bool,
}

impl EntityMeta {
    pub fn value(&self, entity: &CachedEntity) -> Value {
        json!({
            "source": self.source,
            "stale": self.stale,
            "age_seconds": self.age_seconds,
            "refreshing": self.refreshing,
            "balance_source": entity.balance_source(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct EntityResult {
    pub entity: CachedEntity,
    pub meta: EntityMeta,
}

pub fn cache_key(identity: &str) -> String {
    format!("entity:{identity}")
}

fn refresh_key(identity: &str) -> String {
    format!("entity-refresh:{identity}")
}

/// Returns a cached or persisted entity without waiting for a peer.
///
/// A stale result schedules one background refresh and reports that in its
/// metadata.
pub async fn cached(state: &AppState, identity: &str) -> Result<Option<EntityResult>, ApiError> {
    let key = cache_key(identity);
    if let Some((value, age)) = state.live_cache.peek(&key) {
        let Ok(entity) = serde_json::from_value::<CachedEntity>(value) else {
            return Ok(None);
        };
        if state
            .dirty_entity_tick(identity)
            .is_some_and(|tick| entity.tick < tick)
        {
            return Ok(None);
        }
        state.clear_dirty_entity(identity, entity.tick);
        if entity.age_seconds() < ENTITY_FRESH.as_secs() {
            return Ok(Some(EntityResult {
                meta: EntityMeta {
                    source: "cache",
                    stale: false,
                    age_seconds: entity.age_seconds(),
                    refreshing: false,
                },
                entity,
            }));
        }
        let refreshing = spawn_refresh(state.clone(), identity.to_string());
        return Ok(Some(EntityResult {
            meta: EntityMeta {
                source: "cache",
                stale: true,
                age_seconds: age.as_secs().max(entity.age_seconds()),
                refreshing,
            },
            entity,
        }));
    }

    let snapshot = match state.store.entity_snapshot(identity).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::debug!(%identity, %error, "entity snapshot read failed");
            None
        }
    };
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    if state
        .dirty_entity_tick(identity)
        .is_some_and(|tick| snapshot.tick < tick)
    {
        return Ok(None);
    }
    state.clear_dirty_entity(identity, snapshot.tick);
    let entity = CachedEntity::from_snapshot(&snapshot);
    let age = entity.age_seconds();
    let value = serde_json::to_value(&entity)
        .map_err(|error| ApiError::internal(format!("entity encoding failed: {error}")))?;
    state.live_cache.put(key, value);
    let refreshing = spawn_refresh(state.clone(), identity.to_string());
    Ok(Some(EntityResult {
        meta: EntityMeta {
            source: "snapshot",
            stale: age >= ENTITY_FRESH.as_secs(),
            age_seconds: age,
            refreshing,
        },
        entity,
    }))
}

/// Returns an entity immediately when cached, otherwise performs one bounded
/// peer lookup.
pub async fn get_or_fetch(state: &AppState, identity: &str) -> Result<EntityResult, ApiError> {
    if let Some(result) = cached(state, identity).await? {
        return Ok(result);
    }
    match tokio::time::timeout(FIRST_FETCH_TIMEOUT, refresh(state, identity)).await {
        Ok(result) => result,
        Err(_) => Err(ApiError::unavailable(format!(
            "entity lookup for {identity} timed out"
        ))),
    }
}

/// Starts a background refresh unless one is already running for this identity.
pub fn spawn_refresh(state: AppState, identity: String) -> bool {
    let Some(guard) = state.refreshes.try_start(refresh_key(&identity)) else {
        return false;
    };
    tokio::spawn(async move {
        let _guard = guard;
        if let Err(error) = refresh(&state, &identity).await {
            tracing::debug!(%identity, %error, "background entity refresh failed");
        }
    });
    true
}

async fn refresh(state: &AppState, identity: &str) -> Result<EntityResult, ApiError> {
    let key = PublicKey::from_identity(identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let response = state
        .pool
        .entity(key.0)
        .await
        .map_err(|error| ApiError::unavailable(format!("entity lookup failed: {error}")))?;
    let entity = CachedEntity::from_response(identity, &response);
    let behind = state
        .dirty_entity_tick(identity)
        .is_some_and(|tick| entity.tick < tick);
    if behind {
        return Ok(EntityResult {
            meta: EntityMeta {
                source: "peer",
                stale: true,
                age_seconds: 0,
                refreshing: false,
            },
            entity,
        });
    }
    if let Err(error) = state.store.put_entity_snapshot(&entity.snapshot()).await {
        tracing::warn!(%identity, %error, "entity snapshot was not persisted");
    }
    state.clear_dirty_entity(identity, entity.tick);
    let value = serde_json::to_value(&entity)
        .map_err(|error| ApiError::internal(format!("entity encoding failed: {error}")))?;
    state.live_cache.put(cache_key(identity), value);
    Ok(EntityResult {
        meta: EntityMeta {
            source: "peer",
            stale: false,
            age_seconds: 0,
            refreshing: false,
        },
        entity,
    })
}
