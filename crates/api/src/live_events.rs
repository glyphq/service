//! Live cache invalidation from committed writer events.
//!
//! The writer publishes a lightweight `glyph.committed` event after ClickHouse
//! commits a tick. It includes the identities touched by that tick, so the API
//! can drop stale entity and wallet entries immediately instead of waiting for
//! the 60 second freshness window.

use std::time::Duration;

use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::AppState;

pub fn spawn(state: AppState, cancel: CancellationToken) -> Option<tokio::task::JoinHandle<()>> {
    if bus::BusConfig::from_env().is_none() {
        tracing::warn!(
            "NATS_URL is not configured; live balances will wait for the freshness TTL after commits"
        );
        return None;
    }
    Some(tokio::spawn(async move {
        while !cancel.is_cancelled() {
            if let Err(error) = consume(&state, &cancel).await {
                tracing::warn!(%error, "committed event subscriber disconnected");
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            } else {
                break;
            }
        }
    }))
}

async fn consume(state: &AppState, cancel: &CancellationToken) -> anyhow::Result<()> {
    let config =
        bus::BusConfig::from_env().ok_or_else(|| anyhow::anyhow!("NATS_URL is not configured"))?;
    let bus = bus::Bus::connect(config).await?;
    let mut subscriber = bus.client().subscribe(bus::COMMITTED_SUBJECT).await?;
    tracing::info!(
        subject = bus::COMMITTED_SUBJECT,
        "subscribed to committed events"
    );
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            message = subscriber.next() => {
                let Some(message) = message else {
                    return Err(anyhow::anyhow!("committed subscription closed"));
                };
                let event = match serde_json::from_slice::<bus::TickEvent>(&message.payload) {
                    Ok(event) => event,
                    Err(error) => {
                        tracing::warn!(%error, "invalid committed event payload");
                        continue;
                    }
                };
                for identity in &event.identities {
                    state.invalidate_identity(identity, event.tick);
                }
            }
        }
    }
}
