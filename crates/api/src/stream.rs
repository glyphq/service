//! Live tick, transaction, and event streaming over SSE and WebSocket.
//!
//! The hub polls the local index head. It never reads from a peer, so a slow
//! consumer cannot delay verification or ingest.

use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::get;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use store::Store;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::AppState;
use crate::dto::event_value;

const CHANNEL_CAPACITY: usize = 4096;
const MAX_TICKS_PER_POLL: u32 = 100;

#[derive(Debug, Clone, Serialize)]
pub struct StreamEvent {
    pub event: &'static str,
    pub id: String,
    pub data: Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct StreamQuery {
    /// Comma-separated event names: tick, transaction, event.
    #[serde(default)]
    pub types: Option<String>,
}

impl StreamQuery {
    fn allows(&self, event: &str) -> bool {
        let Some(types) = self.types.as_deref() else {
            return true;
        };
        types
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .any(|value| value.eq_ignore_ascii_case(event))
    }

    fn only(mut self, event: &str) -> Self {
        self.types = Some(event.to_owned());
        self
    }
}

pub struct StreamHub {
    sender: broadcast::Sender<StreamEvent>,
}

impl StreamHub {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.sender.subscribe()
    }

    fn publish(&self, event: StreamEvent) {
        let _ = self.sender.send(event);
    }

    /// Publishes a typed event from another service, such as a price update.
    /// Returns the number of active subscribers that received it.
    pub fn publish_event(&self, event: &'static str, id: String, data: Value) -> usize {
        self.sender
            .send(StreamEvent { event, id, data })
            .unwrap_or(0)
    }

    /// Polls the index head until `cancel` is triggered.
    pub async fn run(
        self: Arc<Self>,
        store: Arc<dyn Store>,
        poll_interval: Duration,
        cancel: CancellationToken,
    ) {
        let mut last_tick = None;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(poll_interval) => {}
            }
            let head = match store.head().await {
                Ok(Some(head)) => head,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(%error, "stream head poll failed");
                    continue;
                }
            };
            let Some(last) = last_tick else {
                last_tick = Some(head.tick);
                continue;
            };
            if head.tick <= last {
                continue;
            }
            let first = last.saturating_add(1);
            let last_batch = head.tick.min(first.saturating_add(MAX_TICKS_PER_POLL - 1));
            for tick in first..=last_batch {
                if cancel.is_cancelled() {
                    return;
                }
                if let Err(error) = self.publish_tick(store.as_ref(), tick).await {
                    tracing::warn!(tick, %error, "stream tick publish failed");
                }
            }
            last_tick = Some(last_batch);
            if last_batch < head.tick {
                self.publish(StreamEvent {
                    event: "catchup",
                    id: format!("catchup:{}", head.tick),
                    data: json!({
                        "last_published_tick": last_batch,
                        "head_tick": head.tick,
                        "pending": head.tick - last_batch,
                    }),
                });
            }
        }
    }

    async fn publish_tick(&self, store: &dyn Store, tick: u32) -> store::Result<()> {
        let Some(row) = store.tick(tick).await? else {
            return Ok(());
        };
        self.publish(StreamEvent {
            event: "tick",
            id: format!("tick:{tick}"),
            data: serde_json::to_value(&row).unwrap_or(Value::Null),
        });
        let mut offset = 0;
        loop {
            let rows = store.transactions_for_tick(tick, 4096, offset).await?;
            let count = rows.len();
            for transaction in rows {
                self.publish(StreamEvent {
                    event: "transaction",
                    id: format!("tx:{}", transaction.hash),
                    data: serde_json::to_value(&transaction).unwrap_or(Value::Null),
                });
            }
            if count < 4096 {
                break;
            }
            offset += count as u64;
        }
        let mut offset = 0;
        loop {
            let rows = store.events_for_tick(tick, 4096, offset).await?;
            let count = rows.len();
            for event in rows {
                let log_id = event.log_id;
                self.publish(StreamEvent {
                    event: "event",
                    id: format!("event:{tick}:{log_id}"),
                    data: event_value(event),
                });
            }
            if count < 4096 {
                break;
            }
            offset += count as u64;
        }
        Ok(())
    }
}

impl Default for StreamHub {
    fn default() -> Self {
        Self::new()
    }
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/stream", get(sse_handler))
        .route("/v1/stream/ticks", get(sse_ticks))
        .route("/v1/stream/transactions", get(sse_transactions))
        .route("/v1/stream/events", get(sse_events))
        .route("/v1/ws", get(ws_handler))
}

async fn sse_handler(
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    sse_stream(state, query).await
}

async fn sse_ticks(
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    sse_stream(state, query.only("tick")).await
}

async fn sse_transactions(
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    sse_stream(state, query.only("transaction")).await
}

async fn sse_events(
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    sse_stream(state, query.only("event")).await
}

async fn sse_stream(
    state: AppState,
    query: StreamQuery,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    let hub = state
        .hub
        .clone()
        .ok_or_else(|| crate::ApiError::unavailable("streaming is not enabled"))?;
    let mut receiver = hub.subscribe();
    let stream = async_stream::stream! {
        loop {
            match receiver.recv().await {
                Ok(event) if query.allows(event.event) => {
                    let event = Event::default()
                        .event(event.event)
                        .id(event.id)
                        .json_data(event.data)
                        .unwrap_or_else(|_| Event::default().event("error").data("serialize"));
                    yield Ok(event);
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    yield Ok(Event::default().event("lag").data(count.to_string()));
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
) -> impl IntoResponse {
    let Some(hub) = state.hub.clone() else {
        return crate::ApiError::unavailable("streaming is not enabled").into_response();
    };
    ws.on_upgrade(move |socket| websocket(socket, hub, query))
}

async fn websocket(mut socket: WebSocket, hub: Arc<StreamHub>, query: StreamQuery) {
    let mut receiver = hub.subscribe();
    loop {
        tokio::select! {
            message = receiver.recv() => {
                match message {
                    Ok(event) if query.allows(event.event) => {
                        let payload = serde_json::to_string(&event).unwrap_or_else(|_| "{}".into());
                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        let payload = json!({"event":"lag","data":{"skipped":count}}).to_string();
                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }
}

/// Parses a comma-separated type list for tests and documentation examples.
pub fn parse_types(value: &str) -> HashSet<&str> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_filters_event_names() {
        let query = StreamQuery {
            types: Some("tick,event".into()),
        };
        assert!(query.allows("tick"));
        assert!(query.allows("event"));
        assert!(!query.allows("transaction"));
    }

    #[test]
    fn empty_filter_allows_everything() {
        assert!(StreamQuery::default().allows("tick"));
        assert!(StreamQuery::default().allows("transaction"));
    }

    #[test]
    fn parse_types_trims_whitespace() {
        let types = parse_types(" tick, transaction ,,event ");
        assert_eq!(types.len(), 3);
        assert!(types.contains("tick"));
        assert!(types.contains("transaction"));
        assert!(types.contains("event"));
    }
}
