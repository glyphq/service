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
    /// Comma-separated event names: tick, transaction, event, price, market.
    #[serde(default)]
    pub types: Option<String>,
    /// Only emit transactions and events that mention this identity.
    #[serde(default)]
    pub identity: Option<String>,
    /// Only emit asset events for `{issuer}/{name}`.
    #[serde(default)]
    pub asset: Option<String>,
}

impl StreamQuery {
    fn normalize(mut self) -> Result<Self, crate::ApiError> {
        if let Some(identity) = self.identity.as_deref() {
            let identity = identity.trim().to_ascii_uppercase();
            qubic::PublicKey::from_identity(&identity)
                .map_err(|error| crate::ApiError::invalid_identity(error.to_string()))?;
            self.identity = Some(identity);
        }
        if let Some(asset) = self.asset.as_deref() {
            let (issuer, name) = asset.split_once('/').ok_or_else(|| {
                crate::ApiError::bad_request("asset filter must be {issuer}/{name}")
            })?;
            let issuer = issuer.trim().to_ascii_uppercase();
            qubic::PublicKey::from_identity(&issuer)
                .map_err(|error| crate::ApiError::invalid_identity(error.to_string()))?;
            let name = name.trim().to_ascii_uppercase();
            if name.is_empty()
                || name.len() > 7
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || byte.is_ascii_uppercase())
            {
                return Err(crate::ApiError::bad_request(
                    "asset name must contain 1 to 7 uppercase ASCII letters or digits",
                ));
            }
            self.asset = Some(format!("{issuer}/{name}"));
        }
        Ok(self)
    }

    fn allows(&self, event: &StreamEvent) -> bool {
        if let Some(types) = self.types.as_deref() {
            let allowed = types
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .any(|value| value.eq_ignore_ascii_case(event.event));
            if !allowed {
                return false;
            }
        }
        if let Some(identity) = self.identity.as_deref() {
            if !event_matches_identity(event, identity) {
                return false;
            }
        }
        if let Some(asset) = self.asset.as_deref() {
            if !event_matches_asset(event, asset) {
                return false;
            }
        }
        true
    }

    fn only(mut self, event: &str) -> Self {
        self.types = Some(event.to_owned());
        self
    }
}

fn event_matches_identity(event: &StreamEvent, identity: &str) -> bool {
    match event.event {
        "transaction" => {
            event.data.get("source").and_then(Value::as_str) == Some(identity)
                || event.data.get("destination").and_then(Value::as_str) == Some(identity)
        }
        "event" => json_contains_identity(event.data.get("body"), identity),
        _ => false,
    }
}

fn json_contains_identity(value: Option<&Value>, identity: &str) -> bool {
    match value {
        Some(Value::String(text)) => text == identity,
        Some(Value::Array(items)) => items
            .iter()
            .any(|item| json_contains_identity(Some(item), identity)),
        Some(Value::Object(map)) => map
            .values()
            .any(|item| json_contains_identity(Some(item), identity)),
        _ => false,
    }
}

fn event_matches_asset(event: &StreamEvent, asset: &str) -> bool {
    if event.event != "event" {
        return false;
    }
    let Some((issuer, name)) = asset.split_once('/') else {
        return false;
    };
    let body = event.data.get("body");
    body.and_then(|body| body.get("issuer"))
        .and_then(Value::as_str)
        == Some(issuer)
        && body
            .and_then(|body| body.get("name"))
            .and_then(Value::as_str)
            == Some(name)
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
            let mut published_to = last;
            for tick in first..=last_batch {
                if cancel.is_cancelled() {
                    return;
                }
                match self.publish_tick(store.as_ref(), tick).await {
                    Ok(()) => published_to = tick,
                    Err(error) => {
                        // Do not advance past a failed tick: the next poll
                        // retries it so a transient store error cannot drop a
                        // block of live data permanently.
                        tracing::warn!(tick, %error, "stream tick publish failed; will retry");
                        break;
                    }
                }
            }
            last_tick = Some(published_to);
            if published_to < head.tick {
                self.publish(StreamEvent {
                    event: "catchup",
                    id: format!("catchup:{}", head.tick),
                    data: json!({
                        "last_published_tick": published_to,
                        "head_tick": head.tick,
                        "pending": head.tick - published_to,
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
        .route("/v1/stream/prices", get(sse_prices))
        .route("/v1/stream/market", get(sse_market))
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

async fn sse_prices(
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    sse_stream(state, query.only("price")).await
}

async fn sse_market(
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    sse_stream(state, query.only("market")).await
}

async fn sse_stream(
    state: AppState,
    query: StreamQuery,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    let query = query.normalize()?;
    let hub = state
        .hub
        .clone()
        .ok_or_else(|| crate::ApiError::unavailable("streaming is not enabled"))?;
    let mut receiver = hub.subscribe();
    let stream = async_stream::stream! {
        loop {
            match receiver.recv().await {
                Ok(event) if query.allows(&event) => {
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
    let query = match query.normalize() {
        Ok(query) => query,
        Err(error) => return error.into_response(),
    };
    let Some(hub) = state.hub.clone() else {
        return crate::ApiError::unavailable("streaming is not enabled").into_response();
    };
    ws.on_upgrade(move |socket| websocket(socket, hub, query))
}

async fn websocket(mut socket: WebSocket, hub: Arc<StreamHub>, query: StreamQuery) {
    let mut receiver = hub.subscribe();
    // A price- or market-only client can be idle for a long time between
    // changes. Ping it so proxies do not silently close the connection.
    let mut keep_alive = tokio::time::interval(Duration::from_secs(30));
    keep_alive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            message = receiver.recv() => {
                match message {
                    Ok(event) if query.allows(&event) => {
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
            _ = keep_alive.tick() => {
                if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break;
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

    fn stream_event(name: &'static str, data: Value) -> StreamEvent {
        StreamEvent {
            event: name,
            id: format!("{name}:1"),
            data,
        }
    }

    #[test]
    fn query_filters_event_names() {
        let query = StreamQuery {
            types: Some("tick,event".into()),
            ..StreamQuery::default()
        };
        assert!(query.allows(&stream_event("tick", json!({}))));
        assert!(query.allows(&stream_event("event", json!({}))));
        assert!(!query.allows(&stream_event("transaction", json!({}))));
    }

    #[test]
    fn empty_filter_allows_everything() {
        assert!(StreamQuery::default().allows(&stream_event("tick", json!({}))));
        assert!(StreamQuery::default().allows(&stream_event("transaction", json!({}))));
    }

    #[test]
    fn parse_types_trims_whitespace() {
        let types = parse_types(" tick, transaction ,,event ");
        assert_eq!(types.len(), 3);
        assert!(types.contains("tick"));
        assert!(types.contains("transaction"));
        assert!(types.contains("event"));
    }

    #[test]
    fn type_specific_query_overrides_the_request_filter() {
        let query = StreamQuery {
            types: Some("market".into()),
            ..StreamQuery::default()
        }
        .only("price");
        assert!(query.allows(&stream_event("price", json!({}))));
        assert!(!query.allows(&stream_event("market", json!({}))));
    }

    #[test]
    fn identity_filter_matches_transactions_and_event_bodies() {
        let identity = "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK";
        let query = StreamQuery {
            identity: Some(identity.into()),
            ..StreamQuery::default()
        };
        assert!(query.allows(&stream_event(
            "transaction",
            json!({ "source": identity, "destination": "OTHER" }),
        )));
        assert!(!query.allows(&stream_event(
            "transaction",
            json!({ "source": "OTHER", "destination": "OTHER" }),
        )));
        assert!(query.allows(&stream_event(
            "event",
            json!({ "body": { "from": "OTHER", "to": identity } }),
        )));
        assert!(!query.allows(&stream_event("tick", json!({}))));
    }

    #[test]
    fn asset_filter_matches_issuer_and_name() {
        let issuer = "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK";
        let query = StreamQuery {
            asset: Some(format!("{issuer}/QX")),
            ..StreamQuery::default()
        };
        assert!(query.allows(&stream_event(
            "event",
            json!({ "body": { "issuer": issuer, "name": "QX" } }),
        )));
        assert!(!query.allows(&stream_event(
            "event",
            json!({ "body": { "issuer": issuer, "name": "QCAP" } }),
        )));
    }

    #[test]
    fn publish_event_counts_subscribers_and_keeps_the_payload() {
        let hub = StreamHub::new();
        assert_eq!(
            hub.publish_event("price", "price:1".into(), json!({ "hour": 1 })),
            0,
            "events with no subscribers are dropped without panicking"
        );
        let mut receiver = hub.subscribe();
        assert_eq!(
            hub.publish_event("price", "price:1".into(), json!({ "hour": 1 })),
            1
        );
        let event = receiver.try_recv().expect("a subscribed receiver sees it");
        assert_eq!(event.event, "price");
        assert_eq!(event.id, "price:1");
        assert_eq!(event.data["hour"], 1);
    }

    #[tokio::test]
    async fn slow_subscribers_receive_a_lag_signal() {
        let hub = StreamHub::new();
        let mut receiver = hub.subscribe();
        for hour in 0..(CHANNEL_CAPACITY + 10) {
            hub.publish_event("price", format!("price:{hour}"), json!({ "hour": hour }));
        }
        match receiver.recv().await {
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                assert!(skipped > 0, "the lag count explains the gap");
            }
            other => panic!("expected a lag error, got {other:?}"),
        }
    }
}
