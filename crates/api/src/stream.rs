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
use axum::http::HeaderMap;
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
/// Maximum tick span replayed through `Last-Event-ID`/`since_tick`.
const MAX_REPLAY_TICKS: u32 = 500;

#[derive(Debug, Clone, Serialize)]
pub struct StreamEvent {
    pub event: &'static str,
    pub id: String,
    pub data: Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct StreamQuery {
    /// Comma-separated event names: tick, transaction, whale, event, price, market.
    #[serde(default)]
    pub types: Option<String>,
    /// Only emit transactions and events that mention this identity.
    #[serde(default)]
    pub identity: Option<String>,
    /// Only emit asset events for `{issuer}/{name}`.
    #[serde(default)]
    pub asset: Option<String>,
    /// Replay indexed ticks after this tick before switching to live events.
    #[serde(default)]
    pub since_tick: Option<u32>,
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
        "whale" => {
            let transaction = event.data.get("transaction").unwrap_or(&event.data);
            transaction.get("source").and_then(Value::as_str) == Some(identity)
                || transaction.get("destination").and_then(Value::as_str) == Some(identity)
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
    whale_min_amount: i64,
}

impl StreamHub {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            sender,
            whale_min_amount: crate::overview::whale_min_amount(),
        }
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
        for event in self.collect_tick(store, tick).await? {
            self.publish(event);
        }
        Ok(())
    }

    async fn collect_tick(&self, store: &dyn Store, tick: u32) -> store::Result<Vec<StreamEvent>> {
        let mut events = Vec::new();
        let Some(row) = store.tick(tick).await? else {
            return Ok(events);
        };
        events.push(StreamEvent {
            event: "tick",
            id: format!("tick:{tick}"),
            data: serde_json::to_value(&row).unwrap_or(Value::Null),
        });
        let mut offset = 0;
        loop {
            let rows = store.transactions_for_tick(tick, 4096, offset).await?;
            let count = rows.len();
            for transaction in rows {
                let amount = transaction.amount;
                let hash = transaction.hash.clone();
                let data = serde_json::to_value(&transaction).unwrap_or(Value::Null);
                events.push(StreamEvent {
                    event: "transaction",
                    id: format!("tx:{hash}"),
                    data: data.clone(),
                });
                if amount >= self.whale_min_amount {
                    events.push(StreamEvent {
                        event: "whale",
                        id: format!("whale:{hash}"),
                        data: json!({
                            "amount": amount,
                            "min_amount": self.whale_min_amount,
                            "transaction": data,
                        }),
                    });
                }
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
                events.push(StreamEvent {
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
        Ok(events)
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
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    sse_stream(state, query, headers).await
}

async fn sse_ticks(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    sse_stream(state, query.only("tick"), headers).await
}

async fn sse_transactions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    sse_stream(state, query.only("transaction"), headers).await
}

async fn sse_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    sse_stream(state, query.only("event"), headers).await
}

async fn sse_prices(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    sse_stream(state, query.only("price"), headers).await
}

async fn sse_market(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    sse_stream(state, query.only("market"), headers).await
}

async fn sse_stream(
    state: AppState,
    query: StreamQuery,
    headers: HeaderMap,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, crate::ApiError> {
    let since_tick = query.since_tick.or_else(|| {
        headers
            .get("last-event-id")
            .and_then(|value| value.to_str().ok().and_then(parse_event_tick))
    });
    let query = query.normalize()?;
    let hub = state
        .hub
        .clone()
        .ok_or_else(|| crate::ApiError::unavailable("streaming is not enabled"))?;
    let mut receiver = hub.subscribe();
    let store = state.store.clone();
    let stream = async_stream::stream! {
        let mut replayed_to = since_tick.unwrap_or(0);
        if let Some(since) = since_tick {
            let head = store.head().await.ok().flatten().map(|row| row.tick);
            let to = head.unwrap_or(since);
            let from = to.saturating_sub(MAX_REPLAY_TICKS).max(since);
            yield Ok(Event::default().event("replay").json_data(json!({
                "since_tick": since,
                "from_tick": from.saturating_add(1),
                "to_tick": to,
                "truncated": to.saturating_sub(since) > MAX_REPLAY_TICKS,
                "max_replay_ticks": MAX_REPLAY_TICKS,
            })).unwrap_or_else(|_| Event::default().event("error").data("serialize")));
            for tick in from.saturating_add(1)..=to {
                match hub.collect_tick(store.as_ref(), tick).await {
                    Ok(events) => {
                        for event in events {
                            if query.allows(&event) {
                                yield Ok(sse_event(event));
                            }
                        }
                    }
                    Err(error) => {
                        yield Ok(Event::default().event("replay_error").data(error.to_string()));
                        break;
                    }
                }
            }
            replayed_to = to;
        }
        loop {
            match receiver.recv().await {
                Ok(event) if query.allows(&event) => {
                    if event_tick(&event).is_some_and(|tick| tick <= replayed_to) {
                        continue;
                    }
                    yield Ok(sse_event(event));
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

fn sse_event(event: StreamEvent) -> Event {
    Event::default()
        .event(event.event)
        .id(event.id)
        .json_data(event.data)
        .unwrap_or_else(|_| Event::default().event("error").data("serialize"))
}

fn parse_event_tick(id: &str) -> Option<u32> {
    let mut parts = id.trim().split(':');
    let kind = parts.next()?;
    match kind {
        "tick" | "event" => parts.next()?.parse().ok(),
        _ => None,
    }
}

fn event_tick(event: &StreamEvent) -> Option<u32> {
    event
        .data
        .get("tick")
        .and_then(Value::as_u64)
        .and_then(|tick| u32::try_from(tick).ok())
        .or_else(|| {
            event
                .data
                .get("transaction")
                .and_then(|transaction| transaction.get("tick"))
                .and_then(Value::as_u64)
                .and_then(|tick| u32::try_from(tick).ok())
        })
        .or_else(|| parse_event_tick(&event.id))
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
    let mut query = query;
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
                    Some(Ok(Message::Text(text))) => {
                        let response = match ws_command(&text, &mut query) {
                            Ok(value) => json!({ "event": "subscribed", "data": value }),
                            Err(error) => json!({
                                "event": "error",
                                "data": { "code": error.code, "detail": error.detail },
                            }),
                        };
                        if socket.send(Message::Text(response.to_string().into())).await.is_err() {
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

fn ws_command(text: &str, query: &mut StreamQuery) -> Result<Value, crate::ApiError> {
    let value: Value = serde_json::from_str(text)
        .map_err(|error| crate::ApiError::bad_request(format!("invalid JSON command: {error}")))?;
    let action = value
        .get("action")
        .and_then(Value::as_str)
        .ok_or_else(|| crate::ApiError::bad_request("command requires action"))?;
    match action {
        "subscribe" | "filters" => {
            let types = match value.get("types") {
                Some(Value::Array(items)) => Some(
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(","),
                ),
                Some(Value::String(text)) => Some(text.clone()),
                Some(Value::Null) | None => None,
                Some(_) => {
                    return Err(crate::ApiError::bad_request(
                        "types must be a string or an array of strings",
                    ));
                }
            }
            .filter(|text| !text.trim().is_empty());
            let identity = value
                .get("identity")
                .and_then(Value::as_str)
                .map(str::to_string);
            let asset = value
                .get("asset")
                .and_then(Value::as_str)
                .map(str::to_string);
            *query = StreamQuery {
                types,
                identity,
                asset,
                since_tick: None,
            }
            .normalize()?;
        }
        "unsubscribe" => {
            let remove: HashSet<&str> = value
                .get("types")
                .and_then(Value::as_array)
                .map(|items| items.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            if remove.is_empty() {
                *query = StreamQuery::default();
            } else if let Some(types) = query.types.as_deref() {
                let kept: Vec<&str> = types
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty() && !remove.contains(value))
                    .collect();
                query.types = (!kept.is_empty()).then(|| kept.join(","));
            }
        }
        other => {
            return Err(crate::ApiError::bad_request(format!(
                "unknown command {other:?}; expected subscribe or unsubscribe"
            )));
        }
    }
    Ok(json!({
        "types": query.types,
        "identity": query.identity,
        "asset": query.asset,
    }))
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

    #[test]
    fn event_ids_parse_to_replay_ticks() {
        assert_eq!(parse_event_tick("tick:123"), Some(123));
        assert_eq!(parse_event_tick("event:456:9"), Some(456));
        assert_eq!(parse_event_tick("tx:abcd"), None);
        assert_eq!(parse_event_tick("whale:abcd"), None);
    }

    #[test]
    fn websocket_commands_replace_and_remove_filters() {
        let mut query = StreamQuery::default();
        let identity = "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK";
        let ack = ws_command(
            &format!(
                r#"{{"action":"subscribe","types":["tick","whale"],"identity":"{identity}"}}"#
            ),
            &mut query,
        )
        .expect("subscribe");
        assert_eq!(query.types.as_deref(), Some("tick,whale"));
        assert_eq!(query.identity.as_deref(), Some(identity));
        assert_eq!(ack["types"], "tick,whale");

        ws_command(r#"{"action":"unsubscribe","types":["whale"]}"#, &mut query)
            .expect("unsubscribe one");
        assert_eq!(query.types.as_deref(), Some("tick"));

        ws_command(r#"{"action":"unsubscribe"}"#, &mut query).expect("reset");
        assert!(query.types.is_none());
        assert!(query.identity.is_none());
    }
}
