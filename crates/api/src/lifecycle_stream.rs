//! Temporary encrypted links and the SSE lifecycle progress stream.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{TimeDelta, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::lifecycle;
use crate::{ApiError, AppState};

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const SNAPSHOT_TTL: Duration = Duration::from_millis(900);

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/transactions/{hash}/lifecycle/link", post(create_link))
        .route("/v1/lifecycle/stream", get(stream_lifecycle))
}

#[derive(Debug, Default, Deserialize)]
struct LinkQuery {
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct StreamQuery {
    token: String,
}

async fn create_link(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    Query(query): Query<LinkQuery>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, ApiError> {
    let links = state
        .lifecycle_links
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("lifecycle links are not configured"))?;
    let hash = hash.trim().to_ascii_lowercase();
    qubic::decode_tx_hash(&hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;
    let ttl_seconds = links
        .resolve_ttl(query.ttl_seconds)
        .map_err(ApiError::bad_request)?;
    let token = links
        .issue(&hash, ttl_seconds)
        .map_err(ApiError::internal)?;
    let url = links.stream_url(&token, &headers, &uri);
    let expires_at = Utc::now() + TimeDelta::seconds(ttl_seconds as i64);

    Ok((
        StatusCode::OK,
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({
            "data": {
                "hash": hash,
                "url": url,
                "expires_at": expires_at,
                "expires_in_seconds": ttl_seconds,
                "content_type": "text/event-stream",
            },
            "meta": {
                "source": "encrypted-link",
                "scope": "lifecycle",
                "ttl_seconds": ttl_seconds,
            }
        })),
    )
        .into_response())
}

async fn stream_lifecycle(
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
) -> Result<Response, ApiError> {
    let links = state
        .lifecycle_links
        .as_ref()
        .ok_or_else(|| ApiError::unavailable("lifecycle links are not configured"))?;
    let claims = links.open(&query.token).map_err(|reason| {
        tracing::debug!(%reason, "rejected lifecycle link");
        ApiError::unauthorized("lifecycle link is invalid or expired")
    })?;
    let expires_in = links.expires_in(&claims);
    if expires_in.is_zero() {
        return Err(ApiError::unauthorized(
            "lifecycle link is invalid or expired",
        ));
    }

    let hash = claims.hash.clone();
    let deadline = Instant::now() + expires_in;
    let mut ticker = tokio::time::interval(POLL_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;

    let stream = async_stream::stream! {
        let mut last_signature = String::new();
        let mut waiting_sent = false;
        let mut last_warning = String::new();
        loop {
            match lifecycle::snapshot_cached(&state, &hash, SNAPSHOT_TTL).await {
                Ok(body) => {
                    waiting_sent = false;
                    last_warning.clear();
                    let progress = lifecycle::progress_payload(&body);
                    let signature = progress.to_string();
                    if signature != last_signature {
                        let state_name = progress
                            .get("state")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown");
                        let current_tick = progress
                            .get("current_tick")
                            .and_then(Value::as_u64)
                            .unwrap_or(0);
                        let event_count = progress
                            .get("event_count")
                            .and_then(Value::as_u64)
                            .unwrap_or(0);
                        let id = format!(
                            "{hash}:{state_name}:{current_tick}:{event_count}"
                        );
                        yield Ok::<Event, Infallible>(
                            Event::default()
                                .event("progress")
                                .id(id)
                                .json_data(&progress)
                                .unwrap_or_else(|_| {
                                    Event::default().event("error").data("serialize")
                                }),
                        );
                        last_signature = signature;
                        if lifecycle::is_terminal(state_name) {
                            yield Ok(Event::default()
                                .event("done")
                                .json_data(json!({
                                    "hash": hash,
                                    "state": state_name,
                                }))
                                .unwrap_or_else(|_| {
                                    Event::default().event("error").data("serialize")
                                }));
                            break;
                        }
                    }
                }
                Err(error) if error.status == StatusCode::NOT_FOUND => {
                    if !waiting_sent {
                        waiting_sent = true;
                        yield Ok(Event::default()
                            .event("waiting")
                            .json_data(json!({
                                "hash": hash,
                                "state": "unknown",
                            }))
                            .unwrap_or_else(|_| {
                                Event::default().event("error").data("serialize")
                            }));
                    }
                }
                Err(error) => {
                    tracing::debug!(%hash, %error, "lifecycle stream snapshot failed");
                    let warning = "lifecycle temporarily unavailable";
                    if last_warning != warning {
                        last_warning = warning.to_owned();
                        yield Ok(Event::default()
                            .event("warning")
                            .json_data(json!({
                                "hash": hash,
                                "message": warning,
                            }))
                            .unwrap_or_else(|_| {
                                Event::default().event("error").data("serialize")
                            }));
                    }
                }
            }

            tokio::select! {
                _ = ticker.tick() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    yield Ok(Event::default()
                        .event("link_expired")
                        .json_data(json!({
                            "hash": hash,
                            "reason": "link_expired",
                        }))
                        .unwrap_or_else(|_| {
                            Event::default().event("error").data("serialize")
                        }));
                    break;
                }
            }
        }
    };

    let mut response = Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response();
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_projection_keeps_terminal_state_and_counts() {
        let body = json!({
            "data": {
                "hash": "abc",
                "state": "confirmed",
                "current_tick": 10,
                "events": [{}, {}],
                "errors": [{}],
                "timeline": [{"stage": "included"}],
            }
        });
        let progress = lifecycle::progress_payload(&body);
        assert_eq!(progress["state"], "confirmed");
        assert_eq!(progress["event_count"], 2);
        assert_eq!(progress["error_count"], 1);
        assert!(lifecycle::is_terminal("confirmed"));
        assert!(!lifecycle::is_terminal("broadcast"));
    }
}
