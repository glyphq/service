//! Signed webhook registration and delivery.
//!
//! Webhooks are operational state: a registration binds a URL and an HMAC
//! secret to a set of events and optional transaction hashes. The dispatcher
//! persists every attempt in `webhook_deliveries`, retries with exponential
//! backoff, and dead-letters after `WEBHOOK_MAX_ATTEMPTS`.
//!
//! Signing is `X-Glyph-Signature: sha256=<hex hmac of "{timestamp}.{body}">`.
//! Consumers should recompute the HMAC with the shared secret and reject a
//! timestamp that is outside their replay window.
//!
//! Target validation rejects `localhost` and private IP literals so a webhook
//! cannot be pointed at the API host or an internal service. DNS rebinding is
//! explicitly out of scope: the check inspects the URL host, not the address
//! the resolver returns, and a name can be repointed after registration.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use chrono::Utc;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;
use store::gateway::{MempoolEntry, Webhook, WebhookDelivery};
use tokio_util::sync::CancellationToken;
use url::{Host, Url};

use crate::advanced::json_body;
use crate::{ApiError, AppState};

/// Every event a registration may subscribe to.
pub const EVENTS: &[&str] = &[
    "transaction.queued",
    "transaction.broadcast",
    "transaction.confirmed",
    "transaction.expired",
    "transaction.failed",
    "transaction.cancelled",
];

/// Shortest accepted signing secret.
pub const MIN_SECRET_LEN: usize = 16;

#[derive(Debug, Clone)]
pub struct WebhookConfig {
    pub timeout: Duration,
    pub max_attempts: u32,
    pub allow_private: bool,
}

impl WebhookConfig {
    pub fn from_env() -> Self {
        Self {
            timeout: Duration::from_millis(env_u64("WEBHOOK_TIMEOUT_MS", 5_000)),
            max_attempts: env_u64("WEBHOOK_MAX_ATTEMPTS", 6).clamp(1, 100) as u32,
            allow_private: env_flag("WEBHOOK_ALLOW_PRIVATE"),
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Body accepted by `POST /v1/webhooks`.
#[derive(Debug, Default, Deserialize)]
pub struct RegisterRequest {
    pub url: String,
    pub secret: String,
    #[serde(default)]
    pub events: Vec<String>,
    #[serde(default)]
    pub transaction_hashes: Vec<String>,
    #[serde(default)]
    pub identity: Option<String>,
}

/// Shared webhook service held in [`AppState`].
#[derive(Clone)]
pub struct Webhooks {
    gateway: Arc<store::gateway::GatewayStore>,
    config: Arc<WebhookConfig>,
    client: reqwest::Client,
    cancel: CancellationToken,
}

impl Webhooks {
    pub fn new(
        gateway: Arc<store::gateway::GatewayStore>,
        config: WebhookConfig,
        cancel: CancellationToken,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            // Do not follow redirects: a public URL must not be able to hop to
            // a private address after registration validation.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_default();
        Self {
            gateway,
            config: Arc::new(config),
            client,
            cancel,
        }
    }

    /// Registers a webhook after validating the URL, secret, events, hashes,
    /// and identity. The secret is stored but never returned by the API.
    pub async fn register(&self, request: RegisterRequest) -> Result<Webhook, ApiError> {
        let target = validate_target_url(&request.url, self.config.allow_private)
            .map_err(ApiError::bad_request)?;
        validate_secret(&request.secret).map_err(ApiError::bad_request)?;

        let mut events = Vec::new();
        for event in &request.events {
            let event = event.trim();
            if event.is_empty() {
                continue;
            }
            if !EVENTS.contains(&event) {
                return Err(ApiError::bad_request(format!(
                    "unknown webhook event {event:?}; supported events are {}",
                    EVENTS.join(", ")
                )));
            }
            if !events.iter().any(|existing| existing == event) {
                events.push(event.to_owned());
            }
        }

        let mut hashes = Vec::new();
        for hash in &request.transaction_hashes {
            let hash = hash.trim().to_ascii_lowercase();
            if hash.is_empty() {
                continue;
            }
            qubic::decode_tx_hash(&hash).map_err(|_| {
                ApiError::bad_request(format!("transaction hash {hash:?} is invalid"))
            })?;
            if !hashes.contains(&hash) {
                hashes.push(hash);
            }
        }

        let identity = match request.identity.as_deref().map(str::trim) {
            None | Some("") => String::new(),
            Some(identity) => {
                let identity = identity.to_ascii_uppercase();
                qubic::PublicKey::from_identity(&identity)
                    .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
                identity
            }
        };

        let now = Utc::now();
        let webhook = Webhook {
            id: uuid::Uuid::new_v4().to_string(),
            url: target.to_string(),
            secret: request.secret,
            events,
            transaction_hashes: hashes,
            identity,
            active: true,
            created_at: now,
            updated_at: now,
        };
        self.gateway.insert_webhook(&webhook).await?;
        Ok(webhook)
    }

    /// Registers a webhook scoped to one transaction. Used by inline mempool
    /// submissions. An empty event list subscribes to every event.
    pub async fn register_inline(
        &self,
        url: String,
        secret: String,
        hash: String,
    ) -> Result<Webhook, ApiError> {
        self.register(RegisterRequest {
            url,
            secret,
            events: Vec::new(),
            transaction_hashes: vec![hash],
            identity: None,
        })
        .await
    }

    pub async fn list(&self) -> Result<Vec<Webhook>, ApiError> {
        Ok(self.gateway.active_webhooks().await?)
    }

    pub async fn deactivate(&self, id: &str) -> Result<Option<Webhook>, ApiError> {
        let Some(mut webhook) = self.gateway.webhook(id).await? else {
            return Ok(None);
        };
        if webhook.active {
            webhook.active = false;
            webhook.updated_at = Utc::now();
            self.gateway.update_webhook(&webhook).await?;
        }
        Ok(Some(webhook))
    }

    pub async fn deliveries(&self, id: &str, limit: u32) -> Result<Vec<WebhookDelivery>, ApiError> {
        Ok(self.gateway.deliveries(id, limit).await?)
    }

    /// Queues a pending delivery for every active webhook that matches the
    /// event, transaction hash, and source identity.
    pub async fn dispatch_event(&self, event: &str, entry: &MempoolEntry) -> Result<(), ApiError> {
        let webhooks = self.gateway.active_webhooks().await?;
        if webhooks.is_empty() {
            return Ok(());
        }
        let payload = delivery_payload(event, entry);
        let now = Utc::now();
        for webhook in webhooks {
            if !webhook_matches(&webhook, event, &entry.hash, &entry.source) {
                continue;
            }
            let delivery = WebhookDelivery {
                id: uuid::Uuid::new_v4().to_string(),
                webhook_id: webhook.id,
                event: event.to_owned(),
                transaction_hash: entry.hash.clone(),
                status: "pending".to_owned(),
                attempt: 0,
                next_attempt_at: Some(now),
                response_status: None,
                error: None,
                payload: payload.clone(),
                created_at: now,
            };
            self.gateway.insert_delivery(&delivery).await?;
        }
        Ok(())
    }

    /// Background dispatcher. Polls due deliveries until cancelled.
    pub async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(error) = self.dispatch_due().await {
                        tracing::warn!(?error, "webhook dispatcher iteration failed");
                    }
                }
            }
        }
    }

    async fn dispatch_due(&self) -> Result<(), ApiError> {
        let due = self.gateway.due_deliveries(Utc::now(), 50).await?;
        for delivery in due {
            if let Err(error) = self.attempt(delivery).await {
                tracing::warn!(?error, "webhook delivery attempt failed to persist");
            }
        }
        Ok(())
    }

    /// Sends one delivery and persists the outcome. Retryable failures stay
    /// `pending` with a backoff; exhausted attempts become `failed`.
    async fn attempt(&self, delivery: WebhookDelivery) -> Result<(), ApiError> {
        let Some(webhook) = self.gateway.webhook(&delivery.webhook_id).await? else {
            return self
                .finish(
                    delivery,
                    "failed",
                    None,
                    Some("webhook no longer exists".to_owned()),
                )
                .await;
        };
        if !webhook.active {
            return self
                .finish(
                    delivery,
                    "failed",
                    None,
                    Some("webhook is inactive".to_owned()),
                )
                .await;
        }

        let timestamp = Utc::now().timestamp().to_string();
        let signature = signature_header(&webhook.secret, &timestamp, &delivery.payload);
        let response = self
            .client
            .post(&webhook.url)
            .header("X-Glyph-Event", &delivery.event)
            .header("X-Glyph-Delivery", &delivery.id)
            .header("X-Glyph-Timestamp", &timestamp)
            .header("X-Glyph-Signature", signature)
            .header("content-type", "application/json")
            .body(delivery.payload.clone())
            .send()
            .await;

        match response {
            Ok(response) if response.status().is_success() => {
                self.finish(
                    delivery,
                    "delivered",
                    Some(response.status().as_u16()),
                    None,
                )
                .await
            }
            Ok(response) => {
                let status = response.status();
                self.retry_or_fail(delivery, Some(status.as_u16()), format!("HTTP {status}"))
                    .await
            }
            Err(error) => self.retry_or_fail(delivery, None, error.to_string()).await,
        }
    }

    async fn retry_or_fail(
        &self,
        delivery: WebhookDelivery,
        response_status: Option<u16>,
        error: String,
    ) -> Result<(), ApiError> {
        let attempt = delivery.attempt.saturating_add(1);
        if attempt >= self.config.max_attempts {
            return self
                .finish_with_attempt(
                    delivery,
                    "failed",
                    response_status,
                    Some(error),
                    attempt,
                    None,
                )
                .await;
        }
        let next = Utc::now() + retry_delay(attempt);
        self.finish_with_attempt(
            delivery,
            "pending",
            response_status,
            Some(error),
            attempt,
            Some(next),
        )
        .await
    }

    async fn finish(
        &self,
        delivery: WebhookDelivery,
        status: &str,
        response_status: Option<u16>,
        error: Option<String>,
    ) -> Result<(), ApiError> {
        let attempt = delivery.attempt.saturating_add(1);
        self.finish_with_attempt(delivery, status, response_status, error, attempt, None)
            .await
    }

    async fn finish_with_attempt(
        &self,
        mut delivery: WebhookDelivery,
        status: &str,
        response_status: Option<u16>,
        error: Option<String>,
        attempt: u32,
        next: Option<chrono::DateTime<Utc>>,
    ) -> Result<(), ApiError> {
        delivery.status = status.to_owned();
        delivery.response_status = response_status;
        delivery.error = error;
        delivery.attempt = attempt;
        delivery.next_attempt_at = next;
        self.gateway.insert_delivery(&delivery).await?;
        Ok(())
    }
}

/// JSON body sent to a webhook endpoint. It is stored on the delivery row so
/// every retry sends byte-identical content.
pub fn delivery_payload(event: &str, entry: &MempoolEntry) -> String {
    json!({
        "event": event,
        "transaction": entry,
    })
    .to_string()
}

/// True when `webhook` subscribes to `event` for this hash and identity.
pub fn webhook_matches(webhook: &Webhook, event: &str, hash: &str, identity: &str) -> bool {
    if !webhook.active {
        return false;
    }
    if !webhook.events.is_empty() && !webhook.events.iter().any(|value| value == event) {
        return false;
    }
    if !webhook.transaction_hashes.is_empty()
        && !webhook.transaction_hashes.iter().any(|value| value == hash)
    {
        return false;
    }
    if !webhook.identity.is_empty() && webhook.identity != identity {
        return false;
    }
    true
}

/// Validates a webhook target. Returns the normalized URL.
///
/// DNS rebinding is out of scope: only literal hosts are checked. A deployment
/// that needs stronger guarantees should route deliveries through an egress
/// proxy with its own allowlist.
pub fn validate_target_url(raw: &str, allow_private: bool) -> Result<Url, String> {
    let url = Url::parse(raw.trim()).map_err(|error| format!("invalid webhook url: {error}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("webhook url must use http or https".into());
    }
    if url.host().is_none() {
        return Err("webhook url must include a host".into());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("webhook url must not contain credentials".into());
    }
    if allow_private {
        return Ok(url);
    }
    let blocked = match url.host() {
        Some(Host::Domain(domain)) => {
            let domain = domain.to_ascii_lowercase();
            domain == "localhost"
                || domain.ends_with(".localhost")
                || domain == "localhost.localdomain"
        }
        Some(Host::Ipv4(ip)) => private_v4(ip),
        Some(Host::Ipv6(ip)) => private_v6(ip),
        None => true,
    };
    if blocked {
        return Err(
            "webhook url targets a local or private address; set WEBHOOK_ALLOW_PRIVATE=true to allow it"
                .into(),
        );
    }
    Ok(url)
}

fn private_v4(ip: std::net::Ipv4Addr) -> bool {
    ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.octets()[0] == 0
}

fn private_v6(ip: std::net::Ipv6Addr) -> bool {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return private_v4(v4);
    }
    let segments = ip.segments();
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_unicast_link_local()
        || (segments[0] & 0xfe00) == 0xfc00
}

pub fn validate_secret(secret: &str) -> Result<(), String> {
    if secret.chars().count() < MIN_SECRET_LEN {
        return Err(format!(
            "webhook secret must be at least {MIN_SECRET_LEN} characters"
        ));
    }
    Ok(())
}

/// Hex HMAC-SHA256 of `message` under `secret`.
pub fn hmac_sha256_hex(secret: &str, message: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Hex HMAC over the signed string `"{timestamp}.{body}"`.
pub fn sign_payload(secret: &str, timestamp: &str, body: &str) -> String {
    hmac_sha256_hex(secret, &format!("{timestamp}.{body}"))
}

/// Full `X-Glyph-Signature` header value.
pub fn signature_header(secret: &str, timestamp: &str, body: &str) -> String {
    format!("sha256={}", sign_payload(secret, timestamp, body))
}

fn retry_delay(attempt: u32) -> chrono::TimeDelta {
    // 1s, 2s, 4s, 8s, ... capped at 60s.
    let seconds = 1u64
        .checked_shl(attempt.saturating_sub(1).min(6))
        .unwrap_or(64)
        .min(60);
    chrono::TimeDelta::seconds(i64::try_from(seconds).unwrap_or(60))
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/webhooks", post(register).get(list))
        .route("/v1/webhooks/{id}", delete(deactivate))
        .route("/v1/webhooks/{id}/deliveries", get(deliveries))
}

fn service(state: &AppState) -> Result<Arc<Webhooks>, ApiError> {
    state
        .webhooks
        .clone()
        .ok_or_else(|| ApiError::unavailable("webhooks are disabled; set MEMPOOL_ENABLED=true"))
}

async fn register(
    State(state): State<AppState>,
    payload: Result<Json<RegisterRequest>, JsonRejection>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    let body = json_body(payload)?;
    let webhook = service(&state)?.register(body).await?;
    Ok((StatusCode::CREATED, Json(json!({ "data": webhook }))))
}

async fn list(
    State(state): State<AppState>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    let webhooks = service(&state)?.list().await?;
    Ok(Json(json!({
        "data": webhooks,
        "meta": { "count": webhooks.len() }
    })))
}

async fn deactivate(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    let webhook = service(&state)?
        .deactivate(&id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("webhook {id} does not exist")))?;
    Ok(Json(json!({ "data": webhook })))
}

async fn deliveries(
    State(state): State<AppState>,
    Path(id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<DeliveryQuery>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    let service = service(&state)?;
    if service.gateway.webhook(&id).await?.is_none() {
        return Err(ApiError::not_found(format!("webhook {id} does not exist")));
    }
    let limit = params.limit.unwrap_or(50).clamp(1, 500);
    let deliveries = service.deliveries(&id, limit).await?;
    Ok(Json(json!({
        "data": deliveries,
        "meta": { "limit": limit }
    })))
}

#[derive(Debug, Default, Deserialize)]
struct DeliveryQuery {
    #[serde(default)]
    limit: Option<u32>,
}

/// Helper for tests and callers that want the public webhook shape without the
/// secret.
pub fn public_webhook(webhook: &Webhook) -> Value {
    serde_json::to_value(webhook).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn webhook(events: &[&str], hashes: &[&str], identity: &str) -> Webhook {
        Webhook {
            id: "id".into(),
            url: "https://example.com/hook".into(),
            secret: "0123456789abcdef".into(),
            events: events.iter().map(|value| (*value).to_owned()).collect(),
            transaction_hashes: hashes.iter().map(|value| (*value).to_owned()).collect(),
            identity: identity.to_owned(),
            active: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn hmac_matches_rfc4231_vector() {
        assert_eq!(
            hmac_sha256_hex("Jefe", "what do ya want for nothing?"),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn signature_is_over_timestamp_dot_body() {
        let secret = "0123456789abcdef";
        let timestamp = "1700000000";
        let body = r#"{"event":"transaction.confirmed"}"#;
        let expected = hmac_sha256_hex(secret, &format!("{timestamp}.{body}"));
        assert_eq!(sign_payload(secret, timestamp, body), expected);
        assert_eq!(
            signature_header(secret, timestamp, body),
            format!("sha256={expected}")
        );
    }

    #[test]
    fn secret_requires_sixteen_characters() {
        assert!(validate_secret("short").is_err());
        assert!(validate_secret("0123456789abcdef").is_ok());
    }

    #[test]
    fn rejects_localhost_and_private_targets() {
        assert!(validate_target_url("http://localhost:9000/hook", false).is_err());
        assert!(validate_target_url("http://api.localhost/hook", false).is_err());
        assert!(validate_target_url("http://127.0.0.1/hook", false).is_err());
        assert!(validate_target_url("http://10.0.0.5/hook", false).is_err());
        assert!(validate_target_url("http://192.168.1.5/hook", false).is_err());
        assert!(validate_target_url("http://169.254.169.254/latest", false).is_err());
        assert!(validate_target_url("http://[::1]/hook", false).is_err());
        assert!(validate_target_url("http://[fd00::1]/hook", false).is_err());
        assert!(validate_target_url("ftp://example.com/hook", false).is_err());
        assert!(validate_target_url("https://user:pass@example.com/hook", false).is_err());
        assert!(validate_target_url("https://example.com/hook", false).is_ok());
        assert!(validate_target_url("http://127.0.0.1:9000/hook", true).is_ok());
    }

    #[test]
    fn matching_honours_events_hashes_and_identity() {
        let all = webhook(&[], &[], "");
        assert!(webhook_matches(&all, "transaction.confirmed", "h", "ID"));

        let confirmed = webhook(&["transaction.confirmed"], &[], "");
        assert!(webhook_matches(
            &confirmed,
            "transaction.confirmed",
            "h",
            "ID"
        ));
        assert!(!webhook_matches(
            &confirmed,
            "transaction.expired",
            "h",
            "ID"
        ));

        let hashed = webhook(&[], &["abc"], "");
        assert!(webhook_matches(
            &hashed,
            "transaction.confirmed",
            "abc",
            "ID"
        ));
        assert!(!webhook_matches(
            &hashed,
            "transaction.confirmed",
            "def",
            "ID"
        ));

        let identity = webhook(&[], &[], "OWNER");
        assert!(webhook_matches(
            &identity,
            "transaction.queued",
            "h",
            "OWNER"
        ));
        assert!(!webhook_matches(
            &identity,
            "transaction.queued",
            "h",
            "OTHER"
        ));

        let mut inactive = webhook(&[], &[], "");
        inactive.active = false;
        assert!(!webhook_matches(&inactive, "transaction.queued", "h", "ID"));
    }

    #[test]
    fn payload_includes_event_and_transaction() {
        let entry = MempoolEntry {
            hash: "ab".into(),
            raw: "00".into(),
            source: "SRC".into(),
            destination: "DST".into(),
            amount: 1,
            tick: 9,
            peers: 3,
            status: store::gateway::MempoolStatus::Queued,
            attempts: 0,
            broadcast_peers: 0,
            broadcast_at: None,
            confirmed_at: None,
            confirmed_tick: None,
            error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let payload = delivery_payload("transaction.queued", &entry);
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["event"], "transaction.queued");
        assert_eq!(value["transaction"]["hash"], "ab");
    }

    #[test]
    fn retry_delay_grows_then_caps() {
        assert_eq!(retry_delay(1).num_seconds(), 1);
        assert_eq!(retry_delay(2).num_seconds(), 2);
        assert_eq!(retry_delay(3).num_seconds(), 4);
        assert_eq!(retry_delay(20).num_seconds(), 60);
    }
}
