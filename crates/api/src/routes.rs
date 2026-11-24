use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::time::Duration;

use axum::extract::{FromRequestParts, State};
use axum::http::request::Parts;
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use qubic::messages::RespondEntity;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use store::{EventFilter, TransactionFilter};

use crate::dto::{Page, PageParams, event_value, transaction_value};
use crate::{ApiError, AppState};

const IMMUTABLE: &str = "public, max-age=31536000, immutable";
const RECENT: &str = "public, max-age=2, stale-while-revalidate=5";
const AGGREGATE: &str = "public, max-age=30, stale-while-revalidate=60";
/// Initial recent-page scan window in ticks. Sparse collections expand this
/// until the page is full or the indexed range is exhausted.
const RECENT_PAGE_START_TICKS: u32 = 64;
/// Largest bounded window before falling back to an unbounded historical page.
const RECENT_PAGE_MAX_TICKS: u32 = 8_192;
/// Maximum concurrent peer spectrum lookups used to enrich top identities.
const TOP_IDENTITY_CONCURRENCY: usize = 8;
/// A single peer lookup must not hold the analytics request open.
const TOP_IDENTITY_PEER_TIMEOUT: Duration = Duration::from_millis(300);
/// Enrichment is best effort; after this budget every missed row falls back to
/// its indexed values instead of delaying the response.
const TOP_IDENTITY_BUDGET: Duration = Duration::from_millis(800);
const LOGO_DARK_PNG: &[u8] = include_bytes!("../assets/logo-dark.png");
const LOGO_LIGHT_PNG: &[u8] = include_bytes!("../assets/logo-light.png");
const FAVICON_PNG: &[u8] = include_bytes!("../assets/favicon.png");
const SPACE_GROTESK_WOFF2: &[u8] = include_bytes!("../assets/space-grotesk-latin.woff2");
const SPACE_GROTESK_LICENSE: &[u8] = include_bytes!("../assets/SPACE_GROTESK_OFL.txt");
const FONT_PRELOAD: &str = r#"<link rel="preload" href="/fonts/space-grotesk.woff2" as="font" type="font/woff2" crossorigin>"#;

/// [`axum::extract::Path`] with API error responses.
///
/// The built-in extractor rejects malformed path segments with `text/plain`,
/// which makes numeric path parameters the one place where the API does not
/// return problem JSON. Routing extraction failures through [`ApiError`] keeps
/// the error contract consistent across the surface.
struct Path<T>(T);

/// [`axum::extract::Query`] with API error responses.
struct Query<T>(T);

impl<T, S> FromRequestParts<S> for Path<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        axum::extract::Path::<T>::from_request_parts(parts, state)
            .await
            .map(|axum::extract::Path(value)| Self(value))
            .map_err(|rejection| ApiError::bad_request(rejection.to_string()))
    }
}

impl<T, S> FromRequestParts<S> for Query<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        axum::extract::Query::<T>::from_request_parts(parts, state)
            .await
            .map(|axum::extract::Query(value)| Self(value))
            .map_err(|rejection| ApiError::bad_request(rejection.to_string()))
    }
}

const DOCS_BRAND: &str = r#"<style>
@font-face { font-family: "Space Grotesk"; font-style: normal; font-weight: 300 700; font-display: swap; src: url("/fonts/space-grotesk.woff2") format("woff2"); }
:root, .light-mode, .dark-mode { --scalar-font: "Space Grotesk", ui-sans-serif, system-ui, sans-serif; --scalar-font-code: ui-monospace, "SFMono-Regular", "Cascadia Mono", "Roboto Mono", monospace; }
body, button, input, select, textarea { font-family: "Space Grotesk", ui-sans-serif, system-ui, sans-serif; }
.glyph-brand { display: flex; align-items: center; gap: 8px; padding: 14px 14px 2px; font: 500 14px/1.2 var(--scalar-font, ui-sans-serif, system-ui, sans-serif); color: var(--scalar-sidebar-color-1, #fff); text-decoration: none; }
.glyph-brand:hover { color: var(--scalar-sidebar-color-active, #fff); }
.glyph-brand__mark { display: block; width: 22px; height: 22px; }
.glyph-brand__mark--light { display: none; }
.light-mode .glyph-brand__mark--dark { display: none; }
.light-mode .glyph-brand__mark--light { display: block; }
.glyph-brand__suffix { color: var(--scalar-sidebar-color-2, #999); font-weight: 400; }
</style>
<script>
(() => {
  let observer;
  const mount = () => {
    const sidebar = document.querySelector('aside[aria-label^="Sidebar"]');
    if (!sidebar) return false;
    if (sidebar.querySelector(".glyph-brand")) {
      observer?.disconnect();
      return true;
    }
    const link = document.createElement("a");
    link.className = "glyph-brand";
    link.href = "https://glyphq.org";
    link.target = "_blank";
    link.rel = "noreferrer";
    link.setAttribute("aria-label", "Glyph");
    link.innerHTML = '<img class="glyph-brand__mark glyph-brand__mark--dark" src="/logo.png" alt="" width="22" height="22">'
      + '<img class="glyph-brand__mark glyph-brand__mark--light" src="/logo-light.png" alt="" width="22" height="22">'
      + '<span>Glyph <span class="glyph-brand__suffix">API</span></span>';
    sidebar.prepend(link);
    observer?.disconnect();
    return true;
  };
  observer = new MutationObserver(mount);
  observer.observe(document.body, { childList: true, subtree: true });
  mount();
})();
</script>"#;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/status", get(status))
        .route("/v1/epochs", get(epochs))
        .route("/v1/epochs/{epoch}", get(epoch))
        .route("/v1/epochs/{epoch}/computors", get(epoch_computors))
        .route("/v1/ticks", get(ticks))
        .route("/v1/ticks/{tick}", get(tick))
        .route("/v1/ticks/{tick}/evidence", get(tick_evidence))
        .route("/v1/ticks/{tick}/transactions", get(tick_transactions))
        .route("/v1/ticks/{tick}/events", get(tick_events))
        .route(
            "/v1/transactions",
            get(transactions).post(crate::advanced::broadcast_transaction),
        )
        .route("/v1/transactions/batch", post(transactions_batch))
        .route("/v1/transactions/{hash}", get(transaction))
        .route("/v1/events", get(events))
        .route("/v1/identities", get(identities))
        .route(
            "/v1/identities/{identity}/transactions",
            get(identity_transactions),
        )
        .route("/v1/identities/{identity}/activity", get(identity_activity))
        .route("/v1/identities/{identity}/events", get(identity_events))
        .route("/v1/identities/{identity}", get(identity))
        .route("/v1/assets", get(assets))
        .route("/v1/assets/{issuer}/{name}", get(asset))
        .route("/v1/analytics/activity", get(analytics_activity))
        .route(
            "/v1/analytics/top-identities",
            get(analytics_top_identities),
        )
        .route("/v1/analytics/event-types", get(analytics_event_types))
        .route("/v1/search", get(search))
}

#[derive(Debug, Default, Deserialize)]
struct AnalyticsQuery {
    #[serde(default)]
    interval: Option<u32>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct BatchTransactions {
    hashes: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct TickListQuery {
    #[serde(default)]
    epoch: Option<u16>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct TransactionQuery {
    #[serde(default)]
    epoch: Option<u16>,
    #[serde(default)]
    tick: Option<u32>,
    #[serde(default)]
    identity: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct EventQuery {
    #[serde(default)]
    epoch: Option<u16>,
    #[serde(default)]
    tick: Option<u32>,
    #[serde(default, rename = "type")]
    event_type: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct AssetQuery {
    #[serde(default)]
    issuer: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    offset: Option<u64>,
}

impl TickListQuery {
    fn page(&self) -> PageParams {
        PageParams {
            limit: self.limit,
            cursor: self.cursor.clone(),
            offset: self.offset,
        }
    }
}

impl TransactionQuery {
    fn page(&self) -> PageParams {
        PageParams {
            limit: self.limit,
            cursor: self.cursor.clone(),
            offset: self.offset,
        }
    }
}

impl EventQuery {
    fn page(&self) -> PageParams {
        PageParams {
            limit: self.limit,
            cursor: self.cursor.clone(),
            offset: self.offset,
        }
    }
}

impl AssetQuery {
    fn page(&self) -> PageParams {
        PageParams {
            limit: self.limit,
            cursor: self.cursor.clone(),
            offset: self.offset,
        }
    }
}

async fn healthz() -> Json<Value> {
    Json(json!({ "ok": true }))
}

async fn status(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let cache_key = "status:body";
    if let Some(value) = state.live_cache.get(cache_key, Duration::from_secs(1)) {
        return Ok((
            [(
                header::CACHE_CONTROL,
                "public, max-age=1, stale-while-revalidate=2",
            )],
            Json(value),
        ));
    }
    let (head, earliest, network) = tokio::join!(
        state.store.head(),
        state.store.earliest_tick(),
        tokio::time::timeout(Duration::from_secs(2), state.pool.head()),
    );
    let head = head?;
    let earliest = earliest?;
    let stats = match state.live_cache.get("status:stats", Duration::from_secs(5)) {
        Some(stats) => stats,
        None => {
            let stats = state.store.stats().await?;
            state.live_cache.put("status:stats", stats.clone());
            stats
        }
    };
    let (network, network_error) = match network {
        Ok(Ok(network)) => (Some(network), None),
        Ok(Err(error)) => (None, Some(error.to_string())),
        Err(_) => (None, Some("peer head request timed out".to_string())),
    };
    let last_indexed = stats
        .get("ingest")
        .and_then(|ingest| ingest.get("last_indexed_tick"))
        .and_then(Value::as_u64)
        .and_then(|tick| u32::try_from(tick).ok());
    let indexed_ticks = stats
        .get("counts")
        .and_then(|counts| counts.get("ticks"))
        .and_then(Value::as_u64);
    let network_head = network.as_ref().map(|head| head.tick);
    let rates = state.progress.observe(last_indexed, network_head);
    let lag_ticks = network_head
        .zip(last_indexed)
        .map(|(network, indexed)| network.saturating_sub(indexed));
    let eta_seconds = lag_ticks
        .zip(rates.ingest)
        .filter(|(_, rate)| *rate > 0.0)
        .map(|(lag, rate)| (f64::from(lag) / rate).round() as u64);
    let backfill = backfill_json(
        network.as_ref().map(|head| head.initial_tick),
        network_head,
        earliest,
        last_indexed,
    );
    let body = json!({
        "data": {
            "head": head,
            "ingest": stats.get("ingest").cloned().unwrap_or(Value::Null),
            "ingest_status": {
                "last_indexed_tick": last_indexed,
                "indexed_ticks": indexed_ticks,
                "ticks_per_second": rates.ingest,
                "lag_ticks": lag_ticks,
                "eta_seconds": eta_seconds,
            },
            "network": {
                "reachable": network.is_some(),
                "head": network_head,
                "epoch": network.as_ref().map(|head| head.epoch),
                "initial_tick": network.as_ref().map(|head| head.initial_tick),
                "tick_duration": network.as_ref().map(|head| head.tick_duration),
                "ticks_per_second": rates.network,
                "error": network_error,
            },
            "backfill": backfill,
            "stats": stats,
        },
        "meta": {
            "source": "direct-qubic-peers",
            "version": env!("CARGO_PKG_VERSION"),
            "uptime_seconds": state.uptime_seconds(),
            "streaming": state.hub.is_some(),
            "broadcast": state.allow_broadcast,
            "archive": state.archive.is_some(),
            "peers_configured": state.peers.len(),
        }
    });
    state.live_cache.put(cache_key, body.clone());
    Ok((
        [(
            header::CACHE_CONTROL,
            "public, max-age=1, stale-while-revalidate=2",
        )],
        Json(body),
    ))
}

fn backfill_json(
    initial_tick: Option<u32>,
    network_head: Option<u32>,
    earliest_tick: Option<u32>,
    last_indexed_tick: Option<u32>,
) -> Value {
    let Some(initial_tick) = initial_tick else {
        return json!({ "available": false, "reason": "network head is unavailable" });
    };
    let epoch_ticks = network_head.map(|head| u64::from(head.saturating_sub(initial_tick)) + 1);
    let missing_before =
        earliest_tick.map(|earliest| u64::from(earliest.saturating_sub(initial_tick)));
    let complete = earliest_tick.is_some_and(|earliest| earliest <= initial_tick);
    let remaining = network_head
        .zip(last_indexed_tick)
        .map(|(head, last)| u64::from(head.saturating_sub(last)));
    // Count only ticks that belong to the current epoch so the coverage value does not
    // drift when a previous epoch remains indexed after the epoch boundary.
    let indexed_in_epoch = match (network_head, earliest_tick, last_indexed_tick) {
        (Some(head), Some(earliest), Some(last)) => {
            let start = earliest.max(initial_tick);
            let end = last.min(head);
            (end >= start).then(|| u64::from(end - start) + 1)
        }
        _ => None,
    };
    let coverage = match (indexed_in_epoch, epoch_ticks) {
        (Some(indexed), Some(total)) if total > 0 => {
            Some((indexed as f64 / total as f64).clamp(0.0, 1.0))
        }
        _ => None,
    };
    json!({
        "available": true,
        "complete": complete,
        "initial_tick": initial_tick,
        "first_indexed_tick": earliest_tick,
        "last_indexed_tick": last_indexed_tick,
        "missing_before": missing_before,
        "indexed_ticks": indexed_in_epoch,
        "epoch_ticks": epoch_ticks,
        "remaining_ticks": remaining,
        "coverage": coverage,
        "coverage_percent": coverage.map(|value| (value * 100.0 * 100.0).round() / 100.0),
    })
}

async fn epochs(
    State(state): State<AppState>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    let page = params.resolve("epochs")?;
    let mut rows = state.store.epochs(page.fetch_limit(), page.offset).await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn epoch(
    State(state): State<AppState>,
    Path(epoch): Path<u16>,
) -> Result<impl IntoResponse, ApiError> {
    let row = state
        .store
        .epoch(epoch)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("epoch {epoch} is not indexed")))?;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({ "data": row })),
    ))
}

async fn epoch_computors(
    State(state): State<AppState>,
    Path(epoch): Path<u16>,
) -> Result<impl IntoResponse, ApiError> {
    let rows = state.store.computors(epoch).await?;
    if rows.is_empty() {
        return Err(ApiError::not_found(format!(
            "computors for epoch {epoch} are not indexed"
        )));
    }
    let count = rows.len();
    let rows: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            json!({
                "epoch": row.epoch,
                "computor_index": row.computor_index,
                "public_key": row.public_key,
                "identity": row.identity,
            })
        })
        .collect();
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": rows,
            "meta": { "epoch": epoch, "count": count },
        })),
    ))
}

async fn ticks(
    State(state): State<AppState>,
    Query(params): Query<TickListQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let fingerprint = format!("ticks?epoch={:?}", params.epoch);
    let page = params.page().resolve(&fingerprint)?;
    let mut rows = state
        .store
        .ticks(params.epoch, page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

pub async fn not_found() -> ApiError {
    ApiError::not_found("route not found")
}

async fn tick(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
) -> Result<impl IntoResponse, ApiError> {
    let row = state
        .store
        .tick(tick)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("tick {tick} is not indexed")))?;
    Ok((
        [(header::CACHE_CONTROL, IMMUTABLE)],
        Json(json!({ "data": row })),
    ))
}

async fn tick_evidence(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
) -> Result<impl IntoResponse, ApiError> {
    let row =
        state.store.evidence_for_tick(tick).await?.ok_or_else(|| {
            ApiError::not_found(format!("evidence for tick {tick} is not indexed"))
        })?;
    Ok((
        [(header::CACHE_CONTROL, IMMUTABLE)],
        Json(json!({
            "data": {
                "epoch": row.epoch,
                "tick": row.tick,
                "quorum_digest": hex::encode(row.quorum_digest),
                "tick_data_digest": hex::encode(row.tick_data_digest),
                "required_votes": row.required_votes,
                "aligned_votes": row.aligned_votes,
                "fetched_votes": row.fetched_votes,
                "unique_votes": row.unique_votes,
                "verified_votes": row.verified_votes,
                "verification": row.verification,
                "warning_count": row.warning_count,
                "warnings": row.warnings,
                "timestamp": row.timestamp,
            }
        })),
    ))
}

async fn tick_transactions(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    if state.store.tick(tick).await?.is_none() {
        return Err(ApiError::not_found(format!("tick {tick} is not indexed")));
    }
    let page = params.resolve(&format!("ticks/{tick}/transactions"))?;
    let mut rows = state
        .store
        .transactions_for_tick(tick, page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    let rows: Vec<Value> = rows.iter().map(transaction_value).collect();
    Ok((
        [(header::CACHE_CONTROL, IMMUTABLE)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn tick_events(
    State(state): State<AppState>,
    Path(tick): Path<u32>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    if state.store.tick(tick).await?.is_none() {
        return Err(ApiError::not_found(format!("tick {tick} is not indexed")));
    }
    let page = params.resolve(&format!("ticks/{tick}/events"))?;
    let mut rows = state
        .store
        .events_for_tick(tick, page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    let rows: Vec<Value> = rows.into_iter().map(event_value).collect();
    Ok((
        [(header::CACHE_CONTROL, IMMUTABLE)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn transaction(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let hash = hash.trim().to_ascii_lowercase();
    let _ =
        qubic::decode_tx_hash(&hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;
    let row = state
        .store
        .transaction(&hash)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("transaction {hash} is not indexed")))?;
    Ok((
        [(header::CACHE_CONTROL, IMMUTABLE)],
        Json(json!({ "data": transaction_value(&row) })),
    ))
}

async fn transactions_batch(
    State(state): State<AppState>,
    Json(body): Json<BatchTransactions>,
) -> Result<impl IntoResponse, ApiError> {
    if body.hashes.is_empty() {
        return Err(ApiError::bad_request("hashes must not be empty"));
    }
    // The limit applies to unique hashes, so a caller may submit duplicates of
    // a small working set without tripping the cap.
    let mut hashes = Vec::with_capacity(body.hashes.len());
    let mut seen = HashSet::new();
    for value in body.hashes {
        let hash = value.trim().to_ascii_lowercase();
        if seen.insert(hash.clone()) {
            hashes.push(hash);
            if hashes.len() > 100 {
                return Err(ApiError::bad_request(
                    "at most 100 unique hashes per request",
                ));
            }
        }
    }
    for hash in &hashes {
        qubic::decode_tx_hash(hash).map_err(|error| ApiError::invalid_hash(error.to_string()))?;
    }
    let rows = state.store.transactions_by_hashes(&hashes).await?;
    let found: HashMap<String, store::TransactionRow> = rows
        .into_iter()
        .map(|row| (row.hash.clone(), row))
        .collect();
    let data: Vec<Value> = hashes
        .iter()
        .filter_map(|hash| found.get(hash).map(transaction_value))
        .collect();
    let missing: Vec<&String> = hashes
        .iter()
        .filter(|hash| !found.contains_key(*hash))
        .collect();
    Ok((
        [(header::CACHE_CONTROL, IMMUTABLE)],
        Json(json!({
            "data": data,
            "meta": { "requested": hashes.len(), "found": data.len(), "missing": missing },
        })),
    ))
}

async fn transactions(
    State(state): State<AppState>,
    Query(params): Query<TransactionQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = match params.identity.as_deref() {
        Some(value) => {
            let value = value.trim().to_ascii_uppercase();
            qubic::PublicKey::from_identity(&value)
                .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
            Some(value)
        }
        None => None,
    };
    let filter = TransactionFilter {
        epoch: params.epoch,
        tick: params.tick,
        min_tick: None,
        identity: identity.clone(),
    };
    let fingerprint = format!(
        "transactions?epoch={:?}&tick={:?}&identity={:?}",
        params.epoch, params.tick, identity
    );
    let page = params.page().resolve(&fingerprint)?;
    let mut rows = fetch_transactions_page(&state, &filter, &page).await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    let rows: Vec<Value> = rows.iter().map(transaction_value).collect();
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn events(
    State(state): State<AppState>,
    Query(params): Query<EventQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let filter = EventFilter {
        epoch: params.epoch,
        tick: params.tick,
        min_tick: None,
        type_name: params.event_type.clone(),
    };
    let fingerprint = format!(
        "events?epoch={:?}&tick={:?}&type={:?}",
        params.epoch, params.tick, params.event_type
    );
    let page = params.page().resolve(&fingerprint)?;
    let mut rows = fetch_events_page(&state, &filter, &page).await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    let rows: Vec<Value> = rows.into_iter().map(event_value).collect();
    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

/// Returns the current head tick and the earliest indexed tick when a recent
/// scan is valid for the requested epoch.
async fn recent_window(
    state: &AppState,
    epoch: Option<u16>,
) -> Result<Option<(u32, u32)>, ApiError> {
    let (head, earliest) = tokio::join!(state.store.head(), state.store.earliest_tick());
    let Some(head) = head? else {
        return Ok(None);
    };
    if epoch.is_some_and(|epoch| epoch != head.epoch) {
        return Ok(None);
    }
    let floor = earliest?.unwrap_or(head.tick);
    Ok(Some((head.tick, floor)))
}

/// Fetches a recent transaction page without scanning the whole table. The
/// window doubles until the page is full, then falls back to an unbounded
/// historical page once it reaches the start of the indexed range.
async fn fetch_transactions_page(
    state: &AppState,
    filter: &TransactionFilter,
    page: &Page,
) -> Result<Vec<store::TransactionRow>, ApiError> {
    if filter.tick.is_none() {
        if let Some((head, floor)) = recent_window(state, filter.epoch).await? {
            let mut bounded = filter.clone();
            let mut span = RECENT_PAGE_START_TICKS;
            loop {
                let min_tick = head.saturating_sub(span).max(floor);
                bounded.min_tick = Some(min_tick);
                let rows = state
                    .store
                    .transactions_page(&bounded, page.fetch_limit(), page.offset)
                    .await?;
                if rows.len() >= page.fetch_limit() as usize || min_tick <= floor {
                    return Ok(rows);
                }
                if span >= RECENT_PAGE_MAX_TICKS {
                    bounded.min_tick = None;
                    return Ok(state
                        .store
                        .transactions_page(&bounded, page.fetch_limit(), page.offset)
                        .await?);
                }
                span = span.saturating_mul(2);
            }
        }
    }
    Ok(state
        .store
        .transactions_page(filter, page.fetch_limit(), page.offset)
        .await?)
}

/// Event equivalent of [`fetch_transactions_page`].
async fn fetch_events_page(
    state: &AppState,
    filter: &EventFilter,
    page: &Page,
) -> Result<Vec<store::EventRow>, ApiError> {
    if filter.tick.is_none() {
        if let Some((head, floor)) = recent_window(state, filter.epoch).await? {
            let mut bounded = filter.clone();
            let mut span = RECENT_PAGE_START_TICKS;
            loop {
                let min_tick = head.saturating_sub(span).max(floor);
                bounded.min_tick = Some(min_tick);
                let rows = state
                    .store
                    .events_page(&bounded, page.fetch_limit(), page.offset)
                    .await?;
                if rows.len() >= page.fetch_limit() as usize || min_tick <= floor {
                    return Ok(rows);
                }
                if span >= RECENT_PAGE_MAX_TICKS {
                    bounded.min_tick = None;
                    return Ok(state
                        .store
                        .events_page(&bounded, page.fetch_limit(), page.offset)
                        .await?);
                }
                span = span.saturating_mul(2);
            }
        }
    }
    Ok(state
        .store
        .events_page(filter, page.fetch_limit(), page.offset)
        .await?)
}

async fn identities(
    State(state): State<AppState>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    let page = params.resolve("identities")?;
    let mut rows = state
        .store
        .identities(page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn identity_transactions(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = identity.to_ascii_uppercase();
    let _ = qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let page = params.resolve(&format!("identities/{identity}/transactions"))?;
    let body = state
        .live_cache
        .get_or_insert(
            format!(
                "identity_transactions:{identity}:{}:{}",
                page.limit, page.offset
            ),
            Duration::from_secs(2),
            || async {
                let mut rows = state
                    .store
                    .transactions_for_identity(&identity, page.fetch_limit(), page.offset)
                    .await?;
                let has_more = rows.len() > page.limit as usize;
                rows.truncate(page.limit as usize);
                let rows: Vec<Value> = rows.iter().map(transaction_value).collect();
                Ok::<Value, ApiError>(json!({
                    "data": rows,
                    "meta": page.meta(rows.len(), has_more),
                }))
            },
        )
        .await?;
    Ok(([(header::CACHE_CONTROL, AGGREGATE)], Json(body)))
}

async fn identity_activity(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = identity.trim().to_ascii_uppercase();
    qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let page = params.resolve(&format!("identities/{identity}/activity"))?;
    let body = state
        .live_cache
        .get_or_insert(
            format!(
                "identity_activity:{identity}:{}:{}",
                page.limit, page.offset
            ),
            Duration::from_secs(2),
            || async {
                let mut rows = state
                    .store
                    .activity_for_identity(&identity, page.fetch_limit(), page.offset)
                    .await?;
                let has_more = rows.len() > page.limit as usize;
                rows.truncate(page.limit as usize);
                Ok::<Value, ApiError>(json!({
                    "data": rows,
                    "meta": page.meta(rows.len(), has_more),
                }))
            },
        )
        .await?;
    Ok(([(header::CACHE_CONTROL, RECENT)], Json(body)))
}

async fn identity_events(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(params): Query<PageParams>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = identity.trim().to_ascii_uppercase();
    qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let page = params.resolve(&format!("identities/{identity}/events"))?;
    let body = state
        .live_cache
        .get_or_insert(
            format!("identity_events:{identity}:{}:{}", page.limit, page.offset),
            Duration::from_secs(2),
            || async {
                let mut rows = state
                    .store
                    .events_for_identity(&identity, page.fetch_limit(), page.offset)
                    .await?;
                let has_more = rows.len() > page.limit as usize;
                rows.truncate(page.limit as usize);
                Ok::<Value, ApiError>(json!({
                    "data": rows,
                    "meta": page.meta(rows.len(), has_more),
                }))
            },
        )
        .await?;
    Ok(([(header::CACHE_CONTROL, RECENT)], Json(body)))
}

async fn identity(
    State(state): State<AppState>,
    Path(identity): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = identity.to_ascii_uppercase();
    let _ = qubic::PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let row = state
        .store
        .identity(&identity)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("identity {identity} is not indexed")))?;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": row,
            "meta": {
                "notice": "balance is the net QU flow observed in the indexed epoch range",
                "links": {
                    "wallet": format!("/v1/wallets/{identity}"),
                    "assets": format!("/v1/identities/{identity}/assets"),
                    "live": format!("/v1/live/wallets/{identity}"),
                    "transactions": format!("/v1/identities/{identity}/transactions")
                }
            }
        })),
    ))
}

async fn assets(
    State(state): State<AppState>,
    Query(params): Query<AssetQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let issuer = match params.issuer.as_deref() {
        Some(value) => {
            let value = value.trim().to_ascii_uppercase();
            qubic::PublicKey::from_identity(&value)
                .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
            Some(value)
        }
        None => None,
    };
    let fingerprint = format!("assets?issuer={issuer:?}");
    let page = params.page().resolve(&fingerprint)?;
    let mut rows = state
        .store
        .assets(issuer.as_deref(), page.fetch_limit(), page.offset)
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": rows,
            "meta": page.meta(rows.len(), has_more),
        })),
    ))
}

async fn asset(
    State(state): State<AppState>,
    Path((issuer, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let issuer = issuer.to_ascii_uppercase();
    let _ = qubic::PublicKey::from_identity(&issuer)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    let row = state
        .store
        .asset(&issuer, &name)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("asset {issuer}/{name} is not indexed")))?;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({ "data": row })),
    ))
}

async fn search(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ApiError> {
    let query = params
        .get("q")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("query parameter q is required"))?;

    let mut data = Vec::new();

    if query.chars().all(|c| c.is_ascii_digit()) {
        if let Ok(tick) = query.parse::<u32>() {
            if let Some(row) = state.store.tick(tick).await? {
                data.push(json!({ "type": "tick", "tick": tick, "value": row }));
            }
        }
        if let Ok(epoch) = query.parse::<u16>() {
            if let Some(row) = state.store.epoch(epoch).await? {
                data.push(json!({ "type": "epoch", "epoch": epoch, "value": row }));
            }
        }
    }

    let lower = query.to_ascii_lowercase();
    if qubic::decode_tx_hash(&lower).is_ok() {
        if let Some(row) = state.store.transaction(&lower).await? {
            data.push(json!({ "type": "transaction", "value": row }));
        }
    }

    let upper = query.to_ascii_uppercase();
    if qubic::PublicKey::from_identity(&upper).is_ok() {
        if let Some(row) = state.store.identity(&upper).await? {
            data.push(json!({ "type": "identity", "value": row }));
        }
    }

    if let Some((issuer, name)) = query.split_once('/') {
        let issuer = issuer.trim().to_ascii_uppercase();
        let name = name.trim().to_ascii_uppercase();
        let valid_name = !name.is_empty()
            && name.len() <= 7
            && name
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte.is_ascii_uppercase());
        if valid_name && qubic::PublicKey::from_identity(&issuer).is_ok() {
            if let Some(row) = state.store.asset(&issuer, &name).await? {
                data.push(json!({ "type": "asset", "value": row }));
            }
        }
    }

    Ok((
        [(header::CACHE_CONTROL, RECENT)],
        Json(json!({
            "data": data,
            "meta": { "query": query, "count": data.len() },
        })),
    ))
}

async fn analytics_activity(
    State(state): State<AppState>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let interval = params.interval.unwrap_or(3600).clamp(1, 86_400);
    let limit = params.limit.unwrap_or(24).clamp(1, 500);
    let rows = state.store.activity_analytics(interval, limit).await?;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({
            "data": rows,
            "meta": { "interval_seconds": interval, "limit": limit },
        })),
    ))
}

/// A live spectrum entity value read from a peer.
///
/// The indexed `balance` is the net QU flow observed in the indexed epoch
/// range, which is not the same thing as the balance stored in the spectrum.
/// Keeping both values distinct lets clients (and the response `meta`) say
/// which number they are looking at.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SpectrumBalance {
    public_key: String,
    balance: i64,
    incoming_amount: i64,
    outgoing_amount: i64,
    incoming_transfers: u32,
    outgoing_transfers: u32,
    latest_incoming_tick: u32,
    latest_outgoing_tick: u32,
    spectrum_index: i32,
    tick: u32,
}

impl SpectrumBalance {
    /// Returns `None` when the peer has no spectrum entry for the identity,
    /// which is reported as a negative spectrum index.
    fn from_response(response: RespondEntity) -> Option<Self> {
        if response.spectrum_index < 0 {
            return None;
        }
        let entity = response.entity;
        Some(Self {
            public_key: qubic::PublicKey(entity.public_key).to_hex(),
            balance: entity
                .incoming_amount
                .saturating_sub(entity.outgoing_amount),
            incoming_amount: entity.incoming_amount,
            outgoing_amount: entity.outgoing_amount,
            incoming_transfers: entity.number_of_incoming_transfers,
            outgoing_transfers: entity.number_of_outgoing_transfers,
            latest_incoming_tick: entity.latest_incoming_transfer_tick,
            latest_outgoing_tick: entity.latest_outgoing_transfer_tick,
            spectrum_index: response.spectrum_index,
            tick: response.tick,
        })
    }

    fn value(&self) -> Value {
        json!({
            "public_key": self.public_key,
            "balance": self.balance,
            "incoming_amount": self.incoming_amount,
            "outgoing_amount": self.outgoing_amount,
            "incoming_transfers": self.incoming_transfers,
            "outgoing_transfers": self.outgoing_transfers,
            "latest_incoming_tick": self.latest_incoming_tick,
            "latest_outgoing_tick": self.latest_outgoing_tick,
            "spectrum_index": self.spectrum_index,
            "tick": self.tick,
        })
    }
}

/// Looks up every identity with bounded concurrency. Results keep the input
/// order; a `None` entry means the peer lookup failed or has no entry, and the
/// caller must fall back to the indexed row.
async fn collect_spectrum_balances<F, Fut>(
    identities: Vec<String>,
    lookup: F,
) -> Vec<Option<SpectrumBalance>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Option<SpectrumBalance>>,
{
    if identities.is_empty() {
        return Vec::new();
    }
    let count = identities.len();
    let results: Vec<(usize, Option<SpectrumBalance>)> =
        futures_util::stream::iter(identities.into_iter().enumerate().map(|(index, identity)| {
            let lookup = &lookup;
            async move { (index, lookup(identity).await) }
        }))
        .buffer_unordered(TOP_IDENTITY_CONCURRENCY)
        .collect()
        .await;
    let mut balances = vec![None; count];
    for (index, balance) in results {
        balances[index] = balance;
    }
    balances
}

fn top_identity_value(row: &store::IdentityRow, spectrum: Option<&SpectrumBalance>) -> Value {
    let mut value = serde_json::to_value(row).expect("IdentityRow is serializable");
    value["indexed_balance"] = json!(row.balance);
    match spectrum {
        Some(spectrum) => {
            // Prefer the live value for the headline balance and keep the
            // indexed net flow available under `indexed_balance`.
            value["balance"] = json!(spectrum.balance);
            value["spectrum_balance"] = json!(spectrum.balance);
            value["balance_source"] = json!("spectrum");
            value["spectrum"] = spectrum.value();
        }
        None => {
            value["spectrum_balance"] = Value::Null;
            value["balance_source"] = json!("indexed");
            value["spectrum"] = Value::Null;
        }
    }
    value
}

fn top_identities_meta(
    limit: u32,
    peers_configured: bool,
    indexed_fallbacks: usize,
    live: usize,
) -> Value {
    let balance_source = if live == 0 {
        "indexed_net_flow"
    } else if indexed_fallbacks == 0 {
        "spectrum"
    } else {
        "mixed"
    };
    json!({
        "limit": limit,
        "ranking": "transfer_count_desc_then_indexed_balance_desc_then_identity_asc",
        "balance_source": balance_source,
        "live_spectrum_balances": live,
        "indexed_fallbacks": indexed_fallbacks,
        "peers_configured": peers_configured,
        "notes": "row.balance is the live peer spectrum balance when available and indexed net QU flow otherwise. row.indexed_balance always carries the indexed net QU flow, and row.spectrum.balance is the live spectrum entity. The ranking always uses the indexed transfer_count, so adding live balances does not reorder rows.",
    })
}

async fn analytics_top_identities(
    State(state): State<AppState>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = params.limit.unwrap_or(25).clamp(1, 200);
    let cache_key = format!("top_identities:{limit}");
    if let Some(value) = state.live_cache.get(&cache_key, Duration::from_secs(30)) {
        return Ok(([(header::CACHE_CONTROL, AGGREGATE)], Json(value)));
    }
    let rows = state.store.top_identities(limit).await?;
    let peers_configured = !state.peers.is_empty();
    let mut balances = vec![None; rows.len()];
    if peers_configured {
        let identities = rows.iter().map(|row| row.identity.clone()).collect();
        let pool = state.pool.clone();
        let lookup = move |identity: String| {
            let pool = pool.clone();
            async move {
                match tokio::time::timeout(
                    TOP_IDENTITY_PEER_TIMEOUT,
                    pool.entity_by_identity(&identity),
                )
                .await
                {
                    Ok(Ok(response)) => SpectrumBalance::from_response(response),
                    Ok(Err(_)) | Err(_) => None,
                }
            }
        };
        balances = tokio::time::timeout(
            TOP_IDENTITY_BUDGET,
            collect_spectrum_balances(identities, lookup),
        )
        .await
        .unwrap_or_else(|_| vec![None; rows.len()]);
    }
    let live = balances.iter().filter(|balance| balance.is_some()).count();
    let indexed_fallbacks = rows.len() - live;
    let data: Vec<Value> = rows
        .iter()
        .zip(balances.iter())
        .map(|(row, balance)| top_identity_value(row, balance.as_ref()))
        .collect();
    let body = json!({
        "data": data,
        "meta": top_identities_meta(limit, peers_configured, indexed_fallbacks, live),
    });
    state.live_cache.put(cache_key, body.clone());
    Ok(([(header::CACHE_CONTROL, AGGREGATE)], Json(body)))
}

async fn analytics_event_types(
    State(state): State<AppState>,
    Query(params): Query<AnalyticsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = params.limit.unwrap_or(50).clamp(1, 200);
    let rows = state.store.event_type_counts(limit).await?;
    Ok((
        [(header::CACHE_CONTROL, AGGREGATE)],
        Json(json!({ "data": rows, "meta": { "limit": limit } })),
    ))
}

pub async fn docs() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html><head><meta charset="utf-8"><title>Glyph API Status</title><link rel="icon" href="/favicon.png">
<style>body{font-family:ui-sans-serif,system-ui;max-width:52rem;margin:3rem auto;padding:0 1rem;line-height:1.5}code{background:#f4f4f5;padding:.15rem .35rem;border-radius:.25rem}</style>
</head><body>
<h1>Glyph API</h1>
<p>Read the Qubic chain directly from peers. The OpenAPI 3.1 document is at <a href="/openapi.json"><code>/openapi.json</code></a> and the interactive reference is at <a href="/docs"><code>/docs</code></a>.</p>
<ul>
<li><code>GET /v1/status</code></li>
<li><code>GET /v1/network</code></li>
<li><code>GET /v1/network/index</code></li>
<li><code>GET /v1/network/system</code></li>
<li><code>GET /v1/network/peers</code></li>
<li><code>GET /v1/network/revenue?limit=&amp;sort=&amp;identity=</code></li>
<li><code>GET /v1/network/oracles</code></li>
<li><code>GET /v1/network/oracles/queries?status=&amp;tick=&amp;limit=</code></li>
<li><code>GET /v1/network/oracles/subscriptions?contract=&amp;limit=</code></li>
<li><code>GET /v1/epochs?limit=&amp;cursor=</code></li>
<li><code>GET /v1/epochs/{epoch}</code></li>
<li><code>GET /v1/epochs/{epoch}/computors</code></li>
<li><code>GET /v1/ticks?epoch=&amp;limit=&amp;cursor=</code></li>
<li><code>GET /v1/ticks/{tick}</code></li>
<li><code>GET /v1/ticks/{tick}/evidence</code></li>
<li><code>GET /v1/ticks/{tick}/transactions?limit=&amp;cursor=</code></li>
<li><code>GET /v1/ticks/{tick}/capacity</code></li>
<li><code>GET /v1/ticks/{tick}/events?limit=&amp;cursor=</code></li>
<li><code>GET /v1/transactions?epoch=&amp;tick=&amp;identity=&amp;limit=&amp;cursor=</code></li>
<li><code>GET /v1/transactions/{hash}</code></li>
<li><code>GET /v1/transactions/{hash}/lifecycle</code></li>
<li><code>GET /v1/transactions/inclusion-risk?source=&amp;target_tick=&amp;amount=</code></li>
<li><code>GET /v1/events?epoch=&amp;tick=&amp;type=&amp;limit=&amp;cursor=</code></li>
<li><code>GET /v1/identities?limit=&amp;cursor=</code></li>
<li><code>GET /v1/identities/{identity}</code></li>
<li><code>GET /v1/identities/{identity}/transactions?limit=&amp;cursor=</code></li>
<li><code>GET /v1/identities/{identity}/assets?kind=</code></li>
<li><code>GET /v1/assets?issuer=&amp;limit=&amp;cursor=</code></li>
<li><code>GET /v1/assets/{issuer}/{name}</code></li>
<li><code>GET /v1/assets/{issuer}/{name}/holders?limit=</code></li>
<li><code>GET /v1/wallets/{identity}</code></li>
<li><code>GET /v1/wallets/{identity}/assets?kind=</code></li>
<li><code>GET /v1/live/wallets/{identity}</code></li>
<li><code>GET /v1/prices?from_hour=&amp;limit=</code></li>
<li><code>GET /v1/prices/history?hours=</code></li>
<li><code>GET /v1/prices/latest</code></li>
<li><code>GET /v1/prices/snapshot</code></li>
<li><code>POST /v1/transactions/batch</code></li>
<li><code>POST /v1/transactions</code> (requires <code>ALLOW_BROADCAST=true</code>)</li>
<li><code>GET /v1/analytics/activity?interval=&amp;limit=</code></li>
<li><code>GET /v1/analytics/top-identities?limit=</code></li>
<li><code>GET /v1/analytics/event-types?limit=</code></li>
<li><code>GET /v1/stream?types=</code></li>
<li><code>GET /v1/stream/{ticks|transactions|events|prices|market}</code></li>
<li><code>GET /v1/ws</code></li>
<li><code>GET /v1/live/identities/{identity}</code></li>
<li><code>GET /v1/live/transactions/{hash}</code></li>
<li><code>GET /v1/live/transactions/{hash}/status</code></li>
<li><code>POST /v1/transactions/validate</code></li>
<li><code>GET /v1/live/ticks/{tick}/transactions</code></li>
<li><code>GET /v1/ticks/{tick}/raw?section=</code></li>
<li><code>POST /v1/live/contracts/{contract_index}/query</code></li>
<li><code>GET /v1/search?q=</code></li>
</ul>
</body></html>"#,
    )
}

/// Scalar API reference backed by the embedded bundle, so the docs work
/// without a CDN and without a network.
pub async fn scalar_docs() -> Html<String> {
    let config = json!({
        "url": "/openapi.json",
        "layout": "modern",
        "theme": "purple",
        "darkMode": true,
        "agent": { "disabled": true },
        "favicon": "/favicon.png",
        "metaData": {
            "title": "Glyph API Reference",
            "description": "Read the Qubic chain directly from peers. Ticks, transactions, events, accounts, assets, mining, and markets.",
            "ogTitle": "Glyph API Reference",
            "ogDescription": "Read the Qubic chain directly from peers. Ticks, transactions, events, accounts, assets, mining, and markets.",
            "twitterCard": "summary",
        },
    });
    let html = scalar_api_reference::scalar_html(&config, Some("/scalar.js"))
        .replace(
            "<title>Scalar API Reference</title>",
            "<title>Glyph API Reference</title>",
        )
        .replace("</head>", &format!("{FONT_PRELOAD}</head>"))
        .replace("</body>", &format!("{DOCS_BRAND}</body>"));
    Html(html)
}

pub async fn scalar_js() -> Response {
    match scalar_api_reference::get_asset_with_mime("scalar.js") {
        Some((mime, bytes)) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, mime),
                (
                    header::CACHE_CONTROL,
                    "public, max-age=31536000, immutable".to_owned(),
                ),
            ],
            bytes,
        )
            .into_response(),
        None => ApiError::internal("Scalar JavaScript asset is missing").into_response(),
    }
}

pub async fn logo_dark() -> Response {
    png(LOGO_DARK_PNG)
}

pub async fn logo_light() -> Response {
    png(LOGO_LIGHT_PNG)
}

pub async fn space_grotesk() -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "font/woff2"),
            (header::CACHE_CONTROL, IMMUTABLE),
        ],
        SPACE_GROTESK_WOFF2,
    )
        .into_response()
}

pub async fn space_grotesk_license() -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::CACHE_CONTROL, IMMUTABLE),
        ],
        SPACE_GROTESK_LICENSE,
    )
        .into_response()
}

pub async fn favicon() -> Response {
    png(FAVICON_PNG)
}

fn png(bytes: &'static [u8]) -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "image/png"),
            (header::CACHE_CONTROL, IMMUTABLE),
        ],
        bytes,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn indexed_row(identity: &str, balance: i64, transfers: u64) -> store::IdentityRow {
        store::IdentityRow {
            identity: identity.into(),
            public_key: "0x01".into(),
            balance,
            incoming_amount: balance.saturating_add(10),
            outgoing_amount: 10,
            transfer_count: transfers,
            first_seen_tick: 1,
            last_seen_tick: 2,
        }
    }

    fn spectrum(_identity: &str) -> SpectrumBalance {
        SpectrumBalance {
            public_key: "0x01".into(),
            balance: 12_345,
            incoming_amount: 12_500,
            outgoing_amount: 155,
            incoming_transfers: 4,
            outgoing_transfers: 2,
            latest_incoming_tick: 10,
            latest_outgoing_tick: 9,
            spectrum_index: 7,
            tick: 11,
        }
    }

    #[test]
    fn top_identity_value_prefers_live_balance_and_keeps_indexed_flow() {
        let row = indexed_row("IDENTITY", 175, 39_475);
        let live = spectrum("IDENTITY");
        let value = top_identity_value(&row, Some(&live));

        // Identity and ranking fields stay intact.
        assert_eq!(value["identity"], "IDENTITY");
        assert_eq!(value["transfer_count"], 39_475);
        // The live number is the headline balance; the indexed number is explicit.
        assert_eq!(value["balance"], 12_345);
        assert_eq!(value["indexed_balance"], 175);
        assert_eq!(value["spectrum_balance"], 12_345);
        assert_eq!(value["balance_source"], "spectrum");
        assert_eq!(value["spectrum"]["balance"], 12_345);
        assert_eq!(value["spectrum"]["spectrum_index"], 7);
    }

    #[test]
    fn top_identity_value_falls_back_to_indexed_fields() {
        let row = indexed_row("IDENTITY", -13, 30_935);
        let value = top_identity_value(&row, None);

        assert_eq!(value["balance"], -13);
        assert_eq!(value["indexed_balance"], -13);
        assert!(value["spectrum_balance"].is_null());
        assert!(value["spectrum"].is_null());
        assert_eq!(value["balance_source"], "indexed");
    }

    #[tokio::test]
    async fn partial_peer_failures_only_affect_their_own_rows() {
        let identities = vec!["A".to_owned(), "B".to_owned(), "C".to_owned()];
        let balances = collect_spectrum_balances(identities, |identity| async move {
            (identity != "B").then(|| spectrum(&identity))
        })
        .await;

        assert_eq!(balances.len(), 3);
        assert_eq!(balances[0].as_ref().map(|row| row.balance), Some(12_345));
        assert!(balances[1].is_none());
        assert_eq!(balances[2].as_ref().map(|row| row.balance), Some(12_345));
    }

    #[test]
    fn meta_distinguishes_live_spectrum_from_indexed_net_flow() {
        let indexed = top_identities_meta(25, false, 2, 0);
        assert_eq!(indexed["balance_source"], "indexed_net_flow");
        assert_eq!(
            indexed["ranking"],
            "transfer_count_desc_then_indexed_balance_desc_then_identity_asc"
        );
        assert_eq!(indexed["indexed_fallbacks"], 2);

        let mixed = top_identities_meta(25, true, 1, 1);
        assert_eq!(mixed["balance_source"], "mixed");
        assert_eq!(mixed["live_spectrum_balances"], 1);

        let live = top_identities_meta(25, true, 0, 2);
        assert_eq!(live["balance_source"], "spectrum");
    }

    #[test]
    fn spectrum_response_without_entity_is_treated_as_a_fallback() {
        let response = RespondEntity {
            entity: qubic::messages::EntityRecord {
                public_key: [0; 32],
                incoming_amount: 0,
                outgoing_amount: 0,
                number_of_incoming_transfers: 0,
                number_of_outgoing_transfers: 0,
                latest_incoming_transfer_tick: 0,
                latest_outgoing_transfer_tick: 0,
            },
            tick: 10,
            spectrum_index: -1,
            siblings: Vec::new(),
        };
        assert!(SpectrumBalance::from_response(response).is_none());
    }
}
