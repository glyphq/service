//! Hourly QUBIC/USDT oracle price routes and the background peer poller.
//!
//! The price is read directly from a Qubic peer, without any external HTTP or
//! hosted RPC. The primary source is the latest verified reply of an active
//! `OI::Price` oracle subscription (interface index 0) for the QUBIC/USDT
//! currency pair. The poller discovers active subscriptions through
//! `RequestOracleData` reqType 9, inspects candidate subscriptions, and reads
//! the last revealed query with reqType 5. The QPayhub contract read function
//! `GetQuUsdPrice` (contract index 29, input type 4) is a secondary source.
//!
//! The index stores one row per logical price hour. A later version for the
//! same hour replaces an earlier one, so a poller restart or a replayed packet
//! cannot move a price backwards. Live updates are published to the stream hub
//! only for hours whose value changed. The oracle reply itself is verified by
//! the Qubic network; this poller only selects it and scales its ratio.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, SecondsFormat, Utc};
use node::{NodePool, PeerConfig};
use qubic::crypto::k12::k12_32;
use qubic::messages::{
    PRICE_ORACLE_INTERFACE_INDEX, PRICE_POINT_FLAG_FILLED, PRICE_POINT_FLAG_STALE,
    PriceOracleQuery, PriceOracleReply, currency_id,
};
use serde::Deserialize;
use serde_json::{Value, json};
use store::{PricePointRow, PriceSnapshot, Store};
use tokio_util::sync::CancellationToken;

use crate::stream::StreamHub;
use crate::{ApiError, AppState};

/// Unix seconds of Qubic price hour 0: 2022-04-13T12:00:00Z.
pub const PRICE_EPOCH_UNIX_SECONDS: i64 = 1_649_851_200;
/// `price_e8` is the whole-USDT price scaled by this factor.
pub const PRICE_SCALE: i64 = 100_000_000;
/// Default background poll interval.
pub const PRICE_POLL_MS: u64 = 60_000;
/// Default number of hourly points returned by `/v1/prices`.
pub const PRICE_WINDOW_DEFAULT: u32 = 168;
/// Upper bound for any price window.
pub const PRICE_WINDOW_MAX: u32 = 1000;
/// Default number of hours returned by `/v1/prices/history`.
pub const PRICE_HISTORY_DEFAULT: u32 = 24;
/// Currency pair served by the price routes.
pub const PRICE_PAIR: &str = "QUBIC/USDT";
/// `OI::Price` is oracle interface index 0.
pub const PRICE_SOURCE_ORACLE: u32 = PRICE_ORACLE_INTERFACE_INDEX;
/// QPayhub contract index, used only as a secondary price source.
pub const QPAYHUB_CONTRACT_INDEX: u32 = 29;
/// QPayhub `GetQuUsdPrice` read function input type.
pub const QPAYHUB_GET_QU_USD_PRICE_INPUT_TYPE: u16 = 4;
/// Cap on subscriptions inspected in one discovery pass.
pub const MAX_ORACLE_SUBSCRIPTIONS_SCAN: usize = 256;

/// `RespondOracleDataQueryMetadata.status` value for a revealed query.
const ORACLE_QUERY_STATUS_SUCCESS: u8 = 3;

const NO_STORE: &str = "no-store";
const SHORT_CACHE: &str = "public, max-age=15, stale-while-revalidate=30";

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/prices", get(prices))
        .route("/v1/prices/latest", get(latest))
        .route("/v1/prices/snapshot", get(snapshot))
        .route("/v1/prices/history", get(history))
}

#[derive(Debug, Default, Deserialize)]
struct PriceQuery {
    #[serde(default)]
    from_hour: Option<u32>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
struct HistoryQuery {
    #[serde(default)]
    hours: Option<u32>,
}

async fn prices(
    State(state): State<AppState>,
    Query(params): Query<PriceQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let from_hour = params.from_hour.unwrap_or(0);
    let limit = params
        .limit
        .unwrap_or(PRICE_WINDOW_DEFAULT)
        .clamp(1, PRICE_WINDOW_MAX);
    let latest = state.store.latest_price().await?;
    let rows = state.store.price_points(from_hour, limit).await?;
    let data: Vec<Value> = rows.iter().map(point_json).collect();
    Ok((
        [(header::CACHE_CONTROL, SHORT_CACHE)],
        Json(json!({
            "data": data,
            "meta": meta(&latest, from_hour, limit, rows.len()),
        })),
    ))
}

async fn latest(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let row = state
        .store
        .latest_price()
        .await?
        .ok_or_else(|| ApiError::not_found("no QUBIC/USDT price data has been indexed"))?;
    Ok((
        [(header::CACHE_CONTROL, NO_STORE)],
        Json(json!({
            "data": point_json(&row),
            "meta": {
                "price_scale": PRICE_SCALE,
                "pair": PRICE_PAIR,
                "source": row.source,
                "source_name": source_name(row.source),
                "stale": point_stale(row.flags),
                "available": true,
            },
        })),
    ))
}

async fn snapshot(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let snapshot = state.store.price_snapshot().await?;
    let available = snapshot.is_some();
    let stale = snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.latest.as_ref())
        .is_some_and(|row| point_stale(row.flags));
    let data = match snapshot.as_ref() {
        Some(snapshot) => snapshot_json(snapshot, snapshot.latest.as_ref()),
        None => Value::Null,
    };
    let mut meta = json!({
        "price_scale": PRICE_SCALE,
        "pair": PRICE_PAIR,
        "source": snapshot.as_ref().map_or(0, |snapshot| snapshot.source),
        "source_name": snapshot
            .as_ref()
            .map_or("none", |snapshot| source_name(snapshot.source)),
        "stale": stale,
        "available": available,
    });
    if !available {
        meta["warning"] = json!(no_price_warning());
    } else if stale {
        meta["warning"] =
            json!("the latest QUBIC/USDT price is marked stale by its on-chain source");
    }
    Ok((
        [(header::CACHE_CONTROL, NO_STORE)],
        Json(json!({ "data": data, "meta": meta })),
    ))
}

async fn history(
    State(state): State<AppState>,
    Query(params): Query<HistoryQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let hours = params
        .hours
        .unwrap_or(PRICE_HISTORY_DEFAULT)
        .clamp(1, PRICE_WINDOW_MAX);
    let current = price_hour_at(Utc::now());
    let from_hour = current.saturating_sub(hours.saturating_sub(1));
    let latest = state.store.latest_price().await?;
    let mut rows = state.store.price_points(from_hour, hours).await?;
    rows.retain(|row| row.hour <= current);
    let data: Vec<Value> = rows.iter().map(point_json).collect();
    Ok((
        [(header::CACHE_CONTROL, SHORT_CACHE)],
        Json(json!({
            "data": data,
            "meta": {
                "hours": hours,
                "from_hour": from_hour,
                "latest_hour": latest.as_ref().map(|row| row.hour),
                "count": rows.len(),
                "price_scale": PRICE_SCALE,
                "pair": PRICE_PAIR,
                "source": latest.as_ref().map_or(0, |row| row.source),
                "source_name": latest
                    .as_ref()
                    .map_or("none", |row| source_name(row.source)),
                "stale": latest.as_ref().is_some_and(|row| point_stale(row.flags)),
                "available": latest.is_some(),
            },
        })),
    ))
}

fn meta(latest: &Option<PricePointRow>, from_hour: u32, limit: u32, count: usize) -> Value {
    json!({
        "from_hour": from_hour,
        "limit": limit,
        "count": count,
        "pair": PRICE_PAIR,
        "source": latest.as_ref().map_or(0, |row| row.source),
        "source_name": latest.as_ref().map_or("none", |row| source_name(row.source)),
        "stale": latest.as_ref().is_some_and(|row| point_stale(row.flags)),
        "available": latest.is_some(),
        "synced": latest.as_ref().is_some_and(|row| row.synced != 0),
        "bound": latest.as_ref().is_some_and(|row| row.bound != 0),
        "latest_hour": latest.as_ref().map(|row| row.hour),
        "price_scale": PRICE_SCALE,
    })
}

fn snapshot_json(snapshot: &PriceSnapshot, latest: Option<&PricePointRow>) -> Value {
    json!({
        "latest": latest.map(point_json),
        "points": snapshot.points,
        "hour": latest.map(|row| row.hour),
        "timestamp": latest.map(|row| timestamp_string(row.hour)),
        "price_e8": latest.map(|row| row.price_e8),
        "price_usdt": latest.map(|row| price_usdt(row.price_e8)),
        "append_tick": snapshot.append_tick,
        "source_tick": snapshot.source_tick,
        "epoch": snapshot.epoch,
        "source": snapshot.source,
        "source_name": source_name(snapshot.source),
        "stale": latest.is_some_and(|row| point_stale(row.flags)),
        "synced": snapshot.synced,
        "bound": snapshot.bound,
        "series_digest": hex::encode(snapshot.series_digest),
        "observed_at": observed_string(snapshot.observed_at),
    })
}

/// One point in the public response shape.
pub fn point_json(row: &PricePointRow) -> Value {
    json!({
        "hour": row.hour,
        "timestamp": timestamp_string(row.hour),
        "price_e8": row.price_e8,
        "price_usdt": price_usdt(row.price_e8),
        "append_tick": row.append_tick,
        "source_tick": row.source_tick,
        "epoch": row.epoch,
        "flags": row.flags,
        "filled": point_filled(row.flags),
        "stale": point_stale(row.flags),
        "source": row.source,
        "source_name": source_name(row.source),
        "synced": row.synced != 0,
        "bound": row.bound != 0,
        "series_digest": hex::encode(row.series_digest),
        "observed_at": observed_string(row.observed_at),
    })
}

fn timestamp_string(hour: u32) -> String {
    price_timestamp(hour).to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn observed_string(observed_at: DateTime<Utc>) -> String {
    observed_at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Human-readable name for a stored `source` value.
pub fn source_name(source: u32) -> &'static str {
    match source {
        PRICE_SOURCE_ORACLE => "price_oracle",
        QPAYHUB_CONTRACT_INDEX => "qpayhub",
        _ => "unknown",
    }
}

fn no_price_warning() -> &'static str {
    "no on-chain QUBIC/USDT price is available yet; the poller found no active Price oracle subscription with a revealed QUBIC/USDT reply and the QPayhub GetQuUsdPrice fallback returned no usable value"
}

/// Core price hour for a UTC timestamp. Timestamps before the epoch map to 0.
pub fn price_hour_at(now: DateTime<Utc>) -> u32 {
    let seconds = now.timestamp().saturating_sub(PRICE_EPOCH_UNIX_SECONDS);
    if seconds <= 0 {
        0
    } else {
        u32::try_from(seconds / 3600).unwrap_or(u32::MAX)
    }
}

/// UTC timestamp of a core price hour. Hour 0 is 2022-04-13T12:00:00Z.
pub fn price_timestamp(hour: u32) -> DateTime<Utc> {
    let seconds = PRICE_EPOCH_UNIX_SECONDS + i64::from(hour) * 3600;
    DateTime::<Utc>::from_timestamp(seconds, 0).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

/// Converts a 1e8-scaled price to USDT, rounded half up at eight decimals.
pub fn price_usdt(price_e8: i64) -> f64 {
    round_half_up(price_e8 as f64 / PRICE_SCALE as f64, 8)
}

/// Rounds half away from zero at `decimals` places.
pub fn round_half_up(value: f64, decimals: u32) -> f64 {
    let factor = 10f64.powi(decimals as i32);
    let scaled = value * factor;
    let rounded = if scaled >= 0.0 {
        (scaled + 0.5).floor()
    } else {
        (scaled - 0.5).ceil()
    };
    rounded / factor
}

/// True when `flags` marks an hour filled from the previous hour.
pub fn point_filled(flags: u32) -> bool {
    flags & PRICE_POINT_FLAG_FILLED != 0
}

/// True when `flags` marks a point whose source has declared it stale.
pub fn point_stale(flags: u32) -> bool {
    flags & PRICE_POINT_FLAG_STALE != 0
}

/// Orientation of the discovered oracle currency pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PairOrientation {
    /// `currency1 = QUBIC`, `currency2 = USDT`.
    Direct,
    /// `currency1 = USDT`, `currency2 = QUBIC`.
    Reversed,
}

impl PairOrientation {
    fn price_e8(self, numerator: i64, denominator: i64) -> Option<i64> {
        match self {
            Self::Direct => ratio_price_e8(numerator, denominator),
            Self::Reversed => ratio_price_e8(denominator, numerator),
        }
    }
}

fn pair_orientation(query: &PriceOracleQuery) -> Option<PairOrientation> {
    let qubic = currency_id("QUBIC");
    let usdt = currency_id("USDT");
    if query.is_pair(&qubic, &usdt) {
        Some(PairOrientation::Direct)
    } else if query.is_pair(&usdt, &qubic) {
        Some(PairOrientation::Reversed)
    } else {
        None
    }
}

/// Scales `numerator/denominator` to 1e-8 USDT units with i128 arithmetic.
///
/// Non-positive ratios, zero results, and results above `i64::MAX` are
/// rejected. The division matches the core's floor behaviour for positive
/// values.
pub fn ratio_price_e8(numerator: i64, denominator: i64) -> Option<i64> {
    if numerator <= 0 || denominator <= 0 {
        return None;
    }
    let scaled = i128::from(numerator).checked_mul(i128::from(PRICE_SCALE))?;
    let price = scaled / i128::from(denominator);
    if price <= 0 || price > i128::from(i64::MAX) {
        None
    } else {
        Some(price as i64)
    }
}

/// One verified on-chain QUBIC/USDT price reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnchainPrice {
    pub price_e8: i64,
    /// Oracle interface index or contract index that produced the value.
    pub source: u32,
    pub source_tick: u32,
    pub append_tick: u32,
    pub epoch: u16,
    pub stale: bool,
    pub digest: [u8; 32],
}

impl OnchainPrice {
    /// Maps this reading onto the hourly store row shape.
    pub fn row(&self, hour: u32, observed_at: DateTime<Utc>, version: u64) -> PricePointRow {
        PricePointRow {
            hour,
            append_tick: self.append_tick,
            source_tick: self.source_tick,
            epoch: self.epoch,
            flags: if self.stale {
                PRICE_POINT_FLAG_STALE
            } else {
                0
            },
            price_e8: self.price_e8,
            source: self.source,
            synced: u8::from(!self.stale),
            bound: 0,
            series_digest: self.digest,
            observed_at,
            version,
        }
    }
}

/// Reads the latest verified QUBIC/USDT price from the best reachable peer.
///
/// The Price oracle subscription reader is tried first. If it finds no active
/// QUBIC/USDT subscription (or cannot complete the scan), the QPayhub
/// `GetQuUsdPrice` read function is tried. `Ok(None)` means both sources had no
/// usable value; it is never a silent fallback to BTC.
pub async fn read_price(pool: &NodePool) -> node::Result<Option<OnchainPrice>> {
    match oracle_price(pool).await {
        Ok(Some(price)) => return Ok(Some(price)),
        Ok(None) => tracing::debug!("no active QUBIC/USDT oracle subscription discovered"),
        Err(error) => {
            tracing::warn!(%error, "oracle subscription scan failed; trying QPayhub fallback")
        }
    }
    qpayhub_price(pool).await
}

/// Discovers active Price oracle subscriptions and returns the latest revealed
/// QUBIC/USDT reply.
///
/// The scan is bounded by [`MAX_ORACLE_SUBSCRIPTIONS_SCAN`]. Failures for one
/// subscription are remembered but do not abort the scan, so one undecodable
/// subscription cannot hide a later usable one.
pub async fn oracle_price(pool: &NodePool) -> node::Result<Option<OnchainPrice>> {
    let mut peer = pool.connect_best().await?;
    let ids = peer.oracle_subscription_ids().await?;
    tracing::debug!(
        subscriptions = ids.len(),
        "discovered active oracle subscriptions"
    );
    let mut last_error = None;
    for subscription_id in ids.into_iter().take(MAX_ORACLE_SUBSCRIPTIONS_SCAN) {
        let subscription = match peer.oracle_subscription(subscription_id).await {
            Ok(subscription) => subscription,
            Err(error) => {
                last_error = Some(error);
                continue;
            }
        };
        if subscription.subscription.interface_index != PRICE_ORACLE_INTERFACE_INDEX {
            continue;
        }
        let initial = match PriceOracleQuery::decode(&subscription.query) {
            Ok(query) => query,
            Err(error) => {
                tracing::debug!(%error, subscription_id, "skipping undecodable oracle query");
                continue;
            }
        };
        if pair_orientation(&initial).is_none() {
            continue;
        }
        if subscription.subscription.last_revealed_query_id < 0 {
            continue;
        }
        let response = match peer
            .oracle_query(subscription.subscription.last_revealed_query_id)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                last_error = Some(error);
                continue;
            }
        };
        if response.metadata.interface_index != PRICE_ORACLE_INTERFACE_INDEX
            || response.metadata.status != ORACLE_QUERY_STATUS_SUCCESS
        {
            continue;
        }
        let Some(query_bytes) = response.query.as_deref() else {
            continue;
        };
        let query = PriceOracleQuery::decode(query_bytes)?;
        let orientation = match pair_orientation(&query) {
            Some(orientation) => orientation,
            None => continue,
        };
        let Some(reply_bytes) = response.reply.as_deref() else {
            continue;
        };
        let reply = PriceOracleReply::decode(reply_bytes)?;
        let Some(price_e8) = orientation.price_e8(reply.numerator, reply.denominator) else {
            continue;
        };
        return Ok(Some(OnchainPrice {
            price_e8,
            source: PRICE_SOURCE_ORACLE,
            source_tick: response.metadata.query_tick,
            append_tick: response.metadata.reveal_tick,
            epoch: 0,
            stale: false,
            digest: k12_32(reply_bytes),
        }));
    }
    match last_error {
        Some(error) => Err(error),
        None => Ok(None),
    }
}

/// Reads the QPayhub fallback price (contract index 29, input type 4).
pub async fn qpayhub_price(pool: &NodePool) -> node::Result<Option<OnchainPrice>> {
    let data = pool
        .contract_function(
            QPAYHUB_CONTRACT_INDEX,
            QPAYHUB_GET_QU_USD_PRICE_INPUT_TYPE,
            &[],
        )
        .await?;
    Ok(parse_qpayhub_price(&data))
}

/// Parses the 24-byte `GetQuUsdPrice` output.
///
/// Layout: `i64 numerator`, `i64 denominator`, `u32 updated_tick`,
/// `u8 stale`, padding. A zero denominator means QPayhub has never received a
/// reply and is reported as unavailable.
pub fn parse_qpayhub_price(data: &[u8]) -> Option<OnchainPrice> {
    if data.len() < 21 {
        return None;
    }
    let numerator = i64::from_le_bytes(data[0..8].try_into().expect("8 bytes"));
    let denominator = i64::from_le_bytes(data[8..16].try_into().expect("8 bytes"));
    let updated_tick = u32::from_le_bytes(data[16..20].try_into().expect("4 bytes"));
    let stale = data[20] != 0;
    Some(OnchainPrice {
        price_e8: ratio_price_e8(numerator, denominator)?,
        source: QPAYHUB_CONTRACT_INDEX,
        source_tick: updated_tick,
        append_tick: updated_tick,
        epoch: 0,
        stale,
        digest: k12_32(data),
    })
}

/// Returns a strictly increasing version. Unix milliseconds are used unless
/// the clock has not advanced since the previous poll.
pub fn monotonic_version(last: u64, now_ms: u64) -> u64 {
    now_ms.max(last.saturating_add(1))
}

/// Unix milliseconds saturating at zero for timestamps before 1970.
pub fn unix_millis(now: DateTime<Utc>) -> u64 {
    u64::try_from(now.timestamp_millis()).unwrap_or(0)
}

/// Selects rows whose `(hour, price_e8)` pair has not been published yet and
/// records them in `last_published`.
pub fn changed_rows<'a>(
    rows: &'a [PricePointRow],
    last_published: &mut HashSet<(u32, i64)>,
) -> Vec<&'a PricePointRow> {
    let mut changed = Vec::new();
    for row in rows {
        if last_published.insert((row.hour, row.price_e8)) {
            changed.push(row);
        }
    }
    changed
}

/// Writes a batch and publishes changed hours. A write failure returns the
/// error to the caller; the poller logs it and tries again on the next tick.
pub async fn persist_series(
    store: &dyn Store,
    hub: Option<&StreamHub>,
    rows: &[PricePointRow],
    last_published: &mut HashSet<(u32, i64)>,
) -> store::Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }
    store.write_price_points(rows).await?;
    let changed = changed_rows(rows, last_published);
    if let Some(hub) = hub {
        for row in &changed {
            hub.publish_event("price", format!("price:{}", row.hour), point_json(row));
        }
    }
    Ok(changed.len())
}

/// Polls a direct peer for the latest verified QUBIC/USDT oracle price until
/// `cancel` fires.
///
/// A peer or store failure only logs a warning. The loop always sleeps between
/// attempts, so an unreachable peer can never hot-spin.
pub async fn run_poller(
    store: Arc<dyn Store>,
    peers: Vec<PeerConfig>,
    hub: Option<Arc<StreamHub>>,
    interval: Duration,
    cancel: CancellationToken,
) {
    if peers.is_empty() {
        tracing::warn!("price poller has no QUBIC_PEERS configured; not polling");
        return;
    }

    let interval = if interval.is_zero() {
        Duration::from_millis(PRICE_POLL_MS)
    } else {
        interval
    };
    let pool = NodePool::new(peers);
    let mut last_published = HashSet::new();
    let mut last_version = 0u64;
    match store.latest_price().await {
        Ok(Some(latest)) => {
            last_published.insert((latest.hour, latest.price_e8));
            last_version = latest.version;
        }
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(%error, "price poller could not seed from the latest stored price");
        }
    }

    loop {
        if cancel.is_cancelled() {
            break;
        }
        let epoch = match pool.head().await {
            Ok(head) => head.epoch,
            Err(error) => {
                tracing::debug!(%error, "price poller could not read the peer head for the epoch");
                0
            }
        };
        match read_price(&pool).await {
            Ok(Some(mut price)) => {
                price.epoch = epoch;
                let observed_at = Utc::now();
                let hour = price_hour_at(observed_at);
                let version = monotonic_version(last_version, unix_millis(observed_at));
                last_version = version;
                let row = price.row(hour, observed_at, version);
                match persist_series(store.as_ref(), hub.as_deref(), &[row], &mut last_published)
                    .await
                {
                    Ok(published) => {
                        tracing::debug!(
                            hour,
                            price_e8 = price.price_e8,
                            source = price.source,
                            stale = price.stale,
                            published,
                            "price poll complete"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(%error, hour, "price write failed; retrying next interval");
                    }
                }
            }
            Ok(None) => {
                tracing::warn!(
                    "no on-chain QUBIC/USDT price is available; will retry next interval"
                );
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "QUBIC/USDT price fetch failed; trying the peer pool again next interval"
                );
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::HeaderMap;
    use axum::http::{Request, StatusCode};
    use chrono::TimeZone;
    use qubic::messages::PriceOracleQuery;
    use tower::ServiceExt;

    use super::*;

    fn test_row(hour: u32, price_e8: i64, version: u64) -> PricePointRow {
        PricePointRow {
            hour,
            append_tick: hour + 800,
            source_tick: 900,
            epoch: 42,
            flags: 0,
            price_e8,
            source: PRICE_SOURCE_ORACLE,
            synced: 1,
            bound: 0,
            series_digest: [1u8; 32],
            observed_at: Utc.with_ymd_and_hms(2026, 11, 18, 12, 0, 0).unwrap(),
            version,
        }
    }

    fn price_query(currency1: &str, currency2: &str) -> PriceOracleQuery {
        PriceOracleQuery {
            oracle: currency_id("binance_mexc"),
            timestamp: 1_700_000_000_000_000,
            currency1: currency_id(currency1),
            currency2: currency_id(currency2),
        }
    }

    fn qpayhub_output(numerator: i64, denominator: i64, updated_tick: u32, stale: bool) -> Vec<u8> {
        let mut out = vec![0u8; 24];
        out[0..8].copy_from_slice(&numerator.to_le_bytes());
        out[8..16].copy_from_slice(&denominator.to_le_bytes());
        out[16..20].copy_from_slice(&updated_tick.to_le_bytes());
        out[20] = u8::from(stale);
        out
    }

    #[derive(Default)]
    struct MockStore {
        points: Mutex<Vec<PricePointRow>>,
        fail_write: AtomicBool,
    }

    #[async_trait]
    impl Store for MockStore {
        async fn init(&self) -> store::Result<()> {
            Ok(())
        }

        async fn drop_database(&self) -> store::Result<()> {
            Ok(())
        }

        async fn head(&self) -> store::Result<Option<store::TickRow>> {
            Ok(None)
        }

        async fn tick(&self, _tick: u32) -> store::Result<Option<store::TickRow>> {
            Ok(None)
        }

        async fn transactions_for_tick(
            &self,
            _tick: u32,
            _limit: u32,
            _offset: u64,
        ) -> store::Result<Vec<store::TransactionRow>> {
            Ok(Vec::new())
        }

        async fn transaction(&self, _hash: &str) -> store::Result<Option<store::TransactionRow>> {
            Ok(None)
        }

        async fn transactions_for_identity(
            &self,
            _identity: &str,
            _limit: u32,
            _offset: u64,
        ) -> store::Result<Vec<store::TransactionRow>> {
            Ok(Vec::new())
        }

        async fn recent_events(
            &self,
            _limit: u32,
            _offset: u64,
        ) -> store::Result<Vec<store::EventRow>> {
            Ok(Vec::new())
        }

        async fn recent_transactions(
            &self,
            _limit: u32,
            _offset: u64,
        ) -> store::Result<Vec<store::TransactionRow>> {
            Ok(Vec::new())
        }

        async fn events_for_tick(
            &self,
            _tick: u32,
            _limit: u32,
            _offset: u64,
        ) -> store::Result<Vec<store::EventRow>> {
            Ok(Vec::new())
        }

        async fn identity(&self, _identity: &str) -> store::Result<Option<store::IdentityRow>> {
            Ok(None)
        }

        async fn stats(&self) -> store::Result<Value> {
            Ok(json!({}))
        }

        async fn ticks(
            &self,
            _epoch: Option<u16>,
            _limit: u32,
            _offset: u64,
        ) -> store::Result<Vec<store::TickRow>> {
            Ok(Vec::new())
        }

        async fn epochs(&self, _limit: u32, _offset: u64) -> store::Result<Vec<store::EpochRow>> {
            Ok(Vec::new())
        }

        async fn epoch(&self, _epoch: u16) -> store::Result<Option<store::EpochRow>> {
            Ok(None)
        }

        async fn identities(
            &self,
            _limit: u32,
            _offset: u64,
        ) -> store::Result<Vec<store::IdentityRow>> {
            Ok(Vec::new())
        }

        async fn assets(
            &self,
            _issuer: Option<&str>,
            _limit: u32,
            _offset: u64,
        ) -> store::Result<Vec<store::AssetRow>> {
            Ok(Vec::new())
        }

        async fn asset(
            &self,
            _issuer: &str,
            _name: &str,
        ) -> store::Result<Option<store::AssetRow>> {
            Ok(None)
        }

        async fn transactions_page(
            &self,
            _filter: &store::TransactionFilter,
            _limit: u32,
            _offset: u64,
        ) -> store::Result<Vec<store::TransactionRow>> {
            Ok(Vec::new())
        }

        async fn events_page(
            &self,
            _filter: &store::EventFilter,
            _limit: u32,
            _offset: u64,
        ) -> store::Result<Vec<store::EventRow>> {
            Ok(Vec::new())
        }

        async fn network_stats(&self) -> store::Result<store::NetworkStats> {
            unimplemented!("not used by price tests")
        }

        async fn write_tick(
            &self,
            _batch: &store::TickBatch,
        ) -> store::Result<store::WriteOutcome> {
            unimplemented!("not used by price tests")
        }

        async fn cursor(&self, _key: &str) -> store::Result<Option<String>> {
            Ok(None)
        }

        async fn set_cursor(&self, _key: &str, _value: &str) -> store::Result<()> {
            Ok(())
        }

        async fn ingest_cursor(&self) -> store::Result<store::IngestCursor> {
            Ok(store::IngestCursor::default())
        }

        async fn set_ingest_cursor(&self, _cursor: &store::IngestCursor) -> store::Result<()> {
            Ok(())
        }

        async fn write_price_points(&self, points: &[PricePointRow]) -> store::Result<()> {
            if self.fail_write.load(Ordering::SeqCst) {
                return Err(store::Error::Config("simulated price write failure".into()));
            }
            self.points
                .lock()
                .expect("price test store lock")
                .extend_from_slice(points);
            Ok(())
        }

        async fn price_points(
            &self,
            from_hour: u32,
            limit: u32,
        ) -> store::Result<Vec<PricePointRow>> {
            let stored = self.points.lock().expect("price test store lock");
            let mut by_hour: HashMap<u32, PricePointRow> = HashMap::new();
            for row in stored.iter().filter(|row| row.hour >= from_hour) {
                let replace = by_hour
                    .get(&row.hour)
                    .is_none_or(|existing| existing.version <= row.version);
                if replace {
                    by_hour.insert(row.hour, row.clone());
                }
            }
            let mut rows: Vec<PricePointRow> = by_hour.into_values().collect();
            rows.sort_by_key(|row| row.hour);
            rows.truncate(limit.clamp(1, PRICE_WINDOW_MAX) as usize);
            Ok(rows)
        }

        async fn latest_price(&self) -> store::Result<Option<PricePointRow>> {
            Ok(self.price_points(0, PRICE_WINDOW_MAX).await?.pop())
        }

        async fn price_snapshot(&self) -> store::Result<Option<PriceSnapshot>> {
            let Some(latest) = self.latest_price().await? else {
                return Ok(None);
            };
            let points = self.price_points(0, PRICE_WINDOW_MAX).await?.len() as u64;
            Ok(Some(PriceSnapshot {
                synced: latest.synced != 0,
                bound: latest.bound != 0,
                source: latest.source,
                series_digest: latest.series_digest,
                epoch: latest.epoch,
                append_tick: latest.append_tick,
                source_tick: latest.source_tick,
                observed_at: latest.observed_at,
                latest: Some(latest),
                points,
            }))
        }
    }

    async fn request(store: Arc<MockStore>, uri: &str) -> (StatusCode, Value, HeaderMap) {
        let app = router().with_state(AppState::new(store));
        let response = app
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, body, headers)
    }

    async fn seeded_store() -> Arc<MockStore> {
        let store = Arc::new(MockStore::default());
        store
            .write_price_points(&[
                test_row(100, 1_234_567_890, 10),
                test_row(101, 1_234_567_800, 11),
            ])
            .await
            .unwrap();
        store
    }

    #[test]
    fn epoch_boundary_and_hour_mapping() {
        let epoch = Utc.with_ymd_and_hms(2022, 4, 13, 12, 0, 0).unwrap();
        assert_eq!(price_timestamp(0), epoch);
        assert_eq!(price_hour_at(epoch), 0);
        assert_eq!(
            price_hour_at(epoch - chrono::Duration::seconds(1)),
            0,
            "times before the epoch clamp to hour zero"
        );
        assert_eq!(price_hour_at(epoch + chrono::Duration::seconds(3599)), 0);
        assert_eq!(price_hour_at(epoch + chrono::Duration::seconds(3600)), 1);
        assert_eq!(price_timestamp(24), epoch + chrono::Duration::hours(24));
    }

    #[test]
    fn price_usdt_rounds_half_up() {
        assert!((price_usdt(123_456_789) - 1.234_567_89).abs() < 1e-12);
        assert!((price_usdt(1) - 0.000_000_01).abs() < 1e-12);
        assert_eq!(round_half_up(2.5, 0), 3.0);
        assert_eq!(round_half_up(-2.5, 0), -3.0);
        assert_eq!(round_half_up(1.25, 1), 1.3);
        assert_eq!(round_half_up(-1.25, 1), -1.3);
    }

    #[test]
    fn ratio_math_scales_and_rejects_invalid_values() {
        assert_eq!(ratio_price_e8(3, 2), Some(150_000_000));
        assert_eq!(ratio_price_e8(1, 1_000_000), Some(100));
        assert_eq!(ratio_price_e8(2, 3), Some(66_666_666));
        assert_eq!(ratio_price_e8(0, 1), None);
        assert_eq!(ratio_price_e8(-1, 1), None);
        assert_eq!(ratio_price_e8(1, 0), None);
        assert_eq!(ratio_price_e8(1, -1), None);
        assert_eq!(ratio_price_e8(i64::MAX, 1), None, "overflow is rejected");
        assert_eq!(ratio_price_e8(1, i64::MAX), None, "zero result is rejected");
    }

    #[test]
    fn pair_selection_accepts_qubic_usdt_in_both_orientations() {
        assert_eq!(
            pair_orientation(&price_query("QUBIC", "USDT")),
            Some(PairOrientation::Direct)
        );
        assert_eq!(
            pair_orientation(&price_query("USDT", "QUBIC")),
            Some(PairOrientation::Reversed)
        );
        assert_eq!(pair_orientation(&price_query("BTC", "USDT")), None);
        assert_eq!(pair_orientation(&price_query("QUBIC", "BTC")), None);
        assert_eq!(
            PairOrientation::Reversed.price_e8(2, 8),
            Some(400_000_000),
            "reversed pair inverts the ratio"
        );
    }

    #[test]
    fn rows_mark_stale_flags_without_losing_filled_meaning() {
        assert!(point_filled(PRICE_POINT_FLAG_FILLED));
        assert!(!point_filled(0));
        assert!(point_stale(PRICE_POINT_FLAG_STALE));
        assert!(!point_stale(PRICE_POINT_FLAG_FILLED));
        assert!(!point_filled(PRICE_POINT_FLAG_STALE));

        let observed = Utc.with_ymd_and_hms(2026, 11, 18, 12, 0, 0).unwrap();
        let price = OnchainPrice {
            price_e8: 12_345,
            source: PRICE_SOURCE_ORACLE,
            source_tick: 100,
            append_tick: 101,
            epoch: 240,
            stale: true,
            digest: [7u8; 32],
        };
        let row = price.row(50, observed, 1_700_000_000_000);
        assert_eq!(row.hour, 50);
        assert_eq!(row.flags, PRICE_POINT_FLAG_STALE);
        assert_eq!(row.synced, 0);
        assert_eq!(row.source, 0);
        assert_eq!(row.epoch, 240);
        assert_eq!(row.observed_at, observed);
        assert_eq!(row.version, 1_700_000_000_000);
    }

    #[test]
    fn qpayhub_output_parses_stale_and_unavailable() {
        let price = parse_qpayhub_price(&qpayhub_output(3, 2, 91_000, false))
            .expect("valid QPayhub output");
        assert_eq!(price.price_e8, 150_000_000);
        assert_eq!(price.source, QPAYHUB_CONTRACT_INDEX);
        assert_eq!(price.source_tick, 91_000);
        assert!(!price.stale);

        let stale =
            parse_qpayhub_price(&qpayhub_output(3, 2, 91_000, true)).expect("stale but nonzero");
        assert!(stale.stale);

        assert!(parse_qpayhub_price(&qpayhub_output(0, 0, 0, true)).is_none());
        assert!(parse_qpayhub_price(&qpayhub_output(1, 0, 0, false)).is_none());
        assert!(parse_qpayhub_price(&[0u8; 8]).is_none());
    }

    #[test]
    fn versions_are_monotonic_per_run() {
        assert_eq!(monotonic_version(0, 100), 100);
        assert_eq!(monotonic_version(100, 100), 101);
        assert_eq!(monotonic_version(100, 99), 101);
        let now = Utc.with_ymd_and_hms(2026, 11, 18, 12, 0, 0).unwrap();
        assert_eq!(unix_millis(now), now.timestamp_millis() as u64);
    }

    #[test]
    fn only_changed_hours_are_selected_for_broadcast() {
        let rows = vec![test_row(100, 10, 1), test_row(101, 20, 1)];
        let mut published = HashSet::new();
        let first: Vec<u32> = changed_rows(&rows, &mut published)
            .into_iter()
            .map(|row| row.hour)
            .collect();
        assert_eq!(first, vec![100, 101]);
        assert!(changed_rows(&rows, &mut published).is_empty());

        let mut updated = rows;
        updated[0].price_e8 = 11;
        let changed = changed_rows(&updated, &mut published);
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].hour, 100);
    }

    #[tokio::test]
    async fn store_write_error_does_not_stop_later_polls() {
        let store = Arc::new(MockStore::default());
        store.fail_write.store(true, Ordering::SeqCst);
        let rows = vec![test_row(100, 10, 1)];
        let mut published = HashSet::new();

        let error = persist_series(store.as_ref(), None, &rows, &mut published).await;
        assert!(error.is_err(), "the write error is reported, not panicked");
        assert!(published.is_empty(), "failed writes are not published");

        store.fail_write.store(false, Ordering::SeqCst);
        let count = persist_series(store.as_ref(), None, &rows, &mut published)
            .await
            .expect("the next poll succeeds");
        assert_eq!(count, rows.len());
    }

    #[tokio::test]
    async fn latest_returns_not_found_when_empty() {
        let (status, body, headers) =
            request(Arc::new(MockStore::default()), "/v1/prices/latest").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "not_found");
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    }

    #[tokio::test]
    async fn latest_and_snapshot_expose_the_stored_point() {
        let store = seeded_store().await;
        let (status, body, headers) = request(store.clone(), "/v1/prices/latest").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["hour"], 101);
        assert_eq!(body["data"]["price_e8"], 1_234_567_800i64);
        assert_eq!(body["data"]["source_tick"], 900);
        assert_eq!(body["data"]["epoch"], 42);
        assert!(!body["data"]["filled"].as_bool().unwrap());
        assert!(!body["data"]["stale"].as_bool().unwrap());
        assert_eq!(body["data"]["series_digest"].as_str().unwrap().len(), 64);
        assert_eq!(body["meta"]["pair"], PRICE_PAIR);
        assert_eq!(body["meta"]["stale"], false);
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");

        let (status, body, headers) = request(store, "/v1/prices/snapshot").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["points"], 2);
        assert_eq!(body["data"]["latest"]["hour"], 101);
        assert!(body["data"]["timestamp"].is_string());
        assert_eq!(body["data"]["stale"], false);
        assert_eq!(body["meta"]["pair"], PRICE_PAIR);
        assert_eq!(body["meta"]["available"], true);
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    }

    #[tokio::test]
    async fn list_and_history_are_ascending_and_windowed() {
        let store = seeded_store().await;
        let (status, body, _) = request(store.clone(), "/v1/prices?from_hour=100&limit=1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"].as_array().unwrap().len(), 1);
        assert_eq!(body["data"][0]["hour"], 100);
        assert_eq!(body["meta"]["price_scale"], 100_000_000_i64);
        assert_eq!(body["meta"]["latest_hour"], 101);
        assert_eq!(body["meta"]["pair"], PRICE_PAIR);

        let (status, body, _) = request(store, "/v1/prices/history?hours=1").await;
        assert_eq!(status, StatusCode::OK);
        let data = body["data"].as_array().unwrap();
        assert!(data.len() <= 1);
        assert_eq!(body["meta"]["hours"], 1);
        assert_eq!(body["meta"]["pair"], PRICE_PAIR);
    }

    #[tokio::test]
    async fn empty_snapshot_reports_not_ready_with_warning() {
        let (status, body, headers) =
            request(Arc::new(MockStore::default()), "/v1/prices/snapshot").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["data"].is_null());
        assert_eq!(body["meta"]["available"], false);
        assert_eq!(body["meta"]["pair"], PRICE_PAIR);
        assert!(
            body["meta"]["warning"]
                .as_str()
                .unwrap()
                .contains("no on-chain QUBIC/USDT price")
        );
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    }

    #[tokio::test]
    async fn run_poller_without_peers_returns_immediately() {
        let store: Arc<dyn Store> = Arc::new(MockStore::default());
        run_poller(
            store,
            Vec::new(),
            None,
            Duration::from_millis(1),
            CancellationToken::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn run_poller_honors_cancellation_before_the_first_poll() {
        let store: Arc<dyn Store> = Arc::new(MockStore::default());
        let cancel = CancellationToken::new();
        cancel.cancel();
        run_poller(
            store,
            vec![PeerConfig::new("127.0.0.1:1".parse().unwrap())],
            None,
            Duration::from_millis(1),
            cancel,
        )
        .await;
    }
}
