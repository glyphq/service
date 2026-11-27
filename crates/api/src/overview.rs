//! Composite overview routes for dashboards and wallet screens.
//!
//! Each route joins the index, the operator label registry, and best-effort
//! peer or market data into one response so a client does not need a request
//! waterfall for a first paint. Nothing here is a new source of truth: every
//! section names its source and preserves the underlying rows.

use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, header};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{Duration as ChronoDuration, Utc};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::dto::{event_value, transaction_value};
use crate::{ApiError, AppState, market, prices, wallet};

const CACHE_HEADER: &str = "public, max-age=5, stale-while-revalidate=15";
const PEER_BUDGET: Duration = Duration::from_secs(2);
/// Default whale threshold in qu. Operators can override it with
/// `GLYPH_WHALE_MIN_AMOUNT`.
const DEFAULT_WHALE_MIN_AMOUNT: i64 = 1_000_000_000;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/explorer/overview", get(explorer_overview))
        .route("/v1/wallets/{identity}/overview", get(wallet_overview))
        .route("/v1/identities/{identity}/overview", get(identity_overview))
        .route("/v1/assets/{issuer}/{name}/overview", get(asset_overview))
}

#[derive(Debug, Default, Deserialize)]
struct ExplorerQuery {
    #[serde(default)]
    interval: Option<u32>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    min_amount: Option<i64>,
}

#[derive(Debug, Default, Deserialize)]
struct IdentityQuery {
    #[serde(default)]
    interval: Option<u32>,
    #[serde(default)]
    window: Option<i64>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
struct AssetQuery {
    #[serde(default)]
    holder_limit: Option<u32>,
}

pub fn whale_min_amount() -> i64 {
    std::env::var("GLYPH_WHALE_MIN_AMOUNT")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_WHALE_MIN_AMOUNT)
}

fn cache_header() -> [(header::HeaderName, HeaderValue); 1] {
    [(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_HEADER),
    )]
}

async fn explorer_overview(
    State(state): State<AppState>,
    Query(params): Query<ExplorerQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = params.limit.unwrap_or(10).clamp(1, 50);
    let interval = params.interval.unwrap_or(3600).clamp(1, 86_400);
    let whale_min = params.min_amount.unwrap_or_else(whale_min_amount).max(1);
    let cache_key = format!("explorer:overview:{limit}:{interval}:{whale_min}");
    if let Some(value) = state.live_cache.get(&cache_key, Duration::from_secs(5)) {
        return Ok((cache_header(), Json(value)));
    }

    // The stats read is cached for five seconds and gives the head needed to
    // bound the whale scan. Running it first avoids sorting every historical
    // transfer just to return the newest overview.
    let stats = state.network_stats().await?;
    let whale_since = stats
        .head
        .as_ref()
        .map(|head| head.tick.saturating_sub(10_000));
    let (
        ticks,
        transactions,
        events,
        activity,
        active,
        identities,
        contracts,
        whales,
        latest_price,
    ) = tokio::join!(
        state.store.ticks(None, limit, 0),
        state.store.recent_transactions(limit, 0),
        state.store.recent_events(limit, 0),
        state.store.activity_analytics(interval, 24),
        state.store.active_identity_buckets(interval, 24),
        state.store.top_identities(limit),
        state.store.contracts(limit, 0),
        state
            .store
            .whale_transactions(whale_min, whale_since, limit),
        state.store.latest_price(),
    );
    let ticks = ticks?;
    let transactions = transactions?;
    let events = events?;
    let activity = activity?;
    let active = active?;
    let whales = whales?;
    let latest_price = latest_price?;
    let identities: Vec<Value> = identities?
        .into_iter()
        .map(|row| {
            let identity = row.identity.clone();
            let mut value = serde_json::to_value(row).unwrap_or(Value::Null);
            state.labels.enrich_identity(&mut value, &identity);
            value
        })
        .collect();
    let contracts: Vec<Value> = contracts?
        .into_iter()
        .map(|row| {
            let contract_index = row.contract_index;
            let mut value = serde_json::to_value(row).unwrap_or(Value::Null);
            state.labels.enrich_contract(&mut value, contract_index);
            value
        })
        .collect();

    let market = state.market.snapshot();
    let data = json!({
        "network": {
            "head": stats.head,
            "epochs": stats.epochs,
            "ticks": stats.ticks,
            "transactions": stats.transactions,
            "events": stats.events,
            "identities": stats.identities,
            "latest_timestamp": stats.latest_timestamp,
            "ingest": stats.ingest,
        },
        "latest_ticks": ticks,
        "latest_transactions": transactions.iter().map(transaction_value).collect::<Vec<_>>(),
        "latest_events": events.into_iter().map(event_value).collect::<Vec<_>>(),
        "activity": activity,
        "active_identities": active,
        "top_identities": identities,
        "contracts": contracts,
        "whales": whales.iter().map(transaction_value).collect::<Vec<_>>(),
        "price": latest_price.as_ref().map(prices::point_json),
        "market": market.as_ref().map(market::market_data),
    });

    let body = json!({
        "data": data,
        "meta": {
            "source": "index+market",
            "interval_seconds": interval,
            "limit": limit,
            "whale_min_amount": whale_min,
            "whale_since_tick": whale_since,
            "generated_at": Utc::now(),
        }
    });
    state.live_cache.put(cache_key, body.clone());
    Ok((cache_header(), Json(body)))
}

async fn wallet_overview(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(params): Query<IdentityQuery>,
) -> Result<impl IntoResponse, ApiError> {
    identity_overview_inner(state, identity, params, true).await
}

async fn identity_overview(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(params): Query<IdentityQuery>,
) -> Result<impl IntoResponse, ApiError> {
    identity_overview_inner(state, identity, params, false).await
}

async fn identity_overview_inner(
    state: AppState,
    identity: String,
    params: IdentityQuery,
    include_wallet: bool,
) -> Result<impl IntoResponse, ApiError> {
    let (identity, key) = wallet::parse_identity(&identity)?;
    let limit = params.limit.unwrap_or(10).clamp(1, 50);
    let interval = params.interval.unwrap_or(3600).clamp(60, 86_400);
    let window = params.window.unwrap_or(86_400).clamp(3_600, 2_592_000);
    let from = Utc::now() - ChronoDuration::seconds(window);

    let (local, flow, counterparties, activity, events, recent) = tokio::join!(
        state.store.identity(&identity),
        state.store.identity_flow(&identity, interval, from, 500),
        state.store.counterparties(&identity, None, limit),
        state.store.activity_for_identity(&identity, limit, 0),
        state.store.events_for_identity(&identity, limit, 0),
        state.store.transactions_for_identity(&identity, limit, 0),
    );
    let local = local?;
    let flow = flow.unwrap_or_default();
    let counterparties = counterparties.unwrap_or_default();
    let activity = activity.unwrap_or_default();
    let events = events.unwrap_or_default();
    let recent = recent?;

    let wallet_section = if include_wallet && !state.peers.is_empty() {
        match tokio::time::timeout(PEER_BUDGET, wallet::build_wallet(&state, &identity, key)).await
        {
            Ok(Ok((body, _))) => Some(body.get("data").cloned().unwrap_or(Value::Null)),
            _ => None,
        }
    } else {
        None
    };
    if local.is_none() && wallet_section.is_none() {
        return Err(ApiError::not_found(format!(
            "identity {identity} is neither indexed nor available from a peer"
        )));
    }

    let mut identity_value = local
        .as_ref()
        .map(|row| serde_json::to_value(row).unwrap_or(Value::Null))
        .unwrap_or(Value::Null);
    if !identity_value.is_null() {
        state.labels.enrich_identity(&mut identity_value, &identity);
    }
    let mut event_values: Vec<Value> = events.into_iter().map(event_value).collect();
    for event in &mut event_values {
        let participant = event
            .get("body")
            .and_then(|body| body.get("identity"))
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(participant) = participant {
            state.labels.enrich_identity(event, &participant);
        }
    }
    let counterparty_values: Vec<Value> = counterparties
        .into_iter()
        .map(|row| {
            let counterparty = row.identity.clone();
            let mut value = serde_json::to_value(row).unwrap_or(Value::Null);
            state.labels.enrich_identity(&mut value, &counterparty);
            value
        })
        .collect();
    let recent_values: Vec<Value> = recent
        .iter()
        .map(|row| {
            let mut value = transaction_value(row);
            state.labels.enrich_transaction(&mut value);
            value
        })
        .collect();

    let data = json!({
        "identity": identity,
        "public_key": key.to_hex(),
        "indexed": identity_value,
        "wallet": wallet_section,
        "flow": flow,
        "counterparties": counterparty_values,
        "recent_transactions": recent_values,
        "activity": activity,
        "events": event_values,
    });

    Ok((
        cache_header(),
        Json(json!({
            "data": data,
            "meta": {
                "source": if wallet_section.is_some() { "peer+index" } else { "index" },
                "interval_seconds": interval,
                "window_seconds": window,
                "limit": limit,
                "peers_configured": !state.peers.is_empty(),
                "generated_at": Utc::now(),
            }
        })),
    ))
}

async fn asset_overview(
    State(state): State<AppState>,
    Path((issuer, name)): Path<(String, String)>,
    Query(params): Query<AssetQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let (issuer, _) = wallet::parse_identity(&issuer)?;
    let name = wallet::normalize_asset_name(&name)?;
    let asset = state
        .store
        .asset(&issuer, &name)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("asset {issuer}/{name} is not indexed")))?;
    let (stats, transfers) = tokio::join!(
        state.store.asset_transfer_stats(&issuer, &name),
        state.store.asset_transfers(&issuer, &name, None, 20, 0),
    );
    let stats = stats?;
    let transfers = transfers?;

    let holder_limit = params.holder_limit.unwrap_or(50).clamp(1, 500);
    let (holders, holders_truncated, holders_warning) = if state.peers.is_empty() {
        (Vec::new(), false, Some("no peers configured".to_string()))
    } else {
        match tokio::time::timeout(
            PEER_BUDGET,
            wallet::holders_for(&state, &issuer, &name, holder_limit),
        )
        .await
        {
            Ok(Ok((holders, truncated))) => (holders, truncated, None),
            Ok(Err(error)) => (Vec::new(), false, Some(error.detail)),
            Err(_) => (
                Vec::new(),
                false,
                Some("holder lookup timed out".to_string()),
            ),
        }
    };

    let mut asset_value = serde_json::to_value(&asset).unwrap_or(Value::Null);
    state.labels.enrich_asset(&mut asset_value, &issuer, &name);
    let data = json!({
        "issuance": asset_value,
        "stats": stats,
        "recent_transfers": transfers,
        "holders": holders,
    });

    Ok((
        cache_header(),
        Json(json!({
            "data": data,
            "meta": {
                "issuer": issuer,
                "name": name,
                "holder_limit": holder_limit,
                "holders_truncated": holders_truncated,
                "holder_warning": holders_warning,
                "generated_at": Utc::now(),
            }
        })),
    ))
}
