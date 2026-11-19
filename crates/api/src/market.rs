//! External QUBIC market data, sourced from direct exchange tickers and
//! CoinGecko.
//!
//! On-chain QUBIC/USDT oracle data is preferred where it exists. No QUBIC/USDT
//! oracle subscription is currently active, so the external spot price is the
//! practical source. MEXC and Gate.io are queried directly for the QUBIC/USDT
//! last price; CoinGecko supplies market cap, 24-hour volume, and the 24-hour
//! change. Everything is cached in memory so public reads never fan out to an
//! external API.

use std::collections::HashSet;
use std::sync::RwLock;
use std::time::Duration;

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use store::{PricePointRow, Store};

use crate::prices::{PRICE_SOURCE_MARKET, price_hour_at};
use crate::stream::StreamHub;
use crate::{ApiError, AppState};

const DEFAULT_COIN_ID: &str = "qubic-network";
const DEFAULT_ENDPOINT: &str = "https://api.coingecko.com/api/v3/simple/price";
const DEFAULT_MEXC_ENDPOINT: &str = "https://api.mexc.com/api/v3/ticker/price";
const DEFAULT_MEXC_KLINES_ENDPOINT: &str = "https://api.mexc.com/api/v3/klines";
const DEFAULT_GATE_ENDPOINT: &str = "https://api.gateio.ws/api/v4/spot/tickers";
const PRICE_SCALE: f64 = 100_000_000.0;

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/market", get(market))
}

/// One externally sourced QUBIC market snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub source: String,
    pub coin_id: String,
    pub price_usd: f64,
    pub price_e8: i64,
    pub market_cap_usd: Option<f64>,
    pub volume_24h_usd: Option<f64>,
    pub change_24h_percent: Option<f64>,
    pub last_updated_at: Option<DateTime<Utc>>,
    pub fetched_at: DateTime<Utc>,
    #[serde(default)]
    pub sources: Vec<MarketQuote>,
}

/// One exchange spot quote included in the consensus snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketQuote {
    pub source: String,
    pub pair: String,
    pub price_usd: f64,
    pub volume_24h_usd: Option<f64>,
    pub change_24h_percent: Option<f64>,
}

impl MarketSnapshot {
    /// Seconds since the snapshot was fetched, saturating at zero.
    pub fn age_seconds(&self) -> i64 {
        Utc::now()
            .signed_duration_since(self.fetched_at)
            .num_seconds()
            .max(0)
    }

    /// True when the snapshot is older than five minutes.
    pub fn stale(&self) -> bool {
        self.age_seconds() > 300
    }
}

#[derive(Debug, Default)]
pub struct MarketCache {
    inner: RwLock<MarketState>,
}

#[derive(Debug, Clone)]
struct QuoteEntry {
    quote: MarketQuote,
    fetched_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
struct MarketState {
    snapshot: Option<MarketSnapshot>,
    last_error: Option<String>,
    last_attempt_at: Option<DateTime<Utc>>,
    quotes: Vec<QuoteEntry>,
    coingecko: Option<CoinGeckoMeta>,
}

impl MarketCache {
    pub fn snapshot(&self) -> Option<MarketSnapshot> {
        self.view().0
    }

    /// Replaces the cached snapshot directly. Used by one-shot flows and
    /// tests; the poller uses [`MarketCache::record_quote`] so the per-provider
    /// quote set stays current.
    #[cfg(test)]
    pub(crate) fn store(&self, snapshot: MarketSnapshot) -> Option<MarketSnapshot> {
        if let Ok(mut state) = self.inner.write() {
            state.last_error = None;
            state.last_attempt_at = Some(Utc::now());
            state.snapshot.replace(snapshot)
        } else {
            None
        }
    }

    /// Records one provider's latest quote and recomputes the consensus
    /// snapshot from every provider that answered recently. Returns the new
    /// snapshot when at least one quote is usable.
    pub(crate) fn record_quote(
        &self,
        quote: MarketQuote,
        meta: Option<CoinGeckoMeta>,
        coin_id: &str,
    ) -> Option<MarketSnapshot> {
        if let Ok(mut state) = self.inner.write() {
            let now = Utc::now();
            state.last_attempt_at = Some(now);
            state.last_error = None;
            let entry = QuoteEntry {
                quote,
                fetched_at: now,
            };
            if let Some(existing) = state
                .quotes
                .iter_mut()
                .find(|existing| existing.quote.source == entry.quote.source)
            {
                *existing = entry;
            } else {
                state.quotes.push(entry);
            }
            // A provider that has been down for five minutes should not keep
            // skewing the median with an old price.
            state
                .quotes
                .retain(|entry| now.signed_duration_since(entry.fetched_at).num_seconds() <= 300);
            if let Some(meta) = meta {
                state.coingecko = Some(meta);
            }
            let quotes: Vec<MarketQuote> = state
                .quotes
                .iter()
                .map(|entry| entry.quote.clone())
                .collect();
            let snapshot = snapshot_from_quotes(quotes, state.coingecko.as_ref(), coin_id).ok();
            if snapshot.is_some() {
                state.snapshot = snapshot.clone();
            }
            snapshot
        } else {
            None
        }
    }

    fn error(&self, error: impl Into<String>) {
        if let Ok(mut state) = self.inner.write() {
            state.last_error = Some(error.into());
            state.last_attempt_at = Some(Utc::now());
        }
    }

    fn view(
        &self,
    ) -> (
        Option<MarketSnapshot>,
        Option<String>,
        Option<DateTime<Utc>>,
    ) {
        let Ok(state) = self.inner.read() else {
            return (None, Some("market cache lock poisoned".into()), None);
        };
        (
            state.snapshot.clone(),
            state.last_error.clone(),
            state.last_attempt_at,
        )
    }
}

#[derive(Debug, Clone)]
pub struct MarketConfig {
    pub endpoint: String,
    pub mexc_endpoint: String,
    pub mexc_klines_endpoint: String,
    pub gate_endpoint: String,
    pub coin_id: String,
    pub api_key: Option<String>,
    pub interval: Duration,
}

impl MarketConfig {
    pub fn from_env() -> Self {
        Self {
            endpoint: std::env::var("COINGECKO_URL")
                .unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned()),
            mexc_endpoint: std::env::var("MEXC_URL")
                .unwrap_or_else(|_| DEFAULT_MEXC_ENDPOINT.to_owned()),
            mexc_klines_endpoint: std::env::var("MEXC_KLINES_URL")
                .unwrap_or_else(|_| DEFAULT_MEXC_KLINES_ENDPOINT.to_owned()),
            gate_endpoint: std::env::var("GATE_URL")
                .unwrap_or_else(|_| DEFAULT_GATE_ENDPOINT.to_owned()),
            coin_id: std::env::var("COINGECKO_COIN_ID")
                .unwrap_or_else(|_| DEFAULT_COIN_ID.to_owned()),
            api_key: std::env::var("COINGECKO_API_KEY").ok(),
            interval: std::env::var("MARKET_POLL_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_millis)
                .unwrap_or(Duration::from_secs(5)),
        }
    }
}

/// Polls the external exchange consensus until cancelled. A failed poll keeps
/// the previous snapshot and records the error, so a transient outage does not
/// blank the endpoint.
pub async fn run_poller(
    cache: std::sync::Arc<MarketCache>,
    store: std::sync::Arc<dyn Store>,
    config: MarketConfig,
    hub: Option<std::sync::Arc<StreamHub>>,
    cancel: tokio_util::sync::CancellationToken,
) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent(concat!("glyph-api/", env!("CARGO_PKG_VERSION")))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            cache.error(format!("market http client failed: {error}"));
            return;
        }
    };
    let mut backfilled = false;
    let mut cursor = 0usize;
    loop {
        if cancel.is_cancelled() {
            break;
        }
        // One provider per tick. With a five-second interval each source is
        // contacted every fifteen seconds, which keeps the combined view fresh
        // without burning a free-tier request budget in parallel.
        let provider = cursor % 3;
        cursor = cursor.wrapping_add(1);
        let name = ["mexc", "gate", "coingecko"][provider];
        let result = match provider {
            0 => fetch_mexc(&client, &config.mexc_endpoint)
                .await
                .map(|quote| (quote, None)),
            1 => fetch_gate(&client, &config.gate_endpoint)
                .await
                .map(|quote| (quote, None)),
            _ => fetch_coingecko(&client, &config)
                .await
                .map(|(quote, meta)| (quote, Some(meta))),
        };
        match result {
            Ok((quote, meta)) => {
                let previous = cache.snapshot();
                let snapshot = cache.record_quote(quote, meta, &config.coin_id);
                if let Some(snapshot) = snapshot {
                    let changed = previous
                        .as_ref()
                        .is_none_or(|previous| previous.price_e8 != snapshot.price_e8);
                    if changed {
                        if let Some(hub) = hub.as_deref() {
                            hub.publish_event(
                                "market",
                                format!(
                                    "market:{}:{}",
                                    snapshot.price_e8,
                                    snapshot.fetched_at.timestamp()
                                ),
                                market_data(&snapshot),
                            );
                        }
                    }
                }
            }
            Err(error) => {
                cache.error(format!("{name}: {error}"));
                tracing::warn!(provider = name, %error, "market provider poll failed");
            }
        }
        // The one-time candle backfill uses MEXC klines, so run it only after
        // the first MEXC spot quote succeeds. It is independent of the
        // round-robin schedule and never blocks the next provider slot longer
        // than its HTTP timeout.
        if !backfilled && provider == 0 {
            match fetch_klines(&client, &config, "QUBICUSDT", 168).await {
                Ok(candles) if candles.is_empty() => {
                    // An empty or entirely invalid response is not proof that
                    // there is no history. Retry on the next MEXC turn.
                    tracing::warn!("external QUBIC/USDT candle response was empty; retrying later");
                }
                Ok(candles) => match backfill_klines(store.as_ref(), &candles).await {
                    Ok(count) => {
                        tracing::info!(count, "backfilled external QUBIC/USDT price history");
                        backfilled = true;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "external price history backfill failed")
                    }
                },
                Err(error) => {
                    tracing::warn!(%error, "could not fetch external QUBIC/USDT candles")
                }
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.interval) => {}
        }
    }
}

/// Fetches one external market snapshot without starting the poller.
pub async fn fetch_once(config: &MarketConfig) -> anyhow::Result<MarketSnapshot> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent(concat!("glyph-api/", env!("CARGO_PKG_VERSION")))
        .build()?;
    fetch(&client, config).await
}

/// Closes one completed hourly candle into a storable price point.
#[derive(Debug, Clone, Copy)]
pub struct MarketKline {
    pub open_time: DateTime<Utc>,
    pub open_usd: f64,
    pub close_time: DateTime<Utc>,
    pub close_usd: f64,
}

/// Fetches completed hourly MEXC candles for `symbol`.
pub async fn fetch_klines(
    client: &reqwest::Client,
    config: &MarketConfig,
    symbol: &str,
    limit: u32,
) -> anyhow::Result<Vec<MarketKline>> {
    let limit = limit.clamp(1, 1000).to_string();
    let response = client
        .get(&config.mexc_klines_endpoint)
        .query(&[
            ("symbol", symbol),
            ("interval", "60m"),
            ("limit", limit.as_str()),
        ])
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("mexc klines returned HTTP {status}");
    }
    let body: Value = response.json().await?;
    let rows = body
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("mexc klines response is not an array"))?;
    let now = Utc::now();
    let mut candles = Vec::new();
    for row in rows {
        let Some(open_time_ms) = row.get(0).and_then(Value::as_i64) else {
            continue;
        };
        let Some(close_time_ms) = row.get(6).and_then(Value::as_i64) else {
            continue;
        };
        let Some(open_usd) = row
            .get(1)
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|price| price.is_finite() && *price > 0.0)
        else {
            continue;
        };
        let Some(close_usd) = row
            .get(4)
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|price| price.is_finite() && *price > 0.0)
        else {
            continue;
        };
        let Some(open_time) = DateTime::from_timestamp_millis(open_time_ms) else {
            continue;
        };
        let Some(close_time) = DateTime::from_timestamp_millis(close_time_ms) else {
            continue;
        };
        if close_time >= now {
            continue;
        }
        candles.push(MarketKline {
            open_time,
            open_usd,
            close_time,
            close_usd,
        });
    }
    Ok(candles)
}

/// Writes completed hourly candles as external price points.
///
/// Hours that already have an on-chain point (the oracle or QPayhub) are left
/// untouched. Candle close timestamps are normally newer than the on-chain
/// observation, so writing them unconditionally would let backfilled external
/// data replace the authoritative on-chain series for the same hour.
pub async fn backfill_klines(store: &dyn Store, candles: &[MarketKline]) -> anyhow::Result<usize> {
    let mut rows = candles
        .iter()
        .filter_map(|candle| {
            let price_e8 = price_e8(candle.close_usd)?;
            Some(PricePointRow {
                hour: price_hour_at(candle.close_time),
                append_tick: 0,
                source_tick: 0,
                epoch: 0,
                flags: 0,
                price_e8,
                source: PRICE_SOURCE_MARKET,
                synced: 1,
                bound: 0,
                series_digest: [0u8; 32],
                observed_at: candle.close_time,
                version: u64::try_from(candle.close_time.timestamp_millis()).unwrap_or(0),
            })
        })
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(0);
    }
    let first_hour = rows.iter().map(|row| row.hour).min().unwrap_or(0);
    let last_hour = rows.iter().map(|row| row.hour).max().unwrap_or(first_hour);
    let span = last_hour.saturating_sub(first_hour).saturating_add(1);
    let existing = store
        .price_points(first_hour, span)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let onchain: HashSet<u32> = existing
        .iter()
        .filter(|row| row.source != PRICE_SOURCE_MARKET)
        .map(|row| row.hour)
        .collect();
    let skipped = retain_external_hours(&mut rows, &onchain);
    if skipped > 0 {
        tracing::debug!(
            skipped,
            "kept on-chain price hours during external backfill"
        );
    }
    if rows.is_empty() {
        return Ok(0);
    }
    store
        .write_price_points(&rows)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(rows.len())
}

/// Removes candles whose hour already has an authoritative on-chain point.
fn retain_external_hours(rows: &mut Vec<PricePointRow>, onchain: &HashSet<u32>) -> usize {
    let before = rows.len();
    rows.retain(|row| !onchain.contains(&row.hour));
    before - rows.len()
}

fn price_e8(price_usd: f64) -> Option<i64> {
    let scaled = (price_usd * PRICE_SCALE).round();
    if scaled.is_finite() && scaled > 0.0 && scaled <= i64::MAX as f64 {
        Some(scaled as i64)
    } else {
        None
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CoinGeckoMeta {
    market_cap_usd: Option<f64>,
    volume_24h_usd: Option<f64>,
    change_24h_percent: Option<f64>,
    last_updated_at: Option<DateTime<Utc>>,
}

async fn fetch(client: &reqwest::Client, config: &MarketConfig) -> anyhow::Result<MarketSnapshot> {
    let (mexc, gate, coingecko) = tokio::join!(
        fetch_mexc(client, &config.mexc_endpoint),
        fetch_gate(client, &config.gate_endpoint),
        fetch_coingecko(client, config),
    );
    let mut quotes = Vec::new();
    let mut errors = Vec::new();
    for (name, result) in [("mexc", mexc), ("gate", gate)] {
        match result {
            Ok(quote) => quotes.push(quote),
            Err(error) => errors.push(format!("{name}: {error}")),
        }
    }
    let mut meta = None;
    match coingecko {
        Ok((quote, coingecko)) => {
            quotes.push(quote);
            meta = Some(coingecko);
        }
        Err(error) => errors.push(format!("coingecko: {error}")),
    }
    if quotes.is_empty() {
        anyhow::bail!("no QUBIC/USDT price source answered: {}", errors.join("; "));
    }
    snapshot_from_quotes(quotes, meta.as_ref(), &config.coin_id)
}

/// Builds a consensus snapshot from the quotes that answered recently.
///
/// The median is robust to one exchange printing a bad tick, and the source
/// list makes it explicit which venues contributed. At least one quote is
/// required.
fn snapshot_from_quotes(
    quotes: Vec<MarketQuote>,
    meta: Option<&CoinGeckoMeta>,
    coin_id: &str,
) -> anyhow::Result<MarketSnapshot> {
    if quotes.is_empty() {
        anyhow::bail!("no QUBIC/USDT price source is currently available");
    }
    let price_usd = median_price(quotes.iter().map(|quote| quote.price_usd))?;
    let price_e8 = (price_usd * PRICE_SCALE).round();
    if !price_e8.is_finite() || price_e8 <= 0.0 || price_e8 > i64::MAX as f64 {
        anyhow::bail!("QUBIC/USDT price is out of range: {price_usd}");
    }
    let source = quotes
        .iter()
        .map(|quote| quote.source.as_str())
        .collect::<Vec<_>>()
        .join("+");
    let volume_24h_usd = meta.and_then(|meta| meta.volume_24h_usd).or_else(|| {
        let quote = quotes
            .iter()
            .find(|quote| quote.source == "gate")
            .or_else(|| quotes.iter().find(|quote| quote.volume_24h_usd.is_some()))?;
        quote.volume_24h_usd
    });
    let change_24h_percent = meta.and_then(|meta| meta.change_24h_percent).or_else(|| {
        quotes
            .iter()
            .find(|quote| quote.source == "gate")
            .and_then(|quote| quote.change_24h_percent)
    });
    Ok(MarketSnapshot {
        source,
        coin_id: coin_id.to_owned(),
        price_usd,
        price_e8: price_e8 as i64,
        market_cap_usd: meta.and_then(|meta| meta.market_cap_usd),
        volume_24h_usd,
        change_24h_percent,
        last_updated_at: meta
            .and_then(|meta| meta.last_updated_at)
            .or_else(|| Some(Utc::now())),
        fetched_at: Utc::now(),
        sources: quotes,
    })
}

fn median_price(prices: impl Iterator<Item = f64>) -> anyhow::Result<f64> {
    let mut prices = prices
        .filter(|price| price.is_finite() && *price > 0.0)
        .collect::<Vec<_>>();
    if prices.is_empty() {
        anyhow::bail!("no finite positive price");
    }
    prices.sort_by(|left, right| left.total_cmp(right));
    let middle = prices.len() / 2;
    if prices.len() % 2 == 0 {
        // With an even number of sources, for example when one exchange is
        // down, average the two middle quotes instead of biasing upward.
        Ok((prices[middle - 1] + prices[middle]) / 2.0)
    } else {
        Ok(prices[middle])
    }
}

async fn fetch_mexc(client: &reqwest::Client, endpoint: &str) -> anyhow::Result<MarketQuote> {
    let response = client
        .get(endpoint)
        .query(&[("symbol", "QUBICUSDT")])
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("mexc returned HTTP {status}");
    }
    let body: Value = response.json().await?;
    let price = body
        .get("price")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|price| price.is_finite() && *price > 0.0)
        .ok_or_else(|| anyhow::anyhow!("mexc response has no positive price"))?;
    Ok(MarketQuote {
        source: "mexc".into(),
        pair: "QUBIC/USDT".into(),
        price_usd: price,
        volume_24h_usd: None,
        change_24h_percent: None,
    })
}

async fn fetch_gate(client: &reqwest::Client, endpoint: &str) -> anyhow::Result<MarketQuote> {
    let response = client
        .get(endpoint)
        .query(&[("currency_pair", "QUBIC_USDT")])
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("gate returned HTTP {status}");
    }
    let body: Value = response.json().await?;
    let ticker = body
        .as_array()
        .and_then(|rows| rows.first())
        .ok_or_else(|| anyhow::anyhow!("gate response has no ticker"))?;
    let price = ticker
        .get("last")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|price| price.is_finite() && *price > 0.0)
        .ok_or_else(|| anyhow::anyhow!("gate ticker has no positive price"))?;
    Ok(MarketQuote {
        source: "gate".into(),
        pair: "QUBIC/USDT".into(),
        price_usd: price,
        volume_24h_usd: ticker
            .get("quote_volume")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<f64>().ok()),
        change_24h_percent: ticker
            .get("change_percentage")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<f64>().ok()),
    })
}

async fn fetch_coingecko(
    client: &reqwest::Client,
    config: &MarketConfig,
) -> anyhow::Result<(MarketQuote, CoinGeckoMeta)> {
    let mut request = client.get(&config.endpoint).query(&[
        ("ids", config.coin_id.as_str()),
        ("vs_currencies", "usd"),
        ("include_market_cap", "true"),
        ("include_24hr_vol", "true"),
        ("include_24hr_change", "true"),
        ("include_last_updated_at", "true"),
    ]);
    if let Some(key) = config.api_key.as_deref() {
        request = request.header("x-cg-demo-api-key", key);
    }
    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("coingecko returned HTTP {status}");
    }
    let body: Value = response.json().await?;
    let coin = body
        .get(&config.coin_id)
        .ok_or_else(|| anyhow::anyhow!("coingecko response has no {}", config.coin_id))?;
    let price_usd = coin
        .get("usd")
        .and_then(Value::as_f64)
        .filter(|price| price.is_finite() && *price > 0.0)
        .ok_or_else(|| anyhow::anyhow!("coingecko response has no positive usd price"))?;
    Ok((
        MarketQuote {
            source: "coingecko".into(),
            pair: "QUBIC/USD".into(),
            price_usd,
            volume_24h_usd: coin.get("usd_24h_vol").and_then(Value::as_f64),
            change_24h_percent: coin.get("usd_24h_change").and_then(Value::as_f64),
        },
        CoinGeckoMeta {
            market_cap_usd: coin.get("usd_market_cap").and_then(Value::as_f64),
            volume_24h_usd: coin.get("usd_24h_vol").and_then(Value::as_f64),
            change_24h_percent: coin.get("usd_24h_change").and_then(Value::as_f64),
            last_updated_at: coin
                .get("last_updated_at")
                .and_then(Value::as_i64)
                .and_then(|seconds| DateTime::from_timestamp(seconds, 0)),
        },
    ))
}

async fn market(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let (snapshot, error, last_attempt_at) = state.market.view();
    let Some(snapshot) = snapshot else {
        return Err(ApiError::unavailable(format!(
            "market data is not available yet{}",
            error
                .as_deref()
                .map(|error| format!(": {error}"))
                .unwrap_or_default()
        )));
    };
    let stale = snapshot.stale();
    Ok((
        [(
            header::CACHE_CONTROL,
            "public, max-age=5, stale-while-revalidate=10",
        )],
        Json(json!({
            "data": market_data(&snapshot),
            "meta": {
                "source": snapshot.source,
                "external": true,
                "stale": stale,
                "last_error": error,
                "last_attempt_at": last_attempt_at.map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true)),
                "warning": stale.then_some("the cached market snapshot is older than five minutes; the latest refresh failed"),
                "note": "External spot reference. On-chain QUBIC/USDT oracle points remain authoritative where available."
            }
        })),
    ))
}

/// Public JSON shape shared by `/v1/market` and the market stream.
pub fn market_data(snapshot: &MarketSnapshot) -> Value {
    let age_seconds = snapshot.age_seconds();
    json!({
        "pair": "QUBIC/USDT",
        "price_usd": snapshot.price_usd,
        "price_e8": snapshot.price_e8,
        "market_cap_usd": snapshot.market_cap_usd,
        "volume_24h_usd": snapshot.volume_24h_usd,
        "change_24h_percent": snapshot.change_24h_percent,
        "source": snapshot.source,
        "coin_id": snapshot.coin_id,
        "sources": snapshot.sources,
        "last_updated_at": snapshot.last_updated_at.map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true)),
        "fetched_at": snapshot.fetched_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        "age_seconds": age_seconds,
        "stale": snapshot.stale(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quote(source: &str, price_usd: f64) -> MarketQuote {
        MarketQuote {
            source: source.into(),
            pair: "QUBIC/USDT".into(),
            price_usd,
            volume_24h_usd: None,
            change_24h_percent: None,
        }
    }

    #[test]
    fn cache_merges_provider_turns_without_waiting_for_all_three() {
        let cache = MarketCache::default();
        let snapshot = cache
            .record_quote(quote("mexc", 3.4e-7), None, "qubic-network")
            .expect("one quote is enough");
        assert_eq!(snapshot.sources.len(), 1);
        assert_eq!(snapshot.price_e8, 34);

        let snapshot = cache
            .record_quote(quote("gate", 3.6e-7), None, "qubic-network")
            .expect("two quotes still produce a median");
        assert_eq!(snapshot.sources.len(), 2);
        assert_eq!(snapshot.price_e8, 35);

        let snapshot = cache
            .record_quote(quote("coingecko", 3.5e-7), None, "qubic-network")
            .expect("three quotes produce a median");
        assert_eq!(snapshot.sources.len(), 3);
        assert_eq!(snapshot.price_e8, 35);
        assert_eq!(snapshot.source, "mexc+gate+coingecko");
    }

    #[test]
    fn median_ignores_invalid_quotes() {
        assert_eq!(
            median_price([3.4e-7, 0.0, 3.6e-7, 3.5e-7].into_iter()).unwrap(),
            3.5e-7
        );
        assert_eq!(
            median_price([3.4e-7, 3.6e-7].into_iter()).unwrap(),
            3.5e-7,
            "an even number of quotes averages the two middle values"
        );
        assert_eq!(
            median_price([3.6e-7, 3.4e-7, 3.5e-7].into_iter()).unwrap(),
            3.5e-7
        );
        assert!(median_price([f64::NAN, 0.0].into_iter()).is_err());
    }

    #[test]
    fn price_scales_to_e8() {
        assert_eq!(price_e8(3.5e-7), Some(35));
        assert_eq!(price_e8(0.0), None);
        assert_eq!(price_e8(-1.0), None);
    }

    fn point(hour: u32, source: u32) -> PricePointRow {
        PricePointRow {
            hour,
            append_tick: 0,
            source_tick: 0,
            epoch: 0,
            flags: 0,
            price_e8: 35,
            source,
            synced: 1,
            bound: 0,
            series_digest: [0u8; 32],
            observed_at: Utc::now(),
            version: u64::from(hour),
        }
    }

    #[test]
    fn backfill_never_replaces_onchain_hours() {
        let mut rows = vec![
            point(10, PRICE_SOURCE_MARKET),
            point(11, PRICE_SOURCE_MARKET),
        ];
        let onchain: HashSet<u32> = [11].into_iter().collect();
        assert_eq!(retain_external_hours(&mut rows, &onchain), 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].hour, 10);

        let mut all_onchain = vec![point(12, PRICE_SOURCE_MARKET)];
        assert_eq!(
            retain_external_hours(&mut all_onchain, &[12].into_iter().collect()),
            1
        );
        assert!(all_onchain.is_empty());
    }
}
