#![recursion_limit = "512"]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{MatchedPath, Request, State as AxumState};
use axum::http::{HeaderValue, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use node::{NodePool, PeerConfig};
use store::Store;
use tower_http::LatencyUnit;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};

pub mod advanced;
pub mod dto;
pub mod error;
pub mod identity;
pub mod lifecycle;
pub mod market;
pub mod mempool;
pub mod metrics;
pub mod network;
pub mod openapi;
pub mod peer;
pub mod prices;
pub mod routes;
pub mod status_page;
pub mod stream;
pub mod transactions_ext;
pub mod wallet;
pub mod webhook;

pub use error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn Store>,
    pub hub: Option<Arc<stream::StreamHub>>,
    pub peers: Vec<PeerConfig>,
    /// Shared peer health and ordering for peer-backed routes.
    pub pool: NodePool,
    pub allow_broadcast: bool,
    /// Maximum number of peers a broadcast is sent to concurrently.
    pub broadcast_peers: usize,
    pub archive: Option<archive::SharedArchive>,
    pub started_at: Instant,
    /// Per-process sample used to report observed tick rates without a
    /// background task or an extra database write.
    pub progress: Arc<ProgressSampler>,
    /// Externally sourced QUBIC market snapshot, refreshed in the background.
    pub market: Arc<market::MarketCache>,
    /// Exchange endpoints and timeouts used by market reads.
    pub market_config: Arc<market::MarketConfig>,
    /// Operational mempool and webhook storage. `None` disables both.
    pub gateway: Option<Arc<store::gateway::GatewayStore>>,
    /// Background mempool tracker. `None` disables mempool routes.
    pub mempool: Option<Arc<mempool::Mempool>>,
    /// Signed webhook registration and delivery. `None` disables webhook routes.
    pub webhooks: Option<Arc<webhook::Webhooks>>,
    /// Short-lived cache for peer-backed reads.
    pub live_cache: Arc<peer::LiveCache>,
    /// Cached index-wide counts. `network_stats` runs several `FINAL` counts,
    /// so multiple dashboard reads within a few seconds should share one query.
    pub stats_cache: Arc<tokio::sync::Mutex<Option<(Instant, store::NetworkStats)>>>,
    /// Per-process request and cache counters exposed at `/metrics`.
    pub metrics: Arc<metrics::Metrics>,
}

impl AppState {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self {
            store,
            hub: None,
            peers: Vec::new(),
            pool: NodePool::new(Vec::new()),
            allow_broadcast: false,
            broadcast_peers: 8,
            archive: None,
            started_at: Instant::now(),
            progress: Arc::new(ProgressSampler::default()),
            market: Arc::new(market::MarketCache::default()),
            market_config: Arc::new(market::MarketConfig::from_env()),
            gateway: None,
            mempool: None,
            webhooks: None,
            live_cache: Arc::new(peer::LiveCache::default()),
            stats_cache: Arc::new(tokio::sync::Mutex::new(None)),
            metrics: Arc::new(metrics::Metrics::default()),
        }
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Returns index-wide counts, reusing a recent snapshot. The mutex is held
    /// across the query so a burst of dashboard refreshes performs one scan.
    pub async fn network_stats(&self) -> Result<store::NetworkStats, store::Error> {
        const TTL: Duration = Duration::from_secs(5);
        let mut cache = self.stats_cache.lock().await;
        if let Some((stored_at, stats)) = cache.as_ref() {
            if stored_at.elapsed() < TTL {
                return Ok(stats.clone());
            }
        }
        let stats = self.store.network_stats().await?;
        *cache = Some((Instant::now(), stats.clone()));
        Ok(stats)
    }
}

#[derive(Debug, Default)]
pub struct ProgressSampler {
    previous: Mutex<Option<ProgressSample>>,
}

#[derive(Debug, Clone, Copy)]
struct ProgressSample {
    at: Instant,
    indexed: Option<u32>,
    network: Option<u32>,
}

/// Tick rates measured between two status requests.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct ProgressRates {
    pub ingest: Option<f64>,
    pub network: Option<f64>,
}

impl ProgressSampler {
    /// Records the current indexed and network ticks and returns the rate since
    /// the previous sample. Samples closer than one second return `None` so
    /// dashboard refreshes do not produce spiky numbers.
    pub fn observe(&self, indexed: Option<u32>, network: Option<u32>) -> ProgressRates {
        let now = Instant::now();
        let mut previous = self.previous.lock().expect("progress sampler lock");
        if let Some(sample) = *previous {
            let elapsed = now.duration_since(sample.at).as_secs_f64();
            if elapsed < 1.0 {
                // Do not replace the baseline: frequent pollers would keep
                // resetting the window and every rate would read as zero.
                return ProgressRates::default();
            }
            let rates = ProgressRates {
                ingest: rate(sample.indexed, indexed, elapsed),
                network: rate(sample.network, network, elapsed),
            };
            *previous = Some(ProgressSample {
                at: now,
                indexed,
                network,
            });
            return rates;
        }
        *previous = Some(ProgressSample {
            at: now,
            indexed,
            network,
        });
        ProgressRates::default()
    }
}

fn rate(previous: Option<u32>, current: Option<u32>, elapsed: f64) -> Option<f64> {
    let previous = previous?;
    let current = current?;
    current
        .checked_sub(previous)
        .map(|delta| f64::from(delta) / elapsed)
}

pub fn router(state: AppState) -> Router {
    let request_id = axum::http::HeaderName::from_static("x-request-id");
    let request_metrics = state.metrics.clone();
    let mut app = Router::new()
        .merge(routes::router())
        .merge(identity::router())
        .merge(advanced::router())
        .merge(network::router())
        .merge(lifecycle::router())
        .merge(market::router())
        .merge(prices::router())
        .merge(stream::router())
        .merge(status_page::router())
        .merge(transactions_ext::router())
        .merge(wallet::router())
        .merge(webhook::router())
        .route(
            "/openapi.json",
            axum::routing::get(|| async { axum::Json(openapi::spec()) }),
        )
        .route("/docs", axum::routing::get(routes::scalar_docs))
        .route("/scalar.js", axum::routing::get(routes::scalar_js))
        .route("/logo.png", axum::routing::get(routes::logo_dark))
        .route("/logo-light.png", axum::routing::get(routes::logo_light))
        .route("/favicon.png", axum::routing::get(routes::favicon))
        .route(
            "/fonts/space-grotesk.woff2",
            axum::routing::get(routes::space_grotesk),
        )
        .route(
            "/fonts/space-grotesk-ofl.txt",
            axum::routing::get(routes::space_grotesk_license),
        )
        .route("/metrics", axum::routing::get(metrics_handler))
        .fallback(routes::not_found)
        .with_state(state);
    if let Some(limiter) = RateLimiter::from_env() {
        app = app.layer(axum::middleware::from_fn_with_state(
            limiter,
            enforce_rate_limit,
        ));
    }
    app.layer(CompressionLayer::new())
        .layer(cors_layer())
        .layer(axum::middleware::from_fn_with_state(
            request_metrics,
            track_metrics,
        ))
        .layer(PropagateRequestIdLayer::new(request_id.clone()))
        .layer(SetRequestIdLayer::new(request_id, MakeRequestUuid))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(tracing::Level::INFO))
                .on_response(
                    DefaultOnResponse::new()
                        .level(tracing::Level::INFO)
                        .latency_unit(LatencyUnit::Millis),
                ),
        )
}

async fn metrics_handler(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    let (hits, misses) = state.live_cache.stats();
    (
        [
            (
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            ),
            (header::CACHE_CONTROL, "no-store"),
        ],
        state.metrics.render(hits, misses),
    )
}

async fn track_metrics(
    AxumState(metrics): AxumState<Arc<metrics::Metrics>>,
    request: Request,
    next: Next,
) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let method = request.method().as_str().to_owned();
    let started = Instant::now();
    metrics.request_started();
    let response = next.run(request).await;
    metrics.request_finished(
        &route,
        &method,
        response.status().as_u16(),
        started.elapsed(),
    );
    response
}

/// Permissive by default. Set `CORS_ALLOWED_ORIGINS` to a comma-separated
/// list to restrict browser clients without a code change.
fn cors_layer() -> CorsLayer {
    let Ok(value) = std::env::var("CORS_ALLOWED_ORIGINS") else {
        return CorsLayer::permissive();
    };
    let origins: Vec<HeaderValue> = value
        .split(',')
        .map(str::trim)
        .filter(|origin| !origin.is_empty())
        .filter_map(|origin| origin.parse().ok())
        .collect();
    if origins.is_empty() {
        CorsLayer::permissive()
    } else {
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(tower_http::cors::Any)
            .allow_headers(tower_http::cors::Any)
    }
}

/// Fixed-window, in-process rate-limit hook. It is disabled unless
/// `RATE_LIMIT_PER_MINUTE` is set. A shared store (Redis, ClickHouse) can
/// replace the bucket map later without changing the route layer.
#[derive(Clone)]
struct RateLimiter {
    per_minute: u32,
    buckets: Arc<Mutex<HashMap<String, Bucket>>>,
}

struct Bucket {
    window: Instant,
    used: u32,
}

impl RateLimiter {
    fn from_env() -> Option<Self> {
        let per_minute = std::env::var("RATE_LIMIT_PER_MINUTE")
            .ok()?
            .parse::<u32>()
            .ok()?;
        (per_minute > 0).then(|| Self {
            per_minute,
            buckets: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

async fn enforce_rate_limit(
    AxumState(limiter): AxumState<RateLimiter>,
    request: Request,
    next: Next,
) -> Response {
    let key = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            request
                .headers()
                .get("x-real-ip")
                .and_then(|value| value.to_str().ok())
        })
        .unwrap_or("anonymous")
        .to_owned();

    let now = Instant::now();
    let limited = {
        let mut buckets = limiter
            .buckets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let bucket = buckets.entry(key).or_insert(Bucket {
            window: now,
            used: 0,
        });
        if now.duration_since(bucket.window) >= Duration::from_secs(60) {
            bucket.window = now;
            bucket.used = 0;
        }
        if bucket.used >= limiter.per_minute {
            true
        } else {
            bucket.used += 1;
            false
        }
    };
    if limited {
        let mut response =
            ApiError::too_many_requests("request rate limit exceeded").into_response();
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("60"));
        return response;
    }
    next.run(request).await
}

/// Binds and serves the API until SIGINT or SIGTERM.
pub async fn serve(store: Arc<dyn Store>, bind: &str) -> anyhow::Result<()> {
    let hub = Arc::new(stream::StreamHub::new());
    let cancel = tokio_util::sync::CancellationToken::new();
    let poll = std::env::var("STREAM_POLL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(1000));
    let hub_task = tokio::spawn(hub.clone().run(store.clone(), poll, cancel.clone()));
    let peers: Vec<PeerConfig> = std::env::var("QUBIC_PEERS")
        .or_else(|_| std::env::var("QUBIC_PEER"))
        .map(|value| {
            value
                .split(',')
                .filter_map(|item| item.trim().parse().ok())
                .map(PeerConfig::new)
                .collect()
        })
        .unwrap_or_default();
    let allow_broadcast = std::env::var("ALLOW_BROADCAST").is_ok_and(|value| value == "true");
    let broadcast_peers = std::env::var("BROADCAST_PEERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(8)
        .clamp(1, 8);
    let pool = NodePool::new(peers.clone());
    let archive = archive_from_env();
    let price_interval = std::env::var("PRICE_POLL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(60));
    let market_cache = Arc::new(market::MarketCache::default());
    let market_config = market::MarketConfig::from_env();
    let market_task = if std::env::var("MARKET_ENABLED").is_ok_and(|value| value == "false") {
        None
    } else {
        Some(tokio::spawn(market::run_poller(
            market_cache.clone(),
            store.clone(),
            market_config.clone(),
            Some(hub.clone()),
            cancel.clone(),
        )))
    };
    let price_task = if peers.is_empty() {
        None
    } else {
        Some(tokio::spawn(prices::run_poller(
            store.clone(),
            peers.clone(),
            Some(hub.clone()),
            market_cache.clone(),
            price_interval,
            cancel.clone(),
        )))
    };
    let mempool_config = mempool::MempoolConfig::from_env();
    let mut webhook_config = webhook::WebhookConfig::from_env();
    webhook_config.mempool_max_attempts = mempool_config.max_attempts;
    // Operational state is optional: a deployment without ClickHouse gateway
    // storage still serves the indexed read API. `MEMPOOL_ENABLED=false`
    // skips construction entirely.
    let (gateway, mempool, webhooks, mempool_tasks) = if mempool_config.enabled {
        match store::gateway::GatewayStore::from_env() {
            Ok(gateway) => {
                let gateway = Arc::new(gateway);
                match gateway.init().await {
                    Ok(()) => {
                        let webhooks = Arc::new(webhook::Webhooks::new(
                            gateway.clone(),
                            webhook_config,
                            cancel.clone(),
                        ));
                        let mempool = Arc::new(mempool::Mempool::new(
                            store.clone(),
                            pool.clone(),
                            gateway.clone(),
                            webhooks.clone(),
                            mempool_config,
                            cancel.clone(),
                        ));
                        let webhook_task = tokio::spawn(webhooks.clone().run());
                        let mempool_task = tokio::spawn(mempool.clone().run());
                        (
                            Some(gateway),
                            Some(mempool),
                            Some(webhooks),
                            vec![webhook_task, mempool_task],
                        )
                    }
                    Err(error) => {
                        tracing::warn!(%error, "mempool and webhook storage is unavailable");
                        (None, None, None, Vec::new())
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "mempool and webhook storage is unavailable");
                (None, None, None, Vec::new())
            }
        }
    } else {
        (None, None, None, Vec::new())
    };
    let app = router(AppState {
        store,
        hub: Some(hub),
        peers,
        pool,
        allow_broadcast,
        broadcast_peers,
        archive,
        started_at: Instant::now(),
        progress: Arc::new(ProgressSampler::default()),
        market: market_cache,
        market_config: Arc::new(market_config),
        gateway,
        mempool,
        webhooks,
        live_cache: Arc::new(peer::LiveCache::default()),
        stats_cache: Arc::new(tokio::sync::Mutex::new(None)),
        metrics: Arc::new(metrics::Metrics::default()),
    });
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "glyph api listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    cancel.cancel();
    let _ = hub_task.await;
    if let Some(task) = price_task {
        let _ = task.await;
    }
    if let Some(task) = market_task {
        let _ = task.await;
    }
    for task in mempool_tasks {
        let _ = task.await;
    }
    Ok(())
}

fn archive_from_env() -> Option<archive::SharedArchive> {
    match archive::S3Archive::from_env() {
        Ok(Some(s3)) => return Some(Arc::new(s3)),
        Ok(None) => {}
        Err(error) => tracing::warn!(%error, "S3 raw archive is misconfigured"),
    }
    std::env::var("ARCHIVE_DIR")
        .ok()
        .filter(|dir| !dir.trim().is_empty())
        .map(|dir| Arc::new(archive::FileArchive::new(dir)) as archive::SharedArchive)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
