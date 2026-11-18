#![recursion_limit = "256"]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{Request, State as AxumState};
use axum::http::{HeaderValue, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use node::{NodePool, PeerConfig};
use store::Store;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::TraceLayer;

pub mod advanced;
pub mod dto;
pub mod error;
pub mod openapi;
pub mod prices;
pub mod routes;
pub mod status_page;
pub mod stream;
pub mod transactions_ext;
pub mod wallet;

pub use error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn Store>,
    pub hub: Option<Arc<stream::StreamHub>>,
    pub peers: Vec<PeerConfig>,
    /// Shared peer health and ordering for peer-backed routes.
    pub pool: NodePool,
    pub allow_broadcast: bool,
    pub archive: Option<archive::SharedArchive>,
    pub started_at: Instant,
    /// Per-process sample used to report observed tick rates without a
    /// background task or an extra database write.
    pub progress: Arc<ProgressSampler>,
}

impl AppState {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self {
            store,
            hub: None,
            peers: Vec::new(),
            pool: NodePool::new(Vec::new()),
            allow_broadcast: false,
            archive: None,
            started_at: Instant::now(),
            progress: Arc::new(ProgressSampler::default()),
        }
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
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
        let rates = previous.map_or_else(ProgressRates::default, |sample| {
            let elapsed = now.duration_since(sample.at).as_secs_f64();
            if elapsed < 1.0 {
                return ProgressRates::default();
            }
            ProgressRates {
                ingest: rate(sample.indexed, indexed, elapsed),
                network: rate(sample.network, network, elapsed),
            }
        });
        *previous = Some(ProgressSample {
            at: now,
            indexed,
            network,
        });
        rates
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
    let mut app = Router::new()
        .merge(routes::router())
        .merge(advanced::router())
        .merge(prices::router())
        .merge(stream::router())
        .merge(status_page::router())
        .merge(transactions_ext::router())
        .merge(wallet::router())
        .route(
            "/openapi.json",
            axum::routing::get(|| async { axum::Json(openapi::spec()) }),
        )
        .route("/docs", axum::routing::get(routes::scalar_docs))
        .route("/scalar.js", axum::routing::get(routes::scalar_js))
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
        .layer(PropagateRequestIdLayer::new(request_id.clone()))
        .layer(SetRequestIdLayer::new(request_id, MakeRequestUuid))
        .layer(TraceLayer::new_for_http())
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
    let pool = NodePool::new(peers.clone());
    let archive = archive_from_env();
    let price_interval = std::env::var("PRICE_POLL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(60));
    let price_task = if peers.is_empty() {
        None
    } else {
        Some(tokio::spawn(prices::run_poller(
            store.clone(),
            peers.clone(),
            Some(hub.clone()),
            price_interval,
            cancel.clone(),
        )))
    };
    let app = router(AppState {
        store,
        hub: Some(hub),
        peers,
        pool,
        allow_broadcast,
        archive,
        started_at: Instant::now(),
        progress: Arc::new(ProgressSampler::default()),
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
