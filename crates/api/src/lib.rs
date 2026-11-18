use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{Request, State as AxumState};
use axum::http::{HeaderValue, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use store::Store;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::trace::TraceLayer;

pub mod dto;
pub mod error;
pub mod openapi;
pub mod routes;

pub use error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn Store>,
}

pub fn router(state: AppState) -> Router {
    let request_id = axum::http::HeaderName::from_static("x-request-id");
    let mut app = Router::new()
        .merge(routes::router())
        .route(
            "/openapi.json",
            axum::routing::get(|| async { axum::Json(openapi::spec()) }),
        )
        .route("/docs", axum::routing::get(routes::docs))
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
    let app = router(AppState { store });
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "glyph api listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
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
