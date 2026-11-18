use std::sync::Arc;

use axum::Router;
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
    Router::new()
        .merge(routes::router())
        .route(
            "/openapi.json",
            axum::routing::get(|| async { axum::Json(openapi::spec()) }),
        )
        .route("/docs", axum::routing::get(routes::docs))
        .fallback(routes::not_found)
        .with_state(state)
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(PropagateRequestIdLayer::new(request_id.clone()))
        .layer(SetRequestIdLayer::new(request_id, MakeRequestUuid))
        .layer(TraceLayer::new_for_http())
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
