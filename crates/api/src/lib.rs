use axum::Router;
use std::sync::Arc;
use store::Store;

pub mod error;
pub mod routes;

pub use error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn Store>,
}

pub fn router(state: AppState) -> Router {
    routes::router(state)
}
