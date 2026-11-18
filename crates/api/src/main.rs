use std::sync::Arc;

use api::serve;
use store::{ClickHouseStore, Store};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=info".into()),
        )
        .init();

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
    let clickhouse_db = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "glyph".into());
    let bind = std::env::var("BIND").unwrap_or_else(|_| "127.0.0.1:8080".into());

    let store: Arc<dyn Store> = Arc::new(ClickHouseStore::new(&clickhouse_url, &clickhouse_db)?);
    store.init().await?;

    serve(store, &bind).await?;
    Ok(())
}
