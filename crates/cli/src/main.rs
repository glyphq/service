use std::net::SocketAddr;

use anyhow::Context;
use clap::{Parser, Subcommand};
use node::{NodePool, PeerConfig};
use store::{ClickHouseStore, Store};

#[derive(Parser)]
#[command(name = "glyph", version, about = "Direct Qubic indexer and API")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Query the current head from a peer.
    Head {
        #[arg(long, default_value = "157.180.10.49:21841")]
        peer: SocketAddr,
    },
    /// Print peers exchanged by a seed peer.
    Peers {
        #[arg(long, default_value = "157.180.10.49:21841")]
        peer: SocketAddr,
    },
    /// Fetch one tick bundle and print a summary.
    Tick {
        tick: u32,
        #[arg(long, default_value = "157.180.10.49:21841")]
        peer: SocketAddr,
    },
    /// Run the indexer from the current epoch initial tick.
    Sync {
        #[arg(long, default_value = "157.180.10.49:21841")]
        peer: SocketAddr,
        #[arg(long)]
        from: Option<u32>,
    },
    /// Initialize ClickHouse tables.
    DbInit,
    /// Serve the HTTP API.
    Serve,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Head { peer } => {
            let pool = NodePool::new(vec![PeerConfig::new(peer)]);
            let head = pool.head().await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!(head))?
            );
        }
        Command::Peers { peer } => {
            let connection = node::Peer::connect(PeerConfig::new(peer)).await?;
            let peers: Vec<String> = connection
                .public_peers()
                .iter()
                .map(ToString::to_string)
                .collect();
            println!("{}", serde_json::to_string_pretty(&peers)?);
        }
        Command::Tick { tick, peer } => {
            let pool = NodePool::new(vec![PeerConfig::new(peer)]);
            let mut connection = pool.connect_best().await?;
            let summary = connection.bundle(tick).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "epoch": summary.head.epoch,
                    "tick": summary.tick.tick,
                    "transactions": summary.transactions.len(),
                    "votes": summary.votes.len(),
                    "logs": summary.logs.len(),
                }))?
            );
        }
        Command::Sync { peer, from } => {
            let pool = NodePool::new(vec![PeerConfig::new(peer)]);
            let head = pool.head().await?;
            println!(
                "syncing from {} to {}",
                from.unwrap_or(head.initial_tick),
                head.tick
            );
            anyhow::bail!("indexer sync loop is not wired yet");
        }
        Command::DbInit => {
            let url =
                std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
            let db = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "glyph".into());
            let store = ClickHouseStore::new(&url, &db)?;
            store.init().await.context("initialize ClickHouse")?;
            println!("initialized {db}");
        }
        Command::Serve => {
            anyhow::bail!("run the glyph-api binary for the HTTP server");
        }
    }
    Ok(())
}
