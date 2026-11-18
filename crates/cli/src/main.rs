use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use indexer::IndexerConfig;
use node::{NodePool, PeerConfig};
use store::{ClickHouseStore, Store};
use tokio_util::sync::CancellationToken;

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
        /// Peer addresses; repeat the flag to configure failover.
        #[arg(long = "peer", default_value = "157.180.10.49:21841")]
        peers: Vec<SocketAddr>,
        #[arg(long)]
        from: Option<u32>,
        /// How often to poll the head once caught up.
        #[arg(long, default_value_t = 750)]
        poll_ms: u64,
        /// Skip SchnorrQ signature checks (faster, but weaker verification).
        #[arg(long)]
        no_verify_signatures: bool,
    },
    /// Initialize ClickHouse tables.
    DbInit,
    /// Serve the HTTP API.
    Serve {
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
    },
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
        Command::Sync {
            peers,
            from,
            poll_ms,
            no_verify_signatures,
        } => {
            if peers.is_empty() {
                bail!("at least one --peer is required");
            }
            let config = IndexerConfig {
                peers: peers.iter().copied().map(PeerConfig::new).collect(),
                start_tick: from,
                poll_interval: std::time::Duration::from_millis(poll_ms),
                verify_signatures: !no_verify_signatures,
                ..IndexerConfig::default()
            };
            let url =
                std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
            let db = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "glyph".into());
            let store = ClickHouseStore::new(&url, &db)?;
            let mut indexer =
                indexer::Indexer::new(Arc::new(store), config).context("configure indexer")?;

            let cancel = CancellationToken::new();
            let signal = cancel.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                signal.cancel();
            });

            let verify = if no_verify_signatures {
                "signatures off"
            } else {
                "signatures on"
            };
            println!(
                "indexer started: {} peer(s), from {}, poll {}ms, {verify}, db {db}",
                peers.len(),
                from.map_or_else(|| "epoch initial tick".to_string(), |tick| tick.to_string()),
                poll_ms,
            );
            indexer.run(cancel).await?;
            println!("indexer stopped");
        }
        Command::DbInit => {
            let url =
                std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
            let db = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "glyph".into());
            let store = ClickHouseStore::new(&url, &db)?;
            store.init().await.context("initialize ClickHouse")?;
            println!("initialized {db}");
        }
        Command::Serve { bind } => {
            let url =
                std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
            let db = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "glyph".into());
            let store: Arc<dyn Store> = Arc::new(ClickHouseStore::new(&url, &db)?);
            store.init().await.context("initialize ClickHouse")?;
            api::serve(store, &bind).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_parses_repeatable_peers_and_flags() {
        let cli = Cli::try_parse_from([
            "glyph",
            "sync",
            "--peer",
            "1.2.3.4:21841",
            "--peer",
            "5.6.7.8:21841",
            "--from",
            "12345",
            "--poll-ms",
            "250",
            "--no-verify-signatures",
        ])
        .unwrap();

        match cli.command {
            Command::Sync {
                peers,
                from,
                poll_ms,
                no_verify_signatures,
            } => {
                assert_eq!(
                    peers,
                    vec![
                        "1.2.3.4:21841".parse().unwrap(),
                        "5.6.7.8:21841".parse().unwrap()
                    ]
                );
                assert_eq!(from, Some(12345));
                assert_eq!(poll_ms, 250);
                assert!(no_verify_signatures);
            }
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn sync_defaults_use_seed_peer_and_verify_signatures() {
        let cli = Cli::try_parse_from(["glyph", "sync"]).unwrap();
        match cli.command {
            Command::Sync {
                peers,
                from,
                poll_ms,
                no_verify_signatures,
            } => {
                assert_eq!(peers, vec!["157.180.10.49:21841".parse().unwrap()]);
                assert_eq!(from, None);
                assert_eq!(poll_ms, 750);
                assert!(!no_verify_signatures);
            }
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn serve_defaults_to_loopback_8080() {
        let cli = Cli::try_parse_from(["glyph", "serve"]).unwrap();
        match cli.command {
            Command::Serve { bind } => assert_eq!(bind, "127.0.0.1:8080"),
            _ => panic!("expected serve command"),
        }
    }
}
