use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, bail};
use archive::{FileArchive, S3Archive, SharedArchive};
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
        /// Discover peers and print per-peer health, score, and quarantine.
        #[arg(long)]
        health: bool,
    },
    /// Fetch one tick bundle, verify it, and print a summary.
    Tick {
        tick: u32,
        #[arg(long, default_value = "157.180.10.49:21841")]
        peer: SocketAddr,
        /// Skip SchnorrQ signature checks (faster, but only quorum-tier evidence).
        #[arg(long)]
        no_verify_signatures: bool,
    },
    /// Read the current QUBIC/USDT price from a peer.
    Price {
        #[arg(long, default_value = "157.180.10.49:21841")]
        peer: SocketAddr,
    },
    /// Run the indexer from the current epoch initial tick.
    Sync {
        /// Peer addresses; repeat the flag to configure failover. QUBIC_PEERS
        /// is a comma-separated fallback.
        #[arg(
            long = "peer",
            env = "QUBIC_PEERS",
            value_delimiter = ',',
            default_value = "157.180.10.49:21841"
        )]
        peers: Vec<SocketAddr>,
        #[arg(long)]
        from: Option<u32>,
        /// How often to poll the head once caught up.
        #[arg(long, default_value_t = 750)]
        poll_ms: u64,
        /// Skip SchnorrQ signature checks (faster, but weaker verification).
        #[arg(long)]
        no_verify_signatures: bool,
        /// Log progress every N indexed ticks.
        #[arg(long, default_value_t = 100)]
        progress_every: u64,
    },
    /// Initialize ClickHouse tables.
    DbInit,
    /// Serve the HTTP API.
    Serve {
        /// Bind address. Defaults to BIND, then 127.0.0.1:8080.
        #[arg(long, env = "BIND")]
        bind: Option<String>,
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
        Command::Peers { peer, health } => {
            if health {
                let mut pool = NodePool::new(vec![PeerConfig::new(peer)]);
                let discovered = pool.discover().await?;
                let discovered: Vec<String> = discovered.iter().map(ToString::to_string).collect();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "discovered": discovered,
                        "health": pool.health(),
                    }))?
                );
            } else {
                let connection = node::Peer::connect(PeerConfig::new(peer)).await?;
                let peers: Vec<String> = connection
                    .public_peers()
                    .iter()
                    .map(ToString::to_string)
                    .collect();
                println!("{}", serde_json::to_string_pretty(&peers)?);
            }
        }
        Command::Tick {
            tick,
            peer,
            no_verify_signatures,
        } => {
            let pool = NodePool::new(vec![PeerConfig::new(peer)]);
            let mut connection = pool.connect_best().await?;
            let bundle = connection.bundle(tick).await?;
            let verified = indexer::verify_bundle(bundle, !no_verify_signatures)
                .with_context(|| format!("verify tick {tick}"))?;
            let transactions = verified
                .transactions
                .iter()
                .map(|tx| {
                    serde_json::json!({
                        "hash": qubic::encode_tx_hash(&tx.hash()),
                        "source": qubic::PublicKey(tx.source).to_identity(),
                        "destination": qubic::PublicKey(tx.destination).to_identity(),
                        "amount": tx.amount,
                        "input_type": tx.input_type,
                    })
                })
                .collect::<Vec<_>>();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "epoch": verified.tick.epoch,
                    "tick": verified.tick.tick,
                    "timestamp": verified.tick.display_time(),
                    "computor_index": verified.tick.computor_index,
                    "verification": verified.verification,
                    "tick_digest": hex::encode(verified.tick_data.digest()),
                    "transactions": verified.transactions.len(),
                    "votes": verified.votes.len(),
                    "logs": verified.logs.len(),
                    "warnings": verified.warnings,
                    "transaction_list": transactions,
                }))?
            );
        }
        Command::Price { peer } => {
            let pool = NodePool::new(vec![PeerConfig::new(peer)]);
            match api::prices::read_price(&pool).await? {
                Some(price) => println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "pair": api::prices::PRICE_PAIR,
                        "price_e8": price.price_e8,
                        "price_usdt": api::prices::price_usdt(price.price_e8),
                        "source": price.source,
                        "source_name": api::prices::source_name(price.source),
                        "source_tick": price.source_tick,
                        "append_tick": price.append_tick,
                        "epoch": price.epoch,
                        "stale": price.stale,
                        "series_digest": hex::encode(price.digest),
                    }))?
                ),
                None => println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "pair": api::prices::PRICE_PAIR,
                        "available": false,
                        "detail": "no active QUBIC/USDT oracle subscription and no usable QPayhub price",
                    }))?
                ),
            }
        }
        Command::Sync {
            peers,
            from,
            poll_ms,
            no_verify_signatures,
            progress_every,
        } => {
            if peers.is_empty() {
                bail!("at least one --peer or QUBIC_PEERS entry is required");
            }
            if peers.len() > 64 {
                bail!("at most 64 peers may be configured");
            }
            let config = IndexerConfig {
                peers: peers.iter().copied().map(PeerConfig::new).collect(),
                start_tick: from,
                poll_interval: std::time::Duration::from_millis(poll_ms),
                verify_signatures: !no_verify_signatures,
                progress_every: progress_every.max(1),
                archive: archive_from_env()?,
                archive_fail_closed: std::env::var("ARCHIVE_FAIL_CLOSED")
                    .is_ok_and(|value| value == "true"),
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
            let bind = bind.unwrap_or_else(|| "127.0.0.1:8080".to_string());
            api::serve(store, &bind).await?;
        }
    }
    Ok(())
}

fn archive_from_env() -> anyhow::Result<Option<SharedArchive>> {
    if let Some(s3) = S3Archive::from_env().context("configure S3 raw archive")? {
        return Ok(Some(Arc::new(s3)));
    }
    if let Ok(dir) = std::env::var("ARCHIVE_DIR") {
        if !dir.trim().is_empty() {
            return Ok(Some(Arc::new(FileArchive::new(dir))));
        }
    }
    Ok(None)
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
                progress_every,
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
                assert_eq!(progress_every, 100);
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
                progress_every,
            } => {
                assert_eq!(peers, vec!["157.180.10.49:21841".parse().unwrap()]);
                assert_eq!(from, None);
                assert_eq!(poll_ms, 750);
                assert!(!no_verify_signatures);
                assert_eq!(progress_every, 100);
            }
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn tick_verifies_signatures_by_default() {
        let cli = Cli::try_parse_from(["glyph", "tick", "42"]).unwrap();
        match cli.command {
            Command::Tick {
                tick,
                no_verify_signatures,
                ..
            } => {
                assert_eq!(tick, 42);
                assert!(!no_verify_signatures);
            }
            _ => panic!("expected tick command"),
        }
    }

    #[test]
    fn serve_defaults_to_loopback_8080() {
        let cli = Cli::try_parse_from(["glyph", "serve"]).unwrap();
        match cli.command {
            Command::Serve { bind } => assert_eq!(bind, None),
            _ => panic!("expected serve command"),
        }
    }
}
