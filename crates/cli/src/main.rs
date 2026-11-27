use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, bail};
use archive::{FileArchive, S3Archive, SharedArchive};
use clap::{Parser, Subcommand};
use indexer::IndexerConfig;
use node::{Head, NodePool, PeerConfig, TickBundle};
use qubic::messages::{Computors, LogRanges, Tick, TickData};
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
        /// Verify SchnorrQ signatures locally (slower).
        #[arg(long)]
        verify_signatures: bool,
        /// Skip SchnorrQ signature checks. Kept for compatibility; this is
        /// already the default.
        #[arg(long)]
        no_verify_signatures: bool,
    },
    /// Inspect the raw log proof one peer returns for a tick.
    Logs {
        tick: u32,
        #[arg(long, default_value = "157.180.10.49:21841")]
        peer: SocketAddr,
    },
    /// Read QUBIC/USDT from a peer, falling back to exchange prices.
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
        /// Verify SchnorrQ signatures locally (slower).
        #[arg(long)]
        verify_signatures: bool,
        /// Skip SchnorrQ signature checks. Kept for compatibility; this is
        /// already the default.
        #[arg(long)]
        no_verify_signatures: bool,
        /// Log progress every N indexed ticks.
        #[arg(long, default_value_t = 100)]
        progress_every: u64,
        /// Skip a tick after this many attempts when every peer reports that
        /// its tick data does not exist.
        #[arg(long, env = "INDEXER_MAX_TICK_ATTEMPTS", default_value_t = 4)]
        max_tick_attempts: u32,
    },
    /// Fetch and verify ticks, publishing them to NATS JetStream.
    Fetcher {
        #[arg(
            long = "peer",
            env = "QUBIC_PEERS",
            value_delimiter = ',',
            default_value = "157.180.10.49:21841"
        )]
        peers: Vec<SocketAddr>,
        #[arg(long)]
        from: Option<u32>,
        #[arg(long, default_value_t = 250)]
        poll_ms: u64,
        #[arg(long)]
        verify_signatures: bool,
        #[arg(long)]
        no_verify_signatures: bool,
        #[arg(long, default_value_t = 100)]
        progress_every: u64,
        #[arg(long, env = "INDEXER_MAX_TICK_ATTEMPTS", default_value_t = 4)]
        max_tick_attempts: u32,
    },
    /// Consume verified ticks from NATS JetStream and write them to ClickHouse.
    Writer {
        #[arg(long, env = "WRITER_BATCH", default_value_t = 64)]
        batch: usize,
    },
    /// Initialize ClickHouse tables.
    DbInit,
    /// Drop partitions and metadata for old epochs.
    Prune {
        /// Number of newest epochs to retain.
        #[arg(long, default_value_t = 54)]
        keep: u16,
        /// Print the epochs that would be dropped without modifying ClickHouse.
        #[arg(long)]
        dry_run: bool,
    },
    /// Recompute identity QU totals from the stored events table.
    RebuildIdentities {
        /// Snapshot version. Defaults to the current head tick.
        #[arg(long)]
        version: Option<u64>,
        /// Print the aggregate without replacing the identities table.
        #[arg(long)]
        dry_run: bool,
    },
    /// Write a manifest of archived tick objects, optionally verifying every bundle.
    ArchiveManifest {
        #[arg(long)]
        epoch: u16,
        #[arg(long)]
        from: u32,
        #[arg(long)]
        to: u32,
        /// Read and decode every bundle instead of trusting object metadata.
        #[arg(long)]
        verify: bool,
        /// Write the manifest JSON here instead of stdout.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Replay archived ticks into ClickHouse through the normal indexer path.
    ArchiveReplay {
        #[arg(long)]
        epoch: u16,
        #[arg(long)]
        from: u32,
        #[arg(long)]
        to: u32,
        /// Verify bundles without writing derived rows.
        #[arg(long)]
        dry_run: bool,
    },
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
            verify_signatures,
            no_verify_signatures,
        } => {
            let pool = NodePool::new(vec![PeerConfig::new(peer)]);
            let mut connection = pool.connect_best().await?;
            let bundle = connection.bundle(tick).await?;
            let verify = verify_signatures && !no_verify_signatures;
            let verified = indexer::verify_bundle(bundle, verify)
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
        Command::Logs { tick, peer } => {
            let mut connection = node::Peer::connect(PeerConfig::new(peer)).await?;
            let tick_data = connection.tick_data(tick).await?;
            let protocol = connection
                .log_protocol_name(&tick_data)
                .await
                .unwrap_or("unavailable");
            let fetched = connection.complete_logs(&tick_data).await?;
            let slots = fetched
                .ranges
                .from_log_id
                .iter()
                .zip(fetched.ranges.length.iter())
                .enumerate()
                .filter(|(_, (from, length))| **from >= 0 || **length >= 0)
                .map(|(slot, (from, length))| {
                    serde_json::json!({
                        "slot": slot,
                        "from": from,
                        "length": length,
                    })
                })
                .collect::<Vec<_>>();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "tick": tick,
                    "peer": peer.to_string(),
                    "protocol": protocol,
                    "complete": fetched.complete,
                    "ranges_count": fetched.ranges.count(),
                    "logs": fetched.logs.len(),
                    "slots": slots,
                    "warnings": fetched.warnings,
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
                None => match api::market::fetch_once(&api::market::MarketConfig::from_env()).await
                {
                    Ok(snapshot) => println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "pair": api::prices::PRICE_PAIR,
                            "available": true,
                            "onchain": false,
                            "price_e8": snapshot.price_e8,
                            "price_usdt": api::prices::price_usdt(snapshot.price_e8),
                            "source": api::prices::PRICE_SOURCE_MARKET,
                            "source_name": api::prices::source_name(api::prices::PRICE_SOURCE_MARKET),
                            "sources": snapshot.sources,
                            "fetched_at": snapshot.fetched_at,
                            "stale": snapshot.stale(),
                            "detail": "no active QUBIC/USDT oracle subscription and no usable QPayhub price; using the external exchange consensus",
                        }))?
                    ),
                    Err(error) => println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "pair": api::prices::PRICE_PAIR,
                            "available": false,
                            "detail": format!("no on-chain or external QUBIC/USDT price is available: {error}"),
                        }))?
                    ),
                },
            }
        }
        Command::Sync {
            peers,
            from,
            poll_ms,
            verify_signatures,
            no_verify_signatures,
            progress_every,
            max_tick_attempts,
        } => {
            if peers.is_empty() {
                bail!("at least one --peer or QUBIC_PEERS entry is required");
            }
            if peers.len() > 64 {
                bail!("at most 64 peers may be configured");
            }
            let verify = verify_signatures && !no_verify_signatures;
            let config = IndexerConfig {
                peers: peers
                    .iter()
                    .copied()
                    .map(|addr| {
                        PeerConfig::new(addr)
                            .with_retries(1)
                            .with_timeout(std::time::Duration::from_secs(4))
                    })
                    .collect(),
                start_tick: from,
                poll_interval: std::time::Duration::from_millis(poll_ms),
                verify_signatures: verify,
                progress_every: progress_every.max(1),
                max_tick_attempts: max_tick_attempts.max(1),
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

            let verify = if verify {
                "signatures on"
            } else {
                "signatures off"
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
        Command::Fetcher {
            peers,
            from,
            poll_ms,
            verify_signatures,
            no_verify_signatures,
            progress_every,
            max_tick_attempts,
        } => {
            if peers.is_empty() {
                bail!("at least one --peer or QUBIC_PEERS entry is required");
            }
            let verify = verify_signatures && !no_verify_signatures;
            let config = IndexerConfig {
                peers: peers
                    .iter()
                    .copied()
                    .map(|addr| {
                        PeerConfig::new(addr)
                            .with_retries(1)
                            .with_timeout(std::time::Duration::from_secs(4))
                    })
                    .collect(),
                start_tick: from,
                poll_interval: std::time::Duration::from_millis(poll_ms),
                verify_signatures: verify,
                progress_every: progress_every.max(1),
                max_tick_attempts: max_tick_attempts.max(1),
                archive: archive_from_env()?,
                archive_fail_closed: std::env::var("ARCHIVE_FAIL_CLOSED")
                    .is_ok_and(|value| value == "true"),
            };
            let url =
                std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
            let db = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "glyph".into());
            let store = ClickHouseStore::new(&url, &db)?;
            let bus = connect_bus().await?;
            let mut indexer = indexer::Indexer::new(Arc::new(store), config)
                .context("configure fetcher")?
                .with_sink(Arc::new(NatsSink { bus }));
            let cancel = CancellationToken::new();
            let signal = cancel.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                signal.cancel();
            });
            println!(
                "fetcher started: {} peer(s), db {db}, NATS {}",
                peers.len(),
                std::env::var("NATS_URL").unwrap_or_default()
            );
            indexer.run(cancel).await?;
            println!("fetcher stopped");
        }
        Command::Writer { batch } => {
            let url =
                std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
            let db = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "glyph".into());
            let store: Arc<dyn Store> = Arc::new(ClickHouseStore::new(&url, &db)?);
            store.init().await.context("initialize ClickHouse")?;
            let bus = connect_bus().await?;
            let consumer = bus.writer_consumer().await?;
            let mut messages = consumer.messages().await?;
            let batch = batch.clamp(1, 1024);
            let cancel = CancellationToken::new();
            let signal = cancel.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                signal.cancel();
            });
            println!("writer started: db {db}, batch {batch}");
            let mut written = 0u64;
            loop {
                if cancel.is_cancelled() {
                    break;
                }
                let mut pending = Vec::with_capacity(batch);
                let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(100);
                while pending.len() < batch {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match tokio::time::timeout(remaining, messages.next()).await {
                        Ok(Some(Ok(message))) => pending.push(message),
                        Ok(Some(Err(error))) => {
                            tracing::warn!(%error, "NATS writer message error");
                            break;
                        }
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }
                if pending.is_empty() {
                    continue;
                }
                pending.sort_by_key(|message| message.batch.tick);
                let batches = pending
                    .iter()
                    .map(|message| message.batch.clone())
                    .collect::<Vec<_>>();
                let events = batches
                    .iter()
                    .map(bus::TickEvent::from_batch)
                    .collect::<Vec<_>>();
                match store.write_ticks(&batches).await {
                    Ok(outcomes) => {
                        written += outcomes.len() as u64;
                        for (message, event) in pending.into_iter().zip(events) {
                            if let Err(error) = message.ack().await {
                                tracing::warn!(%error, "NATS writer ack failed");
                            }
                            if let Err(error) = bus.publish_committed(&event).await {
                                tracing::debug!(%error, "committed event publish failed");
                            }
                        }
                        if written % 1_000 < batches.len() as u64 {
                            tracing::info!(
                                tick = batches.last().map(|batch| batch.tick).unwrap_or_default(),
                                written,
                                "writer committed ticks"
                            );
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error, "ClickHouse batch write failed; retrying");
                        for message in pending {
                            let _ = message.nak().await;
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
            println!("writer stopped after {written} ticks");
        }
        Command::DbInit => {
            let url =
                std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
            let db = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "glyph".into());
            let store = ClickHouseStore::new(&url, &db)?;
            store.init().await.context("initialize ClickHouse")?;
            println!("initialized {db}");
        }
        Command::Prune { keep, dry_run } => {
            let url =
                std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
            let db = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "glyph".into());
            let store = ClickHouseStore::new(&url, &db)?;
            if dry_run {
                let epochs = store.prunable_epochs(keep).await.context("plan prune")?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "keep": keep,
                        "dry_run": true,
                        "epochs": epochs,
                    }))?
                );
            } else {
                let epochs = store.prune_epochs(keep).await.context("prune epochs")?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "keep": keep,
                        "dry_run": false,
                        "epochs": epochs,
                    }))?
                );
            }
        }
        Command::RebuildIdentities { version, dry_run } => {
            let url =
                std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
            let db = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "glyph".into());
            let store = ClickHouseStore::new(&url, &db)?;
            let version = match version {
                Some(version) => version,
                None => store
                    .head()
                    .await
                    .context("read head")?
                    .map_or(0, |head| u64::from(head.tick)),
            };
            let totals = store
                .identity_totals_from_events()
                .await
                .context("aggregate identity events")?;
            let zero = qubic::PublicKey([0u8; 32]).to_identity();
            let mut rows = Vec::with_capacity(totals.len());
            let mut skipped = 0u64;
            for total in totals {
                if total.identity == zero {
                    continue;
                }
                let Ok(key) = qubic::PublicKey::from_identity(&total.identity) else {
                    skipped += 1;
                    continue;
                };
                rows.push(store::IdentityRow {
                    identity: total.identity,
                    public_key: key.to_hex(),
                    balance: total.incoming_amount.saturating_sub(total.outgoing_amount),
                    incoming_amount: total.incoming_amount,
                    outgoing_amount: total.outgoing_amount,
                    transfer_count: total.transfer_count,
                    first_seen_tick: total.first_seen_tick,
                    last_seen_tick: total.last_seen_tick,
                });
            }
            if !dry_run {
                store
                    .replace_identities(&rows, version)
                    .await
                    .context("replace identities")?;
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "identities": rows.len(),
                    "skipped": skipped,
                    "version": version,
                    "dry_run": dry_run,
                }))?
            );
        }
        Command::ArchiveManifest {
            epoch,
            from,
            to,
            verify,
            output,
        } => {
            if to < from {
                bail!("--to must be greater than or equal to --from");
            }
            let archive = archive_from_env()?
                .context("no raw archive configured; set S3_BUCKET or ARCHIVE_DIR")?;
            let mut objects = Vec::new();
            let mut missing = Vec::new();
            let mut total_bytes = 0u64;
            for tick in from..=to {
                let key = archive::object_key(epoch, tick);
                let Some(head) = archive.head(&key).await? else {
                    missing.push(tick);
                    continue;
                };
                total_bytes = total_bytes.saturating_add(head.size);
                let mut object = serde_json::json!({
                    "tick": tick,
                    "key": key,
                    "size": head.size,
                    "last_modified": head.last_modified,
                });
                if verify {
                    let bytes = archive.get(&key).await?;
                    let bundle = archive::RawBundle::decode(&bytes)
                        .with_context(|| format!("decode archive bundle {key}"))?;
                    if bundle.epoch != epoch || bundle.tick != tick {
                        bail!(
                            "archive bundle {key} identifies epoch {} tick {}",
                            bundle.epoch,
                            bundle.tick
                        );
                    }
                    object["digest"] =
                        serde_json::json!(archive::digest::hex64(archive::xxh64(&bytes, 0)));
                    object["sections"] = serde_json::json!({
                        "tick_header": bundle.tick_header.len(),
                        "tick_data": bundle.tick_data.len(),
                        "votes": bundle.votes.len(),
                        "transactions": bundle.transactions.len(),
                        "log_ranges": bundle.log_ranges.len(),
                        "logs": bundle.logs.len(),
                        "computors": bundle.computors.len(),
                    });
                }
                objects.push(object);
            }
            let manifest = serde_json::json!({
                "epoch": epoch,
                "from": from,
                "to": to,
                "verified": verify,
                "summary": {
                    "objects": objects.len(),
                    "missing": missing.len(),
                    "bytes": total_bytes,
                },
                "objects": objects,
                "missing": missing,
                "generated_at": chrono::Utc::now(),
            });
            let rendered = serde_json::to_string_pretty(&manifest)?;
            match output {
                Some(path) => {
                    std::fs::write(&path, rendered)
                        .with_context(|| format!("write manifest {}", path.display()))?;
                    println!("manifest written to {}", path.display());
                }
                None => println!("{rendered}"),
            }
        }
        Command::ArchiveReplay {
            epoch,
            from,
            to,
            dry_run,
        } => {
            if to < from {
                bail!("--to must be greater than or equal to --from");
            }
            let archive = archive_from_env()?
                .context("no raw archive configured; set S3_BUCKET or ARCHIVE_DIR")?;
            let url =
                std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
            let db = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "glyph".into());
            let store = Arc::new(ClickHouseStore::new(&url, &db)?);
            if !dry_run {
                store.init().await.context("initialize ClickHouse")?;
            }
            let config = IndexerConfig {
                peers: vec![PeerConfig::new("127.0.0.1:1".parse()?)],
                start_tick: Some(from),
                poll_interval: std::time::Duration::from_millis(250),
                verify_signatures: false,
                progress_every: 1,
                max_tick_attempts: 1,
                archive: Some(archive.clone()),
                archive_fail_closed: false,
            };
            let mut indexer =
                indexer::Indexer::new(store.clone(), config).context("configure archive replay")?;
            let mut replayed = 0;
            let mut transactions = 0;
            let mut events = 0;
            for tick in from..=to {
                let Some(bundle) = archive::get_bundle(archive.as_ref(), epoch, tick).await? else {
                    bail!("archive bundle for epoch {epoch} tick {tick} is missing");
                };
                let verified = verify_archived_bundle(archive.as_ref(), &bundle, epoch, tick)
                    .await
                    .with_context(|| format!("verify archived tick {tick}"))?;
                if dry_run {
                    println!(
                        "tick {tick}: {} transaction(s), {} event(s), {} vote(s)",
                        verified.transactions.len(),
                        verified.logs.len(),
                        verified.votes.len()
                    );
                } else {
                    let batch = indexer.write_verified(&verified).await?;
                    transactions += batch.transactions.len();
                    events += batch.events.len();
                }
                replayed += 1;
            }
            println!(
                "{}: {replayed} tick(s), {transactions} transaction(s), {events} event(s), db {db}",
                if dry_run {
                    "archive dry run"
                } else {
                    "archive replay"
                }
            );
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

async fn connect_bus() -> anyhow::Result<bus::Bus> {
    let config =
        bus::BusConfig::from_env().context("NATS_URL must be set for fetcher/writer roles")?;
    let bus = bus::Bus::connect(config).await.context("connect NATS")?;
    bus.ensure_stream()
        .await
        .context("ensure JetStream stream")?;
    Ok(bus)
}

struct NatsSink {
    bus: bus::Bus,
}

#[async_trait::async_trait]
impl indexer::BatchSink for NatsSink {
    async fn publish(&self, batch: &store::TickBatch) -> indexer::Result<()> {
        self.bus
            .publish_tick(batch)
            .await
            .map_err(|error| indexer::Error::Config(format!("NATS publish failed: {error}")))
    }
}

fn archive_from_env() -> anyhow::Result<Option<SharedArchive>> {
    if std::env::var("ARCHIVE_ENABLED")
        .is_ok_and(|value| matches!(value.as_str(), "0" | "false" | "no" | "off"))
    {
        return Ok(None);
    }
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

async fn verify_archived_bundle(
    archive: &dyn archive::RawArchive,
    bundle: &archive::RawBundle,
    epoch: u16,
    tick: u32,
) -> anyhow::Result<indexer::VerifiedTick> {
    if bundle.epoch != epoch || bundle.tick != tick {
        bail!(
            "bundle identifies epoch {} tick {}, expected epoch {epoch} tick {tick}",
            bundle.epoch,
            bundle.tick
        );
    }
    if bundle.votes.is_empty() {
        bail!("archive bundle has no votes; re-archive with ARCHIVE_VOTES=true before replaying");
    }
    if bundle.votes.len() % qubic::messages::TICK_SIZE != 0 {
        bail!("archived vote section is not a whole number of votes");
    }
    let tick_header = Tick::decode(&bundle.tick_header).context("decode tick header")?;
    let tick_data = TickData::decode(&bundle.tick_data).context("decode tick data")?;
    let computors = if bundle.computors.is_empty() {
        let sidecar = archive::get_computors(archive, epoch)
            .await
            .context("read computor sidecar")?
            .context("computor sidecar is missing")?;
        Computors::decode(&sidecar).context("decode computor sidecar")?
    } else {
        Computors::decode(&bundle.computors).context("decode archived computors")?
    };
    if computors.epoch != epoch {
        bail!(
            "computor set is for epoch {}, expected {epoch}",
            computors.epoch
        );
    }
    let votes = bundle
        .votes
        .chunks_exact(qubic::messages::TICK_SIZE)
        .map(Tick::decode)
        .collect::<Result<Vec<_>, _>>()
        .context("decode archived votes")?;
    let log_ranges = LogRanges::decode(&bundle.log_ranges).context("decode log ranges")?;
    let logs = qubic::LogEvent::decode_many(&bundle.logs).context("decode logs")?;
    let transactions = decode_archived_transactions(&bundle.transactions)?;
    let reconstructed = TickBundle {
        head: Head {
            epoch,
            tick,
            initial_tick: tick,
            tick_duration: 0,
        },
        computors,
        tick: tick_header,
        tick_data,
        votes,
        transactions,
        log_ranges,
        logs,
        logs_complete: true,
        warnings: Vec::new(),
    };
    indexer::verify_bundle(reconstructed, false).map_err(Into::into)
}

fn decode_archived_transactions(
    mut payload: &[u8],
) -> anyhow::Result<Vec<qubic::messages::Transaction>> {
    let mut transactions = Vec::new();
    while !payload.is_empty() {
        if payload.len() < qubic::messages::Transaction::BASE_SIZE + 64 {
            bail!("truncated archived transaction");
        }
        let input_size = u16::from_le_bytes([payload[78], payload[79]]) as usize;
        let size = qubic::messages::Transaction::BASE_SIZE + input_size + 64;
        if payload.len() < size {
            bail!("truncated archived transaction body");
        }
        transactions.push(
            qubic::messages::Transaction::decode(&payload[..size])
                .context("decode archived transaction")?,
        );
        payload = &payload[size..];
    }
    Ok(transactions)
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
                verify_signatures,
                no_verify_signatures,
                progress_every,
                max_tick_attempts,
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
                assert!(!verify_signatures);
                assert!(no_verify_signatures);
                assert_eq!(progress_every, 100);
                assert_eq!(max_tick_attempts, 4);
            }
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn sync_defaults_use_seed_peer_and_skip_signatures() {
        let cli = Cli::try_parse_from(["glyph", "sync"]).unwrap();
        match cli.command {
            Command::Sync {
                peers,
                from,
                poll_ms,
                verify_signatures,
                no_verify_signatures,
                progress_every,
                max_tick_attempts,
            } => {
                assert_eq!(peers, vec!["157.180.10.49:21841".parse().unwrap()]);
                assert_eq!(from, None);
                assert_eq!(poll_ms, 750);
                assert!(!verify_signatures);
                assert!(!no_verify_signatures);
                assert_eq!(progress_every, 100);
                assert_eq!(max_tick_attempts, 4);
            }
            _ => panic!("expected sync command"),
        }
    }

    #[test]
    fn tick_skips_signatures_by_default() {
        let cli = Cli::try_parse_from(["glyph", "tick", "42"]).unwrap();
        match cli.command {
            Command::Tick {
                tick,
                verify_signatures,
                no_verify_signatures,
                ..
            } => {
                assert_eq!(tick, 42);
                assert!(!verify_signatures);
                assert!(!no_verify_signatures);
            }
            _ => panic!("expected tick command"),
        }
    }

    #[test]
    fn prune_parses_keep_and_dry_run() {
        let cli = Cli::try_parse_from(["glyph", "prune", "--keep", "12", "--dry-run"]).unwrap();
        match cli.command {
            Command::Prune { keep, dry_run } => {
                assert_eq!(keep, 12);
                assert!(dry_run);
            }
            _ => panic!("expected prune command"),
        }
    }

    #[test]
    fn prune_defaults_keep_54_without_dry_run() {
        let cli = Cli::try_parse_from(["glyph", "prune"]).unwrap();
        match cli.command {
            Command::Prune { keep, dry_run } => {
                assert_eq!(keep, 54);
                assert!(!dry_run);
            }
            _ => panic!("expected prune command"),
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

#[cfg(test)]
mod archive_tests {
    use super::*;

    #[test]
    fn archived_transactions_decode_sequentially() {
        let first = qubic::messages::Transaction {
            source: [1u8; 32],
            destination: [2u8; 32],
            amount: 10,
            tick: 7,
            input_type: 0,
            input: vec![0xAA, 0xBB],
            signature: [3u8; 64],
        };
        let second = qubic::messages::Transaction {
            source: [4u8; 32],
            destination: [5u8; 32],
            amount: 20,
            tick: 7,
            input_type: 1,
            input: Vec::new(),
            signature: [6u8; 64],
        };
        let mut payload = first.encode();
        payload.extend_from_slice(&second.encode());
        let decoded = decode_archived_transactions(&payload).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].amount, 10);
        assert_eq!(decoded[1].amount, 20);
        assert!(decode_archived_transactions(&[0u8; 10]).is_err());
    }
}
