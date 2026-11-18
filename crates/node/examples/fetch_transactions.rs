//! Fetches the raw transactions for one tick into `crates/node/fixtures`.
//!
//! Usage: cargo run -p node --example fetch_transactions -- <peer> <tick>

use std::net::SocketAddr;
use std::path::PathBuf;

use node::{Peer, PeerConfig};
use qubic::messages::TX_PER_TICK;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let peer: SocketAddr = args
        .next()
        .unwrap_or_else(|| "157.180.10.49:21841".into())
        .parse()?;
    let tick: u32 = args.next().unwrap_or_else(|| "0".into()).parse()?;

    let mut connection = Peer::connect(PeerConfig::new(peer)).await?;
    let head = connection.head().await?;
    if tick > head.tick {
        anyhow::bail!("tick {tick} is ahead of head {}", head.tick);
    }

    let tick_data = connection.tick_data(tick).await?;
    let transactions = connection.transactions(tick, &[false; TX_PER_TICK]).await?;

    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    std::fs::create_dir_all(&fixtures)?;
    std::fs::write(
        fixtures.join(format!("tickdata-{tick}.bin")),
        tick_data.encode(),
    )?;

    let mut report = String::new();
    report.push_str(&format!(
        "tick={tick} epoch={} info_epoch={} transactions={}\n",
        tick_data.epoch,
        head.epoch,
        transactions.len()
    ));
    for (index, digest) in tick_data.non_empty_transactions() {
        report.push_str(&format!(
            "tick_data_slot={index} digest={}\n",
            hex::encode(digest)
        ));
    }

    for (index, transaction) in transactions.iter().enumerate() {
        let raw = transaction.encode();
        let path = fixtures.join(format!("tx-{tick}-{index}.bin"));
        std::fs::write(&path, &raw)?;
        report.push_str(&format!(
            "tx_index={index} size={} tick={} source={} signature={}\n",
            raw.len(),
            transaction.tick,
            hex::encode(transaction.source),
            hex::encode(transaction.signature),
        ));
    }

    std::fs::write(fixtures.join(format!("tx-{tick}.txt")), report)?;
    println!(
        "{}",
        std::fs::read_to_string(fixtures.join(format!("tx-{tick}.txt")))?
    );
    Ok(())
}
