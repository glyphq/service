use std::net::SocketAddr;
use std::time::Duration;

use indexer::{Verification, verify_bundle};
use node::{NodePool, PeerConfig};

fn peer() -> SocketAddr {
    std::env::var("QUBIC_PEER")
        .unwrap_or_else(|_| "157.180.10.49:21841".into())
        .parse()
        .expect("QUBIC_PEER must be a socket address")
}

#[tokio::test]
#[ignore = "connects to a live Qubic peer"]
async fn verifies_live_bundle() {
    let config = PeerConfig::new(peer()).with_timeout(Duration::from_secs(15));
    let pool = NodePool::new(vec![config]);
    let mut connection = pool.connect_best().await.expect("connect");
    let head = connection.head().await.expect("head");
    let tick = head.tick.saturating_sub(1);
    let bundle = connection.bundle(tick).await.expect("bundle");
    assert_eq!(bundle.tick.tick, tick);

    let verify_signatures = std::env::var("VERIFY_SIGNATURES").is_ok_and(|value| value == "1");
    let verified = verify_bundle(bundle, verify_signatures).expect("quorum verification");
    assert_eq!(verified.tick.tick, tick);
    assert!(verified.verification >= Verification::Quorum);
    assert_eq!(verified.tick_data.tick, tick);
    assert!(verified.logs.iter().all(|log| log.verify_digest()));
    eprintln!(
        "verified tick {tick}: votes={} txs={} logs={} level={:?} warnings={:?}",
        verified.votes.len(),
        verified.transactions.len(),
        verified.logs.len(),
        verified.verification,
        verified.warnings
    );
}
