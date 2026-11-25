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
    let tick = std::env::var("QUBIC_TICK")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| head.tick.saturating_sub(1));
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

#[tokio::test]
#[ignore = "connects to a live Qubic peer"]
async fn verifies_live_prefetched_bundle() {
    let tick: u32 = std::env::var("QUBIC_TICK")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(92_264_150);
    let config = PeerConfig::new(peer()).with_timeout(Duration::from_secs(15));
    let pool = NodePool::new(vec![config]);
    let mut connection = pool.connect_best().await.expect("connect");
    let head = connection.head().await.expect("head");
    let computors = connection.computors().await.expect("computors");
    let mut bundles = connection
        .prefetch_bundles(&[tick], head, computors, Some(&pool))
        .await
        .expect("prefetch");
    let bundle = bundles.pop().expect("one bundle").expect("bundle result");
    let expected = bundle.tick_data.non_empty_transactions().count();
    let verified = verify_bundle(bundle, false).expect("quorum verification");
    assert_eq!(verified.tick.tick, tick);
    assert_eq!(verified.transactions.len(), expected);
    assert!(verified.logs.iter().all(|log| log.tick == tick));
    eprintln!(
        "verified prefetch tick {tick}: votes={} txs={} logs={} warnings={:?}",
        verified.votes.len(),
        verified.transactions.len(),
        verified.logs.len(),
        verified.warnings
    );
}

#[tokio::test]
#[ignore = "connects to a live Qubic peer"]
async fn verifies_live_prefetched_batch() {
    let ticks = std::env::var("QUBIC_TICKS")
        .unwrap_or_else(|_| "92260000,92264150,92264151,92264152".into())
        .split(',')
        .map(|value| value.trim().parse().expect("QUBIC_TICKS must be u32s"))
        .collect::<Vec<u32>>();
    let config = PeerConfig::new(peer()).with_timeout(Duration::from_secs(15));
    let pool = NodePool::new(vec![config]);
    let mut connection = pool.connect_best().await.expect("connect");
    let head = connection.head().await.expect("head");
    let computors = connection.computors().await.expect("computors");
    let bundles = connection
        .prefetch_bundles(&ticks, head, computors, Some(&pool))
        .await
        .expect("prefetch");
    assert_eq!(bundles.len(), ticks.len());
    for (tick, bundle) in ticks.into_iter().zip(bundles) {
        let bundle = bundle.unwrap_or_else(|error| panic!("tick {tick}: {error}"));
        let expected = bundle.tick_data.non_empty_transactions().count();
        eprintln!(
            "batch tick {tick}: txs={} logs={} ranges={} complete={} warnings={:?}",
            bundle.transactions.len(),
            bundle.logs.len(),
            bundle.log_ranges.count(),
            bundle.logs_complete,
            bundle.warnings
        );
        let verified = verify_bundle(bundle, false).expect("batch quorum verification");
        assert_eq!(verified.transactions.len(), expected);
        assert!(verified.logs.iter().all(|log| log.tick == tick));
        eprintln!(
            "verified batch tick {tick}: txs={} logs={} warnings={:?}",
            verified.transactions.len(),
            verified.logs.len(),
            verified.warnings
        );
    }
}
