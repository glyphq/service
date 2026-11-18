//! Offline verification against packets captured directly from Qubic peers.

use std::fs;

use qubic::crypto::k12::k12_32;
use qubic::crypto::schnorrq;
use qubic::messages::{Computors, Tick, TickData};
use qubic::{LogEvent, Packet};

fn packet(name: &str) -> Packet {
    let path = format!("{}/../../fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    let bytes = fs::read(&path).unwrap_or_else(|error| panic!("read {path}: {error}"));
    Packet::decode(&bytes).unwrap_or_else(|error| panic!("decode {name}: {error}"))
}

fn vote_digest(vote: &Tick) -> [u8; 32] {
    const BROADCAST_TICK: u16 = 3;
    let mut bytes = vote.encode();
    bytes[..2].copy_from_slice(&(vote.computor_index ^ BROADCAST_TICK).to_le_bytes());
    k12_32(&bytes[..bytes.len() - 64])
}

#[test]
fn golden_tick_data_matches_quorum_vote() {
    let tick_data = TickData::decode(&packet("tick_data.bin").payload).expect("tick data");
    let vote = Tick::decode(&packet("quorum_tick.bin").payload).expect("vote");
    assert_eq!(tick_data.tick, vote.tick);
    assert_eq!(tick_data.epoch, vote.epoch);
    assert_eq!(tick_data.digest(), vote.transaction_digest);
}

#[test]
fn golden_vote_signature_verifies_with_epoch_computors() {
    let computors = Computors::decode(&packet("computors.bin").payload).expect("computors");
    let vote = Tick::decode(&packet("quorum_tick.bin").payload).expect("vote");
    assert_eq!(computors.epoch, vote.epoch);

    let key = computors
        .public_keys
        .get(vote.computor_index as usize)
        .expect("vote computor index");
    assert!(
        schnorrq::verify_digest(key, &vote_digest(&vote), &vote.signature),
        "captured vote signature must verify"
    );
}

#[test]
fn golden_logs_have_valid_digests_and_ranges() {
    let logs = LogEvent::decode_many(&packet("logs.bin").payload).expect("logs");
    assert!(!logs.is_empty());
    for event in &logs {
        assert!(event.verify_digest(), "log {} digest", event.log_id);
        assert_eq!(event.epoch, 240);
        assert_eq!(event.tick, 91_910_000);
    }
    let ids: Vec<u64> = logs.iter().map(|event| event.log_id).collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted);
}
