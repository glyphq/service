//! Regression tests for the pure tick verifier. These build bundles in memory
//! so they never touch a peer or ClickHouse.

use indexer::{Error, Verification, verify_bundle};
use node::{Head, TickBundle};
use qubic::messages::{
    COMPUTOR_COUNT, Computors, LOG_TX_PER_TICK, LogRanges, TX_PER_TICK, Tick, TickData,
    Transaction, ZERO_DIGEST,
};
use qubic::{LogEvent, logs::QU_TRANSFER};

const EPOCH: u16 = 7;
const TICK: u32 = 1_000;

fn head() -> Head {
    Head {
        epoch: EPOCH,
        tick: TICK,
        initial_tick: TICK,
        tick_duration: 1_000,
    }
}

fn vote(index: u16, digest: [u8; 32]) -> Tick {
    Tick {
        computor_index: index,
        epoch: EPOCH,
        tick: TICK,
        millisecond: 0,
        second: 1,
        minute: 2,
        hour: 3,
        day: 4,
        month: 5,
        year: 26,
        prev_resource_testing_digest: 0,
        salted_resource_testing_digest: 0,
        prev_transaction_body_digest: 0,
        salted_transaction_body_digest: 0,
        prev_spectrum_digest: [1; 32],
        prev_universe_digest: [2; 32],
        prev_computer_digest: [3; 32],
        salted_spectrum_digest: [4; 32],
        salted_universe_digest: [5; 32],
        salted_computer_digest: [6; 32],
        transaction_digest: digest,
        expected_next_tick_transaction_digest: [7; 32],
        signature: [0; 64],
    }
}

fn tick_data(digests: &[(usize, [u8; 32])]) -> TickData {
    let mut slots = vec![ZERO_DIGEST; TX_PER_TICK];
    for (index, digest) in digests {
        slots[*index] = *digest;
    }
    TickData {
        computor_index: 0,
        epoch: EPOCH,
        tick: TICK,
        millisecond: 0,
        second: 1,
        minute: 2,
        hour: 3,
        day: 4,
        month: 5,
        year: 26,
        timelock: [0; 32],
        transaction_digests: slots,
        contract_fees: vec![0; 1024],
        signature: [0; 64],
    }
}

fn empty_ranges() -> LogRanges {
    LogRanges {
        from_log_id: vec![-1; LOG_TX_PER_TICK],
        length: vec![0; LOG_TX_PER_TICK],
    }
}

fn computors() -> Computors {
    Computors {
        epoch: EPOCH,
        public_keys: vec![[0; 32]; COMPUTOR_COUNT],
        signature: [0; 64],
    }
}

fn bundle(
    tick_data: TickData,
    votes: Vec<Tick>,
    transactions: Vec<Transaction>,
    log_ranges: LogRanges,
    logs: Vec<LogEvent>,
) -> TickBundle {
    let candidate = votes
        .first()
        .cloned()
        .unwrap_or_else(|| vote(0, ZERO_DIGEST));
    TickBundle {
        head: head(),
        computors: computors(),
        tick: candidate,
        tick_data,
        votes,
        transactions,
        log_ranges,
        logs,
        warnings: Vec::new(),
    }
}

fn transaction() -> Transaction {
    Transaction {
        source: [9; 32],
        destination: [10; 32],
        amount: 42,
        tick: TICK,
        input_type: 0,
        input: Vec::new(),
        signature: [0; 64],
    }
}

#[test]
fn non_empty_tick_needs_451_aligned_votes() {
    let tx = transaction();
    let digest = tx.hash();
    let data = tick_data(&[(0, digest)]);
    let digest = data.digest();
    let votes = (0..451)
        .map(|index| vote(index, digest))
        .collect::<Vec<_>>();

    let verified = verify_bundle(
        bundle(
            data.clone(),
            votes,
            vec![tx.clone()],
            empty_ranges(),
            Vec::new(),
        ),
        false,
    )
    .expect("451 votes should reach quorum");
    assert_eq!(verified.verification, Verification::Quorum);
    assert!(verified.warnings.is_empty(), "{:?}", verified.warnings);

    let votes = (0..450)
        .map(|index| vote(index, digest))
        .collect::<Vec<_>>();
    let error = verify_bundle(
        bundle(data, votes, vec![tx], empty_ranges(), Vec::new()),
        false,
    )
    .unwrap_err();
    assert!(matches!(error, Error::Verification(_)));
}

#[test]
fn empty_tick_needs_226_votes_and_no_transactions() {
    let data = tick_data(&[]);
    let votes = (0..226)
        .map(|index| vote(index, ZERO_DIGEST))
        .collect::<Vec<_>>();
    verify_bundle(
        bundle(data.clone(), votes, Vec::new(), empty_ranges(), Vec::new()),
        false,
    )
    .expect("226 empty votes should reach quorum");

    let votes = (0..225)
        .map(|index| vote(index, ZERO_DIGEST))
        .collect::<Vec<_>>();
    assert!(
        verify_bundle(
            bundle(data.clone(), votes, Vec::new(), empty_ranges(), Vec::new()),
            false
        )
        .is_err()
    );

    // A zero transaction digest with a non-zero transaction slot is a lie.
    let tx = transaction();
    let data = tick_data(&[(0, tx.hash())]);
    let votes = (0..226)
        .map(|index| vote(index, ZERO_DIGEST))
        .collect::<Vec<_>>();
    assert!(
        verify_bundle(
            bundle(data, votes, Vec::new(), empty_ranges(), Vec::new()),
            false
        )
        .is_err()
    );
}

#[test]
fn duplicate_computor_votes_do_not_count_twice() {
    let tx = transaction();
    let data = tick_data(&[(0, tx.hash())]);
    let digest = data.digest();
    let votes = vec![vote(5, digest); 451];
    let error = verify_bundle(
        bundle(data, votes, vec![tx], empty_ranges(), Vec::new()),
        false,
    )
    .unwrap_err();
    assert!(matches!(error, Error::Verification(_)), "{error}");
}

#[test]
fn vote_digest_must_match_tick_data() {
    let tx = transaction();
    let data = tick_data(&[(0, tx.hash())]);
    let votes = (0..451)
        .map(|index| vote(index, [0xAA; 32]))
        .collect::<Vec<_>>();
    let error = verify_bundle(
        bundle(data, votes, vec![tx], empty_ranges(), Vec::new()),
        false,
    )
    .unwrap_err();
    assert!(format!("{error}").contains("digest"));
}

#[test]
fn tampered_log_body_is_rejected() {
    let data = tick_data(&[]);
    let votes = (0..226)
        .map(|index| vote(index, ZERO_DIGEST))
        .collect::<Vec<_>>();
    let mut ranges = empty_ranges();
    ranges.from_log_id[0] = 0;
    ranges.length[0] = 1;
    let event = LogEvent {
        epoch: EPOCH,
        tick: TICK,
        log_type: QU_TRANSFER,
        log_id: 0,
        log_digest: 0,
        body: vec![1, 2, 3],
    };
    let error =
        verify_bundle(bundle(data, votes, Vec::new(), ranges, vec![event]), false).unwrap_err();
    assert!(format!("{error}").contains("digest"));
}

#[test]
fn log_outside_declared_ranges_is_rejected() {
    let data = tick_data(&[]);
    let votes = (0..226)
        .map(|index| vote(index, ZERO_DIGEST))
        .collect::<Vec<_>>();
    let body = vec![1, 2, 3];
    let event = LogEvent {
        epoch: EPOCH,
        tick: TICK,
        log_type: QU_TRANSFER,
        log_id: 9,
        log_digest: u64::from_le_bytes(qubic::crypto::k12::k12_8(&body)),
        body,
    };
    let error = verify_bundle(
        bundle(data, votes, Vec::new(), empty_ranges(), vec![event]),
        false,
    )
    .unwrap_err();
    assert!(format!("{error}").contains("outside"));
}

#[test]
fn extra_transaction_downgrades_but_does_not_reject() {
    let tx = transaction();
    let data = tick_data(&[(0, tx.hash())]);
    let digest = data.digest();
    let votes = (0..451)
        .map(|index| vote(index, digest))
        .collect::<Vec<_>>();
    let mut extra = tx.clone();
    extra.amount = 43;
    let verified = verify_bundle(
        bundle(data, votes, vec![tx, extra], empty_ranges(), Vec::new()),
        false,
    )
    .expect("extra transaction is a data-tier issue");
    assert_eq!(verified.verification, Verification::Quorum);
    assert!(
        verified
            .warnings
            .iter()
            .any(|warning| warning.contains("not in the tick data digest set"))
    );
}
