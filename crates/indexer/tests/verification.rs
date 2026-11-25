//! Regression tests for the pure tick verifier. These build bundles in memory
//! so they never touch a peer or ClickHouse.

use indexer::{
    Error, Verification, select_consensus_group, verify_bundle, verify_vote_group_parallel,
    verify_vote_group_sequential,
};
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
        length: vec![-1; LOG_TX_PER_TICK],
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
        logs_complete: true,
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
fn empty_tick_needs_a_full_quorum_and_no_transactions() {
    let data = tick_data(&[]);
    let votes = (0..451)
        .map(|index| vote(index, ZERO_DIGEST))
        .collect::<Vec<_>>();
    verify_bundle(
        bundle(data.clone(), votes, Vec::new(), empty_ranges(), Vec::new()),
        false,
    )
    .expect("451 empty votes should reach quorum");

    let votes = (0..450)
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
    let votes = (0..451)
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
    let votes = (0..451)
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
fn log_outside_declared_ranges_is_incomplete() {
    let data = tick_data(&[]);
    let votes = (0..451)
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
    .expect_err("an out-of-range log means the range table is incomplete");
    assert!(matches!(error, Error::IncompleteTick { .. }));
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

#[test]
fn missing_transaction_body_is_incomplete_not_empty() {
    let tx = transaction();
    let data = tick_data(&[(0, tx.hash())]);
    let digest = data.digest();
    let votes = (0..451)
        .map(|index| vote(index, digest))
        .collect::<Vec<_>>();
    let error = verify_bundle(
        bundle(data, votes, Vec::new(), empty_ranges(), Vec::new()),
        false,
    )
    .expect_err("a non-empty tick without its body must not verify");
    assert!(matches!(error, Error::IncompleteTick { .. }));
}

#[test]
fn unverified_logs_are_incomplete() {
    let data = tick_data(&[]);
    let votes = (0..451)
        .map(|index| vote(index, ZERO_DIGEST))
        .collect::<Vec<_>>();
    let mut tick = bundle(data, votes, Vec::new(), empty_ranges(), Vec::new());
    tick.logs_complete = false;
    let error = verify_bundle(tick, false).expect_err("missing log proof must not verify");
    assert!(matches!(error, Error::IncompleteTick { .. }));
}

#[test]
fn largest_vote_group_is_selected_before_verification() {
    let largest = [0xAA; 32];
    let smaller = [0xBB; 32];
    let mut votes = (0..30)
        .map(|index| vote(index, largest))
        .collect::<Vec<_>>();
    votes.extend((100..108).map(|index| vote(index, smaller)));
    let votes = votes.iter().collect::<Vec<_>>();

    let consensus =
        select_consensus_group(TICK, &votes, |_| 10, |group, _| group.len()).expect("consensus");
    assert_eq!(consensus.digest, largest);
    assert_eq!(consensus.count, 30);
}

#[test]
fn invalid_largest_group_falls_back_to_valid_smaller_group() {
    let invalid = [0xAA; 32];
    let valid = [0xBB; 32];
    let mut votes = (0..25)
        .map(|index| vote(index, invalid))
        .collect::<Vec<_>>();
    votes.extend((100..115).map(|index| vote(index, valid)));
    let votes = votes.iter().collect::<Vec<_>>();

    // The largest group has the most votes but none of its signatures verify.
    let consensus = select_consensus_group(
        TICK,
        &votes,
        |_| 10,
        |group, _| {
            if group[0].transaction_digest == valid {
                group.len()
            } else {
                0
            }
        },
    )
    .expect("fallback to the smaller valid group");
    assert_eq!(consensus.digest, valid);
    assert_eq!(consensus.count, 15);
}

#[test]
fn mismatched_timestamps_do_not_form_a_quorum() {
    let digest = [0xAA; 32];
    let mut votes = (0..20).map(|index| vote(index, digest)).collect::<Vec<_>>();
    for vote in votes.iter_mut().skip(10) {
        vote.millisecond = 999;
    }
    let votes = votes.iter().collect::<Vec<_>>();
    assert!(
        select_consensus_group(TICK, &votes, |_| 11, |group, _| group.len()).is_err(),
        "two ten-vote timestamps must not satisfy an eleven-vote quorum"
    );
}

#[test]
fn parallel_vote_verification_matches_sequential() {
    let computors = Computors::decode(&include_bytes!("../../../fixtures/computors.bin")[8..])
        .expect("computors fixture");
    let golden = Tick::decode(&include_bytes!("../../../fixtures/quorum_tick.bin")[8..])
        .expect("vote fixture");

    // Only the untouched golden vote has a valid signature; every mutated
    // computor index changes the signed bytes.
    let votes = (0..300u16)
        .map(|index| {
            let mut vote = golden.clone();
            vote.computor_index = index;
            vote
        })
        .collect::<Vec<_>>();
    let votes = votes.iter().collect::<Vec<_>>();

    let sequential = verify_vote_group_sequential(&votes, &computors, 451);
    let parallel = verify_vote_group_parallel(&votes, &computors, 451);
    assert_eq!(parallel, sequential);
    assert_eq!(sequential.valid, 1);
    assert_eq!(sequential.first_invalid, Some(0));
}

#[test]
fn vote_verification_stops_at_quorum() {
    let computors = Computors::decode(&include_bytes!("../../../fixtures/computors.bin")[8..])
        .expect("computors fixture");
    let golden = Tick::decode(&include_bytes!("../../../fixtures/quorum_tick.bin")[8..])
        .expect("vote fixture");

    // The only valid vote is first, so a one-vote threshold is met immediately
    // and the invalid tail must not be counted as a failure.
    let mut votes = vec![golden];
    votes.extend((0..299u16).map(|index| {
        let mut vote = vote(index, ZERO_DIGEST);
        vote.computor_index = index;
        vote
    }));
    let votes = votes.iter().collect::<Vec<_>>();

    let sequential = verify_vote_group_sequential(&votes, &computors, 1);
    let parallel = verify_vote_group_parallel(&votes, &computors, 1);
    assert_eq!(sequential, parallel);
    assert_eq!(sequential.valid, 1);
    assert_eq!(sequential.first_invalid, None);
}
