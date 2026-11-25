//! Composition of raw tick bundles from verified ticks.
//!
//! The bundle format is unchanged: [`archive::RawBundle`] still supports the
//! `Votes` and `Computors` sections so previously archived objects decode
//! exactly as before. This module only decides what new bundles contain.
//!
//! Default policy:
//!
//! - Tick header, `TickData`, transactions, log ranges, and logs are always
//!   stored.
//! - Vote headers are stored only when [`archive::ArchivePolicy::include_votes`]
//!   is set (`ARCHIVE_VOTES=true`).
//! - Computors are never stored per tick. The indexer writes the epoch's
//!   computor set once with [`archive::put_computors`].

use archive::{ArchivePolicy, RawBundle, Section};

use crate::VerifiedTick;

/// Builds the raw bundle for one verified tick under `policy`.
///
/// The returned bundle is a pure function of the tick and policy. It performs
/// no I/O and does not write the computor sidecar.
pub fn raw_bundle(verified: &VerifiedTick, policy: &ArchivePolicy) -> RawBundle {
    let mut bundle = RawBundle::new(verified.tick.epoch, verified.tick.tick);
    bundle.set_section(Section::TickHeader, verified.tick.encode().to_vec());
    bundle.set_section(Section::TickData, verified.tick_data.encode());

    if policy.include_votes {
        let mut votes = Vec::with_capacity(verified.votes.len() * 352);
        for vote in &verified.votes {
            votes.extend_from_slice(&vote.encode());
        }
        bundle.set_section(Section::Votes, votes);
    }

    let mut transactions = Vec::new();
    for transaction in &verified.transactions {
        transactions.extend_from_slice(&transaction.encode());
    }
    bundle.set_section(Section::Transactions, transactions);

    bundle.set_section(Section::LogRanges, verified.log_ranges.encode());

    let mut logs = Vec::new();
    for event in &verified.logs {
        logs.extend_from_slice(&event.encode());
    }
    bundle.set_section(Section::Logs, logs);

    // Computors belong to the epoch sidecar, never to a per-tick bundle.
    bundle.computors.clear();

    bundle
}

#[cfg(test)]
mod tests {
    use super::*;
    use node::Head;
    use qubic::LogEvent;
    use qubic::messages::{Computors, LogRanges, Tick, TickData};

    use crate::Verification;

    fn verified_with_votes(votes: usize) -> VerifiedTick {
        let tick = Tick {
            computor_index: 0,
            epoch: 42,
            tick: 123,
            millisecond: 0,
            second: 0,
            minute: 0,
            hour: 0,
            day: 0,
            month: 0,
            year: 0,
            prev_resource_testing_digest: 0,
            salted_resource_testing_digest: 0,
            prev_transaction_body_digest: 0,
            salted_transaction_body_digest: 0,
            prev_spectrum_digest: [0; 32],
            prev_universe_digest: [0; 32],
            prev_computer_digest: [0; 32],
            salted_spectrum_digest: [0; 32],
            salted_universe_digest: [0; 32],
            salted_computer_digest: [0; 32],
            transaction_digest: [0; 32],
            expected_next_tick_transaction_digest: [0; 32],
            signature: [0; 64],
        };
        let mut vote = tick.clone();
        // Give each vote a distinct computor index so payloads are non-empty.
        vote.computor_index = 1;
        VerifiedTick {
            head: Head {
                epoch: 42,
                tick: 123,
                initial_tick: 100,
                tick_duration: 1,
            },
            tick,
            tick_data: TickData {
                computor_index: 0,
                epoch: 42,
                tick: 123,
                millisecond: 0,
                second: 0,
                minute: 0,
                hour: 0,
                day: 0,
                month: 0,
                year: 0,
                timelock: [0; 32],
                transaction_digests: Vec::new(),
                contract_fees: Vec::new(),
                signature: [0; 64],
            },
            proposer_index: 0,
            tick_digest: [0; 32],
            votes: vec![vote; votes],
            transactions: Vec::new(),
            computors: Computors {
                epoch: 42,
                public_keys: Vec::new(),
                signature: [0; 64],
            },
            log_ranges: LogRanges {
                from_log_id: Vec::new(),
                length: Vec::new(),
            },
            logs: Vec::<LogEvent>::new(),
            verification: Verification::Quorum,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn default_policy_omits_votes_and_computors() {
        let verified = verified_with_votes(3);
        let bundle = raw_bundle(&verified, &ArchivePolicy::default());

        assert_eq!(bundle.epoch, 42);
        assert_eq!(bundle.tick, 123);
        assert!(!bundle.tick_header.is_empty());
        assert!(!bundle.tick_data.is_empty());
        assert!(bundle.votes.is_empty(), "votes must be omitted by default");
        assert!(
            bundle.computors.is_empty(),
            "computors belong to the epoch sidecar"
        );
        // The always-present sections are still encoded.
        assert!(!bundle.encode().unwrap().is_empty());
    }

    #[test]
    fn opt_in_policy_includes_votes_only() {
        let verified = verified_with_votes(3);
        let policy = ArchivePolicy::from_pairs([("ARCHIVE_VOTES", "true")]);
        let bundle = raw_bundle(&verified, &policy);

        assert_eq!(bundle.votes.len(), 3 * 352);
        assert!(
            bundle.computors.is_empty(),
            "computors must never be per-tick"
        );
    }
}
