//! Pipelined batch fetch for historical ticks.
//!
//! One tick normally costs three round trips: tick data/votes/transactions,
//! log-id ranges, and log records. That is fine at the chain tip, where ticks
//! arrive slowly, but it makes a deep catch-up needlessly slow. This module
//! writes the requests for a whole batch before reading any response, so the
//! same three phases cover many ticks instead of one.
//!
//! The public entry point is [`Peer::prefetch_bundles`]. It returns one result
//! per requested tick so a single bad tick does not discard the rest of the
//! batch. Logs are optional, matching [`Peer::bundle_with`]: a peer without
//! logging yields empty ranges and a warning, not a batch failure.

use super::*;
use qubic::messages::{
    COMPUTOR_COUNT, LogRanges, QUORUM, TX_PER_TICK, Tick, TickData, Transaction,
    request_all_log_ranges, request_logs, request_quorum_tick, request_tick_data,
    request_tick_transactions,
};
use qubic::wire::{MessageType, Packet};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Bound a batch so one worker cannot buffer an unbounded number of votes or
/// log probes. Qubic's peer response queue holds 65,536 packets; a batch of 64
/// leaves room for the peer's other clients while keeping every vote response
/// addressable.
const MAX_PREFETCH_TICKS: usize = 64;

#[derive(Debug, Clone, Copy)]
enum MainStream {
    TickData,
    Votes,
    Transactions,
}

impl MainStream {
    fn index(self) -> usize {
        match self {
            Self::TickData => 0,
            Self::Votes => 1,
            Self::Transactions => 2,
        }
    }
}

struct MainState {
    tick: u32,
    tick_data: Option<Packet>,
    votes: Vec<Packet>,
    transactions: Vec<Packet>,
    done: [bool; 3],
    error: Option<Error>,
}

#[derive(Clone)]
struct DecodedTick {
    tick: u32,
    tick_data: TickData,
    votes: Vec<Tick>,
    transactions: Vec<Transaction>,
    warnings: Vec<String>,
}

impl Peer {
    /// Fetch several consecutive ticks over one connection.
    ///
    /// The batch writes every tick-data, quorum-vote, and transaction request
    /// before draining any response, then does the same for log ranges and log
    /// records. Each element of the returned vector belongs to the tick at the
    /// same position in `ticks`.
    pub async fn prefetch_bundles(
        &mut self,
        ticks: &[u32],
        head: Head,
        computors: Computors,
        fallback: Option<&NodePool>,
    ) -> Result<Vec<Result<TickBundle>>> {
        if ticks.is_empty() {
            return Ok(Vec::new());
        }
        if ticks.len() > MAX_PREFETCH_TICKS {
            return Err(Error::InvalidRequest(format!(
                "prefetch batch has {} ticks, maximum is {MAX_PREFETCH_TICKS}",
                ticks.len()
            )));
        }

        health::check_cached_head(self.observed_head.as_ref(), &head)?;
        let newer = self.observed_head.as_ref().is_none_or(|observed| {
            observed.epoch < head.epoch
                || (observed.epoch == head.epoch && observed.tick < head.tick)
        });
        if newer {
            self.observed_head = Some(head.clone());
        }
        if let Some(last) = ticks.last() {
            if *last > head.tick {
                return Err(Error::NotSynchronized {
                    head: head.tick,
                    requested: *last,
                });
            }
        }

        let mut states = ticks
            .iter()
            .map(|tick| MainState {
                tick: *tick,
                tick_data: None,
                votes: Vec::with_capacity(COMPUTOR_COUNT),
                transactions: Vec::with_capacity(TX_PER_TICK),
                done: [false; 3],
                error: None,
            })
            .collect::<Vec<_>>();
        let mut awaiting = HashMap::with_capacity(ticks.len() * 3);
        let mut sent = Vec::with_capacity(ticks.len() * 3);
        for (index, tick) in ticks.iter().enumerate() {
            let requests = [
                (
                    MessageType::RequestTickData,
                    request_tick_data(*tick),
                    MainStream::TickData,
                ),
                (
                    MessageType::RequestQuorumTick,
                    request_quorum_tick(*tick, &[false; COMPUTOR_COUNT]),
                    MainStream::Votes,
                ),
                (
                    MessageType::RequestTickTransactions,
                    request_tick_transactions(*tick, &[false; TX_PER_TICK]),
                    MainStream::Transactions,
                ),
            ];
            for (kind, payload, stream) in requests {
                let dejavu = match self.send(kind, payload).await {
                    Ok(dejavu) => dejavu,
                    Err(error) => {
                        self.clear_pending(&awaiting);
                        return Err(error);
                    }
                };
                awaiting.insert(dejavu, (index, stream));
                sent.push(dejavu);
            }
        }

        let drained = self.drain_main(&mut states, &mut awaiting).await;
        // A failed stream may leave its dejavu out of `awaiting`; the request
        // is no longer useful either way.
        for dejavu in sent {
            self.pending.remove(&dejavu);
        }
        drained?;

        let mut parts = (0..ticks.len()).map(|_| None).collect::<Vec<_>>();
        let mut results = (0..ticks.len()).map(|_| None).collect::<Vec<_>>();
        let mut missing = Vec::new();
        let mut missing_votes = Vec::new();
        let fallback = fallback.cloned();
        for (index, mut state) in states.into_iter().enumerate() {
            match state.error.take() {
                Some(error) => results[index] = Some(Err(error)),
                None => match decode_main_parts(state) {
                    Ok(part) => {
                        if part.votes.is_empty() {
                            missing_votes.push((index, part.tick));
                        }
                        if part.tick_data.is_none() {
                            missing.push((index, part.tick));
                            parts[index] = Some(part);
                            continue;
                        }
                        if part.tick_data.is_some() {
                            parts[index] = Some(part);
                        } else {
                            results[index] =
                                Some(Err(Error::EndResponse(MessageType::RequestTickData)));
                        }
                    }
                    Err(error) => results[index] = Some(Err(error)),
                },
            }
        }

        if !missing_votes.is_empty() {
            if let Some(fallback) = fallback.as_ref() {
                let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(4));
                let mut tasks = tokio::task::JoinSet::new();
                for (index, tick) in missing_votes {
                    let fallback = fallback.clone();
                    let semaphore = semaphore.clone();
                    tasks.spawn(async move {
                        let _permit = semaphore
                            .acquire()
                            .await
                            .expect("vote semaphore is never closed");
                        (index, tick, fallback.votes(tick).await)
                    });
                }
                while let Some(joined) = tasks.join_next().await {
                    match joined {
                        Ok((index, _tick, Ok(votes))) => {
                            if let Some(part) = parts[index].as_mut() {
                                part.votes = votes;
                            }
                        }
                        Ok((index, _tick, Err(error))) => {
                            if results[index].is_none() {
                                results[index] = Some(Err(error));
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, "vote fallback task failed");
                        }
                    }
                }
            }
        }

        if !missing.is_empty() {
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(8));
            let mut tasks = tokio::task::JoinSet::new();
            for (index, tick) in missing {
                let fallback = fallback.clone();
                let semaphore = semaphore.clone();
                tasks.spawn(async move {
                    let _permit = semaphore
                        .acquire()
                        .await
                        .expect("tick-data semaphore is never closed");
                    let Some(fallback) = fallback.as_ref() else {
                        return (index, tick, Err(Error::NoPeers));
                    };
                    match fallback.tick_data(tick).await {
                        Ok(data) => (index, tick, Ok(data)),
                        Err(error) => {
                            tracing::warn!(
                                tick,
                                %error,
                                "no peer supplied missing tick data"
                            );
                            (index, tick, Err(error))
                        }
                    }
                });
            }
            while let Some(joined) = tasks.join_next().await {
                match joined {
                    Ok((index, _tick, Ok(tick_data))) => {
                        if let Some(part) = parts[index].as_mut() {
                            part.tick_data = Some(tick_data);
                        }
                    }
                    Ok((index, tick, Err(_error))) => {
                        let empty_votes = parts[index].as_ref().is_some_and(|part| {
                            !part.votes.is_empty()
                                && part.votes.iter().all(|vote| vote.is_empty_vote())
                                && part
                                    .votes
                                    .iter()
                                    .map(|vote| vote.computor_index)
                                    .collect::<std::collections::HashSet<_>>()
                                    .len()
                                    >= QUORUM
                        });
                        if empty_votes {
                            if let Some(part) = parts[index].as_mut() {
                                part.tick_data = empty_tick_from_votes(&part.votes, tick).ok();
                            }
                        } else {
                            results[index] = Some(Err(Error::MissingNonEmptyTick { tick }));
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "tick-data fallback task failed");
                    }
                }
            }
        }

        let mut decoded = (0..ticks.len()).map(|_| None).collect::<Vec<_>>();
        for (index, part) in parts.into_iter().enumerate() {
            if results[index].is_some() {
                continue;
            }
            let Some(part) = part else {
                continue;
            };
            match finish_decoded(part) {
                Ok(tick) => decoded[index] = Some(tick),
                Err(error) => results[index] = Some(Err(error)),
            }
        }

        for (index, entry) in decoded.iter_mut().enumerate() {
            if results[index].is_some() {
                continue;
            }
            let Some(tick) = entry.as_mut() else {
                continue;
            };
            if let Err(error) = self.complete_transactions(tick, fallback.as_ref()).await {
                results[index] = Some(Err(error));
                *entry = None;
            }
        }

        let (mut log_ranges, mut logs, mut known, mut log_warnings) =
            self.prefetch_logs(&decoded).await;
        let incomplete = (0..decoded.len())
            .filter(|index| {
                decoded[*index].is_some()
                    && !(known[*index]
                        && !log_ranges[*index].is_pending()
                        && (logs[*index].len() as i64) >= log_ranges[*index].count())
            })
            .collect::<Vec<_>>();
        if !incomplete.is_empty() {
            // A 64-tick batch can overrun the peer's response queue or hit
            // its log-range timeout. Retry only the affected ticks on a
            // fresh connection, re-testing the all-ranges request first.
            self.desynced = true;
            self.log_ranges_supported = None;
            for index in incomplete {
                let Some(tick) = decoded[index].clone() else {
                    continue;
                };
                let (retry_ranges, retry_logs, retry_known, retry_warnings) =
                    self.prefetch_logs(&[Some(tick)]).await;
                if retry_known[0]
                    && !retry_ranges[0].is_pending()
                    && (retry_logs[0].len() as i64) >= retry_ranges[0].count()
                {
                    log_ranges[index] = retry_ranges[0].clone();
                    logs[index] = retry_logs[0].clone();
                    known[index] = true;
                }
                log_warnings[index].extend(retry_warnings[0].clone());
            }
        }
        let mut log_ranges = log_ranges.into_iter();
        let mut logs = logs.into_iter();
        let mut known = known.into_iter();
        let mut log_warnings = log_warnings.into_iter();
        for (index, decoded) in decoded.into_iter().enumerate() {
            let ranges = log_ranges.next().expect("one range entry per tick");
            let tick_logs = logs.next().expect("one log entry per tick");
            let ranges_known = known.next().expect("one known entry per tick");
            let tick_warnings = log_warnings.next().expect("one warning entry per tick");
            if results[index].is_some() {
                continue;
            }
            let Some(decoded) = decoded else {
                continue;
            };
            let DecodedTick {
                tick: tick_number,
                tick_data,
                votes,
                transactions,
                warnings: mut decoded_warnings,
            } = decoded;
            let tick = match first_vote_or_synthetic(&votes, &tick_data, tick_number) {
                Ok(tick) => tick,
                Err(error) => {
                    results[index] = Some(Err(error));
                    continue;
                }
            };
            let logs_complete =
                ranges_known && !ranges.is_pending() && (tick_logs.len() as i64) >= ranges.count();
            decoded_warnings.extend(tick_warnings);
            results[index] = Some(Ok(TickBundle {
                head: head.clone(),
                computors: computors.clone(),
                tick,
                tick_data,
                votes,
                transactions,
                log_ranges: ranges,
                logs: tick_logs,
                logs_complete,
                warnings: decoded_warnings,
            }));
        }

        Ok(results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(Error::Verification(
                        "prefetch produced no result for a requested tick".into(),
                    ))
                })
            })
            .collect())
    }

    async fn drain_main(
        &mut self,
        states: &mut [MainState],
        awaiting: &mut HashMap<u32, (usize, MainStream)>,
    ) -> Result<()> {
        let deadline = Instant::now() + self.prefetch_timeout(states.len());
        while !awaiting.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.clear_pending(awaiting);
                self.desynced = true;
                return Err(Error::Timeout(self.prefetch_timeout(states.len())));
            }
            let packet = match self.read_packet_timeout(remaining).await {
                Ok(packet) => packet,
                Err(error) => {
                    self.clear_pending(awaiting);
                    self.desynced = true;
                    return Err(error);
                }
            };
            if packet.kind == MessageType::ExchangePublicPeers {
                self.record_peers(&packet);
                continue;
            }
            let Some(&(index, stream)) = awaiting.get(&packet.dejavu) else {
                continue;
            };
            let state = &mut states[index];

            match stream {
                MainStream::TickData => {
                    awaiting.remove(&packet.dejavu);
                    self.pending.remove(&packet.dejavu);
                    match packet.kind {
                        MessageType::BroadcastFutureTickData => {
                            state.tick_data = Some(packet);
                            finish_stream(state, stream);
                        }
                        MessageType::EndResponse => finish_stream(state, stream),
                        MessageType::TryAgain => {
                            self.desynced = true;
                            fail_tick(
                                states,
                                awaiting,
                                index,
                                Error::TryAgain(MessageType::RequestTickData),
                            );
                        }
                        actual => {
                            self.desynced = true;
                            fail_tick(
                                states,
                                awaiting,
                                index,
                                Error::UnexpectedPacket {
                                    expected: MessageType::BroadcastFutureTickData,
                                    actual,
                                },
                            );
                        }
                    }
                }
                MainStream::Votes => match packet.kind {
                    MessageType::BroadcastTick => {
                        if state.votes.len() >= COMPUTOR_COUNT {
                            awaiting.remove(&packet.dejavu);
                            self.pending.remove(&packet.dejavu);
                            self.desynced = true;
                            fail_tick(
                                states,
                                awaiting,
                                index,
                                Error::InvalidRequest("peer sent too many quorum votes".into()),
                            );
                        } else {
                            state.votes.push(packet);
                        }
                    }
                    MessageType::EndResponse => {
                        awaiting.remove(&packet.dejavu);
                        self.pending.remove(&packet.dejavu);
                        finish_stream(state, stream);
                    }
                    MessageType::TryAgain => {
                        awaiting.remove(&packet.dejavu);
                        self.pending.remove(&packet.dejavu);
                        self.desynced = true;
                        fail_tick(
                            states,
                            awaiting,
                            index,
                            Error::TryAgain(MessageType::RequestQuorumTick),
                        );
                    }
                    actual => {
                        awaiting.remove(&packet.dejavu);
                        self.pending.remove(&packet.dejavu);
                        self.desynced = true;
                        fail_tick(
                            states,
                            awaiting,
                            index,
                            Error::UnexpectedPacket {
                                expected: MessageType::BroadcastTick,
                                actual,
                            },
                        );
                    }
                },
                MainStream::Transactions => match packet.kind {
                    MessageType::BroadcastTransaction => {
                        if state.transactions.len() >= TX_PER_TICK {
                            awaiting.remove(&packet.dejavu);
                            self.pending.remove(&packet.dejavu);
                            self.desynced = true;
                            fail_tick(
                                states,
                                awaiting,
                                index,
                                Error::InvalidRequest("peer sent too many transactions".into()),
                            );
                        } else {
                            state.transactions.push(packet);
                        }
                    }
                    MessageType::EndResponse => {
                        awaiting.remove(&packet.dejavu);
                        self.pending.remove(&packet.dejavu);
                        finish_stream(state, stream);
                    }
                    MessageType::TryAgain => {
                        awaiting.remove(&packet.dejavu);
                        self.pending.remove(&packet.dejavu);
                        self.desynced = true;
                        fail_tick(
                            states,
                            awaiting,
                            index,
                            Error::TryAgain(MessageType::RequestTickTransactions),
                        );
                    }
                    actual => {
                        awaiting.remove(&packet.dejavu);
                        self.pending.remove(&packet.dejavu);
                        self.desynced = true;
                        fail_tick(
                            states,
                            awaiting,
                            index,
                            Error::UnexpectedPacket {
                                expected: MessageType::BroadcastTransaction,
                                actual,
                            },
                        );
                    }
                },
            }
        }
        Ok(())
    }

    /// Fetches transaction bodies that were dropped from the batch response.
    /// A large prefetch can overrun a peer's response queue; the missing-slot
    /// bitmap lets the same connection recover only the lost packets.
    async fn complete_transactions(
        &mut self,
        tick: &mut DecodedTick,
        fallback: Option<&NodePool>,
    ) -> Result<()> {
        let (known, missing) = missing_transaction_flags(&tick.tick_data, &tick.transactions);
        if missing == 0 {
            return Ok(());
        }
        let fetched = self.transactions(tick.tick, &known).await?;
        for transaction in fetched {
            let hash = transaction.hash();
            if !tick
                .transactions
                .iter()
                .any(|existing| existing.hash() == hash)
            {
                tick.transactions.push(transaction);
            }
        }
        let (_, missing) = missing_transaction_flags(&tick.tick_data, &tick.transactions);
        if missing == 0 {
            return Ok(());
        }
        let Some(pool) = fallback else {
            return Err(Error::Verification(format!(
                "tick {} is missing {missing} transaction bodies",
                tick.tick
            )));
        };
        let (known, _) = missing_transaction_flags(&tick.tick_data, &tick.transactions);
        let fetched = pool.transactions(tick.tick, &known).await?;
        for transaction in fetched {
            let hash = transaction.hash();
            if !tick
                .transactions
                .iter()
                .any(|existing| existing.hash() == hash)
            {
                tick.transactions.push(transaction);
            }
        }
        let (_, missing) = missing_transaction_flags(&tick.tick_data, &tick.transactions);
        if missing == 0 {
            return Ok(());
        }
        Err(Error::Verification(format!(
            "tick {} is missing {missing} transaction bodies",
            tick.tick
        )))
    }

    async fn prefetch_logs(
        &mut self,
        decoded: &[Option<DecodedTick>],
    ) -> (
        Vec<LogRanges>,
        Vec<Vec<LogEvent>>,
        Vec<bool>,
        Vec<Vec<String>>,
    ) {
        let count = decoded.len();
        let mut ranges = vec![empty_log_ranges(); count];
        let mut logs = (0..count).map(|_| Vec::new()).collect::<Vec<_>>();
        let mut known = vec![false; count];
        let mut warnings = (0..count).map(|_| Vec::new()).collect::<Vec<_>>();

        let mut need_fallback = vec![false; count];
        let mut all_ranges_ticks = Vec::new();
        for (index, tick) in decoded.iter().enumerate() {
            if tick.is_none() {
                continue;
            }
            if self.log_ranges_supported == Some(false) {
                need_fallback[index] = true;
            } else {
                all_ranges_ticks.push(index);
            }
        }

        if !all_ranges_ticks.is_empty() {
            let mut awaiting = HashMap::with_capacity(all_ranges_ticks.len());
            let mut send_failed = false;
            for &index in &all_ranges_ticks {
                let tick = decoded[index].as_ref().expect("decoded tick").tick;
                match self
                    .send(
                        MessageType::RequestAllLogIdRangesFromTx,
                        request_all_log_ranges(tick, self.config.passcode),
                    )
                    .await
                {
                    Ok(dejavu) => {
                        awaiting.insert(dejavu, index);
                    }
                    Err(error) => {
                        self.clear_pending(&awaiting);
                        self.desynced = true;
                        for &index in &all_ranges_ticks {
                            need_fallback[index] = true;
                            warnings[index].push(format!(
                                "peer {} could not request log ranges: {error}",
                                self.config.addr
                            ));
                        }
                        send_failed = true;
                        break;
                    }
                }
            }

            if !send_failed {
                let deadline = Instant::now()
                    + self
                        .config
                        .log_range_timeout
                        .saturating_mul(2)
                        .max(Duration::from_millis(500));
                let mut successes = 0usize;
                while !awaiting.is_empty() {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        self.clear_pending(&awaiting);
                        self.desynced = true;
                        for (_, index) in awaiting.drain() {
                            need_fallback[index] = true;
                        }
                        break;
                    }
                    let packet = match self.read_packet_timeout(remaining).await {
                        Ok(packet) => packet,
                        Err(_) => {
                            self.clear_pending(&awaiting);
                            self.desynced = true;
                            for (_, index) in awaiting.drain() {
                                need_fallback[index] = true;
                            }
                            break;
                        }
                    };
                    if packet.kind == MessageType::ExchangePublicPeers {
                        self.record_peers(&packet);
                        continue;
                    }
                    let Some(&index) = awaiting.get(&packet.dejavu) else {
                        continue;
                    };
                    awaiting.remove(&packet.dejavu);
                    self.pending.remove(&packet.dejavu);
                    match packet.kind {
                        MessageType::RespondAllLogIdRangesFromTx => {
                            match LogRanges::decode(&packet.payload) {
                                Ok(decoded_ranges) => {
                                    ranges[index] = decoded_ranges;
                                    known[index] = true;
                                    successes += 1;
                                }
                                Err(error) => {
                                    need_fallback[index] = true;
                                    warnings[index].push(format!(
                                        "peer {} returned invalid log ranges: {error}",
                                        self.config.addr
                                    ));
                                }
                            }
                        }
                        MessageType::EndResponse | MessageType::TryAgain => {
                            need_fallback[index] = true;
                        }
                        _ => {
                            need_fallback[index] = true;
                        }
                    }
                }
                if successes == 0 {
                    self.set_log_ranges_supported(
                        false,
                        "batch all-ranges log request returned no usable range",
                    );
                    for &index in &all_ranges_ticks {
                        need_fallback[index] = true;
                    }
                } else {
                    self.set_log_ranges_supported(true, "batch all-ranges log request succeeded");
                }
            }
        }

        let mut probes = Vec::new();
        let mut expected = vec![0usize; count];
        for (index, tick) in decoded.iter().enumerate() {
            if !need_fallback[index] {
                continue;
            }
            let Some(tick) = tick else { continue };
            for probe in log_probe_indices(&tick.tick_data) {
                probes.push((index, probe));
                expected[index] += 1;
            }
        }
        for index in 0..count {
            if need_fallback[index] && expected[index] == 0 {
                known[index] = true;
            }
        }
        if !probes.is_empty() {
            let mut awaiting = HashMap::with_capacity(probes.len());
            let mut answered = vec![0usize; count];
            let mut send_failed = false;
            for &(index, probe) in &probes {
                let tick = decoded[index].as_ref().expect("decoded tick").tick;
                match self
                    .send(
                        MessageType::RequestLogIdRangeFromTx,
                        request_log_id_range_from_tx(tick, probe as u32, self.config.passcode),
                    )
                    .await
                {
                    Ok(dejavu) => {
                        awaiting.insert(dejavu, (index, probe));
                    }
                    Err(error) => {
                        self.clear_pending(&awaiting);
                        self.desynced = true;
                        warnings[index].push(format!(
                            "peer {} could not probe log ranges: {error}",
                            self.config.addr
                        ));
                        send_failed = true;
                        break;
                    }
                }
            }
            if !send_failed {
                // The fallback probes are pipelined, but a peer still has to
                // process one request per slot. Scale the budget with the
                // batch so a large catch-up batch does not lose its logs to a
                // timeout tuned for a single tick.
                let deadline = Instant::now()
                    + self
                        .config
                        .log_fallback_probe_timeout
                        .saturating_mul((probes.len() / 16).clamp(1, 32) as u32)
                        .max(Duration::from_secs(1))
                        .min(Duration::from_secs(20));
                while !awaiting.is_empty() {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        self.clear_pending(&awaiting);
                        self.desynced = true;
                        break;
                    }
                    let packet = match self.read_packet_timeout(remaining).await {
                        Ok(packet) => packet,
                        Err(_) => {
                            self.clear_pending(&awaiting);
                            self.desynced = true;
                            break;
                        }
                    };
                    if packet.kind == MessageType::ExchangePublicPeers {
                        self.record_peers(&packet);
                        continue;
                    }
                    let Some(&(index, probe)) = awaiting.get(&packet.dejavu) else {
                        continue;
                    };
                    awaiting.remove(&packet.dejavu);
                    self.pending.remove(&packet.dejavu);
                    match packet.kind {
                        MessageType::RespondLogIdRangeFromTx => {
                            match decode_log_id_range(&packet.payload) {
                                Ok((from, length)) => {
                                    ranges[index].from_log_id[probe] = from;
                                    ranges[index].length[probe] = length;
                                    answered[index] += 1;
                                }
                                Err(error) => warnings[index].push(format!(
                                    "peer {} returned invalid log range: {error}",
                                    self.config.addr
                                )),
                            }
                        }
                        MessageType::EndResponse => {
                            answered[index] += 1;
                        }
                        MessageType::TryAgain => {}
                        _ => {}
                    }
                }
            }
            for index in 0..count {
                if need_fallback[index] && expected[index] > 0 && answered[index] == 0 {
                    warnings[index].push(format!(
                        "peer {} returned no log ranges for tick {}",
                        self.config.addr,
                        decoded[index]
                            .as_ref()
                            .map(|tick| tick.tick.to_string())
                            .unwrap_or_default()
                    ));
                }
                if need_fallback[index] && answered[index] > 0 && answered[index] < expected[index]
                {
                    warnings[index].push(format!(
                        "peer {} answered {}/{} log range probes for tick {}",
                        self.config.addr,
                        answered[index],
                        expected[index],
                        decoded[index]
                            .as_ref()
                            .map(|tick| tick.tick.to_string())
                            .unwrap_or_default()
                    ));
                }
                if expected[index] == 0 || answered[index] >= expected[index] {
                    known[index] = true;
                }
            }
        }
        if self.log_ranges_supported == Some(false) {
            for index in 0..count {
                if decoded[index]
                    .as_ref()
                    .is_some_and(|tick| tick.tick_data.non_empty_transactions().next().is_some())
                    && ranges[index].count() == 0
                {
                    known[index] = false;
                    warnings[index].push(format!(
                        "log data unavailable from peer {} for tick {}: \
                         fallback returned no log ranges",
                        self.config.addr,
                        decoded[index]
                            .as_ref()
                            .map(|tick| tick.tick.to_string())
                            .unwrap_or_default()
                    ));
                }
            }
        }

        let mut pages = Vec::new();
        for index in 0..count {
            let Some(tick) = decoded[index].as_ref() else {
                continue;
            };
            let Some((from, to)) = ranges[index].range() else {
                continue;
            };
            if from < 0 || to < from {
                continue;
            }
            if ranges[index].is_pending() {
                warnings[index].push(format!(
                    "peer {} reports log generation still pending for tick {}",
                    self.config.addr, tick.tick
                ));
            }
            pages.push(PageState {
                tick_index: index,
                cursor: from as u64,
                to: to as u64,
                pages: 0,
                done: false,
            });
        }

        while pages.iter().any(|page| !page.done) {
            let mut awaiting = HashMap::with_capacity(pages.len());
            for (page_index, page) in pages.iter_mut().enumerate() {
                if page.done {
                    continue;
                }
                if page.cursor > page.to {
                    page.done = true;
                    continue;
                }
                if page.pages >= MAX_LOG_PAGES {
                    page.done = true;
                    warnings[page.tick_index].push(format!(
                        "log range for tick {} exceeded {MAX_LOG_PAGES} pages",
                        decoded[page.tick_index]
                            .as_ref()
                            .map(|tick| tick.tick.to_string())
                            .unwrap_or_default()
                    ));
                    continue;
                }
                match self
                    .send(
                        MessageType::RequestLog,
                        request_logs(page.cursor, page.to, self.config.passcode),
                    )
                    .await
                {
                    Ok(dejavu) => {
                        awaiting.insert(dejavu, page_index);
                        page.pages += 1;
                    }
                    Err(error) => {
                        self.desynced = true;
                        page.done = true;
                        warnings[page.tick_index].push(format!(
                            "peer {} could not request logs: {error}",
                            self.config.addr
                        ));
                    }
                }
            }
            if awaiting.is_empty() {
                break;
            }
            let active = pages.iter().filter(|page| !page.done).count().max(1);
            let deadline = Instant::now()
                + self
                    .config
                    .timeout
                    .saturating_mul(active.min(16) as u32)
                    .min(Duration::from_secs(20));
            while !awaiting.is_empty() {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    self.clear_pending(&awaiting);
                    self.desynced = true;
                    for (_, page_index) in awaiting.drain() {
                        pages[page_index].done = true;
                    }
                    break;
                }
                let packet = match self.read_packet_timeout(remaining).await {
                    Ok(packet) => packet,
                    Err(_) => {
                        self.clear_pending(&awaiting);
                        self.desynced = true;
                        for (_, page_index) in awaiting.drain() {
                            pages[page_index].done = true;
                        }
                        break;
                    }
                };
                if packet.kind == MessageType::ExchangePublicPeers {
                    self.record_peers(&packet);
                    continue;
                }
                let Some(&page_index) = awaiting.get(&packet.dejavu) else {
                    continue;
                };
                awaiting.remove(&packet.dejavu);
                self.pending.remove(&packet.dejavu);
                let page = &mut pages[page_index];
                match packet.kind {
                    MessageType::RespondLog => match LogEvent::decode_many(&packet.payload) {
                        Ok(events) => {
                            let mut highest = None;
                            for event in events {
                                if event.log_id > page.to {
                                    break;
                                }
                                if event.log_id >= page.cursor {
                                    highest = Some(event.log_id);
                                    logs[page.tick_index].push(event);
                                }
                            }
                            match highest {
                                Some(id) => page.cursor = id.saturating_add(1),
                                None => page.done = true,
                            }
                        }
                        Err(error) => {
                            page.done = true;
                            warnings[page.tick_index].push(format!(
                                "peer {} returned invalid log records: {error}",
                                self.config.addr
                            ));
                        }
                    },
                    MessageType::EndResponse => page.done = true,
                    MessageType::TryAgain => {
                        page.done = true;
                        warnings[page.tick_index].push(format!(
                            "peer {} asked to retry log records for tick {}",
                            self.config.addr,
                            decoded[page.tick_index]
                                .as_ref()
                                .map(|tick| tick.tick.to_string())
                                .unwrap_or_default()
                        ));
                    }
                    _ => page.done = true,
                }
            }
        }

        (ranges, logs, known, warnings)
    }

    fn prefetch_timeout(&self, ticks: usize) -> Duration {
        self.config
            .timeout
            .saturating_mul(ticks.clamp(1, 16) as u32)
            .min(Duration::from_secs(60))
    }

    fn clear_pending<T>(&mut self, awaiting: &HashMap<u32, T>) {
        for dejavu in awaiting.keys() {
            self.pending.remove(dejavu);
        }
    }
}

struct PageState {
    tick_index: usize,
    cursor: u64,
    to: u64,
    pages: u32,
    done: bool,
}

fn finish_stream(state: &mut MainState, stream: MainStream) {
    state.done[stream.index()] = true;
}

fn fail_tick(
    states: &mut [MainState],
    awaiting: &mut HashMap<u32, (usize, MainStream)>,
    index: usize,
    error: Error,
) {
    if states[index].error.is_none() {
        states[index].error = Some(error);
    }
    let stale = awaiting
        .iter()
        .filter(|(_, (tick_index, _))| *tick_index == index)
        .map(|(dejavu, _)| *dejavu)
        .collect::<Vec<_>>();
    for dejavu in stale {
        awaiting.remove(&dejavu);
    }
}

struct MainParts {
    tick: u32,
    votes: Vec<Tick>,
    transactions: Vec<Transaction>,
    tick_data: Option<TickData>,
}

fn decode_main_parts(mut state: MainState) -> Result<MainParts> {
    let votes = decode_votes(std::mem::take(&mut state.votes), state.tick)?;
    let transactions = decode_transactions(std::mem::take(&mut state.transactions), state.tick)?;
    let tick_data = state
        .tick_data
        .map(|packet| TickData::decode(&packet.payload))
        .transpose()?;
    Ok(MainParts {
        tick: state.tick,
        votes,
        transactions,
        tick_data,
    })
}

fn finish_decoded(part: MainParts) -> Result<DecodedTick> {
    let tick_data = match part.tick_data {
        Some(tick_data) => tick_data,
        None => empty_tick_from_votes(&part.votes, part.tick)?,
    };
    if tick_data.tick != part.tick {
        return Err(Error::Verification(format!(
            "tick data is for tick {}, requested {}",
            tick_data.tick, part.tick
        )));
    }
    first_vote_or_synthetic(&part.votes, &tick_data, part.tick)?;
    Ok(DecodedTick {
        tick: part.tick,
        tick_data,
        votes: part.votes,
        transactions: part.transactions,
        warnings: Vec::new(),
    })
}

/// Builds the `known` bitmap for `RequestTickTransactions` and counts the
/// non-empty tick-data slots whose body is still missing.
fn missing_transaction_flags(
    tick_data: &TickData,
    transactions: &[Transaction],
) -> ([bool; TX_PER_TICK], usize) {
    let hashes = transactions
        .iter()
        .map(Transaction::hash)
        .collect::<std::collections::HashSet<_>>();
    let mut known = [true; TX_PER_TICK];
    let mut missing = 0usize;
    for (index, digest) in tick_data.non_empty_transactions() {
        if !hashes.contains(digest) {
            known[index] = false;
            missing += 1;
        }
    }
    (known, missing)
}
