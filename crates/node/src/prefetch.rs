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
/// Bound one per-slot range batch. A dense 64-tick batch can otherwise queue
/// hundreds of thousands of tiny requests and overrun a peer's response queue.
const MAX_LOG_PROBES: usize = 32_768;

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

        let mut log_fetches = self.prefetch_logs(&decoded).await;
        let incomplete = (0..decoded.len())
            .filter(|index| decoded[*index].is_some() && !log_fetches[*index].complete)
            .collect::<Vec<_>>();
        if !incomplete.is_empty() {
            if let Some(pool) = fallback.as_ref() {
                // Repair the whole affected set over one connection. Opening
                // a socket per tick turns an empty-tick run into a connection
                // storm against every peer.
                let indices = incomplete
                    .iter()
                    .copied()
                    .filter(|index| decoded[*index].is_some())
                    .collect::<Vec<_>>();
                let data = indices
                    .iter()
                    .filter_map(|index| decoded[*index].as_ref())
                    .map(|tick| tick.tick_data.clone())
                    .collect::<Vec<_>>();
                match pool.log_batch(&data).await {
                    Ok(repaired) => {
                        for (index, fetched) in indices.into_iter().zip(repaired) {
                            if fetched.complete {
                                log_fetches[index] = fetched;
                            } else {
                                log_fetches[index].warnings.extend(fetched.warnings);
                            }
                        }
                    }
                    Err(error) => {
                        for index in indices {
                            log_fetches[index].warnings.push(format!(
                                "no peer supplied complete logs for the batch: {error}"
                            ));
                        }
                    }
                }
            }
        }
        let mut log_fetches = log_fetches.into_iter();
        for (index, decoded) in decoded.into_iter().enumerate() {
            let fetched_logs = log_fetches.next().expect("one log result per tick");
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
            let logs_complete = fetched_logs.complete;
            decoded_warnings.extend(fetched_logs.warnings);
            results[index] = Some(Ok(TickBundle {
                head: head.clone(),
                computors: computors.clone(),
                tick,
                tick_data,
                votes,
                transactions,
                log_ranges: fetched_logs.ranges,
                logs: fetched_logs.logs,
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

    async fn prefetch_logs(&mut self, decoded: &[Option<DecodedTick>]) -> Vec<FetchedLogs> {
        let count = decoded.len();
        let mut ranges: Vec<Option<LogRanges>> = vec![None; count];
        let mut logs = (0..count).map(|_| Vec::new()).collect::<Vec<_>>();
        let mut warnings = (0..count).map(|_| Vec::new()).collect::<Vec<_>>();

        let Some(first_tick) = decoded.iter().flatten().next().cloned() else {
            return (0..count)
                .map(|_| FetchedLogs::incomplete("tick data unavailable"))
                .collect();
        };
        let protocol = match self.log_protocol(&first_tick.tick_data).await {
            Ok(protocol) => protocol,
            Err(error) => {
                for warning in warnings.iter_mut() {
                    warning.push(format!(
                        "peer {} could not determine log protocol: {error}",
                        self.config.addr
                    ));
                }
                return (0..count)
                    .map(|_| FetchedLogs::incomplete("log protocol unavailable"))
                    .collect();
            }
        };
        match protocol {
            LogProtocol::AllRanges => {
                self.fetch_all_log_ranges(decoded, &mut ranges, &mut warnings)
                    .await;
            }
            LogProtocol::TxRanges => {
                self.fetch_tx_log_ranges(decoded, &mut ranges, &mut warnings)
                    .await;
            }
            LogProtocol::NoLogging => {
                for (index, tick) in decoded.iter().enumerate() {
                    if tick.is_some() {
                        warnings[index].push(format!(
                            "peer {} does not expose log ranges",
                            self.config.addr
                        ));
                    }
                }
            }
        }

        let mut pages = Vec::new();
        for index in 0..count {
            let Some(tick) = decoded[index].as_ref() else {
                continue;
            };
            let Some(tick_ranges) = ranges[index].as_ref() else {
                continue;
            };
            if tick_ranges.is_pending() {
                warnings[index].push(format!(
                    "peer {} reports log generation still pending for tick {}",
                    self.config.addr, tick.tick
                ));
                continue;
            }
            let Some((from, to)) = tick_ranges.range() else {
                continue;
            };
            if from < 0 || to < from {
                continue;
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
            // One idle timeout covers every pipelined log request. A peer that
            // has the data answers promptly; waiting for the slowest peer in a
            // large batch is what made catch-up stall.
            let idle = self
                .config
                .log_range_timeout
                .saturating_mul(2)
                .max(Duration::from_secs(1));
            while !awaiting.is_empty() {
                let packet = match self.read_packet_timeout(idle).await {
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

        let mut out = Vec::with_capacity(count);
        for index in 0..count {
            if decoded[index].is_none() {
                out.push(FetchedLogs::incomplete("tick data unavailable"));
                continue;
            }
            let Some(tick_ranges) = ranges[index].take() else {
                out.push(FetchedLogs {
                    ranges: LogRanges::empty(),
                    logs: std::mem::take(&mut logs[index]),
                    complete: false,
                    warnings: std::mem::take(&mut warnings[index]),
                });
                continue;
            };
            let complete = tick_ranges.covers(&logs[index]);
            if !complete && warnings[index].is_empty() {
                warnings[index].push(format!(
                    "peer {} returned {}/{} log records for tick {}",
                    self.config.addr,
                    logs[index].len(),
                    tick_ranges.count(),
                    decoded[index]
                        .as_ref()
                        .map(|tick| tick.tick.to_string())
                        .unwrap_or_default()
                ));
            }
            out.push(FetchedLogs {
                ranges: tick_ranges,
                logs: std::mem::take(&mut logs[index]),
                complete,
                warnings: std::mem::take(&mut warnings[index]),
            });
        }
        out
    }

    /// Fetches one all-ranges response per tick. Used by peers that implement
    /// `RequestAllLogIdRangesFromTick` (core-bob and recent upstream cores).
    async fn fetch_all_log_ranges(
        &mut self,
        decoded: &[Option<DecodedTick>],
        ranges: &mut [Option<LogRanges>],
        warnings: &mut [Vec<String>],
    ) {
        let requested = decoded
            .iter()
            .enumerate()
            .filter_map(|(index, tick)| tick.as_ref().map(|_| index))
            .collect::<Vec<_>>();
        let mut awaiting = HashMap::with_capacity(requested.len());
        for &index in &requested {
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
                    for &pending in &requested {
                        warnings[pending].push(format!(
                            "peer {} could not request log ranges: {error}",
                            self.config.addr
                        ));
                    }
                    return;
                }
            }
        }

        while !awaiting.is_empty() {
            let packet = match self
                .read_packet_timeout(self.config.log_range_timeout)
                .await
            {
                Ok(packet) => packet,
                Err(_) => {
                    self.clear_pending(&awaiting);
                    self.desynced = true;
                    for (_, index) in awaiting.drain() {
                        warnings[index].push(format!(
                            "peer {} did not answer log ranges for tick {}",
                            self.config.addr,
                            decoded[index]
                                .as_ref()
                                .map(|tick| tick.tick.to_string())
                                .unwrap_or_default()
                        ));
                    }
                    break;
                }
            };
            if packet.kind == MessageType::ExchangePublicPeers {
                self.record_peers(&packet);
                continue;
            }
            let Some(index) = awaiting.remove(&packet.dejavu) else {
                continue;
            };
            self.pending.remove(&packet.dejavu);
            match packet.kind {
                MessageType::RespondAllLogIdRangesFromTx => {
                    match LogRanges::decode(&packet.payload).and_then(|decoded_ranges| {
                        decoded_ranges.validate().map(|()| decoded_ranges)
                    }) {
                        Ok(decoded_ranges) => ranges[index] = Some(decoded_ranges),
                        Err(error) => warnings[index].push(format!(
                            "peer {} returned invalid log ranges: {error}",
                            self.config.addr
                        )),
                    }
                }
                MessageType::EndResponse => warnings[index].push(format!(
                    "peer {} has no verified log ranges for tick {}",
                    self.config.addr,
                    decoded[index]
                        .as_ref()
                        .map(|tick| tick.tick.to_string())
                        .unwrap_or_default()
                )),
                MessageType::TryAgain => warnings[index].push(format!(
                    "peer {} asked to retry log ranges for tick {}",
                    self.config.addr,
                    decoded[index]
                        .as_ref()
                        .map(|tick| tick.tick.to_string())
                        .unwrap_or_default()
                )),
                actual => warnings[index].push(format!(
                    "peer {} returned {actual:?} instead of log ranges for tick {}",
                    self.config.addr,
                    decoded[index]
                        .as_ref()
                        .map(|tick| tick.tick.to_string())
                        .unwrap_or_default()
                )),
            }
        }
    }

    /// Fetches per-slot ranges. Used by upstream peers that implement
    /// `RequestLogIdRangeFromTx`. Only a 16-byte response counts as proof;
    /// `EndResponse` means the peer cannot answer for this tick and must not be
    /// interpreted as an empty log set.
    async fn fetch_tx_log_ranges(
        &mut self,
        decoded: &[Option<DecodedTick>],
        ranges: &mut [Option<LogRanges>],
        warnings: &mut [Vec<String>],
    ) {
        let count = decoded.len();
        let mut expected = vec![0usize; count];
        let mut answered = vec![0usize; count];
        let mut failed = vec![false; count];
        let mut awaiting = HashMap::new();
        for (index, tick) in decoded.iter().enumerate() {
            let Some(tick) = tick.as_ref() else {
                continue;
            };
            let indices = log_probe_indices(&tick.tick_data);
            expected[index] = indices.len();
            ranges[index] = Some(LogRanges::empty());
            for slot in indices {
                if awaiting.len() >= MAX_LOG_PROBES {
                    failed[index] = true;
                    warnings[index].push(format!(
                        "tick {} exceeded {MAX_LOG_PROBES} log range probes",
                        tick.tick
                    ));
                    break;
                }
                match self
                    .send(
                        MessageType::RequestLogIdRangeFromTx,
                        qubic::messages::request_log_id_range_from_tx(
                            tick.tick,
                            slot as u32,
                            self.config.passcode,
                        ),
                    )
                    .await
                {
                    Ok(dejavu) => {
                        awaiting.insert(dejavu, (index, slot));
                    }
                    Err(error) => {
                        self.clear_pending(&awaiting);
                        self.desynced = true;
                        for (pending, _) in decoded.iter().enumerate() {
                            if pending >= index {
                                warnings[pending].push(format!(
                                    "peer {} could not probe log ranges: {error}",
                                    self.config.addr
                                ));
                            }
                        }
                        return;
                    }
                }
            }
        }

        let idle = self
            .config
            .log_range_timeout
            .saturating_mul(2)
            .max(Duration::from_secs(1));
        while !awaiting.is_empty() {
            let packet = match self.read_packet_timeout(idle).await {
                Ok(packet) => packet,
                Err(_) => {
                    self.clear_pending(&awaiting);
                    self.desynced = true;
                    for (_, (index, _)) in awaiting.drain() {
                        if !failed[index] {
                            failed[index] = true;
                            warnings[index].push(format!(
                                "peer {} timed out while probing log ranges for tick {}",
                                self.config.addr,
                                decoded[index]
                                    .as_ref()
                                    .map(|tick| tick.tick.to_string())
                                    .unwrap_or_default()
                            ));
                        }
                    }
                    break;
                }
            };
            if packet.kind == MessageType::ExchangePublicPeers {
                self.record_peers(&packet);
                continue;
            }
            let Some((index, slot)) = awaiting.remove(&packet.dejavu) else {
                continue;
            };
            self.pending.remove(&packet.dejavu);
            match packet.kind {
                MessageType::RespondLogIdRangeFromTx => {
                    if packet.payload.len() != 16 {
                        failed[index] = true;
                        warnings[index].push(format!(
                            "peer {} returned a {}-byte log range for tick {}",
                            self.config.addr,
                            packet.payload.len(),
                            decoded[index]
                                .as_ref()
                                .map(|tick| tick.tick.to_string())
                                .unwrap_or_default()
                        ));
                        continue;
                    }
                    let from =
                        i64::from_le_bytes(packet.payload[0..8].try_into().expect("8 bytes"));
                    let length =
                        i64::from_le_bytes(packet.payload[8..16].try_into().expect("8 bytes"));
                    if from == -3 || length == -3 {
                        failed[index] = true;
                        warnings[index].push(format!(
                            "peer {} has not generated logs for tick {}",
                            self.config.addr,
                            decoded[index]
                                .as_ref()
                                .map(|tick| tick.tick.to_string())
                                .unwrap_or_default()
                        ));
                        continue;
                    }
                    if let Some(tick_ranges) = ranges[index].as_mut() {
                        tick_ranges.from_log_id[slot] = from;
                        tick_ranges.length[slot] = length;
                    }
                    answered[index] += 1;
                }
                MessageType::EndResponse => {
                    failed[index] = true;
                    warnings[index].push(format!(
                        "peer {} rejected a log range probe for tick {}",
                        self.config.addr,
                        decoded[index]
                            .as_ref()
                            .map(|tick| tick.tick.to_string())
                            .unwrap_or_default()
                    ));
                }
                MessageType::TryAgain => {
                    failed[index] = true;
                    warnings[index].push(format!(
                        "peer {} asked to retry a log range probe for tick {}",
                        self.config.addr,
                        decoded[index]
                            .as_ref()
                            .map(|tick| tick.tick.to_string())
                            .unwrap_or_default()
                    ));
                }
                _ => {}
            }
        }

        for (index, tick) in decoded.iter().enumerate() {
            if tick.is_none() {
                continue;
            }
            if failed[index] || answered[index] != expected[index] {
                ranges[index] = None;
                if warnings[index].is_empty() {
                    warnings[index].push(format!(
                        "peer {} answered {}/{} log range probes for tick {}",
                        self.config.addr,
                        answered[index],
                        expected[index],
                        tick.as_ref()
                            .map(|tick| tick.tick.to_string())
                            .unwrap_or_default()
                    ));
                }
            }
        }
    }

    /// Fetches one tick's log ranges and records on a fresh connection.
    /// Used by [`NodePool::logs`] when a batch peer cannot supply complete
    /// logs.
    pub async fn complete_logs(&mut self, tick_data: &TickData) -> Result<FetchedLogs> {
        let tick = DecodedTick {
            tick: tick_data.tick,
            tick_data: tick_data.clone(),
            votes: Vec::new(),
            transactions: Vec::new(),
            warnings: Vec::new(),
        };
        let mut fetched = self.prefetch_logs(&[Some(tick)]).await;
        Ok(fetched.pop().expect("one log result"))
    }

    /// Fetches logs for several ticks on one connection. Used by the pool to
    /// repair a whole batch without opening one socket per tick.
    pub async fn complete_logs_batch(
        &mut self,
        tick_data: &[TickData],
    ) -> Result<Vec<FetchedLogs>> {
        let decoded = tick_data
            .iter()
            .cloned()
            .map(|tick_data| {
                Some(DecodedTick {
                    tick: tick_data.tick,
                    tick_data,
                    votes: Vec::new(),
                    transactions: Vec::new(),
                    warnings: Vec::new(),
                })
            })
            .collect::<Vec<_>>();
        Ok(self.prefetch_logs(&decoded).await)
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
