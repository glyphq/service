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
    COMPUTOR_COUNT, LogRanges, TX_PER_TICK, Tick, TickData, Transaction, request_all_log_ranges,
    request_logs, request_quorum_tick, request_tick_data, request_tick_transactions,
};
use qubic::wire::{MessageType, Packet};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Bound a batch so one worker cannot buffer an unbounded number of votes or
/// log probes. The dispatcher uses a smaller default.
const MAX_PREFETCH_TICKS: usize = 256;

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
        for (index, mut state) in states.into_iter().enumerate() {
            match state.error.take() {
                Some(error) => results[index] = Some(Err(error)),
                None => match decode_main_parts(state) {
                    Ok(mut part) => {
                        if part.tick_data.is_none() {
                            if part.votes.iter().any(|vote| !vote.is_empty_vote())
                                || part.votes.is_empty()
                            {
                                missing.push((index, part.tick));
                            } else {
                                part.tick_data = empty_tick_from_votes(&part.votes, part.tick).ok();
                            }
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

        if !missing.is_empty() {
            if let Some(archive) = self.archive.clone() {
                let head = match archive.head().await {
                    Ok(head) => Some(head),
                    Err(error) => {
                        tracing::warn!(%error, "public archive head is unavailable");
                        None
                    }
                };
                let mut tasks = tokio::task::JoinSet::new();
                for (index, tick) in missing {
                    let archive = archive.clone();
                    tasks.spawn(async move {
                        let result = match head {
                            Some(head) => archive.tick_data_at(tick, head).await,
                            None => {
                                Err(Error::ArchiveNotReady("archive head is unavailable".into()))
                            }
                        };
                        (index, tick, result)
                    });
                }
                while let Some(joined) = tasks.join_next().await {
                    match joined {
                        Ok((index, _tick, Ok(tick_data))) => {
                            if let Some(part) = parts[index].as_mut() {
                                part.tick_data = Some(tick_data);
                            }
                        }
                        Ok((index, tick, Err(error))) => {
                            tracing::warn!(
                                tick,
                                %error,
                                "public archive could not supply missing tick data"
                            );
                            results[index] = Some(Err(match error {
                                Error::ArchiveNotAvailable(_) => {
                                    Error::EndResponse(MessageType::RequestTickData)
                                }
                                error => error,
                            }));
                        }
                        Err(error) => {
                            tracing::warn!(%error, "archive tick-data task failed");
                        }
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

        let (log_ranges, logs, log_warnings) = self.prefetch_logs(&decoded).await;
        let mut log_ranges = log_ranges.into_iter();
        let mut logs = logs.into_iter();
        let mut log_warnings = log_warnings.into_iter();
        for (index, decoded) in decoded.into_iter().enumerate() {
            let ranges = log_ranges.next().expect("one range entry per tick");
            let tick_logs = logs.next().expect("one log entry per tick");
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

    async fn prefetch_logs(
        &mut self,
        decoded: &[Option<DecodedTick>],
    ) -> (Vec<LogRanges>, Vec<Vec<LogEvent>>, Vec<Vec<String>>) {
        let count = decoded.len();
        let mut ranges = vec![empty_log_ranges(); count];
        let mut logs = (0..count).map(|_| Vec::new()).collect::<Vec<_>>();
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
        for (index, tick) in decoded.iter().enumerate() {
            if !need_fallback[index] {
                continue;
            }
            let Some(tick) = tick else { continue };
            for probe in log_probe_indices(&tick.tick_data) {
                probes.push((index, probe));
            }
        }
        if !probes.is_empty() {
            let mut awaiting = HashMap::with_capacity(probes.len());
            let mut answered = vec![false; count];
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
                                    answered[index] = true;
                                }
                                Err(error) => warnings[index].push(format!(
                                    "peer {} returned invalid log range: {error}",
                                    self.config.addr
                                )),
                            }
                        }
                        MessageType::EndResponse | MessageType::TryAgain => {}
                        _ => {}
                    }
                }
            }
            for index in 0..count {
                if need_fallback[index] && !answered[index] {
                    warnings[index].push(format!(
                        "peer {} returned no log ranges for tick {}",
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

        (ranges, logs, warnings)
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

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    #[tokio::test]
    #[ignore = "connects to a live Qubic peer and the public archive"]
    async fn prefetch_recovers_missing_tick_data_from_archive() {
        let config = PeerConfig::new("198.27.75.81:21841".parse::<SocketAddr>().unwrap())
            .with_archive_url("https://rpc.qubic.org/query/v1");
        let mut peer = Peer::connect(config).await.expect("peer");
        let head = peer.head().await.expect("head");
        let computors = peer.computors().await.expect("computors");
        let bundles = peer
            .prefetch_bundles(&[92_261_883], head, computors)
            .await
            .expect("prefetch");
        let bundle = bundles
            .into_iter()
            .next()
            .expect("one bundle")
            .expect("tick");
        assert_eq!(bundle.tick_data.tick, 92_261_883);
        assert!(bundle.tick_data.non_empty_transactions().count() > 0);
    }
}
