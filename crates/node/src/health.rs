//! Cross-peer health tracking, discovery, and epoch-safety helpers.
//!
//! `NodePool` owns one [`PeerManager`], but the manager is deliberately
//! independent of the connection type: it only needs peer configs and socket
//! addresses. That makes it usable from tests and keeps the `NodePool` wiring
//! small.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use qubic::messages::{Computors, DEFAULT_PEER_PORT, PeerAddress};
use serde::Serialize;

use crate::{Error, Head, PeerConfig, Result};

/// Maximum number of peers retained after discovery.
pub const DEFAULT_DISCOVERY_LIMIT: usize = 64;

/// Tuning for cross-peer health tracking.
///
/// Scores are milliseconds where lower is better. A peer that fails
/// [`PeerPolicy::quarantine_after`] times in a row is skipped until its
/// quarantine expires. Each further failure doubles the quarantine up to
/// [`PeerPolicy::quarantine_max`].
#[derive(Debug, Clone)]
pub struct PeerPolicy {
    /// Consecutive failures before a peer is quarantined.
    pub quarantine_after: u32,
    /// Quarantine for the first failure that triggers it.
    pub quarantine_base: Duration,
    /// Upper bound for exponential quarantine growth.
    pub quarantine_max: Duration,
    /// Penalty added per consecutive failure, doubled for each extra failure.
    pub failure_penalty: Duration,
    /// Maximum score reduction for a peer that has not been tried recently.
    pub exploration_bonus: Duration,
    /// Time over which the exploration bonus ramps up.
    pub exploration_window: Duration,
    /// Score used until a peer has a measured latency.
    pub unknown_latency: Duration,
    /// EWMA weight for the newest latency sample (0..=1).
    pub latency_alpha: f64,
}

impl Default for PeerPolicy {
    fn default() -> Self {
        Self {
            quarantine_after: 3,
            quarantine_base: Duration::from_secs(30),
            quarantine_max: Duration::from_secs(300),
            failure_penalty: Duration::from_millis(250),
            exploration_bonus: Duration::from_millis(50),
            exploration_window: Duration::from_secs(60),
            unknown_latency: Duration::from_secs(1),
            latency_alpha: 0.3,
        }
    }
}

/// A point-in-time health view of one peer. Intended for operators and tests.
#[derive(Debug, Clone, Serialize)]
pub struct PeerHealth {
    pub addr: SocketAddr,
    /// Lower is better, in milliseconds.
    pub score: f64,
    pub successes: u64,
    pub failures: u64,
    pub consecutive_failures: u32,
    pub last_latency_ms: Option<u64>,
    /// EWMA of observed fetch throughput in milliseconds per tick.
    pub fetch_ms_per_tick: Option<f64>,
    /// Milliseconds until the quarantine ends, or `None` when available.
    pub quarantine_remaining_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct PeerState {
    successes: u64,
    failures: u64,
    consecutive_failures: u32,
    ewma_ms: Option<f64>,
    fetch_ms_per_tick: Option<f64>,
    log_failures: u32,
    head_epoch: Option<u16>,
    head_tick: Option<u32>,
    last_latency: Option<Duration>,
    last_attempt: Option<tokio::time::Instant>,
    quarantined_until: Option<tokio::time::Instant>,
}

/// Shared health bookkeeping used by every [`crate::NodePool`] handle.
///
/// Cloning a [`PeerManager`] or a pool shares the same state so a pool stored
/// in a long-lived API state remembers failures across requests.
#[derive(Debug, Clone)]
pub struct PeerManager {
    policy: PeerPolicy,
    state: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    rotation: Arc<AtomicU64>,
}

impl Default for PeerManager {
    fn default() -> Self {
        Self::new(PeerPolicy::default())
    }
}

impl PeerManager {
    pub fn new(policy: PeerPolicy) -> Self {
        Self {
            policy,
            state: Arc::new(Mutex::new(HashMap::new())),
            rotation: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn policy(&self) -> &PeerPolicy {
        &self.policy
    }

    /// Records a successful handshake or request and updates the EWMA latency.
    pub fn record_success(&self, addr: SocketAddr, latency: Duration) {
        let now = tokio::time::Instant::now();
        let mut state = self.state.lock().expect("peer health mutex poisoned");
        let entry = state.entry(addr).or_default();
        entry.successes = entry.successes.saturating_add(1);
        entry.consecutive_failures = 0;
        entry.last_latency = Some(latency);
        entry.last_attempt = Some(now);
        entry.quarantined_until = None;
        let sample = latency.as_secs_f64() * 1_000.0;
        let alpha = self.policy.latency_alpha.clamp(0.0, 1.0);
        entry.ewma_ms = Some(match entry.ewma_ms {
            Some(previous) => previous * (1.0 - alpha) + sample * alpha,
            None => sample,
        });
    }

    /// Records a failed handshake or request and extends the quarantine when
    /// the consecutive-failure threshold is reached.
    pub fn record_failure(&self, addr: SocketAddr) {
        let now = tokio::time::Instant::now();
        let mut state = self.state.lock().expect("peer health mutex poisoned");
        let entry = state.entry(addr).or_default();
        entry.failures = entry.failures.saturating_add(1);
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        entry.last_attempt = Some(now);
        if entry.consecutive_failures >= self.policy.quarantine_after {
            let extra = entry
                .consecutive_failures
                .saturating_sub(self.policy.quarantine_after)
                .min(16);
            let multiplier = 1u32 << extra;
            let duration = self
                .policy
                .quarantine_base
                .saturating_mul(multiplier)
                .min(self.policy.quarantine_max);
            entry.quarantined_until = Some(now + duration);
        }
    }

    /// Records a successful request without changing the handshake-latency
    /// EWMA. Long tick fetches must not make a healthy peer look slow.
    pub fn record_healthy(&self, addr: SocketAddr) {
        let now = tokio::time::Instant::now();
        let mut state = self.state.lock().expect("peer health mutex poisoned");
        let entry = state.entry(addr).or_default();
        entry.successes = entry.successes.saturating_add(1);
        entry.consecutive_failures = 0;
        entry.last_attempt = Some(now);
        entry.quarantined_until = None;
    }

    /// Records a completed fetch batch. Handshake latency alone cannot rank
    /// peers: a low-RTT peer can still deliver votes slowly because of packet
    /// loss or a narrow uplink.
    pub fn record_fetch(&self, addr: SocketAddr, ticks: usize, elapsed: Duration) {
        if ticks == 0 || elapsed.is_zero() {
            return;
        }
        let now = tokio::time::Instant::now();
        let mut state = self.state.lock().expect("peer health mutex poisoned");
        let entry = state.entry(addr).or_default();
        let sample = elapsed.as_secs_f64() * 1_000.0 / ticks as f64;
        entry.fetch_ms_per_tick = Some(match entry.fetch_ms_per_tick {
            Some(previous) => previous * 0.7 + sample * 0.3,
            None => sample,
        });
        entry.last_attempt = Some(now);
    }

    /// Records the head a peer reported. Peers behind the best observed head
    /// are stale and rank below the live set.
    pub fn record_head(&self, addr: SocketAddr, epoch: u16, tick: u32) {
        let mut state = self.state.lock().expect("peer health mutex poisoned");
        let entry = state.entry(addr).or_default();
        match (entry.head_epoch, entry.head_tick) {
            (Some(existing_epoch), Some(existing_tick))
                if existing_epoch > epoch || (existing_epoch == epoch && existing_tick >= tick) => {
            }
            _ => {
                entry.head_epoch = Some(epoch);
                entry.head_tick = Some(tick);
            }
        }
    }

    pub fn head_tick(&self, addr: SocketAddr) -> Option<u32> {
        let state = self.state.lock().expect("peer health mutex poisoned");
        state.get(&addr).and_then(|entry| entry.head_tick)
    }

    pub fn best_head(&self) -> Option<u32> {
        let state = self.state.lock().expect("peer health mutex poisoned");
        state.values().filter_map(|entry| entry.head_tick).max()
    }

    /// Penalizes a peer that cannot supply complete log data. The main tick
    /// streams stay usable, but workers should prefer peers that can finish a
    /// bundle without a per-tick retry.
    pub fn record_log_failure(&self, addr: SocketAddr) {
        let mut state = self.state.lock().expect("peer health mutex poisoned");
        let entry = state.entry(addr).or_default();
        entry.log_failures = entry.log_failures.saturating_add(1);
    }

    pub fn record_log_success(&self, addr: SocketAddr) {
        let mut state = self.state.lock().expect("peer health mutex poisoned");
        let entry = state.entry(addr).or_default();
        entry.log_failures = 0;
    }

    /// Whether `addr` is currently skipped. Expired quarantines are cleared.
    pub fn is_quarantined(&self, addr: SocketAddr) -> bool {
        let now = tokio::time::Instant::now();
        let mut state = self.state.lock().expect("peer health mutex poisoned");
        let Some(entry) = state.get_mut(&addr) else {
            return false;
        };
        match entry.quarantined_until {
            Some(until) if until > now => true,
            Some(_) => {
                entry.quarantined_until = None;
                false
            }
            None => false,
        }
    }

    /// Current score for `addr`.
    pub fn score(&self, addr: SocketAddr) -> f64 {
        let now = tokio::time::Instant::now();
        let state = self.state.lock().expect("peer health mutex poisoned");
        state
            .get(&addr)
            .map(|entry| self.score_at(entry, now))
            .unwrap_or(self.policy.unknown_latency.as_secs_f64() * 1_000.0)
    }

    /// Candidate peers ordered from best to worst, excluding quarantined peers.
    pub fn attempt_order(&self, peers: &[PeerConfig]) -> Vec<PeerConfig> {
        let now = tokio::time::Instant::now();
        let state = self.state.lock().expect("peer health mutex poisoned");
        let best_head = state.values().filter_map(|entry| entry.head_tick).max();
        let mut scored: Vec<(f64, PeerConfig)> = peers
            .iter()
            .filter(|config| {
                state
                    .get(&config.addr)
                    .and_then(|entry| entry.quarantined_until)
                    .is_none_or(|until| until <= now)
            })
            .map(|config| {
                let mut score = state
                    .get(&config.addr)
                    .map(|entry| self.score_at(entry, now))
                    .unwrap_or(self.policy.unknown_latency.as_secs_f64() * 1_000.0);
                if let Some(best) = best_head {
                    let head = state.get(&config.addr).and_then(|entry| entry.head_tick);
                    score += match head {
                        Some(head) => best.saturating_sub(head) as f64 * 20.0,
                        None => 100_000.0,
                    };
                }
                (score, config.clone())
            })
            .collect();
        scored.sort_by(|left, right| left.0.total_cmp(&right.0));
        // Workers should cycle across the healthy leaders instead of opening
        // every connection to the single lowest-latency peer. Only peers with
        // a clean success history join the rotation, so a stale address cannot
        // win a turn just because it has never been tried.
        let clean = scored
            .iter()
            .take(4)
            .take_while(|(_, config)| {
                state.get(&config.addr).is_some_and(|entry| {
                    entry.successes > 0
                        && entry.consecutive_failures == 0
                        && entry.log_failures == 0
                })
            })
            .count();
        if clean > 1 {
            let offset = self.rotation.fetch_add(1, Ordering::Relaxed) as usize % clean;
            scored[..clean].rotate_left(offset);
        }
        scored.into_iter().map(|(_, config)| config).collect()
    }

    /// Snapshot for operators, including peers that have never been contacted.
    pub fn report(&self, peers: &[PeerConfig]) -> Vec<PeerHealth> {
        let now = tokio::time::Instant::now();
        let state = self.state.lock().expect("peer health mutex poisoned");
        peers
            .iter()
            .map(|config| {
                let entry = state.get(&config.addr);
                let quarantine_remaining = entry
                    .and_then(|entry| entry.quarantined_until)
                    .filter(|until| *until > now)
                    .map(|until| until.saturating_duration_since(now).as_millis() as u64);
                PeerHealth {
                    addr: config.addr,
                    score: entry
                        .map(|entry| self.score_at(entry, now))
                        .unwrap_or(self.policy.unknown_latency.as_secs_f64() * 1_000.0),
                    successes: entry.map_or(0, |entry| entry.successes),
                    failures: entry.map_or(0, |entry| entry.failures),
                    consecutive_failures: entry.map_or(0, |entry| entry.consecutive_failures),
                    last_latency_ms: entry
                        .and_then(|entry| entry.last_latency)
                        .map(|latency| latency.as_millis() as u64),
                    fetch_ms_per_tick: entry.and_then(|entry| entry.fetch_ms_per_tick),
                    quarantine_remaining_ms: quarantine_remaining,
                }
            })
            .collect()
    }

    fn score_at(&self, entry: &PeerState, now: tokio::time::Instant) -> f64 {
        let latency = entry
            .ewma_ms
            .unwrap_or(self.policy.unknown_latency.as_secs_f64() * 1_000.0);
        let penalty = if entry.consecutive_failures == 0 {
            0.0
        } else {
            let exponent = entry.consecutive_failures.saturating_sub(1).min(16);
            self.policy.failure_penalty.as_secs_f64() * 1_000.0 * (1u32 << exponent) as f64
        };
        let age = entry
            .last_attempt
            .map(|attempt| now.saturating_duration_since(attempt))
            .unwrap_or(Duration::ZERO);
        let window = self
            .policy
            .exploration_window
            .as_secs_f64()
            .max(f64::EPSILON);
        let fraction = (age.as_secs_f64() / window).min(1.0);
        let bonus = self.policy.exploration_bonus.as_secs_f64() * 1_000.0 * fraction;
        let fetch = entry.fetch_ms_per_tick.unwrap_or(0.0);
        let log_penalty = f64::from(entry.log_failures) * 60_000.0;
        (latency + fetch + penalty + log_penalty - bonus).max(0.0)
    }
}

/// Returns configs for advertised peers that are not already configured.
///
/// The final configured list never exceeds `limit`. Legacy addresses without a
/// usable port fall back to [`DEFAULT_PEER_PORT`]. Unspecified IPs are dropped.
pub fn discovered_configs(
    existing: &[PeerConfig],
    advertised: &[PeerAddress],
    limit: usize,
) -> Vec<PeerConfig> {
    let mut seen: HashSet<SocketAddr> = existing.iter().map(|config| config.addr).collect();
    let mut added = Vec::new();
    for address in advertised {
        if existing.len().saturating_add(added.len()) >= limit {
            break;
        }
        let port = if address.port == 0 {
            DEFAULT_PEER_PORT
        } else {
            address.port
        };
        let socket = SocketAddr::from((address.ip, port));
        if socket.ip().is_unspecified() || !seen.insert(socket) {
            continue;
        }
        added.push(PeerConfig::new(socket));
    }
    added
}

/// True when `next` is older than `previous` in a way that cannot be a normal
/// forward catch-up. A higher epoch may legitimately have a lower tick, so only
/// a lower epoch or a backwards tick within the same epoch is a regression.
pub fn head_regressed(previous: &Head, next: &Head) -> bool {
    next.epoch < previous.epoch || (next.epoch == previous.epoch && next.tick < previous.tick)
}

/// Rejects a head that moved backwards for the same epoch or to an older epoch.
pub fn check_head(previous: Option<&Head>, next: &Head) -> Result<()> {
    if let Some(previous) = previous {
        if head_regressed(previous, next) {
            return Err(Error::NotSynchronized {
                head: next.tick,
                requested: previous.tick,
            });
        }
    }
    Ok(())
}

/// Rejects a caller-supplied, cached head that is older than the head this peer
/// already observed. This is the epoch-rollover guard for `bundle_with`: a
/// worker holding the previous epoch's head must refresh instead of fetching a
/// stale tick.
pub fn check_cached_head(observed: Option<&Head>, cached: &Head) -> Result<()> {
    if let Some(observed) = observed {
        // A same-epoch cached head below the peer's current head is still a
        // valid lower bound for historical ticks. Only a cached head from an
        // older epoch is unusable after rollover.
        if cached.epoch < observed.epoch {
            return Err(Error::NotSynchronized {
                head: observed.tick,
                requested: cached.tick,
            });
        }
    }
    Ok(())
}

/// Returns the computor set only when it belongs to `epoch`.
pub fn check_computors_epoch(computors: &Computors, epoch: u16) -> Result<()> {
    if computors.epoch != epoch {
        return Err(Error::NotSynchronized {
            head: u32::from(computors.epoch),
            requested: u32::from(epoch),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(octet: u8) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, octet], 21_841))
    }

    fn config(octet: u8) -> PeerConfig {
        PeerConfig::new(addr(octet))
    }

    fn head(epoch: u16, tick: u32) -> Head {
        Head {
            epoch,
            tick,
            initial_tick: 0,
            tick_duration: 1_000,
        }
    }

    fn fast_policy() -> PeerPolicy {
        PeerPolicy {
            quarantine_after: 3,
            quarantine_base: Duration::from_millis(40),
            quarantine_max: Duration::from_millis(160),
            failure_penalty: Duration::from_millis(10),
            exploration_bonus: Duration::from_millis(1),
            exploration_window: Duration::from_millis(20),
            unknown_latency: Duration::from_millis(100),
            latency_alpha: 0.5,
        }
    }

    #[tokio::test]
    async fn quarantine_skips_failing_peer_and_recovers_after_cooldown() {
        let manager = PeerManager::new(fast_policy());
        let failing = config(1);
        let healthy = config(2);
        manager.record_success(healthy.addr, Duration::from_millis(5));
        for _ in 0..3 {
            manager.record_failure(failing.addr);
        }

        assert!(manager.is_quarantined(failing.addr));
        let order = manager.attempt_order(&[failing.clone(), healthy.clone()]);
        assert_eq!(order.len(), 1);
        assert_eq!(order[0].addr, healthy.addr);

        let health = manager.report(std::slice::from_ref(&failing));
        assert!(health[0].quarantine_remaining_ms.is_some());

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(!manager.is_quarantined(failing.addr));
        let order = manager.attempt_order(&[failing.clone(), healthy.clone()]);
        assert_eq!(order.len(), 2);
    }

    #[test]
    fn latency_ordering_chooses_faster_peer() {
        let manager = PeerManager::new(fast_policy());
        let slow = config(1);
        let fast = config(2);
        manager.record_success(slow.addr, Duration::from_millis(80));
        manager.record_success(fast.addr, Duration::from_millis(10));

        let order = manager.attempt_order(&[slow.clone(), fast.clone()]);
        assert_eq!(order[0].addr, fast.addr);
        assert!(manager.score(fast.addr) < manager.score(slow.addr));
    }

    #[test]
    fn healthy_leaders_cycle_between_workers() {
        let manager = PeerManager::new(fast_policy());
        let first = config(1);
        let second = config(2);
        manager.record_success(first.addr, Duration::from_millis(10));
        manager.record_success(second.addr, Duration::from_millis(10));

        let a = manager.attempt_order(&[first.clone(), second.clone()]);
        let b = manager.attempt_order(&[first.clone(), second.clone()]);
        assert_ne!(a[0].addr, b[0].addr);
        assert_eq!(
            [a[0].addr, b[0].addr]
                .into_iter()
                .collect::<HashSet<_>>()
                .len(),
            2
        );
    }

    #[test]
    fn stale_peer_ranks_behind_live_peer() {
        let manager = PeerManager::new(fast_policy());
        let stale = config(1);
        let live = config(2);
        manager.record_success(stale.addr, Duration::from_millis(1));
        manager.record_success(live.addr, Duration::from_millis(1));
        manager.record_head(stale.addr, 241, 1_000);
        manager.record_head(live.addr, 241, 5_000);
        let order = manager.attempt_order(&[stale, live.clone()]);
        assert_eq!(order[0].addr, live.addr);
    }

    #[test]
    fn discovery_dedups_existing_and_advertised_duplicates_and_caps() {
        let existing = vec![config(1)];
        let advertised = vec![
            PeerAddress::new([127, 0, 0, 1], 21_841), // duplicate of configured
            PeerAddress::new([127, 0, 0, 2], 21_841),
            PeerAddress::new([127, 0, 0, 2], 21_841), // duplicate in payload
            PeerAddress::new([127, 0, 0, 3], 0),      // legacy default port
            PeerAddress::new([0, 0, 0, 0], 21_841),   // unspecified
            PeerAddress::new([127, 0, 0, 4], 21_841),
        ];

        let added = discovered_configs(&existing, &advertised, 3);
        assert_eq!(added.len(), 2, "total list is capped at three peers");
        assert_eq!(added[0].addr, addr(2));
        assert_eq!(added[1].addr, addr(3));

        let added = discovered_configs(&[], &advertised, 64);
        assert_eq!(added.len(), 4, "duplicates and unspecified IP are dropped");
    }

    #[test]
    fn backwards_head_and_stale_cached_head_are_rejected() {
        let previous = head(240, 1_000);
        assert!(check_head(Some(&previous), &head(240, 1_001)).is_ok());
        assert!(check_head(Some(&previous), &head(241, 10)).is_ok());
        assert!(matches!(
            check_head(Some(&previous), &head(240, 999)),
            Err(Error::NotSynchronized {
                head: 999,
                requested: 1_000
            })
        ));
        assert!(matches!(
            check_head(Some(&previous), &head(239, 5_000)),
            Err(Error::NotSynchronized { head: 5_000, .. })
        ));

        // Epoch rollover: observed head is in epoch 241, caller still holds 240.
        let observed = head(241, 50);
        assert!(matches!(
            check_cached_head(Some(&observed), &head(240, 9_000)),
            Err(Error::NotSynchronized { head: 50, .. })
        ));
        assert!(
            check_cached_head(Some(&head(241, 100)), &head(241, 90)).is_ok(),
            "a same-epoch cached head remains a valid lower bound"
        );
        assert!(check_cached_head(Some(&observed), &observed).is_ok());

        let computors = Computors {
            epoch: 241,
            public_keys: vec![[0; 32]; qubic::messages::COMPUTOR_COUNT],
            signature: [0; 64],
        };
        assert!(check_computors_epoch(&computors, 241).is_ok());
        assert!(matches!(
            check_computors_epoch(&computors, 240),
            Err(Error::NotSynchronized {
                head: 241,
                requested: 240
            })
        ));
    }

    #[tokio::test]
    async fn clone_shares_health_state() {
        let manager = PeerManager::new(fast_policy());
        let clone = manager.clone();
        let peer = config(7);
        clone.record_failure(peer.addr);
        clone.record_failure(peer.addr);
        clone.record_failure(peer.addr);

        assert!(manager.is_quarantined(peer.addr));
        let health = manager.report(std::slice::from_ref(&peer));
        assert_eq!(health[0].failures, 3);
        assert_eq!(health[0].consecutive_failures, 3);

        clone.record_success(peer.addr, Duration::from_millis(3));
        assert!(!manager.is_quarantined(peer.addr));
        assert_eq!(manager.report(&[peer])[0].successes, 1);
    }

    #[test]
    fn quarantine_growth_is_exponential_and_bounded() {
        let policy = PeerPolicy {
            quarantine_after: 3,
            quarantine_base: Duration::from_secs(30),
            quarantine_max: Duration::from_secs(100),
            ..PeerPolicy::default()
        };
        let manager = PeerManager::new(policy);
        let peer = config(9);
        for _ in 0..3 {
            manager.record_failure(peer.addr);
        }
        let first = manager.report(std::slice::from_ref(&peer))[0]
            .quarantine_remaining_ms
            .expect("quarantined");
        for _ in 0..2 {
            manager.record_failure(peer.addr);
        }
        let grown = manager.report(&[peer])[0]
            .quarantine_remaining_ms
            .expect("still quarantined");
        assert!(grown > first, "penalty should grow: {grown} > {first}");
        assert!(grown <= 100_000);
    }

    #[test]
    fn peer_address_round_trips_port_and_display() {
        let parsed: PeerAddress = serde_json::from_str(r#"{"ip":[1,2,3,4]}"#).unwrap();
        assert_eq!(parsed.port, DEFAULT_PEER_PORT);
        assert_eq!(parsed.to_string(), "1.2.3.4:21841");

        let socket: SocketAddr = PeerAddress::new([1, 2, 3, 4], 12_345).into();
        assert_eq!(socket, "1.2.3.4:12345".parse().unwrap());
    }
}
