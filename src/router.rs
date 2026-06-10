use crate::model_map::ResolvedCandidate;
use crate::tracker::{LatencyMode, Tracker};
use std::sync::atomic::Ordering;

/// One counter per alias, walked over the weighted schedule (no RNG).
#[derive(Debug)]
pub struct RoundRobinState {
    counters: std::collections::HashMap<String, std::sync::atomic::AtomicUsize>,
}

impl Default for RoundRobinState {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundRobinState {
    pub fn new() -> Self {
        Self {
            counters: std::collections::HashMap::new(),
        }
    }

    pub fn register_alias(&mut self, alias: String) {
        self.counters
            .entry(alias)
            .or_insert_with(|| std::sync::atomic::AtomicUsize::new(0));
    }

    fn next(&self, alias: &str, count: usize) -> usize {
        if let Some(counter) = self.counters.get(alias) {
            counter.fetch_add(1, Ordering::Relaxed) % count
        } else {
            0
        }
    }

    fn next_tick(&self, alias: &str) -> u64 {
        self.counters
            .get(alias)
            .map_or(0, |c| c.fetch_add(1, Ordering::Relaxed) as u64)
    }
}

/// Default exploit sharpness (the `routing.exploit_k` default): traffic to each
/// healthy candidate is proportional to `(fastest_ewma / its_ewma)^k`. Higher =
/// stronger preference for the fastest. Scale-invariant (depends on the latency
/// ratio, not absolute ms), so a fixed default generalises: a 2x-slower
/// candidate gets ~1/8 the traffic, a 5x-slower one ~1/125.
pub const EXPLOIT_K: f64 = 3.0;

/// Fastest candidate gets this many weight units, so a slower candidate's
/// minimum share (and probe-traffic floor) is ~`1 / WEIGHT_RESOLUTION`.
const WEIGHT_RESOLUTION: f64 = 1000.0;

/// Cold candidates probe at this fraction of the fastest candidate's rate.
const COLD_PROBE_WEIGHT: f64 = 0.25;

/// 2^64 / φ, the Weyl-sequence step. Odd, so `wrapping_mul` is a bijection.
const WEYL_STEP: u64 = 0x9E37_79B9_7F4A_7C15;

/// Pick a candidate for `alias` by proportional weighting.
///
/// Candidates are bucketed into warm (have EWMA for `mode`), cold (no data
/// yet), and degraded (high error rate). Each healthy (warm + cold) candidate
/// gets traffic proportional to `(fastest_ewma / its_ewma)^k`. Slower
/// candidates keep a small share, so their EWMA stays fresh and a recovered
/// provider wins traffic back at a rate proportional to that share.
pub fn select_candidate<'a>(
    alias: &str,
    candidates: &'a [ResolvedCandidate],
    tracker: &Tracker,
    rr: &RoundRobinState,
    k: f64,
    mode: LatencyMode,
) -> Option<&'a ResolvedCandidate> {
    if candidates.is_empty() {
        return None;
    }

    let mut warm = Vec::new();
    let mut cold = Vec::new();
    let mut degraded = Vec::new();

    for c in candidates {
        let stats = tracker.stats(c.stats_index);
        if tracker.is_degraded(c.stats_index) {
            degraded.push(c);
        } else if stats.is_cold(mode) {
            cold.push(c);
        } else {
            warm.push(c);
        }
    }

    // Nothing healthy: round-robin across degraded so we still attempt delivery.
    if warm.is_empty() && cold.is_empty() {
        if degraded.is_empty() {
            return None;
        }
        let idx = rr.next(alias, degraded.len());
        return Some(degraded[idx]);
    }

    // No data for this mode yet: round-robin across cold to warm them up.
    if warm.is_empty() {
        let idx = rr.next(alias, cold.len());
        return Some(cold[idx]);
    }

    // Reference = fastest warm candidate (guarded against a 0ms EWMA).
    let best_ms = warm
        .iter()
        .map(|c| tracker.stats(c.stats_index).ewma_ms(mode))
        .min()
        .unwrap_or(1)
        .max(1);

    // Floor at 1 unit so no candidate starves.
    let mut weighted: Vec<(&ResolvedCandidate, usize)> =
        Vec::with_capacity(warm.len() + cold.len());
    let mut total: usize = 0;
    for c in warm.iter().chain(cold.iter()) {
        let stats = tracker.stats(c.stats_index);
        let raw = if stats.is_cold(mode) {
            COLD_PROBE_WEIGHT
        } else {
            #[allow(clippy::cast_precision_loss)]
            (best_ms as f64 / stats.ewma_ms(mode).max(1) as f64).powf(k)
        };
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let w = (raw * WEIGHT_RESOLUTION).round().max(1.0) as usize;
        weighted.push((c, w));
        total += w;
    }

    // Golden-ratio Weyl sequence (n·2^64/φ): a deterministic stand-in for the
    // uniform sample in weighted choice (cf. rand::distributions::WeightedIndex).
    // Stays equidistributed even when modes subsample the shared counter.
    let spread = rr.next_tick(alias).wrapping_mul(WEYL_STEP);
    #[allow(clippy::cast_possible_truncation)]
    let pos = ((u128::from(spread) * total as u128) >> 64) as usize;
    let mut acc = 0;
    for (c, w) in &weighted {
        acc += *w;
        if pos < acc {
            return Some(c);
        }
    }
    // pos < total guarantees a hit above; last is a safe fallback.
    weighted.last().map(|(c, _)| *c)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_map::ProviderKind;
    use crate::tracker::Tracker;
    use std::time::Duration;

    fn make_candidate(provider: &str, model: &str, stats_index: usize) -> ResolvedCandidate {
        ResolvedCandidate {
            provider_name: provider.to_string(),
            model: model.to_string(),
            base_url: "http://localhost".to_string(),
            api_key: None,
            kind: ProviderKind::ApiKey,
            stats_index,
            provider_header: hyper::header::HeaderValue::from_str(provider).unwrap(),
            affinity_header: hyper::header::HeaderValue::try_from(format!("{provider}/{model}"))
                .unwrap(),
            attribution_labels: None,
        }
    }

    fn setup(specs: &[(&str, &str)]) -> (Vec<ResolvedCandidate>, Tracker, RoundRobinState) {
        let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
        let mut rr = RoundRobinState::new();
        rr.register_alias("test".to_string());
        let candidates: Vec<_> = specs
            .iter()
            .map(|(provider, model)| {
                let stats_index = tracker.register();
                make_candidate(provider, model, stats_index)
            })
            .collect();
        (candidates, tracker, rr)
    }

    #[test]
    fn empty_candidates_returns_none() {
        let candidates: Vec<ResolvedCandidate> = vec![];
        let (_, tracker, rr) = setup(&[]);
        assert!(select_candidate(
            "test",
            &candidates,
            &tracker,
            &rr,
            EXPLOIT_K,
            LatencyMode::Streaming
        )
        .is_none());
    }

    #[test]
    fn all_cold_round_robins() {
        let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);

        let first = select_candidate(
            "test",
            &candidates,
            &tracker,
            &rr,
            EXPLOIT_K,
            LatencyMode::Streaming,
        )
        .unwrap();
        let second = select_candidate(
            "test",
            &candidates,
            &tracker,
            &rr,
            EXPLOIT_K,
            LatencyMode::Streaming,
        )
        .unwrap();
        assert_ne!(first.provider_name, second.provider_name);
    }

    #[test]
    fn lowest_ewma_gets_the_large_majority() {
        let (candidates, tracker, rr) = setup(&[("slow", "m1"), ("fast", "m2")]);

        tracker.record_latency(
            candidates[0].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(500),
        );
        tracker.record_latency(
            candidates[1].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );

        let n = 10_000;
        let mut fast = 0;
        for _ in 0..n {
            let picked = select_candidate(
                "test",
                &candidates,
                &tracker,
                &rr,
                3.0,
                LatencyMode::Streaming,
            )
            .unwrap();
            if picked.provider_name == "fast" {
                fast += 1;
            }
        }
        let fast_share = f64::from(fast) / f64::from(n);
        assert!(
            fast_share > 0.95,
            "fast (5x quicker) should dominate at k=3, got {fast_share:.3}"
        );
    }

    #[test]
    fn slower_candidate_keeps_a_proportional_share() {
        // Defining property of proportional routing: at k=3 a 2x-slower candidate
        // has weight (1/2)^3 = 0.125 against the fastest's 1, i.e. a normalised
        // share of 0.125 / 1.125 ≈ 0.111 — never 0 (winner-take-all) nor 0.5 (RR).
        let (candidates, tracker, rr) = setup(&[("fast", "m1"), ("slow", "m2")]);
        tracker.record_latency(
            candidates[0].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
        tracker.record_latency(
            candidates[1].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(200),
        );

        let n = 100_000;
        let mut slow = 0;
        for _ in 0..n {
            let picked = select_candidate(
                "test",
                &candidates,
                &tracker,
                &rr,
                3.0,
                LatencyMode::Streaming,
            )
            .unwrap();
            if picked.provider_name == "slow" {
                slow += 1;
            }
        }
        let slow_share = f64::from(slow) / f64::from(n);
        assert!(
            (slow_share - 0.111).abs() < 0.02,
            "slow share {slow_share:.3} should be ≈0.111 (weight 0.125 normalised)"
        );
    }

    #[test]
    fn alternating_modes_do_not_bias_the_weighted_walk() {
        // Modes share the alias tick counter, so each sees every other tick;
        // the Weyl sequence must stay unbiased under that subsampling.
        let (candidates, tracker, rr) = setup(&[("fast", "m1"), ("slow", "m2")]);
        for mode in [LatencyMode::Streaming, LatencyMode::NonStreaming] {
            tracker.record_latency(candidates[0].stats_index, mode, Duration::from_millis(100));
            tracker.record_latency(candidates[1].stats_index, mode, Duration::from_millis(200));
        }

        let n = 50_000;
        let mut slow = [0u32; 2];
        for i in 0..2 * n {
            let mode = if i % 2 == 0 {
                LatencyMode::Streaming
            } else {
                LatencyMode::NonStreaming
            };
            let picked = select_candidate("test", &candidates, &tracker, &rr, 3.0, mode).unwrap();
            if picked.provider_name == "slow" {
                slow[i % 2] += 1;
            }
        }
        for (mode, count) in ["streaming", "nonstreaming"].iter().zip(slow) {
            #[allow(clippy::cast_precision_loss)]
            let share = f64::from(count) / n as f64;
            assert!(
                (share - 0.111).abs() < 0.02,
                "{mode} slow share {share:.3} should be ≈0.111 despite seeing only every other tick"
            );
        }
    }

    /// Majority provider over many weighted picks, for asserting which way the
    /// split leans without depending on a single pick's position.
    fn majority_provider(
        candidates: &[ResolvedCandidate],
        tracker: &Tracker,
        rr: &RoundRobinState,
        mode: LatencyMode,
    ) -> String {
        let mut counts = std::collections::HashMap::new();
        for _ in 0..10_000 {
            let picked = select_candidate("test", candidates, tracker, rr, 3.0, mode).unwrap();
            *counts.entry(picked.provider_name.clone()).or_insert(0) += 1;
        }
        counts.into_iter().max_by_key(|(_, n)| *n).unwrap().0
    }

    #[test]
    fn degraded_candidate_excluded() {
        let (candidates, tracker, rr) = setup(&[("good", "m1"), ("bad", "m2")]);

        tracker.record_latency(
            candidates[0].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(200),
        );
        tracker.record_latency(
            candidates[1].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(50),
        );

        for _ in 0..10 {
            tracker.record_error(candidates[1].stats_index);
        }
        assert!(tracker.is_degraded(candidates[1].stats_index));

        for _ in 0..20 {
            let picked = select_candidate(
                "test",
                &candidates,
                &tracker,
                &rr,
                3.0,
                LatencyMode::Streaming,
            )
            .unwrap();
            assert_eq!(picked.provider_name, "good");
        }
    }

    #[test]
    fn cold_candidate_is_probed() {
        let (candidates, tracker, rr) = setup(&[("warm", "m1"), ("cold", "m2")]);

        tracker.record_latency(
            candidates[0].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );

        // Cold weight 0.25 vs the fastest's 1.0: share 0.25 / 1.25 = 0.2.
        let mut cold_picks = 0;
        let n = 4000;
        for _ in 0..n {
            let picked = select_candidate(
                "test",
                &candidates,
                &tracker,
                &rr,
                3.0,
                LatencyMode::Streaming,
            )
            .unwrap();
            if picked.provider_name == "cold" {
                cold_picks += 1;
            }
        }
        let cold_share = f64::from(cold_picks) / f64::from(n);
        assert!(
            (0.15..0.25).contains(&cold_share),
            "cold should probe at ~0.2, got {cold_share:.3}"
        );
    }

    #[test]
    fn traffic_shifts_when_latency_increases() {
        let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);

        tracker.record_latency(
            candidates[0].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
        tracker.record_latency(
            candidates[1].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(300),
        );
        assert_eq!(
            majority_provider(&candidates, &tracker, &rr, LatencyMode::Streaming),
            "a"
        );

        for _ in 0..20 {
            tracker.record_latency(
                candidates[0].stats_index,
                LatencyMode::Streaming,
                Duration::from_millis(500),
            );
        }
        assert_eq!(
            majority_provider(&candidates, &tracker, &rr, LatencyMode::Streaming),
            "b"
        );
    }

    #[test]
    fn cold_candidate_with_errors_is_degraded_not_weighted() {
        let (candidates, tracker, rr) = setup(&[("good", "m1"), ("bad", "m2")]);

        tracker.record_latency(
            candidates[0].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );

        for _ in 0..10 {
            tracker.record_error(candidates[1].stats_index);
        }

        // Cold (no EWMA) but degraded takes priority
        assert!(tracker
            .stats(candidates[1].stats_index)
            .is_cold(LatencyMode::Streaming));
        assert!(tracker.is_degraded(candidates[1].stats_index));

        for _ in 0..20 {
            let picked = select_candidate(
                "test",
                &candidates,
                &tracker,
                &rr,
                3.0,
                LatencyMode::Streaming,
            )
            .unwrap();
            assert_eq!(picked.provider_name, "good");
        }
    }

    #[test]
    fn nonstreaming_routing_ignores_streaming_ewma() {
        let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);

        // a fast on streaming, slow on non-streaming; b the reverse.
        tracker.record_latency(
            candidates[0].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(50),
        );
        tracker.record_latency(
            candidates[0].stats_index,
            LatencyMode::NonStreaming,
            Duration::from_millis(5_000),
        );
        tracker.record_latency(
            candidates[1].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(5_000),
        );
        tracker.record_latency(
            candidates[1].stats_index,
            LatencyMode::NonStreaming,
            Duration::from_millis(50),
        );

        assert_eq!(
            majority_provider(&candidates, &tracker, &rr, LatencyMode::Streaming),
            "a"
        );
        assert_eq!(
            majority_provider(&candidates, &tracker, &rr, LatencyMode::NonStreaming),
            "b"
        );
    }

    /// Deterministic xorshift PRNG so the simulation is reproducible.
    struct Rng(u64);
    impl Rng {
        fn unit(&mut self) -> f64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            #[allow(clippy::cast_precision_loss)]
            {
                (self.0 >> 11) as f64 / (1u64 << 53) as f64
            }
        }
    }

    /// Closed-loop run against a load-dependent upstream: candidate 0 ("gemini")
    /// is usually fast (~600ms) but episodically throttles to ~3000ms; candidate
    /// 1 ("openai") is a steady ~2000ms. Routes via the shipped `select_candidate`
    /// and feeds observed latency back. Returns (picks, gemini-throttled flags).
    /// Synchronous feedback is optimistic vs production, where concurrent
    /// in-flight picks route on a stale EWMA.
    fn simulate_load_dependent() -> (Vec<usize>, Vec<bool>) {
        let mode = LatencyMode::NonStreaming;
        let mut tracker = Tracker::new(0.3, 300, 0.5, 10_000);
        let g = tracker.register();
        let o = tracker.register();
        let candidates = vec![
            make_candidate("gemini", "m1", g),
            make_candidate("openai", "m2", o),
        ];
        let mut rr = RoundRobinState::new();
        rr.register_alias("test".to_string());
        tracker.record_latency(g, mode, Duration::from_millis(600));
        tracker.record_latency(o, mode, Duration::from_millis(2000));

        let n = 12_000usize;
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut throttled = false;
        let mut picks = Vec::with_capacity(n);
        let mut flags = Vec::with_capacity(n);

        for _ in 0..n {
            // Two-state Markov throttle: rare onset, ~50-tick average duration.
            throttled = if throttled {
                rng.unit() >= 0.02
            } else {
                rng.unit() < 0.004
            };
            flags.push(throttled);

            let picked =
                select_candidate("test", &candidates, &tracker, &rr, EXPLOIT_K, mode).unwrap();
            let idx = usize::from(picked.provider_name == "openai");
            let latency = match idx {
                0 if throttled => 3000,
                0 => 600,
                _ => 2000,
            };
            tracker.record_latency(
                candidates[idx].stats_index,
                mode,
                Duration::from_millis(latency),
            );
            picks.push(idx);
        }
        (picks, flags)
    }

    #[test]
    fn does_not_over_route_to_a_load_dependent_upstream() {
        // The core value of proportional routing: when the fast provider briefly
        // throttles, traffic shifts to the slower one and back — it never herds
        // onto it beyond the actual throttle windows. Because the loser keeps a
        // small share, gemini stays sampled and its recovery is seen at once.
        let (picks, flags) = simulate_load_dependent();
        #[allow(clippy::cast_precision_loss)]
        let overrouting = picks
            .iter()
            .zip(&flags)
            .filter(|(&p, &throttled)| p == 1 && !throttled)
            .count() as f64
            / picks.len() as f64;
        assert!(
            overrouting < 0.06,
            "openai should be used only while gemini is throttled, got {overrouting:.3}"
        );
    }
}
