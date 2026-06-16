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
    let mut tracker = Tracker::new(0.3, 0.5);
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

    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(500),
    );
    tracker.record_success(
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
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );
    tracker.record_success(
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
        tracker.record_success(candidates[0].stats_index, mode, Duration::from_millis(100));
        tracker.record_success(candidates[1].stats_index, mode, Duration::from_millis(200));
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

/// Share of picks going to `provider` over `n` weighted selections.
fn share_of(
    provider: &str,
    n: u32,
    candidates: &[ResolvedCandidate],
    tracker: &Tracker,
    rr: &RoundRobinState,
) -> f64 {
    let mut hits = 0u32;
    for _ in 0..n {
        let picked =
            select_candidate("test", candidates, tracker, rr, 3.0, LatencyMode::Streaming).unwrap();
        if picked.provider_name == provider {
            hits += 1;
        }
    }
    f64::from(hits) / f64::from(n)
}

#[test]
fn erroring_candidate_gets_minimal_share_despite_speed() {
    // bad is 4x faster but failing hard: its effective latency blows up
    // and its share collapses to the probe floor.
    let (candidates, tracker, rr) = setup(&[("good", "m1"), ("bad", "m2")]);

    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(200),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(50),
    );

    for _ in 0..10 {
        tracker.record_error(candidates[1].stats_index);
    }

    let bad_share = share_of("bad", 10_000, &candidates, &tracker, &rr);
    assert!(
        bad_share < 0.05,
        "hard-failing candidate should be near the floor, got {bad_share:.3}"
    );
    assert!(
        bad_share > 0.0,
        "the floor share must keep probing a failing candidate"
    );
}

#[test]
fn hard_failing_candidate_share_is_pinned_at_probe_floor() {
    // The floor bounds both sides of the trade: at most ~PROBE_FLOOR of
    // traffic eats errors during an outage, and recovery is detected
    // within ~1/PROBE_FLOOR requests. Expected share with two candidates:
    // 10 / (1000 + 10) ≈ 0.0099.
    let (candidates, tracker, rr) = setup(&[("good", "m1"), ("bad", "m2")]);
    for c in &candidates {
        tracker.record_success(
            c.stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }
    for _ in 0..50 {
        tracker.record_error(candidates[1].stats_index);
    }

    let bad_share = share_of("bad", 100_000, &candidates, &tracker, &rr);
    assert!(
        (0.005..0.02).contains(&bad_share),
        "share should be pinned near PROBE_FLOOR, got {bad_share:.4}"
    );
}

#[test]
fn single_error_reduces_share_proportionally() {
    // Equal latency, one error at alpha 0.3: success EWMA 0.7, weight
    // 0.7^3 = 0.343, share 0.343/1.343 ≈ 0.26 — proportional, not a cliff.
    let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);
    for c in &candidates {
        tracker.record_success(
            c.stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }
    tracker.record_error(candidates[1].stats_index);

    let b_share = share_of("b", 100_000, &candidates, &tracker, &rr);
    assert!(
        (b_share - 0.255).abs() < 0.02,
        "one error should cost a proportional share, got {b_share:.3}"
    );
}

#[test]
fn erroring_candidate_recovers_share_through_probes() {
    let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);
    for c in &candidates {
        tracker.record_success(
            c.stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }

    for _ in 0..10 {
        tracker.record_error(candidates[1].stats_index);
    }
    assert!(share_of("b", 10_000, &candidates, &tracker, &rr) < 0.05);

    // Probe successes rebuild the EWMA; the share follows.
    for _ in 0..20 {
        tracker.record_success(
            candidates[1].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }
    let recovered = share_of("b", 10_000, &candidates, &tracker, &rr);
    assert!(
        (recovered - 0.5).abs() < 0.05,
        "recovered candidate should win back its share, got {recovered:.3}"
    );
}

#[test]
fn cold_candidate_is_probed() {
    let (candidates, tracker, rr) = setup(&[("warm", "m1"), ("cold", "m2")]);

    tracker.record_success(
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

    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(300),
    );
    assert_eq!(
        majority_provider(&candidates, &tracker, &rr, LatencyMode::Streaming),
        "a"
    );

    for _ in 0..20 {
        tracker.record_success(
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
fn cold_candidate_with_errors_probes_at_the_floor() {
    // Errors never warm the latency EWMA, so a failing candidate can stay
    // cold forever; its probe share must still scale down.
    let (candidates, tracker, rr) = setup(&[("good", "m1"), ("bad", "m2")]);

    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );

    for _ in 0..10 {
        tracker.record_error(candidates[1].stats_index);
    }
    assert!(tracker
        .stats(candidates[1].stats_index)
        .is_cold(LatencyMode::Streaming));

    let bad_share = share_of("bad", 10_000, &candidates, &tracker, &rr);
    assert!(
        bad_share < 0.05,
        "failing cold candidate should be near the floor, got {bad_share:.3}"
    );
}

#[test]
fn nonstreaming_routing_ignores_streaming_ewma() {
    let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);

    // a fast on streaming, slow on non-streaming; b the reverse.
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(50),
    );
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::NonStreaming,
        Duration::from_millis(5_000),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(5_000),
    );
    tracker.record_success(
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
    let mut tracker = Tracker::new(0.3, 0.5);
    let g = tracker.register();
    let o = tracker.register();
    let candidates = vec![
        make_candidate("gemini", "m1", g),
        make_candidate("openai", "m2", o),
    ];
    let mut rr = RoundRobinState::new();
    rr.register_alias("test".to_string());
    tracker.record_success(g, mode, Duration::from_millis(600));
    tracker.record_success(o, mode, Duration::from_millis(2000));

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

        let picked = select_candidate("test", &candidates, &tracker, &rr, EXPLOIT_K, mode).unwrap();
        let idx = usize::from(picked.provider_name == "openai");
        let latency = match idx {
            0 if throttled => 3000,
            0 => 600,
            _ => 2000,
        };
        tracker.record_success(
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
