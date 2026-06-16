use super::*;

#[test]
fn ewma_first_observation_sets_directly() {
    let stats = CandidateStats::new();
    assert!(stats.is_cold(LatencyMode::Streaming));
    stats.update_ewma(LatencyMode::Streaming, 100, 0.3);
    assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 100);
    assert!(!stats.is_cold(LatencyMode::Streaming));
}

#[test]
fn ewma_blends_subsequent_observations() {
    let stats = CandidateStats::new();
    stats.update_ewma(LatencyMode::Streaming, 100, 0.3);
    stats.update_ewma(LatencyMode::Streaming, 200, 0.3);
    assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 130);
}

#[test]
fn ewma_converges_toward_constant_signal() {
    let stats = CandidateStats::new();
    for _ in 0..50 {
        stats.update_ewma(LatencyMode::Streaming, 500, 0.3);
    }
    assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 500);
}

#[test]
fn streaming_and_nonstreaming_ewmas_are_independent() {
    let stats = CandidateStats::new();
    stats.update_ewma(LatencyMode::Streaming, 100, 0.3);
    assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 100);
    assert!(stats.is_cold(LatencyMode::NonStreaming));

    stats.update_ewma(LatencyMode::NonStreaming, 5_000, 0.3);
    assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 100);
    assert_eq!(stats.ewma_ms(LatencyMode::NonStreaming), 5_000);
}

#[test]
fn success_rate_starts_optimistic() {
    let stats = CandidateStats::new();
    assert!((stats.success_rate() - 1.0).abs() < f64::EPSILON);
}

#[test]
fn success_rate_blends_like_an_ewma() {
    let stats = CandidateStats::new();
    stats.record_outcome(false, 0.3);
    assert!((stats.success_rate() - 0.7).abs() < 1e-12);
    stats.record_outcome(true, 0.3);
    assert!((stats.success_rate() - 0.79).abs() < 1e-12);
}

#[test]
fn degraded_when_error_ewma_breaches_threshold() {
    let mut tracker = Tracker::new(0.3, 0.5);
    let idx = tracker.register();
    assert!(!tracker.is_degraded(idx));

    // At alpha 0.3 the error EWMA is 0.3 after one error, 0.51 after two.
    tracker.record_error(idx);
    assert!(!tracker.is_degraded(idx));
    tracker.record_error(idx);
    assert!(tracker.is_degraded(idx));

    tracker.record_success(idx, LatencyMode::Streaming, Duration::from_millis(100));
    assert!(!tracker.is_degraded(idx));
}

#[test]
fn record_success_updates_ewma_for_correct_mode() {
    let mut tracker = Tracker::new(0.3, 0.5);
    let idx = tracker.register();
    tracker.record_success(idx, LatencyMode::Streaming, Duration::from_millis(150));
    assert_eq!(tracker.stats(idx).ewma_ms(LatencyMode::Streaming), 150);
    assert!(tracker.stats(idx).is_cold(LatencyMode::NonStreaming));
    assert!((tracker.stats(idx).success_rate() - 1.0).abs() < f64::EPSILON);
}
