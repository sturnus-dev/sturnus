use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Streaming TTFC and non-streaming response time are tracked separately;
/// blending them in one EWMA would distort routing for both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyMode {
    Streaming,
    NonStreaming,
}

impl LatencyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            LatencyMode::Streaming => "streaming",
            LatencyMode::NonStreaming => "nonstreaming",
        }
    }
}

#[derive(Debug)]
pub struct CandidateStats {
    /// EWMA of streaming TTFC in milliseconds. u64::MAX = cold (no data).
    streaming_ewma_ms: AtomicU64,
    /// EWMA of non-streaming response time in milliseconds. u64::MAX = cold.
    nonstreaming_ewma_ms: AtomicU64,
    /// EWMA of request outcomes (success = 1.0, error = 0.0) as f64 bits.
    /// Starts optimistic at 1.0; recovers via probe traffic, not a clock.
    success_rate: AtomicU64,
}

impl Default for CandidateStats {
    fn default() -> Self {
        Self::new()
    }
}

impl CandidateStats {
    pub fn new() -> Self {
        Self {
            streaming_ewma_ms: AtomicU64::new(u64::MAX),
            nonstreaming_ewma_ms: AtomicU64::new(u64::MAX),
            success_rate: AtomicU64::new(1.0_f64.to_bits()),
        }
    }

    fn ewma_field(&self, mode: LatencyMode) -> &AtomicU64 {
        match mode {
            LatencyMode::Streaming => &self.streaming_ewma_ms,
            LatencyMode::NonStreaming => &self.nonstreaming_ewma_ms,
        }
    }

    pub fn ewma_ms(&self, mode: LatencyMode) -> u64 {
        self.ewma_field(mode).load(Ordering::Relaxed)
    }

    pub fn is_cold(&self, mode: LatencyMode) -> bool {
        self.ewma_ms(mode) == u64::MAX
    }

    pub fn success_rate(&self) -> f64 {
        f64::from_bits(self.success_rate.load(Ordering::Relaxed))
    }

    pub fn record_outcome(&self, success: bool, alpha: f64) {
        let sample = if success { 1.0 } else { 0.0 };
        let mut current = self.success_rate.load(Ordering::Relaxed);
        loop {
            let blended = alpha * sample + (1.0 - alpha) * f64::from_bits(current);
            match self.success_rate.compare_exchange_weak(
                current,
                blended.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed, // another thread updated; retry
            }
        }
    }

    pub fn update_ewma(&self, mode: LatencyMode, observed_ms: u64, alpha: f64) {
        let field = self.ewma_field(mode);
        loop {
            let old = field.load(Ordering::Relaxed);
            // EWMA of positive millis; rounds to a bounded non-negative u64.
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let new_val = if old == u64::MAX {
                observed_ms
            } else {
                let new_f = alpha * observed_ms as f64 + (1.0 - alpha) * old as f64;
                new_f.round() as u64
            };
            match field.compare_exchange_weak(old, new_val, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(_) => continue, // another thread updated; retry with fresh value
            }
        }
    }
}

/// Per-candidate stats stored in a flat `Vec`, addressed by a stable index
/// minted at `register` time. Indices are safe because `register` only
/// appends and the tracker is frozen behind `&Tracker` once moved into
/// `AppState`. Cross-alias sharing is preserved by `ModelMap` deduping
/// (provider, model) pairs to the same index. One `alpha` smooths both the
/// latency and success-rate EWMAs, so the signals react on the same timescale.
#[derive(Debug)]
pub struct Tracker {
    stats: Vec<CandidateStats>,
    alpha: f64,
    error_threshold: f64,
}

impl Tracker {
    pub fn new(alpha: f64, error_threshold: f64) -> Self {
        Self {
            stats: Vec::new(),
            alpha,
            error_threshold,
        }
    }

    /// Allocates a new stats slot and returns its index. The index is stable
    /// for the lifetime of the tracker.
    pub fn register(&mut self) -> usize {
        let idx = self.stats.len();
        self.stats.push(CandidateStats::new());
        idx
    }

    pub fn stats(&self, index: usize) -> &CandidateStats {
        &self.stats[index]
    }

    pub fn record_success(&self, index: usize, mode: LatencyMode, observed: Duration) {
        // request latency in millis always fits u64.
        #[allow(clippy::cast_possible_truncation)]
        let ms = observed.as_millis() as u64;
        self.stats[index].update_ewma(mode, ms, self.alpha);
        self.stats[index].record_outcome(true, self.alpha);
    }

    pub fn record_error(&self, index: usize) {
        self.stats[index].record_outcome(false, self.alpha);
    }

    /// Error-rate EWMA above `error_threshold`. Not consulted by routing —
    /// it only breaks session-affinity pins and labels `/status`.
    pub fn is_degraded(&self, index: usize) -> bool {
        1.0 - self.stats[index].success_rate() > self.error_threshold
    }
}

#[cfg(test)]
mod tests {
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
}
