use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Time-windowed sliding buffer of request outcomes. Entries older than
/// `window` are pruned on access, so errors age out naturally.
#[derive(Debug)]
struct ErrorWindow {
    outcomes: VecDeque<(Instant, bool)>,
    window: Duration,
    max_entries: usize,
}

impl ErrorWindow {
    fn new(window: Duration, max_entries: usize) -> Self {
        Self {
            outcomes: VecDeque::new(),
            window,
            max_entries,
        }
    }

    fn prune(&mut self) {
        let cutoff = Instant::now() - self.window;
        while let Some(&(ts, _)) = self.outcomes.front() {
            if ts < cutoff {
                self.outcomes.pop_front();
            } else {
                break;
            }
        }
    }

    fn record(&mut self, success: bool) {
        self.prune();
        while self.outcomes.len() >= self.max_entries {
            self.outcomes.pop_front();
        }
        self.outcomes.push_back((Instant::now(), success));
    }

    fn error_rate(&mut self) -> f64 {
        self.prune();
        let total = self.outcomes.len();
        if total == 0 {
            return 0.0;
        }
        let errors = self.outcomes.iter().filter(|(_, ok)| !ok).count();
        errors as f64 / total as f64
    }
}

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
    error_window: Mutex<ErrorWindow>,
}

impl CandidateStats {
    pub fn new(error_window_duration: Duration, max_error_window_entries: usize) -> Self {
        Self {
            streaming_ewma_ms: AtomicU64::new(u64::MAX),
            nonstreaming_ewma_ms: AtomicU64::new(u64::MAX),
            error_window: Mutex::new(ErrorWindow::new(
                error_window_duration,
                max_error_window_entries,
            )),
        }
    }

    fn lock_window(&self) -> std::sync::MutexGuard<'_, ErrorWindow> {
        self.error_window
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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

    pub fn error_rate(&self) -> f64 {
        self.lock_window().error_rate()
    }

    pub fn record_success(&self) {
        self.lock_window().record(true);
    }

    pub fn record_error(&self) {
        self.lock_window().record(false);
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
/// (provider, model) pairs to the same index.
#[derive(Debug)]
pub struct Tracker {
    stats: Vec<CandidateStats>,
    alpha: f64,
    error_window_duration: Duration,
    error_threshold: f64,
    max_error_window_entries: usize,
}

impl Tracker {
    pub fn new(
        alpha: f64,
        error_decay_secs: u64,
        error_threshold: f64,
        max_error_window_entries: usize,
    ) -> Self {
        Self {
            stats: Vec::new(),
            alpha,
            error_window_duration: Duration::from_secs(error_decay_secs),
            error_threshold,
            max_error_window_entries,
        }
    }

    /// Allocates a new stats slot and returns its index. The index is stable
    /// for the lifetime of the tracker.
    pub fn register(&mut self) -> usize {
        let idx = self.stats.len();
        self.stats.push(CandidateStats::new(
            self.error_window_duration,
            self.max_error_window_entries,
        ));
        idx
    }

    pub fn stats(&self, index: usize) -> &CandidateStats {
        &self.stats[index]
    }

    pub fn record_latency(&self, index: usize, mode: LatencyMode, observed: Duration) {
        // request latency in millis always fits u64.
        #[allow(clippy::cast_possible_truncation)]
        let ms = observed.as_millis() as u64;
        self.stats[index].update_ewma(mode, ms, self.alpha);
    }

    pub fn record_success(&self, index: usize) {
        self.stats[index].record_success();
    }

    pub fn record_error(&self, index: usize) {
        self.stats[index].record_error();
    }

    pub fn is_degraded(&self, index: usize) -> bool {
        self.stats[index].error_rate() > self.error_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_first_observation_sets_directly() {
        let stats = CandidateStats::new(Duration::from_secs(30), 10_000);
        assert!(stats.is_cold(LatencyMode::Streaming));
        stats.update_ewma(LatencyMode::Streaming, 100, 0.3);
        assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 100);
        assert!(!stats.is_cold(LatencyMode::Streaming));
    }

    #[test]
    fn ewma_blends_subsequent_observations() {
        let stats = CandidateStats::new(Duration::from_secs(30), 10_000);
        stats.update_ewma(LatencyMode::Streaming, 100, 0.3);
        stats.update_ewma(LatencyMode::Streaming, 200, 0.3);
        assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 130);
    }

    #[test]
    fn ewma_converges_toward_constant_signal() {
        let stats = CandidateStats::new(Duration::from_secs(30), 10_000);
        for _ in 0..50 {
            stats.update_ewma(LatencyMode::Streaming, 500, 0.3);
        }
        assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 500);
    }

    #[test]
    fn streaming_and_nonstreaming_ewmas_are_independent() {
        let stats = CandidateStats::new(Duration::from_secs(30), 10_000);
        stats.update_ewma(LatencyMode::Streaming, 100, 0.3);
        assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 100);
        assert!(stats.is_cold(LatencyMode::NonStreaming));

        stats.update_ewma(LatencyMode::NonStreaming, 5_000, 0.3);
        assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 100);
        assert_eq!(stats.ewma_ms(LatencyMode::NonStreaming), 5_000);
    }

    #[test]
    fn error_rate_empty_is_zero() {
        let stats = CandidateStats::new(Duration::from_secs(30), 10_000);
        assert_eq!(stats.error_rate(), 0.0);
    }

    #[test]
    fn error_rate_tracks_recent_outcomes() {
        let stats = CandidateStats::new(Duration::from_secs(30), 10_000);
        stats.record_success();
        stats.record_success();
        stats.record_error();
        stats.record_success();
        stats.record_error();
        assert!((stats.error_rate() - 0.4).abs() < f64::EPSILON);
    }

    #[test]
    fn errors_age_out_of_window() {
        let stats = CandidateStats::new(Duration::from_secs(1), 10_000);
        for _ in 0..10 {
            stats.record_error();
        }
        assert_eq!(stats.error_rate(), 1.0);

        std::thread::sleep(Duration::from_millis(1100));
        assert_eq!(stats.error_rate(), 0.0);
    }

    #[test]
    fn old_errors_retained_alongside_new_ones() {
        let stats = CandidateStats::new(Duration::from_secs(2), 10_000);
        for _ in 0..5 {
            stats.record_error();
        }

        std::thread::sleep(Duration::from_millis(200));
        for _ in 0..5 {
            stats.record_success();
        }

        assert!((stats.error_rate() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn tracker_not_degraded_below_threshold() {
        let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
        let idx = tracker.register();
        for _ in 0..6 {
            tracker.record_success(idx);
        }
        for _ in 0..4 {
            tracker.record_error(idx);
        }
        assert!(!tracker.is_degraded(idx));
    }

    #[test]
    fn tracker_degraded_above_threshold() {
        let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
        let idx = tracker.register();
        for _ in 0..4 {
            tracker.record_success(idx);
        }
        for _ in 0..6 {
            tracker.record_error(idx);
        }
        assert!(tracker.is_degraded(idx));
    }

    #[test]
    fn tracker_degraded_recovers_when_errors_age_out() {
        let mut tracker = Tracker::new(0.3, 1, 0.5, 10_000);
        let idx = tracker.register();
        for _ in 0..10 {
            tracker.record_error(idx);
        }
        assert!(tracker.is_degraded(idx));

        std::thread::sleep(Duration::from_millis(1100));
        assert!(!tracker.is_degraded(idx));
    }

    #[test]
    fn tracker_degraded_recovers_with_successes() {
        let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
        let idx = tracker.register();
        for _ in 0..10 {
            tracker.record_error(idx);
        }
        assert!(tracker.is_degraded(idx));
        for _ in 0..11 {
            tracker.record_success(idx);
        }
        assert!(!tracker.is_degraded(idx));
    }

    #[test]
    fn record_latency_updates_ewma_for_correct_mode() {
        let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
        let idx = tracker.register();
        tracker.record_latency(idx, LatencyMode::Streaming, Duration::from_millis(150));
        assert_eq!(tracker.stats(idx).ewma_ms(LatencyMode::Streaming), 150);
        assert!(tracker.stats(idx).is_cold(LatencyMode::NonStreaming));
    }

    #[test]
    fn poisoned_window_keeps_working_with_history_preserved() {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let stats = CandidateStats::new(Duration::from_secs(30), 10_000);
        stats.record_success();
        stats.record_error();
        assert!((stats.error_rate() - 0.5).abs() < f64::EPSILON);

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = stats.error_window.lock().unwrap();
            panic!("simulated panic while holding the lock");
        }));
        assert!(result.is_err(), "catch_unwind should have caught the panic");
        assert!(
            stats.error_window.is_poisoned(),
            "mutex should be poisoned after holder panic"
        );

        assert!(
            (stats.error_rate() - 0.5).abs() < f64::EPSILON,
            "pre-poison history must survive recovery"
        );
        stats.record_success();
        stats.record_success();
        assert!((stats.error_rate() - 0.25).abs() < f64::EPSILON);
    }
}
