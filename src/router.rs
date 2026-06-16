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

    fn next_tick(&self, alias: &str) -> u64 {
        self.counters
            .get(alias)
            .map_or(0, |c| c.fetch_add(1, Ordering::Relaxed) as u64)
    }
}

/// Default exploit sharpness (the `routing.exploit_k` default): traffic to
/// each candidate is proportional to `(best_effective / its_effective)^k`,
/// where effective latency folds the error rate in. Higher = stronger
/// preference for the best. Scale-invariant (depends on the ratio, not
/// absolute ms), so a fixed default generalises: a 2x-worse candidate gets
/// ~1/8 the traffic, a 5x-worse one ~1/125.
pub const EXPLOIT_K: f64 = 3.0;

/// Fastest candidate gets this many weight units; everyone else's weight is
/// scaled relative to it (but never below the `PROBE_FLOOR` share).
const WEIGHT_RESOLUTION: f64 = 1000.0;

/// Cold candidates probe at this fraction of the fastest candidate's rate.
const COLD_PROBE_WEIGHT: f64 = 0.25;

/// Minimum weight for any candidate, as a fraction of `WEIGHT_RESOLUTION`.
/// This is the probe rate for a hard-failing candidate, and it bounds both
/// sides of the same trade: at most ~this share of alias traffic eats
/// user-facing errors during an outage (there is no failover retry), and a
/// recovered candidate is re-detected within ~`1/PROBE_FLOOR` requests.
const PROBE_FLOOR: f64 = 0.01;

/// Success-rate floor inside effective latency: a fully failing candidate
/// gets a finite (huge) value rather than a division by zero.
const MIN_SUCCESS_RATE: f64 = 1e-3;

/// 2^64 / φ, the Weyl-sequence step. Odd, so `wrapping_mul` is a bijection.
const WEYL_STEP: u64 = 0x9E37_79B9_7F4A_7C15;

/// Pick a candidate for `alias` by proportional weighting.
///
/// Weights use *effective latency* — the latency EWMA over the success-rate
/// EWMA, i.e. expected time per successful response. Each candidate gets
/// traffic proportional to `(best_effective / its_effective)^k`, so slower
/// or erroring candidates keep a small but never-zero share. That share
/// doubles as the probe through which a recovered candidate wins traffic back.
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

    // Effective latency (ms per successful response); None = cold for `mode`.
    let effective: Vec<Option<f64>> = candidates
        .iter()
        .map(|c| {
            let stats = tracker.stats(c.stats_index);
            if stats.is_cold(mode) {
                None
            } else {
                // Guarded against a 0ms EWMA.
                #[allow(clippy::cast_precision_loss)]
                Some(stats.ewma_ms(mode).max(1) as f64 / stats.success_rate().max(MIN_SUCCESS_RATE))
            }
        })
        .collect();

    // Reference = best effective latency; infinite while everyone is cold.
    let best = effective
        .iter()
        .flatten()
        .copied()
        .fold(f64::INFINITY, f64::min);

    // Floor at the probe share so no candidate starves.
    let mut weighted: Vec<(&ResolvedCandidate, usize)> = Vec::with_capacity(candidates.len());
    let mut total: usize = 0;
    for (candidate, effective_latency) in candidates.iter().zip(&effective) {
        let raw_weight = match effective_latency {
            Some(latency) => (best / latency).powf(k),
            // Cold: fixed probe share (full when everyone is cold), scaled
            // by success rate so erroring cold candidates back off too.
            None => {
                let rate = tracker.stats(candidate.stats_index).success_rate();
                let base = if best.is_finite() {
                    COLD_PROBE_WEIGHT
                } else {
                    1.0
                };
                base * rate.max(MIN_SUCCESS_RATE).powf(k)
            }
        };
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let weight = (raw_weight * WEIGHT_RESOLUTION)
            .round()
            .max(WEIGHT_RESOLUTION * PROBE_FLOOR) as usize;
        weighted.push((candidate, weight));
        total += weight;
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
mod tests;
