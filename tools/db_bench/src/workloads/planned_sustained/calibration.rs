//! A planner model fitted from what a planned tree observed while it ran: the
//! bytes per second its merges wrote, the speed its queries kept while a merge
//! ran, its filters' false positive rate, and what a scan pays per sorted run
//! against what a point read pays.
//!
//! A fit moves the model a bounded step toward what it observed, and only
//! once enough of it was observed: without merges, queries or steps to go by,
//! the model stays as it was.

use lsm_tree::compaction::planned::Model;
use std::time::Instant;

/// Merges observed before their rate is believed.
pub(super) const MIN_MERGES: u64 = 4;

/// Queries observed, both while a merge ran and while none did, before the
/// share they keep is believed.
pub(super) const MIN_QUERIES: u64 = 200;

/// Steps observed before the cost of a scan per run is believed.
pub(super) const MIN_STEPS: usize = 8;

/// The weight a new observation gets against the model it updates.
pub(super) const WEIGHT: f64 = 0.25;

/// A fitted value stays within this factor of the prior it started from.
pub(super) const BOUND: f64 = 4.0;

/// Query time and count of one kind.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Timing {
    pub(super) nanos: u64,
    pub(super) count: u64,
}

impl Timing {
    pub(super) fn record(&mut self, nanos: u64) {
        self.nanos += nanos;
        self.count += 1;
    }

    fn mean(self) -> Option<f64> {
        (self.count > 0).then(|| self.nanos as f64 / self.count as f64)
    }
}

/// What one step's queries took, with the runs of the main level they met.
#[derive(Clone, Copy, Debug)]
pub(super) struct StepSample {
    pub(super) main_runs: u32,
    pub(super) scan_nanos: f64,
    pub(super) point_nanos: f64,
}

/// What a planned tree observed so far.
#[derive(Clone, Debug, Default)]
pub(super) struct Observed {
    /// Bytes the merges wrote, and the thread time they took.
    pub(super) merge_bytes: u64,
    pub(super) merge_nanos: u64,
    pub(super) merges: u64,
    /// Query times while no merge ran, and while one did.
    pub(super) idle: Timing,
    pub(super) during: Timing,
    pub(super) steps: Vec<StepSample>,
    /// Filter queries for keys a table did not hold, as the engine counts
    /// them, and those the filter turned away; the rest it let through by
    /// mistake.
    pub(super) filter_queries: u64,
    pub(super) filter_skips: u64,
}

/// The values a fit found, each `None` where too little was observed.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(super) struct Fitted {
    pub(super) encode_rate: Option<u64>,
    pub(super) merge_query_share: Option<f64>,
    /// What a scan pays per sorted run over what a point read pays in all.
    pub(super) scan_weight: Option<f64>,
    pub(super) false_positive_rate: Option<f64>,
}

/// Fits what `observed` holds enough of.
pub(super) fn fit(observed: &Observed) -> Fitted {
    let encode_rate = (observed.merges >= MIN_MERGES && observed.merge_nanos > 0).then(|| {
        let rate = observed.merge_bytes as f64 * 1e9 / observed.merge_nanos as f64;
        rate as u64
    });
    let merge_query_share = (observed.idle.count >= MIN_QUERIES
        && observed.during.count >= MIN_QUERIES)
        .then(|| Some(observed.idle.mean()? / observed.during.mean()?))
        .flatten()
        .map(|share| share.clamp(0.0, 1.0));
    Fitted {
        encode_rate,
        merge_query_share,
        scan_weight: scan_weight(&observed.steps),
        false_positive_rate: false_positive_rate(observed),
    }
}

/// The share of filter queries for a key a table does not hold that the
/// filter let through: the engine counts a query only when the filter turned
/// the key away or let it through to a read that found nothing. `None` with
/// too few such queries.
fn false_positive_rate(observed: &Observed) -> Option<f64> {
    if observed.filter_queries < MIN_QUERIES {
        return None;
    }
    let mistaken = observed.filter_queries.checked_sub(observed.filter_skips)?;
    Some(mistaken as f64 / observed.filter_queries as f64)
}

/// The slope of a scan's time over the runs it reads (`main + 2`, the top and
/// the last level besides the main), over a point read's mean time: `None`
/// with too few steps, runs that never varied, or no slope upward.
fn scan_weight(steps: &[StepSample]) -> Option<f64> {
    if steps.len() < MIN_STEPS {
        return None;
    }
    let n = steps.len() as f64;
    let runs = |s: &StepSample| f64::from(s.main_runs) + 2.0;
    let mean_runs = steps.iter().map(runs).sum::<f64>() / n;
    let mean_scan = steps.iter().map(|s| s.scan_nanos).sum::<f64>() / n;
    let mean_point = steps.iter().map(|s| s.point_nanos).sum::<f64>() / n;
    let spread: f64 = steps.iter().map(|s| (runs(s) - mean_runs).powi(2)).sum();
    // Runs that hardly moved say nothing of what a run costs.
    if spread < n * 0.25 || mean_point <= 0.0 {
        return None;
    }
    let covariance: f64 = steps
        .iter()
        .map(|s| (runs(s) - mean_runs) * (s.scan_nanos - mean_scan))
        .sum();
    let slope = covariance / spread;
    (slope > 0.0).then_some(slope / mean_point)
}

/// A model and a scan weight, moved toward fits by bounded steps.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Calibrated {
    prior: Model,
    pub(super) model: Model,
    /// What a scan pays per run over what a point read pays in all.
    pub(super) scan_weight: f64,
}

impl Calibrated {
    /// Starts from `prior`, which is what it stays at without observations.
    pub(super) fn new(prior: Model) -> Self {
        Self {
            prior,
            model: prior,
            scan_weight: Self::prior_scan_weight(&prior),
        }
    }

    /// The scan weight `model` assumes: a scan pays one per run, a point
    /// read `1 + f` in all.
    fn prior_scan_weight(model: &Model) -> f64 {
        1.0 / (1.0 + model.false_positive_rate)
    }

    /// Moves each value `WEIGHT` of the way toward what `fitted` found,
    /// within `BOUND` of the prior; a value `fitted` left out stays.
    pub(super) fn update(&mut self, fitted: &Fitted) {
        let step = |current: f64, observed: f64, prior: f64| {
            (current + WEIGHT * (observed - current)).clamp(prior / BOUND, prior * BOUND)
        };
        if let Some(rate) = fitted.encode_rate {
            let prior = self.prior.encode_rate as f64;
            self.model.encode_rate =
                step(self.model.encode_rate as f64, rate as f64, prior).max(1.0) as u64;
        }
        if let Some(share) = fitted.merge_query_share {
            let prior = self.prior.merge_query_share;
            self.model.merge_query_share =
                step(self.model.merge_query_share, share, prior).clamp(0.0, 1.0);
        }
        if let Some(rate) = fitted.false_positive_rate {
            let prior = self.prior.false_positive_rate;
            self.model.false_positive_rate =
                step(self.model.false_positive_rate, rate, prior).clamp(0.0, 1.0);
        }
        if let Some(weight) = fitted.scan_weight {
            let prior = Self::prior_scan_weight(&self.prior);
            self.scan_weight = step(self.scan_weight, weight, prior);
        }
    }

    /// The share of scans that makes the model's query cost grow with the
    /// runs as the observed one does, when `counted` of the reads are scans.
    ///
    /// The model prices a query at `(e + 2)·r + (1 − r)·(1 + f)`; the
    /// observed cost is `(e + 2)·counted·w + (1 − counted)` in point reads.
    /// Their slopes over the constant agree for `r = s / (s + 1 − counted)`
    /// with `s = counted·w·(1 + f)`, which is `counted` at the prior weight.
    pub(super) fn scan_share(&self, counted: f64) -> f64 {
        let counted = counted.clamp(0.0, 1.0);
        let s = counted * self.scan_weight * (1.0 + self.model.false_positive_rate);
        let total = s + 1.0 - counted;
        if total == 0.0 { 0.0 } else { s / total }
    }

    /// The point reads one unit of the model's query cost stands for, when
    /// `counted` of the reads are scans: the model's cost times this is the
    /// observed `(e + 2)·counted·w + (1 − counted)`. It moves with the scan
    /// weight, so a prediction recorded in point reads keeps one unit while
    /// the weight does not.
    pub(super) fn point_reads_per_unit(&self, counted: f64) -> f64 {
        let counted = counted.clamp(0.0, 1.0);
        let share = self.scan_share(counted);
        if share > 0.0 {
            // The slopes agree: `share·k = counted·w`.
            counted * self.scan_weight / share
        } else {
            // No scans: the constants agree, `(1 + f)·k = 1`.
            1.0 / (1.0 + self.model.false_positive_rate)
        }
    }
}

/// A query's span, kept until it is known whether a merge ran beside it.
#[derive(Clone, Copy, Debug)]
pub(super) struct QuerySpan {
    pub(super) start: Instant,
    pub(super) end: Instant,
    pub(super) nanos: u64,
}

/// What a tree's compaction threads did: the spans of the merges they
/// finished, and the start of the call each of them is in now.
#[derive(Debug)]
pub(super) struct CallLog {
    pub(super) merges: Vec<(Instant, Instant)>,
    pub(super) running: Vec<Option<Instant>>,
}

impl CallLog {
    /// Nothing done yet by `threads` compaction threads.
    pub(super) fn new(threads: usize) -> Self {
        Self {
            merges: Vec::new(),
            running: vec![None; threads],
        }
    }
}

/// Records each of `pending` that no call in progress overlaps into
/// `observed`, as beside a merge when one of `log`'s merges overlaps it and as
/// idle otherwise; returns those a call in progress may still turn out a merge
/// beside. A call that chose nothing, or moved tables, is no merge: the queries
/// beside it are idle. The merges every query left has passed are forgotten.
pub(super) fn classify(
    pending: Vec<QuerySpan>,
    log: &mut CallLog,
    observed: &mut Observed,
    now: Instant,
) -> Vec<QuerySpan> {
    let mut waiting = Vec::new();
    for query in pending {
        // A call in progress ends after now, so after the query: it overlaps
        // the query when it started before the query ended.
        if log.running.iter().flatten().any(|&start| start < query.end) {
            waiting.push(query);
            continue;
        }
        let beside_merge = log
            .merges
            .iter()
            .any(|&(start, end)| start < query.end && end > query.start);
        if beside_merge {
            observed.during.record(query.nanos);
        } else {
            observed.idle.record(query.nanos);
        }
    }
    // A later query starts after the waiting ones, or after now.
    let horizon = waiting.iter().map(|query| query.start).min().unwrap_or(now);
    log.merges.retain(|&(_, end)| end > horizon);
    waiting
}

/// The mean relative error of predictions scaled to observations: the scale
/// is fitted on the first half of the steps and the error measured on the
/// second, so a model is judged on steps it was not fitted to. `None` with
/// fewer than two steps or no prediction to scale.
pub(super) fn held_out_error(pairs: &[(f64, f64)]) -> Option<f64> {
    let half = pairs.len() / 2;
    let (training, held) = pairs.split_at(half);
    if training.is_empty() || held.is_empty() {
        return None;
    }
    let squares: f64 = training
        .iter()
        .map(|(predicted, _)| predicted * predicted)
        .sum();
    if squares == 0.0 {
        return None;
    }
    let scale = training
        .iter()
        .map(|(predicted, observed)| predicted * observed)
        .sum::<f64>()
        / squares;
    let errors: Vec<f64> = held
        .iter()
        .filter(|(_, observed)| *observed > 0.0)
        .map(|(predicted, observed)| (observed - scale * predicted).abs() / observed)
        .collect();
    (!errors.is_empty()).then(|| errors.iter().sum::<f64>() / errors.len() as f64)
}

#[cfg(test)]
mod tests;
