use hdrhistogram::Histogram;
use serde::Serialize;
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// Derived metrics from a benchmark run.
pub struct Summary {
    pub secs: f64,
    pub ops: u64,
    pub ops_per_sec: f64,
    pub mb_per_sec: f64,
    pub p50: f64,
    pub p99: f64,
    pub p999: f64,
    pub p9999: f64,
}

/// One dashboard series a workload publishes itself, in place of the ops/sec
/// figure the harness derives.
///
/// Rate is the right summary for a workload that repeats one operation, and
/// the wrong one for a workload whose result IS a set of quantities: a
/// scenario sweep that reports bytes moved per row would publish "7 ops/sec"
/// (one per scenario), which is a number about the harness rather than about
/// the engine. Such a workload states its own series and the harness reports
/// those instead.
///
/// Each series states its [`Direction`], and the dashboard keeps the two
/// directions in separate suites: `github-action-benchmark` fixes one
/// direction per suite, so a cost that improves by shrinking is published as a
/// smaller-is-better series rather than as a reciprocal that reads upside down
/// or divides by zero.
///
/// Every output mode reports these in place of the rate: the dashboard entries,
/// the `--json` report and the human summary.
#[derive(Clone, Serialize)]
pub struct PublishedSeries {
    pub name: String,
    pub value: f64,
    pub unit: String,
    pub extra: String,
    pub direction: Direction,
}

/// The lower median of each published series across `iterations`, by its own
/// value: each series is a separate measurement, so its representative comes
/// from its own distribution and not from whichever iteration had the median
/// rate. Series are matched by name and kept in the first iteration's order.
pub fn median_series(iterations: &[&[PublishedSeries]]) -> Vec<PublishedSeries> {
    let Some(first) = iterations.first() else {
        return Vec::new();
    };
    first
        .iter()
        .map(|wanted| {
            let mut runs: Vec<&PublishedSeries> = iterations
                .iter()
                .filter_map(|it| it.iter().find(|s| s.name == wanted.name))
                .collect();
            runs.sort_by(|a, b| a.value.total_cmp(&b.value));
            // Lower median, as for the rate: len 1 → 0, 2 → 0, 3 → 1. `runs`
            // holds at least `wanted` itself, so the length is never zero.
            runs.get((runs.len() - 1) / 2)
                .copied()
                .unwrap_or(wanted)
                .clone()
        })
        .collect()
}

/// Dashboard entries, one suite per direction.
///
/// `github-action-benchmark` takes a single direction per suite, so yields and
/// costs are written to separate files and stored as separate suites; mixing
/// them would read one of the two upside down.
#[derive(Default)]
pub struct GithubSuites {
    /// Bigger-is-better entries: the `customBiggerIsBetter` suite.
    pub yields: Vec<serde_json::Value>,
    /// Smaller-is-better entries: the `customSmallerIsBetter` suite.
    pub costs: Vec<serde_json::Value>,
}

impl GithubSuites {
    /// Adds `entry` to the suite its direction belongs to.
    pub fn push(&mut self, direction: Direction, entry: serde_json::Value) {
        match direction {
            Direction::BiggerIsBetter => self.yields.push(entry),
            Direction::SmallerIsBetter => self.costs.push(entry),
        }
    }
}

/// Which way a series improves.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// A yield: rows per KiB read, operations per second.
    BiggerIsBetter,
    /// A cost: bytes copied per byte decoded, an amplification.
    #[cfg_attr(
        all(not(feature = "counters"), not(test)),
        expect(dead_code, reason = "only the counters workload publishes a cost")
    )]
    SmallerIsBetter,
}

/// Collects per-operation latencies and computes summary statistics.
pub struct Reporter {
    histogram: Histogram<u64>,
    start: Option<Instant>,
    elapsed: Duration,
    ops_counted: u64,
    published: Vec<PublishedSeries>,
}

impl Reporter {
    pub fn new() -> Self {
        Self {
            // Record up to 10 seconds (10_000_000_000 ns) with 3 significant digits.
            // Histogram creation with constant params cannot fail at runtime.
            #[expect(clippy::expect_used, reason = "constant histogram params")]
            histogram: Histogram::new_with_max(10_000_000_000, 3)
                .expect("failed to create histogram"),
            start: None,
            elapsed: Duration::ZERO,
            ops_counted: 0,
            published: Vec::new(),
        }
    }

    /// Publish a series this workload computes itself. See [`PublishedSeries`]
    /// for why a workload would, and [`Direction`] for which way it improves.
    #[cfg_attr(
        all(not(feature = "counters"), not(test)),
        expect(dead_code, reason = "only the counters workload publishes series")
    )]
    pub fn publish_series(
        &mut self,
        name: impl Into<String>,
        value: f64,
        unit: impl Into<String>,
        extra: impl Into<String>,
        direction: Direction,
    ) {
        self.published.push(PublishedSeries {
            name: name.into(),
            value,
            unit: unit.into(),
            extra: extra.into(),
            direction,
        });
    }

    /// The series this workload published, empty for every workload that
    /// reports a rate.
    pub fn published(&self) -> &[PublishedSeries] {
        &self.published
    }

    /// Replaces the published series, for a reporter that stands for several
    /// iterations and carries each series' own median (see [`median_series`]).
    pub fn replace_published(&mut self, series: Vec<PublishedSeries>) {
        self.published = series;
    }

    /// Start the measurement timer, resetting all prior state.
    pub fn start(&mut self) {
        self.histogram.reset();
        self.elapsed = Duration::ZERO;
        self.ops_counted = 0;
        self.published.clear();
        self.start = Some(Instant::now());
    }

    /// Record a single operation's latency in nanoseconds.
    /// Values exceeding the histogram max (10s) are clamped to avoid silent drops.
    #[expect(
        clippy::expect_used,
        reason = "Histogram::record can only fail for out-of-range values, which we clamp"
    )]
    #[inline]
    pub fn record(&mut self, nanos: u64) {
        // Clamp to histogram max (highest trackable value, set in new_with_max)
        // rather than silently dropping extreme values.
        let clamped = nanos.min(self.histogram.high());
        self.histogram
            .record(clamped)
            .expect("failed to record latency in histogram");
        self.ops_counted += 1;
    }

    /// Record a [`Duration`] as nanoseconds, saturating at u64::MAX.
    #[inline]
    pub fn record_duration(&mut self, d: Duration) {
        let nanos = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.record(nanos);
    }

    /// Stop the measurement timer.
    pub fn stop(&mut self) {
        if let Some(start) = self.start.take() {
            self.elapsed = start.elapsed();
        }
    }

    /// Merge another reporter's histogram into this one.
    #[expect(
        clippy::expect_used,
        reason = "Histogram::add can only fail with incompatible configurations — programmer error"
    )]
    pub fn merge(&mut self, other: &Reporter) {
        self.histogram
            .add(&other.histogram)
            .expect("failed to merge histograms: incompatible configurations");
        self.ops_counted += other.ops_counted;
        // Published series are not merged: `merge` folds one thread's share of
        // a rate workload into the whole, and a series is a quantity, not a
        // share — adding, averaging or concatenating per-thread copies would
        // each be wrong in a different way. No threaded workload publishes
        // one; the assertion holds that nothing is being dropped silently.
        debug_assert!(
            other.published.is_empty(),
            "a per-thread reporter published a series; decide how it combines \
             across threads before publishing from a threaded workload",
        );
    }

    /// Compute derived metrics from raw histogram + elapsed time.
    /// Shared by both human-readable and JSON output to avoid drift.
    pub fn summary(&self, entry_size: usize) -> Summary {
        let secs = self.elapsed.as_secs_f64();
        let ops = self.ops_counted;
        let ops_per_sec = if secs > 0.0 { ops as f64 / secs } else { 0.0 };
        // MB/sec = ops_counted * entry_size / elapsed. For mixed workloads
        // (readwhilewriting), ops_counted reflects only measured ops (reads),
        // so MB/sec represents read throughput under write pressure.
        let mb_per_sec = ops_per_sec * entry_size as f64 / (1024.0 * 1024.0);
        Summary {
            secs,
            ops,
            ops_per_sec,
            mb_per_sec,
            p50: self.percentile_us(50.0),
            p99: self.percentile_us(99.0),
            p999: self.percentile_us(99.9),
            p9999: self.percentile_us(99.99),
        }
    }

    /// Print human-readable results. Raw ops/sec, no
    /// cross-runner normalization — the bench is now pinned to a
    /// stable self-hosted runner where calibration smoothing
    /// would hide genuine perf changes alongside the variance it
    /// was originally introduced to mask.
    pub fn print_human(&self, benchmark: &str, entry_size: usize) {
        if !self.published.is_empty() {
            for series in &self.published {
                println!(
                    "{benchmark} / {}: {:.3} {}\n{:20} {}",
                    series.name, series.value, series.unit, "", series.extra,
                );
            }
            return;
        }
        let s = self.summary(entry_size);
        println!(
            "{benchmark:<20} {:>12} ops in {:.2}s  ({:>12.0} ops/sec, {:.1} MB/sec)",
            s.ops, s.secs, s.ops_per_sec, s.mb_per_sec,
        );
        println!(
            "{:20} P50: {:.1}us  P99: {:.1}us  P99.9: {:.1}us  P99.99: {:.1}us",
            "", s.p50, s.p99, s.p999, s.p9999,
        );
    }

    /// Produce JSON output. Same raw-numbers design as
    /// `print_human` — no normalization factor.
    pub fn to_json(&self, benchmark: &str, config: &JsonConfig) -> String {
        let s = self.summary(config.entry_size);

        // A workload that published its own series reports those instead of
        // the rate, exactly as on the dashboard: the rate would count
        // scenarios per second, which says nothing about the engine.
        let rate = self.published.is_empty().then_some(Rate {
            ops_total: s.ops,
            ops_per_sec: s.ops_per_sec,
            mb_per_sec: s.mb_per_sec,
            latency_us: LatencyUs {
                p50: s.p50,
                p99: s.p99,
                p999: s.p999,
                p9999: s.p9999,
            },
        });
        let report = JsonReport {
            benchmark: benchmark.to_string(),
            config: config.clone(),
            elapsed_secs: s.secs,
            rate,
            series: &self.published,
        };

        // Serialization of a fixed struct with primitive fields cannot fail.
        #[expect(clippy::expect_used, reason = "fixed struct serialization")]
        serde_json::to_string_pretty(&report).expect("failed to serialize JSON")
    }

    fn percentile_us(&self, p: f64) -> f64 {
        self.histogram.value_at_percentile(p) as f64 / 1000.0
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonConfig {
    pub num: u64,
    pub key_size: usize,
    pub value_size: usize,
    pub entry_size: usize,
    pub threads: usize,
    pub compression: String,
}

#[derive(Serialize)]
struct JsonReport<'a> {
    benchmark: String,
    config: JsonConfig,
    elapsed_secs: f64,
    /// The rate fields, flattened into the report; absent when the workload
    /// published series instead.
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    rate: Option<Rate>,
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    series: &'a [PublishedSeries],
}

#[derive(Serialize)]
struct Rate {
    ops_total: u64,
    ops_per_sec: f64,
    mb_per_sec: f64,
    latency_us: LatencyUs,
}

#[derive(Serialize)]
struct LatencyUs {
    p50: f64,
    p99: f64,
    p999: f64,
    p9999: f64,
}
