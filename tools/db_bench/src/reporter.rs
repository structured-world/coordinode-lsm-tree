use hdrhistogram::Histogram;
use serde::Serialize;
use std::time::{Duration, Instant};

/// Collects per-operation latencies and computes summary statistics.
pub struct Reporter {
    histogram: Histogram<u64>,
    start: Option<Instant>,
    elapsed: Duration,
    ops_counted: u64,
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
        }
    }

    /// Start the measurement timer, resetting all prior state.
    pub fn start(&mut self) {
        self.histogram.reset();
        self.elapsed = Duration::ZERO;
        self.ops_counted = 0;
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
    }

    /// Print human-readable results.
    pub fn print_human(&self, benchmark: &str, entry_size: usize) {
        let secs = self.elapsed.as_secs_f64();
        let ops = self.ops_counted;
        let ops_per_sec = if secs > 0.0 { ops as f64 / secs } else { 0.0 };
        // MB/sec = ops_counted * entry_size / elapsed. For mixed workloads
        // (readwhilewriting), ops_counted reflects only measured ops (reads),
        // so MB/sec represents read throughput under write pressure.
        let mb_per_sec = ops_per_sec * entry_size as f64 / (1024.0 * 1024.0);

        println!(
            "{benchmark:<20} {ops:>12} ops in {secs:.2}s  ({ops_per_sec:>12.0} ops/sec, {mb_per_sec:.1} MB/sec)"
        );
        println!(
            "{:20} P50: {:.1}us  P99: {:.1}us  P99.9: {:.1}us  P99.99: {:.1}us",
            "",
            self.percentile_us(50.0),
            self.percentile_us(99.0),
            self.percentile_us(99.9),
            self.percentile_us(99.99),
        );
    }

    /// Produce JSON output.
    pub fn to_json(&self, benchmark: &str, config: &JsonConfig) -> String {
        let secs = self.elapsed.as_secs_f64();
        let ops = self.ops_counted;
        let ops_per_sec = if secs > 0.0 { ops as f64 / secs } else { 0.0 };
        let mb_per_sec = ops_per_sec * config.entry_size as f64 / (1024.0 * 1024.0);

        let report = JsonReport {
            benchmark: benchmark.to_string(),
            config: config.clone(),
            elapsed_secs: secs,
            ops_total: ops,
            ops_per_sec,
            mb_per_sec,
            latency_us: LatencyUs {
                p50: self.percentile_us(50.0),
                p99: self.percentile_us(99.0),
                p999: self.percentile_us(99.9),
                p9999: self.percentile_us(99.99),
            },
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
struct JsonReport {
    benchmark: String,
    config: JsonConfig,
    elapsed_secs: f64,
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
