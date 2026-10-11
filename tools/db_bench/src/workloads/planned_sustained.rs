//! Sustained random overwrites with mixed point reads and range scans into two
//! trees side by side, given the same key stream and the same compaction
//! budget: one under the planner for query throughput, one under leveled
//! compaction.
//!
//! Compaction runs as it does in a deployment: in the background, on threads
//! of its own, throttled by the tree's compaction rate limit, while the writer
//! goes on and queries are served. Both trees get the same limit, a multiple
//! of the paced write rate, so a gain bought by spending more of the shared
//! budget does not show as one; the reads they serve contend with their
//! compactions for the machine. The series are taken during the stream, not
//! after it drains: the queries served per second of read time, the time of
//! point reads and of scans, the sorted runs a read meets and the debt each
//! strategy reports at every flush, the time of each flush, and the bytes and
//! thread time compaction spent. The two trees take each step in turn, which
//! goes first alternating, so a machine warming or slowing over the run weighs
//! on both alike.

use crate::config::BenchConfig;
use crate::db::make_sequential_key;
use crate::reporter::{Reporter, Suite};
use crate::workloads::Workload;
use hdrhistogram::Histogram;
use lsm_tree::compaction::{CompactionAction, CompactionStrategy, Leveled, Planned};
use lsm_tree::{AbstractTree, AnyTree, Guard, UserKey, UserValue};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct PlannedSustained;

#[cfg(test)]
mod tests;

/// The key stream's seed: both arms write the same keys in the same order.
const SEED: u64 = 0x5EED_0692;

/// The memtable bytes written between two flushes.
const FLUSH_BYTES: u64 = 1 << 20;

/// The target size of a table, small enough for a run of a few million
/// writes to fill several levels.
const TABLE_TARGET: u64 = 256 << 10;

/// Point reads, and scans, taken after each flush.
const POINTS_PER_FLUSH: u64 = 100;
const SCANS_PER_FLUSH: u64 = 100;

/// The keys a scan reads.
const SCAN_LENGTH: usize = 100;

/// Bytes per second the writer is paced to, into each tree.
const WRITE_RATE: u64 = 8 << 20;

/// Bytes per second each tree's compactions may write: the same budget for
/// both, twice the write rate, less than leveled calls for at depth, so debt
/// can build up while the tree is written to.
const COMPACTION_RATE: u64 = 2 * WRITE_RATE;

/// Compactions each tree may run at once.
const COMPACTION_THREADS: usize = 2;

/// How long a compaction thread with nothing to do waits before asking again.
const IDLE_WAIT: Duration = Duration::from_millis(2);

/// What one arm measured.
struct Arm {
    /// Bytes of keys and values written.
    written: u64,
    /// Bytes of the tables compactions wrote.
    compacted: u64,
    /// Thread time spent compacting, in nanoseconds.
    compaction_nanos: u64,
    /// Merges run.
    merges: u64,
    /// Sorted runs a read meets at each flush, summed and at their peak.
    runs_sum: u64,
    runs_peak: u64,
    /// Debt the arm's strategy reports at each flush.
    debt_peak: u64,
    debt_sum: u64,
    steps: u64,
    /// Point read and scan times, in nanoseconds.
    points: Histogram<u64>,
    scans: Histogram<u64>,
    /// Time of all reads and scans together, in nanoseconds.
    query_nanos: u64,
    /// Time of each flush, in nanoseconds: the time the writer is held.
    flushes: Histogram<u64>,
}

impl Arm {
    /// Nothing measured yet.
    fn new() -> Self {
        #[expect(clippy::expect_used, reason = "constant histogram params")]
        Self {
            written: 0,
            compacted: 0,
            compaction_nanos: 0,
            merges: 0,
            runs_sum: 0,
            runs_peak: 0,
            debt_peak: 0,
            debt_sum: 0,
            steps: 0,
            points: Histogram::new_with_max(10_000_000_000, 3).expect("valid histogram params"),
            scans: Histogram::new_with_max(10_000_000_000, 3).expect("valid histogram params"),
            query_nanos: 0,
            flushes: Histogram::new_with_max(1_000_000_000_000, 3).expect("valid histogram params"),
        }
    }
}

/// What a tree's compaction threads share with the writer.
#[derive(Default)]
struct Background {
    stop: AtomicBool,
    /// The sequence number below which every version may go: no snapshot is
    /// held, so the writer's.
    watermark: AtomicU64,
    merges: AtomicU64,
    nanos: AtomicU64,
}

/// One of the two trees and what it measured so far.
struct ArmRun {
    tree: AnyTree,
    strategy: Arc<dyn CompactionStrategy>,
    arm: Arm,
    /// The keys read after each step: the same stream for both trees.
    lookups: StdRng,
    background: Arc<Background>,
    workers: Vec<JoinHandle<lsm_tree::Result<()>>>,
    /// Removed when the run ends.
    _folder: tempfile::TempDir,
}

impl ArmRun {
    fn open(
        base: &Path,
        config: &BenchConfig,
        strategy: Arc<dyn CompactionStrategy>,
    ) -> lsm_tree::Result<Self> {
        let folder = tempfile::Builder::new()
            .prefix("planned-sustained-")
            .tempdir_in(base)?;
        let tree = crate::config::tree_builder(folder.path(), config)?
            .table_target_size(TABLE_TARGET)
            .compaction_rate_limit(COMPACTION_RATE)
            .open()?;
        let arm = Arm::new();
        let background = Arc::new(Background::default());
        let workers = (0..COMPACTION_THREADS)
            .map(|_| {
                let (tree, strategy, background) =
                    (tree.clone(), Arc::clone(&strategy), Arc::clone(&background));
                std::thread::spawn(move || compact_in_background(&tree, &strategy, &background))
            })
            .collect();
        Ok(Self {
            tree,
            strategy,
            arm,
            lookups: StdRng::seed_from_u64(SEED ^ 1),
            background,
            workers,
            _folder: folder,
        })
    }

    /// Flushes what was written since the last step, then samples runs, debt
    /// and queries while compaction goes on in the background.
    fn step(&mut self, seqno: u64, config: &BenchConfig) -> lsm_tree::Result<()> {
        let keys = key_space(config);
        self.background.watermark.store(seqno, Ordering::Relaxed);
        let flush = Instant::now();
        self.tree.flush_active_memtable(seqno)?;
        self.arm
            .flushes
            .saturating_record(u64::try_from(flush.elapsed().as_nanos()).unwrap_or(u64::MAX));
        let version = self.tree.current_version();
        let runs = version
            .iter_levels()
            .map(|level| level.run_count() as u64)
            .sum::<u64>();
        self.arm.runs_peak = self.arm.runs_peak.max(runs);
        self.arm.runs_sum += runs;
        let debt = self.strategy.pending_compaction_bytes(&version);
        self.arm.debt_peak = self.arm.debt_peak.max(debt);
        self.arm.debt_sum += debt;
        self.arm.steps += 1;
        // Point reads and scans interleaved, so neither meets a tree the
        // other has warmed alone.
        for _ in 0..POINTS_PER_FLUSH.max(SCANS_PER_FLUSH) {
            for scan in [false, true] {
                let (taken, wanted) = if scan {
                    (self.arm.scans.len(), SCANS_PER_FLUSH)
                } else {
                    (self.arm.points.len(), POINTS_PER_FLUSH)
                };
                if taken >= wanted * self.arm.steps {
                    continue;
                }
                let key = make_sequential_key(self.lookups.random_range(0..keys), config.key_size);
                let at = Instant::now();
                if scan {
                    for item in self
                        .tree
                        .range(key.as_slice().., seqno, None)
                        .take(SCAN_LENGTH)
                    {
                        item.value()?;
                    }
                } else {
                    self.tree.get(&key, seqno)?;
                }
                let nanos = u64::try_from(at.elapsed().as_nanos()).unwrap_or(u64::MAX);
                self.arm.query_nanos += nanos;
                // Above the histogram's range a read is recorded at its top.
                if scan {
                    self.arm.scans.saturating_record(nanos);
                } else {
                    self.arm.points.saturating_record(nanos);
                }
            }
        }
        Ok(())
    }

    /// Stops the compaction threads, then drains what the stream left without
    /// a limit, so both trees end at one their strategy has nothing more to do
    /// for and their compaction bytes compare.
    fn finish(mut self, seqno: u64) -> lsm_tree::Result<Arm> {
        self.background.stop.store(true, Ordering::Relaxed);
        for worker in self.workers.drain(..) {
            worker
                .join()
                .map_err(|_| lsm_tree::Error::Unrecoverable)??;
        }
        self.tree.compaction_rate_limiter().set_rate(0);
        let draining = Instant::now();
        loop {
            let result = self.tree.compact(Arc::clone(&self.strategy), seqno)?;
            match result.action {
                CompactionAction::Nothing => break,
                CompactionAction::Merged => self.arm.merges += 1,
                CompactionAction::Moved | CompactionAction::Dropped => {}
            }
        }
        self.arm.compaction_nanos += u64::try_from(draining.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            + self.background.nanos.load(Ordering::Relaxed);
        self.arm.merges += self.background.merges.load(Ordering::Relaxed);
        // A fresh tree: everything its compactions wrote is this run's.
        self.arm.compacted = self.tree.metrics().compaction_work_measured().written;
        Ok(std::mem::replace(&mut self.arm, Arm::new()))
    }
}

impl Drop for ArmRun {
    fn drop(&mut self) {
        // A run that failed midway still stops its threads before its folder
        // goes.
        self.background.stop.store(true, Ordering::Relaxed);
        for worker in self.workers.drain(..) {
            drop(worker.join());
        }
    }
}

/// One compaction thread: compacts while there is work, waits a moment when
/// there is none, until told to stop.
fn compact_in_background(
    tree: &AnyTree,
    strategy: &Arc<dyn CompactionStrategy>,
    background: &Background,
) -> lsm_tree::Result<()> {
    while !background.stop.load(Ordering::Relaxed) {
        let at = Instant::now();
        let result = tree.compact(
            Arc::clone(strategy),
            background.watermark.load(Ordering::Relaxed),
        )?;
        match result.action {
            CompactionAction::Nothing => std::thread::sleep(IDLE_WAIT),
            action => {
                background.nanos.fetch_add(
                    u64::try_from(at.elapsed().as_nanos()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                if action == CompactionAction::Merged {
                    background.merges.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
    Ok(())
}

/// Overwrites over half as many keys as writes reach a steady state where
/// every level is rewritten, not just appended to.
fn key_space(config: &BenchConfig) -> u64 {
    (config.num / 2).max(1)
}

/// The planner, told the scans' length and the tables' size.
fn planned() -> Arc<dyn CompactionStrategy> {
    Arc::new(
        Planned::default()
            .with_scan_length(SCAN_LENGTH as u32)
            .with_table_target_size(TABLE_TARGET),
    )
}

/// Writes the key stream into a planned and a leveled tree beneath `base` at
/// [`WRITE_RATE`], a step every [`FLUSH_BYTES`] and one for what is left at
/// the end, the two trees taking each step in turn with the first
/// alternating.
fn run_arms(base: &Path, config: &BenchConfig) -> lsm_tree::Result<(Arm, Arm)> {
    let mut arms = [
        ArmRun::open(base, config, planned())?,
        ArmRun::open(
            base,
            config,
            Arc::new(Leveled::default().with_table_target_size(TABLE_TARGET)),
        )?,
    ];
    let keys = key_space(config);
    let entry = (config.key_size + config.value_size) as u64;
    let value = UserValue::from(vec![b'v'; config.value_size]);
    let mut stream = StdRng::seed_from_u64(SEED);
    let mut seqno = 1u64;
    let mut pending = 0u64;
    let mut streamed = 0u64;
    let mut steps = 0u64;
    let started = Instant::now();
    let mut step = |arms: &mut [ArmRun; 2], seqno| -> lsm_tree::Result<()> {
        let order = if steps.is_multiple_of(2) {
            [0, 1]
        } else {
            [1, 0]
        };
        steps += 1;
        for index in order {
            if let Some(arm) = arms.get_mut(index) {
                arm.step(seqno, config)?;
            }
        }
        Ok(())
    };
    for _ in 0..config.num {
        // Shared by both trees: a clone is a reference count, not a copy.
        let key = UserKey::from(make_sequential_key(
            stream.random_range(0..keys),
            config.key_size,
        ));
        for arm in &mut arms {
            arm.tree.insert(key.clone(), value.clone(), seqno);
            arm.arm.written += entry;
        }
        seqno += 1;
        pending += entry;
        streamed += entry;
        if pending >= FLUSH_BYTES {
            step(&mut arms, seqno)?;
            pending = 0;
            // Paced: the stream is ahead of its rate, so the writer waits.
            let due = Duration::from_secs_f64(streamed as f64 / WRITE_RATE as f64);
            if let Some(ahead) = due.checked_sub(started.elapsed()) {
                std::thread::sleep(ahead);
            }
        }
    }
    // The writes after the last full step are a step of their own, so every
    // byte counted as written reaches the tables and is measured as the rest.
    if pending > 0 {
        step(&mut arms, seqno)?;
    }
    let [planned, leveled] = arms;
    Ok((planned.finish(seqno)?, leveled.finish(seqno)?))
}

/// Publishes `arm`'s series under `name`.
fn publish(reporter: &mut Reporter, name: &str, arm: &Arm, config: &BenchConfig) {
    let extra = format!(
        "num: {} | value_size: {} | steps: {} | merges: {} | scan length: {SCAN_LENGTH} | write \
         rate: {} MiB/s | compaction rate: {} MiB/s",
        config.num,
        config.value_size,
        arm.steps,
        arm.merges,
        WRITE_RATE >> 20,
        COMPACTION_RATE >> 20,
    );
    let ratio = |a: u64, b: u64| if b == 0 { 0.0 } else { a as f64 / b as f64 };
    let queries = arm.points.len() + arm.scans.len();
    let series: [(&str, f64, &str, Suite); 10] = [
        (
            "queries per second of read time",
            ratio(queries * 1_000_000_000, arm.query_nanos),
            "ops/s",
            Suite::Timings,
        ),
        (
            "point read p99",
            arm.points.value_at_quantile(0.99) as f64 / 1_000.0,
            "us",
            Suite::Timings,
        ),
        (
            "scan p99",
            arm.scans.value_at_quantile(0.99) as f64 / 1_000.0,
            "us",
            Suite::Timings,
        ),
        (
            "flush p99",
            arm.flushes.value_at_quantile(0.99) as f64 / 1_000_000.0,
            "ms",
            Suite::Timings,
        ),
        (
            "compaction thread time",
            arm.compaction_nanos as f64 / 1_000_000_000.0,
            "s",
            Suite::Timings,
        ),
        (
            "compaction bytes per written byte",
            ratio(arm.compacted, arm.written),
            "x",
            Suite::Costs,
        ),
        (
            "mean sorted runs",
            ratio(arm.runs_sum, arm.steps),
            "runs",
            Suite::Costs,
        ),
        (
            "peak sorted runs",
            arm.runs_peak as f64,
            "runs",
            Suite::Costs,
        ),
        (
            "mean debt",
            ratio(arm.debt_sum, arm.steps),
            "bytes",
            Suite::Costs,
        ),
        ("peak debt", arm.debt_peak as f64, "bytes", Suite::Costs),
    ];
    for (label, value, unit, suite) in series {
        reporter.publish_series(
            format!("{name} / {label}"),
            value,
            unit,
            extra.clone(),
            suite,
        );
    }
}

impl Workload for PlannedSustained {
    fn check_config(&self, config: &BenchConfig) -> Result<(), String> {
        if config.threads != 1 {
            return Err("planned-sustained runs one writer; pass --threads 1".to_string());
        }
        // Compaction bytes are counted in tables; a blob tree writes most of
        // its bytes to blob files, which the budget would leave out.
        if config.use_blob_tree {
            return Err(
                "planned-sustained measures table compaction; drop --use-blob-tree".to_string(),
            );
        }
        Ok(())
    }

    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        _seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // The harness's tree holds no data; its directory is where `--db` put
        // the run, so both trees are built beneath it, on the device asked for.
        reporter.start();
        let (planned, leveled) = run_arms(&tree.tree_config().path, config)?;
        publish(reporter, "planned", &planned, config);
        publish(reporter, "leveled", &leveled, config);
        reporter.stop();
        Ok(())
    }
}
