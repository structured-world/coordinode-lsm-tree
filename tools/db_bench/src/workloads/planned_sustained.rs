//! Sustained random overwrites with mixed point reads and range scans into two
//! trees side by side, given the same key stream and the same compaction
//! budget: one under the planner for query throughput, one under leveled
//! compaction.
//!
//! What the planner is for is the queries served while the tree is written
//! to, so the series are taken during the stream, not after it drains: the
//! queries served per second of read time, the time of point reads and of
//! scans, the sorted runs a read meets and the debt each strategy reports
//! after every flush, the time each flush and its compactions hold the writer,
//! and the bytes and time compaction spent. Both trees may write the same
//! compaction bytes per byte flushed, so a gain bought by spending more of the
//! budget does not show as one. The two trees take each step in turn, which
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
use std::sync::atomic::AtomicU64;
use std::time::Instant;

pub struct PlannedSustained;

#[cfg(test)]
mod tests;

/// The key stream's seed: both arms write the same keys in the same order.
const SEED: u64 = 0x5EED_0692;

/// The memtable bytes written between two flushes.
const FLUSH_BYTES: u64 = 1 << 20;

/// The target size of a table, small enough for a run of a few hundred
/// thousand writes to fill several levels.
const TABLE_TARGET: u64 = 256 << 10;

/// Point reads, and scans, taken after each flush and its compactions.
const POINTS_PER_FLUSH: u64 = 100;
const SCANS_PER_FLUSH: u64 = 100;

/// The keys a scan reads.
const SCAN_LENGTH: usize = 100;

/// The bytes compaction may write per byte flushed, the same for both trees:
/// less than leveled calls for, so debt builds up while the tree is written
/// to. A merge under way runs to its end and what it overspends is taken from
/// the next flush's budget; what is left is drained at the end of the stream.
const COMPACTION_BYTES_PER_FLUSHED_BYTE: i64 = 2;

/// What one arm measured.
struct Arm {
    /// Bytes of keys and values written.
    written: u64,
    /// Bytes of the tables compactions wrote.
    compacted: u64,
    /// Time spent compacting, in nanoseconds.
    compaction_nanos: u64,
    /// Merges run.
    merges: u64,
    /// Sorted runs a read meets after each step, summed and at their peak.
    runs_sum: u64,
    runs_peak: u64,
    /// Debt the arm's strategy reports after each step.
    debt_peak: u64,
    debt_sum: u64,
    steps: u64,
    /// Point read and scan times, in nanoseconds.
    points: Histogram<u64>,
    scans: Histogram<u64>,
    /// Time of all reads and scans together, in nanoseconds.
    query_nanos: u64,
    /// Time of each flush and the compactions run after it, in nanoseconds:
    /// the time the writer is held.
    step_times: Histogram<u64>,
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

/// One of the two trees and what it measured so far.
struct ArmRun {
    tree: AnyTree,
    strategy: Arc<dyn CompactionStrategy>,
    arm: Arm,
    /// Bytes compaction may still write: negative after a merge overspent it.
    allowance: i64,
    /// The keys read after each step: the same stream for both trees.
    lookups: StdRng,
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
            .open()?;
        #[expect(clippy::expect_used, reason = "constant histogram params")]
        let arm = Arm {
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
            step_times: Histogram::new_with_max(1_000_000_000_000, 3)
                .expect("valid histogram params"),
        };
        Ok(Self {
            tree,
            strategy,
            arm,
            allowance: 0,
            lookups: StdRng::seed_from_u64(SEED ^ 1),
            _folder: folder,
        })
    }

    /// Flushes the `flushed` bytes written since the last step, runs the
    /// compactions its budget allows, and samples runs, debt and queries.
    fn step(&mut self, flushed: u64, seqno: u64, config: &BenchConfig) -> lsm_tree::Result<()> {
        let keys = key_space(config);
        let step = Instant::now();
        self.tree.flush_active_memtable(seqno)?;
        // A flush's budget is the burst: bytes left unspent while there was
        // nothing to compact are not saved up for one long step later.
        let budget = COMPACTION_BYTES_PER_FLUSHED_BYTE * i64::try_from(flushed).unwrap_or(i64::MAX);
        self.allowance = (self.allowance + budget).min(budget);
        let compacting = Instant::now();
        compact(
            &self.tree,
            &self.strategy,
            seqno,
            Some(&mut self.allowance),
            &mut self.arm,
        )?;
        self.arm.compaction_nanos +=
            u64::try_from(compacting.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.arm
            .step_times
            .saturating_record(u64::try_from(step.elapsed().as_nanos()).unwrap_or(u64::MAX));
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
                let wanted = if scan {
                    SCANS_PER_FLUSH
                } else {
                    POINTS_PER_FLUSH
                };
                let taken = if scan {
                    self.arm.scans.len()
                } else {
                    self.arm.points.len()
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

    /// Drains what the stream left, so both trees end at one their strategy
    /// has nothing more to do for and their compaction bytes compare.
    fn finish(mut self, seqno: u64) -> lsm_tree::Result<Arm> {
        let compacting = Instant::now();
        compact(&self.tree, &self.strategy, seqno, None, &mut self.arm)?;
        self.arm.compaction_nanos +=
            u64::try_from(compacting.elapsed().as_nanos()).unwrap_or(u64::MAX);
        // A fresh tree: everything its compactions wrote is this run's.
        self.arm.compacted = self.tree.metrics().compaction_work_measured().written;
        Ok(self.arm)
    }
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

/// Writes the key stream into a planned and a leveled tree beneath `base`, a
/// step every [`FLUSH_BYTES`] and one for what is left at the end, the two
/// trees taking each step in turn with the first alternating.
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
    let mut steps = 0u64;
    let mut step = |arms: &mut [ArmRun; 2], flushed, seqno| -> lsm_tree::Result<()> {
        let order = if steps.is_multiple_of(2) {
            [0, 1]
        } else {
            [1, 0]
        };
        steps += 1;
        for index in order {
            if let Some(arm) = arms.get_mut(index) {
                arm.step(flushed, seqno, config)?;
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
        if pending >= FLUSH_BYTES {
            step(&mut arms, pending, seqno)?;
            pending = 0;
        }
    }
    // The writes after the last full step are a step of their own, so every
    // byte counted as written reaches the tables and is measured as the rest.
    if pending > 0 {
        step(&mut arms, pending, seqno)?;
    }
    let [planned, leveled] = arms;
    Ok((planned.finish(seqno)?, leveled.finish(seqno)?))
}

/// Runs compactions until the strategy has nothing to do, or while
/// `allowance` has bytes left, taking what each writes from it. No snapshot is
/// held, so every version below `seqno` may go.
fn compact(
    tree: &AnyTree,
    strategy: &Arc<dyn CompactionStrategy>,
    seqno: u64,
    mut allowance: Option<&mut i64>,
    arm: &mut Arm,
) -> lsm_tree::Result<()> {
    while allowance.as_deref().is_none_or(|left| *left > 0) {
        let result = tree.compact(Arc::clone(strategy), seqno)?;
        if let Some(left) = allowance.as_deref_mut() {
            *left -= i64::try_from(result.work.measured.written).unwrap_or(i64::MAX);
        }
        match result.action {
            CompactionAction::Nothing => break,
            CompactionAction::Merged => arm.merges += 1,
            CompactionAction::Moved | CompactionAction::Dropped => {}
        }
    }
    Ok(())
}

/// Publishes `arm`'s series under `name`.
fn publish(reporter: &mut Reporter, name: &str, arm: &Arm, config: &BenchConfig) {
    let extra = format!(
        "num: {} | value_size: {} | steps: {} | merges: {} | scan length: {SCAN_LENGTH}",
        config.num, config.value_size, arm.steps, arm.merges
    );
    let ratio = |a: u64, b: u64| if b == 0 { 0.0 } else { a as f64 / b as f64 };
    let queries = arm.points.len() + arm.scans.len();
    reporter.publish_series(
        format!("{name} / queries per second of read time"),
        ratio(queries * 1_000_000_000, arm.query_nanos),
        "ops/s",
        extra.clone(),
        Suite::Timings,
    );
    reporter.publish_series(
        format!("{name} / point read p99"),
        arm.points.value_at_quantile(0.99) as f64 / 1_000.0,
        "us",
        extra.clone(),
        Suite::Timings,
    );
    reporter.publish_series(
        format!("{name} / scan p99"),
        arm.scans.value_at_quantile(0.99) as f64 / 1_000.0,
        "us",
        extra.clone(),
        Suite::Timings,
    );
    reporter.publish_series(
        format!("{name} / flush and compaction step p99"),
        arm.step_times.value_at_quantile(0.99) as f64 / 1_000_000.0,
        "ms",
        extra.clone(),
        Suite::Timings,
    );
    reporter.publish_series(
        format!("{name} / compaction time"),
        arm.compaction_nanos as f64 / 1_000_000_000.0,
        "s",
        extra.clone(),
        Suite::Timings,
    );
    reporter.publish_series(
        format!("{name} / compaction bytes per written byte"),
        ratio(arm.compacted, arm.written),
        "x",
        extra.clone(),
        Suite::Costs,
    );
    reporter.publish_series(
        format!("{name} / mean sorted runs"),
        ratio(arm.runs_sum, arm.steps),
        "runs",
        extra.clone(),
        Suite::Costs,
    );
    reporter.publish_series(
        format!("{name} / peak sorted runs"),
        arm.runs_peak as f64,
        "runs",
        extra.clone(),
        Suite::Costs,
    );
    reporter.publish_series(
        format!("{name} / mean debt"),
        ratio(arm.debt_sum, arm.steps),
        "bytes",
        extra.clone(),
        Suite::Costs,
    );
    reporter.publish_series(
        format!("{name} / peak debt"),
        arm.debt_peak as f64,
        "bytes",
        extra,
        Suite::Costs,
    );
}
