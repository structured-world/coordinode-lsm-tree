//! Sustained random overwrites under leveled compaction into two trees side by
//! side, given the same key stream: one with compaction outputs cut on the
//! boundaries of the level below, one on size alone.
//!
//! What a cut on a boundary buys is the merge that follows it: an output that
//! straddles two tables of the level below drags both into that merge. So the
//! series are what compaction costs while the tree is written to, not after it
//! drains: the bytes compaction writes per byte written, the tables each merge
//! reads, the debt left after each flush with compaction running behind the
//! writes, the time each flush and its compactions hold the writer, and the
//! time of point reads taken between flushes. The two trees take each flush in
//! turn, which goes first alternating, so a machine warming or slowing over
//! the run weighs on both alike.

use crate::config::BenchConfig;
use crate::db::make_sequential_key;
use crate::reporter::{Reporter, Suite};
use crate::workloads::Workload;
use hdrhistogram::Histogram;
use lsm_tree::compaction::{CompactionAction, CompactionStrategy, Leveled};
use lsm_tree::{AbstractTree, AnyTree, UserKey, UserValue};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

pub struct LeveledSustained;

#[cfg(test)]
mod tests;

/// The key stream's seed: both arms write the same keys in the same order.
const SEED: u64 = 0x5EED_0661;

/// The memtable bytes written between two flushes.
const FLUSH_BYTES: u64 = 1 << 20;

/// The target size of a table, small enough for a run of a few hundred
/// thousand writes to fill several levels.
const TABLE_TARGET: u64 = 256 << 10;

/// Point reads taken after each flush and the compactions it triggered.
const READS_PER_FLUSH: u64 = 200;

/// The bytes compaction may write per byte flushed: less than the writes call
/// for, so debt builds up while the tree is written to, as when compaction I/O
/// lags ingest. Both arms get the same budget in bytes rather than in merges,
/// since a merge of fewer tables costs less. A merge under way runs to its
/// end and what it overspends is taken from the next flush's budget, so over
/// the stream both arms spend alike; what is left is drained at its end.
const COMPACTION_BYTES_PER_FLUSHED_BYTE: i64 = 2;

/// What one arm measured.
struct Arm {
    /// Bytes of keys and values written.
    written: u64,
    /// Bytes of the tables compactions wrote.
    compacted: u64,
    /// Merges run, and the tables they read.
    merges: u64,
    merged_tables: u64,
    /// Tables the merges wrote.
    output_tables: u64,
    /// Debt after each flush and the compactions run after it.
    debt_peak: u64,
    debt_sum: u64,
    flushes: u64,
    /// Point read times, in nanoseconds.
    reads: Histogram<u64>,
    /// Time of each flush and the compactions run after it, in nanoseconds:
    /// the time the writer is held.
    steps: Histogram<u64>,
    /// Bytes the compactions of each step wrote.
    step_bytes: Histogram<u64>,
    /// Time of all steps together, in nanoseconds.
    steps_total: u64,
}

impl Workload for LeveledSustained {
    fn check_config(&self, config: &BenchConfig) -> Result<(), String> {
        if config.threads != 1 {
            return Err("leveled-sustained runs one writer; pass --threads 1".to_string());
        }
        // Compaction bytes are counted in tables; a blob tree writes most of
        // its bytes to blob files, which the ratio would leave out.
        if config.use_blob_tree {
            return Err(
                "leveled-sustained measures table compaction; drop --use-blob-tree".to_string(),
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
        let (aligned, size_only) = run_arms(&tree.tree_config().path, config)?;
        publish(reporter, "aligned", &aligned, config);
        publish(reporter, "size-only", &size_only, config);
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
    fn open(base: &Path, config: &BenchConfig, aligned: bool) -> lsm_tree::Result<Self> {
        let folder = tempfile::Builder::new()
            .prefix("leveled-sustained-")
            .tempdir_in(base)?;
        let tree = crate::config::tree_builder(folder.path(), config)?
            .table_target_size(TABLE_TARGET)
            .compaction_output_alignment(aligned)
            .open()?;
        #[expect(clippy::expect_used, reason = "constant histogram params")]
        let arm = Arm {
            written: 0,
            compacted: 0,
            merges: 0,
            merged_tables: 0,
            output_tables: 0,
            debt_peak: 0,
            debt_sum: 0,
            flushes: 0,
            reads: Histogram::new_with_max(10_000_000_000, 3).expect("valid histogram params"),
            steps: Histogram::new_with_max(1_000_000_000_000, 3).expect("valid histogram params"),
            step_bytes: Histogram::new_with_max(1 << 40, 3).expect("valid histogram params"),
            steps_total: 0,
        };
        Ok(Self {
            tree,
            strategy: Arc::new(Leveled::default().with_table_target_size(TABLE_TARGET)),
            arm,
            allowance: 0,
            lookups: StdRng::seed_from_u64(SEED ^ 1),
            _folder: folder,
        })
    }

    /// Flushes the `flushed` bytes written since the last step, runs the
    /// compactions its budget allows, and samples debt and point reads.
    fn step(&mut self, flushed: u64, seqno: u64, config: &BenchConfig) -> lsm_tree::Result<()> {
        let keys = key_space(config);
        let step = Instant::now();
        self.tree.flush_active_memtable(seqno)?;
        // A flush's budget is the burst: bytes left unspent while there was
        // nothing to compact are not saved up for one long step later.
        let budget = COMPACTION_BYTES_PER_FLUSHED_BYTE * i64::try_from(flushed).unwrap_or(i64::MAX);
        self.allowance = (self.allowance + budget).min(budget);
        let bytes_before = self.tree.metrics().compaction_bytes_written();
        compact(
            &self.tree,
            &self.strategy,
            seqno,
            Some(&mut self.allowance),
            &mut self.arm,
        )?;
        let nanos = u64::try_from(step.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.arm.steps.saturating_record(nanos);
        self.arm.steps_total += nanos;
        self.arm
            .step_bytes
            .saturating_record(self.tree.metrics().compaction_bytes_written() - bytes_before);
        let debt = self
            .strategy
            .pending_compaction_bytes(&self.tree.current_version());
        self.arm.debt_peak = self.arm.debt_peak.max(debt);
        self.arm.debt_sum += debt;
        self.arm.flushes += 1;
        for _ in 0..READS_PER_FLUSH {
            let key = make_sequential_key(self.lookups.random_range(0..keys), config.key_size);
            let at = Instant::now();
            self.tree.get(&key, seqno)?;
            let nanos = u64::try_from(at.elapsed().as_nanos()).unwrap_or(u64::MAX);
            // Above the histogram's range a read is recorded at its top.
            self.arm.reads.saturating_record(nanos);
        }
        Ok(())
    }

    /// Drains what the stream left, so both trees end at one the strategy has
    /// nothing more to do for and their compaction bytes compare.
    fn finish(mut self, seqno: u64) -> lsm_tree::Result<Arm> {
        compact(&self.tree, &self.strategy, seqno, None, &mut self.arm)?;
        // A fresh tree: everything its compactions wrote is this run's.
        self.arm.compacted = self.tree.metrics().compaction_bytes_written();
        Ok(self.arm)
    }
}

/// Overwrites over half as many keys as writes reach a steady state where
/// every level is rewritten, not just appended to.
fn key_space(config: &BenchConfig) -> u64 {
    (config.num / 2).max(1)
}

/// Writes the key stream into an aligned and a size-only tree beneath `base`,
/// a step every [`FLUSH_BYTES`] and one for what is left at the end, the two
/// trees taking each step in turn with the first alternating.
fn run_arms(base: &Path, config: &BenchConfig) -> lsm_tree::Result<(Arm, Arm)> {
    let mut arms = [
        ArmRun::open(base, config, true)?,
        ArmRun::open(base, config, false)?,
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
    let [aligned, size_only] = arms;
    Ok((aligned.finish(seqno)?, size_only.finish(seqno)?))
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
        let before = tree.metrics().compaction_bytes_written();
        let result = tree.compact(Arc::clone(strategy), seqno)?;
        if let Some(left) = allowance.as_deref_mut() {
            let written = tree.metrics().compaction_bytes_written() - before;
            *left -= i64::try_from(written).unwrap_or(i64::MAX);
        }
        match result.action {
            CompactionAction::Nothing => break,
            CompactionAction::Merged => {
                arm.merges += 1;
                arm.merged_tables += result.tables_in as u64;
                arm.output_tables += result.tables_out as u64;
            }
            CompactionAction::Moved | CompactionAction::Dropped => {}
        }
    }
    Ok(())
}

/// Publishes `arm`'s series under `name`.
fn publish(reporter: &mut Reporter, name: &str, arm: &Arm, config: &BenchConfig) {
    let extra = format!(
        "num: {} | value_size: {} | flushes: {} | merges: {}",
        config.num, config.value_size, arm.flushes, arm.merges
    );
    let ratio = |a: u64, b: u64| if b == 0 { 0.0 } else { a as f64 / b as f64 };
    reporter.publish_series(
        format!("{name} / compaction bytes per written byte"),
        ratio(arm.compacted, arm.written),
        "x",
        extra.clone(),
        Suite::Costs,
    );
    reporter.publish_series(
        format!("{name} / tables per merge"),
        ratio(arm.merged_tables, arm.merges),
        "tables",
        extra.clone(),
        Suite::Costs,
    );
    reporter.publish_series(
        format!("{name} / output tables per merge"),
        ratio(arm.output_tables, arm.merges),
        "tables",
        extra.clone(),
        Suite::Costs,
    );
    reporter.publish_series(
        format!("{name} / peak debt"),
        arm.debt_peak as f64,
        "bytes",
        extra.clone(),
        Suite::Costs,
    );
    reporter.publish_series(
        format!("{name} / mean debt"),
        ratio(arm.debt_sum, arm.flushes),
        "bytes",
        extra.clone(),
        Suite::Costs,
    );
    reporter.publish_series(
        format!("{name} / read p99"),
        arm.reads.value_at_quantile(0.99) as f64 / 1_000.0,
        "us",
        extra.clone(),
        Suite::Timings,
    );
    reporter.publish_series(
        format!("{name} / flush and compaction step p99"),
        arm.steps.value_at_quantile(0.99) as f64 / 1_000_000.0,
        "ms",
        extra.clone(),
        Suite::Timings,
    );
    reporter.publish_series(
        format!("{name} / flush and compaction steps total"),
        arm.steps_total as f64 / 1_000_000_000.0,
        "s",
        extra.clone(),
        Suite::Timings,
    );
    reporter.publish_series(
        format!("{name} / compaction bytes per step p99"),
        arm.step_bytes.value_at_quantile(0.99) as f64,
        "bytes",
        extra,
        Suite::Costs,
    );
}
