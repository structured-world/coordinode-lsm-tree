//! Sustained random overwrites under leveled compaction, run twice over the
//! same key stream: once with compaction outputs cut on the boundaries of the
//! level below, once on size alone.
//!
//! What a cut on a boundary buys is the merge that follows it: an output that
//! straddles two tables of the level below drags both into that merge. So the
//! series are what compaction costs while the tree is written to, not after it
//! drains: the bytes compaction writes per byte written, the tables each merge
//! reads, the debt left after each flush with compaction running behind the
//! writes, the time each flush and its compactions hold the writer, and the
//! time of point reads taken between flushes.

use crate::config::BenchConfig;
use crate::db::make_sequential_key;
use crate::reporter::{Reporter, Suite};
use crate::workloads::Workload;
use hdrhistogram::Histogram;
use lsm_tree::compaction::{CompactionAction, CompactionStrategy, Leveled};
use lsm_tree::{AbstractTree, AnyTree, UserValue};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

pub struct LeveledSustained;

/// The key stream's seed: both arms write the same keys in the same order.
const SEED: u64 = 0x5EED_0661;

/// The memtable bytes written between two flushes.
const FLUSH_BYTES: u64 = 1 << 20;

/// The target size of a table, small enough for a run of a few hundred
/// thousand writes to fill several levels.
const TABLE_TARGET: u64 = 256 << 10;

/// Point reads taken after each flush and the compactions it triggered.
const READS_PER_FLUSH: u64 = 200;

/// Compactions run after each flush: fewer than the writes call for, so debt
/// builds up while the tree is written to, as when compaction lags ingest.
/// What is left is drained once the stream ends.
const COMPACTIONS_PER_FLUSH: usize = 2;

/// What one arm measured.
struct Arm {
    /// Bytes of keys and values written.
    written: u64,
    /// Bytes of the tables compactions wrote.
    compacted: u64,
    /// Merges run, and the tables they read.
    merges: u64,
    merged_tables: u64,
    /// Debt after each flush and the compactions run after it.
    debt_peak: u64,
    debt_sum: u64,
    flushes: u64,
    /// Point read times, in nanoseconds.
    reads: Histogram<u64>,
    /// Time of each flush and the compactions run after it, in nanoseconds:
    /// the time the writer is held.
    steps: Histogram<u64>,
}

impl Workload for LeveledSustained {
    fn check_config(&self, config: &BenchConfig) -> Result<(), String> {
        if config.threads != 1 {
            return Err("leveled-sustained runs one writer; pass --threads 1".to_string());
        }
        Ok(())
    }

    fn run(
        &self,
        _tree: &AnyTree,
        config: &BenchConfig,
        _seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        reporter.start();
        for (name, aligned) in [("aligned", true), ("size-only", false)] {
            let arm = run_arm(config, aligned)?;
            publish(reporter, name, &arm, config);
        }
        reporter.stop();
        Ok(())
    }
}

/// Writes the key stream into a fresh tree, flushing every [`FLUSH_BYTES`]
/// and compacting until the strategy has nothing left to do.
fn run_arm(config: &BenchConfig, aligned: bool) -> lsm_tree::Result<Arm> {
    let folder = tempfile::tempdir()?;
    let tree = crate::config::tree_builder(folder.path(), config)?
        .table_target_size(TABLE_TARGET)
        .compaction_output_alignment(aligned)
        .open()?;
    let strategy: Arc<dyn CompactionStrategy> =
        Arc::new(Leveled::default().with_table_target_size(TABLE_TARGET));

    // Overwrites over half as many keys as writes reach a steady state where
    // every level is rewritten, not just appended to.
    let keys = (config.num / 2).max(1);
    let entry = (config.key_size + config.value_size) as u64;
    let value = UserValue::from(vec![b'v'; config.value_size]);
    let mut stream = StdRng::seed_from_u64(SEED);
    let mut lookups = StdRng::seed_from_u64(SEED ^ 1);
    let compacted_before = tree.metrics().compaction_bytes_written();

    #[expect(clippy::expect_used, reason = "constant histogram params")]
    let mut arm = Arm {
        written: 0,
        compacted: 0,
        merges: 0,
        merged_tables: 0,
        debt_peak: 0,
        debt_sum: 0,
        flushes: 0,
        reads: Histogram::new_with_max(10_000_000_000, 3).expect("valid histogram params"),
        steps: Histogram::new_with_max(1_000_000_000_000, 3).expect("valid histogram params"),
    };
    let mut seqno = 1u64;
    let mut pending = 0u64;
    for _ in 0..config.num {
        let key = make_sequential_key(stream.random_range(0..keys), config.key_size);
        tree.insert(key, value.clone(), seqno);
        seqno += 1;
        arm.written += entry;
        pending += entry;
        if pending < FLUSH_BYTES {
            continue;
        }
        pending = 0;
        let step = Instant::now();
        tree.flush_active_memtable(seqno)?;
        compact(
            &tree,
            &strategy,
            seqno,
            Some(COMPACTIONS_PER_FLUSH),
            &mut arm,
        )?;
        let nanos = u64::try_from(step.elapsed().as_nanos()).unwrap_or(u64::MAX);
        arm.steps.saturating_record(nanos);
        let debt = strategy.pending_compaction_bytes(&tree.current_version());
        arm.debt_peak = arm.debt_peak.max(debt);
        arm.debt_sum += debt;
        arm.flushes += 1;
        for _ in 0..READS_PER_FLUSH {
            let key = make_sequential_key(lookups.random_range(0..keys), config.key_size);
            let at = Instant::now();
            tree.get(&key, seqno)?;
            let nanos = u64::try_from(at.elapsed().as_nanos()).unwrap_or(u64::MAX);
            // Above the histogram's range a read is recorded at its top.
            arm.reads.saturating_record(nanos);
        }
    }
    // What the stream left is drained, so both arms end at a tree the strategy
    // has nothing more to do for and their compaction bytes compare.
    compact(&tree, &strategy, seqno, None, &mut arm)?;
    arm.compacted = tree.metrics().compaction_bytes_written() - compacted_before;
    Ok(arm)
}

/// Runs compactions until the strategy has nothing to do, or `limit` of them.
/// No snapshot is held, so every version below `seqno` may go.
fn compact(
    tree: &AnyTree,
    strategy: &Arc<dyn CompactionStrategy>,
    seqno: u64,
    limit: Option<usize>,
    arm: &mut Arm,
) -> lsm_tree::Result<()> {
    for _ in 0..limit.unwrap_or(usize::MAX) {
        let result = tree.compact(Arc::clone(strategy), seqno)?;
        match result.action {
            CompactionAction::Nothing => break,
            CompactionAction::Merged => {
                arm.merges += 1;
                arm.merged_tables += result.tables_in as u64;
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
        extra,
        Suite::Timings,
    );
}
