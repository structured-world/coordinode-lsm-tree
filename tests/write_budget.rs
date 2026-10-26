//! The write budget of a medium: compaction reserves its output before it
//! runs, waits without credit, runs past the budget only as a counted
//! override, and every tree and route on the medium spends the one budget.

use lsm_tree::compaction::{CompactionAction, MoveDown};
use lsm_tree::config::LevelRoute;
use lsm_tree::fs::{CountingFs, Fs, MemFs, WriteIoStats, WriteOrigin};
use lsm_tree::write_budget::{Allowance, BudgetCheckpoint, WriteBudget};
use lsm_tree::{
    AbstractTree, AnyTree, BackpressureThresholds, Clock, Config, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const DAY: Duration = Duration::from_secs(86_400);

/// A gibibyte a day, a gibibyte of burst: credit enough for any merge here.
fn plenty() -> Allowance {
    Allowance::new(1 << 30, DAY)
}

/// A wall clock the test moves by hand.
#[derive(Default)]
struct HandClock(AtomicU64);

impl HandClock {
    fn advance_secs(&self, secs: u64) {
        self.0.fetch_add(secs, Ordering::Relaxed);
    }
}

impl Clock for HandClock {
    fn unix_time(&self) -> Duration {
        Duration::from_secs(self.0.load(Ordering::Relaxed))
    }
}

/// A medium with a budget, the backend writing to it, and the clock the
/// budget reads.
struct Medium {
    budget: Arc<WriteBudget>,
    fs: Arc<dyn Fs>,
    clock: Arc<HandClock>,
}

impl Medium {
    /// A budget of `allowance` with a full burst of credit.
    fn full(allowance: Allowance) -> Self {
        Self::with(WriteBudget::new_at(
            Arc::new(WriteIoStats::default()),
            allowance,
            Duration::ZERO,
        ))
    }

    /// A budget of `allowance` holding no credit yet: every write overruns
    /// it until the refill catches up.
    fn empty(allowance: Allowance) -> Self {
        Self::with(WriteBudget::resume_at(
            Arc::new(WriteIoStats::default()),
            allowance,
            BudgetCheckpoint {
                credit: 0,
                as_of: Duration::ZERO,
            },
            0,
            Duration::ZERO,
        ))
    }

    fn with(budget: WriteBudget) -> Self {
        let clock = Arc::new(HandClock::default());
        let budget = Arc::new(budget.with_clock(Arc::clone(&clock) as Arc<dyn Clock>));
        let fs = Arc::new(CountingFs::with_budget(
            Arc::new(MemFs::new()),
            Arc::clone(&budget),
        ));
        Self { budget, fs, clock }
    }

    fn written(&self, origin: WriteOrigin) -> u64 {
        self.budget.ledger().snapshot().origin(origin).bytes
    }
}

fn config(path: &str, fs: Arc<dyn Fs>) -> Config {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
}

/// Three flushed tables of the same keys, the newest values last.
fn three_tables(tree: &AnyTree, seqno: SeqNo) -> lsm_tree::Result<()> {
    for round in 0..3u64 {
        for i in 0..200u64 {
            tree.insert(
                format!("key{i:05}"),
                format!("value{round}").repeat(8),
                seqno + round * 1_000 + i,
            );
        }
        tree.flush_active_memtable(0)?;
    }
    Ok(())
}

fn assert_latest_values(tree: &AnyTree) -> lsm_tree::Result<()> {
    for i in 0..200u64 {
        assert_eq!(
            tree.get(format!("key{i:05}"), SeqNo::MAX)?.as_deref(),
            Some("value2".repeat(8).as_bytes()),
            "key{i:05}"
        );
    }
    Ok(())
}

fn standard(tree: &AnyTree) -> &lsm_tree::Tree {
    match tree {
        AnyTree::Standard(tree) => tree,
        AnyTree::Blob(_) => unreachable!("no tree here separates blobs"),
    }
}

/// A medium its flushes overran: a merge waits, the tree keeps its tables and
/// serves every key, and the budget reports the refusal and since when the
/// work waits. Once the refill covers the merge it runs.
#[test]
fn a_merge_without_credit_waits_and_leaves_the_tree_whole() -> lsm_tree::Result<()> {
    let medium = Medium::empty(plenty());
    let tree = config("/db", Arc::clone(&medium.fs)).open()?;
    three_tables(&tree, 0)?;
    let tables = tree.table_count();

    let result = tree.major_compact(u64::MAX, SeqNo::MAX)?;

    assert_eq!(result.action, CompactionAction::Nothing);
    assert_eq!(tree.table_count(), tables, "nothing was merged");
    assert_eq!(
        medium.written(WriteOrigin::CompactionTable),
        0,
        "nothing was written"
    );
    assert_latest_values(&tree)?;
    let report = medium.budget.report();
    assert_eq!(report.refused, 1);
    assert_eq!(report.waiting_since, Some(Duration::ZERO));
    assert_eq!(report.reserved, 0, "the refused merge holds nothing");

    medium.clock.advance_secs(DAY.as_secs());
    let result = tree.major_compact(u64::MAX, SeqNo::MAX)?;

    assert_ne!(result.action, CompactionAction::Nothing);
    assert_eq!(tree.table_count(), 1);
    assert_latest_values(&tree)?;
    assert_eq!(medium.budget.report().waiting_since, None);
    Ok(())
}

/// A merge with credit runs and is charged what it wrote; once it has
/// returned it holds nothing.
#[test]
fn a_merge_with_credit_runs_and_is_charged_its_output() -> lsm_tree::Result<()> {
    let medium = Medium::full(plenty());
    let tree = config("/db", Arc::clone(&medium.fs)).open()?;
    three_tables(&tree, 0)?;
    let before = medium.budget.report();

    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    let after = medium.budget.report();
    let written = medium.written(WriteOrigin::CompactionTable);
    assert!(written > 0);
    assert_eq!(after.discretionary_bytes, written);
    assert_eq!(after.reserved, 0, "the hold went back with the run");
    assert!(after.credit < before.credit);
    assert_eq!(after.refused, 0);
    Ok(())
}

/// A tree at its stop threshold on L0 runs merges past an empty budget: its
/// writers would otherwise stall. The merge is charged and counted as a
/// backpressure override.
#[test]
fn a_tree_at_its_l0_stop_merges_past_the_budget() -> lsm_tree::Result<()> {
    let medium = Medium::empty(plenty());
    let tree = config("/db", Arc::clone(&medium.fs)).open()?;
    three_tables(&tree, 0)?;
    standard(&tree).update_runtime_config(|c| {
        c.backpressure = BackpressureThresholds {
            l0_stop: Some(2),
            ..BackpressureThresholds::OFF
        };
    })?;

    let result = tree.major_compact(u64::MAX, SeqNo::MAX)?;

    assert_ne!(result.action, CompactionAction::Nothing);
    assert_latest_values(&tree)?;
    let report = medium.budget.report();
    assert_eq!(report.backpressure_overrides.reservations, 1);
    assert_eq!(report.refused, 0);
    assert!(report.credit < 0, "overrun, and reported so");
    Ok(())
}

/// A memtable past its stop threshold is no reason to merge past the budget:
/// flushes drain it, and no budget holds them back.
#[test]
fn a_full_memtable_does_not_override_the_budget() -> lsm_tree::Result<()> {
    let medium = Medium::empty(plenty());
    let tree = config("/db", Arc::clone(&medium.fs)).open()?;
    three_tables(&tree, 0)?;
    tree.insert("unflushed", "x".repeat(4_096), 10_000);
    standard(&tree).update_runtime_config(|c| {
        c.backpressure = BackpressureThresholds {
            memtable_stop: Some(1),
            ..BackpressureThresholds::OFF
        };
    })?;

    let result = tree.major_compact(u64::MAX, 0)?;

    assert_eq!(result.action, CompactionAction::Nothing);
    assert_eq!(medium.budget.report().refused, 1);
    Ok(())
}

/// The caller forces discretionary work through an empty budget: the merge
/// runs, charged and counted as forced, and work waits again once released.
#[test]
fn a_forced_budget_lets_merges_run_and_counts_them() -> lsm_tree::Result<()> {
    let medium = Medium::empty(plenty());
    let tree = config("/db", Arc::clone(&medium.fs)).open()?;
    three_tables(&tree, 0)?;
    medium.budget.set_forced(true);

    let result = tree.major_compact(u64::MAX, SeqNo::MAX)?;

    assert_ne!(result.action, CompactionAction::Nothing);
    assert_latest_values(&tree)?;
    let report = medium.budget.report();
    assert_eq!(report.forced_overrides.reservations, 1);
    assert!(report.forced_overrides.bytes > 0);

    medium.budget.set_forced(false);
    three_tables(&tree, 10_000)?;
    let result = tree.major_compact(u64::MAX, SeqNo::MAX)?;
    assert_eq!(result.action, CompactionAction::Nothing);
    assert_eq!(medium.budget.report().refused, 1);
    Ok(())
}

/// Two trees on one medium spend one budget: credit that covers one tree's
/// merge is gone once the other tree's flushes spent it.
#[test]
fn trees_on_one_medium_spend_one_budget() -> lsm_tree::Result<()> {
    // A byte a second, so the clock sets the credit to the byte.
    let medium = Medium::empty(Allowance::new(DAY.as_secs(), DAY).with_burst(1 << 30));
    let quiet = config("/quiet", Arc::clone(&medium.fs)).open()?;
    three_tables(&quiet, 0)?;
    // The merge reserves what its inputs hold, which is what their flushes
    // wrote; refill twice that past the overrun.
    let inputs = medium.written(WriteOrigin::FlushTable);
    let overrun = u64::try_from(-medium.budget.report().credit).expect("overrun");
    medium.clock.advance_secs(overrun + 2 * inputs);
    assert!(medium.budget.report().credit >= i64::try_from(inputs).expect("small"));

    let busy = config("/busy", Arc::clone(&medium.fs)).open()?;
    three_tables(&busy, 0)?;
    three_tables(&busy, 10_000)?;

    let result = quiet.major_compact(u64::MAX, SeqNo::MAX)?;
    assert_eq!(result.action, CompactionAction::Nothing);
    assert_eq!(medium.budget.report().refused, 1);
    Ok(())
}

/// A merge into a level routed to another medium reserves from that medium's
/// budget: the primary medium's empty budget does not hold it back, and its
/// output is charged where it lands.
#[test]
fn a_merge_reserves_from_the_medium_it_writes_to() -> lsm_tree::Result<()> {
    let hot = Medium::empty(plenty());
    let cold = Medium::full(plenty());
    let tree = config("/db", Arc::clone(&hot.fs))
        .level_routes(vec![LevelRoute {
            levels: 1..7,
            path: "/cold".into(),
            fs: Arc::clone(&cold.fs),
        }])
        .open()?;
    three_tables(&tree, 0)?;
    assert!(
        hot.budget.report().credit < 0,
        "the flushes overran the hot medium"
    );

    let result = tree.major_compact(u64::MAX, SeqNo::MAX)?;

    assert_ne!(result.action, CompactionAction::Nothing);
    assert_latest_values(&tree)?;
    let (hot, cold) = (hot.budget.report(), cold.budget.report());
    assert_eq!(hot.refused, 0, "nothing asked the hot medium");
    assert_eq!(hot.discretionary_bytes, 0);
    assert!(cold.discretionary_bytes > 0);
    assert_eq!(cold.reserved, 0);
    Ok(())
}

/// A move writes no table, so no budget holds it back.
#[test]
fn a_move_needs_no_credit() -> lsm_tree::Result<()> {
    let medium = Medium::empty(plenty());
    let tree = config("/db", Arc::clone(&medium.fs)).open()?;
    for i in 0..100u64 {
        tree.insert(format!("key{i:05}"), "value", i);
    }
    tree.flush_active_memtable(0)?;

    tree.compact(Arc::new(MoveDown(0, 1)), SeqNo::MAX)?;

    assert_eq!(tree.level_table_count(1), Some(1), "moved");
    assert_eq!(medium.budget.report().refused, 0);
    Ok(())
}

/// A tree on a backend with no budget compacts as it always has.
#[test]
fn a_backend_without_a_budget_holds_nothing_back() -> lsm_tree::Result<()> {
    let fs: Arc<dyn Fs> = Arc::new(CountingFs::new(Arc::new(MemFs::new())));
    assert!(fs.write_budget().is_none());
    let tree = config("/db", fs).open()?;
    three_tables(&tree, 0)?;

    let result = tree.major_compact(u64::MAX, SeqNo::MAX)?;

    assert_ne!(result.action, CompactionAction::Nothing);
    assert_eq!(tree.table_count(), 1);
    assert_latest_values(&tree)?;
    Ok(())
}
