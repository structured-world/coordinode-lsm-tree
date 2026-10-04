use super::*;
use crate::{
    AbstractTree, Config, MAX_SEQNO, SequenceNumberCounter, compaction::CompactionStrategy,
};
use std::sync::Arc;
use test_log::test;

/// Helper: flush N overlapping memtables so each becomes a separate run in L0.
/// Each flush inserts shared boundary keys "a" and "z" plus a unique key to
/// ensure overlapping key ranges (preventing `optimize_runs` from merging
/// disjoint runs into one).
fn flush_overlapping(
    tree: &impl crate::AbstractTree,
    count: u8,
    seqno_base: u64,
) -> crate::Result<()> {
    for i in 0..count {
        let seqno = seqno_base + u64::from(i);
        tree.insert("a", "v", seqno);
        tree.insert([b'k', i].as_slice(), "v", seqno);
        tree.insert("z", "v", seqno);
        tree.flush_active_memtable(seqno)?;
    }
    Ok(())
}

/// Flushes one run spanning "a".."z" with `keys` incompressible values, so its
/// size grows with `keys` and it overlaps every other run.
fn flush_sized_run(tree: &impl AbstractTree, keys: u16, seqno: u64) -> crate::Result<()> {
    tree.insert("a", "v", seqno);
    for k in 0..keys {
        let key = [b"m".as_slice(), &k.to_be_bytes()].concat();
        let noise = u64::from(k)
            .wrapping_add(seqno << 16)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15);
        tree.insert(
            key,
            format!("{noise:016x}{:016x}", noise.rotate_left(17)),
            seqno,
        );
    }
    tree.insert("z", "v", seqno);
    tree.flush_active_memtable(seqno)
}

/// On-disk size of each L0 run, newest first.
fn l0_run_sizes(tree: &crate::AnyTree) -> Vec<u64> {
    tree.current_version()
        .l0()
        .iter()
        .map(|run| run.iter().map(Table::file_size).sum())
        .collect()
}

/// L0 run order is recency order: every run holds only data older than the
/// runs in front of it (all runs here overlap).
fn assert_l0_in_recency_order(tree: &crate::AnyTree) {
    let highest: Vec<u64> = tree
        .current_version()
        .l0()
        .iter()
        .map(|run| {
            run.iter()
                .map(Table::get_highest_seqno)
                .max()
                .unwrap_or_default()
        })
        .collect();
    assert!(
        highest.windows(2).all(|w| w.first() > w.get(1)),
        "L0 runs out of recency order (highest seqno per run, front to back): {highest:?}"
    );
}

/// Two small runs separated in age by a large one are not merged around it:
/// the merged run would hold data both newer and older than the large run.
#[test]
fn stcs_does_not_merge_runs_around_a_differently_sized_one() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    flush_sized_run(&tree, 1, 0)?;
    flush_sized_run(&tree, 2_000, 1)?;
    flush_sized_run(&tree, 1, 2)?;

    let sizes = l0_run_sizes(&tree);
    let (Some(&newest), Some(&middle), Some(&oldest)) = (sizes.first(), sizes.get(1), sizes.get(2))
    else {
        panic!("expected three L0 runs, got {sizes:?}");
    };
    assert!(
        middle > 4 * newest.max(oldest),
        "the middle run must be far larger than its neighbours: {sizes:?}"
    );

    let strategy = Arc::new(
        Strategy::default()
            .with_size_ratio(0.5)
            .with_min_merge_width(2)
            .with_max_space_amplification_percent(u64::MAX),
    );
    tree.compact(strategy, 3)?;

    assert_eq!(3, tree.table_count(), "no run pair is adjacent and similar");
    assert_l0_in_recency_order(&tree);
    Ok(())
}

/// A run held by a running compaction between two available runs splits
/// them: neither rule merges the two around it.
#[test]
fn stcs_does_not_merge_around_a_busy_run() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    flush_overlapping(&tree, 3, 0)?;

    let version = tree.current_version();
    let middle: Vec<TableId> = version
        .l0()
        .iter()
        .nth(1)
        .map(|run| run.iter().map(Table::id).collect())
        .unwrap_or_default();
    assert!(!middle.is_empty(), "expected three L0 runs");
    let mut state = CompactionState::default();
    state.hidden_set_mut().hide(middle);

    let size_ratio = Strategy::default()
        .with_min_merge_width(2)
        .with_max_space_amplification_percent(u64::MAX);
    assert!(
        matches!(
            size_ratio.choose(&version, &Config::default(), &state),
            Choice::DoNothing
        ),
        "the size-ratio rule must not merge across the busy run"
    );

    let space_amp = Strategy::default()
        .with_min_merge_width(100)
        .with_max_space_amplification_percent(0);
    assert!(
        matches!(
            space_amp.choose(&version, &Config::default(), &state),
            Choice::DoNothing
        ),
        "the space-amplification rule must wait for the busy run"
    );
    Ok(())
}

/// A merge of the newest runs lands in front of the older runs it did not
/// take, not behind them.
#[test]
fn stcs_merged_run_takes_the_slot_of_its_inputs() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    flush_overlapping(&tree, 4, 0)?;

    let strategy = Arc::new(
        Strategy::default()
            .with_min_merge_width(2)
            .with_max_merge_width(2)
            .with_max_space_amplification_percent(u64::MAX),
    );
    tree.compact(strategy, 4)?;

    assert_eq!(3, tree.l0_run_count());
    assert_l0_in_recency_order(&tree);
    Ok(())
}

/// Flushes of mixed sizes interleaved with merges keep L0 in recency order
/// after every merge.
#[test]
fn stcs_l0_stays_in_recency_order_across_merges() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let strategy = Arc::new(
        Strategy::default()
            .with_min_merge_width(2)
            .with_max_space_amplification_percent(u64::MAX),
    );

    let sizes = [1u16, 300, 2, 2, 600, 1, 40, 40, 1, 900, 3, 3, 3, 150, 1];
    let mut seqno = 0u64;
    for keys in sizes {
        flush_sized_run(&tree, keys, seqno)?;
        seqno += 1;
        tree.compact(strategy.clone(), seqno)?;
        assert_l0_in_recency_order(&tree);
    }
    Ok(())
}

#[test]
fn stcs_empty_levels() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let strategy = Arc::new(Strategy::default());
    tree.compact(strategy, 0)?;

    assert_eq!(0, tree.table_count());
    Ok(())
}

#[test]
fn stcs_below_min_merge_width() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 2 overlapping tables — below default min_merge_width=4
    flush_overlapping(&tree, 2, 0)?;
    assert_eq!(2, tree.table_count());
    assert!(tree.l0_run_count() > 1, "runs should be separate");

    let strategy = Arc::new(Strategy::default());
    tree.compact(strategy, 2)?;

    // No merge should occur — still 2 tables
    assert_eq!(2, tree.table_count());
    Ok(())
}

#[test]
fn stcs_triggers_merge() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 4 similarly-sized overlapping tables
    flush_overlapping(&tree, 4, 0)?;
    assert_eq!(4, tree.table_count());

    let strategy = Arc::new(Strategy::default().with_min_merge_width(4));
    tree.compact(strategy, 4)?;

    // All 4 should merge into 1
    assert_eq!(1, tree.table_count());

    // All data should be readable
    for i in 0..4u8 {
        assert!(
            tree.get([b'k', i].as_slice(), MAX_SEQNO)?.is_some(),
            "key k{i} should exist after compaction",
        );
    }

    Ok(())
}

#[test]
fn stcs_min_merge_width_2() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 2 overlapping tables
    flush_overlapping(&tree, 2, 0)?;
    assert_eq!(2, tree.table_count());

    let strategy = Arc::new(Strategy::default().with_min_merge_width(2));
    tree.compact(strategy, 2)?;

    // With min_merge_width=2, 2 similar runs should merge
    assert_eq!(1, tree.table_count());

    Ok(())
}

#[test]
fn stcs_space_amp_full_compaction() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create 5 overlapping flushes.
    // With 5 similarly-sized runs: space_amp ~ (5S/S - 1)*100 = 400% > 200%
    flush_overlapping(&tree, 5, 0)?;
    assert_eq!(5, tree.table_count());

    // Use high min_merge_width so the size-ratio path wouldn't trigger,
    // but the space amp check still fires.
    let strategy = Arc::new(
        Strategy::default()
            .with_min_merge_width(100)
            .with_max_space_amplification_percent(200),
    );
    tree.compact(strategy, 5)?;

    // Space amp triggered full compaction
    assert_eq!(1, tree.table_count());

    Ok(())
}

#[test]
fn stcs_max_merge_width_cap() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 8 overlapping tables
    flush_overlapping(&tree, 8, 0)?;
    assert_eq!(8, tree.table_count());

    // max_merge_width=3 should only merge 3 out of 8.
    // Disable space amp check with very high threshold.
    let strategy = Arc::new(
        Strategy::default()
            .with_min_merge_width(2)
            .with_max_merge_width(3)
            .with_max_space_amplification_percent(u64::MAX),
    );
    tree.compact(strategy, 8)?;

    // 8 runs -> merge the 3 newest into 1 -> 6 runs total
    // (8 - 3 + 1 = 6)
    assert_eq!(6, tree.table_count());

    Ok(())
}

#[test]
fn stcs_data_integrity_multi_compact() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Insert keys across 5 overlapping flushes with updates to test MVCC
    for batch in 0..5u64 {
        tree.insert("a", format!("v{batch}").as_bytes(), batch);
        for k in 0..4u8 {
            let key = [b'k', k];
            let val = format!("v{batch}");
            tree.insert(key.as_slice(), val.as_bytes(), batch);
        }
        tree.insert("z", format!("v{batch}").as_bytes(), batch);
        tree.flush_active_memtable(batch)?;
    }

    assert_eq!(5, tree.table_count());

    let strategy = Arc::new(Strategy::default().with_min_merge_width(2));

    // Run compaction multiple times to progressively merge
    for seqno in 5..8u64 {
        tree.compact(strategy.clone(), seqno)?;
    }

    // All keys should be readable with latest values
    for k in 0..4u8 {
        let val = tree.get([b'k', k].as_slice(), MAX_SEQNO)?;
        assert!(val.is_some(), "key k{k} should exist");
        assert_eq!(val.as_deref(), Some(b"v4".as_slice()));
    }

    Ok(())
}

#[test]
fn stcs_no_space_amp_trigger_below_threshold() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // 2 overlapping runs: space_amp = (2S/S - 1) * 100 = 100%. Below 200% threshold.
    flush_overlapping(&tree, 2, 0)?;

    // min_merge_width=100 so size-ratio path won't fire.
    // space_amp (100%) < threshold (200%) so nothing happens.
    let strategy = Arc::new(
        Strategy::default()
            .with_min_merge_width(100)
            .with_max_space_amplification_percent(200),
    );
    tree.compact(strategy, 2)?;

    assert_eq!(2, tree.table_count());
    Ok(())
}

#[test]
fn stcs_multiple_compaction_cycles() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let strategy = Arc::new(Strategy::default().with_min_merge_width(2));
    let mut seqno = 0u64;

    // Flush and compact in cycles
    for _cycle in 0..3 {
        for _k in 0..3 {
            // Overlapping keys to keep separate runs
            tree.insert("a", "val", seqno);
            tree.insert(format!("key_{seqno}").as_bytes(), "val", seqno);
            tree.insert("z", "val", seqno);
            tree.flush_active_memtable(seqno)?;
            seqno += 1;
        }
        tree.compact(strategy.clone(), seqno)?;
    }

    // All keys should be readable
    for s in 0..seqno {
        assert!(
            tree.get(format!("key_{s}").as_bytes(), MAX_SEQNO)?
                .is_some(),
            "key_{s} should exist after multiple compaction cycles",
        );
    }

    Ok(())
}

#[test]
fn stcs_get_name() {
    let strategy = Strategy::default();
    assert_eq!(strategy.get_name(), "SizeTieredCompaction");
}

#[test]
fn stcs_get_config_serialization() {
    let strategy = Strategy::default()
        .with_size_ratio(0.5)
        .with_min_merge_width(8)
        .with_max_merge_width(16)
        .with_max_space_amplification_percent(300)
        .with_table_target_size(128 * 1024 * 1024);

    let config = strategy.get_config();
    assert_eq!(config.len(), 5, "should serialize all 5 parameters");

    // Verify keys exist
    let keys: Vec<_> = config.iter().map(|(k, _)| k.as_ref()).collect();
    assert!(keys.iter().any(|k| k == b"tiered_size_ratio"));
    assert!(keys.iter().any(|k| k == b"tiered_min_merge_width"));
    assert!(keys.iter().any(|k| k == b"tiered_max_merge_width"));
    assert!(keys.iter().any(|k| k == b"tiered_max_space_amp_pct"));
    assert!(keys.iter().any(|k| k == b"tiered_target_size"));
}

#[test]
fn stcs_builder_with_size_ratio() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 4 runs with overlapping keys
    flush_overlapping(&tree, 4, 0)?;

    // Very tight size_ratio=0.01 means only nearly-identical-sized runs merge
    // All our flushes are similar, so this should still trigger
    let strategy = Arc::new(
        Strategy::default()
            .with_size_ratio(0.01)
            .with_min_merge_width(2)
            .with_max_space_amplification_percent(u64::MAX),
    );
    tree.compact(strategy, 4)?;

    // Some merging should occur (runs are similarly sized)
    assert!(tree.table_count() < 4);

    Ok(())
}

#[test]
fn stcs_max_merge_width_less_than_min_no_merge() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 4 overlapping runs
    flush_overlapping(&tree, 4, 0)?;
    assert_eq!(4, tree.table_count());

    // Configure max_merge_width=2 but min_merge_width=4.
    // The similar stretch is 4 long (min), but merge_count = min(4, 2) = 2 < 4 (min).
    // Guard should prevent merge.
    let strategy = Arc::new(
        Strategy::default()
            .with_min_merge_width(4)
            .with_max_merge_width(2)
            .with_max_space_amplification_percent(u64::MAX),
    );
    tree.compact(strategy, 4)?;

    // No merge should occur — misconfigured max < min is guarded
    assert_eq!(4, tree.table_count());
    Ok(())
}

#[test]
fn stcs_single_run_no_compaction() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Single flush = single run in L0
    tree.insert("a", "v", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(1, tree.table_count());

    let strategy = Arc::new(Strategy::default().with_min_merge_width(2));
    tree.compact(strategy, 1)?;

    // Single run → DoNothing (runs.len() < 2)
    assert_eq!(1, tree.table_count());
    Ok(())
}

#[test]
fn stcs_dissimilar_sizes_break() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create runs of very different sizes so the size-ratio break fires.
    // First run: tiny (1 key)
    tree.insert("a", "v", 0);
    tree.insert("z", "v", 0);
    tree.flush_active_memtable(0)?;

    // Second run: much larger (many keys → bigger table)
    for k in 0..200u16 {
        tree.insert("a", "v", 1);
        tree.insert(k.to_be_bytes().as_slice(), "large_value_padding_xxxxx", 1);
        tree.insert("z", "v", 1);
    }
    tree.flush_active_memtable(1)?;

    // Very tight size_ratio=0.01, min_merge_width=2.
    // The two runs differ hugely in size, so ratio > 1.01 → break fires.
    let strategy = Arc::new(
        Strategy::default()
            .with_size_ratio(0.01)
            .with_min_merge_width(2)
            .with_max_space_amplification_percent(u64::MAX),
    );
    tree.compact(strategy, 2)?;

    // No merge — sizes too different
    assert_eq!(2, tree.table_count());
    Ok(())
}

#[test]
fn stcs_pending_compaction_bytes_reflects_space_amplification() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // 5 similarly-sized overlapping runs: space_amp ~ (5S/S - 1) * 100 = 400%.
    flush_overlapping(&tree, 5, 0)?;
    let version = tree.current_version();

    // Past the 200% budget, the size-tiered strategy owes a full compaction, so
    // it must report non-zero pending bytes for the bytes-axis backpressure to
    // engage (it returned a flat 0 before, leaving bytes_slowdown/bytes_stop
    // inert for tiered).
    let over_budget = Strategy::default().with_max_space_amplification_percent(200);
    assert!(
        over_budget.pending_compaction_bytes(&version) > 0,
        "tiered must report pending bytes when over the space-amplification budget"
    );

    // A tolerant (effectively unbounded) budget owes nothing.
    let tolerant = Strategy::default().with_max_space_amplification_percent(u64::MAX);
    assert_eq!(
        tolerant.pending_compaction_bytes(&version),
        0,
        "no debt when amplification is fully tolerated"
    );

    Ok(())
}

#[test]
fn stcs_pending_compaction_bytes_is_zero_below_two_runs() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let strategy = Strategy::default().with_max_space_amplification_percent(0);

    // Empty tree: no runs, nothing to reclaim.
    assert_eq!(
        strategy.pending_compaction_bytes(&tree.current_version()),
        0
    );

    // A single run cannot be space-amplified against itself, so even a 0% budget
    // owes nothing.
    flush_overlapping(&tree, 1, 0)?;
    assert_eq!(
        strategy.pending_compaction_bytes(&tree.current_version()),
        0
    );

    Ok(())
}
