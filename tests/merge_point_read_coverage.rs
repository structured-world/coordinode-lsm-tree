// The merge of a point read (tree.get() when the newest entry is a
// MergeOperand) reads its sources newest first and stops at the first base.
// These tests cover its paths:
//   - Tables the key's filter passes and rules out
//   - Range tombstones held in memtables and tables
//   - A base or a tombstone held deeper than a newer table, or above it
//   - Sealed memtables

use lsm_tree::{AbstractTree, Config, MergeOperator, SequenceNumberCounter, UserValue};
use std::sync::Arc;
use tempfile::tempdir;

struct CounterMerge;

impl MergeOperator for CounterMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut counter: i64 = match base_value {
            Some(bytes) if bytes.len() == 8 => {
                i64::from_le_bytes(bytes.try_into().expect("checked"))
            }
            _ => 0,
        };
        for op in operands {
            if op.len() == 8 {
                counter += i64::from_le_bytes((*op).try_into().expect("checked"));
            }
        }
        Ok(counter.to_le_bytes().to_vec().into())
    }
}

fn get_counter(tree: &lsm_tree::AnyTree, key: &str, seqno: u64) -> Option<i64> {
    tree.get(key, seqno)
        .unwrap()
        .map(|v| i64::from_le_bytes((*v).try_into().unwrap()))
}

fn tree_with_merge(folder: &tempfile::TempDir) -> lsm_tree::AnyTree {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open()
    .unwrap()
}

/// Single-table run path: base on disk, merge operand in memtable.
/// Exercises the len==1 arm with bloom-passing table.
#[test]
fn point_read_merge_single_table() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value on disk
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // Merge operand in memtable
    tree.merge("counter", 5_i64.to_le_bytes(), 1);

    assert_eq!(get_counter(&tree, "counter", 2), Some(105));
}

/// Multiple flushed tables with unrelated keys (bloom rejects them).
/// The target key's base + operand should merge correctly despite many tables.
#[test]
fn point_read_merge_bloom_filters_unrelated_tables() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value on disk
    tree.insert("counter", 50_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // Unrelated tables (bloom should reject for "counter")
    for i in 0..5 {
        let key = format!("other_{i}");
        tree.insert(key, vec![0u8; 8], i as u64 + 1);
        tree.flush_active_memtable(0).unwrap();
    }

    // Merge operand in memtable
    tree.merge("counter", 7_i64.to_le_bytes(), 10);

    assert_eq!(get_counter(&tree, "counter", 11), Some(57));
}

/// Sealed memtable path: merge operand in sealed (not yet flushed) memtable.
#[test]
fn point_read_merge_sealed_memtable() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value on disk
    tree.insert("counter", 10_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // First merge operand — will be sealed via rotate
    tree.merge("counter", 3_i64.to_le_bytes(), 1);
    tree.rotate_memtable();

    // Second merge operand — in active memtable
    tree.merge("counter", 2_i64.to_le_bytes(), 2);

    assert_eq!(get_counter(&tree, "counter", 3), Some(15));
}

/// Range tombstone suppression: RT kills the base value, merge
/// operand should produce result with no base (pure merge).
#[test]
fn point_read_merge_with_range_tombstone_suppression() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value on disk
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // Range tombstone [c, d) at seqno 2 — covers "counter"
    tree.remove_range("c", "d", 2);
    tree.flush_active_memtable(0).unwrap();

    // Merge operand in memtable — base is suppressed by RT,
    // so merge runs with base=None
    tree.merge("counter", 42_i64.to_le_bytes(), 3);

    assert_eq!(get_counter(&tree, "counter", 4), Some(42));
}

/// Multiple merge operands across disk and memtable.
#[test]
fn point_read_merge_multiple_operands_on_disk() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value
    tree.insert("counter", 10_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // Merge operands on disk
    tree.merge("counter", 1_i64.to_le_bytes(), 1);
    tree.flush_active_memtable(0).unwrap();
    tree.merge("counter", 2_i64.to_le_bytes(), 2);
    tree.flush_active_memtable(0).unwrap();

    // Merge operand in memtable
    tree.merge("counter", 3_i64.to_le_bytes(), 3);

    // 10 + 1 + 2 + 3 = 16
    assert_eq!(get_counter(&tree, "counter", 4), Some(16));
}

/// No base value — pure merge operands only.
#[test]
fn point_read_merge_no_base_value() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    tree.merge("counter", 5_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();
    tree.merge("counter", 3_i64.to_le_bytes(), 1);

    assert_eq!(get_counter(&tree, "counter", 2), Some(8));
}

/// Key not present — should return None, not panic.
#[test]
fn point_read_merge_nonexistent_key() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    tree.merge("counter", 5_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    assert_eq!(get_counter(&tree, "missing", 1), None);
}

/// Range tombstone in active memtable (not flushed) suppresses the
/// base value during merge resolution.
#[test]
fn point_read_merge_rt_in_active_memtable() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value on disk
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // RT in active memtable
    tree.remove_range("c", "d", 2);

    // Merge operand in active memtable
    tree.merge("counter", 42_i64.to_le_bytes(), 3);

    // RT suppresses base — result is pure merge from None
    assert_eq!(get_counter(&tree, "counter", 4), Some(42));
}

/// An RT held in a sealed memtable hides the base on disk.
#[test]
fn point_read_merge_rt_in_sealed_memtable() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value on disk
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // RT in sealed memtable
    tree.remove_range("c", "d", 2);
    tree.rotate_memtable();

    // Merge operand in active memtable
    tree.merge("counter", 42_i64.to_le_bytes(), 3);

    assert_eq!(get_counter(&tree, "counter", 4), Some(42));
}

/// A tree whose range tombstone [c, d), with neighbours "a" and "e" keeping
/// the table's key range around it, sits in the last level at `rt_seqno`,
/// and whose base for "counter" sits in a newer table above it at seqno 5.
fn tree_with_a_deep_tombstone(folder: &tempfile::TempDir, rt_seqno: u64) -> lsm_tree::AnyTree {
    let tree = tree_with_merge(folder);
    // Below the base: only the tombstone's seqno can order this table above it.
    tree.insert("a", vec![0u8; 8], 0);
    tree.insert("e", vec![0u8; 8], 0);
    tree.remove_range("c", "d", rt_seqno);
    tree.flush_active_memtable(0).unwrap();
    tree.major_compact(u64::MAX, 0).unwrap();

    tree.insert("counter", 100_i64.to_le_bytes(), 5);
    tree.flush_active_memtable(0).unwrap();
    tree
}

/// A range tombstone newer than the base, held in a table below the base's,
/// still hides the base: the read goes past the base's table because the
/// deeper table holds a higher seqno.
#[test]
fn point_read_merge_deeper_newer_tombstone_hides_the_base() {
    let folder = tempdir().unwrap();
    let tree = tree_with_a_deep_tombstone(&folder, 10);
    tree.merge("counter", 42_i64.to_le_bytes(), 11);

    assert_eq!(get_counter(&tree, "counter", 12), Some(42));
}

/// A range tombstone older than the base, held in a table below it, hides
/// nothing the merge reads.
#[test]
fn point_read_merge_deeper_older_tombstone_leaves_the_base() {
    let folder = tempdir().unwrap();
    let tree = tree_with_a_deep_tombstone(&folder, 3);
    tree.merge("counter", 42_i64.to_le_bytes(), 11);

    assert_eq!(get_counter(&tree, "counter", 12), Some(142));
}

/// An operand in the memtable and a value in a table share one seqno for the
/// key, the table also holding a newer key: the memtable's version is the
/// newer of the two, as the point read takes it, so it is merged onto the
/// table's value rather than hidden by it.
#[test]
fn point_read_merge_same_seqno_takes_the_newer_source_first() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);
    tree.insert("counter", 100_i64.to_le_bytes(), 7);
    tree.insert("zzz", vec![0u8; 8], 100);
    tree.flush_active_memtable(0).unwrap();
    tree.merge("counter", 5_i64.to_le_bytes(), 7);

    assert_eq!(get_counter(&tree, "counter", 101), Some(105));
}

/// A base below a newer table holding operands: the read takes the operands
/// first, then goes down to the base.
#[test]
fn point_read_merge_operands_above_a_deep_base() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();
    tree.major_compact(u64::MAX, 0).unwrap();
    tree.merge("counter", 1_i64.to_le_bytes(), 1);
    tree.merge("counter", 2_i64.to_le_bytes(), 2);
    tree.flush_active_memtable(0).unwrap();
    tree.merge("counter", 3_i64.to_le_bytes(), 3);

    assert_eq!(get_counter(&tree, "counter", 4), Some(106));
    // An older snapshot sees only the versions below it.
    assert_eq!(get_counter(&tree, "counter", 2), Some(101));
}

/// With the block cache off, a merge over a base in the last level and
/// newer whole versions in tables above it loads data blocks of the newest
/// table holding a base only: the older tables hold nothing above that base.
#[cfg(feature = "metrics")]
#[test]
fn point_read_merge_loads_only_the_newest_base_table() {
    const NEWER: u64 = 4;

    let folder = tempdir().unwrap();
    let lsm_tree::AnyTree::Standard(tree) = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(Arc::new(lsm_tree::Cache::with_capacity_bytes(0)))
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open()
    .unwrap() else {
        panic!("expected a standard tree");
    };

    tree.insert("counter", 0_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();
    tree.major_compact(u64::MAX, 0).unwrap();
    for seqno in 1..=NEWER {
        tree.insert("counter", (seqno as i64 * 100).to_le_bytes(), seqno);
        tree.flush_active_memtable(0).unwrap();
    }
    tree.merge("counter", 7_i64.to_le_bytes(), NEWER + 1);

    let before = tree.metrics().data_block_load_count();
    let value = tree.get("counter", NEWER + 2).unwrap().unwrap();
    let loaded = tree.metrics().data_block_load_count() - before;

    assert_eq!(i64::from_le_bytes((*value).try_into().unwrap()), 407);
    assert_eq!(
        loaded,
        1,
        "only the newest of the {} tables holding a base is read",
        NEWER + 1
    );
}

/// Tables whose key range does not overlap the target key are skipped
/// during RT collection (exercises the key-range continue path).
#[test]
fn point_read_merge_non_overlapping_tables_skipped() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Tables with keys far from "counter" — key range won't overlap
    tree.insert("zzz_far_away", vec![0u8; 8], 0);
    tree.flush_active_memtable(0).unwrap();
    tree.insert("yyy_also_far", vec![0u8; 8], 1);
    tree.flush_active_memtable(0).unwrap();

    // Base value
    tree.insert("counter", 50_i64.to_le_bytes(), 2);
    tree.flush_active_memtable(0).unwrap();

    // Merge operand
    tree.merge("counter", 10_i64.to_le_bytes(), 3);

    assert_eq!(get_counter(&tree, "counter", 4), Some(60));
}
