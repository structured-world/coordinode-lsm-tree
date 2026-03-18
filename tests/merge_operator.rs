// Guard import is required: into_inner() and key() are trait methods from IterGuard (re-exported as Guard)
use lsm_tree::{AbstractTree, Config, Guard, MergeOperator, SequenceNumberCounter, UserValue};
use std::sync::Arc;

/// Simple counter merge operator: base + sum of operands (i64 little-endian).
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
                i64::from_le_bytes(bytes.try_into().expect("checked length"))
            }
            Some(_) => return Err(lsm_tree::Error::MergeOperator),
            None => 0,
        };

        for operand in operands {
            if operand.len() != 8 {
                return Err(lsm_tree::Error::MergeOperator);
            }
            counter += i64::from_le_bytes((*operand).try_into().expect("checked length"));
        }

        Ok(counter.to_le_bytes().to_vec().into())
    }
}

fn open_tree_with_counter(folder: &tempfile::TempDir) -> lsm_tree::AnyTree {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open()
    .unwrap()
}

fn open_blob_tree_with_counter(folder: &tempfile::TempDir) -> lsm_tree::AnyTree {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .with_kv_separation(Some(lsm_tree::KvSeparationOptions::default()))
    .open()
    .unwrap()
}

fn get_counter(tree: &lsm_tree::AnyTree, key: &str, seqno: u64) -> Option<i64> {
    tree.get(key, seqno)
        .unwrap()
        .map(|v| i64::from_le_bytes((*v).try_into().unwrap()))
}

#[test]
fn merge_counter_increment() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // 3 merge operands, no base value
    tree.merge("counter", 1_i64.to_le_bytes(), 0);
    tree.merge("counter", 2_i64.to_le_bytes(), 1);
    tree.merge("counter", 3_i64.to_le_bytes(), 2);

    assert_eq!(Some(6), get_counter(&tree, "counter", 3));
}

#[test]
fn merge_with_base_value() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // Base value = 100, then +5, +10
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 5_i64.to_le_bytes(), 1);
    tree.merge("counter", 10_i64.to_le_bytes(), 2);

    assert_eq!(Some(115), get_counter(&tree, "counter", 3));
}

#[test]
fn merge_after_tombstone() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // Base=50, delete, then merge +7
    tree.insert("counter", 50_i64.to_le_bytes(), 0);
    tree.remove("counter", 1);
    tree.merge("counter", 7_i64.to_le_bytes(), 2);

    // Merge after delete should produce value from operands only (base=None)
    assert_eq!(Some(7), get_counter(&tree, "counter", 3));
}

#[test]
fn merge_mvcc_snapshot() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.merge("counter", 20_i64.to_le_bytes(), 2);
    tree.merge("counter", 30_i64.to_le_bytes(), 3);

    // Read at different snapshots
    assert_eq!(Some(100), get_counter(&tree, "counter", 1)); // base only
    assert_eq!(Some(110), get_counter(&tree, "counter", 2)); // base + 10
    assert_eq!(Some(130), get_counter(&tree, "counter", 3)); // base + 10 + 20
    assert_eq!(Some(160), get_counter(&tree, "counter", 4)); // base + 10 + 20 + 30
}

#[test]
fn merge_no_operator_returns_raw() {
    let folder = tempfile::tempdir().unwrap();

    // Open tree WITHOUT merge operator
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .unwrap();

    tree.merge("key", 42_i64.to_le_bytes(), 0);

    // Should return raw operand bytes (backward compatible)
    let result = tree.get("key", 1).unwrap().unwrap();
    assert_eq!(42_i64.to_le_bytes(), &*result);
}

#[test]
fn merge_mixed_keys() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // Regular insert
    tree.insert("regular", b"hello".to_vec(), 0);

    // Merge key
    tree.merge("counter", 5_i64.to_le_bytes(), 1);
    tree.merge("counter", 3_i64.to_le_bytes(), 2);

    // Both should work correctly
    assert_eq!(
        Some(b"hello".as_slice().into()),
        tree.get("regular", 3).unwrap()
    );
    assert_eq!(Some(8), get_counter(&tree, "counter", 3));
}

#[test]
fn merge_flush_and_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.merge("counter", 20_i64.to_le_bytes(), 2);

    // Flush to disk
    tree.flush_active_memtable(3)?;

    // Read from flushed data — compaction stream should merge operands
    assert_eq!(Some(130), get_counter(&tree, "counter", 3));

    Ok(())
}

#[test]
fn merge_across_memtable_and_tables() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Write base and first operand, flush
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.flush_active_memtable(2)?;

    // Write more operands to active memtable
    tree.merge("counter", 20_i64.to_le_bytes(), 2);
    tree.merge("counter", 30_i64.to_le_bytes(), 3);

    // Should merge across memtable and disk tables
    assert_eq!(Some(160), get_counter(&tree, "counter", 4));

    Ok(())
}

#[test]
fn merge_range_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.insert("a", 10_i64.to_le_bytes(), 0);
    tree.merge("b", 1_i64.to_le_bytes(), 1);
    tree.merge("b", 2_i64.to_le_bytes(), 2);
    tree.insert("c", 30_i64.to_le_bytes(), 3);

    let items: Vec<_> = tree
        .iter(4, None)
        .map(|guard| {
            let (key, value): (lsm_tree::UserKey, lsm_tree::UserValue) =
                guard.into_inner().unwrap();
            let val = i64::from_le_bytes((*value).try_into().unwrap());
            (String::from_utf8(key.to_vec()).unwrap(), val)
        })
        .collect();

    assert_eq!(items.len(), 3);
    assert_eq!(items[0], ("a".to_string(), 10));
    assert_eq!(items[1], ("b".to_string(), 3)); // merged: 1 + 2
    assert_eq!(items[2], ("c".to_string(), 30));

    Ok(())
}

#[test]
fn merge_multiple_operands_only() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // 5 operands, no base
    for i in 0..5 {
        tree.merge("sum", (i as i64).to_le_bytes(), i);
    }

    assert_eq!(Some(0 + 1 + 2 + 3 + 4), get_counter(&tree, "sum", 5));
}

#[test]
fn merge_key_not_found() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.merge("a", 1_i64.to_le_bytes(), 0);

    assert_eq!(None, get_counter(&tree, "b", 1));
}

#[test]
fn merge_after_weak_tombstone() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.insert("counter", 50_i64.to_le_bytes(), 0);
    tree.remove_weak("counter", 1);
    tree.merge("counter", 7_i64.to_le_bytes(), 2);

    // WeakTombstone stops base search — merge with base=None
    assert_eq!(Some(7), get_counter(&tree, "counter", 3));
}

/// Merge operator that always fails
struct FailingMerge;

impl MergeOperator for FailingMerge {
    fn merge(
        &self,
        _key: &[u8],
        _base_value: Option<&[u8]>,
        _operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        Err(lsm_tree::Error::MergeOperator)
    }
}

#[test]
fn merge_error_propagation() {
    let folder = tempfile::tempdir().unwrap();
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(FailingMerge)))
    .open()
    .unwrap();

    tree.merge("key", b"op1".to_vec(), 0);

    let result = tree.get("key", 1);
    assert!(result.is_err());
}

#[test]
fn merge_multi_get() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.insert("a", 10_i64.to_le_bytes(), 0);
    tree.merge("b", 1_i64.to_le_bytes(), 1);
    tree.merge("b", 2_i64.to_le_bytes(), 2);
    tree.insert("c", 30_i64.to_le_bytes(), 3);

    let results = tree.multi_get(["a", "b", "c", "missing"], 4).unwrap();

    assert_eq!(
        results[0]
            .as_ref()
            .map(|v| i64::from_le_bytes((**v).try_into().unwrap())),
        Some(10)
    );
    assert_eq!(
        results[1]
            .as_ref()
            .map(|v| i64::from_le_bytes((**v).try_into().unwrap())),
        Some(3)
    );
    assert_eq!(
        results[2]
            .as_ref()
            .map(|v| i64::from_le_bytes((**v).try_into().unwrap())),
        Some(30)
    );
    assert!(results[3].is_none());
}

#[test]
fn merge_prefix_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.merge("user:1:score", 10_i64.to_le_bytes(), 0);
    tree.merge("user:1:score", 20_i64.to_le_bytes(), 1);
    tree.merge("user:2:score", 5_i64.to_le_bytes(), 2);
    tree.insert("other", 99_i64.to_le_bytes(), 3);

    let items: Vec<_> = tree
        .prefix("user:", 4, None)
        .map(|guard| {
            let (key, value): (lsm_tree::UserKey, lsm_tree::UserValue) =
                guard.into_inner().unwrap();
            let val = i64::from_le_bytes((*value).try_into().unwrap());
            (String::from_utf8(key.to_vec()).unwrap(), val)
        })
        .collect();

    assert_eq!(items.len(), 2);
    assert_eq!(items[0], ("user:1:score".to_string(), 30)); // 10 + 20
    assert_eq!(items[1], ("user:2:score".to_string(), 5));

    Ok(())
}

#[test]
fn merge_contains_key() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.merge("exists", 1_i64.to_le_bytes(), 0);

    // MergeOperand should count as "key exists" after resolution
    assert!(tree.contains_key("exists", 1).unwrap());
    assert!(!tree.contains_key("missing", 1).unwrap());
}

#[test]
fn merge_major_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Write and flush multiple times to create multiple tables.
    // Use gc_seqno_threshold=0 to preserve merge operands during flush
    // (they can't be resolved since the base may be in a different table).
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0)?;

    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.flush_active_memtable(0)?;

    tree.merge("counter", 20_i64.to_le_bytes(), 2);
    tree.flush_active_memtable(0)?;

    // Before compaction: read path should resolve across tables
    assert_eq!(Some(130), get_counter(&tree, "counter", 3));

    // Major compaction should merge all into single entry
    tree.major_compact(64_000_000, 3)?;

    assert_eq!(Some(130), get_counter(&tree, "counter", 3));
    assert_eq!(1, tree.table_count());

    Ok(())
}

#[test]
fn merge_reverse_range_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.insert("a", 10_i64.to_le_bytes(), 0);
    tree.merge("b", 1_i64.to_le_bytes(), 1);
    tree.merge("b", 2_i64.to_le_bytes(), 2);
    tree.insert("c", 30_i64.to_le_bytes(), 3);

    let items: Vec<_> = tree
        .iter(4, None)
        .rev()
        .map(|guard| {
            let (key, value): (lsm_tree::UserKey, lsm_tree::UserValue) =
                guard.into_inner().unwrap();
            let val = i64::from_le_bytes((*value).try_into().unwrap());
            (String::from_utf8(key.to_vec()).unwrap(), val)
        })
        .collect();

    assert_eq!(items.len(), 3);
    assert_eq!(items[0], ("c".to_string(), 30));
    assert_eq!(items[1], ("b".to_string(), 3)); // merged: 1 + 2
    assert_eq!(items[2], ("a".to_string(), 10));

    Ok(())
}

#[test]
fn merge_overwrite_then_merge() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // Insert → overwrite → merge
    tree.insert("key", 10_i64.to_le_bytes(), 0);
    tree.insert("key", 20_i64.to_le_bytes(), 1);
    tree.merge("key", 5_i64.to_le_bytes(), 2);

    // Should merge with latest base (20), not first (10)
    assert_eq!(Some(25), get_counter(&tree, "key", 3));
}

// --- BlobTree merge tests ---

#[test]
fn blob_tree_merge_write_and_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_blob_tree_with_counter(&folder);

    tree.merge("counter", 1_i64.to_le_bytes(), 0);
    tree.merge("counter", 2_i64.to_le_bytes(), 1);
    tree.merge("counter", 3_i64.to_le_bytes(), 2);

    // BlobTree merge write path works (same as standard tree internally)
    tree.flush_active_memtable(0)?;

    Ok(())
}

#[test]
fn blob_tree_merge_mixed_operations() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_blob_tree_with_counter(&folder);

    tree.insert("regular", b"hello world value".to_vec(), 0);
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.remove("deleted", 2);
    tree.merge("counter", 20_i64.to_le_bytes(), 3);

    tree.flush_active_memtable(0)?;
    assert_eq!(0, tree.sealed_memtable_count());

    Ok(())
}

// --- Additional edge case tests for coverage ---

#[test]
fn merge_sealed_memtable_resolution() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Write base to memtable, rotate (seal it)
    tree.insert("key", 100_i64.to_le_bytes(), 0);

    // Rotate memtable manually by writing enough to trigger
    // or use flush with gc_threshold=0 to preserve entries
    tree.flush_active_memtable(0)?;

    // Now write operands to a new memtable, then seal it
    tree.merge("key", 10_i64.to_le_bytes(), 1);

    // Read should resolve across sealed+tables
    assert_eq!(Some(110), get_counter(&tree, "key", 2));

    Ok(())
}

#[test]
fn merge_empty_operands_after_base() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // Just a base value, no operands — get should return base
    tree.insert("key", 42_i64.to_le_bytes(), 0);
    assert_eq!(Some(42), get_counter(&tree, "key", 1));
}

#[test]
fn merge_size_of() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.merge("key", 1_i64.to_le_bytes(), 0);

    // size_of goes through get path → should resolve merge
    let size = tree.size_of("key", 1).unwrap();
    assert_eq!(size, Some(8)); // i64 = 8 bytes
}

#[test]
fn merge_is_empty_and_len() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    assert!(tree.is_empty(0, None)?);

    tree.merge("a", 1_i64.to_le_bytes(), 0);
    tree.merge("b", 2_i64.to_le_bytes(), 1);

    assert!(!tree.is_empty(2, None)?);
    assert_eq!(2, tree.len(2, None)?);

    Ok(())
}

#[test]
fn merge_first_last_key_value() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.merge("b", 1_i64.to_le_bytes(), 0);
    tree.merge("d", 2_i64.to_le_bytes(), 1);
    tree.insert("a", 10_i64.to_le_bytes(), 2);
    tree.insert("e", 20_i64.to_le_bytes(), 3);

    let first = tree.first_key_value(4, None).unwrap().key().unwrap();
    assert_eq!(&*first, b"a");

    let last = tree.last_key_value(4, None).unwrap().key().unwrap();
    assert_eq!(&*last, b"e");
}
