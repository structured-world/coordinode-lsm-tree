use lsm_tree::{AbstractTree, Config, Guard as _, SharedComparator, UserComparator};
use std::cmp::Ordering;
use std::sync::Arc;

/// Comparator that reverses the default lexicographic byte ordering.
struct ReverseComparator;

impl UserComparator for ReverseComparator {
    fn name(&self) -> &'static str {
        "reverse-lexicographic"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a) // reversed
    }
}

/// Comparator that orders u64 keys stored as big-endian bytes.
struct U64BigEndianComparator;

impl UserComparator for U64BigEndianComparator {
    fn name(&self) -> &'static str {
        "u64-big-endian"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if a.len() == 8 && b.len() == 8 {
            let a_u64 = u64::from_be_bytes(a.try_into().unwrap());
            let b_u64 = u64::from_be_bytes(b.try_into().unwrap());
            a_u64.cmp(&b_u64)
        } else {
            // Non-8-byte keys: fall back to lexicographic ordering
            // to preserve the bytewise-equality invariant.
            a.cmp(b)
        }
    }
}

#[test]
fn reverse_comparator_point_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);

    // Point reads should work regardless of comparator
    assert_eq!(tree.get("a", 3)?, Some("val_a".as_bytes().into()));
    assert_eq!(tree.get("b", 3)?, Some("val_b".as_bytes().into()));
    assert_eq!(tree.get("c", 3)?, Some("val_c".as_bytes().into()));

    Ok(())
}

#[test]
fn reverse_comparator_iteration_order() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);

    // With reverse comparator, iteration order should be c, b, a
    let items: Vec<_> = tree
        .iter(3, None)
        .map(|g| {
            let (k, v) = g.into_inner().unwrap();
            (
                String::from_utf8(k.to_vec()).unwrap(),
                String::from_utf8(v.to_vec()).unwrap(),
            )
        })
        .collect();

    assert_eq!(
        items,
        vec![
            ("c".into(), "val_c".into()),
            ("b".into(), "val_b".into()),
            ("a".into(), "val_a".into()),
        ]
    );

    Ok(())
}

#[test]
fn reverse_comparator_after_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);

    // Flush to disk
    tree.flush_active_memtable(3)?;

    // Point reads after flush
    assert_eq!(tree.get("a", 4)?, Some("val_a".as_bytes().into()));
    assert_eq!(tree.get("b", 4)?, Some("val_b".as_bytes().into()));
    assert_eq!(tree.get("c", 4)?, Some("val_c".as_bytes().into()));

    // Iteration order should still be reversed after flush
    let items: Vec<_> = tree
        .iter(4, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["c", "b", "a"]);

    Ok(())
}

#[test]
fn u64_comparator_point_read_and_order() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    // Insert u64 keys as big-endian bytes
    let keys = [1u64, 100, 50, 1000, 500];
    for (i, &key) in keys.iter().enumerate() {
        tree.insert(key.to_be_bytes(), format!("val_{key}"), i as u64);
    }

    // Point reads
    assert_eq!(
        tree.get(1u64.to_be_bytes().as_ref(), 5)?,
        Some("val_1".as_bytes().into())
    );
    assert_eq!(
        tree.get(1000u64.to_be_bytes().as_ref(), 5)?,
        Some("val_1000".as_bytes().into())
    );

    // Iteration should be in numeric order: 1, 50, 100, 500, 1000
    let items: Vec<u64> = tree
        .iter(5, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            u64::from_be_bytes(k[..8].try_into().unwrap())
        })
        .collect();

    assert_eq!(items, vec![1, 50, 100, 500, 1000]);

    Ok(())
}

#[test]
fn u64_comparator_after_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    let keys = [1u64, 100, 50, 1000, 500];
    for (i, &key) in keys.iter().enumerate() {
        tree.insert(key.to_be_bytes(), format!("val_{key}"), i as u64);
    }

    tree.flush_active_memtable(5)?;

    // Point reads after flush
    assert_eq!(
        tree.get(50u64.to_be_bytes().as_ref(), 6)?,
        Some("val_50".as_bytes().into())
    );

    // Iteration should still be in numeric order
    let items: Vec<u64> = tree
        .iter(6, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            u64::from_be_bytes(k[..8].try_into().unwrap())
        })
        .collect();

    assert_eq!(items, vec![1, 50, 100, 500, 1000]);

    Ok(())
}

#[test]
fn default_comparator_unchanged_behavior() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // No custom comparator — default lexicographic should work as before
    let tree = Config::new(&folder, Default::default(), Default::default()).open()?;

    tree.insert("banana", "b", 0);
    tree.insert("apple", "a", 1);
    tree.insert("cherry", "c", 2);

    let items: Vec<_> = tree
        .iter(3, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["apple", "banana", "cherry"]);

    Ok(())
}

#[test]
fn reverse_comparator_bounded_range_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "1", 0);
    tree.insert("b", "2", 1);
    tree.insert("c", "3", 2);
    tree.insert("d", "4", 3);
    tree.insert("e", "5", 4);

    // Reverse order: e, d, c, b, a
    // Range "d"..="b" in reverse comparator means: items where cmp says key >= "d" && key <= "b"
    // In reverse: "d" < "c" < "b" (reversed), so range "d"..="b" should yield d, c, b
    let items: Vec<_> = tree
        .range("d"..="b", 5, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["d", "c", "b"]);

    Ok(())
}

#[test]
fn u64_comparator_bounded_range_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    for &key in &[10u64, 50, 100, 500, 1000] {
        tree.insert(key.to_be_bytes(), format!("v{key}"), key);
    }

    // Range scan: 50..=500 should yield 50, 100, 500
    let lo = 50u64.to_be_bytes();
    let hi = 500u64.to_be_bytes();
    let items: Vec<u64> = tree
        .range(lo..=hi, 1001, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            u64::from_be_bytes(k[..8].try_into().unwrap())
        })
        .collect();

    assert_eq!(items, vec![50, 100, 500]);

    Ok(())
}

// --- Tests from #101: comparator name persistence and mismatch detection ---

#[test]
fn reopen_with_same_comparator_succeeds() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree with reverse comparator
    {
        let cmp: SharedComparator = Arc::new(ReverseComparator);
        let tree = Config::new(&folder, Default::default(), Default::default())
            .comparator(cmp)
            .open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(1)?;
    }

    // Reopen with the same comparator — must succeed
    let cmp: SharedComparator = Arc::new(ReverseComparator);
    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    assert_eq!(tree.get("a", 2)?, Some("1".as_bytes().into()));

    Ok(())
}

#[test]
fn reopen_with_different_comparator_fails() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree with reverse comparator
    {
        let cmp: SharedComparator = Arc::new(ReverseComparator);
        let tree = Config::new(&folder, Default::default(), Default::default())
            .comparator(cmp)
            .open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(1)?;
    }

    // Reopen with u64 comparator — must fail
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);
    let result = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open();

    match result {
        Err(lsm_tree::Error::ComparatorMismatch { stored, supplied }) => {
            assert_eq!(stored, "reverse-lexicographic");
            assert_eq!(supplied, "u64-big-endian");
        }
        Ok(_) => panic!("expected ComparatorMismatch, got Ok"),
        Err(e) => panic!("expected ComparatorMismatch, got {e:?}"),
    }

    Ok(())
}

#[test]
fn reopen_custom_with_default_comparator_fails() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree with reverse comparator
    {
        let cmp: SharedComparator = Arc::new(ReverseComparator);
        let tree = Config::new(&folder, Default::default(), Default::default())
            .comparator(cmp)
            .open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(1)?;
    }

    // Reopen without specifying a comparator (uses default) — must fail
    let result = Config::new(&folder, Default::default(), Default::default()).open();

    match result {
        Err(lsm_tree::Error::ComparatorMismatch { stored, supplied }) => {
            assert_eq!(stored, "reverse-lexicographic");
            assert_eq!(supplied, "default");
        }
        Ok(_) => panic!("expected ComparatorMismatch, got Ok"),
        Err(e) => panic!("expected ComparatorMismatch, got {e:?}"),
    }

    Ok(())
}

#[test]
fn reopen_default_tree_with_default_comparator_succeeds() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree with default comparator
    {
        let tree = Config::new(&folder, Default::default(), Default::default()).open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(1)?;
    }

    // Reopen with default comparator — must succeed
    let tree = Config::new(&folder, Default::default(), Default::default()).open()?;
    assert_eq!(tree.get("a", 2)?, Some("1".as_bytes().into()));

    Ok(())
}

/// Comparator with a name exceeding the 256-byte limit.
struct OversizedNameComparator;

impl UserComparator for OversizedNameComparator {
    fn name(&self) -> &'static str {
        // 300 chars — exceeds MAX_COMPARATOR_NAME_BYTES (256)
        "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
}

#[test]
fn oversized_comparator_name_rejected_on_create() {
    let folder = tempfile::tempdir().unwrap();
    let cmp: SharedComparator = Arc::new(OversizedNameComparator);

    let result = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open();

    match result {
        Err(lsm_tree::Error::Io(e)) => {
            assert_eq!(e.kind(), lsm_tree::io::ErrorKind::InvalidInput);
        }
        Ok(_) => panic!("expected InvalidInput error for oversized comparator name"),
        Err(e) => panic!("expected InvalidInput Io error, got {e:?}"),
    }
}

// --- Regression tests for #98: Run::push() comparator bug ---

/// Regression test for #98: Run::push() sorted tables lexicographically,
/// breaking iteration order after compaction with non-lexicographic comparators.
#[test]
fn reverse_comparator_after_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    // Flush two separate SSTs so compaction merges them
    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.flush_active_memtable(2)?;

    tree.insert("c", "val_c", 3);
    tree.insert("d", "val_d", 4);
    tree.flush_active_memtable(5)?;

    // Major compaction merges the two SSTs into one
    tree.major_compact(u64::MAX, 6)?;

    // Iteration order must be reverse: d, c, b, a
    let items: Vec<_> = tree
        .iter(7, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["d", "c", "b", "a"]);

    // Point reads must still work
    assert_eq!(tree.get("a", 7)?, Some("val_a".as_bytes().into()));
    assert_eq!(tree.get("d", 7)?, Some("val_d".as_bytes().into()));

    Ok(())
}

/// Regression test for #98: verify leveled compaction preserves
/// comparator ordering across multiple flushes.
#[test]
fn reverse_comparator_leveled_compaction() -> lsm_tree::Result<()> {
    use lsm_tree::compaction::Leveled;

    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    // Create multiple flushes to trigger L0 -> L1 compaction.
    // Use monotonically increasing seqnos to avoid MVCC visibility issues.
    let mut seqno: u64 = 0;
    for batch in 0..4u8 {
        let base = batch * 3;
        for i in 0..3u8 {
            let key = [base + i + b'a'];
            tree.insert(key, format!("val_{}", key[0] as char), seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(seqno)?;
        seqno += 1;
    }

    // Compact with leveled strategy
    tree.compact(Arc::new(Leveled::default()), seqno)?;

    // All 12 keys should iterate in reverse order
    let items: Vec<_> = tree
        .iter(seqno, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    // Reverse of a..l is l, k, j, i, h, g, f, e, d, c, b, a
    let mut expected: Vec<String> = (b'a'..=b'l').map(|c| String::from(c as char)).collect();
    expected.reverse();

    assert_eq!(items, expected);

    Ok(())
}

/// Regression test for #98: merge operators must resolve correctly
/// after compaction with custom comparator.
#[test]
fn reverse_comparator_compaction_with_merge_operator() -> lsm_tree::Result<()> {
    use lsm_tree::MergeOperator;

    struct ConcatMerge;
    impl MergeOperator for ConcatMerge {
        fn merge(
            &self,
            _key: &[u8],
            _base: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> lsm_tree::Result<lsm_tree::Slice> {
            let mut result = Vec::new();
            for (i, op) in operands.iter().enumerate() {
                if i > 0 {
                    result.push(b',');
                }
                result.extend_from_slice(op);
            }
            Ok(result.into())
        }
    }

    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .with_merge_operator(Some(Arc::new(ConcatMerge)))
        .open()?;

    // Two flushes with merge values for the same key
    tree.merge("key", "v1", 0);
    tree.flush_active_memtable(1)?;

    tree.merge("key", "v2", 2);
    tree.flush_active_memtable(3)?;

    // Compaction should merge the values correctly
    tree.major_compact(u64::MAX, 4)?;

    let val = tree.get("key", 5)?;
    assert_eq!(val, Some(b"v1,v2".as_ref().into()));

    Ok(())
}

/// Regression test for #98: tombstones should be applied correctly
/// after compaction with custom comparator.
#[test]
fn reverse_comparator_compaction_with_tombstone() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);
    tree.flush_active_memtable(3)?;

    // Delete "b" in a second flush
    tree.remove("b", 4);
    tree.flush_active_memtable(5)?;

    tree.major_compact(u64::MAX, 6)?;

    // "b" should be gone
    assert_eq!(tree.get("b", 7)?, None);

    // Remaining keys in reverse order
    let items: Vec<_> = tree
        .iter(7, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["c", "a"]);

    Ok(())
}

/// Regression test for #94: memtable range tombstones must use the configured
/// comparator, not lexicographic ordering, for interval validation and lookup.
#[test]
fn reverse_comparator_memtable_range_tombstone_point_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);
    tree.insert("d", "val_d", 3);
    tree.insert("e", "val_e", 4);

    // Reverse comparator order is e < d < c < b < a, so [d, b) must suppress d and c.
    let size = tree.remove_range("d", "b", 10);
    assert!(
        size > 0,
        "valid comparator-ordered interval must not be rejected"
    );

    assert_eq!(tree.get("e", 11)?, Some("val_e".as_bytes().into()));
    assert_eq!(tree.get("d", 11)?, None);
    assert_eq!(tree.get("c", 11)?, None);
    assert_eq!(tree.get("b", 11)?, Some("val_b".as_bytes().into()));
    assert_eq!(tree.get("a", 11)?, Some("val_a".as_bytes().into()));

    Ok(())
}

/// Regression test for #94: range scans over flushed range tombstones must use
/// the configured comparator through RT collection, filtering, and suppression.
#[test]
fn reverse_comparator_range_tombstone_scan_after_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);
    tree.insert("d", "val_d", 3);
    tree.insert("e", "val_e", 4);
    tree.flush_active_memtable(5)?;

    let size = tree.remove_range("d", "b", 10);
    assert!(
        size > 0,
        "valid comparator-ordered interval must not be rejected"
    );
    tree.flush_active_memtable(11)?;

    let items: Vec<_> = tree
        .iter(12, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            String::from_utf8(k.to_vec()).unwrap()
        })
        .collect();

    assert_eq!(items, vec!["e", "b", "a"]);

    Ok(())
}

/// Exercises RunReader::new_cmp path in range scans.
/// Needs multiple SSTs in a single disjoint run (L1+) so RunReader
/// is used instead of single-table fast path.
#[test]
fn u64_comparator_range_scan_multi_table_run() -> lsm_tree::Result<()> {
    use lsm_tree::compaction::Leveled;

    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;

    // Create 3 flushes with disjoint key ranges → after leveled compaction
    // they end up as one multi-table run in L1.
    for &key in &[10u64, 20, 30] {
        tree.insert(key.to_be_bytes(), format!("v{key}"), key);
    }
    tree.flush_active_memtable(0)?;

    for &key in &[40u64, 50, 60] {
        tree.insert(key.to_be_bytes(), format!("v{key}"), key);
    }
    tree.flush_active_memtable(0)?;

    for &key in &[70u64, 80, 90] {
        tree.insert(key.to_be_bytes(), format!("v{key}"), key);
    }
    tree.flush_active_memtable(0)?;

    // Compact into L1 — creates multi-table run
    tree.compact(Arc::new(Leveled::default()), 100)?;

    // Full range scan — exercises RunReader::new_cmp on multi-table run
    let items: Vec<u64> = tree
        .iter(100, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            u64::from_be_bytes(k[..8].try_into().unwrap())
        })
        .collect();

    assert_eq!(items, vec![10, 20, 30, 40, 50, 60, 70, 80, 90]);

    // Bounded range scan — exercises RunReader::new_cmp with bounds
    let lo = 30u64.to_be_bytes();
    let hi = 70u64.to_be_bytes();
    let items: Vec<u64> = tree
        .range(lo..=hi, 100, None)
        .map(|g| {
            let (k, _) = g.into_inner().unwrap();
            u64::from_be_bytes(k[..8].try_into().unwrap())
        })
        .collect();

    assert_eq!(items, vec![30, 40, 50, 60, 70]);

    Ok(())
}

/// A comparator mismatch is a CONFIGURATION error, not on-disk damage:
/// `open_or_repair` must return it untouched instead of "repairing" — a
/// repair under the wrong ordering would re-sort or drop healthy SSTs and
/// overwrite the manifest that still names the correct comparator.
#[test]
fn open_or_repair_propagates_a_comparator_mismatch_without_repairing() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let cmp: SharedComparator = Arc::new(ReverseComparator);
        let tree = Config::new(&folder, Default::default(), Default::default())
            .comparator(cmp)
            .open()?;
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(1)?;
    }
    let manifest_names = || -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(&folder)
            .expect("read dir")
            .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with('v') || n == "current")
            .collect();
        names.sort();
        names
    };
    let before = manifest_names();

    let cmp: SharedComparator = Arc::new(U64BigEndianComparator);
    let result = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open_or_repair(lsm_tree::RepairPolicy::default().salvage(true));
    assert!(
        matches!(result, Err(lsm_tree::Error::ComparatorMismatch { .. })),
        "the reversible configuration error must propagate, never repair: {:?}",
        result.map(|_| "opened"),
    );
    assert_eq!(
        before,
        manifest_names(),
        "a repair would have rewritten the manifest generation",
    );

    // The correct configuration still opens the untouched store.
    let cmp: SharedComparator = Arc::new(ReverseComparator);
    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .open()?;
    assert_eq!(tree.get("a", 2)?, Some("1".as_bytes().into()));
    Ok(())
}

/// The per-block zone map records a column's bounds in BYTE order, which is the
/// only ordering a value column has. The intrinsic user-key column is different:
/// its consumer compares those bounds with the tree comparator, so under a
/// non-lexicographic comparator the recorded minimum is the comparator's
/// maximum and the range estimate can stop before blocks that overlap the query,
/// undercounting rows and selectivity.
#[test]
fn range_cardinality_is_not_skewed_by_a_reverse_comparator() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);

    let tree = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .data_block_size_policy(lsm_tree::config::BlockSizePolicy::all(512))
        .open()?;
    let lsm_tree::AnyTree::Standard(tree) = tree else {
        panic!("expected Standard tree");
    };
    tree.update_runtime_config(|c| c.zone_map = true)?;

    // Many small blocks, so a mis-ordered bound truncates the walk early.
    for i in 0..400u32 {
        tree.insert(format!("k{i:06}"), vec![b'v'; 64], u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    // A BOUNDED range: the early-stop check only fires against a real upper
    // bound, and under this comparator "k000300" precedes "k000200".
    let lo = b"k000300".to_vec();
    let hi = b"k000200".to_vec();
    let actual = tree
        .range(lo.clone()..hi.clone(), lsm_tree::SeqNo::MAX, None)
        .count() as u64;
    assert!(actual > 50, "test precondition: the range holds rows");

    let est = tree.approximate_range_cardinality(lo..hi, lsm_tree::SeqNo::MAX)?;
    assert!(
        est.rows * 2 >= actual,
        "the estimate must not collapse under a non-lexicographic comparator: \
         estimated {} against {actual} actual",
        est.rows,
    );
    Ok(())
}

/// A COLUMNAR block records per-column bounds in BYTE order, which is the only
/// ordering a value column has. The intrinsic user-key column is different: the
/// range estimator compares those bounds with the tree comparator, so under a
/// non-lexicographic comparator the recorded minimum is the comparator's
/// maximum, the walk stops before blocks that overlap the query, and rows and
/// selectivity are undercounted.
#[cfg(feature = "columnar")]
#[test]
fn columnar_range_cardinality_is_not_skewed_by_a_reverse_comparator() -> lsm_tree::Result<()> {
    use lsm_tree::table::columnar::entries_to_column_batch;
    use lsm_tree::{InternalValue, SeqNo, ValueType};

    let folder = tempfile::tempdir()?;
    let cmp: SharedComparator = Arc::new(ReverseComparator);
    let any = Config::new(&folder, Default::default(), Default::default())
        .comparator(cmp)
        .data_block_size_policy(lsm_tree::config::BlockSizePolicy::all(256))
        .open()?;
    let lsm_tree::AnyTree::Standard(tree) = &any else {
        panic!("expected Standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })?;

    // Keys in COMPARATOR order (descending bytes), across several blocks.
    let entries: Vec<InternalValue> = (0..200u32)
        .rev()
        .map(|i| {
            InternalValue::from_components(
                format!("k{i:06}").into_bytes(),
                vec![b'v'; 32],
                0,
                ValueType::Value,
            )
        })
        .collect();
    let batch = entries_to_column_batch(&entries).expect("transpose");
    let mut ingest = any.ingestion()?;
    ingest.write_columnar_batch(&batch)?;
    ingest.finish()?;

    let lo = b"k000150".to_vec();
    let hi = b"k000050".to_vec();
    let actual = any.range(lo.clone()..hi.clone(), SeqNo::MAX, None).count() as u64;
    assert!(
        actual > 50,
        "test precondition: the range holds rows, got {actual}"
    );

    let est = any.approximate_range_cardinality(lo..hi, SeqNo::MAX)?;
    assert!(
        est.rows * 2 >= actual,
        "the estimate must not collapse under a non-lexicographic comparator: \
         estimated {} against {actual} actual",
        est.rows,
    );
    Ok(())
}
