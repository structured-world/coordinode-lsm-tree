use lsm_tree::{
    AbstractTree, Config, KvSeparationOptions, MergeOperator, SeqNo, SequenceNumberCounter,
    UserValue, get_tmp_folder,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn multi_get_all_existing() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..100u64 {
        tree.insert(format!("key_{i:04}"), format!("value_{i}"), i);
    }

    tree.flush_active_memtable(0)?;

    let keys: Vec<String> = (0..100u64).map(|i| format!("key_{i:04}")).collect();
    let results = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(results.len(), 100);
    for (i, result) in results.iter().enumerate() {
        let expected = format!("value_{i}");
        assert_eq!(
            result.as_deref(),
            Some(expected.as_bytes()),
            "mismatch at index {i}",
        );
    }

    Ok(())
}

#[test]
fn multi_get_mixed_existing_and_missing() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("c", "val_c", 1);
    tree.insert("e", "val_e", 2);

    let results = tree.multi_get(["a", "b", "c", "d", "e"], 3)?;

    assert_eq!(results.len(), 5);
    assert_eq!(results[0].as_deref(), Some(b"val_a".as_slice()));
    assert_eq!(results[1], None);
    assert_eq!(results[2].as_deref(), Some(b"val_c".as_slice()));
    assert_eq!(results[3], None);
    assert_eq!(results[4].as_deref(), Some(b"val_e".as_slice()));

    Ok(())
}

#[test]
fn multi_get_empty_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "val_a", 0);

    let results = tree.multi_get(Vec::<&str>::new(), 1)?;
    assert!(results.is_empty());

    Ok(())
}

#[test]
fn multi_get_snapshot_isolation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "v1", 0);
    tree.insert("b", "v1", 1);

    // Update values at higher seqno
    tree.insert("a", "v2", 2);
    tree.insert("b", "v2", 3);

    // Read at snapshot seqno=2: should see a=v1, b=v1
    // Snapshot semantics: entry visible iff entry.seqno < snapshot_seqno
    // (memtable lookup uses `seqno - 1` as upper bound, see Memtable::get).
    // So a@2 (v2) is NOT visible at seqno=2, only a@0 (v1) is.
    let results = tree.multi_get(["a", "b"], 2)?;
    assert_eq!(results[0].as_deref(), Some(b"v1".as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"v1".as_slice()));

    // Read at snapshot seqno=4: should see a=v2, b=v2
    let results = tree.multi_get(["a", "b"], 4)?;
    assert_eq!(results[0].as_deref(), Some(b"v2".as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"v2".as_slice()));

    Ok(())
}

#[test]
fn multi_get_with_tombstones() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.remove("a", 2);

    let results = tree.multi_get(["a", "b"], 3)?;
    assert_eq!(results[0], None);
    assert_eq!(results[1].as_deref(), Some(b"val_b".as_slice()));

    Ok(())
}

#[test]
fn multi_get_from_disk() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);
    tree.flush_active_memtable(0)?;

    // Insert more to memtable
    tree.insert("d", "val_d", 3);

    // Multi-get spanning both disk and memtable
    let results = tree.multi_get(["a", "b", "c", "d", "e"], SeqNo::MAX)?;
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].as_deref(), Some(b"val_a".as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"val_b".as_slice()));
    assert_eq!(results[2].as_deref(), Some(b"val_c".as_slice()));
    assert_eq!(results[3].as_deref(), Some(b"val_d".as_slice()));
    assert_eq!(results[4], None);

    Ok(())
}

#[test]
fn multi_get_blob_tree_with_kv_separation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 1, // separate all values
        ..Default::default()
    }))
    .open()?;

    let big_val_a = b"aaa".repeat(1000);
    let big_val_b = b"bbb".repeat(1000);

    tree.insert("a", big_val_a.as_slice(), 0);
    tree.insert("b", big_val_b.as_slice(), 1);
    tree.insert("c", b"ccc".repeat(1000).as_slice(), 2);
    tree.remove("c", 3);

    tree.flush_active_memtable(0)?;

    // Verify blob indirections were created
    assert!(tree.blob_file_count() > 0);

    let results = tree.multi_get(["a", "b", "c", "missing"], SeqNo::MAX)?;

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].as_deref(), Some(big_val_a.as_slice()));
    assert_eq!(results[1].as_deref(), Some(big_val_b.as_slice()));
    assert_eq!(results[2], None); // tombstoned
    assert_eq!(results[3], None); // never existed

    Ok(())
}

#[test]
fn multi_get_unsorted_and_duplicate_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);

    // Unsorted keys with a duplicate — results must match input order 1:1
    let results = tree.multi_get(["c", "a", "b", "a", "missing"], 3)?;

    assert_eq!(results.len(), 5);
    assert_eq!(results[0].as_deref(), Some(b"val_c".as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"val_a".as_slice()));
    assert_eq!(results[2].as_deref(), Some(b"val_b".as_slice()));
    assert_eq!(results[3].as_deref(), Some(b"val_a".as_slice())); // duplicate
    assert_eq!(results[4], None);

    Ok(())
}

#[test]
fn multi_get_with_range_tombstones() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);
    tree.insert("d", "val_d", 3);
    tree.remove_range("b", "d", 4); // deletes [b, d)

    let results = tree.multi_get(["a", "b", "c", "d"], 5)?;
    assert_eq!(results[0].as_deref(), Some(b"val_a".as_slice()));
    assert_eq!(results[1], None); // range tombstoned
    assert_eq!(results[2], None); // range tombstoned
    assert_eq!(results[3].as_deref(), Some(b"val_d".as_slice())); // end is exclusive

    Ok(())
}

#[test]
fn multi_get_spanning_multiple_levels() -> lsm_tree::Result<()> {
    use lsm_tree::compaction::Leveled;

    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Write batch 1 → flush → compact to L1
    for i in 0..10u32 {
        tree.insert(format!("key_{i:04}"), format!("batch1_{i}"), u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(Leveled::default()), SeqNo::MAX)?;

    // Write batch 2 → flush (stays in L0)
    for i in 5..15u32 {
        tree.insert(
            format!("key_{i:04}"),
            format!("batch2_{i}"),
            u64::from(i + 100),
        );
    }
    tree.flush_active_memtable(0)?;

    // Write batch 3 → memtable only
    for i in 10..20u32 {
        tree.insert(
            format!("key_{i:04}"),
            format!("batch3_{i}"),
            u64::from(i + 200),
        );
    }

    // multi_get with keys spanning memtable (10-19), L0 (5-14), L1 (0-9)
    let keys: Vec<String> = (0..25u32).map(|i| format!("key_{i:04}")).collect();
    let results = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(results.len(), 25);

    // 0-4: from L1 only (batch1)
    for i in 0..5u32 {
        assert_eq!(
            results[i as usize].as_deref(),
            Some(format!("batch1_{i}").as_bytes()),
            "key_{i:04} should come from L1",
        );
    }

    // 5-9: from L0 (batch2 shadows batch1 in L1)
    for i in 5..10u32 {
        assert_eq!(
            results[i as usize].as_deref(),
            Some(format!("batch2_{i}").as_bytes()),
            "key_{i:04} should come from L0 (shadowing L1)",
        );
    }

    // 10-14: from memtable (batch3 shadows batch2 in L0)
    for i in 10..15u32 {
        assert_eq!(
            results[i as usize].as_deref(),
            Some(format!("batch3_{i}").as_bytes()),
            "key_{i:04} should come from memtable (shadowing L0)",
        );
    }

    // 15-19: from memtable (batch3, no shadowing)
    for i in 15..20u32 {
        assert_eq!(
            results[i as usize].as_deref(),
            Some(format!("batch3_{i}").as_bytes()),
            "key_{i:04} should come from memtable",
        );
    }

    // 20-24: missing
    for i in 20..25u32 {
        assert_eq!(results[i as usize], None, "key_{i:04} should not exist");
    }

    Ok(())
}

#[test]
fn multi_get_large_batch_all_from_disk() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Write 500 keys and flush to disk
    for i in 0..500u64 {
        tree.insert(format!("key_{i:05}"), format!("value_{i}"), i);
    }
    tree.flush_active_memtable(0)?;

    // Batch get all 500 in reverse order (exercises sorting)
    let keys: Vec<String> = (0..500u64).rev().map(|i| format!("key_{i:05}")).collect();
    let results = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(results.len(), 500);
    for (result_idx, i) in (0..500u64).rev().enumerate() {
        let expected = format!("value_{i}");
        assert_eq!(
            results[result_idx].as_deref(),
            Some(expected.as_bytes()),
            "mismatch at result index {result_idx} (key_{i:05})",
        );
    }

    Ok(())
}

struct ConcatMerge;

impl MergeOperator for ConcatMerge {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut result = base.unwrap_or_default().to_vec();
        for op in operands {
            result.extend_from_slice(op);
        }
        Ok(result.into())
    }
}

#[test]
fn multi_get_with_merge_operands() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(ConcatMerge)))
    .open()?;

    tree.insert("a", "base_a", 0);
    tree.merge("a", "_merged", 1);
    tree.insert("b", "val_b", 2);

    // multi_get should resolve merge operand for "a" via pipeline
    let results = tree.multi_get(["a", "b", "c"], 3)?;
    assert_eq!(results[0].as_deref(), Some(b"base_a_merged".as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"val_b".as_slice()));
    assert_eq!(results[2], None);

    Ok(())
}

#[test]
fn multi_get_with_merge_operands_on_disk() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(ConcatMerge)))
    .open()?;

    tree.insert("k1", "A", 0);
    tree.merge("k1", "B", 1);
    tree.insert("k2", "plain", 2);
    tree.flush_active_memtable(0)?;

    // Use 3+ keys to exercise the batch code path (≤2 keys uses simple per-key path)
    let results = tree.multi_get(["k1", "k2", "missing"], SeqNo::MAX)?;
    assert_eq!(results[0].as_deref(), Some(b"AB".as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"plain".as_slice()));
    assert_eq!(results[2], None);

    Ok(())
}

#[test]
fn multi_get_tombstones_on_disk_with_l0() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush batch 1 to L0
    for i in 0..5u32 {
        tree.insert(format!("key_{i:04}"), format!("val_{i}"), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    // Flush batch 2 to L0 with tombstones for some keys
    tree.remove("key_0001", 10);
    tree.remove("key_0003", 11);
    tree.insert("key_0002", "updated", 12);
    tree.flush_active_memtable(0)?;

    // Multi-get: exercises L0 batch path with tombstones
    let keys: Vec<String> = (0..5u32).map(|i| format!("key_{i:04}")).collect();
    let results = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(results[0].as_deref(), Some(b"val_0".as_slice()));
    assert_eq!(results[1], None); // tombstoned
    assert_eq!(results[2].as_deref(), Some(b"updated".as_slice()));
    assert_eq!(results[3], None); // tombstoned
    assert_eq!(results[4].as_deref(), Some(b"val_4".as_slice()));

    Ok(())
}
