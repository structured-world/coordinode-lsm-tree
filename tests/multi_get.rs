use lsm_tree::{
    AbstractTree, Cache, Config, KvSeparationOptions, MergeOperator, SeqNo, SequenceNumberCounter,
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
fn multi_get_duplicate_keys_missing_to_disk_match_per_key_get() -> lsm_tree::Result<()> {
    // Regression: the prior test keeps its keys in the memtable, so its duplicate
    // is resolved in Phase 1 and never reaches the batched on-disk path. A
    // duplicate that MISSES the memtables and resolves on disk is the real edge
    // case: the batch path requires strictly-sorted-unique keys, so a duplicate
    // query key has to be de-duplicated before the batch and the shared answer
    // fanned back to every position. Before the fix the duplicate reached the
    // batch path and broke its sorted-unique contract.
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("k0000", "v0", 0);
    tree.insert("k0008", "v8", 1);
    // Flush so both keys live in an SST: Phase 1 misses them and the duplicate
    // reaches the batched on-disk path (a batch of >= 3 keys skips the per-key
    // shortcut).
    tree.flush_active_memtable(0)?;

    // "k0008" twice + "k0000": three keys, one duplicate, all disk misses.
    let query = ["k0008", "k0008", "k0000"];
    let batched = tree.multi_get(query, SeqNo::MAX)?;
    assert_eq!(batched.len(), 3);

    // Each position must equal the authoritative per-key get for the same key.
    for (i, key) in query.iter().enumerate() {
        let single = tree.get(key, SeqNo::MAX)?;
        assert_eq!(
            batched[i].as_deref(),
            single.as_deref(),
            "multi_get vs get mismatch at position {i} (key {key})"
        );
    }
    // Concretely: both duplicates resolve to v8, the third to v0.
    assert_eq!(batched[0].as_deref(), Some(b"v8".as_slice()));
    assert_eq!(batched[1].as_deref(), Some(b"v8".as_slice()));
    assert_eq!(batched[2].as_deref(), Some(b"v0".as_slice()));

    Ok(())
}

#[test]
fn multi_get_blob_tree_duplicate_keys_missing_to_disk_match_per_key_get() -> lsm_tree::Result<()> {
    // Regression: BlobTree::multi_get forwards its disk-miss keys straight to the
    // shared batched on-disk resolver, which requires strictly-sorted-unique
    // input. A duplicate query key that misses the memtables reaches that path as
    // a repeat and breaks the contract (the standard Tree de-dupes; the BlobTree
    // must do the same and fan the shared answer back to every position).
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 1, // separate all values into blobs
        ..Default::default()
    }))
    .open()?;

    let big0 = b"0".repeat(200);
    let big8 = b"8".repeat(200);
    tree.insert("k0000", big0.as_slice(), 0);
    tree.insert("k0008", big8.as_slice(), 1);
    // Flush so both live in an SST: Phase 1 misses them and the duplicate reaches
    // the batched on-disk path (a batch of >= 3 keys skips the per-key shortcut).
    tree.flush_active_memtable(0)?;
    assert!(tree.blob_file_count() > 0);

    let query = ["k0008", "k0008", "k0000"];
    let batched = tree.multi_get(query, SeqNo::MAX)?;
    assert_eq!(batched.len(), 3);

    for (i, key) in query.iter().enumerate() {
        let single = tree.get(key, SeqNo::MAX)?;
        assert_eq!(
            batched[i].as_deref(),
            single.as_deref(),
            "blob multi_get vs get mismatch at position {i} (key {key})"
        );
    }
    assert_eq!(batched[0].as_deref(), Some(big8.as_slice()));
    assert_eq!(batched[1].as_deref(), Some(big8.as_slice()));
    assert_eq!(batched[2].as_deref(), Some(big0.as_slice()));

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

#[test]
fn multi_get_large_batch_oversize_working_set_matches_per_key_get() -> lsm_tree::Result<()> {
    // Exercises the chunked resolve: a batch far larger than the per-level chunk
    // threshold whose touched data blocks, across several SSTs, exceed the shared
    // cache. With a deliberately tiny cache the level's blocks cannot all be
    // warmed at once, so the resolver reads them in budget-sized chunks into a
    // scratch and point-reads directly. The result must still equal the
    // authoritative per-key `get` for every position.
    let folder = get_tmp_folder();

    // 16 KiB cache → ~8 KiB chunk budget, far below the total block bytes below,
    // forcing multi-chunk reads.
    let cache = Arc::new(Cache::with_capacity_bytes(16 * 1024));

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(cache)
    .open()?;

    // Four flushes over disjoint key ranges → four L0 SSTs the batch spans, so
    // the gathered block tasks cross SSTs (the property the chunked path keeps).
    let per_sst = 600u64;
    let total = 4 * per_sst;
    let mut seqno = 0u64;
    for sst in 0..4u64 {
        let start = sst * per_sst;
        for i in start..start + per_sst {
            tree.insert(format!("key_{i:06}"), format!("value_{i:08}"), seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }

    // Reverse order exercises the sort; include misses interleaved.
    let keys: Vec<String> = (0..total)
        .rev()
        .map(|i| {
            if i % 50 == 0 {
                format!("absent_{i:06}")
            } else {
                format!("key_{i:06}")
            }
        })
        .collect();
    assert!(keys.len() > 512, "batch must exceed the chunk threshold");

    let batched = tree.multi_get(&keys, SeqNo::MAX)?;
    assert_eq!(batched.len(), keys.len());

    for (pos, key) in keys.iter().enumerate() {
        let single = tree.get(key, SeqNo::MAX)?;
        assert_eq!(
            batched[pos].as_deref(),
            single.as_deref(),
            "multi_get vs get mismatch at position {pos} (key {key})"
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

#[test]
fn multi_get_blob_tree_range_tombstone_suppresses() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 1,
        ..Default::default()
    }))
    .open()?;

    let val_a = b"a".repeat(100);
    let val_b = b"b".repeat(100);
    let val_c = b"c".repeat(100);
    let val_d = b"d".repeat(100);

    tree.insert("a", val_a.as_slice(), 0);
    tree.insert("b", val_b.as_slice(), 1);
    tree.insert("c", val_c.as_slice(), 2);
    tree.insert("d", val_d.as_slice(), 3);
    tree.flush_active_memtable(0)?;

    // RT suppresses [b, d)
    tree.remove_range("b", "d", 4);

    // 4 keys → batch path (>2)
    let results = tree.multi_get(["a", "b", "c", "d"], 5)?;
    assert_eq!(results[0].as_deref(), Some(val_a.as_slice()));
    assert_eq!(results[1], None, "b suppressed by RT");
    assert_eq!(results[2], None, "c suppressed by RT");
    assert_eq!(
        results[3].as_deref(),
        Some(val_d.as_slice()),
        "d at exclusive end"
    );

    Ok(())
}

#[test]
fn multi_get_blob_tree_merge_operands() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 100,
        ..Default::default()
    }))
    .with_merge_operator(Some(Arc::new(ConcatMerge)))
    .open()?;

    // Base insert (4 bytes) stays inline (< 100 threshold).
    // Merge operands are always inline in BlobTree.
    // k2 value (200 bytes) goes to blob.
    tree.insert("k1", "BASE", 0);
    tree.merge("k1", "_EXT", 1);
    tree.insert("k2", b"x".repeat(200).as_slice(), 2);
    tree.flush_active_memtable(0)?;

    // 3 keys → batch path; k1 has merge operand on disk
    let results = tree.multi_get(["k1", "k2", "k3"], SeqNo::MAX)?;

    assert_eq!(results[0].as_deref(), Some(b"BASE_EXT".as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"x".repeat(200).as_slice()));
    assert_eq!(results[2], None);

    Ok(())
}

#[test]
fn multi_get_blob_tree_memtable_hits_skip_sst() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 1,
        ..Default::default()
    }))
    .open()?;

    // Some keys on disk
    tree.insert("a", b"disk_a".repeat(50).as_slice(), 0);
    tree.insert("b", b"disk_b".repeat(50).as_slice(), 1);
    tree.flush_active_memtable(0)?;

    // Some keys in memtable (shadow disk)
    tree.insert("a", b"mem_a".repeat(50).as_slice(), 2);
    tree.insert("c", b"mem_c".repeat(50).as_slice(), 3);

    // 4 keys → batch path; "a" from memtable, "b" from disk, "c" from memtable, "d" missing
    let results = tree.multi_get(["a", "b", "c", "d"], SeqNo::MAX)?;
    assert_eq!(results[0].as_deref(), Some(b"mem_a".repeat(50).as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"disk_b".repeat(50).as_slice()));
    assert_eq!(results[2].as_deref(), Some(b"mem_c".repeat(50).as_slice()));
    assert_eq!(results[3], None);

    Ok(())
}

#[test]
fn multi_get_blob_tree_merge_without_operator_returns_raw() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 1,
        ..Default::default()
    }))
    .open()?;

    // No merge operator configured — merge operand should return raw value
    tree.insert("k1", b"x".repeat(100).as_slice(), 0);
    tree.merge("k2", "raw_operand", 1);
    tree.insert("k3", b"y".repeat(100).as_slice(), 2);
    tree.flush_active_memtable(0)?;

    let results = tree.multi_get(["k1", "k2", "k3"], SeqNo::MAX)?;
    assert_eq!(results[0].as_deref(), Some(b"x".repeat(100).as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"raw_operand".as_slice()));
    assert_eq!(results[2].as_deref(), Some(b"y".repeat(100).as_slice()));

    Ok(())
}
