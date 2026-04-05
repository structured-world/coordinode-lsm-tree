use lsm_tree::{
    AbstractTree, Config, KvSeparationOptions, MergeOperator, SeqNo, SequenceNumberCounter,
    UserValue, WriteBatch, get_tmp_folder,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn write_batch_insert_and_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let mut batch = WriteBatch::new();
    batch.insert("a", "val_a");
    batch.insert("b", "val_b");
    batch.insert("c", "val_c");

    let (bytes_added, _memtable_size) = tree.apply_batch(batch, 0)?;
    assert!(bytes_added > 0);

    assert_eq!(tree.get("a", 1)?.as_deref(), Some(b"val_a".as_slice()));
    assert_eq!(tree.get("b", 1)?.as_deref(), Some(b"val_b".as_slice()));
    assert_eq!(tree.get("c", 1)?.as_deref(), Some(b"val_c".as_slice()));

    Ok(())
}

#[test]
fn write_batch_mixed_operations() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Pre-insert a value
    tree.insert("existing", "old_value", 0);

    let mut batch = WriteBatch::new();
    batch.insert("new_key", "new_value");
    batch.remove("existing");
    tree.apply_batch(batch, 1)?;

    assert_eq!(
        tree.get("new_key", 2)?.as_deref(),
        Some(b"new_value".as_slice())
    );
    assert_eq!(tree.get("existing", 2)?, None); // tombstoned

    Ok(())
}

#[test]
fn write_batch_empty_is_noop() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let batch = WriteBatch::new();
    let (bytes_added, _) = tree.apply_batch(batch, 0)?;
    assert_eq!(bytes_added, 0);

    Ok(())
}

#[test]
fn write_batch_shared_seqno_atomic_visibility() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let mut batch = WriteBatch::new();
    batch.insert("x", "vx");
    batch.insert("y", "vy");
    batch.insert("z", "vz");
    tree.apply_batch(batch, 5)?;

    // At seqno=5, none should be visible (memtable uses seqno-1 as upper bound)
    assert_eq!(tree.get("x", 5)?, None);
    assert_eq!(tree.get("y", 5)?, None);
    assert_eq!(tree.get("z", 5)?, None);

    // At seqno=6, all should be visible atomically
    assert_eq!(tree.get("x", 6)?.as_deref(), Some(b"vx".as_slice()));
    assert_eq!(tree.get("y", 6)?.as_deref(), Some(b"vy".as_slice()));
    assert_eq!(tree.get("z", 6)?.as_deref(), Some(b"vz".as_slice()));

    Ok(())
}

#[test]
fn write_batch_with_capacity() {
    let batch = WriteBatch::with_capacity(100);
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
}

#[test]
fn write_batch_clear() {
    let mut batch = WriteBatch::new();
    batch.insert("a", "b");
    batch.remove("c");
    assert_eq!(batch.len(), 2);

    batch.clear();
    assert!(batch.is_empty());
}

#[test]
fn write_batch_survives_flush() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let mut batch = WriteBatch::new();
    for i in 0..50u32 {
        batch.insert(format!("key_{i:04}"), format!("val_{i}"));
    }
    tree.apply_batch(batch, 0)?;
    tree.flush_active_memtable(0)?;

    for i in 0..50u32 {
        let expected = format!("val_{i}");
        assert_eq!(
            tree.get(format!("key_{i:04}"), 1)?.as_deref(),
            Some(expected.as_bytes()),
            "mismatch at key {i} after flush",
        );
    }

    Ok(())
}

#[test]
fn write_batch_blob_tree_kv_separation() -> lsm_tree::Result<()> {
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

    let big_val = b"x".repeat(1000);

    let mut batch = WriteBatch::new();
    batch.insert("k1", big_val.as_slice());
    batch.insert("k2", big_val.as_slice());
    batch.remove("k3"); // tombstone for non-existent key
    tree.apply_batch(batch, 0)?;

    tree.flush_active_memtable(0)?;
    assert!(tree.blob_file_count() > 0);

    assert_eq!(
        tree.get("k1", SeqNo::MAX)?.as_deref(),
        Some(big_val.as_slice())
    );
    assert_eq!(
        tree.get("k2", SeqNo::MAX)?.as_deref(),
        Some(big_val.as_slice())
    );
    assert_eq!(tree.get("k3", SeqNo::MAX)?, None);

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
fn write_batch_with_merge_operand() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(ConcatMerge)))
    .open()?;

    // Base value
    tree.insert("counter", "A", 0);

    // Batch with merge operands
    let mut batch = WriteBatch::new();
    batch.merge("counter", "B");
    batch.merge("counter", "C");
    tree.apply_batch(batch, 1)?;

    let result = tree.get("counter", 2)?;
    assert_eq!(result.as_deref(), Some(b"ABC".as_slice()));

    Ok(())
}

#[test]
fn write_batch_remove_weak() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "val", 0);

    let mut batch = WriteBatch::new();
    batch.remove_weak("a");
    tree.apply_batch(batch, 1)?;

    assert_eq!(tree.get("a", 2)?, None);

    Ok(())
}

#[test]
fn write_batch_multi_get_after_batch() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let mut batch = WriteBatch::new();
    for i in 0..20u32 {
        batch.insert(format!("key_{i:04}"), format!("val_{i}"));
    }
    tree.apply_batch(batch, 0)?;

    // Flush half, keep half in memtable
    tree.flush_active_memtable(0)?;

    let mut batch2 = WriteBatch::new();
    for i in 20..40u32 {
        batch2.insert(format!("key_{i:04}"), format!("val_{i}"));
    }
    tree.apply_batch(batch2, 1)?;

    // multi_get spanning disk (0-19) + memtable (20-39) + missing (40-44)
    let keys: Vec<String> = (0..45u32).map(|i| format!("key_{i:04}")).collect();
    let results = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(results.len(), 45);
    for i in 0..40u32 {
        let expected = format!("val_{i}");
        assert_eq!(
            results[i as usize].as_deref(),
            Some(expected.as_bytes()),
            "mismatch at key_{i:04}",
        );
    }
    for i in 40..45u32 {
        assert_eq!(results[i as usize], None, "key_{i:04} should not exist");
    }

    Ok(())
}

#[test]
fn write_batch_mixed_ops_rejected() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let mut batch = WriteBatch::new();
    batch.insert("conflict", "value");
    batch.remove("conflict"); // mixed op on same key

    let result = tree.apply_batch(batch, 0);
    assert!(
        matches!(result, Err(lsm_tree::Error::MixedOperationBatch)),
        "mixed insert+remove on same key must be rejected",
    );

    Ok(())
}

#[test]
fn write_batch_repeated_merge_accepted() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(ConcatMerge)))
    .open()?;

    let mut batch = WriteBatch::new();
    batch.merge("k", "A");
    batch.merge("k", "B"); // same op type on same key = ok

    tree.apply_batch(batch, 0)?; // must not error

    Ok(())
}
