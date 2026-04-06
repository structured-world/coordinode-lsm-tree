use lsm_tree::{
    AbstractTree, AnyTree, Config, KvSeparationOptions, MergeOperator, PinnableSlice, SeqNo,
    SequenceNumberCounter, UserValue, get_tmp_folder,
};
use std::sync::Arc;
use test_log::test;

fn setup_tree() -> lsm_tree::Result<(AnyTree, tempfile::TempDir)> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    Ok((tree, folder))
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
fn get_pinned_memtable_returns_owned() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    tree.insert("a", "value_a", 0);

    let result = tree.get_pinned("a", 1)?;
    assert!(result.is_some());

    let ps = result.expect("should exist");
    // Memtable values are always Owned
    assert!(!ps.is_pinned());
    assert_eq!(ps.value(), b"value_a");
    assert_eq!(ps.len(), 7);
    assert!(!ps.is_empty());

    Ok(())
}

#[test]
fn get_pinned_disk_returns_pinned() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    tree.insert("a", "disk_value", 0);
    tree.flush_active_memtable(0)?;

    let result = tree.get_pinned("a", SeqNo::MAX)?;
    assert!(result.is_some());

    let ps = result.expect("should exist");
    // Disk values should be Pinned (block cache)
    assert!(ps.is_pinned());
    assert_eq!(ps.value(), b"disk_value");

    Ok(())
}

#[test]
fn get_pinned_missing_key_returns_none() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    let result = tree.get_pinned("nonexistent", 1)?;
    assert!(result.is_none());

    Ok(())
}

#[test]
fn get_pinned_tombstoned_key_returns_none() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    tree.insert("a", "value", 0);
    tree.remove("a", 1);

    let result = tree.get_pinned("a", 2)?;
    assert!(result.is_none());

    Ok(())
}

#[test]
fn get_pinned_into_value_conversion() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    tree.insert("a", "my_value", 0);

    let ps = tree.get_pinned("a", 1)?.expect("should exist");
    let user_value = ps.into_value();
    assert_eq!(&*user_value, b"my_value");

    Ok(())
}

#[test]
fn get_pinned_matches_get() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    for i in 0..20u32 {
        tree.insert(format!("key_{i:04}"), format!("val_{i}"), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    // Insert some in memtable too
    for i in 20..30u32 {
        tree.insert(format!("key_{i:04}"), format!("val_{i}"), u64::from(i));
    }

    // Verify get_pinned returns same data as get for all keys
    for i in 0..35u32 {
        let key = format!("key_{i:04}");
        let regular = tree.get(&key, SeqNo::MAX)?;
        let pinned = tree.get_pinned(&key, SeqNo::MAX)?;

        match (&regular, &pinned) {
            (Some(r), Some(p)) => {
                assert_eq!(r.as_ref(), p.value(), "mismatch at key {key}");
            }
            (None, None) => {}
            _ => panic!("get and get_pinned disagree for key {key}"),
        }
    }

    Ok(())
}

#[test]
fn pinnable_slice_partial_eq() {
    let ps = PinnableSlice::owned(b"hello".as_slice().into());
    assert_eq!(ps.value(), b"hello");
    assert!(ps == b"hello".as_slice());
}

#[test]
fn pinnable_slice_debug_format() {
    let ps = PinnableSlice::owned(b"hello".as_slice().into());
    let debug = format!("{ps:?}");
    assert!(debug.contains("Owned"));

    // Clone
    let ps2 = ps.clone();
    assert_eq!(ps2.value(), b"hello");
}

#[test]
fn get_pinned_blob_tree_returns_owned() -> lsm_tree::Result<()> {
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

    let big_val = b"x".repeat(500);
    tree.insert("a", big_val.as_slice(), 0);
    tree.flush_active_memtable(0)?;

    // BlobTree uses default get_pinned impl → always Owned
    let ps = tree.get_pinned("a", SeqNo::MAX)?;
    assert!(ps.is_some());
    let ps = ps.expect("should exist");
    assert!(!ps.is_pinned()); // blob-resolved values are Owned
    assert_eq!(ps.value(), big_val.as_slice());

    Ok(())
}

#[test]
fn get_pinned_with_range_tombstone_returns_none() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);
    tree.remove_range("a", "c", 3); // deletes [a, c)

    let result_a = tree.get_pinned("a", 4)?;
    let result_b = tree.get_pinned("b", 4)?;
    let result_c = tree.get_pinned("c", 4)?;

    assert!(
        result_a.is_none(),
        "a should be suppressed by range tombstone"
    );
    assert!(
        result_b.is_none(),
        "b should be suppressed by range tombstone"
    );
    assert!(
        result_c.is_some(),
        "c is at the exclusive end, not suppressed"
    );
    assert_eq!(result_c.expect("should exist").value(), b"val_c");

    Ok(())
}

#[test]
fn get_pinned_after_compaction_returns_pinned() -> lsm_tree::Result<()> {
    use lsm_tree::compaction::Leveled;

    let (tree, _folder) = setup_tree()?;

    // Write and flush multiple times to create L0 tables
    for batch in 0..3u32 {
        for i in 0..10u32 {
            let key = format!("key_{i:04}");
            let val = format!("val_{batch}_{i}");
            tree.insert(key, val, u64::from(batch * 10 + i));
        }
        tree.flush_active_memtable(0)?;
    }

    // Compact to push data to L1+
    tree.compact(Arc::new(Leveled::default()), SeqNo::MAX)?;

    // Values from compacted L1+ tables should be Pinned
    let ps = tree.get_pinned("key_0005", SeqNo::MAX)?;
    assert!(ps.is_some());
    let ps = ps.expect("should exist");
    assert!(ps.is_pinned(), "compacted L1+ value should be Pinned");

    Ok(())
}

#[test]
fn get_pinned_sealed_memtable_returns_owned() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    tree.insert("a", "val_sealed", 0);

    // Rotate memtable (seals it) without flushing
    tree.rotate_memtable();

    let ps = tree.get_pinned("a", 1)?;
    assert!(ps.is_some());
    let ps = ps.expect("should exist");
    // Sealed memtable values are still Owned (in-memory)
    assert!(!ps.is_pinned());
    assert_eq!(ps.value(), b"val_sealed");

    Ok(())
}

#[test]
fn get_pinned_disk_exercises_pinned_methods() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    tree.insert("a", "pinned_value", 0);
    tree.flush_active_memtable(0)?;

    let ps = tree.get_pinned("a", SeqNo::MAX)?.expect("should exist");
    assert!(ps.is_pinned());

    // Exercise len/is_empty/value on Pinned variant
    assert_eq!(ps.len(), 12);
    assert!(!ps.is_empty());
    assert_eq!(ps.value(), b"pinned_value");

    // Exercise AsRef<[u8]> on Pinned
    let bytes: &[u8] = ps.as_ref();
    assert_eq!(bytes, b"pinned_value");

    // Exercise PartialEq<&[u8]> on Pinned
    assert!(ps == b"pinned_value".as_slice());

    // Exercise Debug on Pinned
    let debug = format!("{ps:?}");
    assert!(debug.contains("Pinned"));

    // Exercise Clone on Pinned
    let ps2 = ps.clone();
    assert_eq!(ps2.value(), b"pinned_value");
    assert!(ps2.is_pinned());

    // Exercise into_value on Pinned
    let uv: lsm_tree::UserValue = ps.into_value();
    assert_eq!(&*uv, b"pinned_value");

    // Exercise From<PinnableSlice> for UserValue on Pinned
    let uv2: lsm_tree::UserValue = ps2.into();
    assert_eq!(&*uv2, b"pinned_value");

    Ok(())
}

#[test]
fn get_pinned_empty_value_on_disk() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    // Insert with empty value
    tree.insert("empty", "", 0);
    tree.flush_active_memtable(0)?;

    let ps = tree.get_pinned("empty", SeqNo::MAX)?.expect("should exist");
    assert!(ps.is_pinned());
    assert!(ps.is_empty());
    assert_eq!(ps.len(), 0);

    Ok(())
}

#[test]
fn get_pinned_tombstone_on_disk_returns_none() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    tree.insert("a", "value", 0);
    tree.remove("a", 1);
    tree.flush_active_memtable(0)?;

    // Tombstone on disk — get_pinned should return None
    let result = tree.get_pinned("a", SeqNo::MAX)?;
    assert!(result.is_none());

    Ok(())
}

#[test]
fn get_pinned_range_tombstone_on_disk_suppresses() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    tree.insert("a", "v1", 0);
    tree.insert("b", "v2", 1);
    tree.flush_active_memtable(0)?;

    // Range tombstone in a later SST suppresses older disk values
    tree.remove_range("a", "c", 2);
    tree.flush_active_memtable(0)?;

    let result_a = tree.get_pinned("a", 3)?;
    let result_b = tree.get_pinned("b", 3)?;
    assert!(
        result_a.is_none(),
        "disk value should be suppressed by on-disk RT"
    );
    assert!(
        result_b.is_none(),
        "disk value should be suppressed by on-disk RT"
    );

    Ok(())
}

#[test]
fn get_pinned_with_merge_operator_in_memtable() -> lsm_tree::Result<()> {
    // ConcatMerge defined at module scope

    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(ConcatMerge)))
    .open()?;

    tree.insert("k", "A", 0);
    tree.merge("k", "B", 1);

    // Merge operand in active memtable → get_pinned resolves via pipeline → Owned
    let ps = tree.get_pinned("k", 2)?.expect("should resolve merge");
    assert!(!ps.is_pinned());
    assert_eq!(ps.value(), b"AB");

    Ok(())
}

#[test]
fn get_pinned_with_merge_operator_in_sealed_memtable() -> lsm_tree::Result<()> {
    // ConcatMerge defined at module scope

    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(ConcatMerge)))
    .open()?;

    tree.insert("k", "X", 0);
    tree.merge("k", "Y", 1);
    tree.rotate_memtable();

    // Merge operand in sealed memtable → resolves via pipeline → Owned
    let ps = tree.get_pinned("k", 2)?.expect("should resolve merge");
    assert!(!ps.is_pinned());
    assert_eq!(ps.value(), b"XY");

    Ok(())
}

#[test]
fn get_pinned_with_merge_operator_on_disk() -> lsm_tree::Result<()> {
    // ConcatMerge defined at module scope

    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(ConcatMerge)))
    .open()?;

    tree.insert("k", "D", 0);
    tree.flush_active_memtable(0)?;
    tree.merge("k", "E", 1);
    tree.flush_active_memtable(0)?;

    // Cross-SST merge: base in first SST, operand in second → resolves via pipeline → Owned
    let ps = tree.get_pinned("k", 2)?.expect("should resolve merge");
    assert!(!ps.is_pinned());
    assert_eq!(ps.value(), b"DE");

    Ok(())
}

#[test]
fn get_pinned_sealed_memtable_tombstone_returns_none() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    tree.insert("a", "value", 0);
    tree.remove("a", 1);
    tree.rotate_memtable();

    let result = tree.get_pinned("a", 2)?;
    assert!(result.is_none());

    Ok(())
}

#[test]
fn get_pinned_sealed_memtable_range_tombstone_suppresses() -> lsm_tree::Result<()> {
    let (tree, _folder) = setup_tree()?;

    tree.insert("b", "value", 0);
    tree.remove_range("a", "c", 1);
    tree.rotate_memtable();

    // Value and RT both in sealed memtable
    let result = tree.get_pinned("b", 2)?;
    assert!(result.is_none(), "sealed memtable value suppressed by RT");

    Ok(())
}
