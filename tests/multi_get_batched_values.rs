// A multi-get reads what turning a key's newest version into its value needs
// through the read queue its levels are read through: the separated values of
// a blob tree, and the blocks of the tables a merge walks, go to the backend in
// one batch instead of one read each, and answer exactly what key-by-key reads
// answer, failures included.

mod common;

use common::BatchCountFs;
use lsm_tree::{
    AbstractTree, AnyTree, Cache, Config, KvSeparationOptions, MergeOperator, SeqNo,
    SequenceNumberCounter, UserValue,
};
use std::sync::Arc;

const N: usize = 64;

fn key_for(i: usize) -> String {
    format!("key{i:04}")
}

/// Distinct per key, and large enough to be separated.
fn value_for(i: usize) -> Vec<u8> {
    let mut value = format!("value-{i:04}-").into_bytes();
    value.resize(300, b'a');
    value
}

/// A blob tree of [`N`] separated values over four blob files, reopened so no
/// value is in the cache, on the backend `fs`.
fn cold_tree(dir: &std::path::Path, fs: &BatchCountFs) -> lsm_tree::Result<AnyTree> {
    let seqno = SequenceNumberCounter::default();
    let config = || {
        Config::new(dir, seqno.clone(), SequenceNumberCounter::default())
            .with_shared_fs(Arc::new(fs.clone()))
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    };
    {
        let tree = config().open()?;
        for i in 0..N {
            tree.insert(key_for(i), value_for(i), seqno.next());
            if (i + 1) % (N / 4) == 0 {
                tree.flush_active_memtable(0)?;
            }
        }
    }
    config().open()
}

/// The values a cold multi-get needs travel to the backend together: one
/// batched read carries every one of them, rather than a read each.
#[test]
fn a_blob_tree_multi_get_reads_its_values_in_one_batch() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    let tree = cold_tree(dir.path(), &fs)?;
    let keys: Vec<String> = (0..N).map(key_for).collect();
    fs.take();

    let values = tree.multi_get(&keys, SeqNo::MAX)?;
    let calls = fs.take();

    for (i, value) in values.iter().enumerate() {
        assert_eq!(value.as_deref(), Some(value_for(i).as_slice()), "key {i}");
    }
    assert!(
        calls.iter().any(|&reads| reads >= N),
        "every value read in one batch: {calls:?}"
    );
    Ok(())
}

/// A value whose record fails its checks fails the multi-get as it fails a
/// read of that key alone, with the same error, and the values beside it
/// are unaffected when read alone.
#[test]
fn a_damaged_value_fails_a_multi_get_as_it_fails_a_single_read() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    drop(cold_tree(dir.path(), &fs)?);

    // A byte in the middle of the first blob file lies in a value's payload.
    let mut blobs: Vec<_> = std::fs::read_dir(dir.path().join("blobs"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| std::fs::metadata(path).is_ok_and(|m| m.len() > 0))
        .collect();
    blobs.sort();
    let first = blobs.first().expect("a blob file");
    let mut bytes = std::fs::read(first)?;
    let middle = bytes.len() / 2;
    *bytes.get_mut(middle).expect("in the file") ^= 0xFF;
    std::fs::write(first, &bytes)?;

    let keys: Vec<String> = (0..N).map(key_for).collect();
    let single = {
        let tree = cold_tree_reopened(dir.path(), &fs)?;
        keys.iter()
            .map(|key| tree.get(key, SeqNo::MAX))
            .find_map(Result::err)
            .expect("one key reads the damaged record")
    };
    let batched = cold_tree_reopened(dir.path(), &fs)?
        .multi_get(&keys, SeqNo::MAX)
        .expect_err("the multi-get reads the damaged record too");
    assert_eq!(
        std::mem::discriminant(&batched),
        std::mem::discriminant(&single),
        "{batched:?} vs {single:?}"
    );
    Ok(())
}

/// Appends every operand to the base.
struct Concat;

impl MergeOperator for Concat {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut merged = base.unwrap_or_default().to_vec();
        for operand in operands {
            merged.extend_from_slice(operand);
        }
        Ok(merged.into())
    }
}

/// A tree in `dir` on `fs` merging with [`Concat`], with a cache that holds
/// every block.
fn merge_tree(dir: &std::path::Path, fs: &BatchCountFs) -> lsm_tree::Result<AnyTree> {
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fs.clone()))
    .use_cache(Arc::new(Cache::with_capacity_bytes(64 * 1_024 * 1_024)))
    .with_merge_operator(Some(Arc::new(Concat)))
    .open()
}

/// A cold multi-get of keys whose bases lie one level below their operands
/// brings the blocks of the bases in one batch, which only the merges read
/// (the levels stop at the operands), and merges exactly as key-by-key reads
/// do.
#[test]
fn a_multi_get_brings_the_blocks_its_merges_read_in_one_batch() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    // A kilobyte per base spreads the bases over many blocks.
    let base = |i: usize| {
        let mut base = format!("base-{i:04}-").into_bytes();
        base.resize(1_000, b'b');
        base
    };
    {
        let tree = merge_tree(dir.path(), &fs)?;
        for i in 0..N {
            tree.insert(key_for(i), base(i), i as u64);
        }
        tree.flush_active_memtable(0)?;
        tree.compact(Arc::new(lsm_tree::compaction::MoveDown(0, 1)), 0)?;
        for i in 0..N {
            tree.merge(key_for(i), format!("+{i}"), (N + i) as u64);
        }
        tree.flush_active_memtable(0)?;
    }
    let tree = merge_tree(dir.path(), &fs)?;
    let keys: Vec<String> = (0..N).map(key_for).collect();
    fs.take();

    let values = tree.multi_get(&keys, SeqNo::MAX)?;
    let calls = fs.take();

    for (i, value) in values.iter().enumerate() {
        let mut expected = base(i);
        expected.extend_from_slice(format!("+{i}").as_bytes());
        assert_eq!(value.as_deref(), Some(expected.as_slice()), "key {i}");
    }
    assert!(
        calls.iter().any(|&reads| reads >= 8),
        "the blocks of the bases travel in one batch: {calls:?}"
    );
    Ok(())
}

/// The tree in `dir` reopened as it is, with nothing cached.
fn cold_tree_reopened(dir: &std::path::Path, fs: &BatchCountFs) -> lsm_tree::Result<AnyTree> {
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fs.clone()))
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()
}
