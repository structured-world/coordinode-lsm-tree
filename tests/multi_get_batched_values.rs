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

/// A key asked several times in one multi-get reads its separated value once:
/// the batch carries no more reads than the same keys asked once, and every
/// occurrence answers the value.
#[test]
fn a_key_asked_again_reads_its_separated_value_once() -> lsm_tree::Result<()> {
    let once: Vec<String> = (0..4).map(key_for).collect();
    let mut repeated = once.clone();
    repeated.extend(std::iter::repeat_n(key_for(0), 8));
    let mut reads = Vec::new();
    for keys in [&once, &repeated] {
        let dir = tempfile::tempdir()?;
        let fs = BatchCountFs::default();
        let tree = cold_tree(dir.path(), &fs)?;
        fs.take();
        let values = tree.multi_get(keys, SeqNo::MAX)?;
        reads.push(fs.take().iter().sum::<usize>());
        for (key, value) in keys.iter().zip(&values) {
            let i: usize = key.trim_start_matches("key").parse().expect("a key index");
            assert_eq!(value.as_deref(), Some(value_for(i).as_slice()), "{key}");
        }
    }
    assert_eq!(
        reads.first(),
        reads.last(),
        "a repeated key adds no read: {reads:?}"
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

/// Merges whose bases share a block read that block once, however many of
/// them wait on it: the batch carries no more reads than there are blocks.
#[test]
fn merges_sharing_a_base_block_read_it_once() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    {
        let tree = merge_tree(dir.path(), &fs)?;
        // Short bases: all of them fit a few blocks.
        for i in 0..N {
            tree.insert(key_for(i), format!("base-{i}"), i as u64);
        }
        tree.flush_active_memtable(0)?;
        tree.compact(Arc::new(lsm_tree::compaction::MoveDown(0, 1)), 0)?;
        for i in 0..N {
            tree.merge(key_for(i), "+", (N + i) as u64);
        }
        tree.flush_active_memtable(0)?;
    }
    let tree = merge_tree(dir.path(), &fs)?;
    let keys: Vec<String> = (0..N).map(key_for).collect();
    fs.take();

    let values = tree.multi_get(&keys, SeqNo::MAX)?;
    let calls = fs.take();

    for (i, value) in values.iter().enumerate() {
        assert_eq!(
            value.as_deref(),
            Some(format!("base-{i}+").as_bytes()),
            "key {i}"
        );
    }
    assert!(
        calls.iter().all(|&reads| reads <= 4),
        "a shared block is read once, not once per merge: {calls:?}"
    );
    Ok(())
}

/// With values cached as they are read, a second multi-get answers from the
/// cache what the first read from the value log, and reads none of them again.
#[test]
fn a_blob_tree_multi_get_caching_its_values_reads_them_once() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    drop(cold_tree(dir.path(), &fs)?);
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fs.clone()))
    .use_cache(Arc::new(Cache::with_capacity_bytes(64 * 1_024 * 1_024)))
    .blob_cache_fill(lsm_tree::config::BlobCacheFill::Always)
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;
    let keys: Vec<String> = (0..N).map(key_for).collect();
    let first = tree.multi_get(&keys, SeqNo::MAX)?;
    fs.take();

    let second = tree.multi_get(&keys, SeqNo::MAX)?;
    let calls = fs.take();

    assert_eq!(second, first);
    for (i, value) in second.iter().enumerate() {
        assert_eq!(value.as_deref(), Some(value_for(i).as_slice()), "key {i}");
    }
    assert!(
        calls.is_empty(),
        "the values come from the cache: {calls:?}"
    );
    Ok(())
}

/// A blob file that will not open fails a multi-get as it fails a read of a
/// key alone, with the same error.
#[test]
fn a_blob_file_that_will_not_open_fails_a_multi_get_as_a_single_read() -> lsm_tree::Result<()> {
    use lsm_tree::fs::{Fault, FaultFs, FaultOp, FaultRule, StdFs};

    let dir = tempfile::tempdir()?;
    drop(cold_tree(dir.path(), &BatchCountFs::default())?);
    let fault = Arc::new(FaultFs::new(StdFs));
    let injector = fault.injector();
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fault)
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;
    injector.arm(
        FaultRule::new(
            FaultOp::Open,
            Fault::Error(lsm_tree::io::ErrorKind::PermissionDenied),
        )
        .on_path("blobs"),
    );
    let keys: Vec<String> = (0..N).map(key_for).collect();

    let single = tree
        .get(key_for(0), SeqNo::MAX)
        .expect_err("the value's file does not open");
    let batched = tree
        .multi_get(&keys, SeqNo::MAX)
        .expect_err("the values' files do not open");
    injector.clear();

    assert_eq!(
        std::mem::discriminant(&batched),
        std::mem::discriminant(&single),
        "{batched:?} vs {single:?}"
    );
    Ok(())
}

/// A block a merge reads that fails to read hands the merge to the path that
/// resolves a key alone, which reads it again: the merge still answers.
#[test]
fn a_merge_whose_block_fails_to_read_answers_on_its_own_path() -> lsm_tree::Result<()> {
    use lsm_tree::fs::{Fault, FaultFs, FaultOp, FaultRule, StdFs};

    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    {
        let tree = merge_tree(dir.path(), &fs)?;
        for i in 0..N {
            tree.insert(key_for(i), format!("base-{i}"), i as u64);
        }
        tree.flush_active_memtable(0)?;
        tree.compact(Arc::new(lsm_tree::compaction::MoveDown(0, 1)), 0)?;
        for i in 0..N {
            tree.merge(key_for(i), "+", (N + i) as u64);
        }
        tree.flush_active_memtable(0)?;
    }
    // The bases' table: written first, so it sorts first.
    let tables = common::sorted_sst_paths(dir.path());
    let base = tables
        .first()
        .and_then(|path| path.file_name())
        .and_then(|name| name.to_str())
        .expect("the bases' table")
        .to_owned();
    let first_block = first_data_block_offset(tables.first().expect("the bases' table"))?;
    let fault = Arc::new(FaultFs::new(StdFs));
    let injector = fault.injector();
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fault)
    .use_cache(Arc::new(Cache::with_capacity_bytes(64 * 1_024 * 1_024)))
    .with_merge_operator(Some(Arc::new(Concat)))
    .open()?;
    // The first data block of the bases' table, which holds the first key's
    // base: only that merge's block read reaches it, since the levels stop at
    // the operands and locating a key reads the table's index, not its data.
    injector.arm(
        FaultRule::new(
            FaultOp::ReadAt,
            Fault::Error(lsm_tree::io::ErrorKind::Other),
        )
        .on_path(format!("tables/{base}"))
        .at_offset(first_block)
        .once(),
    );
    let keys: Vec<String> = (0..N).map(key_for).collect();

    let values = tree.multi_get(&keys, SeqNo::MAX)?;
    injector.clear();

    for (i, value) in values.iter().enumerate() {
        assert_eq!(
            value.as_deref(),
            Some(format!("base-{i}+").as_bytes()),
            "key {i}"
        );
    }
    Ok(())
}

/// Where the first data block of the table at `path` starts: at the start of
/// its `data` section.
fn first_data_block_offset(path: &std::path::Path) -> lsm_tree::Result<u64> {
    let mut file = std::fs::File::open(path)?;
    let reader = lsm_tree::sfa::Reader::from_reader(&mut file)
        .map_err(|error| std::io::Error::other(format!("read the table's TOC: {error}")))?;
    let data = reader
        .toc()
        .iter()
        .find(|entry| entry.name() == b"data")
        .expect("a table carries a data section");
    Ok(data.pos())
}

/// A merge over a key whose retained history spans many blocks of its base's
/// table reads that table's blocks of the key newest first and stops at the
/// first base, as a read of the key alone stops there: the history beneath it
/// is hidden by it and never read.
#[test]
fn a_merge_stops_reading_its_base_table_at_the_first_base() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    let config = || {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::new(fs.clone()))
        .data_block_size_policy(lsm_tree::config::BlockSizePolicy::all(512))
        .with_merge_operator(Some(Arc::new(Concat)))
    };
    let version = |seqno: u64| format!("{seqno:0>100}");
    {
        let tree = config().open()?;
        // A flush at watermark 0 keeps every version: about forty blocks of
        // history under the newest.
        for seqno in 1..=200 {
            tree.insert("key", version(seqno), seqno);
        }
        // Two more merged keys: a batch of two keys or fewer is read key by
        // key, not through the batch.
        tree.insert("a1", "base-a1", 201);
        tree.insert("a2", "base-a2", 202);
        tree.flush_active_memtable(0)?;
        tree.compact(Arc::new(lsm_tree::compaction::MoveDown(0, 1)), 0)?;
        for (seqno, key) in (203..).zip(["a1", "a2", "key"]) {
            tree.merge(key, "+", seqno);
        }
        tree.flush_active_memtable(0)?;
    }
    let tree = config().open()?;
    fs.take();

    let values = tree.multi_get(["a1", "a2", "key"], SeqNo::MAX)?;
    let calls = fs.take();

    let mut expected = version(200).into_bytes();
    expected.push(b'+');
    assert_eq!(
        values,
        vec![
            Some(UserValue::from("base-a1+")),
            Some(UserValue::from("base-a2+")),
            Some(UserValue::from(expected)),
        ]
    );
    // The operands' block, and at most one block of the bases' table per
    // key: none of the history.
    assert!(
        calls.iter().sum::<usize>() <= 4,
        "the history beneath the newest base is not read: {calls:?}"
    );
    Ok(())
}

/// The blocks merges read are brought in within half the cache at a time, as
/// a level's chunks are: a cold multi-get over many merges with a small cache
/// never holds more of their buffers in flight than that, and still merges
/// exactly as key-by-key reads do.
#[test]
fn merge_block_reads_stay_within_half_the_cache() -> lsm_tree::Result<()> {
    const CACHE: u64 = 16 * 1_024;
    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    let config = |cache: u64| {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::new(fs.clone()))
        .data_block_size_policy(lsm_tree::config::BlockSizePolicy::all(1_024))
        .use_cache(Arc::new(Cache::with_capacity_bytes(cache)))
        .with_merge_operator(Some(Arc::new(Concat)))
    };
    // A kilobyte of noise per base, which no compression shrinks: a block of
    // a kilobyte each, on disk too.
    let base = |i: usize| {
        let mut base = format!("base-{i:04}-").into_bytes();
        let mut state = i as u64 + 1;
        while base.len() < 1_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            base.push((state >> 56) as u8);
        }
        base
    };
    {
        let tree = config(64 * 1_024 * 1_024).open()?;
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
    let tree = config(CACHE).open()?;
    let keys: Vec<String> = (0..N).map(key_for).collect();
    fs.take();

    let values = tree.multi_get(&keys, SeqNo::MAX)?;
    let calls = fs.take();

    for (i, value) in values.iter().enumerate() {
        let mut expected = base(i);
        expected.extend_from_slice(format!("+{i}").as_bytes());
        assert_eq!(value.as_deref(), Some(expected.as_slice()), "key {i}");
    }
    // Half the cache holds at most eight blocks of a kilobyte or more.
    assert!(
        calls.iter().all(|&reads| reads <= 8),
        "no call carries more than half the cache: {calls:?}"
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
