// A multi-get over Page-ECC and columnar tables reads their data blocks in the
// level's batch, as it reads a plain table's, checking and rebuilding each from
// the bytes read, and answers exactly what key-by-key reads answer.

#![cfg(any(feature = "page_ecc", feature = "columnar"))]

mod common;

use common::BatchCountFs;
use lsm_tree::{AbstractTree, AnyTree, Cache, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;

const ROWS: u32 = 2_000;
const CACHE: u64 = 64 * 1_024 * 1_024;

fn key_for(row: u32) -> String {
    format!("k{row:05}")
}

/// Distinct per row, so a block rebuilt wrong cannot answer right.
fn value_for(row: u32) -> Vec<u8> {
    let mut value = format!("value-{row:05}-").into_bytes();
    value.resize(64, b'a');
    value
}

/// One key per fifty rows: spread over the table's data blocks.
fn keys() -> Vec<String> {
    (0..ROWS).step_by(50).map(key_for).collect()
}

/// Writes [`ROWS`] rows into `tree` and flushes them into one table.
fn fill(tree: &AnyTree) -> lsm_tree::Result<()> {
    for row in 0..ROWS {
        tree.insert(key_for(row), value_for(row), u64::from(row));
    }
    tree.flush_active_memtable(0)?;
    Ok(())
}

/// Every key of [`keys`] answers its own value.
fn assert_answers(values: &[Option<lsm_tree::Slice>]) {
    for (key, value) in keys().iter().zip(values) {
        let row: u32 = key
            .get(1..)
            .and_then(|row| row.parse().ok())
            .expect("a row");
        assert_eq!(value.as_deref(), Some(value_for(row).as_slice()), "{key}");
    }
}

/// A Page-ECC tree in `dir` on `fs`, with a cache that holds every block.
#[cfg(feature = "page_ecc")]
fn ecc_tree(dir: &std::path::Path, fs: &BatchCountFs) -> lsm_tree::Result<AnyTree> {
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fs.clone()))
    .use_cache(Arc::new(Cache::with_capacity_bytes(CACHE)))
    .page_ecc(true)
    .ecc_scheme(lsm_tree::runtime_config::EccScheme::Secded)
    .open()
}

/// A cold multi-get over a Page-ECC table reads its data blocks in one batch
/// rather than one load each.
#[cfg(feature = "page_ecc")]
#[test]
fn a_page_ecc_level_reads_its_data_blocks_in_one_batch() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    fill(&ecc_tree(dir.path(), &fs)?)?;
    let tree = ecc_tree(dir.path(), &fs)?;
    fs.take();

    let values = tree.multi_get(keys(), SeqNo::MAX)?;
    let calls = fs.take();

    assert_answers(&values);
    assert!(
        calls.iter().any(|&reads| reads >= keys().len() / 2),
        "the data blocks travel in one batch: {calls:?}"
    );
    Ok(())
}

/// A Page-ECC table's blocks the first multi-get kept in the cache serve the
/// next one from there: a warm multi-get reads nothing.
#[cfg(feature = "page_ecc")]
#[test]
fn a_warm_page_ecc_level_reads_nothing() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    fill(&ecc_tree(dir.path(), &fs)?)?;
    let tree = ecc_tree(dir.path(), &fs)?;
    assert_answers(&tree.multi_get(keys(), SeqNo::MAX)?);
    fs.take();

    let values = tree.multi_get(keys(), SeqNo::MAX)?;
    let calls = fs.take();

    assert_answers(&values);
    assert!(calls.is_empty(), "a warm level reads nothing: {calls:?}");
    Ok(())
}

/// A merge over bases in a Page-ECC table reads that table's versions through
/// its own reader, which recovers and checks its blocks, and answers what a
/// read of each key alone answers.
#[cfg(feature = "page_ecc")]
#[test]
fn merges_over_a_page_ecc_table_answer_as_single_reads() -> lsm_tree::Result<()> {
    /// Appends every operand to the base.
    struct Concat;

    impl lsm_tree::MergeOperator for Concat {
        fn merge(
            &self,
            _key: &[u8],
            base: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> lsm_tree::Result<lsm_tree::UserValue> {
            let mut merged = base.unwrap_or_default().to_vec();
            for operand in operands {
                merged.extend_from_slice(operand);
            }
            Ok(merged.into())
        }
    }

    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    let open = || {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::new(fs.clone()))
        .use_cache(Arc::new(Cache::with_capacity_bytes(CACHE)))
        .page_ecc(true)
        .ecc_scheme(lsm_tree::runtime_config::EccScheme::Secded)
        .with_merge_operator(Some(Arc::new(Concat)))
        .open()
    };
    {
        let tree = open()?;
        fill(&tree)?;
        tree.compact(Arc::new(lsm_tree::compaction::MoveDown(0, 1)), 0)?;
        for key in keys() {
            tree.merge(key, "+", u64::from(ROWS) + 1);
        }
        tree.flush_active_memtable(0)?;
    }
    let tree = open()?;

    let values = tree.multi_get(keys(), SeqNo::MAX)?;

    for (key, value) in keys().iter().zip(&values) {
        let row: u32 = key
            .get(1..)
            .and_then(|row| row.parse().ok())
            .expect("a row");
        let mut expected = value_for(row);
        expected.push(b'+');
        assert_eq!(value.as_deref(), Some(expected.as_slice()), "{key}");
    }
    Ok(())
}

/// A block whose payload Page-ECC has to correct answers what a read of the
/// key alone answers: the batch hands it to the load path, which recovers it
/// and schedules its heal.
#[cfg(feature = "page_ecc")]
#[test]
fn a_block_page_ecc_corrects_answers_as_a_single_read() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    fill(&ecc_tree(dir.path(), &fs)?)?;
    let tables = common::sorted_sst_paths(dir.path());
    let table = tables.first().expect("a table");
    common::corrupt_data_region_with(table, 0x01)?;

    let single: Vec<_> = {
        let tree = ecc_tree(dir.path(), &fs)?;
        keys()
            .iter()
            .map(|key| tree.get(key, SeqNo::MAX))
            .collect::<lsm_tree::Result<_>>()?
    };
    let batched = ecc_tree(dir.path(), &fs)?.multi_get(keys(), SeqNo::MAX)?;

    assert_eq!(batched, single);
    assert_answers(&batched);
    Ok(())
}

/// A columnar tree in `dir` on `fs` whose row groups and pages are cut at
/// 4 KiB, so a table holds many.
#[cfg(feature = "columnar")]
fn columnar_tree(dir: &std::path::Path, fs: &BatchCountFs) -> lsm_tree::Result<AnyTree> {
    use lsm_tree::config::BlockSizePolicy;

    let tree = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fs.clone()))
    .use_cache(Arc::new(Cache::with_capacity_bytes(CACHE)))
    .columnar_row_group_size_policy(BlockSizePolicy::all(4 * 1_024))
    .columnar_page_size_policy(BlockSizePolicy::all(4 * 1_024))
    .open()?;
    if let AnyTree::Standard(standard) = &tree {
        standard.update_runtime_config(|cfg| cfg.columnar = true)?;
    }
    Ok(tree)
}

/// A cold multi-get over a columnar table reads its row groups in one batch,
/// each whole in one request, rather than one load each.
#[cfg(feature = "columnar")]
#[test]
fn a_columnar_level_reads_its_row_groups_in_one_batch() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    fill(&columnar_tree(dir.path(), &fs)?)?;
    let tree = columnar_tree(dir.path(), &fs)?;
    fs.take();

    let values = tree.multi_get(keys(), SeqNo::MAX)?;
    let calls = fs.take();

    assert_answers(&values);
    let single: Vec<_> = keys()
        .iter()
        .map(|key| tree.get(key, SeqNo::MAX))
        .collect::<lsm_tree::Result<_>>()?;
    assert_eq!(values, single);
    assert!(
        calls.iter().any(|&reads| reads >= keys().len() / 2),
        "the row groups travel in one batch: {calls:?}"
    );
    Ok(())
}

/// A columnar group whose directory the cache holds is loaded through its
/// own path, which takes its pages from the cache: a warm multi-get answers
/// what the cold one did.
#[cfg(feature = "columnar")]
#[test]
fn a_warm_columnar_level_answers_from_its_cached_groups() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = BatchCountFs::default();
    fill(&columnar_tree(dir.path(), &fs)?)?;
    let tree = columnar_tree(dir.path(), &fs)?;
    let cold = tree.multi_get(keys(), SeqNo::MAX)?;
    fs.take();

    let warm = tree.multi_get(keys(), SeqNo::MAX)?;
    let calls = fs.take();

    assert_eq!(warm, cold);
    assert_answers(&warm);
    assert!(calls.is_empty(), "a warm level reads nothing: {calls:?}");
    Ok(())
}
