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
