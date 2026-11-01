// A filter or an index the block cache could never keep is written in pieces
// it can, so a read does not fetch the whole block from the file every time.
// The layout is chosen from the cache the table is written under; what fits
// stays one block.
#![cfg(feature = "metrics")]

use lsm_tree::{
    AbstractTree, AnyTree, Cache, Config, Guard, KvSeparationOptions, PrefixExtractor,
    SequenceNumberCounter, config::PinningPolicy, get_tmp_folder,
};
use std::sync::Arc;

struct FirstFour;

impl PrefixExtractor for FirstFour {
    fn name(&self) -> &'static str {
        "first-four"
    }

    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(key.get(..4).into_iter())
    }
}

/// Enough keys for a filter and an index far past a shard of [`SMALL`].
const KEYS: u64 = 200_000;
const READS: u64 = 500;
/// A shard of 8 KiB: every whole block of [`KEYS`] keys is refused.
const SMALL: u64 = 512 * 1024;
/// A shard of 8 MiB: every whole block of [`KEYS`] keys fits.
const LARGE: u64 = 512 * 1024 * 1024;

fn key(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

fn config(folder: &tempfile::TempDir, cache: u64) -> Config {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(Arc::new(Cache::with_capacity_bytes(cache)))
    .table_target_size(u64::MAX / 2)
    .filter_block_pinning_policy(PinningPolicy::all(false))
    .index_block_pinning_policy(PinningPolicy::all(false))
    .filter_block_partitioning_policy(PinningPolicy::all(false))
}

fn fill(tree: &AnyTree) -> lsm_tree::Result<()> {
    for i in 0..KEYS {
        tree.insert(key(i), b"v", 0);
    }
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.table_count(), 1);
    Ok(())
}

fn read_all(tree: &AnyTree) -> lsm_tree::Result<()> {
    for i in 0..READS {
        let at = key(i * (KEYS / READS));
        assert!(tree.get(at, 1)?.is_some(), "a written key");
        let absent = [at.as_slice(), &[0]].concat();
        assert!(tree.get(absent, 1)?.is_none(), "a key never written");
    }
    Ok(())
}

/// Bytes of filter one pass of [`read_all`] reads from the file.
fn filter_bytes_read(tree: &AnyTree) -> lsm_tree::Result<u64> {
    let before = tree.metrics().filter_block_io();
    read_all(tree)?;
    Ok(tree.metrics().filter_block_io() - before)
}

#[test]
fn a_filter_no_shard_can_keep_is_written_in_pieces() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = config(&folder, SMALL).open()?;
    fill(&tree)?;

    read_all(&tree)?;
    let whole = tree.filter_size();
    let again = filter_bytes_read(&tree)?;
    assert!(
        again < whole,
        "a pass read {again} bytes of a {whole} byte filter: no whole block"
    );
    assert_eq!(
        tree.metrics().block_cache_refused_count(),
        0,
        "every piece fits a shard"
    );
    Ok(())
}

#[test]
fn a_filter_a_shard_can_keep_stays_one_block() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = config(&folder, LARGE).open()?;
    fill(&tree)?;

    let first = filter_bytes_read(&tree)?;
    let metrics = tree.metrics();
    assert_eq!(
        metrics.filter_block_load_count() - metrics.filter_block_load_cached_count(),
        1,
        "one block, read once"
    );
    assert!(first >= tree.filter_size());
    assert_eq!(filter_bytes_read(&tree)?, 0, "the cache keeps it");
    Ok(())
}

/// Only a full filter answers prefix queries, so the extractor's filter stays
/// whole even when no shard keeps it, and the refusal shows in the counter.
#[test]
fn a_prefix_filter_stays_whole_and_its_refusals_are_counted() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = config(&folder, SMALL)
        .prefix_extractor(Arc::new(FirstFour))
        .open()?;
    fill(&tree)?;

    read_all(&tree)?;
    assert!(tree.metrics().block_cache_refused_count() >= READS as usize);
    assert!(tree.metrics().block_cache_refused_bytes() >= READS * tree.filter_size());
    Ok(())
}

/// A pinned block never goes through the cache, so it stays whole however
/// small the cache is.
#[test]
fn a_pinned_filter_stays_whole() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = config(&folder, SMALL)
        .filter_block_pinning_policy(PinningPolicy::all(true))
        .open()?;
    fill(&tree)?;

    assert!(
        tree.pinned_filter_size() > 0,
        "one block, held by the table"
    );
    assert_eq!(filter_bytes_read(&tree)?, 0);
    Ok(())
}

#[test]
fn an_index_no_shard_can_keep_is_written_in_pieces() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = config(&folder, SMALL).open()?;
    fill(&tree)?;

    for i in 0..READS {
        let at = key(i * (KEYS / READS));
        let row = tree.range(at.., 1, None).next().map(Guard::into_inner);
        let (found, _) = row.expect("a written key")?;
        assert_eq!(&*found, at.as_slice());
    }
    assert_eq!(tree.metrics().block_cache_refused_count(), 0);
    Ok(())
}

/// The blob tree's index tables take the layout a plain tree's do.
#[test]
fn a_blob_trees_index_tables_are_written_in_pieces_too() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = config(&folder, SMALL)
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .open()?;
    fill(&tree)?;

    read_all(&tree)?;
    assert_eq!(tree.metrics().block_cache_refused_count(), 0);
    Ok(())
}

/// An ingested table is an L0 run, so the L0 policy decides: pinned there by
/// default, its filter stays one block however small the cache.
#[test]
fn an_ingested_table_takes_the_layout_of_l0() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = config(&folder, SMALL)
        .filter_block_pinning_policy(PinningPolicy::new([true, false]))
        .open()?;
    let mut ingestion = tree.ingestion()?;
    for i in 0..KEYS {
        ingestion.write(key(i), b"v")?;
    }
    ingestion.finish()?;

    assert!(
        tree.pinned_filter_size() > 0,
        "one block, held by the table"
    );
    Ok(())
}

/// A table written by compaction takes the layout of its level.
#[test]
fn a_compaction_output_is_written_in_pieces_too() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = config(&folder, SMALL).open()?;
    fill(&tree)?;
    tree.major_compact(u64::MAX / 2, 1)?;
    assert_eq!(tree.table_count(), 1);

    read_all(&tree)?;
    assert_eq!(tree.metrics().block_cache_refused_count(), 0);
    Ok(())
}
