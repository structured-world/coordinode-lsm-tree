// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::config::{BlockSizePolicy, CompressionPolicy, RestartIntervalPolicy};
use crate::{AbstractTree, CompressionType, Config, Guard, SeqNo, SequenceNumberCounter};

const KEYS: u64 = 2_000;

fn key(i: u64) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}

fn value(i: u64, version: u64) -> Vec<u8> {
    format!("v{i}-{version}-row-major-payload").into_bytes()
}

/// A row-major tree of small data blocks, every level written without
/// compression unless `configure` says otherwise.
fn tree(
    folder: &std::path::Path,
    configure: impl FnOnce(Config) -> Config,
) -> crate::Result<crate::Tree> {
    let config = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512));
    let crate::AnyTree::Standard(tree) = configure(config).open()? else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.data_block_compression_policy = CompressionPolicy::all(CompressionType::None);
    })?;
    Ok(tree)
}

/// Every key once at seqno 1, flushed into one table.
fn write_base(tree: &crate::Tree) -> crate::Result<()> {
    for i in 0..KEYS {
        tree.insert(key(i), value(i, 0), 1);
    }
    tree.flush_active_memtable(0)?;
    Ok(())
}

/// A newer version of the first ten keys, flushed into a second table, so
/// a merge of the two changes only the blocks around the start.
fn write_head(tree: &crate::Tree, seqno: SeqNo) -> crate::Result<()> {
    for i in 0..10 {
        tree.insert(key(i), value(i, 1), seqno);
    }
    tree.flush_active_memtable(0)?;
    Ok(())
}

/// Every key reads back with its newest value, by point read, forwards and
/// backwards.
fn assert_contents(tree: &crate::Tree, newer: u64) -> crate::Result<()> {
    let expected = |i: u64| value(i, u64::from(i < newer));
    for i in 0..KEYS {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX)?.as_deref(),
            Some(expected(i).as_slice()),
            "key {i}"
        );
    }
    assert!(tree.get(b"k99999", SeqNo::MAX)?.is_none());
    let forward = tree
        .iter(SeqNo::MAX, None)
        .map(Guard::into_inner)
        .collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(forward.len() as u64, KEYS);
    for (i, (k, v)) in (0..KEYS).zip(&forward) {
        assert_eq!(k.as_ref(), key(i).as_slice());
        assert_eq!(v.as_ref(), expected(i).as_slice());
    }
    let backward = tree
        .iter(SeqNo::MAX, None)
        .rev()
        .map(Guard::into_inner)
        .collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(backward.len() as u64, KEYS);
    assert_eq!(
        backward.first().map(|(k, _)| k.to_vec()),
        Some(key(KEYS - 1))
    );
    Ok(())
}

/// The merge leaves every block past the first few as it read them, so
/// those are copied, and the output reads back exactly as the inputs did.
#[test]
fn unchanged_blocks_merge_copies_them_and_reads_back() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = tree(folder.path(), |c| c)?;
    write_base(&tree)?;
    write_head(&tree, 2)?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    let carried = tree.metrics().compaction_blocks_carried();
    assert!(carried > 0, "no block copied");
    assert!(tree.metrics().compaction_bytes_carried() > 0);
    assert_contents(&tree, 10)
}

/// A table merged alone into the bottom keeps every block.
#[test]
fn untouched_table_merge_copies_every_block() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = tree(folder.path(), |c| c)?;
    write_base(&tree)?;
    let blocks = tree
        .current_version()
        .iter_tables()
        .map(|table| table.metadata.data_block_count)
        .sum::<u64>();

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert_eq!(tree.metrics().compaction_blocks_carried(), blocks);
    assert_contents(&tree, 0)
}

/// Blocks carrying the per-KV footer are copied into an output that keeps
/// it, and refused by one that does not.
#[test]
fn kv_footer_merge_copies_only_when_the_output_keeps_it() -> crate::Result<()> {
    use crate::runtime_config::KvChecksumPolicy;

    for (source, output, copies) in [
        (
            KvChecksumPolicy::AllLevels,
            KvChecksumPolicy::AllLevels,
            true,
        ),
        (KvChecksumPolicy::Off, KvChecksumPolicy::AllLevels, false),
        (KvChecksumPolicy::AllLevels, KvChecksumPolicy::Off, false),
    ] {
        let folder = crate::get_tmp_folder();
        let tree = tree(folder.path(), |c| c)?;
        tree.update_runtime_config(|c| c.kv_checksums = source)?;
        write_base(&tree)?;
        write_head(&tree, 2)?;
        tree.update_runtime_config(|c| c.kv_checksums = output)?;

        tree.major_compact(64 * 1024 * 1024, 0)?;

        assert_eq!(
            tree.metrics().compaction_blocks_carried() > 0,
            copies,
            "{source:?} into {output:?}"
        );
        assert_contents(&tree, 10)?;
    }
    Ok(())
}

/// An output whose blocks restart at another interval writes every row:
/// the locator a block's restarts feed would not match a copy.
#[test]
fn other_restart_interval_merge_copies_nothing() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = tree(folder.path(), |c| {
        c.data_block_restart_interval_policy(RestartIntervalPolicy::new([16, 4]))
    })?;
    write_base(&tree)?;
    write_head(&tree, 2)?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert_eq!(tree.metrics().compaction_blocks_carried(), 0);
    assert_contents(&tree, 10)
}

/// Blocks carrying a hash index are copied into an output of the same ratio,
/// and point reads through the copies' hash indexes find every key; an output
/// of another ratio writes every row, since a table records one ratio for all
/// its blocks.
#[test]
fn hash_indexed_blocks_merge_copies_only_under_the_same_ratio() -> crate::Result<()> {
    use crate::config::HashRatioPolicy;

    for (policy, copies) in [
        (HashRatioPolicy::all(2.0), true),
        (HashRatioPolicy::new([2.0, 0.0]), false),
        (HashRatioPolicy::new([0.0, 2.0]), false),
    ] {
        let folder = crate::get_tmp_folder();
        let tree = tree(folder.path(), |c| {
            c.data_block_hash_ratio_policy(policy.clone())
        })?;
        write_base(&tree)?;
        write_head(&tree, 2)?;

        tree.major_compact(64 * 1024 * 1024, 0)?;

        assert_eq!(
            tree.metrics().compaction_blocks_carried() > 0,
            copies,
            "{policy:?}"
        );
        assert_contents(&tree, 10)?;
    }
    Ok(())
}

/// An output compressed otherwise than the inputs writes every row.
#[cfg(feature = "lz4")]
#[test]
fn other_codec_merge_copies_nothing() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = tree(folder.path(), |c| c)?;
    write_base(&tree)?;
    write_head(&tree, 2)?;
    tree.update_runtime_config(|c| {
        c.data_block_compression_policy = CompressionPolicy::all(CompressionType::Lz4);
    })?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert_eq!(tree.metrics().compaction_blocks_carried(), 0);
    assert_contents(&tree, 10)
}

/// Compressed blocks are copied as compressed into an output of the same
/// codec.
#[cfg(feature = "lz4")]
#[test]
fn compressed_blocks_merge_copies_them_as_compressed() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = tree(folder.path(), |c| c)?;
    tree.update_runtime_config(|c| {
        c.data_block_compression_policy = CompressionPolicy::all(CompressionType::Lz4);
    })?;
    write_base(&tree)?;
    write_head(&tree, 2)?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert!(tree.metrics().compaction_blocks_carried() > 0);
    assert_contents(&tree, 10)
}

/// ECC-protected outputs carry parity a copied plain block lacks, so
/// nothing is copied.
#[cfg(feature = "page_ecc")]
#[test]
fn page_ecc_merge_copies_nothing() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = tree(folder.path(), |c| c.page_ecc(true))?;
    write_base(&tree)?;
    write_head(&tree, 2)?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert_eq!(tree.metrics().compaction_blocks_carried(), 0);
    assert_contents(&tree, 10)
}

/// Deleted keys change only the blocks holding them: the rest are copied,
/// and the deleted keys stay deleted.
#[test]
fn deleted_keys_merge_copies_the_other_blocks() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = tree(folder.path(), |c| c)?;
    write_base(&tree)?;
    for i in 1_000..1_010 {
        tree.remove(key(i), 2);
    }
    tree.flush_active_memtable(0)?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert!(tree.metrics().compaction_blocks_carried() > 0);
    for i in 0..KEYS {
        let read = tree.get(key(i), SeqNo::MAX)?;
        if (1_000..1_010).contains(&i) {
            assert!(read.is_none(), "key {i} resurrected");
        } else {
            assert_eq!(read.as_deref(), Some(value(i, 0).as_slice()), "key {i}");
        }
    }
    assert_eq!(tree.iter(SeqNo::MAX, None).count() as u64, KEYS - 10);
    Ok(())
}

/// A merge that drops superseded versions and zeroes old seqnos rewrites
/// what it changes; whatever it copies, every key reads back right.
#[test]
fn garbage_collecting_merge_reads_back() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = tree(folder.path(), |c| c)?;
    write_base(&tree)?;
    write_head(&tree, 2)?;

    tree.major_compact(64 * 1024 * 1024, SeqNo::MAX)?;

    assert_contents(&tree, 10)?;
    // Only the newest version of a rewritten key is left.
    let stored = tree
        .current_version()
        .iter_tables()
        .map(|table| table.metadata.item_count)
        .sum::<u64>();
    assert_eq!(stored, KEYS);
    Ok(())
}

/// A merge split into ranges run in parallel copies blocks in each.
#[test]
fn subcompaction_merge_copies_blocks() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = tree(folder.path(), |c| {
        c.compaction_threads(4).subcompaction_min_bytes(0)
    })?;
    write_base(&tree)?;
    // Several bottom tables, so the next merge has ranges to split at.
    tree.major_compact(8 * 1024, 0)?;
    assert!(tree.current_version().iter_tables().count() > 2);
    let before = tree.metrics().compaction_blocks_carried();
    write_head(&tree, 2)?;
    for i in (KEYS - 10)..KEYS {
        tree.insert(key(i), value(i, 0), 2);
    }
    tree.flush_active_memtable(0)?;

    tree.major_compact(8 * 1024, 0)?;

    assert!(tree.metrics().compaction_blocks_carried() > before);
    assert_contents(&tree, 10)
}

/// Blocks ordered by a custom comparator are checked and placed by it.
#[test]
fn custom_comparator_merge_copies_blocks() -> crate::Result<()> {
    struct ReverseComparator;
    impl crate::comparator::UserComparator for ReverseComparator {
        fn name(&self) -> &'static str {
            "reverse-lexicographic-test"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
            b.cmp(a)
        }
    }

    let folder = crate::get_tmp_folder();
    let tree = tree(folder.path(), |c| {
        c.comparator(std::sync::Arc::new(ReverseComparator))
    })?;
    write_base(&tree)?;
    write_head(&tree, 2)?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert!(tree.metrics().compaction_blocks_carried() > 0);
    let keys = tree
        .iter(SeqNo::MAX, None)
        .map(Guard::key)
        .collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(keys.len() as u64, KEYS);
    assert_eq!(keys.first().map(|k| k.to_vec()), Some(key(KEYS - 1)));
    for i in 0..KEYS {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX)?.as_deref(),
            Some(value(i, u64::from(i < 10)).as_slice()),
            "key {i}"
        );
    }
    Ok(())
}
