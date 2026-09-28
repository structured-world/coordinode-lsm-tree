// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::{
    AbstractTree, Config, SequenceNumberCounter, Tree,
    config::{BlockSizePolicy, PinningPolicy},
};

const KEYS: u32 = 100_000;

fn key(i: u32) -> String {
    format!("key{i:08}")
}

/// How the table under test is written.
#[derive(Clone, Copy, Debug)]
enum Path {
    Flush,
    Compaction,
    Ingestion,
    /// A flush of a tree separating keys from values.
    BlobFlush,
}

/// Bytes of the top-level indexes of the one table `path` writes from `KEYS`
/// keys under the given partition sizes: the block index's and the filter's,
/// each an entry per partition.
fn top_level_index_bytes(
    path: Path,
    index_partition: u32,
    filter_partition: u32,
) -> crate::Result<(u64, u64)> {
    let folder = tempfile::tempdir()?;
    let config = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .index_block_partitioning_policy(PinningPolicy::all(true))
    .filter_block_partitioning_policy(PinningPolicy::all(true))
    .index_block_partition_size_policy(BlockSizePolicy::all(index_partition))
    .filter_block_partition_size_policy(BlockSizePolicy::all(filter_partition));
    let any = if matches!(path, Path::BlobFlush) {
        config
            .with_kv_separation(Some(crate::KvSeparationOptions::default()))
            .open()?
    } else {
        config.open()?
    };
    let tree = match &any {
        crate::AnyTree::Standard(tree) => tree,
        crate::AnyTree::Blob(blob) => &blob.index,
    };
    // Partition the block index from its first entry.
    tree.update_runtime_config(|c| c.index_partition_spill_threshold = 0)?;
    if matches!(path, Path::BlobFlush) {
        for i in 0..KEYS {
            any.insert(key(i), b"value", u64::from(i));
        }
        any.flush_active_memtable(0)?;
    } else {
        write(tree, path)?;
    }

    let version = tree.current_version();
    let mut tables = version.iter_tables();
    let table = tables.next().expect("a table was written");
    assert!(tables.next().is_none(), "{path:?} wrote one table");
    let filter_tli = table.regions.filter_tli.expect("a partitioned filter");
    Ok((
        u64::from(table.regions.tli.size()),
        u64::from(filter_tli.size()),
    ))
}

fn write(tree: &Tree, path: Path) -> crate::Result<()> {
    match path {
        Path::Flush | Path::Compaction => {
            for i in 0..KEYS {
                tree.insert(key(i), b"value", u64::from(i));
            }
            tree.flush_active_memtable(0)?;
            if matches!(path, Path::Compaction) {
                tree.major_compact(u64::MAX, u64::from(KEYS))?;
            }
        }
        Path::Ingestion => {
            let mut ingestion = super::ingest::Ingestion::new(tree)?;
            for i in 0..KEYS {
                ingestion.write(key(i).into(), b"value".into())?;
            }
            ingestion.finish()?;
        }
        Path::BlobFlush => unreachable!("written through the blob tree"),
    }
    Ok(())
}

/// The configured partition sizes reach the table writers of a flush, a
/// compaction, an ingestion and a blob tree's flush, each on its own: four
/// times the filter partition size gives about a quarter of the filter
/// partitions, a larger index partition size gives fewer index partitions,
/// and neither size moves the other's index. The block index here holds only
/// a few partitions, so its top-level index shrinks by fewer entries.
#[test]
fn the_configured_partition_sizes_reach_the_writers() -> crate::Result<()> {
    for path in [
        Path::Flush,
        Path::Compaction,
        Path::Ingestion,
        Path::BlobFlush,
    ] {
        let (index_default, filter_default) = top_level_index_bytes(path, 4_096, 4_096)?;
        let (index_same, filter_large) = top_level_index_bytes(path, 4_096, 16_384)?;
        let (index_large, filter_same) = top_level_index_bytes(path, 16_384, 4_096)?;

        assert!(
            filter_large * 3 < filter_default,
            "{path:?}: a filter top-level index of {filter_large} bytes under 16 KiB \
             partitions, {filter_default} under 4 KiB",
        );
        assert!(
            index_large < index_default,
            "{path:?}: a block top-level index of {index_large} bytes under 16 KiB \
             partitions, {index_default} under 4 KiB",
        );
        assert_eq!(
            index_same, index_default,
            "{path:?}: the filter size leaves the index"
        );
        assert_eq!(
            filter_same, filter_default,
            "{path:?}: the index size leaves the filter"
        );
    }
    Ok(())
}
