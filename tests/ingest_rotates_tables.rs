use lsm_tree::{AbstractTree, Config, SeqNo, SequenceNumberCounter, get_tmp_folder};
#[cfg(feature = "lz4")]
use lsm_tree::{CompressionType, PrefixExtractor, config::CompressionPolicy};
#[cfg(feature = "lz4")]
use std::sync::Arc;
use test_log::test;

/// The table size an ingestion rotates at.
const TARGET: u64 = 64 * 1_024 * 1_024;

/// Deterministic incompressible bytes, so the table's size on disk is the
/// size of what was written.
fn noise(seed: u64, out: &mut [u8]) {
    let mut state = seed;
    for chunk in out.chunks_mut(8) {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        chunk.copy_from_slice(&z.to_le_bytes()[..chunk.len()]);
    }
}

/// Every prefix of a key that ends at a colon.
#[cfg(feature = "lz4")]
struct ColonPrefixes;

#[cfg(feature = "lz4")]
impl PrefixExtractor for ColonPrefixes {
    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(
            key.iter()
                .enumerate()
                .filter(|(_, b)| **b == b':')
                .map(move |(i, _)| &key[..=i]),
        )
    }
}

/// Ingests `rows` rows of 21-byte keys and 256-byte values, `value` filling
/// row `i`'s value, in one ingestion.
fn ingest(
    tree: &lsm_tree::AnyTree,
    rows: u64,
    value: impl Fn(u64, &mut [u8]),
) -> lsm_tree::Result<()> {
    let mut ingestion = tree.ingestion()?;
    let mut buf = [0u8; 256];
    for i in 0..rows {
        let key = format!("node:{i:016}");
        value(i, &mut buf);
        ingestion.write(key.as_bytes(), buf.as_slice())?;
    }
    ingestion.finish()?;
    assert_eq!(tree.len(SeqNo::MAX, None)?, rows as usize);
    Ok(())
}

/// One ingestion several times the rotation target must be cut into tables of
/// at most the target: an ingestion that keeps writing one table holds the
/// whole volume until it finishes, and its memory grows with what is ingested.
#[test]
fn one_large_ingestion_is_cut_into_tables_of_the_target_size() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    // About 3.5 times the target of incompressible rows.
    ingest(&tree, 850_000, noise)?;
    assert_tables_within_target(tree, folder.path())?;
    Ok(())
}

/// The same for rows that compress well, under a prefix extractor: their data
/// stays far below the target, so only the per-key state the writer holds can
/// bound the table, and it cuts one ingestion into several tables.
#[cfg(feature = "lz4")]
#[test]
fn a_compressible_ingestion_is_cut_into_tables_within_the_target() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = small_rows_tree(folder.path())?;
    ingest(&tree, 1_000_000, |_, buf| buf.fill(0x5a))?;
    let sizes = table_sizes(tree, folder.path())?;
    assert!(sizes.len() >= 2, "one ingestion wrote {sizes:?}");
    assert_within(&sizes, TARGET);
    Ok(())
}

/// A tree of small, well-compressing rows under a prefix extractor.
#[cfg(feature = "lz4")]
fn small_rows_tree(folder: &std::path::Path) -> lsm_tree::Result<lsm_tree::AnyTree> {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4))
    .prefix_extractor(Arc::new(ColonPrefixes))
    .open()
}

/// A flush writes through the same table writer as an ingestion: a memtable
/// of keys with one-byte values holds far less data than the target, while
/// the per-key state its table would hold does not, so it is cut into
/// several tables.
#[cfg(feature = "lz4")]
#[test]
fn a_flush_of_small_rows_is_cut_into_tables_within_the_target() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = small_rows_tree(folder.path())?;
    for i in 0..1_000_000u64 {
        tree.insert(format!("node:{i:016}"), [0x5a], 0);
    }
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1_000_000);
    let sizes = table_sizes(tree, folder.path())?;
    assert!(sizes.len() >= 2, "one flush wrote {sizes:?}");
    assert_within(&sizes, TARGET);
    Ok(())
}

/// A compaction does the same at its own target size: the per-key state of
/// small rows reaches it well before their compressed data.
#[cfg(feature = "lz4")]
#[test]
fn a_compaction_of_small_rows_is_cut_into_tables_within_its_target() -> lsm_tree::Result<()> {
    const COMPACTION_TARGET: u64 = 1_024 * 1_024;
    let folder = get_tmp_folder();
    let tree = small_rows_tree(folder.path())?;
    for i in 0..200_000u64 {
        tree.insert(format!("node:{i:016}"), [0x5a], 0);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(COMPACTION_TARGET, SeqNo::MAX)?;
    assert_eq!(tree.len(SeqNo::MAX, None)?, 200_000);
    let sizes = table_sizes(tree, folder.path())?;
    assert!(sizes.len() >= 2, "the compaction wrote {sizes:?}");
    assert_within(&sizes, COMPACTION_TARGET);
    Ok(())
}

/// Sizes of the table files under `folder`, once `tree` is closed. A replaced
/// compaction input has its space released at once but stays listed until the
/// last version referencing it drops, so counting while the tree is open
/// would count it as an empty table.
fn table_sizes(tree: lsm_tree::AnyTree, folder: &std::path::Path) -> lsm_tree::Result<Vec<u64>> {
    drop(tree);
    let mut sizes = Vec::new();
    for entry in std::fs::read_dir(folder.join("tables"))? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            sizes.push(entry.metadata()?.len());
        }
    }
    Ok(sizes)
}

/// No table is past `target`. A table is cut at the first key boundary past
/// it, so it may exceed it by one data block and the estimate error of the
/// sections written after it.
fn assert_within(sizes: &[u64], target: u64) {
    let limit = target + target / 16;
    assert!(
        sizes.iter().all(|&size| size <= limit),
        "tables past {limit} bytes: {sizes:?}",
    );
}

/// Several tables were written under `folder`, none past the target.
fn assert_tables_within_target(
    tree: lsm_tree::AnyTree,
    folder: &std::path::Path,
) -> lsm_tree::Result<()> {
    let sizes = table_sizes(tree, folder)?;
    let total: u64 = sizes.iter().sum();
    assert!(
        sizes.len() as u64 >= total / TARGET,
        "{total} bytes ingested into {} table(s): {sizes:?}",
        sizes.len(),
    );
    assert_within(&sizes, TARGET);
    Ok(())
}
