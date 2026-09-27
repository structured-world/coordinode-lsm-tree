use lsm_tree::{
    AbstractTree, CompressionType, Config, PrefixExtractor, SeqNo, SequenceNumberCounter,
    config::CompressionPolicy, get_tmp_folder,
};
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
struct ColonPrefixes;

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
    assert_tables_within_target(folder.path())?;
    Ok(())
}

/// The same for rows that compress well, under a prefix extractor: the data
/// alone never reaches the target, so only the per-key state the writer holds
/// can bound the table.
#[cfg(feature = "lz4")]
#[test]
fn a_compressible_ingestion_is_cut_into_tables_of_the_target_size() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4))
    .prefix_extractor(Arc::new(ColonPrefixes))
    .open()?;
    ingest(&tree, 4_000_000, |_, buf| buf.fill(0x5a))?;
    assert_tables_within_target(folder.path())?;
    Ok(())
}

/// Several tables were written under `folder`, none past the target.
fn assert_tables_within_target(folder: &std::path::Path) -> lsm_tree::Result<()> {
    let mut sizes = Vec::new();
    for entry in std::fs::read_dir(folder.join("tables"))? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            sizes.push(entry.metadata()?.len());
        }
    }
    let total: u64 = sizes.iter().sum();
    assert!(
        sizes.len() as u64 >= total / TARGET,
        "{total} bytes ingested into {} table(s): {sizes:?}",
        sizes.len(),
    );
    // A table is cut at the first key boundary past the target, so it may
    // exceed it by one data block and the index and filter written after it.
    let limit = TARGET + TARGET / 16;
    assert!(
        sizes.iter().all(|&size| size <= limit),
        "tables past {limit} bytes: {sizes:?}",
    );
    Ok(())
}
