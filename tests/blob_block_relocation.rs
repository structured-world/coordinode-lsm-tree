// A relocation copies a block whole while its live share stays above the
// share the staleness threshold leaves, and writes the live values of one
// below it into new blocks.

use lsm_tree::{
    AbstractTree, AnyTree, CompressionType, Config, KvSeparationOptions, SeqNo,
    SequenceNumberCounter, blob_tree::FragmentationEntry, get_tmp_folder,
};

/// Bytes of each value: with three-byte keys a packed record takes 230 bytes
/// (its 27-byte fixed part, key and value), so a block of 1,000 bytes closes
/// at five values.
const VALUE: usize = 200;
const ENTRY: u64 = 27 + 3 + VALUE as u64;

fn key(i: usize) -> String {
    format!("k{i:02}")
}

/// The value of key `i` in generation `generation`, different for each.
fn value(i: usize, generation: u8) -> Vec<u8> {
    let mut value = vec![generation; VALUE];
    value[..2].copy_from_slice(&u16::try_from(i).expect("small").to_le_bytes());
    value
}

/// A blob tree separating every value into blocks of five, stored
/// uncompressed, every file old enough to collect.
fn open(path: &std::path::Path) -> lsm_tree::Result<AnyTree> {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(100)
            .block_size(1_000)
            .age_cutoff(1.0),
    ))
    .blob_compression(CompressionType::None)
    .open()
}

/// Every key of `keys` reads its value of the generation `generation` gives it.
fn assert_reads(
    tree: &AnyTree,
    keys: impl Iterator<Item = usize>,
    generation: impl Fn(usize) -> u8,
) -> lsm_tree::Result<()> {
    for i in keys {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX)?.as_deref(),
            Some(&*value(i, generation(i))),
            "{}",
            key(i)
        );
    }
    Ok(())
}

/// Of four blocks, one wholly dead and one with one dead value of five, the
/// file is collected; the three blocks with values left are copied as they
/// were, the dead value with them, which the new file counts as garbage.
#[test]
fn a_mostly_live_block_is_copied_whole_with_its_dead_values() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    for i in 0..20 {
        tree.insert(key(i), value(i, 0), 0);
    }
    tree.flush_active_memtable(0)?;
    // The first block and one value of the second.
    for i in 0..6 {
        tree.insert(key(i), value(i, 1), 1);
    }
    tree.flush_active_memtable(0)?;

    tree.major_compact(64_000_000, 1_000)?;
    tree.major_compact(64_000_000, 1_000)?;
    assert_reads(&tree, 0..20, |i| u8::from(i < 6))?;

    let stats = tree.current_version().gc_stats().clone();
    let entries: Vec<&FragmentationEntry> = stats.values().collect();
    assert_eq!(
        entries,
        [&FragmentationEntry::new(1, VALUE as u64, 0).with_block_bytes(ENTRY)],
        "the dead value of the second block is the new file's only garbage: {stats:?}"
    );
    assert_eq!(tree.blob_file_count(), 2);
    assert!(lsm_tree::verify::verify_integrity(&tree).is_ok());
    Ok(())
}

/// Blocks with two dead values of five fall below the share: their live
/// values are written into new blocks, and the new file holds no garbage.
#[test]
fn a_block_below_the_share_has_its_live_values_rewritten() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    for i in 0..20 {
        tree.insert(key(i), value(i, 0), 0);
    }
    tree.flush_active_memtable(0)?;
    let dead = |i: usize| i % 5 < 2;
    for i in (0..20).filter(|&i| dead(i)) {
        tree.insert(key(i), value(i, 1), 1);
    }
    tree.flush_active_memtable(0)?;

    tree.major_compact(64_000_000, 1_000)?;
    tree.major_compact(64_000_000, 1_000)?;
    assert_reads(&tree, 0..20, |i| u8::from(dead(i)))?;
    assert!(
        tree.current_version().gc_stats().is_empty(),
        "no dead value was carried: {:?}",
        tree.current_version().gc_stats()
    );
    Ok(())
}

/// Two files whose keys interleave, both collected: each one's blocks are
/// copied into a file of their own, so every file stays in key order, which
/// the next collection of those files relies on to match values to pointers.
#[test]
fn blocks_of_interleaved_files_are_copied_into_files_of_their_own() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    for parity in 0..2 {
        for i in (0..40).filter(|i| i % 2 == parity) {
            tree.insert(key(i), value(i, 0), 0);
        }
        tree.flush_active_memtable(0)?;
    }
    // The first block of each file wholly dead, the rest live.
    let first = |i: usize| i < 10;
    for i in (0..40).filter(|&i| first(i)) {
        tree.insert(key(i), value(i, 1), 1);
    }
    tree.flush_active_memtable(0)?;

    tree.major_compact(64_000_000, 1_000)?;
    tree.major_compact(64_000_000, 1_000)?;
    assert_reads(&tree, 0..40, |i| u8::from(first(i)))?;
    assert_eq!(
        tree.blob_file_count(),
        3,
        "a file of the overwrites and one per source of copied blocks"
    );

    // Kill a block's worth of the copies in each, and collect again.
    let second = |i: usize| (10..30).contains(&i);
    for i in (0..40).filter(|&i| second(i)) {
        tree.insert(key(i), value(i, 2), 2);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, 1_000)?;
    tree.major_compact(64_000_000, 1_000)?;
    assert_reads(
        &tree,
        0..40,
        |i| {
            if second(i) { 2 } else { u8::from(first(i)) }
        },
    )?;
    assert!(lsm_tree::verify::verify_integrity(&tree).is_ok());
    Ok(())
}
