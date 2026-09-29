// Blob reference count versus depth, on two layouts of the same 32 blob files.
//
// Each layout writes 32 flushes, one blob file each, then compacts them into a
// single table that still points into all 32 files. In the consecutive layout
// every flush owns one run of adjacent keys, so a scan reads one blob file at a
// time; in the interleaved layout flush `f` owns keys `f`, `f + 32`, ..., so a
// scan through any part of the key space alternates between all 32 files. The
// count is 32 in both; only the depth tells them apart.

use lsm_tree::{
    AbstractTree, AnyTree, BlobReferenceStats, KvSeparationOptions, SequenceNumberCounter,
};
use test_log::test;

const FILES: u64 = 32;
const KEYS_PER_FILE: u64 = 16;

fn build(folder: &std::path::Path, interleaved: bool) -> lsm_tree::Result<AnyTree> {
    let tree = lsm_tree::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;
    let mut seqno = 0;
    for file in 0..FILES {
        for i in 0..KEYS_PER_FILE {
            let key = if interleaved {
                i * FILES + file
            } else {
                file * KEYS_PER_FILE + i
            };
            tree.insert(format!("key{key:06}"), "v".repeat(2_048), seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    assert_eq!(tree.blob_file_count(), FILES as usize);
    Ok(tree)
}

fn stats(count: u64, depth: u64) -> BlobReferenceStats {
    BlobReferenceStats { count, depth }
}

#[test]
fn consecutive_blob_files_have_a_high_count_and_a_depth_of_one() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = build(folder.path(), false)?;

    // Before compaction: 32 disjoint tables in the first level, one file each.
    let first_level = tree.level_segment_stats()?;
    let level = first_level.first().expect("a first level");
    assert_eq!(level.segment_count, FILES as usize);
    assert_eq!(level.blob_references, stats(FILES, 1));
    for segment in &level.segments {
        assert_eq!(segment.blob_references, stats(1, 1));
    }

    tree.major_compact(u64::MAX, u64::MAX)?;
    assert_eq!(tree.table_count(), 1);
    assert_eq!(tree.storage_stats()?.blob_references, stats(FILES, 1));
    Ok(())
}

#[test]
fn interleaved_blob_files_have_the_same_count_and_a_high_depth() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = build(folder.path(), true)?;

    // Before compaction: 32 overlapping tables, one file each, every file's
    // span covering almost the whole key space.
    let first_level = tree.level_segment_stats()?;
    let level = first_level.first().expect("a first level");
    assert_eq!(level.blob_references, stats(FILES, FILES));
    for segment in &level.segments {
        assert_eq!(segment.blob_references, stats(1, 1));
    }

    tree.major_compact(u64::MAX, u64::MAX)?;
    assert_eq!(tree.table_count(), 1);
    let only = tree.storage_stats()?.blob_references;
    assert_eq!(only, stats(FILES, FILES));
    Ok(())
}

/// A tree that separates no value references no blob file: both figures are
/// zero, at every level and for the tree.
#[test]
fn a_tree_without_blob_files_reports_no_references() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = lsm_tree::Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..100u64 {
        tree.insert(format!("key{i:04}"), "v", i);
    }
    tree.flush_active_memtable(0)?;
    assert_eq!(tree.storage_stats()?.blob_references, stats(0, 0));
    for level in tree.level_segment_stats()? {
        assert_eq!(level.blob_references, stats(0, 0));
    }
    Ok(())
}
