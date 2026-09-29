// Blob reference count versus depth, on two layouts of the same 32 blob files,
// and the compaction that relocates blob files when the depth grows too large.
//
// Each layout writes 32 flushes, one blob file each, then compacts them into a
// single table that still points into all 32 files. In the consecutive layout
// every flush owns one run of adjacent keys, so a scan reads one blob file at a
// time; in the interleaved layout flush `f` owns keys `f`, `f + 32`, ..., so a
// scan through any part of the key space alternates between all 32 files. The
// count is 32 in both; only the depth tells them apart.

use core::num::NonZeroU64;
use lsm_tree::fs::MemFs;
use lsm_tree::{
    AbstractTree, AnyTree, BlobReferenceStats, Guard as _, KvSeparationOptions, SeqNo,
    SequenceNumberCounter,
};
use std::collections::BTreeSet;
use std::sync::Arc;
use test_log::test;

const FILES: u64 = 32;
const KEYS_PER_FILE: u64 = 16;
const VALUE_LEN: usize = 2_048;

/// Relocation for locality, with the given depth limit and budget.
fn locality(max_depth: u64, budget: f32) -> KvSeparationOptions {
    KvSeparationOptions::default()
        .relocate_for_locality(NonZeroU64::new(max_depth).expect("non-zero"), budget)
}

fn key(i: u64) -> String {
    format!("key{i:06}")
}

/// A value that does not compress, so blob file sizes are the bytes written.
fn value(i: u64) -> Vec<u8> {
    let mut state = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..VALUE_LEN)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

fn open(config: lsm_tree::Config) -> lsm_tree::Result<AnyTree> {
    config.open()
}

fn config(folder: &std::path::Path, blob_opts: KvSeparationOptions) -> lsm_tree::Config {
    lsm_tree::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(blob_opts))
}

/// Writes the 32 flushes of one layout.
fn fill(tree: &AnyTree, interleaved: bool) -> lsm_tree::Result<()> {
    let mut seqno = 0;
    for file in 0..FILES {
        for i in 0..KEYS_PER_FILE {
            let k = if interleaved {
                i * FILES + file
            } else {
                file * KEYS_PER_FILE + i
            };
            tree.insert(key(k), value(k), seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    assert_eq!(tree.blob_file_count(), FILES as usize);
    Ok(())
}

fn build(
    folder: &std::path::Path,
    interleaved: bool,
    blob_opts: KvSeparationOptions,
) -> lsm_tree::Result<AnyTree> {
    let tree = open(config(folder, blob_opts))?;
    fill(&tree, interleaved)?;
    Ok(tree)
}

fn stats(count: u64, depth: u64) -> BlobReferenceStats {
    BlobReferenceStats { count, depth }
}

/// The files of one kind in the tree folder, by id, with their lengths.
fn files_in(folder: &std::path::Path, kind: &str) -> lsm_tree::Result<Vec<(u64, u64)>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(folder.join(kind))? {
        let entry = entry?;
        if let Some(id) = entry.file_name().to_str().and_then(|n| n.parse().ok()) {
            files.push((id, entry.metadata()?.len()));
        }
    }
    files.sort_unstable();
    Ok(files)
}

/// The blob files the tree's current version holds. A dropped file can
/// linger on disk until its last reader lets go, so the folder is not the
/// answer.
fn live_blob_ids(tree: &AnyTree) -> BTreeSet<u64> {
    tree.current_version()
        .blob_files
        .iter()
        .map(|bf| bf.id())
        .collect()
}

/// Every key comes back with its own value.
fn assert_intact(tree: &AnyTree) -> lsm_tree::Result<()> {
    let mut seen = 0;
    for guard in tree.iter(SeqNo::MAX, None) {
        let (k, v) = guard.into_inner()?;
        assert_eq!(&*k, key(seen).as_bytes());
        assert_eq!(&*v, value(seen).as_slice(), "value of {}", key(seen));
        seen += 1;
    }
    assert_eq!(seen, FILES * KEYS_PER_FILE);
    Ok(())
}

#[test]
fn consecutive_blob_files_have_a_high_count_and_a_depth_of_one() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = build(folder.path(), false, KvSeparationOptions::default())?;

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
    let tree = build(folder.path(), true, KvSeparationOptions::default())?;

    // Before compaction: 32 overlapping tables, one file each, every file's
    // span covering almost the whole key space.
    let first_level = tree.level_segment_stats()?;
    let level = first_level.first().expect("a first level");
    assert_eq!(level.blob_references, stats(FILES, FILES));
    for segment in &level.segments {
        assert_eq!(segment.blob_references, stats(1, 1));
    }

    // Locality relocation is off by default: the compaction keeps every file.
    let before = live_blob_ids(&tree);
    tree.major_compact(u64::MAX, u64::MAX)?;
    assert_eq!(tree.table_count(), 1);
    assert_eq!(tree.storage_stats()?.blob_references, stats(FILES, FILES));
    assert_eq!(live_blob_ids(&tree), before);
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

/// Many blob files in consecutive runs are good locality whatever their
/// count: the tightest limit and an unbounded budget still relocate nothing.
#[test]
fn locality_relocation_leaves_consecutive_blob_files_alone() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = build(folder.path(), false, locality(1, f32::MAX))?;
    let before = live_blob_ids(&tree);

    tree.major_compact(u64::MAX, u64::MAX)?;
    assert_eq!(live_blob_ids(&tree), before);
    assert_eq!(tree.storage_stats()?.blob_references, stats(FILES, 1));
    assert_intact(&tree)?;
    Ok(())
}

/// With room in the budget, the compaction rewrites the interleaved files in
/// key order: the depth falls to one and every value survives.
#[test]
fn locality_relocation_makes_interleaved_blob_files_consecutive() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = build(folder.path(), true, locality(4, f32::MAX))?;
    let before = live_blob_ids(&tree);

    tree.major_compact(u64::MAX, u64::MAX)?;
    let after = live_blob_ids(&tree);
    assert!(
        before.is_disjoint(&after),
        "every interleaved file must be rewritten: before {before:?}, after {after:?}",
    );
    assert_eq!(tree.storage_stats()?.blob_references.depth, 1);
    assert_intact(&tree)?;
    Ok(())
}

/// A scan over the relocated tree reads its values in fewer requests than a
/// scan over the interleaved one: coalesced read-ahead only merges values that
/// sit next to each other in one file.
#[cfg(feature = "metrics")]
#[test]
fn locality_relocation_cuts_the_blob_reads_of_a_scan() -> lsm_tree::Result<()> {
    fn scan_reads(
        folder: &std::path::Path,
        blob_opts: KvSeparationOptions,
    ) -> lsm_tree::Result<usize> {
        let tree = build(folder, true, blob_opts.clone())?;
        tree.major_compact(u64::MAX, u64::MAX)?;
        drop(tree);
        // Reopened, so the scan starts from a cold blob cache.
        let tree = open(config(folder, blob_opts))?;
        let before = tree.metrics().blob_read_count();
        assert_intact(&tree)?;
        Ok(tree.metrics().blob_read_count() - before)
    }

    let interleaved = tempfile::tempdir()?;
    let relocated = tempfile::tempdir()?;
    let scattered = scan_reads(interleaved.path(), KvSeparationOptions::default())?;
    let local = scan_reads(relocated.path(), locality(4, f32::MAX))?;
    assert!(
        local * 4 < scattered,
        "relocated scan issued {local} blob reads, interleaved {scattered}",
    );
    Ok(())
}

/// Reading plus writing the relocated files never exceeds the budget's share
/// of what the compaction writes anyway, and a budget that covers some files
/// relocates those and keeps the rest.
#[test]
fn locality_relocation_stays_within_its_budget() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // The layout first, with relocation off, to measure it: what the merge
    // writes anyway is its input tables, by file length.
    drop(build(folder.path(), true, KvSeparationOptions::default())?);
    let table_bytes: u64 = files_in(folder.path(), "tables")?
        .iter()
        .map(|(_, len)| len)
        .sum();
    let before = files_in(folder.path(), "blobs")?;
    let largest = before
        .iter()
        .map(|(_, size)| *size)
        .max()
        .expect("blob files");

    // Room for about eight files, read and written.
    #[expect(clippy::cast_precision_loss, reason = "test budget from byte counts")]
    let budget = (8 * 2 * largest) as f32 / table_bytes as f32;
    let tree = open(config(folder.path(), locality(4, budget)))?;
    tree.major_compact(u64::MAX, u64::MAX)?;

    let after = live_blob_ids(&tree);
    let relocated: Vec<_> = before
        .iter()
        .filter(|(id, _)| !after.contains(id))
        .collect();
    let relocated_bytes: u64 = relocated.iter().map(|(_, size)| size).sum();
    #[expect(
        clippy::cast_precision_loss,
        reason = "comparison against the float budget"
    )]
    let limit = f64::from(budget) * table_bytes as f64;
    #[expect(clippy::cast_precision_loss, reason = "byte count to float")]
    let spent = (2 * relocated_bytes) as f64;
    assert!(
        spent <= limit,
        "spent {spent} B against a budget of {limit} B"
    );
    assert!(
        (2..FILES as usize).contains(&relocated.len()),
        "relocated {} files; expected some, not all",
        relocated.len(),
    );
    assert!(tree.storage_stats()?.blob_references.depth < FILES);
    assert_intact(&tree)?;
    Ok(())
}

/// Stale files are relocated whatever the locality budget: a budget of zero
/// still rewrites the stale file, and relocates nothing else.
#[test]
fn stale_blob_files_are_relocated_before_the_locality_budget() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = build(folder.path(), true, locality(1, 0.0).age_cutoff(1.0))?;

    // Overwrite most of the first flush's keys with values kept inline, so its
    // blob file holds mostly dead values once a compaction drops them.
    for i in 0..12 {
        tree.insert(key(i * FILES), "inline", 1_000 + i);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, u64::MAX)?;
    let stale_file = *live_blob_ids(&tree).first().expect("blob files");
    assert!(tree.stale_blob_bytes() > 0, "the first file must be stale");

    tree.major_compact(u64::MAX, u64::MAX)?;
    let after = live_blob_ids(&tree);
    assert!(
        !after.contains(&stale_file),
        "the stale file must be rewritten"
    );
    assert_eq!(
        after.len(),
        FILES as usize,
        "the budget of zero relocates nothing beyond the stale file: {after:?}",
    );
    Ok(())
}

/// Locality relocation is optional work: when its output would eat into the
/// reserved free space, the compaction runs without it.
#[test]
fn locality_relocation_is_skipped_under_space_pressure() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let mem = MemFs::with_capacity(u64::MAX);
    let tree =
        open(config(folder.path(), locality(4, f32::MAX)).with_shared_fs(Arc::new(mem.clone())))?;
    fill(&tree, true)?;
    let blob_ids_before = tree
        .current_version()
        .blob_files
        .iter()
        .map(|bf| bf.id())
        .collect::<BTreeSet<_>>();

    // Room for the merged table and some, but not for the rewritten values on
    // top of the reserved free space.
    let used = tree.storage_stats()?.used_bytes;
    mem.set_capacity(used + 3 * 512 * 1_024);

    tree.major_compact(u64::MAX, u64::MAX)?;
    assert_eq!(tree.table_count(), 1, "the merge itself must still run");
    let blob_ids_after = tree
        .current_version()
        .blob_files
        .iter()
        .map(|bf| bf.id())
        .collect::<BTreeSet<_>>();
    assert_eq!(blob_ids_after, blob_ids_before);
    assert_eq!(tree.storage_stats()?.blob_references.depth, FILES);
    Ok(())
}
