//! A snapshot read at or below the retention floor must return
//! `Error::SnapshotBelowRetention`, never panic.
//!
//! An install that collects what older snapshots read (a GC compaction below
//! its watermark, `clear`, `drop_range`, FIFO eviction, a filtering compaction)
//! raises the floor, and a read at or below it has nothing left to be answered
//! from: the engine used to `expect` a version and take the whole tree down.
//! Every read surface that resolves a snapshot is exercised here, on both the
//! standard and the KV-separated tree, plus the `clear` path.
//!
//! The second half covers the boundary's durability: the install that
//! discards what older snapshots saw (a GC compaction, `clear`, `drop_range`,
//! FIFO eviction) persists a retention floor, and a reopen, a checkpoint and
//! a manifest repair each report the boundary the floor implies; additive
//! installs (flush, ingest, trivial move) leave it alone.

mod common;

use lsm_tree::{
    AbstractTree, AnyTree, Config, Error, Guard, SeqNo, SequenceNumberCounter, get_tmp_folder,
};
use std::sync::Arc;
use test_log::test;

/// A pruned tree: two flushed versions of `"k"`, then a major compaction whose
/// watermark sits above both, so the first is collected and the floor rises
/// past it.
///
/// Returns the tree, the seqno of the FIRST write of `"k"` (a snapshot at
/// `first + 1` saw `v1` before the prune) and the seqno counter, plus the
/// temp folder, which must outlive the tree.
fn pruned_tree(kv_separated: bool) -> lsm_tree::Result<PrunedTree> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let mut config = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default());
    if kv_separated {
        config = config.with_kv_separation(Some(Default::default()));
    }
    let tree = config.open()?;

    let first = seqno.next();
    tree.insert("k", "v1", first);
    tree.flush_active_memtable(0)?;

    let second = seqno.next();
    tree.insert("k", "v2", second);
    tree.flush_active_memtable(0)?;

    // The watermark is above every seqno handed out so far: the fold keeps the
    // second version and collects the first, so the floor becomes
    // `watermark - 1`.
    let watermark = seqno.get();
    tree.major_compact(common::COMPACTION_TARGET, watermark)?;

    // Each flush takes a seqno of its own for the version it installs, so the
    // floor sits ABOVE `first + 1`: the snapshot the tests probe is strictly
    // below the boundary, not merely at it. Pinned here so a change to how
    // installs allocate seqnos fails loudly instead of silently turning the
    // strict-below probes into at-boundary ones.
    let oldest = tree.oldest_retained_seqno();
    assert!(
        first + 1 < oldest,
        "fixture: first + 1 = {} must be strictly below the boundary {oldest}",
        first + 1
    );

    Ok(PrunedTree {
        tree,
        first,
        watermark,
        seqno,
        folder,
    })
}

/// A pruned tree with the temp folder that backs it; dropping the folder
/// first would delete the tables under the live tree.
struct PrunedTree {
    tree: AnyTree,
    first: SeqNo,
    /// The GC watermark the compaction ran with.
    watermark: SeqNo,
    seqno: SequenceNumberCounter,
    folder: tempfile::TempDir,
}

/// Reopens the tree at `folder` with the seqno counter restored to `next`,
/// the way a deployment restores it from its own durable state.
fn reopen(folder: &std::path::Path, kv_separated: bool, next: SeqNo) -> lsm_tree::Result<AnyTree> {
    let mut config = Config::new(
        folder,
        SequenceNumberCounter::new(next),
        SequenceNumberCounter::default(),
    );
    if kv_separated {
        config = config.with_kv_separation(Some(Default::default()));
    }
    config.open()
}

/// Asserts `err` is the retention error for exactly this boundary: the
/// requested seqno AND the oldest retained seqno must both be reported, so a
/// caller can tell how far behind the retention window it is.
fn assert_below_retention(err: Error, requested: SeqNo, oldest: SeqNo) {
    match err {
        Error::SnapshotBelowRetention {
            requested: got_requested,
            oldest_retained,
        } => {
            assert_eq!(got_requested, requested, "requested seqno must round-trip");
            assert_eq!(
                oldest_retained, oldest,
                "oldest_retained must name the history's front"
            );
        }
        other => panic!("expected SnapshotBelowRetention, got {other:?}"),
    }
}

/// The boundary the read APIs enforce is exactly the oldest retained seqno:
/// a read AT it fails, a read one above it succeeds.
#[test]
fn oldest_retained_seqno_after_prune_is_the_read_boundary() -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        first,
        seqno,
        folder: _folder,
        ..
    } = pruned_tree(false)?;
    let oldest = tree.oldest_retained_seqno();

    // Pruning happened: the first write's version is gone, the boundary is
    // above it and below the counter.
    assert!(
        oldest > first,
        "history must have been pruned past the first write"
    );
    assert!(oldest < seqno.get(), "boundary must stay below the counter");

    assert_below_retention(tree.get("k", oldest).unwrap_err(), oldest, oldest);
    assert_eq!(
        tree.get("k", oldest + 1)?.as_deref(),
        Some(b"v2".as_slice()),
        "one above the boundary is served from the current version"
    );
    assert_eq!(
        tree.get("k", SeqNo::MAX)?.as_deref(),
        Some(b"v2".as_slice())
    );
    Ok(())
}

/// A snapshot at seqno 0 sees nothing from any version, so it is served
/// (empty) rather than rejected, even after pruning.
#[test]
fn read_at_seqno_zero_after_prune_is_empty_not_error() -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        folder: _folder,
        ..
    } = pruned_tree(false)?;
    assert!(tree.get("k", 0)?.is_none());
    assert_eq!(tree.len(0, None)?, 0);
    assert!(tree.is_empty(0, None)?);
    Ok(())
}

/// Every point-style read surface on the standard tree reports the error.
#[test]
fn point_reads_below_retention_return_error() -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        first,
        folder: _folder,
        ..
    } = pruned_tree(false)?;
    let oldest = tree.oldest_retained_seqno();
    let below = first + 1;

    assert_below_retention(tree.get("k", below).unwrap_err(), below, oldest);
    assert_below_retention(tree.contains_key("k", below).unwrap_err(), below, oldest);
    assert_below_retention(tree.size_of("k", below).unwrap_err(), below, oldest);
    // Both multi_get paths: the small-batch per-key path and the batched path.
    assert_below_retention(tree.multi_get(["k"], below).unwrap_err(), below, oldest);
    assert_below_retention(
        tree.multi_get(["a", "k", "z"], below).unwrap_err(),
        below,
        oldest,
    );
    assert_below_retention(
        tree.approximate_range_stats::<&str, _>(.., below)
            .unwrap_err(),
        below,
        oldest,
    );
    assert_below_retention(
        tree.approximate_range_cardinality::<&str, _>(.., below)
            .unwrap_err(),
        below,
        oldest,
    );
    Ok(())
}

/// Every iterator surface yields the error as its first (and only) item, from
/// both ends, instead of panicking at construction.
#[test]
fn iterators_below_retention_yield_error_item() -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        first,
        folder: _folder,
        ..
    } = pruned_tree(false)?;
    let oldest = tree.oldest_retained_seqno();
    let below = first + 1;

    let mut iter = tree.iter(below, None);
    assert_below_retention(
        iter.next().expect("error item").key().unwrap_err(),
        below,
        oldest,
    );
    assert!(iter.next().is_none(), "the error is the only item");

    let mut range = tree.range::<&str, _>(.., below, None);
    assert_below_retention(
        range.next_back().expect("error item").key().unwrap_err(),
        below,
        oldest,
    );
    assert!(range.next_back().is_none());

    let mut prefix = tree.prefix("k", below, None);
    assert_below_retention(
        prefix.next().expect("error item").key().unwrap_err(),
        below,
        oldest,
    );
    assert!(prefix.next().is_none());

    assert_below_retention(tree.len(below, None).unwrap_err(), below, oldest);
    assert_below_retention(tree.is_empty(below, None).unwrap_err(), below, oldest);
    assert_below_retention(
        tree.first_key_value(below, None)
            .expect("error item")
            .key()
            .unwrap_err(),
        below,
        oldest,
    );
    assert_below_retention(
        tree.last_key_value(below, None)
            .expect("error item")
            .key()
            .unwrap_err(),
        below,
        oldest,
    );
    Ok(())
}

/// The seekable iterator and the batch scan share the seekable pipeline; both
/// surface the error once, and a seek on the failed iterator is a harmless
/// no-op rather than a panic.
#[test]
fn seekable_and_batch_scan_below_retention_yield_error_item() -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        first,
        folder: _folder,
        ..
    } = pruned_tree(false)?;
    let oldest = tree.oldest_retained_seqno();
    let below = first + 1;

    let mut seekable = tree.range_seekable::<&str, _>(.., below, None);
    seekable.seek_to(b"a");
    assert_below_retention(
        seekable.peek_key().expect("error item").unwrap_err(),
        below,
        oldest,
    );
    // `peek_key` consumes an error (documented); the iterator is then done.
    assert!(seekable.next().is_none());
    seekable.seek_to_for_prev(b"z");
    assert!(seekable.next_back().is_none());

    let mut batch = tree.batch_range_scan(["a".."m", "m".."z"], below, None);
    assert_below_retention(
        batch.next().expect("error item").key().unwrap_err(),
        below,
        oldest,
    );
    assert!(batch.next().is_none());
    Ok(())
}

/// The columnar scan resolves its snapshot up front and fails there.
#[cfg(feature = "columnar")]
#[test]
fn columnar_scan_below_retention_returns_error() -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        first,
        folder: _folder,
        ..
    } = pruned_tree(false)?;
    let oldest = tree.oldest_retained_seqno();
    let below = first + 1;
    let AnyTree::Standard(tree) = tree else {
        panic!("pruned_tree(false) opens a standard tree");
    };
    let err = match tree.columnar_scan(&[], None, below, ..) {
        Ok(_) => panic!("columnar scan below retention must fail"),
        Err(e) => e,
    };
    assert_below_retention(err, below, oldest);
    Ok(())
}

/// The KV-separated tree resolves snapshots through its index tree and has
/// its own read implementations; each of them must report the error too.
#[test]
fn blob_tree_reads_below_retention_return_error() -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        first,
        folder: _folder,
        ..
    } = pruned_tree(true)?;
    let oldest = tree.oldest_retained_seqno();
    let below = first + 1;

    assert_below_retention(tree.get("k", below).unwrap_err(), below, oldest);
    assert_below_retention(tree.multi_get(["k"], below).unwrap_err(), below, oldest);
    assert_below_retention(
        tree.multi_get(["a", "k", "z"], below).unwrap_err(),
        below,
        oldest,
    );
    // An EMPTY batch still validates its snapshot: the contract is per read,
    // not per key, and the standard tree already refuses it.
    assert_below_retention(
        tree.multi_get(Vec::<&str>::new(), below).unwrap_err(),
        below,
        oldest,
    );

    let mut range = tree.range::<&str, _>(.., below, None);
    assert_below_retention(
        range.next().expect("error item").key().unwrap_err(),
        below,
        oldest,
    );
    assert!(range.next().is_none());

    let mut prefix = tree.prefix("k", below, None);
    assert_below_retention(
        prefix.next_back().expect("error item").key().unwrap_err(),
        below,
        oldest,
    );
    assert!(prefix.next_back().is_none());

    let mut seekable = tree.range_seekable::<&str, _>(.., below, None);
    assert_below_retention(
        seekable.next().expect("error item").key().unwrap_err(),
        below,
        oldest,
    );
    assert!(seekable.next().is_none());

    let mut batch = tree.batch_range_scan(["a".."z"], below, None);
    assert_below_retention(
        batch.next().expect("error item").key().unwrap_err(),
        below,
        oldest,
    );
    assert!(batch.next().is_none());

    // Above the boundary the current version serves the read.
    assert_eq!(
        tree.get("k", oldest + 1)?.as_deref(),
        Some(b"v2".as_slice())
    );
    Ok(())
}

/// `clear` drains the history to the (new, empty) latest version, so a
/// snapshot taken before the clear is below retention afterwards.
#[test]
fn read_at_pre_clear_snapshot_after_clear_returns_error() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default()).open()?;

    tree.insert("k", "v1", seqno.next());
    let snapshot = seqno.get();
    assert_eq!(tree.get("k", snapshot)?.as_deref(), Some(b"v1".as_slice()));

    tree.clear()?;

    let oldest = tree.oldest_retained_seqno();
    assert!(
        oldest >= snapshot,
        "clear installs a version at or above the snapshot"
    );
    assert_below_retention(tree.get("k", snapshot).unwrap_err(), snapshot, oldest);
    assert!(tree.get("k", SeqNo::MAX)?.is_none());
    Ok(())
}

/// Without pruning nothing changes: the initial version (seqno 0) stays at
/// the front, so every snapshot above 0 is served and the boundary reads 0.
#[test]
fn oldest_retained_seqno_without_prune_is_zero() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default()).open()?;

    let first = seqno.next();
    tree.insert("k", "v1", first);
    tree.flush_active_memtable(0)?;
    tree.insert("k", "v2", seqno.next());
    tree.flush_active_memtable(0)?;
    // Watermark 0 certifies nothing as collapsible: no version is pruned.
    tree.major_compact(common::COMPACTION_TARGET, 0)?;

    assert_eq!(tree.oldest_retained_seqno(), 0);
    assert_eq!(tree.get("k", first + 1)?.as_deref(), Some(b"v1".as_slice()));
    Ok(())
}

/// The boundary a GC compaction establishes must survive a reopen: the
/// version that still held `v1` is gone from disk, so after the restart a
/// snapshot below the watermark has nothing to be served from and must be
/// refused, exactly as it was while the tree was open.
///
/// After a reopen the boundary is the watermark itself (`watermark - 1`, the
/// highest unservable snapshot): the retained pre-compaction version that
/// served reads between the in-memory front and the watermark did not survive
/// the restart, so the persisted boundary is the one the compaction's data
/// loss implies, not the one the in-memory history happened to keep.
fn assert_boundary_survives_reopen(kv_separated: bool) -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        first,
        watermark,
        seqno,
        folder,
        ..
    } = pruned_tree(kv_separated)?;
    drop(tree);

    let tree = reopen(folder.path(), kv_separated, seqno.get())?;
    let oldest = tree.oldest_retained_seqno();
    assert_eq!(
        oldest,
        watermark - 1,
        "the reopened boundary is the highest snapshot below the GC watermark"
    );

    let below = first + 1;
    assert_below_retention(tree.get("k", below).unwrap_err(), below, oldest);
    assert_below_retention(tree.get("k", oldest).unwrap_err(), oldest, oldest);
    assert_below_retention(tree.multi_get(["k"], below).unwrap_err(), below, oldest);
    assert_below_retention(tree.len(below, None).unwrap_err(), below, oldest);
    let mut range = tree.range::<&str, _>(.., below, None);
    assert_below_retention(
        range.next().expect("error item").key().unwrap_err(),
        below,
        oldest,
    );
    let mut prefix = tree.prefix("k", below, None);
    assert_below_retention(
        prefix.next().expect("error item").key().unwrap_err(),
        below,
        oldest,
    );
    let mut seekable = tree.range_seekable::<&str, _>(.., below, None);
    assert_below_retention(
        seekable.next().expect("error item").key().unwrap_err(),
        below,
        oldest,
    );

    // At the watermark and above the retained data is complete, on every
    // surface.
    assert_eq!(
        tree.get("k", watermark)?.as_deref(),
        Some(b"v2".as_slice()),
        "the first snapshot at the watermark is served"
    );
    assert_eq!(
        tree.get("k", SeqNo::MAX)?.as_deref(),
        Some(b"v2".as_slice())
    );
    assert_eq!(tree.len(watermark, None)?, 1);
    let (key, value) = tree
        .iter(watermark, None)
        .next()
        .expect("one row")
        .into_inner()?;
    assert_eq!((&*key, &*value), (b"k".as_slice(), b"v2".as_slice()));
    assert!(tree.get("k", 0)?.is_none());
    Ok(())
}

#[test]
fn boundary_survives_reopen_after_gc_compaction() -> lsm_tree::Result<()> {
    assert_boundary_survives_reopen(false)
}

#[test]
fn blob_tree_boundary_survives_reopen_after_gc_compaction() -> lsm_tree::Result<()> {
    assert_boundary_survives_reopen(true)
}

/// Opens a fresh tree at `folder`, KV-separated or not, on the given counter.
fn open_tree(
    folder: &std::path::Path,
    kv_separated: bool,
    seqno: &SequenceNumberCounter,
) -> lsm_tree::Result<AnyTree> {
    let mut config = Config::new(folder, seqno.clone(), SequenceNumberCounter::default());
    if kv_separated {
        config = config.with_kv_separation(Some(Default::default()));
    }
    config.open()
}

/// `clear` drops every table, so after a reopen the pre-clear snapshot is
/// below retention just as it was before the restart.
fn assert_boundary_survives_reopen_after_clear(kv_separated: bool) -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(folder.path(), kv_separated, &seqno)?;

    tree.insert("k", "v1", seqno.next());
    tree.flush_active_memtable(0)?;
    let snapshot = seqno.get();
    tree.clear()?;
    let live_oldest = tree.oldest_retained_seqno();
    drop(tree);

    let tree = reopen(folder.path(), kv_separated, seqno.get())?;
    assert_eq!(
        tree.oldest_retained_seqno(),
        live_oldest,
        "the clear's boundary is the one the reopen reports"
    );
    assert_below_retention(tree.get("k", snapshot).unwrap_err(), snapshot, live_oldest);
    assert!(tree.get("k", SeqNo::MAX)?.is_none());
    Ok(())
}

#[test]
fn boundary_survives_reopen_after_clear() -> lsm_tree::Result<()> {
    assert_boundary_survives_reopen_after_clear(false)
}

#[test]
fn blob_tree_boundary_survives_reopen_after_clear() -> lsm_tree::Result<()> {
    assert_boundary_survives_reopen_after_clear(true)
}

/// A compaction with watermark `0` drops nothing, so nothing is persisted
/// as a boundary either: the reopened tree serves every snapshot.
fn assert_reopen_without_gc_keeps_boundary_zero(kv_separated: bool) -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(folder.path(), kv_separated, &seqno)?;

    let first = seqno.next();
    tree.insert("k", "v1", first);
    tree.flush_active_memtable(0)?;
    tree.insert("k", "v2", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.major_compact(common::COMPACTION_TARGET, 0)?;
    drop(tree);

    let tree = reopen(folder.path(), kv_separated, seqno.get())?;
    assert_eq!(tree.oldest_retained_seqno(), 0);
    assert_eq!(tree.get("k", first + 1)?.as_deref(), Some(b"v1".as_slice()));
    Ok(())
}

#[test]
fn reopen_without_gc_keeps_boundary_zero() -> lsm_tree::Result<()> {
    assert_reopen_without_gc_keeps_boundary_zero(false)
}

#[test]
fn blob_tree_reopen_without_gc_keeps_boundary_zero() -> lsm_tree::Result<()> {
    assert_reopen_without_gc_keeps_boundary_zero(true)
}

/// Installs that only ADD or MOVE data must not raise the boundary, even
/// when run with a watermark: a flush with a GC watermark, a bulk
/// ingestion, and a leveled trivial move (a single non-overlapping L0 table
/// slides into the last level untouched). Each is followed by a reopen so the
/// persisted floor is what is checked.
#[test]
fn additive_installs_do_not_raise_the_boundary() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(folder.path(), false, &seqno)?;

    let first = seqno.next();
    tree.insert("a", "v1", first);
    // A flush run with a watermark, over one version per key: nothing to fold.
    tree.flush_active_memtable(seqno.get())?;
    // A trivial move with a watermark: one table, nothing to merge with.
    tree.compact(
        Arc::new(lsm_tree::compaction::Leveled::default()),
        seqno.get(),
    )?;
    assert_eq!(
        tree.level_table_count(0).unwrap_or_default(),
        0,
        "the single L0 table was moved, not merged"
    );
    // A bulk ingestion adds a table at its own seqno.
    let mut ingest = tree.ingestion()?;
    ingest.write(b"b", b"v1")?;
    ingest.finish()?;
    drop(tree);

    let tree = reopen(folder.path(), false, seqno.get())?;
    assert_eq!(tree.oldest_retained_seqno(), 0, "nothing was discarded");
    assert_eq!(tree.get("a", first + 1)?.as_deref(), Some(b"v1".as_slice()));
    assert_eq!(
        tree.get("b", SeqNo::MAX)?.as_deref(),
        Some(b"v1".as_slice())
    );
    Ok(())
}

/// A leveled merge (two overlapping L0 tables) run with a watermark drops
/// the shadowed version like a major compaction does, so the reopened
/// boundary is the watermark minus one.
#[test]
fn leveled_merge_with_watermark_raises_the_boundary() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(folder.path(), false, &seqno)?;

    let first = seqno.next();
    tree.insert("k", "v1", first);
    tree.flush_active_memtable(0)?;
    tree.insert("k", "v2", seqno.next());
    tree.flush_active_memtable(0)?;
    let watermark = seqno.get();
    let result = tree.compact(
        Arc::new(lsm_tree::compaction::Leveled::default()),
        watermark,
    )?;
    assert_eq!(
        result.action,
        lsm_tree::compaction::CompactionAction::Merged,
        "overlapping tables merge rather than move"
    );
    drop(tree);

    let tree = reopen(folder.path(), false, seqno.get())?;
    let oldest = tree.oldest_retained_seqno();
    assert_eq!(oldest, watermark - 1);
    assert_below_retention(tree.get("k", first + 1).unwrap_err(), first + 1, oldest);
    assert_eq!(tree.get("k", watermark)?.as_deref(), Some(b"v2".as_slice()));
    Ok(())
}

/// `drop_range` removes whole tables, so every snapshot up to its install is
/// refused, live and after a reopen, for the surviving keys too (the boundary
/// is tree-wide, not per key).
#[test]
fn drop_range_raises_the_boundary_to_its_install() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(folder.path(), false, &seqno)?;

    tree.insert("a", "va", seqno.next());
    tree.flush_active_memtable(0)?;
    let b = seqno.next();
    tree.insert("b", "vb", b);
    tree.flush_active_memtable(0)?;
    let snapshot = seqno.get();
    assert_eq!(tree.get("b", snapshot)?.as_deref(), Some(b"vb".as_slice()));

    // The drop's own install takes the next seqno: that is the boundary, and
    // it holds at once, with no pre-drop version kept to serve the snapshot.
    let install = seqno.get();
    tree.drop_range("a"..="a")?;
    assert_eq!(tree.oldest_retained_seqno(), install);
    assert_below_retention(tree.get("b", snapshot).unwrap_err(), snapshot, install);
    drop(tree);

    let tree = reopen(folder.path(), false, seqno.get())?;
    let oldest = tree.oldest_retained_seqno();
    assert_eq!(oldest, install, "the drop's boundary survives the reopen");
    assert_below_retention(tree.get("b", snapshot).unwrap_err(), snapshot, oldest);
    assert!(tree.get("a", SeqNo::MAX)?.is_none());
    assert_eq!(
        tree.get("b", SeqNo::MAX)?.as_deref(),
        Some(b"vb".as_slice())
    );
    Ok(())
}

/// FIFO eviction drops the oldest tables through the same table-drop
/// install as `drop_range`, so it raises the boundary the same way.
#[test]
fn fifo_eviction_raises_the_boundary_to_its_install() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(folder.path(), false, &seqno)?;

    for i in 0..3u8 {
        tree.insert([b'a' + i], vec![i; 1_024], seqno.next());
        tree.flush_active_memtable(0)?;
    }
    let snapshot = seqno.get();
    let before = tree.table_count();

    let install = seqno.get();
    // A tiny size limit evicts at least one table.
    tree.compact(Arc::new(lsm_tree::compaction::Fifo::new(10, None)), 0)?;
    assert!(tree.table_count() < before, "eviction dropped a table");
    drop(tree);

    let tree = reopen(folder.path(), false, seqno.get())?;
    let oldest = tree.oldest_retained_seqno();
    assert_eq!(
        oldest, install,
        "the eviction's boundary survives the reopen"
    );
    assert_below_retention(tree.get("c", snapshot).unwrap_err(), snapshot, oldest);
    Ok(())
}

/// A GC watermark far above the counter (`SeqNo::MAX` to collect all
/// history) must not push the persisted boundary past the compaction's own
/// install: every snapshot taken AFTER the install sees post-compaction
/// data, which is complete, so the boundary is the install seqno and the
/// counter's next snapshots stay readable after a reopen.
#[test]
fn gc_watermark_above_the_counter_is_capped_at_the_install() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(folder.path(), false, &seqno)?;

    let first = seqno.next();
    tree.insert("k", "v1", first);
    tree.flush_active_memtable(0)?;
    tree.insert("k", "v2", seqno.next());
    tree.flush_active_memtable(0)?;
    // The compaction's install takes the next seqno.
    let install = seqno.get();
    tree.major_compact(common::COMPACTION_TARGET, SeqNo::MAX)?;
    assert_eq!(
        tree.retention_floor(),
        install,
        "the persisted boundary is capped at the install"
    );
    drop(tree);

    let tree = reopen(folder.path(), false, seqno.get())?;
    let oldest = tree.oldest_retained_seqno();
    assert_eq!(oldest, install);
    assert_below_retention(tree.get("k", first + 1).unwrap_err(), first + 1, oldest);
    assert_eq!(
        tree.get("k", install + 1)?.as_deref(),
        Some(b"v2".as_slice()),
        "the first snapshot after the install is served"
    );
    // The counter's own next snapshot, the normal read seqno, is above it.
    assert_eq!(
        tree.get("k", seqno.get())?.as_deref(),
        Some(b"v2".as_slice())
    );
    Ok(())
}

/// A compaction filter removes or rewrites rows regardless of the GC
/// watermark, so a filtering compaction discards what older snapshots saw
/// even at watermark `0`: its install seqno becomes the boundary, which the
/// live tree and the reopened one both enforce.
#[test]
fn compaction_filter_rewrite_raises_the_boundary_to_its_install() -> lsm_tree::Result<()> {
    use lsm_tree::compaction::filter::{
        CompactionFilter, Context as FilterContext, Factory, ItemAccessor, Verdict,
    };

    struct NukeFilter;
    impl CompactionFilter for NukeFilter {
        fn filter_item(
            &mut self,
            _: ItemAccessor<'_>,
            _: &FilterContext,
        ) -> lsm_tree::Result<Verdict> {
            Ok(Verdict::Remove)
        }
    }
    struct NukeFactory;
    impl Factory for NukeFactory {
        fn name(&self) -> &str {
            "Nuke"
        }
        fn make_filter(&self, _: &FilterContext) -> Box<dyn CompactionFilter> {
            Box::new(NukeFilter)
        }
    }

    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let open = |next: SeqNo| {
        Config::new(
            folder.path(),
            SequenceNumberCounter::new(next),
            SequenceNumberCounter::default(),
        )
        .with_compaction_filter_factory(Some(Arc::new(NukeFactory)))
        .open()
    };
    let tree = Config::new(
        folder.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .with_compaction_filter_factory(Some(Arc::new(NukeFactory)))
    .open()?;

    tree.insert("a", "a", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.insert("b", "b", seqno.next());
    tree.flush_active_memtable(0)?;
    let snapshot = seqno.get();

    let install = seqno.get();
    tree.major_compact(common::COMPACTION_TARGET, 0)?;
    // Refused at once: no pre-compaction version is kept to serve it.
    assert_below_retention(tree.get("a", snapshot).unwrap_err(), snapshot, install);
    assert!(
        tree.get("a", SeqNo::MAX)?.is_none(),
        "the filter removed it"
    );
    assert_eq!(tree.retention_floor(), install);
    drop(tree);

    let tree = open(seqno.get())?;
    let oldest = tree.oldest_retained_seqno();
    assert_eq!(oldest, install, "the filter's install is the boundary");
    assert_below_retention(tree.get("a", snapshot).unwrap_err(), snapshot, oldest);
    assert!(tree.get("a", SeqNo::MAX)?.is_none());
    Ok(())
}

/// A `drop_range` that finds no table fully inside its bounds installs
/// nothing and must not raise the boundary: no data changed, so every
/// snapshot stays readable after a reopen.
#[test]
fn drop_range_without_a_contained_table_keeps_the_boundary() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(folder.path(), false, &seqno)?;

    let first = seqno.next();
    tree.insert("a", "va", first);
    tree.insert("z", "vz", seqno.next());
    // One table spanning a..z: no range short of the whole span contains it.
    tree.flush_active_memtable(0)?;
    let snapshot = seqno.get();

    tree.drop_range("m"..="n")?;
    assert_eq!(tree.table_count(), 1, "nothing was dropped");
    assert_eq!(
        tree.retention_floor(),
        0,
        "a no-op drop leaves the boundary"
    );
    drop(tree);

    let tree = reopen(folder.path(), false, seqno.get())?;
    assert_eq!(tree.oldest_retained_seqno(), 0);
    assert_eq!(tree.get("a", snapshot)?.as_deref(), Some(b"va".as_slice()));
    assert_eq!(tree.get("a", first + 1)?.as_deref(), Some(b"va".as_slice()));
    Ok(())
}

/// `retention_floor()` reports the persisted boundary, which is also the live
/// one `oldest_retained_seqno()` reports, and is what a deployment records for
/// a later repair.
#[test]
fn retention_floor_reports_the_persisted_boundary() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(folder.path(), false, &seqno)?;
    assert_eq!(tree.retention_floor(), 0);

    tree.insert("a", "va", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.insert("b", "vb", seqno.next());
    tree.flush_active_memtable(0)?;
    let snapshot = seqno.get();

    let install = seqno.get();
    tree.drop_range("a"..="a")?;
    assert_eq!(
        tree.retention_floor(),
        install,
        "persisted: the drop's install"
    );
    assert_eq!(
        tree.oldest_retained_seqno(),
        install,
        "live: the same boundary"
    );
    assert_below_retention(tree.get("b", snapshot).unwrap_err(), snapshot, install);

    // A later GC compaction below that install does not lower it.
    tree.insert("b", "vb2", seqno.next());
    tree.flush_active_memtable(0)?;
    let watermark = seqno.get();
    tree.major_compact(common::COMPACTION_TARGET, watermark)?;
    assert_eq!(
        tree.retention_floor(),
        watermark - 1,
        "raised by the compaction"
    );
    drop(tree);

    let tree = reopen(folder.path(), false, seqno.get())?;
    assert_eq!(tree.oldest_retained_seqno(), watermark - 1);
    assert_eq!(tree.retention_floor(), watermark - 1);
    Ok(())
}

/// With the edit log rotating on every install, the boundary reaches disk
/// through the snapshot's own section rather than an appended edit; the
/// reopen must read it from there.
#[test]
fn boundary_survives_reopen_through_manifest_rotation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        // Any non-empty log is past the cap: every install writes a fresh
        // snapshot and starts an empty log.
        .manifest_log_rotate_bytes(1)
        .open()?;

    let first = seqno.next();
    tree.insert("k", "v1", first);
    tree.flush_active_memtable(0)?;
    tree.insert("k", "v2", seqno.next());
    tree.flush_active_memtable(0)?;
    let watermark = seqno.get();
    tree.major_compact(common::COMPACTION_TARGET, watermark)?;
    // One more install after the compaction, so the snapshot the reopen
    // loads was written by an install that did NOT raise the floor and
    // must carry it forward.
    tree.insert("z", "v", seqno.next());
    tree.flush_active_memtable(0)?;
    drop(tree);

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::new(seqno.get()),
        SequenceNumberCounter::default(),
    )
    .manifest_log_rotate_bytes(1)
    .open()?;
    let oldest = tree.oldest_retained_seqno();
    assert_eq!(oldest, watermark - 1);
    assert_below_retention(tree.get("k", first + 1).unwrap_err(), first + 1, oldest);
    assert_eq!(tree.get("k", watermark)?.as_deref(), Some(b"v2".as_slice()));
    Ok(())
}

/// The boundary keeps rising across restarts: a second GC compaction after a
/// reopen moves it past the first one's watermark, and the next reopen
/// reports the new boundary, refusing what the first still served.
#[test]
fn boundary_rises_across_a_reopen_chain() -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        watermark: first_watermark,
        seqno,
        folder,
        ..
    } = pruned_tree(false)?;
    drop(tree);

    let tree = reopen(folder.path(), false, seqno.get())?;
    assert_eq!(tree.oldest_retained_seqno(), first_watermark - 1);
    assert_eq!(
        tree.get("k", first_watermark)?.as_deref(),
        Some(b"v2".as_slice())
    );
    tree.insert("k", "v3", seqno.next());
    tree.flush_active_memtable(0)?;
    let second_watermark = seqno.get();
    tree.major_compact(common::COMPACTION_TARGET, second_watermark)?;
    drop(tree);

    let tree = reopen(folder.path(), false, seqno.get())?;
    let oldest = tree.oldest_retained_seqno();
    assert_eq!(oldest, second_watermark - 1, "the later watermark wins");
    assert_below_retention(
        tree.get("k", first_watermark).unwrap_err(),
        first_watermark,
        oldest,
    );
    assert_eq!(
        tree.get("k", second_watermark)?.as_deref(),
        Some(b"v3".as_slice())
    );
    Ok(())
}

/// A deployment that reopens with a counter BELOW the persisted boundary
/// violates the seqno contract, but the boundary must still hold: new
/// versions may not slip under it and let a read below the boundary find a
/// "newer" version with a smaller seqno.
#[test]
fn reset_counter_after_reopen_cannot_bypass_boundary() -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        first,
        watermark,
        folder,
        ..
    } = pruned_tree(false)?;
    drop(tree);

    // Counter restarted at 0: the next install would be allocated seqno 0.
    let tree = reopen(folder.path(), false, 0)?;
    let oldest = tree.oldest_retained_seqno();
    tree.insert("x", "y", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        tree.oldest_retained_seqno(),
        oldest,
        "an install below the boundary does not lower it"
    );
    let below = first + 1;
    assert_below_retention(tree.get("k", below).unwrap_err(), below, oldest);
    assert_below_retention(tree.get("k", oldest).unwrap_err(), oldest, oldest);
    assert_eq!(tree.get("k", watermark)?.as_deref(), Some(b"v2".as_slice()));
    Ok(())
}

/// A checkpoint re-serialises the captured version, so it carries the
/// boundary along: the restored tree refuses the same snapshots.
#[cfg(feature = "std")]
fn assert_checkpoint_carries_retention_boundary(kv_separated: bool) -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        first,
        watermark,
        seqno,
        folder: _folder,
        ..
    } = pruned_tree(kv_separated)?;
    let dst = get_tmp_folder();
    let dst_path = dst.path().join("checkpoint");
    tree.create_checkpoint(&dst_path)?;

    let restored = reopen(&dst_path, kv_separated, seqno.get())?;
    let oldest = restored.oldest_retained_seqno();
    assert_eq!(oldest, watermark - 1);
    let below = first + 1;
    assert_below_retention(restored.get("k", below).unwrap_err(), below, oldest);
    assert_eq!(
        restored.get("k", watermark)?.as_deref(),
        Some(b"v2".as_slice())
    );
    Ok(())
}

#[cfg(feature = "std")]
#[test]
fn checkpoint_carries_retention_boundary() -> lsm_tree::Result<()> {
    assert_checkpoint_carries_retention_boundary(false)
}

#[cfg(feature = "std")]
#[test]
fn blob_tree_checkpoint_carries_retention_boundary() -> lsm_tree::Result<()> {
    assert_checkpoint_carries_retention_boundary(true)
}

/// A manifest rebuilt from the tables alone cannot know which snapshots a
/// past compaction invalidated (the compaction zeroed the settled rows'
/// seqnos, so the tables do not even record how high history went), so the
/// deployment that ran the compaction supplies the boundary to the repair:
/// the rebuilt manifest carries it, and the reopened tree refuses the same
/// snapshots the tree refused before the manifest was lost.
#[cfg(feature = "std")]
fn assert_repair_seeds_the_configured_boundary(kv_separated: bool) -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        first,
        watermark,
        seqno,
        folder,
        ..
    } = pruned_tree(kv_separated)?;
    drop(tree);

    common::nuke_manifest(folder.path())?;
    let next = seqno.get();
    let mut config = Config::new(
        folder.path(),
        SequenceNumberCounter::new(next),
        SequenceNumberCounter::default(),
    )
    .repair_retention_floor(watermark - 1);
    if kv_separated {
        config = config.with_kv_separation(Some(Default::default()));
    }
    config.repair()?;

    let tree = reopen(folder.path(), kv_separated, next)?;
    let oldest = tree.oldest_retained_seqno();
    assert_eq!(
        oldest,
        watermark - 1,
        "repair seeds the boundary the caller supplied"
    );
    let below = first + 1;
    assert_below_retention(tree.get("k", below).unwrap_err(), below, oldest);
    assert_below_retention(tree.get("k", oldest).unwrap_err(), oldest, oldest);
    assert_eq!(
        tree.get("k", oldest + 1)?.as_deref(),
        Some(b"v2".as_slice()),
        "the first snapshot above the boundary reads the latest value"
    );
    assert_eq!(
        tree.get("k", SeqNo::MAX)?.as_deref(),
        Some(b"v2".as_slice())
    );
    Ok(())
}

#[cfg(feature = "std")]
#[test]
fn repair_seeds_the_configured_boundary() -> lsm_tree::Result<()> {
    assert_repair_seeds_the_configured_boundary(false)
}

#[cfg(feature = "std")]
#[test]
fn blob_tree_repair_seeds_the_configured_boundary() -> lsm_tree::Result<()> {
    assert_repair_seeds_the_configured_boundary(true)
}

/// Without a configured floor a repair serves every snapshot: the rebuilt
/// manifest has no evidence of collected history, and the external-WAL
/// reconciliation that follows a repair reads intermediate snapshots back,
/// so the engine must not guess a boundary the caller did not state.
#[cfg(feature = "std")]
#[test]
fn repair_without_a_configured_boundary_serves_every_snapshot() -> lsm_tree::Result<()> {
    let PrunedTree {
        tree,
        first,
        seqno,
        folder,
        ..
    } = pruned_tree(false)?;
    drop(tree);

    common::nuke_manifest(folder.path())?;
    let next = seqno.get();
    Config::new(
        folder.path(),
        SequenceNumberCounter::new(next),
        SequenceNumberCounter::default(),
    )
    .repair()?;

    let tree = reopen(folder.path(), false, next)?;
    assert_eq!(tree.oldest_retained_seqno(), 0);
    // The compaction dropped `v1` and zeroed `v2`'s seqno, so the snapshot
    // that saw `v1` now reads `v2`: exactly the answer the configured floor
    // exists to refuse. Pinned so the default's meaning stays explicit.
    assert_eq!(tree.get("k", first + 1)?.as_deref(), Some(b"v2".as_slice()));
    Ok(())
}
