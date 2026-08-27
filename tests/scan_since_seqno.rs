// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end coverage for `Tree::scan_since_seqno` (CDC event stream):
//! target filtering, increasing-seqno replay order, event-type mapping
//! (Insert / PointTombstone / RangeTombstone), coverage across memtable and
//! SSTs, the per-block seqno-bounds block-skip path, and mixed trees (a tree
//! that holds both SSTs with a `seqno_bounds` section and SSTs without one
//! must scan correctly).

use lsm_tree::{
    AbstractTree, AnyTree, BlobTree, Config, ScanSinceEvent, SeqNo, SequenceNumberCounter, Tree,
    get_tmp_folder,
};
use test_log::test;

fn open_tree(path: &std::path::Path) -> lsm_tree::Result<Tree> {
    let any = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    Ok(match any {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree, got Blob"),
    })
}

fn events(tree: &Tree, target: SeqNo) -> lsm_tree::Result<Vec<ScanSinceEvent>> {
    Ok(tree.scan_since_seqno(target)?.collect())
}

fn open_blob_tree(path: &std::path::Path) -> lsm_tree::Result<BlobTree> {
    let any = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(lsm_tree::KvSeparationOptions::default()))
    .open()?;

    Ok(match any {
        AnyTree::Blob(t) => t,
        AnyTree::Standard(_) => panic!("expected Blob tree, got Standard"),
    })
}

#[test]
fn scan_since_returns_only_entries_at_or_after_target() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    for i in 0..10u64 {
        tree.insert(format!("k{i:02}").as_bytes(), b"v", i);
    }
    tree.flush_active_memtable(0)?;

    let got = events(&tree, 5)?;
    assert_eq!(got.len(), 5, "only seqnos 5..10 qualify");
    assert!(
        got.iter().all(|e| e.seqno() >= 5),
        "no event below the target seqno may be emitted",
    );
    Ok(())
}

#[test]
fn scan_since_emits_events_in_increasing_seqno_order() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    // Insert in a deliberately scrambled key order; seqnos still rise.
    for (i, k) in ["m", "a", "z", "c", "q"].iter().enumerate() {
        tree.insert(k.as_bytes(), b"v", i as u64);
    }
    tree.flush_active_memtable(0)?;

    let seqnos: Vec<SeqNo> = events(&tree, 0)?
        .iter()
        .map(ScanSinceEvent::seqno)
        .collect();
    let mut sorted = seqnos.clone();
    sorted.sort_unstable();
    assert_eq!(
        seqnos, sorted,
        "events must arrive in increasing seqno order"
    );
    Ok(())
}

#[test]
fn scan_since_maps_value_and_point_tombstone() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    tree.insert(b"key", b"val", 0);
    tree.remove(b"key", 1);
    tree.flush_active_memtable(0)?;

    let got = events(&tree, 0)?;
    assert_eq!(got.len(), 2, "the write and the delete are distinct events");

    // Replay order: insert before delete.
    match &got[0] {
        ScanSinceEvent::Insert { key, value, seqno } => {
            assert_eq!(&**key, b"key");
            assert_eq!(&**value, b"val");
            assert_eq!(*seqno, 0);
        }
        other => panic!("expected Insert first, got {other:?}"),
    }
    match &got[1] {
        ScanSinceEvent::PointTombstone { key, seqno } => {
            assert_eq!(&**key, b"key");
            assert_eq!(*seqno, 1);
        }
        other => panic!("expected PointTombstone second, got {other:?}"),
    }
    Ok(())
}

#[test]
fn scan_since_emits_range_tombstone() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    tree.insert(b"a", b"v", 0);
    tree.remove_range(b"a", b"m", 1);
    tree.flush_active_memtable(0)?;

    let got = events(&tree, 0)?;
    let range = got
        .iter()
        .find_map(|e| match e {
            ScanSinceEvent::RangeTombstone {
                start_key,
                end_key,
                seqno,
            } => Some((start_key.to_vec(), end_key.to_vec(), *seqno)),
            _ => None,
        })
        .expect("a RangeTombstone event must be emitted");
    assert_eq!(range.0, b"a");
    assert_eq!(range.1, b"m");
    assert_eq!(range.2, 1);
    Ok(())
}

#[test]
fn scan_since_spans_memtable_and_sst() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    // Flushed to an SST.
    for i in 0..5u64 {
        tree.insert(format!("s{i}").as_bytes(), b"v", i);
    }
    tree.flush_active_memtable(0)?;

    // Still in the active memtable.
    for i in 5..10u64 {
        tree.insert(format!("m{i}").as_bytes(), b"v", i);
    }

    let got = events(&tree, 0)?;
    assert_eq!(
        got.len(),
        10,
        "scan must cover both the flushed SST and the live memtable",
    );
    Ok(())
}

#[test]
fn scan_since_block_skip_on_seqno_indexed_sst() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;
    tree.update_runtime_config(|cfg| {
        cfg.seqno_in_index = true;
    })?;

    // Enough keys to spill multiple data blocks so per-block bounds matter.
    for i in 0..500u64 {
        tree.insert(format!("key{i:05}").as_bytes(), b"v", i);
    }
    tree.flush_active_memtable(0)?;

    let data_blocks: u64 = tree
        .current_version()
        .iter_tables()
        .map(|t| t.metadata.data_block_count)
        .sum();
    assert!(data_blocks > 1, "need >1 data block to exercise block-skip");

    let got = events(&tree, 450)?;
    assert_eq!(got.len(), 50, "only seqnos 450..500 qualify");
    assert!(got.iter().all(|e| e.seqno() >= 450));
    Ok(())
}

#[test]
fn scan_since_mixed_format_tree_scans_correctly() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    // First SST: no seqno_bounds section (seqno_in_index defaults off).
    for i in 0..250u64 {
        tree.insert(format!("key{i:05}").as_bytes(), b"v", i);
    }
    tree.flush_active_memtable(0)?;

    // Toggle on, second SST: emits a seqno_bounds section.
    tree.update_runtime_config(|cfg| {
        cfg.seqno_in_index = true;
    })?;
    for i in 250..500u64 {
        tree.insert(format!("key{i:05}").as_bytes(), b"v", i);
    }
    tree.flush_active_memtable(0)?;

    // Target straddles both SSTs: the legacy one falls back to a full filter,
    // the seqno-bounded one uses block-skip; the union must be exact.
    let got = events(&tree, 200)?;
    assert_eq!(got.len(), 300, "seqnos 200..500 across both formats");
    assert!(got.iter().all(|e| e.seqno() >= 200));
    let seqnos: Vec<SeqNo> = got.iter().map(ScanSinceEvent::seqno).collect();
    let mut sorted = seqnos.clone();
    sorted.sort_unstable();
    assert_eq!(seqnos, sorted, "merged output stays seqno-ordered");
    Ok(())
}

#[test]
fn scan_since_resolves_blob_values_on_blob_tree() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_blob_tree(folder.path())?;

    // A large value is stored out-of-line in a blob file (KV-separation); the
    // index entry becomes an indirection pointer. A small value stays inline.
    let big = b"blobby".repeat(40_000);
    tree.insert(b"big", &big, 0);
    tree.insert(b"small", b"inline", 1);
    tree.flush_active_memtable(0)?;
    assert!(
        tree.blob_file_count() > 0,
        "the big value must be separated"
    );

    let got: Vec<ScanSinceEvent> = tree.scan_since_seqno(0)?.collect();

    // The blob-indirected entry must come back as an Insert carrying the real
    // resolved value, not a pointer.
    let big_value = got
        .iter()
        .find_map(|e| match e {
            ScanSinceEvent::Insert { key, value, .. } if &**key == b"big" => Some(value.to_vec()),
            _ => None,
        })
        .expect("an Insert for the blob-separated key must be emitted");
    assert_eq!(big_value, big, "blob value must be resolved, not a handle");

    let small_value = got.iter().find_map(|e| match e {
        ScanSinceEvent::Insert { key, value, .. } if &**key == b"small" => Some(value.to_vec()),
        _ => None,
    });
    assert_eq!(small_value.as_deref(), Some(b"inline" as &[u8]));
    Ok(())
}

#[test]
fn scan_since_seqno_translates_ingested_global_seqno() -> lsm_tree::Result<()> {
    // Bulk-ingested tables store entries at LOCAL seqno 0 but carry a
    // global_seqno offset; reads translate (target down, results up) the way
    // Table::get does. scan_since_seqno must do the same: comparing a global
    // target against the table's LOCAL seqno bounds would block-skip the whole
    // table (local max 0 < global target) and silently drop ingested changes.
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let visible = SequenceNumberCounter::default();
    let any = Config::new(folder.path(), seqno.clone(), visible.clone()).open()?;
    // `ingestion()` lives on AnyTree; `scan_since_seqno` on the concrete Tree.
    // Tree is an Arc handle, so this clone shares state with `any`.
    let tree = match &any {
        AnyTree::Standard(t) => t.clone(),
        AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    // A regular flushed table at global_seqno 0.
    let s0 = seqno.next();
    tree.insert(b"x", b"x", s0);
    visible.fetch_max(s0 + 1);
    tree.flush_active_memtable(0)?;

    // Bulk-ingest: ingested table carries a nonzero global_seqno offset.
    let global = seqno.get();
    assert!(
        global > 0,
        "ingest must carry a nonzero global_seqno offset"
    );
    let mut ing = any.ingestion()?;
    ing.write("a", "a")?;
    ing.finish()?;

    // Scanning at the ingest's global seqno must surface the ingested entry,
    // in GLOBAL coordinates.
    let events: Vec<ScanSinceEvent> = tree.scan_since_seqno(global)?.collect();
    let a = events
        .iter()
        .find(|e| matches!(e, ScanSinceEvent::Insert { key, .. } if &**key == b"a"));
    assert!(
        a.is_some(),
        "ingested entry must not be block-skipped by a global/local seqno mismatch",
    );
    assert_eq!(
        a.unwrap().seqno(),
        global,
        "event seqno must be reported in GLOBAL coordinates",
    );
    Ok(())
}

#[test]
fn scan_since_caught_up_target_returns_empty() -> lsm_tree::Result<()> {
    // A caught-up CDC poller scans at (or beyond) the current watermark; the
    // window is empty and the scan must return nothing without re-reading the
    // legacy/mixed-format portion of the tree.
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    for i in 0..10u64 {
        tree.insert(format!("k{i:02}").as_bytes(), b"v", i);
    }
    tree.flush_active_memtable(0)?;

    // Highest seqno present is 9; scanning since 10 (one past it) yields nothing.
    assert!(
        events(&tree, 10)?.is_empty(),
        "an empty seqno window must yield no events"
    );
    assert!(
        events(&tree, 100)?.is_empty(),
        "a far-future target must yield no events"
    );
    Ok(())
}

// ---- Corruption matrix (#224) ---------------------------------------------

/// The paranoid full-scan variant disables the per-block seqno-bounds skip but
/// must return byte-identical results to the fast block-skip path. This proves
/// the no-skip path is correct (and, by extension, that a hypothetical
/// undetected-corrupt bound which made the fast path skip a block could only
/// ever cause a missed record, which the full scan recovers).
#[test]
fn scan_since_full_scan_matches_block_skip() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;
    tree.update_runtime_config(|cfg| {
        cfg.seqno_in_index = true;
    })?;
    for i in 0..500u64 {
        tree.insert(format!("key{i:05}").as_bytes(), b"v", i);
    }
    tree.flush_active_memtable(0)?;

    for target in [0u64, 123, 450, 499, 500] {
        let fast: Vec<SeqNo> = events(&tree, target)?
            .iter()
            .map(ScanSinceEvent::seqno)
            .collect();
        let full: Vec<SeqNo> = tree
            .scan_since_seqno_full_scan(target)?
            .map(|e| e.seqno())
            .collect();
        assert_eq!(
            fast, full,
            "paranoid full scan must equal block-skip scan at target {target}",
        );
    }
    Ok(())
}

/// A bit-flip in a sub-index block must be caught by the index block's XXH3-128
/// on the scan's index walk, not silently trusted: the seqno-scoped scan still
/// reads the full index to enumerate data blocks, so a corrupt sub-index block
/// must surface as an error. Forces a partitioned index so
/// `read_top_level_index_entries` yields multiple sub-index blocks.
#[test]
fn scan_since_seqno_index_corruption_is_caught() -> lsm_tree::Result<()> {
    use lsm_tree::config::{BlockSizePolicy, PinningPolicy};
    use lsm_tree::inspect::read_top_level_index_entries;
    use lsm_tree::runtime_config::RuntimeConfig;
    use std::io::{Seek, Write};

    // Runtime config at open: enable the seqno_bounds section + force the index
    // to partition (zero spill threshold) so the index spills into multiple
    // checksummed sub-index blocks.
    let mut rc = RuntimeConfig::default();
    rc.seqno_in_index = true;
    rc.index_partition_spill_threshold = 0;

    let folder = get_tmp_folder();
    {
        let any = Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .index_block_partitioning_policy(PinningPolicy::all(true))
        .data_block_size_policy(BlockSizePolicy::all(256))
        .with_runtime_config(rc)
        .open()?;
        let tree = match any {
            AnyTree::Standard(t) => t,
            AnyTree::Blob(_) => panic!("expected Standard tree"),
        };
        // Large corpus → many small data blocks → many index handles → multiple
        // sub-index partitions (the partition budget is ~4 KiB).
        for i in 0..30_000u64 {
            tree.insert(format!("key{i:08}").as_bytes(), b"v", i);
        }
        tree.flush_active_memtable(0)?;
    }

    // Locate the single SST and a sub-index block to corrupt.
    let sst = std::fs::read_dir(folder.path().join("tables"))
        .expect("tables dir")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| p.is_file())
        .expect("one SST file");
    let tli = read_top_level_index_entries(&sst).expect("read top-level index");
    assert!(
        tli.len() > 1,
        "test needs a partitioned index (>1 sub-index block), got {}",
        tli.len(),
    );
    let victim = &tli[0];
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&sst)
            .expect("open sst");
        f.seek(std::io::SeekFrom::Start(victim.offset))
            .expect("seek");
        f.write_all(&vec![0xFF; victim.size as usize])
            .expect("flip");
        f.sync_all().expect("sync");
    }

    // Reopen and scan: the corrupted sub-index block fails its checksum.
    let tree = open_tree(folder.path())?;
    let res: lsm_tree::Result<Vec<ScanSinceEvent>> =
        tree.scan_since_seqno(0).map(Iterator::collect);
    assert!(
        res.is_err(),
        "a corrupted sub-index block must be caught by XXH3 on the scan, not trusted",
    );
    Ok(())
}

/// The retrieval-ribbon locator (point-read fast path) and the `seqno_bounds`
/// section (scan_since_seqno block-skip) are independent optional SST sections.
/// An SST that carries BOTH must serve both paths correctly: point reads via the
/// locator and seqno-scoped scans via the block-skip, neither perturbing the
/// other. Locks down that the two features compose.
#[test]
fn scan_since_with_locator_enabled_is_correct() -> lsm_tree::Result<()> {
    use lsm_tree::config::{LocatorPolicy, LocatorPolicyEntry, LocatorPrecision};

    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    // Many small data blocks so both the locator's block_id space and the
    // seqno-bounds block-skip are non-trivial.
    .data_block_size_policy(lsm_tree::config::BlockSizePolicy::all(4_096))
    .locator_policy(LocatorPolicy::all(LocatorPolicyEntry::Enabled {
        precision: LocatorPrecision::Block,
        block_id_bits: None,
        slot_bits: None,
    }))
    .open()?;
    let tree = match any {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    // Enable the seqno_bounds section too, so the flushed SST carries BOTH.
    tree.update_runtime_config(|cfg| {
        cfg.seqno_in_index = true;
    })?;

    // Fixed-width big-endian keys (the locator's proven corpus shape) so the
    // ribbon is well-conditioned; rising seqnos so block-skip has work to do.
    for i in 0..1_500u64 {
        tree.insert(i.to_be_bytes(), format!("v{i:05}").as_bytes(), i);
    }
    tree.flush_active_memtable(0)?;

    // Point reads resolve through the locator and must be exact.
    for i in 0..1_500u64 {
        let got = tree.get(i.to_be_bytes(), SeqNo::MAX)?;
        assert_eq!(
            got.as_deref(),
            Some(format!("v{i:05}").as_bytes()),
            "locator point read of key {i} must be exact",
        );
    }

    // Seqno-scoped scans block-skip via the seqno_bounds section; the fast path
    // must equal the paranoid full scan at every target despite the locator
    // also being present in the same SST.
    for target in [0u64, 500, 1_234, 1_499, 1_500] {
        let fast: Vec<SeqNo> = events(&tree, target)?
            .iter()
            .map(ScanSinceEvent::seqno)
            .collect();
        let full: Vec<SeqNo> = tree
            .scan_since_seqno_full_scan(target)?
            .map(|e| e.seqno())
            .collect();
        assert_eq!(
            fast, full,
            "block-skip scan must equal full scan at target {target} with the locator enabled",
        );
    }
    Ok(())
}

/// One committed change must be emitted ONCE even when the record physically
/// lives in two published tables. That state is real: a manifest-loss repair
/// publishes every surviving SST as its own L0 run, including both the inputs
/// and outputs of a compaction that crashed before deleting its inputs (or a
/// tight-space input whose restriction sidecar failed to persist alongside
/// the slice output that re-emitted its prefix). The copies are byte-identical
/// (same key, value, and seqno), so the scanner deduplicates them; without
/// that, a CDC consumer replays every affected change twice.
#[test]
fn scan_since_deduplicates_identical_events_from_duplicated_tables() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    // Two SSTs carrying the SAME committed changes — the post-repair shape of
    // an input + output pair.
    tree.insert(b"dup", b"v1", 10);
    tree.remove_range(b"a", b"m", 11);
    tree.flush_active_memtable(0)?;
    tree.insert(b"dup", b"v1", 10);
    tree.remove_range(b"a", b"m", 11);
    tree.flush_active_memtable(0)?;

    // A distinct change with the SAME seqno as the duplicated insert (a write
    // batch commits many keys under one seqno) must NOT be collapsed.
    tree.insert(b"other", b"v2", 10);
    tree.flush_active_memtable(0)?;

    let got = events(&tree, 0)?;
    assert_eq!(
        got.len(),
        3,
        "the duplicated insert and range tombstone are each emitted once, \
         the distinct same-seqno insert stays: {got:?}",
    );
    assert_eq!(
        got.iter()
            .filter(|e| matches!(e, ScanSinceEvent::Insert { key, .. } if key.as_ref() == b"dup"))
            .count(),
        1,
        "the byte-identical duplicated insert must be deduplicated: {got:?}",
    );
    assert_eq!(
        got.iter()
            .filter(|e| matches!(e, ScanSinceEvent::RangeTombstone { .. }))
            .count(),
        1,
        "the byte-identical duplicated range tombstone must be deduplicated: {got:?}",
    );
    Ok(())
}

/// A range deletion does NOT suppress an entry at its own sequence number —
/// suppression is strictly `entry.seqno < tombstone.seqno` — so the tree keeps
/// such a write visible. A replay that applied the deletion last would drop it,
/// so tied range deletions have to be emitted BEFORE the writes they cover.
#[test]
fn scan_since_replays_a_tied_range_tombstone_before_the_write_it_spares() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    // One seqno for both operations, which `apply_batch`-style callers may do.
    tree.insert(b"k", b"v", 10);
    tree.remove_range(b"a", b"z", 10);

    assert_eq!(
        tree.get(b"k", SeqNo::MAX)?.as_deref(),
        Some(b"v".as_slice()),
        "the tree keeps a write the tied range deletion does not suppress",
    );

    let kinds: Vec<_> = events(&tree, 0)?
        .iter()
        .map(|e| match e {
            ScanSinceEvent::RangeTombstone { .. } => "range",
            ScanSinceEvent::Insert { .. } => "insert",
            _ => "other",
        })
        .collect();
    assert_eq!(
        kinds,
        vec!["range", "insert"],
        "the deletion must be replayed first, or the consumer drops a key the \
         tree still serves",
    );
    Ok(())
}

/// The same tie, but with the two operations in DIFFERENT sources. Source
/// recency decides the order of the remaining events at a seqno, and a range
/// deletion sitting in the NEWER source would then be replayed after an insert
/// from the older one — dropping a key the tree still serves, since suppression
/// is strictly `entry.seqno < tombstone.seqno`.
#[test]
fn scan_since_replays_a_tied_range_tombstone_first_across_sources() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    tree.insert(b"k", b"v", 10);
    tree.insert(b"a", b"v", 10);
    tree.flush_active_memtable(0)?;
    tree.remove_range(b"a", b"z", 10);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        tree.get(b"k", SeqNo::MAX)?.as_deref(),
        Some(b"v".as_slice()),
        "the tree keeps a write the tied range deletion does not suppress",
    );
    assert_eq!(
        tree.get(b"a", SeqNo::MAX)?,
        None,
        "at the range's START key the newer source's own entry wins the tie \
         instead, so the stream has to converge to a deletion there",
    );

    let all = events(&tree, 0)?;
    let kinds: Vec<_> = all
        .iter()
        .map(|e| match e {
            ScanSinceEvent::RangeTombstone { .. } => "range",
            ScanSinceEvent::Insert { .. } => "insert",
            ScanSinceEvent::PointTombstone { .. } => "point",
            ScanSinceEvent::WeakTombstone { .. } => "weak",
            ScanSinceEvent::MergeOperand { .. } => "merge",
        })
        .collect();
    assert_eq!(
        kinds,
        vec!["range", "insert", "insert", "weak"],
        "a tied range deletion is replayed first no matter which source holds \
         it, or the consumer drops a key the tree still serves; the newer \
         source's tied weak-tombstone sentinel still lands last: {all:?}",
    );
    Ok(())
}

/// Replay order must reproduce the tree's own precedence. Two sources can hold
/// DIFFERENT values for one key at one sequence number — `apply_batch` takes a
/// caller-chosen seqno and does not require it to be unique — and the tree
/// serves the newer source's value. A consumer replaying the events in order
/// keeps whichever arrives last, so the newer source's event has to arrive last;
/// ordering tied seqnos by payload bytes would decide it alphabetically.
#[test]
fn scan_since_replays_tied_seqnos_in_source_order() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    tree.insert(b"k", b"z", 10);
    tree.flush_active_memtable(0)?;
    tree.insert(b"k", b"a", 10);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        tree.get(b"k", SeqNo::MAX)?.as_deref(),
        Some(b"a".as_slice()),
        "the newer run is what the tree serves",
    );

    let values: Vec<_> = events(&tree, 0)?
        .iter()
        .filter_map(|e| match e {
            ScanSinceEvent::Insert { value, .. } => Some(value.to_vec()),
            _ => None,
        })
        .collect();
    assert_eq!(
        values,
        vec![b"z".to_vec(), b"a".to_vec()],
        "the value the tree serves must be replayed LAST, or the consumer ends \
         up with the superseded one",
    );
    Ok(())
}

/// Merge operands are never collapsed across sources either. The read path
/// applies EVERY physically stored operand for a key (it collects them without
/// deduplicating by seqno), so two operands in two sources are two applications
/// — even at the same caller-supplied seqno, which `apply_batch` does not
/// require to be unique. A change feed that emitted one would diverge from the
/// tree's own reads. Idempotent event kinds (a write, a deletion) still
/// collapse: replaying them twice reaches the same state, and the read path
/// shadows the copies anyway.
#[test]
fn scan_since_keeps_merge_operands_from_two_sources_at_one_seqno() -> lsm_tree::Result<()> {
    use lsm_tree::WriteBatch;

    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    // Two SEPARATE batches under the same caller-chosen seqno, flushed apart:
    // each source legitimately holds one operand, and a read applies both.
    for _ in 0..2 {
        let mut batch = WriteBatch::new();
        batch.merge(b"counter".as_slice(), b"+1".as_slice());
        tree.apply_batch(batch, 10)?;
        tree.flush_active_memtable(0)?;
    }

    let got = events(&tree, 0)?;
    assert_eq!(
        got.iter()
            .filter(|e| matches!(e, ScanSinceEvent::MergeOperand { key, .. } if key.as_ref() == b"counter"))
            .count(),
        2,
        "each source's operand is a separate application: collapsing them \
         replays one merge where the tree applies two: {got:?}",
    );
    Ok(())
}

/// An entry written at the maximum sequence number must be delivered from an
/// SST exactly as it is from a memtable. The watermark is derived from the
/// data, so it IS `SeqNo::MAX` here, and an exclusive upper bound would drop
/// the very entry that defined it — the event would be visible before a flush
/// and vanish after one.
#[test]
fn scan_since_delivers_an_entry_at_the_maximum_seqno_from_an_sst() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    tree.insert(b"top", b"v", SeqNo::MAX);
    let before_flush = events(&tree, 0)?.len();
    assert_eq!(before_flush, 1, "the memtable delivers it");

    tree.flush_active_memtable(0)?;
    let got = events(&tree, 0)?;
    assert_eq!(got.len(), 1, "the flushed SST must deliver it too: {got:?}");
    Ok(())
}

/// Deduplication must not collapse events a single source genuinely holds
/// more than once. A write batch may carry the same merge operand for a key
/// twice; both are stored, both are applied on read, and both must reach a
/// consumer — replaying one merge where the source applied two diverges the
/// replica. Identical copies across DIFFERENT sources are still collapsed
/// (that is the post-repair case), so the rule is multiplicity per source,
/// not global uniqueness.
#[test]
fn scan_since_keeps_a_merge_operand_a_single_source_holds_twice() -> lsm_tree::Result<()> {
    use lsm_tree::WriteBatch;

    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    // One batch, one key, the SAME operand twice: identical payload AND
    // identical seqno, since a batch shares one.
    let mut batch = WriteBatch::new();
    batch.merge(b"counter".as_slice(), b"+1".as_slice());
    batch.merge(b"counter".as_slice(), b"+1".as_slice());
    tree.apply_batch(batch, 10)?;
    tree.flush_active_memtable(0)?;

    let got = events(&tree, 0)?;
    assert_eq!(
        got.iter()
            .filter(|e| matches!(e, ScanSinceEvent::MergeOperand { key, .. } if key.as_ref() == b"counter"))
            .count(),
        2,
        "both operands must reach the consumer: collapsing them replays the \
         merge once where the source applied it twice: {got:?}",
    );
    Ok(())
}

/// Distinct operands a single source holds at one seqno must be replayed in the
/// order that source applies them, not in payload order. A merge operator need
/// not be commutative (string append, list push, last-writer-wins field patch),
/// so reordering two operands of one batch changes the value the consumer ends
/// up with.
#[test]
fn scan_since_replays_one_source_operands_in_application_order() -> lsm_tree::Result<()> {
    use lsm_tree::WriteBatch;

    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    // Payload order is the REVERSE of application order, so a sort by bytes is
    // visible in the output.
    let mut batch = WriteBatch::new();
    batch.merge(b"k".as_slice(), b"B".as_slice());
    batch.merge(b"k".as_slice(), b"A".as_slice());
    tree.apply_batch(batch, 10)?;

    let got = events(&tree, 0)?;
    let operands: Vec<_> = got
        .iter()
        .filter_map(|e| match e {
            ScanSinceEvent::MergeOperand { operand, .. } => Some(operand.to_vec()),
            _ => None,
        })
        .collect();
    assert_eq!(
        operands,
        vec![b"B".to_vec(), b"A".to_vec()],
        "the tree applies B then A, so the stream must too — an \
         order-sensitive merge operator otherwise converges elsewhere: {got:?}",
    );
    Ok(())
}

/// A REPEATED operand interleaved with a different one (`B, A, B`) must keep
/// every copy at its own application position: grouping the identical copies
/// onto one shared position would replay `A, B, B`, and an order-sensitive
/// merge operator (string append, list push) then converges somewhere the
/// tree never was.
#[test]
fn scan_since_keeps_interleaved_duplicate_operands_in_application_order() -> lsm_tree::Result<()> {
    use lsm_tree::WriteBatch;

    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    let mut batch = WriteBatch::new();
    batch.merge(b"k".as_slice(), b"B".as_slice());
    batch.merge(b"k".as_slice(), b"A".as_slice());
    batch.merge(b"k".as_slice(), b"B".as_slice());
    tree.apply_batch(batch, 10)?;

    let got = events(&tree, 0)?;
    let operands: Vec<_> = got
        .iter()
        .filter_map(|e| match e {
            ScanSinceEvent::MergeOperand { operand, .. } => Some(operand.to_vec()),
            _ => None,
        })
        .collect();
    assert_eq!(
        operands,
        vec![b"B".to_vec(), b"A".to_vec(), b"B".to_vec()],
        "each copy replays at its own position — the duplicate must not be \
         folded onto its twin's slot: {got:?}",
    );
    Ok(())
}

/// The range-scoped scan delivers point events only for keys inside the
/// bounds, range deletions when their span overlaps them, and skips SSTs
/// whose key range cannot intersect — the presence-check primitive for
/// reconciling an external WAL against `RepairReport::lost_coverage` without
/// walking the whole store.
#[test]
fn scan_since_in_range_scopes_events_to_the_key_range() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    // Two disjoint SSTs: only the [d..f] one intersects the queried range.
    tree.insert(b"a", b"v", 1);
    tree.insert(b"b", b"v", 2);
    tree.flush_active_memtable(0)?;
    tree.insert(b"d", b"v", 3);
    tree.insert(b"e", b"v", 4);
    tree.insert(b"f", b"v", 5);
    tree.flush_active_memtable(0)?;
    // Memtable entries on both sides of the bound, plus a range deletion
    // straddling INTO the scope from below.
    tree.insert(b"c", b"v", 6);
    tree.insert(b"g", b"v", 7);
    tree.remove_range(b"b", b"e", 8);

    let got: Vec<_> = tree
        .scan_since_seqno_in_range(0, b"d".as_slice()..=b"f".as_slice())?
        .collect();

    let keys: Vec<Vec<u8>> = got
        .iter()
        .filter_map(|e| match e {
            ScanSinceEvent::Insert { key, .. } => Some(key.to_vec()),
            _ => None,
        })
        .collect();
    assert_eq!(
        keys,
        vec![b"d".to_vec(), b"e".to_vec(), b"f".to_vec()],
        "point events outside [d, f] must not be delivered: {got:?}",
    );
    assert!(
        got.iter().any(|e| matches!(
            e,
            ScanSinceEvent::RangeTombstone { start_key, end_key, seqno: 8 }
                if start_key.as_ref() == b"b" && end_key.as_ref() == b"e"
        )),
        "a range deletion overlapping the scope affects replay inside it and \
         must be delivered: {got:?}",
    );
    // A range deletion wholly OUTSIDE the scope stays out.
    tree.remove_range(b"x", b"z", 9);
    let got: Vec<_> = tree
        .scan_since_seqno_in_range(0, b"d".as_slice()..=b"f".as_slice())?
        .collect();
    assert!(
        !got.iter()
            .any(|e| matches!(e, ScanSinceEvent::RangeTombstone { seqno: 9, .. })),
        "a range deletion that cannot reach the scope is noise: {got:?}",
    );
    Ok(())
}

/// The blob-tree variant resolves KV-separated values inside the scope
/// exactly like the unscoped scan.
#[test]
fn blob_scan_since_in_range_scopes_and_resolves() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_blob_tree(folder.path())?;

    let big = vec![b'x'; 8_192];
    tree.insert(b"a", big.as_slice(), 1);
    tree.insert(b"m", big.as_slice(), 2);
    tree.insert(b"z", big.as_slice(), 3);
    tree.flush_active_memtable(0)?;

    let got: Vec<_> = tree
        .scan_since_seqno_in_range(0, b"m".as_slice()..=b"m".as_slice())?
        .collect();
    let [
        ScanSinceEvent::Insert {
            key,
            value,
            seqno: 2,
        },
    ] = got.as_slice()
    else {
        panic!("exactly the in-scope key, resolved: {got:?}");
    };
    assert_eq!(key.as_ref(), b"m");
    assert_eq!(
        value.as_ref(),
        big.as_slice(),
        "the KV-separated value must be resolved from the blob file",
    );
    Ok(())
}

/// A user-authored `remove_weak` is OBSERVABLY different from a regular
/// delete: a weak tombstone annihilates exactly its matching put during
/// compaction and can expose an older value from another run, while a
/// regular tombstone keeps hiding it. The CDC stream must preserve the
/// distinction, or a replica replaying the events diverges from the source.
#[test]
fn scan_since_distinguishes_a_weak_delete() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    tree.insert(b"k", b"v", 1);
    tree.remove_weak(b"k", 2);
    tree.flush_active_memtable(0)?;

    let got = events(&tree, 0)?;
    assert!(
        !got.iter()
            .any(|e| matches!(e, ScanSinceEvent::PointTombstone { seqno: 2, .. })),
        "a weak single-delete must not surface as a regular point deletion: {got:?}",
    );
    assert!(
        got.iter().any(|e| matches!(
            e,
            ScanSinceEvent::WeakTombstone { key, seqno: 2 } if key.as_ref() == b"k"
        )),
        "the weak delete surfaces as its own event kind: {got:?}",
    );
    Ok(())
}

/// An SST's metadata key range covers its POINT keys only, while a range
/// tombstone it carries can reach past them. The scoped scan's SST pruning
/// must not skip such a table by its point-key range, or the promised
/// overlapping deletion event is omitted and a range-partitioned CDC
/// consumer retains a deleted key.
#[test]
fn scan_since_scoped_sees_a_tombstone_reaching_past_the_tables_point_keys() -> lsm_tree::Result<()>
{
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    // The OLD table holds the key the scope asks about.
    tree.insert(b"m", b"old", 1);
    tree.flush_active_memtable(0)?;

    // The NEW table holds one point key at "a" plus a range deletion
    // reaching to "z": the tombstone's span covers the scope below.
    tree.insert(b"a", b"new", 2);
    tree.remove_range(b"a", b"z", 3);
    tree.flush_active_memtable(0)?;

    let got: Vec<ScanSinceEvent> = tree
        .scan_since_seqno_in_range(0, b"m".as_slice()..=b"m".as_slice())?
        .collect();
    assert!(
        got.iter()
            .any(|e| matches!(e, ScanSinceEvent::RangeTombstone { seqno: 3, .. })),
        "the deletion overlapping the scope must be delivered even though its \
         table's point-key range does not: {got:?}",
    );
    Ok(())
}

/// An RT-only flush writes a synthetic weak-tombstone SENTINEL at the range's
/// start (the writer's `finish`, to give the KV-empty table one index entry).
/// The stream deliberately KEEPS it: it is a real on-disk entry the read path
/// sees — at a seqno tie it is what makes a read at the range's start key
/// converge to a deletion (see `scan_since_replays_a_tied_range_tombstone_
/// first_across_sources`) — and away from a tie it replays as a weak delete
/// under the range deletion's own seqno: a no-op on top of the range event.
#[test]
fn scan_since_surfaces_the_rt_only_sentinel_as_a_tied_weak_delete() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path())?;

    // A memtable holding ONLY a range tombstone, flushed: the SST is RT-only.
    tree.remove_range(b"c", b"f", 5);
    tree.flush_active_memtable(0)?;

    let got = events(&tree, 0)?;
    let [
        ScanSinceEvent::RangeTombstone {
            start_key,
            end_key,
            seqno: 5,
        },
        ScanSinceEvent::WeakTombstone { key, seqno: 5 },
    ] = got.as_slice()
    else {
        panic!("the range deletion plus its start-key sentinel: {got:?}");
    };
    assert_eq!(start_key.as_ref(), b"c");
    assert_eq!(end_key.as_ref(), b"f");
    assert_eq!(
        key.as_ref(),
        b"c",
        "the sentinel replays at the range's start, inside the span it \
         deletes — idempotent for any consumer",
    );
    Ok(())
}
