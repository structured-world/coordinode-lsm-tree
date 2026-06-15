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
