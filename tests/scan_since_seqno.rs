// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end coverage for `Tree::scan_since_seqno` (CDC event stream):
//! target filtering, increasing-seqno replay order, event-type mapping
//! (Insert / PointTombstone / RangeTombstone), coverage across memtable and
//! SSTs, the seqno-bounded block-skip path, and mixed-format trees (a tree
//! that holds both legacy `index_format=0` and seqno-bounded `index_format=1`
//! SSTs must scan correctly).

use lsm_tree::{
    AbstractTree, AnyTree, Config, ScanSinceEvent, SeqNo, SequenceNumberCounter, Tree,
    get_tmp_folder,
};
use test_log::test;

fn open_tree(path: &std::path::Path) -> Tree {
    let any = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("Tree open should succeed");

    match any {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree, got Blob"),
    }
}

fn events(tree: &Tree, target: SeqNo) -> Vec<ScanSinceEvent> {
    tree.scan_since_seqno(target)
        .expect("scan_since_seqno should succeed")
        .collect()
}

#[test]
fn scan_since_returns_only_entries_at_or_after_target() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    for i in 0..10u64 {
        tree.insert(format!("k{i:02}").as_bytes(), b"v", i);
    }
    tree.flush_active_memtable(0)?;

    let got = events(&tree, 5);
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
    let tree = open_tree(folder.path());

    // Insert in a deliberately scrambled key order; seqnos still rise.
    for (i, k) in ["m", "a", "z", "c", "q"].iter().enumerate() {
        tree.insert(k.as_bytes(), b"v", i as u64);
    }
    tree.flush_active_memtable(0)?;

    let seqnos: Vec<SeqNo> = events(&tree, 0).iter().map(ScanSinceEvent::seqno).collect();
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
    let tree = open_tree(folder.path());

    tree.insert(b"key", b"val", 0);
    tree.remove(b"key", 1);
    tree.flush_active_memtable(0)?;

    let got = events(&tree, 0);
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
    let tree = open_tree(folder.path());

    tree.insert(b"a", b"v", 0);
    tree.remove_range(b"a", b"m", 1);
    tree.flush_active_memtable(0)?;

    let got = events(&tree, 0);
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
    let tree = open_tree(folder.path());

    // Flushed to an SST.
    for i in 0..5u64 {
        tree.insert(format!("s{i}").as_bytes(), b"v", i);
    }
    tree.flush_active_memtable(0)?;

    // Still in the active memtable.
    for i in 5..10u64 {
        tree.insert(format!("m{i}").as_bytes(), b"v", i);
    }

    let got = events(&tree, 0);
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
    let tree = open_tree(folder.path());
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

    let got = events(&tree, 450);
    assert_eq!(got.len(), 50, "only seqnos 450..500 qualify");
    assert!(got.iter().all(|e| e.seqno() >= 450));
    Ok(())
}

#[test]
fn scan_since_mixed_format_tree_scans_correctly() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // First SST: legacy index_format=0 (seqno_in_index defaults off).
    for i in 0..250u64 {
        tree.insert(format!("key{i:05}").as_bytes(), b"v", i);
    }
    tree.flush_active_memtable(0)?;

    // Toggle on, second SST: seqno-bounded index_format=1.
    tree.update_runtime_config(|cfg| {
        cfg.seqno_in_index = true;
    })?;
    for i in 250..500u64 {
        tree.insert(format!("key{i:05}").as_bytes(), b"v", i);
    }
    tree.flush_active_memtable(0)?;

    // Target straddles both SSTs: the legacy one falls back to a full filter,
    // the seqno-bounded one uses block-skip; the union must be exact.
    let got = events(&tree, 200);
    assert_eq!(got.len(), 300, "seqnos 200..500 across both formats");
    assert!(got.iter().all(|e| e.seqno() >= 200));
    let seqnos: Vec<SeqNo> = got.iter().map(ScanSinceEvent::seqno).collect();
    let mut sorted = seqnos.clone();
    sorted.sort_unstable();
    assert_eq!(seqnos, sorted, "merged output stays seqno-ordered");
    Ok(())
}
