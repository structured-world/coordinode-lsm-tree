// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A compaction reports its work by kind, as estimated before it ran and as
//! measured: a move moves its tables and reads and writes nothing, a merge
//! reads its inputs and writes its outputs, copying what it can, and every
//! measured figure lies within the estimate's bounds whatever a compaction
//! filter makes of the rows.

#![cfg(feature = "metrics")]

use lsm_tree::compaction::filter::{
    CompactionFilter, Context as FilterContext, Factory, ItemAccessor, Verdict,
};
use lsm_tree::compaction::{CompactionAction, MoveDown, Work, WorkReport};
use lsm_tree::config::LevelRoute;
use lsm_tree::fs::StdFs;
use lsm_tree::{AbstractTree, AnyTree, Config, SequenceNumberCounter, get_tmp_folder};
use std::sync::Arc;
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn value(i: u32, round: u32) -> Vec<u8> {
    format!("value-{i}-round-{round}-{}", "x".repeat(40)).into_bytes()
}

fn config(path: &std::path::Path) -> Config {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
}

fn open(config: Config) -> lsm_tree::Tree {
    let AnyTree::Standard(tree) = config.open().expect("open") else {
        panic!("expected standard tree");
    };
    tree
}

/// Writes keys `range` at round `round`, seqnos from `seqno` up, and flushes
/// them into one table.
fn flush_keys(tree: &lsm_tree::Tree, range: core::ops::Range<u32>, round: u32, seqno: &mut u64) {
    for i in range {
        tree.insert(key(i), value(i, round), *seqno);
        *seqno += 1;
    }
    tree.flush_active_memtable(0).expect("flush");
}

/// The bytes of the files of the tables the tree holds.
fn table_bytes(tree: &lsm_tree::Tree) -> u64 {
    tree.current_version()
        .iter_tables()
        .map(|t| std::fs::metadata(t.path.as_path()).expect("stat").len())
        .sum()
}

/// Every measured figure lies within the estimate's bounds, and the tree's
/// counters hold this compaction's figures, it being the tree's only one.
fn assert_reported(tree: &lsm_tree::Tree, work: &WorkReport) {
    assert!(
        work.estimate.contains(&work.measured),
        "{:?} outside {:?}",
        work.measured,
        work.estimate.ceiling,
    );
    assert_eq!(tree.metrics().compaction_work_measured(), work.measured);
    assert_eq!(
        tree.metrics().compaction_work_estimated(),
        work.estimate.expected
    );
}

/// A move gives its tables another level where they lie: the bytes moved are
/// theirs, exactly as estimated, and nothing is read or written.
#[test]
fn compaction_move_reports_its_tables_as_moved_and_no_io() {
    let folder = get_tmp_folder();
    let tree = open(config(folder.path()));
    let mut seqno = 1;
    flush_keys(&tree, 0..1_000, 0, &mut seqno);
    let bytes = table_bytes(&tree);

    let result = tree.compact(Arc::new(MoveDown(0, 2)), 0).expect("compact");

    assert_eq!(result.action, CompactionAction::Moved);
    assert_eq!(
        result.work.measured,
        Work {
            moved: bytes,
            ..Work::default()
        },
    );
    assert_eq!(result.work.estimate.expected, result.work.measured);
    assert_reported(&tree, &result.work);
}

/// A move into a level another folder holds rewrites its tables there: it is
/// reported as the merge it is, with the bytes it read and wrote, and moves
/// nothing.
#[test]
fn compaction_move_across_folders_reports_the_rewrite() {
    let folder = get_tmp_folder();
    let tree = open(
        config(&folder.path().join("primary")).level_routes(vec![LevelRoute {
            levels: 0..2,
            path: folder.path().join("hot"),
            fs: Arc::new(StdFs),
        }]),
    );
    let mut seqno = 1;
    flush_keys(&tree, 0..1_000, 0, &mut seqno);
    let input = table_bytes(&tree);

    let result = tree.compact(Arc::new(MoveDown(0, 2)), 0).expect("compact");

    assert_eq!(result.action, CompactionAction::Merged);
    let measured = result.work.measured;
    assert_eq!(measured.moved, 0, "the tables were rewritten, not moved");
    assert!(
        measured.read > 0 && measured.read <= input,
        "the input is read once: {} of {input}",
        measured.read,
    );
    assert_eq!(measured.written, table_bytes(&tree));
    assert_eq!(measured.reencoded, measured.written);
    assert_eq!(result.work.estimate.expected.written, input);
    assert_reported(&tree, &result.work);
}

/// A merge reads its inputs and writes its outputs: the written bytes are
/// those of the tables it installed, a merge of row-oriented tables encodes
/// all of them, and every figure lies within the estimate's bounds.
#[test]
fn compaction_merge_reports_what_it_read_and_installed() {
    let folder = get_tmp_folder();
    let tree = open(config(folder.path()));
    let mut seqno = 1;
    flush_keys(&tree, 0..1_000, 0, &mut seqno);
    flush_keys(&tree, 500..1_500, 1, &mut seqno);
    let input = table_bytes(&tree);

    let result = tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    assert_eq!(result.action, CompactionAction::Merged);
    let measured = result.work.measured;
    assert_eq!(measured.written, table_bytes(&tree));
    assert_eq!(measured.carried, 0);
    assert_eq!(measured.reencoded, measured.written);
    assert!(
        measured.read > 0 && measured.read <= input,
        "each input is read once: {} of {input}",
        measured.read,
    );
    assert_eq!(result.work.estimate.expected.written, input);
    assert_eq!(result.work.estimate.expected.read, input);
    assert_reported(&tree, &result.work);
}

/// What the estimate expects does not depend on what a compaction filter
/// decides, since it is made without calling one: a filter that drops every
/// row and one that keeps every row are planned against the same figures,
/// and the drop, which writes nothing, still lies within the bounds.
#[test]
fn compaction_filter_dropping_every_row_is_priced_within_the_bounds() {
    struct Every(fn() -> Verdict);
    impl CompactionFilter for Every {
        fn filter_item(
            &mut self,
            _item: ItemAccessor<'_>,
            _ctx: &FilterContext,
        ) -> lsm_tree::Result<Verdict> {
            Ok((self.0)())
        }
    }
    struct EveryFactory(fn() -> Verdict);
    impl Factory for EveryFactory {
        fn name(&self) -> &str {
            "every"
        }
        fn make_filter(&self, _ctx: &FilterContext) -> Box<dyn CompactionFilter> {
            Box::new(Every(self.0))
        }
    }

    let run = |verdict: fn() -> Verdict| {
        let folder = get_tmp_folder();
        let tree = open(
            config(folder.path())
                .with_compaction_filter_factory(Some(Arc::new(EveryFactory(verdict)))),
        );
        let mut seqno = 1;
        flush_keys(&tree, 0..1_000, 0, &mut seqno);
        flush_keys(&tree, 500..1_500, 1, &mut seqno);
        let result = tree.major_compact(64 * 1024 * 1024, 0).expect("compact");
        assert_reported(&tree, &result.work);
        (result.work, table_bytes(&tree))
    };
    // Destroyed, not removed: a removal leaves a tombstone to write.
    let (dropped, dropped_left) = run(|| Verdict::Destroy);
    let (kept, kept_left) = run(|| Verdict::Keep);

    assert_eq!(dropped.estimate, kept.estimate);
    assert_eq!(dropped_left, 0, "every row was dropped");
    assert_eq!(dropped.measured.written, 0);
    assert!(dropped.measured.read > 0, "the inputs were read");
    assert_eq!(kept.measured.written, kept_left);
    assert!(kept_left > 0);
}

/// A merge that changes no row of its columnar inputs copies their row groups
/// as the estimate expected, every byte of them: only the output's index,
/// filter and metadata are encoded, and each copied group is read twice, by
/// the scan and as it lies.
#[cfg(feature = "columnar")]
#[test]
fn compaction_merge_changing_no_row_carries_the_groups_it_was_estimated_to() {
    let folder = get_tmp_folder();
    let tree = open(config(folder.path()));
    // One codec at every level, as a copy needs.
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.data_block_compression_policy =
            lsm_tree::config::CompressionPolicy::all(lsm_tree::CompressionType::None);
    })
    .expect("enable columnar");
    let mut seqno = 1;
    flush_keys(&tree, 0..2_000, 0, &mut seqno);
    flush_keys(&tree, 2_000..4_000, 0, &mut seqno);

    let result = tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    let work = result.work;
    assert!(work.measured.carried > 0);
    assert_eq!(work.estimate.expected.carried, work.measured.carried);
    assert_eq!(
        work.measured.carried,
        tree.metrics().compaction_bytes_carried()
    );
    assert!(
        work.measured.reencoded * 10 < work.measured.written,
        "{} of {} written bytes encoded",
        work.measured.reencoded,
        work.measured.written,
    );
    assert!(work.measured.read >= 2 * work.measured.carried);
    assert_reported(&tree, &work);
}

/// A lone columnar segment whose own range tombstone deletes some of its rows
/// is relocated: its sections are copied as they lay and only its metadata
/// and delete bitmap are written anew.
#[cfg(feature = "columnar")]
#[test]
fn compaction_relocated_segment_carries_its_copied_sections() {
    use lsm_tree::config::{DeleteStrategy, DeleteStrategyPolicy};

    let folder = get_tmp_folder();
    let tree = open(config(folder.path()));
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::MergeOnRead);
    })
    .expect("enable merge-on-read columnar");
    for i in 0..200u32 {
        tree.insert(key(i), value(i, 0), u64::from(i));
    }
    tree.remove_range(
        lsm_tree::UserKey::from(&key(0)[..]),
        lsm_tree::UserKey::from(&key(50)[..]),
        1_000,
    );
    tree.flush_active_memtable(0).expect("flush");
    let input = table_bytes(&tree);

    let result = tree
        .major_compact(64 * 1024 * 1024, 5_000)
        .expect("compact");

    let version = tree.current_version();
    assert!(
        version.iter_tables().any(|t| !t.delete_bitmap().is_empty()),
        "the segment was relocated, not rewritten",
    );
    let measured = result.work.measured;
    assert_eq!(measured.written, table_bytes(&tree));
    assert!(measured.carried > 0);
    assert!(
        measured.reencoded > 0 && measured.reencoded < measured.carried,
        "the metadata and the bitmap are encoded: {} beside {} copied",
        measured.reencoded,
        measured.carried,
    );
    assert!(measured.carried <= input);
    assert_reported(&tree, &result.work);
}

/// A merge that relocates a blob file copies its live frames, and the
/// estimate expects exactly those: the file's frames less the ones the
/// version charged as garbage.
#[test]
fn compaction_blob_relocation_carries_the_live_frames_it_was_estimated_to() {
    let folder = get_tmp_folder();
    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);
    let tree = config(folder.path())
        .with_kv_separation(Some(
            lsm_tree::KvSeparationOptions::default().age_cutoff(1.0),
        ))
        .blob_compression(lsm_tree::CompressionType::None)
        .open()
        .expect("open");

    tree.insert("big", &big_value, 0);
    tree.insert("big2", &big_value, 0);
    tree.flush_active_memtable(0).expect("flush");
    tree.insert("big", &new_big_value, 1);
    tree.flush_active_memtable(0).expect("flush");
    // Records the stale value; the file is not rewritten yet.
    tree.major_compact(64_000_000, 1_000).expect("compact");
    let relocated_before = tree.metrics().blob_bytes_relocated();

    // Rewrites the first file: `big2` is copied forward, the stale `big` is not.
    let result = tree.major_compact(64_000_000, 1_000).expect("compact");

    let relocated = tree.metrics().blob_bytes_relocated() - relocated_before;
    assert!(relocated >= big_value.len() as u64);
    let work = result.work;
    assert_eq!(work.measured.carried, relocated);
    assert_eq!(work.estimate.expected.carried, relocated);
    assert!(work.measured.read >= relocated);
    assert!(
        work.estimate.contains(&work.measured),
        "{:?} outside {:?}",
        work.measured,
        work.estimate.ceiling,
    );
}
