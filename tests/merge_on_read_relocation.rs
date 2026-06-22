// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end merge-on-read relocation: a lone columnar segment whose own range
//! tombstones delete some rows is compacted by reusing its data blocks verbatim
//! and recording the deleted row positions in a delete-bitmap, instead of
//! re-transposing and dropping the rows. The deleted rows read as absent, the
//! survivors keep their values, and the segment carries a non-empty bitmap (the
//! proof that block reuse, not copy-on-write, happened).

#![cfg(feature = "columnar")]

use lsm_tree::config::{DeleteStrategy, DeleteStrategyPolicy};
use lsm_tree::{
    AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, UserKey, get_tmp_folder,
};
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn value(i: u32) -> Vec<u8> {
    format!("value-{i}-payload").into_bytes()
}

fn open_merge_on_read(folder: &std::path::Path) -> lsm_tree::Tree {
    let any = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::MergeOnRead);
    })
    .expect("enable merge-on-read columnar");
    tree
}

#[test]
fn merge_on_read_relocates_a_single_columnar_segment() {
    let folder = get_tmp_folder();
    let tree = open_merge_on_read(folder.path());

    let n = 200u32;
    for i in 0..n {
        tree.insert(key(i), value(i), u64::from(i));
    }
    // Range-delete the first 50 keys at a seqno above all of them, in the same
    // memtable, so the flushed segment carries both the data and its own range
    // tombstone.
    tree.remove_range(
        UserKey::from(&key(0)[..]),
        UserKey::from(&key(50)[..]),
        1000,
    );
    tree.flush_active_memtable(0).expect("flush");

    // One columnar segment with a below-watermark range tombstone. A major
    // compaction with a watermark above the tombstone materializes the deletes.
    tree.major_compact(64 * 1024 * 1024, 5000).expect("compact");

    // Deleted rows read as absent; survivors keep their exact values.
    for i in 0..n {
        let got = tree.get(key(i), SeqNo::MAX).expect("get");
        if i < 50 {
            assert!(got.is_none(), "deleted key {i} must read as absent");
        } else {
            assert_eq!(
                &*got.expect("live key must be present"),
                value(i).as_slice(),
                "live key {i} value",
            );
        }
    }

    // Merge-on-read proof: the segment carries a non-empty delete-bitmap (rows
    // reused and masked, not dropped) and stays columnar.
    let version = tree.current_version();
    let tables: Vec<_> = version.iter_tables().collect();
    assert!(
        tables.iter().any(|t| !t.delete_bitmap().is_empty()),
        "a delete-bitmap must have been materialized (merge-on-read), not dropped (copy-on-write)",
    );
    assert!(
        tables.iter().all(|t| t.metadata.columnar),
        "the relocated segment stays columnar",
    );
}
