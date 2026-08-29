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

/// The relocation finalizes its output BEFORE reopening it. A reopen that
/// fails leaves a full-sized SST that no manifest names and no handle can mark
/// deleted, so nothing drops it before a restart's orphan sweep — and
/// background compaction retries the same merge, stacking another copy on the
/// volume each time. The failing path must unlink its own output, as the
/// relocation writer already does for failures before finalization.
#[test]
fn a_failed_relocation_reopen_leaves_no_orphan_output() {
    use lsm_tree::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use std::sync::Arc;

    let mem = MemFs::new();
    let fault = FaultFs::new(mem.clone());
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);
    let folder = std::path::absolute("/db").expect("absolute");

    let any = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&fs))
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

    let n = 200u32;
    for i in 0..n {
        tree.insert(key(i), value(i), u64::from(i));
    }
    tree.remove_range(
        UserKey::from(&key(0)[..]),
        UserKey::from(&key(50)[..]),
        1000,
    );
    tree.flush_active_memtable(0).expect("flush");

    // The flush wrote table 0; the relocation writes table 1. Positioned reads
    // on that path happen only when it is REOPENED — the writer streams it out
    // — so this fires exactly on the reopen, after finalization.
    injector.arm(
        FaultRule::new(
            FaultOp::ReadAt,
            Fault::Error(lsm_tree::io::ErrorKind::PermissionDenied),
        )
        .on_path("tables/1"),
    );
    let outcome = tree.major_compact(64 * 1024 * 1024, 5000);
    injector.clear();
    assert!(
        outcome.is_err(),
        "the reopen fault must fail the compaction"
    );

    let orphans: Vec<_> = mem
        .read_dir(&folder.join("tables"))
        .expect("read tables dir")
        .into_iter()
        .filter(|e| e.file_name == "1")
        .collect();
    assert!(
        orphans.is_empty(),
        "the finalized-but-unopenable relocation output must be removed by the \
         path that failed, not left for a restart: {orphans:?}",
    );
}
