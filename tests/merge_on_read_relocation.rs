// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

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

fn open_merge_on_read(folder: &std::path::Path, table_file_pool: usize) -> lsm_tree::Tree {
    let any = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .table_file_pool(table_file_pool)
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
    merge_on_read_relocates_into(16);
}

/// Without a pool the relocation creates its file, whose name is durable only
/// once the install syncs the folder: the same rows read back.
#[test]
fn merge_on_read_relocates_without_a_table_file_pool() {
    merge_on_read_relocates_into(0);
}

fn merge_on_read_relocates_into(table_file_pool: usize) {
    let folder = get_tmp_folder();
    let tree = open_merge_on_read(folder.path(), table_file_pool);

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

    drop(tables);
    drop(version);
    drop(tree);
    let tree = open_merge_on_read(folder.path(), table_file_pool);
    assert!(tree.get(key(0), SeqNo::MAX).expect("get").is_none());
    assert_eq!(
        tree.get(key(n - 1), SeqNo::MAX).expect("get").as_deref(),
        Some(value(n - 1).as_slice()),
        "after a reopen",
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
    // The flush found the pool dry and left its refill to run off its thread:
    // the pooled files are listed once that refill has made them.
    tree.wait_for_table_file_refills();

    // The relocation writes its table into the folder's lowest pooled file
    // above its source, the one table the flush wrote. Positioned reads on
    // that path happen only when it is REOPENED — the writer streams it out —
    // so this fires exactly on the reopen, after finalization.
    //
    // The filter is built from the real path rather than written as a literal:
    // a hardcoded `tables/<id>` matches nothing where the separator is `\`,
    // and the rule would then silently never fire.
    let source_id = tree
        .current_version()
        .iter_tables()
        .map(|table| table.id())
        .max()
        .expect("the flushed table");
    let relocated_name = mem
        .read_dir(&folder.join("tables"))
        .expect("read tables dir")
        .into_iter()
        .filter_map(|e| e.file_name.parse::<u64>().ok().map(|id| (id, e.path)))
        .filter(|(id, path)| *id > source_id && mem.metadata(path).is_ok_and(|meta| meta.len == 0))
        .map(|(id, _)| id)
        .min()
        .expect("a pooled file above the source")
        .to_string();
    let relocated = folder.join("tables").join(&relocated_name);
    injector.arm(
        FaultRule::new(
            FaultOp::ReadAt,
            Fault::Error(lsm_tree::io::ErrorKind::PermissionDenied),
        )
        .on_path(relocated.display().to_string()),
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
        .filter(|e| e.file_name == relocated_name)
        .collect();
    assert!(
        orphans.is_empty(),
        "the finalized-but-unopenable relocation output must be removed by the \
         path that failed, not left for a restart: {orphans:?}",
    );
}

/// A relocation that fails while it writes its output (here at the output's
/// sync) fails the compaction, unlinks that output, and leaves every row where
/// it was. When the unlink fails too, the compaction still reports the write's
/// failure and the rows are still all there.
#[test]
fn a_failed_relocation_write_removes_its_output_and_keeps_the_rows() {
    use lsm_tree::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use std::sync::Arc;

    for unlink_fails in [false, true] {
        let mem = MemFs::new();
        let fault = Arc::new(FaultFs::new(mem.clone()));
        let folder = std::path::absolute("/db").expect("absolute");
        let any = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fault.clone())
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
        // The pooled files are listed once the flush's refill has made them.
        tree.wait_for_table_file_refills();

        // The output is the lowest pooled file above the flushed table.
        let tables = folder.join("tables");
        let source_id = tree
            .current_version()
            .iter_tables()
            .map(|table| table.id())
            .max()
            .expect("the flushed table");
        let output = mem
            .read_dir(&tables)
            .expect("read tables dir")
            .into_iter()
            .filter_map(|e| e.file_name.parse::<u64>().ok().map(|id| (id, e.path)))
            .filter(|(id, path)| {
                *id > source_id && mem.metadata(path).is_ok_and(|meta| meta.len == 0)
            })
            .min_by_key(|(id, _)| *id)
            .map(|(_, path)| path)
            .expect("a pooled file above the source");

        let injector = fault.injector();
        let on_output = output.display().to_string();
        injector.arm(
            FaultRule::new(
                FaultOp::SyncAll,
                Fault::Error(lsm_tree::io::ErrorKind::Other),
            )
            .on_path(on_output.clone()),
        );
        if unlink_fails {
            injector.arm(
                FaultRule::new(
                    FaultOp::RemoveFile,
                    Fault::Error(lsm_tree::io::ErrorKind::PermissionDenied),
                )
                .on_path(on_output),
            );
        }
        let outcome = tree.major_compact(64 * 1024 * 1024, 5000);
        injector.clear();
        assert!(
            outcome.is_err(),
            "the failed write fails the compaction (unlink fails: {unlink_fails})"
        );
        if !unlink_fails {
            assert!(
                !mem.exists(&output).expect("exists"),
                "the failed output is unlinked"
            );
        }

        let version = tree.current_version();
        let ids: Vec<u64> = version.iter_tables().map(|t| t.id()).collect();
        assert_eq!(ids, vec![source_id], "the source stays the only table");
        for i in 50..n {
            assert_eq!(
                tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
                Some(value(i).as_slice()),
                "key {i} (unlink fails: {unlink_fails})",
            );
        }
    }
}
