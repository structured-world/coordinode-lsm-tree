// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end tests for the opt-in compaction space-admission gate: a merge is
//! skipped (or narrowed) when its transient output would not fit the free disk,
//! `Drop` / `Move` stay unaffected, the tree stays consistent and reopenable,
//! and `storage_stats().status` reports full-compaction availability.

use lsm_tree::fs::MemFs;
use lsm_tree::{
    AbstractTree, AnyTree, Config, SequenceNumberCounter, StorageStatus, get_tmp_folder,
};
use std::sync::Arc;

/// Opens a Standard tree on a `MemFs` capped at `capacity` bytes, returning both
/// the tree and the backend clone so a test can re-cap the simulated disk.
fn open_capped(path: &std::path::Path, capacity: u64) -> (lsm_tree::Tree, MemFs) {
    let mem = MemFs::with_capacity(capacity);
    let any = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem.clone()))
    .open()
    .expect("open");
    match any {
        AnyTree::Standard(t) => (t, mem),
        AnyTree::Blob(_) => panic!("expected Standard tree"),
    }
}

/// Inserts `n` keyed entries across two flushes so the tree holds several SSTs
/// worth merging, returning the physical footprint after the second flush.
fn seed_two_generations(tree: &lsm_tree::Tree, n: u64) -> u64 {
    for i in 0..n {
        tree.insert(format!("key{i:08}").as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0).expect("flush 1");
    // Overwrite the whole keyspace into a second SST: a later merge can drop the
    // shadowed first-generation values, so the merge genuinely reclaims space.
    for i in 0..n {
        tree.insert(format!("key{i:08}").as_bytes(), vec![0xEFu8; 64], n + i);
    }
    tree.flush_active_memtable(0).expect("flush 2");
    tree.storage_stats().expect("stats").used_bytes
}

#[test]
fn near_full_disk_skips_a_merge_that_would_not_fit_and_stays_consistent() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    // Start unbounded so the seed writes are never gated; cap afterwards.
    let (tree, mem) = open_capped(folder.path(), u64::MAX);
    let n = 2_000u64;
    let used = seed_two_generations(&tree, n);
    let tables_before = tree.table_count();
    assert!(
        tables_before >= 2,
        "two flushes must leave at least two SSTs to merge (got {tables_before})"
    );

    // Cap the simulated disk just above the live footprint: free space is far
    // below the merge's Σ input bound, so a full major compaction cannot fit.
    mem.set_capacity(used + 64 * 1024);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None; // only the physical free-space probe gates
    })?;

    // The gate must make the major compaction a no-op rather than run it into
    // ENOSPC: the cross-level input set cannot be narrowed to a single run, so
    // it is skipped. The tree keeps its tables and every key stays readable.
    tree.major_compact(64 * 1024 * 1024, 0)?;
    assert_eq!(
        tree.table_count(),
        tables_before,
        "a merge that does not fit must be skipped, leaving the tables in place"
    );
    for i in 0..n {
        assert!(
            tree.get(format!("key{i:08}").as_bytes(), u64::MAX)?
                .is_some(),
            "every key must remain readable after a gated (skipped) compaction"
        );
    }

    // Consistency survives a reopen: nothing was partially committed. The reopen
    // must reuse the SAME in-memory backend (a fresh MemFs would be empty), since
    // the simulated disk's state lives in `mem`.
    drop(tree);
    let reopened = match Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem.clone()))
    .open()?
    {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    assert!(
        reopened.get(b"key00000000".as_slice(), u64::MAX)?.is_some(),
        "data must survive reopen after a gated compaction"
    );
    Ok(())
}

#[test]
fn raising_capacity_lets_the_skipped_merge_run() -> lsm_tree::Result<()> {
    // Proves the gate (not some other limit) caused the skip: with ample space
    // the same major compaction collapses the tree to a single table.
    let folder = get_tmp_folder();
    let (tree, mem) = open_capped(folder.path(), u64::MAX);
    let n = 2_000u64;
    let used = seed_two_generations(&tree, n);
    assert!(tree.table_count() >= 2);

    mem.set_capacity(used + 64 * 1024);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    tree.major_compact(64 * 1024 * 1024, 0)?;
    let after_gated = tree.table_count();

    // Raise the cap far above the footprint: the merge now fits and runs.
    mem.set_capacity(used + 1024 * 1024 * 1024);
    tree.major_compact(64 * 1024 * 1024, 0)?;
    assert!(
        tree.table_count() < after_gated,
        "with ample free space the merge runs and reduces the table count \
         (was {after_gated} while gated)"
    );
    // The reclaiming merge dropped the shadowed first generation; reads still
    // return the latest values.
    assert_eq!(
        tree.get(b"key00000000".as_slice(), u64::MAX)?.as_deref(),
        Some([0xEFu8; 64].as_slice()),
        "latest value survives the merge"
    );
    Ok(())
}

#[test]
fn admission_off_never_gates_compaction() -> lsm_tree::Result<()> {
    // With the gate off, a near-full simulated disk does not affect compaction:
    // the major compaction collapses to a single table as usual.
    let folder = get_tmp_folder();
    let (tree, mem) = open_capped(folder.path(), u64::MAX);
    let n = 2_000u64;
    let used = seed_two_generations(&tree, n);
    assert!(tree.table_count() >= 2);

    // Tight cap but admission OFF (default): compaction is never gated.
    mem.set_capacity(used + 64 * 1024);
    tree.major_compact(64 * 1024 * 1024, 0)?;
    assert_eq!(
        tree.table_count(),
        1,
        "with admission off the merge runs regardless of free space"
    );
    Ok(())
}

#[test]
fn storage_status_reports_full_compaction_availability() -> lsm_tree::Result<()> {
    // AC4: with gating active and ample room the status surfaces
    // FullCompactionAvailable; a near-full disk flips it to ReadOnlyOutOfSpace.
    let folder = get_tmp_folder();
    let (tree, mem) = open_capped(folder.path(), u64::MAX);
    let used = seed_two_generations(&tree, 1_000);

    mem.set_capacity(used + 512 * 1024 * 1024);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    assert_eq!(
        tree.storage_stats()?.status,
        StorageStatus::FullCompactionAvailable,
        "ample free space under an active gate reports full-compaction availability"
    );

    // Shrink to a full disk: the write gate closes and that takes precedence.
    mem.set_capacity(used);
    tree.update_runtime_config(|_c| {})?; // clear the cached probe
    assert_eq!(
        tree.storage_stats()?.status,
        StorageStatus::ReadOnlyOutOfSpace
    );
    Ok(())
}
