// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end tests for the opt-in compaction space-admission gate: a merge is
//! skipped (or narrowed) when its transient output would not fit the free disk,
//! `Drop` / `Move` stay unaffected, the tree stays consistent and reopenable,
//! and `storage_stats().status` reports full-compaction availability.

use lsm_tree::fs::MemFs;
use lsm_tree::{
    AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter, StorageStatus,
    get_tmp_folder,
};
use std::sync::Arc;

/// Opens a KV-separated (blob) tree on a `MemFs` capped at `capacity` bytes.
fn open_capped_blob(path: &std::path::Path, capacity: u64) -> (lsm_tree::BlobTree, MemFs) {
    let mem = MemFs::with_capacity(capacity);
    let any = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem.clone()))
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(64),
    ))
    .open()
    .expect("open");
    match any {
        AnyTree::Blob(t) => (t, mem),
        AnyTree::Standard(_) => panic!("expected Blob tree"),
    }
}

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
fn configured_quota_below_footprint_gates_a_merge_on_ample_disk() -> lsm_tree::Result<()> {
    // A quota set just above the live footprint must gate compaction even when
    // the physical disk has plenty of room: the merge would grow the tree past
    // the operator's budget. Exercises the quota-headroom branch of the gate.
    let folder = get_tmp_folder();
    // Huge simulated disk → physical free is never the constraint here.
    let (tree, _mem) = open_capped(folder.path(), 100 * 1024 * 1024 * 1024);
    let n = 2_000u64;
    let used = seed_two_generations(&tree, n);
    let tables_before = tree.table_count();
    assert!(tables_before >= 2);

    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        // Quota leaves less headroom than a full merge's input bound needs.
        c.storage_limit_bytes = Some(used + 64 * 1024);
    })?;
    tree.major_compact(64 * 1024 * 1024, 0)?;
    assert_eq!(
        tree.table_count(),
        tables_before,
        "a merge exceeding the quota headroom must be gated even with ample disk"
    );
    Ok(())
}

#[test]
fn blob_tree_full_status_ignores_non_stale_blob_footprint() -> lsm_tree::Result<()> {
    // Regression: the full-compaction figure must budget only the STALE blob
    // files a merge actually relocates (`pick_blob_files_to_rewrite`), not the
    // whole live blob footprint. With large incompressible NON-stale blobs (a
    // single flush, no overwrites → zero dead fraction) and tiny index SSTs, a
    // full merge relocates no blobs, so free space that clears the SST level must
    // report FullCompactionAvailable — matching the gate, which would admit the
    // merge — not a stricter TightCompactionAvailable from counting live blobs
    // the merge never touches.
    let folder = get_tmp_folder();
    let (tree, mem) = open_capped_blob(folder.path(), u64::MAX);
    // ~8 MiB of incompressible blob payload so the live footprint comfortably
    // exceeds 3 MiB — under the old all-live accounting this band reported tight.
    let n = 8_000u64;
    for i in 0..n {
        // High-entropy value (xorshift) so blobs do not compress away.
        let mut s = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let value: Vec<u8> = (0..1024u32)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 24) as u8
            })
            .collect();
        tree.insert(format!("key{i:08}").as_bytes(), &value, i);
    }
    tree.flush_active_memtable(0)?;

    let used = tree.storage_stats()?.used_bytes;
    let index_sst = tree.index.disk_space(); // SST-only footprint (tiny: pointers)
    let blob_portion = used - index_sst;
    assert!(
        blob_portion > 4 * 1024 * 1024,
        "live blobs must dominate the footprint (blob {blob_portion}, index {index_sst})"
    );

    // Free space (3 MiB) is well below the live blob footprint but far above the
    // tiny index SSTs. Because the blobs are non-stale (not relocated by a merge),
    // a full compaction's actual transient need is the SST level alone → it fits.
    mem.set_capacity(used + 3 * 1024 * 1024);
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    assert_eq!(
        tree.storage_stats()?.status,
        StorageStatus::FullCompactionAvailable,
        "non-stale blobs are not relocated, so they must not gate a full compaction"
    );
    Ok(())
}

#[test]
fn status_probes_the_last_level_route_volume_not_the_largest_footprint() -> lsm_tree::Result<()> {
    // Regression: a full compaction writes its SST output to the LAST level
    // (level_count - 1), so the status must probe THAT level's volume, not the
    // largest-footprint level's. Here the bottom level is routed to a near-full
    // cold volume while the bulk of the data (L0) sits on a roomy primary. The
    // compaction gate would skip a full compaction for lack of cold-volume room,
    // so the status must report TightCompactionAvailable — not Full from the
    // primary's free space.
    let folder = get_tmp_folder();
    let cold_dir = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(MemFs::new())) // unbounded primary (roomy)
    .level_routes(vec![lsm_tree::config::LevelRoute {
        levels: 6..7,
        path: cold_dir.path().to_path_buf(),
        // Near-full cold tier: above the reserved floor (not read-only) but well
        // below the full-compaction demand below.
        fs: Arc::new(MemFs::with_capacity(2 * 1024 * 1024)),
    }])
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected Standard tree");
    };

    // Write much more than the cold capacity into L0 (lands on the primary): the
    // full-compaction demand (largest level = L0) far exceeds the cold volume's
    // free space.
    let value = vec![0xABu8; 200];
    for i in 0..30_000u64 {
        tree.insert(format!("key{i:08}").as_bytes(), &value, i);
    }
    tree.flush_active_memtable(0)?;
    let demand = tree.storage_stats()?.full_compaction_bytes;
    assert!(
        demand > 2 * 1024 * 1024,
        "L0 demand must exceed the 2 MiB cold capacity (got {demand})"
    );

    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    assert_eq!(
        tree.storage_stats()?.status,
        StorageStatus::TightCompactionAvailable,
        "status must probe the last-level (cold) route volume, not the largest-footprint primary"
    );
    Ok(())
}

#[test]
fn blob_tree_compaction_space_gate_and_status() -> lsm_tree::Result<()> {
    // Drives the KV-separation branch of the gate (blob-file relocation budget)
    // and the blob-tree FullCompactionAvailable status.
    let folder = get_tmp_folder();
    let (tree, mem) = open_capped_blob(folder.path(), u64::MAX);
    let n = 1_000u64;
    let value = vec![0xABu8; 256]; // > separation threshold → blob files
    for i in 0..n {
        tree.insert(format!("key{i:08}").as_bytes(), &value, i);
    }
    tree.flush_active_memtable(0)?;
    for i in 0..n {
        tree.insert(format!("key{i:08}").as_bytes(), &value, n + i);
    }
    tree.flush_active_memtable(0)?;
    let used = tree.storage_stats()?.used_bytes;

    // Ample room: status reports full-compaction availability. Runtime config
    // lives on the blob tree's index tree.
    mem.set_capacity(used + 512 * 1024 * 1024);
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    assert_eq!(
        tree.storage_stats()?.status,
        StorageStatus::FullCompactionAvailable
    );

    // Near-full: the blob-aware gate keeps the tree consistent — every key still
    // resolves through its blob file after a gated compaction.
    mem.set_capacity(used + 64 * 1024);
    tree.index.update_runtime_config(|_c| {})?;
    tree.major_compact(64 * 1024 * 1024, 0)?;
    assert_eq!(
        tree.get(b"key00000000".as_slice(), u64::MAX)?.as_deref(),
        Some(value.as_slice()),
        "blob-backed value still resolves after a gated compaction"
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

#[test]
fn tight_space_compaction_rewrites_a_gated_single_table_and_preserves_data() -> lsm_tree::Result<()>
{
    let folder = get_tmp_folder();
    let (tree, mem) = open_capped(folder.path(), u64::MAX);
    let n = 2_000u64;

    // One generation flushed to a single multi-block SST.
    for i in 0..n {
        tree.insert(format!("key{i:08}").as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0).expect("flush");
    let used = tree.storage_stats().expect("stats").used_bytes;
    let tables_before = tree.table_count();

    // Cap the disk so a full rewrite of the table cannot fit alongside it: the
    // space gate skips the merge, and the opt-in tight-space loop reclaims the
    // input in slices instead.
    mem.set_capacity(used + used / 4);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
        c.tight_space_compaction = true;
    })?;

    // A normal merge needs ~2x the table size transiently and would ENOSPC on
    // this cap; tight-space reclaim completes by punching consumed slices.
    tree.major_compact(64 * 1024 * 1024, 0)?;

    // Engagement: the gated single table is rewritten into multiple slice
    // outputs (a plain skip would leave the one input untouched).
    assert!(
        tree.table_count() > tables_before,
        "tight-space loop must slice the gated table (skip would leave {tables_before})",
    );

    // Reclaim: the consumed input prefixes were punched in place (not merely
    // deleted at the end), which is what let the rewrite proceed on a disk
    // smaller than a normal merge's transient peak. The punched bytes are a
    // large fraction of the input — the punch keeps pace with the slice output,
    // so the transient footprint stays near one input rather than ballooning to
    // input + full output (a peak-bounded reclaim, not a token punch at the end).
    let punched = mem.punched_bytes();
    assert!(
        punched > used / 3,
        "tight-space reclaim must punch the bulk of the input incrementally \
         (punched {punched} of ~{used} used)",
    );

    // Correctness: every key remains readable after the tight-space rewrite.
    for i in 0..n {
        assert!(
            tree.get(format!("key{i:08}").as_bytes(), u64::MAX)?
                .is_some(),
            "key {i} lost after tight-space compaction",
        );
    }

    // Consistency survives a reopen on the same simulated disk.
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
    for i in (0..n).step_by(53) {
        assert!(
            reopened
                .get(format!("key{i:08}").as_bytes(), u64::MAX)?
                .is_some(),
            "key {i} lost after reopen",
        );
    }
    Ok(())
}

#[test]
fn tight_space_compaction_falls_back_to_skip_without_punch_hole_support() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (tree, mem) = open_capped(folder.path(), u64::MAX);
    let n = 2_000u64;

    for i in 0..n {
        tree.insert(format!("key{i:08}").as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0).expect("flush");
    let used = tree.storage_stats().expect("stats").used_bytes;
    let tables_before = tree.table_count();

    // Backend cannot punch holes: tight-space reclaim must NOT engage even
    // though it is enabled and the gate would otherwise skip a too-big merge.
    mem.set_punch_hole_supported(false);
    mem.set_capacity(used + used / 4);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
        c.tight_space_compaction = true;
    })?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert_eq!(
        tree.table_count(),
        tables_before,
        "without punch_hole support the gated merge is skipped, not sliced",
    );
    assert_eq!(
        mem.punched_bytes(),
        0,
        "a backend that cannot punch must never be punched",
    );
    for i in 0..n {
        assert!(
            tree.get(format!("key{i:08}").as_bytes(), u64::MAX)?
                .is_some(),
            "key {i} lost after a skipped compaction",
        );
    }
    Ok(())
}

#[test]
fn tight_space_compaction_rewrites_a_gated_multi_table_merge() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (tree, mem) = open_capped(folder.path(), u64::MAX);
    let n = 2_000u64;
    // Two overlapping generations → a multi-input merge whose latest values win.
    let used = seed_two_generations(&tree, n);
    let tables_before = tree.table_count();
    assert!(
        tables_before >= 2,
        "two generations must leave a multi-input merge"
    );

    // Cap so the cross-generation merge cannot fit; opt in to tight reclaim.
    mem.set_capacity(used + used / 4);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
        c.tight_space_compaction = true;
    })?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert!(
        mem.punched_bytes() > 0,
        "multi-input tight compaction must reclaim consumed slices via punching",
    );
    // The merge dropped the shadowed first generation; every key reads the
    // latest (second-generation) value.
    for i in 0..n {
        assert_eq!(
            tree.get(format!("key{i:08}").as_bytes(), u64::MAX)?
                .as_deref(),
            Some([0xEFu8; 64].as_slice()),
            "key {i} must read the latest value after multi-input tight compaction",
        );
    }

    // Survives reopen on the same simulated disk.
    drop(tree);
    let reopened = match Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem))
    .open()?
    {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    for i in (0..n).step_by(53) {
        assert_eq!(
            reopened
                .get(format!("key{i:08}").as_bytes(), u64::MAX)?
                .as_deref(),
            Some([0xEFu8; 64].as_slice()),
            "key {i} latest value must survive reopen",
        );
    }
    Ok(())
}

#[test]
fn tight_space_compaction_rewrites_a_gated_kv_separated_merge() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (tree, mem) = open_capped_blob(folder.path(), u64::MAX);
    let n = 2_000u64;

    // Values exceed the 64-byte separation threshold, so they live in blob
    // files; two overlapping generations make the merge drop shadowed values
    // (and their now-dead blobs).
    for i in 0..n {
        tree.insert(format!("key{i:08}").as_bytes(), vec![0xCDu8; 200], i);
    }
    tree.flush_active_memtable(0).expect("flush 1");
    for i in 0..n {
        tree.insert(format!("key{i:08}").as_bytes(), vec![0xEFu8; 200], n + i);
    }
    tree.flush_active_memtable(0).expect("flush 2");
    let used = tree.storage_stats().expect("stats").used_bytes;
    assert!(
        tree.table_count() >= 2,
        "two generations → multi-input merge"
    );

    // Cap so the index-tree SST output (small: just blob handles, but it is the
    // gated-merge's transient) does not fit, forcing the gate to skip the merge
    // and the tight path to slice it instead. Completing the merge is what lets
    // the tail drop the shadowed generation's now-dead blob files — the real
    // KV-separated reclaim, which a plain skip would forgo.
    let index_sst = tree.index.disk_space().max(4096);
    mem.set_capacity(used + index_sst / 4);
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
        c.tight_space_compaction = true;
    })?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert!(
        mem.punched_bytes() > 0,
        "KV-separated tight compaction must reclaim consumed slices via punching",
    );
    for i in 0..n {
        assert_eq!(
            tree.get(format!("key{i:08}").as_bytes(), u64::MAX)?
                .as_deref(),
            Some([0xEFu8; 200].as_slice()),
            "key {i} must read the latest value after KV-separated tight compaction",
        );
    }

    drop(tree);
    let reopened = match Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(64),
    ))
    .with_shared_fs(Arc::new(mem))
    .open()?
    {
        AnyTree::Blob(t) => t,
        AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    for i in (0..n).step_by(53) {
        assert_eq!(
            reopened
                .get(format!("key{i:08}").as_bytes(), u64::MAX)?
                .as_deref(),
            Some([0xEFu8; 200].as_slice()),
            "key {i} latest value must survive reopen",
        );
    }
    Ok(())
}

#[test]
fn tight_space_blob_defrag_relocates_a_fragmented_file_in_slices() -> lsm_tree::Result<()> {
    // A KV-separated tree whose blob files are HALF dead (a stale, not fully-dead
    // generation) must be DEFRAGMENTED under tight space: the tight loop relocates
    // each stale file's live entries into a fresh compact file in key-range slices,
    // punching the consumed prefix per slice, and the tail drops the now-consumed
    // stale files. Every key keeps its latest value, through the rewrite and a
    // reopen. Distinct from the fully-dead case (which just drops dead files): here
    // the live half is physically moved.
    let folder = get_tmp_folder();
    let n = 4_000u64;
    // High-entropy values so the blobs do not compress away (the relocation
    // transient must be real for the gate to skip the full merge). Deterministic
    // per (key, generation) for the post-reopen assertion.
    let val = |i: u64, generation: u8| -> Vec<u8> {
        let mut s = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (u64::from(generation) << 1);
        (0..200u32)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 24) as u8
            })
            .collect()
    };

    let mem = MemFs::with_capacity(u64::MAX);
    let tree = match Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem.clone()))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(64)
            // Keep every stale file and treat a lightly-dead file as stale, so the
            // half-shadowed generation is relocated rather than ignored.
            .age_cutoff(1.0)
            .staleness_threshold(0.1)
            // Small blob files → several per generation → the merge slices across
            // multiple stale files.
            .file_target_size(48 * 1024),
    ))
    .open()?
    {
        AnyTree::Blob(t) => t,
        AnyTree::Standard(_) => panic!("expected Blob tree"),
    };

    for i in 0..n {
        tree.insert(format!("key{i:08}").as_bytes(), val(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    // Overwrite EVEN keys only → every gen-1 blob file ends up ~half dead.
    for i in (0..n).step_by(2) {
        tree.insert(format!("key{i:08}").as_bytes(), val(i, 2), n + i);
    }
    tree.flush_active_memtable(0)?;

    // Fragmentation is learned during a merge, so run one ample-space merge (with
    // a watermark above every live seqno) to mark the even-key gen-1 blobs dead.
    let gc_watermark = 4 * n;
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    tree.major_compact(64 * 1024 * 1024, gc_watermark)?;

    let used = tree.storage_stats()?.used_bytes;
    // Cap so the full relocation of the stale generation cannot fit; opt in to
    // tight reclaim.
    mem.set_capacity(used + used / 4);
    tree.index.update_runtime_config(|c| {
        c.tight_space_compaction = true;
    })?;

    tree.major_compact(64 * 1024 * 1024, gc_watermark)?;

    // The defrag punched the consumed stale-file prefixes incrementally
    // (`punched_bytes` is the lifetime reclaim total, so it stays > 0 even though
    // the tail then deletes the fully-consumed stale files) AND the footprint
    // shrank as the dead halves were dropped.
    assert!(
        mem.punched_bytes() > 0,
        "blob defrag must reclaim the consumed stale-file prefixes via punching",
    );
    let used_after = tree.storage_stats()?.used_bytes;
    assert!(
        used_after < used,
        "blob defrag must reclaim the dead halves (used {used} -> {used_after})",
    );
    for i in 0..n {
        let expected = if i % 2 == 0 { val(i, 2) } else { val(i, 1) };
        assert_eq!(
            tree.get(format!("key{i:08}").as_bytes(), u64::MAX)?
                .as_deref(),
            Some(expected.as_slice()),
            "key {i} wrong after blob defrag (odd=gen1 relocated, even=gen2)",
        );
    }

    // Survives reopen on the same simulated disk.
    drop(tree);
    let reopened = match Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(64),
    ))
    .with_shared_fs(Arc::new(mem))
    .open()?
    {
        AnyTree::Blob(t) => t,
        AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    for i in (0..n).step_by(37) {
        let expected = if i % 2 == 0 { val(i, 2) } else { val(i, 1) };
        assert_eq!(
            reopened
                .get(format!("key{i:08}").as_bytes(), u64::MAX)?
                .as_deref(),
            Some(expected.as_slice()),
            "key {i} latest value must survive reopen after blob defrag",
        );
    }
    Ok(())
}
