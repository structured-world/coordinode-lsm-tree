// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end tests for opt-in storage admission control: the computed
//! read-only predicate, `try_*` gated writes, `Error::StorageFull`, the
//! reserved-headroom guarantee (flush still works at the limit), and automatic
//! resume when the budget is raised.

use lsm_tree::config::LevelRoute;
use lsm_tree::fs::MemFs;
use lsm_tree::{
    AbstractTree, AnyTree, Config, Error, KvSeparationOptions, SequenceNumberCounter,
    StorageStatus, get_tmp_folder,
};
use std::sync::Arc;

/// Opens a Standard tree on a shared `MemFs` clone, returning both so a test
/// can drive the backend's simulated free space.
fn open_tree_on_memfs(path: &std::path::Path) -> (lsm_tree::Tree, MemFs) {
    let mem = MemFs::new();
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

/// Opens a Standard tree on a `MemFs` capped at `capacity` bytes, modelling a
/// fixed-size in-memory disk that fills as data is flushed. Returns both the
/// tree and the backend clone.
fn open_tree_on_capped_memfs(path: &std::path::Path, capacity: u64) -> (lsm_tree::Tree, MemFs) {
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

fn open_tree(path: &std::path::Path) -> lsm_tree::Tree {
    let any = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    match any {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree"),
    }
}

fn open_blob_tree(path: &std::path::Path) -> lsm_tree::BlobTree {
    let any = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()
    .expect("open");
    match any {
        AnyTree::Blob(t) => t,
        AnyTree::Standard(_) => panic!("expected Blob tree"),
    }
}

/// Fill an SST so `disk_space()` is non-zero, returning that live size.
fn seed(tree: &lsm_tree::Tree) -> u64 {
    for i in 0..200u64 {
        tree.insert(format!("key{i:05}").as_bytes(), b"value-payload", i);
    }
    tree.flush_active_memtable(0).expect("flush");
    tree.disk_space()
}

#[test]
fn admission_off_by_default_admits_every_write() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());
    seed(&tree);

    // Default config has admission control off: the gate always opens, even
    // without any configured budget.
    assert!(tree.write_admission().is_ok());
    assert!(!tree.is_read_only());
    assert!(
        tree.try_insert(b"k".as_slice(), b"v".as_slice(), 1000)
            .is_ok()
    );
    Ok(())
}

#[test]
fn budget_below_usage_declines_gated_write_and_reports_read_only() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());
    let used = seed(&tree);

    // Enable admission with a budget well below the live footprint.
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = Some(used / 2);
    })?;

    assert!(tree.is_read_only(), "tree must be read-only under budget");

    match tree.try_insert(b"k".as_slice(), b"v".as_slice(), 1000) {
        Err(Error::StorageFull { used: u, limit }) => {
            assert!(u >= used, "reported used must reflect the live footprint");
            assert_eq!(limit, used / 2);
        }
        other => panic!("expected StorageFull, got {other:?}"),
    }

    // Every gated write entry refuses, including the tombstone variants — a
    // tombstone is itself a write that consumes space.
    assert!(
        tree.try_merge(b"k".as_slice(), b"v".as_slice(), 1001)
            .is_err()
    );
    assert!(tree.try_remove(b"k".as_slice(), 1002).is_err());
    assert!(tree.try_remove_weak(b"k".as_slice(), 1003).is_err());
    assert!(
        tree.try_remove_range(b"a".as_slice(), b"z".as_slice(), 1004)
            .is_err()
    );

    // The status surfaces the read-only state for operators / planners.
    assert_eq!(
        tree.storage_stats()?.status,
        StorageStatus::ReadOnlyOutOfSpace
    );

    // The tree stays consistent and reopenable: a previously written key is
    // still readable after reopen (the declined write changed nothing).
    drop(tree);
    let reopened = open_tree(folder.path());
    assert!(reopened.get(b"key00000".as_slice(), u64::MAX)?.is_some());
    Ok(())
}

#[test]
fn raising_budget_clears_read_only_without_restart() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());
    let used = seed(&tree);

    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = Some(used / 2);
    })?;
    assert!(
        tree.try_insert(b"k".as_slice(), b"v".as_slice(), 1000)
            .is_err()
    );

    // Raising the budget clears read-only on the very next check — the
    // predicate is computed, not latched, so no restart / explicit unstick.
    tree.update_runtime_config(|c| {
        c.storage_limit_bytes = Some(used + 1024 * 1024 * 1024);
    })?;
    assert!(!tree.is_read_only());
    assert!(
        tree.try_insert(b"k".as_slice(), b"v".as_slice(), 1001)
            .is_ok()
    );
    // Admission stays enabled with a generous budget: bounded capacity + ample
    // free room reports full-compaction availability, not the gate-off `Healthy`.
    assert_eq!(
        tree.storage_stats()?.status,
        StorageStatus::FullCompactionAvailable
    );
    Ok(())
}

#[test]
fn admission_counts_blob_files_not_just_the_index() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_blob_tree(folder.path());

    // Large values are KV-separated into blob files, so the index SSTs stay
    // small while the real footprint is dominated by blobs. Fill each value
    // with high-entropy bytes (cheap xorshift) so the blob footprint survives
    // compression under `--all-features` — a repeated-byte payload would shrink
    // to almost nothing and the blobs would no longer dominate.
    for i in 0..500u64 {
        let mut state = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let value: Vec<u8> = (0..4096u32)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect();
        tree.insert(format!("key{i:05}").as_bytes(), &value, i);
    }
    tree.flush_active_memtable(0)?;

    let index_only = tree.index.disk_space(); // index SST size, WITHOUT blobs
    let total = tree.storage_stats()?.used_bytes; // physical, includes blobs
    assert!(
        total > index_only + 1024 * 1024,
        "blob footprint must dominate (index {index_only}, total {total})"
    );

    // Budget set just above the index size + reserved headroom: an index-only
    // gate would admit, but the tree is already far over budget in blobs.
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = Some(index_only + 1024 * 1024 + 4096);
    })?;

    assert!(
        tree.is_read_only(),
        "blob bytes must count toward admission, not just the index"
    );
    assert!(
        tree.try_insert(b"k".as_slice(), b"v".as_slice(), 10_000)
            .is_err()
    );
    assert_eq!(
        tree.storage_stats()?.status,
        StorageStatus::ReadOnlyOutOfSpace
    );
    Ok(())
}

#[test]
fn every_gated_write_admits_when_the_gate_is_open() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());
    seed(&tree);

    // Admission enabled but with a generous budget: the gate is open, so every
    // gated write entry takes its Ok branch.
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = Some(1024 * 1024 * 1024);
    })?;
    assert!(tree.write_admission().is_ok());
    assert!(!tree.is_read_only());

    assert!(
        tree.try_insert(b"a".as_slice(), b"v".as_slice(), 1000)
            .is_ok()
    );
    assert!(
        tree.try_merge(b"b".as_slice(), b"v".as_slice(), 1001)
            .is_ok()
    );
    assert!(tree.try_remove(b"a".as_slice(), 1002).is_ok());
    assert!(tree.try_remove_weak(b"b".as_slice(), 1003).is_ok());
    assert!(
        tree.try_remove_range(b"a".as_slice(), b"z".as_slice(), 1004)
            .is_ok()
    );
    Ok(())
}

#[test]
fn admission_enabled_without_a_budget_is_unbounded() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());
    seed(&tree);

    // Admission on but no configured quota → unbounded (the disk-free probe
    // that would otherwise narrow the limit is a follow-up). Writes admit.
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    assert!(!tree.is_read_only());
    assert!(
        tree.try_insert(b"k".as_slice(), b"v".as_slice(), 1000)
            .is_ok()
    );
    Ok(())
}

#[test]
fn blob_tree_admits_gated_writes_when_open() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_blob_tree(folder.path());
    tree.insert(b"seed".as_slice(), b"value".as_slice(), 0);
    tree.flush_active_memtable(0)?;

    // Gate open (admission off by default): the BlobTree forward admits.
    assert!(tree.write_admission().is_ok());
    assert!(!tree.is_read_only());
    assert!(tree.try_insert(b"k".as_slice(), b"v".as_slice(), 1).is_ok());
    Ok(())
}

#[test]
fn cached_usage_refreshes_when_a_new_version_is_installed() -> lsm_tree::Result<()> {
    // Write `range` 1 KiB high-entropy values (incompressible, so it holds
    // under `--all-features` compression) and flush, returning the live size.
    fn flush_batch(tree: &lsm_tree::Tree, range: std::ops::Range<u64>) -> u64 {
        for i in range {
            let mut s = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let value: Vec<u8> = (0..1024u32)
                .map(|_| {
                    s ^= s << 13;
                    s ^= s >> 7;
                    s ^= s << 17;
                    (s >> 24) as u8
                })
                .collect();
            tree.insert(format!("key{i:06}").as_bytes(), &value, i);
        }
        tree.flush_active_memtable(0).expect("flush");
        tree.disk_space()
    }

    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());
    let used1 = flush_batch(&tree, 0..4000); // version V1, ~4 MiB

    // Budget above V1 + reserved headroom (1 MiB floor) but below what a second
    // ~4 MiB batch reaches: the gate caches V1's usage and admits now.
    let budget = used1 + 2 * 1024 * 1024;
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = Some(budget);
    })?;
    assert!(
        !tree.is_read_only(),
        "must admit while under budget (used1 {used1})"
    );

    // Second batch installs a new version (V2), growing the footprint past the
    // budget. The per-version cache must invalidate on the version change rather
    // than serve V1's smaller usage.
    let used2 = flush_batch(&tree, 4000..8000);
    assert!(
        used2 > budget,
        "second flush must exceed the budget (used2 {used2})"
    );

    assert!(
        tree.is_read_only(),
        "cache must refresh on the new version and see the over-budget footprint"
    );
    assert!(
        tree.try_insert(b"k".as_slice(), b"v".as_slice(), 9999)
            .is_err()
    );
    Ok(())
}

#[test]
fn reserved_headroom_counts_sealed_memtables_pending_flush() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Fill ~2 MiB of high-entropy data into the active memtable WITHOUT
    // flushing, then rotate: the large memtable is sealed (queued for flush)
    // and a fresh empty active memtable is installed.
    for i in 0..2000u64 {
        let mut s = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let value: Vec<u8> = (0..1024u32)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 24) as u8
            })
            .collect();
        tree.insert(format!("key{i:06}").as_bytes(), &value, i);
    }
    let sealed = tree.rotate_memtable();
    assert!(sealed.is_some(), "active memtable must seal");

    // Nothing is on disk yet (no flush), but the sealed memtable (~2 MiB)
    // will consume space once flushed. A budget of 1.5 MiB sits above the
    // 1 MiB reserved floor but below the pending sealed footprint: reserved
    // headroom must count the sealed memtable, not just the (now empty) active
    // one, so the gate is closed.
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = Some(3 * 512 * 1024); // 1.5 MiB
    })?;

    assert!(
        tree.is_read_only(),
        "sealed memtable pending flush must count toward reserved headroom"
    );
    Ok(())
}

#[test]
fn disk_free_drives_read_only_without_a_configured_quota() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let (tree, mem) = open_tree_on_memfs(folder.path());
    for i in 0..100u64 {
        tree.insert(format!("key{i:05}").as_bytes(), b"value", i);
    }
    tree.flush_active_memtable(0)?;

    // Admission on, NO configured quota: only physical disk-free can gate.
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    // MemFs defaults to u64::MAX free → no disk pressure → admits.
    assert!(!tree.is_read_only(), "ample free space must admit");

    // Simulate a full disk by capping the MemFs capacity at zero: with any
    // bytes already stored, capacity − stored saturates to zero free. The
    // cached probe still holds the old free value (sampled at the last
    // version), so force a re-probe the way an operator action would:
    // update_runtime_config clears the cache.
    mem.set_capacity(0);
    tree.update_runtime_config(|_c| {})?;

    assert!(
        tree.is_read_only(),
        "a near-full disk must drive read-only even without a configured quota"
    );
    match tree.try_insert(b"k".as_slice(), b"v".as_slice(), 1000) {
        Err(Error::StorageFull { .. }) => {}
        other => panic!("expected StorageFull, got {other:?}"),
    }
    assert_eq!(
        tree.storage_stats()?.status,
        StorageStatus::ReadOnlyOutOfSpace
    );
    Ok(())
}

#[test]
fn disk_free_re_probes_on_ttl_expiry_without_a_version_change() -> lsm_tree::Result<()> {
    // The disk-free sample is re-probed once it ages past the TTL even when the
    // version (and thus the cached footprint) is unchanged: an external process
    // filling the shared disk must drive the tree read-only without waiting for
    // the next flush / compaction. Uses a capped MemFs whose free space we lower
    // out from under a steady version, then waits past the 1s TTL so the next
    // check re-probes rather than serving the stale free value.
    let folder = get_tmp_folder();
    let (tree, mem) = open_tree_on_capped_memfs(folder.path(), 100 * 1024 * 1024);
    seed(&tree); // installs a version; the footprint stays fixed hereafter

    // No quota: only the physical free-space probe gates. 100 MiB free admits.
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    assert!(!tree.is_read_only(), "100 MiB free must admit");

    // Shrink the simulated disk so available falls below the reserved headroom,
    // WITHOUT a version change or a config update (either would clear the cache
    // and force an immediate re-probe, bypassing the path under test). Capacity
    // just above the live footprint leaves only a sliver free (< the 1 MiB
    // reserved floor), since the MemFs also holds the manifest / lock / journal.
    let used = tree.storage_stats()?.used_bytes;
    mem.set_capacity(used + 1024);

    // Within the TTL the stale free value is still served, so the tree is not
    // yet read-only; after the TTL elapses the next check re-probes and sees the
    // near-full disk.
    std::thread::sleep(std::time::Duration::from_millis(1_100));
    assert!(
        tree.is_read_only(),
        "a TTL-expired re-probe must observe the shrunk disk and gate writes"
    );
    assert_eq!(
        tree.storage_stats()?.status,
        StorageStatus::ReadOnlyOutOfSpace
    );
    Ok(())
}

#[test]
fn a_near_full_routed_volume_drives_the_whole_tree_read_only() -> lsm_tree::Result<()> {
    // Per-volume safety: the disk-free gate mins free space across the primary
    // path AND every level route, so a near-full routed (cold-tier) volume
    // turns the whole tree read-only even while the primary has ample room and
    // no quota is configured. The `+ used` in the effective limit cancels out
    // of the disk branch (passing requires reserved <= min_free), so an empty
    // routed volume's slack can never be summed against the primary's
    // occupancy to mask the tight volume. Without that, a later flush /
    // compaction targeting the route could hit ENOSPC.
    let folder = get_tmp_folder();

    // Primary volume: unbounded in-memory disk. Routed cold tier (L5+): a
    // capped in-memory disk with less than the reserved headroom free.
    let primary = MemFs::new();
    let cold = MemFs::with_capacity(64 * 1024); // < MIN_RESERVED_HEADROOM (1 MiB)
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(primary))
    .level_routes(vec![LevelRoute {
        levels: 5..7,
        path: folder.path().join("cold"),
        fs: Arc::new(cold),
    }])
    .open()
    .expect("open");
    let tree = match any {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    // Admission on, NO quota: only the physical free-space probe can gate, and
    // it must reflect the tightest volume.
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;

    assert!(
        tree.is_read_only(),
        "a routed volume below the reserved headroom must drive the tree read-only"
    );
    let stats = tree.storage_stats()?;
    assert_eq!(stats.status, StorageStatus::ReadOnlyOutOfSpace);
    assert!(
        !stats.compaction_possible,
        "no headroom on the tightest volume means compaction cannot run"
    );
    Ok(())
}

#[test]
fn storage_stats_exposes_used_capacity_available_and_compaction_flag() -> lsm_tree::Result<()> {
    // The introspection UX: one storage_stats() call answers "X bytes used of Y
    // total, Z available" plus whether a compaction can still run. The three
    // byte figures satisfy used + available == capacity by construction.
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());
    let seeded = seed(&tree);

    // A generous quota is the tighter bound vs the real tempdir's free space, so
    // capacity is deterministic; ample room means compaction is possible.
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = Some(seeded + 100 * 1024 * 1024);
    })?;
    let s = tree.storage_stats()?;
    let capacity = s
        .capacity_bytes
        .expect("a configured quota sets a finite capacity");
    let available = s
        .available_bytes
        .expect("a finite capacity yields a finite available figure");
    assert_eq!(
        s.used_bytes.saturating_add(available),
        capacity,
        "used + available must equal capacity"
    );
    assert!(
        s.used_bytes < capacity,
        "used must be below a generous capacity"
    );
    assert!(
        s.compaction_possible,
        "ample free space must allow compaction"
    );

    // Tighten the quota to just above the live footprint: introspection still
    // reports the figures, but there is no room left for a compaction to write
    // its merged output.
    tree.update_runtime_config(|c| {
        c.storage_limit_bytes = Some(s.used_bytes + 1024);
    })?;
    let tight = tree.storage_stats()?;
    let tight_avail = tight.available_bytes.expect("finite capacity");
    assert!(
        tight_avail < 1024 * 1024,
        "a near-full quota must leave less than the reserved compaction floor"
    );
    assert!(
        !tight.compaction_possible,
        "no working room must report compaction as not possible"
    );
    Ok(())
}

#[test]
fn storage_stats_reports_unbounded_capacity_without_quota_or_probe() -> lsm_tree::Result<()> {
    // With no quota and a backend that cannot report free space (default MemFs),
    // capacity is genuinely unbounded: the figures are None and compaction is
    // always possible. Honest absence beats a fabricated number.
    let folder = get_tmp_folder();
    let (tree, _mem) = open_tree_on_memfs(folder.path());
    seed(&tree);
    let s = tree.storage_stats()?;
    assert_eq!(
        s.capacity_bytes, None,
        "unbounded capacity is reported as None"
    );
    assert_eq!(s.available_bytes, None);
    assert!(s.compaction_possible);
    Ok(())
}

#[test]
fn capped_memfs_fills_naturally_and_drives_read_only() -> lsm_tree::Result<()> {
    // The unified capacity UX: a fixed-size disk (here an in-memory one) fills
    // as data is flushed, and admission flips to read-only once the remaining
    // free space can no longer cover the reserved headroom, with no operator
    // action and no configured quota. A 3 MiB disk against the >=1 MiB reserved
    // floor crosses that threshold after a handful of flushed SST files.
    let folder = get_tmp_folder();
    let (tree, _mem) = open_tree_on_capped_memfs(folder.path(), 3 * 1024 * 1024);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;

    // Each flush is a version transition that re-probes the simulated disk, so
    // the falling free space is observed without clearing the cache manually.
    let value = vec![0xABu8; 4096];
    let mut seqno = 0u64;
    let mut became_read_only = false;
    for _ in 0..64 {
        for _ in 0..100 {
            tree.insert(format!("key{seqno:08}").as_bytes(), &value, seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
        if tree.is_read_only() {
            became_read_only = true;
            break;
        }
    }

    assert!(
        became_read_only,
        "a capped in-memory disk must fill and flip to read-only as data is flushed"
    );
    match tree.try_insert(b"k".as_slice(), b"v".as_slice(), seqno) {
        Err(Error::StorageFull { .. }) => {}
        other => panic!("expected StorageFull on a full capped disk, got {other:?}"),
    }
    assert_eq!(
        tree.storage_stats()?.status,
        StorageStatus::ReadOnlyOutOfSpace
    );
    Ok(())
}

#[test]
fn flush_and_bare_insert_are_never_gated_at_the_limit() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());
    let used = seed(&tree);

    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = Some(used / 2);
    })?;
    assert!(tree.is_read_only());

    // Bare insert is the ungated convenience path: it still succeeds (the
    // memtable accepts it), so the reserved headroom must absorb the next
    // flush rather than the limit becoming a hard wall.
    let _ = tree.insert(b"ungated".as_slice(), b"v".as_slice(), 2000);

    // Internal flush is never gated by the user budget — it must succeed even
    // while the gate is closed, so the engine can always drain the memtable.
    tree.flush_active_memtable(0)?;
    assert!(
        tree.get(b"ungated".as_slice(), u64::MAX)?.is_some(),
        "ungated write must have flushed despite the closed gate"
    );
    Ok(())
}
