// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end tests for opt-in storage admission control: the computed
//! read-only predicate, `try_*` gated writes, `Error::StorageFull`, the
//! reserved-headroom guarantee (flush still works at the limit), and automatic
//! resume when the budget is raised.

use lsm_tree::{
    AbstractTree, AnyTree, Config, Error, KvSeparationOptions, SequenceNumberCounter,
    StorageStatus, get_tmp_folder,
};

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
    assert_eq!(tree.storage_stats()?.status, StorageStatus::Healthy);
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
