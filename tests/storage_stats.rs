// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end tests for `AbstractTree::storage_stats`: used-byte accounting
//! (cross-checked against the checkpoint total), the average K/V shape recorded
//! per table, and the remaining-capacity estimate.

// `AbstractTree` is used for its trait methods (`insert`,
// `flush_active_memtable`, `storage_stats`, `create_checkpoint`).
use lsm_tree::{
    AbstractTree, AnyTree, Config, SequenceNumberCounter, StorageStatus, get_tmp_folder,
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

#[test]
fn storage_stats_reports_shape_and_capacity_after_flush() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // 500 unique entries, fixed shape: 8-byte keys ("key00000"), 10-byte values
    // ("value00000"). One version each, so item_count == 500 and the averages
    // are exact.
    let n = 500u64;
    for i in 0..n {
        let key = format!("key{i:05}");
        let value = format!("value{i:05}");
        assert_eq!(key.len(), 8);
        assert_eq!(value.len(), 10);
        tree.insert(key.as_bytes(), value.as_bytes(), i);
    }
    tree.flush_active_memtable(0)?;

    let stats = tree.storage_stats()?;

    assert_eq!(stats.item_count, n, "every inserted entry is counted");
    assert!(stats.table_count >= 1, "at least one SST after flush");
    assert!(stats.used_bytes > 0);
    assert_eq!(
        stats.status,
        StorageStatus::Healthy,
        "no compaction running"
    );

    // Exact average shape from the per-table byte sums.
    assert_eq!(stats.avg_key_bytes, Some(8), "8-byte keys");
    assert_eq!(stats.avg_value_bytes, Some(10), "10-byte values");

    // Average on-disk entry size is positive and the remaining-capacity
    // estimate is self-consistent: a budget equal to the current usage fits
    // about as many more entries as are already stored (integer rounding aside).
    assert!(stats.avg_entry_on_disk_bytes > 0);
    let est = stats.estimated_remaining_entries(stats.used_bytes);
    assert!(
        est.abs_diff(n) <= n / 10,
        "estimate {est} should be within ~10% of {n}"
    );

    // Cross-check used_bytes against the checkpoint's independently-computed
    // total (a different code path summing the same live files).
    let cp_dir = get_tmp_folder();
    let cp_path = cp_dir.path().join("checkpoint");
    let info = tree.create_checkpoint(&cp_path)?;
    assert_eq!(
        stats.used_bytes, info.total_bytes,
        "storage_stats used_bytes must match the checkpoint total"
    );

    Ok(())
}

#[test]
fn storage_stats_survive_flush_and_reopen() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    {
        let tree = open_tree(folder.path());
        for i in 0..200u64 {
            tree.insert(
                format!("key{i:05}").as_bytes(),
                format!("value{i:05}").as_bytes(),
                i,
            );
        }
        tree.flush_active_memtable(0)?;
    }

    // Reopen forces the per-table byte sums to be read back from the on-disk
    // meta block (not the in-memory writer metadata), proving they round-trip.
    let reopened = open_tree(folder.path());
    let stats = reopened.storage_stats()?;
    assert_eq!(stats.item_count, 200);
    assert_eq!(stats.avg_key_bytes, Some(8));
    assert_eq!(stats.avg_value_bytes, Some(10));
    assert!(stats.used_bytes > 0);

    Ok(())
}

#[test]
fn storage_stats_empty_tree_has_zero_usage_and_no_estimate() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    let stats = tree.storage_stats()?;
    assert_eq!(stats.used_bytes, 0);
    assert_eq!(stats.item_count, 0);
    assert_eq!(stats.table_count, 0);
    assert_eq!(stats.avg_entry_on_disk_bytes, 0);
    // No basis to extrapolate from an empty tree.
    assert_eq!(stats.estimated_remaining_entries(1_000_000), 0);
    Ok(())
}
