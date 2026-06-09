// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! `clear()` must reclaim the obsolete SST files synchronously enough that a
//! caller measuring on-disk footprint right after it returns sees the drop.
//! Regression guard for the v5 deferred-cleanup gap: `clear()` upgraded the
//! version but left the old SSTs referenced by stale version-history entries,
//! so a directory scan still saw the pre-clear bytes.

use lsm_tree::{AbstractTree, Config, SequenceNumberCounter, get_tmp_folder};
use std::path::Path;
use test_log::test;

/// Total bytes of every regular file under `path` (mirrors a capacity
/// scanner's `walkdir + sum(metadata.len())`).
fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let meta = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            if meta.is_dir() {
                stack.push(entry.path());
            } else {
                total += meta.len();
            }
        }
    }
    total
}

#[test]
fn clear_reclaims_sst_disk_footprint_synchronously() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();
    let tree = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let tree = match tree {
        lsm_tree::AnyTree::Standard(t) => t,
        lsm_tree::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    for i in 0..20_000u64 {
        tree.insert(
            format!("key:{i:010}").as_bytes(),
            b"payload-bytes-padding-padding-padding",
            i,
        );
    }
    tree.flush_active_memtable(0)?;

    let before = dir_size(path);
    assert!(
        before > 200_000,
        "flushed tree should occupy real disk, got {before} bytes",
    );

    tree.clear()?;

    let after = dir_size(path);
    assert!(
        after * 10 < before,
        "clear() must reclaim the SST footprint synchronously: before={before}, after={after}",
    );
    Ok(())
}

#[test]
fn clear_reclaims_blob_files_synchronously() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();
    let any = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(lsm_tree::KvSeparationOptions::default()))
    .open()?;
    let tree = match any {
        lsm_tree::AnyTree::Blob(t) => t,
        lsm_tree::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };

    // Large values are stored out-of-line; clear() must reclaim both the SSTs
    // and the blob files.
    let big = b"blobby-payload".repeat(8_000);
    for i in 0..40u64 {
        tree.insert(format!("k{i:04}").as_bytes(), &big, i);
    }
    tree.flush_active_memtable(0)?;
    assert!(tree.blob_file_count() > 0, "values must be blob-separated");

    // The repetitive payload compresses hard, so the on-disk footprint is far
    // below the logical size; the floor is just a sanity that blob + SST data
    // landed — the relative drop below is the real reclaim assertion.
    let before = dir_size(path);
    assert!(
        before > 20_000,
        "blob tree should occupy real disk, got {before}"
    );

    tree.clear()?;

    // Drop > 50%: the blob + SST bytes are reclaimed; what remains is the
    // small manifest baseline (snapshot + CURRENT + edit log), which is a
    // larger fraction here precisely because the payload compressed so well.
    let after = dir_size(path);
    assert!(
        after * 2 < before,
        "clear() must reclaim SST + blob footprint: before={before}, after={after}",
    );
    Ok(())
}
