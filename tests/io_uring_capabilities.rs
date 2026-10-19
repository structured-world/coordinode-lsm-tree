// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The paths that adapt to the file system run on the `io_uring` backend as
//! they run on the std one: tight-space compaction slices and punches its
//! inputs, a blob defragmentation punches the prefixes it relocated, and a
//! checkpoint clones where the file system clones. Skipped when the kernel
//! lacks `io_uring`.

#![cfg(all(target_os = "linux", feature = "io-uring"))]

use lsm_tree::fs::{Fs, IoUringFs, StdFs, is_io_uring_available};
use lsm_tree::{
    AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter, get_tmp_folder,
};
use std::path::Path;
use std::sync::Arc;

/// The `io_uring` backend, or `None` (and a note) when the kernel lacks it
/// or the directory's file system cannot punch holes. A ring that cannot be
/// set up where the kernel offers one is the test's failure.
fn io_uring_punching(dir: &Path) -> lsm_tree::Result<Option<IoUringFs>> {
    if !is_io_uring_available() {
        eprintln!("skipping: io_uring not supported by the kernel");
        return Ok(None);
    }
    if !StdFs.capabilities(dir).punch_hole {
        eprintln!("skipping: the test directory's file system cannot punch holes");
        return Ok(None);
    }
    Ok(Some(IoUringFs::new()?))
}

fn config(dir: &Path, fs: &IoUringFs) -> Config {
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fs.clone()))
}

/// A table over the tree's quota is rewritten in slices, each consumed input
/// prefix punched as it goes, and every key reads back, also after a reopen.
#[test]
fn a_tight_space_compaction_runs_on_io_uring() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let Some(fs) = io_uring_punching(folder.path())? else {
        return Ok(());
    };
    let AnyTree::Standard(tree) = config(folder.path(), &fs).open()? else {
        panic!("a standard tree");
    };
    let n = 2_000u64;
    for i in 0..n {
        tree.insert(format!("key{i:08}"), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    let used = tree.storage_stats()?.used_bytes;
    let tables_before = tree.table_count();

    // The quota leaves room for a slice of the rewrite, not for all of it.
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = Some(used + used / 4);
        c.tight_space_compaction = true;
    })?;
    tree.major_compact(64 * 1024 * 1024, 0)?;
    assert!(
        tree.table_count() > tables_before,
        "the table is rewritten in slices (a pass that cannot punch leaves {tables_before})"
    );
    for i in 0..n {
        assert_eq!(
            tree.get(format!("key{i:08}"), u64::MAX)?.as_deref(),
            Some([0xCDu8; 64].as_slice()),
            "key {i}"
        );
    }

    drop(tree);
    let reopened = config(folder.path(), &fs).open()?;
    for i in (0..n).step_by(53) {
        assert_eq!(
            reopened.get(format!("key{i:08}"), u64::MAX)?.as_deref(),
            Some([0xCDu8; 64].as_slice()),
            "key {i} after reopen"
        );
    }
    Ok(())
}

/// A half-dead generation of blob files over the tree's quota is relocated
/// in slices, the relocated prefixes punched as it goes: the footprint
/// shrinks and every key keeps its latest value.
#[test]
fn a_tight_space_blob_defragmentation_runs_on_io_uring() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let Some(fs) = io_uring_punching(folder.path())? else {
        return Ok(());
    };
    // High-entropy values, so the relocation is real and does not fit the quota.
    let value = |i: u64, generation: u8| -> Vec<u8> {
        let mut s = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (u64::from(generation) << 1);
        (0..200u32)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                s.to_le_bytes()[3]
            })
            .collect()
    };
    let AnyTree::Blob(tree) = config(folder.path(), &fs)
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(64)
                .age_cutoff(1.0)
                .staleness_threshold(0.1)
                .file_target_size(48 * 1024),
        ))
        .open()?
    else {
        panic!("a blob tree");
    };
    let n = 4_000u64;
    for i in 0..n {
        tree.insert(format!("key{i:08}"), value(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    // Overwriting the even keys leaves every first-generation blob file about
    // half dead, which a merge learns.
    for i in (0..n).step_by(2) {
        tree.insert(format!("key{i:08}"), value(i, 2), n + i);
    }
    tree.flush_active_memtable(0)?;
    let gc_watermark = 4 * n;
    tree.major_compact(64 * 1024 * 1024, gc_watermark)?;

    let used = tree.storage_stats()?.used_bytes;
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = Some(used + used / 4);
        c.tight_space_compaction = true;
    })?;
    tree.major_compact(64 * 1024 * 1024, gc_watermark)?;
    let used_after = tree.storage_stats()?.used_bytes;
    assert!(
        used_after < used,
        "the dead halves are reclaimed (used {used} -> {used_after})"
    );
    for i in 0..n {
        let expected = if i % 2 == 0 { value(i, 2) } else { value(i, 1) };
        assert_eq!(
            tree.get(format!("key{i:08}"), u64::MAX)?.as_deref(),
            Some(expected.as_slice()),
            "key {i}"
        );
    }
    Ok(())
}

/// A checkpoint on the `io_uring` backend clones its tables where the file
/// system clones and links them where it does not, as on the std backend: a
/// clone is an inode of its own, a link a second name of the table's.
#[test]
fn a_checkpoint_on_io_uring_clones_where_the_file_system_does() -> lsm_tree::Result<()> {
    if !is_io_uring_available() {
        eprintln!("skipping: io_uring not supported by the kernel");
        return Ok(());
    }
    let fs = IoUringFs::new()?;
    let folder = get_tmp_folder();
    let AnyTree::Standard(tree) = config(folder.path(), &fs).open()? else {
        panic!("a standard tree");
    };
    tree.update_runtime_config(|c| c.use_reflink_for_checkpoint = true)?;
    tree.insert("key", "value", 0);
    tree.flush_active_memtable(0)?;

    let checkpoints = get_tmp_folder();
    let target = checkpoints.path().join("checkpoint");
    tree.create_checkpoint(&target)?;
    let clones = StdFs.capabilities(checkpoints.path()).reflink;
    let tables = fs.read_dir(&folder.path().join(lsm_tree::file::TABLES_FOLDER))?;
    let [table] = tables.as_slice() else {
        panic!("the flush wrote one table: {tables:?}");
    };
    let links = Fs::hard_link_count(&fs, &table.path)?;
    assert_eq!(
        links,
        if clones { 1 } else { 2 },
        "the table's names (clones: {clones})"
    );
    let reopened = Config::new(
        &target,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    assert_eq!(
        reopened.get("key", u64::MAX)?.as_deref(),
        Some(b"value".as_slice())
    );
    Ok(())
}
