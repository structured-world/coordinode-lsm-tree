// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Tests for the `Fs::available_space` free-space probe across backends.

use lsm_tree::fs::{Fs, MemFs, StdFs};

#[test]
fn std_fs_reports_plausible_free_space() {
    // statvfs on the tempdir's filesystem must return a plausible, non-zero
    // figure (any real mounted FS has some free space and a sane upper bound
    // below u64::MAX — the "unbounded" default only the trait fallback uses).
    let dir = tempfile::tempdir().expect("tempdir");
    let free = StdFs.available_space(dir.path()).expect("statvfs");
    assert!(
        free > 0,
        "a writable tempdir filesystem must report free space"
    );
    assert!(
        free < u64::MAX,
        "a real probe must not return the unbounded sentinel"
    );
}

#[test]
fn std_fs_available_space_nonexistent_path_errors() {
    // statvfs on a path that does not exist must surface an error, not a
    // silent zero or the unbounded sentinel. Own the precondition: a child
    // of a fresh tempdir that we deliberately never create.
    let dir = tempfile::tempdir().expect("tempdir");
    let missing = dir.path().join("definitely-absent-child");
    assert!(StdFs.available_space(&missing).is_err());
}

#[test]
fn mem_fs_defaults_to_unbounded() {
    // A fresh MemFs reports u64::MAX until a capacity is configured, so it
    // never spuriously drives disk-pressure logic.
    let fs = MemFs::new();
    assert_eq!(
        fs.available_space(std::path::Path::new("/")).unwrap(),
        u64::MAX
    );
}

#[test]
fn mem_fs_reports_configured_capacity_shared_across_clones() {
    // set_available_space simulates a near-full disk; the value is shared with
    // clones (same backend) so a tree holding a clone sees it.
    let fs = MemFs::new();
    fs.set_available_space(4096);
    assert_eq!(fs.available_space(std::path::Path::new("/")).unwrap(), 4096);

    let clone = fs.clone();
    assert_eq!(
        clone.available_space(std::path::Path::new("/")).unwrap(),
        4096
    );
    // A write through one handle's setter is observed through the other.
    clone.set_available_space(0);
    assert_eq!(fs.available_space(std::path::Path::new("/")).unwrap(), 0);
}
