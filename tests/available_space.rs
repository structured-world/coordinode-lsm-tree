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

#[cfg(unix)]
#[test]
fn std_fs_available_space_path_with_interior_nul_errors() {
    // A path containing an interior NUL byte cannot become a C string, so the
    // statvfs helper must surface InvalidInput rather than passing a truncated
    // path to the kernel.
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let bad = std::path::Path::new(OsStr::from_bytes(b"/tmp/has\0nul"));
    let err = StdFs
        .available_space(bad)
        .expect_err("an interior NUL must be rejected");
    assert_eq!(err.kind(), lsm_tree::io::ErrorKind::InvalidInput);
}

#[cfg(windows)]
#[test]
fn std_fs_available_space_path_with_interior_nul_errors() {
    // Symmetric to the unix case: Win32 treats the first NUL as the wide-string
    // terminator, so an interior NUL must be rejected with InvalidInput rather
    // than probing the truncated (wrong-volume) path.
    use std::ffi::OsString;
    use std::os::windows::ffi::OsStringExt;

    let bad_os = OsString::from_wide(&[b'C' as u16, b':' as u16, 0, b'x' as u16]);
    let bad = std::path::PathBuf::from(bad_os);
    let err = StdFs
        .available_space(&bad)
        .expect_err("an interior NUL must be rejected");
    assert_eq!(err.kind(), lsm_tree::io::ErrorKind::InvalidInput);
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
fn mem_fs_capacity_minus_stored_shared_across_clones() {
    use lsm_tree::fs::{Fs, FsOpenOptions};
    use std::io::Write;

    // A capped MemFs reports capacity − bytes stored: an empty 4 KiB disk has
    // 4 KiB free.
    let fs = MemFs::with_capacity(4096);
    assert_eq!(fs.available_space(std::path::Path::new("/")).unwrap(), 4096);

    // Writing consumes the simulated disk; free space shrinks by what is stored.
    {
        let mut f = fs
            .open(
                std::path::Path::new("/f"),
                &FsOpenOptions::new().write(true).create(true),
            )
            .expect("open");
        f.write_all(&[0u8; 1000]).expect("write");
        f.sync_all().expect("sync");
    }
    assert_eq!(
        fs.available_space(std::path::Path::new("/")).unwrap(),
        4096 - 1000
    );

    // Capacity (and the stored bytes) are shared with clones (same backend).
    let clone = fs.clone();
    assert_eq!(
        clone.available_space(std::path::Path::new("/")).unwrap(),
        4096 - 1000
    );
    // Lowering capacity below what is stored saturates to zero free.
    clone.set_capacity(500);
    assert_eq!(fs.available_space(std::path::Path::new("/")).unwrap(), 0);
}
