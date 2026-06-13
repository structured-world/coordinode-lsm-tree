// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Cross-process exclusive directory lock (#422).
//!
//! `Config::open` / `Config::repair` take an advisory `flock` on a `LOCK` file
//! in the tree directory. Because `flock` conflicts across distinct open file
//! descriptions even within one process, these single-process tests exercise the
//! real contention path: a second `open()` of the same directory while the first
//! `Tree` is alive must fail fast with [`Error::Locked`], and the lock must be
//! released when the holding `Tree` is dropped.

use lsm_tree::{Config, Error, SequenceNumberCounter};

fn config(path: &std::path::Path) -> Config {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
}

#[test]
fn second_open_of_locked_directory_fails_with_locked() {
    let dir = tempfile::tempdir().expect("tempdir");

    // First open acquires and holds the directory lock for its lifetime.
    let _tree = config(dir.path()).open().expect("first open succeeds");

    // A second open of the same directory must fail fast, not block.
    // (`AnyTree` is not `Debug`, so match rather than `expect_err`.)
    let Err(err) = config(dir.path()).open() else {
        panic!("second open of a locked directory must fail");
    };
    assert!(
        matches!(err, Error::Locked(_)),
        "expected Error::Locked, got {err:?}",
    );
}

#[test]
fn dropping_the_tree_releases_the_directory_lock() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let _tree = config(dir.path()).open().expect("first open succeeds");
        // Lock is held here; dropping `_tree` at the end of this scope releases it.
    }

    // A second open of the same directory must succeed only because the lock was
    // released when the first tree dropped (otherwise it would be Error::Locked).
    let _tree = config(dir.path())
        .open()
        .expect("open after the holder dropped must succeed (lock released)");
}

#[test]
fn repair_of_open_directory_fails_with_locked() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Create the tree so there is a manifest to repair, and keep it open so it
    // holds the lock.
    let _tree = config(dir.path()).open().expect("first open succeeds");

    let err = config(dir.path())
        .repair()
        .expect_err("repair of a live (open) directory must fail");
    assert!(
        matches!(err, Error::Locked(_)),
        "expected Error::Locked, got {err:?}",
    );
}

#[test]
fn directory_lock_opt_out_allows_concurrent_open() {
    let dir = tempfile::tempdir().expect("tempdir");

    // With the lock disabled, a second concurrent open of the same directory is
    // permitted (the embedder is responsible for exclusivity).
    let _tree1 = config(dir.path())
        .with_directory_lock(false)
        .open()
        .expect("first open succeeds");
    let _tree2 = config(dir.path())
        .with_directory_lock(false)
        .open()
        .expect("second open succeeds when the directory lock is disabled");
}

#[test]
fn reopen_after_lock_disabled_open_still_works() {
    // Sanity: a tree opened with the lock disabled can be reopened (with the
    // lock back on) once dropped — the disabled run leaves no stuck lock.
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let _tree = config(dir.path())
            .with_directory_lock(false)
            .open()
            .expect("open with lock disabled");
    }
    let _tree = config(dir.path())
        .open()
        .expect("reopen with lock enabled succeeds");
}
