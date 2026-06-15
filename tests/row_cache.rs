// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Correctness of the opt-in row cache (decoded point-read results keyed by the
//! owning SST + user-key hash). The row cache must never change observed values:
//! it only short-circuits the index walk + data-block decode when the cached
//! newest version of a key is visible at the query snapshot.

use lsm_tree::{AbstractTree, Cache, Config, SeqNo, SequenceNumberCounter, get_tmp_folder};
use std::sync::Arc;
use test_log::test;

const N: u64 = 5_000;

fn key(i: u64) -> Vec<u8> {
    format!("key-{i:08}").into_bytes()
}

fn val(i: u64) -> Vec<u8> {
    format!("value-{i:08}").into_bytes()
}

/// A flushed single-SST tree sharing a row-cache-enabled cache.
fn tree_with_row_cache() -> (tempfile::TempDir, lsm_tree::AnyTree, Arc<Cache>) {
    let folder = get_tmp_folder();
    let dir = tempfile::TempDir::new_in(folder).unwrap();
    let cache = Arc::new(Cache::with_capacity_bytes(64 * 1024 * 1024).with_row_cache(true));
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(cache.clone())
    .open()
    .unwrap();
    (dir, tree, cache)
}

#[test]
fn row_cache_is_disabled_by_default() {
    let cache = Cache::with_capacity_bytes(1024);
    assert!(!cache.row_cache_enabled());
    assert!(
        Cache::with_capacity_bytes(1024)
            .with_row_cache(true)
            .row_cache_enabled()
    );
}

#[test]
fn row_cache_serves_every_key_correctly_on_repeat_reads() {
    // Read every key twice: the first pass populates the row cache (latest-version
    // reads), the second pass is served from it. Every value must match — a hash
    // collision returning a wrong value, or a stale entry, would fail here.
    let (_dir, tree, _cache) = tree_with_row_cache();
    for i in 0..N {
        tree.insert(key(i), val(i), 0);
    }
    tree.flush_active_memtable(0).unwrap();

    for pass in 0..2 {
        for i in 0..N {
            let got = tree
                .get(key(i), SeqNo::MAX)
                .unwrap()
                .unwrap_or_else(|| panic!("pass {pass}: key {i} missing"));
            assert_eq!(&*got, val(i).as_slice(), "pass {pass}: wrong value for {i}");
        }
    }
    // A key that was never inserted stays absent (no false positive from the cache).
    assert!(tree.get(key(N + 1), SeqNo::MAX).unwrap().is_none());
}

#[test]
fn row_cache_snapshot_read_returns_the_older_version() {
    // Two versions of the same key in two SSTs. A latest read (which populates
    // the cache with v2) must not make an older-snapshot read serve the cached
    // v2: the snapshot must still see v1.
    let (_dir, tree, _cache) = tree_with_row_cache();
    let k = key(1);

    tree.insert(&k, val(1), 1); // version @ seqno 1
    tree.flush_active_memtable(0).unwrap();
    tree.insert(&k, val(2), 2); // version @ seqno 2
    tree.flush_active_memtable(0).unwrap();

    // Latest read: sees v2 and caches the newest version for the newer SST.
    assert_eq!(
        &*tree.get(&k, SeqNo::MAX).unwrap().unwrap(),
        val(2).as_slice()
    );
    // Repeat latest read: served from the row cache, still v2.
    assert_eq!(
        &*tree.get(&k, SeqNo::MAX).unwrap().unwrap(),
        val(2).as_slice()
    );

    // Snapshot reads are exclusive (a snapshot at N sees versions with seqno < N).
    // Snapshot 2: v2 (seqno 2) is not visible, so the cached newest must be
    // bypassed and the older v1 (seqno 1) returned.
    assert_eq!(&*tree.get(&k, 2).unwrap().unwrap(), val(1).as_slice());
    // Snapshot 3: v2 (seqno 2) is now visible.
    assert_eq!(&*tree.get(&k, 3).unwrap().unwrap(), val(2).as_slice());
}

#[test]
fn row_cache_overwrite_after_flush_is_not_stale() {
    // Overwriting a key and flushing creates a NEW SST (new table id). The old
    // SST's cached row is keyed by the old table id, so reads of the new SST miss
    // it and see the fresh value — never the stale cached one.
    let (_dir, tree, _cache) = tree_with_row_cache();
    let k = key(7);

    tree.insert(&k, val(100), 1);
    tree.flush_active_memtable(0).unwrap();
    // Populate the row cache for the first SST.
    assert_eq!(
        &*tree.get(&k, SeqNo::MAX).unwrap().unwrap(),
        val(100).as_slice()
    );

    tree.insert(&k, val(200), 2);
    tree.flush_active_memtable(0).unwrap();
    // The newer SST has a different table id; the read must see the new value.
    assert_eq!(
        &*tree.get(&k, SeqNo::MAX).unwrap().unwrap(),
        val(200).as_slice()
    );
}

#[test]
fn row_cache_off_matches_row_cache_on() {
    // The same workload with the row cache off vs on must observe identical
    // values — the cache is a pure read accelerator, never a behaviour change.
    let folder = get_tmp_folder();
    let dir = tempfile::TempDir::new_in(folder).unwrap();
    let plain = Arc::new(Cache::with_capacity_bytes(64 * 1024 * 1024)); // row cache OFF
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(plain)
    .open()
    .unwrap();
    for i in 0..N {
        tree.insert(key(i), val(i), 0);
    }
    tree.flush_active_memtable(0).unwrap();
    for i in 0..N {
        let got = tree.get(key(i), SeqNo::MAX).unwrap().unwrap();
        assert_eq!(&*got, val(i).as_slice());
    }
}
