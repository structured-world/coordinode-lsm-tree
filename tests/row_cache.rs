// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

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

/// A caller who builds a cache without saying anything about rows gets the row
/// cache. This pins the default itself: every tree that does not construct its
/// own `Cache` inherits it, so flipping it silently would change the memory and
/// latency profile of every deployment that never touched the setting.
#[test]
fn row_cache_when_not_configured_is_enabled() {
    let cache = Cache::with_capacity_bytes(1024);
    assert!(cache.row_cache_enabled());
}

/// The opt-out still works. A workload measured to be one of the exceptions,
/// scan-heavy or without key reuse, has to be able to turn the row cache off,
/// and the builder must not be a no-op now that its argument agrees with the
/// default in one direction.
#[test]
fn row_cache_when_switched_off_stays_off() {
    assert!(
        !Cache::with_capacity_bytes(1024)
            .with_row_cache(false)
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

/// A read at a snapshot below a table's newest versions is answered from a
/// cached row like any other read. The row gate samples one key in 256 and
/// sends a sampled read to its block so its simulation sees it, but a
/// snapshot read is not played to the simulation: sampled, it walked to the
/// block on every read and told the gate nothing.
#[cfg(feature = "metrics")]
#[test]
fn a_snapshot_read_of_a_cached_row_walks_to_no_block() {
    let (_dir, tree, _cache) = tree_with_row_cache();
    for i in 0..N {
        tree.insert(key(i), val(i), i);
    }
    tree.flush_active_memtable(0).unwrap();
    for i in 0..N {
        assert!(tree.get(key(i), SeqNo::MAX).unwrap().is_some());
    }

    // The snapshot sees every key but the last written.
    let before = tree.metrics().data_block_load_count();
    for i in 0..N - 1 {
        let got = tree.get(key(i), N - 1).unwrap().unwrap();
        assert_eq!(&*got, val(i).as_slice());
    }
    assert_eq!(tree.metrics().data_block_load_count() - before, 0);
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
    // Explicitly off: the default is on, so a bare cache would not exercise the
    // comparison this test exists to make.
    let plain = Arc::new(Cache::with_capacity_bytes(64 * 1024 * 1024).with_row_cache(false));
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

/// A cache told not to adapt sends every key through the row cache whatever
/// share a tree holds: the switch for a workload measured to do better with
/// rows always on has to override the tree's own choice, not sit beside it.
#[test]
fn a_cache_told_not_to_adapt_fills_rows_for_every_key() {
    let folder = get_tmp_folder();
    let dir = tempfile::TempDir::new_in(folder).unwrap();
    let cache =
        Arc::new(Cache::with_capacity_bytes(64 * 1024 * 1024).with_adaptive_row_cache(false));
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(cache.clone())
    .open()
    .unwrap();
    cache.set_row_share(tree.id(), 0);
    for i in 0..N {
        tree.insert(key(i), val(i), 0);
    }
    tree.flush_active_memtable(0).unwrap();
    for _ in 0..2 {
        for i in 0..N {
            assert!(tree.get(key(i), SeqNo::MAX).unwrap().is_some());
        }
    }
    assert_eq!(
        cache.size(),
        cached_after_two_passes(true, Some(8)),
        "as a tree with every key inside its gate",
    );
}

/// A hot set read beside many cold keys is answered from its rows: nine reads
/// in ten go to 1,200 keys whose rows are a fifth of the cache, the tenth to
/// any of 40,000, whose blocks stream through the cache between two reads of
/// a hot key. A row is kept from its second read on, or that stream evicts
/// it before its key comes back and the hot reads walk to their blocks.
#[cfg(feature = "metrics")]
#[test]
fn a_hot_set_among_cold_keys_is_read_from_its_rows() {
    let folder = get_tmp_folder();
    let dir = tempfile::TempDir::new_in(folder).unwrap();
    let cache =
        Arc::new(Cache::with_capacity_bytes(2 * 1024 * 1024).with_adaptive_row_cache(false));
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(cache)
    .open()
    .unwrap();
    let (keys, hot) = (40_000u64, 1_200u64);
    let spread = |i: u64| (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20).to_be_bytes();
    let value = vec![7u8; 256];
    for i in 0..keys {
        tree.insert(spread(i), &value, 0);
    }
    tree.flush_active_memtable(0).unwrap();

    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let reads = 100_000usize;
    let mut walked = 0;
    for pass in 0..3 {
        let before = tree.metrics().data_block_load_count();
        for _ in 0..reads {
            let any = next() % keys;
            let at = if next() % 10 == 0 { any } else { any % hot };
            assert!(tree.get(spread(at), SeqNo::MAX).unwrap().is_some());
        }
        if pass == 2 {
            walked = tree.metrics().data_block_load_count() - before;
        }
    }
    assert!(
        walked * 4 < reads,
        "{walked} of {reads} reads walked to a block",
    );
}

/// One order of evenly drawn keys read over and over brings each key back
/// after as many reads as the tree has keys, more than the cache remembers
/// of the keys it refused: a row is refused on every read and never held.
/// The tree's model has to forget as soon as the cache does, or it counts
/// those rows as held and keeps them: such a tree read 3769 ns a key with
/// its rows against 2998 without. It gives them up.
#[test]
fn a_tree_replaying_evenly_drawn_keys_past_its_cache_gives_up_its_rows() {
    let folder = get_tmp_folder();
    let dir = tempfile::TempDir::new_in(folder).unwrap();
    let cache = Arc::new(Cache::with_capacity_bytes(16 * 1024 * 1024));
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(cache.clone())
    .open()
    .unwrap();
    let keys = 70_000u64;
    let spread = |i: u64| (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20).to_be_bytes();
    let value = vec![7u8; 256];
    for i in 0..keys {
        tree.insert(spread(i), &value, 0);
    }
    tree.flush_active_memtable(0).unwrap();

    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let order: Vec<u64> = (0..keys)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % keys
        })
        .collect();
    let mut shares = Vec::new();
    for _ in 0..16 {
        for &at in &order {
            assert!(tree.get(spread(at), SeqNo::MAX).unwrap().is_some());
        }
        shares.push(cache.row_share(tree.id()));
    }
    assert!(
        shares.iter().skip(8).all(|&share| share == 0),
        "closed by the ninth pass and stays closed: {shares:?}",
    );
}

/// A tree read key by key, round after round, through a cache that holds
/// most but not all of it gives its rows up: a row comes back only after
/// every other key was read, by when the cache has dropped it. Left on, the
/// rows cost such a tree a probe, a refusal or a fill on every read and
/// answer almost none.
#[test]
fn a_tree_read_in_rounds_past_its_cache_gives_up_its_rows() {
    let folder = get_tmp_folder();
    let dir = tempfile::TempDir::new_in(folder).unwrap();
    let cache = Arc::new(Cache::with_capacity_bytes(16 * 1024 * 1024));
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(cache.clone())
    .open()
    .unwrap();
    let keys = 70_000u64;
    // Scattered over the key space, so a round reads the blocks out of order.
    let spread = |i: u64| (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20).to_be_bytes();
    let value = vec![7u8; 256];
    for i in 0..keys {
        tree.insert(spread(i), &value, 0);
    }
    tree.flush_active_memtable(0).unwrap();

    let mut shares = Vec::new();
    for _ in 0..12 {
        for i in 0..keys {
            assert!(tree.get(spread(i), SeqNo::MAX).unwrap().is_some());
        }
        shares.push(cache.row_share(tree.id()));
    }
    assert_eq!(
        shares.last(),
        Some(&0),
        "shares after each round: {shares:?}"
    );
    assert!(
        shares.iter().skip(5).all(|&share| share == 0),
        "closed by the sixth round and stays closed: {shares:?}",
    );
}

/// What a cache holds after every key of a flushed tree was read twice
/// through it, the tree's row share set to `share` eighths when given.
fn cached_after_two_passes(rows: bool, share: Option<u8>) -> u64 {
    let folder = get_tmp_folder();
    let dir = tempfile::TempDir::new_in(folder).unwrap();
    let cache = Arc::new(Cache::with_capacity_bytes(64 * 1024 * 1024).with_row_cache(rows));
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(cache.clone())
    .open()
    .unwrap();
    if let Some(share) = share {
        cache.set_row_share(tree.id(), share);
    }
    for i in 0..N {
        tree.insert(key(i), val(i), 0);
    }
    tree.flush_active_memtable(0).unwrap();
    for _ in 0..2 {
        for i in 0..N {
            let got = tree.get(key(i), SeqNo::MAX).unwrap().unwrap();
            assert_eq!(&*got, val(i).as_slice());
        }
    }
    cache.size()
}

/// A tree whose row share is none reads as one with no row cache: its point
/// reads leave the blocks they walked in the cache and not one row. A tree
/// with every key inside leaves the rows too, and a tree with half of them
/// about half as many. A share that still filled rows would spend the copy,
/// the insert and the cache's bytes that closing the gate is there to save.
#[test]
fn a_trees_row_share_decides_which_reads_fill_rows() {
    let off = cached_after_two_passes(false, None);
    let none = cached_after_two_passes(true, Some(0));
    let half = cached_after_two_passes(true, Some(4));
    let all = cached_after_two_passes(true, Some(8));

    assert_eq!(none, off, "a closed gate fills no row");
    assert!(all > off, "an open gate fills rows: {all} against {off}");
    let (rows_half, rows_all) = (half - off, all - off);
    assert!(
        rows_half * 10 > rows_all * 3 && rows_half * 10 < rows_all * 7,
        "half the keys fill about half the rows: {rows_half} of {rows_all}",
    );
}

/// A tree reads through the row cache until its own reads say otherwise, and
/// trees sharing one cache each hold a share of their own.
#[test]
fn trees_sharing_a_cache_hold_their_own_row_shares() {
    let (_dir_a, a, cache) = tree_with_row_cache();
    let folder = get_tmp_folder();
    let dir_b = tempfile::TempDir::new_in(folder).unwrap();
    let b = Config::new(
        dir_b.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(cache.clone())
    .open()
    .unwrap();

    assert_eq!(cache.row_share(a.id()), 8, "every key, by default");
    assert_eq!(cache.row_share(b.id()), 8);
    cache.set_row_share(a.id(), 0);
    assert_eq!(cache.row_share(a.id()), 0);
    assert_eq!(cache.row_share(b.id()), 8, "the other tree keeps its own");
}
