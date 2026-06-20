// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Equivalence tests for the seekable range iterator (#495): `seek_to`,
//! `seek_to_for_prev`, and `batch_range_scan` must return exactly what the
//! equivalent plain `range()` calls return.
//!
//! Each test runs against both a standard tree and a KV-separated `BlobTree`
//! (values stored in blob files), so blob-handle resolution on the reposition /
//! batch paths is covered too.

use lsm_tree::{
    AbstractTree, Config, DefaultUserComparator, Guard, InternalValue, KvSeparationOptions,
    Memtable, SeqNo, SequenceNumberCounter, SharedComparator, UserKey, ValueType, get_tmp_folder,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::ops::{Bound, Range};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use test_log::test;

/// Allocation counter for the in-place-reseek micro-bench. Counts `alloc` /
/// `realloc` calls only while [`MEASURING`] is set, so a measured region sees
/// exactly the heap traffic it triggers. nextest runs each test in its own
/// process, so the global counter is isolated per test.
static MEASURED_ALLOCS: AtomicUsize = AtomicUsize::new(0);
static MEASURING: AtomicBool = AtomicBool::new(false);

struct CountingAllocator;

// SAFETY: forwards every call verbatim to the system allocator; the only added
// behaviour is a relaxed counter bump on allocating paths while measuring.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if MEASURING.load(Ordering::Relaxed) {
            MEASURED_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if MEASURING.load(Ordering::Relaxed) {
            MEASURED_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// An explicit `(start, end)` bound pair used to build excluded-bound ranges.
type BoundPair = (Bound<Vec<u8>>, Bound<Vec<u8>>);

fn key(i: u32) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}

fn val(i: u32) -> Vec<u8> {
    format!("v{i:05}-{}", "x".repeat((i % 7) as usize)).into_bytes()
}

/// Build a tree spread across several SSTs plus the active memtable, with
/// overwrites (newer seqno wins) and deletes, so the merge pipeline is
/// non-trivial. With `kv_sep`, values are stored in blob files (threshold 1), so
/// every read resolves a blob handle.
fn build_tree(kv_sep: bool) -> lsm_tree::Result<(lsm_tree::AnyTree, tempfile::TempDir)> {
    let folder = get_tmp_folder();
    let config = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    let config = if kv_sep {
        config.with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    } else {
        config
    };
    let tree = config.open()?;

    // SST 1: keys 0..200 at seqno 0.
    for i in 0..200u32 {
        tree.insert(key(i), val(i), 0);
    }
    tree.flush_active_memtable(0)?;

    // SST 2: overwrite every 3rd key with a newer value at seqno 1, plus new
    // keys 200..260.
    for i in (0..200u32).step_by(3) {
        tree.insert(key(i), val(i + 1_000_000), 1);
    }
    for i in 200..260u32 {
        tree.insert(key(i), val(i), 1);
    }
    tree.flush_active_memtable(1)?;

    // Memtable (unflushed): delete every 7th key, overwrite every 5th, add tail.
    for i in (0..260u32).step_by(7) {
        tree.remove(key(i), 2);
    }
    for i in (0..260u32).step_by(5) {
        tree.insert(key(i), val(i + 2_000_000), 3);
    }
    for i in 260..300u32 {
        tree.insert(key(i), val(i), 3);
    }

    Ok((tree, folder))
}

fn kv(guard: lsm_tree::IterGuardImpl) -> (Vec<u8>, Vec<u8>) {
    let (k, v) = guard.into_inner().expect("guard resolves");
    (k.to_vec(), v.to_vec())
}

/// Reference: concatenation of N independent `range()` calls.
fn reference(tree: &lsm_tree::AnyTree, intervals: &[Range<Vec<u8>>]) -> Vec<(Vec<u8>, Vec<u8>)> {
    intervals
        .iter()
        .flat_map(|iv| tree.range(iv.clone(), SeqNo::MAX, None).map(kv))
        .collect()
}

#[test]
fn batch_range_scan_matches_separate_ranges() -> lsm_tree::Result<()> {
    for kv_sep in [false, true] {
        let (tree, _folder) = build_tree(kv_sep)?;

        // Disjoint, ascending sub-intervals scattered across the keyspace.
        let intervals: Vec<Range<Vec<u8>>> = vec![
            key(3)..key(17),
            key(40)..key(41),  // single-key window
            key(95)..key(140), // spans the SST overwrite + memtable region
            key(205)..key(230),
            key(280)..key(305), // tail, partially past the last key
        ];

        let expected = reference(&tree, &intervals);
        assert!(!expected.is_empty(), "fixture should yield rows");

        let got: Vec<_> = tree
            .batch_range_scan(intervals.clone(), SeqNo::MAX, None)
            .map(kv)
            .collect();

        assert_eq!(
            got, expected,
            "batch scan must equal N separate ranges (kv_sep={kv_sep})"
        );
    }
    Ok(())
}

#[test]
fn batch_range_scan_empty_and_full_intervals() -> lsm_tree::Result<()> {
    for kv_sep in [false, true] {
        let (tree, _folder) = build_tree(kv_sep)?;

        // An empty interval (no keys) plus a normal one.
        let intervals: Vec<Range<Vec<u8>>> = vec![
            key(9000)..key(9100), // empty: past all data
            key(50)..key(60),
        ];
        let expected = reference(&tree, &intervals);
        let got: Vec<_> = tree
            .batch_range_scan(intervals, SeqNo::MAX, None)
            .map(kv)
            .collect();
        assert_eq!(got, expected, "kv_sep={kv_sep}");
    }
    Ok(())
}

#[test]
fn seek_to_matches_range_from_key() -> lsm_tree::Result<()> {
    for kv_sep in [false, true] {
        let (tree, _folder) = build_tree(kv_sep)?;

        for start in [0u32, 1, 7, 42, 150, 199, 200, 259, 260, 299, 500] {
            let mut it = tree.range_seekable::<&[u8], _>(.., SeqNo::MAX, None);
            it.seek_to(&key(start));
            let got: Vec<_> = (&mut it).map(kv).collect();

            let expected: Vec<_> = tree.range(key(start).., SeqNo::MAX, None).map(kv).collect();

            assert_eq!(
                got, expected,
                "seek_to({start}) must equal range(k{start}..) (kv_sep={kv_sep})"
            );
        }
    }
    Ok(())
}

#[test]
fn seek_to_repeated_jumps_stay_consistent() -> lsm_tree::Result<()> {
    for kv_sep in [false, true] {
        let (tree, _folder) = build_tree(kv_sep)?;

        // A single live iterator jumped forward several times must, after each
        // jump, produce the same rows as a fresh range() from that key.
        let mut it = tree.range_seekable::<&[u8], _>(.., SeqNo::MAX, None);
        for start in [10u32, 100, 33, 250, 5] {
            it.seek_to(&key(start));
            let got: Vec<_> = std::iter::from_fn(|| it.next()).take(8).map(kv).collect();
            let expected: Vec<_> = tree
                .range(key(start).., SeqNo::MAX, None)
                .take(8)
                .map(kv)
                .collect();
            assert_eq!(got, expected, "jump to k{start} (kv_sep={kv_sep})");
        }
    }
    Ok(())
}

#[test]
fn seek_to_for_prev_matches_reverse_range() -> lsm_tree::Result<()> {
    for kv_sep in [false, true] {
        let (tree, _folder) = build_tree(kv_sep)?;

        for end in [5u32, 42, 150, 200, 260, 299, 500] {
            let mut it = tree.range_seekable::<&[u8], _>(.., SeqNo::MAX, None);
            it.seek_to_for_prev(&key(end));
            // Pull backward from the repositioned iterator.
            let got: Vec<_> = std::iter::from_fn(|| it.next_back()).map(kv).collect();

            let expected: Vec<_> = tree
                .range(..=key(end), SeqNo::MAX, None)
                .rev()
                .map(kv)
                .collect();

            assert_eq!(
                got, expected,
                "seek_to_for_prev({end}) reverse must equal range(..=k{end}).rev() (kv_sep={kv_sep})"
            );
        }
    }
    Ok(())
}

#[test]
fn seek_outside_declared_window_stays_clamped() -> lsm_tree::Result<()> {
    for kv_sep in [false, true] {
        let (tree, _folder) = build_tree(kv_sep)?;
        // A seekable opened over a bounded window [100, 200) only collected its
        // Phase-1 sources for that window. Seeking outside the window must clamp
        // to it (bounded-iterator semantics), not leak rows below/above the
        // window or drop rows from sources skipped during collection.
        let in_window: Vec<_> = tree
            .range(key(100)..key(200), SeqNo::MAX, None)
            .map(kv)
            .collect();

        // Forward seek below the lower bound → clamps to the window start.
        let mut it = tree.range_seekable(key(100)..key(200), SeqNo::MAX, None);
        it.seek_to(&key(50));
        let got: Vec<_> = (&mut it).map(kv).collect();
        assert_eq!(
            got, in_window,
            "seek_to below the window must clamp to it (kv_sep={kv_sep})"
        );

        // Reverse seek above the upper bound → clamps to the window end.
        let mut it = tree.range_seekable(key(100)..key(200), SeqNo::MAX, None);
        it.seek_to_for_prev(&key(250));
        let mut got: Vec<_> = std::iter::from_fn(|| it.next_back()).map(kv).collect();
        got.reverse();
        assert_eq!(
            got, in_window,
            "seek_to_for_prev above the window must clamp to it (kv_sep={kv_sep})"
        );

        // Same with an excluded-lower / included-upper window, so the other two
        // clamp arms (Excluded lower, Included upper) are exercised too.
        let excl_window = (Bound::Excluded(key(100)), Bound::Included(key(200)));
        let in_excl: Vec<_> = tree
            .range(excl_window.clone(), SeqNo::MAX, None)
            .map(kv)
            .collect();

        let mut it = tree.range_seekable(excl_window.clone(), SeqNo::MAX, None);
        it.seek_to(&key(80));
        let got: Vec<_> = (&mut it).map(kv).collect();
        assert_eq!(
            got, in_excl,
            "seek_to below an excluded-lower window must clamp (kv_sep={kv_sep})"
        );

        let mut it = tree.range_seekable(excl_window, SeqNo::MAX, None);
        it.seek_to_for_prev(&key(250));
        let mut got: Vec<_> = std::iter::from_fn(|| it.next_back()).map(kv).collect();
        got.reverse();
        assert_eq!(
            got, in_excl,
            "seek_to_for_prev above an included-upper window must clamp (kv_sep={kv_sep})"
        );
    }
    Ok(())
}

#[test]
fn peek_key_matches_next_key() -> lsm_tree::Result<()> {
    for kv_sep in [false, true] {
        let (tree, _folder) = build_tree(kv_sep)?;

        // Walking the whole range: peek_key must equal the key the next consuming
        // step yields, at every position, and be None exactly at the end.
        let mut it = tree.range_seekable::<&[u8], _>(.., SeqNo::MAX, None);
        loop {
            let peeked = it.peek_key().transpose()?.map(|k| k.to_vec());
            match it.next() {
                Some(guard) => {
                    let (k, _) = guard.into_inner()?;
                    assert_eq!(
                        peeked,
                        Some(k.to_vec()),
                        "peek_key must match next (kv_sep={kv_sep})"
                    );
                }
                None => {
                    assert!(peeked.is_none(), "peek at end is None (kv_sep={kv_sep})");
                    break;
                }
            }
        }

        // A seek must drop a stale lookahead: peek after seek reflects the new
        // position, not the buffered pre-seek key.
        let mut it = tree.range_seekable::<&[u8], _>(.., SeqNo::MAX, None);
        let _ = it.peek_key(); // prime the buffer at the start
        it.seek_to(&key(150));
        let peeked = it.peek_key().transpose()?.map(|k| k.to_vec());
        let expected = tree
            .range(key(150).., SeqNo::MAX, None)
            .next()
            .map(|g| kv(g).0);
        assert_eq!(peeked, expected, "peek after seek (kv_sep={kv_sep})");
    }
    Ok(())
}

#[test]
fn seekable_with_range_tombstone() -> lsm_tree::Result<()> {
    for kv_sep in [false, true] {
        let (tree, _folder) = build_tree(kv_sep)?;
        // A dropped sub-range plants a range tombstone, so the merge pipeline
        // wraps in the RangeTombstoneFilter branch.
        tree.drop_range(key(50)..key(150))?;

        let intervals: Vec<Range<Vec<u8>>> = vec![key(30)..key(80), key(120)..key(200)];
        let expected = reference(&tree, &intervals);
        let got: Vec<_> = tree
            .batch_range_scan(intervals, SeqNo::MAX, None)
            .map(kv)
            .collect();
        assert_eq!(got, expected, "RT batch (kv_sep={kv_sep})");

        let mut it = tree.range_seekable::<&[u8], _>(.., SeqNo::MAX, None);
        it.seek_to(&key(40));
        let got: Vec<_> = (&mut it).map(kv).collect();
        let exp: Vec<_> = tree.range(key(40).., SeqNo::MAX, None).map(kv).collect();
        assert_eq!(got, exp, "RT seek_to (kv_sep={kv_sep})");
    }
    Ok(())
}

#[test]
fn seekable_with_ephemeral_memtable() -> lsm_tree::Result<()> {
    for kv_sep in [false, true] {
        let (tree, _folder) = build_tree(kv_sep)?;

        // An ephemeral memtable (passed via the `index` parameter) with
        // overriding values and a range tombstone — exercises the ephemeral
        // source branches.
        let comparator: SharedComparator = Arc::new(DefaultUserComparator);
        let mt = Arc::new(Memtable::new(123, comparator));
        for i in (0..300u32).step_by(11) {
            mt.insert(InternalValue::from_components(
                key(i),
                val(i + 9_000_000),
                10,
                ValueType::Value,
            ));
        }
        assert!(
            mt.insert_range_tombstone(
                UserKey::from(key(70).as_slice()),
                UserKey::from(key(90).as_slice()),
                10,
            ) > 0,
            "ephemeral range tombstone rejected",
        );
        let eph = Some((mt, 11u64));

        let intervals: Vec<Range<Vec<u8>>> = vec![key(0)..key(60), key(85)..key(160)];
        let expected: Vec<(Vec<u8>, Vec<u8>)> = intervals
            .iter()
            .flat_map(|iv| tree.range(iv.clone(), SeqNo::MAX, eph.clone()).map(kv))
            .collect();
        let got: Vec<_> = tree
            .batch_range_scan(intervals, SeqNo::MAX, eph.clone())
            .map(kv)
            .collect();
        assert_eq!(got, expected, "ephemeral batch (kv_sep={kv_sep})");

        let mut it = tree.range_seekable::<&[u8], _>(.., SeqNo::MAX, eph.clone());
        it.seek_to(&key(50));
        let got: Vec<_> = (&mut it).map(kv).collect();
        let exp: Vec<_> = tree.range(key(50).., SeqNo::MAX, eph).map(kv).collect();
        assert_eq!(got, exp, "ephemeral seek_to (kv_sep={kv_sep})");
    }
    Ok(())
}

#[test]
fn seekable_over_multi_sst_run() -> lsm_tree::Result<()> {
    for kv_sep in [false, true] {
        let folder = get_tmp_folder();
        let config = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        );
        let config = if kv_sep {
            config.with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        } else {
            config
        };
        let tree = config.open()?;

        // Enough data that a tiny-target compaction splits the merged bottom
        // level into many SSTs forming one multi-SST run, exercising the
        // multi-run RunReader collection / build path.
        for i in 0..4000u32 {
            tree.insert(key(i), val(i % 200), 0);
        }
        tree.flush_active_memtable(0)?;
        tree.major_compact(4096, 1)?;

        let intervals: Vec<Range<Vec<u8>>> = vec![
            key(10)..key(900),
            key(1500)..key(1600),
            key(3000)..key(3990),
        ];
        let expected = reference(&tree, &intervals);
        let got: Vec<_> = tree
            .batch_range_scan(intervals, SeqNo::MAX, None)
            .map(kv)
            .collect();
        assert_eq!(got, expected, "multi-run batch (kv_sep={kv_sep})");

        let mut it = tree.range_seekable::<&[u8], _>(.., SeqNo::MAX, None);
        it.seek_to(&key(2500));
        let got: Vec<_> = (&mut it).map(kv).collect();
        let exp: Vec<_> = tree.range(key(2500).., SeqNo::MAX, None).map(kv).collect();
        assert_eq!(got, exp, "multi-run seek_to (kv_sep={kv_sep})");
    }
    Ok(())
}

#[test]
fn seekable_sealed_memtable_and_excluded_bounds() -> lsm_tree::Result<()> {
    for kv_sep in [false, true] {
        let (tree, _folder) = build_tree(kv_sep)?;
        // Seal a memtable without flushing it (rotate), then keep writing to the
        // new active one — exercises the sealed-memtable source branches.
        for i in 300..340u32 {
            tree.insert(key(i), val(i), 4);
        }
        tree.rotate_memtable();
        for i in 340..360u32 {
            tree.insert(key(i), val(i), 5);
        }

        // Excluded lower bounds exercise the Excluded arm of the bound builders.
        let intervals: Vec<BoundPair> = vec![
            (Bound::Excluded(key(20)), Bound::Excluded(key(90))),
            (Bound::Excluded(key(305)), Bound::Included(key(355))),
        ];
        let expected: Vec<(Vec<u8>, Vec<u8>)> = intervals
            .iter()
            .flat_map(|iv| tree.range(iv.clone(), SeqNo::MAX, None).map(kv))
            .collect();
        let got: Vec<_> = tree
            .batch_range_scan(intervals, SeqNo::MAX, None)
            .map(kv)
            .collect();
        assert_eq!(got, expected, "sealed + excluded batch (kv_sep={kv_sep})");

        // Seekable opened over an excluded-lower union.
        let mut it = tree.range_seekable(
            (Bound::Excluded(key(310)), Bound::Unbounded),
            SeqNo::MAX,
            None,
        );
        it.seek_to(&key(320));
        let got: Vec<_> = (&mut it).map(kv).collect();
        let exp: Vec<_> = tree.range(key(320).., SeqNo::MAX, None).map(kv).collect();
        assert_eq!(got, exp, "sealed + excluded seek_to (kv_sep={kv_sep})");
    }
    Ok(())
}

/// #504 acceptance: a tight `seek_to` loop must not reallocate the merge stack.
///
/// Before the in-place reseek, every reposition rebuilt the whole Phase-2
/// pipeline (loser-tree merger + MVCC stream + tombstone filter, one boxed
/// reader per source) — O(sources) allocations per seek. Now repositioning
/// reuses the entire stack and moves only the leaf cursors, so a tight seek
/// loop allocates nothing after the one-time warm-up build.
#[test]
fn tight_seek_loop_does_not_reallocate_merge_stack() -> lsm_tree::Result<()> {
    // Standard tree spread across several SSTs (incl. a multi-table run) plus
    // the active memtable, so the reposition exercises every leaf flavour.
    let (tree, _guard) = build_tree(false)?;
    let mut it = tree.range_seekable::<&[u8], _>(.., SeqNo::MAX, None);

    // Warm up: the first seek + pull builds the merge stack once (the one-time
    // allocation the acceptance criterion explicitly exempts).
    it.seek_to(&key(5));
    let _ = it.next();

    // Disjoint targets spread across the key space, so each reposition genuinely
    // moves every leaf cursor (SST index re-seek / run-window recompute /
    // skiplist seek_ge) rather than no-op'ing on an unchanged bound.
    let targets: Vec<Vec<u8>> = (0..300u32).map(|i| key((i * 137) % 260)).collect();

    MEASURED_ALLOCS.store(0, Ordering::SeqCst);
    MEASURING.store(true, Ordering::SeqCst);
    for target in &targets {
        it.seek_to(target);
    }
    MEASURING.store(false, Ordering::SeqCst);

    let allocs = MEASURED_ALLOCS.load(Ordering::SeqCst);
    assert_eq!(
        allocs,
        0,
        "{} tight seeks allocated {allocs} times; the merge stack must be reused in place",
        targets.len(),
    );
    Ok(())
}
