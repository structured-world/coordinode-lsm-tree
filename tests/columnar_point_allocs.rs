// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a warm point read of a columnar table allocates, per read, beside the
//! same read of a row table: allocation calls and bytes, counted on the
//! reading thread.
//!
//! Every block the reads touch is in the block cache when they are counted
//! and the row cache is off, so the counts are the read path's own work:
//! finding the key's rows in the row group and handing its value out.

use lsm_tree::config::BlockSizePolicy;
use lsm_tree::{AbstractTree, AnyTree, Cache, Config, SeqNo, SequenceNumberCounter};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

thread_local! {
    /// Allocation calls of this thread while it measures. `const`-initialised,
    /// so reading it from the allocator allocates nothing.
    static CALLS: Cell<usize> = const { Cell::new(0) };
    /// Bytes those calls asked for.
    static BYTES: Cell<usize> = const { Cell::new(0) };
    static MEASURING: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

fn count(bytes: usize) {
    let _ = MEASURING.try_with(|measuring| {
        if measuring.get() {
            let _ = CALLS.try_with(|calls| calls.set(calls.get() + 1));
            let _ = BYTES.try_with(|total| total.set(total.get() + bytes));
        }
    });
}

// SAFETY: every method forwards its arguments unchanged to `System`, which
// upholds the contract; the counters are thread-local cells that allocate
// nothing and do not touch the returned pointer.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        // SAFETY: `layout` is the caller's, forwarded unchanged.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` and `layout` are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count(new_size);
        // SAFETY: the arguments are the caller's, forwarded unchanged.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

// The crate's `mimalloc` feature installs its own global allocator, and a
// program links one: built with it, this one counts nothing and the test
// below is left out.
#[cfg_attr(not(feature = "mimalloc"), global_allocator)]
#[cfg_attr(
    feature = "mimalloc",
    expect(dead_code, reason = "not the global allocator")
)]
static ALLOCATOR: Counting = Counting;

/// Allocation calls and bytes of `runs` runs of `run`, per `per` of them.
#[cfg_attr(feature = "mimalloc", expect(dead_code, reason = "counts nothing"))]
fn counted(runs: usize, per: usize, mut run: impl FnMut(usize)) -> (f64, f64) {
    CALLS.with(|calls| calls.set(0));
    BYTES.with(|bytes| bytes.set(0));
    MEASURING.with(|measuring| measuring.set(true));
    for i in 0..runs {
        run(i);
    }
    MEASURING.with(|measuring| measuring.set(false));
    let calls = CALLS.with(Cell::get);
    let bytes = BYTES.with(Cell::get);
    (calls as f64 / per as f64, bytes as f64 / per as f64)
}

const KEYS: usize = 20_000;

/// The key of row `i`, `len` bytes long; only even rows are written, so an
/// odd one is absent and lies between two present keys.
#[cfg_attr(feature = "mimalloc", expect(dead_code, reason = "counts nothing"))]
fn key(i: usize, len: usize) -> Vec<u8> {
    let mut key = format!("{i:010}").into_bytes();
    key.resize(len.max(key.len()), b'k');
    key
}

/// A tree of `KEYS` rows flushed into tables, each key in `versions`
/// versions, read through a cache that holds all of it.
#[cfg_attr(feature = "mimalloc", expect(dead_code, reason = "counts nothing"))]
fn tree(
    columnar: bool,
    key_len: usize,
    value_len: usize,
    versions: u64,
) -> lsm_tree::Result<(AnyTree, tempfile::TempDir)> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(Arc::new(
        Cache::with_capacity_bytes(256 * 1_024 * 1_024).with_row_cache(false),
    ))
    .data_block_size_policy(BlockSizePolicy::all(4 * 1_024))
    .columnar_row_group_size_policy(BlockSizePolicy::all(64 * 1_024))
    .columnar_page_size_policy(BlockSizePolicy::all(4 * 1_024))
    .open()?;
    if columnar {
        let AnyTree::Standard(standard) = &tree else {
            panic!("a standard tree");
        };
        standard.update_runtime_config(|cfg| cfg.columnar = true)?;
    }
    for version in 0..versions {
        for i in (0..KEYS * 2).step_by(2) {
            let mut value = vec![b'v'; value_len];
            // Not one repeated value, which a column would store once.
            value[..8].copy_from_slice(&(i as u64 ^ version).to_le_bytes());
            tree.insert(key(i, key_len), value, version);
        }
    }
    tree.flush_active_memtable(0)?;
    assert!(tree.table_count() > 0, "self-check: the rows are in tables");
    Ok((tree, folder))
}

/// What one layout's warm reads allocate, as `(calls, bytes)` per key.
#[cfg_attr(feature = "mimalloc", expect(dead_code, reason = "counts nothing"))]
struct Warm {
    hit: (f64, f64),
    miss: (f64, f64),
    /// A batch of 16 present keys read in one call, per key.
    batch: (f64, f64),
}

#[cfg_attr(feature = "mimalloc", expect(dead_code, reason = "counts nothing"))]
fn warm_reads(tree: &AnyTree, key_len: usize, value_len: usize) -> lsm_tree::Result<Warm> {
    const READS: usize = 2_000;
    const BATCH: usize = 16;
    // Spread over the table, so the reads touch many row groups.
    let present = |i: usize| key((i * 7919 % KEYS) * 2, key_len);
    let absent = |i: usize| key((i * 7919 % KEYS) * 2 + 1, key_len);
    // The keys are built before the count starts, and one pass warms the
    // cache with every block the counted pass reads.
    let hits: Vec<Vec<u8>> = (0..READS).map(present).collect();
    let misses: Vec<Vec<u8>> = (0..READS).map(absent).collect();
    for key in hits.iter().chain(&misses) {
        let _ = tree.get(key, SeqNo::MAX)?;
    }
    let mut failed = None;
    let hit = counted(READS, READS, |i| match tree.get(&hits[i], SeqNo::MAX) {
        Ok(Some(value)) => assert_eq!(value.len(), value_len),
        Ok(None) => panic!("a present key read absent"),
        Err(e) => failed = Some(e),
    });
    let miss = counted(READS, READS, |i| match tree.get(&misses[i], SeqNo::MAX) {
        Ok(None) => {}
        Ok(Some(_)) => panic!("an absent key read present"),
        Err(e) => failed = Some(e),
    });
    // A batch's keys in ascending order, as a batch read takes them.
    let mut sorted = hits.clone();
    sorted.sort();
    let batches = READS / BATCH;
    let batch = counted(batches, batches * BATCH, |i| {
        match tree.multi_get(&sorted[i * BATCH..(i + 1) * BATCH], SeqNo::MAX) {
            Ok(values) => assert!(
                values
                    .iter()
                    .all(|value| value.as_ref().is_some_and(|v| v.len() == value_len)),
                "a present key read absent or short"
            ),
            Err(e) => failed = Some(e),
        }
    });
    failed.map_or(Ok(Warm { hit, miss, batch }), Err)
}

/// Prints what a warm point read allocates in each layout, row beside
/// columnar, and holds the columnar read to the row read's: a hit hands its
/// value out as a view of the page that holds it, as a row table hands out a
/// view of its block, and builds nothing on the way; a miss the filter
/// answers allocates nothing either. Guards against a columnar point read
/// that copies the rows it found into a block of their own to search it, and
/// a batch read that rebuilds a whole row group for the few keys in it.
#[cfg(not(feature = "mimalloc"))]
#[test]
fn a_warm_columnar_point_read_allocates_nothing() -> lsm_tree::Result<()> {
    println!(
        "key  value  versions  layout    hit calls/bytes  miss calls/bytes  batch calls/bytes per key"
    );
    for (key_len, value_len, versions) in [
        (10, 64, 1),
        (10, 1_024, 1),
        (64, 64, 1),
        (64, 1_024, 1),
        (10, 64, 3),
        (64, 1_024, 3),
    ] {
        for columnar in [false, true] {
            let (tree, _folder) = tree(columnar, key_len, value_len, versions)?;
            let warm = warm_reads(&tree, key_len, value_len)?;
            let layout = if columnar { "columnar" } else { "row" };
            println!(
                "{key_len:<4} {value_len:<6} {versions:<9} {layout:<9} {:.1} / {:<10.0} {:.1} / {:<11.0} {:.1} / {:.0}",
                warm.hit.0, warm.hit.1, warm.miss.0, warm.miss.1, warm.batch.0, warm.batch.1,
            );
            assert_eq!(
                warm.hit,
                (0.0, 0.0),
                "a warm {layout} hit allocated: keys of {key_len}, values of {value_len}, \
                 {versions} versions"
            );
            assert_eq!(warm.miss, (0.0, 0.0), "a warm {layout} miss allocated");
            // A batch allocates its plan and its answers. A row group rebuilt
            // into a block for the keys that fall in it costs tens of
            // kilobytes a key on top (the group's bytes, several times).
            assert!(
                warm.batch.1 < 8_192.0,
                "a warm {layout} batch read allocated {} bytes a key: keys of {key_len}, \
                 values of {value_len}, {versions} versions",
                warm.batch.1,
            );
        }
    }
    Ok(())
}
