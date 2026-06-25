// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Columnar read-path coverage: reverse iteration, bounded ranges, masked
//! (deleted) scans, and point reads (present / absent / deleted). These exercise
//! the entries-backed columnar scan iterator (`next_back`, the bound seeks) and
//! the single-key columnar point-read fast path, which a plain forward full scan
//! does not.

#![cfg(feature = "columnar")]

use lsm_tree::config::{BlockSizePolicy, DeleteStrategy, DeleteStrategyPolicy};
use lsm_tree::{
    AbstractTree, AnyTree, Config, Guard, SeqNo, SequenceNumberCounter, Tree, UserKey,
    get_tmp_folder,
};
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn value(i: u32) -> Vec<u8> {
    format!("value-{i:06}-payload").into_bytes()
}

fn open_columnar(folder: &std::path::Path) -> Tree {
    let any = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar");
    tree
}

/// Reverse iteration over a columnar segment yields the same rows as forward,
/// reversed — exercising the columnar iterator's `next_back`.
#[test]
fn columnar_reverse_scan_matches_forward() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let n = 200u32;
    for i in 0..n {
        tree.insert(key(i), value(i), u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");

    let collect = |rev: bool| -> Vec<(Vec<u8>, Vec<u8>)> {
        let iter = tree.range(key(0)..key(1_000_000), SeqNo::MAX, None);
        let mut v: Vec<(Vec<u8>, Vec<u8>)> = if rev {
            iter.rev()
                .map(|g| {
                    let (k, val) = g.into_inner().expect("guard");
                    (k.to_vec(), val.to_vec())
                })
                .collect()
        } else {
            iter.map(|g| {
                let (k, val) = g.into_inner().expect("guard");
                (k.to_vec(), val.to_vec())
            })
            .collect()
        };
        if rev {
            v.reverse();
        }
        v
    };

    let forward = collect(false);
    let reversed = collect(true);
    assert_eq!(forward.len(), n as usize);
    assert_eq!(forward, reversed, "reverse scan must mirror forward scan");
}

/// Bounded ranges over a columnar segment respect inclusive / exclusive bounds —
/// exercising the columnar iterator's lower / upper bound seeks.
#[test]
fn columnar_bounded_range_respects_bounds() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let n = 200u32;
    for i in 0..n {
        tree.insert(key(i), value(i), u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");

    // Half-open [50, 150): 100 rows.
    let half_open = tree.range(key(50)..key(150), SeqNo::MAX, None).count();
    assert_eq!(half_open, 100, "[50, 150) spans 100 rows");

    // Inclusive [50, 150]: 101 rows.
    let inclusive = tree.range(key(50)..=key(150), SeqNo::MAX, None).count();
    assert_eq!(inclusive, 101, "[50, 150] spans 101 rows");

    // The first and last keys of the half-open range are exactly as expected.
    let mut iter = tree.range(key(50)..key(150), SeqNo::MAX, None);
    let first = iter.next().expect("first").into_inner().expect("guard").0;
    assert_eq!(&*first, key(50).as_slice());
    let last = iter
        .next_back()
        .expect("last")
        .into_inner()
        .expect("guard")
        .0;
    assert_eq!(&*last, key(149).as_slice());
}

/// Point reads on a columnar segment: present key returns its value, absent key
/// returns `None` (the columnar point-read fast path's empty result).
#[test]
fn columnar_point_read_present_and_absent() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let n = 100u32;
    for i in (0..n).step_by(2) {
        // Only even keys exist.
        tree.insert(key(i), value(i), u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");

    for i in 0..n {
        let got = tree.get(key(i), SeqNo::MAX).expect("get");
        if i % 2 == 0 {
            assert_eq!(
                &*got.expect("even key present"),
                value(i).as_slice(),
                "even key {i}",
            );
        } else {
            assert!(got.is_none(), "odd key {i} is absent");
        }
    }
    // A key past the end of the keyspace is absent too.
    assert!(tree.get(key(10_000), SeqNo::MAX).expect("get").is_none());
}

/// A columnar segment carrying a materialized delete-bitmap: deleted rows are
/// hidden on both the masked scan and the masked point-read paths.
#[test]
fn columnar_masked_scan_and_point_read_hide_deletes() {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::MergeOnRead);
    })
    .expect("enable columnar merge-on-read");

    let n = 200u32;
    for i in 0..n {
        tree.insert(key(i), value(i), u64::from(i));
    }
    // Range-delete the first 50 keys above all of them, then materialize into a
    // delete-bitmap via relocation.
    tree.remove_range(
        UserKey::from(&key(0)[..]),
        UserKey::from(&key(50)[..]),
        1000,
    );
    tree.flush_active_memtable(0).expect("flush");
    tree.major_compact(64 * 1024 * 1024, 5000)
        .expect("relocate");

    // Masked scan: deleted rows are skipped, survivors remain.
    let scanned = tree.range(key(0)..key(1_000_000), SeqNo::MAX, None).count();
    assert_eq!(
        scanned,
        (n - 50) as usize,
        "deleted rows are masked from the scan"
    );

    // Masked point reads: deleted keys read as absent, survivors return values.
    for i in 0..n {
        let got = tree.get(key(i), SeqNo::MAX).expect("get");
        if i < 50 {
            assert!(got.is_none(), "deleted key {i} reads as absent");
        } else {
            assert_eq!(
                &*got.expect("live key"),
                value(i).as_slice(),
                "live key {i}"
            );
        }
    }
}

/// A columnar segment whose delete-bitmap masks whole interior blocks: small
/// data blocks make the segment span many blocks, and a large contiguous delete
/// leaves entire interior blocks fully dead. The masked scan reconstructs each
/// fully-dead block to "nothing" and skips it, yielding only the head and tail
/// survivors (not just per-row masking within a single block).
#[test]
fn columnar_masked_scan_skips_a_wholly_deleted_block() {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    // Small data blocks so the segment spans many blocks for a few hundred rows.
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::Adaptive {
            purge_threshold_percent: 90,
        });
    })
    .expect("enable columnar adaptive");

    let n = 300u32;
    for i in 0..n {
        tree.insert(key(i), value(i), u64::from(i));
    }
    // Delete a large contiguous middle range above every row. 60% < 90%, so the
    // relocation keeps the bitmap; the deleted span covers whole interior blocks.
    tree.remove_range(
        UserKey::from(&key(60)[..]),
        UserKey::from(&key(240)[..]),
        10_000,
    );
    tree.flush_active_memtable(0).expect("flush");
    tree.major_compact(64 * 1024 * 1024, 100_000)
        .expect("relocate");

    {
        let version = tree.current_version();
        let tables: Vec<_> = version.iter_tables().collect();
        assert!(
            tables.iter().any(|t| t.delete_density().is_some()),
            "relocation must keep the sub-threshold delete-bitmap",
        );
    }

    let survivors: Vec<u32> = tree
        .range(key(0)..key(1_000_000), SeqNo::MAX, None)
        .map(|g| {
            let (k, _) = g.into_inner().expect("guard");
            let s = std::str::from_utf8(&k).expect("utf8 key");
            s.trim_start_matches('k').parse::<u32>().expect("key index")
        })
        .collect();
    let expected: Vec<u32> = (0..60).chain(240..n).collect();
    assert_eq!(
        survivors, expected,
        "only the head and tail survive; fully-dead interior blocks are skipped",
    );
}
