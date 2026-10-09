// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A compaction that copies row groups whole leaves the tree reading exactly
//! as one that encodes every row.
//!
//! Random writes (runs of keys, point deletes, range deletes) are flushed into
//! overlapping columnar tables and compacted under watermarks that keep every
//! version, drop some or zero the seqnos, so the merge sometimes leaves a
//! group as it was and sometimes rewrites, drops or interleaves its rows. The
//! zone map is switched on and off between writes, so some tables have no
//! statistics for the groups a destination keeping one would copy.
//! After every compaction each key reads as a `BTreeMap` oracle of the writes
//! says, a full scan returns exactly the live keys, and every block of every
//! table verifies at the place it lies.

#![cfg(all(feature = "columnar", feature = "metrics"))]

use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, UserKey};
use proptest::prelude::*;
use std::collections::BTreeMap;

const KEY_SPACE: u16 = 800;

fn key(i: u16) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

#[derive(Debug, Clone)]
enum Op {
    /// `len` consecutive keys from `start`, each at its own seqno.
    Run {
        start: u16,
        len: u16,
        fill: u8,
    },
    /// A point delete.
    Remove {
        at: u16,
    },
    /// A range delete of `[lo, hi)`.
    RemoveRange {
        lo: u16,
        hi: u16,
    },
    Flush,
    /// A major compaction under a watermark that keeps every version (`0`),
    /// drops what is shadowed and zeroes the rest (`MAX`), or sits in between.
    Compact {
        watermark: Watermark,
    },
    /// Whether the tables written from here on keep a zone map, so a merge
    /// mixes groups that carry their statistics with groups that have none.
    ZoneMap(bool),
}

#[derive(Debug, Clone, Copy)]
enum Watermark {
    Zero,
    Half,
    Max,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (0..KEY_SPACE, 1..300u16, any::<u8>())
            .prop_map(|(start, len, fill)| Op::Run { start, len, fill }),
        2 => (0..KEY_SPACE).prop_map(|at| Op::Remove { at }),
        1 => (0..KEY_SPACE, 0..KEY_SPACE).prop_map(|(a, b)| Op::RemoveRange {
            lo: a.min(b),
            hi: a.max(b),
        }),
        3 => Just(Op::Flush),
        2 => prop_oneof![
            Just(Watermark::Zero),
            Just(Watermark::Half),
            Just(Watermark::Max)
        ]
        .prop_map(|watermark| Op::Compact { watermark }),
        1 => any::<bool>().prop_map(Op::ZoneMap),
    ]
}

fn value(i: u16, fill: u8) -> Vec<u8> {
    let mut v = format!("value-{i}-").into_bytes();
    v.extend(std::iter::repeat_n(fill, 40));
    v
}

fn open(folder: &std::path::Path) -> lsm_tree::Tree {
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
        cfg.data_block_compression_policy =
            lsm_tree::config::CompressionPolicy::all(lsm_tree::CompressionType::None);
    })
    .expect("enable columnar");
    tree
}

fn check(tree: &lsm_tree::Tree, oracle: &BTreeMap<u16, Vec<u8>>) {
    for i in 0..KEY_SPACE {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
            oracle.get(&i).map(Vec::as_slice),
            "key {i}",
        );
    }
    let live: Vec<UserKey> = tree
        .iter(SeqNo::MAX, None)
        .map(|guard| lsm_tree::Guard::key(guard).expect("key"))
        .collect();
    let expected: Vec<UserKey> = oracle.keys().map(|&i| UserKey::from(&key(i)[..])).collect();
    assert_eq!(live, expected, "the scan returns the live keys");
    let report = lsm_tree::verify::verify_block_checksums(tree);
    assert!(report.is_ok(), "block verification failed: {report:?}");
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 48,
        ..ProptestConfig::default()
    })]

    #[test]
    fn carried_compaction_reads_as_the_writes(ops in proptest::collection::vec(op(), 1..40)) {
        let folder = lsm_tree::get_tmp_folder();
        let tree = open(folder.path());
        let mut oracle = BTreeMap::new();
        let mut seqno: SeqNo = 1;
        for op in ops {
            match op {
                Op::Run { start, len, fill } => {
                    // Both below 1_100, far inside a u16.
                    for i in start..(start + len).min(KEY_SPACE) {
                        tree.insert(key(i), value(i, fill), seqno);
                        seqno += 1;
                        oracle.insert(i, value(i, fill));
                    }
                }
                Op::Remove { at } => {
                    tree.remove(key(at), seqno);
                    seqno += 1;
                    oracle.remove(&at);
                }
                Op::RemoveRange { lo, hi } => {
                    tree.remove_range(UserKey::from(&key(lo)[..]), UserKey::from(&key(hi)[..]), seqno);
                    seqno += 1;
                    oracle.retain(|&i, _| !(lo..hi).contains(&i));
                }
                Op::Flush => {
                    tree.flush_active_memtable(0).expect("flush");
                }
                Op::Compact { watermark } => {
                    tree.flush_active_memtable(0).expect("flush");
                    let watermark = match watermark {
                        Watermark::Zero => 0,
                        Watermark::Half => seqno / 2,
                        Watermark::Max => SeqNo::MAX,
                    };
                    tree.major_compact(16 * 1024, watermark).expect("compact");
                    check(&tree, &oracle);
                }
                Op::ZoneMap(on) => {
                    tree.update_runtime_config(|cfg| cfg.zone_map = on).expect("zone map");
                }
            }
        }
        tree.flush_active_memtable(0).expect("flush");
        tree.major_compact(16 * 1024, SeqNo::MAX).expect("compact");
        check(&tree, &oracle);
    }
}
