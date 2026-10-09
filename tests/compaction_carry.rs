// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A compaction copies an input's columnar row group into its output whole
//! when the merge leaves every row of it as it was, and writes the rows
//! otherwise. Each test reads the output back against what was written and
//! verifies every block of it, so a copy that lost a row, kept a stale one or
//! landed with the wrong binding fails here rather than only in the counter.

#![cfg(all(feature = "columnar", feature = "metrics"))]

use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn value(i: u32, round: u32) -> Vec<u8> {
    format!("value-{i}-round-{round}-{}", "x".repeat(40)).into_bytes()
}

fn open_columnar(folder: &std::path::Path) -> lsm_tree::Tree {
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
    // One codec at every level: a flush and the compaction below it write
    // pages of the same transform, which is what a copy needs. The default
    // writes level 0 plain and the levels below compressed, so a merge out of
    // level 0 always encodes its rows again.
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.data_block_compression_policy =
            lsm_tree::config::CompressionPolicy::all(lsm_tree::CompressionType::None);
    })
    .expect("enable columnar");
    tree
}

/// Writes keys `range` at round `round`, seqnos from `seqno` up, and flushes
/// them into one table.
fn flush_keys(tree: &lsm_tree::Tree, range: core::ops::Range<u32>, round: u32, seqno: &mut u64) {
    for i in range {
        tree.insert(key(i), value(i, round), *seqno);
        *seqno += 1;
    }
    tree.flush_active_memtable(0).expect("flush");
}

/// Every block of every table verifies at the place it lies.
fn assert_blocks_verify(tree: &lsm_tree::Tree) {
    let report = lsm_tree::verify::verify_block_checksums(tree);
    assert!(report.is_ok(), "block verification failed: {report:?}");
}

/// The row groups the tables of `tree` hold.
fn row_groups(tree: &lsm_tree::Tree) -> u64 {
    tree.current_version()
        .iter_tables()
        .map(|t| t.metadata.data_block_count)
        .sum()
}

/// Two tables of disjoint keys: no row of one lands among the other's, and a
/// watermark of zero leaves every version as it is, so every group of both is
/// copied, the two tables' tags included.
#[test]
fn compaction_disjoint_columnar_tables_carries_every_row_group() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let mut seqno = 1;
    flush_keys(&tree, 0..2_000, 0, &mut seqno);
    flush_keys(&tree, 2_000..4_000, 0, &mut seqno);
    let groups_in = row_groups(&tree);
    assert!(groups_in > 4, "the inputs span several groups: {groups_in}");

    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    let metrics = tree.metrics();
    assert_eq!(metrics.compaction_groups_carried(), groups_in);
    assert!(metrics.compaction_bytes_carried() > 0);
    for i in 0..4_000 {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
            Some(value(i, 0).as_slice()),
            "key {i}",
        );
    }
    assert_eq!(tree.iter(SeqNo::MAX, None).count(), 4_000);
    assert_blocks_verify(&tree);
}

/// Newer versions of a few keys in a second table land among the first
/// table's rows: the groups they land in are written row by row, every other
/// group is copied, and both versions of each key stay readable.
#[test]
fn compaction_interleaved_versions_rewrites_only_their_groups() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let mut seqno = 1;
    flush_keys(&tree, 0..2_000, 0, &mut seqno);
    let base_seqno = seqno;
    flush_keys(&tree, 1_000..1_010, 1, &mut seqno);
    let base_groups = tree
        .current_version()
        .iter_tables()
        .map(|t| t.metadata.data_block_count)
        .max()
        .expect("tables");

    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    let carried = tree.metrics().compaction_groups_carried();
    assert!(carried > 0, "the groups no update lands in are copied");
    assert!(
        carried < base_groups,
        "the group holding the updated keys is rewritten: {carried} of {base_groups}",
    );
    for i in 0..2_000 {
        let round = u32::from((1_000..1_010).contains(&i));
        assert_eq!(
            tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
            Some(value(i, round).as_slice()),
            "latest of key {i}",
        );
        assert_eq!(
            tree.get(key(i), base_seqno).expect("get").as_deref(),
            Some(value(i, 0).as_slice()),
            "key {i} before the update",
        );
    }
    assert_blocks_verify(&tree);
}

/// At the bottom level, a merge with the watermark above every seqno writes
/// them as zero, which changes every row: nothing is copied, and the output
/// reads the same. A later merge of that output, whose rows are already at
/// zero, copies them.
#[test]
fn compaction_zeroed_seqnos_carries_only_once_rows_stop_changing() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let mut seqno = 1;
    flush_keys(&tree, 0..2_000, 0, &mut seqno);

    tree.major_compact(64 * 1024 * 1024, SeqNo::MAX)
        .expect("compact");
    assert_eq!(tree.metrics().compaction_groups_carried(), 0);

    let zeroed_groups = row_groups(&tree);
    flush_keys(&tree, 2_000..2_100, 0, &mut seqno);
    tree.major_compact(64 * 1024 * 1024, SeqNo::MAX)
        .expect("compact");
    assert_eq!(tree.metrics().compaction_groups_carried(), zeroed_groups);
    for i in 0..2_100 {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
            Some(value(i, 0).as_slice()),
            "key {i}",
        );
    }
    assert_blocks_verify(&tree);
}

/// A key whose older version sits in another table continues past the end of
/// the group holding its newer one: copying that group would split the key's
/// versions across its edge, so its rows are written, and both versions read.
#[test]
fn compaction_key_continued_by_another_input_is_not_split() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    // The older version first, alone in its table.
    tree.insert(key(1_999), value(1_999, 0), 1);
    tree.flush_active_memtable(0).expect("flush");
    let mut seqno = 10;
    flush_keys(&tree, 0..2_000, 1, &mut seqno);

    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    assert_eq!(
        tree.get(key(1_999), SeqNo::MAX).expect("get").as_deref(),
        Some(value(1_999, 1).as_slice()),
    );
    assert_eq!(
        tree.get(key(1_999), 2).expect("get").as_deref(),
        Some(value(1_999, 0).as_slice()),
    );
    for i in 0..1_999 {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
            Some(value(i, 1).as_slice()),
            "key {i}",
        );
    }
    assert_blocks_verify(&tree);
}

/// An output of another data codec cannot take the input's pages as they are:
/// nothing is copied and the rows are encoded under the output's codec.
#[test]
#[cfg(feature = "lz4")]
fn compaction_other_output_codec_carries_nothing() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let mut seqno = 1;
    flush_keys(&tree, 0..2_000, 0, &mut seqno);
    tree.update_runtime_config(|cfg| {
        cfg.data_block_compression_policy =
            lsm_tree::config::CompressionPolicy::all(lsm_tree::CompressionType::Lz4);
    })
    .expect("change codec");

    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    assert_eq!(tree.metrics().compaction_groups_carried(), 0);
    for i in 0..2_000 {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
            Some(value(i, 0).as_slice()),
            "key {i}",
        );
    }
    assert_blocks_verify(&tree);
}
