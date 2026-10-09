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

/// One key updated in a second table lands in one row page of one group:
/// that group is rebuilt around the pages of its other row pages, copied as
/// they lie, every other group is copied whole, and both versions of the key
/// read back.
#[test]
fn compaction_one_updated_row_copies_the_rest_of_its_group() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let mut seqno = 1;
    flush_keys(&tree, 0..2_000, 0, &mut seqno);
    let groups = row_groups(&tree);
    let before = seqno;
    flush_keys(&tree, 1_000..1_001, 1, &mut seqno);

    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    let metrics = tree.metrics();
    assert_eq!(metrics.compaction_groups_partly_carried(), 1);
    assert_eq!(metrics.compaction_groups_carried(), groups - 1);
    for i in 0..2_000 {
        let round = u32::from(i == 1_000);
        assert_eq!(
            tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
            Some(value(i, round).as_slice()),
            "latest of key {i}",
        );
    }
    assert_eq!(
        tree.get(key(1_000), before).expect("get").as_deref(),
        Some(value(1_000, 0).as_slice()),
    );
    assert_eq!(tree.iter(SeqNo::MAX, None).count(), 2_000);
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

/// A compaction split into parallel ranges copies the groups each range holds
/// whole, as the serial merge does: the ranges are cut at the tables of the
/// level written into, so a group of those tables lies in one range.
#[test]
fn compaction_parallel_ranges_carry_their_row_groups() {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .compaction_threads(4)
    .subcompaction_min_bytes(0)
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
    let mut seqno = 1;
    flush_keys(&tree, 0..4_000, 0, &mut seqno);
    // Small outputs, so the bottom level holds several tables to cut the next
    // compaction's ranges at, and zeroed seqnos, so their rows stop changing.
    tree.major_compact(32 * 1024, SeqNo::MAX).expect("compact");
    assert!(
        tree.current_version().iter_tables().count() > 2,
        "the bottom level holds several tables",
    );
    let zeroed_groups = row_groups(&tree);

    flush_keys(&tree, 4_000..4_100, 0, &mut seqno);
    tree.major_compact(32 * 1024, SeqNo::MAX).expect("compact");

    assert_eq!(tree.metrics().compaction_groups_carried(), zeroed_groups);
    for i in 0..4_100 {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
            Some(value(i, 0).as_slice()),
            "key {i}",
        );
    }
    assert_eq!(tree.iter(SeqNo::MAX, None).count(), 4_100);
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

/// Cell rows of a blob tree whose bodies live in blob files: a group of them
/// copied whole still records the objects its rows own in the output, so the
/// bodies are charged when their last holder goes and the files are dropped.
#[test]
fn compaction_carried_cell_rows_keep_owning_their_objects() -> lsm_tree::Result<()> {
    use lsm_tree::KvSeparationOptions;
    use lsm_tree::blob_tree::field_row::{FIRST_FIELD_COLUMN, Field};

    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(64),
    ))
    .blob_compression(lsm_tree::CompressionType::None)
    .open()?;
    let AnyTree::Blob(tree) = any else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    tree.index.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.data_block_compression_policy =
            lsm_tree::config::CompressionPolicy::all(lsm_tree::CompressionType::None);
    })?;
    let body = |i: u32| format!("body-{i}-{}", "b".repeat(200)).into_bytes();
    let docs = 300u32;
    for i in 0..docs {
        tree.insert_cells(
            key(i),
            &[
                Field::bytes(FIRST_FIELD_COLUMN, b"draft"),
                Field::bytes(FIRST_FIELD_COLUMN + 1, &body(i)),
            ],
            u64::from(i),
        )?;
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;

    let groups = tree
        .index
        .current_version()
        .iter_tables()
        .map(|t| t.metadata.data_block_count)
        .sum::<u64>();
    tree.insert(key(docs), "plain", u64::from(docs));
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.index.metrics().compaction_groups_carried(), groups);
    for i in 0..docs {
        let row = tree.get_cells(key(i), SeqNo::MAX)?.expect("written");
        assert_eq!(
            row.resolve(FIRST_FIELD_COLUMN + 1)?.as_deref(),
            Some(&body(i)[..])
        );
    }
    assert_eq!(tree.stale_blob_bytes(), 0, "every body is still held");

    for i in 0..docs {
        tree.remove(key(i), u64::from(docs + 1 + i));
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.blob_file_count(), 0, "the bodies were let go once");
    Ok(())
}

/// A user compaction filter runs on every row as it does without carrying:
/// the rows it replaces come out with their new values, which the groups
/// holding them are rewritten for, and every other group is copied.
#[test]
fn compaction_filter_replacing_values_rewrites_only_their_groups() {
    use lsm_tree::compaction::filter::{
        CompactionFilter, Context as FilterContext, Factory, ItemAccessor, Verdict,
    };
    use std::sync::Arc;

    struct Replace;
    impl CompactionFilter for Replace {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _ctx: &FilterContext,
        ) -> lsm_tree::Result<Verdict> {
            let replaced = (1_000..1_010).any(|i| &item.key()[..] == key(i).as_slice());
            Ok(if replaced {
                Verdict::ReplaceValue(b"replaced".to_vec().into())
            } else {
                Verdict::Keep
            })
        }
    }
    struct ReplaceFactory;
    impl Factory for ReplaceFactory {
        fn name(&self) -> &str {
            "replace"
        }
        fn make_filter(&self, _ctx: &FilterContext) -> Box<dyn CompactionFilter> {
            Box::new(Replace)
        }
    }

    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_compaction_filter_factory(Some(Arc::new(ReplaceFactory)))
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
    let mut seqno = 1;
    flush_keys(&tree, 0..2_000, 0, &mut seqno);
    let groups = row_groups(&tree);

    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    let carried = tree.metrics().compaction_groups_carried();
    assert!(carried > 0 && carried < groups, "{carried} of {groups}");
    for i in 0..2_000 {
        let expected = if (1_000..1_010).contains(&i) {
            b"replaced".to_vec()
        } else {
            value(i, 0)
        };
        assert_eq!(
            tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
            Some(expected.as_slice()),
            "key {i}",
        );
    }
    assert_blocks_verify(&tree);
}

/// An encrypted table's blocks are bound to its id: nothing is copied into
/// another table, and every block of the output verifies as its own.
#[test]
#[cfg(feature = "encryption")]
fn compaction_encrypted_tree_carries_nothing() {
    use std::sync::Arc;

    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_encryption(Some(Arc::new(lsm_tree::Aes256GcmProvider::new(
        &[0x42; 32],
    ))))
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
    let mut seqno = 1;
    flush_keys(&tree, 0..1_000, 0, &mut seqno);
    flush_keys(&tree, 1_000..2_000, 0, &mut seqno);

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
