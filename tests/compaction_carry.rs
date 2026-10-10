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

/// A key rewritten at the seqno it already had leaves two tables holding one
/// internal key; the first merge emits both into one group, and the next must
/// write that group's rows rather than copy a group whose order a copy
/// refuses, and keep every key readable.
#[test]
fn compaction_tied_key_group_is_written_not_copied() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let mut seqno = 1;
    flush_keys(&tree, 0..100, 0, &mut seqno);
    tree.insert(key(0), value(0, 1), 1);
    tree.flush_active_memtable(0).expect("flush");

    tree.major_compact(64 * 1024 * 1024, 0)
        .expect("first compaction");
    tree.major_compact(64 * 1024 * 1024, 0)
        .expect("second compaction");

    for i in 1..100 {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
            Some(value(i, 0).as_slice()),
            "key {i}",
        );
    }
    assert!(tree.get(key(0), SeqNo::MAX).expect("get").is_some());
    assert_blocks_verify(&tree);
}

/// The same tie in a group the next merge changes elsewhere: the group is
/// rebuilt around its unchanged row pages only when its rows keep the order a
/// block requires, so here its rows are written, and every key stays readable.
#[test]
fn compaction_tied_key_group_changed_elsewhere_is_written() {
    // The tree's default policies, a compaction out of level 0 encoding its
    // rows again, and short keys and values of one width.
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
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");
    let short_key = |i: u32| format!("k{i:04}").into_bytes();
    let short = |i: u32| vec![i as u8; 18];
    let mut seqno = 1;
    for i in 365..469 {
        tree.insert(short_key(i), short(i), seqno);
        seqno += 1;
    }
    tree.flush_active_memtable(0).expect("flush");
    tree.insert(short_key(365), vec![0u8; 16], 1);
    tree.flush_active_memtable(0).expect("flush");
    tree.major_compact(64 * 1024 * 1024, 0)
        .expect("first compaction");

    tree.remove(short_key(366), seqno);
    tree.flush_active_memtable(0).expect("flush");
    tree.major_compact(64 * 1024 * 1024, 0)
        .expect("second compaction");

    assert_eq!(tree.get(short_key(366), SeqNo::MAX).expect("get"), None);
    for i in 367..469 {
        assert_eq!(
            tree.get(short_key(i), SeqNo::MAX).expect("get").as_deref(),
            Some(short(i).as_slice()),
            "key {i}",
        );
    }
    assert!(tree.get(short_key(365), SeqNo::MAX).expect("get").is_some());
    assert_blocks_verify(&tree);
}

/// Groups ingested with their value split into sub-columns, copied one after
/// another into one output: a run of groups of the same layout stays in one
/// table, as the size target allows, rather than one table a group.
#[test]
fn compaction_carried_split_groups_share_a_table() -> lsm_tree::Result<()> {
    use lsm_tree::table::columnar::{Column, TypeTag, entries_to_column_batch};
    use lsm_tree::{InternalValue, ValueType};

    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .columnar_row_group_size_policy(lsm_tree::config::BlockSizePolicy::all(4 * 1024))
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.data_block_compression_policy =
            lsm_tree::config::CompressionPolicy::all(lsm_tree::CompressionType::None);
    })?;
    // Batches of disjoint keys whose value is a fixed-4 sub-column, each far
    // past a row group's size, so the ingestion writes several groups.
    let batches = 5u32;
    let rows = 2_000u32;
    let mut ingestion = any.ingestion()?;
    for b in 0..batches {
        let entries: Vec<InternalValue> = (0..rows)
            .map(|r| InternalValue::from_components(key(b * rows + r), b"x", 0, ValueType::Value))
            .collect();
        let mut batch = entries_to_column_batch(&entries)?;
        batch.columns.pop();
        batch.columns.push(Column {
            column_id: 3,
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data: (0..rows)
                .flat_map(|r| (b * rows + r).to_le_bytes())
                .collect::<Vec<u8>>()
                .into(),
        });
        ingestion.write_columnar_batch(&batch)?;
    }
    ingestion.finish()?;
    assert!(
        tree.current_version()
            .iter_tables()
            .all(|t| t.global_seqno() == 0),
        "the first ingestion's groups can be copied"
    );
    let groups_in = row_groups(tree);
    let tables_in = tree.current_version().iter_tables().count();
    assert!(
        groups_in > 1,
        "the ingestion wrote several groups: {groups_in} in {tables_in} tables"
    );

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert_eq!(
        tree.metrics().compaction_groups_carried(),
        groups_in,
        "{tables_in} input tables"
    );
    assert_eq!(
        tree.current_version().iter_tables().count(),
        1,
        "the copied groups share one table"
    );
    assert_eq!(
        tree.iter(SeqNo::MAX, None).count(),
        (batches * rows) as usize
    );
    assert_blocks_verify(tree);
    Ok(())
}

/// A level whose pages are cut to another size than the output level's: its
/// groups would keep the source geometry, so they are encoded again, cut as
/// the output level cuts them.
#[test]
fn compaction_into_another_page_size_carries_nothing() {
    use lsm_tree::config::BlockSizePolicy;

    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .columnar_page_size_policy(BlockSizePolicy::new([
        4 * 1024,
        4 * 1024,
        4 * 1024,
        4 * 1024,
        4 * 1024,
        4 * 1024,
        16 * 1024,
    ]))
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
    flush_keys(&tree, 2_000..4_000, 0, &mut seqno);

    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    assert_eq!(tree.metrics().compaction_groups_carried(), 0);
    assert_eq!(tree.iter(SeqNo::MAX, None).count(), 4_000);
    assert_blocks_verify(&tree);
}

/// Tables written before the page size policy changed keep the pages they
/// were cut into, though every level's policy now agrees with the output's:
/// the first merge after the change encodes their groups again, cut as the
/// policy now cuts them, and the merge after it copies them.
#[test]
fn compaction_after_a_page_size_change_rewrites_older_groups_then_copies_them() {
    use lsm_tree::config::BlockSizePolicy;

    let folder = get_tmp_folder();
    let mut seqno = 1;
    {
        let tree = open_columnar(folder.path());
        flush_keys(&tree, 0..2_000, 0, &mut seqno);
        flush_keys(&tree, 2_000..4_000, 0, &mut seqno);
    }

    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .columnar_page_size_policy(BlockSizePolicy::all(1_024))
    .open()
    .expect("reopen");
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

    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");
    assert_eq!(tree.metrics().compaction_groups_carried(), 0);
    assert_eq!(tree.iter(SeqNo::MAX, None).count(), 4_000);

    // The rewritten groups now have the shape the policy writes, as do the
    // ones a flush adds beside them: the next merge copies all of them.
    flush_keys(&tree, 4_000..6_000, 0, &mut seqno);
    let groups_in = row_groups(&tree);
    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");
    assert_eq!(tree.metrics().compaction_groups_carried(), groups_in);
    for i in 0..6_000 {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
            Some(value(i, 0).as_slice()),
            "key {i}",
        );
    }
    assert_blocks_verify(&tree);
}

/// A compaction run in slices on a disk too small for a full rewrite installs
/// each slice itself: the groups a slice copies are counted as a merge's are.
#[test]
fn tight_space_slices_count_the_groups_they_copy() -> lsm_tree::Result<()> {
    use lsm_tree::fs::MemFs;
    use std::sync::Arc;

    let folder = get_tmp_folder();
    let mem = MemFs::with_capacity(u64::MAX);
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem.clone()))
    .open()?;
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.data_block_compression_policy =
            lsm_tree::config::CompressionPolicy::all(lsm_tree::CompressionType::None);
    })?;
    let mut seqno = 1;
    flush_keys(&tree, 0..4_000, 0, &mut seqno);
    let used = tree.storage_stats()?.used_bytes;
    let tables_before = tree.table_count();
    // Too little room for the rewrite next to its input: the merge runs in
    // slices that punch the input as they go.
    mem.set_capacity(used + used / 4);
    tree.update_runtime_config(|cfg| {
        cfg.storage_admission_check = true;
        cfg.storage_limit_bytes = None;
        cfg.tight_space_compaction = true;
    })?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert!(
        tree.table_count() > tables_before,
        "the merge ran in slices"
    );
    assert!(
        tree.metrics().compaction_groups_carried() > 0,
        "the slices' copied groups are counted"
    );
    for i in 0..4_000 {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX)?.as_deref(),
            Some(value(i, 0).as_slice()),
            "key {i}",
        );
    }
    Ok(())
}

/// A tree keeping no zone map has no statistics to carry along, and needs
/// none: its groups are copied as when it keeps one.
#[test]
fn compaction_without_zone_map_carries_every_row_group() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    tree.update_runtime_config(|cfg| cfg.zone_map = false)
        .expect("drop the zone map");
    let mut seqno = 1;
    flush_keys(&tree, 0..2_000, 0, &mut seqno);
    flush_keys(&tree, 2_000..4_000, 0, &mut seqno);
    let groups_in = row_groups(&tree);

    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    assert_eq!(tree.metrics().compaction_groups_carried(), groups_in);
    for i in 0..4_000 {
        assert_eq!(
            tree.get(key(i), SeqNo::MAX).expect("get").as_deref(),
            Some(value(i, 0).as_slice()),
            "key {i}",
        );
    }
    assert_blocks_verify(&tree);
}

/// A table written without a zone map, merged into a tree that now keeps one:
/// its groups have no statistics for the output's zone map, so they are
/// encoded again, while the groups of a table written with one are copied.
#[test]
fn compaction_into_zone_map_rewrites_groups_written_without_one() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    tree.update_runtime_config(|cfg| cfg.zone_map = false)
        .expect("drop the zone map");
    let mut seqno = 1;
    flush_keys(&tree, 0..2_000, 0, &mut seqno);
    let without = row_groups(&tree);
    tree.update_runtime_config(|cfg| cfg.zone_map = true)
        .expect("keep a zone map");
    flush_keys(&tree, 2_000..4_000, 0, &mut seqno);
    let with = row_groups(&tree) - without;

    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    assert_eq!(tree.metrics().compaction_groups_carried(), with);
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

/// Many new keys land among the rows of the base table's second group, whose
/// range comes out as several groups encoded here: they take tags of the
/// output's own, so every later group of the base keeps a free tag and is
/// copied.
#[test]
fn compaction_groups_encoded_after_a_copy_leave_later_tags_free() {
    let folder = get_tmp_folder();
    let tree = open_columnar(folder.path());
    let mut seqno = 1;
    flush_keys(&tree, 0..2_000, 0, &mut seqno);
    let base_groups = row_groups(&tree);
    assert!(
        base_groups > 4,
        "the base spans several groups: {base_groups}"
    );
    // A key in the middle of the second group, past one group to copy.
    let at = u32::try_from(2_000 / base_groups * 3 / 2).expect("small");
    // Keys sorting right after it, enough for more than two groups of rows
    // of their own, written row-major so only the base's groups can be
    // copied.
    tree.update_runtime_config(|cfg| cfg.columnar = false)
        .expect("row-major");
    for j in 0..800u32 {
        let mut k = key(at);
        k.extend_from_slice(format!("-{j:04}").as_bytes());
        tree.insert(k, value(j, 9), seqno);
        seqno += 1;
    }
    tree.flush_active_memtable(0).expect("flush");
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("columnar");

    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    assert_eq!(
        tree.metrics().compaction_groups_carried(),
        base_groups - 1,
        "every base group but the one the new keys land in is copied",
    );
    assert_eq!(tree.iter(SeqNo::MAX, None).count(), 2_800);
    assert_blocks_verify(&tree);
}

/// A key the parallel ranges are cut at, written in many versions that span
/// several row groups of an input: the range ending at the key reads every
/// group holding a version of it, so no version is lost at the cut.
#[test]
fn compaction_parallel_range_keeps_every_version_of_its_end_key() {
    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    // More threads than bottom tables: every table's last key but the
    // highest cuts a range, the one picked below among them.
    .compaction_threads(32)
    .subcompaction_min_bytes(0)
    .columnar_row_group_size_policy(lsm_tree::config::BlockSizePolicy::all(4 * 1024))
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
    tree.major_compact(32 * 1024, SeqNo::MAX).expect("compact");
    let bottom = tree.current_version().iter_tables().count();
    assert!(
        (3..32).contains(&bottom),
        "fewer bottom tables than threads: {bottom}"
    );
    // The last key of the first bottom table is where the next compaction's
    // first range ends.
    let cut = tree
        .current_version()
        .iter_tables()
        .map(|t| t.metadata.key_range.max().clone())
        .min()
        .expect("tables");

    let first_version = seqno;
    for round in 0..120 {
        tree.insert(cut.clone(), value(round, round), seqno);
        seqno += 1;
    }
    tree.flush_active_memtable(0).expect("flush");
    assert!(
        tree.current_version()
            .iter_tables()
            .any(|t| t.metadata.key_range.min() == &cut && t.metadata.data_block_count > 2),
        "the versions span several groups",
    );
    tree.major_compact(32 * 1024, 0).expect("compact");

    for round in 0..120u32 {
        let at = first_version + u64::from(round);
        assert_eq!(
            tree.get(&cut, at + 1).expect("get").as_deref(),
            Some(value(round, round).as_slice()),
            "version {round} of the cut key",
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
    // Each blob file is linked under the keys that hold its objects: the
    // first and the last cell row, not the last row of each copied group.
    let links: Vec<_> = tree
        .index
        .current_version()
        .iter_tables()
        .map(|t| t.list_blob_file_references())
        .collect::<lsm_tree::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .flatten()
        .collect();
    let first = links
        .iter()
        .map(|l| l.first_key.clone())
        .min()
        .expect("links");
    let last = links
        .iter()
        .map(|l| l.last_key.clone())
        .max()
        .expect("links");
    assert_eq!(&*first, &key(0)[..], "first holder of a body");
    assert_eq!(&*last, &key(docs - 1)[..], "last holder of a body");

    for i in 0..docs {
        tree.remove(key(i), u64::from(docs + 1 + i));
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    tree.major_compact(64_000_000, SeqNo::MAX)?;
    assert_eq!(tree.blob_file_count(), 0, "the bodies were let go once");
    Ok(())
}

/// Newer versions of cell rows that borrow their bodies from the versions
/// they replace: the merge drops the owners and copies the borrowers' groups
/// whole, and a copied borrower takes no ownership, so no body is charged as
/// garbage and every body still reads through the row that borrows it.
#[test]
fn compaction_carried_borrowing_cell_rows_charge_nothing() -> lsm_tree::Result<()> {
    use lsm_tree::KvSeparationOptions;
    use lsm_tree::blob_tree::field_row::{Cell, FIRST_FIELD_COLUMN, Field};

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
    for i in 0..docs {
        let row = tree.get_cells(key(i), SeqNo::MAX)?.expect("written");
        let borrowed = row
            .fields()?
            .into_iter()
            .find(|field| field.column == FIRST_FIELD_COLUMN + 1)
            .expect("the body");
        assert!(
            matches!(borrowed.cell, Cell::Ref(_)),
            "the body is a reference"
        );
        tree.insert_cells(
            key(i),
            &[Field::bytes(FIRST_FIELD_COLUMN, b"final"), borrowed],
            u64::from(docs + i),
        )?;
    }
    tree.flush_active_memtable(0)?;

    tree.major_compact(64_000_000, SeqNo::MAX)?;

    assert!(
        tree.index.metrics().compaction_groups_carried() > 0,
        "the borrowers' groups are copied"
    );
    assert_eq!(tree.stale_blob_bytes(), 0, "a borrowed body is not charged");
    assert_eq!(tree.blob_file_count(), 1, "the bodies' file stays");
    for i in 0..docs {
        let row = tree.get_cells(key(i), SeqNo::MAX)?.expect("written");
        assert_eq!(
            row.resolve(FIRST_FIELD_COLUMN + 1)?.as_deref(),
            Some(&body(i)[..])
        );
    }
    Ok(())
}

/// Values kept whole in blob files: a group of their pointers copied whole
/// still links the blob files under the keys that point into them, every
/// value reads back, and none of their bytes is counted as stale.
#[test]
fn compaction_carried_blob_pointers_keep_their_links() -> lsm_tree::Result<()> {
    use lsm_tree::KvSeparationOptions;

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
        tree.insert(key(i), body(i), u64::from(i));
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
        assert_eq!(
            tree.get(key(i), SeqNo::MAX)?.as_deref(),
            Some(&body(i)[..]),
            "key {i}"
        );
    }
    assert_eq!(tree.stale_blob_bytes(), 0, "every value is still held");
    let links: Vec<_> = tree
        .index
        .current_version()
        .iter_tables()
        .map(|t| t.list_blob_file_references())
        .collect::<lsm_tree::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .flatten()
        .collect();
    let first = links
        .iter()
        .map(|l| l.first_key.clone())
        .min()
        .expect("links");
    let last = links
        .iter()
        .map(|l| l.last_key.clone())
        .max()
        .expect("links");
    assert_eq!(&*first, &key(0)[..], "first pointer into a blob file");
    assert_eq!(&*last, &key(docs - 1)[..], "last pointer into a blob file");
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

/// A filter that removes keys right after a copied group: the removals belong
/// to the output that takes the next written row, as they do without copying,
/// so of the outputs exactly those whose key window holds a removed key are
/// marked transformed.
#[test]
fn compaction_filter_removals_after_a_copied_group_mark_the_next_output() {
    use lsm_tree::compaction::filter::{
        CompactionFilter, Context as FilterContext, Factory, ItemAccessor, Verdict,
    };
    use std::sync::Arc;

    const REMOVED: core::ops::Range<u32> = 100..110;
    struct Remove;
    impl CompactionFilter for Remove {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _ctx: &FilterContext,
        ) -> lsm_tree::Result<Verdict> {
            let removed = REMOVED
                .into_iter()
                .any(|i| &item.key()[..] == key(i).as_slice());
            Ok(if removed {
                Verdict::Remove
            } else {
                Verdict::Keep
            })
        }
    }
    struct RemoveFactory;
    impl Factory for RemoveFactory {
        fn name(&self) -> &str {
            "remove"
        }
        fn make_filter(&self, _ctx: &FilterContext) -> Box<dyn CompactionFilter> {
            Box::new(Remove)
        }
    }

    let folder = get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_compaction_filter_factory(Some(Arc::new(RemoveFactory)))
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
    // One group of keys copied whole, then keys the filter removes, written
    // row-major so they are no group to copy, then more copied groups.
    flush_keys(&tree, 0..20, 0, &mut seqno);
    tree.update_runtime_config(|cfg| cfg.columnar = false)
        .expect("row-major");
    flush_keys(&tree, REMOVED, 0, &mut seqno);
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("columnar");
    flush_keys(&tree, 200..2_200, 0, &mut seqno);

    // A target of one byte: every output closes at its first chance.
    tree.major_compact(1, 0).expect("compact");
    assert!(tree.metrics().compaction_groups_carried() > 0);

    let mut outputs: Vec<(Vec<u8>, bool)> = tree
        .current_version()
        .iter_tables()
        .map(|t| {
            (
                t.metadata.key_range.max().to_vec(),
                t.metadata.lineage_transformed,
            )
        })
        .collect();
    outputs.sort();
    let mut previous_last: Option<Vec<u8>> = None;
    for (i, (last, transformed)) in outputs.iter().enumerate() {
        let is_final = i + 1 == outputs.len();
        let holds_removed = REMOVED.into_iter().any(|r| {
            let k = key(r);
            previous_last.as_ref().is_none_or(|p| k > *p) && (is_final || k <= *last)
        });
        assert_eq!(
            *transformed,
            holds_removed,
            "output {i} ending at {:?}",
            String::from_utf8_lossy(last),
        );
        previous_last = Some(last.clone());
    }
    for i in REMOVED {
        assert_eq!(tree.get(key(i), SeqNo::MAX).expect("get"), None);
    }
    assert_blocks_verify(&tree);
}

/// Inputs compressed with another codec than the output's: their groups
/// cannot be copied, so none is read a second time to be offered for a copy.
/// The compaction reads each group once, through its scan.
#[cfg(feature = "lz4")]
#[test]
fn compaction_into_another_codec_reads_no_group_twice() {
    use lsm_tree::config::CompressionPolicy;
    use lsm_tree::fs::{FaultFs, StdFs};

    let folder = get_tmp_folder();
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fs)
    .open()
    .expect("open");
    let AnyTree::Standard(tree) = any else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.data_block_compression_policy = CompressionPolicy::new([
            lsm_tree::CompressionType::None,
            lsm_tree::CompressionType::Lz4,
        ]);
    })
    .expect("enable columnar");
    let mut seqno = 1;
    flush_keys(&tree, 0..2_000, 0, &mut seqno);
    flush_keys(&tree, 2_000..4_000, 0, &mut seqno);
    let groups = row_groups(&tree);
    assert!(groups > 16, "the inputs span many groups: {groups}");

    injector.clear();
    tree.major_compact(64 * 1024 * 1024, 0).expect("compact");

    assert_eq!(tree.metrics().compaction_groups_carried(), 0);
    let reads = injector.read_count() as u64;
    assert!(
        reads < groups,
        "{reads} positioned reads for {groups} groups read by their scans"
    );
    assert_eq!(tree.iter(SeqNo::MAX, None).count(), 4_000);
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
