// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A compaction into a level with a level below it ends its outputs on the
//! boundaries between the tables below: run as one stream, split into key
//! ranges run side by side, and around a row group it would copy whole.

use crate::{
    AbstractTree, AnyTree, Config, SequenceNumberCounter, Table, TableId,
    compaction::{Choice, CompactionStrategy, Input, state::CompactionState},
    version::Version,
};
use std::sync::Arc;
use test_log::test;

/// The target a flush cuts its tables at.
const FLUSH_TARGET: u64 = 40 << 10;

/// The target the merge under test cuts its outputs at.
const OUTPUT_TARGET: u64 = 64 << 10;

/// Moves exactly the named tables to a level.
struct Place(Vec<TableId>, u8);

impl CompactionStrategy for Place {
    fn get_name(&self) -> &'static str {
        "PlaceTest"
    }

    fn choose(&self, _: &Version, _: &Config, _: &CompactionState) -> Choice {
        Choice::Move(Input {
            table_ids: self.0.iter().copied().collect(),
            dest_level: self.1,
            canonical_level: self.1,
            target_size: u64::MAX,
        })
    }
}

/// Merges exactly the named tables into a level, at [`OUTPUT_TARGET`].
struct Merge(Vec<TableId>, u8);

impl CompactionStrategy for Merge {
    fn get_name(&self) -> &'static str {
        "MergeTest"
    }

    fn choose(&self, _: &Version, _: &Config, _: &CompactionState) -> Choice {
        Choice::Merge(Input {
            table_ids: self.0.iter().copied().collect(),
            dest_level: self.1,
            canonical_level: self.1,
            target_size: OUTPUT_TARGET,
        })
    }
}

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn index(key: &[u8]) -> u32 {
    std::str::from_utf8(key.get(1..).unwrap_or_default())
        .ok()
        .and_then(|digits| digits.parse().ok())
        .unwrap_or(u32::MAX)
}

fn open(config: Config) -> crate::Result<crate::Tree> {
    let AnyTree::Standard(tree) = config.table_target_size(FLUSH_TARGET).open()? else {
        panic!("expected a standard tree");
    };
    Ok(tree)
}

/// The tables of `level`, in key order.
fn tables(tree: &crate::Tree, level: usize) -> Vec<Table> {
    let version = tree.current_version();
    let mut tables: Vec<Table> = version
        .level(level)
        .into_iter()
        .flat_map(|level| level.iter())
        .flat_map(|run| run.iter())
        .cloned()
        .collect();
    tables.sort_by(|a, b| a.metadata.key_range.min().cmp(b.metadata.key_range.min()));
    tables
}

/// The index of the largest key of each of `tables`.
fn last_keys(tables: &[Table]) -> Vec<u32> {
    tables
        .iter()
        .map(|table| index(table.metadata.key_range.max()))
        .collect()
}

/// The boundaries between `tables` of one run: the last key of each but the
/// last.
fn boundaries_of(tables: &[Table]) -> Vec<u32> {
    let mut keys = last_keys(tables);
    keys.pop();
    keys
}

/// Writes keys `keys` with `value_len`-byte values, flushes them and moves the
/// flushed run to `level`.
fn flush_into(
    tree: &crate::Tree,
    keys: core::ops::Range<u32>,
    value_len: usize,
    level: u8,
    seqno: &mut u64,
) -> crate::Result<()> {
    for i in keys {
        tree.insert(key(i), vec![b'v'; value_len], *seqno);
        *seqno += 1;
    }
    tree.flush_active_memtable(0)?;
    let ids = tables(tree, 0).iter().map(Table::id).collect();
    tree.compact(Arc::new(Place(ids, level)), 0)?;
    Ok(())
}

/// Level 3 holds keys `0..3000` in tables cut at another place than the
/// inputs', level 2 a part of the same keys and level 1 all of them; level 1
/// and level 2 are then merged into level 2. Returns the last key of each
/// output, the boundaries of level 3 and the largest key of each table level 2
/// held before the merge, where a split compaction's key ranges end.
fn merge_above_a_level(config: Config) -> crate::Result<(Vec<u32>, Vec<u32>, Vec<u32>)> {
    let tree = open(config)?;
    let mut seqno = 1;
    flush_into(&tree, 0..3_000, 700, 3, &mut seqno)?;
    flush_into(&tree, 1_000..2_000, 1_000, 2, &mut seqno)?;
    flush_into(&tree, 0..3_000, 1_000, 1, &mut seqno)?;

    let boundaries = boundaries_of(&tables(&tree, 3));
    let seams = last_keys(&tables(&tree, 2));
    let ids = tables(&tree, 1)
        .iter()
        .chain(tables(&tree, 2).iter())
        .map(Table::id)
        .collect();
    tree.compact(Arc::new(Merge(ids, 2)), 0)?;

    for i in 0..3_000 {
        assert!(tree.get(key(i), crate::SeqNo::MAX)?.is_some(), "key {i}");
    }
    Ok((last_keys(&tables(&tree, 2)), boundaries, seams))
}

/// A merge run as one stream ends every output but its last on a boundary of
/// the level below.
#[test]
fn a_serial_merge_ends_its_outputs_on_the_level_below() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let config = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .compaction_threads(1);
    let (outputs, boundaries, _) = merge_above_a_level(config)?;
    assert!(outputs.len() > 3, "{} outputs", outputs.len());
    let (_, cut) = outputs.split_last().unwrap();
    for last in cut {
        assert!(boundaries.contains(last), "an output ends at {last}");
    }
    Ok(())
}

/// A merge split into key ranges ends every output on a boundary of the level
/// below, as the stream does, or where its range ends.
#[test]
fn a_split_merge_ends_its_outputs_on_the_level_below_or_its_range() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let config = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .compaction_threads(4)
    .subcompaction_min_bytes(0);
    let (outputs, boundaries, seams) = merge_above_a_level(config)?;
    let (_, cut) = outputs.split_last().unwrap();
    let mut at_boundaries = 0;
    for last in cut {
        assert!(
            boundaries.contains(last) || seams.contains(last),
            "an output ends at {last}, on neither a boundary nor a range's end"
        );
        at_boundaries += usize::from(boundaries.contains(last));
    }
    assert!(at_boundaries > 0, "the ranges' outputs end on boundaries");
    Ok(())
}

/// With alignment off the same merge cuts at the target alone: outputs end
/// where the size says, which is not where the level below divides.
#[test]
fn without_alignment_outputs_end_off_the_level_below() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let config = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .compaction_threads(1)
    .compaction_output_alignment(false);
    let (outputs, boundaries, _) = merge_above_a_level(config)?;
    let (_, cut) = outputs.split_last().unwrap();
    assert!(
        cut.iter().any(|last| !boundaries.contains(last)),
        "a cut at the target alone lands off the level below"
    );
    Ok(())
}

/// A columnar table merged whole into a level above another: its row groups
/// are copied, but one holding a boundary of the level below between its keys
/// is written key by key once the output may end there, since its bytes cost
/// less than the table the output would otherwise straddle. With alignment off
/// every group is copied.
#[cfg(all(feature = "columnar", feature = "metrics"))]
#[test]
fn a_group_holding_a_boundary_is_written_so_the_output_ends_on_it() -> crate::Result<()> {
    use crate::config::{BlockSizePolicy, CompressionPolicy};

    let run = |aligned: bool| -> crate::Result<(u64, u64, Vec<u32>, Vec<u32>)> {
        let folder = tempfile::tempdir()?;
        let tree = open(
            Config::new(
                folder.path(),
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .columnar_row_group_size_policy(BlockSizePolicy::all(4 * 1_024))
            .compaction_threads(1)
            .compaction_output_alignment(aligned),
        )?;
        tree.update_runtime_config(|cfg| {
            cfg.columnar = true;
            cfg.zone_map = true;
            cfg.data_block_compression_policy =
                CompressionPolicy::all(crate::CompressionType::None);
        })?;
        let mut seqno = 1;
        flush_into(&tree, 0..3_000, 700, 3, &mut seqno)?;
        flush_into(&tree, 0..3_000, 1_000, 1, &mut seqno)?;
        let groups_in: u64 = tables(&tree, 1)
            .iter()
            .map(|table| table.metadata.data_block_count)
            .sum();
        let boundaries = boundaries_of(&tables(&tree, 3));
        let ids = tables(&tree, 1).iter().map(Table::id).collect();
        tree.compact(Arc::new(Merge(ids, 2)), 0)?;
        for i in 0..3_000 {
            assert!(tree.get(key(i), crate::SeqNo::MAX)?.is_some(), "key {i}");
        }
        Ok((
            groups_in,
            tree.metrics().compaction_groups_carried(),
            last_keys(&tables(&tree, 2)),
            boundaries,
        ))
    };

    let (groups_in, carried, _, _) = run(false)?;
    assert_eq!(
        carried, groups_in,
        "without alignment every group is copied"
    );

    let (groups_in, carried, outputs, boundaries) = run(true)?;
    assert!(carried > 0, "the groups holding no boundary are copied");
    assert!(
        carried < groups_in,
        "{carried} of {groups_in} groups copied, those an output ends inside included"
    );
    let (_, cut) = outputs.split_last().unwrap();
    for last in cut {
        assert!(boundaries.contains(last), "an output ends at {last}");
    }
    Ok(())
}
