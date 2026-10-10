// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A compaction into a level with a level below it ends its outputs on the
//! boundaries between the tables below: run as one stream, split into key
//! ranges run side by side, in the slices of a tight-space pass, and around a
//! row group it would copy whole.

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

/// A `len`-byte value that does not compress, drawn from `seed`: outputs then
/// close on their size, where a compressible value would leave the writer's
/// held state to close them first.
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed;
    let mut value: Vec<u8> = core::iter::repeat_with(|| {
        // splitmix64
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (z ^ (z >> 31)).to_le_bytes()
    })
    .take(len.div_ceil(8))
    .flatten()
    .collect();
    value.truncate(len);
    value
}

/// Writes keys `keys` with incompressible `value_len`-byte values, flushes
/// them and moves the flushed run to `level`.
fn flush_into(
    tree: &crate::Tree,
    keys: core::ops::Range<u32>,
    value_len: usize,
    level: u8,
    seqno: &mut u64,
) -> crate::Result<()> {
    for i in keys {
        tree.insert(key(i), noise(*seqno, value_len), *seqno);
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
        assert!(
            boundaries.contains(last),
            "an output ends at {last}: outputs end at {outputs:?}, the level below at {boundaries:?}"
        );
    }
    Ok(())
}

/// A merge into a level whose next level is empty aligns to the first level
/// below that holds tables: its outputs move down through the empty levels
/// untouched and are next merged there, as an intra-L0 merge's outputs are
/// merged into the base level.
#[test]
fn a_merge_above_an_empty_level_ends_its_outputs_on_the_next_level_holding_tables()
-> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(
        Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .compaction_threads(1),
    )?;
    let mut seqno = 1;
    flush_into(&tree, 0..3_000, 700, 3, &mut seqno)?;
    flush_into(&tree, 0..3_000, 1_000, 1, &mut seqno)?;
    assert!(
        tables(&tree, 2).is_empty(),
        "the level below the merge is empty"
    );
    let boundaries = boundaries_of(&tables(&tree, 3));
    let ids = tables(&tree, 1).iter().map(Table::id).collect();
    tree.compact(Arc::new(Merge(ids, 1)), 0)?;

    let outputs = last_keys(&tables(&tree, 1));
    assert!(outputs.len() > 3, "{} outputs", outputs.len());
    let (_, cut) = outputs.split_last().unwrap();
    for last in cut {
        assert!(
            boundaries.contains(last),
            "an output ends at {last}: outputs end at {outputs:?}, L3 at {boundaries:?}"
        );
    }
    Ok(())
}

/// A level below holding two overlapping runs has no boundary inside their
/// overlap: an output ending there would reach a table of each run. Every cut
/// lands where no table of either run spans it.
#[test]
fn outputs_end_outside_every_table_of_overlapping_runs_below() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(
        Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .compaction_threads(1),
    )?;
    let mut seqno = 1;
    flush_into(&tree, 0..1_800, 700, 3, &mut seqno)?;
    flush_into(&tree, 1_200..3_000, 700, 3, &mut seqno)?;
    let runs = tree
        .current_version()
        .level(3)
        .map_or(0, |level| level.run_count());
    assert_eq!(runs, 2, "the level below holds two overlapping runs");
    flush_into(&tree, 0..3_000, 1_000, 1, &mut seqno)?;
    let below: Vec<(u32, u32)> = tables(&tree, 3)
        .iter()
        .map(|t| {
            (
                index(t.metadata.key_range.min()),
                index(t.metadata.key_range.max()),
            )
        })
        .collect();
    let ids = tables(&tree, 1).iter().map(Table::id).collect();
    tree.compact(Arc::new(Merge(ids, 2)), 0)?;

    let outputs = last_keys(&tables(&tree, 2));
    assert!(outputs.len() > 3, "{} outputs", outputs.len());
    let (_, cut) = outputs.split_last().unwrap();
    for &last in cut {
        assert!(
            !below.iter().any(|&(min, max)| min <= last && last < max),
            "an output ends at {last}, inside a table below: outputs end at {outputs:?}, \
             the tables below span {below:?}"
        );
    }
    Ok(())
}

/// The compaction byte counter grows by the bytes of the tables a merge
/// installs, and a move, which writes none, leaves it as it was.
#[cfg(feature = "metrics")]
#[test]
fn compaction_bytes_written_counts_the_tables_a_merge_installs() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    ))?;
    let mut seqno = 1;
    flush_into(&tree, 0..1_000, 1_000, 1, &mut seqno)?;
    assert_eq!(
        tree.metrics().compaction_bytes_written(),
        0,
        "a move writes nothing"
    );
    let ids = tables(&tree, 1).iter().map(Table::id).collect();
    tree.compact(Arc::new(Merge(ids, 2)), 0)?;
    let installed: u64 = tables(&tree, 2).iter().map(Table::file_size).sum();
    assert!(installed > 0);
    assert_eq!(tree.metrics().compaction_bytes_written(), installed);
    Ok(())
}

/// A merge split into key ranges ends every output on a boundary of the level
/// below, as the stream does, or where its range ends. In a range with no
/// boundary left before its end, an output ends at the target: no boundary
/// would come to end it on.
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
    for &last in cut {
        if boundaries.contains(&last) {
            at_boundaries += 1;
            continue;
        }
        if seams.contains(&last) {
            continue;
        }
        // A range ends on the first seam at or past its keys.
        let range_end = seams.iter().copied().find(|&seam| seam > last);
        assert!(
            !boundaries
                .iter()
                .any(|&b| b > last && range_end.is_none_or(|end| b < end)),
            "an output ends at {last} with a boundary still ahead in its range: outputs end \
             at {outputs:?}, the level below at {boundaries:?}, ranges at {seams:?}"
        );
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

/// The slices of a tight-space pass end their outputs on the boundaries of the
/// level below as one stream does. An output ends off one only where no
/// boundary is left before its slice ends: at the slice's end, or at the
/// target with no boundary left to wait for.
#[cfg(feature = "std")]
#[test]
fn tight_space_slices_end_their_outputs_on_the_level_below() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};

    // What the quota leaves past the tree's footprint: the budget of a slice,
    // a fraction of the merge's output.
    const HEADROOM: u64 = 256 << 10;

    let folder = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let faulty = FaultFs::new(mem.clone());
    // Free space reads as unknown, so the quota alone constrains the merge.
    faulty.injector().arm(FaultRule::new(
        FaultOp::AvailableSpace,
        Fault::Error(crate::io::ErrorKind::Unsupported),
    ));
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(faulty))
    .compaction_threads(1)
    .open()?
    else {
        panic!("expected a standard tree");
    };
    let mut seqno = 1;
    for start in (0..3_000).step_by(30) {
        flush_into(&tree, start..start + 30, 700, 3, &mut seqno)?;
    }
    // One table above: the space gate finds no narrower merge that fits.
    flush_into(&tree, 0..3_000, 1_000, 1, &mut seqno)?;
    let inputs = tables(&tree, 1);
    assert_eq!(inputs.len(), 1, "the merge reads one table");
    let boundaries = boundaries_of(&tables(&tree, 3));
    let slice_ends: Vec<u32> = super::tight_slice_boundaries(
        &inputs,
        HEADROOM,
        &crate::comparator::DefaultUserComparator,
    )?
    .iter()
    .map(|end| index(end))
    .collect();
    assert!(
        slice_ends.len() > 2,
        "the pass runs in slices: {slice_ends:?}"
    );

    let used = crate::storage_stats::compute_used_bytes(&tree.current_version())?;
    tree.update_runtime_config(|cfg| {
        cfg.storage_admission_check = true;
        cfg.tight_space_compaction = true;
        cfg.storage_limit_bytes = Some(used + HEADROOM);
    })?;
    tree.compact(
        Arc::new(Merge(inputs.iter().map(Table::id).collect(), 2)),
        0,
    )?;
    assert!(
        mem.punched_bytes() > 0,
        "the merge ran as a tight-space pass"
    );
    for i in 0..3_000 {
        assert!(tree.get(key(i), crate::SeqNo::MAX)?.is_some(), "key {i}");
    }

    let outputs = last_keys(&tables(&tree, 2));
    let (_, cut) = outputs.split_last().unwrap();
    let mut at_boundaries = 0;
    for &last in cut {
        if boundaries.contains(&last) {
            at_boundaries += 1;
            continue;
        }
        // A slice holds the keys below its end key.
        let slice_end = slice_ends.iter().copied().find(|&end| end > last);
        assert!(
            !boundaries
                .iter()
                .any(|&b| b > last && slice_end.is_none_or(|end| b < end)),
            "an output ends at {last} with a boundary still ahead in its slice: outputs end at \
             {outputs:?}, the level below at {boundaries:?}, slices at {slice_ends:?}"
        );
    }
    assert!(at_boundaries > 0, "the slices' outputs end on boundaries");
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
        // Tables of 30 keys below: every other boundary falls between two keys
        // of a 4-row group above, once the output holds 28 rows, past half
        // its target, and before the writer's held state closes it.
        for start in (0..3_000).step_by(30) {
            flush_into(&tree, start..start + 30, 700, 3, &mut seqno)?;
        }
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
        "{carried} of {groups_in} groups copied, those an output ends inside included: outputs \
         end at {outputs:?}, the level below at {boundaries:?}"
    );
    let (_, cut) = outputs.split_last().unwrap();
    for last in cut {
        assert!(
            boundaries.contains(last),
            "an output ends at {last}: outputs end at {outputs:?}, the level below at \
             {boundaries:?}"
        );
    }
    Ok(())
}
