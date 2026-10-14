// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A move of several overlapping runs into another level keeps them apart and
//! in age order: a key both hold reads its newer version afterwards, and a
//! key only the older holds is still found.

use crate::compaction::{Choice, CompactionStrategy, Input, state::CompactionState};
use crate::version::Version;
use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, TableId};
use std::sync::Arc;
use test_log::test;

/// Merges exactly the named tables into a level.
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
            target_size: u64::MAX,
        })
    }
}

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

fn level_ids(tree: &AnyTree, level: usize) -> Vec<TableId> {
    tree.current_version()
        .level(level)
        .into_iter()
        .flat_map(|level| level.iter())
        .flat_map(|run| run.iter())
        .map(crate::Table::id)
        .collect()
}

/// The older run starts below the newer one, so ordering the moved tables by
/// their smallest key alone would put the older version of `k` in front.
#[test]
fn a_move_of_overlapping_runs_keeps_the_newer_version_in_front() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Older: keys a..k, `k` at "old".
    tree.insert("a", "a", 1);
    tree.insert("k", "old", 2);
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(Merge(level_ids(&tree, 0), 1)), 0)?;

    // Newer: keys k..z, `k` at "new"; merged on its own into L1 it becomes a
    // second, newer run there.
    tree.insert("k", "new", 3);
    tree.insert("z", "z", 4);
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(Merge(level_ids(&tree, 0), 1)), 0)?;
    assert_eq!(
        tree.current_version().level(1).expect("L1").run_count(),
        2,
        "two overlapping runs in L1"
    );
    assert_eq!(&*tree.get("k", SeqNo::MAX)?.expect("k"), b"new");

    tree.compact(Arc::new(Place(level_ids(&tree, 1), 2)), 0)?;
    let l2 = tree.current_version();
    assert_eq!(l2.level(2).expect("L2").run_count(), 2, "kept apart");
    assert_eq!(
        &*tree.get("k", SeqNo::MAX)?.expect("k"),
        b"new",
        "the newer run stays in front"
    );
    assert_eq!(&*tree.get("a", SeqNo::MAX)?.expect("a"), b"a");
    assert_eq!(&*tree.get("z", SeqNo::MAX)?.expect("z"), b"z");
    Ok(())
}
