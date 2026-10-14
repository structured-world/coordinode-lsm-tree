// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A level below L0 may hold several runs that overlap, newest first, as a
//! tiered layout leaves them: a read finds a key the older run alone holds,
//! even where the newer run's table covers its range.

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

fn l0_ids(tree: &AnyTree) -> Vec<TableId> {
    tree.current_version()
        .level(0)
        .into_iter()
        .flat_map(|level| level.iter())
        .flat_map(|run| run.iter())
        .map(crate::Table::id)
        .collect()
}

/// L1 holds an older run of `a`, `m` and a newer one of `b`, `z`, whose table
/// covers `m` without holding it.
fn two_overlapping_runs() -> crate::Result<(tempfile::TempDir, AnyTree)> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    tree.insert("a", "a", 1);
    tree.insert("m", "m", 2);
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(Merge(l0_ids(&tree), 1)), 0)?;
    tree.insert("b", "b", 3);
    tree.insert("z", "z", 4);
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(Merge(l0_ids(&tree), 1)), 0)?;
    assert_eq!(
        tree.current_version().level(1).expect("L1").run_count(),
        2,
        "two overlapping runs in L1"
    );
    Ok((folder, tree))
}

#[test]
fn a_point_read_finds_a_key_of_the_older_of_two_overlapping_runs() -> crate::Result<()> {
    let (_folder, tree) = two_overlapping_runs()?;
    for key in ["a", "m", "b", "z"] {
        assert_eq!(
            tree.get(key, SeqNo::MAX)?.as_deref(),
            Some(key.as_bytes()),
            "key {key}"
        );
    }
    Ok(())
}

#[test]
fn a_serial_level_resolve_finds_a_key_of_the_older_of_two_overlapping_runs() -> crate::Result<()> {
    let (_folder, tree) = two_overlapping_runs()?;
    let version = tree.current_version();
    let level = version.level(1).expect("L1");
    let keys = ["a", "b", "m", "z"];
    let remaining = keys
        .iter()
        .enumerate()
        .map(|(idx, key)| (idx, crate::hash::hash64(key.as_bytes())))
        .collect();
    let (found, left) = super::Tree::resolve_level_serially(
        false,
        level,
        remaining,
        &keys,
        SeqNo::MAX,
        tree.tree_config().comparator.as_ref(),
    )?;
    let mut found: Vec<usize> = found.into_iter().map(|(idx, _)| idx).collect();
    found.sort_unstable();
    assert_eq!(found, [0, 1, 2, 3], "every key is in L1; left {left:?}");
    Ok(())
}
