// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::*;
use crate::compaction::CompactionAction;
use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

const UNIT: u64 = 8 * 1_024;

/// A round of sixteen units of 8 KiB at least; a written round is about
/// three.
fn strategy(model: Model) -> Arc<Strategy> {
    Arc::new(
        Strategy::default()
            .with_round(16)
            .with_size_ratio(3)
            .with_table_target_size(UNIT)
            .with_model(model),
    )
}

fn tree() -> crate::Result<(tempfile::TempDir, AnyTree)> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    Ok((dir, tree))
}

/// Writes keys `0..200` at round `round`, about two units, and flushes them.
fn write_round(tree: &AnyTree, round: u64) -> crate::Result<()> {
    for i in 0..200u64 {
        tree.insert(
            format!("k{i:04}"),
            format!("{round:08}{}", "x".repeat(80)),
            round * 1_000 + i,
        );
    }
    tree.flush_active_memtable(0)?;
    Ok(())
}

/// Compacts until the strategy has nothing to do, returning what it did.
fn settle(tree: &AnyTree, strategy: &Arc<Strategy>) -> crate::Result<Vec<CompactionAction>> {
    let mut actions = Vec::new();
    loop {
        let strategy: Arc<dyn CompactionStrategy> = strategy.clone();
        match tree.compact(strategy, 0)?.action {
            CompactionAction::Nothing => return Ok(actions),
            action => actions.push(action),
        }
    }
}

fn level_runs(tree: &AnyTree, level: usize) -> usize {
    tree.current_version()
        .level(level)
        .map_or(0, |level| level.run_count())
}

/// A top level of a unit comes down as one run of the main level; once a
/// round of units has come down, the main level goes into the last.
#[test]
fn the_top_level_comes_down_a_unit_at_a_time_and_a_round_ends_in_the_last() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let strategy = strategy(Model::default());

    write_round(&tree, 0)?;
    settle(&tree, &strategy)?;
    assert!(tree.current_version().level(0).expect("L0").is_empty());
    assert_eq!(level_runs(&tree, 1), 1, "one run of the main level");

    // At least a unit per written round, so more rounds than a round's
    // sixteen units.
    for round in 1..24 {
        write_round(&tree, round)?;
        settle(&tree, &strategy)?;
    }
    let last = tree.current_version().level_count() - 1;
    assert!(
        level_runs(&tree, last) == 1,
        "rounds ended in the last level"
    );
    for i in 0..200u64 {
        let value = tree.get(format!("k{i:04}"), SeqNo::MAX)?.expect("kept");
        assert!(
            value.starts_with(b"00000023"),
            "key {i} reads its newest value"
        );
    }
    Ok(())
}

/// With no read seen no merge pays: the main level's runs accumulate until
/// the round ends.
#[test]
fn without_reads_the_main_level_is_not_merged() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let strategy = strategy(Model::default());
    for round in 0..4 {
        write_round(&tree, round)?;
        settle(&tree, &strategy)?;
    }
    assert_eq!(level_runs(&tree, 1), 4, "one run per unit come down");
    Ok(())
}

/// Scans reaching every run, and merges far cheaper than the time between
/// two units coming down, make the plan merge the main level's runs.
#[test]
fn scans_and_cheap_merges_merge_the_main_level() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let strategy = strategy(Model {
        write_rate: 1,
        encode_rate: u64::MAX,
        copy_rate: u64::MAX,
        ..Model::default()
    });
    for round in 0..4 {
        write_round(&tree, round)?;
        settle(&tree, &strategy)?;
        // Scans only: every read a scan.
        for _ in 0..50 {
            assert_eq!(tree.iter(SeqNo::MAX, None).count(), 200);
        }
    }
    assert!(
        level_runs(&tree, 1) < 4,
        "the plan merged the main level's runs: {}",
        level_runs(&tree, 1),
    );
    Ok(())
}

/// A scan share set on the strategy stands in for the tables' counts: with no
/// read made, it plans the merges the counted scans would.
#[test]
fn a_set_scan_share_stands_in_for_the_counted_reads() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let strategy = Arc::new(
        Arc::into_inner(strategy(Model {
            write_rate: 1,
            encode_rate: u64::MAX,
            copy_rate: u64::MAX,
            ..Model::default()
        }))
        .expect("sole owner")
        .with_scan_share(Some(1.0)),
    );
    for round in 0..4 {
        write_round(&tree, round)?;
        settle(&tree, &strategy)?;
    }
    assert!(
        level_runs(&tree, 1) < 4,
        "the plan merged the main level's runs: {}",
        level_runs(&tree, 1),
    );
    Ok(())
}

/// The same version, model and reads choose the same, on two strategies.
#[test]
fn the_choice_is_the_same_for_the_same_inputs() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let (a, b) = (strategy(Model::default()), strategy(Model::default()));
    write_round(&tree, 0)?;
    let version = tree.current_version();
    let config = tree.tree_config();
    let state = CompactionState::default();
    assert_eq!(
        format!("{:?}", a.choose(&version, config, &state)),
        format!("{:?}", b.choose(&version, config, &state)),
    );
    Ok(())
}
