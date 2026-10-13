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

/// A tree written uncompressed whatever codecs are built in: rounds are
/// counted in bytes, which a codec's default would shrink.
fn tree() -> crate::Result<(tempfile::TempDir, AnyTree)> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(crate::config::CompressionPolicy::disabled())
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

/// A round costs its size ratio plus two units of compaction per unit
/// written: the ratio used is what the model's compaction rate pays for,
/// down to one where it pays for none, never past the one set.
#[test]
fn the_size_ratio_is_what_the_compaction_rate_pays_for() {
    let ratio = |encode_rate: u64| {
        Strategy::default()
            .with_size_ratio(3)
            .with_model(Model {
                write_rate: 10,
                encode_rate,
                ..Model::default()
            })
            .size_ratio(0)
    };
    assert_eq!(ratio(10), 1, "one unit per unit written pays for no round");
    assert_eq!(ratio(20), 1, "twice the write rate: the cheapest round");
    assert_eq!(ratio(40), 2, "four times: a ratio of two");
    assert_eq!(ratio(50), 3, "five times: the ratio set");
    assert_eq!(ratio(1_000), 3, "never past the ratio set");
}

/// The tree's compaction budget caps the model's rate: a model that could
/// pay for the ratio set gets the ratio its tree's budget pays for.
#[test]
fn the_size_ratio_is_what_the_trees_compaction_budget_pays_for() {
    let strategy = Strategy::default().with_size_ratio(3).with_model(Model {
        write_rate: 10,
        encode_rate: 1_000,
        ..Model::default()
    });
    assert_eq!(strategy.size_ratio(0), 3, "no budget: the model's rate");
    assert_eq!(strategy.size_ratio(20), 1, "twice the write rate");
    assert_eq!(strategy.size_ratio(40), 2, "four times the write rate");
}

/// Runs the L0 table count sends down before they hold a unit count for the
/// bytes they hold: a round of them, each far below a unit, does not end the
/// round and send the last level into a merge.
#[test]
fn small_runs_sent_down_by_the_table_count_do_not_end_a_round() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let strategy = Arc::new(
        Strategy::default()
            .with_round(4)
            .with_size_ratio(3)
            // A unit far past what a flush writes.
            .with_table_target_size(1 << 30)
            .with_l0_file_threshold(1),
    );
    for round in 0..6 {
        write_round(&tree, round)?;
        settle(&tree, &strategy)?;
    }
    let version = tree.current_version();
    assert!(
        version.level(DRAIN).is_none_or(|level| level.is_empty()),
        "nothing went to the drain level"
    );
    let last = version.level_count() - 1;
    assert!(
        version.level(last).is_none_or(|level| level.is_empty()),
        "nothing went into the last level"
    );
    Ok(())
}

/// The pending bytes are what each level holds past its bound: nothing on an
/// empty tree, the top level past its unit before it comes down, nothing once
/// it has and the main level is within its round.
#[test]
fn pending_bytes_with_levels_past_their_bounds_count_the_excess() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let strategy = strategy(Model::default());
    assert_eq!(strategy.get_name(), NAME);
    assert_eq!(
        strategy.pending_compaction_bytes(&tree.current_version()),
        0
    );

    write_round(&tree, 0)?;
    write_round(&tree, 1)?;
    // The budget the strategy last saw is set by a choice.
    let _ = strategy.choose(
        &tree.current_version(),
        tree.tree_config(),
        &CompactionState::default(),
    );
    let version = tree.current_version();
    let top = version.level(0).expect("L0").size();
    assert!(top > UNIT, "two rounds hold more than a unit");
    let pending = strategy.pending_compaction_bytes(&version);
    assert!(
        pending > 0 && pending < top,
        "the top level past its unit: {pending} of {top}"
    );

    settle(&tree, &strategy)?;
    assert_eq!(
        strategy.pending_compaction_bytes(&tree.current_version()),
        0,
        "the top level came down and the main level is within its round",
    );
    Ok(())
}

/// A main level being compacted holds the top level up: nothing is chosen
/// until it is free.
#[test]
fn choose_with_the_main_level_busy_does_nothing() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let strategy = strategy(Model::default());
    write_round(&tree, 0)?;
    settle(&tree, &strategy)?;
    write_round(&tree, 1)?;
    let version = tree.current_version();
    let config = tree.tree_config();
    assert!(
        !matches!(
            strategy.choose(&version, config, &CompactionState::default()),
            Choice::DoNothing
        ),
        "a free main level takes the top level"
    );
    let mut state = CompactionState::default();
    state
        .hidden_set_mut()
        .hide(version.level(1).expect("L1").list_ids());
    let choice = strategy.choose(&version, config, &state);
    assert!(matches!(choice, Choice::DoNothing), "{choice:?}");
    Ok(())
}

/// An ended round being compacted is not merged into the last level again
/// meanwhile.
#[test]
fn choose_with_the_ended_round_busy_merges_nothing_into_the_last() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let strategy = strategy(Model::default());
    write_round(&tree, 0)?;
    settle(&tree, &strategy)?;
    tree.compact(Arc::new(crate::compaction::MoveDown(1, DRAIN_LEVEL)), 0)?;
    let version = tree.current_version();
    let config = tree.tree_config();
    let last = version.level_count() - 1;
    let into_last = |choice: &Choice| matches!(choice, Choice::Merge(input) if usize::from(input.dest_level) == last);
    assert!(into_last(&strategy.choose(
        &version,
        config,
        &CompactionState::default()
    )));
    let mut state = CompactionState::default();
    state
        .hidden_set_mut()
        .hide(version.level(DRAIN).expect("drain").list_ids());
    let choice = strategy.choose(&version, config, &state);
    assert!(!into_last(&choice), "{choice:?}");
    Ok(())
}

/// Point reads made in a batch count in the read mix as single ones do, so a
/// workload of batched reads beside scans is not planned as scans alone.
#[test]
fn read_mix_with_batched_point_reads_counts_each_key() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_round(&tree, 0)?;
    let reads = || -> u64 {
        tree.current_version()
            .iter_tables()
            .map(Table::point_reads)
            .sum()
    };
    assert_eq!(reads(), 0);
    let keys: Vec<String> = (0..50u64).map(|i| format!("k{i:04}")).collect();
    let found = tree.multi_get(&keys, SeqNo::MAX)?;
    assert!(found.iter().all(Option::is_some));
    assert_eq!(reads(), 50, "one point read per key the table served");
    Ok(())
}

/// A scan counts once in the read mix however many runs it reaches or skips:
/// one scan reaching one of two runs beside one point read is half the reads.
#[test]
fn read_mix_with_a_scan_reaching_one_run_counts_one_scan() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let write = |from: u64, seqno: u64| -> crate::Result<()> {
        for i in from..from + 100 {
            tree.insert(format!("k{i:04}"), "v", seqno + i);
        }
        tree.flush_active_memtable(0)
    };
    write(0, 0)?;
    tree.compact(Arc::new(crate::compaction::MoveDown(0, 1)), 0)?;
    write(1_000, 10_000)?;
    assert_eq!(tree.range("k0010"..="k0020", SeqNo::MAX, None).count(), 11);
    assert!(tree.get("k1050", SeqNo::MAX)?.is_some());

    let version = tree.current_version();
    let runs: usize = version.iter_levels().map(|level| level.run_count()).sum();
    assert_eq!(runs, 2);
    let share = strategy(Model::default())
        .inputs(&version, UNIT, 0)
        .scan_share;
    assert!((share - 0.5).abs() < 1e-9, "{share}");
    Ok(())
}

/// The first `levels` levels of `tree`'s version as a version of their own:
/// the shape a manifest of fewer levels recovers to, which a tree this
/// engine opens anew never has.
fn first_levels(tree: &AnyTree, levels: usize) -> Version {
    let version = tree.current_version();
    Version::from_levels(
        0,
        crate::TreeType::Standard,
        (0..levels)
            .map(|index| version.level(index).expect("a level").clone())
            .collect(),
        crate::version::BlobFileList::default(),
        crate::blob_tree::FragmentationMap::default(),
    )
}

/// A tree of three levels has no level for an ended round to wait in: the
/// round merges into the last at once.
#[test]
fn choose_on_three_levels_merges_an_ended_round_into_the_last() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_round(&tree, 0)?;
    tree.compact(Arc::new(crate::compaction::MoveDown(0, DRAIN_LEVEL)), 0)?;
    write_round(&tree, 1)?;
    tree.compact(Arc::new(crate::compaction::MoveDown(0, 1)), 0)?;
    let version = first_levels(&tree, 3);
    // A round of one unit, which L1 holds.
    let strategy = Strategy::default()
        .with_round(1)
        .with_size_ratio(3)
        .with_table_target_size(UNIT);
    let choice = strategy.choose(&version, tree.tree_config(), &CompactionState::default());
    let Choice::Merge(input) = choice else {
        panic!("the round merges into the last: {choice:?}");
    };
    assert_eq!(input.dest_level, DRAIN_LEVEL);
    let mut expected = version.level(1).expect("L1").list_ids();
    expected.extend(version.level(2).expect("L2").list_ids());
    assert_eq!(input.table_ids, expected);
    Ok(())
}

/// A tree of one level has no level to compact into: nothing is chosen, and
/// nothing is owed.
#[test]
fn choose_on_one_level_does_nothing_and_owes_nothing() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_round(&tree, 0)?;
    let version = first_levels(&tree, 1);
    let strategy = strategy(Model::default());
    assert!(matches!(
        strategy.choose(&version, tree.tree_config(), &CompactionState::default()),
        Choice::DoNothing
    ));
    assert_eq!(strategy.pending_compaction_bytes(&version), 0);
    Ok(())
}

/// A tree of two levels has no main level apart from the last: the top level
/// goes into it whole, and the top level is all it owes.
#[test]
fn choose_on_two_levels_merges_the_top_into_the_last() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    let strategy = strategy(Model::default());
    write_round(&tree, 0)?;
    tree.compact(Arc::new(crate::compaction::MoveDown(0, 1)), 0)?;
    let version = first_levels(&tree, 2);
    assert_eq!(strategy.pending_compaction_bytes(&version), 0);
    assert!(
        matches!(
            strategy.choose(&version, tree.tree_config(), &CompactionState::default()),
            Choice::DoNothing
        ),
        "an empty top level has nothing to send"
    );
    write_round(&tree, 1)?;
    let version = first_levels(&tree, 2);
    let choice = strategy.choose(&version, tree.tree_config(), &CompactionState::default());
    let Choice::Merge(input) = choice else {
        panic!("the top goes into the last: {choice:?}");
    };
    assert_eq!(input.dest_level, 1);
    let mut expected = version.level(0).expect("L0").list_ids();
    expected.extend(version.level(1).expect("L1").list_ids());
    assert_eq!(input.table_ids, expected);
    assert!(strategy.pending_compaction_bytes(&version) > 0);
    Ok(())
}

/// A lookup of an absent key counts in the read mix though no filter lets it
/// through: one scan beside three such lookups is a quarter of the reads.
#[test]
fn read_mix_with_lookups_of_absent_keys_counts_them() -> crate::Result<()> {
    let (_dir, tree) = tree()?;
    write_round(&tree, 0)?;
    assert_eq!(tree.range("k0010"..="k0020", SeqNo::MAX, None).count(), 11);
    for key in ["absent-a", "absent-b", "absent-c"] {
        assert!(tree.get(key, SeqNo::MAX)?.is_none());
    }
    let share = strategy(Model::default())
        .inputs(&tree.current_version(), UNIT, 0)
        .scan_share;
    assert!((share - 0.25).abs() < 1e-9, "{share}");
    Ok(())
}

/// A projected scan counts in the read mix as a row scan does.
#[cfg(feature = "columnar")]
#[test]
fn read_mix_with_a_projected_scan_counts_it() -> crate::Result<()> {
    use crate::table::columnar::COL_USER_KEY;

    let dir = tempfile::tempdir()?;
    let AnyTree::Standard(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?
    else {
        panic!("a standard tree");
    };
    tree.update_runtime_config(|c| c.columnar = true)?;
    for i in 0..100u64 {
        tree.insert(format!("k{i:04}"), "v", i);
    }
    tree.flush_active_memtable(0)?;
    let rows: u32 = tree
        .columnar_scan(&[COL_USER_KEY][..], None, SeqNo::MAX, ..)?
        .map(|batch| batch.map(|b| b.row_count))
        .sum::<crate::Result<u32>>()?;
    assert_eq!(rows, 100);
    assert_eq!(tree.current_version().read_mix().counts(), (0, 1));
    Ok(())
}

/// Every point lookup of a blob tree counts once in the read mix, the size of
/// a merge operand's key among them, which merges the operands too.
#[test]
fn read_mix_with_blob_point_lookups_counts_each_once() -> crate::Result<()> {
    use crate::blob_tree::field_row::Field;

    struct Concat;

    impl crate::merge_operator::MergeOperator for Concat {
        fn merge(
            &self,
            _key: &[u8],
            base: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> crate::Result<crate::UserValue> {
            let mut value = base.unwrap_or_default().to_vec();
            for operand in operands {
                value.extend_from_slice(operand);
            }
            Ok(value.into())
        }
    }

    let dir = tempfile::tempdir()?;
    let AnyTree::Blob(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        crate::KvSeparationOptions::default().separation_threshold(16),
    ))
    .with_merge_operator(Some(Arc::new(Concat)))
    .open()?
    else {
        panic!("a blob tree");
    };
    tree.insert("base", vec![b'x'; 64], 0);
    tree.merge("base", "+", 1);
    tree.insert("plain", "v", 2);
    tree.insert_cells("row", &[Field::bytes(3, b"cell")], 3)?;
    tree.flush_active_memtable(0)?;

    let (before, _) = tree.current_version().read_mix().counts();
    assert_eq!(tree.size_of("base", SeqNo::MAX)?, Some(65));
    assert_eq!(tree.size_of("plain", SeqNo::MAX)?, Some(1));
    assert!(tree.get("base", SeqNo::MAX)?.is_some());
    assert!(tree.get_pinned("base", SeqNo::MAX)?.is_some());
    assert!(tree.contains_key("plain", SeqNo::MAX)?);
    assert_eq!(tree.multi_get(["base", "plain"], SeqNo::MAX)?.len(), 2);
    assert!(tree.get_internal_entry(b"plain", SeqNo::MAX)?.is_some());
    assert!(tree.get_cells("row", SeqNo::MAX)?.is_some());
    let (points, scans) = tree.current_version().read_mix().counts();
    assert_eq!((points - before, scans), (9, 0));
    Ok(())
}

/// The budget a tree's own limiter gives is the live one: a rate changed
/// after open is the rate the planner sees.
#[test]
fn budget_after_a_rate_change_is_the_new_rate() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .compaction_rate_limit(1_000)
    .open()?;
    assert_eq!(budget(tree.tree_config()), 1_000);
    tree.compaction_rate_limiter().set_rate(5_000);
    assert_eq!(budget(tree.tree_config()), 5_000);
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
