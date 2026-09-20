// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Folding a chain of operands where the base is not proven.
//!
//! An operator whose operands are edits against a base cannot fold without one:
//! a removal folded onto an assumed-empty set is an empty set. An operator whose
//! operands compose can, because folding a prefix yields something that is still
//! an operand. `MergeOperator::composes_operands` is how an operator says which
//! of the two it is, and only the second kind folds mid-tree.

use lsm_tree::{AbstractTree, Config, MergeOperator, SequenceNumberCounter, UserValue};
use std::sync::Arc;
use test_log::test;

fn decode(bytes: &[u8]) -> i64 {
    i64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]))
}

fn sum(base_value: Option<&[u8]>, operands: &[&[u8]]) -> lsm_tree::Result<UserValue> {
    let mut total = base_value.map(decode).unwrap_or(0);
    for operand in operands {
        total = total
            .checked_add(decode(operand))
            .ok_or(lsm_tree::Error::MergeOperator)?;
    }
    Ok(UserValue::from(total.to_le_bytes().to_vec()))
}

/// A sum of deltas: a fold of operands is itself a delta, so it composes.
struct ComposingSum;

impl MergeOperator for ComposingSum {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        sum(base_value, operands)
    }

    fn composes_operands(&self) -> bool {
        true
    }
}

/// The same arithmetic, but not declaring itself composable: it must keep
/// today's behaviour, which is what pins the default.
struct PlainSum;

impl MergeOperator for PlainSum {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        sum(base_value, operands)
    }
}

const KEY: &str = "counter";

fn open_tree(
    folder: &tempfile::TempDir,
    seqno: &SequenceNumberCounter,
    operator: Arc<dyn MergeOperator>,
) -> lsm_tree::Result<lsm_tree::AnyTree> {
    Config::new(
        folder.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(operator))
    .open()
}

fn read(tree: &lsm_tree::AnyTree, seqno: u64) -> lsm_tree::Result<Option<i64>> {
    tree.get(KEY, seqno).map(|v| v.map(|v| decode(&v)))
}

/// Builds a tree whose lowest level is occupied by neighbour keys spanning the
/// counter's range, then lays `operands` deltas for the counter in L0. A
/// compaction from there lands above the occupied level, so it holds neither a
/// boundary nor every surviving version of the counter: the base is unproven.
fn tree_with_unproven_base(
    folder: &tempfile::TempDir,
    seqno: &SequenceNumberCounter,
    operator: Arc<dyn MergeOperator>,
    operands: u64,
) -> lsm_tree::Result<lsm_tree::AnyTree> {
    let tree = open_tree(folder, seqno, operator)?;

    // Neighbours either side so every table spans the counter's range: tables
    // with disjoint ranges are only MOVED and the merge path never runs.
    tree.insert("a", b"x", seqno.next());
    tree.insert("z", b"x", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(lsm_tree::compaction::MoveDown(0, 6)), seqno.get())?;

    // A second pair at level 1, so the level covers the counter's range without
    // holding it. The compaction under test then stops at level 1 instead of
    // reaching the bottom, which is what leaves the base unproven.
    tree.insert("a", b"y", seqno.next());
    tree.insert("z", b"y", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(lsm_tree::compaction::MoveDown(0, 1)), seqno.get())?;

    for _ in 0..operands {
        tree.merge(KEY, 1_i64.to_le_bytes(), seqno.next());
        tree.flush_active_memtable(0)?;
    }

    Ok(tree)
}

#[test]
fn a_composing_operator_folds_a_chain_without_a_proven_base() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = tree_with_unproven_base(&folder, &seqno, Arc::new(ComposingSum), 6)?;

    assert_eq!(Some(6), read(&tree, seqno.get())?);
    let before = tree.approximate_len();

    let result = tree.compact(
        Arc::new(lsm_tree::compaction::Leveled::default()),
        seqno.get(),
    )?;
    assert_ne!(
        lsm_tree::compaction::CompactionAction::Nothing,
        result.action,
        "the compaction must have run",
    );

    // The value is what it was: folding a prefix of deltas loses nothing.
    assert_eq!(Some(6), read(&tree, seqno.get())?);

    // And the chain is gone: six operand entries became one.
    let after = tree.approximate_len();
    assert!(
        after < before,
        "the operand chain must shrink, went from {before} to {after}",
    );

    Ok(())
}

#[test]
fn a_non_composing_operator_keeps_its_operands_without_a_proven_base() -> lsm_tree::Result<()> {
    // The default. This is the behaviour a merge operator that edits a base
    // depends on for correctness, so it must not move.
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = tree_with_unproven_base(&folder, &seqno, Arc::new(PlainSum), 6)?;

    let before = tree.approximate_len();
    let result = tree.compact(
        Arc::new(lsm_tree::compaction::Leveled::default()),
        seqno.get(),
    )?;
    assert_ne!(
        lsm_tree::compaction::CompactionAction::Nothing,
        result.action,
        "the compaction must have run",
    );

    assert_eq!(Some(6), read(&tree, seqno.get())?);
    assert_eq!(
        before,
        tree.approximate_len(),
        "an operator that does not compose must keep every operand",
    );

    Ok(())
}

#[test]
fn a_composed_operand_still_folds_onto_a_base_that_appears_later() -> lsm_tree::Result<()> {
    // The composed result is an operand, not a value: when a later compaction
    // does reach the base, it must fold onto it rather than replace it.
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(&folder, &seqno, Arc::new(ComposingSum))?;

    // A real base at the bottom, written as a put.
    tree.insert(KEY, 100_i64.to_le_bytes(), seqno.next());
    tree.insert("a", b"x", seqno.next());
    tree.insert("z", b"x", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(lsm_tree::compaction::MoveDown(0, 6)), seqno.get())?;

    for _ in 0..4 {
        tree.merge(KEY, 1_i64.to_le_bytes(), seqno.next());
        tree.flush_active_memtable(0)?;
    }
    assert_eq!(Some(104), read(&tree, seqno.get())?);

    // Compaction above the base composes the deltas into one operand.
    tree.compact(
        Arc::new(lsm_tree::compaction::Leveled::default()),
        seqno.get(),
    )?;
    assert_eq!(Some(104), read(&tree, seqno.get())?);

    // Now one that does reach the base: 100 + 4, not 4.
    tree.major_compact(64_000_000, seqno.get())?;
    assert_eq!(Some(104), read(&tree, seqno.get())?);

    Ok(())
}

/// Checked arithmetic, so the operator can refuse. Composition moves where a
/// refusal happens, which is the shape the fallback exists for.
struct CheckedSum;

impl MergeOperator for CheckedSum {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut total = base_value.map(decode).unwrap_or(0);
        for operand in operands {
            total = total
                .checked_add(decode(operand))
                .ok_or(lsm_tree::Error::MergeOperator)?;
        }
        Ok(UserValue::from(total.to_le_bytes().to_vec()))
    }

    fn composes_operands(&self) -> bool {
        true
    }
}

#[test]
fn a_composition_the_operator_refuses_falls_back_to_the_operands() -> lsm_tree::Result<()> {
    // The operands compose to something out of range while the whole chain
    // against the real base does not: -1 + i64::MAX + 1 is i64::MAX, but
    // i64::MAX + 1 on its own overflows. The compaction must not fail over an
    // optimisation it can simply decline.
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(&folder, &seqno, Arc::new(CheckedSum))?;

    tree.insert(KEY, (-1_i64).to_le_bytes(), seqno.next());
    tree.insert("a", b"x", seqno.next());
    tree.insert("z", b"x", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(lsm_tree::compaction::MoveDown(0, 6)), seqno.get())?;

    tree.insert("a", b"y", seqno.next());
    tree.insert("z", b"y", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(lsm_tree::compaction::MoveDown(0, 1)), seqno.get())?;

    tree.merge(KEY, i64::MAX.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;
    tree.merge(KEY, 1_i64.to_le_bytes(), seqno.next());
    tree.flush_active_memtable(0)?;

    assert_eq!(Some(i64::MAX), read(&tree, seqno.get())?);
    let before = tree.approximate_len();

    // Composing these two operands overflows, so the fold declines and the
    // compaction still succeeds.
    let result = tree.compact(
        Arc::new(lsm_tree::compaction::Leveled::default()),
        seqno.get(),
    )?;
    assert_ne!(
        lsm_tree::compaction::CompactionAction::Nothing,
        result.action,
        "the compaction must have run",
    );
    assert_eq!(Some(i64::MAX), read(&tree, seqno.get())?);

    // Declined, not silently folded to a wrong value: both operands survive.
    assert_eq!(
        before,
        tree.approximate_len(),
        "a refused composition must leave the operands in place",
    );

    // And the fold that does meet the base is in range, so it happens there.
    tree.major_compact(64_000_000, seqno.get())?;
    assert_eq!(Some(i64::MAX), read(&tree, seqno.get())?);

    Ok(())
}

#[test]
fn a_watermark_below_the_newest_operands_leaves_them_alone() -> lsm_tree::Result<()> {
    // Folding is gated on the watermark exactly as the proven-base fold is: the
    // composed operand carries the head's seqno, so operands at or above the
    // watermark must stay separate and the total must still read right.
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = tree_with_unproven_base(&folder, &seqno, Arc::new(ComposingSum), 4)?;

    let watermark = seqno.get();
    for _ in 0..3 {
        tree.merge(KEY, 1_i64.to_le_bytes(), seqno.next());
        tree.flush_active_memtable(0)?;
    }

    tree.compact(
        Arc::new(lsm_tree::compaction::Leveled::default()),
        watermark,
    )?;

    assert_eq!(Some(7), read(&tree, seqno.get())?);

    Ok(())
}
