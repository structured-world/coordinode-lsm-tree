// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What the merge operator is told about the base it is merging onto.
//!
//! A sum of deltas cannot show the difference, because a sum of deltas is
//! itself a delta: fold it onto an empty base and the result is still the right
//! operand. The operator here is an ordered set with removals, which is the
//! smallest shape that can tell "the base is absent" from "the base is
//! elsewhere": a removal folded onto an assumed-empty set is an empty set, and
//! an empty set is not a removal.

use lsm_tree::{AbstractTree, Config, MergeOperator, SequenceNumberCounter, UserValue};
use std::sync::Arc;
use test_log::test;

/// A sorted set of `u64`s, encoded big-endian back to back. Operands tag
/// themselves: `0x01` adds, `0x02` removes, and anything else is an earlier
/// fold's output, folded in as a whole set.
struct OrderedSetMerge;

fn encode_set(members: &[u64]) -> Vec<u8> {
    members.iter().flat_map(|m| m.to_be_bytes()).collect()
}

fn decode_set(bytes: &[u8]) -> Vec<u64> {
    bytes
        .chunks_exact(8)
        .map(|c| u64::from_be_bytes(c.try_into().expect("chunks_exact(8)")))
        .collect()
}

fn add(member: u64) -> Vec<u8> {
    let mut op = vec![0x01];
    op.extend_from_slice(&member.to_be_bytes());
    op
}

fn remove(member: u64) -> Vec<u8> {
    let mut op = vec![0x02];
    op.extend_from_slice(&member.to_be_bytes());
    op
}

impl MergeOperator for OrderedSetMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut set = base_value.map(decode_set).unwrap_or_default();

        for operand in operands {
            match operand.first() {
                Some(0x01) | Some(0x02) if operand.len() == 9 => {
                    let member = u64::from_be_bytes(
                        operand[1..]
                            .try_into()
                            .map_err(|_| lsm_tree::Error::MergeOperator)?,
                    );
                    if operand[0] == 0x01 {
                        if !set.contains(&member) {
                            set.push(member);
                        }
                    } else {
                        set.retain(|m| *m != member);
                    }
                }
                // Not tagged: a whole set from an earlier fold, so it is state
                // rather than an edit, and the two are unioned.
                _ => {
                    for member in decode_set(operand) {
                        if !set.contains(&member) {
                            set.push(member);
                        }
                    }
                }
            }
        }

        set.sort_unstable();
        Ok(encode_set(&set).into())
    }
}

fn open_tree(
    folder: &tempfile::TempDir,
    seqno: &SequenceNumberCounter,
) -> lsm_tree::Result<lsm_tree::AnyTree> {
    Config::new(folder, seqno.clone(), SequenceNumberCounter::default())
        .with_merge_operator(Some(Arc::new(OrderedSetMerge)))
        .open()
}

fn read_set(tree: &lsm_tree::AnyTree, key: &str, seqno: u64) -> lsm_tree::Result<Option<Vec<u64>>> {
    tree.get(key, seqno)
        .map(|value| value.map(|value| decode_set(&value)))
}

#[test]
fn a_removal_survives_a_compaction_that_does_not_hold_the_base() -> lsm_tree::Result<()> {
    // The base sits at the last level and the compaction runs above it, so the
    // merge operand meets no base in this stream. Folding it there against an
    // assumed-empty set turns "remove 2" into "the set is empty", which then
    // unions with the real set and the removal is gone.
    //
    // Two neighbour keys either side of the key under test make every table
    // span the same range: tables with disjoint ranges are only MOVED, the
    // merge path never runs, and the test would pass without proving anything.
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(&folder, &seqno)?;

    tree.insert("k", encode_set(&[1, 2, 3]), seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(lsm_tree::compaction::MoveDown(0, 6)), seqno.get())?;

    // Level 1 covers the key's range without holding the key, so the next
    // compaction stops there instead of reaching the base.
    tree.merge("a", add(109), seqno.next());
    tree.merge("z", add(109), seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(lsm_tree::compaction::MoveDown(0, 1)), seqno.get())?;

    assert_eq!(Some(vec![1, 2, 3]), read_set(&tree, "k", seqno.get())?);

    tree.merge("k", remove(2), seqno.next());
    for round in 0..4u64 {
        tree.merge("a", add(100 + round), seqno.next());
        tree.merge("z", add(100 + round), seqno.next());
        tree.flush_active_memtable(0)?;
    }

    // The read path applies over every level, so it is right before the
    // compaction and stays the answer afterwards.
    assert_eq!(Some(vec![1, 3]), read_set(&tree, "k", seqno.get())?);

    let watermark = seqno.get();
    let result = tree.compact(
        Arc::new(lsm_tree::compaction::Leveled::default()),
        watermark,
    )?;

    // Without this the test proves nothing: the compaction has to have run,
    // and the base has to have stayed out of it (the untouched last-level
    // table beside the new output).
    assert_ne!(
        lsm_tree::compaction::CompactionAction::Nothing,
        result.action,
        "the compaction must have run",
    );
    assert_eq!(
        2,
        tree.table_count(),
        "the last-level table holding the base must not be an input",
    );

    assert_eq!(Some(vec![1, 3]), read_set(&tree, "k", seqno.get())?);

    Ok(())
}

#[test]
fn a_key_built_only_from_operands_materialises_at_the_last_level() -> lsm_tree::Result<()> {
    // No put ever, so every version of the key is an operand. At the last level
    // nothing below can hold a base, which makes absence proven and the chain
    // collapsible: one value, not a chain that carries its removal forever.
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = open_tree(&folder, &seqno)?;

    tree.merge("k", add(1), seqno.next());
    tree.merge("k", add(2), seqno.next());
    tree.merge("k", remove(1), seqno.next());
    tree.flush_active_memtable(0)?;

    let watermark = seqno.get();
    tree.major_compact(64_000_000, watermark)?;

    assert_eq!(Some(vec![2]), read_set(&tree, "k", seqno.get())?);
    assert_eq!(1, tree.table_count());

    // One entry, not three: the chain folded rather than surviving as operands.
    assert_eq!(1, tree.len(seqno.get(), None)?);

    Ok(())
}
