// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Equal-seqno tie-breaks on the range read path: sources holding the same
//! key at the same caller-assigned seqno must resolve by SOURCE recency,
//! exactly as the point read does.

use lsm_tree::{AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

/// A flushed table and the active memtable can hold the SAME key at the SAME
/// caller-assigned seqno (`apply_batch` does not require unique seqnos). Such
/// a tie is resolved by SOURCE recency — the memtable is the newest source —
/// and the range path must agree with the point path in both directions, or a
/// scan serves a value `get` does not.
#[test]
fn tree_range_breaks_equal_seqno_ties_by_source_recency() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert(b"k", b"old", 7);
    tree.flush_active_memtable(0)?;
    tree.insert(b"k", b"new", 7);

    assert_eq!(
        tree.get(b"k", SeqNo::MAX)?.as_deref(),
        Some(b"new".as_slice()),
        "the point read resolves the tie to the newest source",
    );
    let forward: Vec<Vec<u8>> = tree
        .iter(SeqNo::MAX, None)
        .map(|guard| guard.into_inner().expect("entry").1.to_vec())
        .collect();
    assert_eq!(
        forward,
        vec![b"new".to_vec()],
        "the forward range read must serve what the point read serves",
    );
    let backward: Vec<Vec<u8>> = tree
        .iter(SeqNo::MAX, None)
        .rev()
        .map(|guard| guard.into_inner().expect("entry").1.to_vec())
        .collect();
    assert_eq!(
        backward,
        vec![b"new".to_vec()],
        "the backward range read must serve what the point read serves",
    );
    Ok(())
}

/// The same tie across TWO sealed generations: the newer memtable's copy
/// wins. Exercises the sealed-memtable ordering inside the range source
/// assembly (newest sealed first), which the get path already honors.
#[test]
fn tree_range_breaks_ties_between_flushed_generations() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert(b"k", b"gen0", 7);
    tree.flush_active_memtable(0)?;
    tree.insert(b"k", b"gen1", 7);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        tree.get(b"k", SeqNo::MAX)?.as_deref(),
        Some(b"gen1".as_slice()),
        "the point read resolves the tie to the newer table",
    );
    let forward: Vec<Vec<u8>> = tree
        .iter(SeqNo::MAX, None)
        .map(|guard| guard.into_inner().expect("entry").1.to_vec())
        .collect();
    assert_eq!(
        forward,
        vec![b"gen1".to_vec()],
        "the range read agrees across flushed generations",
    );
    Ok(())
}
