// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A `clear` landing between a flush or a compaction writing its outputs and
//! installing them: the outputs were written from what `clear` removed, so
//! installing them would bring cleared keys back, and a move of tables that
//! are gone has nothing to move.

use crate::compaction::{CompactionStrategy, MoveDown};
use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;

const KEYS: usize = 200;

fn open(path: &std::path::Path, blob: bool) -> crate::Result<AnyTree> {
    let config = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    let config = if blob {
        config.with_kv_separation(Some(
            crate::KvSeparationOptions::default().separation_threshold(1),
        ))
    } else {
        config
    };
    config.open()
}

fn write_keys(tree: &AnyTree, seqno: SeqNo) {
    for (i, seqno) in (0..KEYS).zip(seqno..) {
        tree.insert(format!("key{i:04}"), vec![b'v'; 64], seqno);
    }
}

/// Clears `tree` once, right before the next output install.
fn clear_before_install(tree: &AnyTree) {
    let target = tree.clone();
    tree.tree_config()
        .arm_before_output_install(move || target.clear().expect("clear"));
}

/// The files left in `folder` under `path`, absent counting as none. The
/// empty files the tables folder keeps for tables to come are no tables.
fn files_in(path: &std::path::Path, folder: &str) -> usize {
    let pooled = folder == crate::file::TABLES_FOLDER;
    std::fs::read_dir(path.join(folder)).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .filter(|entry| !pooled || entry.metadata().is_ok_and(|meta| meta.len() > 0))
            .count()
    })
}

/// The tree reads nothing, holds no table, leaves no table or blob file once
/// dropped, still reads nothing after a reopen, and takes writes, flushes and
/// compactions again.
fn assert_cleared(path: &std::path::Path, tree: AnyTree, blob: bool) -> crate::Result<()> {
    assert_eq!(
        tree.iter(SeqNo::MAX, None).count(),
        0,
        "cleared keys came back"
    );
    assert_eq!(tree.table_count(), 0);
    drop(tree);
    assert_eq!(
        files_in(path, crate::file::TABLES_FOLDER),
        0,
        "a table file stayed"
    );
    assert_eq!(
        files_in(path, crate::file::BLOBS_FOLDER),
        0,
        "a blob file stayed"
    );

    let tree = open(path, blob)?;
    assert_eq!(
        tree.iter(SeqNo::MAX, None).count(),
        0,
        "cleared keys came back on reopen"
    );
    write_keys(&tree, 10_000);
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(tree.iter(SeqNo::MAX, None).count(), KEYS);
    Ok(())
}

/// A flush whose sealed memtables `clear` dropped installs nothing.
#[test]
fn a_flush_after_a_clear_installs_nothing() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = open(folder.path(), blob)?;
        write_keys(&tree, 1);
        clear_before_install(&tree);
        tree.flush_active_memtable(0)?;
        assert_cleared(folder.path(), tree, blob)?;
    }
    Ok(())
}

/// A merge whose input tables `clear` dropped installs nothing.
#[test]
fn a_merge_after_a_clear_installs_nothing() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = open(folder.path(), blob)?;
        write_keys(&tree, 1);
        tree.flush_active_memtable(0)?;
        write_keys(&tree, 1_000);
        tree.flush_active_memtable(0)?;
        clear_before_install(&tree);
        tree.major_compact(u64::MAX, 0)?;
        assert_cleared(folder.path(), tree, blob)?;
    }
    Ok(())
}

/// A move whose tables `clear` dropped moves nothing and does not panic.
#[test]
fn a_move_after_a_clear_moves_nothing() -> crate::Result<()> {
    for blob in [false, true] {
        let folder = tempfile::tempdir()?;
        let tree = open(folder.path(), blob)?;
        write_keys(&tree, 1);
        tree.flush_active_memtable(0)?;
        clear_before_install(&tree);
        let strategy: Arc<dyn CompactionStrategy> = Arc::new(MoveDown(0, 1));
        tree.compact(strategy, 0)?;
        assert_cleared(folder.path(), tree, blob)?;
    }
    Ok(())
}
