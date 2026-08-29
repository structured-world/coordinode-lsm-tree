// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! The engine walks its directories by ITS OWN naming grammar. A file whose
//! name matches no shape the engine owns is not engine state: it is never read,
//! never deleted, and never a reason to refuse the store.
//!
//! Without that rule a scanner has to enumerate the foreign names it tolerates,
//! which is unbounded by construction and platform-specific in practice.

use lsm_tree::{
    AbstractTree, Config, KvSeparationOptions, MAX_SEQNO, SequenceNumberCounter, get_tmp_folder,
};
use test_log::test;

/// Names no engine grammar owns: an operator's notes and backups, an editor's
/// swap file, a desktop environment's directory metadata.
const FOREIGN_NAMES: &[&str] = &[
    "backup.tar",
    "notes.txt",
    ".hidden",
    "0.bak",
    "table-0-copy",
    ".DS_Store",
    "._resource-fork",
];

fn open_standard(folder: &std::path::Path) -> lsm_tree::Result<lsm_tree::Tree> {
    match Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?
    {
        lsm_tree::AnyTree::Standard(t) => Ok(t),
        lsm_tree::AnyTree::Blob(_) => panic!("expected Standard tree"),
    }
}

#[test]
fn a_foreign_file_in_tables_neither_blocks_the_open_nor_is_deleted() -> lsm_tree::Result<()> {
    for name in FOREIGN_NAMES {
        let folder = get_tmp_folder();
        {
            let tree = open_standard(folder.path())?;
            tree.insert("a", "value", 0);
            tree.flush_active_memtable(0)?;
        }

        let foreign = folder.path().join("tables").join(name);
        std::fs::write(&foreign, b"not ours")?;

        let tree = open_standard(folder.path())?;
        assert_eq!(
            tree.get("a", MAX_SEQNO)?.as_deref(),
            Some(&b"value"[..]),
            "a file the engine does not own must not affect the open: {name}",
        );
        assert!(
            foreign.try_exists()?,
            "the engine must never delete a file it does not own: {name}",
        );
    }
    Ok(())
}

#[test]
fn a_foreign_file_in_blobs_neither_blocks_the_open_nor_is_deleted() -> lsm_tree::Result<()> {
    for name in FOREIGN_NAMES {
        let folder = get_tmp_folder();
        let config = || {
            Config::new(
                folder.path(),
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .with_kv_separation(Some(
                KvSeparationOptions::default().separation_threshold(16),
            ))
        };
        {
            let lsm_tree::AnyTree::Blob(tree) = config().open()? else {
                panic!("expected Blob tree");
            };
            tree.insert("a", vec![b'v'; 64], 0);
            tree.flush_active_memtable(0)?;
        }

        let foreign = folder.path().join("blobs").join(name);
        std::fs::write(&foreign, b"not ours")?;

        let lsm_tree::AnyTree::Blob(tree) = config().open()? else {
            panic!("expected Blob tree");
        };
        assert_eq!(
            tree.get("a", MAX_SEQNO)?.as_deref(),
            Some(&[b'v'; 64][..]),
            "a file the engine does not own must not affect the open: {name}",
        );
        assert!(
            foreign.try_exists()?,
            "the engine must never delete a file it does not own: {name}",
        );
    }
    Ok(())
}

/// Repair rebuilds the manifest from what the engine owns. A foreign file is
/// not part of that inventory, so it is neither consulted nor removed: a repair
/// that deletes an operator's file to make the tree openable is destroying data
/// to fix a problem it invented.
#[test]
fn repair_leaves_foreign_files_alone() -> lsm_tree::Result<()> {
    for name in FOREIGN_NAMES {
        let folder = get_tmp_folder();
        {
            let tree = open_standard(folder.path())?;
            tree.insert("a", "value", 0);
            tree.flush_active_memtable(0)?;
        }

        let foreign = folder.path().join("tables").join(name);
        std::fs::write(&foreign, b"not ours")?;
        for entry in std::fs::read_dir(folder.path())? {
            let entry = entry?;
            let entry_name = entry.file_name();
            let entry_name = entry_name.to_string_lossy();
            let is_version = entry_name
                .strip_prefix('v')
                .is_some_and(|rest| rest.parse::<u64>().is_ok());
            if is_version || entry_name == "current" {
                std::fs::remove_file(entry.path())?;
            }
        }

        Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .repair()?;

        assert!(
            foreign.try_exists()?,
            "repair must never delete a file the engine does not own: {name}",
        );
        let tree = open_standard(folder.path())?;
        assert_eq!(
            tree.get("a", MAX_SEQNO)?.as_deref(),
            Some(&b"value"[..]),
            "the rebuilt tree still serves its data: {name}",
        );
    }
    Ok(())
}
