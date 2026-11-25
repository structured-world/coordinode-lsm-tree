// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The mark the install of a replacing ingestion leaves beside each table it
//! kept.
//!
//! A replacing ingestion writes its tables before the install that keeps them,
//! so a table cannot say whether it was installed, nor where in the tree: both
//! are the install's, and the manifest records them. A repair that rebuilds a
//! lost manifest reads them here instead.
//!
//! The mark is an empty file, `{table id}.replaced-{bound}`, made once the
//! manifest naming the tables is durable: its presence proves the install, and
//! a table of a replacing ingestion without one was never installed. `bound`
//! is the recency the install gave its run, taken from the tree's counter while
//! no flush was under way, so every table whose recency lies below it was in
//! the tree before the install, which dropped it. A repair without a manifest
//! leaves those out: one of them still on the disk is a file whose removal the
//! install never finished.
//!
//! Nothing is read from the file: the name holds all of it, so making the mark
//! durable is one sync of its folder, whatever the number of tables.

use crate::fs::{Fs, FsOpenOptions, SyncMode};
use crate::path::{Path, PathBuf};
use crate::{Table, TableId};
use alloc::format;
use alloc::sync::Arc;
use alloc::vec::Vec;

/// Where the mark of the table at `table_path`, of id `id`, lies for an
/// install of recency `bound`.
#[must_use]
pub(crate) fn mark_path(table_path: &Path, id: TableId, bound: TableId) -> PathBuf {
    let name = format!("{id}{}{bound}", crate::file::REPLACE_MARK_INFIX);
    match table_path.parent() {
        Some(folder) => folder.join(name),
        None => PathBuf::from(name),
    }
}

/// Makes the mark of one table, without syncing its folder.
fn make(fs: &dyn Fs, table_path: &Path, id: TableId, bound: TableId) -> crate::io::Result<()> {
    fs.open(
        &mark_path(table_path, id, bound),
        &FsOpenOptions::new()
            .write(true)
            .create(true)
            .write_origin(crate::fs::WriteOrigin::Metadata),
    )
    .map(drop)
}

/// Marks `tables` as kept by a replacing install of recency `bound`, durably:
/// each folder they lie in is synced once.
///
/// Called only once the manifest naming `tables` is durable.
///
/// # Errors
///
/// A mark that cannot be made, or a folder that does not sync.
pub(crate) fn publish(tables: &[Table], bound: TableId, sync_mode: SyncMode) -> crate::Result<()> {
    let mut folders: Vec<(&Arc<dyn Fs>, &Path)> = Vec::new();
    for table in tables {
        make(&*table.fs, &table.path, table.id(), bound)?;
        if let Some(folder) = table.path.parent()
            && !folders
                .iter()
                .any(|(fs, seen)| Arc::ptr_eq(fs, &table.fs) && *seen == folder)
        {
            folders.push((&table.fs, folder));
        }
    }
    for (fs, folder) in folders {
        fs.sync_directory_with(folder, sync_mode)?;
    }
    Ok(())
}

/// Marks the one table at `table_path`, durably.
///
/// # Errors
///
/// The mark cannot be made, or its folder does not sync.
pub(crate) fn publish_one(
    fs: &dyn Fs,
    table_path: &Path,
    id: TableId,
    bound: TableId,
    sync_mode: SyncMode,
) -> crate::Result<()> {
    make(fs, table_path, id, bound)?;
    if let Some(folder) = table_path.parent() {
        fs.sync_directory_with(folder, sync_mode)?;
    }
    Ok(())
}

/// Removes the mark of a table that is going, best-effort: one left behind
/// names a table no manifest holds, and the next open sweeps it.
pub(crate) fn remove(fs: &dyn Fs, table_path: &Path, id: TableId, bound: TableId) {
    if let Err(e) = fs.remove_file(&mark_path(table_path, id, bound))
        && e.kind() != crate::io::ErrorKind::NotFound
    {
        log::debug!(
            "the replace mark of table {id} stays until the next open ({e}): {}",
            table_path.display(),
        );
    }
}
