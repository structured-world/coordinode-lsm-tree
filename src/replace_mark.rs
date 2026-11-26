// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The mark the install of a replacing ingestion leaves in the tables folder.
//!
//! A replacing ingestion writes its tables before the install that keeps them,
//! so a table cannot say whether it was installed, nor where in the tree: both
//! are the install's, and the manifest records them. A repair that rebuilds a
//! lost manifest reads them here instead.
//!
//! The mark is one empty file per install, `{run}.replaced-{bound}`, made once
//! the manifest naming the install is durable. `run` is the id the ingestion
//! took when it started and wrote into each of its tables
//! (`descriptor#replacing`), so the mark proves the install of every table of
//! that ingestion at once, and a table of a replacing ingestion whose run has
//! no mark was never installed. One name in one folder is there or is not:
//! no crash leaves the install proven for some of its tables only.
//!
//! `bound` is the recency the install gave its run, taken from the tree's
//! counter while no flush was under way, so every table whose recency lies
//! below it was in the tree before the install, which dropped it. A repair
//! without a manifest leaves those out: one of them still on the disk is a
//! file whose removal the install never finished. An install that kept no
//! table (the ingestion wrote none) leaves its mark for that bound alone.
//!
//! The mark stays while the tree is open, past the last table of its run:
//! the bound holds for as long as a file the install dropped can still lie on
//! the disk. An open removes those files, and with them every mark whose run
//! the manifest no longer names.
//!
//! Nothing is read from the file: the name holds all of it, so making the mark
//! durable is one sync of its folder.

use crate::TableId;
use crate::fs::{Fs, FsOpenOptions, SyncMode};
use crate::path::{Path, PathBuf};
use alloc::format;

/// Where the mark of the replacing ingestion `run`, installed at recency
/// `bound`, lies in the tables folder `folder`.
#[must_use]
pub(crate) fn mark_path(folder: &Path, run: TableId, bound: TableId) -> PathBuf {
    folder.join(format!("{run}{}{bound}", crate::file::REPLACE_MARK_INFIX))
}

/// Leaves the mark of the replacing ingestion `run`, installed at recency
/// `bound`, in `folder`, durably: the mark is made if it is not there, and the
/// folder is synced either way, so a mark an earlier sync failed on becomes
/// durable here.
///
/// Called only once the manifest naming the install is durable.
///
/// # Errors
///
/// The mark cannot be made, or the folder does not sync.
pub(crate) fn publish(
    fs: &dyn Fs,
    folder: &Path,
    run: TableId,
    bound: TableId,
    sync_mode: SyncMode,
) -> crate::Result<()> {
    fs.open(
        &mark_path(folder, run, bound),
        &FsOpenOptions::new()
            .write(true)
            .create(true)
            .write_origin(crate::fs::WriteOrigin::Metadata),
    )
    .map(drop)?;
    fs.sync_directory_with(folder, sync_mode)?;
    Ok(())
}
