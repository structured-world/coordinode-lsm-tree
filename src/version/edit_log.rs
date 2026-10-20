// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! On-disk edit log for the incremental manifest.
//!
//! A manifest generation is a full snapshot (`v{N}`) plus this append-only log
//! of [`VersionEdit`] records written after it. Each flush / compaction appends
//! one framed edit and fsyncs, so the structural change is durable before the
//! operation is acknowledged upward (the engine has no WAL — durability of data
//! lives a layer above, but the manifest is the crash anchor for the LSM's own
//! structure). On recovery the snapshot is loaded and the log replayed; under
//! `TolerateCorruptedTailRecords` a power-loss-truncated trailing record is
//! dropped, while
//! `AbsoluteConsistency` surfaces it for deliberate repair (see [`replay_log`]).
//!
//! Rotation (writing a fresh snapshot and starting a new log) is driven by
//! [`log_size`] exceeding a threshold. Recovery takes the highest `v{N}` and
//! replays its `edits-{N}`; a log that is not there is empty (see the
//! recovery / persist layers).

use super::edit::{VersionEdit, replay_edits};
use crate::fs::{Fs, FsOpenOptions, SyncMode};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(not(feature = "std"))]
use crate::io::{Seek, SeekFrom};
use crate::path::Path;
#[cfg(feature = "std")]
use std::io::{Seek, SeekFrom};

/// The edit log of the snapshot `v{snapshot_id}` in the tree folder `folder`.
#[must_use]
pub fn log_path(folder: &Path, snapshot_id: super::VersionId) -> crate::path::PathBuf {
    folder.join(alloc::format!("edits-{snapshot_id}"))
}

/// Creates the empty edit log of the snapshot `v{snapshot_id}`, as the
/// snapshot is written: the directory sync that makes the snapshot's name
/// durable then makes the log's too, and no append has to sync it. A log
/// left under the name is emptied, since none of its edits belongs to the
/// snapshot being written, and synced empty: the directory sync makes only
/// its name durable, not its new size. Its length tells nothing here, as an
/// earlier emptying whose sync failed leaves it reading empty while its
/// edits are still what a power loss keeps.
///
/// # Errors
///
/// Returns an I/O error if the log cannot be created or emptied.
pub fn create_log(
    fs: &dyn Fs,
    folder: &Path,
    snapshot_id: super::VersionId,
    sync_mode: SyncMode,
) -> crate::Result<()> {
    let path = log_path(folder, snapshot_id);
    let existed = match fs.metadata(&path) {
        Ok(_) => true,
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => false,
        Err(e) => return Err(e.into()),
    };
    let file = fs.open(
        &path,
        &FsOpenOptions::new().write(true).create(true).truncate(true),
    )?;
    if existed {
        file.sync_data_with(sync_mode)?;
    }
    Ok(())
}

/// Appends one framed [`VersionEdit`] to the log at `path` and syncs its data
/// per `sync_mode`, so the edit is durable before the caller acknowledges the
/// flush / compaction. `scratch` is reused for payload assembly across calls
/// (no per-edit heap allocation after warm-up).
///
/// The log is the one the install of its snapshot created, its name durable
/// under that install's directory sync; a log that is not there fails the
/// append rather than be made again under a name no sync covers.
///
/// Returns the appended record's on-disk size in bytes (framing header +
/// payload), so the caller can keep its cached log size exact without a
/// re-measuring `open` + `seek` per install. Returns `None`, having written
/// nothing, when the edit does not fit one record: an edit names every table
/// of each level it changes, so a level of a few thousand tables outgrows the
/// record cap, and the caller has to record that transition in a snapshot,
/// which frames each table on its own.
///
/// # Errors
///
/// Returns an I/O error if the open or the write fails (`NotFound` for a log
/// that is not there), an encoding error from [`VersionEdit::encode`], and
/// [`crate::Error::InstallOutcomeUnknown`] when the record's sync fails.
pub fn append_edit(
    fs: &dyn Fs,
    path: &Path,
    edit: &VersionEdit,
    scratch: &mut Vec<u8>,
    sync_mode: SyncMode,
) -> crate::Result<Option<u64>> {
    edit.encode(scratch)?;
    if scratch.len() > super::framing::MAX_FRAME_PAYLOAD as usize {
        return Ok(None);
    }
    let mut file = fs
        .open(path, &FsOpenOptions::new().write(true).append(true))
        .map_err(crate::Error::from)?;
    // A write that fails leaves the record short of its last byte, a torn
    // tail replay drops: the edit is not installed.
    super::framing::write_frame(&mut file, scratch)?;
    // `sync_data` persists the file size with the data (its contract, POSIX
    // `fdatasync`), which is all replay reads; the log's name is durable
    // since its install. A sync that fails leaves the whole record written,
    // which a power loss may keep: the edit's outcome is unknown.
    file.sync_data_with(sync_mode).map_err(|e| {
        crate::Error::InstallOutcomeUnknown(alloc::boxed::Box::new(crate::Error::from(e)))
    })?;
    // The framing header (u32 len + u64 XXH3) precedes the payload on disk.
    let appended = u64::try_from(super::framing::FRAME_HEADER_LEN + scratch.len())
        .map_err(|_| crate::Error::Unrecoverable)?;
    Ok(Some(appended))
}

/// Replays the durable prefix of the log at `path`. An absent log is an empty
/// edit list (a snapshot with no edits yet).
///
/// `mode` selects the trailing-record policy (see [`replay_edits`]): a clean
/// end-of-log is always tolerated, a writer-incomplete tail is rolled back
/// under `TolerateCorruptedTailRecords`, and a fully-framed corrupt record
/// fails the replay in every mode.
///
/// # Errors
///
/// Returns an I/O error if the open (other than not-found) or a read fails,
/// [`crate::Error::InvalidHeader`] if a checksum-valid record fails to decode,
/// or [`crate::Error::TornManifestEditLog`] when the trailing record is
/// torn / bit-rotted / mis-framed and `mode` does not tolerate that defect.
pub fn replay_log(
    fs: &dyn Fs,
    path: &Path,
    mode: crate::config::ManifestRecoveryMode,
) -> crate::Result<Vec<VersionEdit>> {
    match fs.open(path, &FsOpenOptions::new().read(true)) {
        Ok(mut file) => replay_edits(&mut file, mode),
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(crate::Error::from(e)),
    }
}

/// Current size of the log at `path` in bytes (`0` if absent). Drives snapshot
/// rotation: once the log grows past the configured threshold, the next persist
/// writes a fresh snapshot and starts a new (empty) log.
///
/// # Errors
///
/// Returns an I/O error if the open (other than not-found) or the seek fails.
pub fn log_size(fs: &dyn Fs, path: &Path) -> crate::Result<u64> {
    match fs.open(path, &FsOpenOptions::new().read(true)) {
        Ok(mut file) => file.seek(SeekFrom::End(0)).map_err(crate::Error::from),
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(crate::Error::from(e)),
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
mod tests;
