// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! On-disk edit log for the incremental manifest.
//!
//! A manifest generation is a full snapshot (`v{N}`) plus this append-only log
//! of [`VersionEdit`] records written after it. Each flush / compaction appends
//! one framed edit and fsyncs, so the structural change is durable before the
//! operation is acknowledged upward (the engine has no WAL — durability of data
//! lives a layer above, but the manifest is the crash anchor for the LSM's own
//! structure). On recovery the snapshot is loaded and the log replayed; under
//! tolerant modes a power-loss-truncated trailing record is dropped, while
//! `AbsoluteConsistency` surfaces it for deliberate repair (see [`replay_log`]).
//!
//! Rotation (writing a fresh snapshot and starting a new log) is driven by
//! [`log_size`] exceeding a threshold; the snapshot switch is done atomically
//! via the `CURRENT` pointer (see the recovery / persist layers).

use super::edit::{VersionEdit, replay_edits};
use crate::fs::{Fs, FsOpenOptions, SyncMode};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(not(feature = "std"))]
use crate::io::{Seek, SeekFrom};
use crate::path::Path;
#[cfg(feature = "std")]
use std::io::{Seek, SeekFrom};

/// Appends one framed [`VersionEdit`] to the log at `path` (created on first
/// write) and fsyncs per `sync_mode`, so the edit is durable before the caller
/// acknowledges the flush / compaction. `scratch` is reused for payload
/// assembly across calls (no per-edit heap allocation after warm-up).
///
/// # Errors
///
/// Returns an I/O error if the open, write, or fsync fails, or a framing error
/// if the edit payload exceeds the record cap.
pub fn append_edit(
    fs: &dyn Fs,
    path: &Path,
    edit: &VersionEdit,
    scratch: &mut Vec<u8>,
    sync_mode: SyncMode,
) -> crate::Result<()> {
    let mut file = fs
        .open(
            path,
            &FsOpenOptions::new().write(true).create(true).append(true),
        )
        .map_err(crate::Error::from)?;
    edit.append_to(&mut file, scratch)?;
    file.sync_all_with(sync_mode).map_err(crate::Error::from)?;
    Ok(())
}

/// Replays the durable prefix of the log at `path`. An absent log is an empty
/// edit list (a snapshot with no edits yet).
///
/// `mode` selects the trailing-record policy (see [`replay_edits`]): a clean
/// end-of-log is always tolerated, a writer-incomplete tail is rolled back in
/// every mode except `AbsoluteConsistency`, and a fully-framed corrupt tail is
/// rolled back only under `PointInTimeRecovery` / `SkipAnyCorruptedRecords`.
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
