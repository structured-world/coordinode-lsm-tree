// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! On-disk edit log for the incremental manifest.
//!
//! A manifest generation is a full snapshot (`v{N}`) plus this append-only log
//! of [`VersionEdit`] records written after it. Each flush / compaction appends
//! one framed edit and fsyncs, so the structural change is durable before the
//! operation is acknowledged upward (the engine has no WAL — durability of data
//! lives a layer above, but the manifest is the crash anchor for the LSM's own
//! structure). On recovery the snapshot is loaded and the log replayed; a
//! power-loss-truncated trailing record is dropped (see [`replay_log`]).
//!
//! Rotation (writing a fresh snapshot and starting a new log) is driven by
//! [`log_size`] exceeding a threshold; the snapshot switch is done atomically
//! via the `CURRENT` pointer (see the recovery / persist layers).

use super::edit::{VersionEdit, replay_edits};
use crate::fs::{Fs, FsOpenOptions, SyncMode};
use std::io::{Seek, SeekFrom};
use std::path::Path;

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
        .map_err(crate::Error::Io)?;
    edit.append_to(&mut file, scratch)?;
    file.sync_all_with(sync_mode).map_err(crate::Error::Io)?;
    Ok(())
}

/// Replays the durable prefix of the log at `path`. An absent log is an empty
/// edit list (a snapshot with no edits yet). Replay stops at the first
/// torn / bad-checksum trailing record (see [`replay_edits`]).
///
/// # Errors
///
/// Returns an I/O error if the open (other than not-found) or a read fails, or
/// [`crate::Error::InvalidHeader`] if a checksum-valid record fails to decode.
pub fn replay_log(fs: &dyn Fs, path: &Path) -> crate::Result<Vec<VersionEdit>> {
    match fs.open(path, &FsOpenOptions::new().read(true)) {
        Ok(mut file) => replay_edits(&mut file),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(crate::Error::Io(e)),
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
        Ok(mut file) => file.seek(SeekFrom::End(0)).map_err(crate::Error::Io),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(crate::Error::Io(e)),
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests {
    use super::super::edit::{ChangedLevel, TableDesc, VersionEdit};
    use super::*;
    use crate::fs::StdFs;

    fn edit(id: u64) -> VersionEdit {
        VersionEdit {
            new_version_id: id,
            changed_levels: vec![ChangedLevel {
                level: 0,
                runs: vec![vec![TableDesc {
                    id,
                    checksum: u128::from(id) * 7,
                    global_seqno: id * 10,
                }]],
            }],
            ..Default::default()
        }
    }

    #[test]
    fn append_then_replay_roundtrips_all_edits() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("edits-0");
        let mut scratch = Vec::new();
        let edits: Vec<VersionEdit> = (1..=4).map(edit).collect();
        for e in &edits {
            append_edit(&StdFs, &path, e, &mut scratch, SyncMode::Normal).expect("append");
        }
        let replayed = replay_log(&StdFs, &path).expect("replay");
        assert_eq!(replayed, edits, "append+replay must round-trip in order");
    }

    #[test]
    fn replay_absent_log_is_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("edits-missing");
        assert!(replay_log(&StdFs, &path).expect("replay").is_empty());
        assert_eq!(log_size(&StdFs, &path).expect("size"), 0);
    }

    #[test]
    fn torn_tail_record_is_dropped_on_replay() {
        // Append three edits, then truncate the file mid-third record (simulated
        // power loss): replay keeps the first two, drops the torn tail.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("edits-torn");
        let mut scratch = Vec::new();
        for i in 1..=2 {
            append_edit(&StdFs, &path, &edit(i), &mut scratch, SyncMode::Normal).expect("append");
        }
        let clean = log_size(&StdFs, &path).expect("size");
        append_edit(&StdFs, &path, &edit(3), &mut scratch, SyncMode::Normal).expect("append");

        // Truncate to a few bytes past the clean prefix → partial third record.
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open");
        f.set_len(clean + 5).expect("truncate");
        drop(f);

        let replayed = replay_log(&StdFs, &path).expect("replay");
        assert_eq!(
            replayed,
            vec![edit(1), edit(2)],
            "torn tail dropped, clean prefix kept",
        );
    }

    #[test]
    fn log_size_grows_with_appends() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("edits-size");
        let mut scratch = Vec::new();
        let s0 = log_size(&StdFs, &path).expect("size");
        append_edit(&StdFs, &path, &edit(1), &mut scratch, SyncMode::Normal).expect("append");
        let s1 = log_size(&StdFs, &path).expect("size");
        append_edit(&StdFs, &path, &edit(2), &mut scratch, SyncMode::Normal).expect("append");
        let s2 = log_size(&StdFs, &path).expect("size");
        assert_eq!(s0, 0);
        assert!(s1 > s0 && s2 > s1, "log grows with each appended edit");
    }
}
