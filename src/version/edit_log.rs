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
/// edit list (a snapshot with no edits yet).
///
/// `strict` mirrors [`ManifestRecoveryMode::AbsoluteConsistency`](crate::config::ManifestRecoveryMode::AbsoluteConsistency):
/// when `true`, an incomplete or corrupt trailing record is surfaced as
/// [`crate::Error::TornManifestEditLog`] instead of being rolled back. A clean
/// end-of-log is always tolerated (see [`replay_edits`]).
///
/// # Errors
///
/// Returns an I/O error if the open (other than not-found) or a read fails,
/// [`crate::Error::InvalidHeader`] if a checksum-valid record fails to decode,
/// or [`crate::Error::TornManifestEditLog`] when `strict` and the trailing
/// record is torn / bit-rotted / mis-framed.
pub fn replay_log(fs: &dyn Fs, path: &Path, strict: bool) -> crate::Result<Vec<VersionEdit>> {
    match fs.open(path, &FsOpenOptions::new().read(true)) {
        Ok(mut file) => replay_edits(&mut file, strict),
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
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
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
        let replayed = replay_log(&StdFs, &path, false).expect("replay");
        assert_eq!(replayed, edits, "append+replay must round-trip in order");
    }

    #[test]
    fn replay_absent_log_is_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("edits-missing");
        assert!(replay_log(&StdFs, &path, false).expect("replay").is_empty());
        assert_eq!(log_size(&StdFs, &path).expect("size"), 0);
    }

    /// Appends `count` clean edits, then truncates the file to `clean + 5`
    /// bytes past the durable prefix so the trailing record is partial
    /// (a power-loss-interrupted append). Returns the path.
    fn log_with_torn_tail(dir: &std::path::Path, count: u64) -> std::path::PathBuf {
        let path = dir.join("edits-torn");
        let mut scratch = Vec::new();
        for i in 1..=count {
            append_edit(&StdFs, &path, &edit(i), &mut scratch, SyncMode::Normal).expect("append");
        }
        let clean = log_size(&StdFs, &path).expect("size");
        append_edit(
            &StdFs,
            &path,
            &edit(count + 1),
            &mut scratch,
            SyncMode::Normal,
        )
        .expect("append");
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open");
        f.set_len(clean + 5).expect("truncate");
        drop(f);
        path
    }

    #[test]
    fn torn_tail_record_is_dropped_on_replay() {
        // Lenient mode: a partial trailing record (simulated power loss mid-append)
        // is dropped, keeping the durable prefix.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = log_with_torn_tail(dir.path(), 2);
        let replayed = replay_log(&StdFs, &path, false).expect("replay");
        assert_eq!(
            replayed,
            vec![edit(1), edit(2)],
            "torn tail dropped, clean prefix kept",
        );
    }

    #[test]
    fn torn_tail_record_aborts_under_strict() {
        // AbsoluteConsistency (strict): a partial trailing record is NOT silently
        // rolled back — replay surfaces TornManifestEditLog so an operator must
        // truncate the tail (Tree::repair) before the tree opens.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = log_with_torn_tail(dir.path(), 2);
        let err = replay_log(&StdFs, &path, true).expect_err("strict must reject torn tail");
        assert!(
            matches!(err, crate::Error::TornManifestEditLog { kind: "truncated" }),
            "expected TornManifestEditLog(truncated), got {err:?}",
        );
    }

    #[test]
    fn clean_log_replays_under_strict() {
        // A pristine log (every record fully fsynced) ends in a clean record
        // boundary, which the trailing read sees as EOF — byte-identical to a
        // crash exactly at a boundary. Strict mode MUST tolerate this, else every
        // healthy open would fail under the default AbsoluteConsistency mode.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("edits-clean");
        let mut scratch = Vec::new();
        let edits: Vec<VersionEdit> = (1..=3).map(edit).collect();
        for e in &edits {
            append_edit(&StdFs, &path, e, &mut scratch, SyncMode::Normal).expect("append");
        }
        let replayed = replay_log(&StdFs, &path, true).expect("strict must accept a clean log");
        assert_eq!(replayed, edits, "clean log replays fully under strict");
    }

    #[test]
    fn checksum_mismatch_tail_aborts_under_strict() {
        // A fully-framed trailing record whose payload bit-rotted (length and
        // digest intact, bytes flipped) is corruption, not an unacknowledged
        // write. Strict mode rejects it; lenient mode drops it.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("edits-bitrot");
        let mut scratch = Vec::new();
        for i in 1..=3 {
            append_edit(&StdFs, &path, &edit(i), &mut scratch, SyncMode::Normal).expect("append");
        }
        // Flip the last payload byte of the final (fully written) record.
        let mut bytes = std::fs::read(&path).expect("read");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, &bytes).expect("write");

        let err = replay_log(&StdFs, &path, true).expect_err("strict must reject bit-rot");
        assert!(
            matches!(
                err,
                crate::Error::TornManifestEditLog {
                    kind: "checksum-mismatch"
                }
            ),
            "expected TornManifestEditLog(checksum-mismatch), got {err:?}",
        );

        let replayed = replay_log(&StdFs, &path, false).expect("lenient drops bit-rotted tail");
        assert_eq!(
            replayed,
            vec![edit(1), edit(2)],
            "lenient: bit-rotted tail dropped, prefix kept",
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
