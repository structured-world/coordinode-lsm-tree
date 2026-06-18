// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Crash-resume journal for tight-space incremental-reclaim compaction.
//!
//! Tight-space compaction (issue #487) processes a sorted run one input at a
//! time: it merges an input into one or more finished output SSTs, fsyncs them,
//! records progress here, and only then punches the consumed input's extents
//! (`Fs::punch_hole`) to reclaim space mid-compaction. The invariant is that an
//! input is freed only after its data is durable in finished output SSTs AND
//! that fact is journaled, so a crash at any point is recoverable: the journaled
//! outputs cover the consumed-input prefix, the un-consumed inputs are intact,
//! and the two together form a valid run (the run is key-sorted and
//! non-overlapping, so outputs and remaining inputs never overlap in keys).
//!
//! Each checkpoint is a full framed record (the latest supersedes the rest), so
//! recovery reads the durable prefix and takes the last intact record. The
//! framing primitive is shared with the manifest edit-log
//! ([`crate::version::framing`]) and carries the same torn-tail / bit-rot
//! detection, so a power-loss-truncated trailing checkpoint is dropped back to
//! the previous durable one.

use crate::TableId;
use crate::fs::{Fs, FsOpenOptions, SyncMode};
use crate::io::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crate::path::Path;
use crate::version::framing::{self, FramedRecordOutcome};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// A durable checkpoint of a tight-space compaction's forward progress.
///
/// Records how many inputs of the run (in run order) are fully merged into the
/// finished output SSTs named here. The next checkpoint supersedes this one with
/// a higher [`consumed_inputs`](Self::consumed_inputs).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct CompactionProgress {
    /// Number of inputs (counted in run order from the front) whose data is now
    /// fully durable in [`output_table_ids`](Self::output_table_ids). These
    /// inputs may have been punched and must be dropped on commit / recovery.
    pub consumed_inputs: u32,
    /// Ids of the finished, fsynced output SSTs covering the consumed-input
    /// prefix. On recovery these replace the consumed inputs in the version.
    pub output_table_ids: Vec<TableId>,
}

impl CompactionProgress {
    /// Encodes the record payload: `consumed_inputs` then a length-prefixed list
    /// of output table ids, all little-endian.
    fn encode(&self, out: &mut Vec<u8>) -> crate::Result<()> {
        out.write_u32::<LittleEndian>(self.consumed_inputs)?;
        let count =
            u32::try_from(self.output_table_ids.len()).map_err(|_| crate::Error::Unrecoverable)?;
        out.write_u32::<LittleEndian>(count)?;
        for &id in &self.output_table_ids {
            out.write_u64::<LittleEndian>(id)?;
        }
        Ok(())
    }

    /// Decodes a record payload produced by [`encode`](Self::encode).
    fn decode(mut payload: &[u8]) -> crate::Result<Self> {
        let consumed_inputs = payload.read_u32::<LittleEndian>()?;
        let count = payload.read_u32::<LittleEndian>()? as usize;
        let mut output_table_ids = Vec::with_capacity(count);
        for _ in 0..count {
            output_table_ids.push(payload.read_u64::<LittleEndian>()?);
        }
        Ok(Self {
            consumed_inputs,
            output_table_ids,
        })
    }
}

/// Appends one progress checkpoint to the journal at `path` (created on first
/// write) and fsyncs per `sync_mode`, so the checkpoint is durable before the
/// caller punches the just-consumed input.
///
/// # Errors
///
/// Returns an I/O error if the open, write, or fsync fails.
pub(crate) fn append_progress(
    fs: &dyn Fs,
    path: &Path,
    progress: &CompactionProgress,
    scratch: &mut Vec<u8>,
    sync_mode: SyncMode,
) -> crate::Result<()> {
    let mut file = fs.open(
        path,
        &FsOpenOptions::new().write(true).create(true).append(true),
    )?;
    framing::write_framed_record(&mut file, scratch, |payload| progress.encode(payload))?;
    file.sync_all_with(sync_mode)?;
    Ok(())
}

/// Reads the latest durable checkpoint from the journal at `path`, or `None` if
/// the journal is absent or holds no intact record (a crash before the first
/// fsynced checkpoint).
///
/// A torn / bit-rotted trailing record (power loss mid-append) is dropped and
/// the previous intact checkpoint is returned, mirroring the manifest edit-log's
/// tail-tolerant replay.
///
/// # Errors
///
/// Returns an I/O error if the open (other than not-found) or a read fails, or a
/// decode error if an intact-framed record's payload is malformed.
pub(crate) fn read_latest(fs: &dyn Fs, path: &Path) -> crate::Result<Option<CompactionProgress>> {
    let mut file = match fs.open(path, &FsOpenOptions::new().read(true)) {
        Ok(f) => f,
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut latest = None;
    let mut scratch = Vec::new();
    loop {
        match framing::read_framed_record(&mut file, u64::MAX, None, &mut scratch)? {
            FramedRecordOutcome::Ok => latest = Some(CompactionProgress::decode(&scratch)?),
            // A clean boundary (EOF) or a power-loss-truncated / corrupt trailing
            // record both end the durable prefix: keep the last intact checkpoint.
            FramedRecordOutcome::TailTruncation
            | FramedRecordOutcome::ChecksumMismatch { .. }
            | FramedRecordOutcome::BadHeader
            | FramedRecordOutcome::LenMismatch { .. } => break,
        }
    }
    Ok(latest)
}

/// Removes the journal at `path`. Called on successful commit (the compaction's
/// output is installed, the journal is no longer needed) and is a no-op if the
/// journal was never created.
///
/// # Errors
///
/// Returns an I/O error if removal fails for a reason other than not-found.
pub(crate) fn discard(fs: &dyn Fs, path: &Path) -> crate::Result<()> {
    match fs.remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::MemFs;
    use test_log::test;

    fn progress(consumed: u32, ids: &[TableId]) -> CompactionProgress {
        CompactionProgress {
            consumed_inputs: consumed,
            output_table_ids: ids.to_vec(),
        }
    }

    #[test]
    fn read_latest_returns_none_for_absent_journal() {
        let fs = MemFs::new();
        assert_eq!(read_latest(&fs, Path::new("/j")).unwrap(), None);
    }

    #[test]
    fn append_then_read_latest_returns_the_last_checkpoint() {
        let fs = MemFs::new();
        let path = Path::new("/j");
        let mut scratch = Vec::new();
        for p in [
            progress(1, &[10]),
            progress(2, &[10, 11]),
            progress(3, &[10, 11, 12]),
        ] {
            append_progress(&fs, path, &p, &mut scratch, SyncMode::Normal).unwrap();
        }
        assert_eq!(
            read_latest(&fs, path).unwrap(),
            Some(progress(3, &[10, 11, 12])),
            "the latest checkpoint supersedes earlier ones",
        );
    }

    #[test]
    fn read_latest_drops_a_torn_trailing_checkpoint() {
        let fs = MemFs::new();
        let path = Path::new("/j");
        let mut scratch = Vec::new();
        append_progress(
            &fs,
            path,
            &progress(1, &[10]),
            &mut scratch,
            SyncMode::Normal,
        )
        .unwrap();
        append_progress(
            &fs,
            path,
            &progress(2, &[10, 11]),
            &mut scratch,
            SyncMode::Normal,
        )
        .unwrap();
        // Simulate a power-loss-truncated third append: copy the file, append a
        // partial framed record (just a length prefix, no payload), write it back.
        let clean = {
            let mut f = fs.open(path, &FsOpenOptions::new().read(true)).unwrap();
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut f, &mut buf).unwrap();
            buf
        };
        let mut torn = clean.clone();
        torn.extend_from_slice(&[0xFF, 0xFF]); // partial 4-byte length header
        {
            let mut f = fs
                .open(
                    path,
                    &FsOpenOptions::new().write(true).truncate(true).create(true),
                )
                .unwrap();
            std::io::Write::write_all(&mut f, &torn).unwrap();
        }
        assert_eq!(
            read_latest(&fs, path).unwrap(),
            Some(progress(2, &[10, 11])),
            "torn trailing checkpoint dropped, previous durable one kept",
        );
    }

    #[test]
    fn discard_removes_the_journal_and_is_idempotent() {
        let fs = MemFs::new();
        let path = Path::new("/j");
        let mut scratch = Vec::new();
        append_progress(
            &fs,
            path,
            &progress(1, &[10]),
            &mut scratch,
            SyncMode::Normal,
        )
        .unwrap();
        discard(&fs, path).unwrap();
        assert_eq!(read_latest(&fs, path).unwrap(), None);
        discard(&fs, path).unwrap(); // idempotent: absent journal is fine
    }
}
