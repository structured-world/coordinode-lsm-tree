// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Incremental manifest `VersionEdit`: the delta between two consecutive
//! [`Version`](super::Version)s.
//!
//! Instead of rewriting the whole manifest on every flush / compaction, the
//! engine appends one `VersionEdit` record describing what changed and, on
//! recovery, loads the latest full snapshot then replays the appended edits in
//! order to reconstruct the current version.
//!
//! # Why levels are encoded whole, not per-table
//!
//! A [`Version`](super::Version) lays each level out as a list of *runs*, and
//! each run is an independent comparator-sorted table sequence (L0 keeps one run
//! per flush, overlapping; tiered levels keep several runs). That run grouping
//! is load-bearing — recovery rebuilds it verbatim via
//! [`Version::from_recovery`](super::Version::from_recovery). A flat
//! `added / removed table id` delta would lose which run a table belongs to, so
//! an edit instead carries the **full new run layout of every level it
//! changed** ([`ChangedLevel`]). Table removal is therefore implicit: a table
//! that a compaction drops simply isn't in the new layout of its level.
//!
//! This is not the per-flush waste it looks like: flushes and compactions
//! already rewrite whole runs / levels, so a changed level's layout is exactly
//! what the operation produced. Levels the edit doesn't mention stay as-is.
//!
//! Blob files have no run structure (a flat id-keyed list), so they keep a
//! natural per-id add / remove delta.
//!
//! # Wire format
//!
//! A `VersionEdit` is serialized as the payload of a single
//! [`framing`] record (`len + xxh3_64 + payload`), so a
//! power-loss-truncated or bit-flipped trailing edit is detected and dropped on
//! replay (the torn tail is never applied). The payload is:
//!
//! ```text
//! new_version_id      : u64 LE
//! changed_level_count : u32 LE
//!   repeat (changed level):
//!     level     : u8
//!     run_count : u32 LE
//!       repeat (run):
//!         table_count : u32 LE
//!           repeat (table): id u64 LE | checksum_type u8 | checksum u128 LE | global_seqno u64 LE
//! added_blob_count    : u32 LE
//!   repeat: id u64 LE | checksum_type u8 | checksum u128 LE
//! removed_blob_count  : u32 LE
//!   repeat: id u64 LE
//! gc_stats_len        : u32 LE   (0 = GC stats unchanged from the prior version)
//!   gc_stats bytes    : [u8; gc_stats_len]
//! ```
//!
//! The per-table / per-blob record bodies are byte-identical to the snapshot
//! encoding in [`Version::encode_into`](super::Version::encode_into) (plus the
//! explicit `level`, which the snapshot encodes positionally), so the same
//! record shape is recognised on both paths.

use super::framing;
use crate::UserKey;
use crate::io::{LittleEndian, ReadBytesExt, WriteBytesExt};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(not(feature = "std"))]
use crate::io::{Read, Write};
#[cfg(feature = "std")]
use std::io::{Read, Write};

/// One table in a run: the snapshot's per-table record minus the positional
/// level (the enclosing [`ChangedLevel`] carries the level explicitly).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDesc {
    /// Table id.
    pub id: u64,
    /// XXH3-128 checksum of the table file.
    pub checksum: u128,
    /// Global sequence number stamped on the table.
    pub global_seqno: u64,
}

/// The full new run layout of one LSM level that an edit replaces wholesale.
///
/// `runs` is `[run][table]`: each inner `Vec` is one comparator-sorted run, in
/// the same order the snapshot persists it. An empty `runs` means the edit
/// empties the level (e.g. a compaction drained it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedLevel {
    /// LSM level this layout replaces.
    pub level: u8,
    /// New run layout of the level: `runs[r]` is the tables of run `r`, in
    /// persisted (comparator-sorted) order.
    pub runs: Vec<Vec<TableDesc>>,
}

/// A blob file added by an edit. Mirrors the snapshot's per-blob record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddedBlobFile {
    /// Blob file id.
    pub id: u64,
    /// XXH3-128 checksum of the blob file.
    pub checksum: u128,
}

/// The delta between two consecutive versions: what one flush / compaction
/// changed. Applied in order on top of a snapshot during recovery.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionEdit {
    /// Version id this edit produces (the prior version id + 1).
    pub new_version_id: u64,
    /// Levels this edit changed, each carrying its full new run layout.
    /// Levels not listed are unchanged from the prior version.
    pub changed_levels: Vec<ChangedLevel>,
    /// Blob files this edit adds.
    pub added_blob_files: Vec<AddedBlobFile>,
    /// Blob file ids this edit removes.
    pub removed_blob_file_ids: Vec<u64>,
    /// Encoded GC-stats (`FragmentationMap`) snapshot as of this version, or
    /// `None` when unchanged from the prior version. Opaque bytes at this layer
    /// (encoded / decoded by the version layer that owns the map).
    pub gc_stats: Option<Vec<u8>>,
    /// Per-table key-range lower-bound overrides for tight-space compaction
    /// (`table id → punched-prefix lower bound`). A table listed here has had
    /// its data below the bound punched out and superseded by a freshly merged
    /// output table; recovery rebuilds the restricted view via
    /// [`Table::with_restriction`](crate::Table::with_restriction). Empty on
    /// every edit that did not run tight-space reclaim.
    pub restrictions: Vec<(u64, UserKey)>,
}

/// `checksum_type` byte written for XXH3-128 (matches the snapshot encoder).
const CHECKSUM_TYPE_XXH3: u8 = 0;

impl VersionEdit {
    /// Serializes the edit payload (without the framing header) into `out`.
    fn encode_payload(&self, out: &mut Vec<u8>) -> crate::Result<()> {
        out.write_u64::<LittleEndian>(self.new_version_id)?;

        out.write_u32::<LittleEndian>(u32_len(self.changed_levels.len())?)?;
        for cl in &self.changed_levels {
            out.write_u8(cl.level)?;
            out.write_u32::<LittleEndian>(u32_len(cl.runs.len())?)?;
            for run in &cl.runs {
                out.write_u32::<LittleEndian>(u32_len(run.len())?)?;
                for t in run {
                    out.write_u64::<LittleEndian>(t.id)?;
                    out.write_u8(CHECKSUM_TYPE_XXH3)?;
                    out.write_u128::<LittleEndian>(t.checksum)?;
                    out.write_u64::<LittleEndian>(t.global_seqno)?;
                }
            }
        }

        out.write_u32::<LittleEndian>(u32_len(self.added_blob_files.len())?)?;
        for b in &self.added_blob_files {
            out.write_u64::<LittleEndian>(b.id)?;
            out.write_u8(CHECKSUM_TYPE_XXH3)?;
            out.write_u128::<LittleEndian>(b.checksum)?;
        }

        out.write_u32::<LittleEndian>(u32_len(self.removed_blob_file_ids.len())?)?;
        for &id in &self.removed_blob_file_ids {
            out.write_u64::<LittleEndian>(id)?;
        }

        match &self.gc_stats {
            Some(bytes) => {
                out.write_u32::<LittleEndian>(u32_len(bytes.len())?)?;
                out.write_all(bytes)?;
            }
            None => out.write_u32::<LittleEndian>(0)?,
        }

        out.write_u32::<LittleEndian>(u32_len(self.restrictions.len())?)?;
        for (id, key) in &self.restrictions {
            out.write_u64::<LittleEndian>(*id)?;
            out.write_u32::<LittleEndian>(u32_len(key.len())?)?;
            out.write_all(key)?;
        }
        Ok(())
    }

    /// Appends this edit as one framed record to `writer`, reusing `scratch`
    /// for the payload assembly (no per-edit heap allocation after warm-up).
    ///
    /// # Errors
    ///
    /// Returns an error if the payload exceeds the framing payload cap or a
    /// write fails.
    pub fn append_to<W: Write>(&self, writer: &mut W, scratch: &mut Vec<u8>) -> crate::Result<()> {
        framing::write_framed_record(writer, scratch, |payload| self.encode_payload(payload))
    }

    /// Decodes a `VersionEdit` from a framed-record payload (the bytes between
    /// the framing header and the next record).
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidHeader`] if the payload is truncated or a
    /// count exceeds the remaining bytes (a corrupt edit must surface, never
    /// silently mis-apply).
    pub fn decode_payload(mut bytes: &[u8]) -> crate::Result<Self> {
        const ERR: crate::Error = crate::Error::InvalidHeader("VersionEdit");
        let r = &mut bytes;

        let new_version_id = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;

        let changed_level_count = r.read_u32::<LittleEndian>().map_err(|_| ERR)?;
        let mut changed_levels = Vec::with_capacity(cap(changed_level_count));
        for _ in 0..changed_level_count {
            let level = r.read_u8().map_err(|_| ERR)?;
            let run_count = r.read_u32::<LittleEndian>().map_err(|_| ERR)?;
            let mut runs = Vec::with_capacity(cap(run_count));
            for _ in 0..run_count {
                let table_count = r.read_u32::<LittleEndian>().map_err(|_| ERR)?;
                let mut run = Vec::with_capacity(cap(table_count));
                for _ in 0..table_count {
                    let id = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;
                    let checksum_type = r.read_u8().map_err(|_| ERR)?;
                    if checksum_type != CHECKSUM_TYPE_XXH3 {
                        return Err(ERR);
                    }
                    let checksum = r.read_u128::<LittleEndian>().map_err(|_| ERR)?;
                    let global_seqno = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;
                    run.push(TableDesc {
                        id,
                        checksum,
                        global_seqno,
                    });
                }
                runs.push(run);
            }
            changed_levels.push(ChangedLevel { level, runs });
        }

        let added_blob_count = r.read_u32::<LittleEndian>().map_err(|_| ERR)?;
        let mut added_blob_files = Vec::with_capacity(cap(added_blob_count));
        for _ in 0..added_blob_count {
            let id = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;
            let checksum_type = r.read_u8().map_err(|_| ERR)?;
            if checksum_type != CHECKSUM_TYPE_XXH3 {
                return Err(ERR);
            }
            let checksum = r.read_u128::<LittleEndian>().map_err(|_| ERR)?;
            added_blob_files.push(AddedBlobFile { id, checksum });
        }

        let removed_blob_count = r.read_u32::<LittleEndian>().map_err(|_| ERR)?;
        let mut removed_blob_file_ids = Vec::with_capacity(cap(removed_blob_count));
        for _ in 0..removed_blob_count {
            removed_blob_file_ids.push(r.read_u64::<LittleEndian>().map_err(|_| ERR)?);
        }

        let gc_stats_len = r.read_u32::<LittleEndian>().map_err(|_| ERR)? as usize;
        let gc_stats = if gc_stats_len == 0 {
            None
        } else {
            if r.len() < gc_stats_len {
                return Err(ERR);
            }
            let (head, tail) = r.split_at(gc_stats_len);
            *r = tail;
            Some(head.to_vec())
        };

        let restriction_count = r.read_u32::<LittleEndian>().map_err(|_| ERR)?;
        let mut restrictions = Vec::with_capacity(cap(restriction_count));
        for _ in 0..restriction_count {
            let id = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;
            let key_len = r.read_u32::<LittleEndian>().map_err(|_| ERR)? as usize;
            if r.len() < key_len {
                return Err(ERR);
            }
            let (head, tail) = r.split_at(key_len);
            *r = tail;
            restrictions.push((id, UserKey::from(head)));
        }

        // A well-formed edit consumes its payload exactly. Trailing bytes mean a
        // corrupt / mis-encoded record (format drift, not power loss — the
        // framing checksum already passed), so reject rather than silently
        // accept a truncated interpretation.
        if !r.is_empty() {
            return Err(ERR);
        }

        Ok(Self {
            new_version_id,
            changed_levels,
            added_blob_files,
            removed_blob_file_ids,
            gc_stats,
            restrictions,
        })
    }
}

/// Maps a non-`Ok` trailing-record outcome to the static `kind` carried by
/// [`crate::Error::TornManifestEditLog`].
fn tail_defect_kind(outcome: &framing::FramedRecordOutcome) -> &'static str {
    use framing::FramedRecordOutcome;
    match outcome {
        FramedRecordOutcome::TailTruncation => "truncated",
        FramedRecordOutcome::ChecksumMismatch { .. } => "checksum-mismatch",
        FramedRecordOutcome::BadHeader => "bad-header",
        FramedRecordOutcome::LenMismatch { .. } => "len-mismatch",
        // The caller only routes non-`Ok` outcomes here.
        FramedRecordOutcome::Ok => "ok",
    }
}

/// Replays an edit log: reads framed `VersionEdit` records from `reader` in
/// order and returns the durable prefix. Under the append-only invariant a
/// crash can only ever truncate or corrupt the trailing record, so the clean
/// prefix is exactly the set of durably-committed edits.
///
/// The edit log has no count header and no terminator: a clean end-of-log is a
/// record boundary with no further bytes, which is byte-identical to a crash
/// that landed exactly at a boundary. That clean boundary always ends replay
/// successfully in every mode — otherwise a pristine database would fail to
/// open.
///
/// For a trailing record with bytes present, the policy follows `mode` and the
/// kind of defect, mirroring how the snapshot sections route the same
/// [`ManifestRecoveryMode`](crate::config::ManifestRecoveryMode) variants:
///
/// - **Truncated tail** (`TailTruncation`): the writer never finished the
///   record, so the operation was never acknowledged upward. Rolled back (and
///   the durable prefix recovered) under every mode except
///   [`AbsoluteConsistency`](crate::config::ManifestRecoveryMode::AbsoluteConsistency),
///   which surfaces [`crate::Error::TornManifestEditLog`] so an operator
///   truncates the tail deliberately (via [`Config::repair`](crate::Config::repair)).
/// - **Corrupt fully-framed tail** (`ChecksumMismatch` / `BadHeader` /
///   `LenMismatch`): bit-rot or forgery of already-committed bytes, not an
///   incomplete write. Rolled back only under the corruption-tolerant modes
///   ([`PointInTimeRecovery`](crate::config::ManifestRecoveryMode::PointInTimeRecovery)
///   / [`SkipAnyCorruptedRecords`](crate::config::ManifestRecoveryMode::SkipAnyCorruptedRecords)).
///   Both [`AbsoluteConsistency`](crate::config::ManifestRecoveryMode::AbsoluteConsistency)
///   and [`TolerateCorruptedTailRecords`](crate::config::ManifestRecoveryMode::TolerateCorruptedTailRecords)
///   abort with [`crate::Error::TornManifestEditLog`] — the latter salvages
///   writer-incomplete tails only, never arbitrary bit-rot.
///
/// A record whose framing is `Ok` (full payload + matching checksum) but whose
/// payload fails to decode is a genuine format error, not power loss, and is
/// surfaced as an error in every mode rather than silently truncating the log.
///
/// # Errors
///
/// Returns an I/O error from `reader`, [`crate::Error::InvalidHeader`] if a
/// checksum-valid record fails to decode, or
/// [`crate::Error::TornManifestEditLog`] when the trailing record is
/// torn / bit-rotted / mis-framed and `mode` does not tolerate that defect.
pub fn replay_edits<R: Read>(
    reader: &mut R,
    mode: crate::config::ManifestRecoveryMode,
) -> crate::Result<Vec<VersionEdit>> {
    use crate::config::ManifestRecoveryMode;
    #[cfg(not(feature = "std"))]
    use crate::io::BufRead;
    use framing::FramedRecordOutcome;
    #[cfg(feature = "std")]
    use std::io::BufRead;

    // Only AbsoluteConsistency refuses to roll back a writer-incomplete tail.
    let abort_on_truncation = matches!(mode, ManifestRecoveryMode::AbsoluteConsistency);
    // Only PIT / SkipAny roll back a fully-framed but corrupt trailing record;
    // AbsoluteConsistency and TolerateCorruptedTailRecords abort on it.
    let tolerate_corruption = matches!(
        mode,
        ManifestRecoveryMode::PointInTimeRecovery | ManifestRecoveryMode::SkipAnyCorruptedRecords
    );

    // Buffer the reader so a clean record boundary can be detected (empty fill)
    // without consuming bytes from a genuine trailing record.
    let mut reader = crate::io::BufReader::new(reader);
    let mut edits = Vec::new();
    let mut scratch = Vec::new();
    loop {
        // No bytes left at a record boundary: the normal end of the log. A crash
        // exactly at a boundary is indistinguishable from a pristine close, so
        // this is always a successful end — never a "torn tail", in any mode.
        if reader.fill_buf().map_err(crate::Error::from)?.is_empty() {
            break;
        }
        let outcome = framing::read_framed_record(&mut reader, u64::MAX, None, &mut scratch)?;
        match outcome {
            FramedRecordOutcome::Ok => edits.push(VersionEdit::decode_payload(&scratch)?),
            // Writer-incomplete tail (power loss mid-append): unacknowledged, so
            // tolerant modes drop it; AbsoluteConsistency surfaces it.
            FramedRecordOutcome::TailTruncation => {
                if abort_on_truncation {
                    return Err(crate::Error::TornManifestEditLog { kind: "truncated" });
                }
                break;
            }
            // Fully-framed but corrupt: bit-rot / forgery of committed bytes.
            // PIT / SkipAny roll it back; AbsoluteConsistency and
            // TolerateCorruptedTailRecords (truncation-salvage only) abort.
            FramedRecordOutcome::ChecksumMismatch { .. }
            | FramedRecordOutcome::BadHeader
            | FramedRecordOutcome::LenMismatch { .. } => {
                if tolerate_corruption {
                    break;
                }
                return Err(crate::Error::TornManifestEditLog {
                    kind: tail_defect_kind(&outcome),
                });
            }
        }
    }
    Ok(edits)
}

/// A `usize` count that must fit in `u32` for the wire format.
fn u32_len(n: usize) -> crate::Result<u32> {
    u32::try_from(n).map_err(|_| crate::Error::Unrecoverable)
}

/// Pre-allocation cap for a decoded count: never trust an on-disk count to size
/// a `Vec` directly (a corrupt count could request a huge allocation); the read
/// loop still fails on truncation once the bytes run out.
fn cap(count: u32) -> usize {
    (count as usize).min(1024)
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
mod tests;
