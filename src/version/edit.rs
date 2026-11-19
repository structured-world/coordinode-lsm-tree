// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

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
//!           repeat (table): id u64 LE | checksum_type u8 | checksum u128 LE | global_seqno u64 LE | recency u64 LE
//! added_blob_count    : u32 LE
//!   repeat: id u64 LE | checksum_type u8 | checksum u128 LE
//! removed_blob_count  : u32 LE
//!   repeat: id u64 LE
//! gc_stats_len        : u32 LE   (0 = GC stats unchanged from the prior version)
//!   gc_stats bytes    : [u8; gc_stats_len]
//! restriction_count   : u32 LE
//!   repeat: id u64 LE | key_len u32 LE | key bytes
//! [blob_restriction_count : u32 LE]        (appended section, see below)
//!   repeat: id u64 LE | frontier u64 LE
//! [retention_floor    : u64 LE]            (appended section, see below)
//! [dict_count         : u32 LE]            (appended section, see below)
//!   repeat: id u32 LE
//! [flush_watermark    : u64 LE]            (appended section, see below)
//! [clean_close        : u8 = 1]            (appended section, see below)
//! ```
//!
//! The five bracketed sections were appended after the format shipped and are
//! written only when they carry something: the blob frontiers when the list is
//! non-empty, the retention floor when it changed in this edit, the dictionary
//! ids when the registered set changed, the flush watermark on every edit the
//! engine writes now, the clean-close mark on the edit a clean close appends.
//! A payload that ends before any of them decodes as "absent", so older edit
//! logs replay unchanged.
//!
//! None of them has a presence marker of its own, so they are positional
//! and a later one is written only with every earlier one present: an edit
//! carrying a retention floor writes the blob section first (a zero count when
//! empty), and one carrying dictionary ids writes both (the floor re-stated at
//! its current value when this edit did not change it, which replays as the
//! no-op it is). That is what lets the decoder tell a floor from a blob count
//! from a dictionary count.
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
pub(crate) struct TableDesc {
    /// Table id.
    pub id: u64,
    /// XXH3-128 checksum of the table file.
    pub checksum: u128,
    /// Global sequence number stamped on the table.
    pub global_seqno: u64,
    /// The table's L0 recency key (see [`crate::table::Table::l0_recency`]).
    pub recency: u64,
}

/// The full new run layout of one LSM level that an edit replaces wholesale.
///
/// `runs` is `[run][table]`: each inner `Vec` is one comparator-sorted run, in
/// the same order the snapshot persists it. An empty `runs` means the edit
/// empties the level (e.g. a compaction drained it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChangedLevel {
    /// LSM level this layout replaces.
    pub level: u8,
    /// New run layout of the level: `runs[r]` is the tables of run `r`, in
    /// persisted (comparator-sorted) order.
    pub runs: Vec<Vec<TableDesc>>,
}

/// A blob file added by an edit. Mirrors the snapshot's per-blob record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AddedBlobFile {
    /// Blob file id.
    pub id: u64,
    /// XXH3-128 checksum of the blob file.
    pub checksum: u128,
}

/// The delta between two consecutive versions: what one flush / compaction
/// changed. Applied in order on top of a snapshot during recovery.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct VersionEdit {
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
    /// Per-blob-file live-data frontiers for tight-space relocation
    /// (`blob file id → first live byte offset`). A blob file listed here has
    /// had its consumed prefix punched out after its values were relocated, so
    /// its recorded checksum covers only `[frontier, end)`; whole-file hashing
    /// would read the punched (zeroed) prefix and report a healthy file as
    /// corrupt. The blob analogue of [`Self::restrictions`], carrying a byte
    /// offset because a blob file has no key index to resolve a bound through.
    /// Empty on every edit that did not reclaim a blob prefix.
    pub blob_restrictions: Vec<(u64, u64)>,
    /// The version's retention floor (highest snapshot seqno a reopen may no
    /// longer serve), or `None` when unchanged from the prior version. Only a
    /// GC compaction, a `clear` or a table drop raises it, and only
    /// `AbstractTree::lower_retention_floor` lowers it, so the common edit
    /// carries nothing here. Recovery takes the value as written, either way.
    pub retention_floor: Option<u64>,
    /// The version's FULL set of registered dictionary ids, or `None` when the
    /// set is unchanged from the prior version. Carried whole rather than as a
    /// delta so that a replay lands on the set the version actually had,
    /// exactly as the restriction sets above: an edit that drops a dictionary
    /// has to be able to say the id is gone.
    pub dicts: Option<Vec<crate::file::DictId>>,
    /// The highest synced-flush number the version decides (see
    /// [`super::FlushSeqs`]), or `None` on an edit written before synced
    /// flushes described themselves. Absolute, as the floor.
    pub flush_watermark: Option<u64>,
    /// Whether this edit is the one a clean close appends once every flush is
    /// in the manifest: an open whose log ends with it has no flush to find
    /// beyond what the manifest names (see [`super::routes`]).
    pub clean_close: bool,
}

/// `checksum_type` byte written for XXH3-128 (matches the snapshot encoder).
const CHECKSUM_TYPE_XXH3: u8 = 0;

/// The byte of the clean-close mark, the fifth appended section.
const CLEAN_CLOSE: u8 = 1;

impl VersionEdit {
    /// Serializes the edit payload (without the framing header) into `out`.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when the edit carries dictionary ids but
    /// no retention floor. The appended sections are positional, so such an
    /// edit would encode into bytes the decoder reads as a floor built from the
    /// dictionary count and the first id: a fabricated floor, and the whole set
    /// lost. Refusing to write it is what keeps the encoding total.
    fn encode_payload(&self, out: &mut Vec<u8>) -> crate::Result<()> {
        if self.dicts.is_some() && self.retention_floor.is_none() {
            return Err(crate::Error::InvalidHeader(
                "VersionEdit: dictionary ids without a retention floor",
            ));
        }
        // The watermark is the fourth appended section, after the dictionary
        // ids, so an edit carrying it states the floor and the set in full.
        if self.flush_watermark.is_some() && self.dicts.is_none() {
            return Err(crate::Error::InvalidHeader(
                "VersionEdit: flush watermark without its dictionary ids",
            ));
        }
        // The clean-close mark is the fifth, after the watermark.
        if self.clean_close && self.flush_watermark.is_none() {
            return Err(crate::Error::InvalidHeader(
                "VersionEdit: clean close without its flush watermark",
            ));
        }

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
                    out.write_u64::<LittleEndian>(t.recency)?;
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

        // The appended sections are positional, each with no presence marker
        // of its own: whenever a later one is written every earlier one is
        // too (a zero blob count, the floor re-stated, the dictionary set),
        // so the decoder never mistakes one for another.
        //
        // The flush watermark, last in the chain, is written on every edit,
        // so every edit carries all four. A binary that predates one of them
        // rejects such an edit as trailing data: after the first edit this
        // binary writes, an older one no longer opens the tree. That refusal
        // is the fail-fast, not a format version: those binaries cannot tell
        // which flushes a manifest decides, and an open by one would take in
        // or drop flushes against what the tree returned.
        if !self.blob_restrictions.is_empty()
            || self.retention_floor.is_some()
            || self.dicts.is_some()
        {
            out.write_u32::<LittleEndian>(u32_len(self.blob_restrictions.len())?)?;
            for (id, frontier) in &self.blob_restrictions {
                out.write_u64::<LittleEndian>(*id)?;
                out.write_u64::<LittleEndian>(*frontier)?;
            }
        }
        if let Some(floor) = self.retention_floor {
            out.write_u64::<LittleEndian>(floor)?;
        }
        if let Some(dicts) = &self.dicts {
            // The floor above is already written: `Version::diff` re-states an
            // unchanged one whenever it carries dictionaries, and the guard at
            // the top of this function refuses any edit that did not.
            out.write_u32::<LittleEndian>(u32_len(dicts.len())?)?;
            for id in dicts {
                out.write_u32::<LittleEndian>(*id)?;
            }
        }
        if let Some(watermark) = self.flush_watermark {
            out.write_u64::<LittleEndian>(watermark)?;
        }
        if self.clean_close {
            out.write_u8(CLEAN_CLOSE)?;
        }
        Ok(())
    }

    /// Serializes this edit's record payload into `out`, replacing what it
    /// held, so the caller can check its size before committing it to a log.
    ///
    /// # Errors
    ///
    /// Returns an error if the edit carries dictionary ids without a retention
    /// floor (the positional sections cannot express that).
    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> crate::Result<()> {
        out.clear();
        self.encode_payload(out)
    }

    /// Appends this edit as one framed record to `writer`, reusing `scratch`
    /// for the payload assembly.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload exceeds the framing payload cap, a write
    /// fails, or the edit carries dictionary ids without a retention floor. The
    /// payload is assembled before any of it reaches `writer`, so a refused
    /// edit emits no record.
    #[cfg(test)]
    pub(crate) fn append_to<W: Write>(
        &self,
        writer: &mut W,
        scratch: &mut Vec<u8>,
    ) -> crate::Result<()> {
        self.encode(scratch)?;
        framing::write_frame(writer, scratch)
    }

    /// Decodes a `VersionEdit` from a framed-record payload (the bytes between
    /// the framing header and the next record).
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidHeader`] if the payload is truncated or a
    /// count exceeds the remaining bytes (a corrupt edit must surface, never
    /// silently mis-apply).
    pub(crate) fn decode_payload(mut bytes: &[u8]) -> crate::Result<Self> {
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
                    let recency = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;
                    run.push(TableDesc {
                        id,
                        checksum,
                        global_seqno,
                        recency,
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

        // Blob frontiers are an APPENDED section: edits written before it existed
        // end right here. The framing checksum already passed, so a payload that
        // stops at this boundary is an older-format edit carrying no blob
        // frontier — not a truncated one — and decodes as an empty list.
        let mut blob_restrictions = Vec::new();
        if !r.is_empty() {
            let blob_restriction_count = r.read_u32::<LittleEndian>().map_err(|_| ERR)?;
            blob_restrictions.reserve(cap(blob_restriction_count));
            for _ in 0..blob_restriction_count {
                let id = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;
                let frontier = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;
                blob_restrictions.push((id, frontier));
            }
        }

        // The retention floor is the second appended section: present only
        // when the edit raised it, and always preceded by the blob section
        // (which the encoder writes with a zero count in that case), so a
        // payload that still has bytes here holds the floor.
        let retention_floor = if r.is_empty() {
            None
        } else {
            Some(r.read_u64::<LittleEndian>().map_err(|_| ERR)?)
        };

        // The dictionary ids are the third appended section, under the same
        // positional rule: written only after the floor, so bytes remaining
        // here are the id list and nothing else.
        let dicts = if r.is_empty() {
            None
        } else {
            let count = r.read_u32::<LittleEndian>().map_err(|_| ERR)?;
            let mut ids = Vec::with_capacity(cap(count));
            for _ in 0..count {
                ids.push(r.read_u32::<LittleEndian>().map_err(|_| ERR)?);
            }
            Some(ids)
        };

        // The flush watermark is the fourth appended section, under the same
        // positional rule: written only after the dictionary ids. `u64::MAX`
        // is no number a flush takes.
        let flush_watermark = if r.is_empty() {
            None
        } else {
            let watermark = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;
            if watermark == u64::MAX {
                return Err(ERR);
            }
            Some(watermark)
        };

        // The clean-close mark is the fifth, written only after the
        // watermark, and only by the edit a clean close appends.
        let clean_close = if r.is_empty() {
            false
        } else {
            if r.read_u8().map_err(|_| ERR)? != CLEAN_CLOSE {
                return Err(ERR);
            }
            true
        };

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
            blob_restrictions,
            retention_floor,
            dicts,
            flush_watermark,
            clean_close,
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
/// For a record with bytes present, the policy follows the kind of defect:
///
/// - **Truncated tail** (`TailTruncation`): the writer never finished the
///   record, so the operation was never acknowledged upward. Rolled back (and
///   the durable prefix recovered) under
///   [`TolerateCorruptedTailRecords`](crate::config::ManifestRecoveryMode::TolerateCorruptedTailRecords);
///   [`AbsoluteConsistency`](crate::config::ManifestRecoveryMode::AbsoluteConsistency)
///   surfaces [`crate::Error::TornManifestEditLog`] so an operator resolves it
///   deliberately (via [`Config::repair`](crate::Config::repair)).
/// - **Corrupt fully-framed record** (`ChecksumMismatch` / `BadHeader` /
///   `LenMismatch`): bit-rot or forgery of already-committed bytes, not an
///   incomplete write. Surfaces [`crate::Error::TornManifestEditLog`] in every
///   mode. Rolling it back would drop every committed edit after it too, and
///   the open would then delete the tables those edits added.
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
pub(crate) fn replay_edits<R: Read>(
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
            // TolerateCorruptedTailRecords drops it; AbsoluteConsistency surfaces it.
            FramedRecordOutcome::TailTruncation => {
                if abort_on_truncation {
                    return Err(crate::Error::TornManifestEditLog { kind: "truncated" });
                }
                break;
            }
            // Fully-framed but corrupt: bit-rot / forgery of committed bytes.
            // Refused in every mode; the tables it names stay on disk for
            // repair.
            FramedRecordOutcome::ChecksumMismatch { .. }
            | FramedRecordOutcome::BadHeader
            | FramedRecordOutcome::LenMismatch { .. } => {
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
