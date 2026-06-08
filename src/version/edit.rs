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
//! [`framing`](super::framing) record (`len + xxh3_64 + payload`), so a
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
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
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
                    let _checksum_type = r.read_u8().map_err(|_| ERR)?;
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
            let _checksum_type = r.read_u8().map_err(|_| ERR)?;
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
            let (head, _tail) = r.split_at(gc_stats_len);
            Some(head.to_vec())
        };

        Ok(Self {
            new_version_id,
            changed_levels,
            added_blob_files,
            removed_blob_file_ids,
            gc_stats,
        })
    }
}

/// Replays an edit log: reads framed `VersionEdit` records from `reader` in
/// order and returns the durable prefix. Replay STOPS at the first record that
/// is not cleanly `Ok` (torn tail, bad checksum, bad/short header) — under the
/// append-only invariant a crash can only ever truncate or corrupt the trailing
/// record, so the clean prefix is exactly the set of durably-committed edits.
/// The dropped tail (and anything after it) was never acknowledged upward, so a
/// higher-level journal replays that data.
///
/// A record whose framing is `Ok` (full payload + matching checksum) but whose
/// payload fails to decode is a genuine format error, not power loss, and is
/// surfaced as an error rather than silently truncating the log.
///
/// # Errors
///
/// Returns an I/O error from `reader` (other than EOF, which ends replay), or
/// [`crate::Error::InvalidHeader`] if a checksum-valid record fails to decode.
pub fn replay_edits<R: Read>(reader: &mut R) -> crate::Result<Vec<VersionEdit>> {
    use framing::FramedRecordOutcome;

    let mut edits = Vec::new();
    let mut scratch = Vec::new();
    // Loop until the first non-`Ok` outcome: under the append-only invariant a
    // crash truncates / corrupts only the trailing record, so the first
    // TailTruncation / ChecksumMismatch / Bad-or-LenMismatch header ends the
    // durable prefix. A clean record that fails to *decode* is a real format
    // error and propagates via `?`.
    while matches!(
        framing::read_framed_record(reader, u64::MAX, None, &mut scratch)?,
        FramedRecordOutcome::Ok
    ) {
        edits.push(VersionEdit::decode_payload(&scratch)?);
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
mod tests {
    use super::*;

    fn sample() -> VersionEdit {
        VersionEdit {
            new_version_id: 42,
            changed_levels: vec![
                ChangedLevel {
                    level: 0,
                    // L0: two single-table runs (one per flush), overlapping.
                    runs: vec![
                        vec![TableDesc {
                            id: 7,
                            checksum: 0x1122_3344_5566_7788_99AA_BBCC_DDEE_FF00,
                            global_seqno: 100,
                        }],
                        vec![TableDesc {
                            id: 8,
                            checksum: 1,
                            global_seqno: 101,
                        }],
                    ],
                },
                ChangedLevel {
                    level: 3,
                    // Tiered level: one run holding two sorted tables.
                    runs: vec![vec![
                        TableDesc {
                            id: 10,
                            checksum: 2,
                            global_seqno: 50,
                        },
                        TableDesc {
                            id: 11,
                            checksum: 3,
                            global_seqno: 51,
                        },
                    ]],
                },
            ],
            added_blob_files: vec![AddedBlobFile {
                id: 9,
                checksum: 0xDEAD_BEEF,
            }],
            removed_blob_file_ids: vec![4],
            gc_stats: Some(vec![0xAB; 20]),
        }
    }

    #[test]
    fn framed_roundtrip_recovers_the_edit() {
        let edit = sample();
        let mut buf = Vec::new();
        let mut scratch = Vec::new();
        edit.append_to(&mut buf, &mut scratch).expect("append");

        let mut payload = Vec::new();
        let outcome =
            framing::read_framed_record(&mut &buf[..], u64::MAX, None, &mut payload).expect("read");
        assert!(
            matches!(outcome, framing::FramedRecordOutcome::Ok),
            "clean record must decode Ok, got {outcome:?}",
        );
        let decoded = VersionEdit::decode_payload(&payload).expect("decode");
        assert_eq!(decoded, edit);
    }

    #[test]
    fn empty_level_layout_roundtrips() {
        // A compaction that drains a level emits an empty-runs ChangedLevel.
        let mut edit = sample();
        edit.changed_levels.push(ChangedLevel {
            level: 2,
            runs: vec![],
        });
        let mut buf = Vec::new();
        edit.append_to(&mut buf, &mut Vec::new()).expect("append");
        let mut payload = Vec::new();
        framing::read_framed_record(&mut &buf[..], u64::MAX, None, &mut payload).expect("read");
        assert_eq!(VersionEdit::decode_payload(&payload).expect("decode"), edit);
    }

    #[test]
    fn empty_gc_stats_roundtrips_as_none() {
        let mut edit = sample();
        edit.gc_stats = None;
        let mut buf = Vec::new();
        edit.append_to(&mut buf, &mut Vec::new()).expect("append");
        let mut payload = Vec::new();
        framing::read_framed_record(&mut &buf[..], u64::MAX, None, &mut payload).expect("read");
        assert_eq!(VersionEdit::decode_payload(&payload).expect("decode"), edit);
    }

    #[test]
    fn truncated_trailing_record_is_detected() {
        // A power-loss-truncated edit must NOT decode as Ok — replay stops here.
        let edit = sample();
        let mut buf = Vec::new();
        edit.append_to(&mut buf, &mut Vec::new()).expect("append");
        buf.truncate(buf.len() - 5); // chop the tail
        let mut payload = Vec::new();
        let outcome = framing::read_framed_record(&mut &buf[..], u64::MAX, None, &mut payload)
            .expect("read does not error on truncation");
        assert!(
            !matches!(outcome, framing::FramedRecordOutcome::Ok),
            "truncated record must not be Ok, got {outcome:?}",
        );
    }

    #[test]
    fn bitflip_in_payload_fails_checksum() {
        let edit = sample();
        let mut buf = Vec::new();
        edit.append_to(&mut buf, &mut Vec::new()).expect("append");
        // Flip a byte in the payload region (past the 12-byte framing header).
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        let mut payload = Vec::new();
        let outcome =
            framing::read_framed_record(&mut &buf[..], u64::MAX, None, &mut payload).expect("read");
        assert!(
            matches!(
                outcome,
                framing::FramedRecordOutcome::ChecksumMismatch { .. }
            ),
            "bit-flip must surface as ChecksumMismatch, got {outcome:?}",
        );
    }

    #[test]
    fn replay_recovers_all_durable_edits_in_order() {
        let mut log = Vec::new();
        let mut scratch = Vec::new();
        let edits: Vec<VersionEdit> = (0..5)
            .map(|i| {
                let mut e = sample();
                e.new_version_id = 100 + i;
                e
            })
            .collect();
        for e in &edits {
            e.append_to(&mut log, &mut scratch).expect("append");
        }
        let replayed = replay_edits(&mut &log[..]).expect("replay");
        assert_eq!(replayed, edits, "replay must recover every edit in order");
    }

    #[test]
    fn replay_stops_at_torn_tail_keeping_clean_prefix() {
        // Two clean edits + a truncated third: replay keeps the first two.
        let mut log = Vec::new();
        let mut scratch = Vec::new();
        let mut e0 = sample();
        e0.new_version_id = 1;
        let mut e1 = sample();
        e1.new_version_id = 2;
        e0.append_to(&mut log, &mut scratch).expect("append e0");
        e1.append_to(&mut log, &mut scratch).expect("append e1");
        let clean_len = log.len();
        // Append a third edit, then chop its tail (simulated power loss).
        let mut e2 = sample();
        e2.new_version_id = 3;
        e2.append_to(&mut log, &mut scratch).expect("append e2");
        log.truncate(clean_len + 6); // partial third record

        let replayed = replay_edits(&mut &log[..]).expect("replay");
        assert_eq!(replayed, vec![e0, e1], "torn tail dropped, prefix kept");
    }

    #[test]
    fn replay_stops_at_bitflipped_record() {
        // A bit-flip in the second record stops replay after the first.
        let mut log = Vec::new();
        let mut scratch = Vec::new();
        let mut e0 = sample();
        e0.new_version_id = 1;
        let mut e1 = sample();
        e1.new_version_id = 2;
        e0.append_to(&mut log, &mut scratch).expect("append e0");
        let after_e0 = log.len();
        e1.append_to(&mut log, &mut scratch).expect("append e1");
        // Corrupt a payload byte of the second record (past its framing header).
        let target = after_e0 + framing::FRAME_HEADER_LEN + 2;
        log[target] ^= 0xFF;

        let replayed = replay_edits(&mut &log[..]).expect("replay");
        assert_eq!(replayed, vec![e0], "replay stops at the corrupted record");
    }

    #[test]
    fn replay_of_empty_log_is_empty() {
        let replayed = replay_edits(&mut &[][..]).expect("replay");
        assert!(replayed.is_empty(), "empty log → no edits");
    }

    #[test]
    fn decode_rejects_truncated_payload() {
        let edit = sample();
        let mut payload = Vec::new();
        edit.encode_payload(&mut payload).expect("encode");
        payload.truncate(payload.len() / 2);
        assert!(
            matches!(
                VersionEdit::decode_payload(&payload),
                Err(crate::Error::InvalidHeader("VersionEdit"))
            ),
            "a truncated payload must surface InvalidHeader",
        );
    }
}
