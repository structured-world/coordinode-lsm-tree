// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{
    Checksum, SeqNo, TableId, TreeType,
    coding::Decode,
    config::ManifestRecoveryMode,
    file::CURRENT_VERSION_FILE,
    fs::{Fs, FsOpenOptions, open_section_reader},
    version::VersionId,
    vlog::BlobFileId,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::path::Path;

/// Exact on-disk size of a `tables`-section record payload (post-framing):
/// `id: u64 (8) | checksum_type: u8 (1) | checksum: u128 (16) | global_seqno: u64 (8)`.
///
/// Stored as `u32` because the framing layer's `len` field is `u32`;
/// pinning at this type avoids cast-truncation lints at every
/// `read_framed_record` call site. Used as a `usize` length check
/// inside `decode_table_entry_payload` via the implicit
/// `u32 -> usize` widening.
const TABLE_ENTRY_PAYLOAD_LEN: u32 = 8 + 1 + 16 + 8;

/// Exact on-disk size of a `blob_files`-section record payload (post-framing):
/// `id: u64 (8) | checksum_type: u8 (1) | checksum: u128 (16)`.
const BLOB_ENTRY_PAYLOAD_LEN: u32 = 8 + 1 + 16;

/// Decodes a 33-byte table-record payload (post-framing): `id: u64 |
/// checksum_type: u8 | checksum: u128 | global_seqno: u64`. The
/// surrounding framing header (length + XXH3-64) is handled by
/// [`crate::version::framing::read_framed_record`] before this is
/// called.
///
/// Rejects payloads whose length is not exactly
/// [`TABLE_ENTRY_PAYLOAD_LEN`]. The framing layer already verified
/// the XXH3-64 over the payload matched the header digest, so any
/// length mismatch at this point is writer / reader format drift,
/// not on-disk bit-rot. That makes it categorically different from
/// the per-record corruption shapes PIT / `SkipAny` route around:
/// `Error::InvalidHeader` is propagated unconditionally and aborts
/// recovery in ALL modes, including the tolerant ones — a code
/// bug surfacing as silently-skipped records would be worse than
/// a hard fail at open time.
fn decode_table_entry_payload(payload: &[u8]) -> crate::Result<RecoveredTable> {
    if payload.len() != TABLE_ENTRY_PAYLOAD_LEN as usize {
        return Err(crate::Error::InvalidHeader("tables record payload length"));
    }
    let mut cursor = std::io::Cursor::new(payload);
    let id = cursor.read_u64::<LittleEndian>()?;
    let checksum_type = cursor.read_u8()?;
    if checksum_type != 0 {
        return Err(crate::Error::InvalidTag(("ChecksumType", checksum_type)));
    }
    let checksum = Checksum::from_raw(cursor.read_u128::<LittleEndian>()?);
    let global_seqno = cursor.read_u64::<LittleEndian>()?;
    Ok(RecoveredTable {
        id,
        checksum,
        global_seqno,
    })
}

/// Decodes a 25-byte blob-record payload (post-framing): `id: u64 |
/// checksum_type: u8 | checksum: u128`. Same length-check contract as
/// [`decode_table_entry_payload`].
fn decode_blob_entry_payload(payload: &[u8]) -> crate::Result<(BlobFileId, Checksum)> {
    if payload.len() != BLOB_ENTRY_PAYLOAD_LEN as usize {
        return Err(crate::Error::InvalidHeader(
            "blob_files record payload length",
        ));
    }
    let mut cursor = std::io::Cursor::new(payload);
    let id = cursor.read_u64::<LittleEndian>()?;
    let checksum_type = cursor.read_u8()?;
    if checksum_type != 0 {
        return Err(crate::Error::InvalidTag(("ChecksumType", checksum_type)));
    }
    let checksum = Checksum::from_raw(cursor.read_u128::<LittleEndian>()?);
    Ok((id, checksum))
}

/// Reads and validates the CURRENT version pointer file.
///
/// The file format is: `version_id: u64 | checksum: u128 | checksum_type: u8`
/// (25 bytes total, written atomically by `rewrite_atomic`).
///
/// Returns the version ID after verifying the checksum type tag is valid.
/// The checksum field is read from disk but is not validated here.
pub fn get_current_version(folder: &Path, fs: &dyn Fs) -> crate::Result<VersionId> {
    use byteorder::{LittleEndian, ReadBytesExt};

    let path = folder.join(CURRENT_VERSION_FILE);
    let mut file = fs.open(&path, &FsOpenOptions::new().read(true))?;

    let version_id = file.read_u64::<LittleEndian>()?;
    let _checksum = file.read_u128::<LittleEndian>()?;
    let checksum_type = file.read_u8()?;

    // Validate checksum type tag — a non-zero value indicates corruption
    // or a file from an incompatible version (only xxh3 = 0 is supported).
    if checksum_type != 0 {
        return Err(crate::Error::InvalidTag(("ChecksumType", checksum_type)));
    }

    Ok(version_id)
}

#[derive(Debug)]
pub struct RecoveredTable {
    pub id: TableId,
    pub checksum: Checksum,
    pub global_seqno: SeqNo,
}

#[derive(Debug)]
pub struct Recovery {
    pub tree_type: TreeType,
    pub curr_version_id: VersionId,
    pub table_ids: Vec<Vec<Vec<RecoveredTable>>>,
    pub blob_file_ids: Vec<(BlobFileId, Checksum)>,
    pub gc_stats: crate::blob_tree::FragmentationMap,
}

#[expect(
    clippy::too_many_lines,
    reason = "manifest recovery is inherently a long sequential read of multiple SFA \
              sections; splitting the function would just move the per-mode branching \
              into helpers without clarifying the flow"
)]
pub fn recover(folder: &Path, fs: &dyn Fs, mode: ManifestRecoveryMode) -> crate::Result<Recovery> {
    // Per-record framing constants used by both the tables and
    // blob_files sections. Each on-disk record is a
    // FRAME_HEADER_LEN (12-byte) header followed by a fixed-size
    // payload (TABLE_ENTRY_PAYLOAD_LEN / BLOB_ENTRY_PAYLOAD_LEN).
    // Wiring the totals through the existing payload-length
    // constants keeps the two sites in sync — if a future PR
    // changes a record's payload shape, only one constant moves.
    const FRAMED_TABLE_ENTRY_LEN: u64 =
        crate::version::framing::FRAME_HEADER_LEN as u64 + TABLE_ENTRY_PAYLOAD_LEN as u64;
    const FRAMED_BLOB_ENTRY_LEN: u64 =
        crate::version::framing::FRAME_HEADER_LEN as u64 + BLOB_ENTRY_PAYLOAD_LEN as u64;
    use crate::version::framing::FramedRecordOutcome;

    let curr_version_id = get_current_version(folder, fs)?;
    let version_file_path = folder.join(format!("v{curr_version_id}"));

    log::info!(
        "Recovering current manifest at {} (mode={mode:?})",
        version_file_path.display(),
    );

    let mut file = fs.open(&version_file_path, &FsOpenOptions::new().read(true))?;
    let reader = sfa::Reader::from_reader(&mut file)?;
    let toc = reader.toc();

    // Mode dispatch flags. The per-section loops below treat these
    // four modes as three flag combinations:
    //
    //   AbsoluteConsistency: every flag false → first decode error
    //     anywhere in the section aborts the open.
    //   TolerateCorruptedTailRecords: tolerate_tail = true; the
    //     ChecksumMismatch / BadHeader paths still abort, only
    //     truncated tail (`FramedRecordOutcome::TailTruncation`) is
    //     accepted.
    //   PointInTimeRecovery: tolerate_tail = true AND pit_prefix = true.
    //     On ChecksumMismatch / BadHeader, behaves like reaching EOF
    //     for the current level — the in-progress run + level + all
    //     subsequent levels are dropped. The recovered prefix is the
    //     consistent state up to the last good record-group boundary.
    //   SkipAnyCorruptedRecords: tolerate_tail = true AND skip_any = true.
    //     ChecksumMismatch on a single record is logged and skipped;
    //     reading continues with the next record. BadHeader can no
    //     longer be skipped surgically (the length field itself is
    //     suspect, so the byte boundary of the next record is
    //     unknown) — the rest of the current section is abandoned
    //     under this mode, same as PIT but scoped to one section.
    let tolerate_tail = !matches!(mode, ManifestRecoveryMode::AbsoluteConsistency);
    let pit_prefix = matches!(mode, ManifestRecoveryMode::PointInTimeRecovery);
    let skip_any = matches!(mode, ManifestRecoveryMode::SkipAnyCorruptedRecords);

    // // TODO: vvv move into Version::decode vvv
    let mut levels = vec![];
    // Separate counters for the two distinct tail-truncation shapes
    // so the summary warnings can report them honestly. Header
    // truncation = "the writer didn't even finish writing the
    // count/length byte for this level/run/section"; no per-entry
    // bytes are missing because no entry was supposed to be there
    // yet. Record truncation = "the count says N, only K < N
    // complete entries are present", so N-K entries are actually
    // missing. Conflating them under one counter (the previous
    // behaviour) overcounted: a manifest cut between two runs
    // would log "1 table record dropped" when zero records were
    // lost — only a header byte.
    // Two separate counters per section: "tail-tolerant"-class
    // drops vs "skip_any / pit"-class drops. The tail counter
    // covers the established power-loss-at-write-tail shape;
    // the corruption counter covers checksum mismatches and
    // bad framing headers under the new PIT / SkipAny modes.
    // The post-section summary log surfaces them separately so
    // operators can tell "writer crashed before fsync" (tail)
    // apart from "real bit-rot inside a written record"
    // (corruption).
    let mut tables_dropped_to_tail: u32 = 0;
    let mut tables_dropped_to_corruption: u32 = 0;
    let mut tables_truncated_headers: u32 = 0;

    {
        let section = toc
            .section(b"tables")
            .ok_or(crate::Error::Unrecoverable)
            .inspect_err(|_| {
                log::error!("tables section not found in version #{curr_version_id} - maybe the file is corrupted?");
            })?;

        let mut reader = open_section_reader(fs, &version_file_path, section)?;

        // Wrap the level-count read in tail tolerance too: a manifest
        // truncated before the first byte of `tables` (or right after
        // the SFA TOC was committed but before any payload landed)
        // hits EOF here. Under `TolerateCorruptedTailRecords` that's
        // legitimate "no tables present" — under `AbsoluteConsistency`
        // it's still a hard fail.
        // Track bytes consumed inside this section so the
        // overflow guard below can compare against bytes ACTUALLY
        // remaining at the moment of the check, not the loose
        // section-total bound. With the total bound, a count
        // forgery that's still <= section_total/entry_size slips
        // past the guard, enters the loop, hits EOF after the real
        // records, and gets reclassified as a clean tail truncation
        // under tolerant mode — operators see no signal that the
        // count header was corrupt. The tight bound surfaces the
        // forgery as an explicit "count exceeds remaining" warn
        // before the loop runs.
        let mut tables_bytes_consumed: u64 = 0;

        let level_count = match reader.read_u8() {
            Ok(n) => {
                tables_bytes_consumed += 1;
                n
            }
            Err(e) if tolerate_tail && e.kind() == std::io::ErrorKind::UnexpectedEof => {
                log::warn!(
                    "tables section truncated before level_count byte in version \
                     #{curr_version_id}; tail-tolerant mode produces 0 levels"
                );
                0
            }
            Err(e) => return Err(e.into()),
        };

        'levels: for _ in 0..level_count {
            let mut level = vec![];
            let run_count = match reader.read_u8() {
                Ok(n) => {
                    tables_bytes_consumed += 1;
                    n
                }
                Err(e) if tolerate_tail && e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // No runs in this level had a chance to start;
                    // pushing the empty `level` keeps the level slot
                    // visible to downstream code (instead of silently
                    // dropping it) and matches the cut-mid-run path
                    // below. HEADER truncation — no records were
                    // dropped (none were supposed to be present yet)
                    // so count separately from record-drops.
                    tables_truncated_headers += 1;
                    levels.push(level);
                    break 'levels;
                }
                Err(e) => return Err(e.into()),
            };

            for _ in 0..run_count {
                let mut run = vec![];
                let table_count = match reader.read_u32::<LittleEndian>() {
                    Ok(n) => {
                        tables_bytes_consumed += 4;
                        n
                    }
                    Err(e) if tolerate_tail && e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        // Push the partial `level` so any fully
                        // decoded runs earlier in this level survive
                        // — breaking out of `'levels` without this
                        // push silently drops the consistent prefix.
                        // The current `run` is empty (failed at the
                        // very first byte of its header), nothing to
                        // push for it. HEADER truncation.
                        tables_truncated_headers += 1;
                        levels.push(level);
                        break 'levels;
                    }
                    Err(e) => return Err(e.into()),
                };

                // Tight-bound check: count * framed_entry_size against
                // bytes_remaining at the current cursor. Each framed
                // entry is FRAME_HEADER_LEN (12) + 33 bytes payload =
                // 45 bytes.
                //
                // Under `AbsoluteConsistency` count > remaining is a
                // hard "count header forged" abort. Under any of the
                // tolerant modes it warns and lets the loop walk
                // bytes-actually-present.
                let bytes_remaining = section.len().saturating_sub(tables_bytes_consumed);
                if u64::from(table_count).saturating_mul(FRAMED_TABLE_ENTRY_LEN) > bytes_remaining {
                    if tolerate_tail {
                        log::warn!(
                            "tables: declared table_count={table_count} exceeds \
                             remaining section payload ({bytes_remaining} bytes, \
                             ~{} entries) in version #{curr_version_id}; \
                             tail-tolerant mode walks bytes-actually-present and \
                             stops at the first EOF",
                            bytes_remaining / FRAMED_TABLE_ENTRY_LEN,
                        );
                    } else {
                        return Err(crate::Error::Unrecoverable);
                    }
                }

                for _ in 0..table_count {
                    let remaining = section.len().saturating_sub(tables_bytes_consumed);
                    // Pin the on-disk `len` to the fixed table-record
                    // payload size so a corrupted-but-plausible
                    // `len` cannot mis-align the cursor for the
                    // next record under SkipAny — read_framed_record
                    // returns BadHeader for `len != expected`,
                    // which the section-drop fallback below
                    // handles safely.
                    let outcome = crate::version::framing::read_framed_record(
                        &mut reader,
                        remaining,
                        Some(TABLE_ENTRY_PAYLOAD_LEN),
                    )?;
                    match outcome {
                        FramedRecordOutcome::Ok(payload) => {
                            tables_bytes_consumed += crate::version::framing::FRAME_HEADER_LEN
                                as u64
                                + payload.len() as u64;
                            let t = decode_table_entry_payload(&payload)?;
                            run.push(t);
                        }
                        FramedRecordOutcome::TailTruncation if tolerate_tail => {
                            let recovered = u32::try_from(run.len()).unwrap_or(u32::MAX);
                            tables_dropped_to_tail = tables_dropped_to_tail
                                .saturating_add(table_count.saturating_sub(recovered));
                            level.push(run);
                            levels.push(level);
                            break 'levels;
                        }
                        FramedRecordOutcome::ChecksumMismatch { bytes_consumed, .. }
                            if skip_any =>
                        {
                            // Single bad record under SkipAny: log
                            // it and continue with the next record.
                            // The 12-byte header was internally
                            // consistent (len fits the section), so
                            // the reader has cleanly advanced past
                            // exactly this record and the next
                            // iteration's read lines up on the next
                            // record's header.
                            log::warn!(
                                "skip_any: tables record checksum mismatch \
                                 ({bytes_consumed} bytes) in version \
                                 #{curr_version_id}, skipping"
                            );
                            tables_bytes_consumed += bytes_consumed;
                            tables_dropped_to_corruption =
                                tables_dropped_to_corruption.saturating_add(1);
                        }
                        FramedRecordOutcome::ChecksumMismatch { .. } if pit_prefix => {
                            // PIT: corruption is the boundary. Keep
                            // the consistent prefix collected so far
                            // (records before this one in the run +
                            // any complete earlier runs in this
                            // level + every earlier level), drop
                            // everything that follows (the corrupt
                            // record, the remaining records of this
                            // run, and every level not yet read).
                            // Adapts RocksDB kPointInTimeRecovery's
                            // "accept the consistent prefix" rule
                            // to the level/run/table nesting.
                            log::warn!(
                                "pit: tables record checksum mismatch in version \
                                 #{curr_version_id}; accepting consistent prefix and \
                                 dropping the rest of this run + unread levels"
                            );
                            let recovered = u32::try_from(run.len()).unwrap_or(u32::MAX);
                            tables_dropped_to_corruption = tables_dropped_to_corruption
                                .saturating_add(table_count.saturating_sub(recovered));
                            // Push the partial run + level so the
                            // pre-corruption prefix survives in the
                            // recovered Version. This is the
                            // accept-the-prefix half of the PIT
                            // contract — see the ManifestRecoveryMode
                            // docstring and README for the
                            // user-visible description.
                            level.push(run);
                            levels.push(level);
                            break 'levels;
                        }
                        FramedRecordOutcome::ChecksumMismatch { expected, got, .. } => {
                            return Err(crate::Error::ManifestFrameChecksumMismatch {
                                section: "tables",
                                expected,
                                got,
                            });
                        }
                        FramedRecordOutcome::BadHeader if skip_any || pit_prefix => {
                            // Header itself is suspect — cannot find
                            // the next record boundary. Same fallback
                            // for both modes: drop the rest of this
                            // section's records and call it done.
                            log::warn!(
                                "tables: corrupted framing header in version \
                                 #{curr_version_id}; remaining records in this \
                                 section are unrecoverable"
                            );
                            let recovered = u32::try_from(run.len()).unwrap_or(u32::MAX);
                            tables_dropped_to_corruption = tables_dropped_to_corruption
                                .saturating_add(table_count.saturating_sub(recovered));
                            level.push(run);
                            levels.push(level);
                            break 'levels;
                        }
                        // Strict mode: distinguish "writer crashed
                        // mid-record" (TailTruncation → surface as
                        // the original Io(UnexpectedEof) so
                        // diagnostics match pre-framing recovery)
                        // from "header was structurally implausible"
                        // (BadHeader → Unrecoverable, this is real
                        // corruption that the operator needs to know
                        // about as such).
                        FramedRecordOutcome::TailTruncation => {
                            return Err(crate::Error::Io(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "manifest tables record truncated mid-frame",
                            )));
                        }
                        FramedRecordOutcome::BadHeader => {
                            return Err(crate::Error::Unrecoverable);
                        }
                    }
                }

                level.push(run);
            }

            levels.push(level);
        }
    }

    if tables_dropped_to_tail > 0
        || tables_dropped_to_corruption > 0
        || tables_truncated_headers > 0
    {
        log::warn!(
            "manifest recovery summary for version #{curr_version_id}: \
             {tables_dropped_to_tail} table record(s) dropped to tail-truncation, \
             {tables_dropped_to_corruption} dropped to per-record corruption \
             (skip_any/pit modes), \
             {tables_truncated_headers} level/run header(s) truncated; \
             recovered tree may be missing SSTs",
        );
    }

    let mut blob_dropped_to_tail: u32 = 0;
    let mut blob_dropped_to_corruption: u32 = 0;
    let blob_file_ids = {
        let section = toc
            .section(b"blob_files")
            .ok_or(crate::Error::Unrecoverable)
            .inspect_err(|_| {
                log::error!("blob_files section not found in version #{curr_version_id} - maybe the file is corrupted?");
            })?;

        let mut reader = open_section_reader(fs, &version_file_path, section)?;

        let blob_file_count = match reader.read_u32::<LittleEndian>() {
            Ok(n) => n,
            Err(e) if tolerate_tail && e.kind() == std::io::ErrorKind::UnexpectedEof => {
                log::warn!(
                    "blob_files section truncated before count header in version \
                     #{curr_version_id}; tail-tolerant mode produces 0 blob files"
                );
                0
            }
            Err(e) => return Err(e.into()),
        };

        // Each framed blob entry is FRAME_HEADER_LEN (12) + 25 bytes
        // payload = 37 bytes. Same forged-vs-truncated dispatch as the
        // tables-section count check.
        let blob_section_capacity = section.len().saturating_sub(4) / FRAMED_BLOB_ENTRY_LEN;
        if u64::from(blob_file_count) > blob_section_capacity {
            if tolerate_tail {
                log::warn!(
                    "blob_files: declared count={blob_file_count} exceeds section \
                     capacity (~{blob_section_capacity} entries) in version \
                     #{curr_version_id}; tail-tolerant mode walks \
                     bytes-actually-present and stops at the first EOF",
                );
            } else {
                return Err(crate::Error::Unrecoverable);
            }
        }

        let cap_hint =
            usize::try_from(u64::from(blob_file_count).min(blob_section_capacity)).unwrap_or(0);
        let mut blob_file_ids = Vec::with_capacity(cap_hint);
        let mut blob_bytes_consumed: u64 = 4; // count u32 already read

        for _ in 0..blob_file_count {
            let remaining = section.len().saturating_sub(blob_bytes_consumed);
            // Fixed-length pin on the blob_files record (same
            // SkipAny resync safety net as the tables section).
            let outcome = crate::version::framing::read_framed_record(
                &mut reader,
                remaining,
                Some(BLOB_ENTRY_PAYLOAD_LEN),
            )?;
            match outcome {
                FramedRecordOutcome::Ok(payload) => {
                    blob_bytes_consumed +=
                        crate::version::framing::FRAME_HEADER_LEN as u64 + payload.len() as u64;
                    let entry = decode_blob_entry_payload(&payload)?;
                    blob_file_ids.push(entry);
                }
                FramedRecordOutcome::TailTruncation if tolerate_tail => {
                    let recovered = u32::try_from(blob_file_ids.len()).unwrap_or(u32::MAX);
                    blob_dropped_to_tail = blob_file_count.saturating_sub(recovered);
                    break;
                }
                FramedRecordOutcome::ChecksumMismatch { bytes_consumed, .. } if skip_any => {
                    log::warn!(
                        "skip_any: blob_files record checksum mismatch \
                         ({bytes_consumed} bytes) in version \
                         #{curr_version_id}, skipping"
                    );
                    blob_bytes_consumed += bytes_consumed;
                    blob_dropped_to_corruption = blob_dropped_to_corruption.saturating_add(1);
                }
                FramedRecordOutcome::ChecksumMismatch { .. } if pit_prefix => {
                    log::warn!(
                        "pit: blob_files record checksum mismatch in version \
                         #{curr_version_id}; accepting consistent prefix and \
                         dropping the rest of the blob_files section"
                    );
                    let recovered = u32::try_from(blob_file_ids.len()).unwrap_or(u32::MAX);
                    blob_dropped_to_corruption = blob_file_count.saturating_sub(recovered);
                    break;
                }
                FramedRecordOutcome::ChecksumMismatch { expected, got, .. } => {
                    return Err(crate::Error::ManifestFrameChecksumMismatch {
                        section: "blob_files",
                        expected,
                        got,
                    });
                }
                FramedRecordOutcome::BadHeader if skip_any || pit_prefix => {
                    log::warn!(
                        "blob_files: corrupted framing header in version \
                         #{curr_version_id}; remaining records unrecoverable"
                    );
                    let recovered = u32::try_from(blob_file_ids.len()).unwrap_or(u32::MAX);
                    blob_dropped_to_corruption = blob_file_count.saturating_sub(recovered);
                    break;
                }
                // Strict mode: same Tail vs BadHeader split as the
                // tables section above. TailTruncation surfaces as
                // Io(UnexpectedEof) for diagnostic parity with the
                // pre-framing reader; BadHeader stays Unrecoverable
                // because the writer never produces a header above
                // MAX_FRAME_PAYLOAD on its own.
                FramedRecordOutcome::TailTruncation => {
                    return Err(crate::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "manifest blob_files record truncated mid-frame",
                    )));
                }
                FramedRecordOutcome::BadHeader => {
                    return Err(crate::Error::Unrecoverable);
                }
            }
        }

        blob_file_ids.sort_by_key(|(id, _)| *id);
        blob_file_ids
    };

    if blob_dropped_to_tail > 0 || blob_dropped_to_corruption > 0 {
        log::warn!(
            "manifest blob_files recovery summary for version #{curr_version_id}: \
             {blob_dropped_to_tail} blob-file record(s) dropped to tail-truncation, \
             {blob_dropped_to_corruption} dropped to per-record corruption \
             (skip_any/pit modes); recovered tree may be missing blob files",
        );
    }

    debug_assert!(blob_file_ids.is_sorted_by_key(|(id, _)| id));

    let gc_stats = {
        let section = toc
            .section(b"blob_gc_stats")
            .ok_or(crate::Error::Unrecoverable)
            .inspect_err(|_| {
                log::error!("blob_gc_stats section not found in version #{curr_version_id} - maybe the file is corrupted?");
            })?;

        let mut reader = open_section_reader(fs, &version_file_path, section)?;

        // Same tail-tolerance contract as the record-list sections:
        // a power-loss between the `blob_files` commit and the
        // `blob_gc_stats` payload landing surfaces as
        // `UnexpectedEof` inside `FragmentationMap::decode_from`.
        // Strict mode aborts; tolerant mode warns and uses an empty
        // FragmentationMap (the GC stats are advisory — fragmentation
        // re-accrues on subsequent compactions, so dropping them is
        // a "rebuild on next pass" outcome rather than data loss).
        match crate::blob_tree::FragmentationMap::decode_from(&mut reader) {
            Ok(m) => m,
            Err(crate::Error::Io(e))
                if tolerate_tail && e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                log::warn!(
                    "blob_gc_stats section truncated in version #{curr_version_id}; \
                     tail-tolerant mode produces an empty FragmentationMap (GC stats \
                     will rebuild on the next compaction pass)"
                );
                crate::blob_tree::FragmentationMap::default()
            }
            Err(e) => return Err(e),
        }
    };

    Ok(Recovery {
        tree_type: {
            let section = toc.section(b"tree_type").ok_or(crate::Error::Unrecoverable)
            .inspect_err(|_|{
                log::error!("tree_type section not found in version #{curr_version_id} - maybe the file is corrupted?");
            })?;

            let mut reader = open_section_reader(fs, &version_file_path, section)?;
            let byte = reader.read_u8()?;

            TreeType::try_from(byte).map_err(|()| crate::Error::InvalidHeader("TreeType"))?
        },
        curr_version_id,
        table_ids: levels,
        blob_file_ids,
        gc_stats,
    })
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions"
)]
mod tests {
    use super::*;
    use crate::fs::{FsOpenOptions, MemFs};
    use byteorder::{LittleEndian, WriteBytesExt};
    use std::io::Write;

    /// Write a CURRENT pointer so `recover()` can find the version file.
    fn write_current(folder: &Path, version_id: u64, fs: &dyn Fs) -> crate::Result<()> {
        let path = folder.join(CURRENT_VERSION_FILE);
        let mut f = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        f.write_u64::<LittleEndian>(version_id)?;
        f.write_u128::<LittleEndian>(0)?; // checksum placeholder
        f.write_u8(0)?; // checksum type
        Ok(())
    }

    /// Write a version sfa archive with a corrupt `table_count` (`u32::MAX`).
    ///
    /// All four sfa sections are written because `recover()` requires them
    /// all — only the tables section carries the corrupt payload.
    fn write_corrupt_table_count(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
        let path = folder.join(format!("v{id}"));
        let file = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut w = sfa::Writer::from_writer(std::io::BufWriter::new(file));

        w.start("tree_type")?;
        w.write_u8(0)?;

        w.start("tables")?;
        w.write_u8(1)?; // 1 level
        w.write_u8(1)?; // 1 run
        w.write_u32::<LittleEndian>(u32::MAX)?; // corrupt: exceeds section length

        w.start("blob_files")?;
        w.write_u32::<LittleEndian>(0)?;

        w.start("blob_gc_stats")?;
        w.write_u32::<LittleEndian>(0)?;

        w.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => crate::Error::Unrecoverable,
        })?;
        Ok(())
    }

    /// Write a version sfa archive with a corrupt `blob_file_count` (`u32::MAX`).
    ///
    /// All four sfa sections required by `recover()` are present — only the
    /// `blob_files` section carries the corrupt payload.
    fn write_corrupt_blob_count(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
        let path = folder.join(format!("v{id}"));
        let file = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut w = sfa::Writer::from_writer(std::io::BufWriter::new(file));

        w.start("tree_type")?;
        w.write_u8(0)?;

        w.start("tables")?;
        w.write_u8(0)?; // 0 levels

        w.start("blob_files")?;
        w.write_u32::<LittleEndian>(u32::MAX)?; // corrupt

        w.start("blob_gc_stats")?;
        w.write_u32::<LittleEndian>(0)?;

        w.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => crate::Error::Unrecoverable,
        })?;
        Ok(())
    }

    #[test]
    fn recover_rejects_corrupt_table_count() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/corrupt/tables");
        fs.create_dir_all(folder)?;

        write_current(folder, 1, &fs)?;
        write_corrupt_table_count(folder, 1, &fs)?;

        let Err(err) = recover(folder, &fs, ManifestRecoveryMode::AbsoluteConsistency) else {
            panic!("corrupt table_count should fail");
        };
        assert!(
            matches!(err, crate::Error::Unrecoverable),
            "expected Unrecoverable, got: {err:?}"
        );

        Ok(())
    }

    #[test]
    fn recover_rejects_corrupt_blob_file_count() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/corrupt/blobs");
        fs.create_dir_all(folder)?;

        write_current(folder, 1, &fs)?;
        write_corrupt_blob_count(folder, 1, &fs)?;

        let Err(err) = recover(folder, &fs, ManifestRecoveryMode::AbsoluteConsistency) else {
            panic!("corrupt blob_file_count should fail");
        };
        assert!(
            matches!(err, crate::Error::Unrecoverable),
            "expected Unrecoverable, got: {err:?}"
        );

        Ok(())
    }

    /// Writes a `vN` archive whose `tables` section claims more table
    /// entries than are actually present (count says 5, only 1 full
    /// entry's worth of bytes is written, the next entry's `id: u64`
    /// is cut off mid-stream). Used to exercise the
    /// `TolerateCorruptedTailRecords` recovery path.
    ///
    /// The `id` parameter selects the version file name; the function
    /// otherwise produces a deterministic shape: 1 level, 1 run,
    /// declared count = `declared`, actual full entries = `actual`.
    fn write_truncated_tables_tail(
        folder: &Path,
        id: u64,
        declared: u32,
        actual: u32,
        fs: &dyn Fs,
    ) -> crate::Result<()> {
        assert!(
            actual < declared,
            "actual must be < declared for truncation"
        );
        let path = folder.join(format!("v{id}"));
        let file = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut w = sfa::Writer::from_writer(std::io::BufWriter::new(file));

        w.start("tree_type")?;
        w.write_u8(0)?;

        w.start("tables")?;
        w.write_u8(1)?; // 1 level
        w.write_u8(1)?; // 1 run
        w.write_u32::<LittleEndian>(declared)?;
        // Write `actual` complete framed entries, then stop. SFA pads
        // the section length to whatever bytes we wrote — the
        // truncation surfaces inside the per-entry decode loop, not
        // at the SFA layer.
        for entry_id in 0..actual {
            crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
                payload.write_u64::<LittleEndian>(u64::from(entry_id))?;
                payload.write_u8(0)?; // checksum_type
                payload.write_u128::<LittleEndian>(0)?; // checksum
                payload.write_u64::<LittleEndian>(0)?; // global_seqno
                Ok(())
            })?;
        }

        w.start("blob_files")?;
        w.write_u32::<LittleEndian>(0)?;

        w.start("blob_gc_stats")?;
        w.write_u32::<LittleEndian>(0)?;

        w.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => crate::Error::Unrecoverable,
        })?;
        Ok(())
    }

    #[test]
    fn recover_absolute_consistency_rejects_truncated_tables_tail() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/absolute/tail");
        fs.create_dir_all(folder)?;

        write_current(folder, 1, &fs)?;
        // Section declares 5 entries, only 1 actually written.
        write_truncated_tables_tail(folder, 1, 5, 1, &fs)?;

        let result = recover(folder, &fs, ManifestRecoveryMode::AbsoluteConsistency);
        let err = result.expect_err("truncated tail must abort under AbsoluteConsistency");
        // Either Io(UnexpectedEof) (from the byteorder read) — both are
        // acceptable strict-mode failures. The contract is: SOMETHING
        // surfaces, the open does not silently succeed with partial data.
        assert!(
            matches!(&err, crate::Error::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof)
                || matches!(err, crate::Error::Unrecoverable),
            "expected UnexpectedEof or Unrecoverable, got: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn recover_tolerate_tail_keeps_consistent_prefix_of_tables() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/tolerate/tail");
        fs.create_dir_all(folder)?;

        write_current(folder, 1, &fs)?;
        // Section declares 5 entries, only 1 actually written → expect
        // 1 entry recovered, 4 silently dropped + warn logged.
        write_truncated_tables_tail(folder, 1, 5, 1, &fs)?;

        let recovery = recover(
            folder,
            &fs,
            ManifestRecoveryMode::TolerateCorruptedTailRecords,
        )?;
        assert_eq!(
            recovery.table_ids.len(),
            1,
            "expected 1 level, got {}: {:?}",
            recovery.table_ids.len(),
            recovery.table_ids.iter().map(Vec::len).collect::<Vec<_>>(),
        );
        let level = &recovery.table_ids[0];
        assert_eq!(level.len(), 1, "expected 1 run in the recovered level");
        assert_eq!(
            level[0].len(),
            1,
            "expected 1 table record (the consistent prefix); got {}",
            level[0].len(),
        );
        Ok(())
    }

    #[test]
    fn recover_tolerate_tail_does_not_swallow_invalid_tag() -> crate::Result<()> {
        // A corrupt non-zero `checksum_type` byte is NOT a clean tail
        // truncation; the tail-tolerant mode must still abort on it.
        // Otherwise it'd silently drop the bad record plus everything
        // after it on bit-rot, which is the opposite of the documented
        // contract (tail-tolerance is for write-incomplete scenarios,
        // not for arbitrary corruption).
        let fs = MemFs::new();
        let folder = Path::new("/tolerate/bad_tag");
        fs.create_dir_all(folder)?;

        write_current(folder, 1, &fs)?;
        let path = folder.join("v1");
        let file = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut w = sfa::Writer::from_writer(std::io::BufWriter::new(file));
        w.start("tree_type")?;
        w.write_u8(0)?;
        w.start("tables")?;
        w.write_u8(1)?; // 1 level
        w.write_u8(1)?; // 1 run
        w.write_u32::<LittleEndian>(1)?; // 1 entry
        // Framed record with a corrupt `checksum_type` byte in the
        // payload. The framing XXH3 still covers the payload, so the
        // record decodes cleanly at the framing layer; the InvalidTag
        // surfaces from `decode_table_entry_payload` and aborts even
        // under tolerant modes (the contract: tail-tolerance is for
        // write-incomplete scenarios, not arbitrary corruption).
        crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
            payload.write_u64::<LittleEndian>(0)?; // id
            payload.write_u8(0xFF)?; // corrupt checksum_type
            payload.write_u128::<LittleEndian>(0)?;
            payload.write_u64::<LittleEndian>(0)?;
            Ok(())
        })?;
        w.start("blob_files")?;
        w.write_u32::<LittleEndian>(0)?;
        w.start("blob_gc_stats")?;
        w.write_u32::<LittleEndian>(0)?;
        w.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => crate::Error::Unrecoverable,
        })?;

        let result = recover(
            folder,
            &fs,
            ManifestRecoveryMode::TolerateCorruptedTailRecords,
        );
        let err =
            result.expect_err("InvalidTag must still abort under TolerateCorruptedTailRecords");
        assert!(
            matches!(err, crate::Error::InvalidTag(("ChecksumType", 0xFF))),
            "expected InvalidTag, got: {err:?}",
        );
        Ok(())
    }

    /// Writes a `vN` archive with two runs in one level:
    /// run #0 is a complete entry (declared = actual = 1),
    /// run #1 declares 3 entries but the section is cut so the
    /// `table_count` u32 of run #1 reads partially → EOF.
    /// Exercises the tail-tolerant case where the cut happens mid-RUN
    /// inside an otherwise-valid level. The consistent prefix (run #0
    /// of level #0) MUST survive in the recovered Version.
    fn write_truncated_at_second_run(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
        let path = folder.join(format!("v{id}"));
        let file = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut w = sfa::Writer::from_writer(std::io::BufWriter::new(file));
        w.start("tree_type")?;
        w.write_u8(0)?;
        w.start("tables")?;
        w.write_u8(1)?; // 1 level
        w.write_u8(2)?; // 2 runs in that level
        // run #0: declared=1, actual=1 — the consistent prefix.
        w.write_u32::<LittleEndian>(1)?;
        crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
            payload.write_u64::<LittleEndian>(42)?; // id
            payload.write_u8(0)?; // checksum_type
            payload.write_u128::<LittleEndian>(0)?;
            payload.write_u64::<LittleEndian>(0)?;
            Ok(())
        })?;
        // run #1: only 2 of the 4 bytes of `table_count` are written.
        // The reader gets `UnexpectedEof` on the u32 read.
        w.write_u8(0xAA)?;
        w.write_u8(0xBB)?;
        w.start("blob_files")?;
        w.write_u32::<LittleEndian>(0)?;
        w.start("blob_gc_stats")?;
        w.write_u32::<LittleEndian>(0)?;
        w.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => crate::Error::Unrecoverable,
        })?;
        Ok(())
    }

    #[test]
    fn recover_tolerate_tail_keeps_consistent_prefix_within_a_level() -> crate::Result<()> {
        // Regression: when the EOF cut happens between two runs of
        // the same level, the tail-tolerant break must push the
        // partial level into `levels` so the consistent prefix
        // (run #0 with 1 entry) survives. Earlier behaviour broke
        // out of `'levels` without pushing, silently dropping the
        // already-decoded run.
        let fs = MemFs::new();
        let folder = Path::new("/tolerate/midlevel");
        fs.create_dir_all(folder)?;

        write_current(folder, 1, &fs)?;
        write_truncated_at_second_run(folder, 1, &fs)?;

        let recovery = recover(
            folder,
            &fs,
            ManifestRecoveryMode::TolerateCorruptedTailRecords,
        )?;
        assert_eq!(
            recovery.table_ids.len(),
            1,
            "expected the partially-decoded level to be present, got {} levels",
            recovery.table_ids.len(),
        );
        let level = &recovery.table_ids[0];
        assert_eq!(
            level.len(),
            1,
            "expected 1 surviving run (the consistent prefix), got {}",
            level.len(),
        );
        assert_eq!(
            level[0].len(),
            1,
            "expected the run to contain its 1 fully-decoded entry",
        );
        assert_eq!(level[0][0].id, 42, "wrong entry id recovered");
        Ok(())
    }

    /// Writes a `vN` archive whose `blob_files` section declares
    /// `declared` entries but only writes `actual` complete 25-byte
    /// records, cutting mid-stream after that. Mirrors the analogous
    /// `tables` fixture for the blob-files surface.
    fn write_truncated_blob_tail(
        folder: &Path,
        id: u64,
        declared: u32,
        actual: u32,
        fs: &dyn Fs,
    ) -> crate::Result<()> {
        assert!(
            actual < declared,
            "actual must be < declared for truncation"
        );
        let path = folder.join(format!("v{id}"));
        let file = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut w = sfa::Writer::from_writer(std::io::BufWriter::new(file));
        w.start("tree_type")?;
        w.write_u8(0)?;
        w.start("tables")?;
        w.write_u8(0)?; // 0 levels
        w.start("blob_files")?;
        w.write_u32::<LittleEndian>(declared)?;
        for entry_id in 0..actual {
            crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
                payload.write_u64::<LittleEndian>(u64::from(entry_id))?;
                payload.write_u8(0)?; // checksum_type
                payload.write_u128::<LittleEndian>(0)?; // checksum
                Ok(())
            })?;
        }
        w.start("blob_gc_stats")?;
        w.write_u32::<LittleEndian>(0)?;
        w.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => crate::Error::Unrecoverable,
        })?;
        Ok(())
    }

    #[test]
    fn recover_tolerate_tail_keeps_consistent_prefix_of_blob_files() -> crate::Result<()> {
        // Companion to `recover_tolerate_tail_keeps_consistent_prefix_of_tables`
        // for the blob_files surface. Without this test, a regression
        // in the blob-files tail-tolerant path would slip through
        // because the tables-only test already passes.
        let fs = MemFs::new();
        let folder = Path::new("/tolerate/blob_tail");
        fs.create_dir_all(folder)?;

        write_current(folder, 1, &fs)?;
        write_truncated_blob_tail(folder, 1, 5, 1, &fs)?;

        let recovery = recover(
            folder,
            &fs,
            ManifestRecoveryMode::TolerateCorruptedTailRecords,
        )?;
        assert_eq!(
            recovery.blob_file_ids.len(),
            1,
            "expected 1 surviving blob_file entry (the consistent prefix), got {}",
            recovery.blob_file_ids.len(),
        );
        assert_eq!(
            recovery.blob_file_ids[0].0, 0,
            "wrong blob_file id recovered",
        );
        Ok(())
    }

    /// Writes a `vN` archive whose `blob_gc_stats` section is
    /// truncated to zero bytes. Mimics a power-loss right after the
    /// `blob_files` section was committed but before the
    /// `blob_gc_stats` payload landed — `FragmentationMap::decode_from`
    /// hits `UnexpectedEof` on the first byte.
    fn write_truncated_blob_gc_stats(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
        let path = folder.join(format!("v{id}"));
        let file = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut w = sfa::Writer::from_writer(std::io::BufWriter::new(file));
        w.start("tree_type")?;
        w.write_u8(0)?;
        w.start("tables")?;
        w.write_u8(0)?; // 0 levels
        w.start("blob_files")?;
        w.write_u32::<LittleEndian>(0)?;
        // blob_gc_stats section started but no payload written —
        // section.len() == 0, FragmentationMap::decode_from will
        // surface UnexpectedEof on its first read.
        w.start("blob_gc_stats")?;
        w.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => crate::Error::Unrecoverable,
        })?;
        Ok(())
    }

    #[test]
    fn recover_tolerate_tail_handles_truncated_blob_gc_stats() -> crate::Result<()> {
        // Tail-tolerant mode must extend beyond the record-list
        // sections (tables / blob_files): a power-loss between the
        // blob_files commit and the blob_gc_stats payload is the
        // exact "writer never finished" shape this mode is meant to
        // salvage. Strict mode still aborts; tolerant mode emits a
        // warn and uses a default (empty) FragmentationMap.
        let fs = MemFs::new();
        let folder = Path::new("/tolerate/gc_stats");
        fs.create_dir_all(folder)?;
        write_current(folder, 1, &fs)?;
        write_truncated_blob_gc_stats(folder, 1, &fs)?;

        // Strict mode: hard fail.
        let strict = recover(folder, &fs, ManifestRecoveryMode::AbsoluteConsistency);
        assert!(
            strict.is_err(),
            "strict mode must abort on truncated blob_gc_stats; got Ok",
        );

        // Tolerant mode: succeeds with empty gc_stats. Compare via
        // `Default` (FragmentationMap implements PartialEq) — the
        // type does not expose an `is_empty()` accessor.
        let lenient = recover(
            folder,
            &fs,
            ManifestRecoveryMode::TolerateCorruptedTailRecords,
        )?;
        assert_eq!(
            lenient.gc_stats,
            crate::blob_tree::FragmentationMap::default(),
            "tolerant mode must produce default (empty) gc_stats on truncated section",
        );
        Ok(())
    }

    // ====================================================================
    // PointInTimeRecovery + SkipAnyCorruptedRecords corruption-matrix tests
    // ====================================================================
    //
    // The fixtures below write a complete framed manifest, then pick one
    // specific framed record and emit it with a deliberately-wrong XXH3
    // digest. That gives the reader a `FramedRecordOutcome::ChecksumMismatch`
    // at a known position so each mode's per-record dispatch is exercised
    // end-to-end, not just at the framing-helper unit level.

    /// Writes one framed table record with a CORRECT XXH3 digest.
    fn write_good_table_record<W: std::io::Write>(w: &mut W, id: u64) -> crate::Result<()> {
        crate::version::framing::write_framed_record(w, &mut Vec::new(), |payload| {
            payload.write_u64::<LittleEndian>(id)?;
            payload.write_u8(0)?; // checksum_type
            payload.write_u128::<LittleEndian>(0)?;
            payload.write_u64::<LittleEndian>(0)?;
            Ok(())
        })
    }

    /// Writes one framed table record but with an INTENTIONALLY WRONG
    /// XXH3 digest in the framing header — emulates payload bit-rot
    /// inside an otherwise structurally-valid record. The `len` field
    /// of the header is correct (so the reader's `BadHeader` path does
    /// NOT trigger), which means the `ChecksumMismatch` arm is the one
    /// being exercised.
    fn write_bad_table_record<W: std::io::Write>(w: &mut W, id: u64) -> crate::Result<()> {
        let mut payload: Vec<u8> = Vec::new();
        payload.write_u64::<LittleEndian>(id)?;
        payload.write_u8(0)?;
        payload.write_u128::<LittleEndian>(0)?;
        payload.write_u64::<LittleEndian>(0)?;

        #[expect(
            clippy::cast_possible_truncation,
            reason = "payload is 33 bytes — fits in u32"
        )]
        let len = payload.len() as u32;
        w.write_u32::<LittleEndian>(len)?;
        // INTENTIONALLY WRONG digest. Real one would be
        // `xxh3_64(&payload)`; using `0xDEAD_BEEF_DEAD_BEEF` instead
        // so the reader's mismatch arm fires deterministically.
        w.write_u64::<LittleEndian>(0xDEAD_BEEF_DEAD_BEEF)?;
        w.write_all(&payload)?;
        Ok(())
    }

    /// Builds a manifest with two levels: level 0 has one run with three
    /// table records, where the MIDDLE record carries a corrupt XXH3
    /// digest. Level 1 has one run with two good records.
    ///
    /// The shape lets a test observe three distinct recovery outcomes
    /// from the same on-disk bytes:
    /// - `AbsoluteConsistency` aborts on the corrupt record
    /// - `PointInTimeRecovery` keeps level 0 record #0 only (dropping
    ///   #1 and #2 of the current run, and dropping level 1 entirely)
    /// - `SkipAnyCorruptedRecords` keeps level 0 records #0 and #2
    ///   (skipping #1) AND keeps level 1
    fn write_manifest_with_mid_record_corruption(
        folder: &Path,
        id: u64,
        fs: &dyn Fs,
    ) -> crate::Result<()> {
        let path = folder.join(format!("v{id}"));
        let file = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut w = sfa::Writer::from_writer(std::io::BufWriter::new(file));

        w.start("tree_type")?;
        w.write_u8(0)?;

        w.start("tables")?;
        w.write_u8(2)?; // 2 levels
        // Level 0: 1 run, 3 records, middle one is corrupt.
        w.write_u8(1)?;
        w.write_u32::<LittleEndian>(3)?;
        write_good_table_record(&mut w, 100)?;
        write_bad_table_record(&mut w, 101)?;
        write_good_table_record(&mut w, 102)?;
        // Level 1: 1 run, 2 records, both good.
        w.write_u8(1)?;
        w.write_u32::<LittleEndian>(2)?;
        write_good_table_record(&mut w, 200)?;
        write_good_table_record(&mut w, 201)?;

        w.start("blob_files")?;
        w.write_u32::<LittleEndian>(0)?;
        w.start("blob_gc_stats")?;
        w.write_u32::<LittleEndian>(0)?;
        w.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => crate::Error::Unrecoverable,
        })?;
        Ok(())
    }

    #[test]
    fn recover_absolute_consistency_rejects_mid_record_corruption() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/absolute/mid_corrupt");
        fs.create_dir_all(folder)?;
        write_current(folder, 1, &fs)?;
        write_manifest_with_mid_record_corruption(folder, 1, &fs)?;

        let err = recover(folder, &fs, ManifestRecoveryMode::AbsoluteConsistency)
            .expect_err("corrupt record must abort AbsoluteConsistency");
        assert!(
            matches!(
                err,
                crate::Error::ManifestFrameChecksumMismatch {
                    section: "tables",
                    ..
                }
            ),
            "expected ManifestFrameChecksumMismatch on the tables section, got: {err:?}",
        );
        Ok(())
    }

    #[test]
    fn recover_pit_truncates_at_corrupt_record() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/pit/mid_corrupt");
        fs.create_dir_all(folder)?;
        write_current(folder, 1, &fs)?;
        write_manifest_with_mid_record_corruption(folder, 1, &fs)?;

        let recovery = recover(folder, &fs, ManifestRecoveryMode::PointInTimeRecovery)?;

        // PIT contract: drop in-progress run + level + all subsequent
        // levels. The recovered prefix is level 0 with run 0
        // containing only the consistent prefix record (id=100). The
        // implementation pushes the partial run into the partial
        // level, so the level slot survives even though its run is
        // truncated; level 1 was never reached.
        assert_eq!(
            recovery.table_ids.len(),
            1,
            "expected only level 0 to survive (level 1 truncated by PIT); got {}",
            recovery.table_ids.len(),
        );
        let level = &recovery.table_ids[0];
        assert_eq!(level.len(), 1, "expected 1 run in level 0");
        let run = &level[0];
        assert_eq!(
            run.len(),
            1,
            "expected only the pre-corruption record to survive in run 0; got {} records",
            run.len(),
        );
        assert_eq!(run[0].id, 100, "expected id=100 (the good prefix record)");
        Ok(())
    }

    #[test]
    fn recover_skip_any_skips_corrupt_record_and_keeps_neighbours() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/skip_any/mid_corrupt");
        fs.create_dir_all(folder)?;
        write_current(folder, 1, &fs)?;
        write_manifest_with_mid_record_corruption(folder, 1, &fs)?;

        let recovery = recover(folder, &fs, ManifestRecoveryMode::SkipAnyCorruptedRecords)?;

        // SkipAny contract: log the single bad record, advance past
        // it via the framing length header, keep going. Both
        // surrounding records and the entire next level survive.
        assert_eq!(
            recovery.table_ids.len(),
            2,
            "expected both levels to survive under SkipAny",
        );
        let l0_run = &recovery.table_ids[0][0];
        assert_eq!(
            l0_run.len(),
            2,
            "expected 2 records in level-0 run 0 (id=100 + id=102, skipping the corrupt id=101); got {}",
            l0_run.len(),
        );
        assert_eq!(l0_run[0].id, 100);
        assert_eq!(l0_run[1].id, 102);

        let l1_run = &recovery.table_ids[1][0];
        assert_eq!(
            l1_run.len(),
            2,
            "expected level 1 to recover its full 2 records under SkipAny",
        );
        assert_eq!(l1_run[0].id, 200);
        assert_eq!(l1_run[1].id, 201);
        Ok(())
    }

    /// Builds a manifest where a `blob_files` record (not a table
    /// record) carries the corrupt XXH3 digest. Exercises the same
    /// per-mode dispatch on the `blob_files` section to confirm the
    /// reader's PIT / `SkipAny` logic was wired through symmetrically.
    fn write_manifest_with_corrupt_blob_record(
        folder: &Path,
        id: u64,
        fs: &dyn Fs,
    ) -> crate::Result<()> {
        let path = folder.join(format!("v{id}"));
        let file = fs.open(
            &path,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        let mut w = sfa::Writer::from_writer(std::io::BufWriter::new(file));

        w.start("tree_type")?;
        w.write_u8(0)?;

        w.start("tables")?;
        w.write_u8(0)?; // 0 levels (focus is on blob_files section)

        w.start("blob_files")?;
        w.write_u32::<LittleEndian>(3)?;
        // good, bad, good
        crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
            payload.write_u64::<LittleEndian>(10)?;
            payload.write_u8(0)?;
            payload.write_u128::<LittleEndian>(0)?;
            Ok(())
        })?;
        // Corrupt the middle blob record: write a framed header with
        // a wrong digest but a correct length, so the reader treats
        // it as ChecksumMismatch.
        let mut payload: Vec<u8> = Vec::new();
        payload.write_u64::<LittleEndian>(11)?;
        payload.write_u8(0)?;
        payload.write_u128::<LittleEndian>(0)?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "payload is 25 bytes — fits in u32"
        )]
        let len = payload.len() as u32;
        w.write_u32::<LittleEndian>(len)?;
        w.write_u64::<LittleEndian>(0xDEAD_BEEF_DEAD_BEEF)?;
        w.write_all(&payload)?;
        crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
            payload.write_u64::<LittleEndian>(12)?;
            payload.write_u8(0)?;
            payload.write_u128::<LittleEndian>(0)?;
            Ok(())
        })?;

        w.start("blob_gc_stats")?;
        w.write_u32::<LittleEndian>(0)?;
        w.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => crate::Error::Unrecoverable,
        })?;
        Ok(())
    }

    #[test]
    fn recover_skip_any_skips_corrupt_blob_record() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/skip_any/blob_mid_corrupt");
        fs.create_dir_all(folder)?;
        write_current(folder, 1, &fs)?;
        write_manifest_with_corrupt_blob_record(folder, 1, &fs)?;

        let recovery = recover(folder, &fs, ManifestRecoveryMode::SkipAnyCorruptedRecords)?;
        let ids: Vec<u64> = recovery.blob_file_ids.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            ids,
            vec![10, 12],
            "expected SkipAny to keep ids 10 and 12 while skipping the corrupt id 11",
        );
        Ok(())
    }

    #[test]
    fn recover_pit_truncates_remaining_blob_records_on_corruption() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/pit/blob_mid_corrupt");
        fs.create_dir_all(folder)?;
        write_current(folder, 1, &fs)?;
        write_manifest_with_corrupt_blob_record(folder, 1, &fs)?;

        let recovery = recover(folder, &fs, ManifestRecoveryMode::PointInTimeRecovery)?;
        let ids: Vec<u64> = recovery.blob_file_ids.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            ids,
            vec![10],
            "PIT must drop the corrupt blob record AND every blob record after it; \
             expected only id=10 (the good prefix), got {ids:?}",
        );
        Ok(())
    }
}
