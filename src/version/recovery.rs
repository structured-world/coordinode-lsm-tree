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

/// Per-section counters tracking how many records were dropped during
/// tolerant / PIT / `SkipAny` recovery and why. Exposed as part of
/// [`Recovery`] so operators (and integration tests) can verify the
/// recovery outcome without parsing log output.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Table records dropped because the writer was cut mid-record /
    /// mid-run-header / mid-level-header (tail truncation).
    pub tables_dropped_to_tail: u32,
    /// Table records dropped because their framing checksum or
    /// header failed — i.e. real bit-rot inside an otherwise-written
    /// region (only ever non-zero under `SkipAny` / PIT modes).
    pub tables_dropped_to_corruption: u32,
    /// Number of level / run / table-count header *fields* truncated
    /// by tail-cutting (count of EVENTS, not bytes — incremented by
    /// 1 per truncation regardless of whether the cut-mid field is
    /// 1 byte (`run_count` u8) or 4 bytes (`table_count` u32)). No
    /// per-entry bytes are lost when this counter is non-zero — the
    /// writer didn't even finish writing the count header for the
    /// level or run, so no records were supposed to be present
    /// yet. Distinguished from record-drop accounting so the
    /// summary log can report "K headers truncated" vs "M records
    /// missing" honestly.
    pub tables_truncated_headers: u32,
    /// Blob-file records dropped to tail truncation (analogous to
    /// [`Self::tables_dropped_to_tail`] for the `blob_files` section).
    pub blob_dropped_to_tail: u32,
    /// Blob-file records dropped to per-record corruption.
    pub blob_dropped_to_corruption: u32,
}

#[derive(Debug)]
pub struct Recovery {
    pub tree_type: TreeType,
    pub curr_version_id: VersionId,
    pub table_ids: Vec<Vec<Vec<RecoveredTable>>>,
    pub blob_file_ids: Vec<(BlobFileId, Checksum)>,
    pub gc_stats: crate::blob_tree::FragmentationMap,
    /// Per-section counters describing how many records were dropped
    /// during this recovery. Always zero under
    /// [`ManifestRecoveryMode::AbsoluteConsistency`] (any corruption
    /// or truncation aborts before returning a [`Recovery`]).
    ///
    /// `#[expect]` would be unfulfilled in test builds (the
    /// integration tests read this field), and `#[allow]` would be
    /// noisy in builds where tests are off. Gate `#[expect]` to
    /// non-test builds: under `cargo test` the field IS read so
    /// the lint doesn't fire and the expectation isn't attached;
    /// under `cargo build` the field is unread by in-tree code
    /// (operator-facing telemetry surface only) and the
    /// expectation is fulfilled.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "operator-facing telemetry surface; in-tree \
                      consumers reach Recovery via the public API"
        )
    )]
    pub stats: RecoveryStats,
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

    // Scratch buffer threaded through every `read_framed_record`
    // call across both the `tables` and `blob_files` sections.
    // Grows once to the largest record's payload size (33 bytes
    // for tables, 25 for blob_files) and is reused thereafter, so
    // per-record heap allocations during recovery are zero after
    // the initial growth.
    let mut read_scratch: Vec<u8> = Vec::with_capacity(64);

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
                // Records lost to corruption in THIS run (SkipAny
                // skips, PIT/SkipAny BadHeader drops). Used at
                // tail-truncation to compute the tail-attributed
                // miss-count honestly: tail = table_count -
                // run.len() - corrupted_in_run. Without this, the
                // already-corruption-counted records get
                // re-attributed as tail drops in the summary log.
                let mut corrupted_in_run: u32 = 0;
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
                    // handles safely. The scratch buffer is reused
                    // across every record in this section to keep
                    // per-record heap allocations at zero.
                    let outcome = crate::version::framing::read_framed_record(
                        &mut reader,
                        remaining,
                        Some(TABLE_ENTRY_PAYLOAD_LEN),
                        &mut read_scratch,
                    )?;
                    match outcome {
                        FramedRecordOutcome::Ok => {
                            tables_bytes_consumed += crate::version::framing::FRAME_HEADER_LEN
                                as u64
                                + read_scratch.len() as u64;
                            // Per-record payload decode can fail even
                            // when the framing checksum verified — a
                            // writer-side bug or astronomically-rare
                            // xxh3 collision could produce frame-
                            // valid bytes that don't decode as a
                            // valid record (e.g., InvalidTag on a
                            // corrupt `checksum_type` byte). Under
                            // SkipAny/PIT this is treated the same
                            // as ChecksumMismatch — skip / stop
                            // respectively — instead of aborting the
                            // whole recovery via `?`. Under strict
                            // mode the error propagates as before.
                            match decode_table_entry_payload(&read_scratch) {
                                Ok(t) => run.push(t),
                                Err(e) if skip_any => {
                                    log::warn!(
                                        "skip_any: tables record decode failed in version \
                                         #{curr_version_id}: {e:?}; skipping",
                                    );
                                    tables_dropped_to_corruption =
                                        tables_dropped_to_corruption.saturating_add(1);
                                    corrupted_in_run = corrupted_in_run.saturating_add(1);
                                }
                                Err(e) if pit_prefix => {
                                    log::warn!(
                                        "pit: tables record decode failed in version \
                                         #{curr_version_id}: {e:?}; accepting consistent \
                                         prefix and dropping the rest of this run + unread \
                                         levels",
                                    );
                                    let recovered = u32::try_from(run.len()).unwrap_or(u32::MAX);
                                    tables_dropped_to_corruption = tables_dropped_to_corruption
                                        .saturating_add(table_count.saturating_sub(recovered));
                                    if !run.is_empty() {
                                        level.push(run);
                                    }
                                    if !level.is_empty() {
                                        levels.push(level);
                                    }
                                    break 'levels;
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        FramedRecordOutcome::TailTruncation if tolerate_tail => {
                            // Subtract BOTH successfully decoded records
                            // (run.len()) AND already-corrupted ones in
                            // this run (corrupted_in_run, only ever
                            // non-zero under SkipAny). Without that,
                            // skipped records get double-counted as
                            // tail drops in the summary log.
                            let recovered = u32::try_from(run.len()).unwrap_or(u32::MAX);
                            let processed = recovered.saturating_add(corrupted_in_run);
                            tables_dropped_to_tail = tables_dropped_to_tail
                                .saturating_add(table_count.saturating_sub(processed));
                            // Skip empty run / empty level: Version::from_recovery
                            // requires non-empty runs (Run::new returns None on empty
                            // input and downstream .expect() panics). When corruption
                            // hits the FIRST record of a run, `run` is empty here.
                            if !run.is_empty() {
                                level.push(run);
                            }
                            if !level.is_empty() {
                                levels.push(level);
                            }
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
                            corrupted_in_run = corrupted_in_run.saturating_add(1);
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
                            // user-visible description. Skip if empty:
                            // when the corrupt record is the FIRST of
                            // the run, the pre-corruption prefix here
                            // is the empty set, and Version::from_recovery
                            // would panic on Run::new(empty).
                            if !run.is_empty() {
                                level.push(run);
                            }
                            if !level.is_empty() {
                                levels.push(level);
                            }
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
                            // Same accounting fix as the TailTruncation
                            // arm: subtract BOTH successfully decoded
                            // (run.len()) AND already-corrupted records
                            // in this run (corrupted_in_run, only ever
                            // non-zero under SkipAny) from the
                            // tail-attributed count, otherwise skipped
                            // records get double-counted here.
                            let recovered = u32::try_from(run.len()).unwrap_or(u32::MAX);
                            let processed = recovered.saturating_add(corrupted_in_run);
                            tables_dropped_to_corruption = tables_dropped_to_corruption
                                .saturating_add(table_count.saturating_sub(processed));
                            // Skip empty run / level for the same reason
                            // as the other early-exit arms — see comment
                            // above on the TailTruncation arm.
                            if !run.is_empty() {
                                level.push(run);
                            }
                            if !level.is_empty() {
                                levels.push(level);
                            }
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
                            // Strict mode: the framing header was
                            // structurally implausible (either `len`
                            // above MAX_FRAME_PAYLOAD or `len` did
                            // not match the fixed table-record
                            // size). Surface as InvalidHeader with
                            // a section-tagged static string so
                            // operators can route on the variant
                            // payload instead of parsing the
                            // Display message.
                            log::error!(
                                "manifest tables frame header rejected in version \
                                 #{curr_version_id}: len out of bounds or != \
                                 TABLE_ENTRY_PAYLOAD_LEN"
                            );
                            return Err(crate::Error::InvalidHeader(
                                "manifest tables frame header",
                            ));
                        }
                    }
                }

                // Guard against empty run: under SkipAnyCorruptedRecords
                // every record in the run may be ChecksumMismatched and
                // skipped, leaving run empty. Version::from_recovery
                // calls Run::new(empty).expect() and panics; drop the
                // empty run instead.
                if !run.is_empty() {
                    level.push(run);
                }
            }

            levels.push(level);
        }

        // Preserve the persisted level_count even if PIT/SkipAny/tail
        // early-exited before reading every level. Downstream code
        // (notably compaction/leveled with 'assert! version.level_count()
        // == 7') reads levels.len() directly; shrinking it would crash
        // the tree after an otherwise-successful recovery. Pad with
        // empty Vec<_> so each persisted slot survives as a structural
        // record. Level::from_runs(empty) is legal.
        while levels.len() < usize::from(level_count) {
            levels.push(Vec::new());
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
        // Records lost to corruption in the blob_files section
        // (SkipAny ChecksumMismatch / BadHeader). Used at
        // TailTruncation to compute the tail-attributed miss-count
        // honestly: tail = blob_file_count - blob_file_ids.len()
        // - blob_corrupted. Same accounting fix as the tables section.
        let mut blob_corrupted: u32 = 0;

        for _ in 0..blob_file_count {
            let remaining = section.len().saturating_sub(blob_bytes_consumed);
            // Fixed-length pin on the blob_files record (same
            // SkipAny resync safety net as the tables section).
            let outcome = crate::version::framing::read_framed_record(
                &mut reader,
                remaining,
                Some(BLOB_ENTRY_PAYLOAD_LEN),
                &mut read_scratch,
            )?;
            match outcome {
                FramedRecordOutcome::Ok => {
                    blob_bytes_consumed += crate::version::framing::FRAME_HEADER_LEN as u64
                        + read_scratch.len() as u64;
                    // Same SkipAny/PIT decode-error handling as the
                    // tables section above — see comment there.
                    match decode_blob_entry_payload(&read_scratch) {
                        Ok(entry) => blob_file_ids.push(entry),
                        Err(e) if skip_any => {
                            log::warn!(
                                "skip_any: blob_files record decode failed in version \
                                 #{curr_version_id}: {e:?}; skipping",
                            );
                            blob_dropped_to_corruption =
                                blob_dropped_to_corruption.saturating_add(1);
                            blob_corrupted = blob_corrupted.saturating_add(1);
                        }
                        Err(e) if pit_prefix => {
                            log::warn!(
                                "pit: blob_files record decode failed in version \
                                 #{curr_version_id}: {e:?}; accepting consistent prefix \
                                 and dropping the rest of the blob_files section",
                            );
                            let recovered = u32::try_from(blob_file_ids.len()).unwrap_or(u32::MAX);
                            blob_dropped_to_corruption = blob_dropped_to_corruption
                                .saturating_add(blob_file_count.saturating_sub(recovered));
                            break;
                        }
                        Err(e) => return Err(e),
                    }
                }
                FramedRecordOutcome::TailTruncation if tolerate_tail => {
                    // Subtract BOTH successfully decoded records AND
                    // already-corrupted ones from the tail-attributed
                    // count, otherwise skipped records get
                    // double-counted as tail drops in the summary log.
                    let recovered = u32::try_from(blob_file_ids.len()).unwrap_or(u32::MAX);
                    let processed = recovered.saturating_add(blob_corrupted);
                    blob_dropped_to_tail = blob_file_count.saturating_sub(processed);
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
                    blob_corrupted = blob_corrupted.saturating_add(1);
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
                    // Accumulator (saturating_add), NOT overwrite: the
                    // ChecksumMismatch arm above has already counted
                    // each skipped record in blob_dropped_to_corruption.
                    // This branch adds the still-unread tail
                    // (blob_file_count - processed where processed =
                    // recovered + blob_corrupted) on top, so total
                    // becomes (already-counted skips) + (unread tail)
                    // = blob_file_count - recovered. Previous '='
                    // assignment dropped the earlier skips' contribution
                    // — under-reporting multi-corruption cases by K
                    // when K records skipped earlier in the same
                    // section before BadHeader fired.
                    let recovered = u32::try_from(blob_file_ids.len()).unwrap_or(u32::MAX);
                    let processed = recovered.saturating_add(blob_corrupted);
                    blob_dropped_to_corruption = blob_dropped_to_corruption
                        .saturating_add(blob_file_count.saturating_sub(processed));
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
                    // Strict mode: same shape as the tables section
                    // — surface a section-tagged InvalidHeader so
                    // operators can distinguish a `blob_files`
                    // frame-header failure from a `tables` one
                    // without parsing the Display message.
                    log::error!(
                        "manifest blob_files frame header rejected in version \
                         #{curr_version_id}: len out of bounds or != \
                         BLOB_ENTRY_PAYLOAD_LEN"
                    );
                    return Err(crate::Error::InvalidHeader(
                        "manifest blob_files frame header",
                    ));
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
        stats: RecoveryStats {
            tables_dropped_to_tail,
            tables_dropped_to_corruption,
            tables_truncated_headers,
            blob_dropped_to_tail,
            blob_dropped_to_corruption,
        },
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

        // PIT contract: drop in-progress run contents + all
        // subsequent records; the recovered prefix is level 0 with
        // run 0 containing only the consistent prefix record (id=100).
        // Level slots are preserved (persisted level_count is 2, so
        // 2 level slots survive — level 1 padded empty to keep
        // downstream level_count() invariants intact).
        assert_eq!(
            recovery.table_ids.len(),
            2,
            "expected both persisted level slots to survive (level 1 padded \
             empty after PIT truncated its records); got {}",
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
        assert!(
            recovery.table_ids[1].is_empty(),
            "expected level 1 to be empty (PIT dropped its records); got {} runs",
            recovery.table_ids[1].len(),
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

    /// Builds a manifest where level 0 has one good record but level 1's
    /// FIRST table record carries a corrupt XXH3 digest. With PIT or
    /// `SkipAny` + `BadHeader` handling, the early-exit branches push the
    /// (empty) in-progress run into the level — and the (empty) level
    /// into the levels vec — before breaking out. The recovered
    /// `Recovery` then carries an empty run, which `Version::from_recovery`
    /// later rejects via `Run::new(...).expect("persisted runs should not
    /// be empty")` — a panic in code that was supposed to be the tolerant
    /// path.
    fn write_manifest_with_corrupt_first_record_of_second_level(
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
        // Level 0: 1 run, 1 good record (so the consistent prefix is
        // non-empty and the PIT/SkipAny path will reach level 1).
        w.write_u8(1)?;
        w.write_u32::<LittleEndian>(1)?;
        write_good_table_record(&mut w, 100)?;
        // Level 1: 1 run, 1 corrupt record AS THE FIRST AND ONLY
        // record. Under PIT this triggers the corruption-handling
        // arm BEFORE any record has been pushed into the new run —
        // run.len() == 0 when the branch pushes it into level.
        w.write_u8(1)?;
        w.write_u32::<LittleEndian>(1)?;
        write_bad_table_record(&mut w, 200)?;

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

    /// Regression test for review finding on PR #342: tolerant/PIT
    /// early-exit branches push the in-progress `run` into the current
    /// `level` regardless of whether the run is empty, and push the
    /// `level` regardless of whether it has any runs. When the FIRST
    /// record of a new run is the corrupt one, the run is empty at the
    /// time the branch fires — `Recovery::table_ids` then carries an
    /// empty inner vec, which `Version::from_recovery` later panics on
    /// via `Run::new(...).expect("persisted runs should not be empty")`.
    /// Invariants under test:
    /// 1. `recover()` must never produce empty runs in `table_ids`.
    /// 2. Empty inner levels (no runs) must not appear — they pass
    ///    `Level::from_runs(vec![])` cleanly downstream but offer no
    ///    information; truncated levels are represented by the slot,
    ///    not by a placeholder run.
    /// 3. The number of level SLOTS in `table_ids` must equal the
    ///    persisted `level_count` — downstream code (compaction/leveled
    ///    asserts `version.level_count() == 7`) reads `levels.len()`
    ///    directly and shrinking it crashes the tree.
    #[test]
    fn recover_pit_drops_empty_run_when_corruption_hits_first_record() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/pit/empty_run");
        fs.create_dir_all(folder)?;
        write_current(folder, 1, &fs)?;
        write_manifest_with_corrupt_first_record_of_second_level(folder, 1, &fs)?;

        let recovery = recover(folder, &fs, ManifestRecoveryMode::PointInTimeRecovery)?;

        // The persisted manifest declared 2 levels. The recovered
        // shape must keep that count — level 1 just has no
        // surviving runs after PIT dropped its only (corrupt) record.
        assert_eq!(
            recovery.table_ids.len(),
            2,
            "expected the recovered shape to preserve the persisted \
             level_count (2); got {} levels",
            recovery.table_ids.len(),
        );

        // No empty runs anywhere in the recovered shape.
        for (level_idx, level) in recovery.table_ids.iter().enumerate() {
            for (run_idx, run) in level.iter().enumerate() {
                assert!(
                    !run.is_empty(),
                    "level {level_idx} run {run_idx} is empty — \
                     Version::from_recovery calls Run::new on this and \
                     panics via the .expect(\"persisted runs should not \
                     be empty\")",
                );
            }
        }

        // Level 0 survived intact with its one good record.
        assert_eq!(recovery.table_ids[0].len(), 1, "level 0 should have 1 run");
        assert_eq!(recovery.table_ids[0][0][0].id, 100);
        // Level 1 had its only record dropped → 0 runs (the slot
        // survives empty, not as a placeholder containing an empty run).
        assert!(
            recovery.table_ids[1].is_empty(),
            "level 1 should have no runs after PIT dropped its corrupt-only run",
        );
        Ok(())
    }

    /// Builds a manifest where level 0 has ONE run of ONE corrupt
    /// record. Under `SkipAnyCorruptedRecords` the single record is
    /// skipped → the run is empty when the per-run record loop
    /// completes → the unconditional `level.push(run)` at line 498
    /// produces an empty run in `Recovery::table_ids`.
    fn write_manifest_with_all_records_in_run_corrupt(
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
        w.write_u8(2)?; // 2 levels persisted
        // Level 0: 1 run, 1 record, all corrupt.
        w.write_u8(1)?;
        w.write_u32::<LittleEndian>(1)?;
        write_bad_table_record(&mut w, 100)?;
        // Level 1: 1 run, 1 good record (so the recovered shape
        // still has surviving content + a non-trivial level slot).
        w.write_u8(1)?;
        w.write_u32::<LittleEndian>(1)?;
        write_good_table_record(&mut w, 200)?;

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

    /// Regression test for the second review finding on PR #342:
    /// after the per-run record loop, `level.push(run)` runs
    /// unconditionally. Under `SkipAnyCorruptedRecords` every record
    /// in a run can be `ChecksumMismatched` → run stays empty → the
    /// unconditional push produces an empty run in Recovery.
    /// Same downstream panic as the first finding: `from_recovery`'s
    /// `Run::new(empty).expect()` aborts the tolerant path.
    #[test]
    fn recover_skip_any_drops_run_when_all_records_corrupt() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/skip_any/all_corrupt_run");
        fs.create_dir_all(folder)?;
        write_current(folder, 1, &fs)?;
        write_manifest_with_all_records_in_run_corrupt(folder, 1, &fs)?;

        let recovery = recover(folder, &fs, ManifestRecoveryMode::SkipAnyCorruptedRecords)?;

        // 2 levels persisted, both survive as slots.
        assert_eq!(recovery.table_ids.len(), 2);
        // Level 0's only run had its only record skipped → no
        // surviving runs (the empty-run placeholder must not be
        // pushed; the level slot itself stays as the structural
        // record that "the writer persisted a level here").
        for (run_idx, run) in recovery.table_ids[0].iter().enumerate() {
            assert!(
                !run.is_empty(),
                "level 0 run {run_idx} is empty in Recovery — \
                 Version::from_recovery's Run::new(empty).expect() panics here",
            );
        }
        // Level 1 survived with its one good record.
        assert_eq!(recovery.table_ids[1].len(), 1);
        assert_eq!(recovery.table_ids[1][0][0].id, 200);
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

    /// Builds a manifest where level 0 declares `table_count = 3` but
    /// only writes 1 good record + 1 corrupt record before truncating
    /// (the writer was killed mid-record). Used to exercise the
    /// `SkipAny` + `TailTruncation` accounting fix: previously
    /// `tables_dropped_to_tail` was computed from `run.len()` alone
    /// (= 1), which re-counted the already-skipped corrupt record as
    /// a tail drop; the correct math subtracts BOTH
    /// successfully-decoded AND skipped-corrupt records.
    fn write_manifest_skip_any_then_tail_truncated(
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
        w.write_u8(1)?; // 1 level
        w.write_u8(1)?; // 1 run
        w.write_u32::<LittleEndian>(3)?; // declared 3 records...
        // ...but only 2 actually written (good + corrupt). The third
        // is implicitly truncated — reader hits UnexpectedEof at the
        // 3rd record's frame header.
        write_good_table_record(&mut w, 100)?;
        write_bad_table_record(&mut w, 101)?;

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

    /// Regression test for review finding on PR #342:
    /// `tables_dropped_to_tail` was being computed from `run.len()`
    /// alone, but under `SkipAnyCorruptedRecords` previously-skipped
    /// corrupt records are NOT in `run` — they were already counted
    /// in `tables_dropped_to_corruption`. The pre-fix math would
    /// double-count them at `TailTruncation`, reporting
    /// `tail = table_count - run.len() = 3 - 1 = 2` when the correct
    /// breakdown is `tail = 1, corruption = 1` (the corrupt record
    /// goes to corruption, only the genuinely missing trailing
    /// record goes to tail).
    #[test]
    fn recover_skip_any_then_tail_accounts_corruption_separately() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/skip_any/then_tail");
        fs.create_dir_all(folder)?;
        write_current(folder, 1, &fs)?;
        write_manifest_skip_any_then_tail_truncated(folder, 1, &fs)?;

        let recovery = recover(folder, &fs, ManifestRecoveryMode::SkipAnyCorruptedRecords)?;

        assert_eq!(
            recovery.stats.tables_dropped_to_corruption, 1,
            "the corrupt record must land in the corruption counter",
        );
        assert_eq!(
            recovery.stats.tables_dropped_to_tail, 1,
            "exactly one trailing record was truncated; the previously-skipped \
             corrupt record must NOT be re-counted here as tail (pre-fix value \
             would be 2)",
        );
        Ok(())
    }

    /// Companion fixture for the blob-side accounting regression
    /// test below. Mirrors `write_manifest_skip_any_then_tail_truncated`
    /// (a good record + a corrupt record + truncated tail) but in
    /// the `blob_files` section so the read path's `SkipAny` +
    /// `TailTruncation` arm fires on blob counters instead of table
    /// counters.
    fn write_manifest_blob_skip_any_then_tail_truncated(
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
        w.write_u8(0)?; // 0 levels — focus is on blob_files

        w.start("blob_files")?;
        w.write_u32::<LittleEndian>(3)?; // declared 3...
        // ...but only 2 written: good, bad. The third is implicitly
        // truncated (reader hits UnexpectedEof at the 3rd frame
        // header).
        crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
            payload.write_u64::<LittleEndian>(10)?;
            payload.write_u8(0)?;
            payload.write_u128::<LittleEndian>(0)?;
            Ok(())
        })?;
        // Corrupt second blob record (wrong xxh3, correct length).
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

        w.start("blob_gc_stats")?;
        w.write_u32::<LittleEndian>(0)?;
        w.finish().map_err(|e| match e {
            sfa::Error::Io(e) => crate::Error::from(e),
            _ => crate::Error::Unrecoverable,
        })?;
        Ok(())
    }

    /// Regression test for the blob-side counterpart of the
    /// accounting fix from `fd44c376` / 52db0ccd. Manifest declares
    /// 3 blob records: 1 good (id=10) + 1 corrupt (id=11) +
    /// 1 truncated (never written to disk). Under
    /// `SkipAnyCorruptedRecords` the corrupt record skip-arm
    /// increments `blob_dropped_to_corruption` and `blob_corrupted`
    /// to 1 each; the `TailTruncation` arm then attributes the
    /// remaining 1 unread record to `blob_dropped_to_tail`. Without
    /// the `processed = recovered + blob_corrupted` accounting the
    /// tail value would be 2 (overcount).
    #[test]
    fn recover_skip_any_then_tail_accounts_blob_corruption_separately() -> crate::Result<()> {
        let fs = MemFs::new();
        let folder = Path::new("/skip_any/blob_then_tail");
        fs.create_dir_all(folder)?;
        write_current(folder, 1, &fs)?;
        write_manifest_blob_skip_any_then_tail_truncated(folder, 1, &fs)?;

        let recovery = recover(folder, &fs, ManifestRecoveryMode::SkipAnyCorruptedRecords)?;

        assert_eq!(
            recovery.stats.blob_dropped_to_corruption, 1,
            "the corrupt blob record must land in the corruption counter",
        );
        assert_eq!(
            recovery.stats.blob_dropped_to_tail, 1,
            "exactly one trailing blob record was truncated; the previously-skipped \
             corrupt record must NOT be re-counted here as tail (pre-fix value \
             would be 2)",
        );
        Ok(())
    }
}
