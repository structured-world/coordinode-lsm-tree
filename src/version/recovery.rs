// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::io::{LittleEndian, ReadBytesExt};
use crate::{
    Checksum, SeqNo, TableId, TreeType,
    coding::Decode,
    config::ManifestRecoveryMode,
    file::CURRENT_VERSION_FILE,
    fs::{Fs, FsOpenOptions},
    version::VersionId,
    vlog::BlobFileId,
};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::path::Path;

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
    let mut cursor = crate::io::Cursor::new(payload);
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
    let mut cursor = crate::io::Cursor::new(payload);
    let id = cursor.read_u64::<LittleEndian>()?;
    let checksum_type = cursor.read_u8()?;
    if checksum_type != 0 {
        return Err(crate::Error::InvalidTag(("ChecksumType", checksum_type)));
    }
    let checksum = Checksum::from_raw(cursor.read_u128::<LittleEndian>()?);
    Ok((id, checksum))
}

/// Parses the optional `restrictions` section: `count: u32 | repeat(table id:
/// u64, key_len: u32, key bytes)`. Read strictly (no tail tolerance): a
/// restriction is safety-critical — an un-clamped table whose prefix was
/// punched out would read zeroed blocks — so a malformed section aborts rather
/// than silently dropping a clamp. Absent section is handled by the caller
/// (legitimate: no tight-space reclaim ever ran) and never reaches here.
fn parse_restrictions_section(
    mut bytes: &[u8],
) -> crate::Result<crate::HashMap<TableId, crate::UserKey>> {
    const ERR: crate::Error = crate::Error::InvalidHeader("restrictions section");
    let r = &mut bytes;
    let count = r.read_u32::<LittleEndian>().map_err(|_| ERR)?;
    let mut map = crate::HashMap::default();
    for _ in 0..count {
        let id = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;
        let key_len = r.read_u32::<LittleEndian>().map_err(|_| ERR)? as usize;
        if r.len() < key_len {
            return Err(ERR);
        }
        let (head, tail) = r.split_at(key_len);
        *r = tail;
        // Reject a duplicate table id: a corrupt section that lists a table twice
        // could otherwise silently lower an already-advanced bound and un-clamp a
        // punched prefix on reopen.
        if map.insert(id, crate::UserKey::from(head)).is_some() {
            return Err(ERR);
        }
    }
    if !r.is_empty() {
        return Err(ERR);
    }
    Ok(map)
}

/// Reads and validates the CURRENT version pointer file.
///
/// The file format is: `version_id: u64 | checksum: u128 | checksum_type: u8`
/// (25 bytes total, written atomically by `rewrite_atomic`).
///
/// Reads the version id, opens the referenced `v{id}` manifest via
/// [`ManifestArchiveReader::open`](crate::manifest_blocks::reader::ManifestArchiveReader::open)
/// (so the tail-first / head-mirror-fallback recovery path applies
/// here too — a torn or corrupted trailing size-hint can be
/// recovered through the head mirror without first tripping the
/// CURRENT-pointer validation), then recomputes the canonical
/// footer digest via [`current_digest::compute`](crate::manifest_blocks::current_digest::compute) over the parsed
/// footer payload and compares it against the stamped checksum.
/// Mismatch surfaces as [`crate::Error::ChecksumMismatch`].
///
/// The stored digest is the canonical XXH3-128 over (`version_id` +
/// `layout_version` + flags + sorted TOC entries with each section's
/// own XXH3-128). See [`crate::manifest_blocks::current_digest`]
/// for the exact serialisation and threat model. Critically: this
/// digest does NOT cover raw on-disk section bytes — per-Block
/// XXH3 + Page ECC (when enabled) handle section corruption on
/// `read_section`, and a section bit-flip that ECC heals at decode
/// time does not invalidate the CURRENT pointer here. That's the
/// point: the CURRENT layer binds logical identity, the Block
/// layer handles bit-level integrity, and ECC recovery actually
/// works for manifest sections.
///
/// XXH3-128 is NOT a cryptographic MAC: an attacker with write
/// access can craft matching content. For adversarial tamper
/// resistance enable `Config::with_encryption(...)` (AEAD per
/// Block).
pub fn get_current_version(
    folder: &Path,
    fs: &dyn Fs,
    encryption: Option<alloc::sync::Arc<dyn crate::encryption::EncryptionProvider>>,
) -> crate::Result<VersionId> {
    use crate::io::{LittleEndian, ReadBytesExt};

    let path = folder.join(CURRENT_VERSION_FILE);
    let mut file = fs.open(&path, &FsOpenOptions::new().read(true))?;

    let version_id = file.read_u64::<LittleEndian>()?;
    let stored_checksum = file.read_u128::<LittleEndian>()?;
    let checksum_type = file.read_u8()?;

    // Validate checksum type tag — a non-zero value indicates corruption
    // or a file from an incompatible version (only xxh3 = 0 is supported).
    if checksum_type != 0 {
        return Err(crate::Error::InvalidTag(("ChecksumType", checksum_type)));
    }

    let manifest_path = folder.join(format!("v{version_id}"));

    // Open the manifest through the tail-first / head-mirror-fallback
    // reader so a torn trailing size-hint that the reader can still
    // recover does not invalidate the CURRENT pointer first. The
    // parsed footer's TOC gives us every section's
    // `(block_offset, block_size)`; `section_end` is the maximum of
    // `block_offset + block_size` across the TOC. The runtime
    // snapshot is a placeholder default — get_current_version runs
    // before any Tree exists, and the reader's ECC decisions are
    // per-Block self-describing via the Block header (not driven by
    // the supplied runtime), so the placeholder is safe.
    // Rewrap manifest NotFound so `Tree::open`'s outer `Err(Io(NotFound))`
    // arm — which means "CURRENT file is absent, fresh-init the tree" —
    // never absorbs a missing manifest. A missing manifest with CURRENT
    // pointing at it is half-applied recovery / corruption, not a
    // fresh-init signal; converting it to ManifestFooterInvalid surfaces
    // that distinct failure mode loud and clear.
    let archive = crate::manifest_blocks::reader::ManifestArchiveReader::open(
        &manifest_path,
        fs,
        alloc::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        encryption,
    )
    .map_err(|e| match e {
        crate::Error::Io(io) if io.kind() == crate::io::ErrorKind::NotFound => {
            crate::Error::ManifestFooterInvalid(
                "manifest file referenced by CURRENT does not exist",
            )
        }
        other => other,
    })?;

    // Recompute the CURRENT digest from the parsed footer payload
    // and compare against the value stamped at write time. The
    // footer arrived through `ManifestArchiveReader::open` — tail-
    // first with head-mirror fallback — so a torn tail recoverable
    // via the mirror still produces the right digest here. No raw
    // section-byte hashing: per-Block ECC on `read_section` keeps
    // its repair authority.
    let computed = crate::manifest_blocks::current_digest::compute(version_id, archive.footer())?;
    if computed != stored_checksum {
        return Err(crate::Error::ChecksumMismatch {
            got: Checksum::from_raw(computed),
            expected: Checksum::from_raw(stored_checksum),
        });
    }

    Ok(version_id)
}

#[derive(Debug, PartialEq, Eq)]
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
    ///
    /// This counter is a LOWER BOUND on records actually discarded
    /// under PIT: when PIT hits a corruption boundary it stops
    /// reading the current level (and all subsequent levels) without
    /// parsing their run / table counts, so the records inside those
    /// abandoned levels are NOT included here. Use this counter as
    /// "records dropped in parsed runs", not "total records lost".
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
    /// Version id of the on-disk snapshot the `CURRENT` pointer references —
    /// the base the edit log is replayed on top of. The snapshot file
    /// `v{snapshot_id}` and its log `edits-{snapshot_id}` are the generation
    /// that must survive orphan cleanup; intermediate versions live only in the
    /// log. Equals [`Self::curr_version_id`] when the log is empty (just after a
    /// rotation) and is `<=` it otherwise.
    pub snapshot_id: VersionId,
    /// Version id of the recovered state: the snapshot's id advanced by every
    /// edit replayed from the log (so the next persist continues from here).
    pub curr_version_id: VersionId,
    pub table_ids: Vec<Vec<Vec<RecoveredTable>>>,
    pub blob_file_ids: Vec<(BlobFileId, Checksum)>,
    pub gc_stats: crate::blob_tree::FragmentationMap,
    /// Per-table tight-space key-range lower bounds recovered from the snapshot
    /// `restrictions` section and advanced by replayed edits. A table id present
    /// here is rebuilt as a restricted view ([`super::Version::from_recovery`]);
    /// stale entries for tables no longer in the layout are simply never applied.
    pub restrictions: crate::HashMap<TableId, crate::UserKey>,
    /// Per-section counters describing how many records were dropped
    /// during this recovery. Always zero under
    /// [`ManifestRecoveryMode::AbsoluteConsistency`] (any corruption
    /// or truncation aborts before returning a [`Recovery`]).
    ///
    /// `#[expect]` would be unfulfilled under `cfg(test)` (the
    /// in-crate unit tests in `src/version/recovery.rs::tests`
    /// read this field to assert recovery-mode accounting), and
    /// `#[allow]` would be noisy in builds where tests are off.
    /// Gate `#[expect]` to non-test builds: under `cargo test`
    /// the field IS read by the unit-test module so the lint
    /// doesn't fire and the expectation isn't attached; under
    /// `cargo build` the field is unread by in-tree code. The
    /// `version` module is `mod version` (not pub) at the crate
    /// root, so this field is not part of the published API
    /// surface today; it's kept for in-tree telemetry assertions
    /// and as the canonical site to plumb operator-visible
    /// recovery counters once a public API is exposed.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "in-tree telemetry assertions only; not part \
                      of the published API surface today"
        )
    )]
    pub stats: RecoveryStats,
}

impl Recovery {
    /// Applies one edit-log [`VersionEdit`](super::edit::VersionEdit) on top of
    /// the recovered snapshot state, in place — the consumer side of the
    /// incremental manifest. Edits are replayed in order after the snapshot to
    /// reconstruct the current version.
    ///
    /// A changed level replaces its run layout wholesale (a dropped table is
    /// simply absent from the new layout; an emptied level becomes zero runs).
    /// Blob files take a per-id add / remove (an added id whose entry already
    /// exists overwrites its checksum). GC stats overwrite when the edit carries
    /// them. The version id advances to the edit's `new_version_id`.
    ///
    /// # Errors
    ///
    /// Returns an error if the edit's GC-stats payload fails to decode.
    pub(crate) fn apply_edit(&mut self, edit: &super::edit::VersionEdit) -> crate::Result<()> {
        for cl in &edit.changed_levels {
            let idx = usize::from(cl.level);
            if idx >= self.table_ids.len() {
                self.table_ids.resize_with(idx + 1, Vec::new);
            }
            let new_layout = cl
                .runs
                .iter()
                .map(|run| {
                    run.iter()
                        .map(|t| RecoveredTable {
                            id: t.id,
                            checksum: Checksum::from_raw(t.checksum),
                            global_seqno: t.global_seqno,
                        })
                        .collect()
                })
                .collect();
            // `idx < len` holds: the resize above grew the vec to `idx + 1`.
            if let Some(slot) = self.table_ids.get_mut(idx) {
                *slot = new_layout;
            }
        }

        if !edit.removed_blob_file_ids.is_empty() {
            self.blob_file_ids
                .retain(|(id, _)| !edit.removed_blob_file_ids.contains(id));
        }
        for b in &edit.added_blob_files {
            let checksum = Checksum::from_raw(b.checksum);
            if let Some(entry) = self.blob_file_ids.iter_mut().find(|(id, _)| *id == b.id) {
                entry.1 = checksum;
            } else {
                self.blob_file_ids.push((b.id, checksum));
            }
        }

        if let Some(bytes) = &edit.gc_stats {
            self.gc_stats = crate::blob_tree::FragmentationMap::decode_from(&mut &bytes[..])?;
        }

        // Tight-space restrictions advance per slice: each edit lists the
        // straddling input's new (higher) lower bound, so a later edit overwrites
        // the earlier bound for the same table. A monotonicity (no-regression)
        // check would be COMPARATOR-RELATIVE — "advancing" is defined by the
        // tree's configured comparator, not byte order — but the comparator is not
        // plumbed into recovery, so a byte-order check would wrongly reject valid
        // advances (or miss real regressions) under a custom/reverse comparator.
        // The edit log is framing-checksummed, so a corrupt/reordered edit is
        // already rejected upstream; the comparator-independent duplicate guard
        // lives in `parse_restrictions_section` for the snapshot path.
        // Entries for tables later dropped from the layout are simply never
        // applied by `from_recovery`, and the next snapshot rewrite drops them.
        for (id, key) in &edit.restrictions {
            self.restrictions.insert(*id, key.clone());
        }

        self.curr_version_id = edit.new_version_id;
        Ok(())
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "manifest recovery is inherently a long sequential read of multiple SFA \
              sections; splitting the function would just move the per-mode branching \
              into helpers without clarifying the flow"
)]
pub fn recover(
    folder: &Path,
    fs: &dyn Fs,
    mode: ManifestRecoveryMode,
    encryption: Option<alloc::sync::Arc<dyn crate::encryption::EncryptionProvider>>,
) -> crate::Result<Recovery> {
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

    let curr_version_id = get_current_version(folder, fs, encryption.clone())?;
    let version_file_path = folder.join(format!("v{curr_version_id}"));

    log::info!(
        "Recovering current manifest at {} (mode={mode:?})",
        version_file_path.display(),
    );

    let mut archive = crate::manifest_blocks::reader::ManifestArchiveReader::open(
        &version_file_path,
        fs,
        alloc::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        encryption,
    )?;

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
    //     On ChecksumMismatch / BadHeader, behaves like reaching EOF:
    //     the in-progress record is dropped, the already-decoded
    //     records inside the current run / level are preserved (pushed
    //     before the break), and the rest of the manifest (this run's
    //     remaining records, subsequent runs in this level, and all
    //     subsequent levels) is abandoned. The recovered prefix is the
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
        if archive.section("tables").is_none() {
            log::error!(
                "tables section not found in version #{curr_version_id} - maybe the file is corrupted?"
            );
            return Err(crate::Error::Unrecoverable);
        }
        let section_bytes = archive.read_section("tables")?;
        let section_len: u64 = section_bytes.len() as u64;
        let mut reader = crate::io::Cursor::new(section_bytes);

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
            Err(e) if tolerate_tail && e.kind() == crate::io::ErrorKind::UnexpectedEof => {
                log::warn!(
                    "tables section truncated before level_count byte in version \
                     #{curr_version_id}; tail-tolerant mode produces 0 levels"
                );
                0
            }
            Err(e) => return Err(e.into()),
        };

        // NOTE: `level_count` is the only unframed byte in the
        // section, so a single bit flip can silently transform it
        // into any other u8 value, and tolerant recovery modes
        // would still produce a `Version` whose `level_count()`
        // disagrees with the downstream `assert!` in
        // `src/compaction/leveled/mod.rs`, which now compares
        // against `config.level_count` rather than the literal
        // `7`. Recovery still does not validate the recovered
        // byte against `DEFAULT_LEVEL_COUNT`: fixtures use sub-
        // default level counts for compact test manifests, so a
        // strict gate here would force them to pad to the default
        // level count even when the test only cares about a single
        // level's worth of records. The downstream assertion fires
        // only when a real corruption produces a Version whose
        // count disagrees with the Config the compactor receives.

        'levels: for _ in 0..level_count {
            let mut level = vec![];
            let run_count = match reader.read_u8() {
                Ok(n) => {
                    tables_bytes_consumed += 1;
                    n
                }
                Err(e) if tolerate_tail && e.kind() == crate::io::ErrorKind::UnexpectedEof => {
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
                    Err(e) if tolerate_tail && e.kind() == crate::io::ErrorKind::UnexpectedEof => {
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
                // Clamp-to-zero: `tables_bytes_consumed <= section_len` by loop
                // invariant, so this never actually saturates — it guards the
                // subtraction (clamp is the intended min semantics).
                let bytes_remaining = section_len.saturating_sub(tables_bytes_consumed);
                // `table_count` is an untrusted u32; widened to u64 its product
                // with the 45-byte entry size is at most u32::MAX * 45 < u64::MAX,
                // so a plain multiply cannot overflow (no need to mask it).
                if u64::from(table_count) * FRAMED_TABLE_ENTRY_LEN > bytes_remaining {
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
                    let remaining = section_len.saturating_sub(tables_bytes_consumed);
                    // Pin the on-disk `len` to the fixed table-record
                    // payload size so a corrupted-but-plausible
                    // `len` cannot mis-align the cursor for the
                    // next record under SkipAny. read_framed_record
                    // returns `LenMismatch { got, expected }` for
                    // `len != expected` (schema drift), which the
                    // match arm below hard-aborts in EVERY recovery
                    // mode — distinct from `BadHeader` (truly
                    // implausible `len > MAX_FRAME_PAYLOAD`) which
                    // is treated as in-section corruption and
                    // section-dropped under PIT/SkipAny. The
                    // scratch buffer is reused across every record
                    // in this section to keep per-record heap
                    // allocations at zero.
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
                            return Err(crate::Error::from(crate::io::Error::new(
                                crate::io::ErrorKind::UnexpectedEof,
                                "manifest tables record truncated mid-frame",
                            )));
                        }
                        FramedRecordOutcome::BadHeader => {
                            // Strict mode: the framing header was
                            // structurally implausible (`len` above
                            // MAX_FRAME_PAYLOAD). Surface as
                            // InvalidHeader with a section-tagged
                            // static string so operators can route
                            // on the variant payload instead of
                            // parsing the Display message.
                            log::error!(
                                "manifest tables frame header rejected in version \
                                 #{curr_version_id}: len exceeds MAX_FRAME_PAYLOAD"
                            );
                            return Err(crate::Error::InvalidHeader(
                                "manifest tables frame header",
                            ));
                        }
                        FramedRecordOutcome::LenMismatch { got, expected } => {
                            // `len` is within the implausibility cap
                            // but disagrees with the fixed-size
                            // table-record contract. Could be either
                            // writer / reader format disagreement
                            // (schema drift) or corruption of the
                            // length field that happens to stay
                            // within MAX_FRAME_PAYLOAD; the reader
                            // cannot distinguish the two. Hard-abort
                            // in EVERY mode (including PIT / SkipAny
                            // / tail-tolerant): tolerant modes are
                            // for power-loss recovery at the tail,
                            // not for silently absorbing in-record
                            // ambiguity, and either underlying cause
                            // is unrecoverable from here.
                            log::error!(
                                "manifest tables frame len mismatch in version \
                                 #{curr_version_id}: declared len={got}, \
                                 expected fixed-size TABLE_ENTRY_PAYLOAD_LEN={expected} \
                                 — schema drift, aborting regardless of recovery mode"
                            );
                            return Err(crate::Error::InvalidHeader(
                                "manifest tables frame len mismatch",
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
        if archive.section("blob_files").is_none() {
            log::error!(
                "blob_files section not found in version #{curr_version_id} - maybe the file is corrupted?"
            );
            return Err(crate::Error::Unrecoverable);
        }
        let section_bytes = archive.read_section("blob_files")?;
        let section_len: u64 = section_bytes.len() as u64;
        let mut reader = crate::io::Cursor::new(section_bytes);

        let blob_file_count = match reader.read_u32::<LittleEndian>() {
            Ok(n) => n,
            Err(e) if tolerate_tail && e.kind() == crate::io::ErrorKind::UnexpectedEof => {
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
        // Capacity as a DIVISION (count > section/entry), which is overflow-safe
        // by construction — no multiply to mask. `saturating_sub(4)` clamps to
        // zero when the section is too short to even hold the count header.
        let blob_section_capacity = section_len.saturating_sub(4) / FRAMED_BLOB_ENTRY_LEN;
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
            let remaining = section_len.saturating_sub(blob_bytes_consumed);
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
                    return Err(crate::Error::from(crate::io::Error::new(
                        crate::io::ErrorKind::UnexpectedEof,
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
                         #{curr_version_id}: len exceeds MAX_FRAME_PAYLOAD"
                    );
                    return Err(crate::Error::InvalidHeader(
                        "manifest blob_files frame header",
                    ));
                }
                FramedRecordOutcome::LenMismatch { got, expected } => {
                    // Schema drift on the blob_files section. Same
                    // reasoning as the tables section above: the
                    // bytes on disk are well-formed for SOME schema
                    // but not the one this binary decodes, so
                    // tolerant modes MUST NOT mask the mismatch —
                    // hard-abort regardless of the configured
                    // ManifestRecoveryMode.
                    log::error!(
                        "manifest blob_files frame len mismatch in version \
                         #{curr_version_id}: declared len={got}, \
                         expected fixed-size BLOB_ENTRY_PAYLOAD_LEN={expected} \
                         — schema drift, aborting regardless of recovery mode"
                    );
                    return Err(crate::Error::InvalidHeader(
                        "manifest blob_files frame len mismatch",
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
        if archive.section("blob_gc_stats").is_none() {
            log::error!(
                "blob_gc_stats section not found in version #{curr_version_id} - maybe the file is corrupted?"
            );
            return Err(crate::Error::Unrecoverable);
        }
        let section_bytes = archive.read_section("blob_gc_stats")?;
        let mut reader = crate::io::Cursor::new(section_bytes);

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
                if tolerate_tail && e.kind() == crate::io::ErrorKind::UnexpectedEof =>
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

    // Optional tight-space restrictions section. Absent on versions that never
    // ran tight-space reclaim (→ empty). When present it is read strictly: a
    // restriction is safety-critical, so a malformed section aborts rather than
    // silently un-clamping a punched table.
    let restrictions = if archive.section("restrictions").is_some() {
        parse_restrictions_section(&archive.read_section("restrictions")?)?
    } else {
        crate::HashMap::default()
    };

    let mut recovery = Recovery {
        tree_type: {
            if archive.section("tree_type").is_none() {
                log::error!(
                    "tree_type section not found in version #{curr_version_id} - maybe the file is corrupted?"
                );
                return Err(crate::Error::Unrecoverable);
            }
            let section_bytes = archive.read_section("tree_type")?;
            let byte = section_bytes
                .first()
                .copied()
                .ok_or(crate::Error::InvalidHeader("TreeType"))?;
            TreeType::try_from(byte).map_err(|()| crate::Error::InvalidHeader("TreeType"))?
        },
        snapshot_id: curr_version_id,
        curr_version_id,
        table_ids: levels,
        blob_file_ids,
        gc_stats,
        restrictions,
        stats: RecoveryStats {
            tables_dropped_to_tail,
            tables_dropped_to_corruption,
            tables_truncated_headers,
            blob_dropped_to_tail,
            blob_dropped_to_corruption,
        },
    };

    // Replay the incremental edit log layered on top of the snapshot. The log
    // `edits-{snapshot_id}` holds every VersionEdit appended since the snapshot
    // was written; applying them in order reconstructs the current version.
    // `replay_log` routes the same `mode` the snapshot sections use: a
    // writer-incomplete trailing edit is rolled back in every mode except the
    // default `AbsoluteConsistency` (which surfaces `TornManifestEditLog` so the
    // operator truncates the tail via repair); a fully-framed but bit-rotted
    // trailing edit is rolled back only under PIT / SkipAny, and aborts under
    // AbsoluteConsistency and TolerateCorruptedTailRecords (truncation-salvage
    // only). A clean end-of-log is always accepted. Each applied edit advances
    // `recovery.curr_version_id` past the snapshot's id.
    let log_path = folder.join(format!("edits-{curr_version_id}"));
    let edits = super::edit_log::replay_log(fs, &log_path, mode)?;
    if !edits.is_empty() {
        log::info!(
            "Replaying {} manifest edit(s) on top of snapshot #{curr_version_id}",
            edits.len(),
        );
        for edit in &edits {
            recovery.apply_edit(edit)?;
        }
    }

    Ok(recovery)
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions"
)]
mod tests;
