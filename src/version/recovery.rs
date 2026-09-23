// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

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
/// `level: u8 (1) | run: u32 (4) | id: u64 (8) | checksum_type: u8 (1) |
/// checksum: u128 (16) | global_seqno: u64 (8)`.
///
/// Each record says which level and run it belongs to, and the section holds
/// no run or table counts. The section arrives whole or not at all (it is one checksummed
/// Block bound to the TOC), so counts would add nothing a reader could check
/// against the disk; they could only disagree with the records, which is how a
/// run count once truncated to a byte made a written tree unopenable. `RocksDB`
/// and Pebble keep a file's level in the file's own manifest record for the
/// same reason.
///
/// Stored as `u32` because the framing layer's `len` field is `u32`.
const TABLE_ENTRY_PAYLOAD_LEN: u32 = 1 + 4 + 8 + 1 + 16 + 8;

/// Where a table sits in the version: its level, and the ordinal of its run
/// within that level. Runs are numbered from 0 in the order the level holds
/// them.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct TablePlace {
    pub level: u8,
    pub run: u32,
}

/// Encodes one `tables`-section record payload; the inverse of
/// [`decode_table_entry_payload`].
pub fn encode_table_entry_payload(
    payload: &mut Vec<u8>,
    place: TablePlace,
    id: TableId,
    checksum: Checksum,
    global_seqno: SeqNo,
) -> crate::Result<()> {
    use crate::io::WriteBytesExt;

    payload.write_u8(place.level)?;
    payload.write_u32::<LittleEndian>(place.run)?;
    payload.write_u64::<LittleEndian>(id)?;
    payload.write_u8(0)?; // Checksum type, 0 = XXH3
    payload.write_u128::<LittleEndian>(checksum.into_u128())?;
    payload.write_u64::<LittleEndian>(global_seqno)?;
    Ok(())
}

/// Exact on-disk size of a `blob_files`-section record payload (post-framing):
/// `id: u64 (8) | checksum_type: u8 (1) | checksum: u128 (16)`.
const BLOB_ENTRY_PAYLOAD_LEN: u32 = 8 + 1 + 16;

/// Decodes a table-record payload (post-framing) laid out as
/// [`TABLE_ENTRY_PAYLOAD_LEN`] describes. The surrounding framing header
/// (length + XXH3-64) is handled by
/// [`crate::version::framing::read_framed_record`] before this is called.
///
/// Rejects payloads whose length is not exactly
/// [`TABLE_ENTRY_PAYLOAD_LEN`]. The framing layer already verified
/// the XXH3-64 over the payload matched the header digest, so any
/// length mismatch at this point is writer / reader format drift,
/// not on-disk bit-rot, and it aborts recovery.
fn decode_table_entry_payload(payload: &[u8]) -> crate::Result<(TablePlace, RecoveredTable)> {
    if payload.len() != TABLE_ENTRY_PAYLOAD_LEN as usize {
        return Err(crate::Error::InvalidHeader("tables record payload length"));
    }
    let mut cursor = crate::io::Cursor::new(payload);
    let level = cursor.read_u8()?;
    let run = cursor.read_u32::<LittleEndian>()?;
    let id = cursor.read_u64::<LittleEndian>()?;
    let checksum_type = cursor.read_u8()?;
    if checksum_type != 0 {
        return Err(crate::Error::InvalidTag(("ChecksumType", checksum_type)));
    }
    let checksum = Checksum::from_raw(cursor.read_u128::<LittleEndian>()?);
    let global_seqno = cursor.read_u64::<LittleEndian>()?;
    Ok((
        TablePlace { level, run },
        RecoveredTable {
            id,
            checksum,
            global_seqno,
        },
    ))
}

/// Places a decoded table record into `levels`, which the snapshot's level
/// count has already sized.
///
/// Records arrive in the order the writer emits them: by level, then by run,
/// then by position in the run. A record that names the place of the previous
/// one joins that run; one that names the next run, or run 0 of a later level,
/// opens a new run. A skipped run ordinal, a place behind the previous one, or
/// a level beyond the count is a record no writer produced and is refused.
fn place_table(
    levels: &mut [Vec<Vec<RecoveredTable>>],
    previous: &mut Option<TablePlace>,
    place: TablePlace,
    table: RecoveredTable,
) -> crate::Result<()> {
    const ERR: crate::Error = crate::Error::InvalidHeader("tables record out of place");

    let level = levels.get_mut(usize::from(place.level)).ok_or(ERR)?;
    if *previous == Some(place) {
        level.last_mut().ok_or(ERR)?.push(table);
        return Ok(());
    }
    let follows = match *previous {
        None => place.run == 0,
        Some(prev) if prev.level == place.level => Some(place.run) == prev.run.checked_add(1),
        Some(prev) => prev.level < place.level && place.run == 0,
    };
    if !follows {
        return Err(ERR);
    }
    level.push(vec![table]);
    *previous = Some(place);
    Ok(())
}

/// Reads the `tables` section: the number of levels the layout spans (`u8`),
/// then a sequence of framed records, each naming its own level and run (see
/// [`TABLE_ENTRY_PAYLOAD_LEN`]), grouped back into that many levels. The level
/// count is not a count of records, so it cannot disagree with them; it also
/// keeps the section non-empty for a version without tables, which an
/// encrypted manifest block requires.
///
/// Read strictly, whatever the recovery mode: the section is one checksummed
/// block bound to the `CURRENT` digest, so it arrives whole, and a record that
/// does not verify, decode or fall in place is a writer defect or a forgery.
/// Opening past one would drop tables the manifest committed, and the open
/// then deletes their files as orphans.
fn read_tables_section(
    bytes: Vec<u8>,
    curr_version_id: VersionId,
    scratch: &mut Vec<u8>,
) -> crate::Result<Vec<Vec<Vec<RecoveredTable>>>> {
    use crate::version::framing::{FRAME_HEADER_LEN, FramedRecordOutcome, read_framed_record};

    let section_len = bytes.len() as u64;
    let mut reader = crate::io::Cursor::new(bytes);
    let level_count = reader
        .read_u8()
        .map_err(|_| crate::Error::InvalidHeader("tables section level count"))?;
    let mut consumed: u64 = 1;
    let mut levels = vec![Vec::new(); usize::from(level_count)];
    let mut previous = None;

    while consumed < section_len {
        match read_framed_record(
            &mut reader,
            section_len - consumed,
            Some(TABLE_ENTRY_PAYLOAD_LEN),
            scratch,
        )? {
            FramedRecordOutcome::Ok => {
                consumed += FRAME_HEADER_LEN as u64 + scratch.len() as u64;
                let (place, table) = decode_table_entry_payload(scratch)?;
                place_table(&mut levels, &mut previous, place, table)?;
            }
            FramedRecordOutcome::TailTruncation => {
                return Err(crate::Error::from(crate::io::Error::new(
                    crate::io::ErrorKind::UnexpectedEof,
                    "manifest tables record truncated mid-frame",
                )));
            }
            FramedRecordOutcome::ChecksumMismatch { expected, got, .. } => {
                return Err(crate::Error::ManifestFrameChecksumMismatch {
                    section: "tables",
                    expected,
                    got,
                });
            }
            FramedRecordOutcome::BadHeader => {
                log::error!(
                    "manifest tables frame header rejected in version \
                     #{curr_version_id}: len exceeds MAX_FRAME_PAYLOAD"
                );
                return Err(crate::Error::InvalidHeader("manifest tables frame header"));
            }
            FramedRecordOutcome::LenMismatch { got, expected } => {
                log::error!(
                    "manifest tables frame len mismatch in version #{curr_version_id}: \
                     declared len={got}, expected {expected}"
                );
                return Err(crate::Error::InvalidHeader(
                    "manifest tables frame len mismatch",
                ));
            }
        }
    }

    Ok(levels)
}

/// Reads the `blob_files` section: `count: u32`, then exactly `count` framed
/// records (see [`decode_blob_entry_payload`]), returned sorted by id. Read
/// strictly, for the same reason as [`read_tables_section`]: a count the
/// records do not fill, or bytes left after them, is a writer defect.
fn read_blob_files_section(
    bytes: Vec<u8>,
    curr_version_id: VersionId,
    scratch: &mut Vec<u8>,
) -> crate::Result<Vec<(BlobFileId, Checksum)>> {
    use crate::version::framing::{FRAME_HEADER_LEN, FramedRecordOutcome, read_framed_record};

    const ERR: crate::Error = crate::Error::InvalidHeader("blob_files section");
    const FRAMED_ENTRY_LEN: u64 = FRAME_HEADER_LEN as u64 + BLOB_ENTRY_PAYLOAD_LEN as u64;

    let section_len = bytes.len() as u64;
    let mut reader = crate::io::Cursor::new(bytes);
    let count = reader.read_u32::<LittleEndian>().map_err(|_| ERR)?;
    // The records are fixed-size, so the section's length says exactly how
    // many it holds; checked before reserving, so a corrupt count cannot turn
    // into a huge allocation.
    if section_len - 4 != u64::from(count) * FRAMED_ENTRY_LEN {
        return Err(ERR);
    }

    let mut consumed: u64 = 4;
    let mut blob_file_ids = Vec::with_capacity(count as usize);
    for _ in 0..count {
        match read_framed_record(
            &mut reader,
            section_len - consumed,
            Some(BLOB_ENTRY_PAYLOAD_LEN),
            scratch,
        )? {
            FramedRecordOutcome::Ok => {
                consumed += FRAMED_ENTRY_LEN;
                blob_file_ids.push(decode_blob_entry_payload(scratch)?);
            }
            FramedRecordOutcome::ChecksumMismatch { expected, got, .. } => {
                return Err(crate::Error::ManifestFrameChecksumMismatch {
                    section: "blob_files",
                    expected,
                    got,
                });
            }
            FramedRecordOutcome::TailTruncation
            | FramedRecordOutcome::BadHeader
            | FramedRecordOutcome::LenMismatch { .. } => {
                log::error!("manifest blob_files record malformed in version #{curr_version_id}");
                return Err(ERR);
            }
        }
    }

    blob_file_ids.sort_by_key(|(id, _)| *id);
    Ok(blob_file_ids)
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

/// Parses the optional `blob_restrictions` section: `count: u32 |
/// repeat(blob file id: u64, live-data frontier: u64)`. The blob analogue of
/// [`parse_restrictions_section`], read under the same strictness — a lost or
/// lowered frontier would make integrity checks hash the reclaimed (zeroed)
/// prefix and condemn a healthy blob file. Absent section is handled by the
/// caller (legitimate: no blob prefix was ever reclaimed).
fn parse_blob_restrictions_section(
    mut bytes: &[u8],
) -> crate::Result<crate::HashMap<BlobFileId, u64>> {
    const ERR: crate::Error = crate::Error::InvalidHeader("blob_restrictions section");
    let r = &mut bytes;
    let count = r.read_u32::<LittleEndian>().map_err(|_| ERR)?;
    let mut map = crate::HashMap::default();
    for _ in 0..count {
        let id = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;
        let frontier = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;
        // Reject a duplicate id for the same reason the table section does: a
        // second entry could silently lower an already-advanced frontier.
        if map.insert(id, frontier).is_some() {
            return Err(ERR);
        }
    }
    if !r.is_empty() {
        return Err(ERR);
    }
    Ok(map)
}

/// Parses the optional `retention_floor` section: one `u64 LE`, the highest
/// snapshot seqno the version can no longer serve. Read strictly (exactly
/// eight bytes): a floor read too LOW would let a reopened tree answer a
/// snapshot from data it never saw, which is the very outcome the floor
/// exists to refuse. Absent section is handled by the caller (legitimate: a
/// snapshot written before the floor existed) and never reaches here.
fn parse_retention_floor_section(mut bytes: &[u8]) -> crate::Result<SeqNo> {
    const ERR: crate::Error = crate::Error::InvalidHeader("retention_floor section");
    let r = &mut bytes;
    let floor = r.read_u64::<LittleEndian>().map_err(|_| ERR)?;
    if !r.is_empty() {
        return Err(ERR);
    }
    Ok(floor)
}

/// Reads the registered dictionary ids: `count: u32 | id: u32 * count`.
///
/// Read strictly, like the sections above: a lost id is a dictionary the tree
/// stops accounting for, so the tables written against it would fail to open
/// while its file is left behind as an orphan.
fn parse_dicts_section(mut bytes: &[u8]) -> crate::Result<Vec<crate::file::DictId>> {
    const ERR: crate::Error = crate::Error::InvalidHeader("dicts section");
    let r = &mut bytes;
    let count = r.read_u32::<LittleEndian>().map_err(|_| ERR)? as usize;
    // The ids are fixed-width, so the bytes that remain say how many there can
    // be. Reserving on the count alone would let a corrupt section name four
    // billion ids and turn the recovery into a multi-gigabyte allocation before
    // the first short read could refuse it.
    // Compared by division rather than `count * 4`, which overflows a 32-bit
    // `usize` for the very counts this check exists to reject.
    if count > r.len() / size_of::<crate::file::DictId>() {
        return Err(ERR);
    }
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(r.read_u32::<LittleEndian>().map_err(|_| ERR)?);
    }
    if !r.is_empty() {
        return Err(ERR);
    }
    Ok(ids)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveredTable {
    pub id: TableId,
    pub checksum: Checksum,
    pub global_seqno: SeqNo,
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
    /// `restrictions` section and REPLACED wholesale by each replayed edit
    /// (every edit carries its version's full set). A table id present here is
    /// rebuilt as a restricted view ([`super::Version::from_recovery`]); a
    /// lifted restriction is absent from the next edit's set and so dropped.
    pub restrictions: crate::HashMap<TableId, crate::UserKey>,
    /// Per-blob-file live-data frontiers recovered from the snapshot and
    /// REPLACED wholesale by each replayed edit (`blob file id → first live
    /// byte`). A blob file present here had its consumed prefix reclaimed in
    /// place, so its recorded checksum covers only the suffix from this
    /// offset. The blob analogue of [`Self::restrictions`]; wholesale
    /// replacement (never a merge) matters here because blob ids are reused,
    /// so a removed file's stale frontier must not attach to a later file
    /// under the same id.
    pub blob_restrictions: crate::HashMap<BlobFileId, u64>,
    /// Highest snapshot seqno the recovered version can no longer serve,
    /// from the snapshot's `retention_floor` section (`0` when the snapshot
    /// predates it) and overwritten by each replayed edit that carries one.
    /// Seeds the reopened history's boundary so a snapshot a past GC
    /// compaction or `clear` invalidated is refused, not answered from newer
    /// data.
    pub retention_floor: SeqNo,
    /// Ids of the compression dictionaries the recovered version references,
    /// from the snapshot's `dicts` section (empty when the snapshot predates it
    /// or the tree compresses against none) and REPLACED wholesale by each
    /// replayed edit that carries the section. Wholesale, like the restriction
    /// sets above: an edit that drops a dictionary has to be able to say so.
    pub dicts: Vec<crate::file::DictId>,
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

        // Tight-space restrictions REPLACE wholesale: the encoder derives every
        // edit's `restrictions` / `blob_restrictions` by iterating the new
        // version's tables / blob files, so each edit carries the FULL current
        // set, not a delta. Replacing both advances a bound per slice (the
        // later full set carries the higher bound) AND drops a lifted one (its
        // entry is simply absent). Merging instead would let a removed
        // restricted blob file's frontier outlive it and attach to an
        // unrelated whole file added later under the same id — blob ids are
        // reused (the id counter reseeds from the maximum live id) — making
        // integrity checks hash only that file's suffix. A monotonicity
        // (no-regression) check would be COMPARATOR-RELATIVE — "advancing" is
        // defined by the tree's configured comparator, not byte order — but
        // the comparator is not plumbed into recovery, so a byte-order check
        // would wrongly reject valid advances (or miss real regressions) under
        // a custom/reverse comparator. The edit log is framing-checksummed, so
        // a corrupt/reordered edit is already rejected upstream; the
        // comparator-independent duplicate guard lives in
        // `parse_restrictions_section` for the snapshot path.
        self.restrictions = edit
            .restrictions
            .iter()
            .map(|(id, key)| (*id, key.clone()))
            .collect();
        self.blob_restrictions = edit.blob_restrictions.iter().copied().collect();

        // Carried only when the edit raised it; the value is the version's
        // absolute floor, not a delta.
        if let Some(floor) = edit.retention_floor {
            self.retention_floor = floor;
        }

        // Carried only when the registered set changed, and then in full: an
        // edit that collected a dictionary says so by omitting its id, so this
        // replaces rather than merges.
        if let Some(dicts) = &edit.dicts {
            self.dicts.clone_from(dicts);
        }

        self.curr_version_id = edit.new_version_id;
        Ok(())
    }
}

pub fn recover(
    folder: &Path,
    fs: &dyn Fs,
    mode: ManifestRecoveryMode,
    encryption: Option<alloc::sync::Arc<dyn crate::encryption::EncryptionProvider>>,
) -> crate::Result<Recovery> {
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

    // The snapshot's sections are read strictly whatever `mode` says: each is
    // one checksummed block bound to the CURRENT digest, so it arrives whole,
    // and a record inside it that does not verify or decode is a writer defect
    // or a forgery, not a torn write. `mode` governs only the edit log below.

    // Scratch buffer threaded through every `read_framed_record` call across
    // both the `tables` and `blob_files` sections, so per-record heap
    // allocations during recovery are zero after the initial growth.
    let mut read_scratch: Vec<u8> = Vec::with_capacity(64);

    let levels = read_tables_section(
        archive.read_section("tables")?,
        curr_version_id,
        &mut read_scratch,
    )?;

    let blob_file_ids = read_blob_files_section(
        archive.read_section("blob_files")?,
        curr_version_id,
        &mut read_scratch,
    )?;
    debug_assert!(blob_file_ids.is_sorted_by_key(|(id, _)| id));

    let gc_stats = crate::blob_tree::FragmentationMap::decode_from(&mut crate::io::Cursor::new(
        archive.read_section("blob_gc_stats")?,
    ))?;

    // Optional tight-space restrictions section. Absent on versions that never
    // ran tight-space reclaim (→ empty). When present it is read strictly: a
    // restriction is safety-critical, so a malformed section aborts rather than
    // silently un-clamping a punched table.
    let restrictions = if archive.section("restrictions").is_some() {
        parse_restrictions_section(&archive.read_section("restrictions")?)?
    } else {
        crate::HashMap::default()
    };

    // The blob analogue, read under the same strictness: a lost frontier would
    // make integrity checks hash a reclaimed (zeroed) prefix and condemn a
    // healthy blob file.
    let blob_restrictions = if archive.section("blob_restrictions").is_some() {
        parse_blob_restrictions_section(&archive.read_section("blob_restrictions")?)?
    } else {
        crate::HashMap::default()
    };

    // The retention floor, absent on snapshots written before it existed
    // (→ 0: every snapshot servable, the pre-floor behaviour). Read strictly
    // when present, since a floor read too low serves data a snapshot never
    // saw.
    let retention_floor = if archive.section("retention_floor").is_some() {
        parse_retention_floor_section(&archive.read_section("retention_floor")?)?
    } else {
        0
    };

    // The dictionary ids, absent on a snapshot written before the section
    // existed or by a tree that compresses against none (→ empty, which is
    // what such a tree holds).
    let dicts = if archive.section("dicts").is_some() {
        parse_dicts_section(&archive.read_section("dicts")?)?
    } else {
        Vec::new()
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
        blob_restrictions,
        retention_floor,
        dicts,
    };

    // Replay the incremental edit log layered on top of the snapshot. The log
    // `edits-{snapshot_id}` holds every VersionEdit appended since the snapshot
    // was written; applying them in order reconstructs the current version.
    // A writer-incomplete trailing edit is rolled back under
    // `TolerateCorruptedTailRecords` and surfaces `TornManifestEditLog` under
    // the default `AbsoluteConsistency`; a fully-framed edit that does not
    // verify fails the open in every mode. A clean end-of-log is always
    // accepted. Each applied edit advances `recovery.curr_version_id` past the
    // snapshot's id.
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
