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

/// `io::Error::kind` values that we interpret as a clean tail truncation
/// inside the per-record loop. `UnexpectedEof` is the canonical signal
/// from `Read::read_exact` / `byteorder::ReadBytesExt` when the reader
/// runs out of bytes mid-record; any other kind means a different
/// failure and is not eligible for tail-tolerant recovery.
fn is_clean_tail_truncation(e: &crate::Error) -> bool {
    matches!(
        e,
        crate::Error::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof
    )
}

/// Reads one 33-byte table-run entry from a section reader. Each entry:
/// `id: u64 | checksum_type: u8 | checksum: u128 | global_seqno: u64`.
/// Tail-tolerance: a partial-read past the section bound surfaces as
/// `Error::Io(UnexpectedEof)`, which `recover()` recognises as a clean
/// tail-truncation signal under `TolerateCorruptedTailRecords`. A
/// non-zero `checksum_type` byte is NOT a tail-truncation case — that's
/// a forged/corrupt record and aborts regardless of mode.
fn read_one_table_entry<R: std::io::Read>(reader: &mut R) -> crate::Result<RecoveredTable> {
    let id = reader.read_u64::<LittleEndian>()?;
    let checksum_type = reader.read_u8()?;
    if checksum_type != 0 {
        return Err(crate::Error::InvalidTag(("ChecksumType", checksum_type)));
    }
    let checksum = Checksum::from_raw(reader.read_u128::<LittleEndian>()?);
    let global_seqno = reader.read_u64::<LittleEndian>()?;
    Ok(RecoveredTable {
        id,
        checksum,
        global_seqno,
    })
}

/// Reads one 25-byte blob-file entry:
/// `id: u64 | checksum_type: u8 | checksum: u128`. Same
/// tail-truncation contract as [`read_one_table_entry`].
fn read_one_blob_entry<R: std::io::Read>(reader: &mut R) -> crate::Result<(BlobFileId, Checksum)> {
    let id = reader.read_u64::<LittleEndian>()?;
    let checksum_type = reader.read_u8()?;
    if checksum_type != 0 {
        return Err(crate::Error::InvalidTag(("ChecksumType", checksum_type)));
    }
    let checksum = Checksum::from_raw(reader.read_u128::<LittleEndian>()?);
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
    let curr_version_id = get_current_version(folder, fs)?;
    let version_file_path = folder.join(format!("v{curr_version_id}"));

    log::info!(
        "Recovering current manifest at {} (mode={mode:?})",
        version_file_path.display(),
    );

    let mut file = fs.open(&version_file_path, &FsOpenOptions::new().read(true))?;
    let reader = sfa::Reader::from_reader(&mut file)?;
    let toc = reader.toc();

    let tolerate_tail = matches!(mode, ManifestRecoveryMode::TolerateCorruptedTailRecords);

    // // TODO: vvv move into Version::decode vvv
    let mut levels = vec![];
    let mut tables_dropped_to_tail: u32 = 0;

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
        let level_count = match reader.read_u8() {
            Ok(n) => n,
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
                Ok(n) => n,
                Err(e) if tolerate_tail && e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // No runs in this level had a chance to start;
                    // pushing the empty `level` would be a "zero
                    // runs at level N" marker which downstream code
                    // tolerates. Stay consistent with the cut-mid-run
                    // path below (which DOES push) instead of silently
                    // dropping the level slot.
                    tables_dropped_to_tail += 1;
                    levels.push(level);
                    break 'levels;
                }
                Err(e) => return Err(e.into()),
            };

            for _ in 0..run_count {
                let mut run = vec![];
                let table_count = match reader.read_u32::<LittleEndian>() {
                    Ok(n) => n,
                    Err(e) if tolerate_tail && e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        // Push the partial `level` so any runs that
                        // were fully decoded earlier in this level
                        // survive — breaking out of `'levels` without
                        // this push silently drops the consistent
                        // prefix, contradicting the tail-tolerant
                        // contract. The current `run` is empty (we
                        // failed at the very first byte of its
                        // record header), so nothing to push for it.
                        tables_dropped_to_tail += 1;
                        levels.push(level);
                        break 'levels;
                    }
                    Err(e) => return Err(e.into()),
                };

                // Bound by total section length (33 bytes per entry). Uses
                // section.len() because BufReader buffering makes
                // Take::limit() unreliable for remaining-byte checks.
                //
                // Under `AbsoluteConsistency` this is a hard "the count
                // header was forged / bit-flipped — abort" check. Under
                // `TolerateCorruptedTailRecords` the same condition
                // legitimately fires when the writer was interrupted
                // mid-run (count was committed but the trailing entries
                // weren't), so it must drop to tail-truncation instead
                // of aborting. The per-entry decode loop below then
                // surfaces an `UnexpectedEof` for each missing entry,
                // which the tail-tolerant match arm catches normally.
                if u64::from(table_count) > section.len() / 33 {
                    if tolerate_tail {
                        log::warn!(
                            "tables: declared table_count={table_count} exceeds section \
                             capacity (~{} entries) in version #{curr_version_id}; \
                             tail-tolerant mode walks bytes-actually-present and stops \
                             at the first EOF",
                            section.len() / 33,
                        );
                    } else {
                        return Err(crate::Error::Unrecoverable);
                    }
                }

                for _ in 0..table_count {
                    let entry = read_one_table_entry(&mut reader);
                    match entry {
                        Ok(t) => run.push(t),
                        Err(e) if tolerate_tail && is_clean_tail_truncation(&e) => {
                            // run.len() is bounded by table_count
                            // (which we read above as u32), so the
                            // cast cannot truncate in practice. Use
                            // try_from with a saturating fallback
                            // anyway to keep clippy happy and to
                            // avoid an unrelated panic if the bound
                            // is ever loosened.
                            let recovered = u32::try_from(run.len()).unwrap_or(u32::MAX);
                            tables_dropped_to_tail = tables_dropped_to_tail
                                .saturating_add(table_count.saturating_sub(recovered));
                            level.push(run);
                            levels.push(level);
                            break 'levels;
                        }
                        Err(e) => return Err(e),
                    }
                }

                level.push(run);
            }

            levels.push(level);
        }
    }

    if tables_dropped_to_tail > 0 {
        log::warn!(
            "manifest tail truncation dropped {tables_dropped_to_tail} table record(s) \
             from version #{curr_version_id}; recovered tree may be missing SSTs",
        );
    }

    let mut blob_dropped_to_tail: u32 = 0;
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

        // Same as the `table_count` check: count > section payload
        // is a hard fail under strict mode (the count header is
        // forged), but a power-loss-during-write that committed the
        // count then truncated the entries produces exactly this
        // shape, so tolerant mode must warn and let the per-entry
        // loop walk bytes-actually-present until the first EOF.
        if u64::from(blob_file_count) > section.len().saturating_sub(4) / 25 {
            if tolerate_tail {
                log::warn!(
                    "blob_files: declared count={blob_file_count} exceeds section \
                     capacity (~{} entries) in version #{curr_version_id}; \
                     tail-tolerant mode walks bytes-actually-present and stops at \
                     the first EOF",
                    section.len().saturating_sub(4) / 25,
                );
            } else {
                return Err(crate::Error::Unrecoverable);
            }
        }

        // Don't allocate full `blob_file_count` capacity when the
        // count overflows the section — that's a several-GB
        // Vec::with_capacity allocation on the forged-count branch.
        // Cap at the section-derived upper bound.
        let cap_hint =
            usize::try_from(u64::from(blob_file_count).min(section.len().saturating_sub(4) / 25))
                .unwrap_or(0);
        let mut blob_file_ids = Vec::with_capacity(cap_hint);

        for _ in 0..blob_file_count {
            match read_one_blob_entry(&mut reader) {
                Ok(t) => blob_file_ids.push(t),
                Err(e) if tolerate_tail && is_clean_tail_truncation(&e) => {
                    // try_from over `as u32` for clippy; the cast is
                    // safe in practice because blob_file_ids.len() is
                    // bounded by blob_file_count (u32) via the loop.
                    let recovered = u32::try_from(blob_file_ids.len()).unwrap_or(u32::MAX);
                    blob_dropped_to_tail = blob_file_count.saturating_sub(recovered);
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        blob_file_ids.sort_by_key(|(id, _)| *id);
        blob_file_ids
    };

    if blob_dropped_to_tail > 0 {
        log::warn!(
            "manifest tail truncation dropped {blob_dropped_to_tail} blob-file record(s) \
             from version #{curr_version_id}; recovered tree may be missing blob files",
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

        crate::blob_tree::FragmentationMap::decode_from(&mut reader)?
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
        // Write `actual` complete 33-byte entries, then stop. SFA will
        // pad the section length to whatever bytes we wrote — the
        // truncation surfaces inside the per-entry decode loop, not at
        // the SFA layer.
        for entry_id in 0..actual {
            w.write_u64::<LittleEndian>(u64::from(entry_id))?;
            w.write_u8(0)?; // checksum_type
            w.write_u128::<LittleEndian>(0)?; // checksum
            w.write_u64::<LittleEndian>(0)?; // global_seqno
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
        w.write_u64::<LittleEndian>(0)?; // id
        w.write_u8(0xFF)?; // corrupt checksum_type — should abort
        w.write_u128::<LittleEndian>(0)?;
        w.write_u64::<LittleEndian>(0)?;
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
        w.write_u64::<LittleEndian>(42)?; // id
        w.write_u8(0)?; // checksum_type
        w.write_u128::<LittleEndian>(0)?;
        w.write_u64::<LittleEndian>(0)?;
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
            w.write_u64::<LittleEndian>(u64::from(entry_id))?;
            w.write_u8(0)?; // checksum_type
            w.write_u128::<LittleEndian>(0)?; // checksum
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
}
