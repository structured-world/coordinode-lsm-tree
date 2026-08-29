// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

mod accessor;
pub mod blob_file;
mod handle;

pub use {
    accessor::Accessor, blob_file::BlobFile,
    blob_file::merge::MergeScanner as BlobFileMergeScanner,
    blob_file::multi_writer::MultiWriter as BlobFileWriter,
    blob_file::scanner::Scanner as BlobFileScanner, handle::ValueHandle,
};

use crate::path::{Path, PathBuf};
use crate::{
    Checksum, DescriptorTable, TreeId,
    file_accessor::FileAccessor,
    fs::Fs,
    vlog::blob_file::{Inner as BlobFileInner, Metadata},
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::sync::atomic::AtomicBool;

/// Keeps ONE recovered copy per blob id, moving the rest to `orphaned`.
///
/// The manifest names exactly one file per id and records the checksum of its
/// live suffix, so a copy that reproduces that digest IS the one it named. When
/// none does — both copies rotted, or the digest cannot be read — the canonical
/// spelling (`{id}`) is kept over any alternate one, and first-seen order
/// breaks a remaining tie. That is a deterministic answer rather than a correct
/// one, but it beats letting directory order decide, and the surviving copy's
/// damage still surfaces on the read that meets it.
///
/// # Errors
///
/// Propagates an ENVIRONMENTAL failure while reading a copy's digest. The rank
/// below would otherwise read "could not be determined" as "does not match" and
/// fall through to the filename tiebreak — and if the unreadable copy is the
/// authoritative one under an alternate spelling, the stale canonical file wins
/// and the healthy one is deleted as an orphan. A retryable fault must never
/// decide that.
fn dedupe_blob_sightings(
    fs: &Arc<dyn Fs>,
    blob_files: &mut Vec<BlobFile>,
    orphaned: &mut Vec<PathBuf>,
) -> crate::Result<()> {
    let mut seen: crate::HashMap<BlobFileId, usize> = crate::HashMap::default();
    let mut duplicate_ids: crate::HashSet<BlobFileId> = crate::HashSet::default();
    for (idx, bf) in blob_files.iter().enumerate() {
        if seen.insert(bf.id(), idx).is_some() {
            duplicate_ids.insert(bf.id());
        }
    }
    if duplicate_ids.is_empty() {
        return Ok(());
    }

    // Score each sighting: a matching digest wins outright, then the canonical
    // spelling, then first-seen. `checksum_from_with_overrides` re-reads the
    // file, so this runs ONLY on the rare id that was seen twice.
    let rank = |bf: &BlobFile| -> crate::Result<u8> {
        let digest = match crate::file::checksum_from_with_overrides(
            &**fs,
            &bf.0.path,
            bf.0.live_data_start,
            &[],
        ) {
            Ok(d) => Some(d),
            // A fault in the ENVIRONMENT says nothing about which copy the
            // manifest named, and guessing costs the loser its file.
            Err(e) if e.is_environmental() => return Err(e),
            // A read that fails on the BYTES is evidence: this copy cannot be
            // the intact one the manifest digested.
            Err(_) => None,
        };
        if digest.is_some_and(|d| Checksum::from_raw(d) == bf.0.checksum) {
            return Ok(0);
        }
        // Rebuilt rather than string-compared: `file_name` is `&OsStr` with
        // `std` and `&str` without it, and the join answers both.
        let canonical = bf.0.path.parent().is_some_and(|dir| {
            dir.join(alloc::string::ToString::to_string(&bf.id())) == *bf.0.path
        });
        Ok(if canonical { 1 } else { 2 })
    };

    let mut best: crate::HashMap<BlobFileId, (u8, usize)> = crate::HashMap::default();
    for (idx, bf) in blob_files.iter().enumerate() {
        let score = rank(bf)?;
        match best.get(&bf.id()) {
            Some(&(prev, _)) if prev <= score => {}
            _ => {
                best.insert(bf.id(), (score, idx));
            }
        }
    }
    // The manifest digested exactly one file per id. When an id has duplicates
    // and NONE of them reproduces that digest, the filename is not a tiebreak,
    // it is a guess between two wrong answers, and acting on it opens the tree
    // on the wrong generation while deleting the other copy as an orphan. The
    // damaged authoritative file may still be salvageable frame by frame, which
    // a repair can do and this cannot. Refuse instead.
    if let Some(id) = best
        .iter()
        .find(|(id, (score, _))| *score > 0 && duplicate_ids.contains(*id))
        .map(|(id, _)| *id)
    {
        log::error!(
            "blob file {id} has multiple copies and none reproduces the manifest's \
             checksum; refusing to pick one. Run a repair, which can salvage the \
             authoritative copy's intact frames",
        );
        return Err(crate::Error::Unrecoverable);
    }
    let winners: crate::HashSet<usize> = best.values().map(|&(_, idx)| idx).collect();
    let mut idx = 0;
    blob_files.retain(|bf| {
        let keep = winners.contains(&idx);
        if !keep {
            log::warn!(
                "blob file {} duplicates id {}; the manifest names another copy, so this one \
                 is an orphan",
                bf.0.path.display(),
                bf.id(),
            );
            orphaned.push(bf.0.path.clone());
        }
        idx += 1;
        keep
    });
    Ok(())
}

pub fn recover_blob_files(
    folder: &Path,
    ids: &[(BlobFileId, Checksum, u64)],
    tree_id: TreeId,
    descriptor_table: Option<&Arc<DescriptorTable>>,
    fs: &Arc<dyn Fs>,
) -> crate::Result<(Vec<BlobFile>, Vec<PathBuf>)> {
    // Recover directly from read_dir; treat NotFound as empty only for
    // standard (non-blob) trees where no blob folder is expected.
    // If the manifest references blob files (ids non-empty) but the folder
    // is missing, that is unrecoverable corruption — fail fast.
    let entries = match fs.read_dir(folder) {
        Ok(entries) => entries,
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {
            if ids.is_empty() {
                return Ok((vec![], vec![]));
            }
            return Err(crate::Error::Unrecoverable);
        }
        Err(e) => return Err(e.into()),
    };

    let cnt = ids.len();

    let progress_mod = match cnt {
        _ if cnt <= 20 => 1,
        _ if cnt <= 100 => 10,
        _ => 100,
    };

    log::debug!("Recovering {cnt} blob files from {:?}", folder.display());

    let mut blob_files = Vec::with_capacity(ids.len());
    let mut orphaned_blob_files = vec![];
    // Deferred cache inserts — only committed after all blobs parse
    // successfully, so a partial recovery doesn't leak FDs in the
    // descriptor table.
    let mut pending_cache_inserts = Vec::new();

    for (idx, dirent) in entries.into_iter().enumerate() {
        let file_name = &dirent.file_name;

        if dirent.is_dir {
            continue;
        }

        // The naming grammar decides, not a list of names to tolerate: a shape
        // the engine does not name is not engine state, so it is passed over
        // untouched rather than read, swept, or made a reason to refuse the
        // store.
        let blob_file_id = match crate::file::BlobDirEntry::classify(file_name) {
            crate::file::BlobDirEntry::Blob(id) => id,
            // Published by an atomic rename, so a survivor is from a crashed
            // repair and is referenced by no manifest.
            crate::file::BlobDirEntry::SalvageTmp(_) => {
                orphaned_blob_files.push(dirent.path.clone());
                continue;
            }
            crate::file::BlobDirEntry::Foreign => {
                log::debug!("Ignoring {file_name:?} in the blobs folder: not an engine file");
                continue;
            }
        };

        let blob_file_path = &dirent.path;

        if let Some(&(_, checksum, live_data_start)) =
            ids.iter().find(|(id, _, _)| id == &blob_file_id)
        {
            log::trace!(
                "Recovering blob file #{blob_file_id:?} from {}",
                blob_file_path.display(),
            );

            let mut file = fs.open(blob_file_path, &crate::fs::FsOpenOptions::new().read(true))?;

            let meta = {
                let reader = crate::sfa::Reader::from_reader(&mut file)?;
                let toc = reader.toc();

                let metadata_section = toc.section(b"meta")
                .ok_or(crate::Error::Unrecoverable)
                .inspect_err(|_| {
                    log::error!("meta section in blob file #{blob_file_id} is missing - maybe the file is corrupted?");
                })?;

                let metadata_len = usize::try_from(metadata_section.len())
                    .map_err(|_| crate::Error::Unrecoverable)?;
                let metadata_slice =
                    crate::file::read_exact(&*file, metadata_section.pos(), metadata_len)?;

                Metadata::from_slice(&metadata_slice)?
            };

            let file: Arc<dyn crate::fs::FsFile> = Arc::from(file);
            let file_accessor = if let Some(dt) = descriptor_table.cloned() {
                let global_id = (tree_id, blob_file_id).into();
                // The path rides along so deduplication can drop the handles of
                // the sightings it discards.
                pending_cache_inserts.push((
                    dt.clone(),
                    global_id,
                    blob_file_path.clone(),
                    file.clone(),
                ));
                FileAccessor::DescriptorTable {
                    table: dt,
                    fs: fs.clone(),
                }
            } else {
                FileAccessor::File(file)
            };

            blob_files.push(BlobFile(Arc::new(BlobFileInner {
                id: blob_file_id,
                path: blob_file_path.clone(),
                meta,
                is_deleted: AtomicBool::new(false),
                punch_on_drop: portable_atomic::AtomicU64::new(u64::MAX),
                checksum,
                live_data_start,
                file_accessor,
                tree_id,
                fs: fs.clone(),
                deletion_pause: once_cell::race::OnceBox::new(),

                #[cfg(feature = "std")]
                background_deleter: once_cell::race::OnceBox::new(),
            })));

            if idx % progress_mod == 0 {
                log::debug!("Recovered {idx}/{cnt} blob files");
            }
        } else {
            orphaned_blob_files.push(blob_file_path.clone());
        }
    }

    // Two directory entries can parse to the SAME id — `blobs/0` beside a
    // noncanonical `blobs/00`, which is what a repair leaves when its
    // post-commit removal of the displaced copy fails. Both matched the
    // manifest id above, and the version this feeds builds a map keyed by id,
    // so without resolving them here whichever came last would silently win:
    // a copy with readable metadata but a rotted value frame could replace the
    // authoritative blob and make reads fail. The manifest's checksum covers
    // the live suffix of the copy it named, so it decides; the losers are
    // orphans, which the caller sweeps.
    dedupe_blob_sightings(fs, &mut blob_files, &mut orphaned_blob_files)?;

    if blob_files.len() < ids.len() {
        return Err(crate::Error::Unrecoverable);
    }

    // All blobs parsed successfully — commit FDs to the descriptor cache. The
    // pending inserts hold one entry per SIGHTING, all under the same
    // `GlobalTableId`, so inserting them blindly would leave the cache holding
    // whichever handle came last — possibly the copy just discarded. Keep only
    // the handle whose path survived deduplication.
    let retained: crate::HashSet<PathBuf> = blob_files.iter().map(|bf| bf.0.path.clone()).collect();
    for (dt, global_id, path, file) in pending_cache_inserts {
        if retained.contains(&path) {
            dt.insert_for_blob_file(global_id, file);
        }
    }

    log::debug!("Successfully recovered {} blob files", blob_files.len());

    Ok((blob_files, orphaned_blob_files))
}

/// Recovers a SINGLE blob file from its path, for the manifest-rebuild
/// (`Config::repair`) path where there is no manifest id list to filter against.
///
/// Reads the file's `meta` SFA section and constructs a [`BlobFile`] bound to
/// the caller-computed `checksum`. Unlike [`recover_blob_files`] (which filters
/// a known id list and fails fast on any miss), the caller discovers ids from a
/// directory scan and handles per-file errors itself: a corrupt blob is reported
/// and left in place (it reads back as a harmless orphan on the next open)
/// rather than aborting the whole repair.
///
/// `tree_id` is `0` and there is no descriptor table, mirroring the table
/// recovery in `repair`: the repaired tree is reopened fresh afterwards, so a
/// transient handle must not pollute any shared cache.
///
/// # Errors
///
/// Returns an error if the file cannot be opened or its `meta` section is
/// missing / undecodable.
#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "single-file blob recovery for the std-gated repair surface; the no_std open path uses recover_blob_files"
    )
)]
pub fn recover_blob_file(
    path: &Path,
    id: BlobFileId,
    checksum: Checksum,
    tree_id: TreeId,
    fs: &Arc<dyn Fs>,
) -> crate::Result<BlobFile> {
    recover_blob_file_from(path, id, checksum, tree_id, fs, 0)
}

/// As [`recover_blob_file`], but for a view whose consumed prefix below
/// `live_data_start` was reclaimed by a tight-space relocation: `checksum`
/// covers only `[live_data_start, end)`, and integrity checks hash from there
/// rather than over the punched (zeroed) prefix. `live_data_start = 0` is a
/// whole, unreclaimed file.
///
/// # Errors
///
/// Propagates any error from opening or parsing the blob file.
pub fn recover_blob_file_from(
    path: &Path,
    id: BlobFileId,
    checksum: Checksum,
    tree_id: TreeId,
    fs: &Arc<dyn Fs>,
    live_data_start: u64,
) -> crate::Result<BlobFile> {
    let mut file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;

    // Same meta-section read as `recover_blob_files`' per-id branch above.
    let meta = {
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        let toc = reader.toc();
        let metadata_section = toc.section(b"meta").ok_or_else(|| {
            log::error!("meta section in blob file #{id} is missing (file may be corrupted)");
            crate::Error::Unrecoverable
        })?;
        let metadata_len =
            usize::try_from(metadata_section.len()).map_err(|_| crate::Error::Unrecoverable)?;
        let metadata_slice = crate::file::read_exact(&*file, metadata_section.pos(), metadata_len)?;
        Metadata::from_slice(&metadata_slice)?
    };

    let file: Arc<dyn crate::fs::FsFile> = Arc::from(file);
    Ok(BlobFile(Arc::new(BlobFileInner {
        id,
        path: path.to_path_buf(),
        meta,
        is_deleted: AtomicBool::new(false),
        punch_on_drop: portable_atomic::AtomicU64::new(u64::MAX),
        checksum,
        live_data_start,
        file_accessor: FileAccessor::File(file),
        tree_id,
        fs: fs.clone(),
        deletion_pause: once_cell::race::OnceBox::new(),

        #[cfg(feature = "std")]
        background_deleter: once_cell::race::OnceBox::new(),
    })))
}

/// The unique identifier for a value log blob file.
pub type BlobFileId = u64;

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests;
