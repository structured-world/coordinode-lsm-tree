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

pub fn recover_blob_files(
    folder: &Path,
    ids: &[(BlobFileId, Checksum)],
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

        // https://en.wikipedia.org/wiki/.DS_Store
        if file_name == ".DS_Store" {
            continue;
        }

        // https://en.wikipedia.org/wiki/AppleSingle_and_AppleDouble_formats
        if file_name.starts_with("._") {
            continue;
        }

        // Skip directories before parsing — non-numeric directory names would
        // fail the parse and abort recovery.
        if dirent.is_dir {
            continue;
        }

        let blob_file_id = file_name.parse::<BlobFileId>().map_err(|e| {
            log::error!("invalid blob file name {file_name:?}: {e:?}");
            crate::Error::Unrecoverable
        })?;

        let blob_file_path = &dirent.path;

        if let Some(&(_, checksum)) = ids.iter().find(|(id, _)| id == &blob_file_id) {
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
                pending_cache_inserts.push((dt.clone(), global_id, file.clone()));
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

    if blob_files.len() < ids.len() {
        return Err(crate::Error::Unrecoverable);
    }

    // All blobs parsed successfully — commit FDs to the descriptor cache.
    for (dt, global_id, file) in pending_cache_inserts {
        dt.insert_for_blob_file(global_id, file);
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
