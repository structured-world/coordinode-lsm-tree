// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod accessor;
pub mod blob_file;
mod handle;

pub use {
    accessor::Accessor, blob_file::merge::MergeScanner as BlobFileMergeScanner,
    blob_file::multi_writer::MultiWriter as BlobFileWriter,
    blob_file::scanner::Scanner as BlobFileScanner, blob_file::BlobFile, handle::ValueHandle,
};

use crate::{
    file_accessor::FileAccessor,
    fs::Fs,
    vlog::blob_file::{Inner as BlobFileInner, Metadata},
    Checksum, DescriptorTable, TreeId,
};
use std::{
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};

pub fn recover_blob_files(
    folder: &Path,
    ids: &[(BlobFileId, Checksum)],
    tree_id: TreeId,
    descriptor_table: Option<&Arc<DescriptorTable>>,
    fs: &Arc<dyn Fs>,
) -> crate::Result<(Vec<BlobFile>, Vec<PathBuf>)> {
    if !fs.exists(folder)? {
        return Ok((vec![], vec![]));
    }

    let cnt = ids.len();

    let progress_mod = match cnt {
        _ if cnt <= 20 => 1,
        _ if cnt <= 100 => 10,
        _ => 100,
    };

    log::debug!("Recovering {cnt} blob files from {:?}", folder.display());

    let mut blob_files = Vec::with_capacity(ids.len());
    let mut orphaned_blob_files = vec![];

    for (idx, dirent) in fs.read_dir(folder)?.into_iter().enumerate() {
        let file_name = &dirent.file_name;

        // https://en.wikipedia.org/wiki/.DS_Store
        if file_name == ".DS_Store" {
            continue;
        }

        // https://en.wikipedia.org/wiki/AppleSingle_and_AppleDouble_formats
        if file_name.starts_with("._") {
            continue;
        }

        let blob_file_id = file_name.parse::<BlobFileId>().map_err(|e| {
            log::error!("invalid blob file name {file_name:?}: {e:?}");
            crate::Error::Unrecoverable
        })?;

        let blob_file_path = &dirent.path;
        if dirent.is_dir {
            continue;
        }

        if let Some(&(_, checksum)) = ids.iter().find(|(id, _)| id == &blob_file_id) {
            log::trace!(
                "Recovering blob file #{blob_file_id:?} from {}",
                blob_file_path.display(),
            );

            let mut file = fs.open(blob_file_path, &crate::fs::FsOpenOptions::new().read(true))?;

            let meta = {
                let reader = sfa::Reader::from_reader(&mut file)?;
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

            let file_accessor = if let Some(dt) = descriptor_table.cloned() {
                FileAccessor::DescriptorTable {
                    table: dt,
                    fs: fs.clone(),
                }
            } else {
                FileAccessor::File(Arc::from(file))
            };

            blob_files.push(BlobFile(Arc::new(BlobFileInner {
                id: blob_file_id,
                path: blob_file_path.clone(),
                meta,
                is_deleted: AtomicBool::new(false),
                checksum,
                file_accessor,
                tree_id,
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

    log::debug!("Successfully recovered {} blob files", blob_files.len());

    Ok((blob_files, orphaned_blob_files))
}

/// The unique identifier for a value log blob file.
pub type BlobFileId = u64;

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn vlog_recovery_missing_blob_file() {
        assert!(matches!(
            recover_blob_files(
                Path::new("."),
                &[(0, Checksum::from_raw(0))],
                0,
                None,
                &(Arc::new(crate::fs::StdFs) as Arc<dyn crate::fs::Fs>)
            ),
            Err(crate::Error::Unrecoverable),
        ));
    }
}
