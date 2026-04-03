// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::GlobalTableId;
use crate::descriptor_table::DescriptorTable;
use crate::fs::{Fs, FsFile, FsOpenOptions};
use std::path::Path;
use std::sync::Arc;

/// Allows accessing a file (either cached or pinned)
#[derive(Clone)]
pub enum FileAccessor {
    /// Pinned file descriptor
    ///
    /// This is used in case file descriptor cache is `None` (to skip cache lookups)
    File(Arc<dyn FsFile>),

    /// Access to file descriptor cache with [`Fs`]-based fallback for
    /// cache misses.
    DescriptorTable {
        /// The FD cache.
        table: Arc<DescriptorTable>,
        /// Filesystem backend for opening files on cache miss.
        fs: Arc<dyn Fs>,
    },
}

impl FileAccessor {
    #[must_use]
    pub fn as_descriptor_table(&self) -> Option<&DescriptorTable> {
        match self {
            Self::DescriptorTable { table, .. } => Some(table),
            Self::File(_) => None,
        }
    }

    /// Returns a cached table FD or opens the file via [`Fs`] on cache miss.
    ///
    /// The returned `bool` indicates whether the file descriptor was already
    /// cached (`true`) or freshly opened (`false`).
    pub fn get_or_open_table(
        &self,
        table_id: &GlobalTableId,
        path: &Path,
    ) -> std::io::Result<(Arc<dyn FsFile>, bool)> {
        match self {
            Self::File(fd) => Ok((fd.clone(), true)),
            Self::DescriptorTable { table, fs } => {
                if let Some(fd) = table.access_for_table(table_id) {
                    return Ok((fd, true));
                }
                let fd: Arc<dyn FsFile> =
                    Arc::from(fs.open(path, &FsOpenOptions::new().read(true))?);
                table.insert_for_table(*table_id, fd.clone());
                Ok((fd, false))
            }
        }
    }

    /// Returns a cached blob file FD or opens it via [`Fs`] on cache miss.
    ///
    /// The returned `bool` indicates whether the file descriptor was already
    /// cached (`true`) or freshly opened (`false`).
    pub fn get_or_open_blob_file(
        &self,
        table_id: &GlobalTableId,
        path: &Path,
    ) -> std::io::Result<(Arc<dyn FsFile>, bool)> {
        match self {
            Self::File(fd) => Ok((fd.clone(), true)),
            Self::DescriptorTable { table, fs } => {
                if let Some(fd) = table.access_for_blob_file(table_id) {
                    return Ok((fd, true));
                }
                let fd: Arc<dyn FsFile> =
                    Arc::from(fs.open(path, &FsOpenOptions::new().read(true))?);
                table.insert_for_blob_file(*table_id, fd.clone());
                Ok((fd, false))
            }
        }
    }

    /// Pre-populates the blob file FD cache after creating a new blob file.
    pub fn insert_for_blob_file(&self, table_id: GlobalTableId, fd: Arc<dyn FsFile>) {
        if let Self::DescriptorTable { table, .. } = self {
            table.insert_for_blob_file(table_id, fd);
        }
    }

    pub fn remove_for_table(&self, table_id: &GlobalTableId) {
        if let Self::DescriptorTable { table, .. } = self {
            table.remove_for_table(table_id);
        }
    }

    pub fn remove_for_blob_file(&self, table_id: &GlobalTableId) {
        if let Self::DescriptorTable { table, .. } = self {
            table.remove_for_blob_file(table_id);
        }
    }
}

impl std::fmt::Debug for FileAccessor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::File(_) => write!(f, "FileAccessor::Pinned"),
            Self::DescriptorTable { .. } => {
                write!(f, "FileAccessor::Cached")
            }
        }
    }
}
