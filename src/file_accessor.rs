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

    /// Sentinel used during [`Drop`] to move ownership of the file handle
    /// before deleting the underlying file. Not constructed outside `Drop`.
    #[doc(hidden)]
    Closed,
}

impl FileAccessor {
    #[must_use]
    pub fn as_descriptor_table(&self) -> Option<&DescriptorTable> {
        match self {
            Self::DescriptorTable { table, .. } => Some(table),
            Self::File(_) | Self::Closed => None,
        }
    }

    /// Returns a table FD, opening via [`Fs`] on descriptor-table cache miss.
    ///
    /// Returns `(fd, None)` for pinned FDs (no cache involved),
    /// `(fd, Some(true))` for descriptor-table cache hit,
    /// `(fd, Some(false))` for cache miss (freshly opened and cached).
    pub fn get_or_open_table(
        &self,
        table_id: &GlobalTableId,
        path: &Path,
    ) -> std::io::Result<(Arc<dyn FsFile>, Option<bool>)> {
        match self {
            Self::File(fd) => Ok((fd.clone(), None)),
            Self::DescriptorTable { table, fs } => {
                if let Some(fd) = table.access_for_table(table_id) {
                    return Ok((fd, Some(true)));
                }
                let fd: Arc<dyn FsFile> =
                    Arc::from(fs.open(path, &FsOpenOptions::new().read(true))?);
                table.insert_for_table(*table_id, fd.clone());
                Ok((fd, Some(false)))
            }
            Self::Closed => Err(std::io::Error::other("file accessor closed")),
        }
    }

    /// Returns a blob file FD, opening via [`Fs`] on descriptor-table cache miss.
    ///
    /// See [`get_or_open_table`](Self::get_or_open_table) for
    /// semantics of the `Option<bool>` cache-hit indicator.
    pub fn get_or_open_blob_file(
        &self,
        table_id: &GlobalTableId,
        path: &Path,
    ) -> std::io::Result<(Arc<dyn FsFile>, Option<bool>)> {
        match self {
            Self::File(fd) => Ok((fd.clone(), None)),
            Self::DescriptorTable { table, fs } => {
                if let Some(fd) = table.access_for_blob_file(table_id) {
                    return Ok((fd, Some(true)));
                }
                let fd: Arc<dyn FsFile> =
                    Arc::from(fs.open(path, &FsOpenOptions::new().read(true))?);
                table.insert_for_blob_file(*table_id, fd.clone());
                Ok((fd, Some(false)))
            }
            Self::Closed => Err(std::io::Error::other("file accessor closed")),
        }
    }

    /// Pre-populates the blob file FD cache after creating a new blob file.
    pub fn insert_for_blob_file(&self, table_id: GlobalTableId, fd: Arc<dyn FsFile>) {
        if let Self::DescriptorTable { table, .. } = self {
            table.insert_for_blob_file(table_id, fd);
        }
    }

    /// Removes a table FD from the descriptor cache.
    ///
    /// No-op for pinned `Self::File` variant (no cache to evict from).
    pub fn remove_for_table(&self, table_id: &GlobalTableId) {
        if let Self::DescriptorTable { table, .. } = self {
            table.remove_for_table(table_id);
        }
    }

    /// Removes a blob file FD from the descriptor cache.
    ///
    /// No-op for pinned `Self::File` variant (no cache to evict from).
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
                write!(f, "FileAccessor::DescriptorTable")
            }
            Self::Closed => write!(f, "FileAccessor::Closed"),
        }
    }
}
