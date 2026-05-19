// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

use super::{block_index::BlockIndexImpl, meta::ParsedMeta, regions::ParsedRegions};
use crate::deletion_pause::DeletionPause;
use crate::{
    Checksum, GlobalTableId, SeqNo,
    cache::Cache,
    comparator::SharedComparator,
    encryption::EncryptionProvider,
    file_accessor::FileAccessor,
    fs::Fs,
    range_tombstone::RangeTombstone,
    table::{IndexBlock, filter::block::FilterBlock},
    tree::inner::TreeId,
};
use std::{
    path::PathBuf,
    sync::{Arc, OnceLock, atomic::AtomicBool},
};

pub struct Inner {
    pub path: Arc<PathBuf>,

    pub(crate) tree_id: TreeId,

    #[doc(hidden)]
    pub(crate) file_accessor: FileAccessor,

    /// Filesystem backend for file operations (open, remove, etc.).
    pub(crate) fs: Arc<dyn Fs>,

    /// Parsed metadata
    #[doc(hidden)]
    pub metadata: ParsedMeta,

    /// Parsed region block handles
    #[doc(hidden)]
    pub regions: ParsedRegions,

    /// Translates key (first item of a block) to block offset (address inside file) and (compressed) size
    #[doc(hidden)]
    pub block_index: Arc<BlockIndexImpl>,

    /// Block cache
    ///
    /// Stores index and data blocks
    #[doc(hidden)]
    pub cache: Arc<Cache>,

    /// Pinned filter index (in case of partitioned filters)
    pub(super) pinned_filter_index: Option<IndexBlock>,

    /// Pinned AMQ filter
    pub pinned_filter_block: Option<FilterBlock>,

    /// True when the table was compacted away or dropped
    ///
    /// May be kept alive until all Arcs to the table have been dropped (to facilitate snapshots)
    pub is_deleted: AtomicBool,

    pub(super) checksum: Checksum,

    pub(super) global_seqno: SeqNo,

    pub(crate) comparator: SharedComparator,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,

    /// Cached sum of referenced blob file bytes for this table.
    /// Lazily computed on first access to avoid repeated I/O in compaction decisions.
    pub(crate) cached_blob_bytes: OnceLock<u64>,

    /// Range tombstones stored in this table. Loaded on open.
    pub(crate) range_tombstones: Vec<RangeTombstone>,

    /// Block encryption provider for encryption at rest.
    pub(crate) encryption: Option<Arc<dyn EncryptionProvider>>,

    /// Pre-trained zstd dictionary for dictionary decompression.
    #[cfg(zstd_any)]
    pub(crate) zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,

    /// Tree-wide file-deletion gate. Installed once by
    /// [`Table::install_deletion_pause`](super::Table::install_deletion_pause)
    /// after the table is registered with a tree. When `Some` and active,
    /// the [`Drop`] impl defers the underlying `remove_file` call so that
    /// an in-progress [`Tree::create_checkpoint`](crate::Tree::create_checkpoint)
    /// can hard-link the file before it disappears.
    pub(crate) deletion_pause: OnceLock<Arc<DeletionPause>>,
}

impl Inner {
    /// Gets the global table ID.
    #[must_use]
    pub(super) fn global_id(&self) -> GlobalTableId {
        (self.tree_id, self.metadata.id).into()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let global_id = self.global_id();

        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            log::trace!("Cleanup deleted table {global_id:?} at {:?}", self.path);

            // Move the accessor and block index out so all file handles
            // (including clones held by the block index) are closed before
            // attempting deletion. On Windows, remove_file fails while any
            // handle is open.
            let file_accessor = std::mem::replace(&mut self.file_accessor, FileAccessor::Closed);
            let block_index =
                std::mem::replace(&mut self.block_index, Arc::new(BlockIndexImpl::Closed));

            // Evict cached FD from the descriptor table.
            file_accessor.as_descriptor_table().inspect(|d| {
                d.remove_for_table(&global_id);
            });

            // Drop the accessor and block index (releases all Arc<dyn FsFile>).
            drop(file_accessor);
            drop(block_index);

            // If a checkpoint is active, defer the physical deletion so the
            // file remains hard-linkable until the checkpoint releases its
            // pause. Falls through to immediate removal when no pause is
            // installed or the pause is inactive.
            if let Some(pause) = self.deletion_pause.get()
                && pause.try_enqueue(Arc::clone(&self.fs), (*self.path).clone())
            {
                log::trace!(
                    "Deferred deletion of table {global_id:?} at {:?} (checkpoint active)",
                    self.path,
                );
                return;
            }

            if let Err(e) = self.fs.remove_file(&self.path) {
                log::warn!(
                    "Failed to cleanup deleted table {global_id:?} at {:?}: {e:?}",
                    self.path,
                );
            }
        }
    }
}
