// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

pub mod merge;
pub mod meta;
pub mod multi_writer;
pub mod reader;
pub mod scanner;
pub mod writer;

use crate::path::{Path, PathBuf};
use crate::{
    Checksum, GlobalTableId, TreeId, blob_tree::FragmentationMap, deletion_pause::DeletionPause,
    file_accessor::FileAccessor, fs::Fs, vlog::BlobFileId,
};
#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
use alloc::sync::Arc;
use core::sync::atomic::AtomicBool;
pub use meta::Metadata;

/// A blob file is an immutable, sorted, contiguous file that contains large key-value pairs (blobs)
//
// `#[derive(Debug)]` cannot be used because [`Fs`] is not `Debug` (trait
// objects without an explicit `Debug` bound would require boxing through
// `dyn Debug`). A manual impl that prints stable identifiers gives the
// same operational ergonomics as the previous derived `Debug` without
// pulling `Debug` into the `Fs` trait bound (which would cascade through
// every backend).
pub struct Inner {
    /// Blob file ID
    pub id: BlobFileId,

    pub tree_id: TreeId,

    /// File path
    pub path: PathBuf,

    /// Statistics
    pub meta: Metadata,

    /// Whether this blob file is deleted (logically)
    pub is_deleted: AtomicBool,

    /// Tight-space punch-on-drop offset, or [`u64::MAX`] (default) for "no
    /// punch". When tight-space blob relocation rewrites this file's live
    /// entries below an offset into a fresh compact file, the PRIOR view is
    /// marked here with that absolute data-section offset; once every reader
    /// holding it drops, this view's [`Drop`] reclaims the consumed
    /// `[data_start, offset)` data frames via
    /// [`Fs::punch_hole`](crate::fs::Fs::punch_hole) and LEAVES the file in
    /// place (the restricted view still serves the suffix). Mirrors
    /// `table::Inner::punch_on_drop`. Distinct from [`Self::is_deleted`].
    pub(crate) punch_on_drop: portable_atomic::AtomicU64,

    pub checksum: Checksum,

    pub(crate) file_accessor: FileAccessor,

    /// Filesystem backend used by [`Drop`] for the physical removal.
    /// Carries the same `Fs` instance the file was opened through so that
    /// in-memory and routed-tier backends behave consistently with the
    /// rest of the tree.
    pub(crate) fs: Arc<dyn Fs>,

    /// Tree-wide file-deletion gate. Installed once by
    /// [`BlobFile::install_deletion_pause`] after the file is registered
    /// with a tree. When `Some` and active, the [`Drop`] impl defers the
    /// underlying `remove_file` so an in-progress checkpoint can hard-link
    /// the file before it disappears.
    // `once_cell::race::OnceBox` — see Table::Inner::deletion_pause
    // for the rationale (no-std-friendly one-shot slot).
    pub(crate) deletion_pause: once_cell::race::OnceBox<Arc<DeletionPause>>,

    /// Tree-wide background file deleter. See
    /// [`Table::install_background_deleter`](crate::Table) for the contract:
    /// when present (and no checkpoint pause is active) the [`Drop`] impl frees
    /// the blob file's blocks synchronously via
    /// [`Fs::truncate_file`](crate::fs::Fs::truncate_file) and hands the
    /// directory-entry `unlink` to this deleter, off the foreground path.
    // std-only (the deleter spawns a thread); see Table::Inner for rationale.
    #[cfg(feature = "std")]
    pub(crate) background_deleter: once_cell::race::OnceBox<Arc<crate::BackgroundDeleter>>,
}

impl Inner {
    fn global_id(&self) -> GlobalTableId {
        GlobalTableId::from((self.tree_id, self.id))
    }
}

impl core::fmt::Debug for Inner {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("blob_file::Inner")
            .field("id", &self.id)
            .field("tree_id", &self.tree_id)
            .field("path", &self.path)
            .field(
                "is_deleted",
                &self.is_deleted.load(core::sync::atomic::Ordering::Relaxed),
            )
            .field("meta", &self.meta)
            .finish_non_exhaustive()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.is_deleted.load(core::sync::atomic::Ordering::Acquire) {
            log::trace!(
                "Cleanup deleted blob file {:?} at {}",
                self.id,
                self.path.display(),
            );

            // Move the accessor out and drop it FIRST so every pinned
            // Arc<dyn FsFile> the file_accessor holds is released before
            // we try to unlink. On Windows (and any other platform where
            // an open handle blocks unlink) a live handle here would
            // make remove_file fail silently, leaking the blob file's
            // disk space — the same hazard already handled in
            // table::Inner::drop. Eviction from the descriptor table
            // happens through the same accessor before the drop.
            let global_id = self.global_id();
            let file_accessor = core::mem::replace(&mut self.file_accessor, FileAccessor::Closed);
            file_accessor
                .as_descriptor_table()
                .inspect(|d| d.remove_for_blob_file(&global_id));
            drop(file_accessor);

            // If a checkpoint is active, defer the physical deletion so the
            // file remains hard-linkable until the checkpoint releases its
            // pause. Short-circuit on the common no-checkpoint path: skip
            // the Arc<dyn Fs> bump and PathBuf clone unless a pause is
            // both installed AND currently active. `try_enqueue` still
            // re-checks `is_active()` under the queue lock to close the
            // publish-then-release race, so the outer check is pure perf.
            let deferred = match self.deletion_pause.get() {
                Some(pause) if pause.is_active() => {
                    pause.try_enqueue(Arc::clone(&self.fs), self.path.clone())
                }
                _ => false,
            };

            if deferred {
                log::trace!(
                    "Deferred deletion of blob file {:?} at {} (checkpoint active)",
                    self.id,
                    self.path.display(),
                );
                return;
            }

            // Off-foreground reclaim: free the blocks synchronously (accurate
            // footprint scan) and hand the unlink to the background deleter.
            // Falls through to a synchronous remove_file when none installed.
            #[cfg(feature = "std")]
            if let Some(deleter) = self.background_deleter.get() {
                // Truncate only when we own the sole hard link — a checkpoint
                // may have hard-linked this blob file, and truncating the shared
                // inode would zero the checkpoint's copy. Otherwise skip the
                // truncate and just unlink (data survives via the other link).
                if self.fs.hard_link_count(&self.path).is_ok_and(|n| n <= 1)
                    && let Err(e) = self.fs.truncate_file(&self.path)
                {
                    log::warn!(
                        "Failed to truncate deleted blob file {:?} at {}: {e:?}",
                        self.id,
                        self.path.display(),
                    );
                }
                deleter.enqueue(Arc::clone(&self.fs), self.path.clone());
                return;
            }

            if let Err(e) = self.fs.remove_file(&self.path) {
                log::warn!(
                    "Failed to cleanup deleted blob file {:?} at {}: {e:?}",
                    self.id,
                    self.path.display(),
                );
            }
        } else {
            // Not deleted, but possibly marked for tight-space prefix reclaim:
            // this (old) view's last Arc is dropping, so no reader can touch the
            // relocated prefix anymore. Punch the consumed data frames
            // `[data_start, offset)` and LEAVE the file — the restricted view (a
            // distinct Inner) still serves the suffix. A blob file is an SFA
            // archive, so the punch must start at the `data` section (skip the
            // header); the TOC sits at the tail and stays intact. `offset` is an
            // absolute data-section position (a frame boundary from the
            // relocation scanner). Re-read the data-section start from the TOC
            // here rather than carrying it on every blob-file Inner — the punch
            // is a rare, tight-space-only path.
            let off = self
                .punch_on_drop
                .load(core::sync::atomic::Ordering::Acquire);
            if off != u64::MAX {
                match data_section_start(&*self.fs, &self.path) {
                    Ok(data_start) if off > data_start => {
                        if let Err(e) = self.fs.punch_hole(&self.path, data_start, off - data_start)
                        {
                            log::warn!(
                                "Failed to punch tight-space data [{data_start}, {off}) of blob file {:?} at {}: {e:?}",
                                self.id,
                                self.path.display(),
                            );
                        }
                    }
                    Ok(_) => {} // nothing consumed below the data start
                    Err(e) => log::warn!(
                        "Skipping tight-space punch of blob file {:?} at {}: could not read data section: {e:?}",
                        self.id,
                        self.path.display(),
                    ),
                }
            }
        }
    }
}

/// Byte offset where a blob file's `data` section begins, read from its SFA TOC.
/// Used by the tight-space punch so it reclaims only data frames and never the
/// SFA header that precedes them.
#[cfg(feature = "std")]
fn data_section_start(fs: &dyn Fs, path: &Path) -> crate::Result<u64> {
    let mut file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
    let reader = crate::sfa::Reader::from_reader(&mut file)?;
    let data = reader
        .toc()
        .section(b"data")
        .ok_or(crate::Error::InvalidHeader("BlobFile"))?;
    Ok(data.pos())
}

/// A blob file stores large values and is part of the value log
#[derive(Clone)]
pub struct BlobFile(pub(crate) Arc<Inner>);

impl Eq for BlobFile {}

impl PartialEq for BlobFile {
    fn eq(&self, other: &Self) -> bool {
        self.id().eq(&other.id())
    }
}

impl core::hash::Hash for BlobFile {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

impl BlobFile {
    pub(crate) fn mark_as_deleted(&self) {
        self.0
            .is_deleted
            .store(true, core::sync::atomic::Ordering::Release);
    }

    /// Marks this view to punch the consumed `[0, offset)` frames when its last
    /// `Arc` drops (see [`Inner::punch_on_drop`]). Set on the prior view once a
    /// tight-space relocation slice has moved its `[0, offset)` live entries
    /// into a fresh compact file and that move is durably installed.
    #[cfg(feature = "std")]
    #[expect(
        dead_code,
        reason = "consumed by the tight-space blob relocation loop landing next on this branch"
    )]
    pub(crate) fn mark_punch_on_drop(&self, offset: u64) {
        self.0
            .punch_on_drop
            .store(offset, core::sync::atomic::Ordering::Release);
    }

    /// Installs the tree-wide deletion pause used by checkpoints.
    /// Idempotent: a second call is a no-op.
    pub(crate) fn install_deletion_pause(&self, pause: Arc<DeletionPause>) {
        let _ = self.0.deletion_pause.set(Box::new(pause));
    }

    /// Installs the tree-wide background file deleter. Idempotent.
    #[cfg(feature = "std")]
    pub(crate) fn install_background_deleter(&self, deleter: Arc<crate::BackgroundDeleter>) {
        let _ = self.0.background_deleter.set(Box::new(deleter));
    }

    /// Returns the blob file ID.
    #[must_use]
    pub fn id(&self) -> BlobFileId {
        self.0.id
    }

    /// Returns the full blob file checksum.
    #[must_use]
    pub fn checksum(&self) -> Checksum {
        self.0.checksum
    }

    /// Returns the blob file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.0.path
    }

    /// Returns the blob file accessor.
    #[must_use]
    pub(crate) fn file_accessor(&self) -> &FileAccessor {
        &self.0.file_accessor
    }

    /// Returns the number of items in the blob file.
    #[must_use]
    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.0.meta.item_count
    }

    /// Physical on-disk file size in bytes, including the per-entry framing
    /// (V4 header + key) and the metadata block / trailer — not just the
    /// compressed payload (`meta.total_compressed_bytes`). Used as a
    /// conservative upper bound on the transient output of a blob relocation:
    /// the rewritten file re-emits the same framing, so the source file's
    /// physical size bounds the output (and includes the dead blobs a relocation
    /// drops, making it strictly conservative).
    ///
    /// # Errors
    ///
    /// Returns an error if the blob file's size cannot be stat-ed.
    pub(crate) fn physical_size(&self) -> crate::Result<u64> {
        Ok(self.0.fs.metadata(&self.0.path)?.len)
    }

    /// Returns `true` if the blob file is stale (based on the given staleness threshold).
    pub(crate) fn is_stale(&self, frag_map: &FragmentationMap, threshold: f32) -> bool {
        frag_map.get(&self.id()).is_some_and(|x| {
            #[expect(
                clippy::cast_precision_loss,
                reason = "ok to lose precision as this is an approximate calculation"
            )]
            let stale_bytes = x.bytes as f32;
            #[expect(
                clippy::cast_precision_loss,
                reason = "ok to lose precision as this is an approximate calculation"
            )]
            let all_bytes = self.0.meta.total_uncompressed_bytes as f32;
            let ratio = stale_bytes / all_bytes;
            ratio >= threshold
        })
    }

    /// Returns `true` if the blob file has no more incoming references, and can be safely removed from a Version.
    pub(crate) fn is_dead(&self, frag_map: &FragmentationMap) -> bool {
        frag_map.get(&self.id()).is_some_and(|x| {
            let stale_bytes = x.bytes;
            let all_bytes = self.0.meta.total_uncompressed_bytes;
            stale_bytes == all_bytes
        })
    }
}
