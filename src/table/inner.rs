// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::{
    block_index::BlockIndexImpl, block_layout::BlockLayoutMap, meta::ParsedMeta,
    regions::ParsedRegions,
};
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
use alloc::sync::Arc;
use core::sync::atomic::AtomicBool;

use crate::path::PathBuf;
use portable_atomic::AtomicU64;

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

    /// Tight-space punch-on-drop offset, or [`u64::MAX`] (the default) for "no
    /// punch". When a tight-space compaction restricts a table to `[K, hi)`, the
    /// PRIOR (unrestricted) view of the same physical SST is marked here with
    /// `offset(K)`: once every reader holding that old view has dropped its
    /// `Arc` (so no read can touch the prefix), this view's [`Drop`] reclaims the
    /// `[0, offset)` byte range via [`Fs::punch_hole`](crate::fs::Fs::punch_hole)
    /// while LEAVING the file in place (the restricted view, a distinct `Inner`,
    /// still serves the suffix). Distinct from [`Self::is_deleted`]: a punched
    /// view is not deleted. Using the `Drop`-at-refcount-zero signal makes the
    /// punch safe without an explicit snapshot gate — it fires exactly when the
    /// old view is unreachable. Cumulative across slices and idempotent.
    pub(crate) punch_on_drop: AtomicU64,

    pub(super) checksum: Checksum,

    pub(super) global_seqno: SeqNo,

    pub(crate) comparator: SharedComparator,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,

    /// Cached sum of referenced blob file bytes for this table.
    ///
    /// Initialized to `AtomicU64::new(u64::MAX)`, the "not yet computed"
    /// sentinel (a real sum can never reach 18 EiB). Lazily computed on first
    /// access to avoid repeated I/O in compaction decisions:
    /// `Table::referenced_blob_bytes` does an `Acquire` load, returns early
    /// when the value is not `u64::MAX`, otherwise sums the table's
    /// `LinkedFile.on_disk_bytes` and publishes it with a `Release` store.
    /// The store is idempotent under races because the table's linked-blob-file
    /// region is immutable after open, so every racing computer re-reads the
    /// same on-disk byte counts and computes the same sum.
    pub(crate) cached_blob_bytes: AtomicU64,

    /// Range tombstones stored in this table. Loaded on open.
    pub(crate) range_tombstones: Vec<RangeTombstone>,

    /// Inner zstd-block layout index, loaded on open from the optional
    /// `block_layout` section. Empty (no entries) when the table has no
    /// multi-inner-block data blocks. Lets a range query partial-decode only
    /// the inner blocks covering a key range in a large cold block.
    // Read only by the zstd partial-decode path; in a no-zstd build it is still
    // loaded (always empty there, since no zstd blocks ever split) but unread.
    #[cfg_attr(
        not(feature = "zstd"),
        expect(
            dead_code,
            reason = "consumed only by the zstd partial-decode read path"
        )
    )]
    pub(crate) block_layout: BlockLayoutMap,

    /// Per-data-block seqno bounds, loaded on open from the optional
    /// `seqno_bounds` section. Empty when the table has none (legacy or
    /// `seqno_in_index` off), so `scan_since_seqno` falls back to a full
    /// per-entry filter. Read only by the seqno-scoped scan path.
    pub(crate) seqno_bounds: crate::table::seqno_bounds::SeqnoBoundsMap,

    /// Retrieval-ribbon locator, loaded on open from the optional `locator`
    /// section. `Some` only when the table was written with a locator policy
    /// enabled; lets a point read resolve a key to its data block in O(1),
    /// skipping the index-block binary search. `None` (default) leaves the
    /// point read on the sorted-index path.
    pub(crate) locator_index: Option<crate::table::locator::LoadedLocator>,

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
    // `once_cell::race::OnceBox` rather than `std::sync::OnceLock` so
    // this field doesn't pin the type to `std` — OnceBox is no-std +
    // alloc by construction. The slot is set once after recovery /
    // compaction and read on every Drop; CAS-based race semantics are
    // fine for this single-publisher, many-reader pattern.
    pub(crate) deletion_pause: once_cell::race::OnceBox<Arc<DeletionPause>>,

    /// Tree-wide background file deleter. Installed once by
    /// [`Table::install_background_deleter`](super::Table::install_background_deleter)
    /// after the table is registered with a tree. When present (and no
    /// checkpoint pause is active), the [`Drop`] impl reclaims the SST's blocks
    /// synchronously via [`Fs::truncate_file`](crate::fs::Fs::truncate_file) and
    /// hands the directory-entry `unlink` to this deleter's worker, off the
    /// foreground path. Absent (e.g. orphan cleanup before a tree owns the
    /// table) the Drop falls back to a synchronous `remove_file`.
    // std-only (the deleter spawns a thread); `no_std` builds never install
    // one and keep the synchronous Drop path. OnceBox keeps the field itself
    // alloc-friendly, matching `deletion_pause`.
    #[cfg(feature = "std")]
    pub(crate) background_deleter: once_cell::race::OnceBox<Arc<crate::BackgroundDeleter>>,

    /// Tree-wide ECC heal-hint sink. Installed once by
    /// [`Table::install_heal_hints`](super::Table::install_heal_hints) after the
    /// table joins a tree. When present, a block read that is ECC-corrected and
    /// confirmed persistent (via a cache-bypassing re-read) records this SST's
    /// id so the compaction picker can rewrite it clean. Absent before the table
    /// is tree-owned: corrections are still returned to the caller, they are
    /// just not scheduled for healing.
    // alloc-friendly `OnceBox` (not `std::sync::OnceLock`) so the field does not
    // pin `Inner` to std, matching `deletion_pause`. The hint set itself is
    // `no_std` + alloc (see `crate::heal_hints`).
    pub(crate) heal_hints: once_cell::race::OnceBox<Arc<crate::heal_hints::HealHints>>,
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

        if self.is_deleted.load(core::sync::atomic::Ordering::Acquire) {
            log::trace!("Cleanup deleted table {global_id:?} at {:?}", self.path);

            // Move the accessor and block index out so all file handles
            // (including clones held by the block index) are closed before
            // attempting deletion. On Windows, remove_file fails while any
            // handle is open.
            let file_accessor = core::mem::replace(&mut self.file_accessor, FileAccessor::Closed);
            let block_index =
                core::mem::replace(&mut self.block_index, Arc::new(BlockIndexImpl::Closed));

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
            // Short-circuit on the common no-checkpoint path: skip
            // the Arc<dyn Fs> bump and PathBuf clone unless a pause is
            // both installed AND currently active. `try_enqueue` still
            // re-checks `is_active()` under the queue lock to close
            // the publish-then-release race, so the outer check is a
            // pure perf gate, not a correctness one.
            if let Some(pause) = self.deletion_pause.get()
                && pause.is_active()
                && pause.try_enqueue(Arc::clone(&self.fs), (*self.path).clone())
            {
                log::trace!(
                    "Deferred deletion of table {global_id:?} at {:?} (checkpoint active)",
                    self.path,
                );
                return;
            }

            // Off-foreground reclaim: return the SST's blocks to the filesystem
            // synchronously (so a footprint scan reflects the reclaim at once)
            // and hand the directory-entry unlink to the background deleter.
            // Falls through to a synchronous remove_file when no deleter is
            // installed (e.g. orphan cleanup before a tree owns the table).
            #[cfg(feature = "std")]
            if let Some(deleter) = self.background_deleter.get() {
                // Truncate (instant block-free) only when we own the sole hard
                // link. A completed checkpoint may have hard-linked this SST;
                // truncating the shared inode would zero the checkpoint's copy
                // too. When the link is shared (or the count is unknown), skip
                // the truncate and just unlink our directory entry — the data
                // survives via the other link and its blocks free once the last
                // link is gone. (An in-progress checkpoint is already handled by
                // the deletion-pause branch above.)
                if self.fs.hard_link_count(&self.path).is_ok_and(|n| n <= 1)
                    && let Err(e) = self.fs.truncate_file(&self.path)
                {
                    log::warn!(
                        "Failed to truncate deleted table {global_id:?} at {:?}: {e:?}",
                        self.path,
                    );
                }
                deleter.enqueue(Arc::clone(&self.fs), (*self.path).clone());
                return;
            }

            if let Err(e) = self.fs.remove_file(&self.path) {
                log::warn!(
                    "Failed to cleanup deleted table {global_id:?} at {:?}: {e:?}",
                    self.path,
                );
            }
        } else {
            // Not deleted, but possibly marked for tight-space prefix reclaim:
            // this (old, unrestricted) view's last Arc is dropping, so no read
            // can touch the prefix anymore. Punch `[0, offset)` and LEAVE the
            // file — the restricted view (a distinct Inner) still serves the
            // suffix. `punch_hole` opens the path itself, so it is fine that
            // this view's own handles drop right after this body.
            let off = self
                .punch_on_drop
                .load(core::sync::atomic::Ordering::Acquire);
            if off != u64::MAX
                && let Err(e) = self.fs.punch_hole(&self.path, 0, off)
            {
                log::warn!(
                    "Failed to punch tight-space prefix [0, {off}) of table {global_id:?} at {:?}: {e:?}",
                    self.path,
                );
            }
        }
    }
}
