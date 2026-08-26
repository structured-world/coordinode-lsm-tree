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
    /// [`Fs::punch_hole`] and LEAVES the file in
    /// place (the restricted view still serves the suffix). Mirrors
    /// `table::Inner::punch_on_drop`. Distinct from [`Self::is_deleted`].
    #[cfg_attr(
        not(feature = "std"),
        allow(
            dead_code,
            reason = "tight-space punch-on-drop frontier; the punch consumer is std-gated, so unread under no_std"
        )
    )]
    pub(crate) punch_on_drop: portable_atomic::AtomicU64,

    pub checksum: Checksum,

    /// First LIVE byte of this view, or `0` for a whole file. Set on the
    /// RESTRICTED view a tight-space relocation installs: everything below it
    /// was relocated into a fresh file and its frames are punched out (they
    /// read back as zeros). [`Self::checksum`] then covers only
    /// `[live_data_start, end)`, so integrity checks must hash from here —
    /// whole-file hashing would fold in the punched prefix and report a healthy
    /// file as corrupt. Persisted per version edit, the blob analogue of a
    /// table's restriction bound.
    pub(crate) live_data_start: u64,

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
    /// [`Fs::truncate_file`] and hands the
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
            //
            // Hole punching is a std-only capability (the tight-space relocation
            // loop that arms it is itself `#[cfg(feature = "std")]`), so the punch
            // action is gated. The atomic load is no-std-safe but pointless when
            // nothing can arm it.
            #[cfg(feature = "std")]
            {
                let off = self
                    .punch_on_drop
                    .load(core::sync::atomic::Ordering::Acquire);
                // Reclaim only what this tree exclusively owns. A checkpoint
                // hard-links blob files, and its captured SSTs still reference
                // values in the prefix being reclaimed — punching a shared
                // inode would zero live data inside an immutable snapshot. Same
                // guard the delete path applies before truncating; a link-count
                // probe that FAILS is treated as shared (fail closed), losing
                // only reclaimable space. An ACTIVE deletion pause additionally
                // defers the reclaim: the pause covers the checkpoint's whole
                // copy/link pass, so standing down removes the probe-then-punch
                // window in which the checkpoint could link this inode after
                // the probe read 1 — mirroring the table-prefix punch.
                //
                // The residual window (a checkpoint whose pause lands after
                // this check) is closed by lifetimes, not by a lock: the
                // checkpoint captures its version UNDER the held link window
                // and that version holds an Arc on every blob handle it links,
                // so a capture that still sees the pre-relocation view keeps
                // THIS Inner alive (this drop cannot run concurrently), and a
                // capture of the post-relocation view records the restricted
                // frontier, whose digest never covers the prefix punched here.
                // Blocking on the mutation gate instead is not an option in a
                // Drop impl: the checkpoint drops its captured version while
                // holding the gate's write half, and if that drop releases the
                // last Arc of an armed Inner, taking the read half here would
                // self-deadlock.
                //
                // Deferral does not DISCARD the reclaim: the intent lives in
                // this dropping view, so it is handed to the pause, which
                // re-probes the link count and punches once the checkpoint's
                // window closes.
                if off != u64::MAX {
                    let extent = match data_section_start(&*self.fs, &self.path) {
                        Ok(data_start) if off > data_start => Some((data_start, off - data_start)),
                        Ok(_) => None, // nothing consumed below the data start
                        Err(e) => {
                            log::warn!(
                                "Skipping tight-space punch of blob file {:?} at {}: could not read data section: {e:?}",
                                self.id,
                                self.path.display(),
                            );
                            None
                        }
                    };
                    if let Some((data_start, len)) = extent {
                        let deferred = self.deletion_pause.get().is_some_and(|pause| {
                            pause.is_active()
                                && pause.try_enqueue_punch(
                                    Arc::clone(&self.fs),
                                    self.path.clone(),
                                    alloc::vec![(data_start, len)],
                                )
                        });
                        // A shared inode (a COMPLETED checkpoint's surviving
                        // link), an unanswerable probe, or a failed punch does
                        // not DISCARD the reclaim: this dropping view holds its
                        // only record, so it is RETAINED for
                        // `retry_pending_reclaims` — mirroring the table-prefix
                        // punch. A bare retention, never a blocking re-probe:
                        // this is a Drop impl (see `retain_reclaim`).
                        if !deferred {
                            let exclusively_owned = match self.fs.hard_link_count(&self.path) {
                                Ok(n) => n <= 1,
                                Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {
                                    // The file is gone: its space is already back.
                                    return;
                                }
                                Err(e) => {
                                    log::debug!(
                                        "Retaining tight-space punch of blob file {:?} at {} for a retry: link-count probe failed: {e:?}",
                                        self.id,
                                        self.path.display(),
                                    );
                                    false
                                }
                            };
                            let punch_failed = exclusively_owned
                                && match self.fs.punch_hole(&self.path, data_start, len) {
                                    Ok(()) => false,
                                    Err(e) => {
                                        log::warn!(
                                            "Failed to punch tight-space data [{data_start}, {off}) of blob file {:?} at {}; retaining it for a retry: {e:?}",
                                            self.id,
                                            self.path.display(),
                                        );
                                        true
                                    }
                                };
                            if (!exclusively_owned || punch_failed)
                                && let Some(pause) = self.deletion_pause.get()
                            {
                                pause.retain_reclaim(
                                    Arc::clone(&self.fs),
                                    self.path.clone(),
                                    alloc::vec![(data_start, len)],
                                );
                            }
                        }
                    }
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

    /// Marks this view to punch the consumed `[data_start, offset)` data frames
    /// when its last `Arc` drops (see [`Inner::punch_on_drop`]). `offset` is an
    /// absolute data-section position. Set on the PRIOR view once a tight-space
    /// relocation slice has moved its `[data_start, offset)` live entries into a
    /// fresh compact file and that move is durably installed.
    #[cfg(feature = "std")]
    pub(crate) fn mark_punch_on_drop(&self, offset: u64) {
        self.0
            .punch_on_drop
            .store(offset, core::sync::atomic::Ordering::Release);
    }

    /// Re-opens this blob file as a DISTINCT [`Inner`] (its own file handle and
    /// a fresh punch-on-drop atomic) restricted to `[frontier, end)`: the
    /// tight-space relocation loop installs this view in the new version and
    /// arms the PRIOR view to punch everything below the frontier once its
    /// readers drain, so a stale blob file is reclaimed in place while the
    /// suffix keeps serving the not-yet-relocated entries — the blob analog of
    /// [`Table::reopen_restricted`](crate::Table::reopen_restricted).
    ///
    /// The digest is re-computed over that LIVE SUFFIX now, while the file is
    /// still whole — the punch is what makes a whole-file digest unusable, and
    /// reading the suffix fresh also folds in anything the relocation just
    /// wrote. The frontier rides on the view, so `diff` / the snapshot encoder
    /// persist it and integrity checks hash from there.
    ///
    /// # Errors
    ///
    /// Propagates any error from re-opening the file or hashing its suffix.
    #[cfg(feature = "std")]
    pub(crate) fn reopen_restricted(&self, frontier: u64) -> crate::Result<Self> {
        let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum_from(
            &*self.0.fs,
            &self.0.path,
            frontier,
        )?);
        super::recover_blob_file_from(
            &self.0.path,
            self.0.id,
            checksum,
            self.0.tree_id,
            &self.0.fs,
            frontier,
        )
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

    /// Binds this freshly created blob file to the tree's shared machinery.
    ///
    /// **Every path that makes a new blob file reachable must call this**, for
    /// the same reason its table counterpart exists
    /// ([`Table::bind_to_tree`](crate::Table::bind_to_tree)): a file that
    /// skips it looks healthy and fails silently later. Without the deletion
    /// pause its `Drop` can unlink the file while a checkpoint is capturing —
    /// before the checkpoint links it — and a tight-space prefix punch can
    /// zero bytes the checkpoint has already hard-linked.
    ///
    /// Idempotent per sink, so re-binding is harmless.
    pub(crate) fn bind_to_tree(&self, sinks: &crate::table::TableSinks<'_>) {
        self.install_deletion_pause(Arc::clone(sinks.deletion_pause));
        #[cfg(feature = "std")]
        if let Some(deleter) = sinks.background_deleter {
            self.install_background_deleter(Arc::clone(deleter));
        }
    }

    /// The installed deletion pause, so tests can assert that every path
    /// publishing a blob file binds it.
    #[cfg(test)]
    pub(crate) fn deletion_pause_for_test(&self) -> Option<Arc<DeletionPause>> {
        self.0.deletion_pause.get().cloned()
    }

    /// Returns the blob file ID.
    #[must_use]
    pub fn id(&self) -> BlobFileId {
        self.0.id
    }

    /// First LIVE byte of this view: `0` for a whole file, or the frontier a
    /// tight-space relocation left after reclaiming the consumed prefix. The
    /// recorded [`checksum`](Self::checksum) covers `[live_data_start, end)`,
    /// so integrity checks hash from here rather than over the punched prefix.
    #[must_use]
    pub fn live_data_start(&self) -> u64 {
        self.0.live_data_start
    }

    /// Returns the full blob file checksum.
    #[must_use]
    pub fn checksum(&self) -> Checksum {
        self.0.checksum
    }

    /// The compression applied to this blob file's values (the descriptor a
    /// reader uses to decode each record's on-disk bytes).
    #[must_use]
    pub(crate) fn compression(&self) -> crate::CompressionType {
        self.0.meta.compression
    }

    /// The file's decoded metadata block (counters, key range, compression).
    #[must_use]
    pub(crate) fn meta(&self) -> &Metadata {
        &self.0.meta
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
