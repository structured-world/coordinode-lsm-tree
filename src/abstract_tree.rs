// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::tree::inner::{FlushGuard, VersionsWriteGuard};
use crate::{
    AnyTree, BlobTree, Config, Guard, InternalValue, KvPair, Memtable, SeqNo, TableId, Tree,
    UserKey, UserValue, iter_guard::IterGuardImpl, table::Table, version::Version, vlog::BlobFile,
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};
use core::ops::RangeBounds;

pub type RangeItem = crate::Result<KvPair>;

type FlushToTablesResult = (Vec<Table>, Option<Vec<BlobFile>>);

/// Summary of a checkpoint produced by
/// [`AbstractTree::create_checkpoint`].
///
/// All byte counts are *logical* file sizes — hard links share the
/// underlying inode storage, so a checkpoint's marginal disk usage is
/// typically zero until the original files are compacted away.
#[derive(Debug, Clone, Copy)]
pub struct CheckpointInfo {
    /// Number of SST files captured.
    pub sst_files: usize,
    /// Number of blob (value-log) files captured. Always `0` for a
    /// standard [`Tree`].
    pub blob_files: usize,
    /// Sum of the logical file sizes of every captured SST + blob.
    pub total_bytes: u64,
    /// The version ID embedded in the checkpoint's `current` pointer.
    pub version_id: u64,
    /// Lower-bound visible-seqno watermark for the snapshot.
    ///
    /// Captured from the tree's `visible_seqno` generator BEFORE
    /// [`AbstractTree::current_version`]. Following the standard
    /// "lowest-excluded" watermark convention, `info.seqno = N` means
    /// every record with `seqno < N` was committed at sample time and
    /// is therefore guaranteed to be present in the snapshot. Records
    /// with `seqno == N` may or may not be included (writers can hold
    /// a record in the memtable for an instant before publishing the
    /// next watermark); records with `seqno > N` may also be present
    /// (writers can advance the counter between sample and version
    /// snapshot, and those keys still land in the captured memtable).
    ///
    /// PITR consumers MUST use `seqno < info.seqno` as the inclusion
    /// gate. Using `<=` (treating this as a max-included ceiling)
    /// could move a recovery cutoff past data still needed from WAL
    /// or replication; the field is a strict lower-exclusive watermark,
    /// not a max-included ceiling.
    pub seqno: SeqNo,
}

// Sealed on purpose: this trait is still public as a consumer-side bound
// (`&impl AbstractTree`), but external implementations are no longer part of
// the supported extension surface. Internal flush/version hooks keep evolving
// with crate-owned tree types and must not create downstream semver traps.
//
// `sealed` stays `pub` only so sibling modules in this crate can write
// `crate::abstract_tree::sealed::Sealed` in their impls. The parent module
// `abstract_tree` is not publicly exported from the crate root, so downstream
// crates still cannot name or implement this trait.
pub mod sealed {
    pub trait Sealed {}
}

/// Generic Tree API
#[enum_dispatch::enum_dispatch]
pub trait AbstractTree: sealed::Sealed {
    /// Debug method for tracing the MVCC history of a key.
    #[doc(hidden)]
    fn print_trace(&self, key: &[u8]) -> crate::Result<()>;

    /// Returns the number of cached table file descriptors.
    fn table_file_cache_size(&self) -> usize;

    // TODO: remove
    #[doc(hidden)]
    fn version_memtable_size_sum(&self) -> u64 {
        self.get_version_history_lock().memtable_size_sum()
    }

    #[doc(hidden)]
    fn next_table_id(&self) -> TableId;

    #[doc(hidden)]
    fn id(&self) -> crate::TreeId;

    /// Like [`AbstractTree::get`], but returns the actual internal entry, not just the user value.
    ///
    /// Used in tests.
    #[doc(hidden)]
    fn get_internal_entry(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>>;

    #[doc(hidden)]
    fn current_version(&self) -> Version;

    /// Returns a read-only snapshot of the tree's on-disk storage footprint:
    /// total used bytes, entry count, the average shape of a stored entry
    /// (average key / value bytes), and an estimate of how many more
    /// average-shaped entries fit in a byte budget (see
    /// [`StorageStats::estimated_remaining_entries`](crate::StorageStats::estimated_remaining_entries)).
    ///
    /// Computed from the live version's table + blob metadata plus one
    /// size-stat per live file; it never reads a data block. The default
    /// implementation reports [`StorageStatus::Healthy`](crate::StorageStatus::Healthy);
    /// the standard tree overrides it to report
    /// [`StorageStatus::CompactionInProgress`](crate::StorageStatus::CompactionInProgress) while a
    /// compaction runs.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// use lsm_tree::{AbstractTree, Config};
    ///
    /// let folder = tempfile::tempdir()?;
    /// let tree = Config::new(&folder, Default::default(), Default::default()).open()?;
    ///
    /// for i in 0..100u64 {
    ///     tree.insert(format!("key{i:04}"), "value", i);
    /// }
    /// tree.flush_active_memtable(0)?;
    ///
    /// let stats = tree.storage_stats()?;
    /// assert_eq!(stats.item_count, 100);
    /// // Roughly how many more average-shaped entries fit in another 1 MiB.
    /// let _headroom = stats.estimated_remaining_entries(1024 * 1024);
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if a live file's size cannot be stat-ed.
    fn storage_stats(&self) -> crate::Result<crate::StorageStats> {
        crate::storage_stats::compute_storage_stats(&self.current_version(), false, true)
    }

    /// Proactively verifies every block's XXH3 checksum across every SST in
    /// the tree's current version — a scrubber for catching bit rot before it
    /// surfaces as a user-visible read failure (cron / scrub jobs).
    ///
    /// Reports at block granularity and never aborts early. The returned
    /// [`BlockVerifyReport`](crate::verify::BlockVerifyReport) records
    /// block-corruption findings with `(file, offset)`, while file-level errors
    /// (e.g. [`BlockVerifyError::SstFileUnreadable`](crate::verify::BlockVerifyError::SstFileUnreadable))
    /// carry the file only (no offset). It does not surface per-entry indices or
    /// ECC-correction counts (when ECC-at-rest is enabled a within-budget corrupt
    /// block may still be healed on read as a side effect of the scan, but the
    /// report does not tally corrections).
    ///
    /// Filesystems with native per-block integrity (ZFS, Btrfs, `ReFS`, S3 —
    /// see [`Fs::capabilities`](crate::fs::Fs::capabilities)) already detect
    /// corruption on read; this scrub is the portable check for the rest.
    ///
    /// Use [`Self::verify_checksum_with`] for parallelism / throttle control.
    #[cfg(feature = "std")]
    fn verify_checksum(&self) -> crate::verify::BlockVerifyReport
    where
        Self: Sized,
    {
        crate::verify::verify_block_checksums(self)
    }

    /// Like [`Self::verify_checksum`] but with configurable parallelism and
    /// I/O throttle (see [`VerifyOptions`](crate::verify::VerifyOptions)).
    #[cfg(feature = "std")]
    fn verify_checksum_with(
        &self,
        options: &crate::verify::VerifyOptions,
    ) -> crate::verify::BlockVerifyReport
    where
        Self: Sized,
    {
        crate::verify::verify_block_checksums_with(self, options)
    }

    #[doc(hidden)]
    fn get_version_history_lock(&self) -> VersionsWriteGuard<'_>;

    /// Creates a hard-linked checkpoint of the tree's on-disk state in
    /// `target_path` for point-in-time recovery (PITR) backup.
    ///
    /// The checkpoint is a fully functional tree that can be opened
    /// independently via [`Config::open`](crate::Config::open). For the
    /// common single-filesystem case all SST files (and blob files, for
    /// [`BlobTree`]) are hard-linked rather than copied, so the operation
    /// is O(1) per file and consumes zero additional disk space until the
    /// original files are compacted away — at which point the inode is
    /// kept alive by the checkpoint link.
    ///
    /// # Cross-filesystem / cross-backend fall-back
    ///
    /// When a source file lives on a different filesystem than the
    /// checkpoint target — e.g. an SST routed to a hot tier via
    /// [`level_routes`](crate::Config::level_routes) on a separate volume,
    /// or a backup directory on a foreign mount — the hard link cannot
    /// be created (Unix `EXDEV`). In that case the checkpoint silently
    /// falls back to a streamed byte copy, which:
    ///
    /// - takes time linear in the file size instead of O(1), and
    /// - consumes disk space equal to the copied bytes on the target
    ///   volume (no inode sharing across filesystems).
    ///
    /// Each fall-back call emits one [`log::debug`] line (deliberately not
    /// `warn`: a misconfigured tier could trigger this path once per SST
    /// and per blob — thousands of times per snapshot — and per-file
    /// warnings would drown real signal). Operators wanting hard-visibility
    /// of unexpected full copies should enable debug logging on the `fs`
    /// module or watch the `CheckpointInfo.total_bytes` figure (≫ inode
    /// link cost means the fallback fired). The same `debug` policy applies
    /// when source and target use entirely different [`Fs`](crate::fs::Fs)
    /// backends (e.g. [`MemFs`](crate::fs::MemFs) → [`StdFs`](crate::fs::StdFs)
    /// in tests).
    ///
    /// # Concurrency
    ///
    /// While the checkpoint is being built, compaction continues normally
    /// but the physical removal of obsolete files is deferred until the
    /// checkpoint hard-links are in place. This is implemented by an
    /// internal reference-counted deletion gate; callers do not have to
    /// pause compaction themselves.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - the active memtable could not be flushed,
    /// - `target_path` already exists (to prevent accidental overwrites),
    /// - a hard link / copy fall-back could not be created, or
    /// - the manifest / version pointer files could not be replicated.
    ///
    /// On error any partial checkpoint files are removed automatically
    /// (best-effort) so callers can safely retry against the same path.
    // std-only: checkpoint creation hard-links / copies files via std::fs.
    #[cfg(feature = "std")]
    fn create_checkpoint(&self, target_path: &crate::path::Path) -> crate::Result<CheckpointInfo>;

    /// Seals the active memtable and flushes to table(s).
    ///
    /// If there are already other sealed memtables lined up, those will be flushed as well.
    ///
    /// Only used in tests.
    #[doc(hidden)]
    fn flush_active_memtable(&self, eviction_seqno: SeqNo) -> crate::Result<()> {
        let lock = self.get_flush_lock();
        self.rotate_memtable();
        self.flush(&lock, eviction_seqno)?;
        Ok(())
    }

    /// Synchronously flushes pending sealed memtables to tables.
    ///
    /// Returns the sum of flushed memtable sizes that were flushed.
    ///
    /// The function may not return a result, if nothing was flushed.
    ///
    /// # Errors
    ///
    /// Returns `Err` on an I/O error, or on a memtable residence-verification
    /// failure under [`KvChecksumComputePoint::AtInsert`](crate::runtime_config::KvChecksumComputePoint::AtInsert):
    /// a sealed memtable whose insert-time per-KV digests do not verify before
    /// flush surfaces [`crate::Error::MemtableKvChecksumMismatch`],
    /// [`crate::Error::MemtableKvChecksumCorruptAlgorithm`], or
    /// [`crate::Error::InvalidTag`] (corrupt `value_type`).
    fn flush(&self, _lock: &FlushGuard<'_>, seqno_threshold: SeqNo) -> crate::Result<Option<u64>> {
        use crate::{
            compaction::stream::CompactionStream, merge::Merger, range_tombstone::RangeTombstone,
        };

        let version_history = self.get_version_history_lock();
        let latest = version_history.latest_version();

        if latest.sealed_memtables.len() == 0 {
            return Ok(None);
        }

        let sealed_ids = latest
            .sealed_memtables
            .iter()
            .map(|mt| mt.id)
            .collect::<Vec<_>>();

        let flushed_size = latest.sealed_memtables.iter().map(|mt| mt.size()).sum();

        // AtInsert residence check: verify each sealed memtable's insert-time
        // per-KV digests against a recompute over the entries' current bytes
        // before writing them out. A divergence means an entry was corrupted
        // (a RAM bit-flip) while it sat in the memtable. Memtables with no
        // insert digests (the default) return immediately without walking.
        //
        // This is a SEPARATE pass over the as-inserted memtable entries, not
        // fused into the writer's per-KV footer encode, and deliberately so:
        // the footer digest is computed over POST-merge / post-seqno-filter
        // bytes (the CompactionStream below applies the merge operator), so a
        // merge operator's combined value differs from any single inserted
        // value. Comparing the carried insert digest against the footer digest
        // would false-positive on every legitimate merge. Residence corruption
        // is a property of what was inserted (pre-merge); it must be checked
        // here, against the raw memtable entries.
        for mt in latest.sealed_memtables.iter() {
            mt.verify_kv_residence()?;
        }

        // Collect range tombstones from sealed memtables
        let mut range_tombstones: Vec<RangeTombstone> = Vec::new();
        for mt in latest.sealed_memtables.iter() {
            range_tombstones.extend(mt.range_tombstones_sorted());
        }
        range_tombstones
            .sort_by(|a, b| a.cmp_with_comparator(b, self.tree_config().comparator.as_ref()));
        range_tombstones.dedup();

        let merger = Merger::new(
            latest
                .sealed_memtables
                .iter()
                .map(|mt| mt.iter().map(Ok))
                .collect::<Vec<_>>(),
            self.tree_config().comparator.clone(),
        );
        // RT suppression is not needed here: flush writes both entries and RTs
        // to the output tables. Suppression happens at read time, not write time.
        let stream = CompactionStream::new(merger, seqno_threshold)
            .with_merge_operator(self.tree_config().merge_operator.clone());

        drop(version_history);

        // Clone needed: flush_to_tables_with_rt consumes the Vec, but on the
        // RT-only path (no KV data, tables.is_empty()) we re-insert RTs into the
        // active memtable. Flush is infrequent and RT count is small.
        if let Some((tables, blob_files)) =
            self.flush_to_tables_with_rt(stream, range_tombstones.clone())?
        {
            // If no tables were produced (RT-only memtable), re-insert RTs
            // into active memtable so they aren't lost
            if tables.is_empty() && !range_tombstones.is_empty() {
                let active = self.active_memtable();
                for rt in &range_tombstones {
                    let _ =
                        active.insert_range_tombstone(rt.start.clone(), rt.end.clone(), rt.seqno);
                }
            }

            self.register_tables(
                &tables,
                blob_files.as_deref(),
                None,
                &sealed_ids,
                seqno_threshold,
            )?;
        }

        Ok(Some(flushed_size))
    }

    /// Returns an iterator that scans through the entire tree.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    fn iter(
        &self,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        self.range::<&[u8], _>(.., seqno, index)
    }

    /// Returns an iterator over a prefixed set of items.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static>;

    /// Returns an iterator over a range of items.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static>;

    /// Returns the approximate number of tombstones in the tree.
    fn tombstone_count(&self) -> u64;

    /// Returns the approximate number of weak tombstones (single deletes) in the tree.
    fn weak_tombstone_count(&self) -> u64;

    /// Returns the approximate number of values reclaimable once weak tombstones can be GC'd.
    fn weak_tombstone_reclaimable_count(&self) -> u64;

    /// Drops tables that are fully contained in a given range.
    ///
    /// Accepts any `RangeBounds`, including unbounded or exclusive endpoints.
    /// If the normalized lower bound is greater than the upper bound, the
    /// method returns without performing any work.
    ///
    /// # Errors
    ///
    /// Will return `Err` only if an IO error occurs.
    fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> crate::Result<()>;

    /// Drops all tables and clears all memtables atomically.
    ///
    /// # Errors
    ///
    /// Will return `Err` only if an IO error occurs.
    fn clear(&self) -> crate::Result<()>;

    /// Performs major compaction, blocking the caller until it's done.
    ///
    /// Returns a [`crate::compaction::CompactionResult`] describing what action was taken.
    ///
    /// # Garbage-collection / merge-fold watermark (`seqno_threshold`)
    ///
    /// `seqno_threshold` is the MVCC garbage-collection watermark: the engine may
    /// collapse history that no snapshot reading at a seqno `< seqno_threshold`
    /// can still observe. Concretely, only entries whose seqno is `< seqno_threshold`
    /// are eligible for:
    ///
    /// - dropping shadowed versions / GC-ing tombstones, and
    /// - **folding merge operands** via the [`crate::MergeOperator`]: a key written
    ///   only through [`Self::merge`] (no base value) accumulates one operand per
    ///   call, and reads re-apply the whole chain (`O(operands)` per read) until
    ///   compaction folds it. Folding a chain into a single value is only
    ///   MVCC-safe when no live snapshot reads *between* the operands, which is
    ///   exactly what `seqno_threshold` certifies.
    ///
    /// The engine does **not** track snapshots (unlike a `RocksDB`-style
    /// snapshot list); the caller owns snapshot lifecycle and must supply this
    /// watermark:
    ///
    /// - To fold/GC everything (no active snapshots), pass a value **above every
    ///   live seqno** (e.g. the next value from the [`crate::SequenceNumberCounter`]).
    /// - With outstanding snapshots, pass the **oldest** snapshot's seqno so their
    ///   reads stay correct.
    /// - `seqno_threshold == 0` certifies nothing as collapsible, so **no folding
    ///   or GC happens** — `major_compact(target, 0)` only restructures tables and
    ///   leaves a merge-only key's full operand chain intact.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn major_compact(
        &self,
        target_size: u64,
        seqno_threshold: SeqNo,
    ) -> crate::Result<crate::compaction::CompactionResult>;

    /// Returns the disk space used by stale blobs.
    fn stale_blob_bytes(&self) -> u64 {
        0
    }

    /// Gets the space usage of all filters in the tree.
    ///
    /// May not correspond to the actual memory size because filter blocks may be paged out.
    fn filter_size(&self) -> u64;

    /// Gets the memory usage of all pinned filters in the tree.
    fn pinned_filter_size(&self) -> usize;

    /// Gets the memory usage of all pinned index blocks in the tree.
    fn pinned_block_index_size(&self) -> usize;

    /// Gets the length of the version free list.
    fn version_free_list_len(&self) -> usize;

    /// Returns the metrics structure.
    #[cfg(feature = "metrics")]
    fn metrics(&self) -> &Arc<crate::Metrics>;

    /// Acquires the flush lock which is required to call [`Tree::flush`].
    fn get_flush_lock(&self) -> FlushGuard<'_>;

    /// Synchronously flushes a memtable to a table.
    ///
    /// This method will not make the table immediately available,
    /// use [`AbstractTree::register_tables`] for that.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn flush_to_tables(
        &self,
        stream: impl Iterator<Item = crate::Result<InternalValue>>,
    ) -> crate::Result<Option<FlushToTablesResult>> {
        self.flush_to_tables_with_rt(stream, Vec::new())
    }

    /// Like [`AbstractTree::flush_to_tables`], but also writes range tombstones.
    ///
    /// This is an internal extension hook on the crate's sealed tree types and
    /// is hidden from generated documentation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    fn flush_to_tables_with_rt(
        &self,
        stream: impl Iterator<Item = crate::Result<InternalValue>>,
        range_tombstones: Vec<crate::range_tombstone::RangeTombstone>,
    ) -> crate::Result<Option<FlushToTablesResult>>;

    /// Atomically registers flushed tables into the tree, removing their associated sealed memtables.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn register_tables(
        &self,
        tables: &[Table],
        blob_files: Option<&[BlobFile]>,
        frag_map: Option<crate::blob_tree::FragmentationMap>,
        sealed_memtables_to_delete: &[crate::tree::inner::MemtableId],
        gc_watermark: SeqNo,
    ) -> crate::Result<()>;

    /// Clears the active memtable atomically.
    fn clear_active_memtable(&self);

    /// Returns the number of sealed memtables.
    fn sealed_memtable_count(&self) -> usize;

    /// Performs compaction on the tree's levels, blocking the caller until it's done.
    ///
    /// Returns a [`crate::compaction::CompactionResult`] describing what action was taken.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn compact(
        &self,
        strategy: Arc<dyn crate::compaction::CompactionStrategy>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<crate::compaction::CompactionResult>;

    /// Returns the next table's ID.
    fn get_next_table_id(&self) -> TableId;

    /// Returns the tree config.
    fn tree_config(&self) -> &Config;

    /// Returns the highest sequence number.
    fn get_highest_seqno(&self) -> Option<SeqNo> {
        let memtable_seqno = self.get_highest_memtable_seqno();
        let table_seqno = self.get_highest_persisted_seqno();
        memtable_seqno.max(table_seqno)
    }

    /// Returns the active memtable.
    fn active_memtable(&self) -> Arc<Memtable>;

    /// Returns the tree type.
    fn tree_type(&self) -> crate::TreeType {
        if self.tree_config().kv_separation_opts.is_some() {
            crate::TreeType::Blob
        } else {
            crate::TreeType::Standard
        }
    }

    /// Seals the active memtable.
    fn rotate_memtable(&self) -> Option<Arc<Memtable>>;

    /// Returns the number of tables currently in the tree.
    fn table_count(&self) -> usize;

    /// Returns the number of tables in `levels[idx]`.
    ///
    /// Returns `None` if the level does not exist (if idx >= 7).
    fn level_table_count(&self, idx: usize) -> Option<usize>;

    /// Returns the number of disjoint runs in L0.
    ///
    /// Can be used to determine whether to write stall.
    fn l0_run_count(&self) -> usize;

    /// Returns the number of blob files currently in the tree.
    fn blob_file_count(&self) -> usize;

    /// Approximates the number of items in the tree.
    fn approximate_len(&self) -> usize;

    /// Returns the disk space usage.
    fn disk_space(&self) -> u64;

    /// Returns the highest sequence number of the active memtable.
    fn get_highest_memtable_seqno(&self) -> Option<SeqNo>;

    /// Returns the highest sequence number that is flushed to disk.
    fn get_highest_persisted_seqno(&self) -> Option<SeqNo>;

    /// Scans the entire tree, returning the number of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire tree: O(n) complexity!
    ///
    /// Never, under any circumstances, use .`len()` == 0 to check
    /// if the tree is empty, use [`Tree::is_empty`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let folder = tempfile::tempdir()?;
    /// let tree = Config::new(&folder, Default::default(), Default::default()).open()?;
    ///
    /// assert_eq!(tree.len(0, None)?, 0);
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    /// assert_eq!(tree.len(3, None)?, 3);
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn len(&self, seqno: SeqNo, index: Option<(Arc<Memtable>, SeqNo)>) -> crate::Result<usize> {
        let mut count = 0;

        for item in self.iter(seqno, index) {
            let _ = item.key()?;
            count += 1;
        }

        Ok(count)
    }

    /// Returns `true` if the tree is empty.
    ///
    /// This operation has O(log N) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// assert!(tree.is_empty(0, None)?);
    ///
    /// tree.insert("a", "abc", 0);
    /// assert!(!tree.is_empty(1, None)?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn is_empty(&self, seqno: SeqNo, index: Option<(Arc<Memtable>, SeqNo)>) -> crate::Result<bool> {
        Ok(self
            .first_key_value(seqno, index)
            .map(crate::Guard::key)
            .transpose()?
            .is_none())
    }

    /// Returns the first key-value pair in the tree.
    /// The key in this pair is the minimum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{AbstractTree, Config, Tree, Guard};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    ///
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    ///
    /// let key = tree.first_key_value(3, None).expect("item should exist").key()?;
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn first_key_value(
        &self,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Option<IterGuardImpl> {
        self.iter(seqno, index).next()
    }

    /// Returns the last key-value pair in the tree.
    /// The key in this pair is the maximum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{AbstractTree, Config, Tree, Guard};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// #
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    ///
    /// let key = tree.last_key_value(3, None).expect("item should exist").key()?;
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn last_key_value(
        &self,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Option<IterGuardImpl> {
        self.iter(seqno, index).next_back()
    }

    /// Returns the size of a value if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// tree.insert("a", "my_value", 0);
    ///
    /// let size = tree.size_of("a", 1)?.unwrap_or_default();
    /// assert_eq!("my_value".len() as u32, size);
    ///
    /// let size = tree.size_of("b", 1)?.unwrap_or_default();
    /// assert_eq!(0, size);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn size_of<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<u32>>;

    /// Retrieves an item from the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// tree.insert("a", "my_value", 0);
    ///
    /// let item = tree.get("a", 1)?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn get<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<UserValue>>;

    /// Retrieves an item from the tree as a [`PinnableSlice`](crate::PinnableSlice).
    ///
    /// When the value is backed by an on-disk data block, implementations
    /// may return [`PinnableSlice::Pinned`](crate::PinnableSlice::Pinned) holding a reference to that block's
    /// decompressed buffer (avoiding a data copy). Memtable and blob-resolved
    /// values use [`PinnableSlice::Owned`](crate::PinnableSlice::Owned). The default implementation always
    /// returns `Owned`; only [`Tree`] overrides with the pinned path.
    ///
    /// The existing [`AbstractTree::get`] method is unaffected.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(&folder, Default::default(), Default::default()).open()?;
    /// tree.insert("a", "my_value", 0);
    ///
    /// let item = tree.get_pinned("a", 1)?;
    /// assert_eq!(item.as_ref().map(|v| v.as_ref()), Some("my_value".as_bytes()));
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn get_pinned<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: SeqNo,
    ) -> crate::Result<Option<crate::PinnableSlice>> {
        // Default: delegate to get() and wrap as Owned
        self.get(key, seqno)
            .map(|opt| opt.map(crate::PinnableSlice::owned))
    }

    /// Returns `true` if the tree contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
    /// #
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// assert!(!tree.contains_key("a", 0)?);
    ///
    /// tree.insert("a", "abc", 0);
    /// assert!(tree.contains_key("a", 1)?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn contains_key<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<bool> {
        self.get(key, seqno).map(|x| x.is_some())
    }

    /// Returns `true` if the tree contains any key with the given prefix.
    ///
    /// This is a convenience method that checks whether the corresponding
    /// prefix iterator yields at least one item, while surfacing any IO
    /// errors via the `Result` return type. Implementations may override
    /// this method to provide a more efficient prefix-existence check.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// assert!(!tree.contains_prefix("abc", 0, None)?);
    ///
    /// tree.insert("abc:1", "value", 0);
    /// assert!(tree.contains_prefix("abc", 1, None)?);
    /// assert!(!tree.contains_prefix("xyz", 1, None)?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn contains_prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> crate::Result<bool> {
        match self.prefix(prefix, seqno, index).next() {
            Some(guard) => guard.key().map(|_| true),
            None => Ok(false),
        }
    }

    /// Reads multiple keys from the tree.
    ///
    /// Implementations may choose to perform all lookups against a single
    /// version snapshot and acquire the version lock only once, which can be
    /// more efficient than calling [`AbstractTree::get`] in a loop. The
    /// default trait implementation, however, is a convenience wrapper that
    /// simply calls [`AbstractTree::get`] for each key and therefore does not
    /// guarantee a single-snapshot or single-lock acquisition. Optimized
    /// implementations (such as [`Tree`] and [`BlobTree`]) provide the
    /// single-snapshot/one-lock behavior.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// tree.insert("a", "value_a", 0);
    /// tree.insert("b", "value_b", 1);
    ///
    /// let results = tree.multi_get(["a", "b", "c"], 2)?;
    /// assert_eq!(results[0], Some("value_a".as_bytes().into()));
    /// assert_eq!(results[1], Some("value_b".as_bytes().into()));
    /// assert_eq!(results[2], None);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn multi_get<K: AsRef<[u8]>>(
        &self,
        keys: impl IntoIterator<Item = K>,
        seqno: SeqNo,
    ) -> crate::Result<Vec<Option<UserValue>>> {
        keys.into_iter().map(|key| self.get(key, seqno)).collect()
    }

    /// Applies a [`WriteBatch`](crate::WriteBatch) with the given sequence number.
    ///
    /// All entries share a single seqno. This is more efficient than individual
    /// writes because the version-history lock and memtable size accounting
    /// are performed only once for the entire batch.
    ///
    /// **Visibility:** entries become individually visible to concurrent readers
    /// as they are inserted. For atomic batch visibility, the caller must
    /// publish `seqno` (via `visible_seqno.fetch_max(seqno + 1)`) only
    /// **after** this method returns.
    ///
    /// Returns the total bytes added and new size of the memtable.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree, WriteBatch};
    ///
    /// let tree = Config::new(&folder, Default::default(), Default::default()).open()?;
    ///
    /// let mut batch = WriteBatch::new();
    /// batch.insert("key1", "value1");
    /// batch.insert("key2", "value2");
    /// batch.remove("key3");
    ///
    /// let (bytes_added, memtable_size) = tree.apply_batch(batch, 0)?;
    /// assert!(bytes_added > 0);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::MixedOperationBatch`](crate::Error::MixedOperationBatch)
    /// if the batch contains mixed operation types for the same user key.
    fn apply_batch(&self, batch: crate::WriteBatch, seqno: SeqNo) -> crate::Result<(u64, u64)>;

    /// Inserts a key-value pair into the tree.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// Returns the added item's size and new size of the memtable.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// tree.insert("a", "abc", 0);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
        seqno: SeqNo,
    ) -> (u64, u64);

    /// Removes an item from the tree.
    ///
    /// Returns the added item's size and new size of the memtable.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
    /// #
    /// # let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// tree.insert("a", "abc", 0);
    ///
    /// let item = tree.get("a", 1)?.expect("should have item");
    /// assert_eq!("abc".as_bytes(), &*item);
    ///
    /// tree.remove("a", 1);
    ///
    /// let item = tree.get("a", 2)?;
    /// assert_eq!(None, item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn remove<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64);

    /// Writes a merge operand for a key.
    ///
    /// The operand is stored as a partial update that will be combined with
    /// other operands and/or a base value via the configured [`crate::MergeOperator`]
    /// during reads and compaction.
    ///
    /// Returns the added item's size and new size of the memtable.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{AbstractTree, Config, MergeOperator, UserValue};
    /// # use std::sync::Arc;
    /// # struct SumMerge;
    /// # impl MergeOperator for SumMerge {
    /// #     fn merge(&self, _key: &[u8], base: Option<&[u8]>, operands: &[&[u8]]) -> lsm_tree::Result<UserValue> {
    /// #         let mut sum: i64 = base.map_or(0, |b| i64::from_le_bytes(b.try_into().unwrap()));
    /// #         for op in operands { sum += i64::from_le_bytes((*op).try_into().unwrap()); }
    /// #         Ok(sum.to_le_bytes().to_vec().into())
    /// #     }
    /// # }
    /// # let tree = Config::new(folder, Default::default(), Default::default())
    /// #     .with_merge_operator(Some(Arc::new(SumMerge)))
    /// #     .open()?;
    /// tree.merge("counter", 1_i64.to_le_bytes(), 0);
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    fn merge<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        operand: V,
        seqno: SeqNo,
    ) -> (u64, u64);

    /// Removes an item from the tree.
    ///
    /// The tombstone marker of this delete operation will vanish when it
    /// collides with its corresponding insertion.
    /// This may cause older versions of the value to be resurrected, so it should
    /// only be used and preferred in scenarios where a key is only ever written once.
    ///
    /// Returns the added item's size and new size of the memtable.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
    /// #
    /// # let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// tree.insert("a", "abc", 0);
    ///
    /// let item = tree.get("a", 1)?.expect("should have item");
    /// assert_eq!("abc".as_bytes(), &*item);
    ///
    /// tree.remove_weak("a", 1);
    ///
    /// let item = tree.get("a", 2)?;
    /// assert_eq!(None, item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    fn remove_weak<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64);

    /// Deletes all keys in the range `[start, end)` by inserting a range tombstone.
    ///
    /// This is much more efficient than deleting keys individually when
    /// removing a contiguous range of keys.
    ///
    /// Returns the approximate size added to the memtable.
    /// Returns 0 if `start >= end` (invalid interval is silently ignored).
    ///
    /// This is a required method on the crate's sealed tree types.
    fn remove_range<K: Into<UserKey>>(&self, start: K, end: K, seqno: SeqNo) -> u64;

    /// Deletes all keys with the given prefix by inserting a range tombstone.
    ///
    /// This is sugar over [`AbstractTree::remove_range`] using prefix bounds.
    ///
    /// Returns the approximate size added to the memtable.
    /// Returns 0 for empty prefixes or all-`0xFF` prefixes (cannot form valid half-open range).
    fn remove_prefix<K: AsRef<[u8]>>(&self, prefix: K, seqno: SeqNo) -> u64 {
        use crate::range::prefix_to_range;
        use core::ops::Bound;

        let (lo, hi) = prefix_to_range(prefix.as_ref());

        let Bound::Included(start) = lo else { return 0 };

        // Bound::Unbounded means the prefix is all 0xFF — no representable
        // exclusive upper bound exists, so we cannot form a valid range tombstone.
        let Bound::Excluded(end) = hi else { return 0 };

        self.remove_range(start, end, seqno)
    }
}
