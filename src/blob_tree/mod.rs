// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

mod gc;
pub mod handle;
pub mod ingest;

#[doc(hidden)]
pub use gc::{FragmentationEntry, FragmentationMap};

use crate::path::{Path, PathBuf};
use crate::tree::inner::{FlushGuard, VersionsWriteGuard};
use crate::{
    Cache, Config, Memtable, ScanSinceEvent, SeqNo, TableId, TreeId, UserKey, UserValue,
    abstract_tree::{AbstractTree, RangeItem},
    coding::Decode,
    iter_guard::{IterGuard, IterGuardImpl},
    table::Table,
    tree::inner::MemtableId,
    value::InternalValue,
    version::Version,
    vlog::{Accessor, BlobFile, BlobFileWriter},
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::ToString, vec::Vec};
use core::ops::RangeBounds;
use handle::BlobIndirection;

/// Iterator value guard
pub struct Guard {
    tree: crate::BlobTree,
    version: Version,
    kv: crate::Result<InternalValue>,
}

impl IterGuard for Guard {
    fn into_inner_if(
        self,
        pred: impl Fn(&UserKey) -> bool,
    ) -> crate::Result<(UserKey, Option<UserValue>)> {
        let kv = self.kv?;

        if pred(&kv.key.user_key) {
            resolve_value_handle(
                self.tree.id(),
                self.tree.blobs_folder.as_path(),
                &self.tree.index.config.cache,
                &self.version,
                kv,
                #[cfg(zstd_any)]
                self.tree
                    .index
                    .config
                    .kv_separation_opts
                    .as_ref()
                    .and_then(|o| o.zstd_dictionary.as_deref()),
            )
            .map(|(k, v)| (k, Some(v)))
        } else {
            Ok((kv.key.user_key, None))
        }
    }

    fn key(self) -> crate::Result<UserKey> {
        self.kv.map(|kv| kv.key.user_key)
    }

    fn size(self) -> crate::Result<u32> {
        let kv = self.kv?;

        if kv.key.value_type.is_indirection() {
            let mut cursor = crate::io::Cursor::new(kv.value);
            Ok(BlobIndirection::decode_from(&mut cursor)?.size)
        } else {
            #[expect(clippy::cast_possible_truncation, reason = "values are u32 max length")]
            Ok(kv.value.len() as u32)
        }
    }

    fn into_inner(self) -> crate::Result<(UserKey, UserValue)> {
        resolve_value_handle(
            self.tree.id(),
            self.tree.blobs_folder.as_path(),
            &self.tree.index.config.cache,
            &self.version,
            self.kv?,
            #[cfg(zstd_any)]
            self.tree
                .index
                .config
                .kv_separation_opts
                .as_ref()
                .and_then(|o| o.zstd_dictionary.as_deref()),
        )
    }
}

fn resolve_value_handle(
    tree_id: TreeId,
    blobs_folder: &Path,
    cache: &Cache,
    version: &Version,
    item: InternalValue,
    #[cfg(zstd_any)] zstd_dictionary: Option<&crate::compression::ZstdDictionary>,
) -> RangeItem {
    if item.key.value_type.is_indirection() {
        let mut cursor = crate::io::Cursor::new(item.value);
        let vptr = BlobIndirection::decode_from(&mut cursor)?;

        // Resolve indirection using value log
        let accessor = {
            let a = Accessor::new(&version.blob_files);
            #[cfg(zstd_any)]
            let a = a.with_dict(zstd_dictionary);
            a
        };

        match accessor.get(
            tree_id,
            blobs_folder,
            &item.key.user_key,
            &vptr.vhandle,
            cache,
        ) {
            Ok(Some(v)) => {
                let k = item.key.user_key;
                Ok((k, v))
            }
            Ok(None) => {
                panic!(
                    "value handle ({:?} => {:?}) did not match any blob - this is a bug; version={}",
                    item.key.user_key,
                    vptr.vhandle,
                    version.id(),
                );
            }
            Err(e) => Err(e),
        }
    } else {
        let k = item.key.user_key;
        let v = item.value;
        Ok((k, v))
    }
}

/// A key-value-separated log-structured merge tree
///
/// This tree is a composite structure, consisting of an
/// index tree (LSM-tree) and a log-structured value log
/// to reduce write amplification.
#[derive(Clone)]
pub struct BlobTree {
    /// Index tree that holds value handles or small inline values
    #[doc(hidden)]
    pub index: crate::Tree,

    blobs_folder: Arc<PathBuf>,
}

impl BlobTree {
    /// Physical footprint of the stale blob files a full compaction would
    /// relocate: the linked, stale, non-dead subset across every live table (the
    /// SAME set the merge gate budgets via `pick_blob_files_to_rewrite`), summed
    /// by on-disk size. Zero when KV separation is unconfigured.
    fn full_compaction_blob_need(&self, version: &crate::version::Version) -> crate::Result<u64> {
        let Some(blob_opts) = &self.index.config.kv_separation_opts else {
            return Ok(0);
        };
        let all_tables: crate::HashSet<TableId> = version.iter_tables().map(Table::id).collect();
        crate::compaction::worker::pick_blob_files_to_rewrite(&all_tables, version, blob_opts)?
            .iter()
            .try_fold(0u64, |acc, bf| bf.physical_size().map(|size| acc + size))
    }

    pub(crate) fn open(config: Config) -> crate::Result<Self> {
        use crate::file::{BLOBS_FOLDER, fsync_directory};

        let index = crate::Tree::open(config)?;

        let blobs_folder = index.config.path.join(BLOBS_FOLDER);
        (*index.config.fs).create_dir_all(&blobs_folder)?;
        fsync_directory(&blobs_folder, &*index.config.fs, index.config.sync_mode)?;

        let blob_file_id_to_continue_with = index
            .current_version()
            .blob_files
            .list_ids()
            .max()
            .map(|x| x + 1)
            .unwrap_or_default();

        index
            .0
            .blob_file_id_counter
            .set(blob_file_id_to_continue_with);

        Ok(Self {
            index,
            blobs_folder: Arc::new(blobs_folder),
        })
    }

    /// Resolves a single key against a pre-acquired [`SuperVersion`](crate::version::SuperVersion).
    fn resolve_key(
        &self,
        super_version: &crate::version::SuperVersion,
        key: &[u8],
        seqno: SeqNo,
    ) -> crate::Result<Option<UserValue>> {
        let Some(item) = crate::Tree::get_internal_entry_from_version(
            super_version,
            key,
            seqno,
            self.index.config.comparator.as_ref(),
        )?
        else {
            return Ok(None);
        };

        let (_, v) = resolve_value_handle(
            self.id(),
            self.blobs_folder.as_path(),
            &self.index.config.cache,
            &super_version.version,
            item,
            #[cfg(zstd_any)]
            self.index
                .config
                .kv_separation_opts
                .as_ref()
                .and_then(|o| o.zstd_dictionary.as_deref()),
        )?;

        Ok(Some(v))
    }

    /// Iterate change events with `seqno >= target_seqno`, resolving
    /// KV-separated values.
    ///
    /// Same change-data-capture contract as [`Tree::scan_since_seqno`](crate::Tree::scan_since_seqno),
    /// but a blob-indirected value is resolved from its blob file into a
    /// [`ScanSinceEvent::Insert`] carrying the real value, so a downstream
    /// consumer can replicate without access to the source's blob files.
    /// Block-skip, seqno ordering, and tombstone handling are identical to the
    /// standard-tree path (the same shared aggregation backs both).
    ///
    /// # Panics
    ///
    /// Panics if the internal version-history lock is poisoned.
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading the index, a data block, or a referenced blob
    /// fails.
    pub fn scan_since_seqno(
        &self,
        target_seqno: SeqNo,
    ) -> crate::Result<impl Iterator<Item = ScanSinceEvent> + use<>> {
        self.index
            .scan_since_seqno_with(target_seqno, true, |version, entry| {
                let seqno = entry.key.seqno;
                let (key, value) = resolve_value_handle(
                    self.id(),
                    self.blobs_folder.as_path(),
                    &self.index.config.cache,
                    version,
                    entry,
                    #[cfg(zstd_any)]
                    self.index
                        .config
                        .kv_separation_opts
                        .as_ref()
                        .and_then(|o| o.zstd_dictionary.as_deref()),
                )?;
                Ok(ScanSinceEvent::Insert { key, value, seqno })
            })
    }
}

impl crate::abstract_tree::sealed::Sealed for BlobTree {}

/// Maps a raw merge-pipeline item into a KV-separated iterator guard that
/// resolves the blob handle lazily against `version`.
fn blob_guard(
    tree: &crate::BlobTree,
    version: &Version,
    item: crate::Result<InternalValue>,
) -> IterGuardImpl {
    IterGuardImpl::Blob(Guard {
        tree: tree.clone(),
        version: version.clone(),
        kv: item,
    })
}

/// Wraps a [`SeekableTreeIter`](crate::range::SeekableTreeIter) over the index
/// tree so a KV-separated tree can expose it as a [`SeekableGuardIter`](crate::iter_guard::SeekableGuardIter).
struct BlobSeekable {
    inner: crate::range::SeekableTreeIter,
    tree: crate::BlobTree,
    version: Version,
}

impl Iterator for BlobSeekable {
    type Item = IterGuardImpl;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|item| blob_guard(&self.tree, &self.version, item))
    }
}

impl DoubleEndedIterator for BlobSeekable {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|item| blob_guard(&self.tree, &self.version, item))
    }
}

impl crate::iter_guard::SeekableGuardIter for BlobSeekable {
    fn seek_to(&mut self, key: &[u8]) {
        self.inner.seek_to(key);
    }

    fn seek_to_for_prev(&mut self, key: &[u8]) {
        self.inner.seek_to_for_prev(key);
    }

    fn peek_key(&mut self) -> Option<crate::Result<crate::UserKey>> {
        // The key lives in the index tree (blob separation only moves the value),
        // so the inner seekable's peek already yields the right user key.
        self.inner.peek_key()
    }
}

impl AbstractTree for BlobTree {
    #[cfg(feature = "std")]
    fn create_checkpoint(
        &self,
        target_path: &crate::path::Path,
    ) -> crate::Result<crate::CheckpointInfo> {
        crate::checkpoint::run_checkpoint(
            self,
            &crate::checkpoint::CheckpointParams {
                target_root: target_path,
                target_fs: &self.index.config.fs,
                src_root: &self.index.config.path,
                src_fs: &self.index.config.fs,
                deletion_pause: &self.index.deletion_pause,
                visible_seqno: &self.index.config.visible_seqno,
                include_blobs: true,
                runtime_config: self.index.0.runtime_config.load_full(),
                encryption: self.index.0.config.encryption.clone(),
            },
        )
    }

    fn print_trace(&self, key: &[u8]) -> crate::Result<()> {
        self.index.print_trace(key)
    }

    fn table_file_cache_size(&self) -> usize {
        self.index.table_file_cache_size()
    }

    fn get_version_history_lock(&self) -> VersionsWriteGuard<'_> {
        self.index.get_version_history_lock()
    }

    fn next_table_id(&self) -> TableId {
        self.index.next_table_id()
    }

    fn id(&self) -> crate::TreeId {
        self.index.id()
    }

    fn get_internal_entry(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>> {
        self.index.get_internal_entry(key, seqno)
    }

    fn current_version(&self) -> Version {
        self.index.current_version()
    }

    fn storage_stats(&self) -> crate::Result<crate::StorageStats> {
        // Forward the index tree's compaction state (the default impl would
        // always report idle), and mark value bytes as NOT user values: large
        // values are KV-separated into blob files, so the SST records only
        // indirection pointers. One version snapshot is reused for both the
        // footprint and the blob-headroom sum below: a second `current_version()`
        // could race a concurrent flush / compaction and mix two snapshots.
        let version = self.current_version();
        let mut stats = crate::storage_stats::compute_storage_stats(
            &version,
            self.index.is_compacting(),
            false,
        )?;
        // Capacity is disk-aware and driven by the index tree's runtime config;
        // its footprint basis (`compute_used_bytes`) already stats blob files, so
        // `used_bytes` here is the blob-inclusive figure capacity is measured
        // against.
        let (capacity, available, compaction_possible) =
            self.index.admission_capacity(stats.used_bytes);
        stats.capacity_bytes = capacity;
        stats.available_bytes = available;
        stats.compaction_possible = compaction_possible;
        // A full compaction of a blob tree also relocates STALE blob files, which
        // the merge gate budgets via `pick_blob_files_to_rewrite` (linked, stale,
        // non-dead). Estimate the same subset across every table — NOT the whole
        // live blob footprint, which would overstate the need (large non-stale
        // blobs are not rewritten) and report tight while the gate would admit
        // the merge. Fold it into the gauge figure.
        let blob_need = self.full_compaction_blob_need(&version)?;
        stats.full_compaction_bytes += blob_need;
        // Surface full-vs-tight compaction availability through the SAME two-layer
        // check the compaction space gate enforces (logical quota + physical free
        // per destination volume), so the status matches what the gate admits.
        if self.index.storage_admission_enabled()
            && capacity.is_some()
            && stats.status == crate::StorageStatus::Healthy
        {
            // SST output lands in the LAST configured level's volume
            // (`level_count - 1`), stale blob relocation in the primary blobs
            // volume; the demand is bounded by the largest level's size. The
            // per-volume gate (not `available >= full_compaction_bytes` against
            // the min-volume free) keeps the status from reporting tight when the
            // SST and blob outputs each fit their own volume — see the gate's
            // two-layer model.
            let sst_need = crate::storage_stats::full_compaction_demand_bytes(&version);
            // `saturating_sub`: `level_count >= 1` always (the clamp only guards a
            // degenerate zero-level config) → the last level index.
            let sst_dest_level = self.index.config.level_count.saturating_sub(1);
            let quota_headroom = self.index.quota_headroom(stats.used_bytes);
            let full_fits = crate::compaction::worker::space_fits_two_layer(
                &self.index.config,
                quota_headroom,
                sst_need,
                sst_dest_level,
                blob_need,
            );
            stats.status = if full_fits {
                crate::StorageStatus::FullCompactionAvailable
            } else {
                crate::StorageStatus::TightCompactionAvailable
            };
        }
        // Admission is driven by the index tree's runtime config / footprint;
        // a closed gate is the operator-actionable state (see the standard
        // tree's override for the precedence rationale).
        if self.index.is_read_only() {
            stats.status = crate::StorageStatus::ReadOnlyOutOfSpace;
        }
        Ok(stats)
    }

    fn write_admission(&self) -> crate::Result<()> {
        // Admission state lives on the index tree (which holds the runtime
        // config). The forward is blob-aware: the index tree's version IS this
        // blob tree's version (current_version() delegates here), and the gate's
        // footprint basis (`storage_stats::compute_used_bytes`) stats live
        // tables AND blob files, so blob bytes count toward the budget.
        self.index.write_admission()
    }

    #[cfg(feature = "metrics")]
    fn metrics(&self) -> &Arc<crate::Metrics> {
        self.index.metrics()
    }

    fn version_free_list_len(&self) -> usize {
        self.index.version_free_list_len()
    }

    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        use crate::prefix::compute_prefix_hash;
        use crate::range::prefix_to_range;

        let prefix_bytes = prefix.as_ref();

        let prefix_hash =
            compute_prefix_hash(self.index.config.prefix_extractor.as_ref(), prefix_bytes);

        let super_version = self.index.get_version_for_snapshot(seqno);
        let tree = self.clone();

        let range = prefix_to_range(prefix_bytes);

        Box::new(
            crate::Tree::create_internal_range_with_prefix_hash(
                super_version.clone(),
                &range,
                seqno,
                index,
                None, // BlobTree does not use merge operators for prefix scans
                self.index.config.comparator.clone(),
                prefix_hash,
            )
            .map(move |kv| {
                IterGuardImpl::Blob(Guard {
                    tree: tree.clone(),
                    version: super_version.version.clone(),
                    kv,
                })
            }),
        )
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        let super_version = self.index.get_version_for_snapshot(seqno);
        let tree = self.clone();

        Box::new(
            crate::Tree::create_internal_range(
                super_version.clone(),
                &range,
                seqno,
                index,
                None,
                self.index.config.comparator.clone(),
            )
            .map(move |kv| {
                IterGuardImpl::Blob(Guard {
                    tree: tree.clone(),
                    version: super_version.version.clone(),
                    kv,
                })
            }),
        )
    }

    fn range_seekable<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn crate::iter_guard::SeekableGuardIter + 'static> {
        let (lo, hi) = crate::tree::range_to_user_bounds(&range);
        let inner = self
            .index
            .create_seekable_range_bounds(lo, hi, seqno, index);
        let version = inner.version();
        Box::new(BlobSeekable {
            inner,
            tree: self.clone(),
            version,
        })
    }

    fn batch_range_scan<K: AsRef<[u8]>, R: RangeBounds<K> + 'static, I: IntoIterator<Item = R>>(
        &self,
        intervals: I,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn Iterator<Item = IterGuardImpl> + Send + 'static>
    where
        I::IntoIter: Send + 'static,
    {
        let inner = self.index.create_seekable_range_bounds(
            core::ops::Bound::Unbounded,
            core::ops::Bound::Unbounded,
            seqno,
            index,
        );
        let version = inner.version();
        let tree = self.clone();
        let intervals = intervals
            .into_iter()
            .map(|r| crate::tree::range_to_user_bounds(&r));
        Box::new(
            crate::range::BatchRangeScan::new(inner, intervals)
                .map(move |item| blob_guard(&tree, &version, item)),
        )
    }

    fn tombstone_count(&self) -> u64 {
        self.index.tombstone_count()
    }

    fn weak_tombstone_count(&self) -> u64 {
        self.index.weak_tombstone_count()
    }

    fn weak_tombstone_reclaimable_count(&self) -> u64 {
        self.index.weak_tombstone_reclaimable_count()
    }

    fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> crate::Result<()> {
        self.index.drop_range(range)
    }

    fn clear(&self) -> crate::Result<()> {
        let config = self.tree_config();
        let mut versions = self.get_version_history_lock();

        // Pre-clear snapshot: its tables AND blob files all become garbage once
        // the new empty version is installed.
        let prior = versions.latest_version();

        versions.upgrade_version(
            &config.path,
            |v| {
                let mut copy = v.clone();
                copy.active_memtable = Arc::new(Memtable::new(
                    self.index.memtable_id_counter.next(),
                    config.comparator.clone(),
                ));
                copy.sealed_memtables = Arc::default();
                copy.version = Version::new(v.version.id() + 1, self.tree_type());
                Ok(copy)
            },
            &config.seqno,
            &config.visible_seqno,
            &*config.fs,
            self.index.0.runtime_config.load_full(),
            self.index.0.config.encryption.clone(),
        )?;

        // Same MVCC-safe reclaim as the standard tree, plus the blob files:
        // mark every obsolete table / blob file deleted, drop the history's
        // hold, and let Inner::Drop reclaim each once its last reference is
        // released (a live reader's snapshot clone defers deletion).
        versions.drain_obsolete_to_latest();
        drop(versions);

        for table in prior.version.iter_tables() {
            table.mark_as_deleted();
        }
        for blob_file in prior.version.blob_files.iter() {
            blob_file.mark_as_deleted();
        }

        Ok(())
    }

    fn major_compact(
        &self,
        target_size: u64,
        seqno_threshold: SeqNo,
    ) -> crate::Result<crate::compaction::CompactionResult> {
        self.index.major_compact(target_size, seqno_threshold)
    }

    fn clear_active_memtable(&self) {
        self.index.clear_active_memtable();
    }

    fn l0_run_count(&self) -> usize {
        self.index.l0_run_count()
    }

    fn blob_file_count(&self) -> usize {
        self.current_version().blob_file_count()
    }

    // NOTE: We skip reading from the value log
    // because the vHandles already store the value size
    fn size_of<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<u32>> {
        let Some(item) = self.index.get_internal_entry(key.as_ref(), seqno)? else {
            return Ok(None);
        };

        Ok(Some(if item.key.value_type.is_indirection() {
            let mut cursor = crate::io::Cursor::new(item.value);
            let vptr = BlobIndirection::decode_from(&mut cursor)?;
            vptr.size
        } else {
            #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
            {
                item.value.len() as u32
            }
        }))
    }

    fn stale_blob_bytes(&self) -> u64 {
        self.current_version().gc_stats().stale_bytes()
    }

    fn filter_size(&self) -> u64 {
        self.index.filter_size()
    }

    fn pinned_filter_size(&self) -> usize {
        self.index.pinned_filter_size()
    }

    fn pinned_block_index_size(&self) -> usize {
        self.index.pinned_block_index_size()
    }

    fn sealed_memtable_count(&self) -> usize {
        self.index.sealed_memtable_count()
    }

    fn get_flush_lock(&self) -> FlushGuard<'_> {
        self.index.get_flush_lock()
    }

    #[expect(clippy::too_many_lines, reason = "flush logic is inherently complex")]
    fn flush_to_tables_with_rt(
        &self,
        stream: impl Iterator<Item = crate::Result<InternalValue>>,
        range_tombstones: Vec<crate::range_tombstone::RangeTombstone>,
    ) -> crate::Result<Option<(Vec<Table>, Option<Vec<BlobFile>>)>> {
        use crate::{coding::Encode, file::BLOBS_FOLDER, table::multi_writer::MultiWriter};

        let start = crate::time::Instant::now();

        let (table_folder, level_fs) = self.index.config.tables_folder_for_level(0);

        let data_block_size = self.index.config.data_block_size_policy.get(0);

        let data_block_restart_interval =
            self.index.config.data_block_restart_interval_policy.get(0);
        let index_block_restart_interval =
            self.index.config.index_block_restart_interval_policy.get(0);

        let data_block_compression = self.index.config.data_block_compression_policy.get(0);
        let index_block_compression = self.index.config.index_block_compression_policy.get(0);

        let data_block_hash_ratio = self.index.config.data_block_hash_ratio_policy.get(0);

        let index_partitioning = self.index.config.index_block_partitioning_policy.get(0);
        let filter_partitioning = self.index.config.filter_block_partitioning_policy.get(0);

        log::debug!(
            "Flushing memtable(s) and performing key-value separation, data_block_restart_interval={data_block_restart_interval}, index_block_restart_interval={index_block_restart_interval}, data_block_size={data_block_size}, data_block_compression={data_block_compression:?}, index_block_compression={index_block_compression:?}"
        );
        log::debug!("=> to table(s) in {}", table_folder.display());
        log::debug!("=> to blob file(s) at {}", self.blobs_folder.display());

        let mut table_writer = MultiWriter::new(
            table_folder.clone(),
            self.index.table_id_counter.clone(),
            64 * 1_024 * 1_024,
            0,
            level_fs.clone(),
        )?
        .set_comparator(self.index.config.comparator.clone())
        .use_data_block_restart_interval(data_block_restart_interval)
        .use_index_block_restart_interval(index_block_restart_interval)
        .use_data_block_compression(data_block_compression)
        .use_index_block_compression(index_block_compression)
        .use_data_block_size(data_block_size)
        .use_data_block_hash_ratio(data_block_hash_ratio)
        .use_bloom_policy({
            use crate::config::FilterPolicyEntry::{Bloom, None};
            use crate::table::filter::BloomConstructionPolicy;

            match self.index.config.filter_policy.get(0) {
                Bloom(policy) => policy,
                None => BloomConstructionPolicy::BitsPerKey(0.0),
            }
        });

        if index_partitioning {
            // Size-adaptive index: single-level for small SSTs, spill to
            // partitioned only past the threshold (see flush path). The
            // threshold is a live runtime config, read off the current snapshot.
            let rc = self.index.0.runtime_config.load_full();
            table_writer = table_writer.use_adaptive_index(rc.index_partition_spill_threshold);
        }
        if filter_partitioning {
            table_writer = table_writer.use_partitioned_filter();
        }

        table_writer =
            table_writer.use_prefix_extractor(self.index.config.prefix_extractor.clone());
        table_writer = table_writer.use_encryption(self.index.config.encryption.clone());

        #[cfg(zstd_any)]
        {
            table_writer =
                table_writer.use_zstd_dictionary(self.index.config.zstd_dictionary.clone());
        }

        #[expect(
            clippy::expect_used,
            reason = "cannot create blob tree without defining kv separation options"
        )]
        let kv_opts = self
            .index
            .config
            .kv_separation_opts
            .as_ref()
            .expect("kv separation options should exist");

        let mut blob_writer = {
            let w = BlobFileWriter::new(
                self.index.0.blob_file_id_counter.clone(),
                self.index.config.path.join(BLOBS_FOLDER),
                self.id(),
                self.index.config.descriptor_table.clone(),
                self.index.config.fs.clone(),
            )?
            .use_target_size(kv_opts.file_target_size)
            .use_compression(kv_opts.compression)
            .use_sync_mode(self.index.config.sync_mode);
            #[cfg(zstd_any)]
            let w = w.use_zstd_dictionary(kv_opts.zstd_dictionary.clone());
            w
        };

        let separation_threshold = kv_opts.separation_threshold;

        // Set range tombstones BEFORE writing KV items so that if MultiWriter
        // rotates to a new table during the write loop, earlier tables already
        // carry the RT metadata.
        table_writer.set_range_tombstones(range_tombstones);

        for item in stream {
            let item = item?;

            if item.is_tombstone() {
                // NOTE: Still need to add tombstone to index tree
                // But no blob to blob writer
                table_writer.write(InternalValue::new(item.key, UserValue::empty()))?;
                continue;
            }

            let value = item.value;

            #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
            let value_size = value.len() as u32;

            if value_size >= separation_threshold {
                let vhandle = blob_writer.write(&item.key.user_key, item.key.seqno, &value)?;

                let indirection = BlobIndirection {
                    vhandle,
                    size: value_size,
                };

                table_writer.write({
                    let mut vptr =
                        InternalValue::new(item.key.clone(), indirection.encode_into_vec());
                    vptr.key.value_type = crate::ValueType::Indirection;
                    vptr
                })?;

                table_writer.register_blob(indirection);
            } else {
                table_writer.write(InternalValue::new(item.key, value))?;
            }
        }

        let blob_files = blob_writer.finish()?;

        let result = table_writer.finish()?;

        log::debug!("Flushed memtable(s) in {:?}", start.elapsed());

        let pin_filter = self.index.config.filter_block_pinning_policy.get(0);
        let pin_index = self.index.config.index_block_pinning_policy.get(0);

        // Load tables
        let tables = result
            .into_iter()
            .map(|(table_id, checksum)| -> crate::Result<Table> {
                Table::recover(
                    table_folder.join(table_id.to_string()),
                    checksum,
                    0,
                    self.index.id,
                    table_id,
                    self.index.config.cache.clone(),
                    self.index.config.descriptor_table.clone(),
                    level_fs.clone(),
                    pin_filter,
                    pin_index,
                    self.index.config.encryption.clone(),
                    #[cfg(zstd_any)]
                    self.index.config.zstd_dictionary.clone(),
                    self.index.config.comparator.clone(),
                    #[cfg(feature = "metrics")]
                    self.index.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // Return Some even when tables is empty (RT-only flush): the caller
        // (AbstractTree::flush) handles empty tables by re-inserting RTs into
        // the active memtable and still needs to delete sealed memtables.
        Ok(Some((tables, Some(blob_files))))
    }

    fn register_tables(
        &self,
        tables: &[Table],
        blob_files: Option<&[BlobFile]>,
        frag_map: Option<FragmentationMap>,
        sealed_memtables_to_delete: &[MemtableId],
        gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        self.index.register_tables(
            tables,
            blob_files,
            frag_map,
            sealed_memtables_to_delete,
            gc_watermark,
        )
    }

    fn compact(
        &self,
        strategy: Arc<dyn crate::compaction::CompactionStrategy>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<crate::compaction::CompactionResult> {
        self.index.compact(strategy, seqno_threshold)
    }

    fn get_next_table_id(&self) -> TableId {
        self.index.get_next_table_id()
    }

    fn tree_config(&self) -> &Config {
        &self.index.config
    }

    fn get_highest_seqno(&self) -> Option<SeqNo> {
        self.index.get_highest_seqno()
    }

    fn active_memtable(&self) -> Arc<Memtable> {
        self.index.active_memtable()
    }

    fn rotate_memtable(&self) -> Option<Arc<Memtable>> {
        self.index.rotate_memtable()
    }

    fn table_count(&self) -> usize {
        self.index.table_count()
    }

    fn level_table_count(&self, idx: usize) -> Option<usize> {
        self.index.level_table_count(idx)
    }

    fn approximate_len(&self) -> usize {
        self.index.approximate_len()
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster key reads
    fn is_empty(&self, seqno: SeqNo, index: Option<(Arc<Memtable>, SeqNo)>) -> crate::Result<bool> {
        self.index.is_empty(seqno, index)
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster key reads
    fn contains_key<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<bool> {
        self.index.contains_key(key, seqno)
    }

    // NOTE: Override the default implementation to delegate directly
    // to the index tree, avoiding extra iterator/guard overhead for
    // prefix checks
    fn contains_prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> crate::Result<bool> {
        self.index.contains_prefix(prefix, seqno, index)
    }

    // NOTE: Override the default implementation to not fetch
    // data from the value log, so we get much faster scans
    fn len(&self, seqno: SeqNo, index: Option<(Arc<Memtable>, SeqNo)>) -> crate::Result<usize> {
        self.index.len(seqno, index)
    }

    fn disk_space(&self) -> u64 {
        let version = self.current_version();
        self.index.disk_space() + version.blob_files.on_disk_size()
    }

    fn get_highest_memtable_seqno(&self) -> Option<SeqNo> {
        self.index.get_highest_memtable_seqno()
    }

    fn get_highest_persisted_seqno(&self) -> Option<SeqNo> {
        self.index.get_highest_persisted_seqno()
    }

    fn apply_batch(&self, batch: crate::WriteBatch, seqno: SeqNo) -> crate::Result<(u64, u64)> {
        self.index.apply_batch(batch, seqno)
    }

    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
        seqno: SeqNo,
    ) -> (u64, u64) {
        self.index.insert(key, value.into(), seqno)
    }

    fn get<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<crate::UserValue>> {
        let super_version = self.index.get_version_for_snapshot(seqno);
        self.resolve_key(&super_version, key.as_ref(), seqno)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "indices are generated from 0..n range, always in bounds"
    )]
    fn multi_get<K: AsRef<[u8]>>(
        &self,
        keys: impl IntoIterator<Item = K>,
        seqno: SeqNo,
    ) -> crate::Result<Vec<Option<crate::UserValue>>> {
        let keys: Vec<_> = keys.into_iter().collect();
        let n = keys.len();
        if n == 0 {
            return Ok(Vec::new());
        }

        let super_version = self.index.get_version_for_snapshot(seqno);
        let comparator = self.index.config.comparator.as_ref();

        // For small batches, use the simple per-key path
        if n <= 2 {
            return keys
                .iter()
                .map(|key| self.resolve_key(&super_version, key.as_ref(), seqno))
                .collect();
        }

        // Phase 1: Check memtables (unsorted — defer sort+hash for SST phase)
        let mut internal_entries: Vec<Option<crate::value::InternalValue>> = vec![None; n];
        let mut remaining: Vec<usize> = Vec::with_capacity(n);

        for idx in 0..n {
            let key = keys[idx].as_ref();
            if let Some(entry) = super_version.active_memtable.get(key, seqno) {
                internal_entries[idx] = Some(entry);
                continue;
            }
            if let Some(entry) =
                crate::Tree::get_internal_entry_from_sealed_memtables(&super_version, key, seqno)
            {
                internal_entries[idx] = Some(entry);
                continue;
            }
            remaining.push(idx);
        }

        // Phase 2: Sort + hash only if memtable misses exist
        if !remaining.is_empty() {
            remaining.sort_by(|&a, &b| comparator.compare(keys[a].as_ref(), keys[b].as_ref()));

            let miss_keys: Vec<(usize, u64)> = remaining
                .iter()
                .map(|&idx| {
                    let hash = crate::hash::hash64(keys[idx].as_ref());
                    (idx, hash)
                })
                .collect();

            crate::Tree::batch_get_from_tables(
                &super_version.version,
                &keys,
                miss_keys,
                seqno,
                comparator,
                &mut internal_entries,
            )?;
        }

        // Phase 3: Resolve each entry (tombstones, RT suppression, merge, blob indirections)
        let mut results = vec![None; n];
        for idx in 0..n {
            if let Some(item) = internal_entries[idx].take() {
                if item.is_tombstone() {
                    continue;
                }
                if crate::Tree::is_suppressed_by_range_tombstones(
                    &super_version,
                    keys[idx].as_ref(),
                    item.key.seqno,
                    seqno,
                    comparator,
                ) {
                    continue;
                }
                // Merge operand resolution. Merge operands in BlobTree are stored
                // inline (not as blob indirection), so the pipeline result is a
                // plain value. Without a merge operator, return raw operand value
                // (same as resolve_key / resolve_pinned_entry behavior).
                if item.key.value_type.is_merge_operand() {
                    if let Some(merge_op) = &self.index.config.merge_operator {
                        results[idx] = crate::Tree::resolve_merge_via_pipeline(
                            super_version.clone(),
                            keys[idx].as_ref(),
                            seqno,
                            Arc::clone(merge_op),
                        )?;
                    } else {
                        results[idx] = Some(item.value);
                    }
                    continue;
                }
                let (_, v) = resolve_value_handle(
                    self.id(),
                    self.blobs_folder.as_path(),
                    &self.index.config.cache,
                    &super_version.version,
                    item,
                    #[cfg(zstd_any)]
                    self.index
                        .config
                        .kv_separation_opts
                        .as_ref()
                        .and_then(|o| o.zstd_dictionary.as_deref()),
                )?;
                results[idx] = Some(v);
            }
        }

        Ok(results)
    }

    fn merge<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        operand: V,
        seqno: SeqNo,
    ) -> (u64, u64) {
        self.index.merge(key, operand, seqno)
    }

    fn remove<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        self.index.remove(key, seqno)
    }

    fn remove_weak<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        self.index.remove_weak(key, seqno)
    }

    fn remove_range<K: Into<UserKey>>(&self, start: K, end: K, seqno: SeqNo) -> u64 {
        self.index.remove_range(start, end, seqno)
    }
}
