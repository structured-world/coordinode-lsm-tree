// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

pub mod ingest;
pub mod inner;
pub mod sealed;

use crate::path::Path;
use crate::{
    AbstractTree, Checksum, KvPair, SeqNo, SequenceNumberCounter, TableId, UserKey, UserValue,
    ValueType,
    compaction::{CompactionStrategy, drop_range::OwnedBounds, state::CompactionState},
    config::Config,
    format_version::FormatVersion,
    fs::Fs,
    iter_guard::{IterGuard, IterGuardImpl},
    key::InternalKey,
    manifest::Manifest,
    memtable::Memtable,
    range_tombstone::RangeTombstone,
    scan_since::ScanSinceEvent,
    slice::Slice,
    table::Table,
    value::InternalValue,
    version::{SuperVersion, SuperVersions, Version, recovery::recover},
    vlog::BlobFile,
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::ToString, vec::Vec};
use core::ops::{Bound, RangeBounds};
use inner::{FlushGuard, TreeId, TreeInner, VersionsWriteGuard};
// no-std: spin mirrors parking_lot's Mutex/RwLock API without an allocator.
// parking_lot wins on the std hot path, so keep it for std.
#[cfg(feature = "std")]
use parking_lot::{Mutex, RwLock};
#[cfg(not(feature = "std"))]
use spin::{Mutex, RwLock};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

/// Iterator value guard
pub struct Guard(crate::Result<(UserKey, UserValue)>);

impl IterGuard for Guard {
    fn into_inner_if(
        self,
        pred: impl Fn(&UserKey) -> bool,
    ) -> crate::Result<(UserKey, Option<UserValue>)> {
        let (k, v) = self.0?;

        if pred(&k) {
            Ok((k, Some(v)))
        } else {
            Ok((k, None))
        }
    }

    fn key(self) -> crate::Result<UserKey> {
        self.0.map(|(k, _)| k)
    }

    fn size(self) -> crate::Result<u32> {
        #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
        self.into_inner().map(|(_, v)| v.len() as u32)
    }

    fn into_inner(self) -> crate::Result<(UserKey, UserValue)> {
        self.0
    }
}

/// Trait for monomorphized table point-read results.
///
/// Allows `find_in_tables` to operate generically over `InternalValue` (for
/// `get`) and `(InternalValue, Block)` (for `get_pinned`), generating optimal
/// code for each path without runtime dispatch or extra refcount overhead.
trait TablePointLookup: Sized {
    fn lookup(
        table: &Table,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<Self>>;
    fn entry_seqno(&self) -> SeqNo;
    fn filter_tombstone(self) -> Option<Self>;
}

/// Lookup result for standard `get()` — entry only, no block retained.
type TableEntry = InternalValue;

impl TablePointLookup for TableEntry {
    fn lookup(
        table: &Table,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<Self>> {
        table.get(key, seqno, key_hash)
    }

    fn entry_seqno(&self) -> SeqNo {
        self.key.seqno
    }

    fn filter_tombstone(self) -> Option<Self> {
        ignore_tombstone_value(self)
    }
}

/// Lookup result for `get_pinned()` — entry + block for zero-copy pinning.
type TableEntryWithBlock = (InternalValue, crate::table::Block);

impl TablePointLookup for TableEntryWithBlock {
    fn lookup(
        table: &Table,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<Self>> {
        table.get_with_block(key, seqno, key_hash)
    }

    fn entry_seqno(&self) -> SeqNo {
        self.0.key.seqno
    }

    fn filter_tombstone(self) -> Option<Self> {
        ignore_tombstone_value(self.0).map(|iv| (iv, self.1))
    }
}

/// Lookup result for the value-returning `get()` path: `(value_type, seqno,
/// value)`, no key reconstruction (the caller has the needle).
type TableValue = (ValueType, SeqNo, crate::Slice);

impl TablePointLookup for TableValue {
    fn lookup(
        table: &Table,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<Self>> {
        table.get_value(key, seqno, key_hash)
    }

    fn entry_seqno(&self) -> SeqNo {
        self.1
    }

    fn filter_tombstone(self) -> Option<Self> {
        if self.0.is_tombstone() {
            None
        } else {
            Some(self)
        }
    }
}

fn ignore_tombstone_value(item: InternalValue) -> Option<InternalValue> {
    if item.is_tombstone() {
        None
    } else {
        Some(item)
    }
}

/// A log-structured merge tree (LSM-tree/LSMT)
#[derive(Clone)]
pub struct Tree(#[doc(hidden)] pub Arc<TreeInner>);

impl core::ops::Deref for Tree {
    type Target = TreeInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl crate::abstract_tree::sealed::Sealed for Tree {}

impl AbstractTree for Tree {
    fn table_file_cache_size(&self) -> usize {
        self.config
            .descriptor_table
            .as_ref()
            .map_or(0, |dt| dt.len())
    }

    fn get_version_history_lock(&self) -> VersionsWriteGuard<'_> {
        self.version_history.write()
    }

    fn next_table_id(&self) -> TableId {
        self.0.table_id_counter.get()
    }

    fn id(&self) -> TreeId {
        self.id
    }

    fn blob_file_count(&self) -> usize {
        0
    }

    #[cfg(feature = "std")]
    fn create_checkpoint(
        &self,
        target_path: &crate::path::Path,
    ) -> crate::Result<crate::CheckpointInfo> {
        crate::checkpoint::run_checkpoint(
            self,
            &crate::checkpoint::CheckpointParams {
                target_root: target_path,
                target_fs: &self.config.fs,
                src_root: &self.config.path,
                src_fs: &self.config.fs,
                deletion_pause: &self.deletion_pause,
                visible_seqno: &self.config.visible_seqno,
                include_blobs: false,
                runtime_config: self.0.runtime_config.load_full(),
                encryption: self.0.config.encryption.clone(),
            },
        )
    }

    fn print_trace(&self, key: &[u8]) -> crate::Result<()> {
        let super_version = self.version_history.read().latest_version();

        let key = Slice::from(key);

        for kv in super_version.active_memtable.range_internal((
            Bound::Included(InternalKey::new(key.clone(), SeqNo::MAX, ValueType::Value)),
            Bound::Unbounded,
        )) {
            log::info!("[Active] {kv:?}");
        }

        for mt in super_version.sealed_memtables.iter().rev() {
            for kv in mt.range_internal((
                Bound::Included(InternalKey::new(key.clone(), SeqNo::MAX, ValueType::Value)),
                Bound::Unbounded,
            )) {
                log::info!("[Sealed #{}] {kv:?}", mt.id());
            }
        }

        for table in super_version
            .version
            .iter_levels()
            .flat_map(|lvl| lvl.iter())
            .filter_map(|run| run.get_for_key_cmp(&key, self.config.comparator.as_ref()))
        {
            for kv in table.range(..) {
                let kv = kv?;

                if kv.key.user_key != key {
                    break;
                }

                log::info!("[Table #{}] {kv:?}", table.id());
            }
        }

        Ok(())
    }

    fn get_internal_entry(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>> {
        // Lock-free fast path: when reading at or beyond the latest installed
        // version (always the case for MAX_SEQNO, and the common case), the
        // mirrored latest SuperVersion is exactly what `get_version_for_snapshot`
        // would return (it yields the latest iff `latest.seqno < seqno`), so
        // load it without taking the history RwLock or cloning a deque entry.
        // Recent inserts stay visible because they mutate the shared
        // `active_memtable` behind a stable Arc; the back only changes on
        // flush / compaction, which refresh this mirror under the write lock.
        //
        // std-only: the mirror needs `arc-swap` (not no_std). Under no-std we
        // skip straight to the history RwLock path below.
        #[cfg(feature = "std")]
        {
            let latest = self.latest_super_version.load();
            if seqno > latest.seqno {
                return Self::get_internal_entry_from_version(
                    &latest,
                    key,
                    seqno,
                    self.config.comparator.as_ref(),
                );
            }
        }

        // Historical snapshot read (seqno <= latest.seqno): consult the locked
        // version history for the correct point-in-time SuperVersion.
        let super_version = self.version_history.read().get_version_for_snapshot(seqno);

        Self::get_internal_entry_from_version(
            &super_version,
            key,
            seqno,
            self.config.comparator.as_ref(),
        )
    }

    fn current_version(&self) -> Version {
        self.version_history.read().latest_version().version
    }

    fn get_flush_lock(&self) -> FlushGuard<'_> {
        self.flush_lock.lock()
    }

    #[cfg(feature = "metrics")]
    fn metrics(&self) -> &Arc<crate::Metrics> {
        &self.0.metrics
    }

    fn version_free_list_len(&self) -> usize {
        self.version_history.read().free_list_len()
    }

    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        Box::new(
            self.create_prefix(&prefix, seqno, index)
                .map(|kv| IterGuardImpl::Standard(Guard(kv))),
        )
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        Box::new(
            self.create_range(&range, seqno, index)
                .map(|kv| IterGuardImpl::Standard(Guard(kv))),
        )
    }

    /// Returns the number of tombstones in the tree.
    fn tombstone_count(&self) -> u64 {
        self.current_version()
            .iter_tables()
            .map(Table::tombstone_count)
            .sum()
    }

    /// Returns the number of weak tombstones (single deletes) in the tree.
    fn weak_tombstone_count(&self) -> u64 {
        self.current_version()
            .iter_tables()
            .map(Table::weak_tombstone_count)
            .sum()
    }

    /// Returns the number of value entries that become reclaimable once weak tombstones can be GC'd.
    fn weak_tombstone_reclaimable_count(&self) -> u64 {
        self.current_version()
            .iter_tables()
            .map(Table::weak_tombstone_reclaimable)
            .sum()
    }

    fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> crate::Result<()> {
        let (bounds, is_empty) = Self::range_bounds_to_owned_bounds(&range);

        if is_empty {
            return Ok(());
        }

        let strategy = Arc::new(crate::compaction::drop_range::Strategy::new(bounds));

        // IMPORTANT: Write lock so we can be the only compaction going on
        let _lock = self.0.major_compaction_lock.write();

        log::info!("Starting drop_range compaction");
        self.inner_compact(strategy, 0)?;
        Ok(())
    }

    fn clear(&self) -> crate::Result<()> {
        let config = self.tree_config();
        let mut versions = self.get_version_history_lock();

        // Pre-clear snapshot: every table + blob file it references becomes
        // garbage the moment the new empty version is installed.
        let prior = versions.latest_version();

        versions.upgrade_version(
            &config.path,
            |v| {
                let mut copy = v.clone();
                copy.active_memtable = Arc::new(Memtable::new(
                    self.memtable_id_counter.next(),
                    self.config.comparator.clone(),
                ));
                copy.sealed_memtables = Arc::default();
                copy.version = Version::new(v.version.id() + 1, self.tree_type());
                Ok(copy)
            },
            &config.seqno,
            &config.visible_seqno,
            &*config.fs,
            self.0.runtime_config.load_full(),
            self.0.config.encryption.clone(),
        )?;

        // Release the history's hold on the now-obsolete versions; only the new
        // empty version remains. `prior` still holds them, so nothing reaches
        // refcount zero yet.
        versions.drain_obsolete_to_latest();
        drop(versions); // release the version-history lock before any fs work

        // Mark every obsolete table / blob file deleted so the file is
        // reclaimed (Inner::Drop) once its last reference is released. A
        // concurrent reader still holding the pre-clear snapshot keeps its own
        // clone alive, deferring physical deletion until it finishes — the
        // version-history Arc refcount is the MVCC guard, so reclaim never
        // races a live read. Tables with no other live reference are reclaimed
        // as `prior` drops at the end of this call.
        for table in prior.version.iter_tables() {
            table.mark_as_deleted();
        }
        for blob_file in prior.version.blob_files.iter() {
            blob_file.mark_as_deleted();
        }

        Ok(())
    }

    #[doc(hidden)]
    fn major_compact(
        &self,
        target_size: u64,
        seqno_threshold: SeqNo,
    ) -> crate::Result<crate::compaction::CompactionResult> {
        let strategy = Arc::new(crate::compaction::major::Strategy::new(target_size));

        // IMPORTANT: Write lock so we can be the only compaction going on
        let _lock = self.0.major_compaction_lock.write();

        log::info!("Starting major compaction");
        self.inner_compact(strategy, seqno_threshold)
    }

    fn l0_run_count(&self) -> usize {
        self.current_version()
            .level(0)
            .map(|x| x.run_count())
            .unwrap_or_default()
    }

    fn size_of<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<u32>> {
        #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
        Ok(self.get(key, seqno)?.map(|x| x.len() as u32))
    }

    fn filter_size(&self) -> u64 {
        self.current_version()
            .iter_tables()
            .map(Table::filter_size)
            .map(u64::from)
            .sum()
    }

    fn pinned_filter_size(&self) -> usize {
        self.current_version()
            .iter_tables()
            .map(Table::pinned_filter_size)
            .sum()
    }

    fn pinned_block_index_size(&self) -> usize {
        self.current_version()
            .iter_tables()
            .map(Table::pinned_block_index_size)
            .sum()
    }

    fn sealed_memtable_count(&self) -> usize {
        self.version_history
            .read()
            .latest_version()
            .sealed_memtables
            .len()
    }

    fn flush_to_tables_with_rt(
        &self,
        stream: impl Iterator<Item = crate::Result<InternalValue>>,
        range_tombstones: Vec<crate::range_tombstone::RangeTombstone>,
    ) -> crate::Result<Option<(Vec<Table>, Option<Vec<BlobFile>>)>> {
        use crate::table::multi_writer::MultiWriter;
        use crate::time::Instant;

        let start = Instant::now();

        let (folder, level_fs) = self.config.tables_folder_for_level(0);

        let data_block_size = self.config.data_block_size_policy.get(0);

        let data_block_restart_interval = self.config.data_block_restart_interval_policy.get(0);
        let index_block_restart_interval = self.config.index_block_restart_interval_policy.get(0);

        let data_block_compression = self.config.data_block_compression_policy.get(0);
        let index_block_compression = self.config.index_block_compression_policy.get(0);

        let data_block_hash_ratio = self.config.data_block_hash_ratio_policy.get(0);

        let index_partitioning = self.config.index_block_partitioning_policy.get(0);
        let filter_partitioning = self.config.filter_block_partitioning_policy.get(0);

        // One runtime-config snapshot for the whole flush writer setup. The
        // index spill threshold, `seqno_in_index`, and the per-KV checksum
        // policy are all live (toggleable via `update_runtime_config`); reading
        // `load_full()` per field could straddle a concurrent update and mix two
        // snapshots into one SST. Compaction is the migration mechanism, so a
        // toggle takes effect on the next flush / compaction.
        let rc = self.0.runtime_config.load_full();

        log::debug!(
            "Flushing memtable(s) to {}, data_block_restart_interval={data_block_restart_interval}, index_block_restart_interval={index_block_restart_interval}, data_block_size={data_block_size}, data_block_compression={data_block_compression:?}, index_block_compression={index_block_compression:?}",
            folder.display(),
        );

        let mut table_writer = MultiWriter::new(
            folder.clone(),
            self.table_id_counter.clone(),
            64 * 1_024 * 1_024,
            0,
            level_fs.clone(),
        )?
        .set_comparator(self.config.comparator.clone())
        .use_data_block_restart_interval(data_block_restart_interval)
        .use_index_block_restart_interval(index_block_restart_interval)
        .use_data_block_compression(data_block_compression)
        .use_index_block_compression(index_block_compression)
        .use_data_block_size(data_block_size)
        .use_data_block_hash_ratio(data_block_hash_ratio)
        .use_bloom_policy({
            use crate::config::FilterPolicyEntry::{Bloom, None};
            use crate::table::filter::BloomConstructionPolicy;

            match self.config.filter_policy.get(0) {
                Bloom(policy) => policy,
                None => BloomConstructionPolicy::BitsPerKey(0.0),
            }
        });

        if index_partitioning {
            // Size-adaptive: single-level index for small SSTs (where pinning
            // the whole index is cheap and a two-level lookup is pure overhead),
            // spilling to a partitioned index only once the index grows past the
            // threshold. Recovers the point-read cost of an unconditional
            // two-level index on small/medium SSTs.
            table_writer = table_writer.use_adaptive_index(rc.index_partition_spill_threshold);
        }
        if filter_partitioning {
            table_writer = table_writer.use_partitioned_filter();
        }

        table_writer = table_writer.use_prefix_extractor(self.config.prefix_extractor.clone());
        table_writer = table_writer.use_encryption(self.config.encryption.clone());
        // ECC scheme from the live runtime snapshot (same as `seqno_in_index`
        // / `kv_checksums` below), so a flush after a scheme change writes the
        // SST with the current scheme rather than the startup one.
        table_writer = table_writer.use_page_ecc(self.config.page_ecc, rc.ecc_scheme);
        table_writer = table_writer.use_sync_mode(self.config.sync_mode);

        table_writer = table_writer.use_seqno_in_index(rc.seqno_in_index);
        table_writer = table_writer.use_disable_cow_on_sst(rc.disable_cow_on_sst_files);
        // `Off` (default) emits no per-KV footer and leaves the data-block
        // payload encoding unchanged (the V5 header carries a block_flags byte
        // and the meta block a descriptor key regardless, so the on-disk bytes
        // are not identical to a pre-V5 table).
        table_writer = table_writer.use_kv_checksums(rc.kv_checksums, rc.kv_checksum_algo);

        #[cfg(zstd_any)]
        {
            table_writer = table_writer.use_zstd_dictionary(self.config.zstd_dictionary.clone());
        }

        // Set range tombstones BEFORE writing KV items so that if MultiWriter
        // rotates to a new table during the write loop, earlier tables already
        // carry the RT metadata.
        table_writer.set_range_tombstones(range_tombstones);

        for item in stream {
            table_writer.write(item?)?;
        }

        let result = table_writer.finish()?;

        log::debug!("Flushed memtable(s) in {:?}", start.elapsed());

        let pin_filter = self.config.filter_block_pinning_policy.get(0);
        let pin_index = self.config.index_block_pinning_policy.get(0);

        // Load tables
        let tables = result
            .into_iter()
            .map(|(table_id, checksum)| -> crate::Result<Table> {
                Table::recover(
                    folder.join(table_id.to_string()),
                    checksum,
                    0,
                    self.id,
                    table_id,
                    self.config.cache.clone(),
                    self.config.descriptor_table.clone(),
                    level_fs.clone(),
                    pin_filter,
                    pin_index,
                    self.config.encryption.clone(),
                    #[cfg(zstd_any)]
                    self.config.zstd_dictionary.clone(),
                    self.config.comparator.clone(),
                    #[cfg(feature = "metrics")]
                    self.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // Return Some even when tables is empty (RT-only flush): the caller
        // (AbstractTree::flush) handles empty tables by re-inserting RTs into
        // the active memtable and still needs to delete sealed memtables.
        Ok(Some((tables, None)))
    }

    #[expect(clippy::significant_drop_tightening)]
    fn register_tables(
        &self,
        tables: &[Table],
        blob_files: Option<&[BlobFile]>,
        frag_map: Option<crate::blob_tree::FragmentationMap>,
        sealed_memtables_to_delete: &[crate::tree::inner::MemtableId],
        gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        log::trace!(
            "Registering {} tables, {} blob files",
            tables.len(),
            blob_files.map(<[BlobFile]>::len).unwrap_or_default(),
        );

        // Wire the tree-wide deletion pause into every fresh table / blob
        // file so an in-flight checkpoint defers their cleanup if they
        // later get marked `is_deleted` by compaction.
        for table in tables {
            table.install_deletion_pause(Arc::clone(&self.deletion_pause));
            #[cfg(feature = "std")]
            table.install_background_deleter(Arc::clone(&self.background_deleter));
            table.install_heal_hints(Arc::clone(&self.heal_hints));
        }
        if let Some(bfs) = blob_files {
            for bf in bfs {
                bf.install_deletion_pause(Arc::clone(&self.deletion_pause));
                #[cfg(feature = "std")]
                bf.install_background_deleter(Arc::clone(&self.background_deleter));
            }
        }

        let mut _compaction_state = self.compaction_state.lock();
        let mut version_lock = self.version_history.write();

        version_lock.upgrade_version(
            &self.config.path,
            |current| {
                let mut copy = current.clone();

                let ctx = crate::version::TransformContext::new(self.config.comparator.as_ref());
                copy.version = copy.version.with_new_l0_run(
                    tables,
                    blob_files,
                    frag_map.filter(|x| !x.is_empty()),
                    &ctx,
                );

                for &table_id in sealed_memtables_to_delete {
                    log::trace!("releasing sealed memtable #{table_id}");
                    copy.sealed_memtables = Arc::new(copy.sealed_memtables.remove(table_id));
                }

                Ok(copy)
            },
            &self.config.seqno,
            &self.config.visible_seqno,
            &*self.config.fs,
            self.0.runtime_config.load_full(),
            self.0.config.encryption.clone(),
        )?;

        if let Err(e) = version_lock.maintenance(&self.config.path, gc_watermark, &*self.config.fs)
        {
            log::warn!("Version GC failed: {e:?}");
        }

        Ok(())
    }

    fn clear_active_memtable(&self) {
        use crate::tree::sealed::SealedMemtables;

        let mut version_history_lock = self.version_history.write();
        let super_version = version_history_lock.latest_version();

        if super_version.active_memtable.is_empty() {
            return;
        }

        let mut copy = version_history_lock.latest_version();
        copy.active_memtable = Arc::new(Memtable::new(
            self.memtable_id_counter.next(),
            self.config.comparator.clone(),
        ));
        copy.sealed_memtables = Arc::new(SealedMemtables::default());

        // Rotate does not modify the memtable, so it cannot break snapshots
        copy.seqno = super_version.seqno;

        version_history_lock.replace_latest_version(copy);

        log::trace!("cleared active memtable");
    }

    fn compact(
        &self,
        strategy: Arc<dyn CompactionStrategy>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<crate::compaction::CompactionResult> {
        // NOTE: Read lock major compaction lock
        // That way, if a major compaction is running, we cannot proceed
        // But in general, parallel (non-major) compactions can occur
        let _lock = self.0.major_compaction_lock.read();

        self.inner_compact(strategy, seqno_threshold)
    }

    fn get_next_table_id(&self) -> TableId {
        self.0.get_next_table_id()
    }

    fn tree_config(&self) -> &Config {
        &self.config
    }

    fn active_memtable(&self) -> Arc<Memtable> {
        self.version_history.read().latest_version().active_memtable
    }

    #[expect(clippy::significant_drop_tightening)]
    fn rotate_memtable(&self) -> Option<Arc<Memtable>> {
        let mut version_history_lock = self.version_history.write();
        let super_version = version_history_lock.latest_version();

        if super_version.active_memtable.is_empty() {
            return None;
        }

        let yanked_memtable = super_version.active_memtable;

        let mut copy = version_history_lock.latest_version();
        copy.active_memtable = Arc::new(Memtable::new(
            self.memtable_id_counter.next(),
            self.config.comparator.clone(),
        ));
        copy.sealed_memtables =
            Arc::new(super_version.sealed_memtables.add(yanked_memtable.clone()));

        // Rotate does not modify the memtable so it cannot break snapshots
        copy.seqno = super_version.seqno;

        version_history_lock.replace_latest_version(copy);

        log::trace!(
            "rotate: added memtable id={} to sealed memtables",
            yanked_memtable.id,
        );

        Some(yanked_memtable)
    }

    fn table_count(&self) -> usize {
        self.current_version().table_count()
    }

    fn level_table_count(&self, idx: usize) -> Option<usize> {
        self.current_version().level(idx).map(|x| x.table_count())
    }

    fn approximate_len(&self) -> usize {
        let super_version = self.version_history.read().latest_version();

        let tables_item_count = self
            .current_version()
            .iter_tables()
            .map(|x| x.metadata.item_count)
            .sum::<u64>();

        let memtable_count = super_version.active_memtable.len() as u64;
        let sealed_count = super_version
            .sealed_memtables
            .iter()
            .map(|mt| mt.len())
            .sum::<usize>() as u64;

        #[expect(clippy::expect_used, reason = "result should fit into usize")]
        (memtable_count + sealed_count + tables_item_count)
            .try_into()
            .expect("approximate_len too large for usize")
    }

    fn disk_space(&self) -> u64 {
        self.current_version()
            .iter_levels()
            .map(super::version::Level::size)
            .sum()
    }

    fn get_highest_memtable_seqno(&self) -> Option<SeqNo> {
        let version = self.version_history.read().latest_version();

        let active = version.active_memtable.get_highest_seqno();

        let sealed = version
            .sealed_memtables
            .iter()
            .map(|mt| mt.get_highest_seqno())
            .max()
            .flatten();

        active.max(sealed)
    }

    fn get_highest_persisted_seqno(&self) -> Option<SeqNo> {
        self.current_version()
            .iter_tables()
            .map(Table::get_highest_seqno)
            .max()
    }

    fn get<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<UserValue>> {
        let key = key.as_ref();

        let super_version = self.version_history.read().get_version_for_snapshot(seqno);

        Self::resolve_or_passthrough(
            &super_version,
            key,
            seqno,
            self.config.merge_operator.as_ref(),
            self.config.comparator.as_ref(),
        )
    }

    fn get_pinned<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: SeqNo,
    ) -> crate::Result<Option<crate::PinnableSlice>> {
        let key = key.as_ref();

        let super_version = self.version_history.read().get_version_for_snapshot(seqno);

        Self::resolve_or_passthrough_pinned(
            &super_version,
            key,
            seqno,
            self.config.merge_operator.as_ref(),
            self.config.comparator.as_ref(),
        )
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "indices are generated from 0..n range, always in bounds"
    )]
    fn multi_get<K: AsRef<[u8]>>(
        &self,
        keys: impl IntoIterator<Item = K>,
        seqno: SeqNo,
    ) -> crate::Result<Vec<Option<UserValue>>> {
        let super_version = self.get_version_for_snapshot(seqno);
        let comparator = self.config.comparator.as_ref();
        let merge_operator = self.config.merge_operator.as_ref();

        // Collect keys up front; bloom hashes computed lazily in Phase 2
        let keys: Vec<_> = keys.into_iter().collect();
        let n = keys.len();
        if n == 0 {
            return Ok(Vec::new());
        }

        // For small batches, use the simple per-key path
        if n <= 2 {
            return keys
                .iter()
                .map(|key| {
                    Self::resolve_or_passthrough(
                        &super_version,
                        key.as_ref(),
                        seqno,
                        merge_operator,
                        comparator,
                    )
                })
                .collect();
        }

        // Phase 1: Check active + sealed memtables (unsorted — memtable lookup
        // is O(log n) per key regardless of order, skip sort+hash overhead for
        // memtable-only batches).
        let mut internal_entries: Vec<Option<InternalValue>> = vec![None; n];
        let mut remaining: Vec<usize> = Vec::with_capacity(n);

        for idx in 0..n {
            let key = keys[idx].as_ref();

            // Active memtable
            if let Some(entry) = super_version.active_memtable.get(key, seqno) {
                internal_entries[idx] = Some(entry);
                continue;
            }

            // Sealed memtables (newest first)
            if let Some(entry) =
                Self::get_internal_entry_from_sealed_memtables(&super_version, key, seqno)
            {
                internal_entries[idx] = Some(entry);
                continue;
            }

            remaining.push(idx);
        }

        // Phase 2: Sort remaining keys + compute bloom hashes only if needed
        // (memtable-only batches skip this entirely).
        if !remaining.is_empty() {
            remaining.sort_by(|&a, &b| comparator.compare(keys[a].as_ref(), keys[b].as_ref()));

            // Build (idx, hash) pairs only for miss keys — O(remaining) not O(n).
            let miss_keys: Vec<(usize, u64)> = remaining
                .iter()
                .map(|&idx| {
                    let hash = crate::hash::hash64(keys[idx].as_ref());
                    (idx, hash)
                })
                .collect();

            Self::batch_get_from_tables(
                &super_version.version,
                &keys,
                miss_keys,
                seqno,
                comparator,
                &mut internal_entries,
            )?;
        }

        // Phase 3: Resolve entries (tombstones, RT suppression, merge operands)
        let mut results = vec![None; n];
        for idx in 0..n {
            let entry = internal_entries[idx].take();
            results[idx] = Self::resolve_entry(
                &super_version,
                keys[idx].as_ref(),
                entry,
                seqno,
                merge_operator,
                comparator,
            )?;
        }

        Ok(results)
    }

    fn apply_batch(&self, batch: crate::WriteBatch, seqno: SeqNo) -> crate::Result<(u64, u64)> {
        if batch.is_empty() {
            return Ok((0, self.active_memtable().size()));
        }
        Ok(self.append_batch(batch.materialize(seqno)?))
    }

    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
        seqno: SeqNo,
    ) -> (u64, u64) {
        let value = InternalValue::from_components(key, value, seqno, ValueType::Value);
        self.append_entry(value)
    }

    fn merge<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        operand: V,
        seqno: SeqNo,
    ) -> (u64, u64) {
        let value = InternalValue::new_merge_operand(key, operand, seqno);
        self.append_entry(value)
    }

    fn remove<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        let value = InternalValue::new_tombstone(key, seqno);
        self.append_entry(value)
    }

    fn remove_weak<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        let value = InternalValue::new_weak_tombstone(key, seqno);
        self.append_entry(value)
    }

    fn remove_range<K: Into<UserKey>>(&self, start: K, end: K, seqno: SeqNo) -> u64 {
        let memtable = Arc::clone(&self.version_history.read().latest_version().active_memtable);

        memtable.insert_range_tombstone(start.into(), end.into(), seqno)
    }
}

impl Tree {
    /// Maps a raw internal entry to its change-data-capture event, routing
    /// `Indirection` (KV-separated) values through `resolve_indirection`.
    ///
    /// A standard tree never stores `Indirection` and supplies a resolver that
    /// errors; the blob-tree scan path supplies one that reads the blob and
    /// returns an [`ScanSinceEvent::Insert`].
    fn map_event<F>(
        entry: InternalValue,
        version: &Version,
        resolve_indirection: &F,
    ) -> crate::Result<ScanSinceEvent>
    where
        F: Fn(&Version, InternalValue) -> crate::Result<ScanSinceEvent>,
    {
        if entry.key.value_type == ValueType::Indirection {
            return resolve_indirection(version, entry);
        }
        let seqno = entry.key.seqno;
        let key = entry.key.user_key;
        Ok(match entry.key.value_type {
            ValueType::Value => ScanSinceEvent::Insert {
                key,
                value: entry.value,
                seqno,
            },
            ValueType::MergeOperand => ScanSinceEvent::MergeOperand {
                key,
                operand: entry.value,
                seqno,
            },
            ValueType::Tombstone | ValueType::WeakTombstone => {
                ScanSinceEvent::PointTombstone { key, seqno }
            }
            ValueType::Indirection => unreachable!("Indirection handled above"),
        })
    }

    /// Shared CDC aggregation behind [`Self::scan_since_seqno`] and the
    /// blob-tree scan path: gathers qualifying entries (`seqno >= target`) plus
    /// range tombstones from the active + sealed memtables and every SST (with
    /// block-skip), maps each entry to a [`ScanSinceEvent`] — routing
    /// `Indirection` values through `resolve_indirection` against the same
    /// version snapshot — and returns them in increasing seqno order.
    ///
    /// # Panics
    ///
    /// Panics if the internal version-history lock is poisoned.
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading the index or a data block fails, or if
    /// `resolve_indirection` errors.
    pub(crate) fn scan_since_seqno_with<F>(
        &self,
        target_seqno: SeqNo,
        resolve_indirection: F,
    ) -> crate::Result<alloc::vec::IntoIter<ScanSinceEvent>>
    where
        F: Fn(&Version, InternalValue) -> crate::Result<ScanSinceEvent>,
    {
        let super_version = self.version_history.read().latest_version();
        let version = &super_version.version;

        // Stable upper watermark, captured once before walking any source: the
        // highest seqno present across every source at scan start (in global
        // coordinates — `Table::get_highest_seqno` already adds the offset).
        // The active memtable is shared and mutable, so without this cap a
        // write committed mid-scan could leak in and break the "consistent
        // snapshot of changes in [target, watermark]" contract. The seqno
        // counter is not a reliable bound here because callers may assign
        // seqnos explicitly without advancing it, so derive it from the data.
        let end_seqno = {
            let active = super_version.active_memtable.get_highest_seqno();
            let sealed = super_version
                .sealed_memtables
                .iter()
                .map(|mt| mt.get_highest_seqno())
                .max()
                .flatten();
            let tables = version.iter_tables().map(Table::get_highest_seqno).max();
            active.max(sealed).max(tables)
        };
        // No entries anywhere ⇒ nothing qualifies, regardless of target.
        let Some(end_seqno) = end_seqno else {
            return Ok(Vec::new().into_iter());
        };

        let mut events: Vec<ScanSinceEvent> = Vec::new();

        // Point entries: active + sealed memtables, then SSTs (block-skip).
        for entry in super_version.active_memtable.iter() {
            if entry.key.seqno >= target_seqno && entry.key.seqno <= end_seqno {
                events.push(Self::map_event(entry, version, &resolve_indirection)?);
            }
        }
        for memtable in super_version.sealed_memtables.iter() {
            for entry in memtable.iter() {
                if entry.key.seqno >= target_seqno && entry.key.seqno <= end_seqno {
                    events.push(Self::map_event(entry, version, &resolve_indirection)?);
                }
            }
        }
        for table in version.iter_tables() {
            // `scan_seqno_range` upper bound is exclusive; `end_seqno` is the
            // inclusive max, so add one (saturating for the MAX edge).
            for entry in table.scan_seqno_range(target_seqno, end_seqno.saturating_add(1))? {
                events.push(Self::map_event(entry, version, &resolve_indirection)?);
            }
        }

        // Range tombstones from the same sources, carrying their own seqno.
        let mut push_range_tombstone = |rt: &RangeTombstone| {
            if rt.seqno >= target_seqno && rt.seqno <= end_seqno {
                events.push(ScanSinceEvent::RangeTombstone {
                    start_key: rt.start.clone(),
                    end_key: rt.end.clone(),
                    seqno: rt.seqno,
                });
            }
        };
        for rt in super_version.active_memtable.range_tombstones_sorted() {
            push_range_tombstone(&rt);
        }
        for memtable in super_version.sealed_memtables.iter() {
            for rt in memtable.range_tombstones_sorted() {
                push_range_tombstone(&rt);
            }
        }
        for table in version.iter_tables() {
            for rt in table.range_tombstones() {
                push_range_tombstone(rt);
            }
        }

        // Replay order is increasing seqno across every source.
        events.sort_by_key(ScanSinceEvent::seqno);

        Ok(events.into_iter())
    }

    /// Iterate change events with `seqno >= target_seqno`.
    ///
    /// Returns every change committed at or after `target_seqno` as a stream
    /// of [`ScanSinceEvent`]s in increasing seqno order. This is the canonical
    /// change-data-capture primitive: a downstream consumer (replica, Kafka
    /// connector, Debezium-style pipeline) replays the events in order to
    /// reconstruct the source's history. Superseded versions are not collapsed
    /// (a key written three times after the target yields three events).
    ///
    /// # Block-skip
    ///
    /// On SSTs written in the seqno-bounded index format (`seqno_in_index`),
    /// data blocks whose `seqno_max < target_seqno` are skipped without being
    /// read; legacy SSTs are read and filtered per entry, so mixed-format trees
    /// are handled transparently.
    ///
    /// # KV-separation
    ///
    /// Standard trees never store blob-indirected values. On the inner tree of
    /// a KV-separated (blob) tree this returns an `Err` for indirected entries:
    /// blob resolution into [`ScanSinceEvent::Insert`] is provided by the
    /// blob-tree scan path, which owns the blob files.
    ///
    /// # Panics
    ///
    /// Panics if the internal version-history lock is poisoned.
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading the index or a data block fails, or if an entry
    /// is a KV-separated value (see above).
    pub fn scan_since_seqno(
        &self,
        target_seqno: SeqNo,
    ) -> crate::Result<impl Iterator<Item = ScanSinceEvent> + use<>> {
        // A standard tree never stores blob-indirected values; the resolver
        // errors so an indirected entry (only reachable via a blob tree's inner
        // index) surfaces as a clear error rather than a wrong event.
        self.scan_since_seqno_with(target_seqno, |_version, _entry| {
            Err(crate::Error::FeatureUnsupported(
                "scan_since_seqno on KV-separated values requires the blob-tree scan path",
            ))
        })
    }

    /// Update the live [`crate::runtime_config::RuntimeConfig`].
    ///
    /// Mutator runs on a clone of the current snapshot; the new snapshot
    /// is then atomically swapped in. Subsequent calls to
    /// [`Self::runtime_config`] observe the new snapshot.
    ///
    /// ## Current scope
    ///
    /// This API ships the snapshot + atomic-swap mechanism. No write
    /// path in the current tree consults `runtime_config` yet — that
    /// wiring lands with the V5-batch format features (manifest
    /// hardening, per-KV protection, scan-since-seqno) which extend
    /// [`RuntimeConfig`](crate::runtime_config::RuntimeConfig) with
    /// their own fields and read it at block write / manifest commit /
    /// compaction boundaries.
    ///
    /// ## Designed semantics (effective once wired by V5 features)
    ///
    /// - Subsequent write paths load the new snapshot lockless on their
    ///   next operation.
    /// - Existing on-disk data remains in its original format and reads
    ///   transparently — every block / manifest is self-describing via
    ///   its own header.
    /// - Compaction acts as the live-migration mechanism: source blocks
    ///   are rewritten per the current snapshot over subsequent cycles,
    ///   so all data converges to the current settings without
    ///   stop-the-world coordination.
    ///
    /// ## Concurrency
    ///
    /// **Reader atomicity:** concurrent readers observe either the old
    /// or the new snapshot, never a torn intermediate state.
    ///
    /// **Writer semantics: last-writer-wins.** Two `update` calls racing
    /// from the same starting snapshot will have the second `store`
    /// overwrite the first — the first writer's mutation is lost. There
    /// is no CAS / RCU merge. Callers that need lost-update avoidance
    /// (e.g. two threads concurrently toggling different fields) MUST
    /// serialize their `update_runtime_config` calls, typically via a
    /// `Mutex` around the call site.
    /// # Errors
    ///
    /// Returns [`crate::Error::PageEccUnsupported`] when the mutator
    /// leaves `page_ecc = true` on a binary built without the
    /// `page_ecc` cargo feature. The live snapshot stays at its
    /// pre-mutation value on error.
    pub fn update_runtime_config<F>(&self, mutator: F) -> crate::Result<()>
    where
        F: FnOnce(&mut crate::runtime_config::RuntimeConfig),
    {
        // Route through the validating handle path so an invalid
        // mutation (currently: `page_ecc = true` on a non-`page_ecc`
        // build) is rejected at update time, not silently swallowed
        // at the next manifest write.
        // Capture this update's `auto_heal` inside the mutation so the read-path
        // heal gate reflects exactly the config THIS call commits, rather than a
        // separate `load_full()` that could observe a different concurrent
        // update's value. Concurrent `update_runtime_config` calls must be
        // serialized by the caller (see the last-writer-wins note above); under
        // that contract the gate and the committed config stay in sync. On a
        // validation error `try_update` does not commit and `?` returns before
        // the gate is touched, so it keeps tracking the unchanged config.
        let mut auto_heal = false;
        self.0.runtime_config.try_update(|c| {
            mutator(c);
            auto_heal = c.auto_heal;
        })?;
        self.0.heal_hints.set_enabled(auto_heal);
        Ok(())
    }

    /// Snapshot of the current runtime config. Cheap atomic load —
    /// safe to call on hot paths.
    #[must_use]
    pub fn runtime_config(&self) -> Arc<crate::runtime_config::RuntimeConfig> {
        self.0.runtime_config.load_full()
    }

    /// Shared handle to this tree's ECC heal-hint queue.
    ///
    /// A read that recovers a block from Page-ECC parity records the owning SST
    /// here (when the on-disk fault is confirmed persistent). Pass the handle to
    /// [`compaction::EccHeal`](crate::compaction::EccHeal) and run that strategy
    /// via [`Tree::compact`](crate::AbstractTree::compact) — leader-only in a
    /// clustered deployment — to rewrite the flagged SSTs clean. Check
    /// [`HealHints::is_empty`](crate::heal_hints::HealHints::is_empty) to skip
    /// the pass when nothing is queued.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lsm_tree::{AbstractTree, AnyTree, Config, SequenceNumberCounter, compaction::EccHeal};
    /// use std::sync::Arc;
    /// # fn main() -> lsm_tree::Result<()> {
    /// let AnyTree::Standard(tree) = Config::new(
    ///     "/tmp/db",
    ///     SequenceNumberCounter::default(),
    ///     SequenceNumberCounter::default(),
    /// )
    /// .open()?
    /// else {
    ///     return Ok(());
    /// };
    ///
    /// // Opt into rewrite scheduling; reads that recover a block from parity now
    /// // flag its SST for healing.
    /// tree.update_runtime_config(|c| c.auto_heal = true)?;
    ///
    /// // Drain the queue, rewriting each flagged SST clean (leader-only in a
    /// // clustered deployment).
    /// let hints = tree.heal_hints();
    /// while !hints.is_empty() {
    ///     tree.compact(Arc::new(EccHeal::new(tree.heal_hints(), u64::MAX)), 0)?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn heal_hints(&self) -> Arc<crate::heal_hints::HealHints> {
        Arc::clone(&self.0.heal_hints)
    }

    /// Shared point-read logic for `get()` and `multi_get()`: finds the newest
    /// entry, applies merge resolution or RT suppression, and returns the value.
    fn resolve_or_passthrough(
        super_version: &SuperVersion,
        key: &[u8],
        seqno: SeqNo,
        merge_operator: Option<&Arc<dyn crate::merge_operator::MergeOperator>>,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Option<UserValue>> {
        let entry = Self::get_value(super_version, key, seqno, comparator)?;

        match entry {
            Some((ValueType::MergeOperand, entry_seqno, value)) => {
                if let Some(merge_op) = merge_operator {
                    // Build a bloom-filtered single-key iterator pipeline that
                    // reuses MvccStream for merge/RT/Indirection resolution,
                    // eliminating the previous hand-rolled merge collection.
                    Self::resolve_merge_via_pipeline(
                        super_version.clone(),
                        key,
                        seqno,
                        Arc::clone(merge_op),
                    )
                } else if Self::is_suppressed_by_range_tombstones(
                    super_version,
                    key,
                    entry_seqno,
                    seqno,
                    comparator,
                ) {
                    Ok(None)
                } else {
                    Ok(Some(value))
                }
            }
            Some((_, _, value)) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    /// Shared post-lookup resolution for `get_pinned` and `multi_get`:
    /// tombstone filter, range-tombstone suppression, merge operand resolution.
    /// Returns `None` if entry is tombstoned or suppressed.
    fn resolve_pinned_entry(
        super_version: &SuperVersion,
        key: &[u8],
        entry: InternalValue,
        seqno: SeqNo,
        merge_operator: Option<&Arc<dyn crate::merge_operator::MergeOperator>>,
        comparator: &dyn crate::comparator::UserComparator,
        wrap: impl FnOnce(UserValue) -> crate::PinnableSlice,
    ) -> crate::Result<Option<crate::PinnableSlice>> {
        use crate::PinnableSlice;

        let Some(entry) = ignore_tombstone_value(entry) else {
            return Ok(None);
        };
        if Self::is_suppressed_by_range_tombstones(
            super_version,
            key,
            entry.key.seqno,
            seqno,
            comparator,
        ) {
            return Ok(None);
        }
        if entry.key.value_type == ValueType::MergeOperand
            && let Some(merge_op) = merge_operator
        {
            // Merge resolution always produces Owned (pipeline result).
            return Self::resolve_merge_via_pipeline(
                super_version.clone(),
                key,
                seqno,
                Arc::clone(merge_op),
            )
            .map(|opt| opt.map(PinnableSlice::owned));
        }
        Ok(Some(wrap(entry.value)))
    }

    /// Like [`Tree::resolve_or_passthrough`], but returns a [`PinnableSlice`]
    /// that may keep the decompressed block buffer alive.
    fn resolve_or_passthrough_pinned(
        super_version: &SuperVersion,
        key: &[u8],
        seqno: SeqNo,
        merge_operator: Option<&Arc<dyn crate::merge_operator::MergeOperator>>,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Option<crate::PinnableSlice>> {
        use crate::PinnableSlice;

        // Check memtables first — always Owned
        if let Some(entry) = super_version.active_memtable.get(key, seqno) {
            return Self::resolve_pinned_entry(
                super_version,
                key,
                entry,
                seqno,
                merge_operator,
                comparator,
                PinnableSlice::owned,
            );
        }

        // Sealed memtables — always Owned
        if let Some(entry) =
            Self::get_internal_entry_from_sealed_memtables(super_version, key, seqno)
        {
            return Self::resolve_pinned_entry(
                super_version,
                key,
                entry,
                seqno,
                merge_operator,
                comparator,
                PinnableSlice::owned,
            );
        }

        // Tables — Pinned (value shares decompressed block buffer)
        let key_hash = crate::hash::hash64(key);

        if let Some((entry, block)) = Self::get_internal_entry_with_block_from_tables(
            &super_version.version,
            key,
            seqno,
            key_hash,
            comparator,
        )? {
            return Self::resolve_pinned_entry(
                super_version,
                key,
                entry,
                seqno,
                merge_operator,
                comparator,
                |value| PinnableSlice::pinned(block, value),
            );
        }

        Ok(None)
    }

    /// Like [`Tree::get_internal_entry_from_tables`], but returns the block
    /// along with the entry for pinned zero-copy access.
    fn get_internal_entry_with_block_from_tables(
        version: &Version,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Option<(InternalValue, crate::table::Block)>> {
        Self::find_in_tables::<TableEntryWithBlock>(version, key, seqno, key_hash, comparator)
    }

    /// Resolves merge operands for a point read via a bloom-filtered iterator pipeline.
    ///
    /// Builds a single-key range (`key..=key`) with bloom pre-filtering, wraps
    /// all sources in `Merger → MvccStream`, and takes the first result. This
    /// reuses the unified merge/RT/Indirection resolution logic from `MvccStream`
    /// instead of duplicating it in a hand-rolled collection loop.
    ///
    /// Bloom pre-filtering can reject many disk tables at the filter level,
    /// which typically improves point-read performance on deep LSM trees.
    pub(crate) fn resolve_merge_via_pipeline(
        version: SuperVersion,
        key: &[u8],
        seqno: SeqNo,
        merge_operator: Arc<dyn crate::merge_operator::MergeOperator>,
    ) -> crate::Result<Option<UserValue>> {
        use crate::range::{IterState, TreeIter};

        let key_hash = crate::hash::hash64(key);
        // NOTE: Slice::from(&[u8]) copies the key (small, typically < 100 bytes).
        // This runs once per merge resolution, not per-table — cost is negligible
        // compared to the I/O saved by partition-aware bloom filtering.
        let bloom_key = crate::Slice::from(key);
        let comparator = version.active_memtable.comparator.clone();

        let iter_state = IterState {
            version,
            ephemeral: None,
            merge_operator: Some(merge_operator),
            comparator,
            prefix_hash: None,
            key_hash: Some(key_hash),
            bloom_key: Some(bloom_key),
            #[cfg(feature = "metrics")]
            metrics: None,
        };

        // Point-read fast path: skips eager RT collection, sort+dedup, table-skip,
        // and RangeTombstoneFilter wrapper. MvccStream handles merge-internal RT
        // suppression; a post-merge linear RT check catches the rest.
        let mut iter = TreeIter::create_range_point(iter_state, key, seqno);

        match iter.next() {
            Some(Ok(entry)) => Ok(Some(entry.value)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    #[doc(hidden)]
    pub fn create_internal_range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        version: SuperVersion,
        range: &'a R,
        seqno: SeqNo,
        ephemeral: Option<(Arc<Memtable>, SeqNo)>,
        merge_operator: Option<Arc<dyn crate::merge_operator::MergeOperator>>,
        comparator: crate::comparator::SharedComparator,
    ) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> + 'static {
        Self::create_internal_range_with_prefix_hash(
            version,
            range,
            seqno,
            ephemeral,
            merge_operator,
            comparator,
            None,
        )
    }

    /// Like [`Tree::create_internal_range`], but with an optional prefix hash
    /// for prefix bloom filter skipping during prefix scans.
    #[doc(hidden)]
    pub(crate) fn create_internal_range_with_prefix_hash<
        'a,
        K: AsRef<[u8]> + 'a,
        R: RangeBounds<K> + 'a,
    >(
        version: SuperVersion,
        range: &'a R,
        seqno: SeqNo,
        ephemeral: Option<(Arc<Memtable>, SeqNo)>,
        merge_operator: Option<Arc<dyn crate::merge_operator::MergeOperator>>,
        comparator: crate::comparator::SharedComparator,
        prefix_hash: Option<u64>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> + 'static {
        use crate::range::{IterState, TreeIter};
        use core::ops::Bound::{self, Excluded, Included, Unbounded};

        let lo: Bound<UserKey> = match range.start_bound() {
            Included(x) => Included(x.as_ref().into()),
            Excluded(x) => Excluded(x.as_ref().into()),
            Unbounded => Unbounded,
        };

        let hi: Bound<UserKey> = match range.end_bound() {
            Included(x) => Included(x.as_ref().into()),
            Excluded(x) => Excluded(x.as_ref().into()),
            Unbounded => Unbounded,
        };

        let bounds: (Bound<UserKey>, Bound<UserKey>) = (lo, hi);

        let iter_state = IterState {
            version,
            ephemeral,
            merge_operator,
            comparator,
            prefix_hash,
            key_hash: None,
            bloom_key: None,
            #[cfg(feature = "metrics")]
            metrics: None,
        };

        TreeIter::create_range(iter_state, bounds, seqno)
    }

    pub(crate) fn get_internal_entry_from_version(
        super_version: &SuperVersion,
        key: &[u8],
        seqno: SeqNo,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Option<InternalValue>> {
        // Search order: active → sealed → SST (newest first). A point
        // tombstone in a newer source is authoritative — no older source
        // can contain a newer value, so returning None is correct.
        if let Some(entry) = super_version.active_memtable.get(key, seqno) {
            let Some(entry) = ignore_tombstone_value(entry) else {
                return Ok(None);
            };

            // Check if any range tombstone suppresses this entry
            if Self::is_suppressed_by_range_tombstones(
                super_version,
                key,
                entry.key.seqno,
                seqno,
                comparator,
            ) {
                return Ok(None);
            }
            return Ok(Some(entry));
        }

        // Now look in sealed memtables
        if let Some(entry) =
            Self::get_internal_entry_from_sealed_memtables(super_version, key, seqno)
        {
            let Some(entry) = ignore_tombstone_value(entry) else {
                return Ok(None);
            };

            if Self::is_suppressed_by_range_tombstones(
                super_version,
                key,
                entry.key.seqno,
                seqno,
                comparator,
            ) {
                return Ok(None);
            }
            return Ok(Some(entry));
        }

        // Now look in tables... this may involve disk I/O
        let entry =
            Self::get_internal_entry_from_tables(&super_version.version, key, seqno, comparator)?;

        if let Some(entry) = entry {
            if Self::is_suppressed_by_range_tombstones(
                super_version,
                key,
                entry.key.seqno,
                seqno,
                comparator,
            ) {
                return Ok(None);
            }
            return Ok(Some(entry));
        }

        Ok(None)
    }

    /// Value-only mirror of [`Self::get_internal_entry_from_version`].
    ///
    /// Returns `(value_type, seqno, value)` for the newest visible entry without
    /// reconstructing the entry key. Same search order (active -> sealed -> SST,
    /// newest first), tombstone filtering, and range-tombstone suppression; only
    /// the SST path differs, using the value-only [`TableValue`] lookup that
    /// skips the delta-key fusion of the full `InternalValue` path. Used by the
    /// value-returning `get` path, which never reads the matched key.
    pub(crate) fn get_value(
        super_version: &SuperVersion,
        key: &[u8],
        seqno: SeqNo,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Option<(ValueType, SeqNo, crate::Slice)>> {
        if let Some(entry) = super_version.active_memtable.get(key, seqno) {
            let Some(entry) = ignore_tombstone_value(entry) else {
                return Ok(None);
            };
            if Self::is_suppressed_by_range_tombstones(
                super_version,
                key,
                entry.key.seqno,
                seqno,
                comparator,
            ) {
                return Ok(None);
            }
            return Ok(Some((entry.key.value_type, entry.key.seqno, entry.value)));
        }

        if let Some(entry) =
            Self::get_internal_entry_from_sealed_memtables(super_version, key, seqno)
        {
            let Some(entry) = ignore_tombstone_value(entry) else {
                return Ok(None);
            };
            if Self::is_suppressed_by_range_tombstones(
                super_version,
                key,
                entry.key.seqno,
                seqno,
                comparator,
            ) {
                return Ok(None);
            }
            return Ok(Some((entry.key.value_type, entry.key.seqno, entry.value)));
        }

        let key_hash = crate::hash::hash64(key);
        let entry = Self::find_in_tables::<TableValue>(
            &super_version.version,
            key,
            seqno,
            key_hash,
            comparator,
        )?;
        if let Some((value_type, entry_seqno, value)) = entry {
            if Self::is_suppressed_by_range_tombstones(
                super_version,
                key,
                entry_seqno,
                seqno,
                comparator,
            ) {
                return Ok(None);
            }
            return Ok(Some((value_type, entry_seqno, value)));
        }

        Ok(None)
    }

    /// Checks if a key at `key_seqno` is suppressed by any range tombstone
    /// in the active memtable, sealed memtables, or SST tables, visible at `read_seqno`.
    pub(crate) fn is_suppressed_by_range_tombstones(
        super_version: &SuperVersion,
        key: &[u8],
        key_seqno: SeqNo,
        read_seqno: SeqNo,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> bool {
        // Check active memtable range tombstones.
        // Future optimization: skip lock when memtable has no RTs (atomic count).
        if super_version
            .active_memtable
            .is_key_suppressed_by_range_tombstone(key, key_seqno, read_seqno)
        {
            return true;
        }

        // Check sealed memtable range tombstones
        for mt in super_version.sealed_memtables.iter().rev() {
            if mt.is_key_suppressed_by_range_tombstone(key, key_seqno, read_seqno) {
                return true;
            }
        }

        // Check SST table range tombstones.
        //
        // Per-table RT lists are sorted by start key (using comparator) on load,
        // so binary search narrows candidates to RTs with start <= key.
        // The key_range early reject uses the comparator so it works with
        // non-lexicographic orderings.
        for table in super_version
            .version
            .iter_levels()
            .flat_map(|lvl| lvl.iter())
            .flat_map(|run| run.iter())
            .filter(|t| !t.range_tombstones().is_empty())
            .filter(|t| {
                // Early reject: skip tables whose key range doesn't contain the key.
                let kr = &t.metadata.key_range;
                comparator.compare(kr.min(), key) != core::cmp::Ordering::Greater
                    && comparator.compare(key, kr.max()) != core::cmp::Ordering::Greater
            })
        {
            let rts = table.range_tombstones();

            // Binary search: find the first RT whose start is > key (in comparator order).
            // All RTs before that index have start <= key and are candidates.
            let candidate_end = rts.partition_point(|rt| {
                comparator.compare(&rt.start, key) != core::cmp::Ordering::Greater
            });

            for rt in rts.iter().take(candidate_end) {
                // Check: start <= key < end (in comparator order) AND seqno visibility.
                if rt.visible_at(read_seqno)
                    && comparator.compare(&rt.start, key) != core::cmp::Ordering::Greater
                    && comparator.compare(key, &rt.end) == core::cmp::Ordering::Less
                    && key_seqno < rt.seqno
                {
                    return true;
                }
            }
        }

        false
    }

    /// Resolves a single internal entry into a user value, handling tombstones,
    /// range tombstone suppression, and merge operand resolution.
    /// Resolves an entry for `multi_get`: tombstone filter, RT suppression,
    /// merge operand resolution. Delegates to [`resolve_pinned_entry`] with
    /// `Owned` wrapping, then extracts the value.
    fn resolve_entry(
        super_version: &SuperVersion,
        key: &[u8],
        entry: Option<InternalValue>,
        seqno: SeqNo,
        merge_operator: Option<&Arc<dyn crate::merge_operator::MergeOperator>>,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Option<UserValue>> {
        let Some(entry) = entry else {
            return Ok(None);
        };
        Self::resolve_pinned_entry(
            super_version,
            key,
            entry,
            seqno,
            merge_operator,
            comparator,
            crate::PinnableSlice::owned,
        )
        .map(|opt| opt.map(crate::PinnableSlice::into_value))
    }

    /// Queries tables for multiple keys using sorted access order.
    ///
    /// `miss_keys` contains `(key_index, bloom_hash)` pairs for keys not yet
    /// found, in comparator-sorted order. Keys are looked up individually via
    /// `Table::get`, but sorted order improves I/O locality. The precomputed
    /// bloom hash in each pair is reused across all table probes. Per-SST
    /// batched bloom checks and block walks are tracked in `#223`.
    #[expect(
        clippy::indexing_slicing,
        reason = "miss_keys entries carry batch-local indices; callers must pass a results slice aligned with keys"
    )]
    pub(crate) fn batch_get_from_tables<K: AsRef<[u8]>>(
        version: &Version,
        keys: &[K],
        miss_keys: Vec<(usize, u64)>,
        seqno: SeqNo,
        comparator: &dyn crate::comparator::UserComparator,
        results: &mut [Option<InternalValue>],
    ) -> crate::Result<()> {
        debug_assert_eq!(results.len(), keys.len());
        debug_assert!(miss_keys.iter().all(|&(i, _)| i < keys.len()));

        // Consume the caller's Vec directly — no allocation+copy.
        let mut still_remaining = miss_keys;

        for (level_idx, level) in version.iter_levels().enumerate() {
            if still_remaining.is_empty() {
                break;
            }

            if level_idx == 0 {
                // L0: must check ALL runs, keep highest seqno per key.
                // Track keys at the seqno ceiling (seqno + 1 == read_seqno) —
                // no other L0 run can beat them, so skip in subsequent runs.
                // Bitmap: idx is always in 0..keys.len(), dense enough for Vec<bool>.
                let mut at_ceiling = vec![false; keys.len()];

                for run in level.iter() {
                    for &(idx, hash) in &still_remaining {
                        if at_ceiling[idx] {
                            continue;
                        }
                        let key = keys[idx].as_ref();
                        if let Some(table) = run.get_for_key_cmp(key, comparator)
                            && let Some(item) = table.get(key, seqno, hash)?
                        {
                            match &results[idx] {
                                Some(current) if current.key.seqno >= item.key.seqno => {}
                                _ => {
                                    if item.key.seqno.checked_add(1) == Some(seqno) {
                                        at_ceiling[idx] = true;
                                    }
                                    results[idx] = Some(item);
                                }
                            }
                        }
                    }
                }

                // Remove found keys (both values and tombstones)
                still_remaining.retain(|&(idx, _)| results[idx].is_none());
            } else {
                // L1+ runs have non-overlapping key ranges within a level.
                // Once get_for_key_cmp identifies a covering run for a key,
                // no other run in this level can contain it. We split into:
                // - `not_covered`: key range didn't match any run yet → try next run
                // - `covered_miss`: covering run found but table.get returned None
                //   (bloom false negative or key absent) → skip remaining runs in
                //   this level, but keep for lower levels
                let mut covered_miss: Vec<(usize, u64)> = Vec::new();

                for run in level.iter() {
                    let mut not_covered = Vec::with_capacity(still_remaining.len());
                    for &(idx, hash) in &still_remaining {
                        let key = keys[idx].as_ref();
                        if let Some(table) = run.get_for_key_cmp(key, comparator) {
                            if let Some(item) = table.get(key, seqno, hash)? {
                                results[idx] = Some(item);
                            } else {
                                // Covering run found, but key not present — no other
                                // run in this level can have it. Keep for lower levels.
                                covered_miss.push((idx, hash));
                            }
                        } else {
                            not_covered.push((idx, hash));
                        }
                    }
                    still_remaining = not_covered;
                }

                // Merge back: keys without a covering run + keys with a covering
                // miss both proceed to lower levels. Re-sort to preserve
                // comparator order for the next level's sequential scan.
                let needs_sort = !covered_miss.is_empty();
                still_remaining.extend(covered_miss);
                if needs_sort {
                    still_remaining.sort_by(|&(a, _), &(b, _)| {
                        comparator.compare(keys[a].as_ref(), keys[b].as_ref())
                    });
                }
            }
        }

        Ok(())
    }

    fn get_internal_entry_from_tables(
        version: &Version,
        key: &[u8],
        seqno: SeqNo,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Option<InternalValue>> {
        let key_hash = crate::hash::hash64(key);
        Self::find_in_tables::<TableEntry>(version, key, seqno, key_hash, comparator)
    }

    /// Generic level-walk for point reads, monomorphized over the lookup result type.
    ///
    /// L0: check ALL runs, keep highest seqno (runs may not be newest-first).
    /// L1+: at most one run contains the key — return on first match.
    /// Once a level yields a match, lower levels cannot have newer data.
    fn find_in_tables<T: TablePointLookup>(
        version: &Version,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Option<T>> {
        for (level_idx, level) in version.iter_levels().enumerate() {
            if level_idx == 0 {
                let mut best: Option<T> = None;

                for run in level.iter() {
                    if let Some(table) = run.get_for_key_cmp(key, comparator)
                        && let Some(item) = T::lookup(table, key, seqno, key_hash)?
                    {
                        match &best {
                            Some(current) if current.entry_seqno() >= item.entry_seqno() => {}
                            _ => {
                                // Short-circuit: point reads use exclusive upper bound,
                                // so the highest visible seqno is read_seqno - 1.
                                // If matched, no other L0 run can have a higher one.
                                if item.entry_seqno().checked_add(1) == Some(seqno) {
                                    return Ok(item.filter_tombstone());
                                }
                                best = Some(item);
                            }
                        }
                    }
                }

                if let Some(entry) = best {
                    return Ok(entry.filter_tombstone());
                }
            } else {
                // L1+ runs have non-overlapping key ranges. Once we find the
                // covering run (get_for_key_cmp returns Some), no other run in
                // this level can contain the key — break regardless of hit/miss.
                for run in level.iter() {
                    if let Some(table) = run.get_for_key_cmp(key, comparator) {
                        if let Some(item) = T::lookup(table, key, seqno, key_hash)? {
                            return Ok(item.filter_tombstone());
                        }
                        break;
                    }
                }
            }
        }

        Ok(None)
    }

    pub(crate) fn get_internal_entry_from_sealed_memtables(
        super_version: &SuperVersion,
        key: &[u8],
        seqno: SeqNo,
    ) -> Option<InternalValue> {
        for mt in super_version.sealed_memtables.iter().rev() {
            if let Some(entry) = mt.get(key, seqno) {
                return Some(entry);
            }
        }

        None
    }

    pub(crate) fn get_version_for_snapshot(&self, seqno: SeqNo) -> SuperVersion {
        self.version_history.read().get_version_for_snapshot(seqno)
    }

    /// Normalizes a user-provided range into owned `Bound<Slice>` values.
    ///
    /// Returns a tuple containing:
    /// - the `OwnedBounds` that mirror the original bounds semantics (including
    ///   inclusive/exclusive markers and unbounded endpoints), and
    /// - a `bool` flag indicating whether the normalized range is logically
    ///   empty (e.g., when the lower bound is greater than the upper bound).
    ///
    /// Callers can use the flag to detect empty ranges and skip further work
    /// while still having access to the normalized bounds for non-empty cases.
    fn range_bounds_to_owned_bounds<K: AsRef<[u8]>, R: RangeBounds<K>>(
        range: &R,
    ) -> (OwnedBounds, bool) {
        use Bound::{Excluded, Included, Unbounded};

        let start = match range.start_bound() {
            Included(key) => Included(Slice::from(key.as_ref())),
            Excluded(key) => Excluded(Slice::from(key.as_ref())),
            Unbounded => Unbounded,
        };

        let end = match range.end_bound() {
            Included(key) => Included(Slice::from(key.as_ref())),
            Excluded(key) => Excluded(Slice::from(key.as_ref())),
            Unbounded => Unbounded,
        };

        let is_empty =
            if let (Included(lo) | Excluded(lo), Included(hi) | Excluded(hi)) = (&start, &end) {
                lo.as_ref() > hi.as_ref()
            } else {
                false
            };

        (OwnedBounds { start, end }, is_empty)
    }

    /// Opens an LSM-tree in the given directory.
    ///
    /// Will recover previous state if the folder was previously
    /// occupied by an LSM-tree, including the previous configuration.
    /// If not, a new tree will be initialized with the given config.
    ///
    /// After recovering a previous state, use [`Tree::set_active_memtable`]
    /// to fill the memtable with data from a write-ahead log for full durability.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub(crate) fn open(config: Config) -> crate::Result<Self> {
        log::debug!("Opening LSM-tree at {}", config.path.display());

        // Resolve the per-tree compaction compression pool once, at open: if the
        // caller supplied no shared pool but asked for >1 thread, build the
        // default rayon-backed pool now so every compaction reuses it (building
        // a pool per compaction would spawn threads on each run). A caller-
        // supplied pool is left untouched. Shadowed under `parallel` only, so
        // non-parallel builds don't carry an unused `mut`.
        #[cfg(feature = "parallel")]
        let config = {
            let mut config = config;
            if config.compaction_pool.is_none() && config.compaction_threads > 1 {
                config.compaction_pool = Some(Arc::new(
                    crate::table::writer::RayonSpawner::with_threads(config.compaction_threads)?,
                ));
            }
            config
        };

        // Gate on the `page_ecc` cargo feature: caller asked for ECC
        // but the build does not link the Reed-Solomon codec. We have
        // no way to verify or recover RS parity without the codec, so
        // refuse to open rather than silently downgrade integrity.
        // Two surfaces to check:
        //   - `Config::page_ecc(true)`  → SST data-block ECC
        //   - `Config::with_runtime_config(RuntimeConfig { page_ecc: true, .. })`
        //     → manifest-Block ECC (consumed by manifest_blocks::writer)
        // Both silently no-op without the feature; refusing here is
        // the only place callers see a typed error.
        if (config.page_ecc || config.initial_runtime_config.page_ecc)
            && !cfg!(feature = "page_ecc")
        {
            return Err(crate::Error::PageEccUnsupported);
        }

        // Acquire the cross-process directory lock BEFORE any manifest access
        // (the `CURRENT` probe + `has_existing_version_state` check below, and
        // the recover / create paths). Acquiring it here makes `open()`
        // exclusive end-to-end: a concurrent opener fails fast with
        // `Error::Locked` instead of racing through the probe and observing a
        // peer's half-created directory (which would surface as the InvalidData
        // "half-written checkpoint" path rather than `Locked`). The `LOCK` file
        // needs its directory to exist, so create the root directory first
        // (idempotent; `create_new` re-creates the `tables/` subtree). The lock
        // is threaded into the constructor so it lives for the tree's lifetime.
        #[cfg(feature = "std")]
        let directory_lock = {
            config.fs.create_dir_all(&config.path)?;
            crate::config::acquire_directory_lock(&*config.fs, &config.path, config.directory_lock)?
        };

        // Check for old version
        if config.fs.exists(&config.path.join("version"))? {
            log::error!(
                "It looks like you are trying to open a V1 database - the database needs a manual migration, however a migration tool is not provided, as V1 is extremely outdated."
            );
            return Err(crate::Error::InvalidVersion(FormatVersion::V1.into()));
        }

        // Decide between recovery and fresh creation atomically by attempting
        // to read the CURRENT version file. This avoids a TOCTOU race that
        // would occur if we probed with exists() first.
        let tree = match crate::version::recovery::get_current_version(
            &config.path,
            &*config.fs,
            config.encryption.clone(),
        ) {
            Ok(_) => Self::recover(
                config,
                #[cfg(feature = "std")]
                directory_lock,
            ),
            Err(crate::Error::Io(e)) if e.kind() == crate::io::ErrorKind::NotFound => {
                // Missing CURRENT MUST coincide with a directory that
                // has no version artifacts; otherwise we are looking at
                // a half-written checkpoint (or other interrupted
                // sealing). Silently calling `create_new` in that case
                // would overwrite the partial state with an empty tree,
                // turning a recoverable failure into data loss.
                if has_existing_version_state(&config.path, &*config.fs)? {
                    // Return Error::Io(InvalidData, ...) rather than
                    // Error::Unrecoverable so callers that don't read
                    // logs still get a programmatic surface with the
                    // path and remediation hint embedded. `log::error!`
                    // stays for human ops who DO watch logs and want
                    // the full context at the moment of failure (the
                    // structured error is what propagates up the call
                    // chain; the log line records the diagnosis next
                    // to the timestamp).
                    let msg = format!(
                        "Tree::open: refusing to recover {} — `current` pointer is missing \
                         but the directory still holds version artifacts (tables/, blobs/, \
                         or vN). This is the on-disk signature of a half-written checkpoint \
                         or interrupted sealing. Remove the partial directory and retry the \
                         checkpoint, or restore `current` from a backup before reopening.",
                        config.path.display(),
                    );
                    log::error!("{msg}");
                    return Err(crate::Error::from(crate::io::Error::new(
                        crate::io::ErrorKind::InvalidData,
                        msg,
                    )));
                }
                Self::create_new(
                    config,
                    #[cfg(feature = "std")]
                    directory_lock,
                )
            }
            Err(e) => Err(e),
        }?;

        Ok(tree)
    }

    /// Returns `true` if there are some tables that are being compacted.
    #[doc(hidden)]
    #[must_use]
    pub fn is_compacting(&self) -> bool {
        !self.compaction_state.lock().hidden_set().is_empty()
    }

    fn inner_compact(
        &self,
        strategy: Arc<dyn CompactionStrategy>,
        mvcc_gc_watermark: SeqNo,
    ) -> crate::Result<crate::compaction::CompactionResult> {
        use crate::compaction::worker::{Options, do_compaction};

        let mut opts = Options::from_tree(self, strategy);
        opts.mvcc_gc_watermark = mvcc_gc_watermark;

        let result = do_compaction(&opts)?;

        log::debug!("Compaction run over");

        Ok(result)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn create_iter(
        &self,
        seqno: SeqNo,
        ephemeral: Option<(Arc<Memtable>, SeqNo)>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.create_range::<UserKey, _>(&.., seqno, ephemeral)
    }

    #[doc(hidden)]
    pub fn create_range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &self,
        range: &'a R,
        seqno: SeqNo,
        ephemeral: Option<(Arc<Memtable>, SeqNo)>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        let super_version = self.version_history.read().get_version_for_snapshot(seqno);

        Self::create_internal_range(
            super_version,
            range,
            seqno,
            ephemeral,
            self.config.merge_operator.clone(),
            self.config.comparator.clone(),
        )
        .map(|item| match item {
            Ok(kv) => Ok((kv.key.user_key, kv.value)),
            Err(e) => Err(e),
        })
    }

    #[doc(hidden)]
    pub fn create_prefix<'a, K: AsRef<[u8]> + 'a>(
        &self,
        prefix: K,
        seqno: SeqNo,
        ephemeral: Option<(Arc<Memtable>, SeqNo)>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        use crate::prefix::compute_prefix_hash;
        use crate::range::{IterState, TreeIter, prefix_to_range};

        let prefix_bytes = prefix.as_ref();

        let prefix_hash = compute_prefix_hash(self.config.prefix_extractor.as_ref(), prefix_bytes);

        let range = prefix_to_range(prefix_bytes);

        let super_version = self.version_history.read().get_version_for_snapshot(seqno);

        let iter_state = IterState {
            version: super_version,
            ephemeral,
            merge_operator: self.config.merge_operator.clone(),
            comparator: self.config.comparator.clone(),
            prefix_hash,
            key_hash: None,
            bloom_key: None,
            #[cfg(feature = "metrics")]
            metrics: Some(self.0.metrics.clone()),
        };

        TreeIter::create_range(iter_state, range, seqno).map(|item| match item {
            Ok(kv) => Ok((kv.key.user_key, kv.value)),
            Err(e) => Err(e),
        })
    }

    /// Adds an item to the active memtable.
    ///
    /// Returns the added item's size and new size of the memtable.
    #[doc(hidden)]
    #[must_use]
    pub fn append_entry(&self, value: InternalValue) -> (u64, u64) {
        self.version_history
            .read()
            .latest_version()
            .active_memtable
            .insert(value)
    }

    /// Adds multiple items to the active memtable in bulk.
    ///
    /// Acquires the version-history lock once and delegates to
    /// [`Memtable::insert_batch`] for batch size accounting.
    ///
    /// Returns the total bytes added and new size of the memtable.
    #[doc(hidden)]
    #[must_use]
    pub(crate) fn append_batch(&self, items: Vec<InternalValue>) -> (u64, u64) {
        // Hold the read guard for the entire insert to prevent rotate_memtable()
        // from sealing this memtable mid-batch (which could cause data loss if
        // a concurrent flush persists only a prefix of the batch).
        self.version_history
            .read()
            .latest_version()
            .active_memtable
            .insert_batch(items)
    }

    /// Recovers previous state, by loading the level manifest, tables and blob files.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    #[expect(
        clippy::too_many_lines,
        reason = "Tree::recover threads the whole open sequence (CURRENT validation, \
                  Manifest decode, encryption + runtime plumbing, version recovery, \
                  TreeInner assembly) — splitting it would create helper functions whose \
                  only caller is this one site"
    )]
    fn recover(
        mut config: Config,
        // The cross-process directory lock acquired by `Tree::open` before the
        // manifest probe; held for the tree's lifetime via
        // `TreeInner::_directory_lock`.
        #[cfg(feature = "std")] directory_lock: Option<Box<dyn crate::fs::FsFile>>,
    ) -> crate::Result<Self> {
        use crate::stop_signal::StopSignal;
        use inner::get_next_tree_id;

        log::info!("Recovering LSM-tree at {}", config.path.display());

        // Validate manifest metadata (format version, comparator name)
        // BEFORE recover_levels, so a rejected open is side-effect free
        // — recover_levels loads tables and cleans up orphans.
        // Tree type is checked after recovery (needs the Version object).
        // NOTE: the version file is read twice (here for metadata, then inside
        // recover_levels for table/blob data). This is intentional — metadata
        // validation must complete before any disk-mutating recovery work.
        // Version id of the on-disk snapshot CURRENT references. This is the
        // base the edit log replays on top of; the live version id can be higher
        // (it has no `v{id}` file of its own). Threaded into the version history
        // so the next persist appends to / rotates the right snapshot's log.
        let snapshot_id = crate::version::recovery::get_current_version(
            &config.path,
            &*config.fs,
            config.encryption.clone(),
        )?;
        {
            let version_id = snapshot_id;
            let manifest_path = config.path.join(format!("v{version_id}"));
            // Open the manifest with a default runtime snapshot:
            // ECC awareness is captured per-Block via the header
            // (`ECC_PARITY` flag) so the reader doesn't actually
            // need to know which ECC mode the writer used. The
            // captured runtime here is a placeholder; once we want
            // runtime-driven decisions on the read path (e.g.
            // checksum_algo dispatch per #298) we'll seed it from
            // Config + persisted format-version fields.
            let mut archive_reader = crate::manifest_blocks::reader::ManifestArchiveReader::open(
                &manifest_path,
                &*config.fs,
                alloc::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
                config.encryption.clone(),
            )?;
            let manifest = Manifest::decode_from(&mut archive_reader)?;

            if !matches!(manifest.version, FormatVersion::V5) {
                return Err(crate::Error::InvalidVersion(manifest.version.into()));
            }

            let supplied_name = config.comparator.name();
            if manifest.comparator_name != supplied_name {
                log::warn!(
                    "Comparator mismatch: tree was created with {:?} but opened with {:?}",
                    manifest.comparator_name,
                    supplied_name,
                );
                return Err(crate::Error::ComparatorMismatch {
                    stored: manifest.comparator_name,
                    supplied: supplied_name,
                });
            }

            // IMPORTANT: Restore persisted config
            config.level_count = manifest.level_count;
        }

        let tree_id = get_next_tree_id();

        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let version = Self::recover_levels(
            &config.path,
            tree_id,
            &config,
            #[cfg(feature = "metrics")]
            &metrics,
        )?;

        {
            let requested_tree_type = match config.kv_separation_opts {
                Some(_) => crate::TreeType::Blob,
                None => crate::TreeType::Standard,
            };

            if version.tree_type() != requested_tree_type {
                log::error!(
                    "Tried to open a {requested_tree_type:?}Tree, but the existing tree is of type {:?}Tree. This indicates a misconfiguration or corruption.",
                    version.tree_type(),
                );
                return Err(crate::Error::Unrecoverable);
            }
        }

        let highest_table_id = version
            .iter_tables()
            .map(Table::id)
            .max()
            .unwrap_or_default();

        let comparator = config.comparator.clone();

        let deletion_pause = crate::deletion_pause::DeletionPause::new_shared();
        #[cfg(feature = "std")]
        let background_deleter = Arc::new(crate::BackgroundDeleter::new(None));
        let heal_hints =
            crate::heal_hints::HealHints::new_shared(config.initial_runtime_config.auto_heal);

        // Clone the seed snapshot BEFORE moving config into the Arc
        // below — the runtime handle initializer needs it after the
        // move.
        let initial_runtime = config.initial_runtime_config.clone();
        let sync_mode = config.sync_mode;
        let super_versions = SuperVersions::new(
            version,
            &comparator,
            sync_mode,
            snapshot_id,
            config.manifest_log_rotate_bytes,
        );
        #[cfg(feature = "std")]
        let latest_super_version = super_versions.latest_handle();
        let inner = TreeInner {
            id: tree_id,
            memtable_id_counter: SequenceNumberCounter::new(1),
            table_id_counter: SequenceNumberCounter::new(highest_table_id + 1),
            blob_file_id_counter: SequenceNumberCounter::default(),
            version_history: Arc::new(RwLock::new(super_versions)),
            #[cfg(feature = "std")]
            latest_super_version,
            stop_signal: StopSignal::default(),
            config: Arc::new(config),
            major_compaction_lock: RwLock::default(),
            flush_lock: Mutex::default(),
            #[cfg(feature = "std")]
            _directory_lock: directory_lock,
            compaction_state: Arc::new(Mutex::new(CompactionState::default())),
            deletion_pause: Arc::clone(&deletion_pause),
            #[cfg(feature = "std")]
            background_deleter: Arc::clone(&background_deleter),
            heal_hints: Arc::clone(&heal_hints),
            runtime_config: Arc::new(crate::runtime_config::handle::RuntimeConfigHandle::new(
                initial_runtime,
            )),

            #[cfg(feature = "metrics")]
            metrics,
        };

        // Install the pause on every recovered table / blob file so their
        // Drop impls consult it when a checkpoint is in flight. Snapshot
        // the Arc handles into owned collections so the read lock is
        // released before iterating (avoids `significant_drop_tightening`).
        // Snapshot the version under the read lock, then drop the lock before
        // collecting so the version_history lock isn't held across the clones.
        let version = inner.version_history.read().latest_version().version;
        let recovered_tables: Vec<Table> = version.iter_tables().cloned().collect();
        let recovered_blobs: Vec<BlobFile> = version.blob_files.iter().cloned().collect();

        for table in &recovered_tables {
            table.install_deletion_pause(Arc::clone(&deletion_pause));
            #[cfg(feature = "std")]
            table.install_background_deleter(Arc::clone(&background_deleter));
            table.install_heal_hints(Arc::clone(&heal_hints));
        }
        for blob_file in &recovered_blobs {
            blob_file.install_deletion_pause(Arc::clone(&deletion_pause));
            #[cfg(feature = "std")]
            blob_file.install_background_deleter(Arc::clone(&background_deleter));
        }

        Ok(Self(Arc::new(inner)))
    }

    /// Creates a new LSM-tree in a directory.
    fn create_new(
        config: Config,
        // The cross-process directory lock acquired by `Tree::open`, held for
        // the tree's lifetime.
        #[cfg(feature = "std")] directory_lock: Option<Box<dyn crate::fs::FsFile>>,
    ) -> crate::Result<Self> {
        use crate::file::fsync_directory;

        let path = config.path.clone();
        log::trace!("Creating LSM-tree at {}", path.display());

        let sync_mode = config.sync_mode;

        (*config.fs).create_dir_all(&path)?;

        // Create tables directories for all configured paths (primary + routes).
        // create_dir_all may create both <route> and <route>/tables.
        // Fsync the tables dir, its parent (route dir), AND the route's parent
        // to make all newly-created directory entries durable on POSIX.
        for (table_folder_path, folder_fs) in config.all_tables_folders() {
            folder_fs.create_dir_all(&table_folder_path)?;
            fsync_directory(&table_folder_path, &*folder_fs, sync_mode)?;
            if let Some(parent) = table_folder_path.parent() {
                fsync_directory(parent, &*folder_fs, sync_mode)?;
                if let Some(grandparent) = parent.parent() {
                    fsync_directory(grandparent, &*folder_fs, sync_mode)?;
                }
            }
        }

        // IMPORTANT: fsync primary folder on Unix
        fsync_directory(&path, &*config.fs, sync_mode)?;

        let inner = TreeInner::create_new(
            config,
            #[cfg(feature = "std")]
            directory_lock,
        )?;
        Ok(Self(Arc::new(inner)))
    }

    /// Recovers the level manifest, loading all tables from disk.
    ///
    /// When [`level_routes`](Config::level_routes) is configured, all
    /// configured table folders are scanned so tables on different storage
    /// tiers are discovered correctly.
    #[expect(
        clippy::too_many_lines,
        reason = "recovery logic is inherently complex"
    )]
    fn recover_levels<P: AsRef<Path>>(
        tree_path: P,
        tree_id: TreeId,
        config: &Config,
        #[cfg(feature = "metrics")] metrics: &Arc<Metrics>,
    ) -> crate::Result<Version> {
        use crate::{TableId, file::fsync_directory};

        let tree_path = tree_path.as_ref();

        let recovery = recover(
            tree_path,
            &*config.fs,
            config.manifest_recovery_mode,
            config.encryption.clone(),
        )?;

        // The on-disk snapshot CURRENT points at — the generation orphan cleanup
        // must preserve. Intermediate versions live only in the edit log, so the
        // latest version id (`version.id()`) has no `v{id}` file of its own.
        let snapshot_id = recovery.snapshot_id;

        let mut table_map = {
            let mut result: crate::HashMap<TableId, (u8 /* Level index */, Checksum, SeqNo)> =
                crate::HashMap::default();

            for (level_idx, table_ids) in recovery.table_ids.iter().enumerate() {
                for run in table_ids {
                    for table in run {
                        #[expect(
                            clippy::expect_used,
                            reason = "there are always less than 256 levels"
                        )]
                        result.insert(
                            table.id,
                            (
                                level_idx
                                    .try_into()
                                    .expect("there are less than 256 levels"),
                                table.checksum,
                                table.global_seqno,
                            ),
                        );
                    }
                }
            }

            result
        };

        let cnt = table_map.len();

        log::debug!("Recovering {cnt} tables from {}", tree_path.display());

        let progress_mod = match cnt {
            _ if cnt <= 20 => 1,
            _ if cnt <= 100 => 10,
            _ => 100,
        };

        let mut tables = vec![];
        // Track recovered table IDs so duplicate sightings (via symlinks,
        // junctions, or case-insensitive aliases of the same directory) are
        // skipped rather than orphan-deleted.
        let mut recovered_table_ids: crate::HashSet<TableId> = crate::HashSet::default();
        let mut orphaned_tables: Vec<(crate::path::PathBuf, Arc<dyn crate::fs::Fs>)> = vec![];

        // Scan all configured table folders (primary + level routes).
        let all_folders = config.all_tables_folders();

        for (table_base_folder, folder_fs) in &all_folders {
            if !folder_fs.exists(table_base_folder)? {
                folder_fs.create_dir_all(table_base_folder)?;
                fsync_directory(table_base_folder, &**folder_fs, config.sync_mode)?;
                if let Some(parent) = table_base_folder.parent() {
                    fsync_directory(parent, &**folder_fs, config.sync_mode)?;
                    if let Some(grandparent) = parent.parent() {
                        fsync_directory(grandparent, &**folder_fs, config.sync_mode)?;
                    }
                }
            }

            for dirent in folder_fs.read_dir(table_base_folder)? {
                let crate::fs::FsDirEntry {
                    path: table_file_path,
                    file_name,
                    is_dir,
                } = dirent;

                // https://en.wikipedia.org/wiki/.DS_Store
                if file_name == ".DS_Store" {
                    continue;
                }

                // https://en.wikipedia.org/wiki/AppleSingle_and_AppleDouble_formats
                if file_name.starts_with("._") {
                    continue;
                }

                let table_file_name = &file_name;
                if is_dir {
                    log::warn!(
                        "Skipping unexpected directory in tables folder: {}",
                        table_file_path.display()
                    );
                    continue;
                }

                let table_id = table_file_name.parse::<TableId>().map_err(|e| {
                    log::error!("invalid table file name {table_file_name:?}: {e:?}");
                    crate::Error::Unrecoverable
                })?;

                // Remove from map to prevent duplicate recovery if the same
                // table file exists in multiple scanned folders.
                if let Some((level_idx, checksum, global_seqno)) = table_map.remove(&table_id) {
                    let pin_filter = config.filter_block_pinning_policy.get(level_idx.into());
                    let pin_index = config.index_block_pinning_policy.get(level_idx.into());

                    let table = Table::recover(
                        table_file_path,
                        checksum,
                        global_seqno,
                        tree_id,
                        table_id,
                        config.cache.clone(),
                        config.descriptor_table.clone(),
                        folder_fs.clone(),
                        pin_filter,
                        pin_index,
                        config.encryption.clone(),
                        #[cfg(zstd_any)]
                        config.zstd_dictionary.clone(),
                        config.comparator.clone(),
                        #[cfg(feature = "metrics")]
                        metrics.clone(),
                    )?;

                    tables.push(table);
                    recovered_table_ids.insert(table_id);

                    if tables.len() % progress_mod == 0 {
                        log::debug!("Recovered {}/{cnt} tables", tables.len());
                    }
                } else if recovered_table_ids.contains(&table_id) {
                    // Duplicate sighting of an already-recovered manifest table
                    // (e.g., via symlink or case-insensitive alias). Skip it —
                    // do NOT treat as orphan or the live SST will be deleted.
                    log::warn!(
                        "Skipping duplicate sighting of manifest table {table_id} in {}",
                        table_file_path.display(),
                    );
                } else {
                    orphaned_tables.push((table_file_path, folder_fs.clone()));
                }
            }
        }

        if tables.len() < cnt {
            // Route configuration is NOT persisted.  This is a best-effort
            // heuristic: it checks each missing table's level against the
            // current routes, but cannot detect same-level path changes
            // (e.g., L0 routed to /hot_old → /hot_new).  Persisting route
            // provenance per-table in the manifest would enable exact
            // detection but requires a format change.
            //
            // - Level IS covered by a current route → its directory was scanned
            //   and the file was not found → data corruption / deletion.
            // - Level is NOT covered → falls back to primary (always scanned).
            //   If the table isn't there, it was likely in a route that has
            //   since been removed from the config.
            //
            // Return RouteMismatch only when ALL missing tables are on levels
            // not covered by any current route.  If ANY missing table is on a
            // covered level, at least one SST was genuinely lost.
            if let Some(routes) = &config.level_routes {
                let all_missing_uncovered = table_map
                    .values()
                    .all(|(level, _, _)| !routes.iter().any(|r| r.levels.contains(level)));

                if all_missing_uncovered {
                    let found = tables.len();
                    let missing_ids: Vec<_> = table_map.keys().collect();

                    log::error!(
                        "Route mismatch: expected {cnt} tables but found {found} — \
                         level_routes do not cover all previously used levels. \
                         Missing table IDs: {missing_ids:?}",
                    );
                    return Err(crate::Error::RouteMismatch {
                        expected: cnt,
                        found,
                    });
                }
            }

            log::error!(
                "Recovered less tables than expected: {:?}",
                table_map.keys(),
            );
            return Err(crate::Error::Unrecoverable);
        }

        log::debug!("Successfully recovered {} tables", tables.len());

        let (blob_files, orphaned_blob_files) = crate::vlog::recover_blob_files(
            &tree_path.join(crate::file::BLOBS_FOLDER),
            &recovery.blob_file_ids,
            tree_id,
            config.descriptor_table.as_ref(),
            &config.fs,
        )?;

        let version = Version::from_recovery(recovery, &tables, &blob_files)?;

        // NOTE: Cleanup old versions
        // But only after we definitely recovered the latest version.
        // Preserve the snapshot CURRENT references (and its `edits-` log) — the
        // latest version id has no file of its own under the incremental
        // manifest, so cleaning by it would delete the live snapshot.
        Self::cleanup_orphaned_version(tree_path, snapshot_id, &*config.fs)?;

        for (table_path, orphan_fs) in orphaned_tables {
            log::debug!("Deleting orphaned table {}", table_path.display());
            orphan_fs.remove_file(&table_path)?;
        }

        for blob_file_path in orphaned_blob_files {
            log::debug!("Deleting orphaned blob file {}", blob_file_path.display());
            (*config.fs).remove_file(&blob_file_path)?;
        }

        Ok(version)
    }

    /// Removes stale version files left over from a crash during version swap.
    ///
    /// # Behavior change vs pre-Fs-trait code
    ///
    /// The previous implementation used `std::fs::read_dir` + `to_string_lossy()`,
    /// which silently skipped non-UTF-8 filenames. `Fs::read_dir` returns
    /// `InvalidData` for such entries instead (see [`FsDirEntry`] docs), so this
    /// function now fails fast on non-UTF-8 names. This is intentional: version
    /// files are always `v{u64}` — any non-UTF-8 entry indicates filesystem
    /// corruption and should surface as an error rather than be silently ignored.
    /// Removes stale manifest files left by older generations: every `v{id}`
    /// snapshot except the live one (`v{snapshot_id}`) and every `edits-{id}`
    /// log except the live snapshot's (`edits-{snapshot_id}`). A crashed
    /// rotation can leak an old snapshot or its log; this sweeps them on open.
    /// The live snapshot and log are exactly the generation `CURRENT` points at.
    fn cleanup_orphaned_version(
        path: &Path,
        snapshot_id: crate::version::VersionId,
        fs: &dyn crate::fs::Fs,
    ) -> crate::Result<()> {
        let snapshot_str = format!("v{snapshot_id}");
        let log_str = format!("edits-{snapshot_id}");

        for dirent in fs.read_dir(path)? {
            if dirent.is_dir {
                continue;
            }

            let name = &dirent.file_name;
            let is_orphan_snapshot = name.starts_with('v') && *name != snapshot_str;
            let is_orphan_log = name.starts_with("edits-") && *name != log_str;
            if is_orphan_snapshot || is_orphan_log {
                log::trace!("Cleanup orphaned manifest file {name}");
                match fs.remove_file(&dirent.path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }

        Ok(())
    }
}

/// Returns `true` if the directory contains version-related artifacts
/// (a `tables/` subdir, a `blobs/` subdir, or any `vN` manifest file).
///
/// Used by [`Tree::open`] to distinguish a genuinely fresh directory
/// (safe to `create_new`) from a half-written checkpoint or other
/// interrupted sealing (must error rather than silently overwrite).
///
/// A missing parent directory is treated as "no state" — `create_new`
/// is what creates the directory in the first place, so callers may
/// invoke `Tree::open` against a path that does not exist yet.
fn has_existing_version_state(folder: &Path, fs: &dyn Fs) -> crate::Result<bool> {
    if fs.exists(&folder.join(crate::file::TABLES_FOLDER))?
        || fs.exists(&folder.join(crate::file::BLOBS_FOLDER))?
    {
        return Ok(true);
    }
    let entries = match fs.read_dir(folder) {
        Ok(entries) => entries,
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let name = &entry.file_name;
        if name.starts_with('v') && name.len() > 1 && name[1..].bytes().all(|c| c.is_ascii_digit())
        {
            return Ok(true);
        }
    }
    Ok(false)
}
