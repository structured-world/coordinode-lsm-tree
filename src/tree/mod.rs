// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(feature = "columnar")]
pub mod columnar_scan;
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

/// Floor for the storage-admission reserved headroom band (see
/// [`Tree::compute_write_admission`]). Even with an empty active memtable the
/// gate keeps at least this much room below the budget so the next writes and a
/// space-reclaiming compaction have somewhere to land. 1 MiB.
pub const MIN_RESERVED_HEADROOM: u64 = 1024 * 1024;

/// How long a cached disk-free sample stays valid before the admission gate
/// re-probes. Bounds how stale the physical free-space figure can be when the
/// filesystem fills from another process between flushes, without issuing a
/// `statfs`/`statvfs` syscall on every gated write. 1 second.
const ADMISSION_DISK_FREE_TTL: core::time::Duration = core::time::Duration::from_secs(1);

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

/// One covered key in a batched run resolution: `(input index, key hash,
/// resolved item)`. Aliased to keep `resolve_run_batched`'s return readable.
type CoveredKey = (usize, u64, Option<InternalValue>);

/// `(miss_keys, duplicates)` from [`Tree::dedup_sorted_miss_keys`]: `miss_keys`
/// is `(key_index, bloom_hash)` for the strictly-sorted-unique batched resolver,
/// `duplicates` is `(duplicate_index, representative_index)` for the fan-out.
type DedupedMissKeys = (Vec<(usize, u64)>, Vec<(usize, usize)>);

/// The outcome of resolving a key batch against one run (see `resolve_run_batched`).
struct RunResolve {
    /// Covered, non-skipped keys with their resolved item, in input order.
    covered: Vec<CoveredKey>,
    /// Keys this run does not cover, in input order, for the next run or level.
    not_covered: Vec<(usize, u64)>,
}

/// One data block the chunked `multi_get` resolver will read (see
/// `resolve_level_chunked`): the block, the SST it lives in (`table` + its `file`
/// handle), the table-local read seqno, whether it needs the special load path
/// (Page-ECC / columnar), and the ORIGINAL key indices that fall in this block.
struct BlockTask<'a> {
    table: &'a crate::Table,
    file: Arc<dyn crate::fs::FsFile>,
    handle: crate::table::BlockHandle,
    table_seqno: SeqNo,
    special: bool,
    keys: Vec<usize>,
}

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

/// Maps a raw merge-pipeline item into a standard-tree iterator guard.
fn standard_guard(item: crate::Result<InternalValue>) -> IterGuardImpl {
    IterGuardImpl::Standard(Guard(item.map(|iv| (iv.key.user_key, iv.value))))
}

/// Extract owned user-key bounds from any range.
#[expect(
    clippy::redundant_pub_crate,
    reason = "reached from blob_tree as crate::tree::range_to_user_bounds"
)]
pub(crate) fn range_to_user_bounds<K: AsRef<[u8]>, R: RangeBounds<K>>(
    range: &R,
) -> (Bound<UserKey>, Bound<UserKey>) {
    use core::ops::Bound::{Excluded, Included, Unbounded};
    let lo = match range.start_bound() {
        Included(x) => Included(x.as_ref().into()),
        Excluded(x) => Excluded(x.as_ref().into()),
        Unbounded => Unbounded,
    };
    let hi = match range.end_bound() {
        Included(x) => Included(x.as_ref().into()),
        Excluded(x) => Excluded(x.as_ref().into()),
        Unbounded => Unbounded,
    };
    (lo, hi)
}

/// Wraps a [`SeekableTreeIter`](crate::range::SeekableTreeIter) so a standard
/// tree can expose it as a [`SeekableGuardIter`](crate::iter_guard::SeekableGuardIter).
struct StandardSeekable {
    inner: crate::range::SeekableTreeIter,
}

impl Iterator for StandardSeekable {
    type Item = IterGuardImpl;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(standard_guard)
    }
}

impl DoubleEndedIterator for StandardSeekable {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(standard_guard)
    }
}

impl crate::iter_guard::SeekableGuardIter for StandardSeekable {
    fn seek_to(&mut self, key: &[u8]) {
        self.inner.seek_to(key);
    }

    fn seek_to_for_prev(&mut self, key: &[u8]) {
        self.inner.seek_to_for_prev(key);
    }

    fn peek_key(&mut self) -> Option<crate::Result<crate::UserKey>> {
        self.inner.peek_key()
    }
}

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

    #[cfg(feature = "std")]
    fn refresh_table_checksum(
        &self,
        table_id: TableId,
        checksum: crate::checksum::Checksum,
        expected_restriction: Option<&crate::UserKey>,
    ) -> crate::Result<crate::abstract_tree::ChecksumRefreshOutcome> {
        use crate::abstract_tree::ChecksumRefreshOutcome;
        // Same lock order as flush / compaction version installs: compaction
        // state first, then the version history write lock. But the caller (the
        // patrol reconcile) holds this table's HEAL LOCK across this call, and a
        // concurrent tight-space compaction acquires that heal lock WHILE holding
        // `compaction_state`. Blocking on `compaction_state` here would invert the
        // order (heal_lock -> compaction_state on this path vs
        // compaction_state -> heal_lock on the compaction path) and deadlock
        // permanently. `try_lock` instead: a failed acquire means a compaction is
        // mid-install, so skip this refresh — but report the skip as CONTENDED,
        // not as a benign no-op: the healed bytes are durable while the manifest
        // digest stays stale, so a "clean" report would mislead a later
        // integrity check / checkpoint. The caller keeps the attestation and
        // surfaces a finding; the next patrol retries once the compaction
        // releases the state.
        let Some(mut _compaction_state) = self.compaction_state.try_lock() else {
            return Ok(ChecksumRefreshOutcome::Contended);
        };
        let mut version_lock = self.version_history.write();

        // Under the install lock, resolve the CURRENT view of this table and
        // reject the refresh if either it is gone (compacted away — nothing to
        // refresh) or its restriction no longer matches the one `checksum` was
        // computed for. A tight-space compaction can swap the captured view for a
        // restricted same-id view (punching its prefix) between the caller's read
        // and this lock; installing the caller's digest against a different
        // restriction would record one the punched file can never match. Skipping
        // leaves the current view's own (compaction-installed) digest in place for
        // the next patrol to reconcile.
        let restriction_matches = version_lock
            .latest_version()
            .version
            .iter_tables()
            .find(|t| t.id() == table_id)
            .is_some_and(|t| t.restrict_lower_bound() == expected_restriction);
        if !restriction_matches {
            // No-op: the manifest digest is unchanged, so the caller must keep the
            // attestation for the next patrol.
            return Ok(ChecksumRefreshOutcome::Stale);
        }

        version_lock
            .upgrade_version(
                &self.config.path,
                |current| {
                    let mut copy = current.clone();
                    if let Some(next) = copy
                        .version
                        .with_refreshed_table_checksum(table_id, checksum)
                    {
                        copy.version = next;
                    }
                    Ok(copy)
                },
                &self.config.seqno,
                &self.config.visible_seqno,
                &*self.config.fs,
                self.0.runtime_config.load_full(),
                self.0.config.encryption.clone(),
            )
            .map(|()| ChecksumRefreshOutcome::Refreshed)
    }

    fn sync_mode(&self) -> crate::fs::SyncMode {
        self.config.sync_mode
    }

    fn prefix_extractor(&self) -> Option<alloc::sync::Arc<dyn crate::prefix::PrefixExtractor>> {
        self.config.prefix_extractor.clone()
    }

    fn storage_stats(&self) -> crate::Result<crate::StorageStats> {
        // One version snapshot reused for the footprint and the full-compaction
        // estimate below: a second `current_version()` could race a concurrent
        // flush / compaction and mix two snapshots.
        let version = self.current_version();
        // Standard tree: SST values ARE user values (no KV separation).
        let mut stats =
            crate::storage_stats::compute_storage_stats(&version, self.is_compacting(), true)?;
        // Fill the disk-aware capacity figures (quota + free-space probe) the
        // version-only computation can't know.
        let (capacity, available, compaction_possible) = self.admission_capacity(stats.used_bytes);
        stats.capacity_bytes = capacity;
        stats.available_bytes = available;
        stats.compaction_possible = compaction_possible;
        // When admission gating is active and a compaction is not already
        // running, surface whether a full compaction has working room through the
        // SAME two-layer check the compaction space gate enforces (logical quota +
        // physical free per destination volume), so the reported status matches
        // what the gate will admit. With gating off the gate never runs, so the
        // status stays `Healthy` even though the backend can report a finite
        // capacity.
        if self.storage_admission_enabled()
            && capacity.is_some()
            && stats.status == crate::StorageStatus::Healthy
        {
            // A full compaction's transient output is bounded by the largest
            // level's on-disk size, but it LANDS in the last configured level's
            // volume (`level_count - 1`), which under tiered routing can be a
            // different filesystem than the largest level. A standard tree has no
            // blob relocation. Using the per-volume gate (not `available >=
            // full_compaction_bytes` against the min-volume free) keeps the status
            // from reporting tight when a routed merge would actually be admitted.
            let sst_need = crate::storage_stats::full_compaction_demand_bytes(&version);
            // `saturating_sub`: `level_count >= 1` always, so this is the last
            // level index; the clamp only guards a degenerate zero-level config.
            let sst_dest_level = self.0.config.level_count.saturating_sub(1);
            let quota_headroom = self.quota_headroom(stats.used_bytes);
            let full_fits = crate::compaction::worker::space_fits_two_layer(
                &self.0.config,
                quota_headroom,
                sst_need,
                sst_dest_level,
                0,
            );
            stats.status = if full_fits {
                crate::StorageStatus::FullCompactionAvailable
            } else {
                crate::StorageStatus::TightCompactionAvailable
            };
        }
        // A closed admission gate is the operator-actionable state, so it takes
        // precedence over the others (a read-only tree may well be compacting to
        // reclaim space).
        if self.is_read_only() {
            stats.status = crate::StorageStatus::ReadOnlyOutOfSpace;
        }
        Ok(stats)
    }

    fn write_admission(&self) -> crate::Result<()> {
        self.compute_write_admission()
    }

    fn write_backpressure(
        &self,
        strategy: &dyn crate::compaction::CompactionStrategy,
    ) -> crate::Backpressure {
        // Copy the thresholds out (BackpressureThresholds is Copy) so the
        // arc-swap guard drops immediately; the off check short-circuits before
        // touching the version, keeping the disabled path free.
        let thresholds = self.0.runtime_config.load().backpressure;
        if thresholds.is_off() {
            return crate::Backpressure::None;
        }
        let version = self.current_version();
        // L0 is the first level; its table (file) count is the count-trigger
        // signal, matching the leveled `choose` trigger and the L0 term of
        // `pending_compaction_bytes`.
        let l0_count = version
            .iter_levels()
            .next()
            .map_or(0, |level| level.table_count());
        let pending = strategy.pending_compaction_bytes(&version);
        crate::Backpressure::compute(l0_count, pending, &thresholds)
    }

    fn get_flush_lock(&self) -> FlushGuard<'_> {
        self.flush_lock.lock()
    }

    #[cfg(feature = "metrics")]
    fn metrics(&self) -> &Arc<crate::Metrics> {
        &self.0.metrics
    }

    #[cfg(feature = "metrics")]
    fn cache_stats(&self) -> crate::CacheStats {
        let cache = &self.0.config.cache;
        self.metrics().cache_stats(cache.size(), cache.capacity())
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

    fn range_seekable<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn crate::iter_guard::SeekableGuardIter + 'static> {
        let (lo, hi) = range_to_user_bounds(&range);
        let inner = self.create_seekable_range_bounds(lo, hi, seqno, index);
        Box::new(StandardSeekable { inner })
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
        // Open the seekable iterator over the whole keyspace once; each interval
        // is served by repositioning it (single per-SST setup, amortized).
        let inner =
            self.create_seekable_range_bounds(Bound::Unbounded, Bound::Unbounded, seqno, index);
        let intervals = intervals.into_iter().map(|r| range_to_user_bounds(&r));
        Box::new(crate::range::BatchRangeScan::new(inner, intervals).map(standard_guard))
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
        table_writer = table_writer.use_zone_map(rc.zone_map);
        table_writer = table_writer.use_columnar(rc.columnar);
        table_writer = table_writer.use_disable_cow_on_sst(rc.disable_cow_on_sst_files);
        // `Off` (default) emits no per-KV footer and leaves the data-block
        // payload encoding unchanged (the V5 header carries a block_flags byte
        // and the meta block a descriptor key regardless, so the on-disk bytes
        // are not identical to a pre-V5 table).
        table_writer = table_writer.use_kv_checksums(rc.kv_checksums, rc.kv_checksum_algo);
        // Flush writes level 0; resolve that level's locator policy entry.
        table_writer = table_writer.use_locator(self.config.locator_policy.get(0));

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
                let mut params = crate::table::RecoverParams::new(
                    folder.join(table_id.to_string()),
                    checksum,
                    table_id,
                    level_fs.clone(),
                    self.config.comparator.clone(),
                    self.config.cache.clone(),
                );
                params.tree_id = self.id;
                params
                    .descriptor_table
                    .clone_from(&self.config.descriptor_table);
                params.pin_filter = pin_filter;
                params.pin_index = pin_index;
                params.encryption.clone_from(&self.config.encryption);
                #[cfg(zstd_any)]
                {
                    params
                        .zstd_dictionary
                        .clone_from(&self.config.zstd_dictionary);
                }
                #[cfg(feature = "metrics")]
                {
                    params.metrics = self.metrics.clone();
                }
                Table::recover(params)
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
        let sinks = crate::table::TableSinks {
            deletion_pause: &self.deletion_pause,
            heal_hints: &self.heal_hints,
            #[cfg(feature = "std")]
            background_deleter: Some(&self.background_deleter),
        };
        for table in tables {
            table.bind_to_tree(&sinks);
        }
        for bf in blob_files.unwrap_or(&[]) {
            bf.bind_to_tree(&sinks);
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

    fn approximate_range_stats<K: AsRef<[u8]>, R: core::ops::RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
    ) -> crate::Result<crate::ApproximateRangeStats> {
        use crate::table::block_index::BlockIndex;
        use core::ops::Bound;

        let lo: Bound<&[u8]> = match range.start_bound() {
            Bound::Included(k) => Bound::Included(k.as_ref()),
            Bound::Excluded(k) => Bound::Excluded(k.as_ref()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let hi: Bound<&[u8]> = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.as_ref()),
            Bound::Excluded(k) => Bound::Excluded(k.as_ref()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let bounds = (lo, hi);

        let mut bytes: u64 = 0;
        let mut key_count: u64 = 0;

        // Use ONE snapshot at the requested seqno for both the SST and memtable
        // contributions, so the estimate reflects the same visibility as a read
        // at `seqno` (no entries newer than the snapshot, and a consistent set of
        // tables + memtables even during a concurrent flush / compaction).
        let comparator = self.config.comparator.as_ref();
        let super_version = self.version_history.read().get_version_for_snapshot(seqno);

        // SST contribution: interpolate data-block offsets at the boundaries
        // (block granularity), no data-block reads. For a KV-separated SST the
        // referenced blob bytes are apportioned by the same in-range fraction.
        for table in super_version.version.iter_tables() {
            // Comparator-aware overlap: a custom user comparator orders keys
            // differently from raw bytes, so use the same comparison the read
            // path does instead of default byte ordering.
            if !table
                .metadata
                .key_range
                .overlaps_with_bounds_cmp(&bounds, comparator)
            {
                continue;
            }
            // The block index is keyed by the table-LOCAL seqno; a bulk-ingested
            // table carries a non-zero global seqno, so translate the snapshot
            // seqno the same way the read path does before seeking it. A snapshot
            // below the table's base means the table postdates it and contributes
            // nothing to the estimate, so skip it (`checked_sub` yields `None`).
            let Some(table_seqno) = seqno.checked_sub(table.global_seqno()) else {
                continue;
            };

            // data_end = the data section's byte extent = last data block's end.
            let Some(last) = table.block_index.iter().next_back() else {
                continue;
            };
            let last = last?;
            let data_end = *last.offset() + u64::from(last.size());
            if data_end == 0 {
                continue;
            }

            // The data block that would contain `key`, as (start, end) byte
            // offsets, or `None` when `key` is past the last block. The full
            // extent is returned so the lower bound counts from the block start
            // and the upper bound INCLUDES it (a range inside a single block must
            // not collapse to zero bytes).
            let block_span = |key: &[u8]| -> crate::Result<Option<(u64, u64)>> {
                let Some(mut iter) = table.block_index.forward_reader(key, table_seqno) else {
                    return Ok(None);
                };
                let Some(handle) = iter.next() else {
                    return Ok(None);
                };
                let h = handle?;
                let start = *h.offset();
                Ok(Some((start, (start + u64::from(h.size())).min(data_end))))
            };
            let off_lo = match lo {
                Bound::Included(k) | Bound::Excluded(k) => {
                    block_span(k)?.map_or(data_end, |(start, _)| start)
                }
                Bound::Unbounded => 0,
            };
            // Tight-space restriction: a restricted table view serves only keys
            // at or above its lower bound, with the punched-out prefix served by
            // the replacement table. Raise the lower offset to that bound so the
            // prefix is not double-counted (matching how scans skip it).
            let off_lo = match table.restrict_lower_bound() {
                Some(rb) => {
                    off_lo.max(block_span(rb.as_ref())?.map_or(data_end, |(start, _)| start))
                }
                None => off_lo,
            };
            let off_hi = match hi {
                Bound::Included(k) | Bound::Excluded(k) => {
                    block_span(k)?.map_or(data_end, |(_, end)| end)
                }
                Bound::Unbounded => data_end,
            };
            let idx_bytes = off_hi.saturating_sub(off_lo);
            if idx_bytes == 0 {
                continue;
            }

            // fraction = idx_bytes / data_end, in u128 to avoid overflow. For a
            // standard tree `idx_bytes` already includes the inline values. For a
            // KV-separated SST it covers only the key + pointer bytes, so the
            // SST's referenced blob bytes (recorded per-SST at both flush and
            // compaction) are apportioned by the same in-range fraction; blob
            // files are not key-indexed, so this fraction is the finest estimate
            // possible without reading data blocks.
            let num = u128::from(idx_bytes);
            let den = u128::from(data_end);
            let blob_bytes = table.referenced_blob_bytes()?;
            let sst_blob = u64::try_from(u128::from(blob_bytes) * num / den).unwrap_or(u64::MAX);
            // Round up to at least one entry: a non-empty byte span over a
            // non-empty SST always covers at least one row, so a narrow range
            // never reports bytes with a zero key count.
            let in_range_entries = u64::try_from(u128::from(table.metadata.item_count) * num / den)
                .unwrap_or(u64::MAX)
                .max(1);
            bytes = bytes.saturating_add(idx_bytes).saturating_add(sst_blob);
            key_count = key_count.saturating_add(in_range_entries);
        }

        // Memtable contribution: the in-range fraction of each memtable's
        // approximate size. Built from the SAME snapshot and the SAME
        // `range_internal` + internal-key bounds the read path uses (range.rs),
        // so the counted slice matches what a read at `seqno` would traverse.
        let mt_range = (
            match lo {
                Bound::Included(k) => {
                    Bound::Included(InternalKey::new(k, SeqNo::MAX, crate::ValueType::Tombstone))
                }
                Bound::Excluded(k) => {
                    Bound::Excluded(InternalKey::new(k, 0, crate::ValueType::Tombstone))
                }
                Bound::Unbounded => Bound::Unbounded,
            },
            match hi {
                Bound::Included(k) => {
                    Bound::Included(InternalKey::new(k, 0, crate::ValueType::Value))
                }
                Bound::Excluded(k) => {
                    Bound::Excluded(InternalKey::new(k, SeqNo::MAX, crate::ValueType::Value))
                }
                Bound::Unbounded => Bound::Unbounded,
            },
        );
        let estimate = |mt: &crate::Memtable| -> (u64, u64) {
            let total = mt.len() as u64;
            if total == 0 {
                return (0, 0);
            }
            // Count only entries visible at the snapshot (the same seqno cutoff
            // reads apply), so the estimate excludes writes newer than `seqno`.
            let count = mt
                .range_internal(mt_range.clone())
                .filter(|kv| kv.key.seqno < seqno)
                .count() as u64;
            if count == 0 {
                return (0, 0);
            }
            let mt_bytes =
                u64::try_from(u128::from(mt.size()) * u128::from(count) / u128::from(total))
                    .unwrap_or(u64::MAX);
            (mt_bytes, count)
        };
        let (b, c) = estimate(&super_version.active_memtable);
        bytes = bytes.saturating_add(b);
        key_count = key_count.saturating_add(c);
        for mt in super_version.sealed_memtables.iter() {
            let (b, c) = estimate(mt);
            bytes = bytes.saturating_add(b);
            key_count = key_count.saturating_add(c);
        }

        Ok(crate::ApproximateRangeStats { bytes, key_count })
    }

    fn approximate_range_cardinality<K: AsRef<[u8]>, R: core::ops::RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
    ) -> crate::Result<crate::RangeCardinality> {
        use crate::table::block_index::BlockIndex;
        use core::cmp::Ordering;
        use core::ops::Bound;

        let lo: Bound<&[u8]> = match range.start_bound() {
            Bound::Included(k) => Bound::Included(k.as_ref()),
            Bound::Excluded(k) => Bound::Excluded(k.as_ref()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let hi: Bound<&[u8]> = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.as_ref()),
            Bound::Excluded(k) => Bound::Excluded(k.as_ref()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let bounds = (lo, hi);
        let comparator = self.config.comparator.as_ref();
        let super_version = self.version_history.read().get_version_for_snapshot(seqno);

        let mut rows: u64 = 0;
        let mut total_rows: u64 = 0;

        for table in super_version.version.iter_tables() {
            total_rows = total_rows.saturating_add(table.metadata.item_count);
            if !table
                .metadata
                .key_range
                .overlaps_with_bounds_cmp(&bounds, comparator)
            {
                continue;
            }
            // A snapshot below the table's base means the table postdates it and
            // contributes nothing here, so skip it (`checked_sub` yields `None`).
            let Some(table_seqno) = seqno.checked_sub(table.global_seqno()) else {
                continue;
            };
            // Honor a tight-space restricted view: keys below
            // `restrict_lower_bound()` are the punched-out prefix served by the
            // replacement table, so raise this table's effective lower bound to
            // it (mirrors approximate_range_stats) and never charge that prefix.
            let eff_lo = effective_lower_bound(
                lo,
                table.restrict_lower_bound().map(AsRef::as_ref),
                comparator,
            );
            let zone_map = &table.zone_map;
            if !zone_map.is_empty() {
                // Zone map present: sum the per-block row counts of blocks whose
                // key range overlaps the query. A block is past the range once its
                // minimum key is above the upper bound; the boundary block at the
                // effective lower bound is counted in full (block granularity). A
                // range that lands in a key-space gap legitimately yields zero, so
                // this path is authoritative and never falls back to the byte fraction.
                let reader = match eff_lo {
                    Bound::Included(k) | Bound::Excluded(k) => {
                        table.block_index.forward_reader(k, table_seqno)
                    }
                    Bound::Unbounded => Some(table.block_index.iter()),
                };
                if let Some(reader) = reader {
                    for handle in reader {
                        let handle = handle?;
                        let Some(col) = zone_map
                            .columns_for(*handle.offset())
                            .and_then(|c| c.first())
                        else {
                            continue;
                        };
                        let above_hi = match hi {
                            Bound::Included(hk) => {
                                comparator.compare(&col.min, hk) == Ordering::Greater
                            }
                            Bound::Excluded(hk) => {
                                comparator.compare(&col.min, hk) != Ordering::Less
                            }
                            Bound::Unbounded => false,
                        };
                        if above_hi {
                            break;
                        }
                        rows = rows.saturating_add(u64::from(col.row_count));
                    }
                }
            } else if let Some(last) = table.block_index.iter().next_back() {
                // No zone map: apportion item_count by the in-range
                // data-block byte fraction, mirroring approximate_range_stats.
                let last = last?;
                let data_end = *last.offset() + u64::from(last.size());
                if data_end > 0 {
                    let off = |key: &[u8], end: bool| -> crate::Result<u64> {
                        match table.block_index.forward_reader(key, table_seqno) {
                            Some(mut it) => match it.next() {
                                Some(h) => {
                                    let h = h?;
                                    Ok(if end {
                                        (*h.offset() + u64::from(h.size())).min(data_end)
                                    } else {
                                        *h.offset()
                                    })
                                }
                                None => Ok(data_end),
                            },
                            None => Ok(data_end),
                        }
                    };
                    let off_lo = match eff_lo {
                        Bound::Included(k) | Bound::Excluded(k) => off(k, false)?,
                        Bound::Unbounded => 0,
                    };
                    let off_hi = match hi {
                        Bound::Included(k) | Bound::Excluded(k) => off(k, true)?,
                        Bound::Unbounded => data_end,
                    };
                    let idx_bytes = off_hi.saturating_sub(off_lo);
                    if idx_bytes > 0 {
                        let est = u64::try_from(
                            u128::from(table.metadata.item_count) * u128::from(idx_bytes)
                                / u128::from(data_end),
                        )
                        .unwrap_or(u64::MAX)
                        .max(1);
                        rows = rows.saturating_add(est);
                    }
                }
            }
        }

        // Memtables: count the in-range, snapshot-visible entries and add them to
        // both the matched rows and the total (matching the SST accounting).
        let mt_range = (
            match lo {
                Bound::Included(k) => {
                    Bound::Included(InternalKey::new(k, SeqNo::MAX, crate::ValueType::Tombstone))
                }
                Bound::Excluded(k) => {
                    Bound::Excluded(InternalKey::new(k, 0, crate::ValueType::Tombstone))
                }
                Bound::Unbounded => Bound::Unbounded,
            },
            match hi {
                Bound::Included(k) => {
                    Bound::Included(InternalKey::new(k, 0, crate::ValueType::Value))
                }
                Bound::Excluded(k) => {
                    Bound::Excluded(InternalKey::new(k, SeqNo::MAX, crate::ValueType::Value))
                }
                Bound::Unbounded => Bound::Unbounded,
            },
        );
        let mut add_memtable = |mt: &crate::Memtable| {
            total_rows = total_rows.saturating_add(mt.len() as u64);
            let in_range = mt
                .range_internal(mt_range.clone())
                .filter(|kv| kv.key.seqno < seqno)
                .count() as u64;
            rows = rows.saturating_add(in_range);
        };
        add_memtable(&super_version.active_memtable);
        for mt in super_version.sealed_memtables.iter() {
            add_memtable(mt);
        }

        // selectivity is an approximate ratio; u64 row counts are well within
        // f64's exact-integer range (2^52) for any realistic table.
        #[expect(
            clippy::cast_precision_loss,
            reason = "row counts never approach 2^52; the ratio is approximate anyway"
        )]
        let selectivity = if total_rows == 0 {
            0.0
        } else {
            (rows.min(total_rows) as f64) / (total_rows as f64)
        };
        Ok(crate::RangeCardinality { rows, selectivity })
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

            // De-duplicate equal query keys (the batched on-disk path requires
            // strictly-sorted-unique input) and resolve the misses. Shared with
            // the BlobTree path via these helpers so the two cannot drift.
            let (miss_keys, duplicates) =
                Self::dedup_sorted_miss_keys(&remaining, &keys, comparator);

            Self::batch_get_from_tables(
                &super_version.version,
                &keys,
                miss_keys,
                seqno,
                comparator,
                &*self.config.fs,
                &mut internal_entries,
            )?;

            Self::fan_out_duplicates(&duplicates, &mut internal_entries);
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
        // The read guard is held through the insert, like `append_entry`: the
        // CDC scan's capture (write side of this lock) must exclude every
        // in-flight memtable write, or a backdated range deletion could land
        // after the capture yet below the returned watermark and be lost; the
        // guard also keeps a concurrent `rotate_memtable()` from sealing the
        // memtable mid-insert.
        let history = self.version_history.read();

        #[cfg(test)]
        inner::TestHooks::fire(&self.test_hooks.range_write);

        history
            .latest_version()
            .active_memtable
            .insert_range_tombstone(start.into(), end.into(), seqno)
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
        block_skip: bool,
        resolve_indirection: F,
    ) -> crate::Result<alloc::vec::IntoIter<ScanSinceEvent>>
    where
        F: Fn(&Version, InternalValue) -> crate::Result<ScanSinceEvent>,
    {
        // The active memtable is the one source a writer can still change, and
        // the seqno cap alone does not exclude that: a caller may commit with an
        // explicit seqno at or BELOW the cap (`apply_batch` takes the seqno from
        // the caller), and a live lock-free walk would then see that write or
        // miss it depending on where the node lands relative to the cursor —
        // even splitting one batch. A consumer that advanced past the returned
        // watermark would lose the change for good.
        //
        // So freeze it: writers hold the version-history READ guard for their
        // whole insert (that is what keeps `rotate_memtable` from sealing
        // mid-batch), so taking the WRITE guard excludes them. The cap and the
        // active memtable's raw entries are captured under it; everything else —
        // sealed memtables, tables — is immutable and needs no coordination.
        //
        // Mapping runs AFTER the guard drops: it resolves blob indirections,
        // which reads a blob file, and no I/O may happen with writers blocked.
        let (super_version, end_seqno, active_entries, active_range_tombstones) = {
            let guard = self.version_history.write();
            let super_version = guard.latest_version();
            #[cfg(test)]
            inner::TestHooks::fire(&self.test_hooks.scan_freeze);
            let end_seqno = {
                let active = super_version.active_memtable.get_highest_seqno();
                let sealed = super_version
                    .sealed_memtables
                    .iter()
                    .map(|mt| mt.get_highest_seqno())
                    .max()
                    .flatten();
                let tables = super_version
                    .version
                    .iter_tables()
                    .map(Table::get_highest_seqno)
                    .max();
                active.max(sealed).max(tables)
            };
            let entries: Vec<InternalValue> = end_seqno.map_or_else(Vec::new, |cap| {
                super_version
                    .active_memtable
                    .iter()
                    .filter(|e| e.key.seqno >= target_seqno && e.key.seqno <= cap)
                    .collect()
            });
            let rts = super_version.active_memtable.range_tombstones_sorted();
            // Explicit: the guard must outlive the capture above, and writers
            // resume the moment it goes.
            drop(guard);
            (super_version, end_seqno, entries, rts)
        };
        let version = &super_version.version;
        // No entries anywhere ⇒ nothing qualifies, regardless of target.
        let Some(end_seqno) = end_seqno else {
            return Ok(Vec::new().into_iter());
        };

        // Events are gathered PER SOURCE, not into one flat list: copies of a
        // change across two sources are the same change and collapse, but a
        // single source may legitimately hold a byte-identical event more than
        // once (a write batch may carry the same merge operand for a key
        // twice; both are stored under the batch's shared seqno and both are
        // applied on read). See `merge_source_events`.
        let mut sources: Vec<Vec<ScanSinceEvent>> = Vec::new();

        let in_window = |seqno: SeqNo| seqno >= target_seqno && seqno <= end_seqno;
        let range_tombstone_event = |rt: &RangeTombstone| {
            in_window(rt.seqno).then(|| ScanSinceEvent::RangeTombstone {
                start_key: rt.start.clone(),
                end_key: rt.end.clone(),
                seqno: rt.seqno,
            })
        };

        // Active memtable — mapped from the frozen capture above, not walked
        // again: a second walk would reintroduce exactly the race the freeze
        // closed.
        let mut source = Vec::new();
        for entry in active_entries {
            source.push(Self::map_event(entry, version, &resolve_indirection)?);
        }
        for rt in active_range_tombstones {
            source.extend(range_tombstone_event(&rt));
        }
        sources.push(source);

        // Sealed memtables, NEWEST first: the list is kept in seal order, and
        // every source below must be older than the one before it, because the
        // merge derives replay precedence from that position.
        for memtable in super_version.sealed_memtables.iter().rev() {
            let mut source = Vec::new();
            for entry in memtable.iter() {
                if in_window(entry.key.seqno) {
                    source.push(Self::map_event(entry, version, &resolve_indirection)?);
                }
            }
            for rt in memtable.range_tombstones_sorted() {
                source.extend(range_tombstone_event(&rt));
            }
            sources.push(source);
        }

        // SSTs.
        for table in version.iter_tables() {
            let mut source = Vec::new();
            for entry in table.scan_seqno_range(target_seqno, end_seqno, block_skip)? {
                source.push(Self::map_event(entry, version, &resolve_indirection)?);
            }
            // Clamped to the view's tight-space restriction: the punched
            // prefix's deletions are re-emitted by the slice output that
            // superseded it, so the raw list would duplicate those events.
            for rt in table.visible_range_tombstones() {
                source.extend(range_tombstone_event(&rt));
            }
            sources.push(source);
        }

        Ok(Self::merge_source_events(sources).into_iter())
    }

    /// Merge per-source event lists into one replay-ordered stream, `sources`
    /// ordered NEWEST first.
    ///
    /// Replay order is increasing seqno, then — for events sharing one seqno —
    /// increasing source AGE reversed, so the newest source's event is applied
    /// LAST. That matters because two sources can hold different values for one
    /// key at one seqno ([`AbstractTree::apply_batch`] takes a caller-chosen
    /// seqno and does not require it to be unique), the tree serves the newer
    /// one, and a consumer keeps whatever it applies last. Sorting such ties by
    /// payload instead would decide precedence by byte order.
    ///
    /// What happens to byte-identical copies follows ONE rule: the stream must
    /// mirror what a read of the same tree does, because a consumer replaying
    /// it has to reach the state the tree itself serves.
    ///
    /// - **Merge operands are all kept.** The read path collects every
    ///   physically stored operand for a key and applies them in order — it
    ///   never deduplicates by seqno — so two operands are two applications
    ///   whether they sit in one source or in two. Seqnos do not disambiguate
    ///   here: [`AbstractTree::apply_batch`] takes a caller-chosen seqno and
    ///   does not require it to be unique, and one batch may carry the same
    ///   operand for a key twice.
    /// - **Idempotent events collapse across sources.** A write, a deletion or
    ///   a range deletion replayed twice reaches the same state, and the read
    ///   path shadows the copies by seqno rather than compounding them. One
    ///   committed change can physically live in two published tables — a
    ///   manifest-loss repair publishes every surviving SST as its own L0 run,
    ///   including both the inputs and the outputs of a compaction that crashed
    ///   before deleting its inputs — and delivering it twice would be noise.
    ///   Repeats WITHIN one source are still kept: they are separate entries
    ///   the source genuinely holds.
    fn merge_source_events(sources: Vec<Vec<ScanSinceEvent>>) -> Vec<ScanSinceEvent> {
        // Per source, collapse equal neighbours into (event, count) runs, each
        // tagged with its source's recency (0 = newest) and the position the
        // source itself gives the run.
        //
        // The POSITION is what keeps an order-sensitive merge operator correct:
        // a source applies the operands of one batch in the order they were
        // added, all at one seqno, and grouping them by payload would replay
        // them in a different order — an append or a list push then converges
        // somewhere the tree never was. Grouping still SORTS (equal events have
        // to meet), so each run remembers where its earliest member sat, and the
        // final ordering below restores it.
        struct Run {
            event: ScanSinceEvent,
            count: usize,
            recency: usize,
            position: usize,
        }
        let mut runs: Vec<Run> = Vec::new();
        for (recency, source) in sources.into_iter().enumerate() {
            let mut indexed: Vec<(usize, ScanSinceEvent)> =
                source.into_iter().enumerate().collect();
            indexed.sort_by(|(_, a), (_, b)| ScanSinceEvent::grouping_order(a, b));
            let mut iter = indexed.into_iter();
            let Some((mut position, mut current)) = iter.next() else {
                continue;
            };
            let mut count = 1;
            for (index, event) in iter {
                if event == current {
                    count += 1;
                    position = position.min(index);
                } else {
                    runs.push(Run {
                        event: core::mem::replace(&mut current, event),
                        count,
                        recency,
                        position,
                    });
                    count = 1;
                    position = index;
                }
            }
            runs.push(Run {
                event: current,
                count,
                recency,
                position,
            });
        }

        // Merge the per-source runs: operands accumulate, idempotent events take
        // the count a single source holds. A collapsed event keeps the recency
        // of the NEWEST source holding it, which is the position the tree's own
        // precedence gives it.
        runs.sort_by(|a, b| ScanSinceEvent::grouping_order(&a.event, &b.event));
        let mut merged: Vec<(ScanSinceEvent, usize, usize)> = Vec::new();
        let mut iter = runs.into_iter().peekable();
        while let Some(Run {
            event,
            mut count,
            mut recency,
            mut position,
        }) = iter.next()
        {
            let accumulates = matches!(event, ScanSinceEvent::MergeOperand { .. });
            while let Some(other) = iter.next_if(|next| next.event == event) {
                debug_assert_eq!(other.event, event, "next_if matched the same event");
                count = if accumulates {
                    count.saturating_add(other.count)
                } else {
                    count.max(other.count)
                };
                // The newest source decides both the replay slot and the
                // position within it, so the two stay consistent.
                if other.recency < recency {
                    recency = other.recency;
                    position = other.position;
                }
            }
            for _ in 1..count {
                merged.push((event.clone(), recency, position));
            }
            merged.push((event, recency, position));
        }

        // Finally, replay order: seqno, then range deletions, then oldest source
        // first, then the position that source gave the event — which is how an
        // order-sensitive merge operator converges to what the tree serves.
        //
        // The range-deletion step comes BEFORE source recency, not after: a tied
        // deletion does not suppress the writes it spans (suppression is
        // strictly `entry.seqno < tombstone.seqno`), so the tree keeps them, and
        // a replay that applied the deletion last would drop them. Ordering by
        // recency first would do exactly that whenever the deletion sits in the
        // newer source.
        merged.sort_by(|(a, a_recency, a_pos), (b, b_recency, b_pos)| {
            fn deletion_first(e: &ScanSinceEvent) -> u8 {
                u8::from(!matches!(e, ScanSinceEvent::RangeTombstone { .. }))
            }
            a.seqno()
                .cmp(&b.seqno())
                .then_with(|| deletion_first(a).cmp(&deletion_first(b)))
                .then_with(|| b_recency.cmp(a_recency))
                .then_with(|| {
                    // Within one source and one seqno, a scan yields a key's
                    // versions NEWEST first, and the read path reverses that run
                    // to apply them chronologically. Mirror it: same key ⇒ the
                    // later scan position replays FIRST. Distinct keys at one
                    // seqno touch different state, so their relative order is
                    // free — keep it deterministic by scan position.
                    //
                    // Identity is the engine's one relation (byte equality, see
                    // `same_user_key`), which is what the read path this mirrors
                    // uses to group a key's versions. Asking the comparator here
                    // would answer the same question — its contract makes
                    // `Equal` imply byte equality — while letting the two paths
                    // disagree the moment a comparator broke that.
                    if crate::comparator::same_user_key(a.key(), b.key()) {
                        b_pos.cmp(a_pos)
                    } else {
                        a_pos.cmp(b_pos)
                    }
                })
        });
        merged.into_iter().map(|(event, ..)| event).collect()
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
    /// # Concurrency
    ///
    /// The result is a snapshot of the tree as of the call: the active memtable
    /// is captured with writers excluded, so a batch committed concurrently is
    /// either wholly in the result or wholly absent, never split across it. This
    /// holds even for a caller-chosen sequence number at or below the reported
    /// watermark, which the seqno bound alone would not exclude. Writers are
    /// blocked only while that capture runs — the sealed memtables and SSTs the
    /// scan then reads are immutable.
    ///
    /// # Block-skip
    ///
    /// On SSTs written with the `seqno_bounds` section (`seqno_in_index`), data
    /// blocks whose bounds cannot overlap the target window are skipped without
    /// being read; SSTs without the section are read and filtered per entry, so
    /// mixed trees are handled transparently.
    ///
    /// # KV-separation
    ///
    /// Standard trees never store blob-indirected values. On the inner tree of
    /// a KV-separated (blob) tree this returns an `Err` for indirected entries:
    /// blob resolution into [`ScanSinceEvent::Insert`] is provided by the
    /// blob-tree scan path, which owns the blob files.
    ///
    /// # Corruption resilience
    ///
    /// The per-block seqno-bounds used for skipping live in the optional
    /// `seqno_bounds` SST section, a Block covered by XXH3-128 (+ optional Page
    /// ECC) and verified when it is loaded at open, plus a decode that rejects
    /// non-ascending offsets and inverted bounds, so a corrupted bound is caught
    /// rather than trusted. Even in the impossible case of a fault bypassing
    /// those checks, a bad bound can only cause a *missed* record, never a wrong
    /// one. Callers who want defense against that hypothetical can use
    /// [`Self::scan_since_seqno_full_scan`], which reads every block (slower, no
    /// skip).
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
        self.scan_since_seqno_with(target_seqno, true, |_version, _entry| {
            Err(crate::Error::FeatureUnsupported(
                "scan_since_seqno on KV-separated values requires the blob-tree scan path",
            ))
        })
    }

    /// Paranoid variant of [`Self::scan_since_seqno`] that disables the
    /// per-block seqno-bounds skip: every data block is read and filtered per
    /// entry, even on seqno-indexed SSTs.
    ///
    /// # When to use
    ///
    /// The fast [`Self::scan_since_seqno`] trusts each block's recorded
    /// `[seqno_min, seqno_max]` to skip blocks that cannot hold a qualifying
    /// record. Those bounds live in the `seqno_bounds` SST section, a Block
    /// covered by XXH3-128 (and optional Page ECC) and verified at open, so
    /// on-disk corruption is caught, not silently trusted. This method exists
    /// for callers who
    /// want defense even against a fault that somehow bypassed those checks: a
    /// corrupted `seqno_max` can only ever cause a *missed* record (never a
    /// wrong one), and a full scan cannot miss. It is slower (no skip), so
    /// prefer [`Self::scan_since_seqno`] unless you specifically need this
    /// guarantee.
    ///
    /// # Panics
    ///
    /// Panics if the internal version-history lock is poisoned.
    ///
    /// # Errors
    ///
    /// Same as [`Self::scan_since_seqno`].
    pub fn scan_since_seqno_full_scan(
        &self,
        target_seqno: SeqNo,
    ) -> crate::Result<impl Iterator<Item = ScanSinceEvent> + use<>> {
        self.scan_since_seqno_with(target_seqno, false, |_version, _entry| {
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
        // Drop the cached admission footprint so the next check re-probes
        // disk-free: an operator who just raised the budget (or freed disk)
        // should see it promptly, not at the next flush.
        *self.0.admission_used_cache.lock() = None;
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

    /// Like [`Tree::resolve_or_passthrough`], but returns a [`PinnableSlice`](crate::PinnableSlice)
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
    /// merge operand resolution. Delegates to [`Self::resolve_pinned_entry`] with
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

    /// De-duplicates equal query keys in a comparator-sorted `remaining` index
    /// list, returning the `(key_index, bloom_hash)` pairs for the batched
    /// on-disk resolver (which requires strictly-sorted-unique input) and a
    /// `(duplicate_index, representative_index)` map. Pair with
    /// [`Self::fan_out_duplicates`] after the batch resolves.
    ///
    /// Shared by [`Self::multi_get`] and the `BlobTree` multi-get so the two
    /// cannot silently diverge: forwarding duplicate miss keys into the
    /// strictly-sorted-unique resolver was exactly the regression class this
    /// guards against. `remaining` must already be sorted by `comparator`.
    #[expect(
        clippy::indexing_slicing,
        reason = "remaining/miss_keys carry batch-local key indices < keys.len()"
    )]
    pub(crate) fn dedup_sorted_miss_keys<K: AsRef<[u8]>>(
        remaining: &[usize],
        keys: &[K],
        comparator: &dyn crate::comparator::UserComparator,
    ) -> DedupedMissKeys {
        let mut miss_keys: Vec<(usize, u64)> = Vec::with_capacity(remaining.len());
        let mut duplicates: Vec<(usize, usize)> = Vec::new();
        for &idx in remaining {
            let key = keys[idx].as_ref();
            match miss_keys.last() {
                Some(&(rep_idx, _))
                    if comparator.compare(keys[rep_idx].as_ref(), key)
                        == core::cmp::Ordering::Equal =>
                {
                    duplicates.push((idx, rep_idx));
                }
                _ => miss_keys.push((idx, crate::hash::hash64(key))),
            }
        }
        (miss_keys, duplicates)
    }

    /// Fans each representative's resolved entry out to its duplicate positions,
    /// so every input slot carries the same answer the per-key path would have
    /// produced. Counterpart to [`Self::dedup_sorted_miss_keys`]; call after the
    /// batched resolver fills `internal_entries`.
    #[expect(
        clippy::indexing_slicing,
        reason = "duplicate/representative indices are batch-local key indices < entries.len()"
    )]
    pub(crate) fn fan_out_duplicates(
        duplicates: &[(usize, usize)],
        internal_entries: &mut [Option<InternalValue>],
    ) {
        for &(dup_idx, rep_idx) in duplicates {
            let resolved = internal_entries[rep_idx].clone();
            internal_entries[dup_idx] = resolved;
        }
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
        fs: &dyn crate::fs::Fs,
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

            // Warm the cold data blocks this level will read across ALL its SSTs
            // in one cross-file batched read, so the serial resolve below hits the
            // cache. On io_uring the reads coalesce into one submission and the
            // kernel fans them out across the underlying devices. When the cold
            // working set is too large to warm without thrashing the cache, this
            // signals oversize and warms nothing; the level is then resolved by
            // reading its blocks in budget-sized chunks into a scratch and
            // point-reading directly (no cache, no eviction).
            if Self::prewarm_level_cross_sst(fs, level, &still_remaining, keys, seqno, comparator)
                && Self::resolve_level_chunked(
                    fs,
                    level,
                    &mut still_remaining,
                    keys,
                    seqno,
                    comparator,
                    results,
                )?
            {
                continue;
            }

            if level_idx == 0 {
                // L0: must check ALL runs, keep highest seqno per key. Track keys
                // at the seqno ceiling (seqno + 1 == read_seqno): no other L0 run
                // can beat them, so skip them in subsequent runs. The bitmap is
                // dense over 0..keys.len().
                let mut at_ceiling = vec![false; keys.len()];

                for run in level.iter() {
                    // `at_ceiling` is read as this run's skip set (a key is visited
                    // once per run, so the updates below only affect later runs)
                    // and mutated from the returned outcomes: never both at once.
                    let resolved = Self::resolve_run_batched(
                        run,
                        &still_remaining,
                        keys,
                        seqno,
                        comparator,
                        |idx| at_ceiling[idx],
                    )?;
                    for (idx, _hash, item) in resolved.covered {
                        let Some(item) = item else { continue };
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
                    // Uncovered keys stay in `still_remaining`; the retain below
                    // prunes the ones any run resolved.
                }

                // Remove found keys (both values and tombstones)
                still_remaining.retain(|&(idx, _)| results[idx].is_none());
            } else {
                // L1+ runs have non-overlapping key ranges within a level. A
                // covering run resolves a key definitively: a hit sets the result,
                // a covering miss drops it to lower levels (`covered_miss`), and an
                // uncovered key tries the next run in this level (`not_covered`).
                let mut covered_miss: Vec<(usize, u64)> = Vec::new();

                for run in level.iter() {
                    let resolved = Self::resolve_run_batched(
                        run,
                        &still_remaining,
                        keys,
                        seqno,
                        comparator,
                        |_| false,
                    )?;
                    for (idx, hash, item) in resolved.covered {
                        if let Some(item) = item {
                            results[idx] = Some(item);
                        } else {
                            // Covering run found, key absent: no other run in this
                            // level can have it. Keep for lower levels.
                            covered_miss.push((idx, hash));
                        }
                    }
                    still_remaining = resolved.not_covered;
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

    /// Resolves `remaining` (sorted ascending under `comparator`) against a
    /// single run with per-table batched gets instead of a per-key `table.get`:
    /// consecutive keys covered by the same table within the run share one
    /// [`Table::batch_get`], so co-located keys decode their data block once.
    /// Byte-identical to per-key resolution (the same point reads, the same
    /// values). `skip(idx)` omits a key (e.g. one already pinned at the L0 seqno
    /// ceiling, where no later run can beat it).
    ///
    /// Returns, per covered non-skipped key, `(idx, hash, resolved item)` in
    /// input order, plus the keys this run does not cover (also in input order)
    /// for the caller to pass to the next run or level.
    #[expect(
        clippy::indexing_slicing,
        reason = "i < remaining.len() is loop-checked; idx values are valid key indices (caller's keys/results are aligned, same as batch_get_from_tables)"
    )]
    fn resolve_run_batched<K: AsRef<[u8]>>(
        run: &crate::version::Run<crate::Table>,
        remaining: &[(usize, u64)],
        keys: &[K],
        seqno: SeqNo,
        comparator: &dyn crate::comparator::UserComparator,
        skip: impl Fn(usize) -> bool,
    ) -> crate::Result<RunResolve> {
        let mut covered: Vec<CoveredKey> = Vec::new();
        let mut not_covered: Vec<(usize, u64)> = Vec::new();

        let mut i = 0;
        while i < remaining.len() {
            let (idx, hash) = remaining[i];
            if skip(idx) {
                i += 1;
                continue;
            }
            let key = keys[idx].as_ref();
            let Some(table) = run.get_for_key_cmp(key, comparator) else {
                not_covered.push((idx, hash));
                i += 1;
                continue;
            };

            // Gather the contiguous, non-skipped keys covered by THIS table. The
            // input is sorted and a run's tables partition the key space, so the
            // keys for one table form a contiguous slice; one `batch_get` drains
            // them with a single block decode for co-located keys.
            let table_id = table.id();
            let mut batch: Vec<(&[u8], u64)> = Vec::new();
            let mut batch_keys: Vec<(usize, u64)> = Vec::new();
            while i < remaining.len() {
                let (jdx, jhash) = remaining[i];
                if skip(jdx) {
                    i += 1;
                    continue;
                }
                let jkey = keys[jdx].as_ref();
                match run.get_for_key_cmp(jkey, comparator) {
                    Some(t) if t.id() == table_id => {
                        batch.push((jkey, jhash));
                        batch_keys.push((jdx, jhash));
                        i += 1;
                    }
                    _ => break,
                }
            }

            for ((kidx, khash), item) in batch_keys.into_iter().zip(table.batch_get(&batch, seqno)?)
            {
                covered.push((kidx, khash, item));
            }
        }

        Ok(RunResolve {
            covered,
            not_covered,
        })
    }

    /// Warms an entire level's COLD data blocks across ALL its SSTs in one
    /// cross-file batched read ([`crate::fs::Fs::read_blocks_batched`]), so the
    /// serial resolve walk that follows hits the cache. On `io_uring` the reads of
    /// many SSTs (and, on a multi-device filesystem, many physical devices)
    /// coalesce into one submission and overlap in flight.
    ///
    /// Purely best-effort: it never changes a query result (the resolve walk
    /// re-reads authoritatively), and it is size-bounded to at most half the
    /// shared cache so the warmed blocks survive until the walk reads them.
    ///
    /// Returns `true` when the level's cold working set EXCEEDS that half-cache
    /// bound: warming would thrash the cache, so nothing is warmed and the caller
    /// resolves the level with the chunked read-into-scratch path instead
    /// ([`Tree::resolve_level_chunked`]). Returns `false` when it warmed the
    /// blocks (or had nothing to warm), i.e. the serial resolve should run.
    #[expect(
        clippy::indexing_slicing,
        reason = "planned[ti] and all_buffers[k..end] indices are built from `planned` itself, so they are in range by construction"
    )]
    fn prewarm_level_cross_sst<K: AsRef<[u8]>>(
        fs: &dyn crate::fs::Fs,
        level: &crate::version::Level,
        remaining: &[(usize, u64)],
        keys: &[K],
        seqno: SeqNo,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> bool {
        // Gather per-table prewarm plans across the level's runs (group remaining
        // keys by covering table, mirroring resolve_run_batched's walk).
        let mut planned: Vec<(
            &crate::Table,
            Arc<dyn crate::fs::FsFile>,
            Vec<crate::table::BlockHandle>,
        )> = Vec::new();
        for run in level.iter() {
            let mut i = 0;
            while i < remaining.len() {
                let (idx, _) = remaining[i];
                let key = keys[idx].as_ref();
                let Some(table) = run.get_for_key_cmp(key, comparator) else {
                    i += 1;
                    continue;
                };
                let table_id = table.id();
                let mut batch: Vec<(&[u8], u64)> = Vec::new();
                while i < remaining.len() {
                    let (jdx, jhash) = remaining[i];
                    let jkey = keys[jdx].as_ref();
                    match run.get_for_key_cmp(jkey, comparator) {
                        Some(t) if t.id() == table_id => {
                            batch.push((jkey, jhash));
                            i += 1;
                        }
                        _ => break,
                    }
                }
                if let Some((file, handles)) = table.plan_prewarm(&batch, seqno) {
                    planned.push((table, file, handles));
                }
            }
        }

        let total_cold: usize = planned.iter().map(|(_, _, h)| h.len()).sum();
        if total_cold < 2 {
            return false;
        }
        // Eviction-avoiding bound: warm at most half the (shared) cache.
        let Some((first_table, _, _)) = planned.first() else {
            return false;
        };
        let cap = first_table.cache_capacity();
        let total_bytes: u64 = planned
            .iter()
            .flat_map(|(_, _, h)| h.iter().map(|x| u64::from(x.size())))
            .sum();
        if cap == 0 {
            return false;
        }
        if total_bytes > cap / 2 {
            // Cold working set too large to warm without thrash: signal the caller
            // to resolve this level via the chunked read-into-scratch path.
            return true;
        }

        // One buffer per cold block, in (table, block) order.
        let mut all_buffers: Vec<Vec<u8>> = planned
            .iter()
            .flat_map(|(_, _, handles)| handles.iter().map(|h| vec![0u8; h.size() as usize]))
            .collect();
        // (table index, offset) per buffer, so each request borrows the right file.
        let flat: Vec<(usize, u64)> = planned
            .iter()
            .enumerate()
            .flat_map(|(ti, (_, _, handles))| handles.iter().map(move |h| (ti, *h.offset())))
            .collect();

        {
            let mut reqs: Vec<crate::fs::BlockRead<'_>> = flat
                .iter()
                .zip(all_buffers.iter_mut())
                .map(|(&(ti, offset), buf)| crate::fs::BlockRead {
                    file: planned[ti].1.as_ref(),
                    offset,
                    buf: buf.as_mut_slice(),
                })
                .collect();
            // Best-effort: a batched-read failure just leaves the blocks for the
            // resolve walk to read normally.
            if fs.read_blocks_batched(&mut reqs).is_err() {
                return false;
            }
        }

        // Decode each table's blocks (its contiguous slice of all_buffers).
        let mut k = 0;
        for (table, _, handles) in &planned {
            let end = k + handles.len();
            table.decode_prewarmed(handles, &all_buffers[k..end]);
            k = end;
        }
        false
    }

    /// Plans every data block this level's SSTs will read for `remaining`,
    /// grouping keys by covering table per run (mirrors `resolve_run_batched`'s
    /// walk). Each task carries the ORIGINAL key indices (into `keys`).
    ///
    /// # Errors
    ///
    /// Propagates a table-side planning failure ([`Table::plan_block_tasks`]) so
    /// the resolver surfaces it instead of letting a stale lower level answer.
    #[expect(
        clippy::indexing_slicing,
        reason = "i < remaining.len() loop-checked; idx/jdx are valid key indices; batch_idx[pos] is in range (pos came from this table's own plan)"
    )]
    fn plan_level_block_tasks<'a, K: AsRef<[u8]>>(
        level: &'a crate::version::Level,
        remaining: &[(usize, u64)],
        keys: &[K],
        seqno: SeqNo,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Vec<BlockTask<'a>>> {
        let mut tasks: Vec<BlockTask<'a>> = Vec::new();
        for run in level.iter() {
            let mut i = 0;
            while i < remaining.len() {
                let (idx, _) = remaining[i];
                let key = keys[idx].as_ref();
                let Some(table) = run.get_for_key_cmp(key, comparator) else {
                    i += 1;
                    continue;
                };
                let table_id = table.id();
                let mut batch: Vec<(&[u8], u64)> = Vec::new();
                let mut batch_idx: Vec<usize> = Vec::new();
                while i < remaining.len() {
                    let (jdx, jhash) = remaining[i];
                    let jkey = keys[jdx].as_ref();
                    match run.get_for_key_cmp(jkey, comparator) {
                        Some(t) if t.id() == table_id => {
                            batch.push((jkey, jhash));
                            batch_idx.push(jdx);
                            i += 1;
                        }
                        _ => break,
                    }
                }
                if let Some((file, table_seqno, special, blocks)) =
                    table.plan_block_tasks(&batch, seqno)?
                {
                    for (handle, positions) in blocks {
                        let task_keys: Vec<usize> =
                            positions.iter().map(|&pos| batch_idx[pos]).collect();
                        tasks.push(BlockTask {
                            table,
                            file: Arc::clone(&file),
                            handle,
                            table_seqno,
                            special,
                            keys: task_keys,
                        });
                    }
                }
            }
        }
        Ok(tasks)
    }

    /// Resolves an ENTIRE level by reading its blocks in chunks into a scratch and
    /// point-reading directly (no cache, no eviction). Called after
    /// [`Tree::prewarm_level_cross_sst`] signals the cold working set is too large
    /// to warm. Returns `Ok(true)` when it resolved the level (results updated,
    /// found keys dropped from `still_remaining`); `Ok(false)` when the level has
    /// no blocks to read for this batch (every key bloom-skips) or holds a
    /// Page-ECC / columnar table, in which cases the caller falls through to the
    /// serial resolve.
    #[expect(
        clippy::indexing_slicing,
        reason = "start/end stay within tasks by construction"
    )]
    fn resolve_level_chunked<K: AsRef<[u8]>>(
        fs: &dyn crate::fs::Fs,
        level: &crate::version::Level,
        still_remaining: &mut Vec<(usize, u64)>,
        keys: &[K],
        seqno: SeqNo,
        comparator: &dyn crate::comparator::UserComparator,
        results: &mut [Option<InternalValue>],
    ) -> crate::Result<bool> {
        let tasks = Self::plan_level_block_tasks(level, still_remaining, keys, seqno, comparator)?;
        let Some(first) = tasks.first() else {
            return Ok(false);
        };
        // A Page-ECC / columnar table covers some of these keys (only possible
        // when the columnar/ECC policy differs between the SSTs in this level).
        // The scratch decode path is row-format only, so hand the whole level to
        // the serial resolve, which loads those blocks through their format-aware
        // path. The scratch fast path stays homogeneous and row-only.
        if tasks.iter().any(|t| t.special) {
            return Ok(false);
        }
        // Read blocks in chunks of at most half the shared cache, so a chunk's
        // scratch never dwarfs the cache it is meant to spare. `.max(1)` keeps the
        // chunk loop's `end > start` guard the sole progress condition when the
        // cache is disabled (capacity 0).
        let budget = (first.table.cache_capacity() / 2).max(1);

        let mut start = 0;
        while start < tasks.len() {
            let mut bytes = 0u64;
            let mut end = start;
            while end < tasks.len() {
                let sz = u64::from(tasks[end].handle.size());
                if end > start && bytes + sz > budget {
                    break;
                }
                bytes += sz;
                end += 1;
            }
            Self::resolve_block_task_chunk(fs, &tasks[start..end], keys, results)?;
            start = end;
        }
        still_remaining.retain(|&(idx, _)| results[idx].is_none());
        Ok(true)
    }

    /// Reads one chunk of block-tasks in ONE cross-file `read_blocks_batched`,
    /// decodes each from its scratch buffer, and point-reads its keys, keeping the
    /// highest-seqno hit per key in `results`. Every task is row-format (the caller
    /// routes any level with a Page-ECC / columnar table to the serial resolve).
    #[expect(
        clippy::indexing_slicing,
        reason = "buffers is built from chunk so indices align; key indices are valid (caller's keys/results aligned)"
    )]
    fn resolve_block_task_chunk<K: AsRef<[u8]>>(
        fs: &dyn crate::fs::Fs,
        chunk: &[BlockTask<'_>],
        keys: &[K],
        results: &mut [Option<InternalValue>],
    ) -> crate::Result<()> {
        let mut buffers: Vec<Vec<u8>> = chunk
            .iter()
            .map(|t| vec![0u8; t.handle.size() as usize])
            .collect();
        {
            let mut reqs: Vec<crate::fs::BlockRead<'_>> = chunk
                .iter()
                .zip(buffers.iter_mut())
                .map(|(t, buf)| crate::fs::BlockRead {
                    file: t.file.as_ref(),
                    offset: *t.handle.offset(),
                    buf: buf.as_mut_slice(),
                })
                .collect();
            fs.read_blocks_batched(&mut reqs)?;
        }

        for (task, buf) in chunk.iter().zip(buffers.iter()) {
            if let Some(block) = task.table.decode_data_block_from_bytes(buf)? {
                for &kidx in &task.keys {
                    if let Some(item) = task.table.point_read_translated(
                        &block,
                        keys[kidx].as_ref(),
                        task.table_seqno,
                    )? {
                        Self::keep_highest(results, kidx, item);
                    }
                }
            }
        }
        Ok(())
    }

    /// Keeps the higher-seqno of an existing result and a new candidate (the L0
    /// newest-version-wins merge; correct for L1+ too, where each key has one
    /// candidate).
    #[expect(
        clippy::indexing_slicing,
        reason = "idx is a valid key index aligned with results"
    )]
    fn keep_highest(results: &mut [Option<InternalValue>], idx: usize, item: InternalValue) {
        match &results[idx] {
            Some(current) if current.key.seqno >= item.key.seqno => {}
            _ => results[idx] = Some(item),
        }
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
    /// After recovering a previous state, use `Tree::set_active_memtable`
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
            // Literal discriminant: V1 is a retired format and FormatVersion
            // carries no legacy variants (V5-only contract).
            return Err(crate::Error::InvalidVersion(1));
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

    /// Computed storage admission predicate backing
    /// [`AbstractTree::write_admission`].
    ///
    /// Cheap: reads in-memory size accounting only (no syscall). Returns
    /// `Ok(())` unless admission control is enabled AND a budget is set AND the
    /// live footprint plus reserved headroom exceeds it.
    /// Best-effort minimum free space across every filesystem this tree writes
    /// to: the primary data path AND each per-level route (`Config::level_routes`
    /// can place cold-level SSTs on separate volumes). The admission gate must
    /// reflect the tightest volume, since a full routed disk fails compaction /
    /// flush targeting it even while the primary still has room.
    ///
    /// A backend that cannot report free space (or an I/O hiccup) yields
    /// `u64::MAX` = "no disk pressure", so a probe failure never falsely drives
    /// the tree read-only.
    fn probe_disk_free(&self) -> u64 {
        self.0.config.min_available_space()
    }

    /// Disk-aware capacity figures for [`AbstractTree::storage_stats`], given the
    /// live footprint `used`: `(capacity, available, compaction_possible)`.
    ///
    /// `capacity` is the tighter of the configured quota and the physical disk
    /// headroom (`free + used`) — the same effective limit
    /// [`Self::compute_write_admission`] gates against — reported regardless of
    /// whether the admission gate is enabled (introspection is always available).
    /// `None` capacity/available means unbounded (no quota AND the backend
    /// cannot report free space). `compaction_possible` is `true` when unbounded
    /// or when at least [`MIN_RESERVED_HEADROOM`] of working room remains.
    pub(crate) fn admission_capacity(&self, used: u64) -> (Option<u64>, Option<u64>, bool) {
        let quota = self
            .0
            .runtime_config
            .load()
            .storage_limit_bytes
            .unwrap_or(u64::MAX);
        let free = self.probe_disk_free();
        // `free == u64::MAX` is the "backend can't report free space" sentinel:
        // adding `used` would overflow, so treat capacity as quota-only (the
        // explicit branch avoids the overflow without masking it with saturation).
        // Otherwise `free + used` ≤ ~2× disk capacity and cannot overflow u64.
        let capacity = if free == u64::MAX {
            quota
        } else {
            quota.min(free + used)
        };
        if capacity == u64::MAX {
            return (None, None, true);
        }
        // `available = max(0, capacity - used)`: an operator quota set below the
        // live footprint makes `capacity < used`, and available space cannot be
        // negative. The clamp-to-zero IS the intended semantics here.
        let available = capacity.saturating_sub(used);
        (
            Some(capacity),
            Some(available),
            available >= MIN_RESERVED_HEADROOM,
        )
    }

    /// The logical partition-quota headroom for the two-layer space model:
    /// `max(0, storage_limit_bytes - used)`, or `u64::MAX` when no quota is set.
    ///
    /// This is Layer 1 (volume-agnostic) of [`crate::compaction::worker::space_fits_two_layer`];
    /// the physical free-space probe is Layer 2. An operator quota set below the
    /// live footprint leaves zero headroom — the clamp-to-zero is the intended
    /// min-semantics, not masking.
    pub(crate) fn quota_headroom(&self, used: u64) -> u64 {
        self.0
            .runtime_config
            .load()
            .storage_limit_bytes
            .map_or(u64::MAX, |limit| limit.saturating_sub(used))
    }

    /// Whether the opt-in storage admission gate is active (a near-full disk or
    /// configured quota can drive the tree read-only and gate compaction space).
    /// Capacity introspection figures are reported regardless; this only governs
    /// whether the gate actually enforces.
    pub(crate) fn storage_admission_enabled(&self) -> bool {
        self.0.runtime_config.load().storage_admission_check
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the admission cache lock intentionally spans the recompute \
                  (stat + disk-free probe) so concurrent admission checks \
                  coalesce on a single probe rather than each issuing a syscall"
    )]
    fn compute_write_admission(&self) -> crate::Result<()> {
        let rc = self.0.runtime_config.load();
        if !rc.storage_admission_check {
            return Ok(());
        }

        // Take ONE coherent snapshot of the latest super-version and derive
        // BOTH the on-disk footprint and the pending-memtable bytes from it.
        // Reading them from two separate `latest_version()` loads would be a
        // TOCTOU bug: a flush installing a new version between the two reads
        // could pair an old (larger) disk usage with new (smaller) pending
        // bytes — or vice versa — and open the gate incorrectly.
        let super_version = self.version_history.read().latest_version();
        let vid = super_version.version.id();

        // True physical footprint, including blob files — the SAME basis
        // `storage_stats()` reports, so the gate and the reported usage agree.
        // NOT `disk_space()` (metadata Level::size, which omits blob files and
        // undercounts the physical file by the meta block / footer).
        //
        // Cached so gated writes don't re-stat every live file or re-probe disk
        // on every call. `used_bytes` only changes when a new version is
        // installed (flush / compaction), so it is recomputed on a version
        // change. `disk_free` can change under us (another process writing the
        // same filesystem), so it is ALSO re-probed once its sample is older
        // than `ADMISSION_DISK_FREE_TTL` — bounding staleness without a syscall
        // per write. `update_runtime_config` resets the entry for an immediate
        // re-probe. The values live behind one mutex as a coherent unit (see
        // `TreeInner::admission_used_cache`).
        //
        // The TTL fast-path is std-only: under `no_std` there is no monotonic
        // clock (`crate::time::Instant::elapsed` is a zero stub), so an
        // elapsed-time window cannot bound staleness — a same-version sample
        // would otherwise look fresh forever and a filling disk would never be
        // re-probed. Under `no_std` the fast-path is skipped, so `disk_free` is
        // re-probed on every gated write (the `used` footprint stays cached by
        // version either way), keeping admission safe without a monotonic clock.
        let now = crate::time::Instant::now();
        let (used, disk_free) = {
            let mut cache = self.0.admission_used_cache.lock();
            match *cache {
                // Fresh: same version AND disk sample within the TTL. std-only —
                // `cfg!(feature = "std")` is `false` under `no_std`, so the guard
                // short-circuits there and the next arm re-probes every call.
                Some((cvid, used, free, at))
                    if cvid == vid
                        && cfg!(feature = "std")
                        && at.elapsed() < ADMISSION_DISK_FREE_TTL =>
                {
                    (used, free)
                }
                // Same version, stale disk sample: keep `used`, re-probe disk.
                Some((cvid, used, _, _)) if cvid == vid => {
                    let free = self.probe_disk_free();
                    *cache = Some((vid, used, free, now));
                    (used, free)
                }
                // New version (or unset): recompute footprint and re-probe disk.
                _ => {
                    let used = crate::storage_stats::compute_used_bytes(&super_version.version)?;
                    let free = self.probe_disk_free();
                    *cache = Some((vid, used, free, now));
                    (used, free)
                }
            }
        };

        // Effective limit is the tighter of the configured quota and the
        // physical disk headroom (free + what we already occupy): the disk can
        // fill from other processes even below a generous quota, and a tree with
        // no quota at all must still stop before ENOSPC. `None` quota = unbounded
        // by configuration; disk-free then alone bounds it.
        //
        // `disk_free` is the MINIMUM free across every volume the tree writes to
        // (`probe_disk_free` mins the primary path and all `level_routes`). The
        // `+ used` here is NOT an accounting of one volume's usage against
        // another's free space — it cancels out of the disk branch of the gate:
        // passing requires `used + reserved <= disk_free + used`, i.e.
        // `reserved <= disk_free`. So a passing gate guarantees the TIGHTEST
        // volume alone has at least `reserved` free — a conservative per-volume
        // headroom, never the sum of an empty routed volume's slack plus an
        // unrelated full volume's occupancy. A route that drops below `reserved`
        // free drives the whole tree read-only, exactly so a later flush /
        // compaction targeting that route cannot hit ENOSPC.
        let quota = rc.storage_limit_bytes.unwrap_or(u64::MAX);
        // `disk_free == u64::MAX` is the "backend can't report" sentinel; adding
        // `used` would overflow, so treat the limit as quota-only (explicit
        // branch, no saturation masking). Otherwise `disk_free + used` ≤ ~2× disk
        // capacity and cannot overflow u64.
        let limit = if disk_free == u64::MAX {
            quota
        } else {
            quota.min(disk_free + used)
        };
        // Both sources unbounded → nothing to gate.
        if limit == u64::MAX {
            return Ok(());
        }

        // Reserved headroom keeps the soft budget from becoming a hard wall:
        // enough to flush every pending memtable (plus a margin for the
        // index/filter/footer overhead a flush adds) so a queued flush always
        // fits at the limit, with a floor for compaction working space.
        // Internal flush / compaction are never gated, so this band is the
        // engine's room to reclaim.
        //
        // Count ALL pending memtable bytes in this snapshot — the active one AND
        // any sealed (rotated) memtables awaiting flush — not just the active
        // one: after a rotation the active memtable is empty but the sealed
        // memtable's queued flush will still consume disk, so it must be
        // reserved for. Memtable sizes are bounded by RAM, so the sum (and the
        // +1/8 overhead margin below) cannot overflow u64 → plain arithmetic.
        let pending_memtable_bytes: u64 = super_version.active_memtable.size()
            + super_version
                .sealed_memtables
                .iter()
                .map(|m| m.size())
                .sum::<u64>();

        let reserved =
            (pending_memtable_bytes + pending_memtable_bytes / 8).max(MIN_RESERVED_HEADROOM);
        // `used` (disk) + `reserved` (RAM-bounded) cannot realistically overflow,
        // but keep the comparison fail-closed with checked arithmetic: any
        // overflow means "definitely over budget", so deny.
        match used.checked_add(reserved) {
            Some(total) if total <= limit => Ok(()),
            _ => Err(crate::Error::StorageFull { used, limit }),
        }
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

    /// Build a [`SeekableTreeIter`](crate::range::SeekableTreeIter) over
    /// `[lo, hi)`. Source collection (Phase 1) runs once; repositions reuse it.
    #[doc(hidden)]
    #[must_use]
    pub fn create_seekable_range_bounds(
        &self,
        lo: Bound<UserKey>,
        hi: Bound<UserKey>,
        seqno: SeqNo,
        ephemeral: Option<(Arc<Memtable>, SeqNo)>,
    ) -> crate::range::SeekableTreeIter {
        use crate::range::{IterState, SeekableTreeIter};

        let super_version = self.version_history.read().get_version_for_snapshot(seqno);

        let iter_state = IterState {
            version: super_version,
            ephemeral,
            merge_operator: self.config.merge_operator.clone(),
            comparator: self.config.comparator.clone(),
            prefix_hash: None,
            key_hash: None,
            bloom_key: None,
            #[cfg(feature = "metrics")]
            metrics: Some(self.0.metrics.clone()),
        };

        SeekableTreeIter::create(iter_state, lo, hi, seqno)
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
        use crate::runtime_config::{KvChecksumComputePoint, KvChecksumPolicy};

        // Per-KV residence digest (KvChecksumComputePoint::AtInsert): compute
        // the entry's 4-byte logical-content digest now, so a RAM bit-flip
        // while it sits in the memtable is caught at flush. The digest covers
        // the OWNED `value` and is independent of which active memtable
        // receives it, so computing it before taking the version-history guard
        // is correct (a concurrent rotation just routes the same value+digest
        // into the new active memtable) AND keeps the hash out of the read-lock
        // critical section. Reading the live snapshot is a cheap arc-swap load;
        // under the default `AtBlockCompile` (or `Off`) the two `matches!`
        // checks short-circuit and no digest is computed, so the hot insert
        // path is unchanged.
        let kv_digest = {
            let rc = self.0.runtime_config.load();
            if matches!(
                rc.kv_checksum_compute_point,
                KvChecksumComputePoint::AtInsert
            ) && !matches!(rc.kv_checksums, KvChecksumPolicy::Off)
            {
                crate::table::block::kv_checksum::kv_digest(&value, rc.kv_checksum_algo).map(|d| {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "AtInsert is config-validated to a 4-byte algorithm; the digest fits u32"
                    )]
                    let lo = d as u32;
                    (lo, rc.kv_checksum_algo)
                })
            } else {
                None
            }
        };

        // The `.read()` guard is a temporary that lives until the end of this
        // statement, so the insert runs under the version-history read lock:
        // `value` + its digest land in the current active memtable atomically,
        // and a concurrent `rotate_memtable()` cannot seal it mid-insert.
        self.version_history
            .read()
            .latest_version()
            .active_memtable
            .insert_with_kv_digest(value, kv_digest)
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
        use crate::runtime_config::{KvChecksumComputePoint, KvChecksumPolicy};

        // Per-KV residence digest under AtInsert (see `append_entry`): pass the
        // algorithm so the bulk path fixes each entry's digest at insert. The
        // default path passes `None` and is unchanged.
        let kv_algo = {
            let rc = self.0.runtime_config.load();
            if matches!(
                rc.kv_checksum_compute_point,
                KvChecksumComputePoint::AtInsert
            ) && !matches!(rc.kv_checksums, KvChecksumPolicy::Off)
            {
                Some(rc.kv_checksum_algo)
            } else {
                None
            }
        };

        // Hold the read guard for the entire insert to prevent rotate_memtable()
        // from sealing this memtable mid-batch (which could cause data loss if
        // a concurrent flush persists only a prefix of the batch).
        self.version_history
            .read()
            .latest_version()
            .active_memtable
            .insert_batch_with_kv_algo(items, kv_algo)
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

            // V5 is the only variant `FormatVersion` can decode to (the
            // engine reads exactly one on-disk format, no legacy paths), so
            // a pre-V5 manifest already failed decode_from with
            // InvalidVersion. This match stays as the explicit gate the
            // format contract documents — and as the compile-time hook that
            // forces a review of the open path when a new variant is added.
            match manifest.version {
                FormatVersion::V5 => {}
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
            admission_used_cache: Mutex::new(None),

            #[cfg(feature = "metrics")]
            metrics,

            #[cfg(test)]
            test_hooks: inner::TestHooks::default(),
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

        let sinks = crate::table::TableSinks {
            deletion_pause: &deletion_pause,
            heal_hints: &heal_hints,
            #[cfg(feature = "std")]
            background_deleter: Some(&background_deleter),
        };
        for table in &recovered_tables {
            table.bind_to_tree(&sinks);
        }
        for blob_file in &recovered_blobs {
            blob_file.bind_to_tree(&sinks);
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

        // Immutable snapshot of every table id the manifest knows. `table_map`
        // is drained as tables are recovered below, so it cannot answer "is this
        // id live?" order-independently; this set can. Used to sweep an orphaned
        // `.heal-attest` sidecar whose SST was retired.
        let manifest_ids: crate::HashSet<TableId> = table_map.keys().copied().collect();

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

            // Pending repair swaps resolve FIRST: a committed swap's `{id}` file
            // is the superseded source until the rename lands, so adopting it
            // before the temp entry is processed would hand this session a
            // handle onto bytes the finished swap then replaces on disk.
            let mut dirents = folder_fs.read_dir(table_base_folder)?;
            dirents.sort_by_key(|e| {
                !matches!(
                    crate::file::TableDirEntry::classify(&e.file_name),
                    crate::file::TableDirEntry::RepairTmp(_)
                )
            });
            for dirent in dirents {
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

                // One grammar decides what each name IS (`TableDirEntry`, shared
                // with the repair scan so the two can never disagree on
                // ownership); this match is the open's POLICY for each kind.
                let table_id = match crate::file::TableDirEntry::classify(table_file_name) {
                    // An in-place heal's detach copy, renamed over the live path
                    // on success: a survivor is a crash leftover no manifest ever
                    // references. Sweep it.
                    crate::file::TableDirEntry::HealTmp(_) => {
                        log::warn!(
                            "Removing abandoned heal copy: {}",
                            table_file_path.display()
                        );
                        Self::sweep_artifact(folder_fs.as_ref(), &table_file_path)?;
                        continue;
                    }
                    // A crashed attestation publish: either the live sidecar it
                    // would have replaced still bridges the crash window, or the
                    // heal is re-run. Disposable.
                    crate::file::TableDirEntry::HealAttestTmp(_) => {
                        log::warn!(
                            "Removing abandoned heal-attest temp: {}",
                            table_file_path.display()
                        );
                        Self::sweep_artifact(folder_fs.as_ref(), &table_file_path)?;
                        continue;
                    }
                    // A LIVE table's pending attestation is preserved (the next
                    // scrub reconciles a crashed digest refresh through it). One
                    // whose SST left the manifest is unreconcilable forever:
                    // sweep the orphan rather than re-process it on every open.
                    crate::file::TableDirEntry::HealAttest(attest_id) => {
                        if !manifest_ids.contains(&attest_id) {
                            log::warn!(
                                "Removing orphaned heal attestation (its table is gone): {}",
                                table_file_path.display()
                            );
                            Self::sweep_artifact(folder_fs.as_ref(), &table_file_path)?;
                        }
                        continue;
                    }
                    // A crashed bound publish. Disposable.
                    crate::file::TableDirEntry::RestrictBoundTmp(_) => {
                        log::warn!(
                            "Removing abandoned restrict-bound temp: {}",
                            table_file_path.display()
                        );
                        Self::sweep_artifact(folder_fs.as_ref(), &table_file_path)?;
                        continue;
                    }
                    // A LIVE table's restriction bound is preserved (manifest
                    // repair reads it); an ORPHAN one is swept so a reused id
                    // cannot later pick up a stale restriction.
                    crate::file::TableDirEntry::RestrictBound(bound_id) => {
                        if !manifest_ids.contains(&bound_id) {
                            log::warn!(
                                "Removing orphaned restrict-bound sidecar (its table is gone): {}",
                                table_file_path.display()
                            );
                            Self::sweep_artifact(folder_fs.as_ref(), &table_file_path)?;
                        }
                        continue;
                    }
                    // A repair's replacement still at its temp name. The manifest
                    // is the authority on what it is — but it names the id in
                    // BOTH crash cases (before the commit its entry still
                    // describes the SOURCE beside the temp), so the entry's
                    // checksum decides: only a committed repair recorded the
                    // temp's digest, and it died before the swap — this file is
                    // what the manifest describes, so the swap is finished.
                    // Any other temp is an abandoned build (possibly truncated
                    // mid-write), and swapping it in would destroy the source
                    // the manifest names: it is garbage. An open resolves this
                    // exactly as a re-run of the repair would, from the durable
                    // manifest alone.
                    crate::file::TableDirEntry::RepairTmp(tmp_id) => {
                        #[cfg(feature = "std")]
                        {
                            let published = match table_map.get(&tmp_id) {
                                Some(&(_, manifest_checksum, _)) => {
                                    crate::repair::repair_tmp_is_published(
                                        config,
                                        folder_fs,
                                        &table_file_path,
                                        tmp_id,
                                        manifest_checksum,
                                        recovery.restrictions.get(&tmp_id),
                                    )?
                                }
                                None => false,
                            };
                            if published {
                                log::warn!(
                                    "Finishing a repair's pending swap of table {tmp_id}: {}",
                                    table_file_path.display()
                                );
                                crate::repair::commit_repair_tmp(
                                    folder_fs.as_ref(),
                                    &table_file_path,
                                    &table_base_folder.join(tmp_id.to_string()),
                                    config.sync_mode,
                                    recovery.restrictions.contains_key(&tmp_id),
                                )?;
                            } else {
                                log::warn!(
                                    "Removing abandoned repair replacement: {}",
                                    table_file_path.display()
                                );
                                Self::sweep_artifact(folder_fs.as_ref(), &table_file_path)?;
                            }
                        }
                        // Without the repair module nothing here can verify or
                        // finish a swap the manifest may describe, and sweeping
                        // the temp could destroy the only copy of what the
                        // manifest names.
                        #[cfg(not(feature = "std"))]
                        {
                            if manifest_ids.contains(&tmp_id) {
                                log::error!(
                                    "Table {tmp_id} exists only as an unpublished repair \
                                     replacement; run a repair to finish it: {}",
                                    table_file_path.display()
                                );
                                return Err(crate::Error::Unrecoverable);
                            }
                            log::warn!(
                                "Removing abandoned repair replacement: {}",
                                table_file_path.display()
                            );
                            Self::sweep_artifact(folder_fs.as_ref(), &table_file_path)?;
                        }
                        continue;
                    }
                    crate::file::TableDirEntry::Foreign => {
                        log::error!("invalid table file name {table_file_name:?}");
                        return Err(crate::Error::Unrecoverable);
                    }
                    crate::file::TableDirEntry::Table(id) => id,
                };

                // Remove from map to prevent duplicate recovery if the same
                // table file exists in multiple scanned folders.
                if let Some((level_idx, checksum, global_seqno)) = table_map.remove(&table_id) {
                    let pin_filter = config.filter_block_pinning_policy.get(level_idx.into());
                    let pin_index = config.index_block_pinning_policy.get(level_idx.into());

                    let table = {
                        let mut params = crate::table::RecoverParams::new(
                            table_file_path,
                            checksum,
                            table_id,
                            folder_fs.clone(),
                            config.comparator.clone(),
                            config.cache.clone(),
                        );
                        params.global_seqno = global_seqno;
                        params.tree_id = tree_id;
                        params.descriptor_table.clone_from(&config.descriptor_table);
                        params.pin_filter = pin_filter;
                        params.pin_index = pin_index;
                        params.encryption.clone_from(&config.encryption);
                        #[cfg(zstd_any)]
                        {
                            params.zstd_dictionary.clone_from(&config.zstd_dictionary);
                        }
                        #[cfg(feature = "metrics")]
                        {
                            params.metrics = metrics.clone();
                        }
                        Table::recover(params)?
                    };

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

        // Pair each blob file with its live-data frontier: a file whose consumed
        // prefix was reclaimed in place records a checksum over the suffix only,
        // so the recovered view must carry the offset that digest starts at.
        let blob_ids_with_frontier: Vec<(crate::vlog::BlobFileId, crate::Checksum, u64)> = recovery
            .blob_file_ids
            .iter()
            .map(|&(id, checksum)| {
                (
                    id,
                    checksum,
                    recovery.blob_restrictions.get(&id).copied().unwrap_or(0),
                )
            })
            .collect();
        let (blob_files, orphaned_blob_files) = crate::vlog::recover_blob_files(
            &tree_path.join(crate::file::BLOBS_FOLDER),
            &blob_ids_with_frontier,
            tree_id,
            config.descriptor_table.as_ref(),
            &config.fs,
        )?;

        let version = Version::from_recovery(recovery, &tables, &blob_files)?;

        // Republish any restriction sidecar that never landed. The sidecar is
        // written AFTER its slice commits, so a failure (or a crash) in that
        // window leaves a committed restriction with no `.restrict-bound` file.
        // The manifest still describes it, so a normal open is unaffected — but
        // a later manifest-loss repair would find an unrestricted input beside
        // the slice output and publish BOTH histories, applying every merge
        // operand of the consumed prefix twice (operands are deliberately never
        // deduplicated across sources). The manifest is the authority for the
        // bound, so derive the missing sidecar from it here and the window
        // closes at the next open rather than staying open forever.
        #[cfg(feature = "std")]
        Self::republish_missing_restriction_sidecars(&version, config)?;

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

    /// Writes the `.restrict-bound` sidecar of every restricted table in
    /// `version` that has none, so the bound the manifest holds is recoverable
    /// without it. An existing sidecar is REREAD rather than assumed good: its
    /// mere presence proves nothing, and a repair that later trusts a corrupt
    /// one derives a conservative bound (dropping up to one live block), while
    /// a valid-but-STALE one — the shape a second slice leaves when its own
    /// write fails — restricts LESS than reality and resurrects consumed rows.
    /// A sidecar that already records this table and this bound is left alone,
    /// so a healthy tree does not churn the file on every open. Failures
    /// propagate — an unrecoverable restriction is exactly what this closes, so
    /// silently skipping it would keep the window open.
    ///
    /// The sidecar is a filesystem artifact of the tight-space reclaim path, so
    /// this is a no-op without `std` — a build without it never writes one.
    #[cfg(feature = "std")]
    fn republish_missing_restriction_sidecars(
        version: &Version,
        config: &Config,
    ) -> crate::Result<()> {
        for table in version.iter_tables() {
            let Some(bound) = table.restrict_lower_bound() else {
                continue;
            };
            let recorded =
                crate::restrict_bound::read(&*table.fs, &table.path, config.encryption.as_deref())?;
            let state = match &recorded {
                crate::restrict_bound::SidecarRead::Present(id, recorded_bound)
                    if *id == table.metadata.id && recorded_bound.as_slice() == bound.as_ref() =>
                {
                    continue;
                }
                crate::restrict_bound::SidecarRead::Present(..) => {
                    "disagrees with the manifest (stale bound or another table)"
                }
                crate::restrict_bound::SidecarRead::Corrupt => "unreadable",
                crate::restrict_bound::SidecarRead::Missing => "absent",
            };
            log::warn!(
                "table {} carries a committed restriction whose sidecar is {state}; \
                 republishing it from the manifest",
                table.id(),
            );
            table.write_restrict_sidecar(bound, config.sync_mode)?;
        }
        Ok(())
    }

    /// Removes a recovery-swept artifact (an abandoned heal copy / temp / marker),
    /// treating a concurrent removal (`NotFound`) as success. A benign race (a
    /// retry, or another scanner that already swept it) must not fail recovery,
    /// matching [`Self::cleanup_orphaned_version`].
    fn sweep_artifact(fs: &dyn Fs, path: &Path) -> crate::Result<()> {
        match fs.remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == crate::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Removes stale manifest files left by older generations: every `v{id}`
    /// snapshot except the live one (`v{snapshot_id}`) and every `edits-{id}`
    /// log except the live snapshot's (`edits-{snapshot_id}`). A crashed
    /// rotation can leak an old snapshot or its log; this sweeps them on open.
    /// The live snapshot and log are exactly the generation `CURRENT` points at.
    ///
    /// # Behavior change vs pre-Fs-trait code
    ///
    /// The previous implementation used `std::fs::read_dir` + `to_string_lossy()`,
    /// which silently skipped non-UTF-8 filenames. `Fs::read_dir` returns
    /// `InvalidData` for such entries instead (see [`FsDirEntry`](crate::fs::FsDirEntry) docs), so this
    /// function now fails fast on non-UTF-8 names. This is intentional: version
    /// files are always `v{u64}` — any non-UTF-8 entry indicates filesystem
    /// corruption and should surface as an error rather than be silently ignored.
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

/// Raises a query's lower bound to a table's tight-space restriction, if any.
///
/// Keys below `restriction` are the punched-out prefix served by the
/// replacement table, so a range estimate must not charge them to the
/// restricted view. Returns `lo` unchanged when the table is unrestricted or
/// the restriction is at or below `lo`.
fn effective_lower_bound<'a>(
    lo: core::ops::Bound<&'a [u8]>,
    restriction: Option<&'a [u8]>,
    cmp: &dyn crate::comparator::UserComparator,
) -> core::ops::Bound<&'a [u8]> {
    use core::cmp::Ordering;
    use core::ops::Bound;
    match (lo, restriction) {
        (Bound::Unbounded, Some(rb)) => Bound::Included(rb),
        (Bound::Included(k) | Bound::Excluded(k), Some(rb))
            if cmp.compare(rb, k) == Ordering::Greater =>
        {
            Bound::Included(rb)
        }
        _ => lo,
    }
}

#[cfg(test)]
mod cardinality_tests;

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod scan_since_freeze_tests;

#[cfg(all(test, feature = "metrics"))]
mod cache_stats_tests;
