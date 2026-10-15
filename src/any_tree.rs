// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::{
    AbstractTree, BlobTree, Config, InternalValue, Memtable, SeqNo, TableId, Tree, UserKey,
    UserValue,
    abstract_tree::FlushToTablesResult,
    iter_guard::{IterGuardImpl, SeekableGuardIter},
    table::Table,
    tree::inner::{FlushGuard, VersionsWriteGuard},
    version::Version,
    vlog::BlobFile,
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};
use core::ops::RangeBounds;

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;

/// May be a standard [`Tree`] or a [`BlobTree`]
#[derive(Clone)]
pub enum AnyTree {
    /// Standard LSM-tree, see [`Tree`]
    Standard(Tree),

    /// Key-value separated LSM-tree, see [`BlobTree`]
    Blob(BlobTree),
}

impl crate::abstract_tree::sealed::Sealed for AnyTree {}

impl AnyTree {
    /// Starts a multi-get of `keys` at `seqno` that its caller drives: see
    /// [`Tree::start_multi_get`].
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::SnapshotBelowRetention`] when `seqno` is below
    /// what the tree retains.
    pub fn start_multi_get<K: Into<crate::UserKey>>(
        &self,
        keys: impl IntoIterator<Item = K>,
        seqno: crate::SeqNo,
    ) -> crate::Result<crate::resumable::Step> {
        match self {
            Self::Standard(tree) => tree.start_multi_get(keys, seqno),
            Self::Blob(tree) => tree.start_multi_get(keys, seqno),
        }
    }
}

impl From<Tree> for AnyTree {
    fn from(tree: Tree) -> Self {
        Self::Standard(tree)
    }
}

impl From<BlobTree> for AnyTree {
    fn from(tree: BlobTree) -> Self {
        Self::Blob(tree)
    }
}

impl TryInto<Tree> for AnyTree {
    type Error = &'static str;

    fn try_into(self) -> Result<Tree, Self::Error> {
        match self {
            Self::Standard(tree) => Ok(tree),
            Self::Blob(_) => Err("Tried to convert variant Blob to Standard"),
        }
    }
}

impl TryInto<BlobTree> for AnyTree {
    type Error = &'static str;

    fn try_into(self) -> Result<BlobTree, Self::Error> {
        match self {
            Self::Blob(tree) => Ok(tree),
            Self::Standard(_) => Err("Tried to convert variant Standard to Blob"),
        }
    }
}

/// Calls the [`AbstractTree`] method on the tree inside either variant. The
/// trait path keeps a same-named inherent method of [`Tree`] or [`BlobTree`]
/// from being picked instead.
macro_rules! forward {
    ($self:ident.$method:ident($($arg:expr),*)) => {
        match $self {
            AnyTree::Standard(tree) => AbstractTree::$method(tree, $($arg),*),
            AnyTree::Blob(tree) => AbstractTree::$method(tree, $($arg),*),
        }
    };
}

// Every method is forwarded, defaulted ones included, so an override in `Tree`
// or `BlobTree` is the one that runs.
impl AbstractTree for AnyTree {
    #[inline]
    fn print_trace(&self, key: &[u8]) -> crate::Result<()> {
        forward!(self.print_trace(key))
    }

    #[inline]
    fn table_file_cache_size(&self) -> usize {
        forward!(self.table_file_cache_size())
    }

    #[inline]
    fn next_table_id(&self) -> TableId {
        forward!(self.next_table_id())
    }

    #[inline]
    fn id(&self) -> crate::TreeId {
        forward!(self.id())
    }

    #[inline]
    fn get_internal_entry(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>> {
        forward!(self.get_internal_entry(key, seqno))
    }

    #[inline]
    fn current_version(&self) -> Version {
        forward!(self.current_version())
    }

    #[cfg(feature = "std")]
    #[inline]
    fn refresh_table_checksum(
        &self,
        table_id: TableId,
        checksum: crate::checksum::Checksum,
        expected_restriction: Option<&UserKey>,
    ) -> crate::Result<crate::abstract_tree::ChecksumRefreshOutcome> {
        forward!(self.refresh_table_checksum(table_id, checksum, expected_restriction))
    }

    #[inline]
    fn sync_mode(&self) -> crate::fs::SyncMode {
        forward!(self.sync_mode())
    }

    #[inline]
    fn prefix_extractor(&self) -> Option<Arc<dyn crate::prefix::PrefixExtractor>> {
        forward!(self.prefix_extractor())
    }

    #[inline]
    fn storage_stats(&self) -> crate::Result<crate::StorageStats> {
        forward!(self.storage_stats())
    }

    #[inline]
    fn level_segment_stats(&self) -> crate::Result<Vec<crate::LevelStats>> {
        forward!(self.level_segment_stats())
    }

    #[inline]
    fn compaction_debt(&self, strategy: &dyn crate::compaction::CompactionStrategy) -> u64 {
        forward!(self.compaction_debt(strategy))
    }

    #[inline]
    fn write_backpressure(
        &self,
        strategy: &dyn crate::compaction::CompactionStrategy,
    ) -> crate::Backpressure {
        forward!(self.write_backpressure(strategy))
    }

    #[inline]
    fn write_backpressure_for(
        &self,
        strategy: &dyn crate::compaction::CompactionStrategy,
        bytes: u64,
    ) -> crate::Backpressure {
        forward!(self.write_backpressure_for(strategy, bytes))
    }

    #[inline]
    fn write_rate_limiter(&self) -> Arc<crate::rate_limiter::RateLimiter> {
        forward!(self.write_rate_limiter())
    }

    #[inline]
    fn write_admission(&self) -> crate::Result<()> {
        forward!(self.write_admission())
    }

    #[inline]
    fn is_read_only(&self) -> bool {
        forward!(self.is_read_only())
    }

    #[cfg(feature = "std")]
    #[inline]
    fn verify_checksum(&self) -> crate::verify::BlockVerifyReport
    where
        Self: Sized,
    {
        forward!(self.verify_checksum())
    }

    #[cfg(feature = "std")]
    #[inline]
    fn verify_checksum_with(
        &self,
        options: &crate::verify::VerifyOptions,
    ) -> crate::verify::BlockVerifyReport
    where
        Self: Sized,
    {
        forward!(self.verify_checksum_with(options))
    }

    #[inline]
    fn get_version_history_lock(&self) -> VersionsWriteGuard<'_> {
        forward!(self.get_version_history_lock())
    }

    #[cfg(feature = "std")]
    #[inline]
    fn create_checkpoint(
        &self,
        target_path: &crate::path::Path,
    ) -> crate::Result<crate::CheckpointInfo> {
        forward!(self.create_checkpoint(target_path))
    }

    #[inline]
    fn flush_active_memtable(&self, gc_watermark: SeqNo) -> crate::Result<()> {
        forward!(self.flush_active_memtable(gc_watermark))
    }

    #[inline]
    fn sync_devices(&self) -> crate::Result<()> {
        forward!(self.sync_devices())
    }

    #[inline]
    fn flush(&self, lock: &FlushGuard<'_>, gc_watermark: SeqNo) -> crate::Result<Option<u64>> {
        forward!(self.flush(lock, gc_watermark))
    }

    #[inline]
    fn iter(
        &self,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        forward!(self.iter(seqno, index))
    }

    #[inline]
    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        forward!(self.prefix(prefix, seqno, index))
    }

    #[inline]
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        forward!(self.range(range, seqno, index))
    }

    #[inline]
    fn range_seekable<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn SeekableGuardIter + 'static> {
        forward!(self.range_seekable(range, seqno, index))
    }

    #[inline]
    fn batch_range_scan<K: AsRef<[u8]>, R: RangeBounds<K> + 'static, I: IntoIterator<Item = R>>(
        &self,
        intervals: I,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn Iterator<Item = IterGuardImpl> + Send + 'static>
    where
        I::IntoIter: Send + 'static,
    {
        forward!(self.batch_range_scan(intervals, seqno, index))
    }

    #[inline]
    fn tombstone_count(&self) -> u64 {
        forward!(self.tombstone_count())
    }

    #[inline]
    fn weak_tombstone_count(&self) -> u64 {
        forward!(self.weak_tombstone_count())
    }

    #[inline]
    fn weak_tombstone_reclaimable_count(&self) -> u64 {
        forward!(self.weak_tombstone_reclaimable_count())
    }

    #[inline]
    fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> crate::Result<()> {
        forward!(self.drop_range(range))
    }

    #[inline]
    fn clear(&self) -> crate::Result<()> {
        forward!(self.clear())
    }

    #[inline]
    fn major_compact(
        &self,
        target_size: u64,
        gc_watermark: SeqNo,
    ) -> crate::Result<crate::compaction::CompactionResult> {
        forward!(self.major_compact(target_size, gc_watermark))
    }

    #[inline]
    fn stale_blob_bytes(&self) -> u64 {
        forward!(self.stale_blob_bytes())
    }

    #[inline]
    fn filter_size(&self) -> u64 {
        forward!(self.filter_size())
    }

    #[inline]
    fn pinned_filter_size(&self) -> usize {
        forward!(self.pinned_filter_size())
    }

    #[inline]
    fn filter_memory(&self) -> crate::FilterMemory {
        forward!(self.filter_memory())
    }

    #[inline]
    fn pinned_block_index_size(&self) -> usize {
        forward!(self.pinned_block_index_size())
    }

    #[cfg(feature = "metrics")]
    #[inline]
    fn metrics(&self) -> &Arc<crate::Metrics> {
        forward!(self.metrics())
    }

    #[cfg(feature = "metrics")]
    #[inline]
    fn cache_stats(&self) -> crate::CacheStats {
        forward!(self.cache_stats())
    }

    #[inline]
    fn get_flush_lock(&self) -> FlushGuard<'_> {
        forward!(self.get_flush_lock())
    }

    #[inline]
    fn flush_to_tables(
        &self,
        stream: impl Iterator<Item = crate::Result<InternalValue>>,
    ) -> crate::Result<Option<FlushToTablesResult>> {
        forward!(self.flush_to_tables(stream))
    }

    #[inline]
    fn flush_to_tables_with_rt(
        &self,
        stream: impl Iterator<Item = crate::Result<InternalValue>>,
        range_tombstones: Vec<crate::range_tombstone::RangeTombstone>,
        keys: u64,
        hashes: u64,
        overwritten: Option<&crate::blob_tree::overwritten::OverwrittenKeys>,
    ) -> crate::Result<Option<FlushToTablesResult>> {
        forward!(self.flush_to_tables_with_rt(stream, range_tombstones, keys, hashes, overwritten))
    }

    #[inline]
    fn register_tables(
        &self,
        tables: &[Table],
        blob_files: Option<&[BlobFile]>,
        frag_map: Option<crate::blob_tree::FragmentationMap>,
        sealed_memtables_to_delete: &[crate::tree::inner::MemtableId],
        gc_watermark: SeqNo,
        collected_below_watermark: bool,
    ) -> crate::Result<()> {
        forward!(self.register_tables(
            tables,
            blob_files,
            frag_map,
            sealed_memtables_to_delete,
            gc_watermark,
            collected_below_watermark
        ))
    }

    #[inline]
    fn clear_active_memtable(&self) {
        forward!(self.clear_active_memtable());
    }

    #[inline]
    fn sealed_memtable_count(&self) -> usize {
        forward!(self.sealed_memtable_count())
    }

    #[inline]
    fn compact(
        &self,
        strategy: Arc<dyn crate::compaction::CompactionStrategy>,
        gc_watermark: SeqNo,
    ) -> crate::Result<crate::compaction::CompactionResult> {
        forward!(self.compact(strategy, gc_watermark))
    }

    #[inline]
    fn get_next_table_id(&self) -> TableId {
        forward!(self.get_next_table_id())
    }

    #[inline]
    fn tree_config(&self) -> &Config {
        forward!(self.tree_config())
    }

    #[inline]
    fn get_highest_seqno(&self) -> Option<SeqNo> {
        forward!(self.get_highest_seqno())
    }

    #[inline]
    fn active_memtable(&self) -> Arc<Memtable> {
        forward!(self.active_memtable())
    }

    #[inline]
    fn tree_type(&self) -> crate::TreeType {
        forward!(self.tree_type())
    }

    #[inline]
    fn rotate_memtable(&self) -> Option<Arc<Memtable>> {
        forward!(self.rotate_memtable())
    }

    #[inline]
    fn table_count(&self) -> usize {
        forward!(self.table_count())
    }

    #[inline]
    fn level_table_count(&self, idx: usize) -> Option<usize> {
        forward!(self.level_table_count(idx))
    }

    #[inline]
    fn l0_run_count(&self) -> usize {
        forward!(self.l0_run_count())
    }

    #[inline]
    fn blob_file_count(&self) -> usize {
        forward!(self.blob_file_count())
    }

    #[inline]
    fn approximate_len(&self) -> usize {
        forward!(self.approximate_len())
    }

    #[inline]
    fn disk_space(&self) -> u64 {
        forward!(self.disk_space())
    }

    #[inline]
    fn approximate_range_stats<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
    ) -> crate::Result<crate::ApproximateRangeStats> {
        forward!(self.approximate_range_stats(range, seqno))
    }

    #[inline]
    fn approximate_range_cardinality<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
    ) -> crate::Result<crate::RangeCardinality> {
        forward!(self.approximate_range_cardinality(range, seqno))
    }

    #[inline]
    fn get_highest_memtable_seqno(&self) -> Option<SeqNo> {
        forward!(self.get_highest_memtable_seqno())
    }

    #[inline]
    fn get_highest_persisted_seqno(&self) -> Option<SeqNo> {
        forward!(self.get_highest_persisted_seqno())
    }

    #[inline]
    fn oldest_retained_seqno(&self) -> SeqNo {
        forward!(self.oldest_retained_seqno())
    }

    #[inline]
    fn retention_floor(&self) -> SeqNo {
        forward!(self.retention_floor())
    }

    #[inline]
    fn compaction_rate_limiter(&self) -> Arc<crate::rate_limiter::RateLimiter> {
        forward!(self.compaction_rate_limiter())
    }

    #[inline]
    fn len(&self, seqno: SeqNo, index: Option<(Arc<Memtable>, SeqNo)>) -> crate::Result<usize> {
        forward!(self.len(seqno, index))
    }

    #[inline]
    fn is_empty(&self, seqno: SeqNo, index: Option<(Arc<Memtable>, SeqNo)>) -> crate::Result<bool> {
        forward!(self.is_empty(seqno, index))
    }

    #[inline]
    fn first_key_value(
        &self,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Option<IterGuardImpl> {
        forward!(self.first_key_value(seqno, index))
    }

    #[inline]
    fn last_key_value(
        &self,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Option<IterGuardImpl> {
        forward!(self.last_key_value(seqno, index))
    }

    #[inline]
    fn size_of<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<u32>> {
        forward!(self.size_of(key, seqno))
    }

    #[inline]
    fn get<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<UserValue>> {
        forward!(self.get(key, seqno))
    }

    #[inline]
    fn get_pinned<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: SeqNo,
    ) -> crate::Result<Option<crate::PinnableSlice>> {
        forward!(self.get_pinned(key, seqno))
    }

    #[inline]
    fn contains_key<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<bool> {
        forward!(self.contains_key(key, seqno))
    }

    #[inline]
    fn contains_prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> crate::Result<bool> {
        forward!(self.contains_prefix(prefix, seqno, index))
    }

    #[inline]
    fn multi_get<K: AsRef<[u8]>>(
        &self,
        keys: impl IntoIterator<Item = K>,
        seqno: SeqNo,
    ) -> crate::Result<Vec<Option<UserValue>>> {
        forward!(self.multi_get(keys, seqno))
    }

    #[inline]
    fn apply_batch(&self, batch: crate::WriteBatch, seqno: SeqNo) -> crate::Result<(u64, u64)> {
        forward!(self.apply_batch(batch, seqno))
    }

    #[inline]
    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
        seqno: SeqNo,
    ) -> (u64, u64) {
        forward!(self.insert(key, value, seqno))
    }

    #[inline]
    fn try_insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
        seqno: SeqNo,
    ) -> crate::Result<(u64, u64)> {
        forward!(self.try_insert(key, value, seqno))
    }

    #[inline]
    fn try_merge<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        operand: V,
        seqno: SeqNo,
    ) -> crate::Result<(u64, u64)> {
        forward!(self.try_merge(key, operand, seqno))
    }

    #[inline]
    fn try_remove<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> crate::Result<(u64, u64)> {
        forward!(self.try_remove(key, seqno))
    }

    #[inline]
    fn try_remove_weak<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> crate::Result<(u64, u64)> {
        forward!(self.try_remove_weak(key, seqno))
    }

    #[inline]
    fn try_remove_range<K: Into<UserKey>>(
        &self,
        start: K,
        end: K,
        seqno: SeqNo,
    ) -> crate::Result<u64> {
        forward!(self.try_remove_range(start, end, seqno))
    }

    #[inline]
    fn remove<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        forward!(self.remove(key, seqno))
    }

    #[inline]
    fn merge<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        operand: V,
        seqno: SeqNo,
    ) -> (u64, u64) {
        forward!(self.merge(key, operand, seqno))
    }

    #[inline]
    fn remove_weak<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        forward!(self.remove_weak(key, seqno))
    }

    #[inline]
    fn remove_range<K: Into<UserKey>>(&self, start: K, end: K, seqno: SeqNo) -> u64 {
        forward!(self.remove_range(start, end, seqno))
    }

    #[inline]
    fn remove_prefix<K: AsRef<[u8]>>(&self, prefix: K, seqno: SeqNo) -> u64 {
        forward!(self.remove_prefix(prefix, seqno))
    }
}
