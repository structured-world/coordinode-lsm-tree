// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use core::sync::atomic::Ordering::Relaxed;
use core::sync::atomic::{AtomicU64, AtomicUsize};

/// Runtime metrics
///
/// Are not stored durably, so metrics will reset after a restart/crash.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Number of times a table file was opened using `fopen()`
    pub(crate) table_file_opened_uncached: AtomicUsize,

    /// Number of times a table file was retrieved from descriptor cache
    pub(crate) table_file_opened_cached: AtomicUsize,

    /// Number of index blocks that were actually read from disk
    pub(crate) index_block_load_io: AtomicUsize,

    /// Number of filter blocks that were actually read from disk
    pub(crate) filter_block_load_io: AtomicUsize,

    /// Number of blocks that were actually read from disk
    pub(crate) data_block_load_io: AtomicUsize,

    /// Number of blocks that were read from block cache
    pub(crate) index_block_load_cached: AtomicUsize,

    /// Number of blocks that were read from block cache
    pub(crate) filter_block_load_cached: AtomicUsize,

    /// Number of blocks that were read from block cache
    pub(crate) data_block_load_cached: AtomicUsize,

    /// Number of range tombstone blocks that were actually read from disk
    pub(crate) range_tombstone_block_load_io: AtomicUsize,

    /// Number of range tombstone blocks that were read from block cache
    pub(crate) range_tombstone_block_load_cached: AtomicUsize,

    /// Number of filter queries that were performed
    pub(crate) filter_queries: AtomicUsize,

    /// Number of IOs that were skipped due to filter
    pub(crate) io_skipped_by_filter: AtomicUsize,

    /// Number of segments skipped during prefix scans via
    /// [`Tree::create_prefix`](crate::Tree::create_prefix) where the per-table prefix bloom filter
    /// returned `Ok(false)`. Counted in both single-table and
    /// multi-table run paths of `TreeIter::create_range`.
    ///
    /// Note: `BlobTree` prefix scans do not currently record this metric.
    pub(crate) prefix_bloom_skips: AtomicUsize,

    /// Number of data block bytes that were requested from OS or disk
    pub(crate) data_block_io_requested: AtomicU64,

    /// Number of index block bytes that were requested from OS or disk
    pub(crate) index_block_io_requested: AtomicU64,

    /// Number of filter block bytes that were requested from OS or disk
    pub(crate) filter_block_io_requested: AtomicU64,

    /// Number of range tombstone block bytes that were requested from OS or disk
    pub(crate) range_tombstone_block_io_requested: AtomicU64,

    /// Number of SSTs flagged for a healing recompaction after a read recovered
    /// a block from Page-ECC parity and confirmed the fault persistent (counted
    /// only when `auto_heal` is enabled). Each SST is counted once per pending
    /// schedule.
    pub(crate) ecc_auto_heal_scheduled: AtomicUsize,

    /// On-read blocks healed by the SEC-DED single-bit fast path (one corrected
    /// bit flip). Counted on every primary read that observes the recovery
    /// (point/range loads, partial-decode, patrol scrub); the persistence
    /// confirming re-read does NOT re-count. A non-zero, growing value is a
    /// scrapeable latent-bit-rot signal.
    pub(crate) ecc_secded_corrected: AtomicUsize,

    /// On-read blocks recovered from Reed-Solomon shard parity (the general
    /// multi-byte path). Same counting discipline as
    /// [`Self::ecc_secded_corrected`]; the two are disjoint by recovery
    /// mechanism and sum to the total on-read ECC recoveries.
    pub(crate) ecc_shard_recovered: AtomicUsize,
}

#[expect(
    clippy::cast_precision_loss,
    reason = "metrics can accept precision loss"
)]
impl Metrics {
    /// Returns the cache hit rate for file descriptors in percent (0.0 - 1.0).
    pub fn table_file_cache_hit_rate(&self) -> f64 {
        let uncached = self.table_file_opened_uncached.load(Relaxed) as f64;
        let cached = self.table_file_opened_cached.load(Relaxed) as f64;

        if cached + uncached == 0.0 {
            1.0
        } else {
            cached / (cached + uncached)
        }
    }

    /// Number of I/O data block bytes transferred from disk or OS page cache.
    pub fn data_block_io(&self) -> u64 {
        self.data_block_io_requested.load(Relaxed)
    }

    /// Number of I/O index block bytes transferred from disk or OS page cache.
    pub fn index_block_io(&self) -> u64 {
        self.index_block_io_requested.load(Relaxed)
    }

    /// Number of I/O filter block bytes transferred from disk or OS page cache.
    pub fn filter_block_io(&self) -> u64 {
        self.filter_block_io_requested.load(Relaxed)
    }

    /// Number of I/O range tombstone block bytes transferred from disk or OS page cache.
    pub fn range_tombstone_block_io(&self) -> u64 {
        self.range_tombstone_block_io_requested.load(Relaxed)
    }

    /// Number of I/O block bytes transferred from disk or OS page cache.
    pub fn block_io(&self) -> u64 {
        self.data_block_io_requested.load(Relaxed)
            + self.index_block_io_requested.load(Relaxed)
            + self.filter_block_io_requested.load(Relaxed)
            + self.range_tombstone_block_io_requested.load(Relaxed)
    }

    /// Number of data blocks that were accessed.
    pub fn data_block_load_count(&self) -> usize {
        self.data_block_load_cached.load(Relaxed) + self.data_block_load_io.load(Relaxed)
    }

    /// Number of index blocks that were accessed.
    pub fn index_block_load_count(&self) -> usize {
        self.index_block_load_cached.load(Relaxed) + self.index_block_load_io.load(Relaxed)
    }

    /// Number of filter blocks that were accessed.
    pub fn filter_block_load_count(&self) -> usize {
        self.filter_block_load_cached.load(Relaxed) + self.filter_block_load_io.load(Relaxed)
    }

    /// Number of range tombstone blocks that were accessed.
    pub fn range_tombstone_block_load_count(&self) -> usize {
        self.range_tombstone_block_load_cached.load(Relaxed)
            + self.range_tombstone_block_load_io.load(Relaxed)
    }

    /// Number of SSTs scheduled for a healing recompaction after a persistent
    /// ECC correction on read (`auto_heal` enabled).
    pub fn ecc_auto_heal_scheduled_count(&self) -> usize {
        self.ecc_auto_heal_scheduled.load(Relaxed)
    }

    /// On-read blocks healed by the SEC-DED single-bit fast path.
    pub fn ecc_secded_corrected_count(&self) -> usize {
        self.ecc_secded_corrected.load(Relaxed)
    }

    /// On-read blocks recovered from Reed-Solomon shard parity.
    pub fn ecc_shard_recovered_count(&self) -> usize {
        self.ecc_shard_recovered.load(Relaxed)
    }

    /// Total on-read ECC recoveries across both mechanisms (SEC-DED + RS shard).
    /// A scrapeable latent-bit-rot signal: growth here means the medium is
    /// returning faulty bytes that parity is silently repairing.
    pub fn ecc_recovered_count(&self) -> usize {
        self.ecc_secded_corrected_count() + self.ecc_shard_recovered_count()
    }

    /// Records one on-read ECC recovery, attributing it to the mechanism that
    /// did the repair. Called from the primary read paths (`load_block`, the
    /// partial-decode path, patrol scrub); the persistence-confirming re-read
    /// must NOT call this, to avoid double-counting a single fault.
    pub(crate) fn record_ecc_recovery(&self, kind: crate::table::block::EccRecoveryKind) {
        use crate::table::block::EccRecoveryKind;
        match kind {
            EccRecoveryKind::Secded => &self.ecc_secded_corrected,
            EccRecoveryKind::Shard => &self.ecc_shard_recovered,
        }
        .fetch_add(1, Relaxed);
    }

    /// Number of blocks that were loaded from disk or OS page cache.
    pub fn block_load_io_count(&self) -> usize {
        self.data_block_load_io.load(Relaxed)
            + self.index_block_load_io.load(Relaxed)
            + self.filter_block_load_io.load(Relaxed)
            + self.range_tombstone_block_load_io.load(Relaxed)
    }

    /// Number of data blocks that were served from block cache.
    pub fn data_block_load_cached_count(&self) -> usize {
        self.data_block_load_cached.load(Relaxed)
    }

    /// Number of index blocks that were served from block cache.
    pub fn index_block_load_cached_count(&self) -> usize {
        self.index_block_load_cached.load(Relaxed)
    }

    /// Number of filter blocks that were served from block cache.
    pub fn filter_block_load_cached_count(&self) -> usize {
        self.filter_block_load_cached.load(Relaxed)
    }

    /// Number of range tombstone blocks that were served from block cache.
    pub fn range_tombstone_block_load_cached_count(&self) -> usize {
        self.range_tombstone_block_load_cached.load(Relaxed)
    }

    /// Number of blocks that were served from block cache.
    pub fn block_load_cached_count(&self) -> usize {
        self.data_block_load_cached.load(Relaxed)
            + self.index_block_load_cached.load(Relaxed)
            + self.filter_block_load_cached.load(Relaxed)
            + self.range_tombstone_block_load_cached.load(Relaxed)
    }

    /// Number of blocks that were accessed.
    pub fn block_loads(&self) -> usize {
        self.block_load_io_count() + self.block_load_cached_count()
    }

    /// Data block cache efficiency in percent (0.0 - 1.0).
    pub fn data_block_cache_hit_rate(&self) -> f64 {
        let queries = self.data_block_load_count() as f64;
        let hits = self.data_block_load_cached_count() as f64;

        if queries == 0.0 { 1.0 } else { hits / queries }
    }

    /// Filter block cache efficiency in percent (0.0 - 1.0).
    pub fn filter_block_cache_hit_rate(&self) -> f64 {
        let queries = self.filter_block_load_count() as f64;
        let hits = self.filter_block_load_cached_count() as f64;

        if queries == 0.0 { 1.0 } else { hits / queries }
    }

    /// Index block cache efficiency in percent (0.0 - 1.0).
    pub fn index_block_cache_hit_rate(&self) -> f64 {
        let queries = self.index_block_load_count() as f64;
        let hits = self.index_block_load_cached_count() as f64;

        if queries == 0.0 { 1.0 } else { hits / queries }
    }

    /// Range tombstone block cache efficiency in percent (0.0 - 1.0).
    pub fn range_tombstone_block_cache_hit_rate(&self) -> f64 {
        let queries = self.range_tombstone_block_load_count() as f64;
        let hits = self.range_tombstone_block_load_cached_count() as f64;

        if queries == 0.0 { 1.0 } else { hits / queries }
    }

    /// Block cache efficiency in percent (0.0 - 1.0).
    pub fn block_cache_hit_rate(&self) -> f64 {
        let queries = self.block_loads() as f64;
        let hits = self.block_load_cached_count() as f64;

        if queries == 0.0 { 1.0 } else { hits / queries }
    }

    /// Filter efficiency in percent (0.0 - 1.0).
    ///
    /// Represents the ratio of I/O operations avoided due to filter.
    pub fn filter_efficiency(&self) -> f64 {
        let queries = self.filter_queries.load(Relaxed) as f64;
        let io_skipped = self.io_skipped_by_filter.load(Relaxed) as f64;

        if queries == 0.0 {
            1.0
        } else {
            io_skipped / queries
        }
    }

    /// Number of filter queries performed.
    pub fn filter_queries(&self) -> usize {
        self.filter_queries.load(Relaxed)
    }

    /// Number of I/O operations skipped by filter.
    pub fn io_skipped_by_filter(&self) -> usize {
        self.io_skipped_by_filter.load(Relaxed)
    }

    /// Number of segments skipped during [`Tree::create_prefix`](crate::Tree::create_prefix) scans
    /// by prefix bloom filters (single-table and multi-table run paths).
    ///
    /// Note: `BlobTree` prefix scans do not currently record this metric.
    pub fn prefix_bloom_skips(&self) -> usize {
        self.prefix_bloom_skips.load(Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::Ordering::Relaxed;

    #[test]
    fn range_tombstone_counters_default_zero() {
        let m = Metrics::default();
        assert_eq!(0, m.range_tombstone_block_load_count());
        assert_eq!(0, m.range_tombstone_block_load_cached_count());
        assert_eq!(0, m.range_tombstone_block_io());
    }

    #[test]
    fn record_ecc_recovery_attributes_each_kind_to_its_own_counter() {
        use crate::table::block::EccRecoveryKind;
        let m = Metrics::default();
        assert_eq!(0, m.ecc_recovered_count(), "counters start at zero");

        m.record_ecc_recovery(EccRecoveryKind::Secded);
        m.record_ecc_recovery(EccRecoveryKind::Secded);
        m.record_ecc_recovery(EccRecoveryKind::Shard);

        assert_eq!(2, m.ecc_secded_corrected_count(), "two SEC-DED heals");
        assert_eq!(1, m.ecc_shard_recovered_count(), "one RS shard recovery");
        // The two mechanisms are disjoint and sum to the total.
        assert_eq!(3, m.ecc_recovered_count(), "total across both mechanisms");
    }

    #[test]
    fn range_tombstone_block_load_count_sums_cached_and_io() {
        let m = Metrics::default();
        m.range_tombstone_block_load_cached.store(3, Relaxed);
        m.range_tombstone_block_load_io.store(7, Relaxed);
        assert_eq!(10, m.range_tombstone_block_load_count());
    }

    #[test]
    fn range_tombstone_cache_hit_rate_no_loads_returns_one() {
        let m = Metrics::default();
        assert!((m.range_tombstone_block_cache_hit_rate() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn range_tombstone_cache_hit_rate_mixed_loads() {
        let m = Metrics::default();
        m.range_tombstone_block_load_cached.store(3, Relaxed);
        m.range_tombstone_block_load_io.store(1, Relaxed);
        assert!((m.range_tombstone_block_cache_hit_rate() - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn zero_query_cache_hit_rates_return_one() {
        let m = Metrics::default();
        assert!((m.data_block_cache_hit_rate() - 1.0).abs() < f64::EPSILON);
        assert!((m.filter_block_cache_hit_rate() - 1.0).abs() < f64::EPSILON);
        assert!((m.index_block_cache_hit_rate() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn block_io_includes_range_tombstone() {
        let m = Metrics::default();
        m.data_block_io_requested.store(10, Relaxed);
        m.index_block_io_requested.store(20, Relaxed);
        m.filter_block_io_requested.store(30, Relaxed);
        m.range_tombstone_block_io_requested.store(40, Relaxed);
        assert_eq!(100, m.block_io());
    }

    #[test]
    fn block_load_io_count_includes_range_tombstone() {
        let m = Metrics::default();
        m.data_block_load_io.store(1, Relaxed);
        m.index_block_load_io.store(2, Relaxed);
        m.filter_block_load_io.store(3, Relaxed);
        m.range_tombstone_block_load_io.store(4, Relaxed);
        assert_eq!(10, m.block_load_io_count());
    }

    #[test]
    fn block_load_cached_count_includes_range_tombstone() {
        let m = Metrics::default();
        m.data_block_load_cached.store(5, Relaxed);
        m.index_block_load_cached.store(6, Relaxed);
        m.filter_block_load_cached.store(7, Relaxed);
        m.range_tombstone_block_load_cached.store(8, Relaxed);
        assert_eq!(26, m.block_load_cached_count());
    }
}
