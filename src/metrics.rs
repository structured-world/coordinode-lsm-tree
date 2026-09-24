// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

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

    /// Payload bytes produced by the block transform — what decompression,
    /// decryption and Page-ECC verification turned the bytes read into.
    ///
    /// Counted at the same site as `*_io_requested` and only on the same
    /// uncached path, because a block served from the cache is already
    /// decoded and no transform runs for it.
    ///
    /// This is the counter that tells a PHYSICAL projection from a cosmetic
    /// one. A projection that returns two columns of a wide record but still
    /// loads and decompresses the whole block leaves this figure unchanged
    /// while the returned batch shrinks; one that reads only the pages it
    /// needs moves it. Read alone cannot show that — a 4 KiB compressed block
    /// is 4 KiB read however much it expands to.
    pub(crate) block_bytes_decoded: AtomicU64,

    /// Bytes moved by a gather: an operation that builds a new buffer whose
    /// contents already existed in another one.
    ///
    /// The named set, so a new path cannot win by not being instrumented:
    /// column-batch accumulation, batch filtering, row gathering by index,
    /// row-value reconstruction from sub-columns, the prefix copies and the
    /// synthesized block of a large zstd block's partial decode, what
    /// decoding a columnar block copies out of it (validity bitmaps, and
    /// columns a narrow projection detaches), the effective seqnos written
    /// over a bulk-ingested segment's local ones, the key and value a point
    /// read detaches into the row cache, and the key a resolved blob is
    /// cached under, wherever a read performs them
    /// (single-segment and merged columnar scans, row iteration and point
    /// reads, range reads of a partially decoded block). It does NOT count a
    /// block transform's output (that is `block_bytes_decoded`), a write
    /// path's serialisation, the input decoding of compaction, repair and
    /// salvage (maintenance, not reads), or a move that transfers ownership without
    /// duplicating bytes.
    ///
    /// A view into a decoded buffer is a view whatever its representation: a
    /// short slice stored inline in the handle is not charged, because building
    /// that inline copy costs no more than building the handle itself.
    ///
    /// The quantity this exists to expose is quadratic accumulation and
    /// repeated re-gather: bytes copied per input byte should be a small
    /// constant, and a path that re-materialises its working set several
    /// times shows up here and nowhere else.
    pub(crate) bytes_copied: AtomicU64,

    /// Number of data block bytes that were requested from OS or disk.
    ///
    /// Definition: bytes REQUESTED FROM THE `Fs` TRAIT, which is the block's
    /// on-disk size (`handle.size()`), not device I/O — the OS page cache,
    /// readahead and request coalescing all sit below this line and are not
    /// visible to it. Counted only when the block was not served from the
    /// block cache, so a fully cached read reports zero bytes read, which is
    /// the honest answer to "how much did this read ask the filesystem for".
    pub(crate) data_block_io_requested: AtomicU64,

    /// Blob record bytes requested from the `Fs` trait: the on-disk span of
    /// the records a read or a prefetch asked for, gaps merged into a
    /// coalesced read included.
    ///
    /// Separate from the block counters because a blob is not a block, and
    /// summed into [`Metrics::bytes_read`] because it IS a read of the
    /// filesystem. A key-value-separated tree keeps most of its bytes here, so
    /// leaving them out would let a change that moves work into the blob path
    /// report an improvement by moving it out of sight.
    ///
    /// Counted on the uncached path only, like every other read counter: a
    /// value served from the blob cache asks the filesystem for nothing.
    pub(crate) blob_bytes_io_requested: AtomicU64,

    /// Blob value bytes produced after decompression, decryption and
    /// validation — the blob-side twin of `block_bytes_decoded`, summed into
    /// [`Metrics::bytes_decoded`] for the same reason.
    pub(crate) blob_bytes_decoded: AtomicU64,

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

/// A point-in-time snapshot of block-cache effectiveness and occupancy.
///
/// Derived from [`Metrics`] (the cumulative hit / miss counters) plus the live
/// block cache's current size and capacity, so an observability consumer gets a
/// stable owned value instead of reaching into the mutable `&Arc<Metrics>`.
/// Counts are cumulative since process start (they reset on restart, like all of
/// [`Metrics`]); derive a rate over an interval from the delta between two polls.
// No `PartialEq`: `hit_rate` is an `f64`, so equality would inherit float
// comparison semantics. Compare the integer fields explicitly instead.
#[must_use]
#[derive(Copy, Clone, Debug)]
pub struct CacheStats {
    /// Cumulative block reads served from the block cache (all block types).
    pub hits: u64,
    /// Cumulative block reads that missed the cache and hit disk (all block types).
    pub misses: u64,
    /// Hit rate in `0.0..=1.0` (`hits / (hits + misses)`); `1.0` when no block
    /// has been loaded yet (nothing has missed).
    pub hit_rate: f64,
    /// Current weighted bytes resident in the block cache.
    pub size_bytes: u64,
    /// Configured maximum bytes the block cache may hold.
    pub capacity_bytes: u64,
}

#[expect(
    clippy::cast_precision_loss,
    reason = "metrics can accept precision loss"
)]
impl Metrics {
    /// Builds a [`CacheStats`] snapshot from the cumulative cache counters and
    /// the caller-supplied live cache `size_bytes` / `capacity_bytes` (the block
    /// cache owns its occupancy, [`Metrics`] owns the hit / miss tallies).
    pub fn cache_stats(&self, size_bytes: u64, capacity_bytes: u64) -> CacheStats {
        // Read the counters once so hits / misses / hit_rate are a single
        // consistent snapshot (block_cache_hit_rate would re-read the atomics).
        let hits = self.block_load_cached_count() as u64;
        let misses = self.block_load_io_count() as u64;
        let total = hits + misses;
        let hit_rate = if total == 0 {
            1.0
        } else {
            hits as f64 / total as f64
        };
        CacheStats {
            hits,
            misses,
            hit_rate,
            size_bytes,
            capacity_bytes,
        }
    }

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

    /// Data block bytes requested from the `Fs` trait by reads callers made.
    ///
    /// Charged when each read is issued, so a read that then fails its
    /// checksum, decryption or decompression is counted; maintenance and
    /// monitoring reads are not. See [`Self::bytes_read`].
    pub fn data_block_io(&self) -> u64 {
        self.data_block_io_requested.load(Relaxed)
    }

    /// Index block bytes requested from the `Fs` trait, on the same terms as
    /// [`Self::data_block_io`].
    pub fn index_block_io(&self) -> u64 {
        self.index_block_io_requested.load(Relaxed)
    }

    /// Filter block bytes requested from the `Fs` trait, on the same terms as
    /// [`Self::data_block_io`].
    pub fn filter_block_io(&self) -> u64 {
        self.filter_block_io_requested.load(Relaxed)
    }

    /// Range tombstone block bytes requested from the `Fs` trait, on the same
    /// terms as [`Self::data_block_io`].
    pub fn range_tombstone_block_io(&self) -> u64 {
        self.range_tombstone_block_io_requested.load(Relaxed)
    }

    /// Block bytes requested from the `Fs` trait over every block role, on the
    /// same terms as [`Self::data_block_io`].
    pub fn block_io(&self) -> u64 {
        self.data_block_io_requested.load(Relaxed)
            + self.index_block_io_requested.load(Relaxed)
            + self.filter_block_io_requested.load(Relaxed)
            + self.range_tombstone_block_io_requested.load(Relaxed)
    }

    /// Payload bytes the block transform produced — see
    /// [`Self::bytes_read`] for the figure this is paired with.
    ///
    /// Read and decoded are reported together or not at all: each alone is
    /// misleading. Read without decoded hides a projection that loads and
    /// decompresses everything it then discards; decoded without read hides a
    /// change that decodes the same amount from far more I/O.
    pub fn bytes_decoded(&self) -> u64 {
        self.block_bytes_decoded.load(Relaxed) + self.blob_bytes_decoded.load(Relaxed)
    }

    /// Bytes requested from the `Fs` trait: every block role, plus the blob
    /// records a key-value-separated tree resolves.
    ///
    /// Wider than [`Self::block_io`] on purpose. A separated value's bytes
    /// leave the filesystem through the blob path rather than through a block,
    /// and a figure that omitted them would report a tree that reads gigabytes
    /// as reading only its indirections.
    ///
    /// Reported with [`Self::bytes_decoded`] and [`Self::bytes_copied`]; the
    /// three are one family.
    pub fn bytes_read(&self) -> u64 {
        self.block_io() + self.blob_bytes_io_requested.load(Relaxed)
    }

    /// Bytes requested from the `Fs` trait for separated values alone.
    ///
    /// The blob-only share of [`Self::bytes_read`], so a scan can be asked
    /// whether it paid for the blobs of rows it then discarded.
    pub fn blob_bytes_read(&self) -> u64 {
        self.blob_bytes_io_requested.load(Relaxed)
    }

    /// Bytes moved by a gather — accumulation, filtering, row gathering and
    /// row-value reconstruction. The exact set is on the field.
    ///
    /// Interpreted per input byte: a path that materialises its working set
    /// once sits near a small constant, and one that re-gathers repeatedly
    /// grows with the number of passes rather than with the data.
    pub fn bytes_copied(&self) -> u64 {
        self.bytes_copied.load(Relaxed)
    }

    /// Charges one gather: `bytes` is the size of the buffer it built. Every
    /// site in the named set calls this, so the set and its instrumentation
    /// cannot drift apart one path at a time.
    #[inline]
    pub(crate) fn record_gather(&self, bytes: usize) {
        self.bytes_copied.fetch_add(bytes as u64, Relaxed);
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
mod tests;
