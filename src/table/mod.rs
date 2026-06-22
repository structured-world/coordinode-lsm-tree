// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

pub mod block;
pub(crate) mod block_index;
pub(crate) mod block_layout;
#[cfg(feature = "columnar")]
pub mod columnar;
#[cfg(feature = "columnar")]
pub mod columnar_predicate;
pub mod data_block;
pub mod delete_bitmap;
pub mod filter;
mod id;
mod index_block;
mod inner;
pub(crate) mod iter;
#[cfg(feature = "zstd")]
pub(crate) mod lazy_block;
pub(crate) mod locator;
pub(crate) mod meta;
pub(crate) mod multi_writer;
pub(crate) mod regions;
mod scanner;
pub(crate) mod seqno_bounds;
pub mod util;
pub mod writer;
pub(crate) mod zone_map;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    clippy::needless_borrows_for_generic_args,
    reason = "test code"
)]
mod tests;

pub use block::{Block, BlockOffset};
pub use data_block::DataBlock;
pub use id::{GlobalTableId, TableId};
pub use index_block::{BlockHandle, IndexBlock, KeyedBlockHandle};
pub use scanner::Scanner;
pub use writer::Writer;

use crate::{
    Checksum, CompressionType, InternalValue, SeqNo, TreeId, UserKey,
    cache::Cache,
    comparator::SharedComparator,
    descriptor_table::DescriptorTable,
    file_accessor::FileAccessor,
    fs::{Fs, FsFile, FsOpenOptions},
    range_tombstone::RangeTombstone,
    table::{
        block::{BlockType, ParsedItem},
        block_index::{BlockIndex, FullBlockIndex, TwoLevelBlockIndex, VolatileBlockIndex},
        filter::block::FilterBlock,
        regions::ParsedRegions,
        writer::LinkedFile,
    },
};
use alloc::borrow::Cow;
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};
use block_index::BlockIndexImpl;
use core::ops::{Bound, RangeBounds};
use inner::Inner;
use iter::Iter;

use crate::path::PathBuf;
use portable_atomic::AtomicU64;
use util::load_block;

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

pub type TableInner = Inner;

/// A disk segment (a.k.a. `Table`, `SSTable`, `SST`, `sorted string table`) that is located on disk
///
/// A table is an immutable list of key-value pairs, split into compressed blocks.
/// A reference to the block (`block handle`) is saved in the "block index".
///
/// Deleted entries are represented by tombstones.
///
/// Tables can be merged together to improve read performance and free unneeded disk space by removing outdated item versions.
#[doc(alias("sstable", "sst", "sorted string table"))]
#[derive(Clone)]
pub struct Table(
    Arc<Inner>,
    /// Tight-space restriction: when `Some(bound)`, this version's view of the
    /// table is clamped to keys `>= bound`. The on-disk data blocks below
    /// `bound` have been punched out ([`crate::fs::Fs::punch_hole`]) and their
    /// content lives in a freshly merged output table that supersedes them, so
    /// reads must not touch the punched prefix. Carried on the `Table` wrapper
    /// (not the shared `Arc<Inner>`) so an older snapshot keeps its own
    /// unrestricted view of the same physical SST. `None` on the common path.
    Option<UserKey>,
);

impl core::ops::Deref for Table {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
impl core::fmt::Debug for Table {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Table:{}({:?})", self.id(), self.metadata.key_range)
    }
}

/// Result of a bloom filter check.
enum BloomResult {
    /// Bloom says key is definitely absent — skip point read.
    Skip,
    /// Point read should proceed.
    Proceed {
        /// Whether a filter was present (used for metrics accounting).
        #[cfg_attr(
            not(feature = "metrics"),
            expect(
                dead_code,
                reason = "read by BloomResult::has_filter under metrics feature"
            )
        )]
        has_filter: bool,
    },
}

impl BloomResult {
    fn should_skip(&self) -> bool {
        matches!(self, Self::Skip)
    }

    #[cfg(feature = "metrics")]
    fn has_filter(&self) -> bool {
        matches!(self, Self::Proceed { has_filter: true })
    }
}

impl Table {
    #[must_use]
    pub fn global_seqno(&self) -> SeqNo {
        self.0.global_seqno
    }

    pub fn referenced_blob_bytes(&self) -> crate::Result<u64> {
        let cached = self
            .0
            .cached_blob_bytes
            .load(core::sync::atomic::Ordering::Acquire);
        if cached != u64::MAX {
            return Ok(cached);
        }

        let sum = self
            .list_blob_file_references()?
            .map(|bf| bf.iter().map(|f| f.on_disk_bytes).sum::<u64>())
            .unwrap_or_default();

        self.0
            .cached_blob_bytes
            .store(sum, core::sync::atomic::Ordering::Release);
        Ok(sum)
    }

    pub fn list_blob_file_references(&self) -> crate::Result<Option<Vec<LinkedFile>>> {
        use crate::io::{LE, ReadBytesExt};

        Ok(if let Some(handle) = &self.regions.linked_blob_files {
            let table_id = self.global_id();

            let (fd, _) = self
                .file_accessor
                .get_or_open_table(&table_id, &self.path)?;

            // Read the exact region using pread-style helper
            let buf =
                crate::file::read_exact(fd.as_ref(), *handle.offset(), handle.size() as usize)?;

            // Parse the buffer
            let mut reader = &buf[..];
            let len = reader.read_u32::<LE>()?;
            let mut blob_files = Vec::with_capacity(len as usize);

            for _ in 0..len {
                let blob_file_id = reader.read_u64::<LE>()?;
                let len = reader.read_u64::<LE>()?;
                let bytes = reader.read_u64::<LE>()?;
                let on_disk_bytes = reader.read_u64::<LE>()?;

                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "truncation is not expected to happen"
                )]
                blob_files.push(LinkedFile {
                    blob_file_id,
                    bytes,
                    len: len as usize,
                    on_disk_bytes,
                });
            }

            Some(blob_files)
        } else {
            None
        })
    }

    /// Gets the global table ID.
    #[must_use]
    fn global_id(&self) -> GlobalTableId {
        (self.tree_id, self.id()).into()
    }

    #[must_use]
    pub fn filter_size(&self) -> u32 {
        self.regions.filter.map(|x| x.size()).unwrap_or_default()
    }

    #[must_use]
    pub fn pinned_filter_size(&self) -> usize {
        self.pinned_filter_block
            .as_ref()
            .map(FilterBlock::size)
            .unwrap_or_default()
    }

    #[must_use]
    pub fn pinned_block_index_size(&self) -> usize {
        match &*self.block_index {
            BlockIndexImpl::Full(full_block_index) => full_block_index.inner().inner.size(),
            BlockIndexImpl::VolatileFull(_) | BlockIndexImpl::Closed => 0,
            BlockIndexImpl::TwoLevel(two_level_block_index) => {
                two_level_block_index.top_level_index.inner.size()
            }
        }
    }

    /// Gets the table ID.
    ///
    /// The table ID is unique for this tree, but not
    /// across multiple trees, use [`Table::global_id`] for that.
    #[must_use]
    pub fn id(&self) -> TableId {
        self.metadata.id
    }

    fn load_block(
        &self,
        handle: &BlockHandle,
        block_type: BlockType,
        compression: CompressionType,
        #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<Block> {
        load_block(
            self.global_id(),
            &self.path,
            &self.file_accessor,
            &self.cache,
            handle,
            block_type,
            compression,
            self.encryption.as_deref(),
            self.metadata.ecc_params,
            #[cfg(zstd_any)]
            zstd_dict,
            self.heal_hints.get().map(AsRef::as_ref),
            #[cfg(feature = "metrics")]
            &self.metrics,
        )
    }

    fn load_data_block(&self, handle: &BlockHandle) -> crate::Result<DataBlock> {
        // Columnar SSTs store each data block as a PAX `ColumnBatch`; reconstruct
        // the row entries on load so every row read path works unchanged.
        #[cfg(feature = "columnar")]
        if self.metadata.columnar {
            return self.load_columnar_data_block(handle);
        }
        // `from_loaded` transparently strips the per-KV checksum footer when
        // this SST carries one. Footer presence is a per-SST property
        // (`kv_checksum_algo`), not a per-block header flag — data blocks omit
        // the block_flags byte — so the descriptor supplies it here.
        let has_kv_footer = self.metadata.kv_checksum_algo.is_some();
        self.load_block(
            handle,
            BlockType::Data,
            self.metadata.data_block_compression,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )
        .and_then(|block| DataBlock::from_loaded(block, has_kv_footer))
    }

    /// Loads a columnar data block and reconstructs it as a row-major
    /// [`DataBlock`]: decode the `ColumnBatch`, rebuild the entries, and
    /// re-encode them row-major in memory so the existing point-read / iterator
    /// machinery is reused verbatim. The native column-projection read path
    /// (decode only the referenced columns) is a later optimization.
    #[cfg(feature = "columnar")]
    fn load_columnar_data_block(&self, handle: &BlockHandle) -> crate::Result<DataBlock> {
        let block = self.load_block(
            handle,
            BlockType::Columnar,
            self.metadata.data_block_compression,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )?;
        DataBlock::from_columnar_block(&block.data, self.metadata.data_block_restart_interval)
    }

    /// Loads a columnar data block and decodes only the projected columns,
    /// stepping over the rest without decoding them. The returned batch carries
    /// the requested columns for this block's rows. This is the projection read
    /// the vectorized scan uses, distinct from the whole-block reconstruction
    /// that the row read paths use.
    #[cfg(feature = "columnar")]
    fn load_columnar_block_projected(
        &self,
        handle: &BlockHandle,
        projection: &[u16],
    ) -> crate::Result<crate::table::columnar::ColumnBatch> {
        let block = self.load_block(
            handle,
            BlockType::Columnar,
            self.metadata.data_block_compression,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )?;
        crate::table::columnar::ColumnBatch::decode_projected(&block.data, projection)
    }

    /// Returns the (possibly compressed) file size.
    pub(crate) fn file_size(&self) -> u64 {
        self.metadata.file_size
    }

    /// Patrol-scrubs every data block of this table: a cache-bypassing read that
    /// runs the Page-ECC verify+correct path, recording a heal hint (when
    /// `auto_heal` is on) on a confirmed-persistent correction.
    ///
    /// Returns a partial [`PatrolScrubReport`](crate::scrub::PatrolScrubReport)
    /// for this SST (`sst_files_scanned == 1`) so the caller can merge it across
    /// the tree. Always runs to completion: an uncorrectable / unreadable block
    /// is recorded (and logged), not silently skipped, and the next block is
    /// still scrubbed. A block-index walk failure stops this table early (later
    /// offsets are untrustworthy) but other tables still scrub.
    #[cfg(feature = "std")]
    pub(crate) fn scrub_data_blocks(&self) -> crate::scrub::PatrolScrubReport {
        use crate::scrub::{PatrolScrubReport, ScrubError};
        use crate::table::util::{BlockScrubOutcome, scrub_block};

        let mut report = PatrolScrubReport {
            sst_files_scanned: 1,
            ..PatrolScrubReport::default()
        };

        for entry in self.block_index.iter() {
            let keyed = match entry {
                Ok(h) => h,
                Err(e) => {
                    // A structural index error means later offsets can't be
                    // trusted — stop this table, record it, let others run.
                    log::error!(
                        "patrol scrub: block index of table {} at {} unreadable: {e:?}",
                        self.id(),
                        self.path.display(),
                    );
                    report.errors.push(ScrubError::BlockIndexUnreadable {
                        table_id: self.id(),
                        path: self.path.to_path_buf(),
                        reason: alloc::format!("{e:?}"),
                    });
                    break;
                }
            };

            let block_offset = keyed.offset().0;
            let handle = BlockHandle::new(keyed.offset(), keyed.size());
            report.blocks_scanned += 1;

            match scrub_block(
                self.global_id(),
                &self.path,
                &self.file_accessor,
                &handle,
                BlockType::Data,
                self.metadata.data_block_compression,
                self.encryption.as_deref(),
                self.metadata.ecc_params,
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
                self.heal_hints.get().map(AsRef::as_ref),
                #[cfg(feature = "metrics")]
                &self.metrics,
            ) {
                Ok(BlockScrubOutcome::Clean) => {}
                Ok(BlockScrubOutcome::Corrected { scheduled }) => {
                    report.corrections_applied += 1;
                    if scheduled {
                        // heal_hints dedups per SST, so `scheduled` is true at
                        // most once per table — this counts distinct SSTs.
                        report.ssts_scheduled_for_rewrite += 1;
                    }
                }
                Err(e) => {
                    report.uncorrectable_blocks += 1;
                    log::error!(
                        "patrol scrub: uncorrectable block at offset {block_offset} in table {} \
                         at {}: {e:?}",
                        self.id(),
                        self.path.display(),
                    );
                    report.errors.push(ScrubError::UncorrectableBlock {
                        table_id: self.id(),
                        path: self.path.to_path_buf(),
                        block_offset,
                        reason: alloc::format!("{e:?}"),
                    });
                }
            }
        }

        report
    }

    /// Scrub: verifies the per-KV checksum footer of every data block in this
    /// table, decoding each block and recomputing each entry's logical-content
    /// digest.
    ///
    /// Footer presence is a per-SST property read from the descriptor
    /// (`metadata.kv_checksum_algo`), not a per-block header flag — SST data
    /// blocks omit the `block_flags` byte. When the descriptor reports no
    /// footers the whole scrub is a no-op; otherwise every data block is
    /// verified under the descriptor's algorithm. This is the paranoid /
    /// offline integrity path — the live read path does NOT verify per-entry
    /// digests (the block-level checksum already covers the on-disk bytes).
    /// Stops and returns on the first detected mismatch.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::ChecksumMismatch`] if any entry's recomputed digest
    ///   disagrees with the stored value (corruption of the entry bytes or
    ///   the stored digest).
    /// - Any I/O / decode error encountered while loading a block.
    pub(crate) fn verify_kv_checksums(&self) -> crate::Result<()> {
        // Footer presence is a per-SST property recorded in the descriptor
        // (`kv_checksum_algo`); data blocks omit the block_flags byte, so the
        // descriptor is the authoritative source. When it reports no footers,
        // there is nothing to scrub.
        let Some(expected_algo) = self.metadata.kv_checksum_algo else {
            return Ok(());
        };

        // Descriptor declares this SST footer-bearing, and an SST is
        // homogeneous — every data block carries a footer under `expected_algo`.
        for handle in self.block_index.iter() {
            let handle = handle?;
            let block_handle = BlockHandle::new(handle.offset(), handle.size());
            // Load the RAW block (footer intact) — do NOT route through
            // `load_data_block`, which strips the footer via `from_loaded`.
            let block = self.load_block(
                &block_handle,
                BlockType::Data,
                self.metadata.data_block_compression,
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
            )?;
            DataBlock::verify_kv_checked(
                &block.data,
                block.header,
                self.comparator.clone(),
                Some(expected_algo),
            )?;
        }
        Ok(())
    }

    /// Loads the filter block (if any) and checks the bloom filter.
    ///
    /// Returns `Ok(BloomResult::Skip)` if the bloom filter says the key is definitely absent
    /// (and updates metrics accordingly), `Ok(BloomResult::Proceed { has_filter })` otherwise.
    fn check_bloom(&self, key: &[u8], key_hash: u64) -> crate::Result<BloomResult> {
        debug_assert_eq!(
            key_hash,
            crate::hash::hash64(key),
            "key_hash must match the hash of the provided key"
        );

        let filter_block = if let Some(block) = &self.pinned_filter_block {
            Some(Cow::Borrowed(block))
        } else if let Some(filter_idx) = &self.pinned_filter_index {
            let mut iter = filter_idx.iter(self.comparator.clone());
            // Filter partitions are written with seqno=0, making the seqno
            // parameter irrelevant to partition selection. Use MAX_SEQNO
            // consistently to match the index-block seek in Table::range().
            iter.seek(key, crate::seqno::MAX_SEQNO);

            if let Some(filter_block_handle) = iter.next() {
                let filter_block_handle = filter_block_handle.materialize(filter_idx.as_slice());

                let block = self.load_block(
                    &filter_block_handle.into_inner(),
                    BlockType::Filter,
                    CompressionType::None,
                    #[cfg(zstd_any)]
                    None,
                )?;
                Some(Cow::Owned(FilterBlock::new(block)))
            } else {
                // Key sorts past the last filter partition — definite miss.
                #[cfg(feature = "metrics")]
                {
                    use core::sync::atomic::Ordering::Relaxed;
                    self.metrics.filter_queries.fetch_add(1, Relaxed);
                    self.metrics.io_skipped_by_filter.fetch_add(1, Relaxed);
                }
                return Ok(BloomResult::Skip);
            }
        } else if let Some(_filter_tli_handle) = &self.regions.filter_tli {
            unimplemented!("unpinned filter TLI not supported");
        } else if let Some(filter_block_handle) = &self.regions.filter {
            let block = self.load_block(
                filter_block_handle,
                BlockType::Filter,
                CompressionType::None,
                #[cfg(zstd_any)]
                None,
            )?;
            Some(Cow::Owned(FilterBlock::new(block)))
        } else {
            None
        };

        let has_filter = filter_block.is_some();

        if let Some(filter_block) = &filter_block
            && !filter_block.maybe_contains_hash(key_hash)?
        {
            #[cfg(feature = "metrics")]
            {
                use core::sync::atomic::Ordering::Relaxed;
                self.metrics.filter_queries.fetch_add(1, Relaxed);
                self.metrics.io_skipped_by_filter.fetch_add(1, Relaxed);
            }
            return Ok(BloomResult::Skip);
        }

        Ok(BloomResult::Proceed { has_filter })
    }

    /// Records a data-consulting point read for per-segment tiering / placement
    /// stats: a single `Relaxed` counter bump plus, on `std`, the access time.
    /// Called only after the seqno-range + bloom gates pass, so bloom misses do
    /// not inflate the count. Raw counter; the consumer derives a rate / EMA from
    /// successive polls.
    fn record_access(&self) {
        self.read_count
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        #[cfg(feature = "std")]
        self.last_access_secs.store(
            crate::time::unix_timestamp().as_secs(),
            core::sync::atomic::Ordering::Relaxed,
        );
    }

    pub fn get(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<InternalValue>> {
        // Tight-space restriction: this version sees the table only at keys
        // `>= bound` (the prefix below it is punched out and superseded by a
        // merged output table). Keys below `bound` must miss here so the read
        // falls through to that output; the punched blocks are never touched.
        if self.is_below_restriction(key) {
            return Ok(None);
        }

        let global_seqno = self.global_seqno();
        let seqno = seqno.saturating_sub(global_seqno);

        if self.metadata.seqnos.0 >= seqno {
            return Ok(None);
        }

        let bloom = self.check_bloom(key, key_hash)?;
        if bloom.should_skip() {
            return Ok(None);
        }

        // Access accounting after the seqno-range + bloom gates, so a segment
        // that excludes the key (seqno range) or rejects it (bloom miss) is not
        // counted as serving it.
        self.record_access();

        // Row-cache fast path: a prior latest-version read cached this key's
        // resolved value for this (immutable) SST, so we can skip the index walk
        // + data-block decode. The cached value is in table-local seqno space
        // (same as `point_read`). Use it only when the cached newest version is
        // visible at the query snapshot; otherwise fall through, because an older
        // version may apply at this snapshot.
        if let Some(mut iv) = self.cache.get_row(self.global_id(), key_hash, key) {
            // Snapshot reads are exclusive: a version is visible iff its seqno is
            // strictly less than the query seqno. Only serve the cached newest
            // version when it is visible; otherwise fall through (an older
            // version may apply at this snapshot).
            if iv.key.seqno < seqno {
                iv.key.seqno = iv.key.seqno.saturating_add(global_seqno);
                return Ok(Some(iv));
            }
        }

        let item = self.point_read(key, seqno, key_hash)?;

        // Populate the row cache only when this read could see the SST's newest
        // version (`seqno > max`, exclusive), so the resolved value is the SST's
        // newest version for this key — which keeps the seqno-visibility check
        // above correct for later snapshot reads. SSTs are immutable, so the
        // entry stays valid until the SST is compacted away.
        if seqno > self.metadata.seqnos.1
            && let Some(iv) = &item
        {
            self.cache
                .insert_row(self.global_id(), key_hash, iv.clone());
        }

        // Translate table-local seqno back to global coordinate so callers
        // can compare across tables/memtables (L0 best-selection, RT suppression).
        let item = item.map(|mut iv| {
            iv.key.seqno = iv.key.seqno.saturating_add(global_seqno);
            iv
        });

        #[cfg(feature = "metrics")]
        {
            use core::sync::atomic::Ordering::Relaxed;
            // NOTE: `check_bloom()` accounts for lookups rejected by the filter
            // (skip I/O entirely). This path accounts for negative point lookups
            // that still reached storage even though a filter was present, so
            // `filter_queries` remains interpretable alongside `filter_efficiency()`.
            // https://github.com/fjall-rs/lsm-tree/issues/246
            if item.is_none() && bloom.has_filter() {
                self.metrics.filter_queries.fetch_add(1, Relaxed);
            }
        }

        Ok(item)
    }

    /// Value-only point read: `(value_type, seqno, value)` without
    /// reconstructing the entry key. Used by the value-returning `get` path,
    /// which never reads the matched key (the caller has the needle), so the
    /// delta-key fusion in [`DataBlock::point_read`] is skipped. The value is a
    /// zero-copy slice of the cached block.
    pub(crate) fn get_value(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<(crate::ValueType, SeqNo, crate::Slice)>> {
        // Tight-space restriction (mirrors `Table::get`): a key below the bound
        // misses so the read falls through to the superseding output.
        if self.is_below_restriction(key) {
            return Ok(None);
        }

        let global_seqno = self.global_seqno();
        let seqno = seqno.saturating_sub(global_seqno);

        if self.metadata.seqnos.0 >= seqno {
            return Ok(None);
        }

        let bloom = self.check_bloom(key, key_hash)?;
        if bloom.should_skip() {
            return Ok(None);
        }

        // Access accounting after the seqno-range + bloom gates (mirrors `Table::get`).
        self.record_access();

        // Row-cache fast path (mirrors `Table::get`): serve the value tuple from
        // a prior cached point-read result, skipping the index walk + block
        // decode, when the cached newest version is visible at this snapshot.
        if let Some(iv) = self.cache.get_row(self.global_id(), key_hash, key) {
            // Exclusive snapshot visibility (see `Table::get`): serve only when
            // the cached newest version is strictly older than the query seqno.
            if iv.key.seqno < seqno {
                let s = iv.key.seqno.saturating_add(global_seqno);
                return Ok(Some((iv.key.value_type, s, iv.value)));
            }
        }

        let item = self.point_read_value(key, seqno, key_hash)?;

        // Populate only when this read could see the SST's newest version
        // (`seqno > max`, exclusive), mirroring `Table::get`. The value path does
        // not reconstruct the matched key, so rebuild the `InternalValue` from
        // the query key (the needle) + the resolved `(value_type, seqno, value)`.
        if seqno > self.metadata.seqnos.1
            && let Some((vt, s, v)) = &item
        {
            let iv = InternalValue {
                key: crate::key::InternalKey::new(crate::UserKey::from(key), *s, *vt),
                value: v.clone(),
            };
            self.cache.insert_row(self.global_id(), key_hash, iv);
        }

        // Translate table-local seqno back to the global coordinate, mirroring
        // `Table::get`.
        let item = item.map(|(vt, s, v)| (vt, s.saturating_add(global_seqno), v));

        #[cfg(feature = "metrics")]
        {
            use core::sync::atomic::Ordering::Relaxed;
            if item.is_none() && bloom.has_filter() {
                self.metrics.filter_queries.fetch_add(1, Relaxed);
            }
        }

        Ok(item)
    }

    /// Value-only block-index walk: companion to [`Table::point_read_inner`]
    /// that reads each candidate data block with
    /// [`DataBlock::point_read_value`] (no key fusion, no retained block).
    fn point_read_value(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<(crate::ValueType, SeqNo, crate::Slice)>> {
        // Fast path: retrieval-ribbon locator (see `point_read_inner` for the
        // MVCC-correctness argument). A located-block miss falls through to the
        // index walk below.
        if let Some((handle, hint)) = self.locator_block(key_hash)? {
            let data_block = self.load_data_block(&handle)?;
            let found = match hint {
                Some((slot, is_entry)) => data_block.point_read_value_at_slot(
                    slot,
                    is_entry,
                    key,
                    seqno,
                    &self.comparator,
                )?,
                None => data_block.point_read_value(key, seqno, &self.comparator)?,
            };
            if let Some(found) = found {
                return Ok(Some(found));
            }
        }

        let Some(iter) = self.block_index.point_read_reader(key, seqno) else {
            return Ok(None);
        };

        for block_handle in iter {
            let block_handle = block_handle?;

            let data_block = self.load_data_block(block_handle.as_ref())?;

            if let Some(found) = data_block.point_read_value(key, seqno, &self.comparator)? {
                return Ok(Some(found));
            }

            if self.comparator.compare(block_handle.end_key(), key) == core::cmp::Ordering::Greater
            {
                return Ok(None);
            }
        }

        Ok(None)
    }

    /// Like [`Table::get`], but also returns the [`Block`] containing the value.
    ///
    /// Used by `get_pinned()` to construct `PinnableSlice::Pinned`.
    ///
    pub(crate) fn get_with_block(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<(InternalValue, Block)>> {
        // Tight-space restriction (mirrors `Table::get`): a key below the bound
        // misses so the read falls through to the superseding output and never
        // touches the punched-out prefix.
        if self.is_below_restriction(key) {
            return Ok(None);
        }

        let global_seqno = self.global_seqno();
        let seqno = seqno.saturating_sub(global_seqno);

        if self.metadata.seqnos.0 >= seqno {
            return Ok(None);
        }

        let bloom = self.check_bloom(key, key_hash)?;
        if bloom.should_skip() {
            return Ok(None);
        }

        // Access accounting after the seqno-range + bloom gates (mirrors `Table::get`).
        self.record_access();

        let result = self.point_read_with_block(key, seqno, key_hash)?;

        // Translate table-local seqno back to global coordinate (see Table::get).
        let result = result.map(|(mut iv, block)| {
            iv.key.seqno = iv.key.seqno.saturating_add(global_seqno);
            (iv, block)
        });

        #[cfg(feature = "metrics")]
        {
            use core::sync::atomic::Ordering::Relaxed;
            if result.is_none() && bloom.has_filter() {
                self.metrics.filter_queries.fetch_add(1, Relaxed);
            }
        }

        Ok(result)
    }

    /// Shared block-index walk for point reads. Returns the matching entry
    /// together with the [`DataBlock`] it was found in, so callers that need
    /// the block (e.g. for [`PinnableSlice`]) can keep it alive.
    /// Resolve the data block holding `key_hash`'s newest version (plus an
    /// optional in-block slot hint) via the retrieval-ribbon locator, if one is
    /// loaded. `Ok(None)` means no locator or the ribbon could not answer → the
    /// caller uses the sorted-index walk.
    fn locator_block(
        &self,
        key_hash: u64,
    ) -> crate::Result<Option<crate::table::locator::Located>> {
        match &self.locator_index {
            Some(loc) => loc.locate_block(key_hash),
            None => Ok(None),
        }
    }

    fn point_read_inner(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<(InternalValue, DataBlock)>> {
        // Fast path: a retrieval-ribbon locator resolves the key to its data
        // block in O(1), skipping the index-block binary search. The located
        // block holds the newest version (the run's highest-seqno prefix), so a
        // hit returns the correct MVCC answer and a miss (absent key, or the
        // visible version lives in a later block) safely falls through to the
        // index walk below.
        if let Some((handle, hint)) = self.locator_block(key_hash)? {
            let data_block = self.load_data_block(&handle)?;
            let found = match hint {
                Some((slot, is_entry)) => {
                    data_block.point_read_at_slot(slot, is_entry, key, seqno, &self.comparator)?
                }
                None => data_block.point_read(key, seqno, &self.comparator)?,
            };
            if let Some(item) = found {
                return Ok(Some((item, data_block)));
            }
        }

        // Borrowing point-read seek: avoids cloning the index block + reuses
        // the trailer metadata parsed at table open (see
        // `BlockIndexImpl::point_read_reader`).
        let Some(iter) = self.block_index.point_read_reader(key, seqno) else {
            return Ok(None);
        };

        for block_handle in iter {
            let block_handle = block_handle?;

            let data_block = self.load_data_block(block_handle.as_ref())?;

            if let Some(item) = data_block.point_read(key, seqno, &self.comparator)? {
                return Ok(Some((item, data_block)));
            }

            // NOTE: If the last block key is higher than ours,
            // our key cannot be in the next block
            if self.comparator.compare(block_handle.end_key(), key) == core::cmp::Ordering::Greater
            {
                return Ok(None);
            }
        }

        Ok(None)
    }

    fn point_read(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<InternalValue>> {
        self.point_read_inner(key, seqno, key_hash)
            .map(|opt| opt.map(|(iv, _)| iv))
    }

    /// Like [`Table::point_read`], but also returns the underlying [`Block`].
    ///
    /// Holding on to the returned [`Block`] (e.g. for [`PinnableSlice`]) keeps the
    /// block data alive while the value is in use, but does not guarantee that the
    /// cache will retain its own entry for that block.
    fn point_read_with_block(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<(InternalValue, Block)>> {
        self.point_read_inner(key, seqno, key_hash)
            .map(|opt| opt.map(|(iv, db)| (iv, db.inner)))
    }

    /// Batch point-read variant of [`Table::get`].
    ///
    /// Each input pair is `(key, key_hash)`. The slice **must be
    /// strictly sorted ascending by `key` under this table's
    /// comparator** — duplicate adjacent keys are a caller bug
    /// (callers should dedup before batching; a duplicate
    /// suggests a logic error in the query construction) and
    /// are rejected by a `debug_assert!` in debug builds.
    /// Returns one `Option<InternalValue>` per input pair in
    /// input order: `Some(_)` for found values (including
    /// tombstones — callers distinguish via [`InternalValue`]'s
    /// value type), `None` for absent keys.
    ///
    /// # Hash contract
    ///
    /// `key_hash` **must** equal `crate::hash::hash64(key)` — the
    /// same function the writer used when populating the bloom
    /// filter. The bloom probe consumes the hash; the
    /// key↔hash agreement check is a `debug_assert!` only, so
    /// release builds trust the caller. Passing a wrong hash in
    /// release produces false-negative skips: the corresponding
    /// `results[i]` slot stays `None` as if the key weren't in
    /// the table (the result vector itself is always
    /// `sorted_keys.len()` long — nothing is dropped from it).
    /// Callers should derive both values from the same
    /// `(&[u8], u64) = (key, hash64(key))` expression at the
    /// same scope to make the agreement trivially auditable.
    ///
    /// In partitioned-filter mode (`pinned_filter_index` /
    /// `filter_tli`), `check_bloom` ALSO uses the raw `key`
    /// bytes — not the hash — to select which filter partition
    /// to probe. The hash drives the bit probes inside the
    /// selected partition. Bottom line: BOTH inputs are
    /// load-bearing in the partitioned case; only the
    /// monolithic-filter case is "hash-only".
    ///
    /// # Why this exists vs. calling [`Table::get`] in a loop
    ///
    /// Sequential per-key calls each pay:
    ///
    /// 1. Bloom-filter dereference + N hash probes — duplicated
    ///    across calls.
    /// 2. Block-index seek from scratch — every call walks
    ///    `forward_reader(key, seqno)` and re-pays the index
    ///    binary search even when the previous call already
    ///    landed inside the same data block.
    /// 3. Block load — every call re-fetches the data block
    ///    from cache, so cache hits still pay a hashmap lookup
    ///    + Arc clone per call.
    ///
    /// `batch_get` collapses all three:
    ///
    /// 1. Filter probed once per key in a tight loop. For
    ///    monolithic filters (the default) the filter block is
    ///    fetched once and the loop just checks N hashes
    ///    against it. For partitioned filters
    ///    (`pinned_filter_index` / `filter_tli`), each probe
    ///    still seeks the partition index and may load the
    ///    relevant partition block lazily — so "one filter
    ///    fetch total" only holds in the monolithic case;
    ///    partitioned filters amortise loads across keys that
    ///    land in the same partition rather than across the
    ///    whole batch.
    /// 2. Block-index seek runs once at the smallest passing
    ///    key, then the iterator walks forward across the
    ///    sorted input — no re-seek per key.
    /// 3. Each data block is loaded at most once for the entire
    ///    batch. Multiple input keys that fall in the same block
    ///    share a single load.
    ///
    /// The wire-format is identical to N independent `get()`
    /// calls; the savings are purely call-overhead.
    ///
    /// # Sort requirement
    ///
    /// Sorting is the caller's responsibility because the
    /// `batch_get_from_tables` driver already maintains the
    /// remaining-keys list in comparator order between L1+ runs
    /// (re-sorted after each `covered_miss` split). Re-sorting
    /// inside `batch_get` would be redundant work; passing
    /// pre-sorted input lets the implementation rely on a
    /// monotone two-pointer walk between input keys and block
    /// boundaries.
    ///
    /// # Errors
    ///
    /// Propagates any I/O / corruption error from the filter
    /// fetch, block-index read, or data-block load. On error
    /// the partial `results` vector is discarded — callers
    /// observe an all-or-nothing outcome per call.
    #[expect(
        clippy::indexing_slicing,
        reason = "every index access in this routine is bounded by construction: \
                  `passing` indices are produced from enumerate(sorted_keys) so they're \
                  < sorted_keys.len() == results.len(); `passing[p]` is guarded by \
                  `p < passing.len()` on every loop iteration; `passing[0]` is read \
                  only after an explicit emptiness check above."
    )]
    pub fn batch_get(
        &self,
        sorted_keys: &[(&[u8], u64)],
        seqno: SeqNo,
    ) -> crate::Result<Vec<Option<InternalValue>>> {
        let mut results: Vec<Option<InternalValue>> = vec![None; sorted_keys.len()];

        if sorted_keys.is_empty() {
            return Ok(results);
        }

        // Debug-time guard for the sorted-input contract.
        // Unsorted input would silently return wrong Nones
        // (the two-pointer walk between block_iter and the
        // input slice assumes monotone keys); catch the
        // accidental misuse before it ships to a release
        // benchmark. Strict-monotone is the contract — equal
        // adjacent keys would be a duplicate query, also a
        // caller bug.
        debug_assert!(
            sorted_keys
                .windows(2)
                .all(|w| self.comparator.compare(w[0].0, w[1].0) == core::cmp::Ordering::Less),
            "batch_get input must be strictly sorted ascending by key under \
             the table's comparator; unsorted/duplicate input produces silent \
             None misses because the two-pointer walk assumes monotone keys"
        );

        let global_seqno = self.global_seqno();
        let table_seqno = seqno.saturating_sub(global_seqno);

        // Table is entirely above the snapshot — no key is visible.
        if self.metadata.seqnos.0 >= table_seqno {
            return Ok(results);
        }

        // Filter the input through the bloom filter once. The
        // filter resource (mmap / Arc) is fetched lazily by
        // check_bloom on the first call; subsequent calls reuse
        // it through the table-internal cache.
        let mut passing: Vec<usize> = Vec::with_capacity(sorted_keys.len());
        #[cfg(feature = "metrics")]
        let mut had_filter = false;
        for (i, (key, hash)) in sorted_keys.iter().enumerate() {
            let bloom = self.check_bloom(key, *hash)?;
            if !bloom.should_skip() {
                passing.push(i);
                #[cfg(feature = "metrics")]
                if bloom.has_filter() {
                    had_filter = true;
                }
            }
        }
        if passing.is_empty() {
            return Ok(results);
        }

        // Seek the block index once at the smallest passing key.
        // forward_reader returns the first block whose end_key
        // can cover that key; everything past it walks forward.
        let first_key = sorted_keys[passing[0]].0;
        let Some(mut block_iter) = self.block_index.forward_reader(first_key, table_seqno) else {
            // No block can contain the smallest passing key — every
            // passing key is "negative with filter present" for
            // metrics accounting purposes, mirroring Table::get
            // where a bloom-passing key that point_read can't find
            // increments filter_queries. Falling through to the
            // shared metrics block below ensures the batch path
            // doesn't under-report compared to N independent get()s.
            #[cfg(feature = "metrics")]
            {
                // Use core::* rather than std::* re-exports: the
                // `metrics` feature isn't std-gated in Cargo.toml,
                // and `Ordering` lives in `core::sync::atomic`
                // unchanged — keeps this hot-path import no-std
                // friendly without any runtime impact (the std
                // path is just a re-export of the core symbol).
                use core::sync::atomic::Ordering::Relaxed;
                if had_filter && !passing.is_empty() {
                    self.metrics
                        .filter_queries
                        .fetch_add(passing.len(), Relaxed);
                }
            }
            return Ok(results);
        };

        // Two-pointer walk: outer loop advances block_iter, inner
        // loop drains passing keys that fall inside the current
        // block's range. Both sides are monotone (sorted by the
        // same comparator), so each side advances at most once
        // per pair.
        let mut p = 0_usize;
        while p < passing.len() {
            let Some(handle_result) = block_iter.next() else {
                break;
            };
            let block_handle = handle_result?;
            let end_key = block_handle.end_key();

            // Lazy load: only fetch the data block if at least
            // one passing key falls into this block's range.
            // Most blocks will contain at least one key (we
            // seeked here precisely because the first key did),
            // but bloom may have skipped enough later keys that
            // the next passing one is in a later block — in
            // which case we skip the load.
            let first_in_block = sorted_keys[passing[p]].0;
            if self.comparator.compare(first_in_block, end_key) == core::cmp::Ordering::Greater {
                // The next passing key is BEYOND this block's
                // range. Skip the load and advance to the next
                // block in the index.
                continue;
            }

            let data_block = self.load_data_block(block_handle.as_ref())?;

            // Drain passing keys that fall inside [..end_key].
            //
            // Three-way handling mirrors Table::point_read_inner's
            // end-key boundary check:
            //   - Greater (key > end_key): key belongs to a later
            //     block. Break inner loop, advance outer.
            //   - Less    (key < end_key): key is strictly inside
            //     this block. point_read decides; either way the
            //     key cannot continue into the next block (block
            //     keys are sorted, and a later block's first key
            //     is > this block's end_key), so we always advance
            //     p — set Some on hit, leave None on miss.
            //   - Equal   (key == end_key): block end_key matches
            //     the query exactly. point_read may return None
            //     even when a visible version of THIS user key
            //     exists in the NEXT block (same-key spans block
            //     boundary — common with MVCC versions of a hot
            //     key). On None, do NOT advance p — break out so
            //     the next outer iteration loads the next block
            //     and retries the same key.
            while p < passing.len() {
                let key_idx = passing[p];
                let key = sorted_keys[key_idx].0;
                match self.comparator.compare(key, end_key) {
                    core::cmp::Ordering::Greater => break,
                    core::cmp::Ordering::Less => {
                        if let Some(mut item) =
                            data_block.point_read(key, table_seqno, &self.comparator)?
                        {
                            // Translate table-local seqno back to
                            // the global coordinate so callers can
                            // compare results across tables /
                            // memtables (matches Table::get's
                            // contract).
                            item.key.seqno = item.key.seqno.saturating_add(global_seqno);
                            results[key_idx] = Some(item);
                        }
                        p += 1;
                    }
                    core::cmp::Ordering::Equal => {
                        if let Some(mut item) =
                            data_block.point_read(key, table_seqno, &self.comparator)?
                        {
                            item.key.seqno = item.key.seqno.saturating_add(global_seqno);
                            results[key_idx] = Some(item);
                            p += 1;
                        } else {
                            // Same user key may continue in the
                            // next block — leave p in place so the
                            // outer loop's next iteration retries
                            // this key against the next block.
                            break;
                        }
                    }
                }
            }
        }

        #[cfg(feature = "metrics")]
        {
            // core::* (vs the std re-export) for no-std friendliness;
            // see the comment on the matching import above.
            use core::sync::atomic::Ordering::Relaxed;
            // Mirror Table::get's accounting: count negative
            // point lookups that reached storage despite a
            // filter being present. Only keys that passed bloom
            // AND came back empty count.
            if had_filter {
                let negative_with_filter =
                    passing.iter().filter(|&&i| results[i].is_none()).count();
                if negative_with_filter > 0 {
                    // filter_queries is AtomicUsize; the count is
                    // already a usize, no conversion needed.
                    self.metrics
                        .filter_queries
                        .fetch_add(negative_with_filter, Relaxed);
                }
            }
        }

        Ok(results)
    }

    /// Creates a scanner over the `Table`.
    ///
    /// The scanner is ĺogically the same as a normal iter(),
    /// however it uses its own file descriptor, does not look into the block cache
    /// and uses buffered I/O.
    ///
    /// Used for compactions and thus not available to a user.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn scan(&self) -> crate::Result<Scanner> {
        #[expect(
            clippy::expect_used,
            reason = "there shouldn't be 4 billion data blocks in a single table"
        )]
        let block_count = self
            .metadata
            .data_block_count
            .try_into()
            .expect("data block count should fit");

        Scanner::new(
            &self.fs,
            &self.path,
            block_count,
            self.metadata.data_block_compression,
            self.global_seqno(),
            self.encryption.clone(),
            self.metadata.ecc_params,
            self.metadata.kv_checksum_algo.is_some(),
            #[cfg(zstd_any)]
            self.zstd_dictionary.clone(),
            self.comparator.clone(),
            self.metadata.id,
            self.metadata.columnar,
            self.metadata.data_block_restart_interval,
        )
    }

    /// Scans this columnar SST block by block, returning one [`ColumnBatch`] per
    /// data block that survives the optional predicate, each carrying only the
    /// projected columns.
    ///
    /// `projection` lists the column ids to decode; every other column is
    /// stepped over without decoding. When `predicate` is set, a block whose
    /// zone-map proves it out of range is skipped without being loaded, and each
    /// surviving block is filtered to the rows that match.
    ///
    /// [`ColumnBatch`]: crate::table::columnar::ColumnBatch
    ///
    /// # Errors
    ///
    /// Returns an error if this SST is not columnar, or on a block read / decode
    /// failure.
    #[cfg(feature = "columnar")]
    pub fn columnar_scan(
        &self,
        projection: &[u16],
        predicate: Option<&crate::table::columnar_predicate::ColumnRangePredicate>,
    ) -> crate::Result<Vec<crate::table::columnar::ColumnBatch>> {
        if !self.metadata.columnar {
            return Err(crate::Error::FeatureUnsupported("columnar"));
        }
        // The predicate must see its own column, even when the caller did not
        // project it; decode it too and drop it from each output batch, so a
        // predicate on an unprojected column still filters instead of matching
        // every row.
        let mut decode_projection = projection.to_vec();
        let added_predicate_column = match predicate {
            Some(pred) if !decode_projection.contains(&pred.column_id) => {
                decode_projection.push(pred.column_id);
                Some(pred.column_id)
            }
            _ => None,
        };
        let mut out = Vec::new();
        for keyed in self.block_index.iter() {
            let keyed = keyed?;
            // Zone-map block skip: prove the block is out of range and never
            // load it. A missing entry is conservative (cannot skip).
            if let Some(pred) = predicate
                && let Some(stats) = self.zone_map.columns_for(*keyed.offset())
                && pred.can_skip_block(stats)
            {
                continue;
            }
            let handle = BlockHandle::new(keyed.offset(), keyed.size());
            let batch = self.load_columnar_block_projected(&handle, &decode_projection)?;
            let mut batch = match predicate {
                Some(pred) => {
                    let mask = pred.matching_rows(&batch);
                    crate::table::columnar_predicate::filter_batch(&batch, &mask)
                }
                None => batch,
            };
            if let Some(column_id) = added_predicate_column {
                batch.columns.retain(|c| c.column_id != column_id);
            }
            out.push(batch);
        }
        Ok(out)
    }

    /// Creates an iterator over the `Table`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    #[doc(hidden)]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> + use<> {
        self.range(..)
    }

    /// Collects every entry in this SST with `seqno >= target_seqno`,
    /// applying the per-block seqno-bounds skip when the SST carries it.
    ///
    /// A data block whose `seqno_bounds` section entry reports
    /// `seqno_max < target_seqno` cannot hold a qualifying record, so it is
    /// skipped without being read. When the SST has no `seqno_bounds` section
    /// (the feature was off), every block is read and filtered per entry, so
    /// the result is correct regardless. Entries come back in the SST's stored
    /// order (key-ascending,
    /// seqno-descending within a key); ordering across sources is the caller's
    /// job.
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading the index or a data block fails.
    #[doc(hidden)]
    pub fn scan_since_seqno(&self, target_seqno: SeqNo) -> crate::Result<Vec<InternalValue>> {
        self.scan_seqno_range(target_seqno, SeqNo::MAX, true)
    }

    /// Like [`Self::scan_since_seqno`] but also bounds the result above:
    /// collects entries whose global seqno is in `[target_seqno, end_seqno)`.
    /// The upper bound lets the tree-level scan pin a stable snapshot watermark
    /// so a concurrent write cannot leak in mid-scan.
    ///
    /// `block_skip` enables the per-block seqno-bounds optimization (skip data
    /// blocks whose recorded `[seqno_min, seqno_max]` cannot overlap the
    /// window). Pass `false` for a paranoid full scan that reads every block and
    /// filters per entry, so even an undetected-corrupt seqno bound (one that
    /// somehow slipped past the block XXH3 checksum) cannot cause a qualifying
    /// record to be skipped.
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading the index or a data block fails.
    #[doc(hidden)]
    pub fn scan_seqno_range(
        &self,
        target_seqno: SeqNo,
        end_seqno: SeqNo,
        block_skip: bool,
    ) -> crate::Result<Vec<InternalValue>> {
        // Bulk-ingested tables store entries at LOCAL seqno coordinates with a
        // `global_seqno` offset; the on-disk seqno bounds and per-entry seqnos
        // are all local. Translate the incoming global target down to local
        // for the comparisons, then translate matched record seqnos back up to
        // global before returning — exactly as `Table::get` does. For a
        // non-ingested table `global_seqno` is 0 and both translations are
        // no-ops.
        let global_seqno = self.global_seqno();
        let local_target = target_seqno.saturating_sub(global_seqno);
        // Upper bound in local coords. `SeqNo::MAX` (the unbounded case) stays
        // MAX so every entry passes; a real watermark maps below the offset to
        // 0, correctly excluding the whole table.
        let local_end = end_seqno.saturating_sub(global_seqno);

        // Empty window (e.g. a caught-up CDC poller whose target equals the
        // current watermark): nothing can qualify, so skip walking the index
        // entirely. Without this a legacy SST (no per-block seqno bounds) would
        // load + filter every block to return nothing on every poll.
        if local_target >= local_end {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();

        for handle in self.block_index.iter() {
            let handle = handle?;

            // Tight-space restriction: a block whose last key is below the bound
            // sits entirely in the punched-out (zeroed) prefix — never read it.
            // The block straddling the bound is intact (punch starts at its
            // offset) and is filtered per entry in the loop below.
            if let Some(bound) = &self.1
                && self.comparator.compare(handle.end_key(), bound) == core::cmp::Ordering::Less
            {
                continue;
            }

            // Block-skip: look this block's seqno bounds up in the parallel
            // `seqno_bounds` section (keyed by file offset). If its (local) min
            // exceeds the upper bound, or its (local) max is below the target, it
            // cannot reference a qualifying record — skip the data-block read.
            // Bounds live in the section, NOT inline in the index entry, so a
            // point read never pays for them. Disabled in paranoid full-scan
            // mode (`block_skip == false`); absent for legacy/off tables → no
            // skip, full filter (correct regardless).
            if block_skip
                && let Some((seqno_min, seqno_max)) =
                    self.seqno_bounds.bounds_for(handle.as_ref().offset().0)
                && (seqno_max < local_target || seqno_min >= local_end)
            {
                continue;
            }

            let block = self.load_data_block(handle.as_ref())?;
            let data = &block.inner.data;
            for item in block.iter(self.comparator.clone()) {
                let mut value = item.materialize(data);
                // Drop entries below the restriction bound in the straddling
                // block (their authoritative copy lives in the superseding
                // output table).
                if let Some(bound) = &self.1
                    && self.comparator.compare(&value.key.user_key, bound)
                        == core::cmp::Ordering::Less
                {
                    continue;
                }
                if value.key.seqno >= local_target && value.key.seqno < local_end {
                    value.key.seqno = value.key.seqno.saturating_add(global_seqno);
                    out.push(value);
                }
            }
        }

        Ok(out)
    }

    /// Creates a ranged iterator over the `Table`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    #[doc(hidden)]
    pub fn range<R: RangeBounds<UserKey> + Send>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> + Send + use<R> {
        self.range_iter(range)
    }

    /// Like [`Self::range`] but returns the concrete [`iter::Iter`] reader.
    ///
    /// The seekable range pipeline holds the concrete type so it can re-position
    /// the reader in place via [`Self::reseek_range`] instead of rebuilding it.
    pub(crate) fn range_iter<R: RangeBounds<UserKey> + Send>(&self, range: R) -> iter::Iter {
        let index_iter = self.block_index.iter();

        let mut iter = Iter::new(
            self.global_id(),
            self.global_seqno(),
            self.path.clone(),
            index_iter,
            self.file_accessor.clone(),
            self.cache.clone(),
            self.metadata.data_block_compression,
            self.encryption.clone(),
            self.metadata.ecc_params,
            self.heal_hints.get().cloned(),
            self.metadata.kv_checksum_algo.is_some(),
            self.metadata.columnar,
            #[cfg(zstd_any)]
            self.zstd_dictionary.clone(),
            self.comparator.clone(),
            #[cfg(feature = "zstd")]
            self.block_layout.clone(),
            #[cfg(feature = "zstd")]
            self.metadata.data_block_restart_interval,
            #[cfg(feature = "metrics")]
            self.metrics.clone(),
        );

        match range.start_bound() {
            Bound::Included(key) => iter.set_lower_bound(iter::Bound::Included(key.clone())),
            Bound::Excluded(key) => iter.set_lower_bound(iter::Bound::Excluded(key.clone())),
            Bound::Unbounded => {}
        }

        // Tight-space restriction: raise the scan's lower bound up to `bound`
        // when this version restricts the table, so the iterator never walks
        // index entries pointing into the punched-out (zeroed) prefix below
        // `bound`. Only raises (never lowers) the requested start: a request
        // already at or above `bound` is left untouched.
        if let Some(bound) = &self.1 {
            let raise = match range.start_bound() {
                Bound::Included(key) | Bound::Excluded(key) => {
                    self.comparator.compare(bound, key) == core::cmp::Ordering::Greater
                }
                Bound::Unbounded => true,
            };
            if raise {
                iter.set_lower_bound(iter::Bound::Included(bound.clone()));
            }
        }

        match range.end_bound() {
            Bound::Included(key) => iter.set_upper_bound(iter::Bound::Included(key.clone())),
            Bound::Excluded(key) => iter.set_upper_bound(iter::Bound::Excluded(key.clone())),
            Bound::Unbounded => {}
        }

        iter
    }

    /// Re-position an existing [`iter::Iter`] (produced by [`Self::range`] on
    /// this same table) to a fresh `range`, reusing its owned index iterator and
    /// `Arc` handles instead of constructing a new reader.
    ///
    /// Applies the exact same bound translation as [`Self::range`] (including the
    /// tight-space lower-bound raise), so the re-seeked iterator yields the same
    /// entries a freshly-built `self.range(range)` would. Used by the seekable
    /// range pipeline to move leaf cursors without per-seek allocation.
    #[doc(hidden)]
    pub fn reseek_range<R: RangeBounds<UserKey> + Send>(&self, iter: &mut iter::Iter, range: R) {
        iter.reset_for_reseek();

        match range.start_bound() {
            Bound::Included(key) => iter.set_lower_bound(iter::Bound::Included(key.clone())),
            Bound::Excluded(key) => iter.set_lower_bound(iter::Bound::Excluded(key.clone())),
            Bound::Unbounded => {}
        }

        // Mirror `range()`'s tight-space restriction: raise the scan's lower
        // bound up to `bound` when this version restricts the table.
        if let Some(bound) = &self.1 {
            let raise = match range.start_bound() {
                Bound::Included(key) | Bound::Excluded(key) => {
                    self.comparator.compare(bound, key) == core::cmp::Ordering::Greater
                }
                Bound::Unbounded => true,
            };
            if raise {
                iter.set_lower_bound(iter::Bound::Included(bound.clone()));
            }
        }

        match range.end_bound() {
            Bound::Included(key) => iter.set_upper_bound(iter::Bound::Included(key.clone())),
            Bound::Excluded(key) => iter.set_upper_bound(iter::Bound::Excluded(key.clone())),
            Bound::Unbounded => {}
        }
    }

    fn read_tli(
        regions: &ParsedRegions,
        file: &dyn FsFile,
        table_id: TableId,
        compression: CompressionType,
        encryption: Option<&dyn crate::encryption::EncryptionProvider>,
        ecc: Option<crate::table::block::EccParams>,
    ) -> crate::Result<IndexBlock> {
        // Tail copy first (preferred): if a fresh `tli_tail` exists it
        // landed after the head `tli`, so it's the most-recently
        // fsynced copy. On any decode / decrypt / checksum failure
        // fall back to the head `tli` if present.
        //
        // Both copies encode the same handles list (the writer hands
        // a single `tli_bytes` buffer to both sites) and both are
        // written under the same `CompressionType`
        // (`metadata.index_block_compression`); the block header does
        // not record a compression tag, so this single value decodes
        // either copy. Encryption nonce differs per copy (fresh per
        // `Block::write_into`) and the ciphertext therefore differs
        // byte-for-byte, but both decrypt to the same plaintext
        // IndexBlock.
        //
        // Tables written before the TLI-mirror change have no
        // `tli_tail`; reader falls straight through to the head copy.
        if let Some(tail_handle) = regions.tli_tail {
            log::trace!("Reading TLI tail mirror, with tli_tail_ptr={tail_handle:?}");
            match Self::read_tli_at(file, tail_handle, table_id, compression, encryption, ecc) {
                Ok(idx) => return Ok(idx),
                Err(tail_err) => {
                    log::warn!(
                        "TLI tail mirror unreadable ({tail_err}); falling back to TLI head copy at {:?}",
                        regions.tli,
                    );
                    // Match the meta-mirror pattern: when BOTH
                    // copies fail, surface the original `tail_err`
                    // (callers care about the authoritative /
                    // preferred copy's failure mode). The head
                    // failure goes to the log so it's not silently
                    // dropped from diagnostics.
                    log::trace!("Reading TLI head copy, with tli_ptr={:?}", regions.tli);
                    return match Self::read_tli_at(
                        file,
                        regions.tli,
                        table_id,
                        compression,
                        encryption,
                        ecc,
                    ) {
                        Ok(idx) => Ok(idx),
                        Err(head_err) => {
                            log::warn!(
                                "TLI head copy also unreadable ({head_err}); returning original tail error",
                            );
                            Err(tail_err)
                        }
                    };
                }
            }
        }

        log::trace!("Reading TLI head copy, with tli_ptr={:?}", regions.tli);
        Self::read_tli_at(file, regions.tli, table_id, compression, encryption, ecc)
    }

    fn read_tli_at(
        file: &dyn FsFile,
        handle: BlockHandle,
        table_id: TableId,
        compression: CompressionType,
        encryption: Option<&dyn crate::encryption::EncryptionProvider>,
        ecc: Option<crate::table::block::EccParams>,
    ) -> crate::Result<IndexBlock> {
        let block = Block::from_file(
            file,
            handle,
            crate::table::block::BlockIdentity {
                table_id,
                block_type: BlockType::Index,
                dict_id: 0,
                window_log: 0,
            },
            &{
                // Index blocks are SST blocks that omit the block_flags byte,
                // so ECC presence comes from the per-SST descriptor: upgrade
                // to the `*Ecc` transform when this table was written with
                // Page ECC. Identity without the feature.
                let t = crate::table::block::BlockTransform::from_parts(
                    compression,
                    encryption,
                    #[cfg(zstd_any)]
                    None,
                )?;
                if let Some(ecc) = ecc {
                    t.with_ecc(ecc)
                } else {
                    t
                }
            },
        )?;

        if block.header.block_type != BlockType::Index {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }

        Ok(IndexBlock::new(block))
    }

    /// Tries to recover a table from a file.
    #[expect(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "recovery requires many context parameters and is inherently complex"
    )]
    pub fn recover(
        file_path: PathBuf,
        checksum: Checksum,
        global_seqno: SeqNo,
        tree_id: TreeId,
        table_id: TableId,
        cache: Arc<Cache>,
        descriptor_table: Option<Arc<DescriptorTable>>,
        fs: Arc<dyn Fs>,
        pin_filter: bool,
        pin_index: bool,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
        #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
        comparator: SharedComparator,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> crate::Result<Self> {
        use core::sync::atomic::AtomicBool;
        use meta::ParsedMeta;
        use regions::ParsedRegions;

        log::debug!("Recovering table from file {}", file_path.display());
        let mut file = fs.open(&file_path, &FsOpenOptions::new().read(true))?;
        let file_path = Arc::new(file_path);

        #[cfg(feature = "metrics")]
        metrics
            .table_file_opened_uncached
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);

        let trailer = crate::sfa::Reader::from_reader(&mut file)?;
        let regions = ParsedRegions::parse_from_toc(trailer.toc())?;

        log::trace!("Reading meta block, with meta_ptr={:?}", regions.metadata);
        // TAIL first (authoritative copy by convention; physically
        // identical content to MID — same `file_size`, same
        // `created_at`, same KV map — the only difference is which
        // SFA section is loaded). On any decode/decrypt/checksum
        // failure fall back to the MID copy if present.
        let metadata = match ParsedMeta::load_with_handle(
            &*file,
            &regions.metadata,
            Some(table_id),
            encryption.as_deref(),
        ) {
            Ok(m) => m,
            Err(tail_err) => {
                if let Some(mid_handle) = regions.metadata_mid {
                    log::warn!(
                        "TAIL meta block unreadable for {} ({tail_err}); falling back to MID copy",
                        file_path.display(),
                    );
                    // Match the PR contract: when BOTH copies fail,
                    // surface the original TAIL error (callers care
                    // about the authoritative copy's failure mode).
                    // The MID failure goes to the log so it's not
                    // silently dropped from diagnostics.
                    // MID and TAIL are byte-identical: same `file_size`
                    // (= `*self.meta.file_pos`, only bumped inside
                    // `spill_block`, unchanged between the two writes),
                    // same `created_at` (snapshotted once in
                    // `finish()`), same KV map. MID payload is usable
                    // directly — no sentinel patching, no
                    // `std::fs::metadata` (which would also bypass the
                    // pluggable `Fs` backend).
                    match ParsedMeta::load_with_handle(
                        &*file,
                        &mid_handle,
                        Some(table_id),
                        encryption.as_deref(),
                    ) {
                        Ok(mid) => mid,
                        Err(mid_err) => {
                            log::warn!(
                                "MID meta block also unreadable for {}: {mid_err}; \
                                 returning original TAIL error",
                                file_path.display(),
                            );
                            return Err(tail_err);
                        }
                    }
                } else {
                    return Err(tail_err);
                }
            }
        };

        // Fail-fast: if this table was written with dictionary compression,
        // verify the caller provided the matching dictionary. Without this
        // check, reopening with the wrong dictionary (or None) would only
        // surface as a decompression error on the first data-block read.
        #[cfg(zstd_any)]
        if let CompressionType::ZstdDict { dict_id, .. } = metadata.data_block_compression {
            let got = zstd_dictionary.as_ref().map(|d| d.id());
            if got != Some(dict_id) {
                return Err(crate::Error::ZstdDictMismatch {
                    expected: dict_id,
                    got,
                });
            }
        }

        let file_handle: Arc<dyn FsFile> = Arc::from(file);

        let file_accessor = if let Some(dt) = descriptor_table {
            FileAccessor::DescriptorTable {
                table: dt,
                fs: fs.clone(),
            }
        } else {
            FileAccessor::File(file_handle.clone())
        };

        let block_index = if regions.index.is_some() {
            log::trace!(
                "Creating partitioned block index, with tli_ptr={:?}",
                regions.tli,
            );

            let block = Self::read_tli(
                &regions,
                file_handle.as_ref(),
                metadata.id,
                metadata.index_block_compression,
                encryption.as_deref(),
                metadata.ecc_params,
            )?;

            BlockIndexImpl::TwoLevel(TwoLevelBlockIndex {
                top_level_index: block,
                cache: cache.clone(),
                compression: metadata.index_block_compression,
                path: Arc::clone(&file_path),
                file_accessor: file_accessor.clone(),
                table_id: (tree_id, metadata.id).into(),
                encryption: encryption.clone(),
                ecc: metadata.ecc_params,
                comparator: comparator.clone(),

                #[cfg(feature = "metrics")]
                metrics: metrics.clone(),
            })
        } else if pin_index {
            log::trace!(
                "Creating pinned, full block index, with tli_ptr={:?}",
                regions.tli,
            );

            let block = Self::read_tli(
                &regions,
                file_handle.as_ref(),
                metadata.id,
                metadata.index_block_compression,
                encryption.as_deref(),
                metadata.ecc_params,
            )?;
            BlockIndexImpl::Full(FullBlockIndex::new(block, comparator.clone())?)
        } else {
            log::trace!("Creating volatile, full block index");

            BlockIndexImpl::VolatileFull(VolatileBlockIndex {
                cache: cache.clone(),
                compression: metadata.index_block_compression,
                file_accessor: file_accessor.clone(),
                handle: regions.tli,
                path: Arc::clone(&file_path),
                table_id: (tree_id, metadata.id).into(),
                encryption: encryption.clone(),
                ecc: metadata.ecc_params,
                comparator: comparator.clone(),

                #[cfg(feature = "metrics")]
                metrics: metrics.clone(),
            })
        };

        let pinned_filter_index = if let Some(filter_tli_handle) = regions.filter_tli {
            let block = Block::from_file(
                file_handle.as_ref(),
                filter_tli_handle,
                crate::table::block::BlockIdentity {
                    table_id: metadata.id,
                    block_type: BlockType::Index,
                    dict_id: 0,
                    window_log: 0,
                },
                &{
                    // Filter TLI is an Index (SST) block: no block_flags byte,
                    // so ECC presence comes from the per-SST descriptor.
                    let t = crate::table::block::BlockTransform::from_parts(
                        metadata.index_block_compression,
                        encryption.as_deref(),
                        #[cfg(zstd_any)]
                        None,
                    )?;
                    if let Some(ecc) = metadata.ecc_params {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;
            if block.header.block_type != BlockType::Index {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    block.header.block_type.into(),
                )));
            }
            let idx = IndexBlock::new(block);
            // Validate filter index trailer eagerly (same as FullBlockIndex::new)
            // so later iter() calls cannot panic on malformed blocks.
            idx.try_iter(comparator.clone())?;
            Some(idx)
        } else {
            None
        };

        // TODO: FilterBlock newtype
        let pinned_filter_block = if pinned_filter_index.is_none() && pin_filter {
            regions
                .filter
                .map(|filter_handle| {
                    log::debug!(
                        "Loading and pinning filter block, with filter_ptr={filter_handle:?}"
                    );

                    let block = Block::from_file(
                        file_handle.as_ref(),
                        filter_handle,
                        crate::table::block::BlockIdentity {
                            table_id: metadata.id,
                            block_type: BlockType::Filter,
                            dict_id: 0,
                            window_log: 0,
                        },
                        // Filter blocks are never written compressed, so the
                        // transform is Plain or Encrypted depending on whether
                        // the table is keyed. Filter is an SST block (no
                        // block_flags byte), so ECC presence comes from the
                        // per-SST descriptor: upgrade to `*Ecc` when page_ecc.
                        &{
                            let t = match encryption.as_deref() {
                                Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                                None => crate::table::block::BlockTransform::PLAIN,
                            };
                            if let Some(ecc) = metadata.ecc_params {
                                t.with_ecc(ecc)
                            } else {
                                t
                            }
                        },
                    )
                    .and_then(|block| {
                        if block.header.block_type == BlockType::Filter {
                            Ok(block)
                        } else {
                            Err(crate::Error::InvalidTag((
                                "BlockType",
                                block.header.block_type.into(),
                            )))
                        }
                    })?;

                    Ok::<_, crate::Error>(FilterBlock::new(block))
                })
                .transpose()?
        } else {
            None
        };

        // Load range tombstones (if present)
        let range_tombstones = if let Some(rt_handle) = regions.range_tombstones {
            log::trace!("Loading range tombstone block, with rt_ptr={rt_handle:?}");
            let block = Block::from_file(
                file_handle.as_ref(),
                rt_handle,
                crate::table::block::BlockIdentity {
                    table_id: metadata.id,
                    block_type: BlockType::RangeTombstone,
                    dict_id: 0,
                    window_log: 0,
                },
                // Range-tombstone blocks are always uncompressed; the
                // transform is Plain or Encrypted depending on whether the
                // table is keyed. RangeTombstone is an SST block (no
                // block_flags byte), so ECC presence comes from the per-SST
                // descriptor: upgrade to `*Ecc` when page_ecc.
                &{
                    let t = match encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = metadata.ecc_params {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;

            if block.header.block_type != BlockType::RangeTombstone {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    block.header.block_type.into(),
                )));
            }

            let mut rts = Self::decode_range_tombstones(&block, comparator.as_ref())?;
            // Sort range tombstones by (start asc, seqno desc) using the
            // user comparator so the order matches the tree's key ordering.
            // The seqno-desc tiebreaker ensures higher-seqno RTs are checked
            // first when multiple share the same start key.
            let cmp = &comparator;
            rts.sort_unstable_by(|a, b| {
                cmp.compare(&a.start, &b.start)
                    .then_with(|| b.seqno.cmp(&a.seqno))
            });
            rts
        } else {
            Vec::new()
        };

        // Load the optional inner-block layout section (present only when the
        // table has data blocks that split into >= 2 inner zstd blocks). Mirrors
        // the range-tombstone loader: same Plain/Encrypted (+ optional ECC)
        // transform the writer used for this uncompressed meta section.
        let block_layout = if let Some(bl_handle) = regions.block_layout {
            let block = Block::from_file(
                file_handle.as_ref(),
                bl_handle,
                crate::table::block::BlockIdentity {
                    table_id: metadata.id,
                    block_type: BlockType::BlockLayout,
                    dict_id: 0,
                    window_log: 0,
                },
                &{
                    let t = match encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = metadata.ecc_params {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;

            if block.header.block_type != BlockType::BlockLayout {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    block.header.block_type.into(),
                )));
            }

            let map = crate::table::block_layout::BlockLayoutMap::decode(&block.data)?;
            log::trace!(
                "Loaded block-layout index with {} multi-inner-block entries",
                map.len(),
            );
            map
        } else {
            crate::table::block_layout::BlockLayoutMap::default()
        };

        // Load the optional seqno-bounds section (parallel to the index; powers
        // the scan_since_seqno block-skip). Absent unless seqno_in_index was on.
        //
        // Best-effort, like the zone map below: the seqno-bounds section is
        // derived, non-authoritative metadata, so a corrupt / unreadable section
        // disables the block-skip (falling back to a full per-entry filter)
        // rather than failing the whole table open.
        let seqno_bounds = if let Some(sb_handle) = regions.seqno_bounds {
            let load = || -> crate::Result<crate::table::seqno_bounds::SeqnoBoundsMap> {
                let block = Block::from_file(
                    file_handle.as_ref(),
                    sb_handle,
                    crate::table::block::BlockIdentity {
                        table_id: metadata.id,
                        block_type: BlockType::SeqnoBounds,
                        dict_id: 0,
                        window_log: 0,
                    },
                    &{
                        let t = match encryption.as_deref() {
                            Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                            None => crate::table::block::BlockTransform::PLAIN,
                        };
                        if let Some(ecc) = metadata.ecc_params {
                            t.with_ecc(ecc)
                        } else {
                            t
                        }
                    },
                )?;
                if block.header.block_type != BlockType::SeqnoBounds {
                    return Err(crate::Error::InvalidTag((
                        "BlockType",
                        block.header.block_type.into(),
                    )));
                }
                crate::table::seqno_bounds::SeqnoBoundsMap::decode(&block.data)
            };
            load().unwrap_or_else(|e| {
                log::warn!(
                    "seqno-bounds section for table {:?} is unreadable ({e}); disabling seqno block-skip",
                    metadata.id
                );
                crate::table::seqno_bounds::SeqnoBoundsMap::default()
            })
        } else {
            crate::table::seqno_bounds::SeqnoBoundsMap::default()
        };
        if !seqno_bounds.is_empty() {
            log::trace!("Loaded {} seqno-bounds entries", seqno_bounds.len());
        }

        // Load the optional zone-map section (parallel to the index; powers the
        // predicate-based block-skip). Absent unless the zone-map policy was on.
        //
        // Best-effort: the zone map is DERIVED, non-authoritative metadata. A
        // corrupt or unreadable section disables block-skip for this table (an
        // empty map) rather than failing the whole `Table::recover` — turning an
        // optimization's bit-rot into a hard availability loss would be wrong.
        let zone_map = if let Some(zm_handle) = regions.zone_map {
            let load = || -> crate::Result<crate::table::zone_map::ZoneMap> {
                let block = Block::from_file(
                    file_handle.as_ref(),
                    zm_handle,
                    crate::table::block::BlockIdentity {
                        table_id: metadata.id,
                        block_type: BlockType::ZoneMap,
                        dict_id: 0,
                        window_log: 0,
                    },
                    &{
                        let t = match encryption.as_deref() {
                            Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                            None => crate::table::block::BlockTransform::PLAIN,
                        };
                        if let Some(ecc) = metadata.ecc_params {
                            t.with_ecc(ecc)
                        } else {
                            t
                        }
                    },
                )?;
                if block.header.block_type != BlockType::ZoneMap {
                    return Err(crate::Error::InvalidTag((
                        "BlockType",
                        block.header.block_type.into(),
                    )));
                }
                crate::table::zone_map::ZoneMap::decode(&block.data)
            };
            load().unwrap_or_else(|e| {
                log::warn!(
                    "zone-map section for table {:?} is unreadable ({e}); disabling block-skip",
                    metadata.id
                );
                crate::table::zone_map::ZoneMap::default()
            })
        } else {
            crate::table::zone_map::ZoneMap::default()
        };

        // Load the optional retrieval-ribbon locator section and pair it with an
        // ordinal → data-block-handle map (the index yields handles in key/write
        // order, which is the writer's block_id ordering). Only when the section
        // exists, so non-locator tables pay nothing.
        // Load the optional retrieval-ribbon locator as a BEST-EFFORT point-read
        // accelerator: any failure (corrupt locator section, unexpected block
        // type, or a corrupt sub-index block hit while walking the index to pair
        // locators with their data-block handles) degrades to `None` rather than
        // failing the table open. Point reads then use the sorted-index path,
        // which isolates a corrupt sub-index partition to its own keys — so
        // enabling the locator by default does NOT widen the blast radius of a
        // partitioned-index corruption from "one partition" back to "whole SST".
        let locator_index = regions.locator.and_then(|loc_handle| {
            let block = Block::from_file(
                file_handle.as_ref(),
                loc_handle,
                crate::table::block::BlockIdentity {
                    table_id: metadata.id,
                    block_type: BlockType::Locator,
                    dict_id: 0,
                    window_log: 0,
                },
                &{
                    let t = match encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = metadata.ecc_params {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )
            .inspect_err(|e| {
                log::warn!("retrieval-ribbon locator disabled: section load failed: {e:?}");
            })
            .ok()?;
            if block.header.block_type != BlockType::Locator {
                log::warn!(
                    "retrieval-ribbon locator disabled: unexpected block type {:?}",
                    block.header.block_type
                );
                return None;
            }
            let blocks: Vec<BlockHandle> = block_index
                .iter()
                .map(|r| r.map(|kbh| *kbh.as_ref()))
                .collect::<crate::Result<Vec<_>>>()
                .inspect_err(|e| {
                    log::warn!("retrieval-ribbon locator disabled: index walk failed: {e:?}");
                })
                .ok()?;
            log::trace!(
                "Loaded retrieval-ribbon locator over {} blocks",
                blocks.len()
            );
            Some(crate::table::locator::LoadedLocator::new(
                block.data, blocks,
            ))
        });

        log::debug!(
            "Recovered table #{} from {}",
            metadata.id,
            file_path.display(),
        );

        Ok(Self(
            Arc::new(Inner {
                path: file_path,
                tree_id,

                metadata,
                regions,

                cache,

                file_accessor,
                fs,

                block_index: Arc::new(block_index),

                pinned_filter_index,

                pinned_filter_block,

                is_deleted: AtomicBool::default(),
                punch_on_drop: AtomicU64::new(u64::MAX),

                checksum,
                global_seqno,

                comparator,

                #[cfg(feature = "metrics")]
                metrics,

                cached_blob_bytes: AtomicU64::new(u64::MAX),
                read_count: AtomicU64::new(0),
                last_access_secs: AtomicU64::new(0),
                range_tombstones,
                block_layout,
                seqno_bounds,
                zone_map,
                locator_index,
                encryption,

                #[cfg(zstd_any)]
                zstd_dictionary,

                deletion_pause: once_cell::race::OnceBox::new(),

                #[cfg(feature = "std")]
                background_deleter: once_cell::race::OnceBox::new(),

                heal_hints: once_cell::race::OnceBox::new(),
            }),
            None,
        ))
    }

    /// The tight-space restriction lower bound for this version's view of the
    /// table, or `None` on the common path. `Some(bound)` means the data below
    /// `bound` has been punched out and superseded by a merged output table, so
    /// reads route keys `< bound` elsewhere and clamp this table's scans to
    /// start at `bound` (its index still references the punched prefix).
    #[must_use]
    pub(crate) fn restrict_lower_bound(&self) -> Option<&UserKey> {
        self.1.as_ref()
    }

    /// True when `key` is below this version's tight-space restriction bound, so
    /// a point read must miss here and fall through to the output table that
    /// superseded the punched-out prefix. Every point-read entry point
    /// ([`get`](Self::get), [`get_value`](Self::get_value),
    /// [`get_with_block`](Self::get_with_block)) consults this first.
    #[inline]
    fn is_below_restriction(&self, key: &[u8]) -> bool {
        self.1
            .as_ref()
            .is_some_and(|bound| self.comparator.compare(key, bound) == core::cmp::Ordering::Less)
    }

    /// Returns a view of this table restricted to keys `>= lower`, for
    /// tight-space compaction. Shares the same `Arc<Inner>` (no file re-open,
    /// no extra handle, no [`Drop`] interaction), so the original and the
    /// restricted view are one physical SST seen by different versions. The
    /// caller punches the data blocks below `lower` only after this view is
    /// durably installed.
    #[must_use]
    pub(crate) fn with_restriction(&self, lower: UserKey) -> Self {
        Self(self.0.clone(), Some(lower))
    }

    /// Re-opens this table as a DISTINCT [`Inner`](inner::Inner) (its own file
    /// handle and fresh drop / punch-on-drop atomics) restricted to keys
    /// `>= lower`. Used by tight-space compaction so the PRIOR unrestricted view
    /// can drop — and punch its consumed prefix on that drop — independently of
    /// this restricted view, which keeps serving the suffix. Heavier than
    /// [`with_restriction`](Self::with_restriction) (re-reads the footer + block
    /// index), which is acceptable on the opt-in, emergency tight-space path.
    ///
    /// Opens with its own file handle (no shared descriptor table) so the old
    /// view's handle lifecycle stays fully separate.
    ///
    /// # Errors
    ///
    /// Propagates any error from re-opening the SST file.
    pub(crate) fn reopen_restricted(&self, lower: UserKey) -> crate::Result<Self> {
        let reopened = Self::recover(
            (*self.path).clone(),
            self.checksum,
            self.global_seqno,
            self.tree_id,
            self.metadata.id,
            self.cache.clone(),
            None,
            self.fs.clone(),
            self.pinned_filter_size() > 0,
            self.pinned_block_index_size() > 0,
            self.encryption.clone(),
            #[cfg(zstd_any)]
            self.zstd_dictionary.clone(),
            self.comparator.clone(),
            #[cfg(feature = "metrics")]
            self.metrics.clone(),
        )?;
        Ok(reopened.with_restriction(lower))
    }

    /// Marks this view to punch `[0, offset)` when its last `Arc` drops (see
    /// [`Inner::punch_on_drop`](inner::Inner::punch_on_drop)). Set on the PRIOR
    /// unrestricted view once a tight-space slice has been installed, so the
    /// consumed prefix is reclaimed exactly when no reader can still see it.
    pub(crate) fn mark_punch_on_drop(&self, offset: u64) {
        self.0
            .punch_on_drop
            .store(offset, core::sync::atomic::Ordering::Release);
    }

    /// Byte offset of the first data block whose last key reaches `key`. Punching
    /// `[0, offset)` reclaims every data block strictly below `key` while leaving
    /// the straddling block and the index / footer (which follow all data blocks)
    /// intact. When `key` is past the last block's keys, returns the end of the
    /// data region (every data block is punchable).
    ///
    /// # Errors
    ///
    /// Propagates a block-index read error.
    pub(crate) fn punch_offset_for(&self, key: &[u8]) -> crate::Result<u64> {
        let mut data_end = 0u64;
        for handle in self.block_index.iter() {
            let handle = handle?;
            if self.comparator.compare(handle.end_key(), key) != core::cmp::Ordering::Less {
                return Ok(handle.offset().0);
            }
            data_end = handle.offset().0 + u64::from(handle.size());
        }
        Ok(data_end)
    }

    /// Installs the tree-wide deletion pause used by checkpoints.
    ///
    /// Idempotent: a second call is a no-op. Called by the owning tree
    /// after recovery and after compaction registers freshly-built tables.
    pub(crate) fn install_deletion_pause(&self, pause: Arc<crate::deletion_pause::DeletionPause>) {
        let _ = self.0.deletion_pause.set(Box::new(pause));
    }

    /// Installs the tree-wide background file deleter.
    ///
    /// Idempotent: a second call is a no-op. Called by the owning tree after
    /// recovery and after compaction registers freshly-built tables, so an
    /// obsolete SST's `unlink` runs off the foreground path while its blocks
    /// are reclaimed synchronously at Drop.
    #[cfg(feature = "std")]
    pub(crate) fn install_background_deleter(&self, deleter: Arc<crate::BackgroundDeleter>) {
        let _ = self.0.background_deleter.set(Box::new(deleter));
    }

    /// Installs the tree-wide ECC heal-hint sink.
    ///
    /// Idempotent: a second call is a no-op. Called by the owning tree after
    /// recovery and after compaction registers freshly-built tables, so a
    /// confirmed-persistent ECC correction on a read can queue this SST for a
    /// healing recompaction.
    pub(crate) fn install_heal_hints(&self, hints: Arc<crate::heal_hints::HealHints>) {
        let _ = self.0.heal_hints.set(Box::new(hints));
    }

    #[must_use]
    pub fn checksum(&self) -> Checksum {
        self.0.checksum
    }

    /// Read `len` bytes from the cursor position with checked arithmetic.
    /// Uses `.get()` instead of direct indexing to satisfy `clippy::indexing_slicing`.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "block sizes are bounded well within usize on all supported platforms"
    )]
    fn read_checked_slice(
        cursor: &mut crate::io::Cursor<&[u8]>,
        field: &'static str,
        len: usize,
    ) -> crate::Result<Vec<u8>> {
        let offset = cursor.position();
        let data = cursor.get_ref();
        let pos = offset as usize;
        let end_pos = pos
            .checked_add(len)
            .ok_or(crate::Error::RangeTombstoneDecode { field, offset })?;
        let buf = data
            .get(pos..end_pos)
            .ok_or(crate::Error::RangeTombstoneDecode { field, offset })?
            .to_vec();
        cursor.set_position(end_pos as u64);
        Ok(buf)
    }

    /// Decodes range tombstones from a raw block.
    ///
    /// Wire format (repeated): `[start_len:u16_le][start][end_len:u16_le][end][seqno:u64_le]`
    ///
    /// # Errors
    ///
    /// Will return `Err` if the block data is malformed.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "block sizes are bounded well within usize on all supported platforms"
    )]
    fn decode_range_tombstones(
        block: &Block,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Vec<RangeTombstone>> {
        use crate::io::{Cursor, LE, ReadBytesExt};

        let mut tombstones = Vec::new();
        let data = block.data.as_ref();

        // A dedicated RT block with empty payload is corruption — the writer
        // only creates an RT block handle when at least one tombstone exists.
        if data.is_empty() {
            log::error!("Range tombstone block: missing start_len");
            return Err(crate::Error::RangeTombstoneDecode {
                field: "start_len",
                offset: 0,
            });
        }

        let mut cursor = Cursor::new(data);

        while (cursor.position() as usize) < data.len() {
            let entry_offset = cursor.position();
            let start_len_offset = entry_offset;
            let start_len =
                cursor
                    .read_u16::<LE>()
                    .map_err(|_| crate::Error::RangeTombstoneDecode {
                        field: "start_len",
                        offset: start_len_offset,
                    })? as usize;

            // Validate length against remaining data before allocating
            let remaining = data.len() - cursor.position() as usize;
            if start_len > remaining {
                log::error!(
                    "Range tombstone block: start_len {start_len} exceeds remaining {remaining}"
                );
                return Err(crate::Error::RangeTombstoneDecode {
                    field: "start_len",
                    offset: start_len_offset,
                });
            }

            // Extract validated slice from cursor position.
            // Using .get() instead of direct indexing to satisfy clippy::indexing_slicing.
            let start_buf = Self::read_checked_slice(&mut cursor, "start", start_len)?;

            let end_len_offset = cursor.position();
            let end_len =
                cursor
                    .read_u16::<LE>()
                    .map_err(|_| crate::Error::RangeTombstoneDecode {
                        field: "end_len",
                        offset: end_len_offset,
                    })? as usize;

            let remaining = data.len() - cursor.position() as usize;
            if end_len > remaining {
                log::error!(
                    "Range tombstone block: end_len {end_len} exceeds remaining {remaining}"
                );
                return Err(crate::Error::RangeTombstoneDecode {
                    field: "end_len",
                    offset: end_len_offset,
                });
            }

            let end_buf = Self::read_checked_slice(&mut cursor, "end", end_len)?;

            let seqno_offset = cursor.position();
            let seqno =
                cursor
                    .read_u64::<LE>()
                    .map_err(|_| crate::Error::RangeTombstoneDecode {
                        field: "seqno",
                        offset: seqno_offset,
                    })?;

            let start = UserKey::from(start_buf);
            let end = UserKey::from(end_buf);

            // Validate invariant: start < end using the tree's comparator
            // (reject corrupted or misordered intervals)
            if comparator.compare(&start, &end) != core::cmp::Ordering::Less {
                log::error!("Range tombstone block: invalid interval (start >= end)");
                return Err(crate::Error::RangeTombstoneDecode {
                    field: "interval",
                    offset: entry_offset,
                });
            }

            tombstones.push(RangeTombstone::new(start, end, seqno));
        }

        Ok(tombstones)
    }

    /// Returns the range tombstones stored in this table.
    #[must_use]
    pub(crate) fn range_tombstones(&self) -> &[RangeTombstone] {
        &self.0.range_tombstones
    }

    pub(crate) fn mark_as_deleted(&self) {
        self.0
            .is_deleted
            .store(true, core::sync::atomic::Ordering::Release);
    }

    /// Checks if a key range overlaps (partially or fully) with this table's key range.
    pub(crate) fn check_key_range_overlap_cmp(
        &self,
        bounds: &(Bound<&[u8]>, Bound<&[u8]>),
        cmp: &dyn crate::comparator::UserComparator,
    ) -> bool {
        if !self
            .metadata
            .key_range
            .overlaps_with_bounds_cmp(bounds, cmp)
        {
            return false;
        }

        // Tight-space restriction: the live range is `[bound, hi]`. If the
        // query's upper bound is strictly below `bound`, the query targets only
        // the punched-out prefix (now served by a superseding output table), so
        // this table does not overlap.
        if let Some(bound) = &self.1 {
            match bounds.1 {
                Bound::Included(end) => {
                    if cmp.compare(end, bound) == core::cmp::Ordering::Less {
                        return false;
                    }
                }
                Bound::Excluded(end) => {
                    // end <= bound: every key the query can reach is below the
                    // live range.
                    if cmp.compare(end, bound) != core::cmp::Ordering::Greater {
                        return false;
                    }
                }
                Bound::Unbounded => {}
            }
        }

        true
    }

    /// Checks the full-table bloom filter for a hash value.
    ///
    /// Returns `Ok(true)` if the hash may exist in the filter (or if no full
    /// filter is available), `Ok(false)` if the hash is definitely absent.
    ///
    /// Handles full (non-partitioned) filters directly. Partitioned / TLI
    /// filters are keyed by user key, not raw hash, so this method returns
    /// `Ok(true)` conservatively for those types.
    fn bloom_may_contain_hash(&self, hash: u64) -> crate::Result<bool> {
        // Full (non-partitioned) filter — single bloom covers the entire table
        if let Some(block) = &self.pinned_filter_block {
            return block.maybe_contains_hash(hash);
        }

        // Partitioned / TLI filters: partition index is keyed by user key, not
        // raw hash — we would need to scan ALL partitions to check,
        // which is O(partitions) I/O and defeats the purpose of bloom skip.
        // Returning Ok(true) is correct (conservative: segment is NOT skipped).
        if self.pinned_filter_index.is_some() || self.regions.filter_tli.is_some() {
            return Ok(true);
        }

        // Unpinned full filter — load from disk.
        // Safe: if we reach here, filter_tli is None (no partitioned filter),
        // so regions.filter is a single full-table bloom, not a concatenation.
        if let Some(filter_block_handle) = &self.regions.filter {
            let block = self.load_block(
                filter_block_handle,
                BlockType::Filter,
                CompressionType::None, // NOTE: Filter blocks are never compressed (crate invariant)
                #[cfg(zstd_any)]
                None,
            )?;
            let block = FilterBlock::new(block);
            return block.maybe_contains_hash(hash);
        }

        // No filter available — cannot rule out the hash
        Ok(true)
    }

    /// Checks the bloom filter for a prefix hash.
    ///
    /// Returns `Ok(true)` if the prefix may exist in this table (or if no
    /// filter is available), `Ok(false)` if the prefix is definitely absent.
    ///
    /// This is used by prefix scans to skip segments that contain no keys
    /// with a matching prefix. The prefix must have been indexed at write
    /// time via a [`PrefixExtractor`](crate::PrefixExtractor).
    pub(crate) fn maybe_contains_prefix(&self, prefix_hash: u64) -> crate::Result<bool> {
        self.bloom_may_contain_hash(prefix_hash)
    }

    /// Checks the bloom filter for a precomputed key hash.
    ///
    /// Returns `Ok(true)` if the key may exist in this table (or if no
    /// filter is available), `Ok(false)` if the key is definitely absent.
    ///
    /// Used by the point-read merge pipeline to pre-filter disk tables
    /// before building range iterators. For partitioned or TLI filter
    /// configurations, the underlying check returns `Ok(true)` conservatively,
    /// so pre-filtering is best-effort and configuration-dependent.
    pub(crate) fn bloom_may_contain_key_hash(&self, key_hash: u64) -> crate::Result<bool> {
        self.bloom_may_contain_hash(key_hash)
    }

    /// Checks the bloom filter for a key, with partition-aware seeking.
    ///
    /// Unlike [`bloom_may_contain_key_hash`](Self::bloom_may_contain_key_hash)
    /// which falls back to `Ok(true)` for partitioned filters, this method
    /// uses the user key to seek the partition index and check only the
    /// matching partition's bloom filter.
    ///
    /// `key_hash` must be the xxh3 hash of `key` (pre-computed by the caller
    /// to avoid redundant hashing — same pattern as [`Table::get`]).
    pub(crate) fn bloom_may_contain_key(&self, key: &[u8], key_hash: u64) -> crate::Result<bool> {
        debug_assert_eq!(
            crate::hash::hash64(key),
            key_hash,
            "bloom_may_contain_key: key_hash must be crate::hash::hash64(key)"
        );

        // Full (non-partitioned) filter — delegate to hash-only path.
        // A table has either pinned_filter_block (full) or pinned_filter_index
        // (partitioned), never both — checked at construction time.
        if self.pinned_filter_block.is_some() {
            return self.bloom_may_contain_hash(key_hash);
        }

        // Partitioned filter with pinned TLI — seek to the matching partition
        if let Some(filter_idx) = &self.pinned_filter_index {
            let mut iter = filter_idx.iter(self.comparator.clone());
            iter.seek(key, crate::seqno::MAX_SEQNO);

            if let Some(filter_block_handle) = iter.next() {
                let filter_block_handle = filter_block_handle.materialize(filter_idx.as_slice());

                let block = self.load_block(
                    &filter_block_handle.into_inner(),
                    BlockType::Filter,
                    CompressionType::None,
                    #[cfg(zstd_any)]
                    None,
                )?;
                let block = FilterBlock::new(block);
                return block.maybe_contains_hash(key_hash);
            }

            // iter.next() == None means the key is beyond all partition
            // boundaries (seek found no ceiling entry in the TLI, which is
            // ordered by each partition's last user key). The key cannot
            // exist in this table. Same logic as Table::get (line ~265).
            return Ok(false);
        }

        // Unpinned filter — fall through to hash-only path (handles both
        // unpinned full filters and the no-filter case)
        self.bloom_may_contain_hash(key_hash)
    }

    /// Returns the highest effective sequence number in the table.
    ///
    /// For tables produced by flush/compaction (`global_seqno == 0`), this
    /// returns the highest item seqno directly.
    ///
    /// For tables produced by bulk ingestion (`global_seqno > 0`), items
    /// are written with local seqno 0 and the table carries a global offset.
    /// The effective seqno of each item is `global_seqno + local_seqno`,
    /// which mirrors the translation in [`Table::get`].
    #[must_use]
    pub fn get_highest_seqno(&self) -> SeqNo {
        self.metadata.seqnos.1 + self.global_seqno()
    }

    /// Returns the highest sequence number from KV entries only,
    /// excluding range tombstone seqnos.
    ///
    /// This enables more aggressive table-skip: a covering RT stored
    /// in the same table can trigger skip because its seqno may exceed
    /// the KV-only max even though it doesn't exceed the overall max.
    ///
    /// For tables written before this field was introduced, falls back
    /// to `get_highest_seqno()` (conservative but correct).
    #[must_use]
    pub fn get_highest_kv_seqno(&self) -> SeqNo {
        self.metadata.highest_kv_seqno + self.global_seqno()
    }

    /// Returns the number of tombstone markers in the `Table`.
    #[must_use]
    #[doc(hidden)]
    pub fn tombstone_count(&self) -> u64 {
        self.metadata.tombstone_count
    }

    /// Returns the number of weak (single delete) tombstones in the `Table`.
    #[must_use]
    #[doc(hidden)]
    pub fn weak_tombstone_count(&self) -> u64 {
        self.metadata.weak_tombstone_count
    }

    /// Returns the number of value entries reclaimable once weak tombstones can be GC'd.
    #[must_use]
    #[doc(hidden)]
    pub fn weak_tombstone_reclaimable(&self) -> u64 {
        self.metadata.weak_tombstone_reclaimable
    }

    /// Returns the ratio of tombstone markers in the `Table`.
    #[must_use]
    #[doc(hidden)]
    pub fn tombstone_ratio(&self) -> f32 {
        todo!()

        //  self.metadata.tombstone_count as f32 / self.metadata.key_count as f32
    }
}
