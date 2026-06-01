// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

pub mod block;
pub(crate) mod block_index;
pub mod data_block;
pub mod filter;
mod id;
mod index_block;
mod inner;
pub(crate) mod iter;
pub(crate) mod meta;
pub(crate) mod multi_writer;
pub(crate) mod regions;
mod scanner;
pub mod util;
pub mod writer;

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
use block_index::BlockIndexImpl;
use inner::Inner;
use iter::Iter;
use std::{
    borrow::Cow,
    ops::{Bound, RangeBounds},
    path::PathBuf,
    sync::Arc,
};
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
pub struct Table(Arc<Inner>);

impl std::ops::Deref for Table {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        if let Some(v) = self.0.cached_blob_bytes.get() {
            return Ok(*v);
        }

        let sum = self
            .list_blob_file_references()?
            .map(|bf| bf.iter().map(|f| f.on_disk_bytes).sum::<u64>())
            .unwrap_or_default();

        let _ = self.0.cached_blob_bytes.set(sum);
        Ok(sum)
    }

    pub fn list_blob_file_references(&self) -> crate::Result<Option<Vec<LinkedFile>>> {
        use byteorder::{LE, ReadBytesExt};

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
            self.metadata.page_ecc,
            #[cfg(zstd_any)]
            zstd_dict,
            #[cfg(feature = "metrics")]
            &self.metrics,
        )
    }

    fn load_data_block(&self, handle: &BlockHandle) -> crate::Result<DataBlock> {
        // `from_loaded` transparently strips the per-KV checksum footer when
        // the block's KV_CHECKSUM_FOOTER header flag is set, so the rest of
        // the read path is unchanged regardless of the kv-checksum policy
        // the writer used.
        self.load_block(
            handle,
            BlockType::Data,
            self.metadata.data_block_compression,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )
        .and_then(DataBlock::from_loaded)
    }

    /// Returns the (possibly compressed) file size.
    pub(crate) fn file_size(&self) -> u64 {
        self.metadata.file_size
    }

    /// Scrub: verifies the per-KV checksum footer of every footer-bearing
    /// data block in this table (those with the `KV_CHECKSUM_FOOTER` header
    /// flag set), decoding each block and recomputing each entry's
    /// logical-content digest.
    ///
    /// Data blocks without the footer flag are skipped. This is the
    /// paranoid / offline integrity path — the live read path does NOT
    /// verify per-entry digests (the block-level checksum already covers
    /// the on-disk bytes). Stops and returns on the first detected mismatch.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::ChecksumMismatch`] if any entry's recomputed digest
    ///   disagrees with the stored value (corruption of the entry bytes or
    ///   the stored digest).
    /// - Any I/O / decode error encountered while loading a block.
    pub(crate) fn verify_kv_checksums(&self) -> crate::Result<()> {
        use crate::table::block::header::block_flags::KV_CHECKSUM_FOOTER;

        // Homogeneous SST: the per-SST descriptor records whether ANY data
        // block carries a per-KV footer. When it says none do, the scan can
        // be skipped — but the scrub is the paranoid integrity path, so it
        // does not skip on the strength of a single descriptor byte alone.
        // That byte rides the meta block checksum, yet could have flipped in
        // RAM before that checksum was taken. Confirm the first data block's
        // footer flag agrees (all-or-none, so block 0 is representative)
        // before trusting "no footers". If it disagrees, fall through to the
        // full scan rather than skip.
        if self.metadata.kv_checksum_algo.is_none() {
            let mut iter = self.block_index.iter();
            match iter.next() {
                // Empty table: nothing to scrub.
                None => return Ok(()),
                Some(handle) => {
                    let handle = handle?;
                    let block = self.load_block(
                        &BlockHandle::new(handle.offset(), handle.size()),
                        BlockType::Data,
                        self.metadata.data_block_compression,
                        #[cfg(zstd_any)]
                        self.zstd_dictionary.as_deref(),
                    )?;
                    if block.header.block_flags & KV_CHECKSUM_FOOTER == 0 {
                        // Descriptor confirmed by the first block: no footers.
                        return Ok(());
                    }
                    // Descriptor claimed "none" but a footer is present —
                    // do not trust it; fall through to verify every block.
                }
            }
        }

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
            if block.header.block_flags & KV_CHECKSUM_FOOTER != 0 {
                DataBlock::verify_kv_checked(&block.data, block.header, self.comparator.clone())?;
            } else if self.metadata.kv_checksum_algo.is_some() {
                // The descriptor declares this SST footer-bearing, and an SST
                // is homogeneous, so every data block must carry the footer.
                // A block without it is structural corruption — fail the
                // scrub instead of silently returning Ok for a malformed
                // table.
                return Err(crate::Error::InvalidTrailer);
            }
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
                    use std::sync::atomic::Ordering::Relaxed;
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
                use std::sync::atomic::Ordering::Relaxed;
                self.metrics.filter_queries.fetch_add(1, Relaxed);
                self.metrics.io_skipped_by_filter.fetch_add(1, Relaxed);
            }
            return Ok(BloomResult::Skip);
        }

        Ok(BloomResult::Proceed { has_filter })
    }

    pub fn get(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<InternalValue>> {
        let global_seqno = self.global_seqno();
        let seqno = seqno.saturating_sub(global_seqno);

        if self.metadata.seqnos.0 >= seqno {
            return Ok(None);
        }

        let bloom = self.check_bloom(key, key_hash)?;
        if bloom.should_skip() {
            return Ok(None);
        }

        let item = self.point_read(key, seqno)?;

        // Translate table-local seqno back to global coordinate so callers
        // can compare across tables/memtables (L0 best-selection, RT suppression).
        let item = item.map(|mut iv| {
            iv.key.seqno = iv.key.seqno.saturating_add(global_seqno);
            iv
        });

        #[cfg(feature = "metrics")]
        {
            use std::sync::atomic::Ordering::Relaxed;
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
        let global_seqno = self.global_seqno();
        let seqno = seqno.saturating_sub(global_seqno);

        if self.metadata.seqnos.0 >= seqno {
            return Ok(None);
        }

        let bloom = self.check_bloom(key, key_hash)?;
        if bloom.should_skip() {
            return Ok(None);
        }

        let result = self.point_read_with_block(key, seqno)?;

        // Translate table-local seqno back to global coordinate (see Table::get).
        let result = result.map(|(mut iv, block)| {
            iv.key.seqno = iv.key.seqno.saturating_add(global_seqno);
            (iv, block)
        });

        #[cfg(feature = "metrics")]
        {
            use std::sync::atomic::Ordering::Relaxed;
            if result.is_none() && bloom.has_filter() {
                self.metrics.filter_queries.fetch_add(1, Relaxed);
            }
        }

        Ok(result)
    }

    /// Shared block-index walk for point reads. Returns the matching entry
    /// together with the [`DataBlock`] it was found in, so callers that need
    /// the block (e.g. for [`PinnableSlice`]) can keep it alive.
    fn point_read_inner(
        &self,
        key: &[u8],
        seqno: SeqNo,
    ) -> crate::Result<Option<(InternalValue, DataBlock)>> {
        let Some(iter) = self.block_index.forward_reader(key, seqno) else {
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

    fn point_read(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>> {
        self.point_read_inner(key, seqno)
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
    ) -> crate::Result<Option<(InternalValue, Block)>> {
        self.point_read_inner(key, seqno)
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
            &self.path,
            block_count,
            self.metadata.data_block_compression,
            self.global_seqno(),
            self.encryption.clone(),
            #[cfg(zstd_any)]
            self.zstd_dictionary.clone(),
            self.comparator.clone(),
            self.metadata.id,
        )
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
            self.metadata.page_ecc,
            #[cfg(zstd_any)]
            self.zstd_dictionary.clone(),
            self.comparator.clone(),
            #[cfg(feature = "metrics")]
            self.metrics.clone(),
        );

        match range.start_bound() {
            Bound::Included(key) => iter.set_lower_bound(iter::Bound::Included(key.clone())),
            Bound::Excluded(key) => iter.set_lower_bound(iter::Bound::Excluded(key.clone())),
            Bound::Unbounded => {}
        }

        match range.end_bound() {
            Bound::Included(key) => iter.set_upper_bound(iter::Bound::Included(key.clone())),
            Bound::Excluded(key) => iter.set_upper_bound(iter::Bound::Excluded(key.clone())),
            Bound::Unbounded => {}
        }

        iter
    }

    fn read_tli(
        regions: &ParsedRegions,
        file: &dyn FsFile,
        table_id: TableId,
        compression: CompressionType,
        encryption: Option<&dyn crate::encryption::EncryptionProvider>,
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
            match Self::read_tli_at(file, tail_handle, table_id, compression, encryption) {
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
        Self::read_tli_at(file, regions.tli, table_id, compression, encryption)
    }

    fn read_tli_at(
        file: &dyn FsFile,
        handle: BlockHandle,
        table_id: TableId,
        compression: CompressionType,
        encryption: Option<&dyn crate::encryption::EncryptionProvider>,
    ) -> crate::Result<IndexBlock> {
        let block = Block::from_file(
            file,
            handle,
            crate::table::block::BlockIdentity {
                tree_id: 0,
                table_id,
                // Match the writer: both `tli` and `tli_tail` are
                // emitted with `block_offset: 0` (the partitioned /
                // full index writers do not currently thread their
                // SFA section offset through `BlockIndexWriter`).
                // BlockIdentity is ignored by `Block::from_file`
                // today, but once #251 wires it into AEAD AAD,
                // reader and writer MUST encode the same value or
                // encrypted tables fail to reopen. Threading real
                // section offsets through is tracked alongside the
                // BlockIndexWriter::finish surface in #251.
                block_offset: 0,
                block_type: BlockType::Index,
                dict_id: 0,
                window_log: 0,
            },
            &crate::table::block::BlockTransform::from_parts(
                compression,
                encryption,
                #[cfg(zstd_any)]
                None,
            )?,
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
        use meta::ParsedMeta;
        use regions::ParsedRegions;
        use std::sync::atomic::AtomicBool;

        log::debug!("Recovering table from file {}", file_path.display());
        let mut file = fs.open(&file_path, &FsOpenOptions::new().read(true))?;
        let file_path = Arc::new(file_path);

        #[cfg(feature = "metrics")]
        metrics
            .table_file_opened_uncached
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

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
                    match ParsedMeta::load_with_handle(&*file, &mid_handle, encryption.as_deref()) {
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
            )?;

            BlockIndexImpl::TwoLevel(TwoLevelBlockIndex {
                top_level_index: block,
                cache: cache.clone(),
                compression: metadata.index_block_compression,
                path: Arc::clone(&file_path),
                file_accessor: file_accessor.clone(),
                table_id: (tree_id, metadata.id).into(),
                encryption: encryption.clone(),
                page_ecc: metadata.page_ecc,
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
                page_ecc: metadata.page_ecc,
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
                    tree_id: 0,
                    table_id: metadata.id,
                    block_offset: *filter_tli_handle.offset(),
                    block_type: BlockType::Index,
                    dict_id: 0,
                    window_log: 0,
                },
                &crate::table::block::BlockTransform::from_parts(
                    metadata.index_block_compression,
                    encryption.as_deref(),
                    #[cfg(zstd_any)]
                    None,
                )?,
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
                            tree_id: 0,
                            table_id: metadata.id,
                            block_offset: *filter_handle.offset(),
                            block_type: BlockType::Filter,
                            dict_id: 0,
                            window_log: 0,
                        },
                        // Filter blocks are never written compressed,
                        // so the transform is Plain or Encrypted
                        // depending on whether the table is keyed.
                        &match encryption.as_deref() {
                            Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                            None => crate::table::block::BlockTransform::PLAIN,
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
                    tree_id: 0,
                    table_id: metadata.id,
                    block_offset: *rt_handle.offset(),
                    block_type: BlockType::RangeTombstone,
                    dict_id: 0,
                    window_log: 0,
                },
                // Range-tombstone blocks are always uncompressed; the
                // transform is Plain or Encrypted depending on whether
                // the table is keyed.
                &match encryption.as_deref() {
                    Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                    None => crate::table::block::BlockTransform::PLAIN,
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

        log::debug!(
            "Recovered table #{} from {}",
            metadata.id,
            file_path.display(),
        );

        Ok(Self(Arc::new(Inner {
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

            checksum,
            global_seqno,

            comparator,

            #[cfg(feature = "metrics")]
            metrics,

            cached_blob_bytes: std::sync::OnceLock::new(),
            range_tombstones,
            encryption,

            #[cfg(zstd_any)]
            zstd_dictionary,

            deletion_pause: once_cell::race::OnceBox::new(),
        })))
    }

    /// Installs the tree-wide deletion pause used by checkpoints.
    ///
    /// Idempotent: a second call is a no-op. Called by the owning tree
    /// after recovery and after compaction registers freshly-built tables.
    pub(crate) fn install_deletion_pause(&self, pause: Arc<crate::deletion_pause::DeletionPause>) {
        let _ = self.0.deletion_pause.set(Box::new(pause));
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
        cursor: &mut std::io::Cursor<&[u8]>,
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
        use byteorder::{LE, ReadBytesExt};
        use std::io::Cursor;

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
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Checks if a key range overlaps (partially or fully) with this table's key range.
    pub(crate) fn check_key_range_overlap_cmp(
        &self,
        bounds: &(Bound<&[u8]>, Bound<&[u8]>),
        cmp: &dyn crate::comparator::UserComparator,
    ) -> bool {
        self.metadata
            .key_range
            .overlaps_with_bounds_cmp(bounds, cmp)
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
