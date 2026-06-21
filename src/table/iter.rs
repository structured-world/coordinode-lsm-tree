// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(feature = "zstd")]
use super::KeyedBlockHandle;
use super::{BlockOffset, DataBlock, GlobalTableId, data_block::Iter as DataBlockIter};
use crate::{
    Cache, CompressionType, InternalValue, SeqNo, UserKey,
    comparator::SharedComparator,
    encryption::EncryptionProvider,
    file_accessor::FileAccessor,
    table::{
        BlockHandle,
        block::ParsedItem,
        block_index::{BlockIndexIter, BlockIndexIterImpl},
        util::load_block,
    },
};
use alloc::sync::Arc;

use crate::path::PathBuf;
use self_cell::self_cell;

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

type InnerIter<'a> = DataBlockIter<'a>;

/// Whether the cold-block partial-tier read path is engaged (opt-in, OFF by
/// default). Set `LSM_PARTIAL_DECODE` to `1` / `on` / `true` / `yes` to enable.
///
/// On a large cold zstd block, a bounded range decodes only the inner blocks up
/// to its upper bound and keeps just that decompressed prefix resident; growing
/// the extent RESUMES from a cached entropy/window snapshot (decoding only the
/// new tail blocks) rather than re-decompressing from block 0. Repeatedly read
/// or mostly-decoded blocks promote to a full resident block. Off by default
/// while the path is being validated against full decode + block cache; flip the
/// env to compare.
// no-std: feature-gate behind `std`; a no-std build has no env, default OFF.
#[cfg(feature = "zstd")]
fn partial_decode_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("LSM_PARTIAL_DECODE")
            .ok()
            .is_some_and(|v| matches!(v.trim(), "1" | "on" | "true" | "yes"))
    })
}

/// Minimum decompressed block size for the partial tier to engage. Below this a
/// block decodes fully: the decode it would save is cheap relative to the
/// per-call setup (decoder reset, window prime, trailer synthesis), so partial
/// would cost more than it saves. Only genuinely large cold blocks qualify.
#[cfg(feature = "zstd")]
const PARTIAL_MIN_BLOCK_BYTES: usize = 256 * 1024;

/// Promote a partially-decoded cold block to a full resident block once it has
/// been served this many times: a repeatedly-read block earns its full memory.
#[cfg(feature = "zstd")]
const PARTIAL_PROMOTE_HITS: u32 = 4;

/// Promote once covering a query would decode at least this percentage of the
/// block's inner zstd blocks: past this point the partial saves little memory
/// and a single full decode beats re-growing the extent.
#[cfg(feature = "zstd")]
const PARTIAL_PROMOTE_FRACTION_PCT: u32 = 75;

/// Whether `covered` of `total` inner blocks reaches the promotion fraction.
/// `total == 0` never promotes (no layout → partial tier does not apply).
#[cfg(feature = "zstd")]
fn promote_by_fraction(covered: u32, total: u32) -> bool {
    total > 0
        && u64::from(covered) * 100 >= u64::from(total) * u64::from(PARTIAL_PROMOTE_FRACTION_PCT)
}

pub enum Bound {
    Included(UserKey),
    Excluded(UserKey),
}
type Bounds = (Option<Bound>, Option<Bound>);

self_cell!(
    pub struct OwnedDataBlockIter {
        owner: DataBlock,

        #[covariant]
        dependent: InnerIter,
    }
);

impl OwnedDataBlockIter {
    fn seek_lower_inclusive(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek(needle, seqno))
    }

    fn seek_upper_inclusive(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_upper(needle, seqno))
    }

    fn seek_lower_exclusive(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_exclusive(needle, seqno))
    }

    fn seek_upper_exclusive(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_upper_exclusive(needle, seqno))
    }

    pub fn seek_lower_bound(&mut self, bound: &Bound, seqno: SeqNo) -> bool {
        match bound {
            Bound::Included(key) => self.seek_lower_inclusive(key, seqno),
            Bound::Excluded(key) => self.seek_lower_exclusive(key, seqno),
        }
    }

    pub fn seek_upper_bound(&mut self, bound: &Bound, seqno: SeqNo) -> bool {
        match bound {
            Bound::Included(key) => self.seek_upper_inclusive(key, seqno),
            Bound::Excluded(key) => self.seek_upper_exclusive(key, seqno),
        }
    }
}

impl Iterator for OwnedDataBlockIter {
    type Item = InternalValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next().map(|item| item.materialize(&block.inner.data))
        })
    }
}

impl DoubleEndedIterator for OwnedDataBlockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next_back()
                .map(|item| item.materialize(&block.inner.data))
        })
    }
}

fn create_data_block_reader(
    block: DataBlock,
    comparator: SharedComparator,
) -> crate::Result<OwnedDataBlockIter> {
    OwnedDataBlockIter::try_new(block, |b| b.try_iter(comparator))
}

#[expect(
    clippy::struct_excessive_bools,
    reason = "each bool is an independent per-SST read-path flag (kv footer, columnar, init, poisoned), not a state enum"
)]
pub struct Iter {
    table_id: GlobalTableId,
    path: Arc<PathBuf>,

    global_seqno: SeqNo,

    #[expect(clippy::struct_field_names)]
    index_iter: BlockIndexIterImpl,

    file_accessor: FileAccessor,
    cache: Arc<Cache>,
    compression: CompressionType,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    /// Per-SST Page-ECC scheme from table metadata; the block reader needs
    /// it to size + recover the parity trailer. `None` = no parity.
    ecc: Option<crate::table::block::EccParams>,
    /// Tree-wide ECC heal sink. A scan that recovers a data block from parity
    /// records this SST here (on confirmed persistence) for a healing
    /// recompaction. `None` before the table is tree-owned.
    heal_hints: Option<Arc<crate::heal_hints::HealHints>>,
    /// Per-SST per-KV-footer flag from table metadata
    /// (`kv_checksum_algo.is_some()`); data blocks omit the `block_flags` byte,
    /// so `from_loaded` is told here whether to strip a footer.
    has_kv_footer: bool,
    #[cfg(zstd_any)]
    zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
    comparator: SharedComparator,

    /// Per-table inner-block layout (clone). Drives the partial-decode path:
    /// when a data block has a recorded layout and the query upper bound falls
    /// within it, only the inner zstd blocks covering `[start, upper]` are
    /// decoded (trailing blocks skipped).
    #[cfg(feature = "zstd")]
    block_layout: crate::table::block_layout::BlockLayoutMap,
    /// Data-block restart interval, to rebuild a positional index when
    /// synthesizing a partial block's trailer.
    #[cfg(feature = "zstd")]
    data_block_restart_interval: u8,

    /// Whether this SST's data blocks are columnar (PAX); when set, each block is
    /// read as `BlockType::Columnar` and reconstructed into a row block.
    columnar: bool,

    index_initialized: bool,

    lo_offset: BlockOffset,
    lo_data_block: Option<OwnedDataBlockIter>,

    hi_offset: BlockOffset,
    hi_data_block: Option<OwnedDataBlockIter>,

    range: Bounds,

    /// Set on unrecoverable block-init error so subsequent `next()` /
    /// `next_back()` calls return `None` instead of skipping past the
    /// corrupt block.
    poisoned: bool,

    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
}

impl Iter {
    #[expect(
        clippy::too_many_arguments,
        reason = "encryption, comparator and metrics add extra parameters to the constructor"
    )]
    pub fn new(
        table_id: GlobalTableId,
        global_seqno: SeqNo,
        path: Arc<PathBuf>,
        index_iter: BlockIndexIterImpl,
        file_accessor: FileAccessor,
        cache: Arc<Cache>,
        compression: CompressionType,
        encryption: Option<Arc<dyn EncryptionProvider>>,
        ecc: Option<crate::table::block::EccParams>,
        heal_hints: Option<Arc<crate::heal_hints::HealHints>>,
        has_kv_footer: bool,
        columnar: bool,
        #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
        comparator: SharedComparator,
        #[cfg(feature = "zstd")] block_layout: crate::table::block_layout::BlockLayoutMap,
        #[cfg(feature = "zstd")] data_block_restart_interval: u8,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            table_id,
            path,

            global_seqno,

            index_iter,
            file_accessor,
            cache,
            compression,
            encryption,
            ecc,
            heal_hints,
            has_kv_footer,
            columnar,
            #[cfg(zstd_any)]
            zstd_dictionary,
            comparator,

            #[cfg(feature = "zstd")]
            block_layout,
            #[cfg(feature = "zstd")]
            data_block_restart_interval,

            index_initialized: false,

            lo_offset: BlockOffset(0),
            lo_data_block: None,

            hi_offset: BlockOffset(u64::MAX),
            hi_data_block: None,

            range: (None, None),
            poisoned: false,

            #[cfg(feature = "metrics")]
            metrics,
        }
    }

    /// Loads and resolves a data block by handle, dispatching on the SST layout:
    /// a columnar SST's block is read as `BlockType::Columnar` and reconstructed
    /// into a row-major block; a row SST's block is loaded directly. The
    /// reconstructed block is in-memory, so a fixed restart interval yields a
    /// correct, iterable block regardless of the SST's recorded value.
    fn load_and_resolve(&self, handle: &BlockHandle) -> crate::Result<DataBlock> {
        let block_type = if self.columnar {
            crate::table::block::BlockType::Columnar
        } else {
            crate::table::block::BlockType::Data
        };
        let raw = load_block(
            self.table_id,
            &self.path,
            &self.file_accessor,
            &self.cache,
            handle,
            block_type,
            self.compression,
            self.encryption.as_deref(),
            self.ecc,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
            self.heal_hints.as_ref().map(AsRef::as_ref),
            #[cfg(feature = "metrics")]
            &self.metrics,
        )?;
        if self.columnar {
            #[cfg(feature = "columnar")]
            {
                const REENCODE_RESTART_INTERVAL: u8 = 16;
                return DataBlock::from_columnar_block(&raw.data, REENCODE_RESTART_INTERVAL);
            }
            #[cfg(not(feature = "columnar"))]
            {
                return Err(crate::Error::FeatureUnsupported("columnar"));
            }
        }
        DataBlock::from_loaded(raw, self.has_kv_footer)
    }

    pub fn set_lower_bound(&mut self, bound: Bound) {
        self.range.0 = Some(bound);
    }

    pub fn set_upper_bound(&mut self, bound: Bound) {
        self.range.1 = Some(bound);
    }

    /// Reset this iterator to an un-positioned state so it can be re-seeked to a
    /// fresh range WITHOUT rebuilding the index iterator or re-cloning the
    /// table's `Arc` handles.
    ///
    /// Clears the bound pair, the materialized low/high data blocks, the offset
    /// cursors, and the poison flag, and rewinds the retained `index_iter` to
    /// the table start. The rewind is load-bearing: lazy init only calls
    /// `seek_lower` when the new range HAS a lower bound, so a re-seek to a
    /// range with an UNBOUNDED lower would otherwise inherit the previous pass's
    /// front cursor and skip earlier blocks. After this call the next
    /// [`Iterator::next`] / [`DoubleEndedIterator::next_back`] re-positions from
    /// scratch against the bounds set via [`set_lower_bound`](Self::set_lower_bound)
    /// / [`set_upper_bound`](Self::set_upper_bound).
    pub fn reset_for_reseek(&mut self) {
        self.range = (None, None);
        self.index_initialized = false;
        self.lo_offset = BlockOffset(0);
        self.lo_data_block = None;
        self.hi_offset = BlockOffset(u64::MAX);
        self.hi_data_block = None;
        // A fresh `table.range()` would produce a non-poisoned iterator; match
        // that so a re-seek past a previously-corrupt block can make progress
        // again (the block is re-read, and re-poisons only if still corrupt).
        self.poisoned = false;
        // Rewind the front (and back) index cursor to the first block. An empty
        // needle is `<=` every key, so `seek_lower` positions at the start and
        // resets both caches; an unbounded-lower re-seek then starts fresh, and
        // a bounded-lower one re-seeks over this in init.
        self.index_iter.seek_lower(&[], u64::MAX);
    }

    /// Adaptive partial-tier read for `handle`: keep a cold block only partially
    /// decoded (the touched fraction) instead of materializing the whole block,
    /// and promote it to a full resident block once access justifies the memory.
    /// Returns `None` (caller does a full load + caches the whole block) when the
    /// partial tier does not apply or the block should be promoted:
    ///
    /// - not plain zstd, encrypted, no recorded multi-inner-block layout, or no
    ///   query upper bound (an unbounded scan reads the whole block anyway);
    /// - the block sits entirely below `upper` (fully in range → full decode);
    /// - the block is already resident as a full block (let the cache serve it);
    /// - PROMOTION: the cached partial has been served [`PARTIAL_PROMOTE_HITS`]
    ///   times, or covering the query would decode at least
    ///   [`PARTIAL_PROMOTE_FRACTION_PCT`]% of the inner blocks — at that point a
    ///   single full decode + block cache beats re-growing the partial extent
    ///   (which, lacking a resumable decoder, re-decodes from inner block 0).
    ///
    /// Otherwise it returns a synthesized covering block and caches it with the
    /// extent + access stats for the next read.
    #[cfg(feature = "zstd")]
    fn try_partial_block(&self, handle: &KeyedBlockHandle) -> crate::Result<Option<DataBlock>> {
        if !partial_decode_enabled()
            || !matches!(self.compression, CompressionType::Zstd(_))
            || self.encryption.is_some()
            // Columnar blocks have a different on-disk shape and are
            // reconstructed whole, so the inner-block partial decode never
            // applies to them.
            || self.columnar
        {
            return Ok(None);
        }
        let Some(Bound::Included(upper) | Bound::Excluded(upper)) = &self.range.1 else {
            return Ok(None);
        };
        if self.comparator.compare(upper, handle.end_key()) == std::cmp::Ordering::Greater {
            return Ok(None);
        }
        let Some(ends) = self.block_layout.ends_for(*handle.offset()) else {
            return Ok(None);
        };
        let offset = BlockOffset(*handle.offset());
        // Already promoted to a full resident block → let the normal cached load
        // path serve it (no point re-synthesizing a partial alongside it).
        if self.cache.has_block(self.table_id, offset) {
            return Ok(None);
        }
        // Cold-block gate: only engage on genuinely large blocks (the cold tier
        // runs multi-MB blocks) where decoding a fraction saves far more than the
        // per-call setup. The total decompressed size is the last cumulative end.
        let total_bytes = ends.last().copied().unwrap_or(0) as usize;
        if total_bytes < PARTIAL_MIN_BLOCK_BYTES {
            return Ok(None);
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "inner-block count is bounded well within u32"
        )]
        let total_blocks = ends.len() as u32;
        let ends = ends.to_vec();

        // Existing partial entry: serve from it (bumping hits) when it already
        // covers the query, else decide whether to grow it or promote. The fall-
        // through carries the hit count + the resume payload to continue from.
        let (carried_hits, carried_resume) =
            match self.cache.peek_partial_block(self.table_id, offset) {
                Some(entry) => {
                    let covered = self.comparator.compare(&entry.covered_upper, upper)
                        != std::cmp::Ordering::Less;
                    if covered {
                        let hits = entry.hits + 1;
                        if hits >= PARTIAL_PROMOTE_HITS {
                            // Hot enough: promote to a full resident block.
                            self.cache.evict_partial_block(self.table_id, offset);
                            return Ok(None);
                        }
                        // Synthesize the served block from the cached decompressed
                        // prefix (only the touched fraction is held in memory).
                        let block = crate::table::lazy_block::synthesize_data_block(
                            &entry.resume.window_prime,
                            self.data_block_restart_interval,
                        )?;
                        self.cache.insert_partial_block(
                            self.table_id,
                            offset,
                            crate::cache::PartialBlockEntry { hits, ..entry },
                        );
                        return Ok(Some(block));
                    }
                    // Coverage miss → grow the extent by resuming from the snapshot.
                    // If most of the block is already decoded, a full decode + block
                    // cache beats holding a near-complete partial, so promote.
                    if promote_by_fraction(entry.resume.decoded_blocks, total_blocks) {
                        self.cache.evict_partial_block(self.table_id, offset);
                        return Ok(None);
                    }
                    (entry.hits, Some(entry.resume))
                }
                None => (0, None),
            };

        let (fd, _cache_event) = self
            .file_accessor
            .get_or_open_table(&self.table_id, &self.path)?;
        let transform = crate::table::block::BlockTransform::from_parts(
            self.compression,
            None,
            #[cfg(zstd_any)]
            None,
        )?;
        let transform = match self.ecc {
            Some(ecc) => transform.with_ecc(ecc),
            None => transform,
        };
        let block_handle = BlockHandle::new(handle.offset(), handle.size());
        let (_header, frame, recovery) =
            crate::table::block::Block::read_data_frame(fd.as_ref(), block_handle, &transform)?;
        // The partial path bypasses `load_block`, so schedule auto-heal here too:
        // an ECC-corrected frame read flags the SST for a healing rewrite (the
        // partial path is non-encrypted by the guard above). This is a primary
        // read, so count the recovery by mechanism as well.
        if let Some(kind) = recovery {
            #[cfg(feature = "metrics")]
            self.metrics.record_ecc_recovery(kind);
            #[cfg(not(feature = "metrics"))]
            let _ = kind;
            crate::table::util::maybe_record_persistent_heal(
                self.table_id,
                &self.path,
                &self.file_accessor,
                &block_handle,
                crate::table::block::BlockType::Data,
                self.compression,
                None,
                self.ecc,
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
                self.heal_hints.as_ref().map(AsRef::as_ref),
                #[cfg(feature = "metrics")]
                &self.metrics,
            );
        }
        // Cold first touch (carried_resume None) or resume-grow from the cached
        // snapshot — either way only the new tail blocks are decompressed.
        let (block, covered_upper, payload) = crate::table::lazy_block::partial_data_block(
            frame.to_vec(),
            ends,
            self.data_block_restart_interval,
            &self.comparator,
            upper,
            carried_resume,
        )?;
        // Covering this query decoded most of the block → promote: drop the
        // partial and let the caller cache the whole block.
        if promote_by_fraction(payload.decoded_blocks, total_blocks) {
            self.cache.evict_partial_block(self.table_id, offset);
            return Ok(None);
        }
        // Cache the resume payload tagged with the extent it covers + carried
        // access stats, so the next read serves or resume-grows it.
        if let Some(covered) = covered_upper {
            self.cache.insert_partial_block(
                self.table_id,
                offset,
                crate::cache::PartialBlockEntry {
                    resume: payload,
                    covered_upper: covered,
                    total_blocks,
                    hits: carried_hits,
                },
            );
        }
        Ok(Some(block))
    }
}

impl Iter {
    /// Marks the iterator as permanently failed so subsequent `next()` /
    /// `next_back()` calls return `None` instead of skipping past the error.
    ///
    /// Returns `Some(Err(...))` so callers can `return self.poison(e)` directly
    /// inside `Iterator::next`.
    #[expect(
        clippy::unnecessary_wraps,
        reason = "matches Iterator::next return type"
    )]
    fn poison<E: Into<crate::Error>>(&mut self, err: E) -> Option<crate::Result<InternalValue>> {
        self.poisoned = true;
        Some(Err(err.into()))
    }
}

impl Iterator for Iter {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return None;
        }

        // Always try to keep iterating inside the already-materialized low data block first; this
        // lets callers consume multiple entries without touching the index or cache again.
        if let Some(block) = &mut self.lo_data_block
            && let Some(item) = block
                .next()
                .map(|mut v| {
                    v.key.seqno += self.global_seqno;
                    v
                })
                .map(Ok)
        {
            return Some(item);
        }

        if !self.index_initialized {
            // Lazily initialize the index iterator here (not in `new`) so callers can set bounds
            // before we incur any seek or I/O cost. Bounds exclusivity is enforced at the data-
            // block level; index seeks only narrow the span of blocks to touch.
            let mut ok = if let Some(bound) = &self.range.0 {
                // Seek to the first block whose end key is ≥ lower bound.
                // If this fails we can immediately conclude the range is empty.
                let key = match bound {
                    Bound::Included(k) | Bound::Excluded(k) => k,
                };
                self.index_iter.seek_lower(key, u64::MAX)
            } else {
                true
            };

            if ok && let Some(bound) = &self.range.1 {
                // Apply an upper-bound seek to cap the block span, but keep exact high-key
                // handling inside the data block so exclusivity is respected precisely.
                let key = match bound {
                    Bound::Included(k) | Bound::Excluded(k) => k,
                };
                ok = self.index_iter.seek_upper(key, u64::MAX);
            }

            self.index_initialized = true;

            if !ok {
                // No block in the index overlaps the requested window, so we clear state and return
                // EOF without attempting to touch any data blocks.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            }
        }

        loop {
            let Some(handle) = self.index_iter.next() else {
                // No more block handles coming from the index.  Flush any pending items buffered on
                // the high side (used by reverse iteration) before signalling completion.
                if let Some(block) = &mut self.hi_data_block
                    && let Some(item) = block
                        .next()
                        .map(|mut v| {
                            v.key.seqno += self.global_seqno;
                            v
                        })
                        .map(Ok)
                {
                    return Some(item);
                }

                // Nothing left to serve; drop both buffers so the iterator can be reused safely.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            };
            let handle = match handle {
                Ok(h) => h,
                Err(e) => return self.poison(e),
            };

            // Partial-decode fast path: for a large multi-inner-block zstd block
            // whose recorded layout lets us skip the inner blocks past the query
            // upper bound, decode only the covering prefix. Falls back to a full
            // load (which checks the block cache) otherwise.
            #[cfg(feature = "zstd")]
            let partial = match self.try_partial_block(&handle) {
                Ok(p) => p,
                Err(e) => return self.poison(e),
            };
            #[cfg(not(feature = "zstd"))]
            let partial: Option<DataBlock> = None;

            let block = if let Some(db) = partial {
                db
            } else {
                match self.load_and_resolve(&BlockHandle::new(handle.offset(), handle.size())) {
                    Ok(b) => b,
                    Err(e) => return self.poison(e),
                }
            };

            let mut reader = match create_data_block_reader(block, self.comparator.clone()) {
                Ok(r) => r,
                Err(e) => return self.poison(e),
            };

            // Forward path: seek the low side first to avoid returning entries below the lower
            // bound, then clamp the iterator on the high side. This guarantees iteration stays in
            // [low, high] with exact control over inclusivity/exclusivity.
            if let Some(bound) = &self.range.0 {
                reader.seek_lower_bound(bound, SeqNo::MAX);
            }
            if let Some(bound) = &self.range.1 {
                reader.seek_upper_bound(bound, SeqNo::MAX);
            }

            let item = reader.next();

            self.lo_offset = handle.offset();
            self.lo_data_block = Some(reader);

            if let Some(mut item) = item {
                item.key.seqno += self.global_seqno;

                // Serving the first item immediately avoids stashing it in a temporary buffer and
                // keeps block iteration semantics identical to the simple case at the top.
                return Some(Ok(item));
            }
        }
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return None;
        }

        // Mirror the forward iterator: prefer consuming buffered items from the high data block to
        // avoid touching the index once a block has been materialized.
        if let Some(block) = &mut self.hi_data_block
            && let Some(item) = block
                .next_back()
                .map(|mut v| {
                    v.key.seqno += self.global_seqno;
                    v
                })
                .map(Ok)
        {
            return Some(item);
        }

        if !self.index_initialized {
            // Mirror forward iteration: initialize lazily so bounds can be applied up-front. The
            // index only restricts which blocks we consider; tight bound enforcement happens in
            // the data block readers below.
            let mut ok = if let Some(bound) = &self.range.0 {
                let key = match bound {
                    Bound::Included(k) | Bound::Excluded(k) => k,
                };
                self.index_iter.seek_lower(key, u64::MAX)
            } else {
                true
            };

            if ok && let Some(bound) = &self.range.1 {
                let key = match bound {
                    Bound::Included(k) | Bound::Excluded(k) => k,
                };
                ok = self.index_iter.seek_upper(key, u64::MAX);
            }

            self.index_initialized = true;

            if !ok {
                // No index span overlaps the requested window; clear both buffers and finish early.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            }
        }

        loop {
            let Some(handle) = self.index_iter.next_back() else {
                // Once we exhaust the index in reverse order, flush any items that were buffered on
                // the low side (set when iterating forward first) before signalling completion.
                if let Some(block) = &mut self.lo_data_block
                    && let Some(item) = block
                        .next_back()
                        .map(|mut v| {
                            v.key.seqno += self.global_seqno;
                            v
                        })
                        .map(Ok)
                {
                    return Some(item);
                }

                // Nothing left to produce; reset both buffers to keep the iterator reusable.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            };
            let handle = match handle {
                Ok(h) => h,
                Err(e) => return self.poison(e),
            };

            // Partial-decode fast path: for a large multi-inner-block zstd block
            // whose recorded layout lets us skip the inner blocks past the query
            // upper bound, decode only the covering prefix. Falls back to a full
            // load (which checks the block cache) otherwise.
            #[cfg(feature = "zstd")]
            let partial = match self.try_partial_block(&handle) {
                Ok(p) => p,
                Err(e) => return self.poison(e),
            };
            #[cfg(not(feature = "zstd"))]
            let partial: Option<DataBlock> = None;

            let block = if let Some(db) = partial {
                db
            } else {
                match self.load_and_resolve(&BlockHandle::new(handle.offset(), handle.size())) {
                    Ok(b) => b,
                    Err(e) => return self.poison(e),
                }
            };

            let mut reader = match create_data_block_reader(block, self.comparator.clone()) {
                Ok(r) => r,
                Err(e) => return self.poison(e),
            };

            // Reverse path: clamp the high side first so `next_back` never yields an entry above
            // the upper bound, then apply the low-side seek to avoid stepping below the lower
            // bound during reverse traversal.
            if let Some(bound) = &self.range.1 {
                reader.seek_upper_bound(bound, SeqNo::MAX);
            }
            if let Some(bound) = &self.range.0 {
                reader.seek_lower_bound(bound, SeqNo::MAX);
            }

            let item = reader.next_back();

            self.hi_offset = handle.offset();
            self.hi_data_block = Some(reader);

            if let Some(mut item) = item {
                item.key.seqno += self.global_seqno;

                // Emit the first materialized entry immediately to match the forward path and avoid
                // storing it in a temporary buffer.
                return Some(Ok(item));
            }
        }
    }
}

#[cfg(all(test, feature = "zstd"))]
mod promote_tests {
    use super::promote_by_fraction;

    #[test]
    fn promote_by_fraction_triggers_at_or_above_threshold() {
        // 75% threshold: 3 of 4 blocks == exactly 75% → promote.
        assert!(promote_by_fraction(3, 4));
        // 6 of 8 == 75% → promote.
        assert!(promote_by_fraction(6, 8));
        // Full coverage always promotes.
        assert!(promote_by_fraction(10, 10));
    }

    #[test]
    fn promote_by_fraction_holds_below_threshold() {
        // 2 of 4 == 50% → keep partial.
        assert!(!promote_by_fraction(2, 4));
        // 1 of 8 → keep partial.
        assert!(!promote_by_fraction(1, 8));
        // 5 of 8 == 62.5% → keep partial.
        assert!(!promote_by_fraction(5, 8));
    }

    #[test]
    fn promote_by_fraction_zero_total_never_promotes() {
        // No layout (total == 0): the partial tier does not apply, never promote.
        assert!(!promote_by_fraction(0, 0));
        assert!(!promote_by_fraction(5, 0));
    }
}
