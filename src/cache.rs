// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(feature = "zstd")]
use crate::UserKey;
use crate::sharded_cache::{Priority, ShardedCache, Weighter};
use crate::table::block::{BlockType, Header};
use crate::table::{Block, BlockOffset};
use crate::value::InternalValue;
use crate::{GlobalTableId, UserValue};

const TAG_BLOCK: u8 = 0;
const TAG_BLOB: u8 = 1;
#[cfg(feature = "zstd")]
const TAG_PARTIAL_BLOCK: u8 = 2;
/// Row-cache tag: a fully resolved point-read result (`InternalValue`) keyed by
/// the owning SST's id + the user key's hash, so a repeat point read returns the
/// decoded value without re-loading and re-decoding its data block.
const TAG_ROW: u8 = 3;

#[derive(Clone)]
enum Item {
    Block(Block),
    Blob(UserValue),
    /// A resolved point-read result for one user key in one (immutable) SST: the
    /// newest version found there. The full key is carried in `key.user_key` so a
    /// hash collision on the cache key is caught (verified on lookup) rather than
    /// returning a wrong value.
    Row(InternalValue),
    /// The adaptive partial-tier entry for a cold zstd block: the decompressed
    /// prefix + resume snapshot (so a later read extends it without re-decoding
    /// from block 0) plus the access stats driving promotion to a full resident
    /// block. The served data block is synthesized on demand from the prefix, so
    /// only the touched fraction stays resident. See [`Cache::peek_partial_block`].
    #[cfg(feature = "zstd")]
    PartialBlock(PartialBlockEntry),
}

/// A cached partial-tier entry: the resumable decode state for a cold block plus
/// the access stats the promotion heuristic reads. When the block is read often
/// enough or its decoded fraction passes the promotion threshold, the reader
/// decodes it fully and caches the whole block instead, evicting this entry.
#[cfg(feature = "zstd")]
#[doc(hidden)]
#[derive(Clone)]
pub struct PartialBlockEntry {
    /// Resumable decode state: the decompressed prefix (`window_prime`), the
    /// entropy/repcode snapshot, the inner-block count, and the compressed
    /// cursor. The served data block is synthesized from `window_prime`; growth
    /// resumes from the snapshot.
    pub resume: crate::table::lazy_block::PartialResume,
    /// Highest user key the decoded prefix covers (its last complete entry).
    pub covered_upper: UserKey,
    /// Total inner zstd blocks in the full data block.
    pub total_blocks: u32,
    /// Number of times this partial entry has served a read (promotion input).
    pub hits: u32,
}

#[derive(Clone, Copy, Eq, core::hash::Hash, PartialEq)]
struct CacheKey(u8, u64, u64, u64);

impl From<(u8, u64, u64, u64)> for CacheKey {
    fn from((tag, root_id, table_id, offset): (u8, u64, u64, u64)) -> Self {
        Self(tag, root_id, table_id, offset)
    }
}

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<CacheKey, Item> for BlockWeighter {
    fn weight(&self, _: &CacheKey, item: &Item) -> u64 {
        use Item::{Blob, Block};

        match item {
            Block(b) => {
                (Header::header_len(b.header.block_type) as u64)
                    + u64::from(b.header.uncompressed_length)
            }
            Blob(b) => b.len() as u64,
            // Key bytes + value bytes + a fixed term for the InternalKey scalars
            // (seqno + value_type) and the entry's own bookkeeping.
            Item::Row(iv) => iv.key.user_key.len() as u64 + iv.value.len() as u64 + 16,
            // Weighed by the resident decompressed prefix + covered key; the
            // shared `Arc<ResumeState>` scratch is approximated by a small fixed
            // term rather than counted per entry.
            #[cfg(feature = "zstd")]
            Item::PartialBlock(entry) => {
                entry.resume.window_prime.len() as u64 + entry.covered_upper.len() as u64 + 64
            }
        }
    }
}

/// Cache, in which blocks or blobs are cached in-memory
/// after being retrieved from disk
///
/// This speeds up consecutive queries to nearby data, improving
/// read performance for hot data.
///
/// # Examples
///
/// Sharing cache between multiple trees
///
/// ```
/// # use lsm_tree::{Tree, Config, Cache};
/// # use std::sync::Arc;
/// #
/// // Provide 64 MB of cache capacity
/// let cache = Arc::new(Cache::with_capacity_bytes(64 * 1_000 * 1_000));
///
/// # let folder = tempfile::tempdir()?;
/// let tree1 = Config::new(folder, Default::default(), Default::default()).use_cache(cache.clone()).open()?;
/// # let folder = tempfile::tempdir()?;
/// let tree2 = Config::new(folder, Default::default(), Default::default()).use_cache(cache.clone()).open()?;
/// #
/// # Ok::<(), lsm_tree::Error>(())
/// ```
pub struct Cache {
    // NOTE: rustc_hash performed best: https://fjall-rs.github.io/post/fjall-2-1
    /// In-tree sharded S3-FIFO cache (byte-weighted).
    data: ShardedCache<CacheKey, Item, BlockWeighter, rustc_hash::FxBuildHasher>,
    /// Opt-in: when false, the row cache (decoded point-read results) is off, so
    /// `get_row` always misses and `insert_row` is a no-op. Blocks / blobs are
    /// cached regardless. Off by default to avoid spending the shared capacity on
    /// rows for workloads that do not benefit (e.g. uniform / scan-heavy).
    row_cache_enabled: bool,
    /// When true (default), index / filter / range-tombstone blocks are admitted
    /// at [`Priority::High`] so heavy data-block churn (working set >> cache)
    /// cannot evict the metadata blocks every seek touches, sparing a re-read +
    /// re-decode on the next index descent. Disable to put every block on equal
    /// footing (the pre-priority behaviour), e.g. for A/B measurement.
    metadata_priority: bool,
}

/// Number of shards in the block cache. 64 keeps per-shard write contention low
/// on many-core hosts while the lock array stays small; reads take a shared lock
/// and don't contend regardless of shard count.
const BLOCK_CACHE_SHARDS: usize = 64;
/// Seeds the per-shard ghost-queue sizing (S3-FIFO remembers recently-evicted
/// fingerprints to fast-track re-admission). Matches the previous
/// `estimated_items_capacity`.
const BLOCK_CACHE_EST_ITEMS: usize = 10_000;

impl Cache {
    /// Creates a new block cache with roughly `n` bytes of capacity.
    #[must_use]
    pub fn with_capacity_bytes(bytes: u64) -> Self {
        Self {
            data: ShardedCache::with_weighter(
                bytes,
                BLOCK_CACHE_SHARDS,
                BLOCK_CACHE_EST_ITEMS,
                BlockWeighter,
                rustc_hash::FxBuildHasher,
            ),
            row_cache_enabled: false,
            metadata_priority: true,
        }
    }

    /// Enables or disables the row cache (decoded point-read results), returning
    /// the cache for builder-style configuration. Off by default; rows share the
    /// block cache's byte capacity when enabled.
    #[must_use]
    pub fn with_row_cache(mut self, enabled: bool) -> Self {
        self.row_cache_enabled = enabled;
        self
    }

    /// Whether the row cache is enabled (see [`Cache::with_row_cache`]).
    #[must_use]
    pub fn row_cache_enabled(&self) -> bool {
        self.row_cache_enabled
    }

    /// Enables or disables high-priority pinning of index / filter /
    /// range-tombstone blocks (see [`Cache::metadata_priority`] field docs),
    /// returning the cache for builder-style configuration. On by default.
    #[must_use]
    pub fn with_metadata_priority(mut self, enabled: bool) -> Self {
        self.metadata_priority = enabled;
        self
    }

    /// Whether metadata-block priority pinning is enabled (see
    /// [`Cache::with_metadata_priority`]).
    #[must_use]
    pub fn metadata_priority(&self) -> bool {
        self.metadata_priority
    }

    /// Returns the amount of cached bytes.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.data.weight()
    }

    /// Returns the cache capacity in bytes.
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.data.capacity()
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_block(&self, id: GlobalTableId, offset: BlockOffset) -> Option<Block> {
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into();

        Some(match self.data.get(&key)? {
            Item::Block(block) => block,
            Item::Blob(_) | Item::Row(_) => unreachable!("invalid cache item"),
            #[cfg(feature = "zstd")]
            Item::PartialBlock(_) => unreachable!("invalid cache item"),
        })
    }

    /// Whether a full (non-partial) data block is already resident for `offset`.
    /// The partial-tier reader uses this to bail out (let the normal cached path
    /// serve) once a block has been promoted to a full resident block.
    #[cfg(feature = "zstd")]
    #[doc(hidden)]
    #[must_use]
    pub fn has_block(&self, id: GlobalTableId, offset: BlockOffset) -> bool {
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into();
        self.data.peek(&key).is_some()
    }

    /// Reads the cached partial-tier entry for `offset` (resume state + access
    /// stats), without mutating it. The caller checks coverage against its query,
    /// applies the promotion heuristic, then re-inserts with bumped stats, grows
    /// the extent, or promotes to a full block.
    #[cfg(feature = "zstd")]
    #[doc(hidden)]
    #[must_use]
    pub fn peek_partial_block(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
    ) -> Option<PartialBlockEntry> {
        let key: CacheKey = (TAG_PARTIAL_BLOCK, id.tree_id(), id.table_id(), *offset).into();
        match self.data.peek(&key) {
            Some(Item::PartialBlock(entry)) => Some(entry),
            _ => None,
        }
    }

    /// Inserts or replaces the partial-tier entry for `offset` (high-water
    /// growth: a wider covering prefix with more decoded inner blocks replaces a
    /// narrower one).
    #[cfg(feature = "zstd")]
    #[doc(hidden)]
    pub fn insert_partial_block(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
        entry: PartialBlockEntry,
    ) {
        self.data.insert(
            (TAG_PARTIAL_BLOCK, id.tree_id(), id.table_id(), *offset).into(),
            Item::PartialBlock(entry),
        );
    }

    /// Drops the partial-tier entry for `offset` (used on promotion to a full
    /// resident block, so the stale partial does not linger).
    #[cfg(feature = "zstd")]
    #[doc(hidden)]
    pub fn evict_partial_block(&self, id: GlobalTableId, offset: BlockOffset) {
        let key: CacheKey = (TAG_PARTIAL_BLOCK, id.tree_id(), id.table_id(), *offset).into();
        self.data.remove(&key);
    }

    #[doc(hidden)]
    pub fn insert_block(&self, id: GlobalTableId, offset: BlockOffset, block: Block) {
        // Pin index / filter / range-tombstone blocks: they are touched on every
        // seek (index descent + bloom check), so under data-block churn (working
        // set >> cache) they must outlive the data blocks that would otherwise
        // evict them and force a metadata re-read + re-decode on the next seek.
        let priority = if self.metadata_priority
            && matches!(
                block.header.block_type,
                BlockType::Index | BlockType::Filter | BlockType::RangeTombstone
            ) {
            Priority::High
        } else {
            Priority::Normal
        };
        self.data.insert_with_priority(
            (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into(),
            Item::Block(block),
            priority,
        );
    }

    /// Looks up the cached point-read result for `user_key` in SST `id`. The
    /// stored key is verified against `user_key` so a hash collision on the
    /// cache slot is rejected (returns `None`) rather than serving a wrong value.
    /// `key_hash` is the same hash the bloom filter uses, so the caller passes
    /// the value it already computed.
    #[doc(hidden)]
    #[must_use]
    pub fn get_row(
        &self,
        id: GlobalTableId,
        key_hash: u64,
        user_key: &[u8],
    ) -> Option<InternalValue> {
        if !self.row_cache_enabled {
            return None;
        }
        let key: CacheKey = (TAG_ROW, id.tree_id(), id.table_id(), key_hash).into();
        match self.data.get(&key)? {
            Item::Row(iv) if &*iv.key.user_key == user_key => Some(iv),
            // Hash collision (a different key hashed to this slot) or a foreign
            // item kind: treat as a miss so the caller does the real lookup.
            _ => None,
        }
    }

    /// Caches the resolved point-read result `iv` for SST `id`, keyed by
    /// `key_hash`. Only a newest-version result (from a latest-version read)
    /// should be inserted, so the seqno-visibility check on lookup stays correct.
    /// SSTs are immutable, so an entry stays valid until its SST is compacted
    /// away (after which its `table_id` is never read again and the entry ages
    /// out of the cache).
    #[doc(hidden)]
    pub fn insert_row(&self, id: GlobalTableId, key_hash: u64, iv: InternalValue) {
        if !self.row_cache_enabled {
            return;
        }
        self.data.insert(
            (TAG_ROW, id.tree_id(), id.table_id(), key_hash).into(),
            Item::Row(iv),
        );
    }

    #[doc(hidden)]
    pub fn insert_blob(
        &self,
        vlog_id: crate::TreeId,
        vhandle: &crate::vlog::ValueHandle,
        value: UserValue,
    ) {
        self.data.insert(
            (TAG_BLOB, vlog_id, vhandle.blob_file_id, vhandle.offset).into(),
            Item::Blob(value),
        );
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_blob(
        &self,
        vlog_id: crate::TreeId,
        vhandle: &crate::vlog::ValueHandle,
    ) -> Option<UserValue> {
        let key: CacheKey = (TAG_BLOB, vlog_id, vhandle.blob_file_id, vhandle.offset).into();

        Some(match self.data.get(&key)? {
            Item::Blob(blob) => blob,
            Item::Block(_) | Item::Row(_) => unreachable!("invalid cache item"),
            #[cfg(feature = "zstd")]
            Item::PartialBlock(_) => unreachable!("invalid cache item"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Cache;

    #[test]
    fn metadata_priority_defaults_on_and_toggles() {
        // On by default.
        assert!(Cache::with_capacity_bytes(1024).metadata_priority());
        // Builder turns it off and back on.
        let off = Cache::with_capacity_bytes(1024).with_metadata_priority(false);
        assert!(!off.metadata_priority());
        assert!(off.with_metadata_priority(true).metadata_priority());
    }
}
