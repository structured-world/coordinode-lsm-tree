// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(feature = "zstd")]
use crate::UserKey;
use crate::table::block::Header;
use crate::table::{Block, BlockOffset};
use crate::{GlobalTableId, UserValue};
use quick_cache::Weighter;
use quick_cache::sync::Cache as QuickCache;

const TAG_BLOCK: u8 = 0;
const TAG_BLOB: u8 = 1;
#[cfg(feature = "zstd")]
const TAG_PARTIAL_BLOCK: u8 = 2;

#[derive(Clone)]
enum Item {
    Block(Block),
    Blob(UserValue),
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

#[derive(Eq, std::hash::Hash, PartialEq)]
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
    /// Concurrent cache implementation
    data: QuickCache<CacheKey, Item, BlockWeighter, rustc_hash::FxBuildHasher>,

    /// Capacity in bytes
    capacity: u64,
}

impl Cache {
    /// Creates a new block cache with roughly `n` bytes of capacity.
    #[must_use]
    pub fn with_capacity_bytes(bytes: u64) -> Self {
        use quick_cache::sync::DefaultLifecycle;

        #[expect(clippy::expect_used, reason = "nothing we can do if it fails")]
        let opts = quick_cache::OptionsBuilder::new()
            .weight_capacity(bytes)
            .hot_allocation(0.8)
            .estimated_items_capacity(10_000)
            .build()
            .expect("cache options should be valid");

        let quick_cache = QuickCache::with_options(
            opts,
            BlockWeighter,
            rustc_hash::FxBuildHasher,
            DefaultLifecycle::default(),
        );

        Self {
            data: quick_cache,
            capacity: bytes,
        }
    }

    /// Returns the amount of cached bytes.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.data.weight()
    }

    /// Returns the cache capacity in bytes.
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_block(&self, id: GlobalTableId, offset: BlockOffset) -> Option<Block> {
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into();

        Some(match self.data.get(&key)? {
            Item::Block(block) => block,
            Item::Blob(_) => unreachable!("invalid cache item"),
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
        self.data.insert(
            (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into(),
            Item::Block(block),
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
            Item::Block(_) => unreachable!("invalid cache item"),
            #[cfg(feature = "zstd")]
            Item::PartialBlock(_) => unreachable!("invalid cache item"),
        })
    }
}
