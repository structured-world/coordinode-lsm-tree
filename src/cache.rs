// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

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
/// Decoded-directory tag: a columnar row group's page directory, decoded and
/// checked once, keyed by the group's offset.
#[cfg(feature = "columnar")]
const TAG_DIRECTORY: u8 = 4;

#[derive(Clone)]
enum Item {
    Block(Block),
    /// A separated value, carrying everything about the handle that the cache
    /// key does not: the user key it belongs to and the record's on-disk size.
    ///
    /// The cache key is the blob file id plus the offset, which identifies a
    /// POSITION, not a record. A corrupt index entry can name that position
    /// with a different key, or with the right key and the wrong size. A direct
    /// read rejects both (the reader compares the key it was asked for AND the
    /// declared size against the record header), so the cached path keeps the
    /// same two fields and compares them, rather than trusting the position.
    Blob(crate::UserKey, u32, UserValue),
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
    /// A columnar row group's page directory, decoded and checked, with the
    /// directory block's on-disk length. Every read of a group starts from its
    /// directory, and a directory of many row pages is dozens to hundreds of
    /// entries whose decode sorts them to prove the page grid: caching it
    /// decoded makes every later read of the group skip both, where caching
    /// the raw block would repeat them per read.
    #[cfg(feature = "columnar")]
    Directory(
        alloc::sync::Arc<crate::table::column_page::PageDirectory>,
        u32,
    ),
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

/// What a cached row costs the cache: key bytes + value bytes + a fixed term for
/// the `InternalKey` scalars (seqno + `value_type`) and the entry's own
/// bookkeeping.
///
/// Shared by the weigher and by `insert_row`'s admission pre-check so the two
/// cannot drift: a pre-check that charged less than the weigher would copy a
/// value the cache then refuses, and one that charged more would skip a row that
/// would have fit.
const fn row_weight(key_len: usize, value_len: usize) -> u64 {
    key_len as u64 + value_len as u64 + 16
}

/// What a block weighs in the cache: its header and its decoded payload.
pub fn block_weight(block: &crate::table::Block) -> u64 {
    (Header::header_len(block.header.block_type) as u64)
        + u64::from(block.header.uncompressed_length)
}

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<CacheKey, Item> for BlockWeighter {
    fn weight(&self, _: &CacheKey, item: &Item) -> u64 {
        use Item::{Blob, Block};

        match item {
            Block(b) => block_weight(b),
            // Key + value; the size field is an inline scalar. The prefetch's
            // admission budget charges itself the same way, so the two agree.
            Blob(key, _, b) => (key.len() + b.len()) as u64,
            Item::Row(iv) => row_weight(iv.key.user_key.len(), iv.value.len()),
            // Weighed by the resident decompressed prefix + covered key; the
            // shared `Arc<ResumeState>` scratch is approximated by a small fixed
            // term rather than counted per entry.
            #[cfg(feature = "zstd")]
            Item::PartialBlock(entry) => {
                entry.resume.window_prime.len() as u64 + entry.covered_upper.len() as u64 + 64
            }
            #[cfg(feature = "columnar")]
            Item::Directory(directory, _) => directory.heap_size() as u64 + 64,
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
    /// When false, the row cache (decoded point-read results) is off, so
    /// `get_row` always misses and `insert_row` is a no-op. Blocks / blobs are
    /// cached regardless.
    ///
    /// On by default. Rows share the block cache's byte capacity, so the worry
    /// was that a workload without key reuse would spend capacity on rows at the
    /// blocks' expense. Measurement says it does not: scan-heavy and
    /// larger-than-cache arms are unchanged or better, while a repeat point read
    /// skips the index walk and the data-block decode outright. Turn it off for
    /// a workload measured to be one of the exceptions.
    row_cache_enabled: bool,
    /// When true (default), index / filter / range-tombstone blocks are admitted
    /// at [`Priority::High`] so heavy data-block churn (working set >> cache)
    /// cannot evict the metadata blocks every seek touches, sparing a re-read +
    /// re-decode on the next index descent. Disable to put every block on equal
    /// footing (the pre-priority behaviour), e.g. for A/B measurement.
    metadata_priority: bool,
    /// The cold loads in flight, keyed like the entries they will fill, so
    /// readers that miss the same block or blob at once share one read and
    /// one decode. Only a miss consults it.
    loads: Loads,
}

/// The registry of cold loads in flight.
type Loads =
    crate::inflight::InflightLoads<CacheKey, Item, LoadIdentity, rustc_hash::FxBuildHasher>;

/// The lead of one cold load, borrowing the cache's registry.
type LoadLead<'a> =
    crate::inflight::Lead<'a, CacheKey, Item, LoadIdentity, rustc_hash::FxBuildHasher>;

/// What a cold load reads, beyond its position: a waiter takes a load's
/// result only when it asked for the same thing, so a request with another
/// extent, role or codec never receives bytes interpreted for a different one.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LoadIdentity {
    /// A table block: its role, on-disk length, codec (dictionary included)
    /// and whether it is encrypted.
    Block {
        block_type: BlockType,
        size: u32,
        compression: crate::CompressionType,
        encrypted: bool,
    },
    /// A separated value: its record's on-disk size. The user key is checked
    /// against the value the load returns, as a cache hit checks it.
    Blob { on_disk_size: u32 },
    /// Growth of a block's partially decoded prefix. Waiters take no value:
    /// once the growth ends they look at the cached prefix again.
    #[cfg(feature = "zstd")]
    PartialGrowth,
}

/// How a read that needs a block's partial prefix grown proceeds.
#[cfg(feature = "zstd")]
pub enum GrowthClaim<'a> {
    /// No growth of the block runs: grow it, holding the lead until the
    /// grown prefix is cached.
    Lead(GrowthLead<'a>),
    /// Another read grew the block while this one waited: look at the cached
    /// prefix again before growing anything.
    Waited,
    /// Grow without coordination: the registry is full.
    Alone,
}

/// The lead of a partial prefix's growth. Dropping it, after the grown prefix
/// is cached or on any failure, lets the reads waiting on it look again.
#[cfg(feature = "zstd")]
pub struct GrowthLead<'a>(
    #[expect(dead_code, reason = "held for its drop, which releases the waiters")] LoadLead<'a>,
);

/// How a block miss proceeds: lead the load, take a concurrent one's block,
/// or load alone.
pub enum BlockClaim<'a> {
    /// No load of the block is in flight: read it, then [`BlockLead::publish`].
    Lead(BlockLead<'a>),
    /// A load of the block finished while this request waited for it.
    Shared(Block),
    /// Read the block without coordination: the registry is full, an
    /// incompatible load of the position is in flight, or the load this
    /// request waited for failed.
    Alone,
}

/// The lead of a block load. Dropping it unpublished (an error or an unwind)
/// releases its waiters to read on their own. A batched read's lead also
/// keeps its thread from waiting on other loads while it lives.
pub struct BlockLead<'a>(LoadLead<'a>, Option<crate::inflight::BatchToken>);

impl BlockLead<'_> {
    /// Hands the loaded block to the waiters, if any; insert it into the
    /// cache first, so a miss after this sees it there.
    pub(crate) fn publish(self, block: &Block) {
        let Self(lead, _batch) = self;
        lead.publish_with(|| Item::Block(block.clone()));
    }
}

/// How a miss of a row group's decoded directory proceeds, as [`BlockClaim`]
/// for a block.
#[cfg(feature = "columnar")]
pub enum DirectoryClaim<'a> {
    /// Read the directory, then [`DirectoryLead::publish`] it.
    Lead(DirectoryLead<'a>),
    /// A concurrent read of the group's directory finished: the directory
    /// and its block's on-disk length.
    Shared(
        alloc::sync::Arc<crate::table::column_page::PageDirectory>,
        u32,
    ),
    /// Read the directory without coordination.
    Alone,
}

/// The lead of a directory load, as [`BlockLead`] for a block.
#[cfg(feature = "columnar")]
pub struct DirectoryLead<'a>(LoadLead<'a>);

#[cfg(feature = "columnar")]
impl DirectoryLead<'_> {
    /// Hands the decoded directory and its block's on-disk length to the
    /// waiters; cache it first, so a miss after this sees it there.
    pub(crate) fn publish(
        self,
        directory: &alloc::sync::Arc<crate::table::column_page::PageDirectory>,
        len: u32,
    ) {
        self.0
            .publish_with(|| Item::Directory(alloc::sync::Arc::clone(directory), len));
    }
}

/// How a blob miss proceeds, as [`BlockClaim`] for a block.
pub enum BlobClaim<'a> {
    /// Read the value, then [`BlobLead::publish`].
    Lead(BlobLead<'a>),
    /// A concurrent read of the same record, for the same key, finished.
    Shared(UserValue),
    /// Read the value without coordination.
    Alone,
}

/// The lead of a blob load, as [`BlockLead`] for a block.
pub struct BlobLead<'a>(LoadLead<'a>);

impl BlobLead<'_> {
    /// Hands the value read for `user_key` to the waiters, caching it first
    /// when `fill` says so. Returns the bytes copied to own the key, which the
    /// cache entry and the waiters share: none when the value is neither
    /// cached nor awaited.
    pub(crate) fn publish(
        self,
        cache: &Cache,
        vlog_id: crate::TreeId,
        vhandle: &crate::vlog::ValueHandle,
        user_key: &[u8],
        value: UserValue,
        fill: bool,
    ) -> usize {
        if !fill && !self.0.has_waiters() {
            // Dropped unpublished: nothing to share, and the key is not
            // copied for nobody.
            return 0;
        }
        let item = Item::Blob(user_key.into(), vhandle.on_disk_size, value);
        if fill {
            cache.data.insert(
                (TAG_BLOB, vlog_id, vhandle.blob_file_id, vhandle.offset).into(),
                item.clone(),
            );
        }
        self.0.publish(item);
        user_key.len()
    }
}

/// Shards of the load registry: as many as the cache, so two loads that
/// would contend on one cache shard are the ones that share a registry shard.
const LOAD_SHARDS: usize = BLOCK_CACHE_SHARDS;
/// Loads one registry shard coordinates at once, so the registry holds at
/// most `LOAD_SHARDS * LOADS_PER_SHARD` tickets; a miss past that loads on
/// its own.
const LOADS_PER_SHARD: usize = 64;
/// How long cold loads must take before concurrent misses of one object wait
/// for a single load instead of each loading it (see
/// [`Cache::with_load_coalescing_after`]). Loads served from the page cache
/// take a few microseconds for a block and tens for a whole column row group,
/// and waiting on another reader there loses to loading alone; this sits
/// above them and below a read that reaches a device or the network.
const DEFAULT_LOAD_COALESCING_AFTER: core::time::Duration = core::time::Duration::from_micros(64);

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
            row_cache_enabled: true,
            metadata_priority: true,
            loads: crate::inflight::InflightLoads::new(
                LOAD_SHARDS,
                LOADS_PER_SHARD,
                DEFAULT_LOAD_COALESCING_AFTER,
                rustc_hash::FxBuildHasher,
            ),
        }
    }

    /// Claims the cold load of the block at `offset` after a cache miss.
    ///
    /// Blocks while a compatible load of the block runs on another thread,
    /// returning its block. A [`BlockClaim::Lead`] holder checks the cache
    /// once more before reading: a load that finished between its miss and
    /// this claim left the block there.
    ///
    /// A thread that holds a batched read's lead never waits (see
    /// [`crate::inflight::may_wait`]): it takes a block another read already
    /// published and reads one still being read again on its own.
    pub(crate) fn claim_block(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
        identity: LoadIdentity,
    ) -> BlockClaim<'_> {
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into();
        match self.loads.claim(key, identity) {
            crate::inflight::Claim::Lead(lead) => BlockClaim::Lead(BlockLead(lead, None)),
            crate::inflight::Claim::Join(join) => {
                let shared = if crate::inflight::may_wait() {
                    join.wait()
                } else {
                    join.try_take()
                };
                match shared {
                    Some(Item::Block(block)) => BlockClaim::Shared(block),
                    _ => BlockClaim::Alone,
                }
            }
            crate::inflight::Claim::Alone => BlockClaim::Alone,
        }
    }

    /// Claims the block at `offset` for a batched read about to read it along
    /// with others. Never waits, since the read goes on to lead more blocks:
    /// a block another read already published is taken, and one still being
    /// read is read again alone.
    ///
    /// `lead` says whether the read may lead the load (see
    /// [`crate::inflight::batches_may_lead`]); a lead it takes counts on this
    /// thread until it is published or dropped, so the thread waits on
    /// nothing meanwhile. Without it no load is claimed at all.
    pub(crate) fn claim_batched_block(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
        identity: LoadIdentity,
        lead: bool,
    ) -> BlockClaim<'_> {
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into();
        if !(lead && crate::inflight::batches_may_lead()) {
            return match self.loads.take_ready(&key, &identity) {
                Some(Item::Block(block)) => BlockClaim::Shared(block),
                _ => BlockClaim::Alone,
            };
        }
        match self.loads.claim(key, identity) {
            crate::inflight::Claim::Lead(lead) => {
                BlockClaim::Lead(BlockLead(lead, Some(crate::inflight::BatchToken::new())))
            }
            crate::inflight::Claim::Join(join) => match join.try_take() {
                Some(Item::Block(block)) => BlockClaim::Shared(block),
                _ => BlockClaim::Alone,
            },
            crate::inflight::Claim::Alone => BlockClaim::Alone,
        }
    }

    /// Claims the cold read of the decoded directory of the row group at
    /// `offset`, as [`Self::claim_block`] does for a block. `identity` names
    /// the group's extent, so a read of another extent at the same offset
    /// never takes this directory.
    #[cfg(feature = "columnar")]
    pub(crate) fn claim_directory(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
        identity: LoadIdentity,
    ) -> DirectoryClaim<'_> {
        let key: CacheKey = (TAG_DIRECTORY, id.tree_id(), id.table_id(), *offset).into();
        match self.loads.claim(key, identity) {
            crate::inflight::Claim::Lead(lead) => DirectoryClaim::Lead(DirectoryLead(lead)),
            crate::inflight::Claim::Join(join) => {
                let shared = if crate::inflight::may_wait() {
                    join.wait()
                } else {
                    join.try_take()
                };
                match shared {
                    Some(Item::Directory(directory, len)) => DirectoryClaim::Shared(directory, len),
                    _ => DirectoryClaim::Alone,
                }
            }
            crate::inflight::Claim::Alone => DirectoryClaim::Alone,
        }
    }

    /// How many requests wait on cold loads in flight, for tests.
    #[cfg(test)]
    pub(crate) fn load_waiters(&self) -> usize {
        self.loads.waiters()
    }

    /// How many cold loads are in flight, for tests.
    #[cfg(test)]
    pub(crate) fn loads_in_flight(&self) -> usize {
        self.loads.len()
    }

    /// Claims the cold read of the separated value `vhandle` names for
    /// `user_key`, as [`Self::claim_block`] does for a block. A shared value
    /// is returned only when the read it came from was for the same key and
    /// size: the position alone names a record, not whose it is.
    pub(crate) fn claim_blob(
        &self,
        vlog_id: crate::TreeId,
        vhandle: &crate::vlog::ValueHandle,
        user_key: &[u8],
    ) -> BlobClaim<'_> {
        let key: CacheKey = (TAG_BLOB, vlog_id, vhandle.blob_file_id, vhandle.offset).into();
        let identity = LoadIdentity::Blob {
            on_disk_size: vhandle.on_disk_size,
        };
        match self.loads.claim(key, identity) {
            crate::inflight::Claim::Lead(lead) => BlobClaim::Lead(BlobLead(lead)),
            crate::inflight::Claim::Join(join) => {
                let shared = if crate::inflight::may_wait() {
                    join.wait()
                } else {
                    join.try_take()
                };
                match shared {
                    Some(Item::Blob(read_key, size, value))
                        if &*read_key == user_key && size == vhandle.on_disk_size =>
                    {
                        BlobClaim::Shared(value)
                    }
                    _ => BlobClaim::Alone,
                }
            }
            crate::inflight::Claim::Alone => BlobClaim::Alone,
        }
    }

    /// Enables or disables the row cache (decoded point-read results), returning
    /// the cache for builder-style configuration. On by default; rows share the
    /// block cache's byte capacity.
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

    /// Sets how long cold loads must take before readers that miss the same
    /// block, blob or column directory at once share one read and decode
    /// instead of each doing it, returning the cache for builder-style
    /// configuration.
    ///
    /// The cache times a sample of its cold loads. While they finish quicker
    /// than `after` (blocks served from the page cache, with cores to spare),
    /// a reader loads on its own: waiting for another reader's load would
    /// cost it more than its own. Once loads take longer (slow or remote
    /// storage, or cores saturated), concurrent misses of one object wait for
    /// a single load. `Duration::ZERO` shares every load; `Duration::MAX`
    /// none. Without the `std` feature loads are not timed, so only
    /// `Duration::ZERO` shares there.
    ///
    /// # Examples
    ///
    /// ```
    /// use core::time::Duration;
    /// use lsm_tree::Cache;
    ///
    /// let cache = Cache::with_capacity_bytes(64 << 20)
    ///     .with_load_coalescing_after(Duration::from_micros(50));
    /// assert_eq!(cache.load_coalescing_after(), Duration::from_micros(50));
    /// ```
    #[must_use]
    pub fn with_load_coalescing_after(mut self, after: core::time::Duration) -> Self {
        self.loads.set_coalesce_after(after);
        self
    }

    /// How long cold loads must take before concurrent misses share them
    /// (see [`Cache::with_load_coalescing_after`]).
    #[must_use]
    pub fn load_coalescing_after(&self) -> core::time::Duration {
        self.loads.coalesce_after()
    }

    /// Starts timing a cold load about to run on the calling thread, for the
    /// estimate that decides whether misses share loads.
    pub(crate) fn time_load() -> crate::inflight::LoadTimer {
        crate::inflight::LoadTimer::start()
    }

    /// Records the cold load of the block at `offset` that `timer` timed.
    pub(crate) fn record_block_load(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
        timer: crate::inflight::LoadTimer,
    ) {
        self.loads.record_load(
            &(TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into(),
            timer,
        );
    }

    /// Records a wait of a batched read that `timer` timed, which handed back
    /// `reads` block reads.
    pub(crate) fn record_batch_wait(&self, timer: crate::inflight::LoadTimer, reads: usize) {
        self.loads.record_batch_wait(timer, reads);
    }

    /// Records the cold read of the separated value `vhandle` names that
    /// `timer` timed.
    pub(crate) fn record_blob_load(
        &self,
        vlog_id: crate::TreeId,
        vhandle: &crate::vlog::ValueHandle,
        timer: crate::inflight::LoadTimer,
    ) {
        self.loads.record_load(
            &(TAG_BLOB, vlog_id, vhandle.blob_file_id, vhandle.offset).into(),
            timer,
        );
    }

    /// Records the cold read of the row group at `offset` that `timer` timed.
    #[cfg(feature = "columnar")]
    pub(crate) fn record_directory_load(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
        timer: crate::inflight::LoadTimer,
    ) {
        self.loads.record_load(
            &(TAG_DIRECTORY, id.tree_id(), id.table_id(), *offset).into(),
            timer,
        );
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

    /// The heaviest entry this cache can actually keep resident: one shard's
    /// capacity. Heavier inserts are refused by the cache; callers that would
    /// build such an entry (e.g. the scan read-ahead) can skip the work.
    #[must_use]
    pub(crate) fn max_entry_weight(&self) -> u64 {
        self.data.max_entry_weight()
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_block(&self, id: GlobalTableId, offset: BlockOffset) -> Option<Block> {
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into();

        Some(match self.data.get(&key)? {
            Item::Block(block) => block,
            Item::Blob(..) | Item::Row(_) => unreachable!("invalid cache item"),
            #[cfg(feature = "zstd")]
            Item::PartialBlock(_) => unreachable!("invalid cache item"),
            #[cfg(feature = "columnar")]
            Item::Directory(..) => unreachable!("invalid cache item"),
        })
    }

    /// [`Self::get_block`] that leaves the entry's recency and the hit
    /// statistics as they were, for a reader that must not change what a later
    /// read finds cached.
    #[must_use]
    pub(crate) fn peek_block(&self, id: GlobalTableId, offset: BlockOffset) -> Option<Block> {
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into();

        Some(match self.data.peek(&key)? {
            Item::Block(block) => block,
            Item::Blob(..) | Item::Row(_) => unreachable!("invalid cache item"),
            #[cfg(feature = "zstd")]
            Item::PartialBlock(_) => unreachable!("invalid cache item"),
            #[cfg(feature = "columnar")]
            Item::Directory(..) => unreachable!("invalid cache item"),
        })
    }

    /// The decoded directory of the row group at `offset`, with its block's
    /// on-disk length, when it is cached. `touch` says whether the lookup may
    /// promote the entry and count as a hit, as [`Self::get_block`] does, or
    /// must leave both as they were, as [`Self::peek_block`] does.
    #[cfg(feature = "columnar")]
    #[must_use]
    pub(crate) fn get_directory(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
        touch: bool,
    ) -> Option<(
        alloc::sync::Arc<crate::table::column_page::PageDirectory>,
        u32,
    )> {
        let key: CacheKey = (TAG_DIRECTORY, id.tree_id(), id.table_id(), *offset).into();
        let item = if touch {
            self.data.get(&key)
        } else {
            self.data.peek(&key)
        };
        match item? {
            Item::Directory(directory, len) => Some((directory, len)),
            _ => unreachable!("invalid cache item"),
        }
    }

    /// Caches the decoded directory of the row group at `offset`, with its
    /// block's on-disk length. A directory is read before any page of its
    /// group, like an index block before its data blocks, so it is pinned at
    /// the same priority.
    #[cfg(feature = "columnar")]
    pub(crate) fn insert_directory(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
        directory: alloc::sync::Arc<crate::table::column_page::PageDirectory>,
        len: u32,
    ) {
        let priority = if self.metadata_priority {
            Priority::High
        } else {
            Priority::Normal
        };
        self.data.insert_with_priority(
            (TAG_DIRECTORY, id.tree_id(), id.table_id(), *offset).into(),
            Item::Directory(directory, len),
            priority,
        );
    }

    /// Whether a full (non-partial) data block is already resident for `offset`.
    ///
    /// The partial-tier reader uses this to bail out (let the normal cached path
    /// serve) once a block has been promoted to a full resident block, and the
    /// batched prewarm to tell a cold block from one it would re-read for
    /// nothing. Neither wants the block itself, so this answers without cloning
    /// it out and without counting a hit for a read that never happens.
    #[doc(hidden)]
    #[must_use]
    pub fn has_block(&self, id: GlobalTableId, offset: BlockOffset) -> bool {
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into();
        self.data.contains(&key)
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

    /// Records the partial-tier entry for `offset`, never letting its decoded
    /// prefix shrink: when the resident entry already decoded more inner
    /// blocks (a concurrent read grew it further), that one stays, with the
    /// higher of the two hit counts. A read that served from an older copy of
    /// the entry, or grew it less, can therefore not undo another read's
    /// growth.
    #[cfg(feature = "zstd")]
    #[doc(hidden)]
    pub fn insert_partial_block(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
        entry: PartialBlockEntry,
    ) {
        self.data.insert_merged(
            (TAG_PARTIAL_BLOCK, id.tree_id(), id.table_id(), *offset).into(),
            |resident| match resident {
                Some(Item::PartialBlock(resident))
                    if resident.resume.decoded_blocks > entry.resume.decoded_blocks =>
                {
                    Some(Item::PartialBlock(PartialBlockEntry {
                        hits: resident.hits.max(entry.hits),
                        ..resident.clone()
                    }))
                }
                Some(Item::PartialBlock(resident)) => Some(Item::PartialBlock(PartialBlockEntry {
                    hits: resident.hits.max(entry.hits),
                    ..entry
                })),
                _ => Some(Item::PartialBlock(entry)),
            },
        );
    }

    /// Claims the growth of the partially decoded prefix of the block at
    /// `offset`, so two reads that both need it longer decode the new inner
    /// blocks once: the second waits for the first, then finds the longer
    /// prefix cached.
    #[cfg(feature = "zstd")]
    pub(crate) fn claim_partial_growth(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
    ) -> GrowthClaim<'_> {
        let key: CacheKey = (TAG_PARTIAL_BLOCK, id.tree_id(), id.table_id(), *offset).into();
        match self.loads.claim(key, LoadIdentity::PartialGrowth) {
            crate::inflight::Claim::Lead(lead) => GrowthClaim::Lead(GrowthLead(lead)),
            crate::inflight::Claim::Join(join) if crate::inflight::may_wait() => {
                let _ = join.wait();
                GrowthClaim::Waited
            }
            crate::inflight::Claim::Join(_) | crate::inflight::Claim::Alone => GrowthClaim::Alone,
        }
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
            Item::Row(iv) if crate::comparator::same_user_key(&iv.key.user_key, user_key) => {
                Some(iv)
            }
            // Hash collision (a different key hashed to this slot) or a foreign
            // item kind: treat as a miss so the caller does the real lookup.
            _ => None,
        }
    }

    /// Caches the resolved point-read result (`user_key`, `seqno`,
    /// `value_type`, `value`) for SST `id`, keyed by `key_hash`, and returns
    /// the bytes it copied to own the row (zero when the row cache is off or
    /// the row is too heavy to admit). Only a newest-version result (from a
    /// latest-version read) should be inserted, so the seqno-visibility check
    /// on lookup stays correct. SSTs are immutable, so an entry stays valid
    /// until its SST is compacted away (after which its `table_id` is never
    /// read again and the entry ages out of the cache).
    #[doc(hidden)]
    #[must_use = "the bytes copied belong to the reading caller's gather count"]
    pub fn insert_row(
        &self,
        id: GlobalTableId,
        key_hash: u64,
        user_key: &[u8],
        seqno: crate::SeqNo,
        value_type: crate::ValueType,
        value: &[u8],
    ) -> usize {
        if !self.row_cache_enabled {
            return 0;
        }
        // The point-read path hands over a key and value that may be SUBSLICES
        // of the decoded data block, and a subslice keeps the whole block
        // allocation alive. The weigher charges a row its own key and value
        // bytes only, so a 100-byte row viewing a 4 KiB block would be
        // accounted as 100 bytes while holding 4096 — and a workload touching
        // one key per block, the one a row cache helps least, would overrun the
        // requested capacity by that ratio. Copying both out costs small
        // allocations per MISS, on a path that has just walked the index and
        // decoded a block, and it makes the charge equal to what is actually
        // retained.
        //
        // Unless the row cannot be admitted at all. A row heavier than one shard
        // is refused by the insert below, and a value that big is re-read as
        // often as any other, so copying it first would burn an allocation and a
        // memcpy on every one of those reads to produce something immediately
        // discarded. The copy does not change the weight, so projecting it here
        // is exact.
        if row_weight(user_key.len(), value.len()) > self.data.max_entry_weight() {
            return 0;
        }
        let iv = InternalValue {
            key: crate::key::InternalKey::new(crate::UserKey::from(user_key), seqno, value_type),
            value: crate::UserValue::from(value),
        };
        self.data.insert(
            (TAG_ROW, id.tree_id(), id.table_id(), key_hash).into(),
            Item::Row(iv),
        );
        user_key.len() + value.len()
    }

    /// Caches a separated value under its position, together with the two
    /// fields the position does not carry (`user_key` and the handle's declared
    /// on-disk size) so a lookup can prove the entry belongs to the handle it
    /// is asked about. Returns the bytes copied to own the entry's key.
    #[doc(hidden)]
    #[must_use = "the bytes copied belong to the reading caller's gather count"]
    pub fn insert_blob(
        &self,
        vlog_id: crate::TreeId,
        vhandle: &crate::vlog::ValueHandle,
        user_key: &[u8],
        value: UserValue,
    ) -> usize {
        self.data.insert(
            (TAG_BLOB, vlog_id, vhandle.blob_file_id, vhandle.offset).into(),
            Item::Blob(user_key.into(), vhandle.on_disk_size, value),
        );
        user_key.len()
    }

    /// Whether a separated value is already cached.
    ///
    /// For a caller deciding whether it has to read, not what the value is:
    /// [`get_blob`](Self::get_blob) would clone the value out (an atomic bump
    /// on the refcounted payload) and count a cache hit for a read that never
    /// happens, skewing the eviction policy toward entries nobody consumed.
    #[doc(hidden)]
    #[must_use]
    pub fn contains_blob(
        &self,
        vlog_id: crate::TreeId,
        vhandle: &crate::vlog::ValueHandle,
    ) -> bool {
        let key: CacheKey = (TAG_BLOB, vlog_id, vhandle.blob_file_id, vhandle.offset).into();
        self.data.contains(&key)
    }

    /// The cached value for `vhandle`, but only if it belongs to that exact
    /// handle: same `user_key` and same declared on-disk size.
    ///
    /// The cache key is a POSITION in a blob file, and a corrupt index entry
    /// can name that position with a different key, or with the right key and
    /// the wrong size. The reader rejects both on a direct read, comparing what
    /// it was asked for against the record header; these comparisons are how
    /// the cached path reaches the same verdict instead of serving whatever
    /// sits at the offset. A mismatch reads as a miss, so the caller does the
    /// real read and gets the real error.
    #[doc(hidden)]
    #[must_use]
    pub fn get_blob(
        &self,
        vlog_id: crate::TreeId,
        vhandle: &crate::vlog::ValueHandle,
        user_key: &[u8],
    ) -> Option<UserValue> {
        let key: CacheKey = (TAG_BLOB, vlog_id, vhandle.blob_file_id, vhandle.offset).into();

        match self.data.get(&key)? {
            Item::Blob(cached_key, cached_size, blob)
                if &*cached_key == user_key && cached_size == vhandle.on_disk_size =>
            {
                Some(blob)
            }
            Item::Blob(..) => None,
            Item::Block(_) | Item::Row(_) => unreachable!("invalid cache item"),
            #[cfg(feature = "zstd")]
            Item::PartialBlock(_) => unreachable!("invalid cache item"),
            #[cfg(feature = "columnar")]
            Item::Directory(..) => unreachable!("invalid cache item"),
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod coalesce_tests;
