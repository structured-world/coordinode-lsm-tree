// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::GlobalTableId;
#[cfg(feature = "zstd")]
use crate::UserKey;
use crate::sharded_cache::{Priority, ShardedCache, Weighter};
use crate::table::block::{BlockType, Header};
use crate::table::{Block, BlockOffset};
use crate::value::InternalValue;

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
/// Blob-record tag: one value of a packed block, read and decoded alone, keyed
/// by its frame and its record's offset in it.
const TAG_BLOB_RECORD: u8 = 5;

#[derive(Clone)]
enum Item {
    Block(Block),
    /// A blob file frame, checked and decoded, under its file and frame number.
    ///
    /// The frame lists the key of every value it holds, so a reader that finds
    /// it compares the key it was asked for against the one at its index, as a
    /// direct read does, rather than trusting the position.
    BlobFrame(alloc::sync::Arc<crate::vlog::blob_file::frame::DecodedFrame>),
    /// One value of a packed block, read alone: its record with the value
    /// stored decoded (see [`crate::vlog::blob_file::packed::cached_record`]).
    /// Its key is compared against the one asked for, as for a frame.
    BlobRecord(crate::Slice),
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
pub(crate) fn block_weight(block: &crate::table::Block) -> u64 {
    (Header::header_len(block.header.block_type) as u64)
        + u64::from(block.header.uncompressed_length)
}

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<CacheKey, Item> for BlockWeighter {
    fn weight(&self, _: &CacheKey, item: &Item) -> u64 {
        use Item::{BlobFrame, Block};

        match item {
            Block(b) => block_weight(b),
            // What it decoded and its entries. The prefetch's admission budget
            // charges itself the same way, so the two agree.
            BlobFrame(frame) => frame.weight(),
            Item::BlobRecord(record) => record.len() as u64,
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
pub(crate) enum LoadIdentity {
    /// A table block: its role, on-disk length, codec (dictionary included)
    /// and whether it is encrypted.
    Block {
        block_type: BlockType,
        size: u32,
        compression: crate::CompressionType,
        encrypted: bool,
    },
    /// A blob file frame. The key a reader asks for is checked against the
    /// frame the load returns, as a cache hit checks it.
    BlobFrame,
    /// Growth of a block's partially decoded prefix. Waiters take no value:
    /// once the growth ends they look at the cached prefix again.
    #[cfg(feature = "zstd")]
    PartialGrowth,
}

/// How a read that needs a block's partial prefix grown proceeds.
#[cfg(feature = "zstd")]
pub(crate) enum GrowthClaim<'a> {
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
pub(crate) struct GrowthLead<'a>(
    #[expect(dead_code, reason = "held for its drop, which releases the waiters")] LoadLead<'a>,
);

/// How a block miss proceeds: lead the load, take a concurrent one's block,
/// or load alone.
pub(crate) enum BlockClaim<'a> {
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
pub(crate) struct BlockLead<'a>(LoadLead<'a>, Option<crate::inflight::BatchToken>);

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
pub(crate) enum DirectoryClaim<'a> {
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
pub(crate) struct DirectoryLead<'a>(LoadLead<'a>);

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

/// How a blob frame miss proceeds, as [`BlockClaim`] for a block.
pub(crate) enum BlobClaim<'a> {
    /// Read the frame, then [`BlobLead::publish`].
    Lead(BlobLead<'a>),
    /// A concurrent read of the same frame finished.
    Shared(alloc::sync::Arc<crate::vlog::blob_file::frame::DecodedFrame>),
    /// Read the frame without coordination.
    Alone,
}

/// How a miss of a packed record read alone proceeds, as [`BlobClaim`] for a
/// frame.
pub(crate) enum RecordClaim<'a> {
    /// Read the record, then [`BlobLead::hand_over`] it.
    Lead(BlobLead<'a>),
    /// A concurrent read of the same record finished: the record as the cache
    /// holds it.
    Shared(crate::Slice),
    /// Read the record without coordination.
    Alone,
}

/// The lead of a blob frame load, as [`BlockLead`] for a block.
pub(crate) struct BlobLead<'a>(LoadLead<'a>);

impl BlobLead<'_> {
    /// Hands the frame read to the waiters, caching it first when `fill` says
    /// so: a frame is cached at once, since the values beside the one read
    /// are read next and each would otherwise decode the frame again.
    pub(crate) fn publish(
        self,
        cache: &Cache,
        vlog_id: crate::TreeId,
        at: BlobFrameAt,
        frame: &alloc::sync::Arc<crate::vlog::blob_file::frame::DecodedFrame>,
        fill: bool,
    ) {
        if !fill && !self.0.has_waiters() {
            return;
        }
        let item = Item::BlobFrame(alloc::sync::Arc::clone(frame));
        if fill {
            cache.data.insert(blob_frame_key(vlog_id, at), item.clone());
        }
        self.0.publish(item);
    }

    /// Whether a read waits on this one.
    pub(crate) fn has_waiters(&self) -> bool {
        self.0.has_waiters()
    }

    /// Hands `record`, a packed record read alone as the cache holds it, to
    /// the reads waiting on it.
    pub(crate) fn hand_over(self, record: crate::Slice) {
        self.0.publish(Item::BlobRecord(record));
    }

    /// [`Self::publish`] for the frame this read decoded: shared only when
    /// the cache keeps it or a read waits on it, and otherwise handed back as
    /// the read's own, with no shared allocation made for it.
    pub(crate) fn publish_owned(
        self,
        cache: &Cache,
        vlog_id: crate::TreeId,
        at: BlobFrameAt,
        frame: crate::vlog::blob_file::frame::DecodedFrame,
        fill: bool,
    ) -> crate::vlog::accessor::HeldFrame {
        use crate::vlog::accessor::HeldFrame;
        if !fill && !self.0.has_waiters() {
            return HeldFrame::Own(frame);
        }
        let frame = alloc::sync::Arc::new(frame);
        self.publish(cache, vlog_id, at, &frame, fill);
        HeldFrame::Shared(frame)
    }
}

/// Where a blob frame lies: its file and its number in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BlobFrameAt {
    pub(crate) blob_file_id: crate::vlog::BlobFileId,
    pub(crate) frame: u32,
}

fn blob_frame_key(vlog_id: crate::TreeId, at: BlobFrameAt) -> CacheKey {
    (TAG_BLOB, vlog_id, at.blob_file_id, u64::from(at.frame)).into()
}

fn blob_record_key(vlog_id: crate::TreeId, at: BlobFrameAt, index: u16) -> CacheKey {
    let place = (u64::from(at.frame) << 16) | u64::from(index);
    (TAG_BLOB_RECORD, vlog_id, at.blob_file_id, place).into()
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

    /// Claims the lead of the load of the block at `offset` for a batched
    /// read about to read it along with others; `None` when the read reads
    /// it alone. Never waits, since the read goes on to lead more blocks: a
    /// block another read is loading is read again alone. Nor does it take
    /// one: the registry holds only loads not yet published (a lead leaves
    /// it before it settles), and a published one is in the cache the caller
    /// just missed.
    ///
    /// `lead` says whether the read may lead the load (see
    /// [`crate::inflight::batches_may_lead`]); a lead it takes counts on this
    /// thread until it is published or dropped, so the thread waits on
    /// nothing meanwhile.
    pub(crate) fn claim_batched_block(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
        identity: LoadIdentity,
        lead: bool,
    ) -> Option<BlockLead<'_>> {
        if !(lead && crate::inflight::batches_may_lead()) {
            return None;
        }
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into();
        match self.loads.claim(key, identity) {
            crate::inflight::Claim::Lead(lead) => {
                Some(BlockLead(lead, Some(crate::inflight::BatchToken::new())))
            }
            crate::inflight::Claim::Join(_) | crate::inflight::Claim::Alone => None,
        }
    }

    /// Claims the load of a row group's page at `offset` for a read about to
    /// fetch it along with the group's other pages. Waits for a compatible
    /// load another read runs only while this thread holds no lead of a
    /// batched read, so until this read leads a page of its own: from then
    /// on others may be waiting on it, and a page another read is loading is
    /// read again alone. A lead taken here counts as a batched one.
    ///
    /// `with_directory` says the load of the group's directory is
    /// coordinated, by this read or one it took the directory from: its pages
    /// are then coordinated too, whatever their own loads measured, since a
    /// page read along with the directory is never timed on its own.
    #[cfg(feature = "columnar")]
    pub(crate) fn claim_page(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
        identity: LoadIdentity,
        with_directory: bool,
    ) -> BlockClaim<'_> {
        if !crate::inflight::batches_may_lead() {
            return BlockClaim::Alone;
        }
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into();
        match self.loads.claim_as(key, identity, with_directory) {
            crate::inflight::Claim::Lead(lead) => {
                BlockClaim::Lead(BlockLead(lead, Some(crate::inflight::BatchToken::new())))
            }
            crate::inflight::Claim::Join(join) if crate::inflight::may_wait() => {
                match join.wait() {
                    Some(Item::Block(block)) => BlockClaim::Shared(block),
                    _ => BlockClaim::Alone,
                }
            }
            crate::inflight::Claim::Join(_) | crate::inflight::Claim::Alone => BlockClaim::Alone,
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

    /// The load-time estimates of every registry shard summed, for tests.
    #[cfg(test)]
    pub(crate) fn load_estimates_total(&self) -> u64 {
        self.loads.estimates_total()
    }

    /// The load-time estimate of the shard that the partial-prefix growth of
    /// the block at `offset` coordinates in, for tests.
    #[cfg(all(test, feature = "zstd"))]
    pub(crate) fn partial_growth_estimate(&self, id: GlobalTableId, offset: BlockOffset) -> u64 {
        self.loads
            .estimate(&(TAG_PARTIAL_BLOCK, id.tree_id(), id.table_id(), *offset).into())
    }

    /// Claims the cold read of the blob frame at `at`, as
    /// [`Self::claim_block`] does for a block.
    pub(crate) fn claim_blob_frame(
        &self,
        vlog_id: crate::TreeId,
        at: BlobFrameAt,
    ) -> BlobClaim<'_> {
        match self
            .loads
            .claim(blob_frame_key(vlog_id, at), LoadIdentity::BlobFrame)
        {
            crate::inflight::Claim::Lead(lead) => BlobClaim::Lead(BlobLead(lead)),
            crate::inflight::Claim::Join(join) => {
                let shared = if crate::inflight::may_wait() {
                    join.wait()
                } else {
                    join.try_take()
                };
                match shared {
                    Some(Item::BlobFrame(frame)) => BlobClaim::Shared(frame),
                    _ => BlobClaim::Alone,
                }
            }
            crate::inflight::Claim::Alone => BlobClaim::Alone,
        }
    }

    /// Claims the cold read of the packed record at `at` and `index`, as
    /// [`Self::claim_blob_frame`] does for a frame: the reads waiting on it
    /// take the record the lead read.
    pub(crate) fn claim_blob_record(
        &self,
        vlog_id: crate::TreeId,
        at: BlobFrameAt,
        index: u16,
    ) -> RecordClaim<'_> {
        match self
            .loads
            .claim(blob_record_key(vlog_id, at, index), LoadIdentity::BlobFrame)
        {
            crate::inflight::Claim::Lead(lead) => RecordClaim::Lead(BlobLead(lead)),
            crate::inflight::Claim::Join(join) => {
                let shared = if crate::inflight::may_wait() {
                    join.wait()
                } else {
                    join.try_take()
                };
                match shared {
                    Some(Item::BlobRecord(record)) => RecordClaim::Shared(record),
                    _ => RecordClaim::Alone,
                }
            }
            crate::inflight::Claim::Alone => RecordClaim::Alone,
        }
    }

    /// Records the cold read of the packed record at `at` and `index` that
    /// `timer` timed.
    pub(crate) fn record_blob_record_load(
        &self,
        vlog_id: crate::TreeId,
        at: BlobFrameAt,
        index: u16,
        timer: crate::inflight::LoadTimer,
    ) {
        self.loads
            .record_load(&blob_record_key(vlog_id, at, index), timer);
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

    /// Records a read of the blocks at `offsets` that `timer` timed, issued
    /// together: each takes an even share of it.
    #[cfg(feature = "columnar")]
    pub(crate) fn record_block_loads(
        &self,
        id: GlobalTableId,
        offsets: impl Iterator<Item = BlockOffset> + Clone,
        timer: crate::inflight::LoadTimer,
    ) {
        self.loads.record_loads(
            offsets.map(|offset| (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into()),
            timer,
        );
    }

    /// Records the growth of the partially decoded prefix of the block at
    /// `offset` that `timer` timed.
    #[cfg(feature = "zstd")]
    pub(crate) fn record_partial_growth(
        &self,
        id: GlobalTableId,
        offset: BlockOffset,
        timer: crate::inflight::LoadTimer,
    ) {
        self.loads.record_load(
            &(TAG_PARTIAL_BLOCK, id.tree_id(), id.table_id(), *offset).into(),
            timer,
        );
    }

    /// Whether a full load of the block at `offset` is in flight, which a
    /// read that would decode only a prefix of it can take instead.
    #[cfg(feature = "zstd")]
    pub(crate) fn block_load_in_flight(&self, id: GlobalTableId, offset: BlockOffset) -> bool {
        self.loads
            .in_flight(&(TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into())
    }

    /// Records a wait of a batched read that `timer` timed, which handed back
    /// `reads` block reads.
    pub(crate) fn record_batch_wait(&self, timer: crate::inflight::LoadTimer, reads: usize) {
        self.loads.record_batch_wait(timer, reads);
    }

    /// Records the cold read of the blob frame at `at` that `timer` timed.
    pub(crate) fn record_blob_load(
        &self,
        vlog_id: crate::TreeId,
        at: BlobFrameAt,
        timer: crate::inflight::LoadTimer,
    ) {
        self.loads.record_load(&blob_frame_key(vlog_id, at), timer);
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

    /// The largest payload of a block of `block_type` this cache can keep.
    #[must_use]
    pub(crate) fn largest_block(&self, block_type: BlockType) -> u64 {
        // A cache smaller than a header keeps no block at all: zero is the
        // answer, not a masked underflow.
        self.max_entry_weight()
            .saturating_sub(Header::header_len(block_type) as u64)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_block(&self, id: GlobalTableId, offset: BlockOffset) -> Option<Block> {
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into();

        Some(match self.data.get(&key)? {
            Item::Block(block) => block,
            Item::BlobFrame(_) | Item::BlobRecord(_) | Item::Row(_) => {
                unreachable!("invalid cache item")
            }
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
            Item::BlobFrame(_) | Item::BlobRecord(_) | Item::Row(_) => {
                unreachable!("invalid cache item")
            }
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

    /// Caches a blob file frame, checked and decoded, under its file and
    /// frame number.
    pub(crate) fn insert_blob_frame(
        &self,
        vlog_id: crate::TreeId,
        at: BlobFrameAt,
        frame: alloc::sync::Arc<crate::vlog::blob_file::frame::DecodedFrame>,
    ) {
        self.data
            .insert(blob_frame_key(vlog_id, at), Item::BlobFrame(frame));
    }

    /// Whether a blob file frame is already cached.
    ///
    /// For a caller deciding whether it has to read, not what the frame
    /// holds: [`get_blob_frame`](Self::get_blob_frame) counts a cache hit for
    /// a read that never happens, skewing the eviction policy toward entries
    /// nobody consumed.
    #[must_use]
    pub(crate) fn contains_blob_frame(&self, vlog_id: crate::TreeId, at: BlobFrameAt) -> bool {
        self.data.contains(&blob_frame_key(vlog_id, at))
    }

    /// The cached frame at `at`. The caller compares the key at the index it
    /// wants against the one it asked for, as a direct read does.
    #[must_use]
    pub(crate) fn get_blob_frame(
        &self,
        vlog_id: crate::TreeId,
        at: BlobFrameAt,
    ) -> Option<alloc::sync::Arc<crate::vlog::blob_file::frame::DecodedFrame>> {
        self.blob_item(&blob_frame_key(vlog_id, at))
    }

    /// Caches one value of a packed block, read alone, under its frame and
    /// its record's offset `index`: `record`, the record as the cache holds
    /// it. On probation: it stays only if read again, so a stream of values
    /// read once each cannot push table blocks out of the main queue. A fill
    /// is a hint: one that finds its shard's lock held by another insert
    /// leaves the value uncached rather than wait on the read's path.
    pub(crate) fn fill_blob_record(
        &self,
        vlog_id: crate::TreeId,
        at: BlobFrameAt,
        index: u16,
        record: crate::Slice,
    ) {
        self.data.try_insert_with_priority(
            blob_record_key(vlog_id, at, index),
            Item::BlobRecord(record),
            Priority::Probation,
        );
    }

    /// Whether the value of a packed block at `at` and `index`, just missed
    /// and `weight` once cached, goes into the cache: only into a shard that
    /// has ever filled, so caching it there replaces what it evicts instead
    /// of growing the cache; there while the shard has room for it, and
    /// otherwise only on a miss that repeats a recent one, so a value read
    /// once is not built into an entry only to be evicted unread.
    #[must_use]
    pub(crate) fn admit_blob_record(
        &self,
        vlog_id: crate::TreeId,
        at: BlobFrameAt,
        index: u16,
        weight: u64,
    ) -> bool {
        self.data
            .admit_once_filled(&blob_record_key(vlog_id, at, index), weight)
    }

    /// Whether the blob frame at `at`, just read and `weight` once cached,
    /// goes into the cache under a fill that does not cache everything, as
    /// [`Self::admit_blob_record`] decides for a packed value.
    #[must_use]
    pub(crate) fn admit_blob_frame(
        &self,
        vlog_id: crate::TreeId,
        at: BlobFrameAt,
        weight: u64,
    ) -> bool {
        self.data.admit(&blob_frame_key(vlog_id, at), weight)
    }

    /// The value of a packed block cached at `at` and `index`, as the record
    /// the cache holds. The caller compares its key, as for a frame.
    #[must_use]
    pub(crate) fn get_blob_record(
        &self,
        vlog_id: crate::TreeId,
        at: BlobFrameAt,
        index: u16,
    ) -> Option<crate::Slice> {
        match self.data.get(&blob_record_key(vlog_id, at, index))? {
            Item::BlobRecord(record) => Some(record),
            Item::BlobFrame(_) | Item::Block(_) | Item::Row(_) => {
                unreachable!("invalid cache item")
            }
            #[cfg(feature = "zstd")]
            Item::PartialBlock(_) => unreachable!("invalid cache item"),
            #[cfg(feature = "columnar")]
            Item::Directory(..) => unreachable!("invalid cache item"),
        }
    }

    fn blob_item(
        &self,
        key: &CacheKey,
    ) -> Option<alloc::sync::Arc<crate::vlog::blob_file::frame::DecodedFrame>> {
        match self.data.get(key)? {
            Item::BlobFrame(frame) => Some(frame),
            Item::BlobRecord(_) | Item::Block(_) | Item::Row(_) => {
                unreachable!("invalid cache item")
            }
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
