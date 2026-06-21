// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

mod filter;
mod index;
mod meta;
#[cfg(feature = "std")] // no-std: parallel compaction unavailable (no threads)
pub(crate) mod parallel_compressor;

pub(crate) use index::DEFAULT_SPILL_THRESHOLD;
#[cfg(feature = "std")]
pub use parallel_compressor::CompactionSpawner;
#[cfg(feature = "parallel")]
pub use parallel_compressor::RayonSpawner;

use super::{Block, BlockOffset, DataBlock, KeyedBlockHandle, filter::BloomConstructionPolicy};
use crate::{
    Checksum, CompressionType, InternalValue, TableId, UserKey, ValueType,
    checksum::{ChecksumType, ChecksummedWriter},
    coding::Encode,
    encryption::EncryptionProvider,
    fs::{FileHint, Fs, FsFile, FsOpenOptions, SyncMode},
    prefix::PrefixExtractor,
    range_tombstone::RangeTombstone,
    table::{
        BlockHandle,
        writer::{
            filter::{FilterWriter, FullFilterWriter},
            index::FullIndexWriter,
        },
    },
    time::unix_timestamp,
    vlog::BlobFileId,
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};
use index::BlockIndexWriter;

use crate::io::BufWriter;
use crate::path::PathBuf;

#[cfg(feature = "std")] // no-std: parallel compaction unavailable (no threads)
use {parallel_compressor::BlockCompressor, std::collections::VecDeque};

/// Order-independent index-handle data for a spilled block, captured before the
/// chunk is consumed so the parallel pipeline can write + register the block
/// later (after a worker has compressed it) without the original chunk in hand.
#[cfg(feature = "std")]
struct HandleMeta {
    last_key: UserKey,
    last_seqno: crate::SeqNo,
    seqno_bounds: Option<(u64, u64)>,
    item_count: usize,
    /// The block's first (min) user key, captured for the zone-map synthetic
    /// column. `Some` only when the zone-map policy is on (else no clone).
    zone_block_min: Option<UserKey>,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, core::hash::Hash)]
pub struct LinkedFile {
    pub blob_file_id: BlobFileId,
    pub bytes: u64,
    pub on_disk_bytes: u64,
    pub len: usize,
}

/// Serializes and compresses values into blocks and writes them to disk as a table.
///
/// `BlockIndexWriter` / `FilterWriter` are generic over a writer `W: Write + Seek`.
/// `Fs::open()` returns `Box<dyn FsFile>` which implements `Write + Seek`,
/// so `BufWriter<Box<dyn FsFile>>` satisfies the required trait bounds.
pub struct Writer {
    /// Filesystem backend
    fs: Arc<dyn Fs>,

    /// Table file path
    pub(crate) path: PathBuf,

    table_id: TableId,

    data_block_restart_interval: u8,
    index_block_restart_interval: u8,

    meta_partition_size: u32,

    data_block_size: u32,

    data_block_hash_ratio: f32,

    /// Compression to use for data blocks
    data_block_compression: CompressionType,

    /// Compression to use for data blocks
    index_block_compression: CompressionType,

    /// Buffer to serialize blocks into
    block_buffer: Vec<u8>,

    /// File writer
    #[expect(clippy::struct_field_names)]
    file_writer: crate::sfa::Writer<ChecksummedWriter<BufWriter<Box<dyn FsFile>>>>,

    /// Writer of index blocks
    #[expect(clippy::struct_field_names)]
    index_writer: Box<dyn BlockIndexWriter<BufWriter<Box<dyn FsFile>>>>,

    /// Writer of filter
    #[expect(clippy::struct_field_names)]
    filter_writer: Box<dyn FilterWriter<BufWriter<Box<dyn FsFile>>>>,

    /// Buffer of KVs
    chunk: Vec<InternalValue>,
    chunk_size: usize,

    pub(crate) meta: meta::Metadata,

    /// Stores the previous block position (used for creating back links)
    prev_pos: (BlockOffset, BlockOffset),

    current_key: Option<UserKey>,

    bloom_policy: BloomConstructionPolicy,

    /// Stored so `use_partitioned_filter()` can re-apply it to the new writer
    prefix_extractor: Option<Arc<dyn PrefixExtractor>>,

    /// Tracks the previously written item to detect weak tombstone/value pairs
    previous_item: Option<(UserKey, ValueType)>,

    linked_blob_files: Vec<LinkedFile>,

    /// Range tombstones to be written as a separate block
    range_tombstones: Vec<RangeTombstone>,

    /// Inner zstd-block layout per data block, accumulated in write order and
    /// serialized into the optional `block_layout` SST section at finish. Only
    /// data blocks that split into >= 2 inner zstd blocks contribute an entry
    /// `(block_offset, cumulative_decompressed_ends)`; small single-inner-block
    /// blocks (the default) contribute nothing, so the section is absent for a
    /// table with no large multi-inner-block data blocks. Powers range-query
    /// partial decode.
    block_layouts: Vec<(BlockOffset, Vec<u32>)>,

    /// Per-data-block `(offset, (seqno_min, seqno_max))`, accumulated in write
    /// order when `use_seqno_in_index` is on and serialized into the optional
    /// `seqno_bounds` SST section at finish. The bounds live here (a parallel
    /// section) rather than inline in the index entries, so a point read's index
    /// walk never pays for them; only a seqno-scoped scan loads the section.
    /// Empty when `use_seqno_in_index` is off (section absent, zero extra bytes).
    seqno_bounds_section: Vec<(BlockOffset, (u64, u64))>,

    /// Per-data-block zone-map stats, accumulated in write order when
    /// `use_zone_map` is on and serialized into the optional `zone_map` SST
    /// section at finish. Kept parallel to the index (like
    /// [`Self::seqno_bounds_section`]) so a point read never loads it. Empty when
    /// `use_zone_map` is off (section absent, zero extra bytes).
    zone_map_section: Vec<(BlockOffset, Vec<crate::table::zone_map::ColumnStats>)>,

    /// Resolved retrieval-ribbon locator settings for this table, or `None`
    /// (default) when the level's [`crate::config::LocatorPolicy`] is disabled.
    /// `Some` makes the writer accumulate a per-key locator and emit the
    /// optional `locator` section at finish. Wired via [`Self::use_locator`].
    locator: Option<crate::table::locator::LocatorSpec>,

    /// Accumulated `(key_hash, block_id, slot)` triples, one per unique user key
    /// (the newest version's position), captured at the global new-key boundary
    /// in [`Self::write`]. `block_id` is the data block's ordinal; `slot` is the
    /// restart index (or exact entry index for `Entry` precision) of the key's
    /// newest version within that block. Empty unless `locator` is `Some`;
    /// packed and built into the `locator` section at finish.
    locators: Vec<(u64, u64, u64)>,

    /// Ordinal of the data block currently being filled (0-based, in write
    /// order). Incremented once per spilled block, so it matches the block's
    /// eventual registration ordinal in both the serial and parallel paths.
    /// Stamped into each accumulated locator's `block_id`.
    locator_block_id: u64,

    initial_level: u8,

    /// Block encryption provider (if encryption at rest is enabled)
    encryption: Option<Arc<dyn EncryptionProvider>>,

    /// Per-block Page ECC scheme. `Some(params)` makes every
    /// `Block::write_into` call this writer makes upgrade its
    /// `BlockTransform` to the matching `*Ecc` variant carrying that
    /// scheme (so the writer emits a parity trailer sized by the scheme
    /// and sets the `ECC_PARITY` flag). `None` (default) = no parity.
    /// Caller resolves the `page_ecc` flag and `ecc_scheme` into this
    /// via [`Self::use_ecc`] before the first key is added.
    ecc: Option<crate::table::block::EccParams>,

    /// Durability level for the SST file + folder fsync at finish. Default
    /// [`SyncMode::Normal`]; caller wires `Config::sync_mode` via
    /// [`Self::use_sync_mode`].
    sync_mode: SyncMode,

    /// Per-KV checksum policy + algorithm for data blocks. `None` (default)
    /// means no per-KV checksums: data blocks carry no per-KV footer, with
    /// the `KV_CHECKSUM_FOOTER` header flag clear.
    /// When `Some((policy, algo))`, each data block whose `(level,
    /// table_id)` satisfies `policy.applies` is emitted with a per-entry
    /// checksum footer under `algo` and the `KV_CHECKSUM_FOOTER` flag set
    /// (the block role stays [`BlockType::Data`](crate::table::block::BlockType::Data)). Wired from the tree's
    /// runtime `kv_checksums` config via [`Self::use_kv_checksums`].
    kv_checksum: Option<(
        crate::runtime_config::KvChecksumPolicy,
        crate::runtime_config::ChecksumAlgorithm,
    )>,

    /// Per-block seqno bounds opt-in (the `seqno_in_index` runtime config).
    /// When `true`, the writer accumulates each data block's `(seqno_min,
    /// seqno_max)` and emits them in the optional parallel `seqno_bounds`
    /// SST section, letting a seqno-scoped scan skip blocks that cannot
    /// overlap the target window without reading them. The index entries
    /// stay byte-identical to the off-mode layout, so a point read pays
    /// nothing. Default `false` (no section emitted). Caller wires the live
    /// runtime config into this field via [`Self::use_seqno_in_index`]
    /// before the first key is added.
    use_seqno_in_index: bool,

    /// Zone-map opt-in (the zone-map policy). When `true`, the writer records
    /// each data block's per-column stats (for row blocks: a single synthetic
    /// column with the block's key min / max + row count) and emits them in the
    /// optional parallel `zone_map` SST section, letting a predicate scan skip
    /// blocks that cannot match without reading them. Default `false` (no section
    /// emitted). Caller wires the live runtime config in via
    /// [`Self::use_zone_map`] before the first key is added.
    use_zone_map: bool,

    /// Columnar opt-in. When `true`, each spilled data block stores its entries
    /// column-organized (a PAX row-group of the intrinsic fields) instead of
    /// row-major, tagged [`BlockType::Columnar`](crate::table::block::BlockType::Columnar)
    /// so the reader reconstructs the exact entries. Default `false` (row-major).
    /// Caller wires the live runtime config in via [`Self::use_columnar`] before
    /// the first key is added.
    use_columnar: bool,

    /// Pre-trained zstd dictionary for dictionary compression
    #[cfg(zstd_any)]
    zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,

    /// Optional executor for parallel block compression. `None` (default) =
    /// serial path: each block is compressed and written inline. `Some` =
    /// blocks are compressed on worker threads while writes stay ordered here.
    /// Wired from the tree's compaction pool via [`Self::use_parallel_compression`].
    #[cfg(feature = "std")]
    spawner: Option<Arc<dyn CompactionSpawner>>,

    /// Lazily built on the first spill once all transform params are finalized,
    /// so builder-call order doesn't matter. Only `Some` when `spawner` is set.
    #[cfg(feature = "std")]
    parallel: Option<BlockCompressor>,

    /// Max blocks in flight (submitted but not yet drained) before the writer
    /// drains one to bound buffered-output memory. Derived from thread count.
    #[cfg(feature = "std")]
    parallel_cap: usize,

    /// Per-block index-handle data queued in submission order, popped as each
    /// prepared block is drained and written. Length tracks the pipeline's
    /// `pending` count.
    #[cfg(feature = "std")]
    pending_meta: VecDeque<HandleMeta>,

    /// Uncompressed bytes of submitted-but-not-yet-drained blocks. The parallel
    /// path writes blocks lazily, so `meta.file_pos` lags; this keeps a
    /// synchronous size proxy for size-based table rotation (see
    /// [`Self::output_size_hint`]). Added on submit, subtracted on drain.
    #[cfg(feature = "std")]
    parallel_pending_bytes: u64,
}

impl Writer {
    pub fn new(
        path: PathBuf,
        table_id: TableId,
        initial_level: u8,
        fs: Arc<dyn Fs>,
    ) -> crate::Result<Self> {
        // Normalize path once so open(), remove_file(), and fsync_directory()
        // all see the same absolute path.
        // no-std: caller must pass an already-absolute path (no cwd concept)
        #[cfg(feature = "std")]
        let path = std::path::absolute(path)?;

        let file = fs.open(&path, &FsOpenOptions::new().write(true).create_new(true))?;
        let writer = BufWriter::with_capacity(u16::MAX.into(), file);
        let writer = ChecksummedWriter::new(writer);
        let mut writer = crate::sfa::Writer::from_writer(writer);
        writer.start("data")?;

        Ok(Self {
            fs,
            initial_level,

            meta: meta::Metadata::default(),

            table_id,

            data_block_restart_interval: 16,
            index_block_restart_interval: 1,

            data_block_hash_ratio: 0.0,

            meta_partition_size: 4_096,

            data_block_size: 4_096,

            data_block_compression: CompressionType::None,
            index_block_compression: CompressionType::None,

            path,

            // Promote to trait object via the use_table_id call —
            // the field type forces the W coercion, no inline cast
            // needed (mirrors the use_partitioned_* pattern below).
            // The trait method returns Box<dyn …<W>> with W inferred
            // from the assignment context.
            index_writer: Box::new(FullIndexWriter::new()).use_table_id(table_id),
            filter_writer: Box::new(FullFilterWriter::new(BloomConstructionPolicy::default()))
                .use_table_id(table_id),

            block_layouts: Vec::new(),
            seqno_bounds_section: Vec::new(),
            zone_map_section: Vec::new(),

            locator: None,
            locators: Vec::new(),
            locator_block_id: 0,

            block_buffer: Vec::new(),
            file_writer: writer,
            chunk: Vec::new(),

            prev_pos: (BlockOffset(0), BlockOffset(0)),

            chunk_size: 0,

            current_key: None,

            bloom_policy: BloomConstructionPolicy::default(),

            prefix_extractor: None,

            previous_item: None,

            linked_blob_files: Vec::new(),
            range_tombstones: Vec::new(),

            encryption: None,

            ecc: None,
            sync_mode: SyncMode::Normal,

            kv_checksum: None,
            use_seqno_in_index: false,
            use_zone_map: false,
            use_columnar: false,

            #[cfg(zstd_any)]
            zstd_dictionary: None,

            #[cfg(feature = "std")]
            spawner: None,
            #[cfg(feature = "std")]
            parallel: None,
            #[cfg(feature = "std")]
            parallel_cap: 0,
            #[cfg(feature = "std")]
            pending_meta: VecDeque::new(),
            #[cfg(feature = "std")]
            parallel_pending_bytes: 0,
        })
    }

    /// Current output-size estimate for table-rotation decisions: the bytes
    /// already on disk plus the uncompressed size of blocks still in flight on
    /// the parallel pipeline (whose on-disk size isn't known until written).
    /// Equals `meta.file_pos` exactly on the serial path.
    pub(crate) fn output_size_hint(&self) -> u64 {
        #[cfg(feature = "std")]
        {
            *self.meta.file_pos + self.parallel_pending_bytes
        }
        #[cfg(not(feature = "std"))]
        {
            *self.meta.file_pos
        }
    }

    /// Enables parallel block compression on this writer using `spawner` to run
    /// per-block transform work on worker threads. `threads` sizes the in-flight
    /// cap (`2 * threads`), bounding buffered-output memory. The compressor is
    /// built lazily on the first spill, so this may be called in any builder
    /// order. Default (not called) keeps the serial path.
    #[cfg(feature = "std")]
    #[must_use]
    pub(crate) fn use_parallel_compression(
        mut self,
        spawner: Arc<dyn CompactionSpawner>,
        threads: usize,
    ) -> Self {
        self.spawner = Some(spawner);
        self.parallel_cap = (threads * 2).max(1);
        self
    }

    pub fn link_blob_file(
        &mut self,
        blob_file_id: BlobFileId,
        len: usize,
        bytes: u64,
        on_disk_bytes: u64,
    ) {
        self.linked_blob_files.push(LinkedFile {
            blob_file_id,
            bytes,
            on_disk_bytes,
            len,
        });
    }

    fn assert_not_started(&self, setting: &str) {
        // A parallel spill clears `chunk` and submits the block to the
        // pipeline immediately, but `data_block_count` only ticks once
        // `drain_one_parallel` registers it — so neither alone proves "no
        // block is in flight". Also check the pipeline's pending queue /
        // byte counter so a setter can't change the per-table layout after
        // a block was already queued under the old one.
        #[cfg(feature = "std")]
        let in_flight = !self.pending_meta.is_empty() || self.parallel_pending_bytes > 0;
        #[cfg(not(feature = "std"))]
        let in_flight = false;
        assert!(
            self.meta.data_block_count == 0 && self.chunk.is_empty() && !in_flight,
            "{setting} must be configured before writing starts",
        );
    }

    #[must_use]
    pub fn use_partitioned_filter(mut self) -> Self {
        self.assert_not_started("partitioned filter");
        self.filter_writer = Box::new(filter::PartitionedFilterWriter::new(self.bloom_policy))
            .use_tli_compression(self.index_block_compression)
            .use_partition_size(self.meta_partition_size)
            .set_prefix_extractor(self.prefix_extractor.clone())
            .use_encryption(self.encryption.clone())
            .use_table_id(self.table_id);
        self
    }

    #[must_use]
    pub fn use_partitioned_index(mut self) -> Self {
        self.assert_not_started("partitioned index");
        self.index_writer = Box::new(index::PartitionedIndexWriter::new())
            .use_compression(self.index_block_compression)
            .use_partition_size(self.meta_partition_size)
            .use_restart_interval(self.index_block_restart_interval)
            .use_encryption(self.encryption.clone())
            .use_table_id(self.table_id)
            // Reapply page_ecc — swapping the index writer would otherwise drop
            // a flag set earlier in the builder chain (order-independence).
            .use_ecc(self.ecc);
        self
    }

    /// Size-adaptive index: single-level until the index exceeds
    /// `spill_threshold` bytes, then a streaming two-level (partitioned)
    /// index. See [`index::AdaptiveIndexWriter`]. The per-bottom-partition
    /// size (once spilled) is `meta_partition_size`, matching
    /// [`Self::use_partitioned_index`].
    #[must_use]
    pub fn use_adaptive_index(mut self, spill_threshold: u64) -> Self {
        self.assert_not_started("adaptive index");
        self.index_writer = Box::new(index::AdaptiveIndexWriter::new(spill_threshold))
            .use_compression(self.index_block_compression)
            .use_partition_size(self.meta_partition_size)
            .use_restart_interval(self.index_block_restart_interval)
            .use_encryption(self.encryption.clone())
            .use_table_id(self.table_id)
            // Swapping in a fresh index writer drops any flag set earlier in the
            // builder chain; reapply page_ecc so it survives regardless of call
            // order (otherwise the table mixes ECC and non-ECC index blocks
            // while `Writer` still records ECC as enabled).
            .use_ecc(self.ecc);
        self
    }

    #[must_use]
    pub fn use_data_block_restart_interval(mut self, interval: u8) -> Self {
        assert!(
            interval > 0,
            "data block restart interval must be greater than zero",
        );
        self.assert_not_started("data block restart interval");
        self.data_block_restart_interval = interval;
        self
    }

    #[must_use]
    pub fn use_index_block_restart_interval(mut self, interval: u8) -> Self {
        assert!(
            interval > 0,
            "index block restart interval must be greater than zero",
        );
        self.assert_not_started("index block restart interval");
        self.index_block_restart_interval = interval;
        self.index_writer = self.index_writer.use_restart_interval(interval);
        self
    }

    #[must_use]
    pub fn use_data_block_hash_ratio(mut self, ratio: f32) -> Self {
        self.data_block_hash_ratio = ratio;
        self
    }

    #[must_use]
    pub fn use_data_block_size(mut self, size: u32) -> Self {
        assert!(
            size <= 4 * 1_024 * 1_024,
            "data block size must be <= 4 MiB",
        );
        self.data_block_size = size;
        self
    }

    #[must_use]
    pub fn use_meta_partition_size(mut self, size: u32) -> Self {
        assert!(
            size <= 4 * 1_024 * 1_024,
            "data block size must be <= 4 MiB",
        );
        self.meta_partition_size = size;
        self.index_writer = self.index_writer.use_partition_size(size);
        self.filter_writer = self.filter_writer.use_partition_size(size);
        self
    }

    #[must_use]
    pub fn use_data_block_compression(mut self, compression: CompressionType) -> Self {
        self.data_block_compression = compression;
        self
    }

    #[must_use]
    pub fn use_index_block_compression(mut self, compression: CompressionType) -> Self {
        // ZstdDict is not useful for index/filter blocks (dictionaries are trained
        // on data block content). Downgrade to plain Zstd to avoid ZstdDictMismatch
        // errors on read — index readers never carry a dictionary.
        #[cfg(zstd_any)]
        let compression = match compression {
            CompressionType::ZstdDict { level, .. } => CompressionType::Zstd(level),
            other => other,
        };

        self.index_block_compression = compression;
        self.index_writer = self.index_writer.use_compression(compression);
        self.filter_writer = self.filter_writer.use_tli_compression(compression);
        self
    }

    /// Sets the encryption provider for block-level encryption at rest.
    ///
    /// When set, all blocks (data, index, filter, meta) are encrypted after
    /// compression and before checksumming.
    #[must_use]
    pub fn use_encryption(mut self, encryption: Option<Arc<dyn EncryptionProvider>>) -> Self {
        self.index_writer = self.index_writer.use_encryption(encryption.clone());
        self.filter_writer = self.filter_writer.use_encryption(encryption.clone());
        self.encryption = encryption;
        self
    }

    /// Sets the zstd dictionary for dictionary compression of data blocks.
    #[cfg(zstd_any)]
    #[must_use]
    pub fn use_zstd_dictionary(
        mut self,
        dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
    ) -> Self {
        self.zstd_dictionary = dictionary;
        self
    }

    /// Wires the tree's `Config::page_ecc` flag into this writer.
    /// When `true`, every block this writer emits upgrades its
    /// `BlockTransform` to the matching `*Ecc` variant (so the
    /// `Block::write_into` call emits a Reed-Solomon parity
    /// trailer and sets the `ECC_PARITY` flag in the header).
    /// Must be called BEFORE the first key is added so all blocks
    /// in the table use the same setting; the contract is
    /// enforced by callers (`Tree::open` + compaction worker pass
    /// the config once at writer construction).
    ///
    /// No-op on builds without the `page_ecc` cargo feature —
    /// `BlockTransform::with_ecc` becomes the identity function
    /// in that build and the flag is dead.
    #[must_use]
    pub fn use_ecc(mut self, ecc: Option<crate::table::block::EccParams>) -> Self {
        // The ECC scheme is recorded once per-SST in the final descriptor,
        // so every block in the table must be written under the same layout.
        // Toggling it mid-write would stamp blocks with mixed layouts under a
        // single descriptor; enforce the "configure before first key" contract
        // (shard counts are already non-zero by `EccParams` construction).
        self.assert_not_started("page ecc");
        self.ecc = ecc;
        self.index_writer = self.index_writer.use_ecc(ecc);
        self.filter_writer = self.filter_writer.use_ecc(ecc);
        self
    }

    /// Convenience wiring: resolves the tree's `Config::page_ecc` flag +
    /// the configured `EccScheme` into the per-block [`EccParams`] and
    /// applies it via [`Self::use_ecc`]. `page_ecc == false` (or a
    /// non-shard scheme such as `Secded`, which is gated until #255)
    /// yields no parity. See [`resolve_ecc`].
    #[must_use]
    pub fn use_page_ecc(self, page_ecc: bool, scheme: crate::runtime_config::EccScheme) -> Self {
        self.use_ecc(resolve_ecc(page_ecc, scheme))
    }

    /// Wires the tree's `Config::sync_mode` into this writer's final SST +
    /// folder fsync.
    #[must_use]
    pub fn use_sync_mode(mut self, sync_mode: SyncMode) -> Self {
        self.sync_mode = sync_mode;
        self
    }

    /// Wires the tree's runtime `kv_checksums` policy + algorithm into this
    /// writer. When `policy != Off`, every data block whose
    /// `(level, table_id)` satisfies `policy.applies` gets a per-entry
    /// checksum footer and the `KV_CHECKSUM_FOOTER` header flag set; all
    /// other data blocks stay plain (flag clear). `Off` (or never calling
    /// this) leaves data blocks without a per-KV footer (flag clear).
    ///
    /// Must be called BEFORE the first key is added (same contract as
    /// [`Self::use_page_ecc`]).
    #[must_use]
    pub fn use_kv_checksums(
        mut self,
        policy: crate::runtime_config::KvChecksumPolicy,
        algo: crate::runtime_config::ChecksumAlgorithm,
    ) -> Self {
        // Must be fixed before the first key: toggling mid-write would mix
        // footer-bearing and plain data blocks in one table at an arbitrary
        // boundary, which the per-SST policy contract does not allow.
        self.assert_not_started("use_kv_checksums");
        self.kv_checksum = if matches!(policy, crate::runtime_config::KvChecksumPolicy::Off) {
            None
        } else {
            Some((policy, algo))
        };
        self
    }

    /// Enables the optional per-block seqno-bounds section (the
    /// `seqno_in_index` runtime config). When `true`, the writer records each
    /// data block's `(seqno_min, seqno_max)` and emits the parallel
    /// `seqno_bounds` section at `finish()`, powering the `scan_since_seqno`
    /// block-skip. Must be called BEFORE the first key is added so the section
    /// covers every block; callers (`Tree` flush + compaction worker) pass the
    /// live config once at writer construction. Default `false` (no section).
    #[must_use]
    pub fn use_seqno_in_index(mut self, seqno_in_index: bool) -> Self {
        // Must be fixed before the first key: the section must cover every
        // block, so the flag is snapshotted per block in `spill_block` and a
        // mid-write toggle would leave some blocks unrecorded. Same contract
        // as `use_page_ecc` / `use_kv_checksums`.
        self.assert_not_started("use_seqno_in_index");
        self.use_seqno_in_index = seqno_in_index;
        self
    }

    /// Enables the zone-map section: the writer records each data block's stats
    /// (for row blocks, a synthetic column with the block's key min / max + row
    /// count) and emits the optional `zone_map` section at finish, for
    /// predicate-based block-skip. Must be fixed before the first key (the
    /// section must cover every block); a mid-write toggle would leave some
    /// blocks unrecorded. Default off.
    #[must_use]
    pub fn use_zone_map(mut self, zone_map: bool) -> Self {
        self.assert_not_started("use_zone_map");
        self.use_zone_map = zone_map;
        self
    }

    /// Enables column-organized data blocks (see [`Self::use_columnar`] field).
    /// Must be set before the first key is written.
    #[must_use]
    pub fn use_columnar(mut self, columnar: bool) -> Self {
        self.assert_not_started("use_columnar");
        self.use_columnar = columnar;
        self
    }

    /// Wires the resolved per-level retrieval-ribbon locator policy entry.
    ///
    /// `Enabled` makes the writer accumulate a per-key `(block_id, slot)`
    /// locator and emit the optional `locator` section at finish; `None` leaves
    /// the writer producing byte-identical SSTs (no section, no padding). Must
    /// be set before the first key, like the other format-affecting `use_*`
    /// toggles, since it changes which sections the table emits.
    #[must_use]
    pub fn use_locator(mut self, entry: crate::config::LocatorPolicyEntry) -> Self {
        self.assert_not_started("use_locator");
        self.locator = match entry {
            crate::config::LocatorPolicyEntry::None => None,
            crate::config::LocatorPolicyEntry::Enabled {
                precision,
                block_id_bits,
                slot_bits,
            } => Some(crate::table::locator::LocatorSpec {
                precision,
                block_id_bits,
                slot_bits,
            }),
        };
        self
    }

    /// When `enabled`, clears per-file copy-on-write on this table's (still
    /// empty) file via [`crate::fs::Fs::try_disable_cow`], so a write-once SST
    /// on a copy-on-write filesystem (Btrfs) avoids the fragmentation penalty.
    ///
    /// Called at construction, before any block is written, because the inode
    /// flag only takes effect on a file with no data blocks yet. Best-effort:
    /// a failure (or a filesystem that does not support the flag) is logged and
    /// ignored: disabling copy-on-write is a throughput optimization, never a
    /// correctness requirement, so it must not fail SST creation.
    #[must_use]
    pub fn use_disable_cow(self, enabled: bool) -> Self {
        if enabled && let Err(e) = self.fs.try_disable_cow(&self.path) {
            log::warn!(
                "try_disable_cow({}) failed; continuing with CoW enabled: {e}",
                self.path.display(),
            );
        }
        self
    }

    #[must_use]
    pub fn use_bloom_policy(mut self, bloom_policy: BloomConstructionPolicy) -> Self {
        self.bloom_policy = bloom_policy;
        self.filter_writer = self.filter_writer.set_filter_policy(bloom_policy);
        self
    }

    #[must_use]
    pub fn use_prefix_extractor(mut self, extractor: Option<Arc<dyn PrefixExtractor>>) -> Self {
        self.prefix_extractor.clone_from(&extractor);
        self.filter_writer = self.filter_writer.set_prefix_extractor(extractor);
        self
    }

    /// Adds a range tombstone to be written into this table's RT block.
    pub(crate) fn write_range_tombstone(&mut self, rt: RangeTombstone) {
        self.meta.lowest_seqno = self.meta.lowest_seqno.min(rt.seqno);
        self.meta.highest_seqno = self.meta.highest_seqno.max(rt.seqno);
        self.range_tombstones.push(rt);
    }

    /// Writes an item.
    ///
    /// # Note
    ///
    /// It's important that the incoming stream of items is correctly
    /// sorted as described by the [`UserKey`], otherwise the block layout will
    /// be non-sense.
    pub fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        let value_type = item.key.value_type;
        let seqno = item.key.seqno;
        let user_key = item.key.user_key.clone();
        let value_len = item.value.len();

        // Per-entry shape accounting (pairs with item_count: counts every
        // version). usize -> u64 is a widening cast on every supported target.
        self.meta.sum_user_key_bytes += user_key.len() as u64;
        self.meta.sum_value_bytes += value_len as u64;

        if item.is_tombstone() {
            self.meta.tombstone_count += 1;
        }

        if value_type == ValueType::WeakTombstone {
            self.meta.weak_tombstone_count += 1;
        }

        if value_type == ValueType::Value
            && let Some((prev_key, prev_type)) = &self.previous_item
            && prev_type == &ValueType::WeakTombstone
            && prev_key.as_ref() == user_key.as_ref()
        {
            self.meta.weak_tombstone_reclaimable_count += 1;
        }

        // NOTE: Check if we visit a new key
        if Some(&user_key) != self.current_key.as_ref() {
            self.meta.key_count += 1;
            self.current_key = Some(user_key.clone());

            // IMPORTANT: Do not buffer *every* item's key
            // because there may be multiple versions
            // of the same key

            if self.bloom_policy.is_active() {
                self.filter_writer.register_key(&user_key)?;
            }

            // Retrieval-ribbon locator: record this key's newest version (its
            // first occurrence in scan order) at its position in the forming
            // data block. `chunk.len()` is the item index this key will take
            // (it is pushed below at the same index); `locator_block_id` is the
            // current block's ordinal. `slot` is the restart index the encoder
            // will assign (`item_index / restart_interval`) for `Restart`, or
            // the exact item index for `Entry`.
            if let Some(spec) = self.locator {
                let pos = self.chunk.len() as u64;
                let slot = match spec.precision {
                    // Per-block precision drops slot (the section build masks it
                    // to 0-width); record 0 to keep max_slot minimal.
                    crate::config::LocatorPrecision::Block => 0,
                    crate::config::LocatorPrecision::Restart => {
                        pos / u64::from(self.data_block_restart_interval)
                    }
                    crate::config::LocatorPrecision::Entry => pos,
                };
                self.locators
                    .push((crate::hash::hash64(&user_key), self.locator_block_id, slot));
            }
        }

        if self.meta.first_key.is_none() {
            self.meta.first_key = Some(user_key.clone());
        }

        self.chunk_size += user_key.len() + value_len;
        self.chunk.push(item);
        self.previous_item = Some((user_key, value_type));

        if self.chunk_size >= self.data_block_size as usize {
            self.spill_block()?;
        }

        self.meta.lowest_seqno = self.meta.lowest_seqno.min(seqno);
        self.meta.highest_seqno = self.meta.highest_seqno.max(seqno);
        // highest_kv_seqno tracks the highest seqno among user KV entries
        // written via write() (values, point tombstones, weak tombstones).
        // Range tombstones (via write_range_tombstone) are excluded. In
        // RT-only tables, finish() writes a synthetic sentinel via write()
        // but restores highest_kv_seqno afterwards, so this bound reflects
        // only actual user KV items.
        self.meta.highest_kv_seqno = self.meta.highest_kv_seqno.max(seqno);

        Ok(())
    }

    /// Writes a compressed block to disk.
    ///
    /// This is triggered when a `Writer::write` causes the buffer to grow to the configured `block_size`.
    ///
    /// Should only be called when the block has items in it.
    pub(crate) fn spill_block(&mut self) -> crate::Result<()> {
        let Some(last) = self.chunk.last() else {
            return Ok(());
        };

        // Advance the locator block ordinal: this spill flushes the block whose
        // keys were recorded with the current `locator_block_id`, so the next
        // block's keys belong to the next ordinal. Done here (not per write
        // path) so it stays correct for both the serial and parallel spills.
        // Gated on the feature so an unenabled writer pays nothing.
        if self.locator.is_some() {
            self.locator_block_id += 1;
        }

        // Order-independent index-handle data, captured before the chunk is
        // consumed. The parallel path needs it owned because the chunk is
        // cleared at submit time, before a worker has produced the block.
        let last_key = last.key.user_key.clone();
        let last_seqno = last.key.seqno;
        let item_count = self.chunk.len();
        // Per-block seqno bounds let a seqno-scoped scan skip this whole block
        // when its `max` is below the target: one fold over entries in hand.
        let seqno_bounds = self.use_seqno_in_index.then(|| {
            self.chunk
                .iter()
                .fold((u64::MAX, u64::MIN), |(min, max), e| {
                    (min.min(e.key.seqno), max.max(e.key.seqno))
                })
        });
        // Block's first (min) key for the zone-map synthetic column; cloned only
        // when the policy is on. The chunk is non-empty here (early return
        // above), so `first()` is `Some`.
        let zone_block_min = self
            .use_zone_map
            .then(|| self.chunk.first().map(|e| e.key.user_key.clone()))
            .flatten();

        // Columnar layout: transpose the chunk into a PAX block rather than a
        // row-major data block. Serial path only for now (the parallel pipeline
        // assumes a Data block); columnar is opt-in.
        #[cfg(feature = "columnar")]
        if self.use_columnar {
            return self.spill_columnar_block(
                last_key,
                last_seqno,
                seqno_bounds,
                item_count,
                zone_block_min,
            );
        }

        // Decide per-block whether to emit the per-KV checksum footer.
        // `kv_checksum` is None unless the tree opted in; even then only blocks
        // whose (level, table_id) satisfy the policy carry the footer. A
        // per-KV-checked block is a plain Data block plus a footer (role stays
        // Data; the footer is a header flag bit, not a distinct block type).
        let kv_emit = self.kv_checksum.and_then(|(policy, algo)| {
            policy
                .applies(self.initial_level, self.table_id)
                .then_some(algo)
        });

        // Parallel path: encode into a fresh owned buffer (the reusable
        // block_buffer can't serve here — it would be in flight on a worker),
        // submit it, and let an ordered drain write it later.
        #[cfg(feature = "std")]
        if self.spawner.is_some() {
            self.ensure_parallel();
            let mut encoded = Vec::new();
            let kv_flags = Self::encode_chunk_into(
                &self.chunk,
                self.data_block_restart_interval,
                self.data_block_hash_ratio,
                kv_emit,
                &mut encoded,
            )?;
            // Backpressure: bound buffered output by draining before the cap.
            while self.parallel.as_ref().map_or(0, BlockCompressor::pending) >= self.parallel_cap {
                self.drain_one_parallel()?;
            }
            self.pending_meta.push_back(HandleMeta {
                last_key,
                last_seqno,
                seqno_bounds,
                item_count,
                zone_block_min,
            });
            // Track in-flight uncompressed bytes for the rotation size hint
            // (file_pos only advances once the block is drained and written).
            self.parallel_pending_bytes += encoded.len() as u64;
            if let Some(par) = self.parallel.as_mut() {
                par.submit(encoded, kv_flags);
            }
            self.chunk.clear();
            self.chunk_size = 0;
            return Ok(());
        }

        // Serial path: encode into the reusable buffer, prepare + write inline.
        self.block_buffer.clear();
        let kv_flags = Self::encode_chunk_into(
            &self.chunk,
            self.data_block_restart_interval,
            self.data_block_hash_ratio,
            kv_emit,
            &mut self.block_buffer,
        )?;

        // Prepare then write in two steps (instead of `write_into_with_flags`)
        // so the inner-block layout can be taken from the prepared block before
        // `write_to` consumes it.
        let transform = {
            let t = crate::table::block::BlockTransform::from_parts(
                self.data_block_compression,
                self.encryption.as_deref(),
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
            )?;
            if let Some(ecc) = self.ecc {
                t.with_ecc(ecc)
            } else {
                t
            }
        };
        let mut prepared = Block::prepare_with_flags(
            &self.block_buffer,
            super::block::BlockIdentity {
                table_id: self.table_id,
                block_type: super::block::BlockType::Data,
                dict_id: self.data_block_compression.dict_id(),
                window_log: 0,
            },
            // Data blocks use the configured codec and may carry a zstd dict;
            // encryption is optional. page_ecc upgrades the transform to its
            // matching `*Ecc` variant when the tree was opened with page_ecc.
            &transform,
            kv_flags,
        )?;
        let layout = core::mem::take(&mut prepared.layout);
        let header = prepared.write_to(&mut self.file_writer)?;

        self.register_written_block(
            header,
            layout,
            last_key,
            last_seqno,
            seqno_bounds,
            item_count,
            zone_block_min,
        )?;

        // IMPORTANT: Clear chunk after everything else
        self.chunk.clear();
        self.chunk_size = 0;

        Ok(())
    }

    /// Spills the current chunk as a columnar (PAX) block: transpose the entries
    /// into a `ColumnBatch`, encode it, and write + register it like a data block
    /// (same index handle, seqno bounds, and zone-map key range). Serial path
    /// only; the parallel pipeline keeps producing row blocks.
    #[cfg(feature = "columnar")]
    fn spill_columnar_block(
        &mut self,
        last_key: crate::UserKey,
        last_seqno: crate::SeqNo,
        seqno_bounds: Option<(u64, u64)>,
        item_count: usize,
        zone_block_min: Option<crate::UserKey>,
    ) -> crate::Result<()> {
        let batch = crate::table::columnar::entries_to_column_batch(&self.chunk)?;
        let payload = batch.encode(crate::table::columnar::CodecId::Plain)?;
        let transform = {
            let t = crate::table::block::BlockTransform::from_parts(
                self.data_block_compression,
                self.encryption.as_deref(),
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
            )?;
            if let Some(ecc) = self.ecc {
                t.with_ecc(ecc)
            } else {
                t
            }
        };
        let mut prepared = Block::prepare_with_flags(
            &payload,
            super::block::BlockIdentity {
                table_id: self.table_id,
                block_type: super::block::BlockType::Columnar,
                dict_id: self.data_block_compression.dict_id(),
                window_log: 0,
            },
            &transform,
            0, // columnar blocks carry no per-KV checksum footer
        )?;
        let layout = core::mem::take(&mut prepared.layout);
        let header = prepared.write_to(&mut self.file_writer)?;
        self.register_written_block(
            header,
            layout,
            last_key,
            last_seqno,
            seqno_bounds,
            item_count,
            zone_block_min,
        )?;
        self.chunk.clear();
        self.chunk_size = 0;
        Ok(())
    }

    /// Encodes `chunk` into `buf`, returning the `block_flags` bits the transform
    /// can't derive (the per-KV checksum-footer bit). Associated fn (no `&self`)
    /// so callers can pass disjoint field borrows (`&self.chunk`,
    /// `&mut self.block_buffer`) without aliasing.
    fn encode_chunk_into(
        chunk: &[InternalValue],
        restart_interval: u8,
        hash_ratio: f32,
        kv_emit: Option<crate::runtime_config::ChecksumAlgorithm>,
        buf: &mut Vec<u8>,
    ) -> crate::Result<u8> {
        if let Some(algo) = kv_emit {
            // One logical-content digest per entry, in scan order.
            let mut digests = Vec::with_capacity(chunk.len());
            for item in chunk {
                let d = crate::table::block::kv_checksum::kv_digest(item, algo)
                    .ok_or(crate::Error::FeatureUnsupported("kv-checksum-algorithm"))?;
                digests.push(d);
            }
            DataBlock::encode_kv_checked_into(
                buf,
                chunk,
                &digests,
                algo,
                restart_interval,
                hash_ratio,
            )?;
            Ok(crate::table::block::header::block_flags::KV_CHECKSUM_FOOTER)
        } else {
            DataBlock::encode_into(buf, chunk, restart_interval, hash_ratio)?;
            Ok(0)
        }
    }

    /// Records a just-written data block: index entry (with optional seqno
    /// bounds), metadata counters, back link, and the running last key. Single
    /// source of truth for both the serial and parallel write paths.
    #[expect(
        clippy::too_many_arguments,
        reason = "cohesive per-written-block fields; a param struct adds indirection without clarity"
    )]
    fn register_written_block(
        &mut self,
        header: crate::table::block::Header,
        layout: Vec<u32>,
        last_key: UserKey,
        last_seqno: crate::SeqNo,
        seqno_bounds: Option<(u64, u64)>,
        item_count: usize,
        zone_block_min: Option<UserKey>,
    ) -> crate::Result<()> {
        self.meta.uncompressed_size += u64::from(header.uncompressed_length);

        // Record the inner zstd-block layout keyed by this block's file offset
        // (`meta.file_pos`, captured before the increment below). Only
        // multi-inner-block data blocks carry a non-empty layout, so single
        // (default 4 KiB) blocks add nothing.
        if !layout.is_empty() {
            self.block_layouts.push((self.meta.file_pos, layout));
        }
        // Size the block-handle with the scheme this writer actually wrote
        // the parity under (NOT the fixed RS(4,2) `on_disk_size` assumes),
        // or the handle over-reads on a non-default scheme.
        let bytes_written = header.on_disk_size_with(self.ecc);

        let handle = KeyedBlockHandle::new(
            last_key.clone(),
            last_seqno,
            BlockHandle::new(self.meta.file_pos, bytes_written),
        );
        // Seqno bounds go into the parallel `seqno_bounds` section keyed by this
        // block's file offset, NOT inline in the index entry: keeping them out of
        // the index keeps point-read index probes at their legacy cost while the
        // seqno-scoped scan loads the section.
        if let Some((seqno_min, seqno_max)) = seqno_bounds {
            self.seqno_bounds_section
                .push((self.meta.file_pos, (seqno_min, seqno_max)));
        }
        // Zone-map entry for this block (parallel section keyed by file offset,
        // like seqno_bounds). A row block records one synthetic column with the
        // block's key range; the row count never approaches `u32::MAX` (a data
        // block holds at most a few thousand entries), so the cap is defensive.
        if let Some(min_key) = zone_block_min {
            let row_count = u32::try_from(item_count).unwrap_or(u32::MAX);
            let columns = alloc::vec![crate::table::zone_map::ColumnStats {
                column_id: 0,
                type_tag: 0,
                codec_id: 0,
                null_count: 0,
                row_count,
                min: min_key.to_vec(),
                max: last_key.to_vec(),
            }];
            self.zone_map_section.push((self.meta.file_pos, columns));
        }
        self.index_writer.register_data_block(handle)?;

        self.meta.file_pos += u64::from(bytes_written);
        self.meta.item_count += item_count;
        self.meta.data_block_count += 1;

        self.prev_pos.0 = self.prev_pos.1;
        self.prev_pos.1 += u64::from(bytes_written);

        self.meta.last_key = Some(last_key);
        Ok(())
    }

    /// Builds the parallel compressor on first use, capturing the now-finalized
    /// transform params. No-op once built. Only reachable when `spawner` is set.
    #[cfg(feature = "std")]
    fn ensure_parallel(&mut self) {
        if self.parallel.is_some() {
            return;
        }
        if let Some(spawner) = self.spawner.clone() {
            self.parallel = Some(BlockCompressor::new(
                spawner,
                self.table_id,
                self.data_block_compression,
                self.encryption.clone(),
                #[cfg(zstd_any)]
                self.zstd_dictionary.clone(),
                self.ecc,
            ));
        }
    }

    /// Drains one prepared block from the parallel pipeline in submission order,
    /// writes it, and registers its index entry. No-op when nothing is in
    /// flight; the per-block error is propagated.
    #[cfg(feature = "std")]
    fn drain_one_parallel(&mut self) -> crate::Result<()> {
        let Some(prepared) = self.parallel.as_mut().and_then(BlockCompressor::take_next) else {
            return Ok(());
        };
        let mut prepared = prepared?;
        let Some(meta) = self.pending_meta.pop_front() else {
            return Err(crate::Error::Io(crate::io::Error::other(
                "parallel block pipeline meta desync",
            )));
        };
        // Take the inner-block layout before `write_to` consumes the block.
        let layout = core::mem::take(&mut prepared.layout);
        let header = prepared.write_to(&mut self.file_writer)?;
        // Header is Copy; read the in-flight size before handing it off.
        // Clamp-to-zero: this block's bytes were counted into the in-flight total
        // when queued, so the subtraction stays non-negative.
        self.parallel_pending_bytes = self
            .parallel_pending_bytes
            .saturating_sub(u64::from(header.uncompressed_length));
        self.register_written_block(
            header,
            layout,
            meta.last_key,
            meta.last_seqno,
            meta.seqno_bounds,
            meta.item_count,
            meta.zone_block_min,
        )
    }

    // TODO: split meta writing into new function
    #[expect(clippy::too_many_lines)]
    /// Finishes the table, making sure all data is written durably
    pub fn finish(mut self) -> crate::Result<Option<(TableId, Checksum)>> {
        #[cfg(not(feature = "std"))]
        use crate::io::Write;
        #[cfg(feature = "std")]
        use std::io::Write;

        self.spill_block()?;

        // Drain any blocks still being compressed on worker threads before we
        // read item_count or write the index, filter and metadata.
        #[cfg(feature = "std")]
        while self.parallel.as_ref().map_or(0, BlockCompressor::pending) > 0 {
            self.drain_one_parallel()?;
        }

        // No items and no range tombstones — delete the empty table file.
        if self.meta.item_count == 0 && self.range_tombstones.is_empty() {
            self.fs.remove_file(&self.path)?;
            return Ok(None);
        }

        // If we have range tombstones but no KV items, write a synthetic
        // weak tombstone at the first RT's start key to produce a valid index.
        // Preserve seqno bounds for real entries by saving/restoring metadata
        // around the sentinel write. The sentinel uses the table's lowest RT
        // seqno and should not influence user-visible metadata.
        // Also ensure the table metadata key range covers all range tombstones.
        if self.meta.item_count == 0 {
            // Compute the coverage of all range tombstones.
            let mut min_start: Option<UserKey> = None;
            let mut max_end: Option<UserKey> = None;
            let mut sentinel_start: Option<UserKey> = None;
            let mut sentinel_seqno: Option<crate::SeqNo> = None;
            for rt in &self.range_tombstones {
                match &min_start {
                    None => min_start = Some(rt.start.clone()),
                    Some(cur_min) if rt.start < *cur_min => min_start = Some(rt.start.clone()),
                    _ => {}
                }
                match &max_end {
                    None => max_end = Some(rt.end.clone()),
                    Some(cur_max) if rt.end > *cur_max => max_end = Some(rt.end.clone()),
                    _ => {}
                }

                match (sentinel_seqno, &sentinel_start) {
                    (None, _) => {
                        sentinel_seqno = Some(rt.seqno);
                        sentinel_start = Some(rt.start.clone());
                    }
                    (Some(cur_seqno), Some(cur_start))
                        if rt.seqno < cur_seqno
                            || (rt.seqno == cur_seqno && rt.start < *cur_start) =>
                    {
                        sentinel_seqno = Some(rt.seqno);
                        sentinel_start = Some(rt.start.clone());
                    }
                    _ => {}
                }
            }

            if let (Some(start), Some(end), Some(sentinel_key), Some(sentinel_seqno)) =
                (min_start, max_end, sentinel_start, sentinel_seqno)
            {
                let saved_lo = self.meta.lowest_seqno;
                let saved_hi = self.meta.highest_seqno;
                let saved_kv_hi = self.meta.highest_kv_seqno;

                // Write a sentinel key to force index block creation in RT-only
                // tables. The sentinel must use the start key of the same
                // tombstone that contributes the lowest seqno; otherwise it can
                // become visible at a key that is not yet covered by any visible
                // range tombstone and incorrectly mask older values.
                self.write(InternalValue::new_weak_tombstone(
                    sentinel_key,
                    sentinel_seqno,
                ))?;
                self.spill_block()?;

                // Restore seqno bounds — sentinel seqno is derived from RT
                // metadata, not from user data, so it should not shift the
                // table's seqno range. Item/key counts are NOT decremented:
                // the sentinel IS an on-disk entry and counts must match
                // actual block contents for consistency with recovery/tests.
                self.meta.lowest_seqno = saved_lo;
                self.meta.highest_seqno = saved_hi;
                self.meta.highest_kv_seqno = saved_kv_hi;

                // Ensure the table's key range covers all range tombstones.
                self.meta.first_key = Some(start);
                self.meta.last_key = Some(end);
            }
        }

        // Drain any block submitted to the parallel pipeline after the initial
        // drain above — notably the RT-only sentinel spill — so the index sees
        // every data block before it is finalized.
        #[cfg(feature = "std")]
        while self.parallel.as_ref().map_or(0, BlockCompressor::pending) > 0 {
            self.drain_one_parallel()?;
        }

        // Write index
        log::trace!("Finishing index writer");
        let (index_block_count, tli_bytes) = self.index_writer.finish(&mut self.file_writer)?;

        // Write filter
        log::trace!("Finishing filter writer");
        let filter_block_count = self.filter_writer.finish(&mut self.file_writer)?;

        // Write the optional inner-block layout section (only when at least one
        // data block split into >= 2 inner zstd blocks). Absent otherwise, so
        // default small-block tables gain no bytes.
        if !self.block_layouts.is_empty() {
            self.file_writer.start("block_layout")?;

            self.block_buffer.clear();
            crate::table::block_layout::encode_block_layouts(
                &mut self.block_buffer,
                &self.block_layouts,
            );

            Block::write_into(
                &mut self.file_writer,
                &self.block_buffer,
                crate::table::block::BlockIdentity {
                    table_id: self.table_id,
                    block_type: crate::table::block::BlockType::BlockLayout,
                    dict_id: 0,
                    window_log: 0,
                },
                // Layout metadata is small and read on the cold-block path;
                // store it uncompressed. Encryption / page_ecc still apply via
                // the configured providers, matching the other meta sections.
                &{
                    let t = match self.encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = self.ecc {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;
        }

        // Write the optional seqno-bounds section (only when seqno_in_index is on
        // and at least one block was written). Parallel to the index, keyed by
        // data-block offset; absent otherwise, so the index stays legacy-sized.
        if !self.seqno_bounds_section.is_empty() {
            self.file_writer.start("seqno_bounds")?;
            self.block_buffer.clear();
            crate::table::seqno_bounds::encode_seqno_bounds(
                &mut self.block_buffer,
                &self.seqno_bounds_section,
            )?;
            Block::write_into(
                &mut self.file_writer,
                &self.block_buffer,
                crate::table::block::BlockIdentity {
                    table_id: self.table_id,
                    block_type: crate::table::block::BlockType::SeqnoBounds,
                    dict_id: 0,
                    window_log: 0,
                },
                &{
                    let t = match self.encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = self.ecc {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;
        }

        // Write the optional zone-map section (only when the policy is on and at
        // least one block was written). Parallel to the index, keyed by
        // data-block offset; absent otherwise, so a point read pays nothing.
        if !self.zone_map_section.is_empty() {
            self.file_writer.start("zone_map")?;
            self.block_buffer.clear();
            crate::table::zone_map::encode_zone_map(
                &mut self.block_buffer,
                &self.zone_map_section,
            )?;
            Block::write_into(
                &mut self.file_writer,
                &self.block_buffer,
                crate::table::block::BlockIdentity {
                    table_id: self.table_id,
                    block_type: crate::table::block::BlockType::ZoneMap,
                    dict_id: 0,
                    window_log: 0,
                },
                &{
                    let t = match self.encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = self.ecc {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;
        }

        // Write the optional retrieval-ribbon locator section. Emitted only when
        // the level's locator policy is enabled AND the per-SST widths fit the
        // actual layout (`build_locator_section` returns `None` to skip
        // gracefully otherwise). Absent for a default table, so no bytes added.
        if let Some(spec) = self.locator
            && let Some(section) =
                crate::table::locator::build_locator_section(&self.locators, spec)
        {
            self.file_writer.start("locator")?;
            Block::write_into(
                &mut self.file_writer,
                &section,
                crate::table::block::BlockIdentity {
                    table_id: self.table_id,
                    block_type: crate::table::block::BlockType::Locator,
                    dict_id: 0,
                    window_log: 0,
                },
                // Same protection envelope as the other meta sections: optional
                // encryption + page ECC via the configured providers.
                &{
                    let t = match self.encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = self.ecc {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;
        }

        // Write range tombstones block (if any)
        if !self.range_tombstones.is_empty() {
            use crate::io::{LE, WriteBytesExt};

            self.file_writer.start("range_tombstones")?;

            // Wire format (repeated): [start_len:u16_le][start][end_len:u16_le][end][seqno:u64_le]
            self.block_buffer.clear();
            for rt in &self.range_tombstones {
                let start_len = u16::try_from(rt.start.len()).map_err(|_| {
                    crate::io::Error::new(
                        crate::io::ErrorKind::InvalidData,
                        "range tombstone start key length exceeds u16::MAX",
                    )
                })?;
                let end_len = u16::try_from(rt.end.len()).map_err(|_| {
                    crate::io::Error::new(
                        crate::io::ErrorKind::InvalidData,
                        "range tombstone end key length exceeds u16::MAX",
                    )
                })?;

                self.block_buffer.write_u16::<LE>(start_len)?;
                self.block_buffer.extend_from_slice(&rt.start);
                self.block_buffer.write_u16::<LE>(end_len)?;
                self.block_buffer.extend_from_slice(&rt.end);
                self.block_buffer.write_u64::<LE>(rt.seqno)?;
            }

            Block::write_into(
                &mut self.file_writer,
                &self.block_buffer,
                crate::table::block::BlockIdentity {
                    table_id: self.table_id,
                    block_type: crate::table::block::BlockType::RangeTombstone,
                    dict_id: 0,
                    window_log: 0,
                },
                // Range-tombstone blocks are always uncompressed; the
                // transform is Plain or Encrypted depending on the
                // configured provider. page_ecc upgrades to the
                // matching `*Ecc` variant when the tree opted in.
                &{
                    let t = match self.encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = self.ecc {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;
        }

        // Snapshot all meta fields once — reused for both MID and TAIL
        // copies. Borrowing `first_key` / `last_key` from `self.meta`
        // here is fine because neither the MID write nor any of the
        // intermediate tail sections (`linked_blob_files`,
        // `table_version`, `meta_separator`) touch `self.meta`'s key
        // buffers — those stay owned by `self` through to the TAIL
        // write. Note that `self.index_writer` and `self.filter_writer`
        // are already moved by the `.finish()` calls above; we don't
        // touch them again here.
        #[expect(clippy::expect_used, reason = "non-empty table guaranteed earlier")]
        let first_key = self
            .meta
            .first_key
            .as_ref()
            .expect("first_key should exist");
        #[expect(clippy::expect_used, reason = "non-empty table guaranteed earlier")]
        let last_key = self.meta.last_key.as_ref().expect("last_key should exist");
        let range_tombstone_count = self.range_tombstones.len() as u64;
        // Snapshot the wall-clock once — both MID and TAIL copies
        // must report the SAME created_at so MID-fallback recovery
        // produces the same timestamp as a clean TAIL recovery.
        let created_at_nanos = unix_timestamp().as_nanos();
        let mut meta_params = MetaSectionParams {
            section_name: "meta_mid",
            index_block_count,
            filter_block_count,
            // MID and TAIL must encode the SAME file_size — recovery
            // falls back transparently and downstream consumers
            // (compaction window picking, etc.) read it as if it were
            // authoritative. `self.meta.file_pos` is only ever bumped
            // inside spill_block, so its value is identical at MID
            // and TAIL write time.
            file_size: *self.meta.file_pos,
            table_id: self.table_id,
            data_block_count: self.meta.data_block_count as u64,
            item_count: self.meta.item_count as u64,
            tombstone_count: self.meta.tombstone_count as u64,
            weak_tombstone_count: self.meta.weak_tombstone_count as u64,
            weak_tombstone_reclaimable: self.meta.weak_tombstone_reclaimable_count as u64,
            key_count: self.meta.key_count as u64,
            sum_user_key_bytes: self.meta.sum_user_key_bytes,
            sum_value_bytes: self.meta.sum_value_bytes,
            uncompressed_size: self.meta.uncompressed_size,
            first_key,
            last_key,
            lowest_seqno: self.meta.lowest_seqno,
            highest_seqno: self.meta.highest_seqno,
            highest_kv_seqno: self.meta.highest_kv_seqno,
            data_block_compression: self.data_block_compression,
            index_block_compression: self.index_block_compression,
            // Evaluate the kv-checksum policy once for this table's
            // (level, table_id). The result is constant across all the
            // table's data blocks (homogeneous SST), so it doubles as
            // the per-SST descriptor value — identical to the per-block
            // decision `spill_block` makes via the same expression.
            kv_checksum_algo: self.kv_checksum.and_then(|(policy, algo)| {
                policy
                    .applies(self.initial_level, self.table_id)
                    .then_some(algo)
            }),
            data_block_hash_ratio: self.data_block_hash_ratio,
            data_block_restart_interval: self.data_block_restart_interval,
            index_block_restart_interval: self.index_block_restart_interval,
            initial_level: self.initial_level,
            use_columnar: self.use_columnar,
            range_tombstone_count,
            created_at_nanos,
        };

        // MID meta copy — defends against torn-write at the file tail
        // (incomplete fsync). Written at the current writer cursor,
        // after `range_tombstones` and before
        // `linked_blob_files` / `table_version` / `meta_separator` /
        // `meta`. The 4 KiB `meta_separator` between MID and TAIL
        // guarantees the two copies land on different filesystem
        // sectors. `file_size` is `*self.meta.file_pos` so MID and
        // TAIL encode identical values: `file_pos` is only ever bumped
        // inside `spill_block`, so it doesn't change between the two
        // writes.
        write_meta_section(
            &mut self.file_writer,
            &mut self.block_buffer,
            self.encryption.as_deref(),
            self.ecc,
            self.table_id,
            &meta_params,
        )?;

        if !self.linked_blob_files.is_empty() {
            use crate::io::{LE, WriteBytesExt};

            self.file_writer.start("linked_blob_files")?;

            #[expect(
                clippy::cast_possible_truncation,
                reason = "there are never 4 billion blob files linked to a single table"
            )]
            self.file_writer
                .write_u32::<LE>(self.linked_blob_files.len() as u32)?;

            for file in &self.linked_blob_files {
                self.file_writer.write_u64::<LE>(file.blob_file_id)?;
                self.file_writer.write_u64::<LE>(file.len as u64)?;
                self.file_writer.write_u64::<LE>(file.bytes)?;
                self.file_writer.write_u64::<LE>(file.on_disk_bytes)?;
            }
        }

        self.file_writer.start("table_version")?;
        self.file_writer.write_all(&[0x3])?;

        // 4 KiB padding section so MID copy and TAIL copy live on
        // different 4 KiB filesystem sectors. Without this, a single
        // bad sector at the tail could take out both copies (only
        // ~tens of bytes separate the linked_blob_files / table_version
        // sections between them).
        self.file_writer.start("meta_separator")?;
        self.file_writer.write_all(&[0u8; 4096])?;

        // TLI mirror near the file tail. The head `tli` section was
        // emitted earlier in `finish()` by `index_writer.finish()` —
        // after `data` and the partitioned `index` sub-blocks (if any)
        // but before `filter` / `filter_tli` / `range_tombstones` /
        // `meta_mid` / `linked_blob_files` / `table_version` /
        // `meta_separator`. So the head copy lives in the middle of
        // the file, and `tli_tail` lives after `meta_separator` and
        // before the canonical `meta` section. Several KiB of
        // unrelated sections plus the 4 KiB `meta_separator`
        // guarantee the two copies land in different 4 KiB
        // filesystem sectors (for any typical table they are
        // hundreds of KiB to MiB apart). A torn-write or bad sector
        // at either end leaves the other copy intact. Reader prefers the tail
        // copy on open and transparently falls back to the head copy
        // on decode/checksum/decrypt failure. Re-encodes the same
        // `tli_bytes` returned by `index_writer.finish()` so the two
        // sections decode to the same logical TLI. Both copies MUST
        // be written with the same `CompressionType` (this call uses
        // `self.index_block_compression`, identical to what the
        // partitioned/full index writer used for the head): the block
        // header does not record the compression tag, so the reader
        // supplies a single `CompressionType` from table metadata
        // when decoding either copy. If the two were written under
        // different codecs, at least one copy would be undecodable.
        // The encryption nonce differs per `Block::write_into` call,
        // so the resulting ciphertext differs byte-for-byte across
        // the two copies, but both decrypt to the same plaintext
        // IndexBlock.
        self.file_writer.start("tli_tail")?;
        Block::write_into(
            &mut self.file_writer,
            &tli_bytes,
            crate::table::block::BlockIdentity {
                table_id: self.table_id,
                block_type: crate::table::block::BlockType::Index,
                dict_id: 0,
                window_log: 0,
            },
            // TLI tail mirror uses the same codec as the head and
            // never carries a zstd dict. page_ecc upgrades the
            // transform when the tree opted in.
            &{
                let t = crate::table::block::BlockTransform::from_parts(
                    self.index_block_compression,
                    self.encryption.as_deref(),
                    #[cfg(zstd_any)]
                    None,
                )?;
                if let Some(ecc) = self.ecc {
                    t.with_ecc(ecc)
                } else {
                    t
                }
            },
        )?;

        // TAIL meta — the canonical, authoritative copy. `file_size`
        // is the LOGICAL size up to the end of the data blocks
        // (`self.meta.file_pos`), NOT the on-disk offset of this
        // block. `file_pos` is bumped only inside `spill_block`, so
        // it does not track the absolute file cursor here; the value
        // is the same one MID encoded above, which is the contract
        // downstream consumers (`Table::file_size`, compaction window
        // picking, checkpoint sizing — see the comment in
        // `checkpoint.rs:170-185`) read.
        meta_params.section_name = "meta";
        // file_size already carries `*self.meta.file_pos` from the MID write;
        // `file_pos` hasn't moved since (no intermediate write touches it), so
        // re-assigning is a no-op. Kept explicit for readability.
        meta_params.file_size = *self.meta.file_pos;
        write_meta_section(
            &mut self.file_writer,
            &mut self.block_buffer,
            self.encryption.as_deref(),
            self.ecc,
            self.table_id,
            &meta_params,
        )?;

        // Write fixed-size trailer
        // and flush & fsync the table file.
        // SyncMode coverage: SST writer (here), manifest, version persist,
        // directory syncs, and the blob-file writer all honor
        // `Config::sync_mode`. The only remaining unconditional F_FULLFSYNC is
        // `StdFs::hard_link`'s cross-device copy fallback (no trait param),
        // tracked in #377.
        let mut checksum = self.file_writer.into_inner()?;
        {
            let file = checksum.inner_mut().get_mut();
            FsFile::sync_all_with(&**file, self.sync_mode)?;
            // Cold-level output (L1+) is compaction product: it isn't read
            // back immediately, so drop its just-written pages from the OS
            // page cache (advisory POSIX_FADV_DONTNEED) instead of letting
            // them evict hot pages of files we are still serving reads from.
            // L0 (flush output) is left cached — freshly written keys are
            // often read back soon. Best-effort: hint() is advisory, so a
            // failure just leaves the kernel on its default policy.
            if self.initial_level >= 1 {
                let _ = FsFile::hint(&**file, FileHint::WriteOnce);
            }
        }
        let checksum = checksum.checksum();

        // IMPORTANT: fsync folder on Unix

        #[expect(
            clippy::expect_used,
            reason = "if there's no parent folder, something has gone horribly wrong"
        )]
        crate::file::fsync_directory(
            self.path.parent().expect("should have folder"),
            &*self.fs,
            self.sync_mode,
        )?;

        log::debug!(
            "Written {} items in {} blocks into new table file #{}, written {} MiB",
            self.meta.item_count,
            self.meta.data_block_count,
            self.table_id,
            *self.meta.file_pos / 1_024 / 1_024,
        );

        Ok(Some((self.table_id, checksum)))
    }
}

/// Parameters bundle for [`write_meta_section`]. Free-standing struct
/// (not a `Writer` method) because `finish()` calls this AFTER
/// `index_writer.finish()` / `filter_writer.finish()` have consumed
/// those two fields by-value, leaving `self` partially-moved and so
/// unable to dispatch through `&mut self` methods.
struct MetaSectionParams<'a> {
    section_name: &'static str,
    index_block_count: usize,
    filter_block_count: usize,
    file_size: u64,
    table_id: TableId,
    data_block_count: u64,
    item_count: u64,
    tombstone_count: u64,
    weak_tombstone_count: u64,
    weak_tombstone_reclaimable: u64,
    key_count: u64,
    sum_user_key_bytes: u64,
    sum_value_bytes: u64,
    uncompressed_size: u64,
    first_key: &'a [u8],
    last_key: &'a [u8],
    lowest_seqno: crate::SeqNo,
    highest_seqno: crate::SeqNo,
    highest_kv_seqno: crate::SeqNo,
    data_block_compression: CompressionType,
    index_block_compression: CompressionType,
    /// Effective per-SST per-KV-footer algorithm: `Some(algo)` when this
    /// table's `(level, table_id)` satisfies the kv-checksum policy (so
    /// every data block carries a footer under `algo`), `None` otherwise.
    /// An SST is homogeneous, so this single value describes the whole
    /// table. Recorded in meta as the `descriptor#kv_checksum` byte.
    kv_checksum_algo: Option<crate::runtime_config::ChecksumAlgorithm>,
    data_block_hash_ratio: f32,
    data_block_restart_interval: u8,
    index_block_restart_interval: u8,
    initial_level: u8,
    use_columnar: bool,
    range_tombstone_count: u64,
    /// `created_at` snapshot taken once in `finish()`. Both MID and
    /// TAIL writes consume this same value; generating it inside
    /// `write_meta_section` per call would stamp the two copies with
    /// different wall-clock readings and shift TTL/FIFO ordering on
    /// MID-fallback recovery.
    created_at_nanos: u128,
}

/// Resolves a `(page_ecc, EccScheme)` config pair into the per-block
/// [`EccParams`](crate::table::block::EccParams) the writer emits.
///
/// `page_ecc == false` → `None` (no parity). `Secded` maps to the per-word
/// SEC-DED scheme; a shard scheme (`Xor` / `ReedSolomon`) maps to its
/// `(data_shards, parity_shards)`. There is no implicit RS(4,2) fallback.
#[must_use]
#[expect(
    clippy::expect_used,
    reason = "a zero shard count here is an upstream config-invariant violation \
              (try_update rejects it on the live path), so it is a programming \
              error that must fail loud, like the writer's other contract asserts"
)]
pub(crate) fn resolve_ecc(
    page_ecc: bool,
    scheme: crate::runtime_config::EccScheme,
) -> Option<crate::table::block::EccParams> {
    // Without the `page_ecc` feature the parity codecs are not compiled, so no
    // block can actually carry a trailer; returning `None` keeps the stored
    // writer state (`self.ecc`) aligned with the real on-disk layout instead of
    // pushing a compensating feature guard into every consumer.
    if !page_ecc || !cfg!(feature = "page_ecc") {
        return None;
    }
    if matches!(scheme, crate::runtime_config::EccScheme::Secded) {
        return Some(crate::table::block::EccParams::SECDED);
    }
    scheme.shard_params().map(|(data, parity)| {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "shard counts originate as u8 in EccScheme"
        )]
        let (data, parity) = (data as u8, parity as u8);
        // Shard counts come from a runtime-validated `EccScheme`
        // (`try_update` rejects zero counts), so `try_new` cannot fail on
        // the live path. The `expect` localizes the invariant for the one
        // remaining injection point — a hand-built `Config::ecc_scheme`
        // with a zero count — to a loud config error rather than a corrupt
        // SST descriptor.
        crate::table::block::EccParams::try_new(data, parity)
            .expect("EccScheme shard counts are non-zero")
    })
}

fn meta_kv(key: &str, value: &[u8]) -> InternalValue {
    InternalValue::from_components(key, value, 0, crate::ValueType::Value)
}

fn write_meta_section<W: crate::io::Write + crate::io::Seek>(
    file_writer: &mut crate::sfa::Writer<ChecksummedWriter<W>>,
    block_buffer: &mut Vec<u8>,
    encryption: Option<&dyn EncryptionProvider>,
    ecc: Option<crate::table::block::EccParams>,
    table_id: TableId,
    p: &MetaSectionParams<'_>,
) -> crate::Result<()> {
    file_writer.start(p.section_name)?;

    // Record the EFFECTIVE Page-ECC scheme, not the requested one. On
    // builds without the `page_ecc` cargo feature, `with_ecc()` compiles to
    // the identity and no parity trailer is ever emitted, so the descriptor
    // must read "off" to stay consistent with the on-disk blocks (otherwise
    // an SST advertises parity that isn't there).
    let effective_ecc = if cfg!(feature = "page_ecc") {
        ecc
    } else {
        None
    };

    // Per-SST ECC descriptor: 4 bytes [kind, data_shards, parity_shards,
    // granularity], recording the exact scheme the blocks were written
    // with so the read path re-derives the parity layout. Map the
    // resolved EccParams back to a scheme (parity_shards == 1 => XOR,
    // else Reed-Solomon); granularity is Block (the only level today).
    let ecc_descriptor = {
        use crate::runtime_config::{EccGranularity, EccScheme};
        use crate::table::block::EccParams;
        let cfg = effective_ecc.map(|p| {
            // Branch on the scheme: the shard accessors panic on the per-word
            // SEC-DED variant, so match it explicitly. (SEC-DED carries no
            // shard counts; its descriptor is kind=1.)
            let scheme = match p {
                EccParams::Secded => EccScheme::Secded,
                EccParams::Shard {
                    data_shards,
                    parity_shards: 1,
                } => EccScheme::Xor { data_shards },
                EccParams::Shard {
                    data_shards,
                    parity_shards,
                } => EccScheme::ReedSolomon {
                    data_shards,
                    parity_shards,
                },
            };
            (scheme, EccGranularity::Block)
        });
        crate::runtime_config::ecc_descriptor_bytes(cfg)
    };

    let meta = meta_kv;
    let meta_items = [
        meta("block_count#data", &p.data_block_count.to_le_bytes()),
        meta(
            "block_count#filter",
            &(p.filter_block_count as u64).to_le_bytes(),
        ),
        meta(
            "block_count#index",
            &(p.index_block_count as u64).to_le_bytes(),
        ),
        meta("checksum_type", &[u8::from(ChecksumType::Xxh3)]),
        meta(
            "compression#data",
            &p.data_block_compression.encode_into_vec(),
        ),
        meta(
            "compression#index",
            &p.index_block_compression.encode_into_vec(),
        ),
        meta("crate_version", env!("CARGO_PKG_VERSION").as_bytes()),
        meta("created_at", &p.created_at_nanos.to_le_bytes()),
        meta(
            "data_block_hash_ratio",
            &p.data_block_hash_ratio.to_le_bytes(),
        ),
        // Per-SST layout descriptor: whether every data block in this table is
        // column-organized (PAX) rather than row-major. One byte for the whole
        // homogeneous SST, so the read path learns the layout from the
        // descriptor instead of inspecting a block header.
        meta("descriptor#columnar", &[u8::from(p.use_columnar)]),
        // Per-SST transform descriptor: per-KV-footer presence + algorithm
        // as one byte (0 = no footer, else 1 + algo wire tag). Lets the
        // reader know the whole table's footer state without inspecting any
        // data block — the foundation for descriptor-driven block layout.
        meta(
            "descriptor#kv_checksum",
            &[crate::table::block::kv_checksum::descriptor_byte(
                p.kv_checksum_algo,
            )],
        ),
        // Per-SST transform descriptor: whether every block in this table
        // carries a Reed-Solomon parity trailer (Page ECC). One byte for the
        // whole homogeneous SST, so the read path learns ECC presence from
        // the descriptor instead of a per-block header field.
        meta("descriptor#page_ecc", &ecc_descriptor),
        meta("file_size", &p.file_size.to_le_bytes()),
        meta("filter_hash_type", &[u8::from(ChecksumType::Xxh3)]),
        meta("index_keys_have_seqno", &[0x1]),
        meta("initial_level", &p.initial_level.to_le_bytes()),
        meta("item_count", &p.item_count.to_le_bytes()),
        meta("key#max", p.last_key),
        meta("key#min", p.first_key),
        meta("key_bytes#sum", &p.sum_user_key_bytes.to_le_bytes()),
        meta("key_count", &p.key_count.to_le_bytes()),
        meta("prefix_truncation#data", &[1]),
        meta("prefix_truncation#index", &[1]),
        meta(
            "range_tombstone_count",
            &p.range_tombstone_count.to_le_bytes(),
        ),
        meta(
            "restart_interval#data",
            &p.data_block_restart_interval.to_le_bytes(),
        ),
        meta(
            "restart_interval#index",
            &p.index_block_restart_interval.to_le_bytes(),
        ),
        meta("seqno#kv_max", &p.highest_kv_seqno.to_le_bytes()),
        meta("seqno#max", &p.highest_seqno.to_le_bytes()),
        meta("seqno#min", &p.lowest_seqno.to_le_bytes()),
        meta("table_id", &p.table_id.to_le_bytes()),
        meta("table_version", &[3u8]),
        meta("tombstone_count", &p.tombstone_count.to_le_bytes()),
        meta("user_data_size", &p.uncompressed_size.to_le_bytes()),
        meta("value_bytes#sum", &p.sum_value_bytes.to_le_bytes()),
        meta(
            "weak_tombstone_count",
            &p.weak_tombstone_count.to_le_bytes(),
        ),
        meta(
            "weak_tombstone_reclaimable",
            &p.weak_tombstone_reclaimable.to_le_bytes(),
        ),
    ];

    #[cfg(debug_assertions)]
    {
        let is_sorted = meta_items.iter().is_sorted_by_key(|kv| &kv.key);
        assert!(is_sorted, "meta items not sorted correctly");
    }

    block_buffer.clear();
    DataBlock::encode_into(block_buffer, &meta_items, 1, 0.0)?;

    Block::write_into(
        file_writer,
        block_buffer,
        crate::table::block::BlockIdentity {
            // Seal the Meta block under the writer's own (durable) table id.
            // The reader supplies the same id out-of-band (SST path / manifest
            // entry), so a Meta block transplanted from another SST fails AEAD
            // verification. The reader cannot take it from the meta payload
            // (that is what it is about to decrypt), hence the out-of-band
            // source on both sides.
            table_id,
            block_type: crate::table::block::BlockType::Meta,
            dict_id: 0,
            window_log: 0,
        },
        // Meta blocks are always written uncompressed; the transform
        // is Plain or Encrypted depending on whether the table is
        // keyed. page_ecc upgrades to the matching `*Ecc` variant
        // when the tree opted in.
        &{
            let t = match encryption {
                Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                None => crate::table::block::BlockTransform::PLAIN,
            };
            // The Meta block is read at table-open BEFORE its own
            // descriptor is parsed (chicken-and-egg: the scheme is IN
            // the descriptor), so the reader cannot know a per-config
            // scheme for it. Self-describing blocks therefore use the
            // FIXED RS(4,2) layout (matching the reader's fallback);
            // the configurable scheme applies only to the SST data /
            // index / filter blocks, whose scheme the reader learns
            // from this descriptor.
            if effective_ecc.is_some() {
                t.with_ecc(crate::table::block::EccParams::RS_4_2)
            } else {
                t
            }
        },
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::StdFs;
    use test_log::test;

    #[test]
    fn table_writer_count() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("1");
        let mut writer = Writer::new(path, 1, 0, Arc::new(StdFs))?;

        assert_eq!(0, writer.meta.key_count);
        assert_eq!(0, writer.chunk_size);

        writer.write(InternalValue::from_components(
            b"a",
            b"a",
            0,
            ValueType::Value,
        ))?;
        assert_eq!(1, writer.meta.key_count);
        assert_eq!(2, writer.chunk_size);

        writer.write(InternalValue::from_components(
            b"b",
            b"b",
            0,
            ValueType::Value,
        ))?;
        assert_eq!(2, writer.meta.key_count);
        assert_eq!(4, writer.chunk_size);

        writer.write(InternalValue::from_components(
            b"c",
            b"c",
            0,
            ValueType::Value,
        ))?;
        assert_eq!(3, writer.meta.key_count);
        assert_eq!(6, writer.chunk_size);

        writer.spill_block()?;
        assert_eq!(0, writer.chunk_size);

        Ok(())
    }

    #[test]
    #[should_panic(expected = "index block restart interval must be greater than zero")]
    fn writer_rejects_zero_index_block_restart_interval() {
        let dir = match tempfile::tempdir() {
            Ok(dir) => dir,
            Err(e) => panic!("tempdir should be created: {e}"),
        };
        let path = dir.path().join("1");
        let writer = match Writer::new(path, 1, 0, Arc::new(StdFs)) {
            Ok(writer) => writer,
            Err(e) => panic!("writer should be created: {e}"),
        };
        let _writer = writer.use_index_block_restart_interval(0);
    }

    #[test]
    #[should_panic(expected = "data block restart interval must be greater than zero")]
    fn writer_rejects_zero_data_block_restart_interval() {
        let dir = match tempfile::tempdir() {
            Ok(dir) => dir,
            Err(e) => panic!("tempdir should be created: {e}"),
        };
        let path = dir.path().join("1");
        let writer = match Writer::new(path, 1, 0, Arc::new(StdFs)) {
            Ok(writer) => writer,
            Err(e) => panic!("writer should be created: {e}"),
        };
        let _writer = writer.use_data_block_restart_interval(0);
    }

    #[test]
    #[should_panic(
        expected = "data block restart interval must be configured before writing starts"
    )]
    fn writer_rejects_data_block_restart_interval_change_after_write() {
        let dir = match tempfile::tempdir() {
            Ok(dir) => dir,
            Err(e) => panic!("tempdir should be created: {e}"),
        };
        let path = dir.path().join("1");
        let mut writer = match Writer::new(path, 1, 0, Arc::new(StdFs)) {
            Ok(writer) => writer,
            Err(e) => panic!("writer should be created: {e}"),
        };
        if let Err(e) = writer.write(InternalValue::from_components(
            b"a",
            b"v",
            0,
            ValueType::Value,
        )) {
            panic!("write should succeed: {e}");
        }
        let _writer = writer.use_data_block_restart_interval(2);
    }

    #[test]
    #[should_panic(
        expected = "index block restart interval must be configured before writing starts"
    )]
    fn writer_rejects_index_block_restart_interval_change_after_write() {
        let dir = match tempfile::tempdir() {
            Ok(dir) => dir,
            Err(e) => panic!("tempdir should be created: {e}"),
        };
        let path = dir.path().join("1");
        let mut writer = match Writer::new(path, 1, 0, Arc::new(StdFs)) {
            Ok(writer) => writer,
            Err(e) => panic!("writer should be created: {e}"),
        };
        if let Err(e) = writer.write(InternalValue::from_components(
            b"a",
            b"v",
            0,
            ValueType::Value,
        )) {
            panic!("write should succeed: {e}");
        }
        let _writer = writer.use_index_block_restart_interval(2);
    }

    #[test]
    #[should_panic(expected = "partitioned index must be configured before writing starts")]
    fn writer_rejects_partitioned_index_switch_after_write() {
        let dir = match tempfile::tempdir() {
            Ok(dir) => dir,
            Err(e) => panic!("tempdir should be created: {e}"),
        };
        let path = dir.path().join("1");
        let mut writer = match Writer::new(path, 1, 0, Arc::new(StdFs)) {
            Ok(writer) => writer,
            Err(e) => panic!("writer should be created: {e}"),
        };
        if let Err(e) = writer.write(InternalValue::from_components(
            b"a",
            b"v",
            0,
            ValueType::Value,
        )) {
            panic!("write should succeed: {e}");
        }
        let _writer = writer.use_partitioned_index();
    }

    #[test]
    fn writer_meta_partition_size_is_chainable_with_full_index_writer() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("full-index");
        let mut writer = Writer::new(path, 1, 0, Arc::new(StdFs))?.use_meta_partition_size(8_192);

        writer.write(InternalValue::from_components(
            b"k",
            b"v",
            0,
            ValueType::Value,
        ))?;
        writer.spill_block()?;

        Ok(())
    }

    #[test]
    #[should_panic(expected = "partitioned filter must be configured before writing starts")]
    fn writer_rejects_partitioned_filter_switch_after_write() {
        let dir = match tempfile::tempdir() {
            Ok(dir) => dir,
            Err(e) => panic!("tempdir should be created: {e}"),
        };
        let path = dir.path().join("1");
        let mut writer = match Writer::new(path, 1, 0, Arc::new(StdFs)) {
            Ok(writer) => writer,
            Err(e) => panic!("writer should be created: {e}"),
        };
        if let Err(e) = writer.write(InternalValue::from_components(
            b"a",
            b"v",
            0,
            ValueType::Value,
        )) {
            panic!("write should succeed: {e}");
        }
        let _writer = writer.use_partitioned_filter();
    }
}
