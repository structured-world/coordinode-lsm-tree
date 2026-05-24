// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

mod filter;
mod index;
mod meta;

use super::{
    Block, BlockOffset, DataBlock, KeyedBlockHandle, block::Header as BlockHeader,
    filter::BloomConstructionPolicy,
};
use crate::{
    Checksum, CompressionType, InternalValue, TableId, UserKey, ValueType,
    checksum::{ChecksumType, ChecksummedWriter},
    coding::Encode,
    encryption::EncryptionProvider,
    fs::{Fs, FsFile, FsOpenOptions},
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
use index::BlockIndexWriter;
use std::{io::BufWriter, path::PathBuf, sync::Arc};

#[derive(Copy, Clone, PartialEq, Eq, Debug, std::hash::Hash)]
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
    file_writer: sfa::Writer<ChecksummedWriter<BufWriter<Box<dyn FsFile>>>>,

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

    initial_level: u8,

    /// Block encryption provider (if encryption at rest is enabled)
    encryption: Option<Arc<dyn EncryptionProvider>>,

    /// Pre-trained zstd dictionary for dictionary compression
    #[cfg(zstd_any)]
    zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
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
        let path = std::path::absolute(path)?;

        let file = fs.open(&path, &FsOpenOptions::new().write(true).create_new(true))?;
        let writer = BufWriter::with_capacity(u16::MAX.into(), file);
        let writer = ChecksummedWriter::new(writer);
        let mut writer = sfa::Writer::from_writer(writer);
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

            #[cfg(zstd_any)]
            zstd_dictionary: None,
        })
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
        assert!(
            self.meta.data_block_count == 0 && self.chunk.is_empty(),
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
            .use_table_id(self.table_id);
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

        self.block_buffer.clear();

        DataBlock::encode_into(
            &mut self.block_buffer,
            &self.chunk,
            self.data_block_restart_interval,
            self.data_block_hash_ratio,
        )?;

        let header = Block::write_into(
            &mut self.file_writer,
            &self.block_buffer,
            super::block::BlockIdentity {
                tree_id: 0,
                table_id: self.table_id,
                block_offset: *self.meta.file_pos,
                block_type: super::block::BlockType::Data,
                dict_id: self.data_block_compression.dict_id(),
                window_log: 0,
            },
            self.data_block_compression,
            self.encryption.as_deref(),
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )?;

        self.meta.uncompressed_size += u64::from(header.uncompressed_length);

        #[expect(
            clippy::cast_possible_truncation,
            reason = "block header is a couple of bytes only, so cast is fine"
        )]
        let bytes_written = BlockHeader::serialized_len() as u32 + header.data_length;

        self.index_writer
            .register_data_block(KeyedBlockHandle::new(
                last.key.user_key.clone(),
                last.key.seqno,
                BlockHandle::new(self.meta.file_pos, bytes_written),
            ))?;

        // Adjust metadata
        self.meta.file_pos += u64::from(bytes_written);
        self.meta.item_count += self.chunk.len();
        self.meta.data_block_count += 1;

        // Back link stuff
        self.prev_pos.0 = self.prev_pos.1;
        self.prev_pos.1 += u64::from(bytes_written);

        // Set last key
        self.meta.last_key = Some(
            // NOTE: We are allowed to remove the last item
            // to get ownership of it, because the chunk is cleared after
            // this anyway
            #[expect(clippy::expect_used, reason = "chunk is not empty")]
            self.chunk
                .pop()
                .expect("chunk should not be empty")
                .key
                .user_key,
        );

        // IMPORTANT: Clear chunk after everything else
        self.chunk.clear();
        self.chunk_size = 0;

        Ok(())
    }

    // TODO: split meta writing into new function
    #[expect(clippy::too_many_lines)]
    /// Finishes the table, making sure all data is written durably
    pub fn finish(mut self) -> crate::Result<Option<(TableId, Checksum)>> {
        use std::io::Write;

        self.spill_block()?;

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

        // Write index
        log::trace!("Finishing index writer");
        let index_block_count = self.index_writer.finish(&mut self.file_writer)?;

        // Write filter
        log::trace!("Finishing filter writer");
        let filter_block_count = self.filter_writer.finish(&mut self.file_writer)?;

        // Write range tombstones block (if any)
        if !self.range_tombstones.is_empty() {
            use byteorder::{LE, WriteBytesExt};

            self.file_writer.start("range_tombstones")?;

            // Wire format (repeated): [start_len:u16_le][start][end_len:u16_le][end][seqno:u64_le]
            self.block_buffer.clear();
            for rt in &self.range_tombstones {
                let start_len = u16::try_from(rt.start.len()).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "range tombstone start key length exceeds u16::MAX",
                    )
                })?;
                let end_len = u16::try_from(rt.end.len()).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
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
                    tree_id: 0,
                    table_id: self.table_id,
                    block_offset: *self.meta.file_pos,
                    block_type: crate::table::block::BlockType::RangeTombstone,
                    dict_id: 0,
                    window_log: 0,
                },
                CompressionType::None,
                self.encryption.as_deref(),
                #[cfg(zstd_any)]
                None,
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
            uncompressed_size: self.meta.uncompressed_size,
            first_key,
            last_key,
            lowest_seqno: self.meta.lowest_seqno,
            highest_seqno: self.meta.highest_seqno,
            highest_kv_seqno: self.meta.highest_kv_seqno,
            data_block_compression: self.data_block_compression,
            index_block_compression: self.index_block_compression,
            data_block_hash_ratio: self.data_block_hash_ratio,
            data_block_restart_interval: self.data_block_restart_interval,
            index_block_restart_interval: self.index_block_restart_interval,
            initial_level: self.initial_level,
            range_tombstone_count,
            // `block_offset` here feeds `BlockIdentity` which today is
            // unused (`let _ = identity;` in `Block::write_into` /
            // `Block::from_file`). Once AAD is wired in #251 the read
            // side will derive this from `*handle.offset()` (the SFA
            // TOC section offset, distinct for MID vs TAIL), so this
            // shared `*self.meta.file_pos` value will need to be
            // replaced with each section's actual file position at
            // write time. Tracked as part of the #251 AAD-wiring
            // surface — the writer here will need to query the SFA
            // writer's current file position per section.
            block_offset: *self.meta.file_pos,
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
            &meta_params,
        )?;

        if !self.linked_blob_files.is_empty() {
            use byteorder::{LE, WriteBytesExt};

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
        // file_size and block_offset already carry `*self.meta.file_pos`
        // from the MID write; `file_pos` hasn't moved since (no
        // intermediate write touches it), so re-assigning is a
        // no-op. Kept explicit for readability — if someone later
        // adds a Block::write_into call before TAIL that DOES bump
        // file_pos, this line will need to re-snapshot.
        meta_params.file_size = *self.meta.file_pos;
        meta_params.block_offset = *self.meta.file_pos;
        write_meta_section(
            &mut self.file_writer,
            &mut self.block_buffer,
            self.encryption.as_deref(),
            &meta_params,
        )?;

        // Write fixed-size trailer
        // and flush & fsync the table file
        let mut checksum = self.file_writer.into_inner()?;
        FsFile::sync_all(&**checksum.inner_mut().get_mut())?;
        let checksum = checksum.checksum();

        // IMPORTANT: fsync folder on Unix

        #[expect(
            clippy::expect_used,
            reason = "if there's no parent folder, something has gone horribly wrong"
        )]
        crate::file::fsync_directory(self.path.parent().expect("should have folder"), &*self.fs)?;

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
    uncompressed_size: u64,
    first_key: &'a [u8],
    last_key: &'a [u8],
    lowest_seqno: crate::SeqNo,
    highest_seqno: crate::SeqNo,
    highest_kv_seqno: crate::SeqNo,
    data_block_compression: CompressionType,
    index_block_compression: CompressionType,
    data_block_hash_ratio: f32,
    data_block_restart_interval: u8,
    index_block_restart_interval: u8,
    initial_level: u8,
    range_tombstone_count: u64,
    block_offset: u64,
    /// `created_at` snapshot taken once in `finish()`. Both MID and
    /// TAIL writes consume this same value; generating it inside
    /// `write_meta_section` per call would stamp the two copies with
    /// different wall-clock readings and shift TTL/FIFO ordering on
    /// MID-fallback recovery.
    created_at_nanos: u128,
}

fn meta_kv(key: &str, value: &[u8]) -> InternalValue {
    InternalValue::from_components(key, value, 0, crate::ValueType::Value)
}

fn write_meta_section<W: std::io::Write + std::io::Seek>(
    file_writer: &mut sfa::Writer<ChecksummedWriter<W>>,
    block_buffer: &mut Vec<u8>,
    encryption: Option<&dyn EncryptionProvider>,
    p: &MetaSectionParams<'_>,
) -> crate::Result<()> {
    file_writer.start(p.section_name)?;

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
        meta("file_size", &p.file_size.to_le_bytes()),
        meta("filter_hash_type", &[u8::from(ChecksumType::Xxh3)]),
        meta("index_keys_have_seqno", &[0x1]),
        meta("initial_level", &p.initial_level.to_le_bytes()),
        meta("item_count", &p.item_count.to_le_bytes()),
        meta("key#max", p.last_key),
        meta("key#min", p.first_key),
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
            tree_id: 0,
            // Meta is read FIRST during table open, BEFORE the reader
            // knows the table_id (the table_id is what meta itself
            // carries). Writer mirrors that with table_id=0 so write
            // and read sides agree on the AAD discriminator once
            // #251 wires AAD.
            table_id: 0,
            block_offset: p.block_offset,
            block_type: crate::table::block::BlockType::Meta,
            dict_id: 0,
            window_log: 0,
        },
        CompressionType::None,
        encryption,
        #[cfg(zstd_any)]
        None,
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
