// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::FilterWriter;
use crate::{
    CompressionType, UserKey,
    checksum::ChecksummedWriter,
    config::BloomConstructionPolicy,
    encryption::EncryptionProvider,
    prefix::PrefixExtractor,
    table::{
        Block, BlockHandle, BlockOffset, IndexBlock, KeyedBlockHandle,
        filter::build_burr_filter_bytes,
    },
};
use std::{
    io::{Seek, Write},
    sync::Arc,
};

pub struct PartitionedFilterWriter {
    final_filter_buffer: Vec<u8>,

    tli_handles: Vec<KeyedBlockHandle>,

    /// Key hashes for AMQ filter
    pub bloom_hash_buffer: Vec<u64>,
    approx_filter_size: usize,

    partition_size: u32,

    bloom_policy: BloomConstructionPolicy,

    relative_file_pos: u64,

    last_key: Option<UserKey>,

    compression: CompressionType,

    // Accepted to keep the FilterWriter API uniform — written by
    // set_prefix_extractor but not read (partitioned filters cannot be
    // probed by prefix hash; see Table::maybe_contains_prefix).
    prefix_extractor: Option<Arc<dyn PrefixExtractor>>,

    encryption: Option<Arc<dyn EncryptionProvider>>,

    /// Owning SST's table id. Set by the outer Writer via
    /// `use_table_id` before `spill_filter_partition` / `finish`.
    table_id: crate::TableId,
}

impl PartitionedFilterWriter {
    pub fn new(bloom_policy: BloomConstructionPolicy) -> Self {
        Self {
            final_filter_buffer: Vec::new(),

            bloom_hash_buffer: Vec::new(),
            approx_filter_size: 0,

            tli_handles: Vec::new(),
            partition_size: 4_096,
            bloom_policy,

            relative_file_pos: 0,

            last_key: None,

            compression: CompressionType::None,

            prefix_extractor: None,

            encryption: None,
            table_id: 0,
        }
    }

    fn spill_filter_partition(&mut self, key: &UserKey) -> crate::Result<()> {
        let hash_count = self.bloom_hash_buffer.len();
        let partition_index = self.tli_handles.len();
        // mem::replace (rather than mem::take) preserves the buffer's
        // grown capacity for the next partition. `take` leaves a
        // capacity-0 Vec behind, which would force a reallocation on
        // every register_key call following a spill. Tables with many
        // partitions can spill thousands of times during a single
        // flush/compaction, so the saved reallocations matter on the
        // write hot path.
        let old_cap = self.bloom_hash_buffer.capacity();
        let hashes = std::mem::replace(&mut self.bloom_hash_buffer, Vec::with_capacity(old_cap));
        let filter_bytes = build_burr_filter_bytes(self.bloom_policy, hashes)?;

        // An empty BuRR build result means the policy is inactive for
        // this key population (e.g. fpr <= 0 or bpk out of [1, 64]).
        // For PARTITIONED filters, silently skipping a partition AND
        // its TLI entry causes false negatives at read time: keys in
        // this range would binary-search to a later partition's
        // filter, which doesn't contain them, and Table::check_bloom
        // would report "definitely not present" → false negative on a
        // live key.
        //
        // Fail closed: return Unrecoverable so the writer aborts table
        // creation rather than persisting a partially-filtered table.
        // In practice this path is unreachable — BloomConstructionPolicy::
        // is_active() is checked upstream before any keys are buffered.
        if filter_bytes.is_empty() {
            log::error!(
                "BuRR partitioned writer received empty filter bytes for partition {partition_index} \
                 ({hash_count} hashes) — policy likely inactive (silent skip would cause false negatives)",
            );
            return Err(crate::Error::Unrecoverable);
        }

        let header = Block::write_into(
            &mut self.final_filter_buffer,
            &filter_bytes,
            crate::table::block::BlockIdentity {
                tree_id: 0,
                table_id: self.table_id,
                block_offset: 0,
                block_type: crate::table::block::BlockType::Filter,
                dict_id: 0,
                window_log: 0,
            },
            // Per-partition filter bodies are uncompressed.
            &match self.encryption.as_deref() {
                Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                None => crate::table::block::BlockTransform::PLAIN,
            },
        )?;

        let bytes_written = header.on_disk_size();

        self.tli_handles.push(KeyedBlockHandle::new(
            key.clone(),
            0,
            BlockHandle::new(BlockOffset(self.relative_file_pos), bytes_written),
        ));

        log::trace!(
            "Built BuRR filter partition ({}B) with end_key={key:?} at +{:#X?}",
            filter_bytes.len(),
            self.relative_file_pos,
        );

        self.approx_filter_size = 0;
        self.relative_file_pos += u64::from(bytes_written);

        Ok(())
    }

    fn write_top_level_index<WR: Write + Seek>(
        &mut self,
        file_writer: &mut sfa::Writer<ChecksummedWriter<WR>>,
        index_base_offset: BlockOffset,
    ) -> crate::Result<()> {
        file_writer.start("filter_tli")?;

        for item in &mut self.tli_handles {
            item.shift(index_base_offset);
        }

        let mut bytes = vec![];
        IndexBlock::encode_into(&mut bytes, &self.tli_handles)?;

        let header = Block::write_into(
            file_writer,
            &bytes,
            crate::table::block::BlockIdentity {
                tree_id: 0,
                table_id: self.table_id,
                block_offset: 0,
                block_type: crate::table::block::BlockType::Index,
                dict_id: 0,
                window_log: 0,
            },
            // TLI for the partitioned filter uses the configured
            // index codec; no zstd dict is ever attached at this
            // writer level.
            &crate::table::block::BlockTransform::from_parts(
                self.compression,
                self.encryption.as_deref(),
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        let bytes_written = header.on_disk_size();

        debug_assert!(bytes_written > 0, "Top level index should never be empty");

        log::trace!(
            "Written filter top level index, with {} pointers ({bytes_written} bytes) at {index_base_offset:#X?}",
            self.tli_handles.len(),
        );

        Ok(())
    }
}

impl<W: std::io::Write + std::io::Seek> FilterWriter<W> for PartitionedFilterWriter {
    fn use_encryption(
        mut self: Box<Self>,
        encryption: Option<Arc<dyn EncryptionProvider>>,
    ) -> Box<dyn FilterWriter<W>> {
        self.encryption = encryption;
        self
    }

    fn use_table_id(mut self: Box<Self>, table_id: crate::TableId) -> Box<dyn FilterWriter<W>> {
        self.table_id = table_id;
        self
    }

    fn use_partition_size(mut self: Box<Self>, size: u32) -> Box<dyn FilterWriter<W>> {
        self.partition_size = size;
        self
    }

    fn use_tli_compression(
        mut self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn FilterWriter<W>> {
        self.compression = compression;
        self
    }

    fn set_filter_policy(
        mut self: Box<Self>,
        policy: BloomConstructionPolicy,
    ) -> Box<dyn FilterWriter<W>> {
        self.bloom_policy = policy;
        self
    }

    fn set_prefix_extractor(
        mut self: Box<Self>,
        extractor: Option<Arc<dyn PrefixExtractor>>,
    ) -> Box<dyn FilterWriter<W>> {
        self.prefix_extractor = extractor;
        self
    }

    fn register_key(&mut self, key: &UserKey) -> crate::Result<()> {
        self.bloom_hash_buffer.push(crate::hash::hash64(key));

        // NOTE: Prefix hashes are NOT inserted for partitioned filters.
        // Table::maybe_contains_prefix returns Ok(true) for partitioned/TLI
        // filters (partition index is keyed by user key, not prefix hash),
        // so prefix hashes would only increase CPU and filter size with no
        // read-side benefit.

        self.approx_filter_size = self
            .bloom_policy
            .estimated_filter_size(self.bloom_hash_buffer.len());

        self.last_key = Some(key.clone());

        if self.approx_filter_size >= self.partition_size as usize {
            self.spill_filter_partition(key)?;
        }

        Ok(())
    }

    fn finish(
        mut self: Box<Self>,
        file_writer: &mut sfa::Writer<ChecksummedWriter<W>>,
    ) -> crate::Result<usize> {
        if self.last_key.is_none() {
            log::trace!("Filter writer has not seen any writes - not building filter");
            return Ok(0);
        }

        if !self.bloom_hash_buffer.is_empty() {
            #[expect(
                clippy::expect_used,
                reason = "last key must exist because of initial check"
            )]
            let last_key = self.last_key.take().expect("last key should exist");
            self.spill_filter_partition(&last_key)?;
        }

        let index_base_offset = BlockOffset(file_writer.get_mut().stream_position()?);

        file_writer.start("filter")?;
        file_writer.write_all(&self.final_filter_buffer)?;
        log::trace!("Concatted filter partitions onto blocks file");

        let block_count = self.tli_handles.len();

        self.write_top_level_index(file_writer, index_base_offset)?;

        Ok(block_count)
    }
}
