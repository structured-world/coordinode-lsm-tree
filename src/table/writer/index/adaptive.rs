// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Size-adaptive block-index writer (streaming spill).
//!
//! Picks the index layout by actual index size instead of an
//! unconditional policy:
//!
//! - While the accumulated index stays at or below `spill_threshold`
//!   bytes, entries are buffered and the index is written as a
//!   **single-level** ([`FullIndexWriter`]) index at `finish`. The
//!   whole index is one block; the reader pins it in `FullBlockIndex`
//!   and a point read costs one index-block lookup.
//! - The moment the buffer would exceed `spill_threshold`, the writer
//!   **spills**: it replays the buffered handles into a streaming
//!   [`PartitionedIndexWriter`] and forwards every subsequent handle to
//!   it, producing a **two-level** (partitioned) index. The reader then
//!   keeps only the top-level index resident and pages bottom partitions
//!   through the block cache.
//!
//! Rationale: a two-level index bounds resident index RAM per open SST
//! (only the TLI stays pinned), which matters for large datasets — but
//! it costs an extra index level on every point read. For small/medium
//! SSTs, pinning the whole index is cheap and the extra level is pure
//! overhead, so single-level is both faster and memory-fine. This writer
//! gets the best of both: single-level until the index is large enough
//! that pinning it whole stops being cheap, then partitioned.
//!
//! Memory is bounded at `spill_threshold` (after spilling, the streaming
//! partition writer flushes to the file as it goes), so even a huge SST
//! never buffers its entire index.

use crate::{
    CompressionType,
    checksum::ChecksummedWriter,
    encryption::EncryptionProvider,
    table::{
        index_block::KeyedBlockHandle,
        writer::index::{BlockIndexWriter, FullIndexWriter, PartitionedIndexWriter},
    },
};
use std::{
    io::{Seek, Write},
    sync::Arc,
};

/// Index-size threshold (bytes) at or below which the index is written
/// single-level. The estimate mirrors [`PartitionedIndexWriter`]'s own
/// per-entry accounting (`end_key.len() + size_of::<KeyedBlockHandle>()`).
///
/// A single-level index is reached by one lookup (versus two-level's two).
/// On hot levels the reader pins the index block; on cold levels it pages
/// the block through the shared block cache (evictable), so a multi-MiB
/// threshold does not pin unbounded RAM. 4 MiB keeps SSTs up to a
/// few-hundred-MB single-level (where single-level wins point reads) while
/// genuinely huge indexes still partition. Tunable per tree (see the
/// index-partition policy / runtime config).
pub const DEFAULT_SPILL_THRESHOLD: u64 = 4 * 1024 * 1024;

pub struct AdaptiveIndexWriter<W: Write + Seek + 'static> {
    // Forwarded config (applied to whichever inner writer is built).
    compression: CompressionType,
    restart_interval: u8,
    /// Per-bottom-partition size, forwarded to the partitioned writer
    /// once spilled. Distinct from `spill_threshold` (which decides
    /// *whether* to partition at all).
    partition_size: u32,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    table_id: crate::TableId,
    tree_id: crate::tree::inner::TreeId,
    ecc: Option<crate::table::block::EccParams>,

    /// Total index size, in bytes, above which we spill to two-level.
    spill_threshold: u64,

    // Pre-spill state: buffered handles + running size estimate.
    buffer: Vec<KeyedBlockHandle>,
    buffered_bytes: u64,

    /// `Some` once spilled — every subsequent handle is forwarded here
    /// and `finish` delegates to it.
    spilled: Option<Box<dyn BlockIndexWriter<W>>>,
}

impl<W: Write + Seek + 'static> AdaptiveIndexWriter<W> {
    #[must_use]
    pub fn new(spill_threshold: u64) -> Self {
        Self {
            compression: CompressionType::None,
            restart_interval: 1,
            partition_size: 4_096,
            encryption: None,
            table_id: 0,
            tree_id: 0,
            ecc: None,
            spill_threshold,
            buffer: Vec::new(),
            buffered_bytes: 0,
            spilled: None,
        }
    }

    /// Applies the buffered config to a freshly-constructed inner writer
    /// (`Full` or `Partitioned`), so both layouts inherit identical
    /// compression / encryption / restart / ecc / table-id settings.
    fn configure(&self, inner: Box<dyn BlockIndexWriter<W>>) -> Box<dyn BlockIndexWriter<W>> {
        inner
            .use_compression(self.compression)
            .use_restart_interval(self.restart_interval)
            .use_partition_size(self.partition_size)
            .use_encryption(self.encryption.clone())
            .use_table_id(self.table_id)
            .use_tree_id(self.tree_id)
            .use_ecc(self.ecc)
    }

    /// Transition to two-level: build a partitioned writer with the
    /// current config and replay the buffered handles into it.
    fn spill(&mut self) -> crate::Result<()> {
        let mut partitioned = self.configure(Box::new(PartitionedIndexWriter::new()));
        for handle in self.buffer.drain(..) {
            partitioned.register_data_block(handle)?;
        }
        self.buffered_bytes = 0;
        self.spilled = Some(partitioned);
        Ok(())
    }
}

impl<W: Write + Seek + 'static> BlockIndexWriter<W> for AdaptiveIndexWriter<W> {
    fn register_data_block(&mut self, block_handle: KeyedBlockHandle) -> crate::Result<()> {
        if let Some(partitioned) = &mut self.spilled {
            return partitioned.register_data_block(block_handle);
        }

        // Mirror PartitionedIndexWriter's per-entry size estimate so the
        // threshold compares like-for-like against the partition logic.
        let entry_size =
            (block_handle.end_key().len() + std::mem::size_of::<KeyedBlockHandle>()) as u64;
        self.buffered_bytes += entry_size;
        self.buffer.push(block_handle);

        if self.buffered_bytes > self.spill_threshold {
            self.spill()?;
        }
        Ok(())
    }

    fn finish(
        self: Box<Self>,
        file_writer: &mut crate::sfa::Writer<ChecksummedWriter<W>>,
    ) -> crate::Result<(usize, Vec<u8>)> {
        let this = *self;

        // Spilled → two-level index is already streaming; just finish it.
        if let Some(partitioned) = this.spilled {
            return partitioned.finish(file_writer);
        }

        // Stayed small → single-level (Full) index.
        let mut full = this.configure(Box::new(FullIndexWriter::new()));
        for handle in this.buffer {
            full.register_data_block(handle)?;
        }
        full.finish(file_writer)
    }

    fn use_compression(
        mut self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn BlockIndexWriter<W>> {
        self.compression = compression;
        self
    }

    fn use_restart_interval(mut self: Box<Self>, interval: u8) -> Box<dyn BlockIndexWriter<W>> {
        self.restart_interval = interval;
        self
    }

    fn use_partition_size(mut self: Box<Self>, size: u32) -> Box<dyn BlockIndexWriter<W>> {
        self.partition_size = size;
        self
    }

    fn use_encryption(
        mut self: Box<Self>,
        encryption: Option<Arc<dyn EncryptionProvider>>,
    ) -> Box<dyn BlockIndexWriter<W>> {
        self.encryption = encryption;
        self
    }

    fn use_table_id(mut self: Box<Self>, table_id: crate::TableId) -> Box<dyn BlockIndexWriter<W>> {
        self.table_id = table_id;
        self
    }

    fn use_tree_id(
        mut self: Box<Self>,
        tree_id: crate::tree::inner::TreeId,
    ) -> Box<dyn BlockIndexWriter<W>> {
        self.tree_id = tree_id;
        self
    }

    fn use_ecc(
        mut self: Box<Self>,
        ecc: Option<crate::table::block::EccParams>,
    ) -> Box<dyn BlockIndexWriter<W>> {
        self.ecc = ecc;
        self
    }
}
