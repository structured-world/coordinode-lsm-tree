// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::{
    CompressionType,
    checksum::ChecksummedWriter,
    encryption::EncryptionProvider,
    table::{
        Block, BlockHandle, BlockOffset, IndexBlock, index_block::KeyedBlockHandle,
        writer::index::BlockIndexWriter,
    },
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

// Concrete writers (sfa::Writer / ChecksummedWriter) carry the io trait via
// their own impls; the defining trait must be in scope for raw method calls.
#[cfg(not(feature = "std"))]
use crate::io::{Seek, Write};
#[cfg(feature = "std")]
use std::io::{Seek, Write};

pub struct PartitionedIndexWriter {
    relative_file_pos: u64,

    compression: CompressionType,
    restart_interval: u8,

    /// Whether zstd levels 19-22 run the `btultra2` two-pass seed. Defaults to
    /// `true`, the codec's own behaviour.
    #[cfg(zstd_any)]
    zstd_two_pass_seed: bool,

    tli_handles: Vec<KeyedBlockHandle>,
    /// Bytes the top-level index entries hold: each handle and its end key.
    tli_bytes: usize,
    /// Bytes the top-level index entries take encoded, bounded from above.
    tli_encoded: usize,
    data_block_handles: Vec<KeyedBlockHandle>,
    /// Bytes the open partition's entries take encoded, bounded from above.
    open_encoded: usize,

    buffer_size: u32,
    partition_size: u32,

    index_block_count: usize,

    block_buffer: Vec<u8>,

    final_write_buffer: Vec<u8>,

    encryption: Option<Arc<dyn EncryptionProvider>>,

    /// Owning SST's table id. Set by the outer Writer via
    /// `use_table_id` before `cut_index_block` / `finish` runs.
    table_id: crate::TableId,

    /// `Config::page_ecc` threaded by the outer Writer via
    /// `use_page_ecc`. When `true`, every `BlockTransform` this
    /// index writer constructs for the sub-index + TLI blocks
    /// upgrades to its matching `*Ecc` variant.
    ecc: Option<crate::table::block::EccParams>,
}

impl PartitionedIndexWriter {
    pub fn new() -> Self {
        Self {
            relative_file_pos: 0,
            buffer_size: 0,
            index_block_count: 0,

            partition_size: 4_096,
            compression: CompressionType::None,
            restart_interval: 1,
            #[cfg(zstd_any)]
            zstd_two_pass_seed: true,

            tli_handles: Vec::new(),
            tli_bytes: 0,
            tli_encoded: 0,
            data_block_handles: Vec::new(),
            open_encoded: 0,
            block_buffer: Vec::with_capacity(4_096),

            final_write_buffer: Vec::new(),

            encryption: None,
            table_id: 0,
            ecc: None,
        }
    }

    /// The top-level index's encoded bytes once `finish` cuts the open
    /// partition, which adds an entry under its last key.
    fn tli_at_finish(&self) -> usize {
        self.data_block_handles
            .last()
            .map_or(self.tli_encoded, |last| {
                self.tli_encoded + last.encoded_len_bound_unplaced()
            })
    }

    fn cut_index_block(&mut self) -> crate::Result<()> {
        let mut bytes = vec![];
        IndexBlock::encode_into_with_restart_interval(
            &mut bytes,
            &self.data_block_handles,
            self.restart_interval,
        )?;

        let header = Block::write_into(
            &mut self.block_buffer,
            &bytes,
            crate::table::block::BlockIdentity {
                table_id: self.table_id,
                block_type: crate::table::block::BlockType::Index,
                dict_id: 0,
                window_log: 0,
            },
            // Index blocks (sub-blocks and TLI) use the configured
            // codec but never a zstd dict (dicts are trained on
            // data, not index structures). page_ecc upgrades the
            // transform to its matching `*Ecc` variant so index
            // blocks get the same Reed-Solomon parity trailer
            // that data blocks do (no-op without the feature).
            &{
                let t = crate::table::block::BlockTransform::from_parts(
                    self.compression,
                    self.encryption.as_deref(),
                    #[cfg(zstd_any)]
                    None,
                )?;
                #[cfg(zstd_any)]
                let t = t.with_two_pass_seed(self.zstd_two_pass_seed);
                if let Some(ecc) = self.ecc {
                    t.with_ecc(ecc)
                } else {
                    t
                }
            },
            // Framed ahead of its place in the file; `finish` binds it.
            crate::table::block::ChecksumAt::Unbound,
        )?;

        let bytes_written = header.on_disk_size_with(self.ecc);

        // Also, we are allowed to remove the last item
        // to get ownership of it, because the chunk is cleared after
        // this anyway
        #[expect(clippy::expect_used, reason = "chunk is not empty")]
        let last = self
            .data_block_handles
            .pop()
            .expect("Chunk should not be empty");

        let index_block_handle = KeyedBlockHandle::new(
            last.end_key().clone(),
            last.seqno(),
            BlockHandle::new(BlockOffset(self.relative_file_pos), bytes_written),
        );

        log::trace!(
            "Built Bloom filter partition ({bytes_written}B) with end_key={:?} at +{:#X?}",
            last.end_key(),
            self.relative_file_pos,
        );

        self.tli_bytes +=
            core::mem::size_of::<KeyedBlockHandle>() + index_block_handle.end_key().len();
        // Its offset is relative until `finish` shifts it into the file.
        self.tli_encoded += index_block_handle.encoded_len_bound_unplaced();
        self.tli_handles.push(index_block_handle);
        self.final_write_buffer.append(&mut self.block_buffer);

        // Adjust metadata
        self.index_block_count += 1;
        self.relative_file_pos += u64::from(bytes_written);

        // IMPORTANT: Clear buffer after everything else
        self.data_block_handles.clear();
        self.buffer_size = 0;
        self.open_encoded = 0;

        Ok(())
    }

    fn write_top_level_index<WR: Write + Seek>(
        &mut self,
        file_writer: &mut crate::sfa::Writer<ChecksummedWriter<WR>>,
        index_base_offset: BlockOffset,
    ) -> crate::Result<Vec<u8>> {
        file_writer.start("tli")?;

        for item in &mut self.tli_handles {
            item.shift(index_base_offset);
        }

        let mut bytes = vec![];
        IndexBlock::encode_into_with_restart_interval(
            &mut bytes,
            &self.tli_handles,
            self.restart_interval,
        )?;

        let at = super::super::next_block_at(self.table_id, file_writer);
        let header = Block::write_into(
            file_writer,
            &bytes,
            crate::table::block::BlockIdentity {
                table_id: self.table_id,
                block_type: crate::table::block::BlockType::Index,
                dict_id: 0,
                window_log: 0,
            },
            // Index blocks (sub-blocks and TLI) use the configured
            // codec but never a zstd dict (dicts are trained on
            // data, not index structures). page_ecc upgrades the
            // transform to its matching `*Ecc` variant so index
            // blocks get the same Reed-Solomon parity trailer
            // that data blocks do (no-op without the feature).
            &{
                let t = crate::table::block::BlockTransform::from_parts(
                    self.compression,
                    self.encryption.as_deref(),
                    #[cfg(zstd_any)]
                    None,
                )?;
                #[cfg(zstd_any)]
                let t = t.with_two_pass_seed(self.zstd_two_pass_seed);
                if let Some(ecc) = self.ecc {
                    t.with_ecc(ecc)
                } else {
                    t
                }
            },
            at,
        )?;

        let bytes_written = header.on_disk_size_with(self.ecc);

        debug_assert!(bytes_written > 0, "Top level index should never be empty");

        log::trace!(
            "Written top level index, with {} pointers ({bytes_written} bytes)",
            self.tli_handles.len(),
        );

        Ok(bytes)
    }
}

#[cfg(test)]
mod tests;

impl<W: crate::io::Write + crate::io::Seek> BlockIndexWriter<W> for PartitionedIndexWriter {
    fn use_encryption(
        mut self: Box<Self>,
        encryption: Option<Arc<dyn EncryptionProvider>>,
    ) -> Box<dyn BlockIndexWriter<W>> {
        self.encryption = encryption;
        self
    }

    fn use_partition_size(mut self: Box<Self>, size: u32) -> Box<dyn BlockIndexWriter<W>> {
        self.partition_size = size;
        self
    }

    fn use_restart_interval(mut self: Box<Self>, interval: u8) -> Box<dyn BlockIndexWriter<W>> {
        self.restart_interval = interval;
        self
    }

    fn use_table_id(mut self: Box<Self>, table_id: crate::TableId) -> Box<dyn BlockIndexWriter<W>> {
        self.table_id = table_id;
        self
    }

    fn use_ecc(
        mut self: Box<Self>,
        ecc: Option<crate::table::block::EccParams>,
    ) -> Box<dyn BlockIndexWriter<W>> {
        self.ecc = ecc;
        self
    }

    fn use_compression(
        mut self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn BlockIndexWriter<W>> {
        self.compression = compression;
        self
    }

    #[cfg(zstd_any)]
    fn use_zstd_two_pass_seed(mut self: Box<Self>, enabled: bool) -> Box<dyn BlockIndexWriter<W>> {
        self.zstd_two_pass_seed = enabled;
        self
    }

    fn register_data_block(&mut self, block_handle: KeyedBlockHandle) -> crate::Result<()> {
        log::trace!(
            "Registering block at {:?} with size {} [end_key={:?}]",
            block_handle.offset(),
            block_handle.size(),
            block_handle.end_key(),
        );

        #[expect(
            clippy::cast_possible_truncation,
            reason = "key is u16 max, so we can not exceed u32::MAX"
        )]
        let block_handle_size =
            (block_handle.end_key().len() + core::mem::size_of::<KeyedBlockHandle>()) as u32;

        self.buffer_size += block_handle_size;
        self.open_encoded += block_handle.encoded_len_bound();

        self.data_block_handles.push(block_handle);

        if self.buffer_size >= self.partition_size {
            self.cut_index_block()?;
        }

        Ok(())
    }

    fn held_bytes(&self) -> u64 {
        // The cut partitions stay buffered until `finish` writes them.
        (self.final_write_buffer.capacity()
            + self.block_buffer.capacity()
            + super::handles_held(
                self.buffer_size as usize,
                &self.data_block_handles,
                self.data_block_handles.capacity(),
            )
            + super::handles_held(
                self.tli_bytes,
                &self.tli_handles,
                self.tli_handles.capacity(),
            )) as u64
    }

    fn finish_scratch_bytes(&self) -> u64 {
        use crate::table::block::{BlockType, framed_len_bound, transform_scratch_bound};
        let transform = |payload: usize| {
            transform_scratch_bound(
                payload as u64,
                BlockType::Index,
                self.compression,
                self.encryption.as_deref(),
                self.ecc,
            )
        };
        // `finish` encodes the open partition and frames it, then appends the
        // frame to the partition buffer, which reallocates when it outgrows
        // its capacity and holds both copies while it moves.
        let open = if self.data_block_handles.is_empty() {
            0
        } else {
            let frame = framed_len_bound(
                self.open_encoded as u64,
                BlockType::Index,
                self.compression,
                self.encryption.as_deref(),
                self.ecc,
            );
            let needed = self.final_write_buffer.len() as u64 + frame;
            let capacity = self.final_write_buffer.capacity() as u64;
            let growth = if needed > capacity {
                needed.max(2 * capacity)
            } else {
                0
            };
            self.open_encoded as u64 + frame + growth
        };
        // Then the top-level index, which the table keeps until it writes the
        // tail mirror, and its framed copy.
        let tli = self.tli_at_finish();
        open + tli as u64 + transform(tli)
    }

    fn finish_output_bytes(&self) -> u64 {
        use crate::table::block::{BlockType, framed_len_bound};
        let frame = |payload: usize| {
            framed_len_bound(
                payload as u64,
                BlockType::Index,
                self.compression,
                self.encryption.as_deref(),
                self.ecc,
            )
        };
        // The cut partitions are framed already. `finish` cuts the open one,
        // and the table writes the top-level index twice, at the head and as
        // the tail mirror.
        let open = if self.data_block_handles.is_empty() {
            0
        } else {
            frame(self.open_encoded)
        };
        let tli = self.tli_at_finish();
        if tli == 0 {
            return self.final_write_buffer.len() as u64;
        }
        self.final_write_buffer.len() as u64 + open + 2 * frame(tli)
    }

    fn finish(
        mut self: Box<Self>,
        file_writer: &mut crate::sfa::Writer<ChecksummedWriter<W>>,
    ) -> crate::Result<(usize, Vec<u8>)> {
        if self.buffer_size > 0 {
            self.cut_index_block()?;
        }

        let index_base_offset = BlockOffset(file_writer.get_mut().stream_position()?);

        // The partitions were framed before their place in the file was
        // known: bind each one's checksum to where it lands now.
        for handle in &self.tli_handles {
            let relative = *handle.offset();
            let frame = usize::try_from(relative)
                .ok()
                .and_then(|at| self.final_write_buffer.get_mut(at..))
                .ok_or(crate::Error::InvalidHeader(
                    "index partition outside its buffer",
                ))?;
            crate::table::block::Header::rebind_frame(
                frame,
                crate::table::block::ChecksumAt::Unbound,
                crate::table::block::ChecksumAt::table(
                    self.table_id,
                    *index_base_offset + relative,
                ),
            )?;
        }

        file_writer.start("index")?;
        file_writer.write_all(&self.final_write_buffer)?;
        log::trace!("Concatted index partitions onto blocks file");

        let tli_bytes = self.write_top_level_index(file_writer, index_base_offset)?;

        Ok((self.index_block_count, tli_bytes))
    }
}
