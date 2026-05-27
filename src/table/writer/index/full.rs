// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{
    CompressionType,
    checksum::ChecksummedWriter,
    encryption::EncryptionProvider,
    table::{Block, IndexBlock, index_block::KeyedBlockHandle, writer::index::BlockIndexWriter},
};
use std::sync::Arc;

pub struct FullIndexWriter {
    compression: CompressionType,
    restart_interval: u8,
    block_handles: Vec<KeyedBlockHandle>,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    /// Owning SST's table id; passed by the outer Writer via
    /// `use_table_id` before `finish()`. Used to populate
    /// `BlockIdentity::table_id` when writing the top-level
    /// index block.
    table_id: crate::TableId,
    /// `Config::page_ecc` threaded by the outer Writer via
    /// `use_page_ecc`. When `true`, the `BlockTransform` used for
    /// the top-level-index block emit upgrades to its matching
    /// `*Ecc` variant so the index block gets the same
    /// Reed-Solomon parity trailer that data blocks do.
    page_ecc: bool,
}

impl FullIndexWriter {
    pub fn new() -> Self {
        Self {
            compression: CompressionType::None,
            restart_interval: 1,
            block_handles: Vec::new(),
            encryption: None,
            table_id: 0,
            page_ecc: false,
        }
    }
}

impl<W: std::io::Write + std::io::Seek> BlockIndexWriter<W> for FullIndexWriter {
    fn use_encryption(
        mut self: Box<Self>,
        encryption: Option<Arc<dyn EncryptionProvider>>,
    ) -> Box<dyn BlockIndexWriter<W>> {
        self.encryption = encryption;
        self
    }

    fn use_partition_size(self: Box<Self>, _: u32) -> Box<dyn BlockIndexWriter<W>> {
        self
    }

    fn use_restart_interval(mut self: Box<Self>, interval: u8) -> Box<dyn BlockIndexWriter<W>> {
        self.restart_interval = interval;
        self
    }

    fn use_compression(
        mut self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn BlockIndexWriter<W>> {
        self.compression = compression;
        self
    }

    fn use_table_id(mut self: Box<Self>, table_id: crate::TableId) -> Box<dyn BlockIndexWriter<W>> {
        self.table_id = table_id;
        self
    }

    fn use_page_ecc(mut self: Box<Self>, page_ecc: bool) -> Box<dyn BlockIndexWriter<W>> {
        self.page_ecc = page_ecc;
        self
    }

    fn register_data_block(&mut self, block_handle: KeyedBlockHandle) -> crate::Result<()> {
        log::trace!(
            "Registering block at {:?} with size {} [end_key={:?}]",
            block_handle.offset(),
            block_handle.size(),
            block_handle.end_key(),
        );

        self.block_handles.push(block_handle);

        Ok(())
    }

    fn finish(
        self: Box<Self>,
        file_writer: &mut crate::sfa::Writer<ChecksummedWriter<W>>,
    ) -> crate::Result<(usize, Vec<u8>)> {
        file_writer.start("tli")?;

        let mut bytes = vec![];
        IndexBlock::encode_into_with_restart_interval(
            &mut bytes,
            &self.block_handles,
            self.restart_interval,
        )?;

        let header = Block::write_into(
            file_writer,
            &bytes,
            crate::table::block::BlockIdentity {
                tree_id: 0,
                table_id: self.table_id,
                // crate::sfa::Writer doesn't expose its position cursor;
                // outer table Writer tracks meta.file_pos but the
                // BlockIndexWriter trait doesn't surface it here.
                // Leaving offset=0 intentionally — extending
                // BlockIndexWriter::finish to thread block_offset
                // alongside file_writer is a planned follow-up
                // (alongside the canary check that would refuse
                // this combination once both fields can be real).
                block_offset: 0,
                block_type: crate::table::block::BlockType::Index,
                dict_id: 0,
                window_log: 0,
            },
            // Index blocks use the configured codec but never a
            // zstd dict (dicts are trained on data, not index
            // structures). When the tree was opened with
            // `Config::page_ecc(true)`, upgrade the transform to
            // its matching `*Ecc` variant so the TLI block gets
            // a Reed-Solomon parity trailer alongside the data
            // blocks (no-op on builds without the page_ecc cargo
            // feature).
            &{
                let t = crate::table::block::BlockTransform::from_parts(
                    self.compression,
                    self.encryption.as_deref(),
                    #[cfg(zstd_any)]
                    None,
                )?;
                if self.page_ecc { t.with_ecc() } else { t }
            },
        )?;

        let bytes_written = header.on_disk_size();

        debug_assert!(bytes_written > 0, "Block index should never be empty");

        log::trace!(
            "Written top level index, with {} pointers ({bytes_written}B)",
            self.block_handles.len(),
        );

        Ok((1, bytes))
    }
}
