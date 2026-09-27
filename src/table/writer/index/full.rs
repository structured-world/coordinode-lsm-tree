// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::{
    CompressionType,
    checksum::ChecksummedWriter,
    encryption::EncryptionProvider,
    table::{Block, IndexBlock, index_block::KeyedBlockHandle, writer::index::BlockIndexWriter},
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

pub struct FullIndexWriter {
    compression: CompressionType,

    /// Whether zstd levels 19-22 run the `btultra2` two-pass seed. Defaults to
    /// `true`, the codec's own behaviour.
    #[cfg(zstd_any)]
    zstd_two_pass_seed: bool,
    restart_interval: u8,
    block_handles: Vec<KeyedBlockHandle>,
    /// Bytes the handles hold: each handle and its end key.
    handle_bytes: usize,
    /// Bytes the handles take encoded, bounded from above.
    encoded_bytes: usize,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    /// Owning SST's table id; passed by the outer Writer via
    /// `use_table_id` before `finish()`. Used to populate
    /// `BlockIdentity::table_id` when writing the top-level
    /// index block.
    table_id: crate::TableId,
    /// Page ECC scheme threaded by the outer Writer via `use_ecc`.
    /// `Some(params)` makes the top-level-index block emit upgrade to
    /// its matching `*Ecc` variant so the index block gets the same
    /// parity scheme as data blocks; `None` = no parity.
    ecc: Option<crate::table::block::EccParams>,
}

impl FullIndexWriter {
    pub fn new() -> Self {
        Self {
            compression: CompressionType::None,
            #[cfg(zstd_any)]
            zstd_two_pass_seed: true,
            restart_interval: 1,
            block_handles: Vec::new(),
            handle_bytes: 0,
            encoded_bytes: 0,
            encryption: None,
            table_id: 0,
            ecc: None,
        }
    }

    /// A writer over handles already collected, which hold `handle_bytes` and
    /// take `encoded_bytes` encoded.
    pub(super) fn with_handles(
        block_handles: Vec<KeyedBlockHandle>,
        handle_bytes: usize,
        encoded_bytes: usize,
    ) -> Self {
        Self {
            block_handles,
            handle_bytes,
            encoded_bytes,
            ..Self::new()
        }
    }
}

impl<W: crate::io::Write + crate::io::Seek> BlockIndexWriter<W> for FullIndexWriter {
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

    #[cfg(zstd_any)]
    fn use_zstd_two_pass_seed(mut self: Box<Self>, enabled: bool) -> Box<dyn BlockIndexWriter<W>> {
        self.zstd_two_pass_seed = enabled;
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

    fn register_data_block(&mut self, block_handle: KeyedBlockHandle) -> crate::Result<()> {
        log::trace!(
            "Registering block at {:?} with size {} [end_key={:?}]",
            block_handle.offset(),
            block_handle.size(),
            block_handle.end_key(),
        );

        self.handle_bytes +=
            core::mem::size_of::<KeyedBlockHandle>() + block_handle.end_key().len();
        self.encoded_bytes += block_handle.encoded_len_bound();
        self.block_handles.push(block_handle);

        Ok(())
    }

    fn held_bytes(&self) -> u64 {
        self.handle_bytes as u64
    }

    fn finish_scratch_bytes(&self) -> u64 {
        // `finish` encodes every handle into one block buffer, which the
        // table keeps until it writes the tail mirror.
        self.encoded_bytes as u64
    }

    fn finish_output_bytes(&self) -> u64 {
        // Written twice: at the head and as the tail mirror.
        (2 * self.encoded_bytes) as u64
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
                table_id: self.table_id,
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
                #[cfg(zstd_any)]
                let t = t.with_two_pass_seed(self.zstd_two_pass_seed);
                if let Some(ecc) = self.ecc {
                    t.with_ecc(ecc)
                } else {
                    t
                }
            },
        )?;

        let bytes_written = header.on_disk_size_with(self.ecc);

        debug_assert!(bytes_written > 0, "Block index should never be empty");

        log::trace!(
            "Written top level index, with {} pointers ({bytes_written}B)",
            self.block_handles.len(),
        );

        Ok((1, bytes))
    }
}
