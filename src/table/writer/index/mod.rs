// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

mod full;
mod partitioned;

pub use full::FullIndexWriter;
pub use partitioned::PartitionedIndexWriter;

use crate::{
    CompressionType, checksum::ChecksummedWriter, encryption::EncryptionProvider,
    table::index_block::KeyedBlockHandle,
};
use std::sync::Arc;

pub trait BlockIndexWriter<W: std::io::Write + std::io::Seek> {
    /// Registers a data block in the block index.
    fn register_data_block(&mut self, block_handle: KeyedBlockHandle) -> crate::Result<()>;

    /// Writes the block index to a file.
    ///
    /// Returns the number of index blocks written and the raw
    /// IndexBlock-encoded top-level-index bytes (uncompressed,
    /// unencrypted). The outer table writer re-encodes these bytes
    /// into a `tli_tail` mirror section near the file tail so that a
    /// torn-write or bad sector at the head TLI position is
    /// recoverable. The buffer reflects post-shift handles (matching
    /// what `tli` head section encodes), so head and tail decode to
    /// the same logical TLI.
    fn finish(
        self: Box<Self>,
        file_writer: &mut sfa::Writer<ChecksummedWriter<W>>,
    ) -> crate::Result<(usize, Vec<u8>)>;

    fn use_compression(
        self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn BlockIndexWriter<W>>;

    // No default: `Box<Self> -> Box<dyn>` requires `Sized` which would break
    // object safety. Same constraint applies to all `use_*` builder methods.
    fn use_restart_interval(self: Box<Self>, interval: u8) -> Box<dyn BlockIndexWriter<W>>;

    fn use_partition_size(self: Box<Self>, size: u32) -> Box<dyn BlockIndexWriter<W>>;

    /// Sets the encryption provider for index blocks.
    fn use_encryption(
        self: Box<Self>,
        encryption: Option<Arc<dyn EncryptionProvider>>,
    ) -> Box<dyn BlockIndexWriter<W>>;

    /// Sets the owning table id. Used by `finish()` to populate
    /// `BlockIdentity::table_id` when writing index blocks via the
    /// Block I/O API. MUST be called by the Writer that owns this
    /// index writer before `finish()`, otherwise the written
    /// blocks bind to `table_id = 0` and the block-swap defence
    /// degrades to "any table can substitute".
    fn use_table_id(self: Box<Self>, table_id: crate::TableId) -> Box<dyn BlockIndexWriter<W>>;

    /// Wires the tree's `Config::page_ecc` flag through to every
    /// `Block::write_into` call this index writer makes. When
    /// `true`, the `BlockTransform` passed to `write_into` gets
    /// `.with_ecc()` applied so the matching `*Ecc` variant emits
    /// a Reed-Solomon parity trailer.
    fn use_page_ecc(self: Box<Self>, page_ecc: bool) -> Box<dyn BlockIndexWriter<W>>;
}

// FilterWriter mirrors the use_page_ecc pattern via its own trait
// method declared in `super::filter` — see
// `super::filter::FilterWriter::use_page_ecc`.
