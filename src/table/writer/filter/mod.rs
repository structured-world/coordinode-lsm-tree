// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

mod full;
mod partitioned;

pub use full::FullFilterWriter;
pub use partitioned::PartitionedFilterWriter;

use crate::{
    CompressionType, UserKey, checksum::ChecksummedWriter, config::BloomConstructionPolicy,
    encryption::EncryptionProvider, prefix::PrefixExtractor,
};
#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
use alloc::sync::Arc;

// All methods are required (no defaults) by design so that implementations must
// explicitly handle configuration changes (e.g., filter policies, prefix extractors).
pub trait FilterWriter<W: crate::io::Write + crate::io::Seek> {
    // NOTE: We purposefully use a UserKey instead of &[u8]
    // so we can clone it without heap allocation, if needed
    /// Registers a key in the filter, and returns the hashes it buffered for
    /// it: one, and one a new prefix under a prefix extractor.
    fn register_key(&mut self, key: &UserKey) -> crate::Result<usize>;

    /// Heap bytes this writer holds until [`finish`](Self::finish).
    fn held_bytes(&self) -> u64;

    /// Heap bytes [`finish`](Self::finish) allocates on top of
    /// [`held_bytes`](Self::held_bytes) while it builds, at its peak.
    fn finish_scratch_bytes(&self) -> u64;

    /// Bytes [`finish`](Self::finish) will append to the table file,
    /// estimated from what is held now.
    fn finish_output_bytes(&self) -> u64;

    /// Writes the filter to a file.
    ///
    /// Returns the number of filter blocks written (always 1 in case of full filter block).
    fn finish(
        self: Box<Self>,
        file_writer: &mut crate::sfa::Writer<ChecksummedWriter<W>>,
    ) -> crate::Result<usize>;

    fn set_filter_policy(
        self: Box<Self>,
        policy: BloomConstructionPolicy,
    ) -> Box<dyn FilterWriter<W>>;

    fn use_tli_compression(
        self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn FilterWriter<W>>;

    /// Selects whether zstd levels 19-22 run the `btultra2` two-pass seed for
    /// the blocks this writer compresses. See
    /// [`crate::runtime_config::RuntimeConfig::zstd_two_pass_seed`].
    #[cfg(zstd_any)]
    fn use_zstd_two_pass_seed(self: Box<Self>, enabled: bool) -> Box<dyn FilterWriter<W>>;

    fn use_partition_size(self: Box<Self>, size: u32) -> Box<dyn FilterWriter<W>>;

    fn set_prefix_extractor(
        self: Box<Self>,
        extractor: Option<Arc<dyn PrefixExtractor>>,
    ) -> Box<dyn FilterWriter<W>>;

    /// Sets the encryption provider for filter blocks.
    fn use_encryption(
        self: Box<Self>,
        encryption: Option<Arc<dyn EncryptionProvider>>,
    ) -> Box<dyn FilterWriter<W>>;

    /// Sets the owning table id. Used by `finish()` to populate
    /// `BlockIdentity::table_id` when writing filter blocks via
    /// the Block I/O API. MUST be called by the Writer that owns
    /// this filter writer before `finish()`, otherwise the
    /// written blocks bind to `table_id = 0`.
    fn use_table_id(self: Box<Self>, table_id: crate::TableId) -> Box<dyn FilterWriter<W>>;

    /// Wires the resolved Page ECC scheme through to every
    /// `Block::write_into` call this filter writer makes. `Some(params)`
    /// applies `.with_ecc(params)` so the matching `*Ecc` variant emits
    /// the parity trailer; `None` = no parity.
    fn use_ecc(
        self: Box<Self>,
        ecc: Option<crate::table::block::EccParams>,
    ) -> Box<dyn FilterWriter<W>>;

    /// Builds this writer's filter blocks on `parallel`'s workers, published
    /// in the order they were spilled; `None` builds them on the writer's
    /// thread. A writer with a single filter built at `finish` has nothing to
    /// hand out and keeps building it there.
    #[cfg(feature = "std")]
    fn use_parallel(
        self: Box<Self>,
        parallel: Option<crate::table::writer::ParallelCompression>,
    ) -> Box<dyn FilterWriter<W>>;
}
