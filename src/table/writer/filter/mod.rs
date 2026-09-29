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
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

/// What a filter writer wrote into its table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FilterOutput {
    /// Filter blocks written: one for a full filter, one per partition.
    pub blocks: usize,
    /// Hashes the filters hold: a key's, and under a prefix extractor each
    /// distinct prefix's. Zero when no filter was written.
    pub hashes: u64,
}

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

    /// Writes the filter to a file, and returns what it wrote.
    fn finish(
        self: Box<Self>,
        file_writer: &mut crate::sfa::Writer<ChecksummedWriter<W>>,
    ) -> crate::Result<FilterOutput>;

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

    /// Chooses each filter's width by probe load against the tree's filter
    /// budget instead of building every one at the filter policy. `None`
    /// builds at the policy.
    fn use_sizing(
        self: Box<Self>,
        sizing: Option<crate::filter_budget::FilterPlan>,
    ) -> Box<dyn FilterWriter<W>>;

    /// The table's first and last key, given before [`finish`](Self::finish)
    /// so a sized filter knows the key range its load is drawn over.
    fn set_key_range(&mut self, first: &UserKey, last: &UserKey);
}

/// The on-disk bytes of an uncompressed filter block of `len` payload bytes.
fn framed_filter_len(
    len: u64,
    encryption: Option<&dyn EncryptionProvider>,
    ecc: Option<crate::table::block::EccParams>,
) -> u64 {
    crate::table::block::framed_len_bound(
        len,
        crate::table::block::BlockType::Filter,
        CompressionType::None,
        encryption,
        ecc,
    )
}

/// A filter budget plan, and the key range of the filter it sizes.
type SizedFilter<'a> = (
    &'a crate::filter_budget::FilterSizing,
    (core::ops::Bound<&'a [u8]>, core::ops::Bound<&'a [u8]>),
);

/// Builds the filter over `hashes`: at `policy`, or, when the tree sizes its
/// filters, at the first candidate width over the keys in `bounds` whose
/// encoded block the filter budget admits.
fn build_filter(
    policy: BloomConstructionPolicy,
    sizing: Option<SizedFilter<'_>>,
    hashes: Vec<u64>,
    encryption: Option<&dyn EncryptionProvider>,
    ecc: Option<crate::table::block::EccParams>,
) -> crate::Result<Vec<u8>> {
    let Some((sizing, bounds)) = sizing else {
        return crate::table::filter::build_burr_filter_bytes(policy, hashes);
    };
    let n = hashes.len();
    let frame = |len: u64| framed_filter_len(len, encryption, ecc);
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a non-negative byte count of one filter"
    )]
    let estimated =
        |policy: BloomConstructionPolicy| frame(libm::round(policy.expected_filter_size(n)) as u64);
    let mut candidates = sizing.candidates(bounds, n)?;
    let Some(narrowest) = candidates.pop() else {
        return crate::table::filter::build_burr_filter_bytes(policy, hashes);
    };
    for candidate in candidates {
        // A refused build is retried narrower, so each attempt but the last
        // builds from a copy.
        let bytes = crate::table::filter::build_burr_filter_bytes(candidate, hashes.clone())?;
        if sizing.admit(
            bounds.0,
            n,
            frame(bytes.len() as u64),
            estimated(candidate),
            &frame,
            false,
        ) {
            return Ok(bytes);
        }
    }
    let bytes = crate::table::filter::build_burr_filter_bytes(narrowest, hashes)?;
    // The narrowest is taken whether or not it fits.
    let admitted = sizing.admit(
        bounds.0,
        n,
        frame(bytes.len() as u64),
        estimated(narrowest),
        &frame,
        true,
    );
    debug_assert!(admitted, "the last candidate is always taken");
    Ok(bytes)
}
