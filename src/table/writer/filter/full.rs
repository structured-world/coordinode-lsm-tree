// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::FilterWriter;
use crate::{
    CompressionType, UserKey,
    checksum::ChecksummedWriter,
    config::BloomConstructionPolicy,
    encryption::EncryptionProvider,
    prefix::PrefixExtractor,
    table::{Block, filter::build_burr_filter_bytes},
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

pub struct FullFilterWriter {
    /// Key hashes for AMQ filter
    pub bloom_hash_buffer: Vec<u64>,

    bloom_policy: BloomConstructionPolicy,

    prefix_extractor: Option<Arc<dyn PrefixExtractor>>,

    encryption: Option<Arc<dyn EncryptionProvider>>,

    /// Owning SST's table id. Set by the outer Writer via
    /// `use_table_id` before `finish` runs.
    table_id: crate::TableId,

    /// Page ECC scheme threaded by the outer Writer via `use_ecc`.
    /// `Some(params)` upgrades the filter block's `BlockTransform` to
    /// the matching `*Ecc` variant; `None` = no parity.
    ecc: Option<crate::table::block::EccParams>,
}

impl FullFilterWriter {
    pub fn new(bloom_policy: BloomConstructionPolicy) -> Self {
        Self {
            bloom_hash_buffer: Vec::new(),
            bloom_policy,
            prefix_extractor: None,
            encryption: None,
            table_id: 0,
            ecc: None,
        }
    }
}

impl<W: crate::io::Write + crate::io::Seek> FilterWriter<W> for FullFilterWriter {
    fn use_partition_size(self: Box<Self>, _: u32) -> Box<dyn FilterWriter<W>> {
        self
    }

    fn use_tli_compression(self: Box<Self>, _: CompressionType) -> Box<dyn FilterWriter<W>> {
        self
    }

    // A full filter writes one block under the transform the caller hands it
    // and compresses nothing itself, so there is no encoder here to configure.
    #[cfg(zstd_any)]
    fn use_zstd_two_pass_seed(self: Box<Self>, _: bool) -> Box<dyn FilterWriter<W>> {
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

    fn use_ecc(
        mut self: Box<Self>,
        ecc: Option<crate::table::block::EccParams>,
    ) -> Box<dyn FilterWriter<W>> {
        self.ecc = ecc;
        self
    }

    fn register_key(&mut self, key: &UserKey) -> crate::Result<()> {
        self.bloom_hash_buffer.push(crate::hash::hash64(key));

        // Prefix hashes are pushed as they come; `finish` sorts and dedups
        // the buffer once, which is cheaper than keeping a set here on the
        // per-key write path.
        if let Some(extractor) = &self.prefix_extractor {
            for prefix in extractor.prefixes(key.as_ref()) {
                self.bloom_hash_buffer.push(crate::hash::hash64(prefix));
            }
        }

        Ok(())
    }

    fn finish(
        self: Box<Self>,
        file_writer: &mut crate::sfa::Writer<ChecksummedWriter<W>>,
    ) -> crate::Result<usize> {
        if self.bloom_hash_buffer.is_empty() {
            log::trace!("Filter writer has no buffered hashes - not building filter");
            return Ok(0);
        }

        // A prefix extractor emits one token per prefix per key, and prefixes
        // repeat across neighbouring keys by construction — that is what a
        // prefix is. Duplicates add no members, so deduplicating costs a sort
        // and removes real tokens: fewer slots to solve, a smaller payload,
        // and a faster build. Under the row-major layout the saving was in
        // slots only; now every removed token would also have cost `r` bits.
        //
        // Gated on the extractor being configured. Without one, `register_key`
        // pushes exactly one hash per key over a key set that is already
        // unique, so a duplicate needs a 64-bit hash collision — the sort
        // would be pure cost on the write path.
        let has_prefix_tokens = self.prefix_extractor.is_some();
        let mut hashes = self.bloom_hash_buffer;
        let raw = hashes.len();
        if has_prefix_tokens {
            hashes.sort_unstable();
            hashes.dedup();
        }
        let n = hashes.len();

        log::trace!(
            "Constructing BuRR filter with {n} entries ({raw} before dedup): {:?}",
            self.bloom_policy,
        );

        // no-std: caller-provided Clock trait (timing is trace-only here)
        #[cfg(feature = "std")]
        let start = std::time::Instant::now();
        // Build BEFORE opening the archive section. An invalid policy
        // can produce empty bytes; opening start("filter") and then
        // bailing out would leave an empty unfinished section in the
        // output and desynchronise the reported block count from what
        // was actually written.
        // `finish` consumes `Box<Self>`, so we can move `bloom_hash_buffer`
        // into the BuRR builder directly — no `to_vec()` clone.
        let filter_bytes = build_burr_filter_bytes(self.bloom_policy, hashes)?;

        if filter_bytes.is_empty() {
            log::trace!("BuRR policy produced empty filter — skipping block write");
            return Ok(0);
        }

        file_writer.start("filter")?;

        #[cfg(feature = "std")]
        log::trace!(
            "Built BuRR filter ({}B) in {:?}",
            filter_bytes.len(),
            start.elapsed(),
        );
        #[cfg(not(feature = "std"))]
        log::trace!("Built BuRR filter ({}B)", filter_bytes.len());

        Block::write_into(
            file_writer,
            &filter_bytes,
            crate::table::block::BlockIdentity {
                table_id: self.table_id,
                block_type: crate::table::block::BlockType::Filter,
                dict_id: 0,
                window_log: 0,
            },
            // Filter blocks are always written uncompressed; the
            // transform is Plain or Encrypted depending on the
            // configured provider, plus `with_ecc` when the tree
            // was opened with `Config::page_ecc(true)`.
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

        Ok(1)
    }
}
