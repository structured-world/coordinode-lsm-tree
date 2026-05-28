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
    table::{Block, filter::build_burr_filter_bytes},
};
use std::sync::Arc;

pub struct FullFilterWriter {
    /// Key hashes for AMQ filter
    pub bloom_hash_buffer: Vec<u64>,

    bloom_policy: BloomConstructionPolicy,

    prefix_extractor: Option<Arc<dyn PrefixExtractor>>,

    encryption: Option<Arc<dyn EncryptionProvider>>,

    /// Owning SST's table id. Set by the outer Writer via
    /// `use_table_id` before `finish` runs.
    table_id: crate::TableId,

    /// `Config::page_ecc` threaded by the outer Writer via
    /// `use_page_ecc`. When `true`, the filter block this writer
    /// emits upgrades its `BlockTransform` to the matching `*Ecc`
    /// variant.
    page_ecc: bool,
}

impl FullFilterWriter {
    pub fn new(bloom_policy: BloomConstructionPolicy) -> Self {
        Self {
            bloom_hash_buffer: Vec::new(),
            bloom_policy,
            prefix_extractor: None,
            encryption: None,
            table_id: 0,
            page_ecc: false,
        }
    }
}

impl<W: std::io::Write + std::io::Seek> FilterWriter<W> for FullFilterWriter {
    fn use_partition_size(self: Box<Self>, _: u32) -> Box<dyn FilterWriter<W>> {
        self
    }

    fn use_tli_compression(self: Box<Self>, _: CompressionType) -> Box<dyn FilterWriter<W>> {
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

    fn use_page_ecc(mut self: Box<Self>, page_ecc: bool) -> Box<dyn FilterWriter<W>> {
        self.page_ecc = page_ecc;
        self
    }

    fn register_key(&mut self, key: &UserKey) -> crate::Result<()> {
        self.bloom_hash_buffer.push(crate::hash::hash64(key));

        // Prefix hashes are intentionally not deduplicated. The filter
        // treats each hash as an independent membership token; duplicates
        // inflate the entry count but keep construction simple and lower
        // effective FPR slightly.
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

        let n = self.bloom_hash_buffer.len();

        log::trace!(
            "Constructing BuRR filter with {n} entries: {:?}",
            self.bloom_policy,
        );

        let start = std::time::Instant::now();
        // Build BEFORE opening the archive section. An invalid policy
        // can produce empty bytes; opening start("filter") and then
        // bailing out would leave an empty unfinished section in the
        // output and desynchronise the reported block count from what
        // was actually written.
        // `finish` consumes `Box<Self>`, so we can move `bloom_hash_buffer`
        // into the BuRR builder directly — no `to_vec()` clone.
        let filter_bytes = build_burr_filter_bytes(self.bloom_policy, self.bloom_hash_buffer)?;

        if filter_bytes.is_empty() {
            log::trace!("BuRR policy produced empty filter — skipping block write");
            return Ok(0);
        }

        file_writer.start("filter")?;

        log::trace!(
            "Built BuRR filter ({}B) in {:?}",
            filter_bytes.len(),
            start.elapsed(),
        );

        Block::write_into(
            file_writer,
            &filter_bytes,
            crate::table::block::BlockIdentity {
                tree_id: 0,
                table_id: self.table_id,
                block_offset: 0,
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
                if self.page_ecc { t.with_ecc() } else { t }
            },
        )?;

        Ok(1)
    }
}
