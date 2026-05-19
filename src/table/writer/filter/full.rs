// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

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
}

impl FullFilterWriter {
    pub fn new(bloom_policy: BloomConstructionPolicy) -> Self {
        Self {
            bloom_hash_buffer: Vec::new(),
            bloom_policy,
            prefix_extractor: None,
            encryption: None,
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
        file_writer: &mut sfa::Writer<ChecksummedWriter<W>>,
    ) -> crate::Result<usize> {
        if self.bloom_hash_buffer.is_empty() {
            log::trace!("Filter writer has no buffered hashes - not building filter");
        } else {
            let n = self.bloom_hash_buffer.len();

            log::trace!(
                "Constructing BuRR filter with {n} entries: {:?}",
                self.bloom_policy,
            );

            let start = std::time::Instant::now();
            // Build BEFORE opening the archive section. An invalid
            // policy can produce empty bytes; opening start("filter")
            // and then bailing out would leave an empty unfinished
            // section in the output and desynchronise the reported
            // block count from what was actually written.
            let filter_bytes = build_burr_filter_bytes(self.bloom_policy, &self.bloom_hash_buffer)?;

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
                crate::table::block::BlockType::Filter,
                CompressionType::None,
                self.encryption.as_deref(),
                #[cfg(zstd_any)]
                None,
            )?;
        }

        Ok(1)
    }
}
