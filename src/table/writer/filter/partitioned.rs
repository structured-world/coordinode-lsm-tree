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
    table::{Block, BlockHandle, BlockOffset, IndexBlock, KeyedBlockHandle},
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};
use core::ops::Bound;

// Concrete writers (sfa::Writer / ChecksummedWriter) carry the io trait via
// their own impls; the defining trait must be in scope for raw method calls.
#[cfg(not(feature = "std"))]
use crate::io::{Seek, Write};
#[cfg(feature = "std")]
use std::io::{Seek, Write};

pub struct PartitionedFilterWriter {
    final_filter_buffer: Vec<u8>,

    tli_handles: Vec<KeyedBlockHandle>,

    /// Bytes the top-level index entries hold: each handle and its end key.
    tli_bytes: usize,

    /// Key hashes for AMQ filter
    pub bloom_hash_buffer: Vec<u64>,
    approx_filter_size: usize,

    partition_size: u32,

    bloom_policy: BloomConstructionPolicy,

    relative_file_pos: u64,

    last_key: Option<UserKey>,

    compression: CompressionType,

    /// Whether zstd levels 19-22 run the `btultra2` two-pass seed for the TLI
    /// block. Defaults to `true`, the codec's own behaviour.
    #[cfg(zstd_any)]
    zstd_two_pass_seed: bool,

    // Accepted to keep the FilterWriter API uniform — written by
    // set_prefix_extractor but not read (partitioned filters cannot be
    // probed by prefix hash; see Table::maybe_contains_prefix).
    prefix_extractor: Option<Arc<dyn PrefixExtractor>>,

    encryption: Option<Arc<dyn EncryptionProvider>>,

    /// Owning SST's table id. Set by the outer Writer via
    /// `use_table_id` before `spill_filter_partition` / `finish`.
    table_id: crate::TableId,

    /// Page ECC scheme threaded by the outer Writer via `use_ecc`.
    /// `Some(params)` upgrades every partition + TLI block transform to
    /// its matching `*Ecc` variant; `None` = no parity.
    ecc: Option<crate::table::block::EccParams>,

    /// Where partitions are built when the table is written in parallel.
    #[cfg(feature = "std")]
    parallel: Option<ParallelPartitions>,

    /// The partitions handed to workers and not yet published, oldest first.
    #[cfg(feature = "std")]
    pending: std::collections::VecDeque<PendingPartition>,

    /// Chooses each partition's width when the tree allocates filter memory
    /// by probe load; `bloom_policy` decides otherwise, and always decides
    /// where a partition splits.
    sizing: Option<crate::filter_budget::FilterPlan>,

    /// The table's first key, the lower end of the first partition's range.
    /// Kept only for a sized filter.
    first_key: Option<UserKey>,

    /// The last key of the partition spilled last, the lower end (excluded)
    /// of the next one's range; the top-level index lags it while
    /// partitions are on workers. Kept only for a sized filter.
    spilled_key: Option<UserKey>,

    /// Hashes the table's partitions hold so far, which `finish` reports.
    table_hashes: usize,

    /// Hashes of the largest partition so far, which `finish` reports.
    largest_partition: usize,
}

impl PartitionedFilterWriter {
    pub fn new(bloom_policy: BloomConstructionPolicy) -> Self {
        Self {
            final_filter_buffer: Vec::new(),

            bloom_hash_buffer: Vec::new(),
            approx_filter_size: 0,

            tli_handles: Vec::new(),
            tli_bytes: 0,
            partition_size: 4_096,
            bloom_policy,

            relative_file_pos: 0,

            last_key: None,

            compression: CompressionType::None,
            #[cfg(zstd_any)]
            zstd_two_pass_seed: true,

            prefix_extractor: None,

            encryption: None,
            table_id: 0,
            ecc: None,
            #[cfg(feature = "std")]
            parallel: None,
            #[cfg(feature = "std")]
            pending: std::collections::VecDeque::new(),
            sizing: None,
            first_key: None,
            spilled_key: None,
            table_hashes: 0,
            largest_partition: 0,
        }
    }

    /// A writer for table `table_id` that writes `partitions`, each the wire
    /// bytes of a filter over the keys up to and including its key and the
    /// hashes it holds, in key order, instead of building them. Framed under
    /// `encryption` and `ecc`, its top-level index compressed with
    /// `tli_compression`.
    ///
    /// # Errors
    ///
    /// Returns an error when framing a partition fails.
    pub fn prebuilt(
        partitions: Vec<(UserKey, Vec<u8>, u64)>,
        table_id: crate::TableId,
        encryption: Option<Arc<dyn EncryptionProvider>>,
        ecc: Option<crate::table::block::EccParams>,
        tli_compression: CompressionType,
    ) -> crate::Result<Self> {
        let mut writer = Self::new(BloomConstructionPolicy::default());
        writer.table_id = table_id;
        writer.encryption = encryption;
        writer.ecc = ecc;
        writer.compression = tli_compression;
        for (end_key, filter, hashes) in partitions {
            writer.push_prebuilt_partition(&end_key, &filter, hashes)?;
        }
        Ok(writer)
    }

    /// The widest partition a build may produce, which the size estimates
    /// bound.
    fn bound_policy(&self) -> BloomConstructionPolicy {
        self.sizing
            .as_ref()
            .map_or(self.bloom_policy, |sizing| sizing.bound_policy())
    }

    /// The top-level index's bytes once `finish` publishes the partitions
    /// still on workers and spills the open one, each adding an entry under
    /// its last key.
    fn tli_at_finish(&self) -> usize {
        let entry = |key: &UserKey| core::mem::size_of::<KeyedBlockHandle>() + key.len();
        #[cfg(feature = "std")]
        let pending: usize = self.pending.iter().map(|p| entry(&p.key)).sum();
        #[cfg(not(feature = "std"))]
        let pending = 0;
        let open = match &self.last_key {
            Some(last) if !self.bloom_hash_buffer.is_empty() => entry(last),
            _ => 0,
        };
        self.tli_bytes + pending + open
    }

    /// The open partition's filter bytes, bounded from above: what `finish`
    /// builds it into. The prediction that splits partitions is not a bound.
    fn open_partition_bound(&self) -> u64 {
        self.bound_policy()
            .filter_size_bound(self.bloom_hash_buffer.len()) as u64
    }

    /// The settings a partition is built and framed under.
    fn partition_settings(&self) -> PartitionSettings {
        PartitionSettings {
            bloom_policy: self.bloom_policy,
            sizing: self
                .sizing
                .as_ref()
                .map(crate::filter_budget::FilterPlan::sizing),
            table_id: self.table_id,
            encryption: self.encryption.clone(),
            ecc: self.ecc,
        }
    }

    /// The key range of the partition ending at `key`, which a sized
    /// partition picks its width over: past the previous partition's last
    /// key, or from the table's first key.
    fn partition_range(&self, key: &UserKey) -> PartitionRange {
        let lower = match (&self.spilled_key, &self.first_key) {
            (Some(previous), _) => Bound::Excluded(previous.clone()),
            (None, Some(first)) => Bound::Included(first.clone()),
            (None, None) => Bound::Unbounded,
        };
        (lower, key.clone())
    }

    /// Builds the open partition from `hashes`, taken out of
    /// `bloom_hash_buffer` by the caller, on a worker when the writer has
    /// any and here otherwise.
    fn spill_filter_partition(&mut self, key: &UserKey, hashes: Vec<u64>) -> crate::Result<()> {
        self.approx_filter_size = 0;
        let partition_index = self.tli_handles.len() + self.pending_count();
        let range = self.partition_range(key);
        self.table_hashes += hashes.len();
        self.largest_partition = self.largest_partition.max(hashes.len());
        if self.sizing.is_some() {
            self.spilled_key = Some(key.clone());
        }
        #[cfg(feature = "std")]
        if self.parallel.is_some() {
            // At the cap, one built partition is published before the next
            // is handed out, so a table with many partitions never holds
            // them all unwritten at once.
            if self
                .parallel
                .as_ref()
                .is_some_and(|parallel| self.pending.len() >= parallel.cap())
            {
                self.publish_next()?;
            }
            // A sized partition builds from a copy of its hashes while a
            // narrower retry may still need them.
            let copies = if self.sizing.is_some() { 2 } else { 1 };
            self.pending.push_back(PendingPartition {
                key: key.clone(),
                hash_bytes: (copies * hashes.len() * core::mem::size_of::<u64>()) as u64,
                framed: self.partition_bound(hashes.len()),
            });
            // Started on the first partition, once every setting is final.
            if self
                .parallel
                .as_ref()
                .is_some_and(|parallel| !parallel.started())
            {
                let settings = self.partition_settings();
                if let Some(parallel) = self.parallel.as_mut() {
                    parallel.start(settings);
                }
            }
            if let Some(parallel) = self.parallel.as_mut() {
                parallel.submit(PartitionJob {
                    hashes,
                    partition_index,
                    range,
                });
            }
            return Ok(());
        }
        let bytes_written = build_partition(
            &self.partition_settings(),
            hashes,
            partition_index,
            &range,
            &mut self.final_filter_buffer,
        )?;
        self.record_partition(key, bytes_written);
        Ok(())
    }

    /// Appends a partition built elsewhere: `filter`, the wire bytes of a
    /// filter over the keys up to and including `end_key` that holds `hashes`
    /// hashes, after every partition appended before it. A writer fed this
    /// way registers no keys.
    ///
    /// # Errors
    ///
    /// Returns an error when framing the partition fails, and
    /// [`crate::Error::InvalidHeader`] when the writer holds registered keys.
    fn push_prebuilt_partition(
        &mut self,
        end_key: &UserKey,
        filter: &[u8],
        hashes: u64,
    ) -> crate::Result<()> {
        if !self.bloom_hash_buffer.is_empty() || self.pending_count() > 0 {
            return Err(crate::Error::InvalidHeader(
                "a prebuilt filter partition follows registered keys",
            ));
        }
        let bytes = frame_partition(
            &self.partition_settings(),
            filter,
            &mut self.final_filter_buffer,
        )?;
        self.record_partition(end_key, bytes);
        let hashes = usize::try_from(hashes)
            .map_err(|_| crate::Error::InvalidHeader("filter partition hash count"))?;
        self.table_hashes += hashes;
        self.largest_partition = self.largest_partition.max(hashes);
        self.last_key = Some(end_key.clone());
        Ok(())
    }

    /// Partitions handed to workers and not yet published.
    fn pending_count(&self) -> usize {
        #[cfg(feature = "std")]
        return self.pending.len();
        #[cfg(not(feature = "std"))]
        0
    }

    /// Adds the partition just appended to `final_filter_buffer`, `bytes`
    /// long and ending at `key`, to the top-level index.
    fn record_partition(&mut self, key: &UserKey, bytes: u32) {
        self.tli_handles.push(KeyedBlockHandle::new(
            key.clone(),
            0,
            BlockHandle::new(BlockOffset(self.relative_file_pos), bytes),
        ));
        self.tli_bytes += core::mem::size_of::<KeyedBlockHandle>() + key.len();
        log::trace!(
            "Built BuRR filter partition ({bytes}B framed) with end_key={key:?} at +{:#X?}",
            self.relative_file_pos,
        );
        self.relative_file_pos += u64::from(bytes);
    }

    /// Appends the oldest partition built on a worker and indexes it, in the
    /// order the partitions were spilled.
    #[cfg(feature = "std")]
    fn publish_next(&mut self) -> crate::Result<()> {
        let Some(partition) = self.pending.pop_front() else {
            return Ok(());
        };
        let Some(built) = self
            .parallel
            .as_mut()
            .and_then(ParallelPartitions::take_next)
        else {
            return Err(crate::Error::Io(crate::io::Error::other(
                "parallel filter partitions out of step with their keys",
            )));
        };
        let (framed, bytes) = built?;
        debug_assert_eq!(framed.len(), bytes as usize, "a partition is its frame");
        self.final_filter_buffer.extend_from_slice(&framed);
        self.record_partition(&partition.key, bytes);
        Ok(())
    }

    /// Framed bytes a partition of `hashes` hashes takes, bounded from above.
    #[cfg(feature = "std")]
    fn partition_bound(&self, hashes: usize) -> u64 {
        crate::table::block::framed_len_bound(
            self.bound_policy().filter_size_bound(hashes) as u64,
            crate::table::block::BlockType::Filter,
            CompressionType::None,
            self.encryption.as_deref(),
            self.ecc,
        )
    }

    /// The hash bytes and the framed-output bound of the partitions handed
    /// to workers and not yet published.
    fn pending_bytes(&self) -> (u64, u64) {
        #[cfg(feature = "std")]
        return self.pending.iter().fold((0, 0), |(hashes, framed), p| {
            (hashes + p.hash_bytes, framed + p.framed)
        });
        #[cfg(not(feature = "std"))]
        (0, 0)
    }

    fn write_top_level_index<WR: Write + Seek>(
        &mut self,
        file_writer: &mut crate::sfa::Writer<ChecksummedWriter<WR>>,
        index_base_offset: BlockOffset,
    ) -> crate::Result<()> {
        file_writer.start("filter_tli")?;

        for item in &mut self.tli_handles {
            item.shift(index_base_offset);
        }

        let mut bytes = vec![];
        IndexBlock::encode_into(&mut bytes, &self.tli_handles)?;

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
            // TLI for the partitioned filter uses the configured
            // index codec; no zstd dict is ever attached at this
            // writer level. page_ecc upgrades to the matching
            // `*Ecc` variant when the tree opted in.
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
            "Written filter top level index, with {} pointers ({bytes_written} bytes) at {index_base_offset:#X?}",
            self.tli_handles.len(),
        );

        Ok(())
    }
}

/// What every partition of one table is built and framed under.
struct PartitionSettings {
    bloom_policy: BloomConstructionPolicy,
    /// The filter budget plan a sized partition picks its width by. Not a
    /// [`crate::filter_budget::FilterPlan`]: a pool thread may let go of it
    /// after the rewrite has ended, which must not delay the end.
    sizing: Option<Arc<crate::filter_budget::FilterSizing>>,
    table_id: crate::TableId,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    ecc: Option<crate::table::block::EccParams>,
}

/// A partition's key range: from its lower bound through its last key.
type PartitionRange = (Bound<UserKey>, UserKey);

/// Builds the filter of `hashes`, over the keys of `range`, and frames it
/// onto `out`, returning the framed length. The frame's checksum is left
/// unbound: its place in the file is known only when `finish` writes the
/// partitions out.
fn build_partition(
    settings: &PartitionSettings,
    hashes: Vec<u64>,
    partition_index: usize,
    range: &PartitionRange,
    out: &mut Vec<u8>,
) -> crate::Result<u32> {
    let hash_count = hashes.len();
    let sizing = settings.sizing.as_deref().map(|sizing| {
        let (lower, upper) = range;
        (
            sizing,
            (
                lower.as_ref().map(AsRef::as_ref),
                Bound::Included(upper.as_ref()),
            ),
        )
    });
    // A partition holds key hashes only (see `register_key`).
    let filter_bytes = super::build_filter(
        settings.bloom_policy,
        sizing,
        hashes,
        hash_count,
        settings.encryption.as_deref(),
        settings.ecc,
    )?;

    // An empty BuRR build result means the policy is inactive for this key
    // population (e.g. fpr <= 0 or bpk out of [1, 64]). For PARTITIONED
    // filters, silently skipping a partition AND its TLI entry causes false
    // negatives at read time: keys in this range would binary-search to a
    // later partition's filter, which doesn't contain them, and
    // Table::check_bloom would report "definitely not present" → false
    // negative on a live key.
    //
    // Fail closed: return Unrecoverable so the writer aborts table creation
    // rather than persisting a partially-filtered table. In practice this
    // path is unreachable: BloomConstructionPolicy::is_active() is checked
    // upstream before any keys are buffered.
    if filter_bytes.is_empty() {
        log::error!(
            "BuRR partitioned writer received empty filter bytes for partition {partition_index} \
             ({hash_count} hashes): policy likely inactive (silent skip would cause false negatives)",
        );
        return Err(crate::Error::Unrecoverable);
    }
    frame_partition(settings, &filter_bytes, out)
}

/// Frames the partition filter `filter_bytes` onto `out` and returns the
/// framed length, its checksum left unbound until `finish` places it.
fn frame_partition(
    settings: &PartitionSettings,
    filter_bytes: &[u8],
    out: &mut Vec<u8>,
) -> crate::Result<u32> {
    let header = Block::write_into(
        out,
        filter_bytes,
        crate::table::block::BlockIdentity {
            table_id: settings.table_id,
            block_type: crate::table::block::BlockType::Filter,
            dict_id: 0,
            window_log: 0,
        },
        // Per-partition filter bodies are uncompressed; layer ECC on top when
        // the tree was opened with `Config::page_ecc(true)`.
        &{
            let t = match settings.encryption.as_deref() {
                Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                None => crate::table::block::BlockTransform::PLAIN,
            };
            if let Some(ecc) = settings.ecc {
                t.with_ecc(ecc)
            } else {
                t
            }
        },
        // Framed ahead of its place in the file; `finish` binds it.
        crate::table::block::ChecksumAt::Unbound,
    )?;
    Ok(header.on_disk_size_with(settings.ecc))
}

/// One partition to build on a worker.
#[cfg(feature = "std")]
struct PartitionJob {
    hashes: Vec<u64>,
    /// Its place among the table's partitions, for the failure log.
    partition_index: usize,
    /// The keys it covers, which a sized partition picks its width over.
    range: PartitionRange,
}

#[cfg(feature = "std")]
impl crate::table::writer::ordered_pipeline::OrderedJob for PartitionJob {
    type Context = PartitionSettings;
    /// The framed partition and its framed length.
    type Output = crate::Result<(Vec<u8>, u32)>;

    fn run(self, settings: &PartitionSettings) -> Self::Output {
        let mut framed = Vec::new();
        let bytes = build_partition(
            settings,
            self.hashes,
            self.partition_index,
            &self.range,
            &mut framed,
        )?;
        Ok((framed, bytes))
    }
}

/// A partition handed to a worker and not yet published: what the writer
/// indexes it under, and what it holds meanwhile.
#[cfg(feature = "std")]
struct PendingPartition {
    key: UserKey,
    /// The hashes it is built from.
    hash_bytes: u64,
    /// Its framed length, bounded from above.
    framed: u64,
}

/// The ordered pipeline a table's partitions are built on, started on the
/// first partition.
#[cfg(feature = "std")]
struct ParallelPartitions {
    parallel: crate::table::writer::ParallelCompression,
    pipeline: Option<crate::table::writer::ordered_pipeline::OrderedPipeline<PartitionJob>>,
}

#[cfg(feature = "std")]
impl ParallelPartitions {
    /// The most partitions a writer keeps on workers at once, the same bound
    /// the table's blocks run under.
    fn cap(&self) -> usize {
        (self.parallel.threads * 2).max(1)
    }

    fn started(&self) -> bool {
        self.pipeline.is_some()
    }

    fn start(&mut self, settings: PartitionSettings) {
        let cap = self.cap();
        self.pipeline = Some(
            crate::table::writer::ordered_pipeline::OrderedPipeline::new(
                Arc::clone(&self.parallel.spawner),
                settings,
                self.parallel.threads,
                cap,
            ),
        );
    }

    fn submit(&mut self, job: PartitionJob) {
        if let Some(pipeline) = self.pipeline.as_mut() {
            pipeline.submit(job);
        }
    }

    fn take_next(&mut self) -> Option<crate::Result<(Vec<u8>, u32)>> {
        self.pipeline.as_mut()?.take_next()
    }
}

#[cfg(test)]
mod tests;

impl<W: crate::io::Write + crate::io::Seek> FilterWriter<W> for PartitionedFilterWriter {
    #[cfg(feature = "std")]
    fn use_parallel(
        mut self: Box<Self>,
        parallel: Option<crate::table::writer::ParallelCompression>,
    ) -> Box<dyn FilterWriter<W>> {
        self.parallel = parallel.map(|parallel| ParallelPartitions {
            parallel,
            pipeline: None,
        });
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

    fn use_sizing(
        mut self: Box<Self>,
        sizing: Option<crate::filter_budget::FilterPlan>,
    ) -> Box<dyn FilterWriter<W>> {
        self.sizing = sizing;
        self
    }

    // Partitions are sized as they spill, before the table's last key is
    // known; the first key is kept as keys arrive.
    fn set_key_range(&mut self, _: &UserKey, _: &UserKey) {}

    fn use_partition_size(mut self: Box<Self>, size: u32) -> Box<dyn FilterWriter<W>> {
        self.partition_size = size;
        self
    }

    fn use_tli_compression(
        mut self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn FilterWriter<W>> {
        self.compression = compression;
        self
    }

    #[cfg(zstd_any)]
    fn use_zstd_two_pass_seed(mut self: Box<Self>, enabled: bool) -> Box<dyn FilterWriter<W>> {
        self.zstd_two_pass_seed = enabled;
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

    fn register_key(&mut self, key: &UserKey) -> crate::Result<usize> {
        self.bloom_hash_buffer.push(crate::hash::hash64(key));

        // NOTE: Prefix hashes are NOT inserted for partitioned filters.
        // Table::maybe_contains_prefix returns Ok(true) for partitioned/TLI
        // filters (partition index is keyed by user key, not prefix hash),
        // so prefix hashes would only increase CPU and filter size with no
        // read-side benefit.

        self.approx_filter_size = self
            .bloom_policy
            .estimated_filter_size(self.bloom_hash_buffer.len());

        if self.sizing.is_some() && self.first_key.is_none() {
            self.first_key = Some(key.clone());
        }
        self.last_key = Some(key.clone());

        if self.approx_filter_size >= self.partition_size as usize {
            // mem::replace (rather than mem::take) preserves the buffer's
            // grown capacity for the next partition. `take` leaves a
            // capacity-0 Vec behind, which would force a reallocation on
            // every register_key call following a spill. Tables with many
            // partitions can spill thousands of times during a single
            // flush/compaction, so the saved reallocations matter on the
            // write hot path.
            let old_cap = self.bloom_hash_buffer.capacity();
            let hashes =
                core::mem::replace(&mut self.bloom_hash_buffer, Vec::with_capacity(old_cap));
            self.spill_filter_partition(key, hashes)?;
        }

        Ok(1)
    }

    fn held_bytes(&self) -> u64 {
        // The built partitions stay buffered until `finish` writes them, and
        // those on workers hold their hashes and then their framed bytes.
        let hashes = self.bloom_hash_buffer.capacity() * core::mem::size_of::<u64>();
        let tli = super::super::handles_held(
            self.tli_bytes,
            &self.tli_handles,
            self.tli_handles.capacity(),
        );
        let (pending_hashes, pending_framed) = self.pending_bytes();
        (self.final_filter_buffer.capacity() + hashes + tli) as u64
            + pending_hashes
            + pending_framed
    }

    fn finish_scratch_bytes(&self) -> u64 {
        use crate::table::block::{BlockType, framed_len_bound, transform_scratch_bound};
        let encryption = self.encryption.as_deref();
        // `finish` builds the open partition and frames it, and appends it and
        // the partitions still on workers to the partition buffer, which
        // reallocates when it outgrows its capacity and holds both copies
        // while it moves.
        let (build, filter, frame) = if self.bloom_hash_buffer.is_empty() {
            (0, 0, 0)
        } else {
            let filter = self.open_partition_bound();
            (
                crate::table::filter::ribbon::burr::builder::build_peak_bytes(
                    self.bloom_hash_buffer.len(),
                    false,
                ) as u64,
                filter,
                framed_len_bound(
                    filter,
                    BlockType::Filter,
                    CompressionType::None,
                    encryption,
                    self.ecc,
                ),
            )
        };
        let (_, pending_framed) = self.pending_bytes();
        let needed = self.final_filter_buffer.len() as u64 + pending_framed + frame;
        let capacity = self.final_filter_buffer.capacity() as u64;
        let growth = if needed > capacity {
            needed.max(2 * capacity)
        } else {
            0
        };
        // A sized partition builds from a copy of the hashes while a narrower
        // retry may still need them.
        let retry_copy = if self.sizing.is_some() {
            (self.bloom_hash_buffer.len() * core::mem::size_of::<u64>()) as u64
        } else {
            0
        };
        let open = build + filter + frame + growth + retry_copy;
        // Then the top-level index, counted at its in-memory size, and its
        // framed copy.
        let tli = self.tli_at_finish() as u64;
        open + tli
            + transform_scratch_bound(
                tli,
                BlockType::Index,
                self.compression,
                encryption,
                self.ecc,
            )
    }

    fn finish_output_bytes(&self) -> u64 {
        use crate::table::block::{BlockType, framed_len_bound};
        if self.last_key.is_none() {
            return 0;
        }
        let encryption = self.encryption.as_deref();
        // The built partitions are framed already; `finish` builds the open
        // one and the top-level index, counted at its in-memory size, above
        // what its encoding takes.
        let open = if self.bloom_hash_buffer.is_empty() {
            0
        } else {
            framed_len_bound(
                self.open_partition_bound(),
                BlockType::Filter,
                CompressionType::None,
                encryption,
                self.ecc,
            )
        };
        let tli = self.tli_at_finish();
        let (_, pending_framed) = self.pending_bytes();
        self.final_filter_buffer.len() as u64
            + pending_framed
            + open
            + framed_len_bound(
                tli as u64,
                BlockType::Index,
                self.compression,
                encryption,
                self.ecc,
            )
    }

    fn finish(
        mut self: Box<Self>,
        file_writer: &mut crate::sfa::Writer<ChecksummedWriter<W>>,
    ) -> crate::Result<super::FilterOutput> {
        if self.last_key.is_none() {
            log::trace!("Filter writer has not seen any writes - not building filter");
            return Ok(super::FilterOutput::default());
        }

        if !self.bloom_hash_buffer.is_empty() {
            #[expect(
                clippy::expect_used,
                reason = "last key must exist because of initial check"
            )]
            let last_key = self.last_key.take().expect("last key should exist");
            // No partition follows the last one, so the buffer goes whole.
            let hashes = core::mem::take(&mut self.bloom_hash_buffer);
            self.spill_filter_partition(&last_key, hashes)?;
        }
        // Every partition is in the buffer, in spill order, before any of
        // them is bound to its place in the file.
        #[cfg(feature = "std")]
        while !self.pending.is_empty() {
            self.publish_next()?;
        }
        if let Some(sizing) = &self.sizing {
            sizing.table_finished(self.table_hashes);
        }

        let index_base_offset = BlockOffset(file_writer.get_mut().stream_position()?);

        // The partitions were framed before their place in the file was
        // known: bind each one's checksum to where it lands now.
        for handle in &self.tli_handles {
            let relative = *handle.offset();
            let frame = usize::try_from(relative)
                .ok()
                .and_then(|at| self.final_filter_buffer.get_mut(at..))
                .ok_or(crate::Error::InvalidHeader(
                    "filter partition outside its buffer",
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

        file_writer.start("filter")?;
        file_writer.write_all(&self.final_filter_buffer)?;
        log::trace!("Concatted filter partitions onto blocks file");

        let block_count = self.tli_handles.len();

        self.write_top_level_index(file_writer, index_base_offset)?;

        Ok(super::FilterOutput {
            blocks: block_count,
            hashes: if block_count == 0 {
                0
            } else {
                self.table_hashes as u64
            },
            partition_hashes: if block_count == 0 {
                0
            } else {
                self.largest_partition as u64
            },
        })
    }
}
