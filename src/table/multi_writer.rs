// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::{filter::BloomConstructionPolicy, writer::Writer};
use crate::{
    Checksum, CompressionType, HashMap, SequenceNumberCounter, TableId, UserKey,
    blob_tree::handle::BlobIndirection,
    encryption::EncryptionProvider,
    fs::{Fs, SyncMode},
    prefix::PrefixExtractor,
    range_tombstone::RangeTombstone,
    table::writer::LinkedFile,
    value::InternalValue,
    vlog::BlobFileId,
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec::Vec};

use crate::path::PathBuf;

/// Like `Writer` but will rotate to a new table, once a table grows larger than `target_size`
///
/// This results in a sorted "run" of tables
#[expect(
    clippy::struct_excessive_bools,
    reason = "writer config: each bool is an independent feature toggle carried \
              across table rotations (partitioned filter, range-tombstone clip, \
              seqno-in-index, disable-CoW); enums would obscure the per-feature wiring"
)]
pub struct MultiWriter {
    pub(crate) fs: Arc<dyn Fs>,

    pub(crate) base_path: PathBuf,

    data_block_hash_ratio: f32,

    data_block_size: u32,

    data_block_restart_interval: u8,
    index_block_restart_interval: u8,

    use_partitioned_filter: bool,

    /// `Some(threshold)` selects the size-adaptive index (single-level
    /// until the index exceeds `threshold` bytes, then a streaming
    /// partitioned index). `None` leaves the writer's default single-level
    /// index. Re-applied to each rotated table writer. Pure
    /// always-partition is `Some(0)` (spill on the first entry).
    index_spill_threshold: Option<u64>,

    /// Target size of tables in bytes
    ///
    /// If a table reaches the target size, a new one is started,
    /// resulting in a sorted "run" of tables
    pub target_size: u64,

    results: Vec<(TableId, Checksum)>,

    table_id_generator: SequenceNumberCounter,

    pub writer: Writer,

    pub data_block_compression: CompressionType,
    pub index_block_compression: CompressionType,

    bloom_policy: BloomConstructionPolicy,

    current_key: Option<UserKey>,
    comparator: crate::SharedComparator,

    linked_blobs: HashMap<BlobFileId, LinkedFile>,

    /// Range tombstones to distribute across output tables.
    /// During compaction these are clipped to each table's key range;
    /// during flush they are written unmodified (they must cover keys in older SSTs).
    range_tombstones: Vec<RangeTombstone>,

    /// When true, range tombstones are clipped to each output table's KV key range
    /// via `intersect_opt`. This is correct for compaction (input tables are consumed)
    /// but wrong for flush (RTs must cover keys in older SSTs outside the memtable's range).
    clip_range_tombstones: bool,

    /// Level the tables are written to
    initial_level: u8,

    prefix_extractor: Option<Arc<dyn PrefixExtractor>>,

    encryption: Option<Arc<dyn EncryptionProvider>>,

    /// Resolved Page ECC scheme — preserved here so the rotation path
    /// can stamp the same scheme on every successor [`Writer`].
    ecc: Option<crate::table::block::EccParams>,

    /// `Config::sync_mode` — preserved so every successor [`Writer`]
    /// finishes its SST with the same durability level.
    sync_mode: SyncMode,

    /// Per-KV checksum policy + algorithm (from the runtime
    /// `kv_checksums` config) — preserved here so the rotation path
    /// stamps the same setting on every successor [`Writer`].
    kv_checksum: Option<(
        crate::runtime_config::KvChecksumPolicy,
        crate::runtime_config::ChecksumAlgorithm,
    )>,

    /// `seqno_in_index` runtime config — preserved here so the rotation
    /// path sets the same flag on every successor [`Writer`], so all SSTs
    /// of one flush / compaction uniformly emit (or omit) the `seqno_bounds`
    /// section.
    use_seqno_in_index: bool,

    /// Preserved like `use_seqno_in_index` so every successor [`Writer`] of one
    /// flush / compaction uniformly emits (or omits) the `zone_map` section.
    use_zone_map: bool,

    /// Preserved across writer rotation so every successor [`Writer`] of one
    /// flush / compaction uniformly writes columnar (or row-major) data blocks.
    use_columnar: bool,

    /// Delete strategy applied to every successor [`Writer`], preserved across
    /// rotation. Under copy-on-write the writers persist no delete-bitmap; under
    /// merge-on-read / adaptive a populated bitmap is written.
    delete_strategy: crate::config::DeleteStrategy,

    /// When `true`, each output SST has per-file copy-on-write cleared at
    /// creation (Btrfs `FS_NOCOW_FL`) so write-once SSTs avoid the
    /// copy-on-write fragmentation penalty. Preserved across rotations so every successor
    /// table in the run is flagged the same way. No-op on non-CoW filesystems.
    disable_cow_on_sst: bool,

    /// Resolved retrieval-ribbon locator policy entry for this run's level,
    /// preserved here so the rotation path stamps the same setting on every
    /// successor [`Writer`]. Each table gets its own per-SST locator section
    /// (block ordinals reset per table). Defaults to `None` (disabled).
    locator_entry: crate::config::LocatorPolicyEntry,

    #[cfg(zstd_any)]
    zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,

    /// Optional parallel block-compression executor + worker count, preserved
    /// here so every successor [`Writer`] of a rotated run shares the same pool.
    #[cfg(feature = "std")]
    spawner: Option<Arc<dyn crate::table::writer::CompactionSpawner>>,
    #[cfg(feature = "std")]
    parallel_threads: usize,
}

impl MultiWriter {
    /// Sets up a new `MultiWriter` at the given tables folder
    pub fn new(
        base_path: PathBuf,
        table_id_generator: SequenceNumberCounter,
        target_size: u64,
        initial_level: u8,
        fs: Arc<dyn Fs>,
    ) -> crate::Result<Self> {
        let current_table_id = table_id_generator.next();

        let path = base_path.join(current_table_id.to_string());
        let writer = Writer::new(path, current_table_id, initial_level, fs.clone())?;

        Ok(Self {
            fs,
            initial_level,

            base_path,

            data_block_hash_ratio: 0.0,

            data_block_size: 4_096,

            data_block_restart_interval: 16,
            index_block_restart_interval: 1,

            target_size,
            results: Vec::new(),
            table_id_generator,
            writer,

            data_block_compression: CompressionType::None,
            index_block_compression: CompressionType::None,

            use_partitioned_filter: false,
            index_spill_threshold: None,

            bloom_policy: BloomConstructionPolicy::default(),

            current_key: None,
            comparator: crate::comparator::default_comparator(),

            linked_blobs: HashMap::default(),
            range_tombstones: Vec::new(),
            clip_range_tombstones: false,

            prefix_extractor: None,

            encryption: None,

            ecc: None,
            sync_mode: SyncMode::Normal,

            kv_checksum: None,
            use_seqno_in_index: false,
            use_zone_map: false,
            use_columnar: false,
            delete_strategy: crate::config::DeleteStrategy::default(),
            disable_cow_on_sst: false,
            locator_entry: crate::config::LocatorPolicyEntry::None,

            #[cfg(zstd_any)]
            zstd_dictionary: None,

            #[cfg(feature = "std")]
            spawner: None,
            #[cfg(feature = "std")]
            parallel_threads: 0,
        })
    }

    /// Enables parallel block compression for this run, sharing `spawner` across
    /// every rotated successor table. `threads` sizes the in-flight cap. No-op
    /// when `spawner` is `None`.
    #[cfg(feature = "std")]
    #[must_use]
    pub fn use_parallel_compression(
        mut self,
        spawner: Option<Arc<dyn crate::table::writer::CompactionSpawner>>,
        threads: usize,
    ) -> Self {
        if let Some(spawner) = spawner {
            self.spawner = Some(Arc::clone(&spawner));
            self.parallel_threads = threads;
            self.writer = self.writer.use_parallel_compression(spawner, threads);
        }
        self
    }

    /// Enables RT clipping: each tombstone is intersected with the output
    /// Sets the user comparator used for output ordering and RT clipping.
    #[must_use]
    pub fn set_comparator(mut self, comparator: crate::SharedComparator) -> Self {
        self.comparator = comparator;
        self
    }

    /// Enables RT clipping to the output table's responsibility range.
    ///
    /// Clipped RTs may extend beyond the table's own KV key range to cover the
    /// gap up to the next output table. Use this for compaction where input
    /// tables are consumed; do NOT use for flush where RTs must cover older
    /// SSTs.
    #[must_use]
    pub fn use_clip_range_tombstones(mut self) -> Self {
        self.clip_range_tombstones = true;
        self
    }

    /// Sets range tombstones to be distributed across output tables.
    pub fn set_range_tombstones(&mut self, tombstones: Vec<RangeTombstone>) {
        self.range_tombstones = tombstones;
    }

    /// Writes range tombstones to the given writer, respecting the clip mode.
    ///
    /// - **clip=true** (compaction): intersect each RT with the table's
    ///   "responsibility range".  For intermediate tables (rotation)
    ///   `clip_upper` is the first key of the *next* output table, so the
    ///   range extends past the table's last KV key and covers the gap.
    ///   For the final table `clip_upper` is `None` and we fall back to
    ///   `upper_bound_exclusive(last_key)`.
    /// - **clip=false** (flush): write all overlapping RTs unmodified so they
    ///   cover keys in older SSTs outside this memtable's key range.
    fn write_rts_to_writer(
        tombstones: &[RangeTombstone],
        clip: bool,
        writer: &mut Writer,
        clip_upper: Option<&UserKey>,
        comparator: &dyn crate::comparator::UserComparator,
    ) {
        if let (Some(first_key), Some(last_key)) =
            (writer.meta.first_key.clone(), writer.meta.last_key.clone())
        {
            if clip {
                // Compaction mode: clip RTs to this table's responsibility range.
                //
                // For intermediate tables (rotation) `clip_upper` is the first key
                // of the next output table — the range [first_key, next_key) covers
                // the gap between tables so RTs spanning it are preserved.
                //
                // For the final table `clip_upper` is None and we derive the
                // exclusive upper bound from the table's last KV key.
                let derived_upper;
                let max_exclusive: Option<&[u8]> = if let Some(upper) = clip_upper {
                    Some(upper.as_ref())
                } else {
                    derived_upper =
                        crate::range_tombstone::upper_bound_exclusive(last_key.as_ref());
                    derived_upper.as_deref()
                };

                if let Some(max_exclusive) = max_exclusive {
                    for rt in tombstones {
                        if let Some(clipped) =
                            rt.intersect_opt_with(first_key.as_ref(), max_exclusive, comparator)
                        {
                            // Widen last_key so point reads for keys in the
                            // gap will consult this table for RT suppression.
                            //
                            // Only widen during rotation (clip_upper is Some)
                            // where we know the exact boundary.  For the final
                            // table (clip_upper is None) widening with the
                            // derived exclusive bound could overlap a non-
                            // compacted adjacent table at the same level.
                            //
                            // Even during rotation, clipped.end must be
                            // strictly less than clip_upper (the next table's
                            // first key) — equality would make key_ranges
                            // overlap, breaking Run::get_for_key_cmp.
                            //
                            // Only last_key needs widening: intersect_opt
                            // already clamps clipped.start >= first_key.
                            if let Some(existing) = &mut writer.meta.last_key {
                                let safe = clip_upper.is_some_and(|upper| {
                                    comparator.compare(&clipped.end, upper.as_ref())
                                        == core::cmp::Ordering::Less
                                });
                                if safe
                                    && comparator.compare(&clipped.end, existing.as_ref())
                                        == core::cmp::Ordering::Greater
                                {
                                    *existing = clipped.end.clone();
                                }
                            }

                            writer.write_range_tombstone(clipped);
                        }
                    }
                } else {
                    // `last_key` is the lexicographically maximal encodable user
                    // key, so there is no strict successor. In that case clip
                    // only on the lower bound and keep the persisted key_range
                    // unchanged; widening it during compaction would break the
                    // disjoint-run invariant that point reads rely on.
                    for rt in tombstones {
                        let clipped_start = if comparator.compare(&rt.start, first_key.as_ref())
                            == core::cmp::Ordering::Greater
                        {
                            rt.start.as_ref()
                        } else {
                            first_key.as_ref()
                        };

                        if comparator.compare(clipped_start, &rt.end) == core::cmp::Ordering::Less {
                            writer.write_range_tombstone(RangeTombstone::new(
                                UserKey::from(clipped_start),
                                rt.end.clone(),
                                rt.seqno,
                            ));
                        }
                    }
                }
            } else {
                // Flush mode: write ALL RTs without clipping so they cover keys
                // in older SSTs outside this memtable's key range. No overlap
                // filter — an RT disjoint from this table's KV range (e.g.,
                // delete_range on keys only in older SSTs) must still be persisted.
                //
                // Conservatively widen key_range to include RT coverage so leveled
                // compaction overlap selection can discover these RTs. Using rt.end
                // (exclusive) as an inclusive upper bound over-approximates the
                // actual KV max but does not lose entries.
                for rt in tombstones {
                    match &mut writer.meta.first_key {
                        Some(existing) => {
                            if comparator.compare(&rt.start, existing.as_ref())
                                == core::cmp::Ordering::Less
                            {
                                *existing = rt.start.clone();
                            }
                        }
                        None => {
                            writer.meta.first_key = Some(rt.start.clone());
                        }
                    }
                    match &mut writer.meta.last_key {
                        Some(existing) => {
                            if comparator.compare(&rt.end, existing.as_ref())
                                == core::cmp::Ordering::Greater
                            {
                                *existing = rt.end.clone();
                            }
                        }
                        None => {
                            writer.meta.last_key = Some(rt.end.clone());
                        }
                    }
                    writer.write_range_tombstone(rt.clone());
                }
            }
        } else {
            // RT-only table (no KV items yet) — write all tombstones unclipped.
            for rt in tombstones {
                writer.write_range_tombstone(rt.clone());
            }
        }
    }

    pub fn register_blob(&mut self, indirection: BlobIndirection) {
        self.linked_blobs
            .entry(indirection.vhandle.blob_file_id)
            .and_modify(|entry| {
                entry.bytes += u64::from(indirection.size);
                entry.on_disk_bytes += u64::from(indirection.vhandle.on_disk_size);
                entry.len += 1;
            })
            .or_insert_with(|| LinkedFile {
                blob_file_id: indirection.vhandle.blob_file_id,
                bytes: u64::from(indirection.size),
                on_disk_bytes: u64::from(indirection.vhandle.on_disk_size),
                len: 1,
            });
    }

    #[must_use]
    pub fn use_adaptive_index(mut self, spill_threshold: u64) -> Self {
        self.index_spill_threshold = Some(spill_threshold);
        self.writer = self.writer.use_adaptive_index(spill_threshold);
        self
    }

    #[must_use]
    pub fn use_partitioned_filter(mut self) -> Self {
        self.use_partitioned_filter = true;
        self.writer = self.writer.use_partitioned_filter();
        self
    }

    #[must_use]
    pub fn use_data_block_restart_interval(mut self, interval: u8) -> Self {
        self.data_block_restart_interval = interval;
        self.writer = self.writer.use_data_block_restart_interval(interval);
        self
    }

    #[must_use]
    pub fn use_index_block_restart_interval(mut self, interval: u8) -> Self {
        self.index_block_restart_interval = interval;
        self.writer = self.writer.use_index_block_restart_interval(interval);
        self
    }

    #[must_use]
    pub fn use_data_block_hash_ratio(mut self, ratio: f32) -> Self {
        self.data_block_hash_ratio = ratio;
        self.writer = self.writer.use_data_block_hash_ratio(ratio);
        self
    }

    #[must_use]
    pub(crate) fn use_data_block_size(mut self, size: u32) -> Self {
        assert!(
            size <= 4 * 1_024 * 1_024,
            "data block size must be <= 4 MiB",
        );
        self.data_block_size = size;
        self.writer = self.writer.use_data_block_size(size);
        self
    }

    #[must_use]
    pub fn use_data_block_compression(mut self, compression: CompressionType) -> Self {
        self.data_block_compression = compression;
        self.writer = self.writer.use_data_block_compression(compression);
        self
    }

    #[must_use]
    pub fn use_index_block_compression(mut self, compression: CompressionType) -> Self {
        self.index_block_compression = compression;
        self.writer = self.writer.use_index_block_compression(compression);
        self
    }

    #[must_use]
    pub fn use_bloom_policy(mut self, bloom_policy: BloomConstructionPolicy) -> Self {
        self.bloom_policy = bloom_policy;
        self.writer = self.writer.use_bloom_policy(bloom_policy);
        self
    }

    #[must_use]
    pub fn use_prefix_extractor(mut self, extractor: Option<Arc<dyn PrefixExtractor>>) -> Self {
        self.prefix_extractor.clone_from(&extractor);
        self.writer = self.writer.use_prefix_extractor(extractor);
        self
    }

    #[must_use]
    pub fn use_encryption(mut self, encryption: Option<Arc<dyn EncryptionProvider>>) -> Self {
        self.encryption.clone_from(&encryption);
        self.writer = self.writer.use_encryption(encryption);
        self
    }

    /// Wires the tree's `Config::page_ecc` flag through to the
    /// inner [`Writer`] and preserves it across rotations so every
    /// successor writer stamps the same setting on its blocks.
    #[must_use]
    pub fn use_ecc(mut self, ecc: Option<crate::table::block::EccParams>) -> Self {
        self.ecc = ecc;
        self.writer = self.writer.use_ecc(ecc);
        self
    }

    /// Convenience: resolves `(page_ecc, EccScheme)` into the per-block
    /// scheme and applies it via [`Self::use_ecc`]. Mirrors
    /// [`crate::table::writer::Writer::use_page_ecc`].
    #[must_use]
    pub fn use_page_ecc(self, page_ecc: bool, scheme: crate::runtime_config::EccScheme) -> Self {
        self.use_ecc(crate::table::writer::resolve_ecc(page_ecc, scheme))
    }

    /// Wires the tree's `Config::sync_mode` through to the inner [`Writer`]
    /// and preserves it across rotations so every successor SST is finished
    /// at the same durability level.
    #[must_use]
    pub fn use_sync_mode(mut self, sync_mode: SyncMode) -> Self {
        self.sync_mode = sync_mode;
        self.writer = self.writer.use_sync_mode(sync_mode);
        self
    }

    /// Wires the runtime `kv_checksums` policy + algorithm through to the
    /// inner [`Writer`] and preserves it across rotations so every
    /// successor writer applies the same per-KV checksum setting. `Off`
    /// emits no per-KV footer and leaves the `KV_CHECKSUM_FOOTER` flag clear
    /// (the data-block payload encoding is unchanged; the V5 header/meta
    /// layout still differs from pre-V5 regardless).
    #[must_use]
    pub fn use_kv_checksums(
        mut self,
        policy: crate::runtime_config::KvChecksumPolicy,
        algo: crate::runtime_config::ChecksumAlgorithm,
    ) -> Self {
        self.kv_checksum = if matches!(policy, crate::runtime_config::KvChecksumPolicy::Off) {
            None
        } else {
            Some((policy, algo))
        };
        self.writer = self.writer.use_kv_checksums(policy, algo);
        self
    }

    /// Wires the `seqno_in_index` runtime config through to the inner
    /// [`Writer`] and preserves it across rotations so every successor
    /// writer emits a matching `seqno_bounds` section.
    #[must_use]
    pub fn use_seqno_in_index(mut self, seqno_in_index: bool) -> Self {
        self.use_seqno_in_index = seqno_in_index;
        self.writer = self.writer.use_seqno_in_index(seqno_in_index);
        self
    }

    /// Enables the `zone_map` section on this writer and every successor it
    /// rotates to, so all SSTs of one flush / compaction emit it.
    #[must_use]
    pub fn use_zone_map(mut self, zone_map: bool) -> Self {
        self.use_zone_map = zone_map;
        self.writer = self.writer.use_zone_map(zone_map);
        self
    }

    #[must_use]
    pub fn use_columnar(mut self, columnar: bool) -> Self {
        self.use_columnar = columnar;
        self.writer = self.writer.use_columnar(columnar);
        self
    }

    /// Sets the delete strategy for this and every rotated successor writer.
    #[must_use]
    pub fn delete_strategy(mut self, strategy: crate::config::DeleteStrategy) -> Self {
        self.delete_strategy = strategy;
        self.writer = self.writer.delete_strategy(strategy);
        self
    }

    /// Wires the resolved retrieval-ribbon locator policy entry through to the
    /// inner [`Writer`] and preserves it across rotations, so every successor
    /// table in the run emits its own per-SST locator section (or none, when
    /// disabled). Block ordinals reset per table.
    #[must_use]
    pub fn use_locator(mut self, entry: crate::config::LocatorPolicyEntry) -> Self {
        self.locator_entry = entry;
        self.writer = self.writer.use_locator(entry);
        self
    }

    /// Wires the `disable_cow_on_sst_files` runtime config through to the inner
    /// [`Writer`] (clearing per-file copy-on-write on the current output file) and
    /// preserves it across rotations so every successor SST is flagged the same
    /// way. A no-op on non-CoW filesystems.
    #[must_use]
    pub fn use_disable_cow_on_sst(mut self, disable_cow: bool) -> Self {
        self.disable_cow_on_sst = disable_cow;
        self.writer = self.writer.use_disable_cow(disable_cow);
        self
    }

    #[cfg(zstd_any)]
    #[must_use]
    pub fn use_zstd_dictionary(
        mut self,
        dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
    ) -> Self {
        self.zstd_dictionary.clone_from(&dictionary);
        self.writer = self.writer.use_zstd_dictionary(dictionary);
        self
    }

    /// Flushes the current writer, stores its metadata, and sets up a new writer for the next table
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating table writer");

        let new_table_id = self.table_id_generator.next();
        let path = self.base_path.join(new_table_id.to_string());

        let mut new_writer = Writer::new(path, new_table_id, self.initial_level, self.fs.clone())?
            .use_data_block_compression(self.data_block_compression)
            .use_index_block_compression(self.index_block_compression)
            .use_data_block_size(self.data_block_size)
            .use_data_block_restart_interval(self.data_block_restart_interval)
            .use_index_block_restart_interval(self.index_block_restart_interval)
            .use_bloom_policy(self.bloom_policy)
            .use_data_block_hash_ratio(self.data_block_hash_ratio);

        if let Some(threshold) = self.index_spill_threshold {
            new_writer = new_writer.use_adaptive_index(threshold);
        }
        if self.use_partitioned_filter {
            new_writer = new_writer.use_partitioned_filter();
        }

        new_writer = new_writer.use_prefix_extractor(self.prefix_extractor.clone());
        new_writer = new_writer.use_encryption(self.encryption.clone());
        new_writer = new_writer.use_ecc(self.ecc);
        new_writer = new_writer.use_sync_mode(self.sync_mode);
        if let Some((policy, algo)) = self.kv_checksum {
            new_writer = new_writer.use_kv_checksums(policy, algo);
        }
        new_writer = new_writer.use_seqno_in_index(self.use_seqno_in_index);
        new_writer = new_writer.use_zone_map(self.use_zone_map);
        new_writer = new_writer.use_columnar(self.use_columnar);
        new_writer = new_writer.delete_strategy(self.delete_strategy);
        new_writer = new_writer.use_disable_cow(self.disable_cow_on_sst);
        new_writer = new_writer.use_locator(self.locator_entry);

        #[cfg(zstd_any)]
        {
            new_writer = new_writer.use_zstd_dictionary(self.zstd_dictionary.clone());
        }

        #[cfg(feature = "std")]
        if let Some(spawner) = self.spawner.clone() {
            new_writer = new_writer.use_parallel_compression(spawner, self.parallel_threads);
        }

        let mut old_writer = core::mem::replace(&mut self.writer, new_writer);
        old_writer.spill_block()?;

        // Write range tombstones to the finishing writer.
        // In flush mode (clip=false) tombstones are written unmodified because
        // they must cover keys in older SSTs outside this memtable's key range.
        // In compaction mode (clip=true) tombstones are clipped to the table's
        // responsibility range [first_key, current_key) — current_key is the
        // first key of the NEW table, so this covers the gap between tables.
        if !self.range_tombstones.is_empty() {
            Self::write_rts_to_writer(
                &self.range_tombstones,
                self.clip_range_tombstones,
                &mut old_writer,
                self.current_key.as_ref(),
                self.comparator.as_ref(),
            );
        }

        for linked in self.linked_blobs.values() {
            old_writer.link_blob_file(
                linked.blob_file_id,
                linked.len,
                linked.bytes,
                linked.on_disk_bytes,
            );
        }
        self.linked_blobs.clear();

        if let Some((table_id, checksum)) = old_writer.finish()? {
            self.results.push((table_id, checksum));
        }

        Ok(())
    }

    /// Writes an item
    pub fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        let is_next_key = self.current_key.as_ref() < Some(&item.key.user_key);

        if is_next_key {
            self.current_key = Some(item.key.user_key.clone());

            if self.writer.output_size_hint() >= self.target_size {
                self.rotate()?;
            }
        }

        self.writer.write(item)?;

        Ok(())
    }

    /// Writes a consumer-provided columnar batch as one columnar block, rotating
    /// to a fresh table first if the current one has reached the target size (a
    /// batch is a block boundary, mirroring the new-key rotation in
    /// [`Self::write`]). Forwards to the inner writer's columnar-ingest path.
    #[cfg(feature = "columnar")]
    pub(crate) fn write_columnar_batch(
        &mut self,
        batch: &crate::table::columnar::ColumnBatch,
    ) -> crate::Result<Option<crate::UserKey>> {
        if self.writer.output_size_hint() >= self.target_size {
            self.rotate()?;
        }
        let comparator = self.comparator.clone();
        self.writer.write_columnar_batch(batch, &comparator)
    }

    /// Validates a columnar batch against the ingest contract without writing,
    /// so the ingestion can reject a malformed batch eagerly while block emission
    /// stays deferred. Forwards to the inner writer's validation.
    #[cfg(feature = "columnar")]
    pub(crate) fn validate_columnar_batch(
        &self,
        batch: &crate::table::columnar::ColumnBatch,
    ) -> crate::Result<()> {
        let comparator = self.comparator.clone();
        self.writer.validate_columnar_batch(batch, &comparator)
    }

    /// Finishes the last table, making sure all data is written durably
    ///
    /// Returns the metadata of created tables
    pub fn finish(mut self) -> crate::Result<Vec<(TableId, Checksum)>> {
        self.writer.spill_block()?;

        // Write range tombstones to the last writer. No next table exists,
        // so clip_upper=None falls back to upper_bound_exclusive(last_key).
        if !self.range_tombstones.is_empty() {
            Self::write_rts_to_writer(
                &self.range_tombstones,
                self.clip_range_tombstones,
                &mut self.writer,
                None,
                self.comparator.as_ref(),
            );
        }

        for linked in self.linked_blobs.values() {
            self.writer.link_blob_file(
                linked.blob_file_id,
                linked.len,
                linked.bytes,
                linked.on_disk_bytes,
            );
        }

        if let Some((table_id, checksum)) = self.writer.finish()? {
            self.results.push((table_id, checksum));
        }

        Ok(self.results)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests;
