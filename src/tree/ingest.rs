// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::Tree;
use crate::{
    BlobIndirection, SeqNo, UserKey, UserValue, config::FilterPolicyEntry, fs::Fs,
    table::multi_writer::MultiWriter,
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec::Vec};
use core::cmp::Ordering;

use crate::path::PathBuf;

pub const INITIAL_CANONICAL_LEVEL: usize = 1;

/// Bulk ingestion
///
/// Items NEED to be added in ascending key order.
///
/// Ingested data bypasses memtables and is written directly into new tables,
/// using the same table writer configuration that is used for flush and compaction.
pub struct Ingestion<'a> {
    pub(crate) folder: PathBuf,
    /// Level-routed filesystem backend for the target level.
    pub(crate) level_fs: Arc<dyn Fs>,
    tree: &'a Tree,
    pub(crate) writer: MultiWriter,
    seqno: SeqNo,
    last_key: Option<UserKey>,
}

impl<'a> Ingestion<'a> {
    /// Creates a new ingestion.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new(tree: &'a Tree) -> crate::Result<Self> {
        // Ingested tables are placed at L0 (via with_new_l0_run), so use
        // the level-routed folder for level 0.
        let (folder, level_fs) = tree.config.tables_folder_for_level(0);
        log::debug!("Ingesting into tables in {}", folder.display());

        let index_partitioning = tree
            .config
            .index_block_partitioning_policy
            .get(INITIAL_CANONICAL_LEVEL);

        let filter_partitioning = tree
            .config
            .filter_block_partitioning_policy
            .get(INITIAL_CANONICAL_LEVEL);

        // Ingested tables are an L0 run; their canonical config level is
        // INITIAL_CANONICAL_LEVEL, the same level every per-level policy
        // below is read at. The writer's level only feeds the per-block
        // policy decisions (e.g. `PerLevel` kv-checksums) — `meta.initial_level`
        // is write-only and placement is forced to L0 by `with_new_l0_run`
        // regardless — so this keeps kv-checksum level-gating consistent with
        // the compression / filter / restart policies applied to ingested
        // tables.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "INITIAL_CANONICAL_LEVEL is 1, well within u8"
        )]
        let ingest_level = INITIAL_CANONICAL_LEVEL as u8;
        // TODO: maybe create a PrepareMultiWriter that can be used by flush, ingest and compaction worker
        let mut writer = MultiWriter::new(
            folder.clone(),
            tree.table_id_counter.clone(),
            64 * 1_024 * 1_024,
            ingest_level,
            level_fs.clone(),
        )?
        .set_comparator(tree.config.comparator.clone())
        .use_bloom_policy({
            if tree.config.expect_point_read_hits {
                crate::config::BloomConstructionPolicy::BitsPerKey(0.0)
            } else if let FilterPolicyEntry::Bloom(p) =
                tree.config.filter_policy.get(INITIAL_CANONICAL_LEVEL)
            {
                p
            } else {
                crate::config::BloomConstructionPolicy::BitsPerKey(0.0)
            }
        })
        .use_data_block_size(
            tree.config
                .data_block_size_policy
                .get(INITIAL_CANONICAL_LEVEL),
        )
        .use_data_block_hash_ratio(
            tree.config
                .data_block_hash_ratio_policy
                .get(INITIAL_CANONICAL_LEVEL),
        )
        .use_data_block_compression(
            tree.config
                .data_block_compression_policy
                .get(INITIAL_CANONICAL_LEVEL),
        )
        .use_index_block_compression(
            tree.config
                .index_block_compression_policy
                .get(INITIAL_CANONICAL_LEVEL),
        )
        .use_data_block_restart_interval(
            tree.config
                .data_block_restart_interval_policy
                .get(INITIAL_CANONICAL_LEVEL),
        )
        .use_index_block_restart_interval(
            tree.config
                .index_block_restart_interval_policy
                .get(INITIAL_CANONICAL_LEVEL),
        );

        // One runtime-config snapshot for the whole ingestion writer setup, so
        // a concurrent `update_runtime_config` can't leave the ingested SST
        // with `seqno_in_index` from one snapshot and checksum settings from
        // another. `Off` (default) emits no per-KV footer and leaves the
        // data-block payload encoding unchanged; the index format follows the
        // policy in force at ingestion.
        let rc = tree.0.runtime_config.load_full();

        if index_partitioning {
            // Size-adaptive index: single-level for small SSTs, spill to
            // partitioned only past the threshold (see flush path).
            writer = writer.use_adaptive_index(rc.index_partition_spill_threshold);
        }
        if filter_partitioning {
            writer = writer.use_partitioned_filter();
        }

        writer = writer.use_prefix_extractor(tree.config.prefix_extractor.clone());
        writer = writer.use_encryption(tree.config.encryption.clone());
        // ECC scheme from the same live snapshot as `seqno_in_index` /
        // `kv_checksums` below, so an ingestion started after a scheme change
        // writes its SST with the current scheme rather than the startup one.
        writer = writer.use_page_ecc(tree.config.page_ecc, rc.ecc_scheme);
        writer = writer.use_sync_mode(tree.config.sync_mode);

        writer = writer.use_seqno_in_index(rc.seqno_in_index);
        writer = writer.use_zone_map(rc.zone_map);
        // Match the flush writer: when the columnar layout is enabled, ingested
        // tables are columnar too (a row ingest is transposed at spill, a
        // columnar batch is stored directly via `write_columnar_batch`).
        writer = writer.use_columnar(rc.columnar);
        writer = writer.use_disable_cow_on_sst(rc.disable_cow_on_sst_files);
        writer = writer.use_kv_checksums(rc.kv_checksums, rc.kv_checksum_algo);
        writer = writer.use_locator(tree.config.locator_policy.get(INITIAL_CANONICAL_LEVEL));

        #[cfg(zstd_any)]
        {
            writer = writer.use_zstd_dictionary(tree.config.zstd_dictionary.clone());
        }

        Ok(Self {
            folder,
            level_fs,
            tree,
            writer,
            seqno: 0,
            last_key: None,
        })
    }

    /// Writes a key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(crate) fn write_indirection(
        &mut self,
        key: UserKey,
        indirection: BlobIndirection,
    ) -> crate::Result<()> {
        use crate::coding::Encode;

        if let Some(prev) = &self.last_key {
            assert!(
                self.tree.config.comparator.compare(prev, &key) == Ordering::Less,
                "next key in ingestion must be ordered after last key by configured comparator"
            );
        }

        let cloned_key = key.clone();
        self.writer.write(crate::InternalValue::from_components(
            key,
            indirection.encode_into_vec(),
            self.seqno,
            crate::ValueType::Indirection,
        ))?;

        self.writer.register_blob(indirection);

        // Remember the last user key to validate the next call's ordering
        self.last_key = Some(cloned_key);

        Ok(())
    }

    /// Writes a key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write(&mut self, key: UserKey, value: UserValue) -> crate::Result<()> {
        if let Some(prev) = &self.last_key {
            assert!(
                self.tree.config.comparator.compare(prev, &key) == Ordering::Less,
                "next key in ingestion must be ordered after last key by configured comparator"
            );
        }

        self.writer.write(crate::InternalValue::from_components(
            key.clone(),
            value,
            self.seqno,
            crate::ValueType::Value,
        ))?;

        // Remember the last user key to validate the next call's ordering
        self.last_key = Some(key);

        Ok(())
    }

    /// Writes a tombstone for a key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write_tombstone(&mut self, key: UserKey) -> crate::Result<()> {
        if let Some(prev) = &self.last_key {
            assert!(
                self.tree.config.comparator.compare(prev, &key) == Ordering::Less,
                "next key in ingestion must be ordered after last key by configured comparator"
            );
        }

        self.writer.write(crate::InternalValue::from_components(
            key.clone(),
            crate::UserValue::empty(),
            self.seqno,
            crate::ValueType::Tombstone,
        ))?;

        // Remember the last user key to validate the next call's ordering
        self.last_key = Some(key);

        Ok(())
    }

    /// Writes a weak tombstone for a key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write_weak_tombstone(&mut self, key: UserKey) -> crate::Result<()> {
        if let Some(prev) = &self.last_key {
            assert!(
                self.tree.config.comparator.compare(prev, &key) == Ordering::Less,
                "next key in ingestion must be ordered after last key by configured comparator"
            );
        }

        self.writer.write(crate::InternalValue::from_components(
            key.clone(),
            crate::UserValue::empty(),
            self.seqno,
            crate::ValueType::WeakTombstone,
        ))?;

        // Remember the last user key to validate the next call's ordering
        self.last_key = Some(key);

        Ok(())
    }

    /// Writes a consumer-provided columnar batch (its value sub-columns) as one
    /// columnar block.
    ///
    /// The batch carries the three intrinsic columns ([key, seqno, value-type])
    /// plus one or more value sub-columns. Its keys must be strictly increasing
    /// (by the tree comparator) within the batch and after any previously written
    /// data, and every per-row seqno must be `0`: the ingestion assigns the atomic
    /// global sequence number at [`finish`](Self::finish), shared by every
    /// ingested table.
    ///
    /// Requires the columnar layout (enable `columnar` in the runtime config
    /// before opening the ingestion); a row-mode ingestion rejects the batch. An
    /// ingestion is either row-oriented (via [`write`](Self::write)) or
    /// columnar, not both.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch shape is invalid, the keys are not strictly
    /// increasing, any row carries a non-zero seqno, the layout is not columnar,
    /// or a block write fails.
    #[cfg(feature = "columnar")]
    pub fn write_columnar_batch(
        &mut self,
        batch: &crate::table::columnar::ColumnBatch,
    ) -> crate::Result<()> {
        // Carry the batch's last key forward: it both records the ordering
        // boundary for any later write and signals `finish` that data was
        // written (it installs nothing when `last_key` is `None`).
        if let Some(last) = self.writer.write_columnar_batch(batch)? {
            self.last_key = Some(last);
        }
        Ok(())
    }

    /// Finishes the ingestion.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(clippy::significant_drop_tightening)]
    pub fn finish(self) -> crate::Result<()> {
        use crate::{AbstractTree, Table};

        if self.last_key.is_none() {
            log::trace!("No data written to Ingestion, returning early");
            return Ok(());
        }

        // CRITICAL SECTION: Atomic flush + seqno allocation + registration
        //
        // We must ensure no concurrent writes interfere between flushing the
        // active memtable and registering the ingested tables. The sequence is:
        //   1. Acquire flush lock (prevents concurrent flushes)
        //   2. Flush active memtable (ensures no pending writes)
        //   3. Finish ingestion writer (creates table files)
        //   4. Allocate next global seqno (atomic timestamp)
        //   5. Recover tables with that seqno
        //   6. Register version with same seqno
        //
        // Why not flush in new()?
        // If we flushed in new(), there would be a race condition:
        //   new() -> flush -> [TIME PASSES + OTHER WRITES] -> finish() -> seqno
        // The seqno would be disconnected from the flush, violating MVCC.
        //
        // By holding the flush lock throughout, we guarantee atomicity.
        let flush_lock = self.tree.get_flush_lock();

        // Flush any pending memtable writes to ensure ingestion sees a
        // consistent snapshot and lookup order remains correct.
        // We call rotate + flush directly because we already hold the lock.
        self.tree.rotate_memtable();
        self.tree.flush(&flush_lock, 0)?;

        // Finalize the ingestion writer, writing all buffered data to disk.
        let results = self.writer.finish()?;

        log::info!("Finished ingestion writer");

        // Acquire locks for version registration. We must hold both the
        // compaction state lock and version history lock to safely modify
        // the tree's version.
        let mut _compaction_state = self.tree.compaction_state.lock();

        let mut version_lock = self.tree.version_history.write();

        // Allocate the next global sequence number. This seqno will be shared
        // by all ingested tables and the version that registers them, ensuring
        // consistent MVCC snapshots.
        let global_seqno = self.tree.config.seqno.next();

        // Recover all created tables, assigning them the global_seqno we just
        // allocated. This ensures all ingested tables share the same sequence
        // number, which is critical for MVCC correctness.
        //
        // We intentionally do NOT pin filter/index blocks here. Large ingests
        // are typically placed in level 1, and pinning would increase memory
        // pressure unnecessarily.
        let created_tables = results
            .into_iter()
            .map(|(table_id, checksum)| -> crate::Result<Table> {
                Table::recover(
                    self.folder.join(table_id.to_string()),
                    checksum,
                    global_seqno,
                    self.tree.id,
                    table_id,
                    self.tree.config.cache.clone(),
                    self.tree.config.descriptor_table.clone(),
                    self.level_fs.clone(),
                    false,
                    false,
                    self.tree.config.encryption.clone(),
                    #[cfg(zstd_any)]
                    self.tree.config.zstd_dictionary.clone(),
                    self.tree.config.comparator.clone(),
                    #[cfg(feature = "metrics")]
                    self.tree.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // Upgrade the version with our ingested tables, using the global_seqno
        // we allocated earlier. This ensures the version and all tables share
        // the same sequence number.
        //
        // We use upgrade_version_with_seqno (instead of upgrade_version) because
        // we need precise control over the seqno: it must match the seqno we
        // already assigned to the recovered tables.
        version_lock.upgrade_version_with_seqno(
            &self.tree.config.path,
            |current| {
                let mut copy = current.clone();
                let ctx =
                    crate::version::TransformContext::new(self.tree.config.comparator.as_ref());
                copy.version = copy
                    .version
                    .with_new_l0_run(&created_tables, None, None, &ctx);
                Ok(copy)
            },
            global_seqno,
            &self.tree.config.visible_seqno,
            &*self.tree.config.fs,
            self.tree.0.runtime_config.load_full(),
            self.tree.0.config.encryption.clone(),
        )?;

        // Perform maintenance on the version history (e.g., clean up old versions).
        // We use gc_watermark=0 since ingestion doesn't affect sealed memtables.
        if let Err(e) = version_lock.maintenance(&self.tree.config.path, 0, &*self.tree.config.fs) {
            log::warn!("Version GC failed: {e:?}");
        }

        Ok(())
    }
}
