// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::blob_tree::FragmentationMap;
use crate::blob_tree::handle::BlobIndirection;
use crate::coding::{Decode, Encode};
use crate::compaction::Input as CompactionPayload;
use crate::compaction::worker::Options;
use crate::range_tombstone::RangeTombstone;
use crate::table::multi_writer::MultiWriter;
use crate::time::Instant;
use crate::version::{SuperVersions, Version};
use crate::vlog::blob_file::scanner::ScanEntry;
use crate::vlog::{BlobFileId, BlobFileMergeScanner, BlobFileWriter};
use crate::{BlobFile, HashSet, InternalValue, Table};
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::ToString, vec::Vec};
use core::iter::Peekable;

/// Drains all blobs that come "before" the given vptr.
fn drain_blobs<I: Iterator<Item = crate::Result<(ScanEntry, BlobFileId)>>>(
    scanner: &mut Peekable<I>,
    key: &[u8],
    vptr: &BlobIndirection,
) -> crate::Result<()> {
    loop {
        let Some(blob) = scanner.next_if(|x| match x {
            Ok((entry, blob_file_id)) => {
                entry.key != key
                    || (*blob_file_id != vptr.vhandle.blob_file_id)
                    || (entry.offset < vptr.vhandle.offset)
            }
            Err(_) => true,
        }) else {
            break;
        };
        let (entry, _) = blob?;

        assert!(entry.key <= key, "vptr was not matched with blob");
    }

    Ok(())
}

pub(super) fn prepare_table_writer(
    version: &Version,
    opts: &Options,
    payload: &CompactionPayload,
    // When false, the writer compresses blocks serially. Used by parallel
    // sub-compactions, which already run on the compaction pool: re-submitting
    // block compression to the same pool from a pool thread would deadlock
    // (the draining thread parks on a worker slot that can't be scheduled).
    block_parallel: bool,
) -> crate::Result<MultiWriter> {
    let (table_base_folder, level_fs) = opts.config.tables_folder_for_level(payload.dest_level);

    let dst_lvl = payload.canonical_level.into();

    let data_block_size = opts.config.data_block_size_policy.get(dst_lvl);

    let data_block_restart_interval = opts.config.data_block_restart_interval_policy.get(dst_lvl);
    let index_block_restart_interval = opts.config.index_block_restart_interval_policy.get(dst_lvl);

    let data_block_compression = opts.config.data_block_compression_policy.get(dst_lvl);
    let index_block_compression = opts.config.index_block_compression_policy.get(dst_lvl);

    let data_block_hash_ratio = opts.config.data_block_hash_ratio_policy.get(dst_lvl);

    let index_partitioning = opts.config.index_block_partitioning_policy.get(dst_lvl);
    let filter_partitioning = opts.config.filter_block_partitioning_policy.get(dst_lvl);

    log::debug!(
        "Compacting tables {:?} into L{} (canonical L{}), target_size={}, data_block_restart_interval={data_block_restart_interval}, index_block_restart_interval={index_block_restart_interval}, data_block_size={data_block_size}, data_block_compression={data_block_compression:?}, index_block_compression={index_block_compression:?}, mvcc_gc_watermark={}",
        payload.table_ids,
        payload.dest_level,
        payload.canonical_level,
        payload.target_size,
        opts.mvcc_gc_watermark,
    );

    let mut table_writer = MultiWriter::new(
        table_base_folder,
        opts.table_id_generator.clone(),
        payload.target_size,
        payload.dest_level,
        level_fs,
    )?
    .set_comparator(opts.config.comparator.clone())
    // Compaction consumes input tables, so clip RTs to each output table's key range.
    .use_clip_range_tombstones();

    // One runtime-config snapshot for the whole writer setup: reading
    // `load_full()` per field could straddle a concurrent
    // `update_runtime_config`, letting one SST mix `seqno_in_index` from
    // snapshot A with `kv_checksums` from snapshot B and breaking the
    // single-snapshot-per-compaction contract.
    let rc = opts.runtime_config.load_full();

    if index_partitioning {
        // Size-adaptive index: single-level for small SSTs, spill to a
        // partitioned index only past the threshold (see flush path).
        table_writer = table_writer.use_adaptive_index(rc.index_partition_spill_threshold);
    }
    if filter_partitioning {
        table_writer = table_writer.use_partitioned_filter();
    }

    #[expect(clippy::cast_possible_truncation, reason = "max key size = u16")]
    let last_level = (version.level_count() - 1) as u8;
    let is_last_level = payload.dest_level == last_level;

    let table_writer = table_writer
        .use_data_block_restart_interval(data_block_restart_interval)
        .use_index_block_restart_interval(index_block_restart_interval)
        .use_data_block_compression(data_block_compression)
        .use_data_block_size(data_block_size)
        .use_data_block_hash_ratio(data_block_hash_ratio)
        .use_index_block_compression(index_block_compression)
        // NOTE: prefix_extractor before bloom_policy is safe here because
        // use_bloom_policy calls set_filter_policy which mutates the existing
        // filter writer (preserving the extractor). Only use_partitioned_filter
        // replaces the writer entirely (handled above, lines 85-90).
        .use_prefix_extractor(opts.config.prefix_extractor.clone())
        .use_encryption(opts.config.encryption.clone())
        // Read the ECC scheme from the same live snapshot as the other
        // runtime-config-driven settings (e.g. `seqno_in_index` below) so a
        // compaction started after a scheme change stamps its output SSTs
        // with the current scheme, not the startup one.
        .use_page_ecc(opts.config.page_ecc, rc.ecc_scheme)
        .use_sync_mode(opts.config.sync_mode)
        // `seqno_in_index` is a live runtime config: read off the current
        // snapshot so a compaction started after a toggle rewrites its
        // output SSTs in the new index format (compaction is the migration
        // mechanism for the on-disk index layout).
        .use_seqno_in_index(rc.seqno_in_index)
        .use_disable_cow_on_sst(rc.disable_cow_on_sst_files)
        .use_bloom_policy({
            use crate::config::FilterPolicyEntry::{Bloom, None};
            use crate::table::filter::BloomConstructionPolicy;

            if is_last_level && opts.config.expect_point_read_hits {
                BloomConstructionPolicy::BitsPerKey(0.0)
            } else {
                match opts
                    .config
                    .filter_policy
                    .get(usize::from(payload.dest_level))
                {
                    Bloom(policy) => policy,
                    None => BloomConstructionPolicy::BitsPerKey(0.0),
                }
            }
        });

    // Per-KV checksums follow the LIVE runtime config snapshot, so a toggle
    // via `update_runtime_config` migrates data through compaction: each
    // rewritten block reflects the current policy. `Off` (default) emits no
    // per-KV footer and leaves the `KV_CHECKSUM_FOOTER` flag clear (the
    // data-block payload encoding is unchanged; the V5 header/meta layout
    // still differs from pre-V5 regardless).
    let table_writer = table_writer.use_kv_checksums(rc.kv_checksums, rc.kv_checksum_algo);
    // Resolve the locator policy for the output level (compaction is the
    // migration mechanism: a toggle takes effect as data is rewritten down).
    let table_writer = table_writer.use_locator(
        opts.config
            .locator_policy
            .get(usize::from(payload.dest_level)),
    );

    #[cfg(zstd_any)]
    let table_writer = table_writer.use_zstd_dictionary(opts.config.zstd_dictionary.clone());

    // Parallel block compression: hand the (per-tree or caller-shared) pool to
    // the writer so its CPU-bound transform work runs on worker threads while
    // writes stay ordered. None / single-thread leaves the serial path.
    // Skipped for sub-compaction writers (block_parallel = false), which already
    // occupy a pool thread and would deadlock re-submitting to the same pool.
    #[cfg(feature = "std")]
    let table_writer = if block_parallel {
        table_writer.use_parallel_compression(
            opts.config.compaction_pool.clone(),
            opts.config.compaction_threads,
        )
    } else {
        table_writer
    };
    #[cfg(not(feature = "std"))]
    let _ = block_parallel;

    Ok(table_writer)
}

/// Output of one (sub-)compaction's write phase: finalized SSTs and blob files
/// plus what it consumed — everything needed to install a version edit, but
/// WITHOUT touching the shared version. Splitting "produce" from "install" lets
/// N parallel sub-compactions each finalize their files independently, then a
/// single atomic version upgrade ([`install_merge`]) merges all of them.
pub(super) struct ProducedOutput {
    created_tables: Vec<Table>,
    created_blob_files: Vec<BlobFile>,
    /// Blob files this (sub-)compaction rewrote and must drop. Globally-dead
    /// blob files are added once at install time, not per sub-compaction.
    rewritten_blob_files_to_drop: Vec<BlobFile>,
    tables_to_delete: Vec<Table>,
    blob_frag_map: FragmentationMap,
}

impl ProducedOutput {
    /// Marks this produced-but-not-installed output's freshly written files as
    /// deleted. Used when a sibling sub-compaction fails and the shared
    /// [`install_merge`] is skipped: each already-finished sub-compaction has
    /// finalized its SSTs and blob files on disk, so without this they would be
    /// orphaned. Only the newly created files are dropped — input tables stay
    /// intact (the caller un-hides them) and rewritten-blob-file drops are left
    /// for a later successful compaction.
    /// The SSTs this (sub-)compaction finalized on disk but has not installed.
    /// Used by the tight-space loop, which installs them via a custom version
    /// edit (restricting the input) rather than the standard [`install_merge`].
    pub(super) fn created_tables(&self) -> &[Table] {
        &self.created_tables
    }

    pub(super) fn rollback_uninstalled(&self) {
        for table in &self.created_tables {
            table.mark_as_deleted();
        }
        for blob_file in &self.created_blob_files {
            blob_file.mark_as_deleted();
        }
    }
}

// TODO: find a better name
pub(super) trait CompactionFlavour {
    fn write(&mut self, item: InternalValue) -> crate::Result<()>;

    /// Writes range tombstones to the current output table.
    fn write_range_tombstones(&mut self, tombstones: &[RangeTombstone]);

    /// Finalizes this (sub-)compaction's output files (flushing the table
    /// writer, finishing blob writers) WITHOUT installing a version edit. The
    /// returned [`ProducedOutput`] is handed to [`install_merge`] — alone for a
    /// single compaction, or alongside sibling outputs for parallel
    /// sub-compactions.
    fn produce(
        self: Box<Self>,
        opts: &Options,
        dst_lvl: usize,
        blob_frag_map: FragmentationMap,
        extra_blob_files: Vec<BlobFile>,
    ) -> crate::Result<ProducedOutput>;
}

/// Installs one atomic version edit replacing `payload.table_ids` with the SSTs
/// and blob files produced by all `outputs` (one for a single compaction, N for
/// parallel sub-compactions). Globally-dead blob files are dropped once here.
/// Returns the total number of output tables.
pub(super) fn install_merge(
    super_version: &mut SuperVersions,
    opts: &Options,
    payload: &CompactionPayload,
    outputs: Vec<ProducedOutput>,
) -> crate::Result<usize> {
    let mut created_tables = Vec::new();
    let mut created_blob_files = Vec::new();
    let mut blob_files_to_drop = Vec::new();
    let mut tables_to_delete = Vec::new();
    let mut blob_frag_map = FragmentationMap::default();

    for out in outputs {
        created_tables.extend(out.created_tables);
        created_blob_files.extend(out.created_blob_files);
        blob_files_to_drop.extend(out.rewritten_blob_files_to_drop);
        tables_to_delete.extend(out.tables_to_delete);
        out.blob_frag_map.merge_into(&mut blob_frag_map);
    }

    let tables_out = created_tables.len();

    // Globally-dead blob files are dropped once, from the install-time version.
    let current_version = super_version.latest_version();
    for blob_file in current_version.version.blob_files.iter() {
        if blob_file.is_dead(current_version.version.gc_stats()) {
            blob_files_to_drop.push(blob_file.clone());
        }
    }

    // Handles kept for rollback: the output SSTs and blob files are already
    // finalized on disk, so if the version edit fails they must be marked
    // deleted here or they leak (the caller only un-hides the input tables).
    // `created_blob_files` is moved into the closure, so clone for cleanup.
    let rollback_blob_files = created_blob_files.clone();
    super_version
        .upgrade_version(
            &opts.config.path,
            |current| {
                let mut copy = current.clone();

                let ctx = crate::version::TransformContext::new(opts.config.comparator.as_ref());
                copy.version = copy.version.with_merge(
                    &payload.table_ids.iter().copied().collect::<Vec<_>>(),
                    &created_tables,
                    payload.dest_level as usize,
                    if blob_frag_map.is_empty() {
                        None
                    } else {
                        Some(blob_frag_map)
                    },
                    created_blob_files,
                    &blob_files_to_drop
                        .iter()
                        .map(BlobFile::id)
                        .collect::<HashSet<_>>(),
                    &ctx,
                );

                Ok(copy)
            },
            &opts.global_seqno,
            &opts.visible_seqno,
            &*opts.config.fs,
            opts.runtime_config.load_full(),
            opts.encryption.clone(),
        )
        .inspect_err(|_| {
            for table in &created_tables {
                table.mark_as_deleted();
            }
            for blob_file in &rollback_blob_files {
                blob_file.mark_as_deleted();
            }
        })?;

    // NOTE: If the application were to crash >here< it's fine — the tables /
    // blob files are not referenced anymore and are cleaned up upon recovery.
    for table in tables_to_delete {
        table.mark_as_deleted();
    }
    for blob_file in blob_files_to_drop {
        blob_file.mark_as_deleted();
    }

    Ok(tables_out)
}

/// Compaction worker that will relocate blobs that sit in blob files that are being rewritten
pub struct RelocatingCompaction {
    inner: StandardCompaction,
    blob_scanner: Peekable<BlobFileMergeScanner>,
    blob_writer: BlobFileWriter,
    rewriting_blob_file_ids: HashSet<BlobFileId>,
    rewriting_blob_files: Vec<BlobFile>,
    /// Paces relocated-blob I/O. The merge loop's limiter only sees the
    /// encoded handle in `item.value`; the real payload moved here is
    /// debited at the relocation write site so KV-separated compactions
    /// are throttled by their actual bandwidth.
    rate_limiter: alloc::sync::Arc<crate::rate_limiter::RateLimiter>,
    /// Polled by the blob throttle so a long wait under a low limit stays
    /// interruptible by tree drop / shutdown.
    stop_signal: crate::stop_signal::StopSignal,
}

impl RelocatingCompaction {
    pub fn new(
        inner: StandardCompaction,
        blob_scanner: Peekable<BlobFileMergeScanner>,
        blob_writer: BlobFileWriter,
        rewriting_blob_files: Vec<BlobFile>,
        rate_limiter: alloc::sync::Arc<crate::rate_limiter::RateLimiter>,
        stop_signal: crate::stop_signal::StopSignal,
    ) -> Self {
        Self {
            inner,
            blob_scanner,
            blob_writer,
            rewriting_blob_file_ids: rewriting_blob_files.iter().map(BlobFile::id).collect(),
            rewriting_blob_files,
            rate_limiter,
            stop_signal,
        }
    }

    // TODO: vvv validate/unit test this vvv
    fn drain_blobs(&mut self, key: &[u8], indirection: &BlobIndirection) -> crate::Result<()> {
        drain_blobs(&mut self.blob_scanner, key, indirection)
    }
}

impl CompactionFlavour for RelocatingCompaction {
    fn write_range_tombstones(&mut self, tombstones: &[RangeTombstone]) {
        self.inner.write_range_tombstones(tombstones);
    }

    fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        if item.key.value_type.is_indirection() {
            let mut reader = &item.value[..];

            let indirection = BlobIndirection::decode_from(&mut reader).inspect_err(|e| {
                log::error!("Failed to deserialize blob indirection {item:?}: {e:?}");
            })?;

            log::trace!(
                "{:?}:{} => encountered indirection: {indirection:?}",
                item.key.user_key,
                item.key.seqno,
            );

            let indirection = if self
                .rewriting_blob_file_ids
                .contains(&indirection.vhandle.blob_file_id)
            {
                self.drain_blobs(&item.key.user_key, &indirection)?;

                #[expect(clippy::expect_used, reason = "vptr is expected to match with blob")]
                let (blob_entry, blob_file_id) = self
                    .blob_scanner
                    .next()
                    .expect("vptr was not matched with blob (scanner is unexpectedly exhausted)")?;

                assert_eq!(
                    blob_file_id, indirection.vhandle.blob_file_id,
                    "matched blob has different blob file ID than vptr",
                );
                assert_eq!(
                    blob_entry.key, item.key.user_key,
                    "matched blob has different key than vptr",
                );
                assert_eq!(
                    blob_entry.offset, indirection.vhandle.offset,
                    "matched blob has different offset than vptr",
                );

                log::trace!(
                    "=> use blob: {:?}:{} offset: {} from BF {}",
                    blob_entry.key,
                    blob_entry.seqno,
                    blob_entry.offset,
                    blob_file_id,
                );

                log::trace!("RELOCATE to {indirection:?}");

                // Throttle the relocated blob payload — this is the heavy
                // KV-separation I/O the merge-loop limiter cannot see (it
                // only has the encoded handle). Interruptible so a low
                // limit can't stall shutdown; the return is ignored because
                // the blob is already read and must be written to keep the
                // new vptr valid — only the *wait* is shortened on stop.
                let _ = self
                    .rate_limiter
                    .request_interruptible(blob_entry.value.len() as u64, || {
                        self.stop_signal.is_stopped()
                    });

                let new_indirection = BlobIndirection {
                    vhandle: self.blob_writer.write_raw(
                        &item.key.user_key,
                        item.key.seqno,
                        &blob_entry.value,
                        blob_entry.uncompressed_len,
                    )?,
                    size: indirection.size,
                };

                debug_assert_eq!(
                    new_indirection.vhandle.on_disk_size, indirection.vhandle.on_disk_size,
                    "redirecting blob should not change its size",
                );

                self.inner
                    .table_writer
                    .write(InternalValue::from_components(
                        item.key.user_key,
                        new_indirection.encode_into_vec(),
                        item.key.seqno,
                        crate::ValueType::Indirection,
                    ))?;

                new_indirection
            } else {
                // This blob is not part of the rewritten blob files
                // So just pass it through
                log::trace!("Pass through {indirection:?} because it is not being relocated");
                self.inner.table_writer.write(item)?;

                indirection
            };

            self.inner.table_writer.register_blob(indirection);
        } else {
            self.inner.table_writer.write(item)?;
        }

        Ok(())
    }

    fn produce(
        mut self: Box<Self>,
        opts: &Options,
        dst_lvl: usize,
        blob_frag_map: FragmentationMap,
        extra_blob_files: Vec<BlobFile>,
    ) -> crate::Result<ProducedOutput> {
        log::debug!(
            "Relocating compaction done in {:?}",
            self.inner.start.elapsed(),
        );

        let tables_to_delete = core::mem::take(&mut self.inner.tables_to_rewrite);

        let created_tables = self.inner.consume_writer(opts, dst_lvl)?;
        // The output SSTs are already finalized; if blob finalization fails the
        // compaction aborts, so delete them here or they orphan on disk.
        let mut created_blob_files = self.blob_writer.finish().inspect_err(|_| {
            for table in &created_tables {
                table.mark_as_deleted();
            }
        })?;
        created_blob_files.extend(extra_blob_files);

        Ok(ProducedOutput {
            created_tables,
            created_blob_files,
            rewritten_blob_files_to_drop: self.rewriting_blob_files,
            tables_to_delete,
            blob_frag_map,
        })
    }
}

/// Standard compaction worker that just passes through all its data
pub struct StandardCompaction {
    start: Instant,
    table_writer: MultiWriter,
    tables_to_rewrite: Vec<Table>,
}

impl StandardCompaction {
    pub fn new(table_writer: MultiWriter, tables_to_rewrite: Vec<Table>) -> Self {
        Self {
            start: Instant::now(),
            table_writer,
            tables_to_rewrite,
        }
    }

    fn consume_writer(self, opts: &Options, dst_lvl: usize) -> crate::Result<Vec<Table>> {
        let table_base_folder = self.table_writer.base_path.clone();
        let level_fs = self.table_writer.fs.clone();

        let pin_filter = opts.config.filter_block_pinning_policy.get(dst_lvl);
        let pin_index = opts.config.index_block_pinning_policy.get(dst_lvl);

        self.table_writer
            .finish()?
            .into_iter()
            .map(|(table_id, checksum)| -> crate::Result<Table> {
                Table::recover(
                    table_base_folder.join(table_id.to_string()),
                    checksum,
                    0,
                    opts.tree_id,
                    table_id,
                    opts.config.cache.clone(),
                    opts.config.descriptor_table.clone(),
                    level_fs.clone(),
                    pin_filter,
                    pin_index,
                    opts.config.encryption.clone(),
                    #[cfg(zstd_any)]
                    opts.config.zstd_dictionary.clone(),
                    opts.config.comparator.clone(),
                    #[cfg(feature = "metrics")]
                    opts.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()
    }
}

impl CompactionFlavour for StandardCompaction {
    fn write_range_tombstones(&mut self, tombstones: &[RangeTombstone]) {
        self.table_writer.set_range_tombstones(tombstones.to_vec());
    }

    fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        let indirection = if item.key.value_type.is_indirection() {
            Some({
                let mut reader = &item.value[..];
                BlobIndirection::decode_from(&mut reader)?
            })
        } else {
            None
        };

        self.table_writer.write(item)?;

        if let Some(indirection) = indirection {
            self.table_writer.register_blob(indirection);
        }

        Ok(())
    }

    fn produce(
        mut self: Box<Self>,
        opts: &Options,
        dst_lvl: usize,
        blob_frag_map: FragmentationMap,
        extra_blob_files: Vec<BlobFile>,
    ) -> crate::Result<ProducedOutput> {
        log::debug!("Compaction done in {:?}", self.start.elapsed());

        let tables_to_delete = core::mem::take(&mut self.tables_to_rewrite);
        let created_tables = self.consume_writer(opts, dst_lvl)?;

        Ok(ProducedOutput {
            created_tables,
            // A standard compaction rewrites no blob files; it only passes
            // through indirections. The only blob files it emits are those the
            // compaction filter created (threaded in as `extra_blob_files`).
            created_blob_files: extra_blob_files,
            rewritten_blob_files_to_drop: Vec::new(),
            tables_to_delete,
            blob_frag_map,
        })
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{UserKey, UserValue, vlog::ValueHandle};

    #[expect(clippy::unnecessary_wraps)]
    fn entry(
        blob_file_id: BlobFileId,
        key: &[u8],
        offset: u64,
    ) -> crate::Result<(ScanEntry, BlobFileId)> {
        Ok((
            ScanEntry {
                key: UserKey::from(key),
                offset,
                seqno: 0,
                uncompressed_len: 0,
                value: UserValue::empty(),
            },
            blob_file_id,
        ))
    }

    #[test]
    fn drain_blobs_simple() -> crate::Result<()> {
        let mut iter = [
            entry(0, b"a", 0),
            entry(0, b"a", 1),
            entry(0, b"a", 2),
            entry(0, b"a", 3),
            entry(0, b"a", 4),
        ]
        .into_iter()
        .peekable();

        drain_blobs(
            &mut iter,
            b"a",
            &BlobIndirection {
                size: 0,
                vhandle: ValueHandle {
                    blob_file_id: 0,
                    offset: 4,
                    on_disk_size: 0,
                },
            },
        )?;

        assert_eq!(entry(0, b"a", 4)?, iter.next().unwrap()?);

        Ok(())
    }

    #[test]
    fn drain_blobs_multiple_keys() -> crate::Result<()> {
        let mut iter = [
            entry(0, b"a", 0),
            entry(0, b"b", 0),
            entry(0, b"c", 0),
            entry(0, b"d", 0),
            entry(0, b"e", 0),
        ]
        .into_iter()
        .peekable();

        drain_blobs(
            &mut iter,
            b"e",
            &BlobIndirection {
                size: 0,
                vhandle: ValueHandle {
                    blob_file_id: 0,
                    offset: 0,
                    on_disk_size: 0,
                },
            },
        )?;

        assert_eq!(entry(0, b"e", 0)?, iter.next().unwrap()?);

        Ok(())
    }
}
