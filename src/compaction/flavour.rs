// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

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
///
/// `record_consumed` is invoked with `(blob_file_id, frame_end)` for every
/// drained entry so the tight-space relocation loop can advance its per-file
/// punch / resume frontier past the bytes this drain reclaimed. The non-tight
/// path passes a no-op.
fn drain_blobs<I: Iterator<Item = crate::Result<(ScanEntry, BlobFileId)>>>(
    scanner: &mut Peekable<I>,
    key: &[u8],
    vptr: &BlobIndirection,
    comparator: &dyn crate::comparator::UserComparator,
    record_consumed: &mut dyn FnMut(BlobFileId, u64),
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
        let (entry, blob_file_id) = blob?;

        // The scan and the table stream advance in the same comparator order,
        // so everything drained before a key sorts at or before it.
        assert!(
            comparator.compare(&entry.key, key) != core::cmp::Ordering::Greater,
            "vptr was not matched with blob"
        );
        // A RESYNCED entry's boundary is unproven (see `ScanEntry::resynced`):
        // advancing the reclaim frontier to its `frame_end` could punch past —
        // and then skip on resume — a later frame that is actually valid. Drop
        // the tainted entry WITHOUT advancing the frontier, so a resumed scan
        // re-reads from the last proven boundary (fail closed). The matched-vptr
        // relocation path refuses a resynced frame outright for the same reason.
        if !entry.resynced {
            record_consumed(blob_file_id, entry.frame_end);
        }
    }

    Ok(())
}

/// Drains the frames of keys below `bound`, recording each proven frame's end
/// through `record_consumed` as `drain_blobs` does; a resynchronized frame is
/// consumed without moving the frontier. `is_stopped` is polled before every
/// frame: the drain can cover a whole dead prefix after the merge loop's last
/// cancellation check, so a stop must not wait for it.
fn drain_unclaimed_below<I: Iterator<Item = crate::Result<(ScanEntry, BlobFileId)>>>(
    scanner: &mut Peekable<I>,
    bound: &[u8],
    comparator: &dyn crate::comparator::UserComparator,
    record_consumed: &mut dyn FnMut(BlobFileId, u64),
    is_stopped: &dyn Fn() -> bool,
) -> crate::Result<()> {
    loop {
        if is_stopped() {
            return Err(super::worker::cancelled_compaction());
        }
        let Some(next) = scanner.next_if(|x| match x {
            Ok((entry, _)) => comparator.compare(&entry.key, bound) == core::cmp::Ordering::Less,
            Err(_) => true,
        }) else {
            return Ok(());
        };
        let (entry, blob_file_id) = next?;
        if !entry.resynced {
            record_consumed(blob_file_id, entry.frame_end);
        }
    }
}

/// The filter policy of a compaction's destination level: none at the last
/// level when point reads are expected to hit.
fn output_bloom_policy(
    version: &Version,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::table::filter::BloomConstructionPolicy {
    use crate::config::FilterPolicyEntry::{Bloom, None};
    use crate::table::filter::BloomConstructionPolicy;

    #[expect(clippy::cast_possible_truncation, reason = "max key size = u16")]
    let last_level = (version.level_count() - 1) as u8;
    if payload.dest_level == last_level && opts.config.expect_point_read_hits {
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
}

/// Plans the filter widths of a compaction's outputs when the tree allocates
/// filter memory by probe load. One plan serves every writer of the
/// compaction, so its sub-compactions draw on one reservation of the budget;
/// `boundaries` split it into the key ranges they run side by side (none for
/// a compaction written in one key order); `span` names the keys it writes
/// when it rewrites only part of its inputs, as a tight-space slice does.
pub(super) fn plan_filters(
    version: &Version,
    opts: &Options,
    payload: &CompactionPayload,
    boundaries: &[crate::UserKey],
    span: Option<(
        core::ops::Bound<crate::UserKey>,
        core::ops::Bound<crate::UserKey>,
    )>,
    // The compaction's runtime-config snapshot, whose ECC scheme frames the
    // filter blocks its writers write.
    rc: &crate::runtime_config::RuntimeConfig,
) -> Option<crate::filter_budget::FilterPlan> {
    let advisor = opts.config.filter_advisor.as_ref()?;
    let inputs = version
        .iter_tables()
        .filter(|table| payload.table_ids.contains(&table.id()))
        .cloned()
        .collect();
    let split = (!boundaries.is_empty()).then(|| crate::filter_budget::Split {
        boundaries: boundaries.to_vec(),
        comparator: opts.config.comparator.clone(),
    });
    let span = span.map(|(lower, upper)| crate::filter_budget::Span {
        lower,
        upper,
        comparator: opts.config.comparator.clone(),
    });
    let dst_lvl = payload.canonical_level.into();
    crate::filter_budget::plan(
        advisor,
        &opts.filter_budget,
        &crate::filter_budget::live(version),
        crate::filter_budget::Rewrite {
            inputs,
            split,
            span,
            count: crate::filter_budget::FilterCount::default(),
            comparator: Some(opts.config.comparator.clone()),
            framing: crate::filter_budget::Framing {
                encryption: opts.config.encryption.clone(),
                ecc: crate::table::writer::resolve_ecc(opts.config.page_ecc, rc.ecc_scheme),
            },
        },
        output_bloom_policy(version, opts, payload),
        opts.config
            .filter_block_partitioning_policy
            .get(dst_lvl)
            .then(|| opts.config.filter_block_partition_size_policy.get(dst_lvl)),
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "each argument is a per-compaction choice its writers share (the filter \
              plan, the transform counter, the runtime snapshot); bundling them would \
              only move the list"
)]
pub(super) fn prepare_table_writer(
    version: &Version,
    opts: &Options,
    payload: &CompactionPayload,
    // The compaction's filter plan (see `plan_filters`), shared by all of its
    // writers; `None` builds every filter at the level's policy.
    filter_sizing: Option<crate::filter_budget::FilterPlan>,
    // When false, the writer compresses blocks serially. Used by parallel
    // sub-compactions, which already run on the compaction pool: with N ranges
    // occupying the pool's N workers, submitting block jobs there mostly
    // degrades to the pipeline's inline help path anyway (extra queue traffic
    // for no extra parallelism — no longer a deadlock since the help-first
    // drain, but not a win either without spare workers).
    block_parallel: bool,
    // The compaction-filter transform counter shared with the filter adapter
    // (`Some` iff a filter is instantiated for this run). Lineage is stamped
    // unconditionally; the writer MARKS it transformed on any output whose
    // window saw a non-`Keep` verdict — that output is not derivable from
    // its inputs (Remove / RemoveWeak / ReplaceValue / Destroy are
    // authoritative transformations), so manifest repair supersedes the
    // inputs it covers instead of trading it back for them. A `Keep`-only
    // filtered run stays plain, so the rebuild's duplicate-history dedup is
    // unaffected.
    transform_marker: Option<alloc::sync::Arc<portable_atomic::AtomicU64>>,
    // `true` when this writer receives the run's ENTIRE merged stream (a
    // serial compaction): its final output then carries the `lineage_last`
    // marker, proving to manifest repair that a surviving first-to-last
    // chain is the complete output set. `false` for parallel
    // sub-compactions and tight-space slices, which share one lineage
    // across several writers.
    whole_run: bool,
    // The compaction's runtime-config snapshot, the one it takes for
    // everything it writes (the filter's blob files too) and holds until the
    // install. Reading `load_full()` per field could straddle a concurrent
    // `update_runtime_config`, letting one SST mix its compression or
    // `seqno_in_index` from snapshot A with `kv_checksums` from snapshot B.
    rc: &crate::runtime_config::RuntimeConfig,
) -> crate::Result<MultiWriter> {
    let (table_base_folder, level_fs) = opts.config.tables_folder_for_level(payload.dest_level);

    let dst_lvl = payload.canonical_level.into();

    let data_block_size = opts.config.data_block_size_policy.get(dst_lvl);

    let data_block_restart_interval = opts.config.data_block_restart_interval_policy.get(dst_lvl);
    let index_block_restart_interval = opts.config.index_block_restart_interval_policy.get(dst_lvl);

    let data_block_compression = rc.data_block_compression_policy.get(dst_lvl);
    let index_block_compression = rc.index_block_compression_policy.get(dst_lvl);

    let data_block_hash_ratio = opts.config.data_block_hash_ratio_policy.get(dst_lvl);

    let index_partitioning = opts.config.index_block_partitioning_policy.get(dst_lvl);
    let filter_partitioning = opts.config.filter_block_partitioning_policy.get(dst_lvl);

    log::debug!(
        "Compacting tables {:?} into L{} (canonical L{}), target_size={}, data_block_restart_interval={data_block_restart_interval}, index_block_restart_interval={index_block_restart_interval}, data_block_size={data_block_size}, data_block_compression={data_block_compression:?}, index_block_compression={index_block_compression:?}, gc_watermark={}",
        payload.table_ids,
        payload.dest_level,
        payload.canonical_level,
        payload.target_size,
        opts.gc_watermark,
    );

    // The outputs' L0 recency key: the newest input's recency. An output's own
    // id is allocated HERE, at write start, while newer flushes with lower ids
    // can still install before this compaction commits — so the id must not
    // stand in for content recency when manifest repair rebuilds L0 from the
    // files alone.
    let recency = version
        .iter_tables()
        .filter(|t| payload.table_ids.contains(&t.id()))
        .map(crate::table::Table::l0_recency)
        .max();

    let mut table_writer = MultiWriter::new(
        table_base_folder,
        opts.table_id_generator.clone(),
        payload.target_size,
        payload.dest_level,
        level_fs,
    )?
    .use_output_ledger(opts.outputs.clone())
    .set_comparator(opts.config.comparator.clone())
    .use_recency(recency)
    // The outputs' compaction lineage: the input ids this run merges. Lets a
    // manifest-loss rebuild recognize the output as DERIVED and exclude it
    // when every input survived a crash-before-commit, instead of publishing
    // both histories and double-applying the merge operands on read. An
    // output a filter TRANSFORMS is marked so (see `transform_marker`).
    .use_lineage(Some(payload.table_ids.iter().copied().collect()))
    .use_lineage_whole_run(whole_run)
    // Compaction consumes input tables, so clip RTs to each output table's key range.
    .use_clip_range_tombstones();

    if let Some(marker) = transform_marker {
        table_writer = table_writer.use_transform_marker(marker);
    }

    if index_partitioning {
        // Size-adaptive index: single-level for small SSTs, spill to a
        // partitioned index only past the threshold (see flush path).
        table_writer = table_writer.use_adaptive_index(rc.index_partition_spill_threshold);
    }
    if filter_partitioning {
        table_writer = table_writer.use_partitioned_filter();
    }

    let table_writer = table_writer
        .use_data_block_restart_interval(data_block_restart_interval)
        .use_index_block_restart_interval(index_block_restart_interval)
        .use_data_block_compression(data_block_compression)
        .use_data_block_size(data_block_size)
        .use_index_partition_size(opts.config.index_block_partition_size_policy.get(dst_lvl))
        .use_filter_partition_size(opts.config.filter_block_partition_size_policy.get(dst_lvl))
        .use_row_group_size(opts.config.columnar_row_group_size_policy.get(dst_lvl))
        .use_columnar_page_size(opts.config.columnar_page_size_policy.get(dst_lvl))
        // What this writer writes. A merge-on-read relocation writes nothing
        // and keeps the source's pages, as it keeps their compression.
        .use_column_encoding(opts.config.column_encoding_policy.get(dst_lvl))
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
        .use_writeback_bytes(opts.config.writeback_bytes)
        // `seqno_in_index` is a live runtime config: read off the current
        // snapshot so a compaction started after a toggle rewrites its
        // output SSTs in the new index format (compaction is the migration
        // mechanism for the on-disk index layout).
        .use_seqno_in_index(rc.seqno_in_index)
        .use_zone_map(rc.zone_map)
        .use_columnar(rc.columnar)
        // A blob tree's rows may be written as cells, which its columnar
        // tables split into the columns of their fields.
        .use_cell_rows(rc.columnar && opts.config.kv_separation_opts.is_some())
        // Per-level delete strategy: under copy-on-write the output SSTs persist
        // no delete-bitmap (deleted rows are dropped); merge-on-read / adaptive
        // keep a populated bitmap. Read off the live snapshot so a policy change
        // migrates segments at the next compaction.
        .delete_strategy(rc.delete_strategy.get(dst_lvl))
        .use_disable_cow_on_sst(rc.disable_cow_on_sst_files)
        .use_bloom_policy(output_bloom_policy(version, opts, payload))
        .use_filter_sizing(filter_sizing);

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

    // The dictionary this snapshot's policy names for the destination level.
    // The compaction holds the snapshot until its outputs are installed: a
    // collection spares what a held snapshot names, however the policy has
    // changed since.
    #[cfg(zstd_any)]
    let table_writer = table_writer
        .use_zstd_dictionary(
            opts.config
                .current_zstd_dictionaries()
                .for_compression(data_block_compression)?,
        )
        // Live like the settings above: a compaction started after a toggle
        // writes its outputs under the new strategy, which is how existing
        // SSTs migrate to it.
        .use_zstd_two_pass_seed(rc.zstd_two_pass_seed);

    // Parallel block compression: hand the (per-tree or caller-shared) pool to
    // the writer so its CPU-bound transform work runs on worker threads while
    // writes stay ordered. None / single-thread leaves the serial path.
    // Skipped for sub-compaction writers (block_parallel = false), which
    // already occupy the pool's workers — see the `block_parallel` parameter
    // note on `prepare_table_writer`. Also skipped (like the flush writers)
    // when the per-block transform does no real CPU work: with the identity
    // transform the pipeline's owned buffer + queue hop per block only adds
    // lock traffic and pool churn over the serial reusable-buffer path.
    #[cfg(feature = "std")]
    let transform_does_work = data_block_compression != crate::CompressionType::None
        || opts.config.encryption.is_some()
        || opts.config.page_ecc;
    #[cfg(feature = "std")]
    let table_writer = if block_parallel && transform_does_work {
        table_writer.use_parallel_compression(opts.config.parallel_compression())
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
#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "parallel sub-compaction output; the threaded parallel install + tight-space consumers are std-gated, so unused under no_std"
    )
)]
pub(super) struct ProducedOutput {
    created_tables: Vec<Table>,
    created_blob_files: Vec<BlobFile>,
    /// Blob files this (sub-)compaction rewrote and must drop. Globally-dead
    /// blob files are added once at install time, not per sub-compaction.
    rewritten_blob_files_to_drop: Vec<BlobFile>,
    tables_to_delete: Vec<Table>,
    blob_frag_map: FragmentationMap,
    /// Per-rewritten-file consumed frontier (`blob_file_id -> frame_end`) from a
    /// relocating sub-compaction. Empty for non-relocating outputs. The
    /// tight-space loop punches `[data_start, frontier)` of each stale file and
    /// resumes the next slice's scan here.
    consumed_through: crate::HashMap<BlobFileId, u64>,
    /// Whether the user compaction filter removed or rewrote at least one row
    /// of this output. A filter acts regardless of the GC watermark, so the
    /// install must raise the persisted retention floor to its own seqno
    /// (see [`RetentionEffect::DropsData`](crate::version::RetentionEffect::DropsData))
    /// instead of the watermark-derived floor.
    filter_transformed: bool,
    /// Whether the merge stream dropped a version because it sat below the GC
    /// watermark. A run that dropped none collected no history, so its install
    /// must not raise the retention floor: doing so refuses snapshots whose
    /// data is still on disk.
    collected_below_watermark: bool,
    /// The filter plan the output was sized by, held until the output is
    /// installed or dropped: the budget keeps the room its filters take until
    /// then (see [`crate::filter_budget::FilterSizing::release_replaced`]).
    filter_sizing: Option<crate::filter_budget::FilterPlan>,
    /// The cell-row objects this output charged as garbage, recorded at the
    /// install for the references read before it (see
    /// [`crate::blob_tree::released`]).
    released_objects: Vec<crate::vlog::ValueHandle>,
}

#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "parallel sub-compaction output accessors; the threaded parallel install + tight-space consumers are std-gated, so unused under no_std"
    )
)]
impl ProducedOutput {
    /// The SSTs this (sub-)compaction finalized on disk but has not installed.
    /// Used by the tight-space loop, which installs them via a custom version
    /// edit (restricting the input) rather than the standard [`install_merge`].
    pub(super) fn created_tables(&self) -> &[Table] {
        &self.created_tables
    }

    /// The blob files this (sub-)compaction wrote (KV separation). Empty on the
    /// non-relocating tight-space path; the loop folds them into its slice edit.
    pub(super) fn created_blob_files(&self) -> &[BlobFile] {
        &self.created_blob_files
    }

    /// The blob fragmentation this (sub-)compaction accumulated (entries dropped
    /// from the merge), folded into the version's running GC stats by the
    /// tight-space loop so dead blob files are detected at the final removal.
    pub(super) fn blob_frag_map(&self) -> &FragmentationMap {
        &self.blob_frag_map
    }

    /// The per-rewritten-file consumed frontier from a relocating slice. The
    /// tight-space loop punches and resumes each stale file at these offsets.
    pub(super) fn consumed_through(&self) -> &crate::HashMap<BlobFileId, u64> {
        &self.consumed_through
    }

    /// Records that the user compaction filter transformed at least one row of
    /// this output (called by the producer, which owns the filter counter).
    pub(super) fn mark_filter_transformed(&mut self) {
        self.filter_transformed = true;
    }

    /// Records that the merge stream collected history below the GC watermark
    /// (called by the producer, which owns the GC counter).
    pub(super) fn mark_collected_below_watermark(&mut self) {
        self.collected_below_watermark = true;
    }

    /// Records the cell-row objects the run charged as garbage (called by the
    /// producer, which owns the ownership ledger).
    pub(super) fn set_released_objects(&mut self, objects: Vec<crate::vlog::ValueHandle>) {
        self.released_objects = objects;
    }

    /// The cell-row objects the run charged as garbage.
    pub(super) fn released_objects(&self) -> &[crate::vlog::ValueHandle] {
        &self.released_objects
    }

    /// Builds the output for a merge-on-read relocation: the `created` segment
    /// (the source's blocks reused verbatim plus a delete-bitmap) replaces the
    /// `deleted` source segment, with no blob files and no fragmentation. Lets
    /// the relocation path reuse [`install_merge`]'s atomic version edit instead
    /// of hand-rolling one.
    #[cfg(feature = "std")]
    pub(super) fn for_relocation(created: Table, deleted: Table) -> Self {
        Self {
            created_tables: vec![created],
            created_blob_files: Vec::new(),
            rewritten_blob_files_to_drop: Vec::new(),
            tables_to_delete: vec![deleted],
            blob_frag_map: FragmentationMap::default(),
            consumed_through: crate::HashMap::default(),
            // A relocation copies the source's rows verbatim, so no filter ran.
            filter_transformed: false,
            // It does not leave them all readable, though. This path is reached
            // only for a NON-EMPTY delete bitmap built from range tombstones
            // strictly below the watermark, so the replacement masks rows that a
            // snapshot below the watermark could read before. Nothing is
            // physically removed and no filter acted, so neither of the other
            // two signals catches it: say it here.
            collected_below_watermark: true,
            // The source's filter is reused verbatim, not sized again.
            filter_sizing: None,
            released_objects: Vec::new(),
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
    let mut filter_transformed = false;
    let mut collected_below_watermark = false;
    // Held to the end of the install, success or failure: the filter budget
    // keeps the outputs' room until then.
    let mut filter_sizings = Vec::new();

    let mut released_objects = Vec::new();
    for out in outputs {
        released_objects.extend(out.released_objects);
        filter_sizings.extend(out.filter_sizing);
        created_tables.extend(out.created_tables);
        created_blob_files.extend(out.created_blob_files);
        blob_files_to_drop.extend(out.rewritten_blob_files_to_drop);
        tables_to_delete.extend(out.tables_to_delete);
        out.blob_frag_map.merge_into(&mut blob_frag_map);
        filter_transformed |= out.filter_transformed;
        collected_below_watermark |= out.collected_below_watermark;
    }

    // What this install does to older snapshots, read off what the run
    // actually did rather than off the watermark it was handed. The rule
    // itself lives with the effect it produces, so the flush install cannot
    // drift from it.
    let retention = crate::version::RetentionEffect::of_run(
        filter_transformed,
        collected_below_watermark,
        opts.gc_watermark,
    );

    let tables_out = created_tables.len();

    // Install the tree-wide sinks on every output BEFORE the version edit makes
    // it visible. A flush registers these via `register_tables`; a compaction
    // installs its outputs here. Without the deletion pause an output's
    // in-place heal would skip the checkpoint mutation window and could race a
    // checkpoint hard-link; without the heal-hint sink a confirmed-persistent
    // ECC correction on a read could never queue the SST for a healing
    // rewrite, leaving the bitrot on disk.
    let sinks = crate::table::TableSinks {
        deletion_pause: &opts.deletion_pause,
        heal_hints: &opts.heal_hints,
        read_budget: opts.config.columnar_read_budget,
        #[cfg(feature = "std")]
        background_deleter: &opts.background_deleter,
        track_filter_probes: opts.config.filter_advisor.is_some(),
    };
    for table in &created_tables {
        table.bind_to_tree(&sinks);
    }
    // Blob files this compaction produced need the same binding: they become
    // reachable through the very same version edit, and an unbound one can be
    // unlinked out from under a capturing checkpoint.
    for blob_file in &created_blob_files {
        blob_file.bind_to_tree(&sinks);
    }

    // Globally-dead blob files are dropped once, from the install-time version.
    let current_version = super_version.latest_version();
    for blob_file in current_version.version.blob_files.iter() {
        if blob_file.is_dead(current_version.version.gc_stats()) {
            blob_files_to_drop.push(blob_file.clone());
        }
    }

    // A file a cell row still references stays, whatever its counts say: a
    // borrowed reference charges nothing when it goes, so a file's counts can
    // reach its size while a row that does not own an object still points at
    // it, from a table this edit keeps or writes or from a memtable. The
    // install holds the version lock that orders writes of such rows, so no
    // memtable gains a reference to these files behind this check.
    let kept = referenced_blob_files(
        &blob_files_to_drop,
        &current_version,
        &payload.table_ids,
        &created_tables,
    )?;
    if !kept.is_empty() {
        blob_files_to_drop.retain(|file| !kept.contains(&file.id()));
        // A relocated file that stays is fully charged: every object in it
        // was copied or is garbage, so it goes as soon as its last reference
        // does, and nothing charges it further (its rows only borrow).
        for &id in &kept {
            let Some(file) = current_version.version.blob_files.get(id) else {
                continue;
            };
            // What the stats and this pass charged so far, as objects,
            // uncompressed bytes and bytes on disk.
            let charged = |map: &FragmentationMap| {
                map.get(&id)
                    .map_or((0, 0, 0), |e| (e.len, e.bytes, e.on_disk_bytes))
            };
            let (stats, pass) = (
                charged(current_version.version.gc_stats()),
                charged(&blob_frag_map),
            );
            let meta = &file.0.meta;
            // The rest of each counter, so the file reads as fully charged
            // for what it frees on disk too. A counter already at its total
            // adds nothing.
            let rest = |total: u64, a: u64, b: u64| total.saturating_sub(a + b);
            // A file of more objects than a `usize` counts is not one this
            // build wrote.
            let len = usize::try_from(meta.item_count)
                .unwrap_or(usize::MAX)
                .saturating_sub(stats.0 + pass.0);
            let bytes = rest(meta.total_uncompressed_bytes, stats.1, pass.1);
            let on_disk = rest(meta.total_compressed_bytes, stats.2, pass.2);
            // Any counter short of its total: objects of zero bytes still
            // count, and a file is dead only when its entry reaches every
            // total, so an entry is made for them too.
            if len > 0 || bytes > 0 || on_disk > 0 {
                let mut fill = FragmentationMap::default();
                fill.insert(
                    id,
                    crate::blob_tree::FragmentationEntry::new(len, bytes, on_disk),
                );
                fill.merge_into(&mut blob_frag_map);
            }
        }
    }

    // The outputs take the probe counts of the inputs they replace, by the
    // range each covers. Read off the payload rather than the outputs' delete
    // lists, which parallel sub-compactions each fill with the same inputs.
    if opts.config.filter_advisor.is_some() {
        let inputs: Vec<Table> = payload
            .table_ids
            .iter()
            .filter_map(|id| current_version.version.get_table(*id).cloned())
            .collect();
        crate::table::probe_stats::inherit_into(&created_tables, &inputs)?;
    }

    // The version edit drops the inputs' filters from the published figure:
    // the plans give back their credit for them first, so the budget holds
    // no fewer bytes than there are at any point of the install.
    for sizing in &filter_sizings {
        sizing.release_replaced();
    }
    super_version.upgrade_version(
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
        retention,
    )?;
    // The version names the outputs now, so the run must not remove them.
    opts.outputs.installed();
    // Still under the write lock that published the version: a reference read
    // from an older version names an object this run let go of.
    if !released_objects.is_empty() {
        let published = super_version.latest_version().version.id();
        super_version
            .released()
            .record(published, released_objects, []);
    }

    // NOTE: If the application were to crash >here< it's fine — the tables /
    // blob files are not referenced anymore and are cleaned up upon recovery.
    for table in tables_to_delete {
        table.mark_as_deleted();
    }
    for blob_file in blob_files_to_drop {
        blob_file.mark_as_deleted();
    }
    // The published version now counts the outputs' filters.
    drop(filter_sizings);

    Ok(tables_out)
}

/// The ids among `candidates` that something will still reference once an
/// edit replacing the tables `replaced` with `created` installs over
/// `current`: a table the edit keeps or writes links them, or a memtable row
/// references them.
///
/// # Errors
///
/// Returns an error if a table's blob links cannot be read.
pub(super) fn referenced_blob_files(
    candidates: &[BlobFile],
    current: &crate::version::SuperVersion,
    replaced: &HashSet<crate::TableId>,
    created: &[Table],
) -> crate::Result<HashSet<BlobFileId>> {
    let mut referenced = HashSet::default();
    if candidates.is_empty() {
        return Ok(referenced);
    }
    for file in candidates {
        if current.memtables_reference_blob_file(file.id()) {
            referenced.insert(file.id());
        }
    }
    let kept_tables = current
        .version
        .iter_tables()
        .filter(|table| !replaced.contains(&table.id()));
    for table in kept_tables.chain(created.iter()) {
        if referenced.len() == candidates.len() {
            break;
        }
        for link in table.blob_links()? {
            if candidates.iter().any(|file| file.id() == link.blob_file_id) {
                referenced.insert(link.blob_file_id);
            }
        }
    }
    Ok(referenced)
}

/// Compaction worker that will relocate blobs that sit in blob files that are being rewritten
pub struct RelocatingCompaction {
    inner: StandardCompaction,
    blob_scanner: Peekable<BlobFileMergeScanner>,
    blob_writer: BlobFileWriter,
    /// Codec of each file being rewritten, by id. Relocation copies frames
    /// VERBATIM, so the output file must record the codec of the SOURCE those
    /// frames came from; the current blob policy describes what would be
    /// written fresh, which is a different question and need not agree.
    rewriting_blob_file_codecs: crate::HashMap<BlobFileId, crate::CompressionType>,
    rewriting_blob_files: Vec<BlobFile>,
    /// Paces relocated-blob I/O. The merge loop's limiter only sees the
    /// encoded handle in `item.value`; the real payload moved here is
    /// debited at the relocation write site so KV-separated compactions
    /// are throttled by their actual bandwidth.
    rate_limiter: alloc::sync::Arc<crate::rate_limiter::RateLimiter>,
    /// Polled by the blob throttle so a long wait under a low limit stays
    /// interruptible by tree drop / shutdown.
    stop_signal: crate::stop_signal::StopSignal,
    /// Per-rewritten-file frontier: the highest `frame_end` consumed (relocated
    /// or drained) so far. The tight-space slice loop reads this after a slice to
    /// punch `[data_start, frontier)` of each stale file and to resume the next
    /// slice's scan there. Maintained unconditionally; the non-tight path ignores
    /// it.
    consumed_through: crate::HashMap<BlobFileId, u64>,
    /// The tree's key order: the table stream and the blob scan both advance
    /// in it, and draining compares keys by it.
    comparator: crate::comparator::SharedComparator,
    /// A tight-space slice's exclusive upper key. Every version of a key below
    /// it went through this slice, so once the slice is written its frames that
    /// no emitted pointer claimed belong to dropped pointers and are drained
    /// into the frontier. `None` for a whole-file relocation, which drops the
    /// stale files outright and has no frontier to move.
    drain_below: Option<crate::UserKey>,
    /// The tree the rewritten files belong to, which names them in the
    /// descriptor table a cell row's frames are read through.
    tree_id: crate::TreeId,
    /// The copies made for the cell rows of the current key, by the handle of
    /// the frame each copies. Every version that holds an object is a version
    /// of one key, so a later version of the key that holds the same object
    /// points at the same copy, and no other key can.
    cell_copies: crate::HashMap<crate::vlog::ValueHandle, BlobIndirection>,
    /// The key `cell_copies` belongs to.
    cell_copies_key: Option<crate::UserKey>,
}

impl RelocatingCompaction {
    #[expect(
        clippy::too_many_arguments,
        reason = "each argument is an independent input of the pass; a struct would only rename them"
    )]
    pub fn new(
        inner: StandardCompaction,
        blob_scanner: Peekable<BlobFileMergeScanner>,
        blob_writer: BlobFileWriter,
        rewriting_blob_files: Vec<BlobFile>,
        rate_limiter: alloc::sync::Arc<crate::rate_limiter::RateLimiter>,
        stop_signal: crate::stop_signal::StopSignal,
        comparator: crate::comparator::SharedComparator,
        tree_id: crate::TreeId,
    ) -> Self {
        Self {
            inner,
            blob_scanner,
            blob_writer,
            rewriting_blob_file_codecs: rewriting_blob_files
                .iter()
                .map(|bf| (bf.id(), bf.compression()))
                .collect(),
            rewriting_blob_files,
            rate_limiter,
            stop_signal,
            consumed_through: crate::HashMap::default(),
            comparator,
            drain_below: None,
            tree_id,
            cell_copies: crate::HashMap::default(),
            cell_copies_key: None,
        }
    }

    /// Writes the cell row `item`, every reference into a file being rewritten
    /// pointed at a copy of its object.
    ///
    /// A row's frames are read by handle, not matched against the sequential
    /// scan: the frames of one key sit in the scan newest version first, and a
    /// row's borrowed objects belong to older versions, so claiming them in
    /// cell order would drain frames a version still to come holds. The scan
    /// drains them later like any frame no pointer claimed, after they are
    /// copied.
    ///
    /// Each reference keeps its owner bit: the rows that reach a relocation
    /// already carry exactly one owner among the kept holders of an object.
    /// Every holder is in the pass, since a file is relocated only when every
    /// table that links it is an input, and the ownership ledger settled the
    /// key before writing it: ownership moved off an owner the pass dropped,
    /// and an object whose owner went with a whole-table drop passed to its
    /// oldest holder. So the copy has exactly one owner.
    fn write_cell_row(&mut self, item: InternalValue) -> crate::Result<()> {
        use crate::blob_tree::field_row::{RowCell, decode_row, encode_row};

        if self
            .cell_copies_key
            .as_ref()
            .is_none_or(|key| !crate::comparator::same_user_key(key, &item.key.user_key))
        {
            self.cell_copies.clear();
            self.cell_copies_key = Some(item.key.user_key.clone());
        }

        let mut cells = decode_row(&item.value)?;
        let mut rewritten = false;
        for field in &mut cells {
            let RowCell::Ref { indirection, .. } = &mut field.cell else {
                continue;
            };
            if !self
                .rewriting_blob_file_codecs
                .contains_key(&indirection.vhandle.blob_file_id)
            {
                continue;
            }
            rewritten = true;
            if let Some(copy) = self.cell_copies.get(&indirection.vhandle) {
                *indirection = *copy;
                continue;
            }
            let copy = self.copy_frame(&item.key.user_key, item.key.seqno, indirection)?;
            self.cell_copies.insert(indirection.vhandle, copy);
            *indirection = copy;
        }

        let row = if rewritten {
            crate::UserValue::from(encode_row(&cells)?)
        } else {
            item.value
        };
        self.inner
            .table_writer
            .write(InternalValue::from_components(
                item.key.user_key,
                row.clone(),
                item.key.seqno,
                crate::ValueType::CellRow,
            ))?;
        self.inner.table_writer.register_cell_row(&row)
    }

    /// Copies the frame `indirection` names, verbatim, into this pass's
    /// output and returns the copy's indirection.
    fn copy_frame(
        &mut self,
        key: &[u8],
        seqno: crate::SeqNo,
        indirection: &BlobIndirection,
    ) -> crate::Result<BlobIndirection> {
        let blob_file_id = indirection.vhandle.blob_file_id;
        let Some(blob_file) = self
            .rewriting_blob_files
            .iter()
            .find(|file| file.id() == blob_file_id)
        else {
            return Err(crate::Error::InvalidHeader(
                "cell row references a rewritten blob file the pass does not hold",
            ));
        };
        let file_id = crate::GlobalTableId::from((self.tree_id, blob_file_id));
        let (file, _) = blob_file
            .file_accessor()
            .get_or_open_blob_file(&file_id, &blob_file.0.path)?;
        let (stored, uncompressed_len) =
            crate::vlog::blob_file::reader::Reader::new(blob_file, file.as_ref())
                .read_raw(key, &indirection.vhandle)?;

        // Paced like a matched frame: see the whole-value path in `write`.
        let _ = self
            .rate_limiter
            .request_interruptible(stored.len() as u64, || self.stop_signal.is_stopped());

        self.blob_writer
            .record_source_compression(blob_file.compression())?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "a frame's decoded length is bounded by the 256 MiB value cap"
        )]
        let vhandle = self
            .blob_writer
            .write_raw(key, seqno, &stored, uncompressed_len as u32)?;
        Ok(BlobIndirection {
            vhandle,
            size: indirection.size,
        })
    }

    /// Marks this as a tight-space slice ending (exclusively) at `bound`; see
    /// the `drain_below` field.
    #[must_use]
    pub fn with_drain_below(mut self, bound: Option<crate::UserKey>) -> Self {
        self.drain_below = bound;
        self
    }

    /// Drains the frames of keys below `bound` that no emitted pointer claimed,
    /// advancing the frontier past them as `drain_blobs` does: a pointer this
    /// slice dropped (a version the GC fold collected, an entry a range
    /// tombstone deletes, a base a merge fold consumed) never reaches `write`,
    /// so its frame is accounted here. No table outside the compaction
    /// references these frames: a file is relocated only when every table
    /// referencing it is an input, and the inputs' versions of these keys all
    /// went through this slice. A resynchronized frame is consumed without
    /// moving the frontier, so a resumed scan re-reads from the last proven
    /// boundary.
    fn drain_unclaimed_below(&mut self, bound: &[u8]) -> crate::Result<()> {
        let Self {
            blob_scanner,
            consumed_through,
            comparator,
            stop_signal,
            ..
        } = self;
        drain_unclaimed_below(
            blob_scanner,
            bound,
            comparator.as_ref(),
            &mut |id, frame_end| {
                let slot = consumed_through.entry(id).or_insert(0);
                *slot = (*slot).max(frame_end);
            },
            &|| stop_signal.is_stopped(),
        )
    }

    /// Advances the per-file frontier for `blob_file_id` to the max of its
    /// current value and `frame_end` (frames are consumed in increasing offset
    /// order, so this is monotonic; `max` only guards reordering bugs).
    fn record_consumed(&mut self, blob_file_id: BlobFileId, frame_end: u64) {
        let slot = self.consumed_through.entry(blob_file_id).or_insert(0);
        *slot = (*slot).max(frame_end);
    }

    fn drain_blobs(&mut self, key: &[u8], indirection: &BlobIndirection) -> crate::Result<()> {
        // Disjoint borrows: drain advances `blob_scanner` while recording each
        // reclaimed frame into `consumed_through`.
        let Self {
            blob_scanner,
            consumed_through,
            comparator,
            ..
        } = self;
        drain_blobs(
            blob_scanner,
            key,
            indirection,
            comparator.as_ref(),
            &mut |id, frame_end| {
                let slot = consumed_through.entry(id).or_insert(0);
                *slot = (*slot).max(frame_end);
            },
        )
    }
}

impl CompactionFlavour for RelocatingCompaction {
    fn write_range_tombstones(&mut self, tombstones: &[RangeTombstone]) {
        self.inner.write_range_tombstones(tombstones);
    }

    fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        if item.key.value_type.is_cell_row() {
            return self.write_cell_row(item);
        }
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
                .rewriting_blob_file_codecs
                .contains_key(&indirection.vhandle.blob_file_id)
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

                // A RESYNCHRONIZED frame was reached by a byte-wise scan after
                // damage: its boundary is unproven (the magic may sit inside a
                // damaged record's user-controlled bytes, and every frame chained
                // past it inherits that unanchored start). Relocating it would
                // rewrite fabricated bytes as a genuine record. `salvage_blob_file`
                // drops such frames; relocation must fail closed the same way.
                if blob_entry.resynced {
                    return Err(crate::Error::InvalidHeader(
                        "resynchronized blob frame reached after damage; refusing to \
                         relocate a record whose boundary cannot be proven original",
                    ));
                }

                // Advance the consumed frontier past this relocated frame so the
                // tight-space loop can punch / resume here once the slice installs.
                self.record_consumed(blob_file_id, blob_entry.frame_end);

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
                // new vptr valid — only the *wait* is shortened on stop, and
                // the debit stays spent for the write that follows.
                let _ = self
                    .rate_limiter
                    .request_interruptible(blob_entry.value.len() as u64, || {
                        self.stop_signal.is_stopped()
                    });

                // These bytes are the SOURCE's, still encoded as it left them.
                // They go to the output file that records that codec, one per
                // codec for the whole pass: a blob file describes one codec, and
                // a relocation can draw from sources that do not share one (the
                // blob policy may have changed since they were written).
                // Stamping the current policy instead would label these frames
                // with a codec they are not in, and the next read of them would
                // fail to decode.
                #[expect(
                    clippy::expect_used,
                    reason = "the id came from `rewriting_blob_file_codecs`'s own key set"
                )]
                let source_codec = *self
                    .rewriting_blob_file_codecs
                    .get(&blob_file_id)
                    .expect("relocated frame comes from a file being rewritten");
                self.blob_writer.record_source_compression(source_codec)?;

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

        if let Some(bound) = self.drain_below.take() {
            self.drain_unclaimed_below(&bound)?;
        }

        let tables_to_delete = core::mem::take(&mut self.inner.tables_to_rewrite);

        let filter_sizing = self.inner.table_writer.filter_sizing();
        let created_tables = self.inner.consume_writer(opts, dst_lvl)?;
        let mut created_blob_files = self.blob_writer.finish()?;
        created_blob_files.extend(extra_blob_files);

        Ok(ProducedOutput {
            created_tables,
            created_blob_files,
            rewritten_blob_files_to_drop: self.rewriting_blob_files,
            tables_to_delete,
            blob_frag_map,
            consumed_through: self.consumed_through,
            // The producer owns the filter counter and marks this after.
            filter_transformed: false,
            collected_below_watermark: false,
            filter_sizing,
            // The producer owns the ownership ledger and sets this after.
            released_objects: Vec::new(),
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
                let mut params = crate::table::RecoverParams::new(
                    table_base_folder.join(table_id.to_string()),
                    checksum,
                    table_id,
                    level_fs.clone(),
                    opts.config.comparator.clone(),
                    opts.config.cache.clone(),
                );
                params.tree_id = opts.tree_id;
                params
                    .descriptor_table
                    .clone_from(&opts.config.descriptor_table);
                params.pin_filter = pin_filter;
                params.pin_index = pin_index;
                params.encryption.clone_from(&opts.config.encryption);
                #[cfg(zstd_any)]
                {
                    params.zstd_dictionaries = opts.config.current_zstd_dictionaries();
                }
                #[cfg(feature = "metrics")]
                {
                    params.metrics = opts.metrics.clone();
                }
                Table::recover(params)
            })
            .collect::<crate::Result<Vec<_>>>()
    }
}

impl CompactionFlavour for StandardCompaction {
    fn write_range_tombstones(&mut self, tombstones: &[RangeTombstone]) {
        self.table_writer.set_range_tombstones(tombstones.to_vec());
    }

    fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        if item.key.value_type.is_cell_row() {
            // The row is kept as it is, ownership included; the slice clone
            // is a reference-count bump, since the writer takes the item.
            let row = item.value.clone();
            self.table_writer.write(item)?;
            return self.table_writer.register_cell_row(&row);
        }

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
        let filter_sizing = self.table_writer.filter_sizing();
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
            // A standard sub-compaction relocates nothing.
            consumed_through: crate::HashMap::default(),
            // The producer owns the filter counter and marks this after.
            filter_transformed: false,
            collected_below_watermark: false,
            filter_sizing,
            // The producer owns the ownership ledger and sets this after.
            released_objects: Vec::new(),
        })
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests;
