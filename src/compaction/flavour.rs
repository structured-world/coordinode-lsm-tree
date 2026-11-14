// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(feature = "std")]
use crate::UserKey;
use crate::blob_tree::FragmentationMap;
use crate::blob_tree::handle::BlobIndirection;
use crate::coding::Decode;
use crate::compaction::Input as CompactionPayload;
use crate::compaction::worker::Options;
use crate::range_tombstone::RangeTombstone;
use crate::table::multi_writer::{MultiWriter, TombstonesOutside};
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
/// path passes a no-op. `seen` is told of every drained entry, the
/// resynchronized ones included.
fn drain_blobs<I: Iterator<Item = crate::Result<(ScanEntry, BlobFileId)>>>(
    scanner: &mut Peekable<I>,
    key: &[u8],
    vptr: &BlobIndirection,
    comparator: &dyn crate::comparator::UserComparator,
    record_consumed: &mut dyn FnMut(BlobFileId, u64),
    seen: &mut dyn FnMut(BlobFileId, &ScanEntry),
) -> crate::Result<()> {
    loop {
        let Some(blob) = scanner.next_if(|x| match x {
            Ok((entry, blob_file_id)) => {
                entry.key != key
                    || (*blob_file_id != vptr.vhandle.blob_file_id)
                    || ((entry.frame, entry.index) < (vptr.vhandle.frame, vptr.vhandle.index))
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
        seen(blob_file_id, &entry);
    }

    Ok(())
}

/// Drains the frames of keys below `bound`, recording each proven frame's end
/// through `record_consumed` as `drain_blobs` does; a resynchronized frame is
/// consumed without moving the frontier. `is_stopped` is polled before every
/// frame: the drain can cover a whole dead prefix after the merge loop's last
/// cancellation check, so a stop must not wait for it. `seen` is told of every
/// drained entry.
fn drain_unclaimed_below<I: Iterator<Item = crate::Result<(ScanEntry, BlobFileId)>>>(
    scanner: &mut Peekable<I>,
    bound: &[u8],
    comparator: &dyn crate::comparator::UserComparator,
    record_consumed: &mut dyn FnMut(BlobFileId, u64),
    seen: &mut dyn FnMut(BlobFileId, &ScanEntry),
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
        seen(blob_file_id, &entry);
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

/// The boundaries between the tables of the level `payload`'s outputs are
/// merged into next, in key order, across all its runs, within `span`, the
/// keys the compaction writes: none when output alignment is off or the
/// strategy names no such level (see
/// [`CompactionStrategy::next_merge_level`](super::CompactionStrategy::next_merge_level)).
/// Only the tables meeting `span` are walked: a boundary outside it is one no
/// output of the compaction can end on. A boundary lies after a table that
/// the next one, in key order, starts above; tables that overlap, in a level
/// holding several runs, have none between them, since an output reaching
/// either reaches both.
/// The keys a merge of `payload` ends its outputs on, past the target size:
/// the boundaries of the level below it meets, in key order.
pub(super) fn output_cut_keys(
    version: &Version,
    opts: &Options,
    payload: &CompactionPayload,
) -> Vec<crate::UserKey> {
    next_level_boundaries(version, opts, payload, inputs_span(version, opts, payload))
        .iter()
        .map(|boundary| boundary.key.clone())
        .collect()
}

fn next_level_boundaries(
    version: &Version,
    opts: &Options,
    payload: &CompactionPayload,
    span: Option<(&crate::UserKey, &crate::UserKey)>,
) -> alloc::sync::Arc<[crate::table::multi_writer::Boundary]> {
    use crate::version::run::Ranged;
    use core::ops::Bound;

    if opts.config.compaction_output_cuts == crate::config::OutputCuts::TargetSize {
        return alloc::sync::Arc::from([]);
    }
    let (Some(level), Some((lo, hi))) = (
        opts.strategy
            .next_merge_level(version, payload.dest_level)
            .and_then(|index| version.level(index)),
        span,
    ) else {
        return alloc::sync::Arc::from([]);
    };
    let comparator = opts.config.comparator.as_ref();
    let bounds = (Bound::Included(lo), Bound::Included(hi));
    // One run is already in key order with disjoint tables and is walked in
    // place; only several runs are merged into one order first.
    let boundaries = match level.first_run() {
        Some(run) if level.run_count() == 1 => {
            let tables = tables_meeting(run, &bounds, comparator);
            boundaries_between(tables.iter(), tables.len(), comparator)
        }
        _ => {
            let mut tables: Vec<&Table> = level
                .iter()
                .flat_map(|run| tables_meeting(run, &bounds, comparator))
                .collect();
            tables.sort_by(|a, b| comparator.compare(a.key_range().min(), b.key_range().min()));
            let capacity = tables.len();
            boundaries_between(tables.into_iter(), capacity, comparator)
        }
    };
    boundaries.into()
}

/// The tables of `run` meeting `bounds`, a contiguous slice of it.
fn tables_meeting<'a>(
    run: &'a crate::version::run::Run<Table>,
    bounds: &(
        core::ops::Bound<&crate::UserKey>,
        core::ops::Bound<&crate::UserKey>,
    ),
    comparator: &dyn crate::comparator::UserComparator,
) -> &'a [Table] {
    run.range_overlap_indexes_cmp::<crate::UserKey, _, _>(bounds, comparator)
        .and_then(|(first, last)| run.get(first..=last))
        .unwrap_or_default()
}

/// The boundaries between `tables`, given in order of their min key.
pub(super) fn boundaries_between<'a>(
    tables: impl Iterator<Item = &'a Table>,
    capacity: usize,
    comparator: &dyn crate::comparator::UserComparator,
) -> Vec<crate::table::multi_writer::Boundary> {
    use crate::version::run::Ranged;
    use core::cmp::Ordering;

    let mut boundaries = Vec::with_capacity(capacity);
    // The largest key reached so far: a table starting above it starts past
    // every table before it, whichever run they are in.
    let mut reached: Option<&crate::UserKey> = None;
    let mut tables = tables.peekable();
    while let (Some(before), Some(&after)) = (tables.next(), tables.peek()) {
        let max = before.key_range().max();
        let reach = match reached {
            Some(far) if comparator.compare(far, max) == Ordering::Greater => far,
            _ => max,
        };
        reached = Some(reach);
        if comparator.compare(reach, after.key_range().min()) == Ordering::Less {
            boundaries.push(crate::table::multi_writer::Boundary {
                key: reach.clone(),
                after: Vec::new(),
            });
        }
        // Every table belongs to the cluster after the last boundary before
        // it; tables before the first boundary to none.
        if let Some(boundary) = boundaries.last_mut() {
            boundary
                .after
                .push((after.key_range().min().clone(), after.file_size()));
        }
    }
    boundaries
}

/// The smallest and the largest key among `payload`'s inputs: every key the
/// compaction writes lies between them.
fn inputs_span<'a>(
    version: &'a Version,
    opts: &Options,
    payload: &CompactionPayload,
) -> Option<(&'a crate::UserKey, &'a crate::UserKey)> {
    use core::cmp::Ordering;

    let comparator = opts.config.comparator.as_ref();
    version
        .iter_tables()
        .filter(|table| payload.table_ids.contains(&table.id()))
        .map(|table| {
            (
                table.metadata.key_range.min(),
                table.metadata.key_range.max(),
            )
        })
        .reduce(|(lo, hi), (min, max)| {
            (
                if comparator.compare(min, lo) == Ordering::Less {
                    min
                } else {
                    lo
                },
                if comparator.compare(max, hi) == Ordering::Greater {
                    max
                } else {
                    hi
                },
            )
        })
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
    // Every output's id is above its inputs': an output takes the age of its
    // newest input, and the id is what tells the two apart, the higher one
    // the output that supersedes it.
    let above = payload.table_ids.iter().copied().max();
    let table_writer = MultiWriter::from_pool(
        table_base_folder,
        alloc::sync::Arc::clone(&opts.table_files),
        above,
        payload.target_size,
        payload.dest_level,
        level_fs,
        crate::fs::WriteOrigin::CompactionTable,
    )?
    .use_output_ledger(opts.outputs.clone());
    let mut table_writer = configure_table_writer(
        table_writer,
        version,
        opts,
        payload,
        filter_sizing,
        whole_run,
        rc,
    )?;

    if let Some(marker) = transform_marker {
        table_writer = table_writer.use_transform_marker(marker);
    }

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
    let transform_does_work = rc
        .data_block_compression_policy
        .get(payload.canonical_level.into())
        != crate::CompressionType::None
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

/// A writer with every setting a compaction of `payload` writes its outputs
/// with, its file in memory: what bounds the bytes those outputs take before
/// any is written (see [`MultiWriter::rewrite_bound`]). The plan the
/// compaction draws its filter widths from may build wider filters than the
/// level's policy, so its widest choice is taken.
///
/// # Errors
///
/// As [`prepare_table_writer`].
pub(super) fn output_bound_probe(
    version: &Version,
    opts: &Options,
    payload: &CompactionPayload,
    rc: &crate::runtime_config::RuntimeConfig,
) -> crate::Result<crate::table::multi_writer::BoundProbe> {
    let probe = MultiWriter::probe(payload.target_size, payload.dest_level)?;
    let probe = configure_table_writer(probe, version, opts, payload, None, false, rc)?;
    crate::table::multi_writer::BoundProbe::new(match opts.config.filter_advisor.as_ref() {
        Some(advisor) => probe.use_bloom_policy(crate::filter_budget::widest_policy(
            advisor.bits_per_key(),
            output_bloom_policy(version, opts, payload),
        )),
        None => probe,
    })
}

/// Gives `table_writer` every setting a compaction of `payload` writes its
/// outputs with: what they hold, how they are encoded and laid out, and where
/// they are cut. One place for both the writer that writes them and the one
/// that bounds what they take.
#[cfg_attr(
    not(zstd_any),
    expect(
        clippy::unnecessary_wraps,
        reason = "only the zstd dictionary a level names can fail to resolve"
    )
)]
fn configure_table_writer(
    table_writer: MultiWriter,
    version: &Version,
    opts: &Options,
    payload: &CompactionPayload,
    filter_sizing: Option<crate::filter_budget::FilterPlan>,
    whole_run: bool,
    rc: &crate::runtime_config::RuntimeConfig,
) -> crate::Result<MultiWriter> {
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

    // The outputs' age: each takes the newest age among the inputs its key
    // range meets, as the data it carries is no newer than that. Stamping the
    // write time instead would make a rewrite look like fresh data: a TTL
    // would restart at every compaction, and age-ordered drops would follow
    // the order the outputs were written in, which is key order.
    let input_ages = crate::table::writer::AgeSweep::new(
        version
            .iter_tables()
            .filter(|t| payload.table_ids.contains(&t.id()))
            .map(|t| crate::table::writer::InputAge {
                min: t.metadata.key_range.min().clone(),
                max: t.metadata.key_range.max().clone(),
                created_at: *t.metadata.created_at,
            })
            .collect(),
        opts.config.comparator.clone(),
    );

    let span = inputs_span(version, opts, payload);
    let boundaries = next_level_boundaries(version, opts, payload, span);
    log::debug!(
        "Outputs into L{} cut on {} boundaries of the level below",
        payload.dest_level,
        boundaries.len(),
    );
    let mut table_writer = table_writer
        .set_comparator(opts.config.comparator.clone())
        .use_inherited_age(input_ages)
        .use_recency(recency)
        // The outputs' compaction lineage: the input ids this run merges. Lets a
        // manifest-loss rebuild recognize the output as DERIVED and exclude it
        // when every input survived a crash-before-commit, instead of publishing
        // both histories and double-applying the merge operands on read. An
        // output a filter TRANSFORMS is marked so (see `transform_marker`).
        .use_lineage(Some(payload.table_ids.iter().copied().collect()))
        .use_lineage_whole_run(whole_run)
        // Compaction consumes input tables, so clip RTs to each output table's key range.
        .use_clip_range_tombstones()
        .use_cut_alignment(boundaries, span.map(|(_, upper)| upper.clone()));

    if index_partitioning {
        // Size-adaptive index: single-level for small SSTs, spill to a
        // partitioned index only past the threshold (see flush path).
        table_writer = table_writer.use_adaptive_index(
            opts.config
                .index_spill_threshold(dst_lvl, rc.index_partition_spill_threshold),
        );
    }
    if filter_partitioning {
        table_writer = table_writer.use_partitioned_filter();
    } else if let Some(limit) = opts.config.whole_filter_limit(dst_lvl) {
        table_writer = table_writer.use_whole_filter_up_to(limit);
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
        // A merge-on-read relocation, which bypasses this writer, refuses a
        // source holding rows of two domains or a range tombstone leaving its
        // rows' domain, so that source is rewritten here.
        .use_key_domains(opts.config.key_domains.clone())
        .use_encryption(opts.config.encryption.clone())
        .use_clock(opts.config.clock.clone())
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
    /// Blob bytes this output's relocation moved into new blob files, charged
    /// to the tree's metrics once the output is installed: what each value
    /// takes (see [`BlobIndirection::stored`]).
    relocated_bytes: u64,
    /// What this output copied instead of encoding, charged to the tree's
    /// metrics once the output is installed.
    carried: super::carry::Carried,
    /// The bytes a relocated segment's file copied from its source as they
    /// lay.
    segment_copied: u64,
    /// The bytes of the blob frames its relocation copied as they lay: the
    /// frames of values with frames of their own. A value out of a block is
    /// encoded again, into a block of the output.
    blob_carried: u64,
    /// The bytes of blob frames this output read by their handles, outside
    /// the paced reads of the run's inputs.
    frames_read: u64,
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

    /// The on-disk blob bytes this output's relocation copied, for the
    /// tight-space loop, which installs its slices itself.
    pub(super) fn relocated_bytes(&self) -> u64 {
        self.relocated_bytes
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

    /// Records what the run copied instead of encoding (called by the
    /// producer, which owns the carry matcher).
    pub(super) fn set_carried(&mut self, carried: super::carry::Carried) {
        self.carried = carried;
    }

    /// What the run copied instead of encoding, for the tight-space loop,
    /// which installs its slices itself.
    pub(super) fn carried(&self) -> super::carry::Carried {
        self.carried
    }

    /// What installing this output puts in the version, as the run's work
    /// counts it: the bytes of its tables and of its blob files, framing and
    /// metadata included as they lie on disk, the part of them copied rather
    /// than encoded, and the frames read by handle.
    ///
    /// # Errors
    ///
    /// Any error reading the length of a blob file it wrote.
    pub(super) fn installed_work(&self) -> crate::Result<super::work::Installed> {
        let tables: u64 = self.created_tables.iter().map(Table::file_len).sum();
        let mut blob_files = 0u64;
        for file in &self.created_blob_files {
            // Bounded by the bytes the run wrote.
            blob_files += file.physical_size()?;
        }
        Ok(super::work::Installed {
            written: tables + blob_files,
            carried: self.carried.bytes + self.segment_copied + self.blob_carried,
            read: self.frames_read,
        })
    }

    /// Builds the output for a merge-on-read relocation: the `created` segment
    /// (the source's blocks reused verbatim plus a delete-bitmap, `copied`
    /// bytes of it taken from the source as they lay) replaces the `deleted`
    /// source segment, with no blob files and no fragmentation. Lets the
    /// relocation path reuse [`install_merge`]'s atomic version edit instead
    /// of hand-rolling one.
    #[cfg(feature = "std")]
    pub(super) fn for_relocation(created: Table, deleted: Table, copied: u64) -> Self {
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
            relocated_bytes: 0,
            carried: super::carry::Carried::default(),
            segment_copied: copied,
            blob_carried: 0,
            frames_read: 0,
        }
    }
}

// TODO: find a better name
pub(super) trait CompactionFlavour {
    fn write(&mut self, item: InternalValue) -> crate::Result<()>;

    /// [`Self::write`] of a row known to carry a user key no earlier row
    /// carries, for a flavour that can spare its output the comparison.
    fn write_key_start(&mut self, item: InternalValue) -> crate::Result<()> {
        self.write(item)
    }

    /// This compaction as the one that copies row groups into its output,
    /// `None` for a flavour that writes its rows another way: a carry plan is
    /// made only for a compaction that writes every row itself (see `carry`).
    fn carrying(&mut self) -> Option<&mut StandardCompaction>;

    /// Writes range tombstones to the output tables, keeping the pieces of
    /// `outside`'s that fall past them (see [`TombstonesOutside`]).
    fn write_range_tombstones(&mut self, tombstones: &[RangeTombstone], outside: TombstonesOutside);

    /// Ends the keys this merge answers for before `end`, a key it found the
    /// end of only as it ran (a tight-space slice): no piece of a range
    /// tombstone written beside its outputs reaches `end`, and a relocation's
    /// frames below it that no pointer claimed are drained into its frontier.
    #[cfg(feature = "std")]
    fn end_before(&mut self, end: UserKey);

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

/// Charges the counters an installed output moves: the blob bytes its
/// relocation copied, and the groups, partly copied groups, blocks and bytes
/// it copied instead of encoding. Every path that installs an output calls it
/// once the version edit is published.
#[cfg(feature = "metrics")]
pub(super) fn charge_installed(
    metrics: &crate::metrics::Metrics,
    relocated_bytes: u64,
    carried: super::carry::Carried,
) {
    use core::sync::atomic::Ordering::Relaxed;
    metrics
        .blob_bytes_relocated
        .fetch_add(relocated_bytes, Relaxed);
    metrics
        .compaction_groups_carried
        .fetch_add(carried.groups, Relaxed);
    metrics
        .compaction_groups_partly_carried
        .fetch_add(carried.partial_groups, Relaxed);
    metrics
        .compaction_blocks_carried
        .fetch_add(carried.blocks, Relaxed);
    metrics
        .compaction_bytes_carried
        .fetch_add(carried.bytes, Relaxed);
}

/// Installs one atomic version edit replacing `payload.table_ids` with the SSTs
/// and blob files produced by all `outputs` (one for a single compaction, N for
/// parallel sub-compactions). Globally-dead blob files are dropped once here.
/// Returns the total number of output tables, or `None` when an input left the
/// version while the compaction ran: the outputs were written from tables the
/// tree no longer holds (a `clear` removed them), so they are declined, and the
/// run removes them as it removes any output it did not install.
pub(super) fn install_merge(
    super_version: &mut SuperVersions,
    opts: &Options,
    payload: &CompactionPayload,
    outputs: Vec<ProducedOutput>,
) -> crate::Result<Option<usize>> {
    if !holds_every_input(
        &super_version.latest_version().version,
        payload.table_ids.iter().copied(),
    ) {
        log::warn!("Compaction inputs left the version before the install, declining the outputs");
        return Ok(None);
    }

    let mut created_tables = Vec::new();
    let mut created_blob_files = Vec::new();
    let mut blob_files_to_drop = Vec::new();
    let mut tables_to_delete = Vec::new();
    let mut blob_frag_map = FragmentationMap::default();
    let mut filter_transformed = false;
    let mut collected_below_watermark = false;
    let mut relocated_bytes = 0;
    // Held to the end of the install, success or failure: the filter budget
    // keeps the outputs' room until then.
    let mut filter_sizings = Vec::new();

    let mut released_objects = Vec::new();
    let mut carried = super::carry::Carried::default();
    let mut work = super::work::Installed::default();
    let mut installed_files = crate::tree::InstalledCharge::default();
    for out in outputs {
        let installed = out.installed_work()?;
        installed_files.add(
            &out.created_tables,
            &out.created_blob_files,
            crate::fs::WriteOrigin::CompactionTable,
            crate::fs::WriteOrigin::CompactionBlob,
        )?;
        work.written += installed.written;
        work.carried += installed.carried;
        work.read += installed.read;
        released_objects.extend(out.released_objects);
        relocated_bytes += out.relocated_bytes;
        carried += out.carried;
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

    // Globally-dead blob files are dropped once, from the install-time version,
    // counting what this install charges too: a file whose last values this
    // compaction dropped goes with it, not with whichever install comes next,
    // which on a store gone quiet may never come.
    let current_version = super_version.latest_version();
    for blob_file in current_version.version.blob_files.iter() {
        if blob_file.is_dead_after(current_version.version.gc_stats(), &blob_frag_map)
            && !blob_files_to_drop.contains(blob_file)
        {
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
                map.get(&id).map_or((0, 0, 0, 0), |e| {
                    (e.len, e.bytes, e.on_disk_bytes, e.block_bytes)
                })
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
            let on_disk = rest(meta.own_frame_bytes(), stats.2, pass.2);
            let block = rest(meta.block_entry_bytes, stats.3, pass.3);
            // Any counter short of its total: objects of zero bytes still
            // count, and a file is dead only when its entry reaches every
            // total, so an entry is made for them too.
            if len > 0 || bytes > 0 || on_disk > 0 || block > 0 {
                blob_frag_map.add(
                    id,
                    crate::blob_tree::FragmentationEntry::new(len, bytes, on_disk)
                        .with_block_bytes(block),
                );
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
    opts.work.installed(work);
    // Still under the write lock that published the version: a reference read
    // from an older version names an object this run let go of.
    if !released_objects.is_empty() {
        let published = super_version.latest_version().version.id();
        super_version
            .released()
            .record(published, released_objects, []);
    }
    installed_files.charge();
    #[cfg(feature = "metrics")]
    charge_installed(&opts.metrics, relocated_bytes, carried);
    #[cfg(not(feature = "metrics"))]
    let _ = (relocated_bytes, carried);

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

    Ok(Some(tables_out))
}

/// Whether `version` still holds every table of `ids`: a compaction reads its
/// inputs, then installs what it wrote from them, and anything that removed an
/// input in between (a `clear`) leaves outputs the tree must not take.
pub(super) fn holds_every_input(
    version: &Version,
    ids: impl IntoIterator<Item = crate::TableId>,
) -> bool {
    ids.into_iter().all(|id| version.get_table(id).is_some())
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
pub(crate) struct RelocatingCompaction {
    inner: StandardCompaction,
    blob_scanner: Peekable<BlobFileMergeScanner>,
    blob_writer: BlobFileWriter,
    /// Codec of each file being rewritten, by id, the lifetime class its
    /// relocated values go to, and where it lies in `rewriting_blob_files`.
    /// Relocation copies frames VERBATIM, so the output file must record the
    /// codec of the SOURCE those frames came from, and hold the dictionary of
    /// its own the source holds; the current blob policy describes what would
    /// be written fresh, which is a different question and need not agree.
    rewriting_blob_file_outputs: crate::HashMap<BlobFileId, (crate::CompressionType, u8, usize)>,
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
    /// Bytes of the values moved into the new blob files so far, whole
    /// values and cell-row objects alike (see [`BlobIndirection::stored`]).
    relocated_bytes: u64,
    /// The part of them copied as frames as they lay.
    blob_carried: u64,
    /// The bytes of the records read by handle for cell rows so far, outside
    /// the paced scan of the rewritten files.
    frames_read: u64,
    /// How a value at or above the separation threshold that reaches this
    /// pass inline is separated into `blob_writer`: a value the compaction
    /// filter or the merge operator put in place, which the stream leaves
    /// inline for this pass to write beside the relocated frames. `None`
    /// writes every inline value inline.
    fresh: Option<FreshValues>,
    /// The live share of a block, by its entries' bytes, at or above which
    /// the block is copied whole as it was stored, its dead values with it;
    /// below it, its live values are written into new blocks. `None` copies
    /// no block whole.
    block_copy_share: Option<f64>,
    /// The blocks of the rewritten files met so far whose values are not all
    /// seen yet, by file and frame.
    blocks: crate::HashMap<(BlobFileId, u32), BlockTally>,
    /// How each block a pointer of this pass names is relocated, once its
    /// values are all seen.
    fates: crate::HashMap<(BlobFileId, u32), BlockFate>,
    /// The rows from the first pointer into a block not tallied yet on, in
    /// stream order: the table takes rows in key order, so a row waits for
    /// every row before it.
    /// Its bytes are bounded by [`HELD_BYTES_MAX`].
    held: HeldRows,
    /// The blocks given up at that bound: their values are written anew, and
    /// the rest of them is not tallied again.
    abandoned: HashSet<(BlobFileId, u32)>,
    /// The dead values the blocks copied whole carry into the new files.
    carried_garbage: FragmentationMap,
}

/// What a relocation has seen of one block of a rewritten file.
struct BlockTally {
    /// The block as stored, for a copy.
    stored: crate::Slice,
    /// The key of its first value, which the scan reaches first.
    first_key: crate::UserKey,
    /// How many values the block holds, how many were seen, and their
    /// decoded bytes so far.
    values: u16,
    seen: u16,
    value_bytes: u64,
    /// The entry bytes of the values a pointer claimed, and how many.
    live: u64,
    claimed: u16,
    /// The values no pointer claimed: how many, their bytes and their entry
    /// bytes.
    dead: (usize, u64, u64),
    /// A value was reached by a resynchronized scan.
    resynced: bool,
    /// A claimed value is listed at another seqno than its table row's (an
    /// ingested one): a copy as stored would keep the wrong listing.
    relisted: bool,
}

/// How a value the scan reached counts in its block's tally.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Claim {
    /// No pointer claimed it.
    Dead,
    /// A pointer claimed it, listed at the seqno its frame lists.
    Live,
    /// A pointer claimed it, listed at another seqno than its frame lists.
    Relisted,
}

/// How a relocation moves the values of a block it copies whole; a block
/// with no fate has its live values written into new blocks. Each kind counts
/// the claimed values still to place, and the fate is dropped with the last
/// of them, so a pass holds the blocks of its scan window, not of all it
/// copied.
enum BlockFate {
    /// The block is to be copied as it was stored, once; its dead values go
    /// with it.
    Whole { block: WholeBlock, pending: u16 },
    /// The block was copied whole into the frame `at` names: the values
    /// still to place point there, and the stored bytes are let go.
    Copied { at: (BlobFileId, u32), pending: u16 },
}

/// At most the bytes of the rows a relocation holds back while the blocks
/// they wait for are tallied. A block's last value arrives within a few rows
/// of its first unless the block spans keys a denser input fills, such as a
/// block of a sparse flush merged into a full level: past this much, the
/// blocks still tallied are given up and their values written anew, which
/// costs only the copy as stored, not the rows' memory.
const HELD_BYTES_MAX: usize = 8 * 1024 * 1024;

/// A block a relocation copies whole, and what the scan counted of it: what
/// the copy adds to the new file, so the block is not read again for it.
struct WholeBlock {
    stored: crate::Slice,
    first_key: crate::UserKey,
    last_key: crate::UserKey,
    values: u16,
    value_bytes: u64,
    entry_bytes: u64,
    /// Its values no pointer claimed: how many, their bytes and their entry
    /// bytes.
    dead: (usize, u64, u64),
}

/// A row a relocation holds back.
enum Held {
    /// A row this pass writes as it came.
    Row(InternalValue),
    /// A pointer into a rewritten file, its value claimed from the scan.
    Pointer {
        item: InternalValue,
        indirection: BlobIndirection,
        claimed: Claimed,
    },
}

/// Blocks a relocation gave up because the rows held for them passed the
/// bound, counted for the tests that reach that branch.
#[cfg(test)]
pub(crate) static BLOCKS_GIVEN_UP: core::sync::atomic::AtomicUsize =
    core::sync::atomic::AtomicUsize::new(0);

/// A decoded block held values are views of, and the held row that counts it.
struct HeldBlock {
    /// Tells the block's allocation apart from every other one alive while
    /// a held value views it.
    id: usize,
    bytes: usize,
    /// The number of the row that counts it: the last held value of it, so
    /// it is counted until the last of them is written.
    counted_by: u64,
}

/// The rows a relocation holds back, in stream order, and the bytes they keep
/// in memory.
#[derive(Default)]
struct HeldRows {
    /// Each with the bytes it is counted at.
    rows: alloc::collections::VecDeque<(Held, usize)>,
    bytes: usize,
    /// How many rows were held and let go: a row's place is its number less
    /// this.
    released: u64,
    /// The decoded block the last held value is a view of.
    block: Option<HeldBlock>,
}

impl HeldRows {
    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn front(&self) -> Option<&Held> {
        self.rows.front().map(|(held, _)| held)
    }

    /// Holds `held` last. A value viewing a decoded block keeps the block
    /// alive: the block is counted once, with the last held value of it, so
    /// it stays counted until every held value of it is let go.
    fn push(&mut self, mut held: Held) {
        let mut bytes = held_len(&held);
        if let Held::Pointer { claimed, .. } = &mut held
            && claimed.in_block
            && let Some(value) = &mut claimed.value
        {
            match value.allocation() {
                crate::slice::Allocation::Own => {}
                crate::slice::Allocation::Shared { id, len } => {
                    let number = self.released + self.rows.len() as u64;
                    match &mut self.block {
                        Some(block) if block.id == id => {
                            // The row counting the block is still held: it
                            // stops being `block` as it is let go.
                            let at = usize::try_from(block.counted_by - self.released).ok();
                            if let Some((_, counted)) = at.and_then(|at| self.rows.get_mut(at)) {
                                *counted -= block.bytes;
                                self.bytes -= block.bytes;
                            }
                            block.counted_by = number;
                        }
                        // A block held again past another one is counted
                        // again.
                        _ => {
                            self.block = Some(HeldBlock {
                                id,
                                bytes: len,
                                counted_by: number,
                            });
                        }
                    }
                    bytes += len;
                }
                // The block it keeps alive cannot be counted, so the value is
                // copied into a buffer of its own.
                crate::slice::Allocation::Unknown => {
                    *value = crate::UserValue::from(&**value);
                }
            }
        }
        self.bytes += bytes;
        self.rows.push_back((held, bytes));
    }

    /// Lets the first held row go.
    fn pop_front(&mut self) -> Option<Held> {
        let (held, bytes) = self.rows.pop_front()?;
        self.bytes -= bytes;
        if self
            .block
            .as_ref()
            .is_some_and(|block| block.counted_by == self.released)
        {
            self.block = None;
        }
        self.released += 1;
        Some(held)
    }
}

/// The bytes `held` keeps in memory: its place in the queue, its row's key
/// and value, and for a pointer the value it claimed. A held value decoded
/// out of a block keeps the whole block alive besides, counted apart. A held
/// row's key and value are counted alone: rows arrive in key order from the
/// tables' blocks one after the other, so the rows held share the blocks they
/// come from.
fn held_len(held: &Held) -> usize {
    core::mem::size_of::<Held>()
        + match held {
            Held::Row(item) => item.key.user_key.len() + item.value.len(),
            Held::Pointer { item, claimed, .. } => {
                item.key.user_key.len() + item.value.len() + claimed.uncompressed_len as usize
            }
        }
}

/// What a relocation keeps of a value it claimed from the scan to place it.
struct Claimed {
    blob_file_id: BlobFileId,
    frame: u32,
    index: u16,
    /// The seqno its frame lists the value at.
    seqno: crate::SeqNo,
    /// The value decoded, for a value written anew; `None` for a value with
    /// frames of its own, copied as stored.
    value: Option<crate::UserValue>,
    uncompressed_len: u32,
    /// The value's frames as stored, for a value with frames of its own.
    frames: Option<Vec<crate::Slice>>,
    /// The value shares a block.
    in_block: bool,
}

impl Claimed {
    fn of(entry: ScanEntry, blob_file_id: BlobFileId) -> Self {
        Self {
            blob_file_id,
            frame: entry.frame,
            index: entry.index,
            seqno: entry.seqno,
            value: entry.value,
            uncompressed_len: entry.uncompressed_len,
            frames: entry.frames,
            in_block: entry.block.is_some(),
        }
    }
}

/// Counts `entry`, a value of `blob_file_id` the scan reached, claimed by a
/// pointer or not, into its block's tally, and settles the block's fate in
/// `fates` once its last value is counted. A value outside a block, or a pass
/// that copies no block whole, counts nothing.
fn tally_block(
    blocks: &mut crate::HashMap<(BlobFileId, u32), BlockTally>,
    fates: &mut crate::HashMap<(BlobFileId, u32), BlockFate>,
    abandoned: &HashSet<(BlobFileId, u32)>,
    share: Option<f64>,
    blob_file_id: BlobFileId,
    entry: &ScanEntry,
    claim: Claim,
) {
    let (Some(share), Some(block)) = (share, &entry.block) else {
        return;
    };
    let at = (blob_file_id, entry.frame);
    if abandoned.contains(&at) {
        return;
    }
    let tally = blocks.entry(at).or_insert_with(|| BlockTally {
        stored: block.stored.clone(),
        first_key: entry.key.clone(),
        values: block.values,
        seen: 0,
        value_bytes: 0,
        live: 0,
        claimed: 0,
        dead: (0, 0, 0),
        resynced: false,
        relisted: false,
    });
    let bytes = block.entry_bytes;
    tally.seen += 1;
    tally.value_bytes += u64::from(entry.uncompressed_len);
    tally.resynced |= entry.resynced;
    tally.relisted |= claim == Claim::Relisted;
    if claim == Claim::Dead {
        tally.dead.0 += 1;
        tally.dead.1 += u64::from(entry.uncompressed_len);
        tally.dead.2 += bytes;
    } else {
        tally.live += bytes;
        tally.claimed += 1;
    }
    if tally.seen < tally.values {
        return;
    }
    let Some(tally) = blocks.remove(&at) else {
        return;
    };
    // A block no pointer claimed has nothing to move.
    if tally.live == 0 {
        return;
    }
    // Entry bytes stay far below 2^53, where an f64 holds every integer.
    #[expect(clippy::cast_precision_loss, reason = "exact below 2^53")]
    let whole = !tally.resynced
        && !tally.relisted
        && tally.live as f64 >= share * (tally.live + tally.dead.2) as f64;
    // A block below the share gets no fate: its values are written anew.
    if whole {
        fates.insert(
            at,
            BlockFate::Whole {
                block: WholeBlock {
                    stored: tally.stored,
                    first_key: tally.first_key,
                    // The value counted last is the block's last.
                    last_key: entry.key.clone(),
                    values: tally.values,
                    value_bytes: tally.value_bytes,
                    entry_bytes: tally.live + tally.dead.2,
                    dead: tally.dead,
                },
                pending: tally.claimed,
            },
        );
    }
}

/// What a relocating pass separates new values under: the tree's separation
/// options and the blob codec of the compaction's runtime snapshot.
pub(crate) struct FreshValues {
    pub(crate) options: crate::KvSeparationOptions,
    pub(crate) compression: crate::CompressionType,
}

impl RelocatingCompaction {
    #[expect(
        clippy::too_many_arguments,
        reason = "each argument is an independent input of the pass; a struct would only rename them"
    )]
    pub(crate) fn new(
        inner: StandardCompaction,
        blob_scanner: Peekable<BlobFileMergeScanner>,
        blob_writer: BlobFileWriter,
        rewriting_blob_files: Vec<BlobFile>,
        rate_limiter: alloc::sync::Arc<crate::rate_limiter::RateLimiter>,
        stop_signal: crate::stop_signal::StopSignal,
        comparator: crate::comparator::SharedComparator,
        tree_id: crate::TreeId,
    ) -> Self {
        let groups = blob_writer.lifetime_groups();
        Self {
            inner,
            blob_scanner,
            blob_writer,
            rewriting_blob_file_outputs: rewriting_blob_files
                .iter()
                .enumerate()
                .map(|(at, bf)| {
                    (
                        bf.id(),
                        (bf.compression(), groups.relocated(bf.lifetime_class()), at),
                    )
                })
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
            relocated_bytes: 0,
            blob_carried: 0,
            frames_read: 0,
            fresh: None,
            block_copy_share: None,
            blocks: crate::HashMap::default(),
            fates: crate::HashMap::default(),
            held: HeldRows::default(),
            abandoned: HashSet::default(),
            carried_garbage: FragmentationMap::default(),
        }
    }

    /// Copies a block whole, its dead values with it, while the live share of
    /// its entry bytes is at least `share`; below it, or with no share, writes
    /// its live values into new blocks.
    #[must_use]
    pub(crate) fn with_block_copy_share(mut self, share: Option<f32>) -> Self {
        self.block_copy_share = share.map(f64::from);
        self
    }

    /// Writes the table row of `key` at `seqno` pointing at `indirection`.
    fn write_pointer(
        &mut self,
        key: crate::UserKey,
        seqno: crate::SeqNo,
        indirection: &BlobIndirection,
    ) -> crate::Result<()> {
        let mut encoded = [0; BlobIndirection::ENCODED_CAP];
        let pointer = indirection.encode_in(&mut encoded);
        self.inner
            .table_writer
            .write(InternalValue::from_components(
                key,
                pointer,
                seqno,
                crate::ValueType::Indirection,
            ))?;
        Ok(())
    }

    /// Separates values at or above `fresh`'s threshold that reach this pass
    /// inline into the pass's own blob output (see the `fresh` field).
    #[must_use]
    pub(crate) fn with_fresh_values(mut self, fresh: FreshValues) -> Self {
        self.fresh = Some(fresh);
        self
    }

    /// Writes `item`, an inline value at or above the separation threshold,
    /// into this pass's blob output as a value new to it, encoded under
    /// `compression` in a file of lifetime `class`, and its pointer into the
    /// table.
    fn write_fresh(
        &mut self,
        compression: crate::CompressionType,
        class: u8,
        item: InternalValue,
    ) -> crate::Result<()> {
        let indirection = self.blob_writer.write_fresh(
            &item.key.user_key,
            item.key.seqno,
            &item.value,
            compression,
            class,
        )?;
        self.write_pointer(item.key.user_key, item.key.seqno, &indirection)?;
        self.inner.table_writer.register_blob(indirection);
        Ok(())
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
                .rewriting_blob_file_outputs
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

    /// Copies the value `indirection` names into this pass's output and
    /// returns the copy's indirection: its frames verbatim when it has frames
    /// of its own, the value itself when it shares a block.
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
        let (source, read) = crate::vlog::blob_file::reader::Reader::new(blob_file, file.as_ref())
            .copy_source(key, &indirection.vhandle)?;
        self.frames_read += read;

        // Paced like a matched frame: see the whole-value path in `write`.
        let _ = self
            .rate_limiter
            .request_interruptible(read, || self.stop_signal.is_stopped());

        // A file's own dictionary goes with its frames into the file that
        // copies them, which holds it as its own.
        let class = self
            .blob_writer
            .lifetime_groups()
            .relocated(blob_file.lifetime_class());
        let verbatim = self.blob_writer.select_copy_output(blob_file, class)?;
        let copy = match &source {
            crate::vlog::blob_file::reader::CopySource::Frames { frames, .. } if !verbatim => {
                let value = crate::vlog::blob_file::reader::decode_copied(blob_file, frames)?;
                self.blob_writer.write(key, seqno, &value)?
            }
            crate::vlog::blob_file::reader::CopySource::Frames { frames, value_len } => {
                // A row's objects may have been written at older sequence
                // numbers than the row (one it borrows), and its copies follow
                // one another at its key: listed at the row's own, they keep
                // the file's order. Only the listing is rewritten; the payload
                // is written from the frame read, as stored.
                let copy = self
                    .blob_writer
                    .write_verbatim_at(key, frames, *value_len, seqno)?;
                // A verbatim copy is frames of its own, its bytes exact.
                self.blob_carried += u64::from(copy.stored.bytes());
                copy
            }
            crate::vlog::blob_file::reader::CopySource::Value(value) => {
                self.blob_writer.write(key, seqno, value)?
            }
        };
        self.relocated_bytes += u64::from(copy.stored.bytes());
        Ok(copy)
    }

    /// Marks this as a tight-space slice ending (exclusively) at `bound`; see
    /// the `drain_below` field.
    #[cfg(feature = "std")]
    #[must_use]
    pub(crate) fn with_drain_below(mut self, bound: Option<crate::UserKey>) -> Self {
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
            blocks,
            fates,
            abandoned,
            block_copy_share,
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
            &mut |id, entry| {
                tally_block(
                    blocks,
                    fates,
                    abandoned,
                    *block_copy_share,
                    id,
                    entry,
                    Claim::Dead,
                );
            },
            &|| stop_signal.is_stopped(),
        )
    }

    /// Claims the value `indirection` names for the pointer of `key` at
    /// `seqno` from the scan of the rewritten files, the values before it
    /// drained as no pointer's, and advances the frontier past it.
    fn claim(
        &mut self,
        key: &crate::UserKey,
        seqno: crate::SeqNo,
        indirection: &BlobIndirection,
    ) -> crate::Result<Claimed> {
        self.drain_blobs(key, indirection)?;

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
            blob_entry.key, *key,
            "matched blob has different key than vptr",
        );
        assert_eq!(
            (blob_entry.frame, blob_entry.index),
            (indirection.vhandle.frame, indirection.vhandle.index),
            "matched blob lies elsewhere than vptr names",
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
        // An ingestion lists its values before its table is given a seqno: a
        // block holding one is relisted value by value, never copied whole.
        let claim = if blob_entry.seqno == seqno {
            Claim::Live
        } else {
            Claim::Relisted
        };
        tally_block(
            &mut self.blocks,
            &mut self.fates,
            &self.abandoned,
            self.block_copy_share,
            blob_file_id,
            &blob_entry,
            claim,
        );
        Ok(Claimed::of(blob_entry, blob_file_id))
    }

    /// Writes the pointer of `item`, whose value `claimed` this pass claimed,
    /// at the value's new place: its frames copied as they lay, its block
    /// copied whole when the block's fate says so, or the value written anew
    /// into a block.
    fn place(
        &mut self,
        item: InternalValue,
        indirection: &BlobIndirection,
        claimed: &Claimed,
    ) -> crate::Result<()> {
        let blob_file_id = claimed.blob_file_id;
        log::trace!(
            "=> use blob: {:?}:{} frame: {} index: {} from BF {}",
            item.key.user_key,
            item.key.seqno,
            claimed.frame,
            claimed.index,
            blob_file_id,
        );

        // These bytes are the SOURCE's, still encoded as it left them. They go
        // to the output file that records that codec, one per codec for the
        // whole pass: a blob file describes one codec, and a relocation can
        // draw from sources that do not share one (the blob policy may have
        // changed since they were written). Stamping the current policy
        // instead would label these frames with a codec they are not in, and
        // the next read of them would fail to decode.
        #[expect(
            clippy::expect_used,
            reason = "the id came from `rewriting_blob_file_outputs`'s own key set"
        )]
        let (_, class, source_at) = *self
            .rewriting_blob_file_outputs
            .get(&blob_file_id)
            .expect("relocated frame comes from a file being rewritten");

        let block_at = (blob_file_id, claimed.frame);
        let whole = if claimed.in_block {
            self.place_in_block(&item.key.user_key, block_at, (class, source_at))?
        } else {
            None
        };
        let new_indirection = if let Some((file, frame)) = whole {
            // The value keeps its place in the block, and its entry the bytes
            // it is charged.
            BlobIndirection {
                vhandle: crate::vlog::ValueHandle {
                    blob_file_id: file,
                    frame,
                    index: claimed.index,
                },
                size: indirection.size,
                stored: indirection.stored,
            }
        } else {
            // Throttle the relocated blob payload — this is the heavy
            // KV-separation I/O the merge-loop limiter cannot see (it only has
            // the encoded handle). Interruptible so a low limit can't stall
            // shutdown; the return is ignored because the blob is already read
            // and must be written to keep the new vptr valid — only the *wait*
            // is shortened on stop, and the debit stays spent for the write
            // that follows.
            let _ = self
                .rate_limiter
                .request_interruptible(u64::from(claimed.uncompressed_len), || {
                    self.stop_signal.is_stopped()
                });
            // A file's own dictionary goes with its frames into the file that
            // copies them, which holds it as its own.
            let Some(source) = self.rewriting_blob_files.get(source_at) else {
                return Err(crate::Error::InvalidHeader("Blob"));
            };
            let verbatim = self.blob_writer.select_copy_output(source, class)?;
            match (&claimed.frames, &claimed.value) {
                // Past the outputs holding a source's dictionary: decoded and
                // written anew.
                (Some(frames), _) if !verbatim => {
                    let value = crate::vlog::blob_file::reader::decode_copied(source, frames)?;
                    self.blob_writer
                        .write(&item.key.user_key, item.key.seqno, &value)?
                }
                (Some(frames), _) => {
                    // A value is listed at the seqno its table gives it. An
                    // ingestion writes its blob entries before it is given
                    // one, so its frames are relisted as they are copied; any
                    // other frame lists its table's already.
                    let copy = if claimed.seqno == item.key.seqno {
                        self.blob_writer.write_verbatim(
                            &item.key.user_key,
                            frames,
                            claimed.uncompressed_len,
                        )?
                    } else {
                        self.blob_writer.write_verbatim_at(
                            &item.key.user_key,
                            frames,
                            claimed.uncompressed_len,
                            item.key.seqno,
                        )?
                    };
                    // A verbatim copy is frames of its own, its bytes exact.
                    self.blob_carried += u64::from(copy.stored.bytes());
                    copy
                }
                (None, Some(value)) => {
                    self.blob_writer
                        .write(&item.key.user_key, item.key.seqno, value)?
                }
                // A scan yields a value either in a block, decoded, or with
                // frames of its own.
                (None, None) => return Err(crate::Error::InvalidHeader("Blob")),
            }
        };
        debug_assert_eq!(
            new_indirection.size, indirection.size,
            "relocating a value does not change it",
        );
        log::trace!("RELOCATE to {new_indirection:?}");
        self.relocated_bytes += u64::from(new_indirection.stored.bytes());

        self.write_pointer(item.key.user_key, item.key.seqno, &new_indirection)?;
        self.inner.table_writer.register_blob(new_indirection);
        Ok(())
    }

    /// Where a value of the block `block_at` goes when the block is copied
    /// whole: the copy, made by the first of its claimed values to arrive,
    /// into the output of `output`'s class for the source at `output`'s place
    /// in the rewritten files. `None` when its values are written
    /// anew, or it has no fate. Counts the value placed off the block's fate,
    /// which goes with the last.
    fn place_in_block(
        &mut self,
        key: &[u8],
        block_at: (BlobFileId, u32),
        output: (u8, usize),
    ) -> crate::Result<Option<(BlobFileId, u32)>> {
        let (placed, done) = match self.fates.get_mut(&block_at) {
            None => return Ok(None),
            Some(BlockFate::Copied { at, pending }) => {
                debug_assert!(*pending > 0, "a fate counts the values still to place");
                *pending -= 1;
                (*at, *pending == 0)
            }
            Some(BlockFate::Whole { .. }) => {
                let Some(BlockFate::Whole { block, pending }) = self.fates.remove(&block_at) else {
                    return Ok(None);
                };
                // A block a key domain splits (the policy set after it was
                // written), or of a source past the ones the writer copies
                // blocks of, keeps no fate and has its values written anew:
                // copied whole, its file would span two domains, or one more
                // file would be held open for its source.
                if !self
                    .blob_writer
                    .can_copy_block(block_at.0, &block.first_key, &block.last_key)
                {
                    return Ok(None);
                }
                let at = self.copy_whole(key, output, &block)?;
                // The stored bytes go here: the values after this one need
                // only where the copy lies.
                if pending > 1 {
                    self.fates.insert(
                        block_at,
                        BlockFate::Copied {
                            at,
                            pending: pending - 1,
                        },
                    );
                }
                return Ok(Some(at));
            }
        };
        if done {
            self.fates.remove(&block_at);
        }
        Ok(Some(placed))
    }

    /// Copies `block` whole into the output of its source, the rewritten file
    /// at `output`'s place, under that file's codec and own dictionary and
    /// `output`'s class, and counts its dead values as the new file's garbage.
    fn copy_whole(
        &mut self,
        key: &[u8],
        output: (u8, usize),
        block: &WholeBlock,
    ) -> crate::Result<(BlobFileId, u32)> {
        let stored = &block.stored;
        // Paced once for the block, as a whole-value copy is.
        let _ = self
            .rate_limiter
            .request_interruptible(stored.len() as u64, || self.stop_signal.is_stopped());
        let copied = crate::vlog::blob_file::writer::CopiedBlock {
            stored,
            first_key: &block.first_key,
            last_key: &block.last_key,
            values: block.values,
            value_bytes: block.value_bytes,
            entry_bytes: block.entry_bytes,
        };
        // A file's own dictionary goes with its blocks into the file that
        // copies them, as it does with its values in `place`.
        let Some(source) = self.rewriting_blob_files.get(output.1) else {
            return Err(crate::Error::InvalidHeader("Blob"));
        };
        let at = self
            .blob_writer
            .copy_block(key, source, output.0, &copied)?;
        self.blob_carried += stored.len() as u64;
        // The block's dead values arrive dead in the new file.
        let dead = block.dead;
        if dead.0 > 0 {
            self.carried_garbage.add(
                at.0,
                crate::blob_tree::FragmentationEntry::new(dead.0, dead.1, 0)
                    .with_block_bytes(dead.2),
            );
        }
        Ok(at)
    }

    /// Writes `item`, a row whose value this pass does not relocate.
    fn write_row(&mut self, item: InternalValue) -> crate::Result<()> {
        if item.key.value_type.is_cell_row() {
            return self.write_cell_row(item);
        }
        if item.key.value_type.is_indirection() {
            let indirection = BlobIndirection::decode_slice(&item.value)?;
            // This blob is not part of the rewritten blob files, so just pass it
            // through.
            log::trace!("Pass through {indirection:?} because it is not being relocated");
            self.inner.table_writer.write(item)?;
            self.inner.table_writer.register_blob(indirection);
        } else if let Some(fresh) = &self.fresh
            && item.key.value_type == crate::ValueType::Value
            && item.value.len() >= fresh.options.separation_threshold as usize
        {
            // A value new to the value log: the filter's or the merge
            // operator's, or one written inline before the threshold came
            // down to it. It goes beside the frames this pass relocates.
            let class = fresh
                .options
                .lifetime_classifier()
                .class_of(&item.key.user_key, false);
            let compression = fresh.compression;
            self.write_fresh(compression, class, item)?;
        } else {
            self.inner.table_writer.write(item)?;
        }
        Ok(())
    }

    /// Whether a pointer into `frame` of `blob_file_id` waits: the frame is a
    /// block whose values are not all seen yet.
    fn waits_for(&self, blob_file_id: BlobFileId, frame: u32) -> bool {
        self.blocks.contains_key(&(blob_file_id, frame))
    }

    /// Whether `held` can be written now.
    fn is_ready(&self, held: &Held) -> bool {
        match held {
            Held::Row(_) => true,
            Held::Pointer { claimed, .. } => !self.waits_for(claimed.blob_file_id, claimed.frame),
        }
    }

    /// Writes `held`, which is ready.
    fn write_held(&mut self, held: Held) -> crate::Result<()> {
        match held {
            Held::Row(item) => self.write_row(item),
            Held::Pointer {
                item,
                indirection,
                claimed,
            } => self.place(item, &indirection, &claimed),
        }
    }

    /// Writes the held rows from the front as long as they are ready.
    fn release_ready(&mut self) -> crate::Result<()> {
        while self.held.front().is_some_and(|held| self.is_ready(held)) {
            let Some(held) = self.held.pop_front() else {
                break;
            };
            self.write_held(held)?;
        }
        Ok(())
    }

    /// Holds `held` back behind the rows before it, and writes what is ready.
    /// Past [`HELD_BYTES_MAX`], the blocks still tallied are given up so the
    /// rows waiting for them go out.
    fn hold(&mut self, held: Held) -> crate::Result<()> {
        self.held.push(held);
        if self.held.bytes > HELD_BYTES_MAX {
            #[cfg(test)]
            BLOCKS_GIVEN_UP.fetch_add(self.blocks.len(), core::sync::atomic::Ordering::Relaxed);
            self.abandoned.extend(self.blocks.drain().map(|(at, _)| at));
        }
        self.release_ready()
    }

    /// Writes every held row: a block whose values were not all seen, its last
    /// ones lying past where this pass reads, keeps no fate and has its live
    /// values written anew, its live share unknown.
    fn release_all(&mut self) -> crate::Result<()> {
        self.blocks.clear();
        self.release_ready()
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
            blocks,
            fates,
            abandoned,
            block_copy_share,
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
            &mut |id, entry| {
                tally_block(
                    blocks,
                    fates,
                    abandoned,
                    *block_copy_share,
                    id,
                    entry,
                    Claim::Dead,
                );
            },
        )
    }
}

impl CompactionFlavour for RelocatingCompaction {
    fn write_range_tombstones(
        &mut self,
        tombstones: &[RangeTombstone],
        outside: TombstonesOutside,
    ) {
        self.inner.write_range_tombstones(tombstones, outside);
    }

    #[cfg(feature = "std")]
    fn end_before(&mut self, end: UserKey) {
        self.inner.end_before(end.clone());
        self.drain_below = Some(end);
    }

    /// A relocating compaction moves the blob values its rows point at, so it
    /// writes every row and copies no group.
    fn carrying(&mut self) -> Option<&mut StandardCompaction> {
        None
    }

    /// A pointer into a rewritten file claims its value from the scan as it
    /// arrives, in stream order. A pointer into a block waits, with every row
    /// after it, until the block's values are all seen and its fate is known:
    /// copied whole or written value by value.
    fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        if item.key.value_type.is_indirection() {
            let indirection = BlobIndirection::decode_slice(&item.value).inspect_err(|e| {
                log::error!("Failed to deserialize blob indirection {item:?}: {e:?}");
            })?;
            log::trace!(
                "{:?}:{} => encountered indirection: {indirection:?}",
                item.key.user_key,
                item.key.seqno,
            );
            if self
                .rewriting_blob_file_outputs
                .contains_key(&indirection.vhandle.blob_file_id)
            {
                let claimed = self.claim(&item.key.user_key, item.key.seqno, &indirection)?;
                if self.held.is_empty() && !self.waits_for(claimed.blob_file_id, claimed.frame) {
                    return self.place(item, &indirection, &claimed);
                }
                return self.hold(Held::Pointer {
                    item,
                    indirection,
                    claimed,
                });
            }
        }
        if self.held.is_empty() {
            return self.write_row(item);
        }
        // The stream is past every key below this one, so the values of those
        // keys no pointer claimed never will be: drained, they complete the
        // tallies of the blocks a held pointer waits for.
        self.drain_unclaimed_below(&item.key.user_key)?;
        self.hold(Held::Row(item))
    }

    fn produce(
        mut self: Box<Self>,
        opts: &Options,
        dst_lvl: usize,
        mut blob_frag_map: FragmentationMap,
        extra_blob_files: Vec<BlobFile>,
    ) -> crate::Result<ProducedOutput> {
        log::debug!(
            "Relocating compaction done in {:?}",
            self.inner.start.elapsed(),
        );

        if let Some(bound) = self.drain_below.take() {
            self.drain_unclaimed_below(&bound)?;
        }
        self.release_all()?;
        core::mem::take(&mut self.carried_garbage).merge_into(&mut blob_frag_map);

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
            relocated_bytes: self.relocated_bytes,
            // A relocating run writes its rows; it copies no group or block.
            carried: super::carry::Carried::default(),
            segment_copied: 0,
            blob_carried: self.blob_carried,
            frames_read: self.frames_read,
        })
    }
}

/// Standard compaction worker that just passes through all its data
pub(crate) struct StandardCompaction {
    start: Instant,
    table_writer: MultiWriter,
    tables_to_rewrite: Vec<Table>,
}

impl StandardCompaction {
    pub(crate) fn new(table_writer: MultiWriter, tables_to_rewrite: Vec<Table>) -> Self {
        Self {
            start: Instant::now(),
            table_writer,
            tables_to_rewrite,
        }
    }

    /// Records the blob objects `rows`, written into the output not one by
    /// one, reference, each under its own row's key, as
    /// [`CompactionFlavour::write`] records a written row's.
    fn register_references(&mut self, rows: &[InternalValue]) -> crate::Result<()> {
        self.table_writer.register_carried_rows(rows)
    }

    fn consume_writer(self, opts: &Options, dst_lvl: usize) -> crate::Result<Vec<Table>> {
        let table_base_folder = self.table_writer.base_path.clone();
        let level_fs = self.table_writer.fs.clone();

        let pin_filter = opts.config.filter_block_pinning_policy.get(dst_lvl);
        let pin_index = opts.config.index_block_pinning_policy.get(dst_lvl);
        let written = self.table_writer.finish()?;
        let entries_durable = written.entries_durable;
        #[cfg(all(test, feature = "std"))]
        opts.config.fire_while_outputs_finish();

        written
            .tables
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
                params
                    .prefix_extractor
                    .clone_from(&opts.config.prefix_extractor);
                #[cfg(zstd_any)]
                {
                    params.zstd_dictionaries = opts.config.current_zstd_dictionaries();
                }
                #[cfg(feature = "metrics")]
                {
                    params.metrics = opts.metrics.clone();
                }
                let table = Table::recover(params)?;
                if entries_durable {
                    table.mark_entry_durable();
                }
                Ok(table)
            })
            .collect::<crate::Result<Vec<_>>>()
    }
}

/// The row group and data block copies of a compaction that writes every row
/// of its output itself: the only flavour a carry plan is made for (see
/// `carry`).
impl StandardCompaction {
    /// The count of the compaction filter's transformations so far (see
    /// [`crate::compaction::carry::CarrySink`]).
    pub(super) fn transforms_seen(&self) -> u64 {
        self.table_writer.transforms_seen()
    }

    /// Whether `key` is the user key of the last row the output took (see
    /// [`crate::compaction::carry::CarrySink`]).
    pub(super) fn continues_last(&self, key: &[u8]) -> bool {
        self.table_writer.continues_last(key)
    }

    /// Closes the current output's transformation window at `seen` for the
    /// row just put in it (see [`crate::compaction::carry::CarrySink`]).
    pub(super) fn settle_transforms(&mut self, seen: u64) {
        self.table_writer.settle_transforms(seen);
    }

    /// Copies `candidate`'s row group or data block into the output in place
    /// of its rows, returning `false`, having written nothing, when this output
    /// cannot take it as it is; the caller then writes the rows. `pace` is
    /// told how long reading a group takes; a block's bytes are already read.
    pub(super) fn carry(
        &mut self,
        candidate: &crate::table::group_carry::CarryCandidate,
        pace: Option<&dyn crate::table::util::ReadPacer>,
    ) -> crate::Result<bool> {
        use crate::table::group_carry::CarryUnit;

        match &candidate.unit {
            #[cfg(feature = "columnar")]
            CarryUnit::RowGroup(group) => self.carry_group(candidate, *group, pace),
            CarryUnit::DataBlock(frame) => {
                let _ = pace;
                self.carry_block(candidate, frame)
            }
        }
    }

    /// Copies `candidate`'s data block, whose bytes are `frame`, into the
    /// output in place of its rows (see [`Self::carry`]).
    fn carry_block(
        &mut self,
        candidate: &crate::table::group_carry::CarryCandidate,
        frame: &[u8],
    ) -> crate::Result<bool> {
        let table = &candidate.table;
        let source = crate::table::writer::VerbatimSource {
            table_id: table.id(),
            offset: candidate.group.offset().0,
        };
        let shape = crate::table::writer::CarriedBlockShape {
            restart_interval: table.metadata.data_block_restart_interval,
            kv_checksum: table.metadata.kv_checksum_algo,
            hash_ratio_bits: table.metadata.data_block_hash_ratio_bits,
        };
        if !self.table_writer.carry_data_block(
            (frame, source),
            (table.metadata.data_block_compression, shape),
            &candidate.rows,
        )? {
            return Ok(false);
        }
        self.register_references(&candidate.rows)?;
        Ok(true)
    }

    /// Copies `candidate`'s row group, named in its index entry as `group`,
    /// into the output in place of its rows (see [`Self::carry`]).
    #[cfg(feature = "columnar")]
    fn carry_group(
        &mut self,
        candidate: &crate::table::group_carry::CarryCandidate,
        group: crate::table::group_carry::RowGroupRef,
        pace: Option<&dyn crate::table::util::ReadPacer>,
    ) -> crate::Result<bool> {
        let table = &candidate.table;
        let columns = table
            .zone_map
            .columns_for(candidate.group.offset().0)
            .map(<[_]>::to_vec);
        // An output keeping a zone map refuses a group without statistics,
        // so it is refused before its bytes are read for nothing.
        if columns.is_none() && self.table_writer.keeps_zone_map() {
            return Ok(false);
        }
        // Read as the inputs are read: the limiter is told how long it takes
        // and charged nothing, since the merge loop already pays for every
        // row of the group as it pays for rows written anew.
        let raw = table.read_row_group_raw(&candidate.group, pace)?;
        let source = crate::table::writer::VerbatimSource {
            table_id: table.id(),
            offset: candidate.group.offset().0,
        };
        if !self.table_writer.carry_row_group(
            (&raw, source),
            group,
            (
                table.metadata.data_block_compression,
                table.metadata.value_layout,
            ),
            &candidate.rows,
            columns,
        )? {
            return Ok(false);
        }
        self.register_references(&candidate.rows)?;
        Ok(true)
    }

    /// Writes `emitted`, what the merge made of `candidate`'s key range, as
    /// one group that copies the candidate's pages on the row pages left as
    /// they were, returning the bytes copied; `None`, having written nothing,
    /// when this output cannot. The caller then writes the rows. The matcher
    /// asks only once a row of the group came out as read, so the group is
    /// not read for a merge that changed every row. A data block has no pages
    /// to copy apart: `None`.
    #[cfg_attr(
        not(feature = "columnar"),
        expect(
            clippy::unused_self,
            clippy::unnecessary_wraps,
            clippy::needless_pass_by_ref_mut,
            reason = "without columnar tables there are only data blocks, which copy no pages"
        )
    )]
    pub(super) fn carry_pages(
        &mut self,
        candidate: &crate::table::group_carry::CarryCandidate,
        emitted: &[InternalValue],
        pace: Option<&dyn crate::table::util::ReadPacer>,
    ) -> crate::Result<Option<u64>> {
        #[cfg(feature = "columnar")]
        if let crate::table::group_carry::CarryUnit::RowGroup(group) = candidate.unit {
            return self.carry_group_pages(candidate, group, emitted, pace);
        }
        let _ = (candidate, emitted, pace);
        Ok(None)
    }

    /// [`Self::carry_pages`] of a row group named in its index entry as
    /// `group`.
    #[cfg(feature = "columnar")]
    fn carry_group_pages(
        &mut self,
        candidate: &crate::table::group_carry::CarryCandidate,
        group: crate::table::group_carry::RowGroupRef,
        emitted: &[InternalValue],
        pace: Option<&dyn crate::table::util::ReadPacer>,
    ) -> crate::Result<Option<u64>> {
        use crate::coding::Decode;
        use crate::table::block::{Block, BlockIdentity, BlockTransform, BlockType, ChecksumAt};

        let table = &candidate.table;
        // Pages of values stored whole only: a group of cells has the columns
        // its rows' fields make, which a rebuilt group need not repeat.
        if table.metadata.value_layout != crate::table::meta::ValueLayout::Whole {
            return Ok(None);
        }
        // Paced and uncharged, as in `carry`.
        let raw = table.read_row_group_raw(&candidate.group, pace)?;
        let offset = candidate.group.offset().0;
        // The directory is checked at the place it lies, as a read checks it.
        let directory_block = Block::from_reader(
            &mut &raw[..],
            BlockIdentity {
                table_id: table.id(),
                block_type: BlockType::ColumnPageDirectory,
                dict_id: 0,
                window_log: 0,
            },
            &BlockTransform::PLAIN,
            ChecksumAt::table(table.id(), offset),
        )?;
        let directory_len =
            crate::table::block::Header::decode_from(&mut &raw[..])?.on_disk_size_with(None);
        if directory_len != group.directory_len.get() {
            return Ok(None);
        }
        let directory = crate::table::column_page::PageDirectory::decode(&directory_block.data)?;
        let Some((row_pages, copied)) =
            super::carry::align_row_pages(&candidate.rows, directory.row_pages(), emitted)
        else {
            return Ok(None);
        };
        // A changed row page takes all its rows in one page; one the writer
        // would have cut apart for its size is written as rows instead.
        if !super::carry::changed_pages_fit(
            emitted,
            (&row_pages, &copied),
            u64::from(self.table_writer.row_group_size()),
        ) {
            return Ok(None);
        }
        let carry = crate::table::writer::PageCarry {
            tag: group.tag.get(),
            row_pages,
            copied,
            raw: &raw,
            source: crate::table::writer::VerbatimSource {
                table_id: table.id(),
                offset,
            },
            directory: &directory,
            directory_len,
        };
        if !self.table_writer.carry_row_pages(
            emitted,
            &carry,
            table.metadata.data_block_compression,
        )? {
            return Ok(None);
        }
        self.register_references(emitted)?;
        // What the copied row pages hold on disk, every column's page of each.
        let row_page_count = directory.row_pages().len();
        let bytes = directory
            .entries()
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                carry
                    .copied
                    .get(index % row_page_count.max(1))
                    .copied()
                    .unwrap_or(false)
            })
            .map(|(_, entry)| u64::from(entry.length))
            .sum();
        Ok(Some(bytes))
    }
}

impl CompactionFlavour for StandardCompaction {
    fn write_range_tombstones(
        &mut self,
        tombstones: &[RangeTombstone],
        outside: TombstonesOutside,
    ) {
        self.table_writer.set_range_tombstones(tombstones.to_vec());
        self.table_writer.keep_tombstones_outside(outside);
    }

    #[cfg(feature = "std")]
    fn end_before(&mut self, end: UserKey) {
        self.table_writer.answer_below(end);
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

    fn write_key_start(&mut self, item: InternalValue) -> crate::Result<()> {
        if item.key.value_type.is_cell_row() || item.key.value_type.is_indirection() {
            return self.write(item);
        }
        self.table_writer.write_key_start(item)
    }

    fn carrying(&mut self) -> Option<&mut StandardCompaction> {
        Some(self)
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
            relocated_bytes: 0,
            // The producer owns the carry matcher and sets this after.
            carried: super::carry::Carried::default(),
            segment_copied: 0,
            blob_carried: 0,
            frames_read: 0,
        })
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests;
