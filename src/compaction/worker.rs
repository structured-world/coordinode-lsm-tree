// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::{CompactionAction, CompactionResult, CompactionStrategy, Input as CompactionPayload};
use crate::{
    BlobFile, Config, HashSet, InternalValue, SeqNo, SequenceNumberCounter,
    SharedSequenceNumberGenerator, Table, TableId, UserKey,
    blob_tree::FragmentationMap,
    compaction::{
        Choice,
        filter::{Context, StreamFilterAdapter},
        flavour::{RelocatingCompaction, StandardCompaction},
        state::CompactionState,
        stream::CompactionStream,
    },
    file::BLOBS_FOLDER,
    merge::Merger,
    run_scanner::RunScanner,
    stop_signal::StopSignal,
    tree::inner::TreeId,
    version::{Run, SuperVersions, Version},
    vlog::{BlobFileMergeScanner, BlobFileScanner, BlobFileWriter},
};
use std::{
    sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard},
    time::Instant,
};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

pub type CompactionReader<'a> = Box<dyn Iterator<Item = crate::Result<InternalValue>> + 'a>;

/// Minimum total input size for a compaction to be split into parallel
/// sub-compactions. Below this the per-thread setup + the extra output tables
/// (one per sub-range) cost more than the parallelism buys, so the compaction
/// stays single-threaded (which also keeps small/test compactions producing a
/// single merged table). Default for [`Config::subcompaction_min_bytes`].
#[cfg(feature = "std")]
pub const SUBCOMPACTION_MIN_INPUT_BYTES: u64 = 8 * 1024 * 1024;

/// Compaction options
pub struct Options {
    pub tree_id: TreeId,

    pub global_seqno: SharedSequenceNumberGenerator,

    pub visible_seqno: SharedSequenceNumberGenerator,

    pub table_id_generator: SequenceNumberCounter,

    pub blob_file_id_generator: SequenceNumberCounter,

    /// Configuration of tree.
    pub config: Arc<Config>,

    pub version_history: Arc<RwLock<SuperVersions>>,

    /// Compaction strategy to use.
    pub strategy: Arc<dyn CompactionStrategy>,

    /// Stop signal to interrupt a compaction worker in case
    /// the tree is dropped.
    pub stop_signal: StopSignal,

    /// Evicts items that are older than this seqno (MVCC GC).
    pub mvcc_gc_watermark: u64,

    pub compaction_state: Arc<Mutex<CompactionState>>,

    /// Shared handle to the live runtime config. Compaction loads
    /// a fresh snapshot via [`crate::runtime_config::handle::RuntimeConfigHandle::load_full`]
    /// each time it writes the manifest, so toggles applied via
    /// [`crate::Tree::update_runtime_config`] take effect on the
    /// next compaction cycle without restart.
    pub runtime_config: Arc<crate::runtime_config::handle::RuntimeConfigHandle>,

    /// Optional per-tree encryption provider, threaded into manifest
    /// writes so compaction-driven version commits inherit the same
    /// AEAD pipeline the data blocks use.
    pub encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,

    /// Per-compaction I/O rate limiter. Built from
    /// [`Config::compaction_rate_limit`]; a limit of `0` makes every
    /// request immediate (no throttling). Only the compaction merge loop
    /// calls it, so flush and user reads are never throttled.
    pub rate_limiter: Arc<crate::rate_limiter::RateLimiter>,

    #[cfg(feature = "metrics")]
    pub metrics: Arc<Metrics>,
}

impl Options {
    pub fn from_tree(tree: &crate::Tree, strategy: Arc<dyn CompactionStrategy>) -> Self {
        Self {
            global_seqno: tree.config.seqno.clone(),
            visible_seqno: tree.config.visible_seqno.clone(),
            tree_id: tree.id,
            table_id_generator: tree.table_id_counter.clone(),
            blob_file_id_generator: tree.blob_file_id_counter.clone(),
            config: tree.config.clone(),
            version_history: tree.version_history.clone(),
            stop_signal: tree.stop_signal.clone(),
            strategy,
            mvcc_gc_watermark: 0,

            compaction_state: tree.compaction_state.clone(),
            runtime_config: tree.runtime_config.clone(),
            encryption: tree.config.encryption.clone(),
            rate_limiter: Arc::new(crate::rate_limiter::RateLimiter::new(
                tree.config.compaction_rate_limit,
            )),

            #[cfg(feature = "metrics")]
            metrics: tree.metrics.clone(),
        }
    }
}

/// Runs compaction task.
///
/// This will block until the compactor is fully finished.
pub fn do_compaction(opts: &Options) -> crate::Result<CompactionResult> {
    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
    let compaction_state = opts.compaction_state.lock().expect("lock is poisoned");

    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
    let version_history_lock = opts.version_history.read().expect("lock is poisoned");

    let start = Instant::now();
    log::trace!(
        "Consulting compaction strategy {:?}",
        opts.strategy.get_name(),
    );
    let choice = opts.strategy.choose(
        &version_history_lock.latest_version().version,
        &opts.config,
        &compaction_state,
    );

    log::debug!("Compaction choice: {choice:?} in {:?}", start.elapsed());

    match choice {
        Choice::Merge(payload) => {
            merge_tables(compaction_state, version_history_lock, opts, &payload)
        }
        Choice::Move(payload) => {
            // Cross-folder trivial moves are not possible — the file must be
            // rewritten to end up in the correct storage tier directory.
            // This applies even when both folders are on the same filesystem,
            // because rename() across tiered paths would break the routing
            // invariant (table path = f(level)).
            if opts.config.level_routes.is_some() {
                let (dst_folder, _) = opts.config.tables_folder_for_level(payload.dest_level);
                let version = &version_history_lock.latest_version().version;
                // Check actual on-disk table paths (not configured routing) to
                // handle tables that may have been recovered from a different
                // tier after route reconfiguration.
                let cross_folder = version
                    .iter_levels()
                    .flat_map(|level| level.iter())
                    .flat_map(|run| run.iter())
                    .filter(|t| payload.table_ids.contains(&t.id()))
                    .any(|t| t.path.parent() != Some(dst_folder.as_path()));
                if cross_folder {
                    log::debug!("Converting trivial move to merge: cross-folder level routing");
                    return merge_tables(compaction_state, version_history_lock, opts, &payload);
                }
            }

            drop(version_history_lock);

            move_tables(&compaction_state, opts, &payload)
        }
        Choice::Drop(payload) => {
            drop(version_history_lock);

            let ids = payload.into_iter().collect::<Vec<_>>();
            drop_tables(compaction_state, opts, &ids)
        }
        Choice::DoNothing => {
            log::trace!("Compactor chose to do nothing");
            Ok(CompactionResult::nothing())
        }
    }
}

fn pick_run_indexes(run: &Run<Table>, to_compact: &[TableId]) -> Option<(usize, usize)> {
    let lo = run
        .iter()
        .position(|table| to_compact.contains(&table.id()))?;

    let hi = run
        .iter()
        .rposition(|table| to_compact.contains(&table.id()))?;

    Some((lo, hi))
}

fn create_compaction_stream<'a>(
    version: &Version,
    to_compact: &[TableId],
    eviction_seqno: SeqNo,
    merge_operator: Option<Arc<dyn crate::merge_operator::MergeOperator>>,
    comparator: crate::comparator::SharedComparator,
) -> crate::Result<Option<CompactionStream<'a, Merger<CompactionReader<'a>>>>> {
    let mut readers: Vec<CompactionReader<'_>> = vec![];
    let mut found = 0;

    for run in version.iter_levels().flat_map(|lvl| lvl.iter()) {
        if run.len() > 1 {
            let Some((lo, hi)) = pick_run_indexes(run, to_compact) else {
                continue;
            };

            readers.push(Box::new(RunScanner::culled(
                run.clone(),
                (Some(lo), Some(hi)),
            )?));

            found += hi - lo + 1;
        } else {
            for table in run.iter().filter(|x| to_compact.contains(&x.metadata.id)) {
                found += 1;
                readers.push(Box::new(table.scan()?));
            }
        }
    }

    Ok(if found == to_compact.len() {
        Some(
            CompactionStream::new(Merger::new(readers, comparator), eviction_seqno)
                .with_merge_operator(merge_operator),
        )
    } else {
        None
    })
}

/// Like [`create_compaction_stream`] but restricts every input table to the key
/// range `bounds`, used by parallel sub-compactions where each thread owns a
/// disjoint slice of the key space. Each input table is read via
/// [`crate::Table::range`] (a raw, all-seqno, key-bounded scan that seeks within
/// the table), so a sub-compaction only touches the blocks overlapping its
/// slice. The bounds must partition the key space across sub-compactions
/// (`Included(lo)..Excluded(hi)`) so every entry lands in exactly one.
#[cfg(feature = "std")]
fn create_bounded_compaction_stream<'a>(
    version: &'a Version,
    to_compact: &[TableId],
    bounds: (std::ops::Bound<UserKey>, std::ops::Bound<UserKey>),
    eviction_seqno: SeqNo,
    merge_operator: Option<Arc<dyn crate::merge_operator::MergeOperator>>,
    comparator: crate::comparator::SharedComparator,
) -> Option<CompactionStream<'a, Merger<CompactionReader<'a>>>> {
    let mut readers: Vec<CompactionReader<'_>> = vec![];
    let mut found = 0;

    for run in version.iter_levels().flat_map(|lvl| lvl.iter()) {
        for table in run.iter().filter(|x| to_compact.contains(&x.metadata.id)) {
            found += 1;
            readers.push(Box::new(table.range(bounds.clone())));
        }
    }

    if found == to_compact.len() {
        Some(
            CompactionStream::new(Merger::new(readers, comparator), eviction_seqno)
                .with_merge_operator(merge_operator),
        )
    } else {
        None
    }
}

/// Interior split-boundary keys for a parallel compaction, derived from the
/// destination level's existing table boundaries (`RocksDB`'s approach: aligning
/// sub-compaction cuts to target-level files keeps outputs structured). Returns
/// at most `max_ranges - 1` keys (evenly sampled), sorted by `comparator`.
/// Empty → the compaction stays single-threaded (no usable cut points).
#[cfg(feature = "std")]
fn subcompaction_boundaries(
    version: &Version,
    dest_level: usize,
    max_ranges: usize,
    comparator: &crate::comparator::SharedComparator,
) -> Vec<UserKey> {
    if max_ranges < 2 {
        return Vec::new();
    }
    let Some(level) = version.level(dest_level) else {
        return Vec::new();
    };
    let mut keys: Vec<UserKey> = level
        .iter()
        .flat_map(|run| run.iter())
        .map(|t| t.metadata.key_range.max().clone())
        .collect();
    if keys.len() < 2 {
        return Vec::new();
    }
    keys.sort_by(|a, b| comparator.compare(a, b));
    keys.dedup();
    // The global maximum is not a cut point (splitting after it yields an empty
    // trailing range); the rest are interior boundaries.
    keys.pop();
    if keys.is_empty() {
        return Vec::new();
    }
    let want = (max_ranges - 1).min(keys.len());
    if want == keys.len() {
        return keys;
    }
    // Evenly sample `want` boundaries across the candidates.
    let mut out = Vec::with_capacity(want);
    for i in 1..=want {
        let idx = ((i * keys.len()) / (want + 1)).min(keys.len() - 1);
        if let Some(key) = keys.get(idx) {
            out.push(key.clone());
        }
    }
    out.dedup();
    out
}

/// Turns interior boundary keys into `boundaries.len() + 1` disjoint key ranges
/// that partition the whole key space: `(Unbounded, Excluded(b0))`,
/// `[Included(b_i), Excluded(b_{i+1}))`, …, `[Included(b_last), Unbounded)`.
/// Every entry falls in exactly one range, so the sub-compaction outputs union
/// to the same set the serial compaction would produce.
#[cfg(feature = "std")]
fn ranges_from_boundaries(
    boundaries: &[UserKey],
) -> Vec<(std::ops::Bound<UserKey>, std::ops::Bound<UserKey>)> {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    let mut ranges = Vec::with_capacity(boundaries.len() + 1);
    let mut lo = Unbounded;
    for b in boundaries {
        ranges.push((lo.clone(), Excluded(b.clone())));
        lo = Included(b.clone());
    }
    ranges.push((lo, Unbounded));
    ranges
}

/// Runs one sub-compaction over the key range `bounds`: builds a bounded merge
/// stream of the inputs, applies the same transforms as the serial path
/// (tombstone eviction, KV-separation drop tracking, compaction filter), writes
/// its output SSTs, and returns a [`ProducedOutput`](super::flavour::ProducedOutput)
/// WITHOUT installing a version edit (the caller merges all outputs into one).
/// Only the sub-compaction that `owns_input_deletion` carries the input tables
/// to be dropped, so the shared inputs are marked deleted exactly once.
#[cfg(feature = "std")]
#[expect(
    clippy::too_many_arguments,
    reason = "a sub-compaction needs the full compaction context (opts, payload, \
              version, inputs, range, level info, blob folder) threaded in by value/ref \
              so it can run on its own thread; bundling into a struct would just move \
              the argument list"
)]
fn run_subcompaction(
    opts: &Options,
    payload: &CompactionPayload,
    version: &Version,
    tables_for_deletion: Vec<Table>,
    input_range_tombstones: &[crate::range_tombstone::RangeTombstone],
    bounds: (std::ops::Bound<UserKey>, std::ops::Bound<UserKey>),
    dst_lvl: usize,
    is_last_level: bool,
    blobs_folder: &std::path::Path,
) -> crate::Result<super::flavour::ProducedOutput> {
    use super::flavour::CompactionFlavour;

    let mut blob_frag_map = FragmentationMap::default();

    let to_compact = payload.table_ids.iter().copied().collect::<Vec<_>>();

    let Some(mut merge_iter) = create_bounded_compaction_stream(
        version,
        &to_compact,
        bounds,
        opts.mvcc_gc_watermark,
        opts.config.merge_operator.clone(),
        opts.config.comparator.clone(),
    ) else {
        // Inputs vanished mid-flight (validated by the caller, so unexpected):
        // emit an empty output rather than corrupting the merge.
        let table_writer = super::flavour::prepare_table_writer(version, opts, payload)?;
        return Box::new(StandardCompaction::new(table_writer, tables_for_deletion)).produce(
            opts,
            dst_lvl,
            blob_frag_map,
            Vec::new(),
        );
    };

    merge_iter = merge_iter
        .evict_tombstones(is_last_level)
        .zero_seqnos(false);

    let filter_ctx = Context { is_last_level };
    let mut compaction_filter = opts
        .config
        .compaction_filter_factory
        .as_ref()
        .map(|f| f.make_filter(&filter_ctx));

    // KV separation (no relocation on this path): track fragmentation from
    // dropped/GC'd entries so the merged install updates blob GC stats.
    if opts.config.kv_separation_opts.is_some() {
        merge_iter = merge_iter.with_drop_callback(&mut blob_frag_map);
    }

    let mut filter_blob_writer = None;
    let merge_iter = merge_iter.with_filter(StreamFilterAdapter::new(
        compaction_filter.as_deref_mut(),
        opts,
        version,
        blobs_folder,
        &mut filter_blob_writer,
        &filter_ctx,
    ));

    let table_writer = super::flavour::prepare_table_writer(version, opts, payload)?;
    let mut compactor: Box<dyn CompactionFlavour> =
        Box::new(StandardCompaction::new(table_writer, tables_for_deletion));

    // Propagate range tombstones to this sub-range's output; the writer clips
    // them to each output table's key range (a boundary-spanning RT is written
    // clipped on both sides, which is correct).
    if !input_range_tombstones.is_empty() {
        compactor.write_range_tombstones(input_range_tombstones);
    }

    for (idx, item) in merge_iter.enumerate() {
        let item = item?;

        let io_bytes = (item.key.user_key.len() as u64).saturating_add(item.value.len() as u64);
        if opts
            .rate_limiter
            .request_interruptible(io_bytes, || opts.stop_signal.is_stopped())
        {
            log::debug!("Stopping sub-compaction because of stop signal (I/O throttle)");
            break;
        }

        compactor.write(item)?;

        if idx % 1_000_000 == 0 && opts.stop_signal.is_stopped() {
            log::debug!("Stopping sub-compaction because of stop signal");
            break;
        }
    }

    if let Some(filter) = compaction_filter {
        filter.finish();
    }

    let extra_blob_files = filter_blob_writer
        .map(BlobFileWriter::finish)
        .transpose()?
        .unwrap_or_default();

    compactor.produce(opts, dst_lvl, blob_frag_map, extra_blob_files)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "version_history_lock must be held across upgrade_version and maintenance"
)]
fn move_tables(
    compaction_state: &MutexGuard<'_, CompactionState>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<CompactionResult> {
    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
    let mut version_history_lock = opts.version_history.write().expect("lock is poisoned");

    // Fail-safe for buggy compaction strategies
    if compaction_state
        .hidden_set()
        .should_decline_compaction(payload.table_ids.iter().copied())
    {
        log::warn!(
            "Compaction task created by {:?} contained hidden tables, declining to run it - please report this at https://github.com/fjall-rs/lsm-tree/issues/new?template=bug_report.md",
            opts.strategy.get_name(),
        );
        return Ok(CompactionResult::nothing());
    }

    let table_count = payload.table_ids.len();
    let table_ids = payload.table_ids.iter().copied().collect::<Vec<_>>();

    version_history_lock.upgrade_version(
        &opts.config.path,
        |current| {
            let mut copy = current.clone();

            let ctx = crate::version::TransformContext::new(opts.config.comparator.as_ref());
            copy.version = copy
                .version
                .with_moved(&table_ids, payload.dest_level as usize, &ctx);

            Ok(copy)
        },
        &opts.global_seqno,
        &opts.visible_seqno,
        &*opts.config.fs,
        opts.runtime_config.load_full(),
        opts.encryption.clone(),
    )?;

    if let Err(e) = version_history_lock.maintenance(
        &opts.config.path,
        opts.mvcc_gc_watermark,
        &*opts.config.fs,
    ) {
        log::error!("Manifest maintenance failed: {e:?}");
        return Err(e);
    }

    Ok(CompactionResult {
        action: CompactionAction::Moved,
        dest_level: Some(payload.dest_level),
        tables_in: table_count,
        tables_out: table_count,
    })
}

/// Picks blob files to rewrite (defragment)
fn pick_blob_files_to_rewrite(
    picked_tables: &HashSet<TableId>,
    current_version: &Version,
    blob_opts: &crate::KvSeparationOptions,
) -> crate::Result<Vec<BlobFile>> {
    use crate::Table;

    // We start off by getting all the blob files that are referenced by the tables
    // that we want to compact.
    let linked_blob_files = picked_tables
        .iter()
        .map(|&id| {
            current_version.get_table(id).unwrap_or_else(|| {
                panic!("Table {id} should exist");
            })
        })
        .map(Table::list_blob_file_references)
        .collect::<Result<Vec<_>, _>>()?;

    // Then we filter all blob files that are not fragmented or old enough.
    let mut linked_blob_files = linked_blob_files
        .into_iter()
        .flatten()
        .flatten()
        .map(|blob_file_ref| {
            current_version
                .blob_files
                .get(blob_file_ref.blob_file_id)
                .unwrap_or_else(|| {
                    panic!("Blob file {} should exist", blob_file_ref.blob_file_id);
                })
        })
        .filter(|blob_file| {
            blob_file.is_stale(current_version.gc_stats(), blob_opts.staleness_threshold)
        })
        .filter(|blob_file| {
            // NOTE: Dead blob files are dropped anyway during current_version change commit
            !blob_file.is_dead(current_version.gc_stats())
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    linked_blob_files.sort_by_key(|a| a.id());

    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "precision loss and truncation are acceptable for cutoff calculation"
    )]
    let cutoff_point = {
        let len = linked_blob_files.len() as f32;
        (len * blob_opts.age_cutoff) as usize
    };
    linked_blob_files.drain(cutoff_point..);

    // IMPORTANT: Additionally, we also have to check if any other tables reference any of our candidate blob files.
    // We have to *not* include blob files that are referenced by other tables, because otherwise those
    // blob references would point into nothing (becoming dangling).
    for table in current_version.iter_tables() {
        if picked_tables.contains(&table.id()) {
            continue;
        }

        let other_refs = table
            .list_blob_file_references()?
            .unwrap_or_default()
            .into_iter()
            .filter(|x| linked_blob_files.iter().any(|bf| bf.id() == x.blob_file_id))
            .collect::<Vec<_>>();

        for additional_ref in other_refs {
            linked_blob_files.retain(|x| x.id() != additional_ref.blob_file_id);
        }
    }

    Ok(linked_blob_files.into_iter().cloned().collect::<Vec<_>>())
}

fn hidden_guard<T>(
    payload: &CompactionPayload,
    opts: &Options,
    f: impl FnOnce() -> crate::Result<T>,
) -> crate::Result<T> {
    f().inspect_err(|e| {
        log::error!("Compaction failed: {e:?}");

        // IMPORTANT: We need to show tables again on error
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut compaction_state = opts.compaction_state.lock().expect("lock is poisoned");

        compaction_state
            .hidden_set_mut()
            .show(payload.table_ids.iter().copied());
    })
}

#[expect(clippy::too_many_lines)]
fn merge_tables(
    mut compaction_state: MutexGuard<'_, CompactionState>,
    version_history_lock: RwLockReadGuard<'_, SuperVersions>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<CompactionResult> {
    if opts.stop_signal.is_stopped() {
        log::debug!("Stopping before compaction because of stop signal");
        return Ok(CompactionResult::nothing());
    }

    // Fail-safe for buggy compaction strategies
    if compaction_state
        .hidden_set()
        .should_decline_compaction(payload.table_ids.iter().copied())
    {
        log::warn!(
            "Compaction task created by {:?} contained hidden tables, declining to run it - please report this at https://github.com/fjall-rs/lsm-tree/issues/new?template=bug_report.md",
            opts.strategy.get_name(),
        );
        return Ok(CompactionResult::nothing());
    }

    let current_super_version = version_history_lock.latest_version();

    let Some(tables) = payload
        .table_ids
        .iter()
        .map(|&id| current_super_version.version.get_table(id).cloned())
        .collect::<Option<Vec<_>>>()
    else {
        log::warn!(
            "Compaction task created by {:?} contained tables not referenced in the level manifest",
            opts.strategy.get_name(),
        );
        return Ok(CompactionResult::nothing());
    };

    let tables_in = payload.table_ids.len();

    // Collect range tombstones from input tables before they are moved.
    // Canonicalize to avoid duplicate RTs across input tables (MultiWriter
    // rotation copies the same RT into every output table during flush).
    let mut input_range_tombstones: Vec<crate::range_tombstone::RangeTombstone> = tables
        .iter()
        .flat_map(|t| t.range_tombstones().iter().cloned())
        .collect();
    input_range_tombstones.sort();
    input_range_tombstones.dedup();

    // ---- Parallel sub-compaction (std only) ----
    // A non-relocating compaction can be split into disjoint key ranges that
    // compact in parallel — each writes its own SSTs, then all merge into one
    // atomic version edit. Active blob relocation stays single-threaded and
    // falls through to the serial path below.
    #[cfg(feature = "std")]
    {
        let threads = opts.config.compaction_threads;
        let dst_lvl: usize = payload.canonical_level.into();
        let is_last_level = payload.dest_level == opts.config.level_count - 1;

        // Only KV-separated trees with fragmented blob files relocate; that
        // path is not split here.
        let relocating = match &opts.config.kv_separation_opts {
            Some(blob_opts) => !pick_blob_files_to_rewrite(
                &payload.table_ids,
                &current_super_version.version,
                blob_opts,
            )?
            .is_empty(),
            None => false,
        };

        let total_input_bytes: u64 = tables.iter().map(Table::file_size).sum();

        let boundaries = if threads > 1
            && !relocating
            && total_input_bytes >= opts.config.subcompaction_min_bytes
        {
            subcompaction_boundaries(
                &current_super_version.version,
                payload.dest_level as usize,
                threads,
                &opts.config.comparator,
            )
        } else {
            Vec::new()
        };

        if !boundaries.is_empty() {
            let blobs_folder = opts.config.path.join(BLOBS_FOLDER);
            let ranges = ranges_from_boundaries(&boundaries);

            // Hand off: hide inputs, release the exclusive + version locks for
            // the CPU-heavy parallel phase.
            drop(version_history_lock);
            compaction_state
                .hidden_set_mut()
                .hide(payload.table_ids.iter().copied());
            drop(compaction_state);

            // First range runs on this thread; the rest on scoped workers. Only
            // the first carries the input tables to delete, so they are dropped
            // exactly once at install.
            let num_ranges = ranges.len();
            // `ranges_from_boundaries` always emits at least two ranges when
            // boundaries is non-empty (checked above), so split_first is Some.
            let Some((first_range, rest_ranges)) = ranges.split_first() else {
                unreachable!("non-empty boundaries yield >= 2 ranges");
            };

            let outputs: Vec<crate::Result<super::flavour::ProducedOutput>> =
                std::thread::scope(|scope| {
                    let version = &current_super_version.version;
                    let rts = input_range_tombstones.as_slice();
                    let blobs_dir = blobs_folder.as_path();

                    let handles: Vec<_> = rest_ranges
                        .iter()
                        .map(|range| {
                            let range = range.clone();
                            scope.spawn(move || {
                                run_subcompaction(
                                    opts,
                                    payload,
                                    version,
                                    Vec::new(),
                                    rts,
                                    range,
                                    dst_lvl,
                                    is_last_level,
                                    blobs_dir,
                                )
                            })
                        })
                        .collect();

                    let first = run_subcompaction(
                        opts,
                        payload,
                        version,
                        tables.clone(),
                        rts,
                        first_range.clone(),
                        dst_lvl,
                        is_last_level,
                        blobs_dir,
                    );

                    let mut outputs = Vec::with_capacity(num_ranges);
                    outputs.push(first);
                    for handle in handles {
                        outputs.push(handle.join().unwrap_or_else(|_| {
                            Err(crate::Error::Io(std::io::Error::other(
                                "sub-compaction worker panicked",
                            )))
                        }));
                    }
                    outputs
                });

            let outputs = outputs
                .into_iter()
                .collect::<crate::Result<Vec<_>>>()
                .inspect_err(|e| {
                    log::error!("Sub-compaction failed: {e:?}");
                    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
                    let mut state = opts.compaction_state.lock().expect("lock is poisoned");
                    state
                        .hidden_set_mut()
                        .show(payload.table_ids.iter().copied());
                })?;

            // Re-acquire locks and install one atomic version edit for all outputs.
            #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
            let mut compaction_state = opts.compaction_state.lock().expect("lock is poisoned");
            #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
            let mut version_history_lock = opts.version_history.write().expect("lock is poisoned");

            let tables_out =
                super::flavour::install_merge(&mut version_history_lock, opts, payload, outputs)
                    .inspect_err(|e| {
                        log::error!("Sub-compaction install failed: {e:?}");
                        compaction_state
                            .hidden_set_mut()
                            .show(payload.table_ids.iter().copied());
                    })?;

            compaction_state
                .hidden_set_mut()
                .show(payload.table_ids.iter().copied());

            version_history_lock
                .maintenance(&opts.config.path, opts.mvcc_gc_watermark, &*opts.config.fs)
                .inspect_err(|e| log::error!("Manifest maintenance failed: {e:?}"))?;

            drop(version_history_lock);
            drop(compaction_state);

            log::trace!("Parallel compaction done in {num_ranges} sub-ranges");

            return Ok(CompactionResult {
                action: CompactionAction::Merged,
                dest_level: Some(payload.dest_level),
                tables_in,
                tables_out,
            });
        }
    }

    let mut blob_frag_map = FragmentationMap::default();

    let Some(mut merge_iter) = create_compaction_stream(
        &current_super_version.version,
        &payload.table_ids.iter().copied().collect::<Vec<_>>(),
        opts.mvcc_gc_watermark,
        opts.config.merge_operator.clone(),
        opts.config.comparator.clone(),
    )?
    else {
        log::warn!(
            "Compaction task tried to compact tables that do not exist, declining to run it"
        );
        return Ok(CompactionResult::nothing());
    };

    let dst_lvl = payload.canonical_level.into();
    let is_last_level = payload.dest_level == opts.config.level_count - 1;

    merge_iter = merge_iter
        .evict_tombstones(is_last_level)
        .zero_seqnos(false);

    let blobs_folder = opts.config.path.join(BLOBS_FOLDER);

    let filter_ctx = Context { is_last_level };

    // Construct the compaction filter
    let mut compaction_filter = opts.config.compaction_filter_factory.as_ref().map(|f| {
        log::trace!("Installing custom compaction filter {:?}", f.name());
        f.make_filter(&filter_ctx)
    });

    // This is used by the compaction filter if it wants to write new blobs
    // TODO: the filter should really pipe new blobs into the compaction stream directly,
    // TODO: but that will probably require to change the protocol between filter <-> compaction stream a bit
    let mut filter_blob_writer = None;
    let mut merge_iter = merge_iter.with_filter(StreamFilterAdapter::new(
        compaction_filter.as_deref_mut(),
        opts,
        &current_super_version.version,
        &blobs_folder,
        &mut filter_blob_writer,
        &filter_ctx,
    ));

    let table_writer =
        super::flavour::prepare_table_writer(&current_super_version.version, opts, payload)?;

    let start = Instant::now();

    let mut compactor = match &opts.config.kv_separation_opts {
        Some(blob_opts) => {
            merge_iter = merge_iter.with_drop_callback(&mut blob_frag_map);

            let blob_files_to_rewrite = pick_blob_files_to_rewrite(
                &payload.table_ids,
                &current_super_version.version,
                blob_opts,
            )?;

            if blob_files_to_rewrite.is_empty() {
                log::debug!("No blob relocation needed");

                Box::new(StandardCompaction::new(table_writer, tables))
                    as Box<dyn super::flavour::CompactionFlavour>
            } else {
                log::debug!(
                    "Relocate blob files: {:?}",
                    blob_files_to_rewrite
                        .iter()
                        .map(BlobFile::id)
                        .collect::<Vec<_>>(),
                );

                let scanner = BlobFileMergeScanner::new(
                    blob_files_to_rewrite
                        .iter()
                        .map(|bf| BlobFileScanner::new(&bf.0.path, bf.id()))
                        .collect::<crate::Result<Vec<_>>>()?,
                );

                let writer = BlobFileWriter::new(
                    opts.blob_file_id_generator.clone(),
                    &blobs_folder,
                    opts.tree_id,
                    opts.config.descriptor_table.clone(),
                    opts.config.fs.clone(),
                )?
                .use_target_size(blob_opts.file_target_size)
                .use_passthrough_compression(blob_opts.compression)
                .use_sync_mode(opts.config.sync_mode);

                let inner = StandardCompaction::new(table_writer, tables);

                Box::new(RelocatingCompaction::new(
                    inner,
                    scanner.peekable(),
                    writer,
                    blob_files_to_rewrite,
                    opts.rate_limiter.clone(),
                    opts.stop_signal.clone(),
                ))
            }
        }
        None => Box::new(StandardCompaction::new(table_writer, tables)),
    };

    log::trace!("Blob file GC preparation done in {:?}", start.elapsed());

    drop(version_history_lock);

    {
        compaction_state
            .hidden_set_mut()
            .hide(payload.table_ids.iter().copied());
    }

    // IMPORTANT: Unlock exclusive compaction lock as we are now doing the actual (CPU-intensive) compaction
    drop(compaction_state);

    hidden_guard(payload, opts, || {
        // Propagate range tombstones to output tables BEFORE writing KV items,
        // so that if the compactor rotates tables during the merge loop,
        // earlier tables already carry the RT metadata.
        //
        // Keep RTs even at the last level until compaction itself becomes
        // RT-aware and can physically drop covered KVs. Dropping the RT first
        // would only remove the logical delete marker and can resurrect data.
        if !input_range_tombstones.is_empty() {
            log::debug!(
                "Propagating {} range tombstones to compaction output",
                input_range_tombstones.len(),
            );
            compactor.write_range_tombstones(&input_range_tombstones);
        }

        for (idx, item) in merge_iter.enumerate() {
            let item = item?;

            // Pace compaction I/O so it cannot saturate the device and
            // starve user reads. Short-circuits to a single relaxed atomic
            // load when `compaction_rate_limit` is 0 (the default), so the
            // unthrottled hot path stays cheap. The wait is interruptible by
            // the stop signal so a low limit plus a large item can't stall
            // tree drop / shutdown for the whole wait.
            //
            // Accounting here covers the SST entry's key + value bytes
            // (for KV-separated entries `item.value` is the encoded handle).
            // Each length is widened to u64 before the add, so there is no
            // intermediate usize sum; the saturating add only guards the
            // (practically impossible) u64 overflow. The relocated blob
            // payload of KV-separated compactions is debited separately at
            // its write site in `RelocatingCompaction::write`, where the
            // real moved bytes are known.
            let io_bytes = (item.key.user_key.len() as u64).saturating_add(item.value.len() as u64);
            if opts
                .rate_limiter
                .request_interruptible(io_bytes, || opts.stop_signal.is_stopped())
            {
                log::debug!("Stopping amidst compaction because of stop signal (I/O throttle)");
                return Ok(());
            }

            compactor.write(item)?;

            // NOTE: When stop_signal fires mid-merge, the loop exits early but
            // compaction proceeds to commit whatever was written so far. The
            // resulting CompactionResult will report `Merged` even though not
            // all input items were processed. This is pre-existing behavior:
            // partial merge output is valid and committed to the version history.
            if idx % 1_000_000 == 0 && opts.stop_signal.is_stopped() {
                log::debug!("Stopping amidst compaction because of stop signal");
                return Ok(());
            }
        }

        Ok(())
    })?;

    if let Some(filter) = compaction_filter {
        filter.finish();
    }

    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
    let mut compaction_state = opts.compaction_state.lock().expect("lock is poisoned");

    log::trace!("Acquiring super version write lock");
    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
    let mut version_history_lock = opts.version_history.write().expect("lock is poisoned");
    log::trace!("Acquired super version write lock");

    log::trace!("Blob fragmentation diff: {blob_frag_map:#?}");

    let extra_blob_files = filter_blob_writer
        .map(BlobFileWriter::finish)
        .transpose()
        .inspect_err(|e| {
            // NOTE: We cannot use hidden_guard here because we already locked the compaction state

            log::error!("Compaction failed while finishing filter blob writer: {e:?}");

            compaction_state
                .hidden_set_mut()
                .show(payload.table_ids.iter().copied());
        })?
        .unwrap_or_default();

    // Filter-created blob files are already finalized on disk; if `produce`
    // fails they would be orphaned (produce consumes the Vec, so keep a handle
    // to mark them deleted on the error path).
    let rollback_extra_blob_files = extra_blob_files.clone();

    // Phase split: `produce` finalizes this compaction's output files (no
    // version touch); `install_merge` commits one atomic version edit. With a
    // single output the result is identical to the old combined `finish`; the
    // split is what lets parallel sub-compactions each produce independently
    // and then share one install.
    let produce_output = compactor
        .produce(opts, dst_lvl, blob_frag_map, extra_blob_files)
        .inspect_err(|e| {
            // NOTE: We cannot use hidden_guard here because we already locked the compaction state

            log::error!("Compaction failed: {e:?}");

            compaction_state
                .hidden_set_mut()
                .show(payload.table_ids.iter().copied());

            for blob_file in &rollback_extra_blob_files {
                blob_file.mark_as_deleted();
            }
        })?;

    let tables_out = super::flavour::install_merge(
        &mut version_history_lock,
        opts,
        payload,
        vec![produce_output],
    )
    .inspect_err(|e| {
        // NOTE: We cannot use hidden_guard here because we already locked the compaction state

        log::error!("Compaction failed: {e:?}");

        compaction_state
            .hidden_set_mut()
            .show(payload.table_ids.iter().copied());
    })?;

    compaction_state
        .hidden_set_mut()
        .show(payload.table_ids.iter().copied());

    version_history_lock
        .maintenance(&opts.config.path, opts.mvcc_gc_watermark, &*opts.config.fs)
        .inspect_err(|e| {
            log::error!("Manifest maintenance failed: {e:?}");
        })?;

    drop(version_history_lock);
    drop(compaction_state);

    log::trace!("Compaction successful");

    Ok(CompactionResult {
        action: CompactionAction::Merged,
        dest_level: Some(payload.dest_level),
        tables_in,
        tables_out,
    })
}

fn drop_tables(
    compaction_state: MutexGuard<'_, CompactionState>,
    opts: &Options,
    ids_to_drop: &[TableId],
) -> crate::Result<CompactionResult> {
    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
    let mut version_history_lock = opts.version_history.write().expect("lock is poisoned");

    // Fail-safe for buggy compaction strategies
    if compaction_state
        .hidden_set()
        .should_decline_compaction(ids_to_drop.iter().copied())
    {
        log::warn!(
            "Compaction task created by {:?} contained hidden tables, declining to run it - please report this at https://github.com/fjall-rs/lsm-tree/issues/new?template=bug_report.md",
            opts.strategy.get_name(),
        );
        return Ok(CompactionResult::nothing());
    }

    let Some(tables) = ids_to_drop
        .iter()
        .map(|&id| {
            version_history_lock
                .latest_version()
                .version
                .get_table(id)
                .cloned()
        })
        .collect::<Option<Vec<_>>>()
    else {
        log::warn!(
            "Compaction task created by {:?} contained tables not referenced in the level manifest",
            opts.strategy.get_name(),
        );
        return Ok(CompactionResult::nothing());
    };

    log::debug!("Dropping tables: {ids_to_drop:?}");

    let mut dropped_blob_files = vec![];

    // IMPORTANT: Write the manifest with the removed tables first
    // Otherwise the table files are deleted, but are still referenced!
    version_history_lock.upgrade_version(
        &opts.config.path,
        |current| {
            let mut copy = current.clone();

            let ctx = crate::version::TransformContext::new(opts.config.comparator.as_ref());
            copy.version = copy
                .version
                .with_dropped(ids_to_drop, &mut dropped_blob_files, &ctx)?;

            Ok(copy)
        },
        &opts.global_seqno,
        &opts.visible_seqno,
        &*opts.config.fs,
        opts.runtime_config.load_full(),
        opts.encryption.clone(),
    )?;

    if let Err(e) = version_history_lock.maintenance(
        &opts.config.path,
        opts.mvcc_gc_watermark,
        &*opts.config.fs,
    ) {
        log::error!("Manifest maintenance failed: {e:?}");
        return Err(e);
    }

    drop(version_history_lock);

    // NOTE: If the application were to crash >here< it's fine
    // The tables are not referenced anymore, and will be
    // cleaned up upon recovery
    for table in tables {
        table.mark_as_deleted();
    }

    for blob_file in dropped_blob_files {
        blob_file.mark_as_deleted();
    }

    let tables_dropped = ids_to_drop.len();

    drop(compaction_state);

    log::trace!("Dropped {tables_dropped} tables");

    Ok(CompactionResult {
        action: CompactionAction::Dropped,
        dest_level: None,
        tables_in: tables_dropped,
        tables_out: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::{create_compaction_stream, pick_run_indexes};
    use crate::{
        AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter, TableId,
        compaction::{Choice, CompactionStrategy, Input, state::CompactionState},
        config::BlockSizePolicy,
        version::Version,
    };
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn compaction_stream_run_not_found() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        assert!(
            create_compaction_stream(
                &tree.current_version(),
                &[666],
                0,
                None,
                crate::comparator::default_comparator()
            )?
            .is_none()
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn compaction_stream_run() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("c", "c", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(
            Some((0, 2)),
            pick_run_indexes(
                tree.current_version()
                    .level(0)
                    .unwrap()
                    .iter()
                    .next()
                    .unwrap(),
                &[0, 1, 2],
            )
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn compaction_stream_run_2() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("c", "c", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(
            Some((0, 0)),
            pick_run_indexes(
                tree.current_version()
                    .level(0)
                    .unwrap()
                    .iter()
                    .next()
                    .unwrap(),
                &[0],
            )
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn compaction_stream_run_3() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("c", "c", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(
            Some((2, 2)),
            pick_run_indexes(
                tree.current_version()
                    .level(0)
                    .unwrap()
                    .iter()
                    .next()
                    .unwrap(),
                &[2],
            )
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn compaction_stream_run_4() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("c", "c", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(
            None,
            pick_run_indexes(
                tree.current_version()
                    .level(0)
                    .unwrap()
                    .iter()
                    .next()
                    .unwrap(),
                &[4],
            )
        );

        Ok(())
    }

    #[test]
    fn compaction_drop_tables() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.approximate_len());
        assert_eq!(0, tree.sealed_memtable_count());

        tree.insert("b", "a", 1);
        tree.flush_active_memtable(0)?;
        assert_eq!(2, tree.approximate_len());
        assert_eq!(0, tree.sealed_memtable_count());

        tree.insert("c", "a", 2);
        tree.flush_active_memtable(0)?;
        assert_eq!(3, tree.approximate_len());
        assert_eq!(0, tree.sealed_memtable_count());

        tree.compact(Arc::new(crate::compaction::Fifo::new(1, None)), 3)?;

        assert_eq!(0, tree.table_count());

        Ok(())
    }

    #[test]
    fn blob_file_picking_simple() -> crate::Result<()> {
        struct InPlaceStrategy(Vec<TableId>);

        impl CompactionStrategy for InPlaceStrategy {
            fn get_name(&self) -> &'static str {
                "InPlaceCompaction"
            }

            fn choose(&self, _: &Version, _: &Config, _: &CompactionState) -> Choice {
                Choice::Merge(Input {
                    table_ids: self.0.iter().copied().collect(),
                    dest_level: 6,
                    target_size: 64_000_000,
                    canonical_level: 6, // We don't really care - this compaction is only used for very specific unit tests
                })
            }
        }

        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_size_policy(BlockSizePolicy::all(1))
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(1)
                .age_cutoff(1.0)
                .staleness_threshold(0.01)
                .compression(crate::CompressionType::None),
        ))
        .open()?;

        tree.insert("a", "a", 0);
        tree.insert("b", "b", 0);
        tree.insert("c", "c", 0);
        tree.flush_active_memtable(1_000)?;
        assert_eq!(0, tree.sealed_memtable_count());
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.major_compact(1, 1_000)?;
        assert_eq!(3, tree.table_count());
        assert_eq!(1, tree.blob_file_count());
        // We now have tables [1, 2, 3] pointing into blob file 0

        tree.drop_range("a"..="a")?;
        assert_eq!(2, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        {
            assert_eq!(
                &{
                    let mut map = crate::HashMap::default();
                    map.insert(0, crate::blob_tree::FragmentationEntry::new(1, 1, 1));
                    map
                },
                &**tree.current_version().gc_stats(),
            );
        }

        // Even though we are compacting table #2, blob file is not rewritten
        // because table #3 still points into it
        tree.compact(Arc::new(InPlaceStrategy(vec![2])), 1_000)?;
        assert_eq!(2, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        {
            assert_eq!(
                &{
                    let mut map = crate::HashMap::default();
                    map.insert(0, crate::blob_tree::FragmentationEntry::new(1, 1, 1));
                    map
                },
                &**tree.current_version().gc_stats(),
            );
        }

        // Because tables #3 & #4 both point into the blob file
        // Only selecting both for compaction will actually rewrite the file
        tree.compact(Arc::new(InPlaceStrategy(vec![3, 4])), 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        // Fragmentation is cleared up because blob file was relocated
        {
            assert_eq!(
                crate::HashMap::default(),
                **tree.current_version().gc_stats(),
            );
        }

        Ok(())
    }
}
