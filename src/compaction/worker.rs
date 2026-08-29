// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::{CompactionAction, CompactionResult, CompactionStrategy, Input as CompactionPayload};
use crate::time::Instant;
use crate::tree::inner::{CompactionGuard, VersionsReadGuard};
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
// `BlobFileId` only appears in the std-only parallel sub-compaction + tight-space
// relocation paths; gate the import so the no_std build does not see it unused.
#[cfg(feature = "std")]
use crate::vlog::BlobFileId;
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};
// no-std: spin mirrors parking_lot's Mutex/RwLock API without an allocator.
#[cfg(feature = "std")]
use parking_lot::{Mutex, RwLock};
#[cfg(not(feature = "std"))]
use spin::{Mutex, RwLock};

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
#[derive(Clone)]
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

    /// The tree-wide deletion pause, installed on every compaction-produced table
    /// before it becomes visible. A flush registers it via `register_tables`, but
    /// compaction installs its outputs directly (`install_merge` / tight-space),
    /// so it must be threaded here — without it a Page-ECC output's in-place heal
    /// skips the checkpoint mutation window and can race a checkpoint hard-link.
    pub deletion_pause: Arc<crate::deletion_pause::DeletionPause>,

    /// The tree-wide ECC heal-hint sink, installed on every
    /// compaction-produced table alongside the deletion pause and for the
    /// same reason: a flush registers it via `register_tables`, but
    /// compaction installs its outputs directly. Without it a
    /// confirmed-persistent ECC correction while reading such an output
    /// corrects the block in memory yet can never queue the SST for a
    /// healing rewrite, so the bitrot stays on disk indefinitely.
    pub heal_hints: Arc<crate::heal_hints::HealHints>,

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
            deletion_pause: tree.deletion_pause.clone(),
            heal_hints: tree.heal_hints.clone(),
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
    let compaction_state = opts.compaction_state.lock();

    let version_history_lock = opts.version_history.read();

    let start = Instant::now();
    log::trace!(
        "Consulting compaction strategy {:?}",
        opts.strategy.get_name(),
    );
    // Structural decision plus the runtime-config-driven density-rewrite
    // fallback (applied here, where the live runtime config lives, so the
    // structural strategies stay free of it).
    let runtime_config = opts.runtime_config.load_full();
    let choice = super::choose_with_density_rewrite(
        &*opts.strategy,
        &version_history_lock.latest_version().version,
        &opts.config,
        &runtime_config,
        &compaction_state,
    );

    log::debug!("Compaction choice: {choice:?} in {:?}", start.elapsed());

    match choice {
        Choice::Merge(payload) => {
            // Space-admission gate (opt-in). A merge transiently needs room for
            // its output while the inputs still exist, so on a near-full disk a
            // naive run can hit ENOSPC. Gating merges like user writes would
            // deadlock (compaction is what frees space), so the gate keeps an
            // emergency reserve and narrows rather than blanket-skips. `Move` /
            // `Drop` are zero / negative space and always run (below).
            let decision = {
                let super_version = version_history_lock.latest_version();
                space_gate_for_merge(&super_version.version, opts, &payload)?
            };
            match decision {
                SpaceGate::Run => {
                    merge_tables(compaction_state, version_history_lock, opts, &payload)
                }
                SpaceGate::Narrowed(narrowed) => {
                    log::debug!(
                        "Compaction space gate: narrowed merge from {} to {} tables to fit free space",
                        payload.table_ids.len(),
                        narrowed.table_ids.len(),
                    );
                    merge_tables(compaction_state, version_history_lock, opts, &narrowed)
                }
                SpaceGate::Skip => {
                    #[cfg(feature = "std")]
                    if opts.runtime_config.load().tight_space_compaction {
                        return run_tight_space_compaction(
                            compaction_state,
                            version_history_lock,
                            opts,
                            &payload,
                        );
                    }
                    log::info!(
                        "Compaction space gate: skipping {}-table merge — free space cannot cover the transient output and no fitting subset exists (opt-in tight-space reclaim handles this)",
                        payload.table_ids.len(),
                    );
                    Ok(CompactionResult::nothing())
                }
            }
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

/// Outcome of the compaction space-admission gate for a [`Choice::Merge`].
enum SpaceGate {
    /// Run the merge as chosen (admission off, backend can't report free space,
    /// or the inputs fit the available space).
    Run,
    /// Run a smaller fitting subset (a minimal reclaiming merge) instead of the
    /// chosen one, which did not fit.
    Narrowed(CompactionPayload),
    /// No fitting subset exists; skip this cycle. The truly-too-big single merge
    /// is the opt-in tight-space mode's domain; meanwhile `Drop` / `Move` cycles
    /// still reclaim and reorganize.
    Skip,
}

/// The two-layer space model shared by the compaction gate and the
/// `storage_stats` forward-looking status, so the reported availability matches
/// what the gate will actually admit.
///
/// **Layer 1 (logical partition quota).** The configured `storage_limit_bytes`
/// caps the tree's total footprint regardless of which volume the bytes land on.
/// `quota_headroom` is `max(0, limit - used)`, or `u64::MAX` when no quota is
/// set. The total new bytes (`sst_bytes + blob_bytes`) must fit it.
///
/// **Layer 2 (physical free space, per destination volume).** SST output lands
/// in `sst_dest_level`'s volume; KV-separated blob relocation lands in the
/// primary blobs volume. The two are budgeted **independently only when they are
/// proven to be on different physical volumes** ([`Fs::volume_id`](crate::fs::Fs::volume_id) reports
/// distinct device ids) — then a full cold-tier route never stalls a hot-level
/// merge. Otherwise (same volume, or independence not proven) their transient
/// peak is the **combined sum** on the tighter volume: checking them
/// independently would over-admit, since each fits the free space alone while
/// together they exhaust it and the merge hits `ENOSPC`. `level_routes` maps
/// levels to paths but does NOT prove a separate mount (a route may point at the
/// same filesystem), so routing alone is not treated as independence. Each
/// volume leaves the reserved flush floor when ample, or consumes it (raw free)
/// to break the no-space-to-free-space deadlock.
///
/// A volume that cannot report free space contributes `u64::MAX`, so a probe
/// failure never fabricates disk pressure. With no quota and every volume
/// unbounded, the result is unconditionally `true` (nothing constrains).
/// Whether two paths sit on PROVABLY separate free-space pools: both backends
/// report a volume id AND the ids differ.
///
/// The unproven cases (a backend that cannot answer, a route to the same mount)
/// deliberately answer `false` — shared. Every caller uses this to decide
/// whether space freed on one path relieves the other, and guessing
/// "independent" there admits a transient peak straight into `ENOSPC` or a
/// reclaim that lands where nothing needed it.
fn separate_volumes(
    a_fs: &dyn crate::fs::Fs,
    a_path: &crate::path::Path,
    b_fs: &dyn crate::fs::Fs,
    b_path: &crate::path::Path,
) -> bool {
    match (a_fs.volume_id(a_path), b_fs.volume_id(b_path)) {
        (Some(a), Some(b)) => a != b,
        _ => false,
    }
}

// `pub` (not `pub(crate)`) inside this crate-private module: clippy flags the
// redundant restriction since the module itself is already crate-scoped.
pub fn space_fits_two_layer(
    config: &Config,
    quota_headroom: u64,
    sst_bytes: u64,
    sst_dest_level: u8,
    blob_bytes: u64,
) -> bool {
    const RESERVE: u64 = crate::tree::MIN_RESERVED_HEADROOM;

    // Layer 1: logical partition quota on the total new bytes. The sum of two
    // on-disk byte counts is bounded by filesystem capacity and cannot overflow.
    if sst_bytes + blob_bytes > quota_headroom {
        return false;
    }

    // Layer 2: physical free space per destination volume. A volume's demand
    // fits when it leaves the reserved flush floor (ample) or, to break the
    // no-space-to-free-space deadlock, when it fits raw free (emergency,
    // consuming the reserve). `>= RESERVE` guards the subtraction.
    let volume_fits = |demand: u64, free: u64| -> bool {
        (free >= RESERVE && demand <= free - RESERVE) || demand <= free
    };

    let (sst_path, sst_fs) = config.tables_folder_for_level(sst_dest_level);
    let sst_free = sst_fs.available_space(&sst_path).unwrap_or(u64::MAX);
    let blob_dir = config.path.join(BLOBS_FOLDER);
    let blob_free = config.fs.available_space(&blob_dir).unwrap_or(u64::MAX);

    let independent = separate_volumes(&*sst_fs, &sst_path, &*config.fs, &blob_dir);

    if independent {
        volume_fits(sst_bytes, sst_free) && volume_fits(blob_bytes, blob_free)
    } else {
        // Same (or unproven-distinct) volume: the transient peak is the combined
        // sum against the tighter free probe.
        volume_fits(sst_bytes + blob_bytes, sst_free.min(blob_free))
    }
}

/// Decides whether a chosen merge fits the available space, narrowing it to a
/// minimal reclaiming subset when it does not.
///
/// The merge's transient output is checked through [`space_fits_two_layer`]: the
/// logical partition quota bounds the total new bytes, and the physical free
/// space is checked per destination volume (SST output volume + primary blob
/// volume, combined onto one budget when they share a filesystem).
///
/// The SST bound is `Σ input live_file_size` (a tight-space-restricted input
/// contributes only its live suffix — the punched prefix is superseded and never
/// rewritten); the blob bound is the physical on-disk
/// size of the stale blob files selected for relocation. This is a best-effort
/// estimate: a merge operator / compaction filter that *grows* values can still
/// write more than the inputs, so a mid-merge `ENOSPC` remains possible — that
/// case is handled by the atomic-commit guarantee (orphan output, intact inputs,
/// unchanged manifest, retried later), not by this gate. The gate's job is to
/// keep the common doomed merge from starting.
///
/// - Admission off → [`SpaceGate::Run`].
/// - The chosen merge fits both layers → [`SpaceGate::Run`].
/// - It does not fit, but a run-adjacent pair does → [`SpaceGate::Narrowed`].
///   Candidates are tried in ascending SST size; a larger pair with fewer stale
///   blob rewrites can fit where the smallest does not, so each is re-checked
///   through the full two-layer budget (its own relocation set included).
/// - No fitting subset exists → [`SpaceGate::Skip`].
fn space_gate_for_merge(
    version: &Version,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<SpaceGate> {
    let rc = opts.runtime_config.load_full();
    if !rc.storage_admission_check {
        return Ok(SpaceGate::Run);
    }

    // Layer 1 headroom (tree-wide). `max(0, limit - used)`: an operator quota set
    // below the live footprint leaves zero headroom — the clamp-to-zero is the
    // intended min-semantics (not masking). The footprint stat is propagated, not
    // swallowed (an undercounted `used` would overstate the headroom).
    let quota_headroom = match rc.storage_limit_bytes {
        Some(limit) => limit.saturating_sub(crate::storage_stats::compute_used_bytes(version)?),
        None => u64::MAX,
    };

    // Per-payload transient demand. Recomputed per payload because narrowing
    // changes both the SST input set AND (via `pick_blob_files_to_rewrite` on the
    // actual payload) the stale blob relocation set, so a narrowed merge can fit
    // where the full one did not. Stat failures propagate (a silent 0 would
    // undercount and admit the very rewrite this gate must block).
    let fits = |p: &CompactionPayload| -> crate::Result<bool> {
        // SST input bound (output never exceeds the inputs for a pure compaction).
        let sst_sigma: u64 = p
            .table_ids
            .iter()
            .filter_map(|&id| version.get_table(id))
            .try_fold(0u64, |acc, t| t.live_file_size().map(|size| acc + size))?;
        let blob_sigma: u64 = match &opts.config.kv_separation_opts {
            Some(blob_opts) => pick_blob_files_to_rewrite(&p.table_ids, version, blob_opts)?
                .iter()
                .try_fold(0u64, |acc, bf| bf.physical_size().map(|size| acc + size))?,
            None => 0,
        };
        Ok(space_fits_two_layer(
            &opts.config,
            quota_headroom,
            sst_sigma,
            p.dest_level,
            blob_sigma,
        ))
    };

    if fits(payload)? {
        return Ok(SpaceGate::Run);
    }

    // Constrained: try the run-adjacent narrowing candidates in ascending SST
    // size and run the first that fits the full two-layer budget. Skip only when
    // none fit (a truly-too-big single merge is the opt-in tight-space domain).
    for narrowed in narrow_merge_candidates(version, payload)? {
        if fits(&narrowed)? {
            return Ok(SpaceGate::Narrowed(narrowed));
        }
    }
    Ok(SpaceGate::Skip)
}

/// The run-adjacent table-pair narrowing candidates for a too-large merge,
/// sorted by combined SST `live_file_size` ascending (the same measure the gate
/// charges, so a restricted input's superseded prefix does not distort the
/// order), each preserving the merge's destination.
///
/// Narrowing is only safe within a single run (key-sorted, non-overlapping
/// tables): a run-adjacent pair is always a valid, reclaiming merge, and the
/// merge stream culls exactly that pair. A cross-run / cross-level payload (e.g.
/// a leveled `Lₙ → Lₙ₊₁` overlap set) cannot be narrowed here without risking an
/// invalid partial merge, so it yields no candidates (the chosen merge is
/// skipped and left to the opt-in tight-space mode). The pair must be adjacent
/// IN THE RUN — a gap would make the merge stream pull the in-between tables
/// too, breaking the size bound.
///
/// The caller tries the candidates in order and runs the first that fits the
/// full space gate: the smallest-SST pair can still fail on its stale blob
/// relocation set or the combined single-volume budget, while a slightly larger
/// pair with fewer blob rewrites fits — ranking by SST size alone and stopping at
/// the first would wrongly skip the merge.
fn narrow_merge_candidates(
    version: &Version,
    payload: &CompactionPayload,
) -> crate::Result<Vec<CompactionPayload>> {
    // The single run that holds every payload table (if any).
    let Some(run) = version
        .iter_levels()
        .flat_map(|level| level.iter())
        .find(|run| {
            payload
                .table_ids
                .iter()
                .all(|id| run.iter().any(|t| t.id() == *id))
        })
    else {
        return Ok(Vec::new());
    };

    let run_tables: Vec<&Table> = run.iter().collect();
    let mut candidates: Vec<(u64, CompactionPayload)> = Vec::new();
    for pair in run_tables.windows(2) {
        let [a, b] = pair else { continue };
        if !payload.table_ids.contains(&a.id()) || !payload.table_ids.contains(&b.id()) {
            continue;
        }
        // Two on-disk file sizes; the sum is bounded by filesystem capacity and
        // cannot overflow u64.
        let combined = a.live_file_size()? + b.live_file_size()?;
        candidates.push((
            combined,
            CompactionPayload {
                table_ids: [a.id(), b.id()].into_iter().collect(),
                dest_level: payload.dest_level,
                canonical_level: payload.canonical_level,
                target_size: payload.target_size,
            },
        ));
    }
    candidates.sort_by_key(|(combined, _)| *combined);
    Ok(candidates.into_iter().map(|(_, p)| p).collect())
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
    to_compact: &HashSet<TableId>,
    bounds: (core::ops::Bound<UserKey>, core::ops::Bound<UserKey>),
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

/// Sorts the destination-level max-keys, drops comparator-equal duplicates, and
/// removes the global maximum (splitting after it yields an empty trailing
/// range), returning the candidate interior boundary keys. Split out of
/// [`subcompaction_boundaries`] so the comparator-equality dedup is unit
/// testable without constructing a full [`Version`].
/// Gathers every range tombstone in the version (all levels), to gate
/// bottommost seqno-zeroing: a key covered by any tombstone keeps its real
/// seqno. Includes tombstones from tables NOT in the current compaction, so
/// "beyond output level" coverage is respected.
fn collect_version_tombstones(version: &Version) -> Vec<crate::range_tombstone::RangeTombstone> {
    version
        .iter_levels()
        .flat_map(|level| level.iter())
        .flat_map(|run| run.iter())
        .flat_map(|t| t.range_tombstones().iter().cloned())
        .collect()
}

/// Garbage-collects range tombstones for a bottommost compaction's output.
///
/// A tombstone at or below the watermark has been fully applied (every live
/// snapshot sees it, and this compaction physically dropped the keys it covers),
/// so it can be dropped — UNLESS a table outside this compaction overlaps its
/// range and might still hold a covered key, in which case dropping it would
/// resurrect that key. Tombstones above the watermark are always kept (read-time
/// application still needs them). Non-bottommost compactions keep everything.
#[cfg(feature = "std")]
fn range_tombstones_after_gc(
    input_rts: &[crate::range_tombstone::RangeTombstone],
    version: &Version,
    input_ids: &HashSet<TableId>,
    watermark: SeqNo,
    is_last_level: bool,
    comparator: &crate::comparator::SharedComparator,
) -> Vec<crate::range_tombstone::RangeTombstone> {
    if !is_last_level {
        return input_rts.to_vec();
    }
    let cmp = comparator.as_ref();
    input_rts
        .iter()
        .filter(|rt| {
            // Strict visibility: a tombstone at or above the watermark is still
            // needed by the oldest live snapshot (which reads at the watermark
            // and does not see `RT@watermark`), so keep it. Only strictly-below
            // tombstones are candidates for GC.
            if !rt.visible_at(watermark) {
                return true;
            }
            version
                .iter_levels()
                .flat_map(|level| level.iter())
                .flat_map(|run| run.iter())
                .filter(|t| !input_ids.contains(&t.id()))
                .any(|t| {
                    let kr = &t.metadata.key_range;
                    // [rt.start, rt.end) overlaps [kr.min, kr.max].
                    cmp.compare(&rt.start, kr.max()) != core::cmp::Ordering::Greater
                        && cmp.compare(kr.min(), &rt.end) == core::cmp::Ordering::Less
                })
        })
        .cloned()
        .collect()
}

#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "parallel sub-compaction boundary picker; the threaded parallel path is std-gated, so unused under no_std"
    )
)]
fn boundary_candidates(
    mut keys: Vec<UserKey>,
    comparator: &crate::comparator::SharedComparator,
) -> Vec<UserKey> {
    if keys.len() < 2 {
        return Vec::new();
    }
    keys.sort_by(|a, b| comparator.compare(a, b));
    // Dedup under the configured comparator, not raw bytes: a custom comparator
    // can rank two byte-distinct keys as equal, and a leftover equal cut point
    // would make adjacent sub-compaction ranges overlap or gap.
    keys.dedup_by(|a, b| comparator.compare(a, b).is_eq());
    keys.pop();
    keys
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
    let keys: Vec<UserKey> = level
        .iter()
        .flat_map(|run| run.iter())
        .map(|t| t.metadata.key_range.max().clone())
        .collect();
    let keys = boundary_candidates(keys, comparator);
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
) -> Vec<(core::ops::Bound<UserKey>, core::ops::Bound<UserKey>)> {
    use core::ops::Bound::{Excluded, Included, Unbounded};
    let mut ranges = Vec::with_capacity(boundaries.len() + 1);
    let mut lo = Unbounded;
    for b in boundaries {
        ranges.push((lo.clone(), Excluded(b.clone())));
        lo = Included(b.clone());
    }
    ranges.push((lo, Unbounded));
    ranges
}

/// Error returned when a sub-compaction is interrupted by the stop signal, so
/// the parallel caller re-shows the inputs and skips the atomic install instead
/// of committing a truncated sub-range.
#[cfg(feature = "std")]
fn cancelled_compaction() -> crate::Error {
    crate::Error::from(crate::io::Error::new(
        crate::io::ErrorKind::Interrupted,
        "sub-compaction cancelled by stop signal",
    ))
}

/// Key-range boundaries that split the combined data of `inputs` into slices of
/// roughly `slice_budget` bytes each. Block end keys from every input are merged
/// in comparator order and a boundary is emitted each time the accumulated block
/// size crosses the budget. The global maximum end key is never a boundary (its
/// keys are the rewrite's tail, handled by the final removal), so a set of
/// single-block inputs yields no boundaries (nothing to reclaim incrementally).
#[cfg(feature = "std")]
fn tight_slice_boundaries(
    inputs: &[Table],
    slice_budget: u64,
    cmp: &dyn crate::comparator::UserComparator,
) -> crate::Result<Vec<UserKey>> {
    use crate::table::block_index::BlockIndex;

    let mut entries: Vec<(UserKey, u32)> = Vec::new();
    for input in inputs {
        for handle in input.block_index.iter() {
            let handle = handle?;
            entries.push((handle.end_key().clone(), handle.size()));
        }
    }
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    entries.sort_by(|a, b| cmp.compare(&a.0, &b.0));
    let global_max = entries.last().map(|(k, _)| k.clone());

    let mut boundaries = Vec::new();
    let mut acc = 0u64;
    for (end_key, size) in &entries {
        // `size` is a u32 block length and `acc` resets each boundary; the
        // running sum is bounded by the inputs' on-disk size, so a plain add
        // cannot overflow u64.
        acc += u64::from(*size);
        if acc >= slice_budget
            && global_max.as_ref() != Some(end_key)
            && boundaries.last() != Some(end_key)
        {
            boundaries.push(end_key.clone());
            acc = 0;
        }
    }
    Ok(boundaries)
}

/// Builds a per-slice compaction payload over the still-surviving input ids,
/// inheriting the original payload's level / target fields.
#[cfg(feature = "std")]
fn slice_payload_for(views: &[Table], payload: &CompactionPayload) -> CompactionPayload {
    CompactionPayload {
        table_ids: views.iter().map(Table::id).collect(),
        dest_level: payload.dest_level,
        canonical_level: payload.canonical_level,
        target_size: payload.target_size,
    }
}

/// Opt-in tight-space compaction: rewrites the merge's inputs in key-range
/// slices, installing each slice as one durable version edit and reclaiming each
/// consumed input prefix via hole punching once the prior view drains, so the
/// peak transient footprint is one slice rather than the whole rewrite. Engaged
/// from [`do_compaction`] when the space gate finds no fitting merge and the tree
/// opts in.
///
/// Handles single- and multi-input merges, including KV-separated trees. Each
/// slice merges the surviving inputs over `[lower, boundary)`; an input whose
/// data extends past the boundary is re-opened as a restricted view (clamped +
/// prefix punched), while one fully consumed by the slice is dropped outright.
/// Blob fragmentation each slice produces is folded into the running GC stats so
/// dead blob files are dropped at the final removal (a blob may still be
/// referenced by an unprocessed slice, so per-slice dropping would be unsafe).
#[cfg(feature = "std")]
fn run_tight_space_compaction(
    mut compaction_state: CompactionGuard<'_>,
    version_history_lock: VersionsReadGuard<'_>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<CompactionResult> {
    use core::ops::Bound;

    // Capability gate: the destination volume must support hole punching.
    let (dest_path, dest_fs) = opts.config.tables_folder_for_level(payload.dest_level);
    if !dest_fs.capabilities(&dest_path).punch_hole {
        log::info!("Tight-space compaction unavailable: backend lacks punch_hole");
        return Ok(CompactionResult::nothing());
    }

    // A reclaim a checkpoint's window forced the deletion pause to retain is
    // space this very compaction is short of, so collect it before deciding
    // anything: the checkpoint that blocked it has usually released by now.
    opts.deletion_pause.retry_pending_reclaims();

    let latest = version_history_lock.latest_version();
    let Some(inputs) = payload
        .table_ids
        .iter()
        .map(|&id| latest.version.get_table(id).cloned())
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(CompactionResult::nothing());
    };

    // KV-separated trees: identify the stale (fragmented) blob files this merge
    // would relocate. When non-empty the slice loop runs a RELOCATING tight
    // compaction (blob defrag) — moving live entries into a fresh compact file
    // and punching each stale file's consumed prefix per slice. We keep only the
    // IDs, not the `BlobFile` handles: retaining an Arc would pin the stale
    // `Inner` and block its punch-on-drop. Each slice re-fetches the current
    // handle from the version.
    let (stale_ids, stale_total_bytes): (Vec<BlobFileId>, u64) =
        match &opts.config.kv_separation_opts {
            Some(blob_opts) => {
                let picked: HashSet<TableId> = payload.table_ids.iter().copied().collect();
                let files = pick_blob_files_to_rewrite(&picked, &latest.version, blob_opts)?;
                // Sum of on-disk blob-file sizes; bounded by filesystem capacity,
                // so it cannot overflow u64 (matches the gate's `acc + size`).
                let mut total = 0u64;
                for bf in &files {
                    total += bf.physical_size()?;
                }
                // Each stale file is punched through its OWN backend, which a
                // separately backed `blobs/` can refuse.
                if let Some(bf) = files
                    .iter()
                    .find(|bf| !bf.0.fs.capabilities(&bf.0.path).punch_hole)
                {
                    log::info!(
                        "Tight-space compaction unavailable: the backend of blob file {} \
                         lacks punch_hole, so its relocated prefix would stay allocated",
                        bf.0.path.display(),
                    );
                    return Ok(CompactionResult::nothing());
                }
                (files.iter().map(BlobFile::id).collect(), total)
            }
            None => (Vec::new(), 0),
        };
    let relocating = !stale_ids.is_empty();

    // The reclaim runs through each INPUT's own backend, not the destination's:
    // under level routing an input can sit on a volume that refuses the punch.
    // Admitting the slice anyway commits its restriction and writes its output
    // while the consumed prefix stays allocated, so the pass ends using MORE
    // space on the volume it was meant to relieve.
    if let Some(t) = inputs
        .iter()
        .find(|t| !t.fs.capabilities(&t.path).punch_hole)
    {
        log::info!(
            "Tight-space compaction unavailable: the backend of input table {} lacks \
             punch_hole, so its consumed prefix would stay allocated",
            t.path.display(),
        );
        return Ok(CompactionResult::nothing());
    }

    // Being able to punch is not the same as punching where it helps. Each
    // slice adds its WHOLE output to the destination volume but frees only the
    // share of its inputs that lives THERE, so a destination under pressure
    // with inputs on another volume grows monotonically across slices: the pass
    // commits several, hits ENOSPC, and never relieves the pool that engaged
    // it. When the destination can hold the whole merge it is not the
    // constrained pool (the gate skipped for the blob volume instead) and the
    // reclaim landing elsewhere is fine.
    let inputs_total: u64 = inputs.iter().map(Table::file_size).sum();
    let dest_free = dest_fs.available_space(&dest_path).unwrap_or(u64::MAX);
    if dest_free < inputs_total {
        // Only a PROVEN different volume forfeits the relief; an unproven pair
        // counts as shared, leaving the single-volume default untouched.
        //
        // Some inputs elsewhere is not the same as no relief. The output is
        // bounded by the inputs, and each slice frees the DESTINATION-local
        // share as it goes, so the volume only has to absorb the remote share:
        // 90 MB local plus 10 MB remote completes with 20 MB free. Requiring
        // every input to be colocated would leave a constrained tree read-only
        // with the reclaimable bytes sitting on the very volume that is full.
        let remote_total: u64 = inputs
            .iter()
            .filter(|t| separate_volumes(&*t.fs, &t.path, &*dest_fs, &dest_path))
            .map(Table::file_size)
            .sum();
        if dest_free < remote_total {
            log::info!(
                "Tight-space compaction unavailable: {remote_total} bytes of input live on a \
                 different volume than the constrained destination and only {dest_free} bytes \
                 are free there, so the slices would grow it without relief",
            );
            return Ok(CompactionResult::nothing());
        }
    }

    drop(latest);
    drop(version_history_lock);
    if inputs.is_empty() {
        return Ok(CompactionResult::nothing());
    }
    let tables_in = inputs.len();

    let comparator = opts.config.comparator.clone();
    // Slice boundaries are derived from SST block sizes, but on a KV-separated
    // relocating merge the transient is dominated by the RELOCATED blob payload,
    // not the (tiny, handle-only) SSTs. Scale the SST-space budget so a slice
    // covering `b` SST bytes relocates roughly `b * stale_total / inputs_total`
    // blob bytes ≈ the free space: without this, a few small SST blocks would map
    // to the whole blob payload in one slice and overflow the very disk the gate
    // already flagged as too tight.
    //
    // That payload lands on the BLOB volume, which level routing can separate
    // from the SST destination. Budgeting it against the destination's free
    // space then scales by the wrong pool in both directions: an ample
    // destination beside a full `blobs/` yields a budget larger than every
    // input block (`tight_slice_boundaries` returns empty, so no reclaim
    // happens at all) or a slice the blob volume cannot hold.
    let slice_budget = if relocating && stale_total_bytes > 0 {
        let blob_dir = opts.config.path.join(BLOBS_FOLDER);
        let blob_free = opts
            .config
            .fs
            .available_space(&blob_dir)
            .unwrap_or(u64::MAX);
        let independent = separate_volumes(&*dest_fs, &dest_path, &*opts.config.fs, &blob_dir);
        // A slice covering `b` SST bytes writes `b` to the SST volume and
        // `b * stale_total / inputs_total` to the blob volume. Separate pools
        // bound each share on its own volume; a shared pool must hold the SUM,
        // so the budget solves `b * (inputs_total + stale_total) <= free`
        // against the tighter probe.
        let (pool, share) = if independent {
            (blob_free, stale_total_bytes)
        } else {
            (
                dest_free.min(blob_free),
                inputs_total.max(1) + stale_total_bytes,
            )
        };
        // Both factors are u64, so their u128 product is at most
        // (2^64-1)^2 < u128::MAX — plain multiply cannot overflow.
        let scaled = u128::from(pool) * u128::from(inputs_total.max(1)) / u128::from(share);
        let by_blob = u64::try_from(scaled).unwrap_or(u64::MAX).max(1);
        // The SST side still has to fit its own slice on its own volume.
        by_blob.min(dest_free.max(1))
    } else {
        dest_free.max(1)
    };
    let boundaries = tight_slice_boundaries(&inputs, slice_budget, comparator.as_ref())?;
    if boundaries.is_empty() {
        // Indivisible (single block across all inputs) — no incremental reclaim.
        return Ok(CompactionResult::nothing());
    }

    // Hide the inputs so the strategy cannot re-select them mid-loop.
    compaction_state
        .hidden_set_mut()
        .hide(payload.table_ids.iter().copied());

    let dst_lvl: usize = payload.canonical_level.into();
    // Tight-space slice merges apply NO removal semantics: no bottommost GC
    // (tombstone drop, seqno zeroing, range-tombstone application) even when the
    // destination IS the last level, and no user compaction filter (passed as
    // `false` to each slice's sub-compaction below). A slice restricts a surviving
    // input and hides its consumed prefix; if a fully-consumed sibling carried the
    // tombstone for a key that also lives in that prefix, last-level GC would drop
    // the tombstone from the output — and a filter verdict would drop a record
    // that survives nowhere else. A crash window that leaves the survivor
    // unrestricted (sidecar not yet written, prefix not yet punched) would then
    // let manifest repair re-expose the removed rows. Retaining every record makes
    // the slice output a SUPERSET that shadows any surviving prefix in every crash
    // window; a later normal last-level compaction reclaims
    // the deferred GC. Space is still reclaimed here via the hole punch, which is
    // what tight-space is for.
    let bottommost_gc = false;
    let blobs_folder = opts.config.path.join(BLOBS_FOLDER);
    // Canonicalize range tombstones across all inputs (a boundary-spanning RT is
    // otherwise copied into every slice output).
    let mut rts: Vec<crate::range_tombstone::RangeTombstone> = inputs
        .iter()
        .flat_map(|t| t.range_tombstones().iter().cloned())
        .collect();
    rts.sort();
    rts.dedup();

    let mut current_views = inputs;
    let mut lower: Bound<UserKey> = Bound::Unbounded;
    // Cumulative per-stale-file consumed frontier (`blob_file_id -> frame_end`).
    // Doubles as the next slice's scan-resume map and the punch offset for each
    // stale file's prior view. Empty (and unused) on the non-relocating path.
    let mut resume_offsets: crate::HashMap<BlobFileId, u64> = crate::HashMap::default();

    let result = (|| -> crate::Result<usize> {
        let mut tables_out = 0usize;

        for boundary in &boundaries {
            // Merge [lower, boundary) over the surviving (restricted) input views.
            let slice_payload = slice_payload_for(&current_views, payload);
            let version = opts.version_history.read().latest_version();

            // Re-fetch the stale blob files' CURRENT handles (slice 0: originals;
            // later: the prior slice's re-opened views). Never retained across the
            // iteration — a lingering Arc would block the prior view's punch.
            let current_stale: Vec<BlobFile> = if relocating {
                stale_ids
                    .iter()
                    .filter_map(|id| version.version.blob_files.get(*id).cloned())
                    .collect()
            } else {
                Vec::new()
            };
            let reloc = if relocating {
                Some(RelocationSetup {
                    stale_files: current_stale.clone(),
                    resume_offsets: resume_offsets.clone(),
                })
            } else {
                None
            };

            let produced = run_subcompaction(
                opts,
                &slice_payload,
                &version.version,
                Vec::new(),
                &rts,
                (lower.clone(), Bound::Excluded(boundary.clone())),
                dst_lvl,
                bottommost_gc,
                // Slices apply no removal semantics; see the parameter docs.
                false,
                &blobs_folder,
                reloc,
            )?;
            drop(version);

            let outputs: Vec<Table> = produced.created_tables().to_vec();
            // Install the tree-wide sinks on each slice output before the
            // version edit makes it visible (mirrors `install_merge`): the
            // deletion pause, so a later in-place heal cannot race a
            // checkpoint hard-link, and the heal-hint sink, so a
            // confirmed-persistent ECC correction on a read can queue this
            // SST for a healing rewrite instead of leaving the bitrot on
            // disk. NOT the background deleter — a rolled-back slice must
            // return its space immediately, and this loop rolls back exactly
            // when space is scarce.
            for table in &outputs {
                table.bind_to_tree(&crate::table::TableSinks {
                    deletion_pause: &opts.deletion_pause,
                    heal_hints: &opts.heal_hints,
                    #[cfg(feature = "std")]
                    background_deleter: None,
                });
            }
            // KV-separation: blob files this slice relocated live entries into,
            // plus the GC diff of entries it dropped.
            let mut new_blobs: Vec<BlobFile> = produced.created_blob_files().to_vec();
            // Capture the rollback set NOW, before the reopened stale views are
            // pushed onto `new_blobs` below: `mark_as_deleted` on a reopened view
            // unlinks its SHARED path (the still-live stale blob the current version
            // references), so a rollback must delete only the genuinely-new blob
            // files this slice created, never the reopened views.
            let blobs_for_cleanup = new_blobs.clone();
            let frag = produced.blob_frag_map().clone();
            let gc_diff = if frag.is_empty() { None } else { Some(frag) };

            // Advance the cumulative frontier from this slice's relocation, then
            // release `produced` (and its clones of the stale Inners) so the prior
            // views can punch once drained.
            if relocating {
                for (id, fe) in produced.consumed_through() {
                    let slot = resume_offsets.entry(*id).or_insert(0);
                    *slot = (*slot).max(*fe);
                }
            }
            drop(produced);

            // Serialize each surviving input's suffix-digest capture (inside
            // `reopen_restricted` below) and this slice's manifest install
            // against a concurrent in-place heal on the same table identity. A
            // patrol that heals a suffix block BETWEEN the digest read and the
            // version edit would bind the restricted manifest to a pre-heal
            // suffix digest; the patrol then sees the restriction change and
            // skips its whole-file refresh, and its attestation binds whole-file
            // (not suffix) digests, so once the prefix is punched the mismatch is
            // permanently unattributable. Hold each restricted input's heal lock
            // across the reopen AND the `upgrade_version` below (released when
            // these guards drop at the end of this slice iteration). Only inputs
            // that extend past the boundary (`max >= boundary`) are re-opened, so
            // only those are locked. The patrol holds at most one table's heal
            // lock at a time, so acquiring several here cannot deadlock it.
            //
            // Lock-order note: this path takes `compaction_state` (held from
            // `do_compaction`) BEFORE these heal locks, while the patrol reconcile
            // takes a heal lock and then `compaction_state` inside
            // `refresh_table_checksum`. To keep that inversion from deadlocking,
            // the patrol side `try_lock`s `compaction_state` and skips its refresh
            // on contention rather than blocking — so it never waits on
            // `compaction_state` while holding a heal lock this loop wants.
            #[cfg(feature = "page_ecc")]
            let heal_locks: Vec<Arc<parking_lot::Mutex<()>>> = current_views
                .iter()
                .filter(|v| {
                    comparator.compare(v.metadata.key_range.max(), boundary)
                        != core::cmp::Ordering::Less
                })
                .map(Table::heal_lock_arc)
                .collect();
            #[cfg(feature = "page_ecc")]
            let _heal_guards: Vec<parking_lot::MutexGuard<'_, ()>> =
                heal_locks.iter().map(|l| l.lock()).collect();

            // Classify each input at this boundary: restrict (extends past it) or
            // remove (fully consumed by this slice). Re-open restricted views as
            // distinct Inners so a prior view drops and punches independently.
            //
            // `run_subcompaction` above already finalized this slice's output SSTs
            // and blob files, but the install below is what references them. A
            // failure between here and the install (a sidecar write or a restricted
            // reopen) returns before any version points at those outputs, so, like
            // the install error path below, roll them back now instead of pinning
            // the space until the next open's orphan sweep. Tight-space runs exactly
            // when free space is scarce, so an ENOSPC here would otherwise both abort
            // the reclaim and hold the bytes this slice just consumed.
            // Pre-install rollback. The slice's finalized outputs/blobs are still
            // unreferenced (no version points at them), so on any failure before the
            // install commits they must be retracted or the scarce space stays pinned
            // until the next orphan sweep. No `.restrict-bound` sidecar is touched
            // here: the sidecar write is STRICTLY POST-COMMIT (the mark step below the
            // install), so an aborted slice never publishes one and there is nothing
            // to retract. That ordering is the crash-safety invariant — a sidecar on
            // disk always denotes a committed restriction (see the mark step and
            // `docs/manifest-recovery.md`).
            let rollback = |e: crate::Error| -> crate::Error {
                for t in &outputs {
                    t.mark_as_deleted();
                }
                for b in &blobs_for_cleanup {
                    b.mark_as_deleted();
                }
                e
            };

            // Re-open each stale blob file as a distinct Inner: the re-opened view
            // replaces the original in the new version (same id), and the original
            // — held only by prior snapshots after this — punches its consumed
            // `[data_start, frontier)` prefix when it drains. Mirrors the SST
            // `reopen_restricted` swap. Files with no consumption yet are skipped
            // (nothing to punch, original stays installed). Sits BELOW the
            // rollback closure on purpose: this reopen hashes the stale file's
            // live suffix, and a failure here — like the restricted SST reopen
            // below — must retract the slice's finalized-but-unreferenced
            // outputs, not leak them until an orphan sweep.
            let mut prior_to_punch: Vec<(BlobFile, u64)> = Vec::new();
            if relocating {
                // Test-only failpoint: a restricted-blob reopen failure at this
                // exact point (outputs finalized, install not yet run).
                #[cfg(test)]
                if opts
                    .config
                    .fail_tight_blob_reopen
                    .swap(false, core::sync::atomic::Ordering::SeqCst)
                {
                    return Err(rollback(cancelled_compaction()));
                }
                for sf in &current_stale {
                    if let Some(off) = resume_offsets.get(&sf.id()).copied() {
                        // The re-opened view serves only `[off, end)`: the prior
                        // view punches everything below once its readers drain.
                        // Capture the digest over that live suffix NOW, while the
                        // file is still whole, and record `off` as the view's
                        // frontier — a whole-file digest would never match the
                        // punched file, and integrity checks hash from the
                        // frontier for exactly this reason.
                        new_blobs.push(sf.reopen_restricted(off).map_err(rollback)?);
                        prior_to_punch.push((sf.clone(), off));
                    }
                }
            }

            let mut restricted_pairs: Vec<(TableId, Table)> = Vec::new();
            let mut removed_ids: Vec<TableId> = Vec::new();
            let mut next_views: Vec<Table> = Vec::new();
            for view in &current_views {
                if comparator.compare(view.metadata.key_range.max(), boundary)
                    == core::cmp::Ordering::Less
                {
                    removed_ids.push(view.id());
                } else {
                    // Open the restricted view over the same physical SST. This is an
                    // IN-MEMORY reopen (it reads the live suffix digest but writes
                    // nothing to disk), so a failure here commits nothing and just
                    // rolls back the finalized outputs. The `.restrict-bound` sidecar
                    // that records this view's exact bound is written only AFTER the
                    // install commits (the mark step below), never here.
                    //
                    // Later ordinary compactions read this view through
                    // `Table::scan()`, which starts at the restriction bound
                    // (blocks below it are punched and unreadable, and the
                    // prefix rows' authoritative copies live in the slice
                    // output) — so the restricted SST stays compactable and is
                    // eventually retired through the normal path.
                    let restricted = view.reopen_restricted(boundary.clone()).map_err(rollback)?;
                    restricted_pairs.push((view.id(), restricted.clone()));
                    next_views.push(restricted);
                }
            }

            // Bind the blob files this slice publishes, exactly as its SST
            // outputs were bound above — both the freshly written ones and the
            // re-opened restricted views, since the version edit below makes
            // all of them reachable at once. An unbound blob's `Drop` can
            // unlink it while a checkpoint is capturing, and its prefix punch
            // can zero bytes the checkpoint has already hard-linked.
            for blob_file in &new_blobs {
                blob_file.bind_to_tree(&crate::table::TableSinks {
                    deletion_pause: &opts.deletion_pause,
                    heal_hints: &opts.heal_hints,
                    #[cfg(feature = "std")]
                    background_deleter: None,
                });
            }

            // Install one atomic, durable version edit for the slice.
            let install = opts.version_history.write().upgrade_version(
                &opts.config.path,
                |sv| {
                    let mut copy = sv.clone();
                    let ctx = crate::version::TransformContext::new(comparator.as_ref());
                    copy.version = copy.version.with_tight_slice(
                        &restricted_pairs,
                        &removed_ids,
                        &outputs,
                        new_blobs,
                        gc_diff,
                        payload.dest_level as usize,
                        &ctx,
                    );
                    Ok(copy)
                },
                &opts.global_seqno,
                &opts.visible_seqno,
                &*opts.config.fs,
                opts.runtime_config.load_full(),
                opts.encryption.clone(),
            );
            if let Err(e) = install {
                // The install did not commit, so no sidecar was written (the mark
                // step below is strictly post-commit) and the punches never arm:
                // retract only the finalized-but-unreferenced outputs so the scarce
                // space is freed now instead of at the next orphan sweep.
                return Err(rollback(e));
            }

            // Mark, then punch. The install committed, so now record each restricted
            // input's exact bound to its `.restrict-bound` sidecar — STRICTLY AFTER
            // the commit, so a sidecar on disk always denotes a committed restriction
            // (manifest repair honors it without a commit protocol). Then arm the
            // prefix punch. A fully-consumed input is deleted outright (its data is in
            // the outputs).
            //
            // A sidecar write that fails HERE is not fatal: the restriction is already
            // durable in the manifest, which is what the bound is recovered FROM —
            // the next open republishes any sidecar that never landed. Log it and
            // leave that input UNPUNCHED meanwhile, so a manifest-loss repair that
            // runs before that open sees no-sidecar + unpunched and keeps the whole
            // input rather than deriving a conservative bound and dropping up to one
            // live block. Keeping it whole is itself only tolerable transiently: the
            // repair would publish both the input and the slice output as L0 runs,
            // and while point reads are shadowed by the output, merge operands are
            // deliberately never deduplicated across sources, so an operand in the
            // consumed prefix WOULD be applied twice. Hence the republish on open —
            // never punch without the sidecar, and never leave it missing for good.
            for view in &current_views {
                if removed_ids.contains(&view.id()) {
                    view.mark_as_deleted();
                } else {
                    match view.write_restrict_sidecar(boundary, opts.config.sync_mode) {
                        Ok(()) => match view.punch_offset_for(boundary) {
                            Ok(off) => view.mark_punch_on_drop(off),
                            // Post-commit, so a punch-offset lookup failure is non-fatal,
                            // exactly like the sidecar-write failure below: the restriction
                            // is already durable (manifest + sidecar), so leave the input
                            // unpunched and let a later compaction reclaim it. Propagating
                            // `?` here would abort an already-committed slice.
                            Err(e) => log::error!(
                                "tight-space could not resolve the punch offset for table \
                                 {} after committing its restriction: {e}; leaving the input \
                                 unpunched, a later compaction reclaims it",
                                view.id(),
                            ),
                        },
                        // On a LATER slice of the same input, this failure leaves
                        // the PREVIOUS slice's sidecar in place — valid but
                        // stale (a lower bound). Deliberately left there: a
                        // manifest-loss repair before the next open then
                        // restricts to that lower bound and republishes only the
                        // newly consumed interval beside the slice output — the
                        // same already-tolerated duplication class as a FIRST
                        // slice's failure (whole unpunched input republished),
                        // and strictly less of it. REMOVING the stale sidecar
                        // gains nothing and can lose data: the punch geometry
                        // re-derives the SAME lower bound (the holes end where
                        // that slice punched), while a backend that cannot
                        // prove holes then classifies the zeroed prefix as
                        // damage and salvage drops live rows the slice output
                        // does not carry. With the manifest alive, the next
                        // open replaces a DISAGREEING sidecar from the
                        // manifest's bound, which closes this window.
                        Err(e) => log::error!(
                            "tight-space could not write the restrict-bound sidecar for \
                             table {} after committing its restriction: {e}; leaving the \
                             input unpunched, a later compaction rewrites it",
                            view.id(),
                        ),
                    }
                }
            }
            // Arm each re-opened stale blob file's prior view to punch its
            // relocated `[data_start, frontier)` prefix once it drains.
            for (bf, off) in &prior_to_punch {
                bf.mark_punch_on_drop(*off);
            }

            tables_out += outputs.len();
            current_views = next_views;
            lower = Bound::Included(boundary.clone());

            // Drop this iteration's handles to the stale Inners so the only
            // remaining Arcs are the prior version snapshots; draining them then
            // drops + punches the originals NOW, reclaiming space before the next
            // slice. (Concurrent reader snapshots defer their share safely.)
            drop(prior_to_punch);
            drop(current_stale);
            opts.version_history.write().drain_obsolete_to_latest();

            // Test-only crash point: abort right after the first slice is durably
            // installed and punched, exercising the reopen-with-restriction path.
            #[cfg(test)]
            if opts
                .config
                .fail_tight_after_first_slice
                .swap(false, core::sync::atomic::Ordering::SeqCst)
            {
                return Err(cancelled_compaction());
            }

            if current_views.is_empty() {
                // Every input was consumed before the last boundary.
                return Ok(tables_out);
            }
        }

        // Tail [last boundary, hi): merge the remainder, remove the inputs, and
        // drop the now fully-consumed stale blob files (install_merge deletes
        // them outright — the prior slices already punched their prefixes).
        let slice_payload = slice_payload_for(&current_views, payload);
        let version = opts.version_history.read().latest_version();
        let tail_reloc = if relocating {
            let current_stale: Vec<BlobFile> = stale_ids
                .iter()
                .filter_map(|id| version.version.blob_files.get(*id).cloned())
                .collect();
            Some(RelocationSetup {
                stale_files: current_stale,
                resume_offsets: resume_offsets.clone(),
            })
        } else {
            None
        };
        let produced = run_subcompaction(
            opts,
            &slice_payload,
            &version.version,
            current_views.clone(),
            &rts,
            (lower.clone(), Bound::Unbounded),
            dst_lvl,
            bottommost_gc,
            // Slices apply no removal semantics; see the parameter docs.
            false,
            &blobs_folder,
            tail_reloc,
        )?;
        drop(version);
        let tail_out = produced.created_tables().len();
        super::flavour::install_merge(
            &mut opts.version_history.write(),
            opts,
            &slice_payload,
            vec![produced],
        )?;
        Ok(tables_out + tail_out)
    })();

    compaction_state
        .hidden_set_mut()
        .show(payload.table_ids.iter().copied());

    let tables_out = result?;

    opts.version_history.write().maintenance(
        &opts.config.path,
        opts.mvcc_gc_watermark,
        &*opts.config.fs,
    )?;

    Ok(CompactionResult {
        action: CompactionAction::Merged,
        dest_level: Some(payload.dest_level),
        tables_in,
        tables_out,
    })
}

/// Per-slice relocation inputs for the tight-space blob-defrag path: the stale
/// blob files to rewrite and, per file, the absolute data-section offset to
/// resume its scan at (the prior slice's consumed frontier, with everything
/// before it already hole-punched).
#[cfg(feature = "std")]
struct RelocationSetup {
    stale_files: Vec<BlobFile>,
    resume_offsets: crate::HashMap<BlobFileId, u64>,
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
    bounds: (core::ops::Bound<UserKey>, core::ops::Bound<UserKey>),
    dst_lvl: usize,
    is_last_level: bool,
    // Whether the user compaction filter runs in this merge. `false` on
    // tight-space slices, which apply NO removal semantics (bottommost GC is
    // deferred the same way): a slice's output must be a SUPERSET that shadows
    // any surviving input prefix in every crash window — a post-commit sidecar
    // write failure deliberately leaves the input unpunched, and a
    // manifest-loss repair then republishes that prefix whole, so a record the
    // filter removed here would survive nowhere else and resurrect. The
    // filtering is deferred, not lost: the next normal compaction applies it.
    apply_compaction_filter: bool,
    blobs_folder: &std::path::Path,
    // When `Some` (tight-space blob defrag only), this sub-compaction relocates
    // the live entries of `stale_files` into a fresh compact blob file, resuming
    // each stale file's scan at `resume_offsets` so an already-punched prefix is
    // never re-read. `None` is the pass-through path (no blob relocation).
    relocation: Option<RelocationSetup>,
) -> crate::Result<super::flavour::ProducedOutput> {
    use super::flavour::CompactionFlavour;

    // Test-only failpoint: the first range to observe an armed flag fails (and
    // disarms it), so exactly one sub-compaction errors while its siblings
    // succeed — deterministically driving the rollback path that marks the
    // siblings' finalized files deleted and restores the hidden inputs.
    #[cfg(test)]
    if opts
        .config
        .fail_one_subcompaction
        .swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        return Err(cancelled_compaction());
    }

    let mut blob_frag_map = FragmentationMap::default();

    let Some(mut merge_iter) = create_bounded_compaction_stream(
        version,
        &payload.table_ids,
        bounds,
        opts.mvcc_gc_watermark,
        opts.config.merge_operator.clone(),
        opts.config.comparator.clone(),
    ) else {
        // The caller validated every input exists, so a missing table here is
        // unexpected. Fail closed: an empty output would let the install delete
        // the source tables (this range may own input deletion) while producing
        // no replacement SSTs. Returning an error makes the parallel caller
        // re-show the inputs and skip the install entirely.
        return Err(crate::Error::from(crate::io::Error::other(
            "sub-compaction input tables disappeared mid-flight",
        )));
    };

    // Whole-version range tombstones drive both compaction-time RT application
    // (drop covered KVs in the merge, with blob-GC accounting) and the
    // bottommost seqno-zeroing gate below. Gathered from every level so coverage
    // outside this compaction is respected.
    let version_tombstones = if is_last_level {
        collect_version_tombstones(version)
    } else {
        Vec::new()
    };

    merge_iter = merge_iter
        .evict_tombstones(is_last_level)
        .zero_seqnos(false);
    if is_last_level {
        merge_iter = merge_iter.with_range_tombstone_application(
            version_tombstones.clone(),
            opts.config.comparator.clone(),
        );
    }

    let filter_ctx = Context { is_last_level };
    let mut compaction_filter = if apply_compaction_filter {
        opts.config
            .compaction_filter_factory
            .as_ref()
            .map(|f| f.make_filter(&filter_ctx))
    } else {
        None
    };
    // Shared transform counter, created for EVERY sub-compaction: the filter
    // adapter ticks it on non-Keep verdicts, the merge stream on its own
    // visibility-changing drops (bottommost tombstone elision, RT
    // application, weak annihilation), and the table writer marks
    // transformed the lineage of any output whose window saw one (see
    // `prepare_table_writer`).
    let transform_marker = Some(Arc::new(portable_atomic::AtomicU64::new(0)));
    if let Some(marker) = &transform_marker {
        merge_iter = merge_iter.with_transform_marker(Arc::clone(marker));
    }

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
        transform_marker.clone(),
    ));

    // Bottommost seqno-zeroing: at the last level, entries below the GC
    // watermark and not covered by any range tombstone get seqno 0 (packs to
    // one byte). Tombstones are gathered from the whole version so coverage in
    // levels outside this compaction still blocks zeroing.
    let merge_iter = super::seqno_zeroer::BottommostSeqnoZeroer::new(
        merge_iter,
        is_last_level,
        version_tombstones,
        opts.mvcc_gc_watermark,
        opts.config.comparator.clone(),
    );

    // block_parallel = false: this sub-compaction already runs on a pool thread,
    // so its block compression must stay serial (nested-pool deadlock otherwise).
    // whole_run = false: several sub-compactions (or tight-space slices) share
    // this payload's lineage, so no single writer may close the run.
    let table_writer = super::flavour::prepare_table_writer(
        version,
        opts,
        payload,
        false,
        transform_marker,
        false,
    )?;
    let mut compactor: Box<dyn CompactionFlavour> = match relocation {
        // Tight-space blob defrag: relocate the stale files' live entries into a
        // fresh compact file, resuming each stale scan at its carried-over
        // frame boundary (the prior slice punched everything before it).
        Some(reloc) if opts.config.kv_separation_opts.is_some() => {
            #[expect(clippy::expect_used, reason = "guarded by is_some() above")]
            let blob_opts = opts
                .config
                .kv_separation_opts
                .as_ref()
                .expect("kv_separation_opts present");

            let scanner = BlobFileMergeScanner::new(
                reloc
                    .stale_files
                    .iter()
                    .map(|bf| match reloc.resume_offsets.get(&bf.id()) {
                        Some(&off) => BlobFileScanner::resume(&bf.0.path, &*bf.0.fs, bf.id(), off),
                        None => BlobFileScanner::new(&bf.0.path, &*bf.0.fs, bf.id()),
                    })
                    .collect::<crate::Result<Vec<_>>>()?,
            );

            let writer = BlobFileWriter::new(
                opts.blob_file_id_generator.clone(),
                blobs_folder,
                opts.tree_id,
                opts.config.descriptor_table.clone(),
                opts.config.fs.clone(),
            )?
            .use_target_size(blob_opts.file_target_size)
            .use_passthrough_compression(blob_opts.compression)
            .use_sync_mode(opts.config.sync_mode);

            let inner = StandardCompaction::new(table_writer, tables_for_deletion);
            Box::new(RelocatingCompaction::new(
                inner,
                scanner.peekable(),
                writer,
                reloc.stale_files,
                opts.rate_limiter.clone(),
                opts.stop_signal.clone(),
            ))
        }
        _ => Box::new(StandardCompaction::new(table_writer, tables_for_deletion)),
    };

    // Propagate range tombstones to this sub-range's output (GC'd at the last
    // level when fully applied); the writer clips them to each output table's
    // key range (a boundary-spanning RT is written clipped on both sides).
    let output_rts = range_tombstones_after_gc(
        input_range_tombstones,
        version,
        &payload.table_ids,
        opts.mvcc_gc_watermark,
        is_last_level,
        &opts.config.comparator,
    );
    if !output_rts.is_empty() {
        compactor.write_range_tombstones(&output_rts);
    }

    for (idx, item) in merge_iter.enumerate() {
        let item = item?;

        // One key + value length; the sum is far below u64::MAX → plain add.
        let io_bytes = item.key.user_key.len() as u64 + item.value.len() as u64;
        if opts
            .rate_limiter
            .request_interruptible(io_bytes, || opts.stop_signal.is_stopped())
        {
            // Abort, do not produce: a truncated sub-range would be installed
            // atomically alongside its siblings, dropping the unwritten tail of
            // this range (a gap in the middle of the key space). The error makes
            // the caller re-show the inputs and skip the install.
            return Err(cancelled_compaction());
        }

        compactor.write(item)?;

        if idx % 1_000_000 == 0 && opts.stop_signal.is_stopped() {
            return Err(cancelled_compaction());
        }
    }

    if let Some(filter) = compaction_filter {
        filter.finish();
    }

    let extra_blob_files = filter_blob_writer
        .map(BlobFileWriter::finish)
        .transpose()?
        .unwrap_or_default();

    // produce() consumes the (already finalized on disk) filter blob files; if
    // it fails, mark them deleted so they are not orphaned. The parallel caller
    // rolls back sibling outputs on error but cannot reach this range's own
    // filter blobs, so clean them up here.
    let rollback_extra_blob_files = extra_blob_files.clone();
    compactor
        .produce(opts, dst_lvl, blob_frag_map, extra_blob_files)
        .inspect_err(|_| {
            for blob_file in &rollback_extra_blob_files {
                blob_file.mark_as_deleted();
            }
        })
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "version_history_lock must be held across upgrade_version and maintenance"
)]
fn move_tables(
    compaction_state: &CompactionGuard<'_>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<CompactionResult> {
    let mut version_history_lock = opts.version_history.write();

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

/// Picks blob files to rewrite (defragment): the linked, stale, non-dead blob
/// files a compaction of `picked_tables` would relocate. Also the basis for the
/// `storage_stats` blob-relocation estimate, so the reported status budgets the
/// SAME stale subset the gate does (not every live blob file).
// `pub` (not `pub(crate)`) inside this crate-private module — see
// `space_fits_two_layer`.
pub fn pick_blob_files_to_rewrite(
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
        let mut compaction_state = opts.compaction_state.lock();

        compaction_state
            .hidden_set_mut()
            .show(payload.table_ids.iter().copied());
    })
}

/// Decides whether the merge reduces to a merge-on-read relocation and, if so,
/// returns the positional delete-bitmap to materialize.
///
/// The conservative, provably-safe trigger: a SINGLE input segment that is
/// columnar, relocatable (non-encrypted, non-ECC, carries a zone map), under a
/// bitmap-writing strategy, whose own below-watermark range tombstones delete at
/// least one row. A single input means no foreign live keys can be lost and no
/// foreign point tombstone can be missed. The bitmap is built over the segment's
/// own scan (every version in block-index = position order) so it matches the
/// read mask. `None` falls through to the normal copy-on-write merge, including
/// the adaptive purge once the deleted fraction crosses the threshold.
#[cfg(feature = "std")]
fn plan_merge_on_read(
    opts: &Options,
    payload: &CompactionPayload,
    tables: &[Table],
    input_range_tombstones: &[crate::range_tombstone::RangeTombstone],
) -> crate::Result<Option<(Table, crate::table::delete_bitmap::DeleteBitmap)>> {
    let dst_lvl: usize = payload.canonical_level.into();
    let strategy = opts.runtime_config.load_full().delete_strategy.get(dst_lvl);
    if !strategy.writes_bitmap() {
        return Ok(None);
    }
    // Single relocatable columnar segment only; anything else is copy-on-write.
    let [source] = tables else {
        return Ok(None);
    };
    if !source.metadata.columnar
        || source.encryption.is_some()
        || source.metadata.ecc_params.is_some()
        || source.metadata.ecc_unrecognized
        || source.zone_map.is_empty()
        // A source that already carries a delete bitmap reads through the mask,
        // so `scan()` below yields the surviving rows under renumbered ordinals
        // that no longer line up with the verbatim-copied physical blocks. Relocating
        // it would alias the wrong rows; fall back to copy-on-write instead.
        || !source.delete_bitmap().is_empty()
        // A tight-space restricted view renumbers for the same reason — `scan()`
        // starts at its bound — and the relocation would additionally copy the
        // punched prefix and publish it unrestricted.
        || source.restrict_lower_bound().is_some()
    {
        return Ok(None);
    }

    // Positional bitmap from the segment's own below-watermark range tombstones,
    // over every stored version in block-index order (scan order = the writer's
    // position numbering = the read mask's expectation). Keep only the key and
    // seqno per row, dropping each scanned value so planning does not retain a
    // whole segment's values in memory.
    let keys = source
        .scan()?
        .map(|entry| {
            let entry = entry?;
            Ok((entry.key.user_key, entry.key.seqno))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    let bitmap = super::delete_materialize::build_position_bitmap(
        keys.iter()
            .map(|(user_key, seqno)| (user_key.as_ref(), *seqno)),
        input_range_tombstones,
        opts.mvcc_gc_watermark,
        &opts.config.comparator,
    )?;
    if bitmap.is_empty() {
        return Ok(None);
    }

    // Adaptive: once the deleted fraction crosses the threshold, purge instead
    // (fall through to the copy-on-write merge, which drops the rows and clears
    // the bitmap).
    if let crate::config::DeleteStrategy::Adaptive {
        purge_threshold_percent,
    } = strategy
    {
        let item_count = source.metadata.item_count.max(1);
        // `bitmap.len() <= item_count`, both far below `u64::MAX / 100`.
        let deleted_percent = bitmap.len() * 100 / item_count;
        if deleted_percent >= u64::from(purge_threshold_percent) {
            return Ok(None);
        }
    }

    Ok(Some((source.clone(), bitmap)))
}

/// Relocates `source` into a new segment that reuses its data blocks verbatim
/// plus `bitmap`, then installs the version edit replacing `source`. The caller
/// has already released the version read lock (the install takes the write lock).
#[cfg(feature = "std")]
fn run_merge_on_read_relocation(
    compaction_state: &mut CompactionGuard<'_>,
    opts: &Options,
    payload: &CompactionPayload,
    source: &Table,
    bitmap: &crate::table::delete_bitmap::DeleteBitmap,
) -> crate::Result<CompactionResult> {
    let dst_lvl: usize = payload.canonical_level.into();
    let new_id = opts.table_id_generator.next();
    let (folder, level_fs) = opts.config.tables_folder_for_level(payload.dest_level);
    let new_path = folder.join(new_id.to_string());

    // Write the relocated SST through the DESTINATION level's filesystem (the
    // same one that recovers and installs it below), not the source table's.
    let checksum = source.relocate_columnar_with_deletes(
        &new_path,
        &*level_fs,
        new_id,
        bitmap,
        opts.config.sync_mode,
    )?;

    let relocated = {
        let mut params = crate::table::RecoverParams::new(
            new_path.clone(),
            checksum,
            new_id,
            Arc::clone(&level_fs),
            opts.config.comparator.clone(),
            opts.config.cache.clone(),
        );
        params.tree_id = opts.tree_id;
        params
            .descriptor_table
            .clone_from(&opts.config.descriptor_table);
        params.pin_filter = opts.config.filter_block_pinning_policy.get(dst_lvl);
        params.pin_index = opts.config.index_block_pinning_policy.get(dst_lvl);
        params.encryption.clone_from(&opts.config.encryption);
        #[cfg(zstd_any)]
        {
            params
                .zstd_dictionary
                .clone_from(&opts.config.zstd_dictionary);
        }
        #[cfg(feature = "metrics")]
        {
            params.metrics = opts.metrics.clone();
        }
        match Table::recover(params) {
            Ok(table) => table,
            Err(e) => {
                // The output is finalized but no manifest names it and no
                // handle exists to mark it deleted, so nothing else will drop
                // it before a restart's orphan sweep. Background compaction
                // retries the same merge, and each attempt would leave another
                // full-sized copy on the volume this pass was meant to relieve.
                // The relocation writer already unlinks on a pre-finalization
                // failure; this closes the window after it.
                if let Err(rm) = level_fs.remove_file(&new_path)
                    && rm.kind() != crate::io::ErrorKind::NotFound
                {
                    log::error!(
                        "relocation output {} could not be removed after its reopen failed \
                         ({rm}); it stays until the next orphan sweep",
                        new_path.display(),
                    );
                }
                return Err(e);
            }
        }
    };

    compaction_state
        .hidden_set_mut()
        .hide(payload.table_ids.iter().copied());

    let produced = super::flavour::ProducedOutput::for_relocation(relocated, source.clone());
    let result = (|| -> crate::Result<usize> {
        let mut version_history_lock = opts.version_history.write();
        let tables_out = super::flavour::install_merge(
            &mut version_history_lock,
            opts,
            payload,
            vec![produced],
        )?;
        version_history_lock.maintenance(
            &opts.config.path,
            opts.mvcc_gc_watermark,
            &*opts.config.fs,
        )?;
        drop(version_history_lock);
        Ok(tables_out)
    })();

    compaction_state
        .hidden_set_mut()
        .show(payload.table_ids.iter().copied());

    let tables_out = result?;
    Ok(CompactionResult {
        action: CompactionAction::Merged,
        dest_level: Some(payload.dest_level),
        tables_in: 1,
        tables_out,
    })
}

#[expect(clippy::too_many_lines)]
fn merge_tables(
    mut compaction_state: CompactionGuard<'_>,
    version_history_lock: VersionsReadGuard<'_>,
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

    // Arc so the snapshot can be shared with sub-compactions running on the
    // configured compaction pool (fire-and-forget executor needs `'static`).
    let current_super_version = Arc::new(version_history_lock.latest_version());

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

    // Merge-on-read fast path: a lone columnar segment whose own range
    // tombstones (below the watermark) delete some of its rows is relocated (its
    // data blocks reused verbatim plus a positional delete-bitmap) instead of
    // being re-transposed. Detection is read-only, so it runs under the held
    // read lock; on a hit the lock is released before the relocation installs
    // its own version edit. Multi-input merges and any non-relocatable segment
    // fall through to the normal copy-on-write merge below.
    #[cfg(feature = "std")]
    if let Some((source, bitmap)) =
        plan_merge_on_read(opts, payload, &tables, &input_range_tombstones)?
    {
        drop(current_super_version);
        drop(version_history_lock);
        return run_merge_on_read_relocation(
            &mut compaction_state,
            opts,
            payload,
            &source,
            &bitmap,
        );
    }

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

            // Run the sub-compactions on the configured compaction pool (NOT raw
            // threads), so a caller-shared pool bounds total threads across
            // trees. The executor is fire-and-forget + `'static`, so each task
            // owns an Arc'd snapshot of the context; results come back over an
            // mpsc channel, indexed by range. Only range 0 carries the input
            // tables to delete, so they are dropped exactly once at install. No
            // pool configured (e.g. caller injected none) → run sequentially.
            let num_ranges = ranges.len();
            let only_first_owns_inputs =
                |idx: usize| if idx == 0 { tables.clone() } else { Vec::new() };

            let outputs: Vec<crate::Result<super::flavour::ProducedOutput>> =
                if let Some(spawner) = opts.config.compaction_pool.clone() {
                    let opts = Arc::new(opts.clone());
                    let payload = Arc::new(payload.clone());
                    let version = Arc::clone(&current_super_version);
                    let rts = Arc::new(input_range_tombstones.clone());
                    let blobs = Arc::new(blobs_folder);

                    let (tx, rx) = std::sync::mpsc::channel();
                    for (idx, range) in ranges.iter().cloned().enumerate() {
                        let tx = tx.clone();
                        let opts = Arc::clone(&opts);
                        let payload = Arc::clone(&payload);
                        let version = Arc::clone(&version);
                        let rts = Arc::clone(&rts);
                        let blobs = Arc::clone(&blobs);
                        let tables_for_deletion = only_first_owns_inputs(idx);
                        spawner.spawn(Box::new(move || {
                            let out = run_subcompaction(
                                &opts,
                                &payload,
                                &version.version,
                                tables_for_deletion,
                                &rts,
                                range,
                                dst_lvl,
                                is_last_level,
                                true,
                                &blobs,
                                // The parallel path never relocates blobs; tight
                                // blob defrag is the serial slice loop's domain.
                                None,
                            );
                            // The receiver outlives every send (it drains N
                            // items below), so this cannot fail.
                            let _ = tx.send((idx, out));
                        }));
                    }
                    drop(tx);

                    let mut slots: Vec<Option<crate::Result<super::flavour::ProducedOutput>>> =
                        (0..num_ranges).map(|_| None).collect();
                    for (idx, out) in rx {
                        if let Some(slot) = slots.get_mut(idx) {
                            *slot = Some(out);
                        }
                    }
                    slots
                        .into_iter()
                        .map(|slot| {
                            slot.unwrap_or_else(|| {
                                // A worker panicked before sending: treat as a
                                // failed sub-compaction so install is skipped.
                                Err(crate::Error::from(crate::io::Error::other(
                                    "sub-compaction worker did not report a result",
                                )))
                            })
                        })
                        .collect()
                } else {
                    ranges
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(idx, range)| {
                            run_subcompaction(
                                opts,
                                payload,
                                &current_super_version.version,
                                only_first_owns_inputs(idx),
                                &input_range_tombstones,
                                range,
                                dst_lvl,
                                is_last_level,
                                true,
                                &blobs_folder,
                                None,
                            )
                        })
                        .collect()
                };

            // Collect outputs keeping every successful one, so a single failed
            // range can roll back the SSTs/blob files its succeeded siblings
            // already finalized on disk (collecting straight into Result would
            // drop those Ok outputs and leak their files). On the first error:
            // mark the committed outputs deleted, un-hide the inputs, propagate.
            let mut committed = Vec::with_capacity(outputs.len());
            let mut first_err = None;
            for out in outputs {
                match out {
                    Ok(done) => committed.push(done),
                    // Keep scanning after an error: ranges complete in any order,
                    // so a later Ok must still be collected and rolled back (an
                    // [Ok, Err, Ok] layout would otherwise orphan the trailing
                    // Ok's finalized files). Keep only the first error to return.
                    Err(e) => {
                        first_err.get_or_insert(e);
                    }
                }
            }
            if let Some(err) = first_err {
                log::error!("Sub-compaction failed: {err:?}");
                for done in &committed {
                    done.rollback_uninstalled();
                }
                {
                    let mut state = opts.compaction_state.lock();
                    state
                        .hidden_set_mut()
                        .show(payload.table_ids.iter().copied());
                }
                return Err(err);
            }
            let outputs = committed;

            // Re-acquire locks and install one atomic version edit for all outputs.
            let mut compaction_state = opts.compaction_state.lock();
            let mut version_history_lock = opts.version_history.write();

            let tables_out =
                super::flavour::install_merge(&mut version_history_lock, opts, payload, outputs)
                    .inspect_err(|e| {
                        // install_merge marks its own created tables/blob files
                        // deleted if the version edit fails, so the caller only
                        // restores the hidden inputs here (the outputs are gone).
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

    // Whole-version tombstones for compaction-time RT application (drop covered
    // KVs in the merge) and the bottommost seqno-zeroing gate; gathered from
    // every level so coverage outside this compaction is respected. The scan and
    // the seqno-zeroer wrapper are `core` + `alloc`, so the bottommost
    // RT-application + zeroing runs on the `no_std` serial path too (see the
    // zeroer wrap below).
    let zeroing_tombstones = if is_last_level {
        collect_version_tombstones(&current_super_version.version)
    } else {
        Vec::new()
    };
    if is_last_level {
        merge_iter = merge_iter.with_range_tombstone_application(
            zeroing_tombstones.clone(),
            opts.config.comparator.clone(),
        );
    }

    let blobs_folder = opts.config.path.join(BLOBS_FOLDER);

    let filter_ctx = Context { is_last_level };

    // Construct the compaction filter
    let mut compaction_filter = opts.config.compaction_filter_factory.as_ref().map(|f| {
        log::trace!("Installing custom compaction filter {:?}", f.name());
        f.make_filter(&filter_ctx)
    });
    // Shared transform counter, created for EVERY compaction: the filter
    // adapter ticks it on non-Keep verdicts, the merge stream ticks it on
    // its own visibility-changing drops (bottommost tombstone elision,
    // range-tombstone application, weak-tombstone annihilation), and the
    // table writer marks transformed the lineage of any output whose window
    // saw one (see `prepare_table_writer`).
    let transform_marker = Some(Arc::new(portable_atomic::AtomicU64::new(0)));
    if let Some(marker) = &transform_marker {
        merge_iter = merge_iter.with_transform_marker(Arc::clone(marker));
    }

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
        transform_marker.clone(),
    ));

    // Serial (single-stream) compaction: block compression may use the pool,
    // and the single writer owns the whole run (its final output carries the
    // `lineage_last` marker).
    let table_writer = super::flavour::prepare_table_writer(
        &current_super_version.version,
        opts,
        payload,
        true,
        transform_marker,
        true,
    )?;

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
                        .map(|bf| BlobFileScanner::new(&bf.0.path, &*bf.0.fs, bf.id()))
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
        // NOTE: this path does NOT GC fully-applied tombstones (unlike the
        // parallel sub-compaction path). The serial stop-signal handling below
        // commits whatever was written so far (`return Ok(())`), so a stop
        // landing after this write but before the covered tail is processed
        // could drop a below-watermark tombstone while some covered keys were
        // never visited — resurrecting them. Tombstone GC therefore only runs in
        // the sub-compaction path, which is atomic (it returns an error and
        // rolls back on stop). Covered keys are still physically dropped here by
        // the merge stream; keeping the tombstone is the conservative, correct
        // choice when the compaction may commit partial output.
        if !input_range_tombstones.is_empty() {
            log::debug!(
                "Propagating {} range tombstones to compaction output",
                input_range_tombstones.len(),
            );
            compactor.write_range_tombstones(&input_range_tombstones);
        }

        // Bottommost seqno-zeroing: at the last level, entries below the GC
        // watermark and not covered by any range tombstone get seqno 0. The
        // zeroer and the whole-version tombstone scan are `core` + `alloc`, so
        // this runs on the `no_std` serial path too.
        let merge_iter = super::seqno_zeroer::BottommostSeqnoZeroer::new(
            merge_iter,
            is_last_level,
            zeroing_tombstones,
            opts.mvcc_gc_watermark,
            opts.config.comparator.clone(),
        );

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
            // One key + value length; the sum is far below u64::MAX → plain add.
            let io_bytes = item.key.user_key.len() as u64 + item.value.len() as u64;
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

    let mut compaction_state = opts.compaction_state.lock();

    log::trace!("Acquiring super version write lock");
    let mut version_history_lock = opts.version_history.write();
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
    compaction_state: CompactionGuard<'_>,
    opts: &Options,
    ids_to_drop: &[TableId],
) -> crate::Result<CompactionResult> {
    let mut version_history_lock = opts.version_history.write();

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
mod tests;
