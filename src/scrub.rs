// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! ECC patrol scrub: a proactive background sweep over Page-ECC-protected SST
//! blocks.
//!
//! It reads (typically cold) data blocks to detect and correct latent bit-rot
//! *before* it accumulates past the parity budget: the storage-engine analogue
//! of ECC-RAM patrol scrub or `zpool scrub`. Without it, an isolated correctable
//! single-block fault sits unnoticed until a *second* fault in the same block
//! pushes it past the parity's correction budget and becomes unrecoverable.
//!
//! A scrub pass reads every data block straight from disk, runs the normal
//! read-path verify+correct (SEC-DED single-bit fast path → Reed-Solomon shard
//! recovery), and, when
//! [`auto_heal`](crate::runtime_config::RuntimeConfig::auto_heal) is on,
//! schedules a healing recompaction of any SST that needed correction (the same
//! [`HealHints`](crate::heal_hints::HealHints) queue the live read path feeds).
//!
//! # Layering: a primitive, not a daemon
//!
//! This module exposes the scrub *pass* ([`patrol_scrub`](crate::scrub::patrol_scrub)); it does not own a
//! timer thread or any cluster awareness. Like the auto-heal rewrite it feeds
//! (drive with [`EccHeal`](crate::compaction::EccHeal) over
//! [`Tree::heal_hints`](crate::Tree::heal_hints)), the *cadence* and the
//! *leader-only* gating in a clustered deployment are the caller's concern: run
//! [`patrol_scrub`](crate::scrub::patrol_scrub) on a schedule from the cluster leader only, since a healing
//! recompaction is a background mutation. The pass is off by default; it costs
//! nothing until called.
//!
//! Scrub targets **data blocks**, where the cold bulk of an SST's bytes (and
//! thus its latent-fault exposure) lives. Index / filter / meta blocks are tiny,
//! pinned in memory after open, and already checksum-verified at open time and
//! whenever a read recovers them via the live path.
//!
//! # Throttle
//!
//! A scrub competes with production reads for disk bandwidth, so
//! [`PatrolScrubOptions::throttle`](crate::scrub::PatrolScrubOptions::throttle) makes each worker pause between SSTs to cap
//! I/O pressure, and [`PatrolScrubOptions::parallelism`](crate::scrub::PatrolScrubOptions::parallelism) bounds how many SSTs
//! are scrubbed concurrently. The pass deliberately bypasses the block cache in
//! both directions: it re-reads the medium (a cached clean copy would hide an
//! on-disk fault) and never evicts the live working set with cold blocks.

use crate::AbstractTree;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) mod heal_attest;

/// One uncorrectable finding from a patrol scrub.
///
/// Emitted when a block failed its checksum and Page-ECC parity could not
/// recover it (or the block was otherwise unreadable). The scrub never silently
/// skips such a block: each lands here and in
/// [`PatrolScrubReport::uncorrectable_blocks`], and is logged at error level.
#[derive(Debug)]
#[non_exhaustive]
pub enum ScrubError {
    /// A data block could not be read or its checksum failed and ECC could not
    /// recover it. The SST and block offset localise the fault for an operator;
    /// `reason` carries the underlying error rendered as text (the engine error
    /// type is not `Clone`, so it is captured eagerly here).
    UncorrectableBlock {
        /// Table the faulty block belongs to.
        table_id: crate::table::TableId,
        /// On-disk path of the SST.
        path: PathBuf,
        /// Block offset within the SST.
        block_offset: u64,
        /// The underlying read / decode error, rendered as text.
        reason: String,
    },

    /// The table's block index could not be walked to enumerate data blocks.
    /// The rest of that table is skipped; other tables still scrub.
    BlockIndexUnreadable {
        /// Table whose index failed to iterate.
        table_id: crate::table::TableId,
        /// On-disk path of the SST.
        path: PathBuf,
        /// The underlying error, rendered as text.
        reason: String,
    },

    /// An in-place heal rewrote this table's bytes, but its refreshed
    /// full-file digest could not be persisted to the manifest (recomputing
    /// the digest or installing the new version failed). The healed BYTES are
    /// durable; the manifest may still carry a stale pre-heal digest, so a
    /// later [`crate::verify::verify_integrity`] can flag the healed file as
    /// corrupt until a re-run of the scrub (or a manifest rebuild) refreshes
    /// it.
    ChecksumRefreshFailed {
        /// Table whose digest could not be refreshed.
        table_id: crate::table::TableId,
        /// On-disk path of the SST.
        path: PathBuf,
        /// The underlying error, rendered as text.
        reason: String,
    },
}

/// Aggregated result of a [`patrol_scrub`] run.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct PatrolScrubReport {
    /// Number of SST table files visited.
    pub sst_files_scanned: usize,
    /// Total data blocks read across all SSTs (includes corrected and
    /// uncorrectable blocks: every block the scrub attempted).
    pub blocks_scanned: usize,
    /// Blocks recovered from their Page-ECC parity (a latent on-disk fault was
    /// corrected in-flight). Each such block's SST still holds the fault until a
    /// healing rewrite lands.
    pub corrections_applied: usize,
    /// Distinct SSTs newly queued for a healing recompaction by this scrub
    /// (confirmed-persistent correction with `auto_heal` enabled). Zero when
    /// `auto_heal` is off (correction-on-read still happens, only the rewrite
    /// scheduling is suppressed), and zero in heal-in-place mode (the correction
    /// is persisted directly, so no full-file rewrite is scheduled).
    pub ssts_scheduled_for_rewrite: usize,
    /// Blocks whose Page-ECC correction was persisted **in place** — the
    /// corrected bytes written back at the block's existing offset
    /// (size-preserving), no full-file rewrite. Non-zero only when
    /// [`PatrolScrubOptions::heal_in_place`] is set; it is the O(damage)
    /// counterpart to [`ssts_scheduled_for_rewrite`](Self::ssts_scheduled_for_rewrite).
    pub blocks_healed_in_place: usize,
    /// Blocks that failed their checksum and could NOT be recovered from parity
    /// (or were otherwise unreadable). These are real, unhealed corruption.
    pub uncorrectable_blocks: usize,
    /// Per-block / per-table findings collected during the sweep. The scrub
    /// always runs to completion across all SSTs even when individual blocks or
    /// whole index walks fail.
    pub errors: Vec<ScrubError>,
}

impl PatrolScrubReport {
    /// `true` when every block the scrub read was clean or successfully
    /// corrected AND the sweep finished with no findings at all: no
    /// uncorrectable corruption, no block-index walk that failed to
    /// enumerate a table's blocks, and no manifest-digest refresh that
    /// could not be persisted after an in-place heal. Any entry in
    /// [`errors`](Self::errors) means the tree needs operator attention
    /// even when every block that WAS read verified clean.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.uncorrectable_blocks == 0 && self.errors.is_empty()
    }

    /// Folds a per-SST partial report into this accumulator. `pub(crate)` for
    /// the heal path's read-only fallback (it folds a scrub report into the
    /// heal report when the file cannot be opened read+write).
    pub(crate) fn merge(&mut self, other: Self) {
        self.sst_files_scanned += other.sst_files_scanned;
        self.blocks_scanned += other.blocks_scanned;
        self.corrections_applied += other.corrections_applied;
        self.ssts_scheduled_for_rewrite += other.ssts_scheduled_for_rewrite;
        self.blocks_healed_in_place += other.blocks_healed_in_place;
        self.uncorrectable_blocks += other.uncorrectable_blocks;
        self.errors.extend(other.errors);
    }
}

/// Options for [`patrol_scrub`].
#[derive(Clone, Debug)]
pub struct PatrolScrubOptions {
    /// Number of SSTs to scrub concurrently. Clamped to `>= 1` and to the table
    /// count. `1` (the default) scrubs sequentially in table order with no
    /// thread spawn. Per-SST scrubs are independent (each opens its own file
    /// through the table's `Fs` handle), so they parallelize cleanly.
    pub parallelism: usize,

    /// Minimum delay each worker waits after finishing one SST before taking
    /// the next, capping I/O pressure on a production box during a scrub.
    /// `None` (the default) runs at full speed.
    pub throttle: Option<std::time::Duration>,

    /// Persist each Page-ECC correction **in place** rather than scheduling a
    /// full-file healing rewrite: a corrected block's bytes are written back at
    /// its existing offset (size-preserving), leaving every healthy block
    /// untouched (O(damage), not O(file)). `false` (the default) keeps the
    /// classic behaviour — correct on read and queue the SST for a clean rewrite
    /// via [`HealHints`](crate::heal_hints::HealHints). Requires the `page_ecc`
    /// feature; without it the flag is inert (there is no parity to heal from).
    pub heal_in_place: bool,
}

impl Default for PatrolScrubOptions {
    fn default() -> Self {
        Self {
            parallelism: 1,
            throttle: None,
            heal_in_place: false,
        }
    }
}

impl PatrolScrubOptions {
    /// Sets the number of SSTs to scrub concurrently.
    #[must_use]
    pub const fn parallelism(mut self, workers: usize) -> Self {
        self.parallelism = workers;
        self
    }

    /// Sets the per-worker inter-SST throttle delay.
    #[must_use]
    pub const fn throttle(mut self, delay: std::time::Duration) -> Self {
        self.throttle = Some(delay);
        self
    }

    /// Enables persisting corrections in place (see
    /// [`heal_in_place`](Self::heal_in_place)).
    #[must_use]
    pub const fn heal_in_place(mut self, enable: bool) -> Self {
        self.heal_in_place = enable;
        self
    }
}

/// Runs an ECC patrol scrub over every SST in `tree`'s current version.
///
/// Reads each table's data blocks straight from disk (bypassing the block
/// cache), correcting any single-block Page-ECC fault in-flight and, when
/// [`auto_heal`](crate::runtime_config::RuntimeConfig::auto_heal) is enabled,
/// queueing each corrected SST for a healing recompaction via the tree's
/// [`HealHints`](crate::heal_hints::HealHints). Drain that queue with
/// [`EccHeal`](crate::compaction::EccHeal) (leader-only in a clustered
/// deployment) to persist the corrected bytes into fresh SSTs.
///
/// The pass always runs to completion: a block that fails its checksum and
/// cannot be recovered from parity is recorded in
/// [`PatrolScrubReport::uncorrectable_blocks`] (and logged at error level), and
/// the scrub moves on rather than aborting. SSTs written without Page ECC carry
/// no parity to correct from, so for them a scrub is an integrity *read*: a
/// checksum failure surfaces as uncorrectable.
///
/// Honours [`PatrolScrubOptions::throttle`] and
/// [`PatrolScrubOptions::parallelism`] so a scrub does not starve production
/// I/O. Off by default in the sense that it only runs when called: schedule it
/// from the cluster leader on whatever cadence the deployment wants.
///
/// # Examples
///
/// ```no_run
/// use lsm_tree::{AbstractTree, AnyTree, Config, SequenceNumberCounter};
/// use lsm_tree::scrub::{patrol_scrub, PatrolScrubOptions};
/// use std::time::Duration;
/// # fn main() -> lsm_tree::Result<()> {
/// let AnyTree::Standard(tree) = Config::new(
///     "/tmp/db",
///     SequenceNumberCounter::default(),
///     SequenceNumberCounter::default(),
/// )
/// .open()?
/// else {
///     return Ok(());
/// };
///
/// // Opt into rewrite scheduling so a scrub that corrects a block also queues
/// // the SST for a clean rewrite.
/// tree.update_runtime_config(|c| c.auto_heal = true)?;
///
/// let opts = PatrolScrubOptions::default().throttle(Duration::from_millis(50));
/// let report = patrol_scrub(&tree, &opts);
/// if !report.is_ok() {
///     eprintln!("scrub found {} uncorrectable blocks", report.uncorrectable_blocks);
/// }
/// # Ok(())
/// # }
/// ```
#[must_use]
// `Sync`: the parallel path shares `tree` with the scan workers so each can
// reconcile its table's manifest digest inside the same checkpoint-exclusion
// window as its heal scan (both tree types are `Sync`).
pub fn patrol_scrub(
    tree: &(impl AbstractTree + Sync),
    options: &PatrolScrubOptions,
) -> PatrolScrubReport {
    let version = tree.current_version();
    let tables: Vec<crate::table::Table> = version.iter_tables().cloned().collect();

    let workers = options.parallelism.max(1).min(tables.len().max(1));

    // Sequential fast path: no thread spawn, deterministic table order.
    if workers <= 1 {
        let mut report = PatrolScrubReport::default();
        for (idx, table) in tables.iter().enumerate() {
            report.merge(scan_and_reconcile(tree, table, options));
            // Inter-SST pause only; skip the sleep after the final table so a
            // finished scrub returns promptly instead of idling one extra
            // throttle interval.
            if idx + 1 < tables.len()
                && let Some(delay) = options.throttle
            {
                std::thread::sleep(delay);
            }
        }
        return report;
    }

    let cursor = AtomicUsize::new(0);
    let partials = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let mut local = PatrolScrubReport::default();
                    let mut idx = cursor.fetch_add(1, Ordering::Relaxed);
                    while let Some(table) = tables.get(idx) {
                        local.merge(scan_and_reconcile(tree, table, options));
                        // Claim the next SST first; only pause if this worker
                        // still has another table, so no worker sleeps after its
                        // final SST.
                        idx = cursor.fetch_add(1, Ordering::Relaxed);
                        if tables.get(idx).is_some()
                            && let Some(delay) = options.throttle
                        {
                            std::thread::sleep(delay);
                        }
                    }
                    local
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| match handle.join() {
                Ok(local) => local,
                // A scrub worker panicking is a bug, not a corruption finding;
                // propagate it rather than silently dropping that worker's SSTs.
                Err(payload) => std::panic::resume_unwind(payload),
            })
            .collect::<Vec<_>>()
    });

    let mut report = PatrolScrubReport::default();
    for partial in partials {
        report.merge(partial);
    }
    report
}

/// Scans one SST and, for a corruption-free heal scan, reconciles its
/// manifest digest — all under ONE checkpoint-exclusion window: a checkpoint
/// that slipped in between the heal's last write and the digest refresh
/// would link the already-healed bytes while capturing the still-stale
/// version digest, permanently desynchronizing the immutable checkpoint
/// manifest from its own files. A table without an installed pause has no
/// checkpoint machinery, so nothing can link it concurrently and the window
/// is skipped; plain (non-heal) scrubs mutate nothing and never take it.
fn scan_and_reconcile(
    tree: &impl AbstractTree,
    table: &crate::table::Table,
    options: &PatrolScrubOptions,
) -> PatrolScrubReport {
    // Only a Page-ECC table can have had its bytes legitimately healed in
    // place (this pass or an earlier one whose refresh failed), so only
    // those get the digest reconciliation. On a table WITHOUT ECC a
    // manifest digest mismatch is real evidence — an in-band alteration
    // whose block checksums were re-stamped has NO other detector — and
    // restamping would erase the only record of it; skipping also spares
    // every ordinary SST the full-file digest read. The COMPILED feature
    // gates it too: without `page_ecc` an ECC table falls back to the
    // read-only scrub (nothing can have healed), so reconciling would burn
    // a digest read only to fail closed on the walk's parity warning.
    let heals =
        cfg!(feature = "page_ecc") && options.heal_in_place && table.metadata.ecc_params.is_some();
    // Serializes same-table heals for the WHOLE scan-to-reconcile span (not
    // just the scan): with two overlapping heal patrols, A could compute a
    // digest, B could heal a fresh fault and install its own, and A would
    // then install the stale one. Also keeps the link-count probe honest
    // (the probed handle is always the current live inode).
    #[cfg(feature = "page_ecc")]
    let heal_lock = heals.then(|| table.heal_lock_arc());
    #[cfg(feature = "page_ecc")]
    let _heal_exclusive = heal_lock.as_ref().map(|l| l.lock());
    // The pause is installed on EVERY live table before it becomes visible: flush
    // outputs via `register_tables`, compaction outputs via `install_merge` and the
    // tight-space slice loop (both call `install_deletion_pause`). So for a healable
    // (Page-ECC) table `deletion_pause.get()` is `Some`, and this heal enters the
    // checkpoint mutation window rather than racing a concurrent checkpoint's
    // hard-link. `None` (a table with no pause, e.g. a diagnostic reader) just
    // skips the window.
    let _mutation_window = heals
        .then(|| {
            table
                .deletion_pause
                .get()
                .map(|p| p.enter_mutation_window())
        })
        .flatten();
    // The CURRENT view of this table, resolved under the heal lock: a
    // concurrent patrol may have refreshed the manifest — or a tight-space
    // compaction may have installed a RESTRICTED same-id view — after this
    // caller's table view was captured, and both the attribution probe and
    // the reconciliation must judge against what the manifest says NOW.
    // `None` when the table has already been compacted away; the captured
    // view and its snapshot digest then stand in (nothing left to attribute
    // against).
    let current = tree
        .current_version()
        .iter_tables()
        .find(|t| t.id() == table.id())
        .cloned();
    // When the current view's RESTRICTION differs from the captured one, the
    // captured view is stale: its file region (and so its pre-heal digest)
    // can never match the current manifest digest, which covers a different
    // region. Scanning the stale view would trip the divergent-heal guard
    // before the block walk and return a clean report with a known fault
    // untouched. Scan the CURRENT view instead — the heal lock held above is
    // SHARED across same-id views (`reopen_restricted` carries it forward),
    // so the serialization still covers the substituted view.
    let scan_table: &crate::table::Table = match &current {
        Some(cur) if cur.restrict_lower_bound() != table.restrict_lower_bound() => cur,
        _ => table,
    };
    let manifest_checksum = current
        .as_ref()
        .map_or_else(|| table.checksum(), crate::table::Table::checksum);
    let (mut partial, heal_attributable) =
        scan_one(scan_table, options, tree.sync_mode(), manifest_checksum);
    if heals
        && wants_checksum_refresh(&partial)
        && let Some(finding) = refresh_healed_checksum(tree, scan_table, heal_attributable)
    {
        partial.errors.push(finding);
    }
    partial
}

/// Reconciles every table that still carries a pending `.heal-attest` sidecar,
/// so a checkpoint about to snapshot the tree captures each table's REFRESHED
/// digest rather than the stale pre-heal one (the sidecar is not copied into
/// the checkpoint, so an unreconciled table would fail the immutable
/// checkpoint's integrity check forever with no marker to reconcile).
///
/// Runs the same per-table scan-and-reconcile as a patrol, but only for the
/// tables that actually have a pending attestation (the common case is none, so
/// this is a cheap `exists` probe per table and no scan). Must be called BEFORE
/// a checkpoint takes its link window: the reconcile acquires each table's
/// mutation window, which the link window mutually excludes.
///
/// # Errors
///
/// Returns an error if a pending-heal table could NOT be reconciled (corruption
/// remains, or the digest refresh failed): the caller must not snapshot a table
/// whose on-disk bytes disagree with the digest it would record.
#[cfg(feature = "page_ecc")]
pub(crate) fn reconcile_pending_heals(tree: &impl AbstractTree) -> crate::Result<()> {
    let options = PatrolScrubOptions::default().heal_in_place(true);
    let version = tree.current_version();
    // A sidecar-probe FAILURE aborts (fail-closed): mistaking an unreadable
    // probe for "no pending heal" would let the snapshot capture a stale digest.
    let mut pending: Vec<crate::table::Table> = Vec::new();
    for table in version.iter_tables() {
        if table.metadata.ecc_params.is_some()
            && heal_attest::exists(&*table.fs, &table.path).map_err(crate::Error::Io)?
        {
            pending.push(table.clone());
        }
    }
    if pending.is_empty() {
        return Ok(());
    }

    let mut report = PatrolScrubReport::default();
    for table in &pending {
        report.merge(scan_and_reconcile(tree, table, &options));
    }
    if report.is_ok() {
        Ok(())
    } else {
        Err(crate::Error::from(std::io::Error::other(alloc::format!(
            "checkpoint aborted: {} pending heal attestation(s) could not be reconciled \
             ({} uncorrectable block(s), {} finding(s)); run a scrub and retry",
            pending.len(),
            report.uncorrectable_blocks,
            report.errors.len(),
        ))))
    }
}

/// Abort a checkpoint if any table still carries a pending `.heal-attest`
/// marker, when the marker cannot be reconciled at this point:
///
/// - On a build WITHOUT `page_ecc` (no ECC scan machinery), used BEFORE the
///   link window in place of reconciliation.
/// - On ANY build, used AFTER the link window is held: reconciliation there is
///   impossible because it needs each table's mutation window, which the link
///   window (the write half of the same lock) mutually excludes. This closes
///   the residual race where a concurrent heal left a fresh marker between the
///   pre-window reconcile and the link-window acquisition.
///
/// Either way, snapshotting a healed table under its stale pre-heal digest with
/// no marker copied would produce an immutable checkpoint that fails integrity
/// verification forever. The operator retries; the next attempt reconciles it.
///
/// # Errors
///
/// Returns an error if any table has a pending marker, or if a marker probe
/// itself fails (fail-closed).
#[cfg(feature = "std")]
pub(crate) fn abort_checkpoint_if_pending_heals(
    tree: &impl AbstractTree,
    reason: &str,
) -> crate::Result<()> {
    let version = tree.current_version();
    for table in version.iter_tables() {
        if heal_attest::exists(&*table.fs, &table.path).map_err(crate::Error::Io)? {
            // The marker may be OBSOLETE: a heal that refreshed the manifest
            // digest but crashed (or whose best-effort unlink failed) before
            // removing the sidecar leaves a marker whose file ALREADY matches the
            // manifest. A build WITHOUT `page_ecc` never runs reconciliation to
            // clear it, so an unconditional abort would wedge EVERY checkpoint
            // forever. When the live-region digest already agrees with the
            // manifest the heal completed and snapshotting under that digest is
            // consistent: reclaim the stale marker and continue. This removal is
            // safe at both call sites — the pre-window call is `page_ecc`-free
            // (no concurrent heal can write a marker) and the post-window call
            // holds the link window that excludes heals. A digest read failure
            // falls through to the abort (fail-closed).
            if let Ok(fresh) = table.live_region_checksum()
                && fresh == table.checksum()
            {
                heal_attest::remove(&*table.fs, &table.path);
                continue;
            }
            return Err(crate::Error::from(std::io::Error::other(alloc::format!(
                "checkpoint aborted: table #{} has a pending heal attestation that cannot be \
                 reconciled here ({reason}); retry the checkpoint",
                table.id(),
            ))));
        }
    }
    Ok(())
}

/// Whether a per-SST HEAL scan warrants the manifest-digest reconciliation:
/// the scan must have left the file free of known corruption (no
/// uncorrectable blocks, no findings). Restamping a partially-healed file
/// would compute the fresh digest over the still-corrupt bytes, making a
/// later `verify_integrity` pass on an SST with known, unrepaired corruption.
///
/// Deliberately NOT gated on "this pass healed something": a refresh that
/// failed on an earlier pass (or a crash between the heal's `sync_data` and
/// the manifest update) leaves a stale digest that a later scan — which then
/// reads only clean blocks — must still reconcile, or the mismatch survives
/// forever. The reconciliation itself is a no-op when the digests already
/// agree, so a clean scan of a healthy table costs one streaming read and no
/// manifest write.
fn wants_checksum_refresh(partial: &PatrolScrubReport) -> bool {
    partial.uncorrectable_blocks == 0 && partial.errors.is_empty()
}

/// Reconciles a table's manifest digest with its on-disk bytes after a
/// corruption-free heal scan: an in-place heal (this pass or an earlier one
/// whose manifest update failed / was interrupted) may have left the digest
/// captured at recovery stale, and a later verify (or repair) would flag the
/// healed file against it. When the recomputed digest already matches the
/// manifest, nothing is written. A failure is returned as a
/// [`ScrubError::ChecksumRefreshFailed`] finding (the caller folds it into
/// the report) — the healed bytes are durable either way, and the next heal
/// scan retries the reconciliation.
/// `heal_attributable` comes from the heal scan: `true` when the file's
/// digest was probed right before the pass's first write-back and matched
/// the manifest, so every byte the file now differs by is provably one of
/// that pass's verified corrections. It is what lets a table carrying
/// deletion metadata — which no semantic gate can authenticate — still be
/// reconciled after a legitimate heal.
fn refresh_healed_checksum(
    tree: &impl AbstractTree,
    table: &crate::table::Table,
    heal_attributable: bool,
) -> Option<ScrubError> {
    let finding = |reason: String| {
        log::warn!(
            "failed to persist refreshed checksum for healed table #{}: {reason}",
            table.id(),
        );
        Some(ScrubError::ChecksumRefreshFailed {
            table_id: table.id(),
            path: (*table.path).clone(),
            reason,
        })
    };

    // Restriction-aware: a tight-space RESTRICTED view digests only its live
    // suffix (its punched prefix reads as zeros), matching the suffix digest the
    // manifest records for it.
    let fresh = match table.live_region_checksum() {
        Ok(ck) => ck,
        Err(e) => return finding(e.to_string()),
    };
    // Compare against the CURRENT manifest entry, not the caller's captured
    // view: two concurrent heal patrols capture the same version before the
    // per-table heal lock serializes them, so by the time the loser gets
    // here the winner may have already installed a refreshed checksum. The
    // captured view's stale snapshot would then flag an already-reconciled
    // file as mismatched (and, on a table the semantic gates cannot clear
    // without heal attribution, surface a spurious ChecksumRefreshFailed).
    let binding = tree.current_version();
    let Some(current_view) = binding.iter_tables().find(|t| t.id() == table.id()) else {
        // Table already compacted away while the heal ran: the old file is
        // on its way out, there is no manifest entry left to reconcile.
        return None;
    };
    // A tight-space compaction can replace the captured view with a restricted
    // same-id view (punching its prefix) while this heal runs, and it does NOT
    // take the per-table heal lock. `fresh` was hashed over the CAPTURED view's
    // live region, so installing it against a current view of a different
    // restriction would record a digest the file can never match: a whole-file
    // digest in a suffix-only manifest (or vice versa), permanently unreconcilable
    // once the prefix is punched. If the current view's restriction no longer
    // matches the captured one, abort: the current restricted view already carries
    // the digest compaction installed for it, and the next patrol reconciles it
    // under the correct restriction. Any pending marker is kept for that retry.
    if current_view.restrict_lower_bound() != table.restrict_lower_bound() {
        return None;
    }
    let current = current_view.checksum();
    if fresh == current {
        // Digest already agrees with the manifest: no version upgrade to
        // install. A `.heal-attest` sidecar here is now OBSOLETE: a prior
        // reconcile installed this digest but crashed (or its best-effort unlink
        // transiently failed) before removing the marker. Leaving it makes every
        // future checkpoint classify the table as pending and run a full heal
        // scan before snapshotting, so reclaim it now. Best-effort: a missing
        // sidecar (the common case) is a no-op.
        heal_attest::remove(&*table.fs, &table.path);
        return None;
    }

    // A mismatch is attributable to a heal either DIRECTLY (this pass wrote the
    // corrections and probed a matching pre-heal digest) or via a COMPLETED
    // attestation left by an earlier heal whose reconciliation crashed / failed.
    // That attestation binds `post == current`, so it can ONLY re-authorize the
    // exact bytes the heal produced — never an unrelated later forge. A marker
    // that bound merely `pre == manifest` (the former in-progress marker) would
    // authorize ANY structurally valid current bytes, so a crash after the
    // marker but before any heal, followed by a checksum-restamped alteration to
    // a non-authenticatable surface, could be legitimized; the heal now writes a
    // post-bound completed marker UP FRONT instead, and a bare pre-only marker
    // is ignored here. Every path still re-verifies the file structurally below
    // before the digest is reconciled.
    let attest_result = heal_attest::attests(
        &*table.fs,
        &table.path,
        table.encryption.as_deref(),
        table.id(),
        fresh,
        current,
    );
    let attributable =
        heal_attributable || matches!(attest_result, heal_attest::AttestResult::Attests);
    // A sidecar the probe could not read CONCLUSIVELY must never trigger marker
    // removal in the unattributable branch below: it may be a
    // transiently-unreadable VALID marker, and deleting it would strand the
    // healed table under the stale digest forever. Only a conclusively absent /
    // non-attesting marker is safe to clear.
    let sidecar_inconclusive = matches!(attest_result, heal_attest::AttestResult::Inconclusive);
    // Re-record the attestation before reconciling, binding the CURRENT manifest
    // digest: this is the crash-recovery insurance for the install below. If it
    // cannot be made durable, ABORT the reconcile rather than install a digest
    // whose recovery marker did not land — a crash after the install begins would
    // then strand the healed bytes under a mismatch a later patrol refuses to
    // attribute. Fail closed on the durability-primitive failure; the next patrol
    // retries once the write succeeds (any earlier up-front marker is kept for it).
    if heal_attributable
        && let Err(e) = heal_attest::write(
            &*table.fs,
            &table.path,
            table.encryption.as_deref(),
            table.id(),
            current,
            fresh,
        )
    {
        return finding(alloc::format!(
            "could not persist the reconcile attestation ({e}); the manifest digest \
             was not refreshed"
        ));
    }

    // A refusal drops the heal sidecar only when it PROVES the file is bad: an
    // attestation refused for proven corruption must not survive to authorize an
    // UNRELATED later mismatch (its `pre == manifest` binding does not expire on
    // its own, and a corrupt file needs a compaction / repair rewrite, not a
    // lingering marker). An INCONCLUSIVE refusal — a transient I/O read, or a
    // walk that could only warn (unverifiable, not corrupt, bytes) — instead
    // KEEPS the marker: the block was genuinely healed and the marker is its
    // only durable attribution, so deleting it on a retryable error would strand
    // the healed SST under the stale digest and every later clean patrol would
    // reject the reconcile forever. The completed marker binds `post ==
    // current`, so a kept marker can only ever re-authorize the SAME healed
    // bytes on retry, never a new mismatch. The transient install failure at the
    // very end keeps the marker for the same reason.
    let refuse = |reason: String, remove_marker: bool| -> Option<ScrubError> {
        if remove_marker {
            heal_attest::remove(&*table.fs, &table.path);
        }
        finding(reason)
    };
    // A cross-check that fails with an I/O error is inconclusive (the read may
    // succeed on retry), so it keeps the marker; every other error kind
    // (structural / checksum / decode) proves corruption and removes it.
    let definitive = |e: &crate::Error| !matches!(e, crate::Error::Io(_));

    // AUTHORITATIVE content has no cross-check, so an UNATTRIBUTED mismatch
    // (the pre-heal digest did NOT probe equal to the manifest, so the file's
    // difference from the manifest is not provably this pass's verified
    // corrections) must never be restamped over. Every table carries content
    // no gate below can re-derive:
    // - non-derivable meta scalars — `created_at` (a wall clock has no in-file
    //   source; FIFO trusts it for TTL) and the per-KV footer descriptor (an
    //   on/off flag). The field-for-field bounds check authenticates these
    //   only against the RECOVERY-TIME copy, which an OFFLINE restamp before
    //   open poisons, so it cannot arbitrate a re-stamp of them;
    // - range_tombstones / delete_bitmap: nothing in-file re-derives which
    //   rows or ranges were genuinely deleted;
    // - VALUE bytes of a footer-less table: a value changed behind a
    //   re-stamped block checksum decodes cleanly.
    // The manifest's whole-file digest is the ONLY surviving record of the
    // honest bytes for all three, so an unattributed mismatch fails closed
    // REGARDLESS of footers. This is checked BEFORE the out-of-band walk and
    // the semantic cross-checks below: an unattributed mismatch fails closed
    // no matter what they find, so running them first only repeats a full-file
    // verification every patrol pass until a compaction / repair rewrite
    // installs a legitimized digest. The attributable path — the pre-heal
    // digest matched the manifest, proving the file differs solely by this
    // pass's corrections — falls through to the walk + gates and reconciles.
    if !attributable {
        // Name deletion metadata specifically — a reconcile that laundered a
        // re-stamped range_tombstones / delete_bitmap would resurrect the rows
        // it masked, the scariest of the three surfaces.
        // This branch means the mismatch is NOT attributable to a heal, so no
        // marker of this pass attests the current bytes: a conclusively
        // absent / non-attesting marker is stale and clearing it is correct
        // hygiene. But if the sidecar read was INCONCLUSIVE (a transient I/O /
        // AEAD / malformed read), keep the marker: it may be a valid marker that
        // reads cleanly on retry, and deleting it would strand the healed table.
        let remove_marker = !sidecar_inconclusive;
        match table.has_deletion_metadata() {
            Ok(true) => {
                return refuse(
                    "digest mismatch not attributable to this pass's heal on a \
                     table carrying deletion metadata (range tombstones / delete \
                     bitmap), which no cross-check can authenticate; the manifest \
                     digest was not refreshed"
                        .into(),
                    remove_marker,
                );
            }
            Ok(false) => {}
            Err(e) => return refuse(e.to_string(), remove_marker),
        }
        return refuse(
            "digest mismatch not attributable to this pass's heal; the file's \
             non-derivable content (meta scalars such as created_at and the \
             per-KV footer descriptor, plus any footer-less value bytes) has no \
             cross-check to authenticate it and the recovery-time copy may \
             itself be a pre-open restamp; the manifest digest was not refreshed"
                .into(),
            remove_marker,
        );
    }

    // The heal scan covered DATA blocks only, so a digest mismatch is not
    // yet attributable to a heal: rot in a side section (filter, zone map,
    // range tombstones, block layout) also moves the digest while leaving
    // the scan clean. Walk EVERY section out-of-band before installing the
    // fresh digest: restamping over unverified bytes would launder the
    // corruption into the manifest and blind `verify_integrity` to it. The
    // walk also cross-checks each block's role against its TOC section and
    // compares the two FULLY-decoded meta mirrors, so a re-stamped
    // internally-consistent forge (a relabeled block, a tail meta whose
    // fields diverge from meta_mid) fails the refresh too. Fail closed on
    // warnings as well: an unrecognized-ECC or parity-unverifiable walk
    // skipped bytes, so the file is not provably clean.
    // A restricted view's punched data-block prefix reads as zeros; walk only
    // its live suffix. The punch-offset lookup is fallible (a volatile /
    // partitioned index read), and falling back to `0` would walk the punched
    // prefix — whose zero bytes surface as STRUCTURAL errors the definitive
    // classifier below then treats as proven corruption, removing this pass's
    // valid heal attestation while the manifest digest stays stale (later
    // patrols can no longer attribute the healed bytes). Treat a lookup failure
    // as INCONCLUSIVE instead: keep the marker for the next patrol to retry.
    let data_start = match table.restrict_lower_bound() {
        Some(bound) => match table.punch_offset_for(bound) {
            Ok(offset) => offset,
            Err(e) => {
                return refuse(
                    alloc::format!(
                        "restricted-view punch-offset lookup failed ({e}); the manifest \
                         digest was not refreshed"
                    ),
                    false,
                );
            }
        },
        None => 0,
    };
    let walk = crate::verify::verify_sst_file_with_context(
        &*table.fs,
        &table.path,
        table.encryption.as_deref(),
        Some(table.id()),
        data_start,
    );
    if !walk.errors.is_empty() || !walk.warnings.is_empty() {
        // A walk error is definitive only when it proves corruption. A transient
        // read failure (`SstFileUnreadable` / `DataReadError`) may succeed on
        // retry, and the warnings on their own (skipped, unverifiable-but-not-
        // corrupt bytes) are inconclusive — both keep the marker so a genuine
        // heal is not stranded by a flaky read or an unverifiable parity walk.
        let walk_definitive = walk.errors.iter().any(|e| {
            use crate::verify::BlockVerifyError as E;
            !matches!(e, E::SstFileUnreadable { .. } | E::DataReadError { .. })
        });
        return refuse(
            "digest mismatch with corruption outside the scanned data blocks; \
             the manifest digest was not refreshed"
                .into(),
            walk_definitive,
        );
    }

    // The walk verifies raw block checksums but never DECODES entries, so a
    // stale per-KV footer behind a re-stamped block checksum still reads
    // clean at the block level. Footer-bearing tables get the per-KV
    // verification too before the digest is trusted (a table without
    // footers makes this a no-op).
    if let Err(e) = table.verify_kv_checksums() {
        return refuse(
            alloc::format!(
                "digest mismatch with a per-KV verification failure ({e}); the \
             manifest digest was not refreshed"
            ),
            definitive(&e),
        );
    }

    // The `linked_blob_files` section carries no per-section checksum and
    // the walk can only validate its SHAPE, so a flipped blob id passes it.
    // Cross-check the recorded ids against the table's own indirection
    // entries (a no-op without the section) before trusting the digest.
    if let Err(e) = table.verify_blob_links() {
        return refuse(
            alloc::format!(
                "digest mismatch with a blob-link cross-check failure ({e}); \
             the manifest digest was not refreshed"
            ),
            definitive(&e),
        );
    }

    // Each TLI mirror verified independently clean above, but a forged tail
    // that DECODES to a different handle list than the head passes every
    // byte-level check — and the next recovery prefers the tail, silently
    // hiding blocks. Compare the decoded mirrors before trusting the digest.
    if let Err(e) = table.verify_tli_mirrors() {
        return refuse(
            alloc::format!(
                "digest mismatch with a TLI mirror comparison failure ({e}); \
             the manifest digest was not refreshed"
            ),
            definitive(&e),
        );
    }

    // The seqno_bounds block is checksum-clean to the walk even when its
    // payload was re-stamped to another structurally valid map, and
    // scan_since_seqno trusts it to SKIP blocks. Cross-check every recorded
    // range against the blocks' decoded entries (a no-op without the
    // section) before trusting the digest.
    if let Err(e) = table.verify_seqno_bounds() {
        return refuse(
            alloc::format!(
                "digest mismatch with a seqno-bounds cross-check failure ({e}); \
             the manifest digest was not refreshed"
            ),
            definitive(&e),
        );
    }

    // The walk verifies only the outer frame, so a checksum-clean block whose
    // trailer declares more entries than it decodes reads clean; full-decode
    // every block and confirm the counts match before trusting the digest,
    // or restamping would legitimize a silently-truncated tail.
    if let Err(e) = table.verify_block_entry_counts() {
        return refuse(
            alloc::format!(
                "digest mismatch with a block entry-count mismatch ({e}); \
             the manifest digest was not refreshed"
            ),
            definitive(&e),
        );
    }

    // The zone_map block is checksum-clean to the walk even when its payload
    // was re-stamped to another structurally valid map, and a predicate scan
    // trusts its min/max to SKIP blocks. Cross-check every recorded range
    // against the blocks' decoded key ranges (a no-op without the section)
    // before trusting the digest.
    if let Err(e) = table.verify_zone_map() {
        return refuse(
            alloc::format!(
                "digest mismatch with a zone-map cross-check failure ({e}); \
             the manifest digest was not refreshed"
            ),
            definitive(&e),
        );
    }

    // The locator block is checksum-clean to the walk even when re-stamped to
    // resolve a key to a block other than its newest-version block, and
    // point_read trusts its answer. Cross-check every key's mapping against
    // its decoded newest-version block (a no-op without the section) before
    // trusting the digest.
    if let Err(e) = table.verify_locator() {
        return refuse(
            alloc::format!(
                "digest mismatch with a locator cross-check failure ({e}); \
             the manifest digest was not refreshed"
            ),
            definitive(&e),
        );
    }

    // The filter block is checksum-clean to the walk even when its payload
    // was re-stamped to another parseable filter, and check_bloom trusts it
    // to SKIP point reads — a key made into a false negative silently
    // disappears from every read. Probe every decoded key against the
    // on-disk filter (a no-op without one) before trusting the digest.
    if let Err(e) = table.verify_filter(tree.prefix_extractor().as_ref()) {
        return refuse(
            alloc::format!(
                "digest mismatch with a filter cross-check failure ({e}); \
             the manifest digest was not refreshed"
            ),
            definitive(&e),
        );
    }

    // The block_layout block is checksum-clean to the walk even when a
    // cumulative end was re-stamped to another structurally valid value, and
    // the partial range-read path trusts it to bound decompression — a
    // mis-mapped boundary silently omits keys. Cross-check every recorded
    // boundary against the frames' actual inner blocks (a no-op without the
    // section) before trusting the digest.
    if let Err(e) = table.verify_block_layout() {
        return refuse(
            alloc::format!(
                "digest mismatch with a block-layout cross-check failure ({e}); \
             the manifest digest was not refreshed"
            ),
            definitive(&e),
        );
    }

    // A data block's embedded hash / binary index is checksum-clean to the
    // walk even when a bucket was re-stamped to MARKER_FREE, yet point_read
    // trusts it and returns None for the affected keys. Probe every decoded
    // key through the full point-read path before trusting the digest.
    if let Err(e) = table.verify_point_read_reachability() {
        return refuse(
            alloc::format!(
                "digest mismatch with a point-read reachability failure ({e}); \
             the manifest digest was not refreshed"
            ),
            definitive(&e),
        );
    }

    // Both meta mirrors re-stamped CONSISTENTLY pass the mirror comparison,
    // yet run selection trusts the recorded key range to route reads AROUND
    // this table — a narrowed range silently hides real keys and the range
    // tombstones that mask older tables. Cross-check the recorded bounds
    // against the decoded contents before trusting the digest.
    if let Err(e) = table.verify_metadata_bounds() {
        return refuse(
            alloc::format!(
                "digest mismatch with a metadata-bounds cross-check failure ({e}); \
             the manifest digest was not refreshed"
            ),
            definitive(&e),
        );
    }

    // Pass the captured view's restriction so the install, under its own lock,
    // rejects the refresh if a compaction swapped the current view to a different
    // restriction after the pre-window check below (closing that TOCTOU: `fresh`
    // describes the captured region and must not be recorded against a view of a
    // different restriction).
    use crate::abstract_tree::ChecksumRefreshOutcome;
    match tree.refresh_table_checksum(table.id(), fresh, table.restrict_lower_bound()) {
        Ok(ChecksumRefreshOutcome::Refreshed) => {
            // The manifest now holds a legitimate digest; the attestation that
            // may have authorized this reconciliation has served its purpose.
            heal_attest::remove(&*table.fs, &table.path);
            None
        }
        // No-op install: the table was compacted away, or a restriction swap made
        // the digest inapplicable to the current view. The manifest digest is
        // unchanged, so KEEP the marker — the next patrol reconciles the current
        // view through it rather than losing the only attribution.
        Ok(ChecksumRefreshOutcome::Stale) => None,
        // The install lock was held by a concurrent compaction. The healed
        // bytes are durable while the manifest digest stays stale and the
        // attestation stays pending: a clean report would mislead a later
        // integrity check (mismatch) or a checkpoint (abort on the pending
        // reconcile). Surface a finding; the kept marker lets the next patrol
        // retry once the compaction releases the state.
        Ok(ChecksumRefreshOutcome::Contended) => finding(
            "the manifest install lock was held by a concurrent compaction; the \
             healed bytes are durable but the manifest digest is stale until a \
             later patrol reconciles the kept attestation"
                .to_string(),
        ),
        Err(e) => finding(e.to_string()),
    }
}

/// Scans one SST: heals corrections in place when
/// [`heal_in_place`](PatrolScrubOptions::heal_in_place) is set (and `page_ecc` is
/// built), otherwise runs the classic correct-on-read + schedule-rewrite scrub.
/// The `bool` is the heal-attribution flag (see
/// [`refresh_healed_checksum`]); always `false` on the read-only scrub path,
/// which never writes.
fn scan_one(
    table: &crate::table::Table,
    options: &PatrolScrubOptions,
    sync_mode: crate::fs::SyncMode,
    manifest_checksum: crate::Checksum,
) -> (PatrolScrubReport, bool) {
    // In-place heal only applies to SSTs written with Page-ECC parity — there is
    // nothing to reconstruct without it. A table with no ECC still needs its
    // integrity checked, so it takes the normal scrub path (which verifies each
    // block's checksum and reports uncorrectable ones) rather than the heal path,
    // whose per-block reconstruction is a no-op there.
    #[cfg(feature = "page_ecc")]
    if options.heal_in_place && table.metadata.ecc_params.is_some() {
        return table.heal_data_blocks_in_place(sync_mode, manifest_checksum);
    }
    let _ = (options, sync_mode, manifest_checksum);
    (table.scrub_data_blocks(), false)
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "page_ecc"))]
mod ecc_tests;
