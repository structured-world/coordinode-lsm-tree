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
    // every ordinary SST the full-file digest read.
    let heals = options.heal_in_place && table.metadata.ecc_params.is_some();
    // Serializes same-table heals for the WHOLE scan-to-reconcile span (not
    // just the scan): with two overlapping heal patrols, A could compute a
    // digest, B could heal a fresh fault and install its own, and A would
    // then install the stale one. Also keeps the link-count probe honest
    // (the probed handle is always the current live inode).
    #[cfg(feature = "page_ecc")]
    let _heal_exclusive = heals.then(|| table.heal_lock.lock());
    let _mutation_window = heals
        .then(|| {
            table
                .deletion_pause
                .get()
                .map(|p| p.enter_mutation_window())
        })
        .flatten();
    let mut partial = scan_one(table, options);
    if heals
        && wants_checksum_refresh(&partial)
        && let Some(finding) = refresh_healed_checksum(tree, table)
    {
        partial.errors.push(finding);
    }
    partial
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
fn refresh_healed_checksum(
    tree: &impl AbstractTree,
    table: &crate::table::Table,
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

    let fresh = match crate::repair::compute_table_checksum(&*table.fs, &table.path) {
        Ok(raw) => crate::Checksum::from_raw(raw),
        Err(e) => return finding(e.to_string()),
    };
    if fresh == table.checksum() {
        // Digest already agrees with the manifest: no pending heal to
        // reconcile, no version upgrade to install.
        return None;
    }

    // The heal scan covered DATA blocks only, so a digest mismatch is not
    // yet attributable to a heal: rot in a side section (filter, zone map,
    // range tombstones, block layout) also moves the digest while leaving
    // the scan clean. Walk EVERY section out-of-band before installing the
    // fresh digest: restamping over unverified bytes would launder the
    // corruption into the manifest and blind `verify_integrity` to it.
    // Fail closed on warnings too: an unrecognized-ECC or
    // parity-unverifiable walk skipped bytes, so the file is not provably
    // clean.
    let walk = crate::verify::verify_sst_file_with_context(
        &*table.fs,
        &table.path,
        table.encryption.as_deref(),
        Some(table.id()),
    );
    if !walk.errors.is_empty() || !walk.warnings.is_empty() {
        return finding(
            "digest mismatch with corruption outside the scanned data blocks; \
             the manifest digest was not refreshed"
                .into(),
        );
    }

    // The walk verifies raw block checksums but never DECODES entries, so a
    // stale per-KV footer behind a re-stamped block checksum still reads
    // clean at the block level. Footer-bearing tables get the per-KV
    // verification too before the digest is trusted (a table without
    // footers makes this a no-op).
    if let Err(e) = table.verify_kv_checksums() {
        return finding(alloc::format!(
            "digest mismatch with a per-KV verification failure ({e}); the \
             manifest digest was not refreshed"
        ));
    }

    // The `linked_blob_files` section carries no per-section checksum and
    // the walk can only validate its SHAPE, so a flipped blob id passes it.
    // Cross-check the recorded ids against the table's own indirection
    // entries (a no-op without the section) before trusting the digest.
    if let Err(e) = table.verify_blob_links() {
        return finding(alloc::format!(
            "digest mismatch with a blob-link cross-check failure ({e}); \
             the manifest digest was not refreshed"
        ));
    }

    match tree.refresh_table_checksum(table.id(), fresh) {
        Ok(()) => None,
        Err(e) => finding(e.to_string()),
    }
}

/// Scans one SST: heals corrections in place when
/// [`heal_in_place`](PatrolScrubOptions::heal_in_place) is set (and `page_ecc` is
/// built), otherwise runs the classic correct-on-read + schedule-rewrite scrub.
fn scan_one(table: &crate::table::Table, options: &PatrolScrubOptions) -> PatrolScrubReport {
    // In-place heal only applies to SSTs written with Page-ECC parity — there is
    // nothing to reconstruct without it. A table with no ECC still needs its
    // integrity checked, so it takes the normal scrub path (which verifies each
    // block's checksum and reports uncorrectable ones) rather than the heal path,
    // whose per-block reconstruction is a no-op there.
    #[cfg(feature = "page_ecc")]
    if options.heal_in_place && table.metadata.ecc_params.is_some() {
        return table.heal_data_blocks_in_place();
    }
    let _ = options;
    table.scrub_data_blocks()
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "page_ecc"))]
mod ecc_tests;
