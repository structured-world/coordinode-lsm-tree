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
    /// scheduling is suppressed).
    pub ssts_scheduled_for_rewrite: usize,
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
    /// corrected (no uncorrectable corruption was found).
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.uncorrectable_blocks == 0
    }

    /// Folds a per-SST partial report into this accumulator.
    fn merge(&mut self, other: Self) {
        self.sst_files_scanned += other.sst_files_scanned;
        self.blocks_scanned += other.blocks_scanned;
        self.corrections_applied += other.corrections_applied;
        self.ssts_scheduled_for_rewrite += other.ssts_scheduled_for_rewrite;
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
}

impl Default for PatrolScrubOptions {
    fn default() -> Self {
        Self {
            parallelism: 1,
            throttle: None,
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
pub fn patrol_scrub(tree: &impl AbstractTree, options: &PatrolScrubOptions) -> PatrolScrubReport {
    let version = tree.current_version();
    let tables: Vec<crate::table::Table> = version.iter_tables().cloned().collect();

    let workers = options.parallelism.max(1).min(tables.len().max(1));

    // Sequential fast path: no thread spawn, deterministic table order.
    if workers <= 1 {
        let mut report = PatrolScrubReport::default();
        for (idx, table) in tables.iter().enumerate() {
            report.merge(table.scrub_data_blocks());
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
                        local.merge(table.scrub_data_blocks());
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

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "page_ecc"))]
mod ecc_tests;
