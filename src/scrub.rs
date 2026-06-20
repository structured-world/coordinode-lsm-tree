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
mod tests {
    #![expect(
        clippy::expect_used,
        reason = "tests assert on known-present values; a panic is the failure signal"
    )]

    use super::*;
    use crate::{AbstractTree, AnyTree, Config, SequenceNumberCounter};

    fn standard_tree(dir: &std::path::Path) -> AnyTree {
        Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()
        .expect("open tree")
    }

    #[test]
    fn report_merge_sums_every_counter_and_concatenates_errors() {
        let mut acc = PatrolScrubReport {
            sst_files_scanned: 1,
            blocks_scanned: 10,
            corrections_applied: 2,
            ssts_scheduled_for_rewrite: 1,
            uncorrectable_blocks: 0,
            errors: vec![],
        };
        acc.merge(PatrolScrubReport {
            sst_files_scanned: 2,
            blocks_scanned: 5,
            corrections_applied: 1,
            ssts_scheduled_for_rewrite: 1,
            uncorrectable_blocks: 3,
            errors: vec![ScrubError::UncorrectableBlock {
                table_id: 7,
                path: "/x".into(),
                block_offset: 42,
                reason: "boom".into(),
            }],
        });
        assert_eq!(acc.sst_files_scanned, 3);
        assert_eq!(acc.blocks_scanned, 15);
        assert_eq!(acc.corrections_applied, 3);
        assert_eq!(acc.ssts_scheduled_for_rewrite, 2);
        assert_eq!(acc.uncorrectable_blocks, 3);
        assert_eq!(acc.errors.len(), 1);
    }

    #[test]
    fn report_is_ok_only_when_no_uncorrectable_blocks() {
        let mut report = PatrolScrubReport::default();
        assert!(report.is_ok(), "a fresh empty report is ok");
        report.corrections_applied = 5;
        assert!(report.is_ok(), "corrected blocks do not make a scrub fail");
        report.uncorrectable_blocks = 1;
        assert!(!report.is_ok(), "an uncorrectable block fails the scrub");
    }

    #[test]
    fn options_builder_sets_parallelism_and_throttle() {
        let opts = PatrolScrubOptions::default()
            .parallelism(4)
            .throttle(std::time::Duration::from_millis(7));
        assert_eq!(opts.parallelism, 4);
        assert_eq!(opts.throttle, Some(std::time::Duration::from_millis(7)));
    }

    #[test]
    fn patrol_scrub_on_clean_non_ecc_tree_reads_blocks_without_findings() {
        let dir = tempfile::tempdir().expect("tempdir");
        let AnyTree::Standard(tree) = standard_tree(dir.path()) else {
            unreachable!("standard tree configured");
        };
        for i in 0u64..500 {
            tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
        }
        tree.flush_active_memtable(500).expect("flush");

        let report = patrol_scrub(&tree, &PatrolScrubOptions::default());
        assert_eq!(report.sst_files_scanned, 1, "one flushed SST");
        assert!(report.blocks_scanned >= 1, "at least one data block read");
        assert_eq!(report.corrections_applied, 0, "no ECC, nothing to correct");
        assert_eq!(
            report.uncorrectable_blocks, 0,
            "clean tree has no corruption"
        );
        assert!(report.is_ok());
    }

    #[test]
    fn patrol_scrub_empty_tree_scans_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let AnyTree::Standard(tree) = standard_tree(dir.path()) else {
            unreachable!("standard tree configured");
        };
        let report = patrol_scrub(&tree, &PatrolScrubOptions::default());
        assert_eq!(report.sst_files_scanned, 0);
        assert_eq!(report.blocks_scanned, 0);
        assert!(report.is_ok());
    }

    #[test]
    fn patrol_scrub_parallel_over_many_ssts_visits_every_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let AnyTree::Standard(tree) = standard_tree(dir.path()) else {
            unreachable!("standard tree configured");
        };
        // Flush four times → four SSTs (no compaction triggered at this size).
        for batch in 0u64..4 {
            for i in 0u64..200 {
                let k = batch * 1_000 + i;
                tree.insert(format!("key-{k:06}"), format!("v{k:06}"), k);
            }
            tree.flush_active_memtable((batch + 1) * 1_000)
                .expect("flush");
        }

        let opts = PatrolScrubOptions::default()
            .parallelism(3)
            .throttle(std::time::Duration::from_millis(1));
        let report = patrol_scrub(&tree, &opts);
        assert_eq!(report.sst_files_scanned, 4, "every SST scrubbed once");
        assert!(report.blocks_scanned >= 4);
        assert!(report.is_ok());
    }
}

#[cfg(all(test, feature = "page_ecc"))]
mod ecc_tests {
    #![expect(
        clippy::expect_used,
        reason = "tests assert on known-present values; a panic is the failure signal"
    )]
    // Target-conditional: `u64 as usize` on a block offset only narrows on
    // 32-bit pointer widths, so clippy does NOT fire on the 64-bit CI host;
    // `allow` (not `expect`, which would be unfulfilled there).
    #![allow(
        clippy::cast_possible_truncation,
        reason = "in-file block offsets fit usize; only narrow on 32-bit targets"
    )]

    use super::*;
    use crate::{
        AbstractTree,
        MAX_SEQNO,
        SequenceNumberCounter,
        runtime_config::EccScheme,
        // `BlockIndex` is imported only for its `.iter()` method on
        // `table.block_index` (a trait method); `as _` keeps it in scope for
        // method resolution without binding the unused type name.
        table::{block::Header, block_index::BlockIndex as _},
    };

    /// Opens an RS(8,2) Page-ECC tree at `dir`.
    fn open_ecc_tree(dir: &std::path::Path) -> crate::Tree {
        let crate::AnyTree::Standard(tree) = crate::Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .page_ecc(true)
        .ecc_scheme(EccScheme::ReedSolomon {
            data_shards: 8,
            parity_shards: 2,
        })
        .open()
        .expect("open ecc tree") else {
            unreachable!("standard tree configured (no kv separation)");
        };
        tree
    }

    /// Writes one ECC SST under `dir` and returns `(sst_path, first_data_block)`.
    fn write_ecc_sst(dir: &std::path::Path) -> (std::path::PathBuf, crate::table::BlockHandle) {
        let tree = open_ecc_tree(dir);
        for i in 0u64..2_000 {
            tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
        }
        tree.flush_active_memtable(2_000).expect("flush");

        let binding = tree.version_history.read().latest_version();
        let table = binding
            .version
            .iter_tables()
            .next()
            .expect("flush produced one table");
        let keyed = table
            .block_index
            .iter()
            .next()
            .expect("table has at least one data block")
            .expect("block index entry decodes");
        let handle = crate::table::BlockHandle::new(keyed.offset(), keyed.size());
        ((*table.path).clone(), handle)
    }

    #[test]
    fn patrol_scrub_corrects_seeded_single_bit_fault_and_schedules_heal() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let (sst_path, block) = write_ecc_sst(dir.path());

        // Flip one payload byte of the first data block (RS-correctable: a single
        // byte error is within the RS(8,2) budget).
        let corrupt_pos = block.offset().0 as usize + Header::MIN_LEN + 3;
        let mut bytes = std::fs::read(&sst_path)?;
        let slot = bytes
            .get_mut(corrupt_pos)
            .expect("corrupt_pos in range for the SST bytes");
        *slot ^= 0x80;
        std::fs::write(&sst_path, &bytes)?;

        // Reopen (fresh caches + fds) and opt into rewrite scheduling.
        let tree = open_ecc_tree(dir.path());
        tree.update_runtime_config(|c| c.auto_heal = true)?;
        assert!(tree.heal_hints().is_empty(), "fresh tree has no hints");

        let report = patrol_scrub(&tree, &PatrolScrubOptions::default());

        assert!(
            report.corrections_applied >= 1,
            "scrub must correct the seeded fault: {report:?}",
        );
        assert_eq!(
            report.ssts_scheduled_for_rewrite, 1,
            "the corrected SST is queued for healing exactly once: {report:?}",
        );
        assert_eq!(report.uncorrectable_blocks, 0, "{report:?}");
        assert!(
            report.is_ok(),
            "a fully-correctable scrub is ok: {report:?}"
        );
        assert!(
            !tree.heal_hints().is_empty(),
            "the SST is recorded in the heal queue",
        );
        #[cfg(feature = "metrics")]
        assert_eq!(
            tree.metrics().ecc_auto_heal_scheduled_count(),
            1,
            "the scheduled SST is counted once in metrics",
        );
        Ok(())
    }

    #[test]
    fn patrol_scrub_corrects_without_scheduling_when_auto_heal_off() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let (sst_path, block) = write_ecc_sst(dir.path());

        let corrupt_pos = block.offset().0 as usize + Header::MIN_LEN + 3;
        let mut bytes = std::fs::read(&sst_path)?;
        let slot = bytes.get_mut(corrupt_pos).expect("corrupt_pos in range");
        *slot ^= 0x80;
        std::fs::write(&sst_path, &bytes)?;

        // Reopen WITHOUT enabling auto_heal (default off).
        let tree = open_ecc_tree(dir.path());
        assert!(!tree.heal_hints().is_enabled(), "auto_heal defaults off");

        let report = patrol_scrub(&tree, &PatrolScrubOptions::default());

        assert!(
            report.corrections_applied >= 1,
            "correction-on-read still happens with auto_heal off: {report:?}",
        );
        assert_eq!(
            report.ssts_scheduled_for_rewrite, 0,
            "auto_heal off suppresses rewrite scheduling: {report:?}",
        );
        assert!(
            tree.heal_hints().is_empty(),
            "no SST queued when scheduling is off",
        );
        assert!(report.is_ok());
        Ok(())
    }

    #[test]
    fn patrol_scrub_reports_uncorrectable_block_not_silently_skipped() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let (sst_path, block) = write_ecc_sst(dir.path());

        // Wreck the whole payload+parity of the first data block (header left
        // intact so it still parses): far beyond the RS(8,2) correction budget,
        // so the block is uncorrectable.
        let payload_start = block.offset().0 as usize + Header::MIN_LEN;
        let payload_end = block.offset().0 as usize + block.size() as usize;
        let mut bytes = std::fs::read(&sst_path)?;
        for slot in bytes
            .get_mut(payload_start..payload_end)
            .expect("block payload range in bounds")
        {
            *slot ^= 0xFF;
        }
        std::fs::write(&sst_path, &bytes)?;

        let tree = open_ecc_tree(dir.path());
        tree.update_runtime_config(|c| c.auto_heal = true)?;

        let report = patrol_scrub(&tree, &PatrolScrubOptions::default());

        assert!(
            report.uncorrectable_blocks >= 1,
            "an unrecoverable block must be reported, not skipped: {report:?}",
        );
        assert!(!report.is_ok(), "uncorrectable corruption fails the scrub");
        assert!(
            report
                .errors
                .iter()
                .any(|e| matches!(e, ScrubError::UncorrectableBlock { .. })),
            "the finding is an UncorrectableBlock: {report:?}",
        );
        Ok(())
    }

    #[test]
    fn patrol_scrub_clean_ecc_tree_reports_no_corrections() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let _ = write_ecc_sst(dir.path());

        let tree = open_ecc_tree(dir.path());
        let report = patrol_scrub(&tree, &PatrolScrubOptions::default());

        assert_eq!(report.sst_files_scanned, 1);
        assert!(report.blocks_scanned >= 1);
        assert_eq!(report.corrections_applied, 0, "no fault → no correction");
        assert_eq!(report.uncorrectable_blocks, 0);
        assert!(report.is_ok());

        // Sanity: a clean read of a key still returns the right value.
        let got = tree.get(b"key-000000", MAX_SEQNO)?.expect("key present");
        assert_eq!(&*got, b"v000000");
        Ok(())
    }
}
