// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Tree-wide queue of SSTs whose blocks were ECC-corrected on a read that was
//! confirmed *persistent* (the on-disk bytes are still faulty after a
//! cache-bypassing re-read).
//!
//! A block-read that finds a checksum mismatch but recovers the payload from
//! its Page-ECC parity returns the *correct* bytes to the caller, yet leaves
//! the faulty bytes on disk. Without intervention the same latent fault is
//! re-corrected on every future read, and a second bit-flip in the same block
//! could push it past the parity's correction budget into unrecoverable
//! territory. Recording the SST here lets the compaction picker rewrite it
//! clean (a new file + atomic manifest swap) instead.
//!
//! # Why a tree-wide set and not a per-table flag
//!
//! Tables only know their own id + [`crate::fs::Fs`]; they have no
//! back-reference to the owning tree. Each table carries an
//! [`Arc<HealHints>`] (installed once after it joins a tree, [`None`] before
//! that) so the read path can record a hint in O(log n) (a `BTreeSet` insert,
//! n = pending SSTs, only on the cold corrected path), and the compaction picker
//! drains the whole set in one place. The set dedups by
//! [`GlobalTableId`]: many corrected block-reads against
//! the same SST enqueue it once.

use crate::GlobalTableId;
use alloc::collections::BTreeSet;
use alloc::sync::Arc;
#[cfg(test)]
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};
use spin::Mutex;

/// Shared set of SSTs awaiting priority recompaction after a confirmed
/// persistent ECC correction.
///
/// Cheap to clone (it is always held behind an [`Arc`]). The inner mutex is
/// only ever contended on the cold corrected-read path and the periodic
/// compaction drain, never on a clean read.
// `spin::Mutex` rather than `std::sync::Mutex` keeps this module's std
// footprint at zero — it is `no_std` + `alloc` by construction, matching
// `crate::deletion_pause`. The set is never touched on a clean read, so
// spin contention is irrelevant in practice.
#[derive(Default)]
pub struct HealHints {
    /// SSTs queued for healing recompaction, deduped by id.
    pending: Mutex<BTreeSet<GlobalTableId>>,

    /// Mirrors [`RuntimeConfig::auto_heal`](crate::runtime_config::RuntimeConfig::auto_heal).
    /// When `false`, the read path skips the persistence re-read and records
    /// nothing — correction-on-read still happens, only the rewrite scheduling
    /// is suppressed. The owning tree keeps this in sync with its runtime
    /// config. Default `false` (scheduling is opt-in).
    enabled: AtomicBool,
}

impl HealHints {
    /// Creates a fresh, empty shared hint set with scheduling set to `enabled`
    /// (mirrors the tree's initial `auto_heal`).
    #[must_use]
    pub fn new_shared(enabled: bool) -> Arc<Self> {
        let hints = Self::default();
        hints.set_enabled(enabled);
        Arc::new(hints)
    }

    /// Returns `true` when rewrite scheduling is enabled (mirrors
    /// [`RuntimeConfig::auto_heal`](crate::runtime_config::RuntimeConfig::auto_heal)).
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Enables or disables rewrite scheduling. The owning tree calls this on open
    /// and on every runtime-config update so the read-path gate tracks
    /// `auto_heal`.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Records `id` as needing a healing recompaction.
    ///
    /// Returns `true` when the id was newly inserted, `false` when it was
    /// already queued — callers use this to count each SST once.
    pub fn record(&self, id: GlobalTableId) -> bool {
        self.pending.lock().insert(id)
    }

    /// Claims one queued id, removing and returning it (`None` when empty).
    ///
    /// The heal compaction strategy pops one SST per pass; losing a popped id to
    /// a failed compaction is self-correcting (the next read re-records it).
    #[must_use]
    pub fn pop(&self) -> Option<GlobalTableId> {
        self.pending.lock().pop_first()
    }

    /// Returns `true` when no SST is currently queued for healing.
    ///
    /// Lets a scheduler skip running the heal strategy when there is nothing to
    /// do.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.lock().is_empty()
    }

    /// Returns a snapshot of the queued ids (test-only helper).
    #[cfg(test)]
    #[must_use]
    pub fn snapshot(&self) -> Vec<GlobalTableId> {
        self.pending.lock().iter().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(tree: u64, table: u64) -> GlobalTableId {
        GlobalTableId::from((tree, table))
    }

    #[test]
    fn record_dedups_same_id_returns_false_on_repeat() {
        let hints = HealHints::default();
        assert!(hints.record(id(1, 7)));
        assert!(!hints.record(id(1, 7)));
        assert_eq!(hints.snapshot(), vec![id(1, 7)]);
    }

    #[test]
    fn record_collects_distinct_ids() {
        let hints = HealHints::default();
        hints.record(id(1, 1));
        hints.record(id(1, 2));
        hints.record(id(1, 1));
        let snapshot = hints.snapshot();
        assert_eq!(snapshot, vec![id(1, 1), id(1, 2)]);
    }
}
