// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Compaction strategy that rewrites SSTs flagged for ECC self-healing.
//!
//! A block read that recovers its payload from Page-ECC parity returns correct
//! bytes but leaves the fault on disk (SSTs are immutable, so the block cannot
//! be patched). The read path records the owning SST in the tree's
//! [`HealHints`]; this strategy claims one such SST per pass and emits a
//! single-table [`Merge`](super::Choice::Merge) back into its own level. The
//! merge re-reads the block (correcting it once more on the way in) and writes a
//! fresh SST with newly computed parity, so subsequent reads need no correction.
//!
//! Run it repeatedly (from the compaction loop, leader-only in a clustered
//! deployment) until [`HealHints::is_empty`] reports the queue drained.

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{
    HashSet, compaction::state::CompactionState, config::Config, heal_hints::HealHints,
    version::Version,
};
use alloc::sync::Arc;

/// Name reported by [`CompactionStrategy::get_name`].
pub const NAME: &str = "EccHealCompaction";

/// Rewrites one ECC-flagged SST per invocation to clear a latent parity fault.
///
/// Holds a shared handle to the tree's [`HealHints`]; obtain it from
/// [`Tree::heal_hints`](crate::Tree::heal_hints).
pub struct Strategy {
    hints: Arc<HealHints>,
    target_size: u64,
}

impl Strategy {
    /// Builds a heal strategy over `hints`.
    ///
    /// `target_size` caps the rewritten output run's table size (use the level's
    /// target, or [`u64::MAX`] to keep the SST a single table).
    #[must_use]
    pub fn new(hints: Arc<HealHints>, target_size: u64) -> Self {
        Self { hints, target_size }
    }
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        NAME
    }

    fn choose(&self, version: &Version, _cfg: &Config, state: &CompactionState) -> Choice {
        // Claim flagged SSTs one at a time. An id no longer in the tree (already
        // compacted away since the hint) is dropped; one currently hidden in
        // another compaction is re-queued for the next pass.
        while let Some(global_id) = self.hints.pop() {
            let table_id = global_id.table_id();

            let Some(level_idx) = version
                .iter_levels()
                .position(|level| level.list_ids().contains(&table_id))
            else {
                // Gone — nothing left to heal for this id.
                continue;
            };

            if state.hidden_set().is_hidden(table_id) {
                // Busy in another compaction; put it back and try next pass.
                self.hints.record(global_id);
                return Choice::DoNothing;
            }

            #[expect(
                clippy::cast_possible_truncation,
                reason = "level index is bounded by level_count, which is a u8"
            )]
            let level = level_idx as u8;

            return Choice::Merge(CompactionInput {
                table_ids: core::iter::once(table_id).collect::<HashSet<_>>(),
                dest_level: level,
                canonical_level: level,
                target_size: self.target_size,
            });
        }

        Choice::DoNothing
    }
}

#[cfg(test)]
mod strategy_tests;

#[cfg(all(test, feature = "page_ecc"))]
mod tests;
