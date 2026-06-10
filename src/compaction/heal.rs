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
mod strategy_tests {
    use crate::{AbstractTree, AnyTree, Config, GlobalTableId, SequenceNumberCounter};
    use alloc::sync::Arc;

    /// A queued id that is no longer present in the tree (already compacted away
    /// since the hint) is dropped: the strategy drains it and chooses nothing,
    /// rather than emitting a merge for a missing table.
    #[test]
    fn ecc_heal_drops_ids_no_longer_in_the_tree() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let AnyTree::Standard(tree) = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?
        else {
            unreachable!("standard tree configured (no kv separation)");
        };
        tree.insert("k", "v", 1);
        tree.flush_active_memtable(1)?;

        // Flag an SST id that does not exist in this tree.
        let hints = tree.heal_hints();
        hints.record(GlobalTableId::from((tree.id(), 999_999)));
        assert!(!hints.is_empty());

        let result = tree.compact(Arc::new(super::Strategy::new(hints.clone(), u64::MAX)), 0)?;
        assert!(
            hints.is_empty(),
            "a stale id must be drained, not left queued; got {result:?}",
        );
        Ok(())
    }
}

#[cfg(all(test, feature = "page_ecc"))]
mod tests {
    use crate::{
        AbstractTree, Config, MAX_SEQNO, SequenceNumberCounter,
        runtime_config::EccScheme,
        table::{block::Header, block_index::BlockIndex},
    };
    use alloc::sync::Arc;

    /// Full self-heal cycle: a read that recovers a corrupted data block via RS
    /// parity records the SST; running [`super::Strategy`] rewrites that SST so
    /// the re-read is clean and records no further hint.
    #[test]
    fn ecc_heal_strategy_rewrites_flagged_sst_so_reread_is_clean() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        // Write one SST whose data blocks carry RS(8,2) parity, then capture the
        // first data block's on-disk offset before dropping the tree.
        let (sst_path, corrupt_pos) = {
            let tree = Config::new(
                dir.path(),
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .page_ecc(true)
            .ecc_scheme(EccScheme::ReedSolomon {
                data_shards: 8,
                parity_shards: 2,
            })
            .open()?;
            let crate::AnyTree::Standard(tree) = tree else {
                unreachable!("standard tree configured (no kv separation)");
            };
            for i in 0u64..2_000 {
                tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
            }
            tree.flush_active_memtable(2_000)?;

            #[expect(clippy::expect_used, reason = "test asserts the tree shape")]
            let versions = tree.version_history.read().expect("lock not poisoned");
            let binding = versions.latest_version();
            #[expect(clippy::expect_used, reason = "flush produced exactly one table")]
            let table = binding.version.iter_tables().next().expect("one table");
            #[expect(clippy::expect_used, reason = "table has at least one data block")]
            let keyed = table.block_index.iter().next().expect("a data block")?;
            let off = keyed.offset().0 as usize;
            ((*table.path).clone(), off + Header::MIN_LEN + 3)
        };

        // Flip one payload byte of the first data block (RS-correctable).
        let mut bytes = std::fs::read(&sst_path)?;
        bytes[corrupt_pos] ^= 0x80;
        std::fs::write(&sst_path, &bytes)?;

        // Reopen (fresh caches + fds) so the read hits the tampered bytes.
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .page_ecc(true)
        .ecc_scheme(EccScheme::ReedSolomon {
            data_shards: 8,
            parity_shards: 2,
        })
        .open()?;
        let crate::AnyTree::Standard(tree) = tree else {
            unreachable!("standard tree configured (no kv separation)");
        };
        assert!(tree.heal_hints().is_empty(), "fresh tree has no hints");

        // Opt into rewrite scheduling (default off). The gate tracks auto_heal.
        assert!(!tree.heal_hints().is_enabled(), "auto_heal defaults off");
        tree.update_runtime_config(|c| c.auto_heal = true)?;
        assert!(
            tree.heal_hints().is_enabled(),
            "auto_heal toggle syncs the gate"
        );

        // A read of a key in the corrupted block repairs it (correct value) and
        // records the SST for healing.
        #[expect(clippy::expect_used, reason = "key was inserted before flush")]
        let got = tree.get(b"key-000000", MAX_SEQNO)?.expect("key present");
        assert_eq!(&*got, b"v000000", "ECC must repair the value on read");
        assert!(
            !tree.heal_hints().is_empty(),
            "a persistent ECC correction must record a heal hint",
        );

        // Run the heal strategy: it claims the SST and rewrites it clean.
        let result = tree.compact(
            Arc::new(super::Strategy::new(tree.heal_hints(), u64::MAX)),
            0,
        )?;
        assert!(
            tree.heal_hints().is_empty(),
            "heal compaction must drain the hint queue, got {result:?}",
        );

        // The healed SST reads clean: correct value, no fresh correction hint.
        #[expect(clippy::expect_used, reason = "key survives the rewrite")]
        let got = tree
            .get(b"key-000000", MAX_SEQNO)?
            .expect("key present after heal");
        assert_eq!(&*got, b"v000000", "healed value must still be correct");
        assert!(
            tree.heal_hints().is_empty(),
            "the rewritten SST must read clean (no further correction)",
        );

        Ok(())
    }
}
