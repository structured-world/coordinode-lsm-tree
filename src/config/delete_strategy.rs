// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Row-level delete strategy for columnar segments.
//!
//! A columnar segment can apply a delete either by rewriting itself
//! ([`DeleteStrategy::CopyOnWrite`]) or by recording the deleted row positions
//! in a delete-bitmap overlay and deferring the rewrite
//! ([`DeleteStrategy::MergeOnRead`]). [`DeleteStrategy::Adaptive`] starts
//! merge-on-read and purges to a rewrite once too large a fraction of the
//! segment is deleted. The choice is a per-level policy (see
//! [`DeleteStrategyPolicy`]) so read-heavy lower levels can favour copy-on-write
//! while write-heavy upper levels favour merge-on-read.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// How a columnar segment applies row-level deletes.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DeleteStrategy {
    /// Rewrite the segment at compaction, physically dropping deleted rows and
    /// writing no delete-bitmap. Read-fast (no mask) at the cost of rewriting
    /// the whole segment per delete batch. Best for read-heavy segments.
    CopyOnWrite,
    /// Record deleted rows in the positional delete-bitmap and reuse the
    /// existing columnar blocks, deferring the rewrite. Cheap deletes, masked at
    /// read. Best for write-heavy segments.
    MergeOnRead,
    /// Merge-on-read until the deleted fraction exceeds `purge_threshold_percent`,
    /// then purge: rewrite the segment, drop deleted rows, and clear the bitmap.
    /// Balances write and read amplification automatically.
    Adaptive {
        /// Deleted-row fraction, in percent (`0..=100`), past which a compaction
        /// purges the segment back to copy-on-write. Bounds read amplification.
        purge_threshold_percent: u8,
    },
}

impl DeleteStrategy {
    /// The default adaptive strategy: merge-on-read, purging once more than 5% of
    /// the segment's rows are deleted (matching the lakehouse auto-purge default).
    #[must_use]
    pub const fn default_adaptive() -> Self {
        Self::Adaptive {
            purge_threshold_percent: 5,
        }
    }

    /// Whether this strategy records deletes in a bitmap (merge-on-read) rather
    /// than rewriting the segment immediately (copy-on-write).
    #[must_use]
    pub const fn writes_bitmap(self) -> bool {
        matches!(self, Self::MergeOnRead | Self::Adaptive { .. })
    }

    /// Whether an [`Adaptive`](Self::Adaptive) threshold is within its documented
    /// `0..=100` range. A threshold above 100 can never be reached (deleted
    /// percent caps at 100), so the segment would never purge.
    #[must_use]
    const fn has_valid_threshold(self) -> bool {
        match self {
            Self::Adaptive {
                purge_threshold_percent,
            } => purge_threshold_percent <= 100,
            Self::CopyOnWrite | Self::MergeOnRead => true,
        }
    }
}

impl Default for DeleteStrategy {
    fn default() -> Self {
        Self::default_adaptive()
    }
}

/// Per-level [`DeleteStrategy`], mirroring the compression / locator policies.
///
/// Index `level` selects that level's strategy, and a level past the end reuses
/// the last entry (so a single-entry policy applies everywhere).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeleteStrategyPolicy(Vec<DeleteStrategy>);

impl core::ops::Deref for DeleteStrategyPolicy {
    type Target = [DeleteStrategy];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DeleteStrategyPolicy {
    /// The strategy for `level`, clamping to the last entry for levels past the
    /// end of the policy. Empty policies fall back to [`DeleteStrategy::default`].
    #[must_use]
    pub fn get(&self, level: usize) -> DeleteStrategy {
        self.0
            .get(level)
            .copied()
            .unwrap_or_else(|| self.0.last().copied().unwrap_or_default())
    }

    /// Uses the same strategy in every level.
    ///
    /// # Panics
    ///
    /// Panics if an [`Adaptive`](DeleteStrategy::Adaptive) purge threshold is
    /// above 100 (outside its documented `0..=100` range).
    #[must_use]
    pub fn all(strategy: DeleteStrategy) -> Self {
        assert!(
            strategy.has_valid_threshold(),
            "adaptive purge threshold must be in 0..=100"
        );
        Self(vec![strategy])
    }

    /// Constructs a custom per-level policy.
    ///
    /// # Panics
    ///
    /// Panics if the policy is empty, contains more than 255 elements, or any
    /// [`Adaptive`](DeleteStrategy::Adaptive) purge threshold is above 100.
    #[must_use]
    pub fn new(policy: impl Into<Vec<DeleteStrategy>>) -> Self {
        let policy = policy.into();
        assert!(
            !policy.is_empty(),
            "delete strategy policy may not be empty"
        );
        assert!(policy.len() <= 255, "delete strategy policy is too large");
        assert!(
            policy
                .iter()
                .copied()
                .all(DeleteStrategy::has_valid_threshold),
            "adaptive purge threshold must be in 0..=100"
        );
        Self(policy)
    }
}

impl Default for DeleteStrategyPolicy {
    /// Adaptive (5% purge threshold) in every level.
    fn default() -> Self {
        Self::all(DeleteStrategy::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_adaptive_five_percent() {
        assert_eq!(
            DeleteStrategy::default(),
            DeleteStrategy::Adaptive {
                purge_threshold_percent: 5
            }
        );
    }

    #[test]
    fn writes_bitmap_only_for_mor_and_adaptive() {
        assert!(!DeleteStrategy::CopyOnWrite.writes_bitmap());
        assert!(DeleteStrategy::MergeOnRead.writes_bitmap());
        assert!(DeleteStrategy::default_adaptive().writes_bitmap());
    }

    #[test]
    fn policy_get_clamps_to_last_level() {
        let policy =
            DeleteStrategyPolicy::new([DeleteStrategy::MergeOnRead, DeleteStrategy::CopyOnWrite]);
        assert_eq!(policy.get(0), DeleteStrategy::MergeOnRead);
        assert_eq!(policy.get(1), DeleteStrategy::CopyOnWrite);
        // Levels past the end reuse the last entry (read-heavy bottom).
        assert_eq!(policy.get(2), DeleteStrategy::CopyOnWrite);
        assert_eq!(policy.get(99), DeleteStrategy::CopyOnWrite);
    }

    #[test]
    fn all_applies_everywhere() {
        let policy = DeleteStrategyPolicy::all(DeleteStrategy::MergeOnRead);
        assert_eq!(policy.get(0), DeleteStrategy::MergeOnRead);
        assert_eq!(policy.get(7), DeleteStrategy::MergeOnRead);
    }

    #[test]
    #[should_panic(expected = "may not be empty")]
    fn new_rejects_empty_policy() {
        let _ = DeleteStrategyPolicy::new(Vec::new());
    }

    #[test]
    #[should_panic(expected = "adaptive purge threshold must be in 0..=100")]
    fn all_rejects_threshold_above_100() {
        let _ = DeleteStrategyPolicy::all(DeleteStrategy::Adaptive {
            purge_threshold_percent: 101,
        });
    }

    #[test]
    #[should_panic(expected = "adaptive purge threshold must be in 0..=100")]
    fn new_rejects_threshold_above_100() {
        let _ = DeleteStrategyPolicy::new([
            DeleteStrategy::MergeOnRead,
            DeleteStrategy::Adaptive {
                purge_threshold_percent: 200,
            },
        ]);
    }

    #[test]
    fn threshold_100_is_accepted() {
        // The documented upper bound is inclusive.
        let policy = DeleteStrategyPolicy::all(DeleteStrategy::Adaptive {
            purge_threshold_percent: 100,
        });
        assert_eq!(
            policy.get(0),
            DeleteStrategy::Adaptive {
                purge_threshold_percent: 100
            }
        );
    }
}
