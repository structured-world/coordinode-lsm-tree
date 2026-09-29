// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

pub use crate::table::filter::BloomConstructionPolicy;

/// Filter policy entry
///
/// Each level can be configured with a different filter type and bits per key
#[derive(Copy, Debug, Clone, PartialEq)]
pub enum FilterPolicyEntry {
    /// Skip filter construction
    None,

    /// Standard bloom filter with K bits per key
    Bloom(BloomConstructionPolicy),
}

/// Allocates filter memory across a tree's tables by how often each table's
/// filter answers for keys the table does not hold, instead of by level.
///
/// Each table written while it is set gets the filter width, among
/// [`Self::bits_per_key`], that spends [`Self::budget_bytes`] where false
/// positives cost the most. Existing filters are never rebuilt: a table's
/// width is chosen only when the table is written anyway, so the budget is a
/// target each flush and compaction allocates against, not a bound the tree
/// can always hold. The tree reports when its filters exceed it. A flush and
/// a compaction running together draw on the one budget.
///
/// The probe load of a table is the count of probes its filter answered for
/// keys the table does not hold, over a window: whenever a flush or compaction
/// finds the live tables holding more than [`Self::window_probes`] probes in
/// all, every count halves, so the activity of each window weighs half as
/// much as the one after it. The counts are not durable; a reopened tree
/// starts cold and uses the [`FilterPolicy`] until it has observed probes.
///
/// Every false positive is charged the same cost, one data block read.
///
/// Off unless configured; without it the [`FilterPolicy`] decides every
/// filter, as it always has, and nothing is counted on the read path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterAdvisor {
    budget_bytes: u64,
    bits_per_key: Vec<u8>,
    window_probes: u64,
}

impl FilterAdvisor {
    /// The widths a filter is chosen from when none are given.
    const DEFAULT_BITS_PER_KEY: [u8; 6] = [6, 8, 10, 12, 14, 16];

    /// The probes a window holds when not given.
    const DEFAULT_WINDOW_PROBES: u64 = 1 << 20;

    /// Allocates `budget_bytes` of serialised filter bytes across the tree's
    /// live tables, choosing each table's width from 6 to 16 bits per key in
    /// steps of two, over windows of about a million probes.
    #[must_use]
    pub fn new(budget_bytes: u64) -> Self {
        Self {
            budget_bytes,
            bits_per_key: Self::DEFAULT_BITS_PER_KEY.to_vec(),
            window_probes: Self::DEFAULT_WINDOW_PROBES,
        }
    }

    /// Halves the probe counts whenever the live tables hold more than
    /// `probes` of them: a smaller window follows a shifting workload sooner
    /// and weighs each table on fewer probes.
    ///
    /// # Panics
    ///
    /// Panics if `probes` is zero.
    #[must_use]
    pub fn with_window_probes(mut self, probes: u64) -> Self {
        assert!(probes > 0, "a probe window holds at least one probe");
        self.window_probes = probes;
        self
    }

    /// The probes the live tables hold before their counts halve.
    #[must_use]
    pub fn window_probes(&self) -> u64 {
        self.window_probes
    }

    /// Chooses each filter's width from `bits_per_key` instead.
    ///
    /// # Panics
    ///
    /// Panics if `bits_per_key` is empty or holds a width outside `1..=64`.
    #[must_use]
    pub fn with_bits_per_key(mut self, bits_per_key: impl Into<Vec<u8>>) -> Self {
        let mut bits_per_key = bits_per_key.into();
        assert!(
            !bits_per_key.is_empty(),
            "a filter advisor needs at least one width"
        );
        assert!(
            bits_per_key.iter().all(|bits| (1..=64).contains(bits)),
            "filter widths must lie in 1..=64 bits per key",
        );
        bits_per_key.sort_unstable();
        bits_per_key.dedup();
        self.bits_per_key = bits_per_key;
        self
    }

    /// Serialised filter bytes the tree's live tables are allocated against:
    /// the on-disk size of their filter sections, what
    /// `AbstractTree::filter_size` reports. A partitioned filter's top-level
    /// index is not counted, and the bytes resident in memory are a separate
    /// figure the budget does not bound.
    #[must_use]
    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    /// The widths a filter is chosen from, in bits per key, ascending.
    #[must_use]
    pub fn bits_per_key(&self) -> &[u8] {
        &self.bits_per_key
    }
}

/// Filter policy
#[derive(Debug, Clone, PartialEq)]
pub struct FilterPolicy(Vec<FilterPolicyEntry>);

impl core::ops::Deref for FilterPolicy {
    type Target = [FilterPolicyEntry];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FilterPolicy {
    pub(crate) fn get(&self, level: usize) -> FilterPolicyEntry {
        #[expect(clippy::expect_used, reason = "policy is expected not to be empty")]
        self.0
            .get(level)
            .copied()
            .unwrap_or_else(|| self.last().copied().expect("policy should not be empty"))
    }

    /// Disables all filters.
    ///
    /// **Not recommended unless you know what you are doing!**
    #[must_use]
    pub fn disabled() -> Self {
        Self::all(FilterPolicyEntry::None)
    }

    /// Uses the same block size in every level.
    #[must_use]
    pub fn all(c: FilterPolicyEntry) -> Self {
        Self(vec![c])
    }

    /// Constructs a custom block size policy.
    ///
    /// # Panics
    ///
    /// Panics if the policy is empty or contains more than 255 elements.
    #[must_use]
    pub fn new(policy: impl Into<Vec<FilterPolicyEntry>>) -> Self {
        let policy = policy.into();
        assert!(!policy.is_empty(), "compression policy may not be empty");
        assert!(policy.len() <= 255, "compression policy is too large");
        Self(policy)
    }
}
