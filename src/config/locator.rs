// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// In-block addressing precision for the retrieval-ribbon locator.
///
/// Selects the locator granularity, trading ribbon width against in-block read
/// work. The three modes correspond to **per-block**, **per-sub-block**, and
/// **per-key** addressing. Data blocks are delta-coded within restart intervals,
/// so only a restart head has a directly-addressable byte offset; the finer
/// modes land there and differ in how the exact entry is reached.
#[derive(Copy, Debug, Clone, PartialEq, Eq)]
pub enum LocatorPrecision {
    /// **Per-block**: the locator is `block_id` only (no `slot`). The read
    /// resolves the data block in O(1) and does the existing in-block binary
    /// search. Most compact; eliminates the index-block search (the structural
    /// win over a data-block hash index) while leaving the in-block lookup.
    Block,
    /// **Per-sub-block**: `slot` is the restart index. The read jumps to the
    /// restart head and scans up to `restart_interval` entries. In-block
    /// granularity matches a data-block hash index.
    Restart,
    /// **Per-key**: `slot` is the exact entry index. The read jumps to the
    /// restart head and decodes forward to the entry with no key comparisons.
    /// Costs roughly `log2(restart_interval)` more bits per key than
    /// [`Restart`].
    ///
    /// [`Restart`]: LocatorPrecision::Restart
    Entry,
}

/// Per-level retrieval-ribbon locator policy entry.
///
/// Each level can independently disable the locator or enable it with its own
/// precision and width budget.
#[derive(Copy, Debug, Clone, PartialEq, Eq)]
pub enum LocatorPolicyEntry {
    /// No locator section is built for this level. The point read uses the
    /// sorted index, exactly as without the feature, producing byte-identical
    /// SSTs (no section, no padding).
    None,

    /// Build a per-SST retrieval-ribbon locator section for this level.
    Enabled {
        /// What `slot` addresses within a data block.
        precision: LocatorPrecision,
        /// Bits reserved for the data-block id. `None` = auto, sized per SST as
        /// `ceil(log2(num_data_blocks))`. An explicit value too small for an
        /// SST's real layout makes that SST skip the section (graceful: it
        /// falls back to the index), never a build error.
        block_id_bits: Option<u8>,
        /// Bits reserved for `slot`. `None` = auto, sized per SST as
        /// `ceil(log2(max restarts per block))` (or entries per block for
        /// [`LocatorPrecision::Entry`]). Same overflow handling as
        /// `block_id_bits`.
        slot_bits: Option<u8>,
    },
}

/// Retrieval-ribbon locator policy.
///
/// One [`LocatorPolicyEntry`] per LSM level (the last entry covers any deeper
/// level). Defaults to [`Self::block_level`]; the optional, format-gated
/// `locator` section it adds to written SSTs lets a point read resolve a key to
/// its data block in O(1) via the retrieval ribbon, skipping the index-block
/// binary search. Finer precisions ([`LocatorPrecision::Restart`] /
/// [`LocatorPrecision::Entry`]) also skip the in-block search; [`Self::disabled`]
/// turns the section off entirely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocatorPolicy(Vec<LocatorPolicyEntry>);

impl core::ops::Deref for LocatorPolicy {
    type Target = [LocatorPolicyEntry];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl LocatorPolicy {
    /// The locator entry for `level`, falling back to the last entry for levels
    /// beyond the configured length.
    #[must_use]
    pub(crate) fn get(&self, level: usize) -> LocatorPolicyEntry {
        #[expect(clippy::expect_used, reason = "policy is expected not to be empty")]
        self.0
            .get(level)
            .copied()
            .unwrap_or_else(|| self.last().copied().expect("policy should not be empty"))
    }

    /// Disables the locator on every level.
    #[must_use]
    pub fn disabled() -> Self {
        Self::all(LocatorPolicyEntry::None)
    }

    /// Enables the locator at [`LocatorPrecision::Block`] on every level with
    /// auto-sized widths (the default).
    ///
    /// Block precision is the cheapest tier: the ribbon stores only `block_id`
    /// (no `slot`), so it is the narrowest ribbon to build and store while still
    /// delivering the structural win — a point read resolves the data block in
    /// O(1) and skips the index-block binary search (the costly part on
    /// partitioned indexes), then does the existing in-block lookup. An SST
    /// whose ribbon cannot be built simply omits the section and falls back to
    /// the sorted index, so enabling this by default never risks a write.
    #[must_use]
    pub fn block_level() -> Self {
        Self::all(LocatorPolicyEntry::Enabled {
            precision: LocatorPrecision::Block,
            block_id_bits: None,
            slot_bits: None,
        })
    }

    /// Uses the same locator entry on every level.
    #[must_use]
    pub fn all(entry: LocatorPolicyEntry) -> Self {
        Self(vec![entry])
    }

    /// Constructs a custom per-level locator policy.
    ///
    /// # Panics
    ///
    /// Panics if the policy is empty or contains more than 255 elements.
    #[must_use]
    pub fn new(policy: impl Into<Vec<LocatorPolicyEntry>>) -> Self {
        let policy = policy.into();
        assert!(!policy.is_empty(), "locator policy may not be empty");
        assert!(policy.len() <= 255, "locator policy is too large");
        Self(policy)
    }
}

#[cfg(test)]
mod tests;
