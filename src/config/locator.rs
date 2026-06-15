// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// In-block addressing precision for the retrieval-ribbon locator.
///
/// A locator is `(block_id, slot)`; this selects what `slot` indexes, trading
/// ribbon width against in-block read work. Data blocks are delta-coded within
/// restart intervals, so only a restart head has a directly-addressable byte
/// offset; both modes land there and differ in how the exact entry is reached.
#[derive(Copy, Debug, Clone, PartialEq, Eq)]
pub enum LocatorPrecision {
    /// `slot` is the restart index. The read jumps to the restart head and
    /// scans up to `restart_interval` entries. Most compact; the in-block
    /// granularity matches a data-block hash index.
    Restart,
    /// `slot` is the exact entry index. The read jumps to the restart head and
    /// decodes forward to the entry with no key comparisons. Costs roughly
    /// `log2(restart_interval)` more bits per key than [`Restart`].
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
    /// sorted index, exactly as without the feature. This is the default and
    /// produces byte-identical SSTs (no section, no padding).
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
/// level). Off by default ([`Self::disabled`]); enabling it adds an optional,
/// format-gated `locator` section to written SSTs so point reads can resolve a
/// key to its data block and slot in O(1) via the retrieval ribbon, skipping
/// both the index-block and in-block binary searches.
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

    /// Disables the locator on every level (the default).
    #[must_use]
    pub fn disabled() -> Self {
        Self::all(LocatorPolicyEntry::None)
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
mod tests {
    use super::*;

    fn is_enabled(entry: LocatorPolicyEntry) -> bool {
        matches!(entry, LocatorPolicyEntry::Enabled { .. })
    }

    #[test]
    fn disabled_policy_reports_no_level_enabled() {
        let policy = LocatorPolicy::disabled();
        assert!(!is_enabled(policy.get(0)));
        assert!(!is_enabled(policy.get(7)));
    }

    #[test]
    fn get_beyond_length_falls_back_to_last_entry() {
        let policy = LocatorPolicy::new(vec![
            LocatorPolicyEntry::Enabled {
                precision: LocatorPrecision::Restart,
                block_id_bits: None,
                slot_bits: None,
            },
            LocatorPolicyEntry::None,
        ]);
        assert!(is_enabled(policy.get(0)));
        assert!(!is_enabled(policy.get(1)));
        // Level 5 has no explicit entry → falls back to the last (None).
        assert!(!is_enabled(policy.get(5)));
    }

    #[test]
    fn all_applies_one_entry_to_every_level() {
        let policy = LocatorPolicy::all(LocatorPolicyEntry::Enabled {
            precision: LocatorPrecision::Entry,
            block_id_bits: Some(20),
            slot_bits: Some(10),
        });
        for level in 0..4 {
            assert_eq!(
                policy.get(level),
                LocatorPolicyEntry::Enabled {
                    precision: LocatorPrecision::Entry,
                    block_id_bits: Some(20),
                    slot_bits: Some(10),
                },
            );
        }
    }

    #[test]
    #[should_panic(expected = "locator policy may not be empty")]
    fn new_rejects_empty_policy() {
        let _ = LocatorPolicy::new(Vec::new());
    }
}
