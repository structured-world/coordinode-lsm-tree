// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// How a columnar table's column pages store their values.
///
/// A read hands a column out in its own layout. A page stored that way is
/// handed out as a view of itself; any other encoding is built back into
/// the layout by the read that wants the column whole, which costs time on
/// every read of a warm page and saves bytes on the ones that come from disk.
/// A level whose tables are read often wants [`Plain`](Self::Plain); a level
/// whose tables are mostly stored wants [`Auto`](Self::Auto), as it wants a
/// stronger compression.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum ColumnEncoding {
    /// Every page in its column's own layout.
    #[default]
    Plain,
    /// Each page as the expression over light operators (constant, runs,
    /// dictionary, bit packing, deltas) that costs least to store and to
    /// read, which is the plain layout when nothing beats it.
    Auto,
}

/// Column encoding policy: the [`ColumnEncoding`] of each level's tables.
///
/// A level past the policy's last entry takes that entry.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnEncodingPolicy(Vec<ColumnEncoding>);

impl core::ops::Deref for ColumnEncodingPolicy {
    type Target = [ColumnEncoding];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for ColumnEncodingPolicy {
    /// [`ColumnEncoding::Plain`] at every level.
    fn default() -> Self {
        Self::all(ColumnEncoding::Plain)
    }
}

impl ColumnEncodingPolicy {
    pub(crate) fn get(&self, level: usize) -> ColumnEncoding {
        #[expect(clippy::expect_used, reason = "policy is expected not to be empty")]
        self.0
            .get(level)
            .copied()
            .unwrap_or_else(|| self.last().copied().expect("policy should not be empty"))
    }

    /// Uses the same encoding in every level.
    #[must_use]
    pub fn all(encoding: ColumnEncoding) -> Self {
        Self(vec![encoding])
    }

    /// Constructs a custom policy, one entry per level from level 0.
    ///
    /// # Panics
    ///
    /// Panics if the policy is empty or contains more than 255 elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::config::{ColumnEncoding, ColumnEncodingPolicy};
    ///
    /// // Plain where tables are read while warm, encoded from level 3 down.
    /// let policy = ColumnEncodingPolicy::new([
    ///     ColumnEncoding::Plain,
    ///     ColumnEncoding::Plain,
    ///     ColumnEncoding::Plain,
    ///     ColumnEncoding::Auto,
    /// ]);
    /// assert_eq!(policy.len(), 4);
    /// ```
    #[must_use]
    pub fn new(policy: impl Into<Vec<ColumnEncoding>>) -> Self {
        let policy = policy.into();
        assert!(
            !policy.is_empty(),
            "column encoding policy may not be empty"
        );
        assert!(policy.len() <= 255, "column encoding policy is too large");
        Self(policy)
    }
}
