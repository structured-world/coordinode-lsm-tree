// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// The largest block, row group or page size a table writer accepts.
pub const MAX_BLOCK_SIZE: u32 = 4 * 1_024 * 1_024;

/// Block size policy
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BlockSizePolicy(Vec<u32>);

impl core::ops::Deref for BlockSizePolicy {
    type Target = [u32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl BlockSizePolicy {
    pub(crate) fn get(&self, level: usize) -> u32 {
        #[expect(clippy::expect_used, reason = "policy is expected not to be empty")]
        self.0
            .get(level)
            .copied()
            .unwrap_or_else(|| self.last().copied().expect("policy should not be empty"))
    }

    /// Uses the same block size in every level.
    ///
    /// # Panics
    ///
    /// Panics if `c` exceeds [`MAX_BLOCK_SIZE`]: here, where the policy is
    /// built, rather than in the table writer during a background flush.
    #[must_use]
    pub fn all(c: u32) -> Self {
        assert!(c <= MAX_BLOCK_SIZE, "block size must be <= 4 MiB");
        Self(vec![c])
    }

    /// Constructs a custom block size policy.
    ///
    /// # Panics
    ///
    /// Panics if the policy is empty, contains more than 255 elements, or
    /// holds a size over [`MAX_BLOCK_SIZE`].
    #[must_use]
    pub fn new(policy: impl Into<Vec<u32>>) -> Self {
        let policy = policy.into();
        assert!(!policy.is_empty(), "block size policy may not be empty");
        assert!(policy.len() <= 255, "block size policy is too large");
        assert!(
            policy.iter().all(|&size| size <= MAX_BLOCK_SIZE),
            "block size must be <= 4 MiB",
        );
        Self(policy)
    }
}

#[cfg(test)]
mod tests;
