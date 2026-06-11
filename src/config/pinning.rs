// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Pinning policy
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PinningPolicy(Vec<bool>);

impl core::ops::Deref for PinningPolicy {
    type Target = [bool];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PinningPolicy {
    pub(crate) fn get(&self, level: usize) -> bool {
        #[expect(clippy::expect_used, reason = "policy is expected not to be empty")]
        self.0
            .get(level)
            .copied()
            .unwrap_or_else(|| self.last().copied().expect("policy should not be empty"))
    }

    /// Uses the same policy in every level.
    #[must_use]
    pub fn all(c: bool) -> Self {
        Self(vec![c])
    }

    /// Fully disables pinning.
    #[must_use]
    pub fn disabled() -> Self {
        Self::all(false)
    }

    /// Constructs a custom policy.
    ///
    /// # Panics
    ///
    /// Panics if the policy is empty or contains more than 255 elements.
    #[must_use]
    pub fn new(policy: impl Into<Vec<bool>>) -> Self {
        let policy = policy.into();
        assert!(!policy.is_empty(), "compression policy may not be empty");
        assert!(policy.len() <= 255, "compression policy is too large");
        Self(policy)
    }
}
