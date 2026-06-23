// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

pub mod block;
pub mod ribbon;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use ribbon::burr::{BurrBuilder, BurrParams};

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum BloomConstructionPolicy {
    BitsPerKey(f32),
    FalsePositiveRate(f32),
}

impl Default for BloomConstructionPolicy {
    fn default() -> Self {
        Self::BitsPerKey(10.0)
    }
}

impl BloomConstructionPolicy {
    /// Returns `true` if this policy can produce a valid filter
    /// (`burr_params` would return `Some` for any non-zero `n`). False
    /// means the writer should skip filter construction entirely
    /// instead of buffering hashes that will later be dropped.
    #[must_use]
    pub fn is_active(&self) -> bool {
        // Delegate to `burr_params` so this method is exact-equivalent
        // to "would this policy produce a non-empty filter for n=1?".
        // Anything stricter (e.g. fpr too small → r > 64) is captured
        // by the params constructor's own validation.
        self.burr_params(1).is_some()
    }

    /// Build `BurrParams` for the given key count under this policy.
    ///
    /// Returns `None` if `n == 0` or the policy translates to an invalid
    /// `BurrParams` (e.g. `bpk > 64` or `fpr` outside `(0,1)`). Callers
    /// should treat `None` as "skip filter construction for this block".
    pub(crate) fn burr_params(self, n: usize) -> Option<BurrParams> {
        if n == 0 {
            return None;
        }
        match self {
            Self::BitsPerKey(bpk) => BurrParams::with_bpk(n, bpk).ok(),
            Self::FalsePositiveRate(fpr) => BurrParams::with_fp_rate(n, fpr).ok(),
        }
    }

    /// Returns the estimated filter size in bytes.
    ///
    /// Returns `0` if the policy is inactive for the given `n`
    /// (`burr_params` would return `None`). Otherwise estimates the
    /// `BuRR` body size as `n * r * 1.05 / 8` — `r` is the fingerprint
    /// width chosen by the params constructor, `1.05` is a flat 5%
    /// overhead for layer thresholds + last-layer enlargement.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "estimation, precision loss is acceptable"
    )]
    pub fn estimated_filter_size(&self, n: usize) -> usize {
        // Delegate to burr_params so the estimate is 0 exactly when the
        // builder would also return empty — keeps memory accounting in
        // sync with build behavior.
        let Some(params) = self.burr_params(n) else {
            return 0;
        };
        let r_bits = f32::from(params.r);
        ((n as f32) * r_bits * 1.05 / 8.0) as usize
    }
}

/// Build a `BuRR` filter block payload from pre-hashed keys under the given
/// policy. Returns the serialized wire bytes the
/// [`block::FilterBlock`] reader can parse.
///
/// Returns an empty `Vec` if `hashes` is empty or the policy parameters
/// are invalid for `n = hashes.len()` — callers should treat that as
/// "no filter for this block".
///
/// Consumes `hashes` so the writer's accumulated `bloom_hash_buffer` can
/// be `mem::take`n straight in without a `to_vec()` copy at the boundary.
pub(crate) fn build_burr_filter_bytes(
    policy: BloomConstructionPolicy,
    hashes: Vec<u64>,
) -> crate::Result<Vec<u8>> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }
    let Some(params) = policy.burr_params(hashes.len()) else {
        return Ok(Vec::new());
    };
    let builder = BurrBuilder::new(params).map_err(|e| {
        log::error!("BuRR builder init failed: {e:?}");
        crate::Error::Unrecoverable
    })?;
    let filter = builder.build_from_hashes_owned(hashes).map_err(|e| {
        log::error!("BuRR build_from_hashes failed: {e:?}");
        crate::Error::Unrecoverable
    })?;
    Ok(filter.to_wire_bytes())
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests;

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
#[expect(clippy::unwrap_used, reason = "test code")]
mod extra_tests;
