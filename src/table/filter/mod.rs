// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

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

    /// Estimates, before the build, how many bytes a filter over `n` keys
    /// will serialise to.
    ///
    /// This is a prediction and cannot be exact: each layer's slot count is
    /// derived from the keys the previous layer *bumped*, and how many a
    /// layer bumps depends on the hashes, not on the count. The exact figure
    /// is [`BurrFilter::encoded_len`], available only after the build; that
    /// is what memory accounting uses. This one exists to decide **where a
    /// partition splits**, so being cheap and close matters more than being
    /// right.
    ///
    /// The model computes the first layer exactly — `m` inflated by the
    /// per-layer overhead and rounded up to a whole block, its bit-sliced
    /// payload of `segments * r * 8` bytes, one threshold byte per block, and
    /// the layer header — then applies a measured multiplier for the layers
    /// the bumped keys land in. Against real builds that lands within about
    /// 4% for `n` from 10³ to 10⁵ and `r` from 8 to 16, the range the tests
    /// pin.
    ///
    /// `partition_size` is a **target**, not a ceiling: the writer spills
    /// when this estimate reaches it, so a built partition may exceed it by
    /// the model's error. That matches the partitioned INDEX writer, which
    /// spills on an accumulated byte count and likewise overshoots by its
    /// last unit.
    ///
    /// Returns `0` if the policy is inactive for the given `n`
    /// (`burr_params` would return `None`).
    ///
    /// [`BurrFilter::encoded_len`]: ribbon::burr::BurrFilter::encoded_len
    #[must_use]
    pub fn estimated_filter_size(&self, n: usize) -> usize {
        use ribbon::burr::{packed, wire};

        // Delegate to burr_params so the estimate is 0 exactly when the
        // builder would also return empty — keeps memory accounting in
        // sync with build behavior.
        let Some(params) = self.burr_params(n) else {
            return 0;
        };
        let b = usize::from(params.b);

        // First layer, exactly as `BurrParams::layer_m` will size it.
        #[expect(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "an estimate over a key count bounded by the partition policy"
        )]
        let inflated = crate::f32_ceil((n as f32) * (1.0 + params.per_layer_overhead)) as usize;
        let m0 = inflated.max(b).div_ceil(b) * b;
        let first_layer = wire::LAYER_HEADER_LEN
            + m0 / b
            + packed::z_byte_len(m0, params.r).unwrap_or(usize::MAX);

        // The bumped keys' layers, as a fraction of the first. Measured over
        // built filters rather than derived: the bump rate is a property of
        // the threshold scheme's load factor and the hash distribution.
        const TAIL_NUM: usize = 115;
        const TAIL_DEN: usize = 100;
        wire::HEADER_LEN + first_layer.saturating_mul(TAIL_NUM) / TAIL_DEN
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
