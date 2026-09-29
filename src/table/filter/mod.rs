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

    /// Bytes the filter over `n` distinct hashes encodes to, bounded from
    /// above, for the writers to count a filter they have not built yet. The
    /// first layer is exact. Each later one solves the keys the layer before
    /// bumped. Over many key sets the second takes a tenth of the first's
    /// slots, spread by about `2.5 / sqrt(n)` of them (18% at a thousand keys,
    /// 10% at a hundred thousand), the third a hundredth, the last under that;
    /// but a small filter builds them at their floors, two blocks, two and
    /// four. They are taken at a tenth plus `3 / sqrt(n)`, a fiftieth and a
    /// hundredth of the first, and at those floors.
    ///
    /// This bounds the filters real keys build, not the worst case: hashes
    /// chosen to collide can bump a whole block, and in the limit every key,
    /// into the next layer. Charging each later layer for all `n` keys would
    /// double the estimate of every table to guard against inputs built to
    /// defeat a 64-bit hash, so a table rotates on the bumps keys make in
    /// practice.
    #[must_use]
    pub(crate) fn filter_size_bound(self, n: usize) -> usize {
        use ribbon::burr::{packed, wire};

        let Some(params) = self.burr_params(n) else {
            return 0;
        };
        let b = usize::from(params.b);
        // A layer too large to size stays at the top rather than wrapping.
        let layer = |m: usize| {
            wire::LAYER_HEADER_LEN
                .saturating_add(m / b)
                .saturating_add(packed::z_byte_len(m, params.r).unwrap_or(usize::MAX))
        };
        let first = layer(params.layer_m(n));
        let spread = first.saturating_mul(3) / n.isqrt().max(1);
        let second = layer(2 * b).max((first / 10).saturating_add(spread));
        let third = layer(2 * b).max(first / 50);
        let last = layer(4 * b).max(first / 100);
        debug_assert_eq!(params.max_layers, 4, "the bound sizes four layers");
        wire::HEADER_LEN
            .saturating_add(first)
            .saturating_add(second)
            .saturating_add(third)
            .saturating_add(last)
    }

    /// The mean bytes a filter over `n` distinct hashes encodes to, for
    /// sizing filters against a byte budget, where an estimate that runs
    /// short on small filters would be charged to the filters after them.
    ///
    /// Follows the layers the build makes. A key's band starts in one of
    /// `m - w + 1` rows, so every block but the last draws a Poisson number
    /// of keys at `b` times the per-row rate, keeps up to the threshold
    /// capacity and bumps the excess, plus the keys tied at the threshold,
    /// into the next layer. Small filters bump a large share this way: 301
    /// keys over 320 slots start in 257 rows, 75 keys a block against a
    /// capacity of 57. A layer is built when at least one key is bumped
    /// into it; the last one at twice its slots and four blocks at least.
    ///
    /// Each layer is taken at the mean count bumped into it, so where only
    /// some key sets bump a key into the last layer, between about 500 and
    /// 5000 keys, this comes out at the builds without it, up to a few
    /// percent under the mean. A budget corrects such a proportional error
    /// by what its builds take.
    pub(crate) fn expected_filter_size(self, n: usize) -> f64 {
        self.burr_params(n)
            .map_or(0.0, |params| ExpectedSize::of(n).at(params.r))
    }
}

/// The mean bytes of a filter over some number of keys, as a fixed part and
/// a part per fingerprint bit: the layers a build makes depend on the keys,
/// not on the width, so one shape serves every width.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ExpectedSize {
    fixed: f64,
    per_bit: f64,
}

impl ExpectedSize {
    /// The mean bytes at `r` bits per key.
    pub(crate) fn at(self, r: u8) -> f64 {
        libm::fma(self.per_bit, f64::from(r), self.fixed)
    }

    /// The shape of a filter over `n` distinct hashes; see
    /// [`BloomConstructionPolicy::expected_filter_size`].
    pub(crate) fn of(n: usize) -> Self {
        use ribbon::burr::{packed, wire};

        // The layer layout is the same at every width.
        let Ok(params) = BurrParams::with_bpk(n, 1.0) else {
            return Self::default();
        };
        let b = usize::from(params.b);
        let w = usize::from(params.w);
        let capacity = as_f64(ribbon::burr::threshold::block_capacity(b));

        let mut shape = Self {
            fixed: as_f64(wire::HEADER_LEN),
            per_bit: 0.0,
        };
        // A layer of `m` slots, built with chance `built`: its header and
        // threshold bytes, and one bit-sliced word per segment and bit.
        let mut add = |m: usize, built: f64| {
            shape.fixed += built * as_f64(wire::LAYER_HEADER_LEN + m / b);
            shape.per_bit += built * as_f64(packed::segments_for(m) * 8);
        };
        // Keys entering the layer, given that it is built, and the chance
        // that it is.
        let mut input = as_f64(n);
        let mut built = 1.0;
        for index in 0..params.max_layers {
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "a key count below the filter's own"
            )]
            let keys = libm::ceil(input) as usize;
            let slots = params.layer_m(keys);
            if index + 1 == params.max_layers {
                add((slots * 2).max(b * 4), built);
                break;
            }
            add(slots, built);

            // Rows a band can start in, and the full blocks they span; the
            // last block holds the one row past them.
            let rows = slots - w + 1;
            let bumped = if rows == 1 {
                // Every key starts in the one row, at one offset: a block
                // over its capacity ties them all at the threshold.
                if input > capacity { input } else { 0.0 }
            } else {
                let blocks = as_f64(rows / b);
                blocks * overflow(input * as_f64(b) / as_f64(rows), capacity)
            };
            if bumped <= 0.0 {
                break;
            }
            // At least one key is bumped with the chance that a Poisson count
            // of this mean is not zero; the layer's input is that count given
            // it is not.
            let exists = -libm::expm1(-bumped);
            built *= exists;
            input = bumped / exists;
        }
        shape
    }
}

/// The mean keys a block drawing a Poisson count of mean `mean` bumps at
/// `capacity`: those past it, and about one more, the key the threshold ties
/// with, whenever it overflows.
fn overflow(mean: f64, capacity: f64) -> f64 {
    if mean <= 0.0 {
        return 0.0;
    }
    // The normal approximation of the count, with continuity correction.
    let sd = libm::sqrt(mean);
    let z = (mean - capacity - 0.5) / sd;
    let density = libm::exp(-0.5 * z * z) / libm::sqrt(2.0 * core::f64::consts::PI);
    let tail = 0.5 * libm::erfc(-z / core::f64::consts::SQRT_2);
    // no-std: `f64::mul_add` needs std; `libm::fma` is the same operation.
    libm::fma(sd, density, (mean - capacity + 0.5) * tail)
}

/// A count as `f64`: exact below 2^53, which every count here is.
#[expect(
    clippy::cast_precision_loss,
    reason = "counts of keys and bytes, far below 2^53"
)]
const fn as_f64(n: usize) -> f64 {
    n as f64
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
