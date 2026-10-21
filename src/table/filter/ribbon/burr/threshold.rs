//! Per-block threshold computation for BuRR.
//!
//! Given a layer's `m` row count, block size `b`, and the set of equations
//! that each key would generate (each with a `start` row index), this
//! module decides — block by block — a threshold `τ_i ∈ [0, b]` such that
//! a key with `offset_in_block < τ_i` is KEPT in this layer, while a key
//! with `offset_in_block >= τ_i` is BUMPED to the next layer.
//!
//! The threshold is chosen as the largest value for which the kept set
//! fits the block's effective capacity. In the BuRR paper this capacity
//! is derived rigorously from the Gaussian-elimination success
//! probability; the MVP here uses a conservative load factor (`CAP_NUM /
//! CAP_DEN ≈ 90% of b`) — enough to bring construction failure below the
//! level Standard Ribbon already retries through, while leaving the
//! per-block analytic upgrade for a follow-up.
//!
//! All offsets in this module are measured WITHIN their block (i.e.
//! `start % b`), so a threshold of `b` means "accept everything" and `0`
//! means "bump everything".
//!
//! Every public item in this module is consumed by the BuRR builder
//! (`builder.rs`) and the probe path (`filter.rs`); the previous
//! crate-level `#![allow(dead_code)]` blanket suppression has been
//! removed so any future genuinely-dead code in here surfaces a
//! warning.

use super::super::hashing::StandardEquation;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Capacity numerator: per-block keep-capacity = `b * CAP_NUM / CAP_DEN`.
/// 90% load factor (CAP_NUM=9, CAP_DEN=10) — leaves ~10% margin for the
/// banded-solver's worst-case eliminations.
const CAP_NUM: usize = 9;
const CAP_DEN: usize = 10;

/// The keys a block of `b` rows keeps before its threshold bumps the rest.
pub(crate) const fn block_capacity(b: usize) -> usize {
    (b * CAP_NUM) / CAP_DEN
}

/// Compute per-block thresholds for a layer.
///
/// Returns `Vec<u8>` of length `block_count = ceil(m / b)`. Entry `i` is
/// the threshold for block `i` — keys whose `start` falls in row range
/// `[i*b, (i+1)*b)` and whose `offset = start - i*b` is `< thresholds[i]`
/// are KEPT; those with `offset >= thresholds[i]` are BUMPED.
///
/// `m` is the layer's slot count (must be > 0). `b` is the block size in
/// rows (must be > 0). `equations.len()` may be anything — the function
/// works whether all keys fit, some fit, or every block is overloaded.
///
/// # Algorithm
///
/// 1. Count the keys starting at each row of the layer's blocks.
/// 2. Per block, target keep-capacity `cap = b * CAP_NUM / CAP_DEN`.
/// 3. If the block holds `≤ cap` keys, threshold = `b` (accept all).
/// 4. Otherwise the threshold is the `cap`-th smallest offset (0-indexed):
///    the first offset at which the keys counted so far exceed `cap`. Keys
///    with offset strictly less than this value are kept, the smallest `cap`
///    of them. Counting gives that order statistic in one pass, without
///    collecting and sorting each block's offsets.
///
/// Pathological case: if `cap` keys share the same offset value (rare
/// for well-distributed hashes), the threshold will be that offset, and
/// strict `<` may discard a tying key. That's safe — it just means one
/// extra key bumps. Tolerable for the MVP; the analytic per-block
/// variant from the paper handles this exactly.
#[must_use]
#[expect(
    clippy::indexing_slicing,
    reason = "a start below `rows` indexes `counts`, sized to `rows`; a block's \
              rows `block * b .. (block + 1) * b` lie within it as `block < block_count`"
)]
pub(crate) fn compute_thresholds(equations: &[StandardEquation], m: usize, b: u8) -> Vec<u8> {
    debug_assert!(m > 0, "compute_thresholds requires m > 0");
    debug_assert!(b > 0, "compute_thresholds requires b > 0");

    let b_usize = usize::from(b);
    let block_count = m.div_ceil(b_usize);
    let cap_per_block = block_capacity(b_usize);

    // Keys starting at each row; a start past the last block counts nowhere.
    // A row's count is bounded by the key count, which fits `u32` for any
    // layer a filter builds (its slots are counted in `u32` on the wire).
    let rows = block_count * b_usize;
    let mut counts = alloc::vec![0_u32; rows];
    for eq in equations {
        if eq.start < rows {
            counts[eq.start] += 1;
        }
    }

    let mut thresholds = alloc::vec![b; block_count];
    for (block, threshold) in thresholds.iter_mut().enumerate() {
        let block_counts = &counts[block * b_usize..(block + 1) * b_usize];
        let mut kept = 0_usize;
        for (offset, &count) in (0_u8..).zip(block_counts) {
            kept += count as usize;
            if kept > cap_per_block {
                // More than `cap` keys start at or below this offset, at most
                // `cap` below it: this is the `cap`-th smallest offset.
                *threshold = offset;
                break;
            }
        }
    }

    thresholds
}

/// The keys a split moves to the next layer, and their values for a
/// retrieval build, in their order.
pub(crate) struct Bumped {
    pub(crate) hashes: Vec<u64>,
    pub(crate) values: Option<Vec<u64>>,
}

/// Moves the keys `thresholds` bump out of `hashes`, with the `values` and
/// `equations` parallel to them, keeping the kept ones at the front in their
/// order; returns the bumped ones, in their order, appended to `bumped`.
///
/// `thresholds` is indexed by `block_idx = start / b`.
#[expect(
    clippy::indexing_slicing,
    reason = "`at` ranges over the parallel vectors' common length and \
              `kept <= at`, so both index within them"
)]
pub(crate) fn split_bumped(
    hashes: &mut Vec<u64>,
    mut values: Option<&mut Vec<u64>>,
    equations: &mut Vec<StandardEquation>,
    thresholds: &[u8],
    b: u8,
    bumped: &mut Bumped,
) {
    debug_assert_eq!(hashes.len(), equations.len());
    let value_count = values.as_deref().map(Vec::len);
    debug_assert!(value_count.is_none_or(|count| count == hashes.len()));
    debug_assert_eq!(values.is_some(), bumped.values.is_some());
    let moving = equations
        .iter()
        .filter(|eq| is_bumped(eq, thresholds, b))
        .count();
    bumped.hashes.reserve_exact(moving);
    if let Some(bumped_values) = &mut bumped.values {
        bumped_values.reserve_exact(moving);
    }
    let mut kept = 0;
    for at in 0..equations.len() {
        let eq = equations[at];
        if is_bumped(&eq, thresholds, b) {
            bumped.hashes.push(hashes[at]);
            if let (Some(values), Some(bumped_values)) = (values.as_deref(), &mut bumped.values) {
                bumped_values.push(values[at]);
            }
        } else {
            equations[kept] = eq;
            hashes[kept] = hashes[at];
            if let Some(values) = values.as_deref_mut() {
                values[kept] = values[at];
            }
            kept += 1;
        }
    }
    equations.truncate(kept);
    hashes.truncate(kept);
    if let Some(values) = values {
        values.truncate(kept);
    }
}

/// Predicate variant: does a single equation get bumped under the given
/// thresholds? Used by the probe path to decide which layer holds a key.
#[expect(
    clippy::inline_always,
    reason = "called per layer on the filter probe hot path; the function is ~5 instructions and \
              inlining lets LLVM fold the threshold-table indexing into the caller's layer loop"
)]
#[inline(always)]
#[must_use]
pub(crate) fn is_bumped(eq: &StandardEquation, thresholds: &[u8], b: u8) -> bool {
    let b_usize = usize::from(b);
    let block_idx = eq.start / b_usize;
    let offset = (eq.start % b_usize) as u8;
    let threshold = thresholds.get(block_idx).copied().unwrap_or(0);
    offset >= threshold
}

#[cfg(test)]
#[expect(
    clippy::indexing_slicing,
    reason = "tests index into known-sized fixture vectors; bounds are part of the assertion"
)]
mod tests;
