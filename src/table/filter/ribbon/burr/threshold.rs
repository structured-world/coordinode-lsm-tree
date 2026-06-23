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
/// 1. Bucket equations by `block_idx = start / b`. Per-bucket list of
///    `offset = start % b` values (capped at `b - 1`).
/// 2. Per block, target keep-capacity `cap = b * CAP_NUM / CAP_DEN`.
/// 3. If the bucket has `≤ cap` keys, threshold = `b` (accept all).
/// 4. Otherwise sort offsets ascending; the smallest `cap` offsets are
///    kept. Threshold = the `cap`-th sorted offset (0-indexed) — keys
///    with offset strictly less than this value are kept.
///
/// Pathological case: if `cap` keys share the same offset value (rare
/// for well-distributed hashes), the threshold will be that offset, and
/// strict `<` may discard a tying key. That's safe — it just means one
/// extra key bumps. Tolerable for the MVP; the analytic per-block
/// variant from the paper handles this exactly.
#[must_use]
#[expect(
    clippy::indexing_slicing,
    reason = "block_idx is bounds-checked by `if block_idx < block_count`; \
              thresholds[i] uses `i` from `block_offsets.iter().enumerate()` \
              over a vec we just sized to block_count; offsets[cap_per_block] \
              is gated by the earlier `if offsets.len() <= cap_per_block` early-continue"
)]
pub(crate) fn compute_thresholds(equations: &[StandardEquation], m: usize, b: u8) -> Vec<u8> {
    debug_assert!(m > 0, "compute_thresholds requires m > 0");
    debug_assert!(b > 0, "compute_thresholds requires b > 0");

    let b_usize = usize::from(b);
    let block_count = m.div_ceil(b_usize);
    let cap_per_block = (b_usize * CAP_NUM) / CAP_DEN;

    // First pass: count keys per block to size each bucket exactly.
    let mut block_counts = vec![0_usize; block_count];
    for eq in equations {
        let block_idx = eq.start / b_usize;
        if block_idx < block_count {
            block_counts[block_idx] += 1;
        }
    }

    // Second pass: collect offsets per block.
    let mut block_offsets: Vec<Vec<u8>> = block_counts
        .iter()
        .map(|&n| Vec::with_capacity(n))
        .collect();
    for eq in equations {
        let block_idx = eq.start / b_usize;
        if block_idx < block_count {
            let offset = (eq.start % b_usize) as u8;
            block_offsets[block_idx].push(offset);
        }
    }

    // Third pass: derive each block's threshold.
    let mut thresholds = vec![b; block_count];
    for (i, offsets) in block_offsets.iter_mut().enumerate() {
        if offsets.len() <= cap_per_block {
            // Block underloaded — accept everything.
            continue;
        }
        offsets.sort_unstable();
        // The threshold is the offset value at the `cap_per_block`-th
        // position (sorted ascending). Strict `<` against this threshold
        // keeps exactly the `cap_per_block` smallest-offset keys (modulo
        // ties at the boundary, which lean toward bumping — safe).
        thresholds[i] = offsets[cap_per_block];
    }

    thresholds
}

/// Partition keys into (kept, bumped) according to the given thresholds.
///
/// Inputs are parallel slices — `keys[i]` must correspond to
/// `equations[i]` (same hash, same layer seed). `thresholds` indexed by
/// `block_idx = start / b`.
///
/// Returns `(kept, bumped)` where `kept` is built for this layer and
/// `bumped` is forwarded to the next BuRR layer.
pub(crate) fn partition_keys_by_threshold<K: Clone>(
    keys: &[K],
    equations: &[StandardEquation],
    thresholds: &[u8],
    b: u8,
) -> (Vec<K>, Vec<K>) {
    debug_assert_eq!(keys.len(), equations.len());
    let b_usize = usize::from(b);

    let mut kept = Vec::with_capacity(keys.len());
    let mut bumped = Vec::with_capacity(keys.len() / 10); // expect ~10%
    for (key, eq) in keys.iter().zip(equations.iter()) {
        let block_idx = eq.start / b_usize;
        let offset = (eq.start % b_usize) as u8;
        let threshold = thresholds.get(block_idx).copied().unwrap_or(0);
        if offset < threshold {
            kept.push(key.clone());
        } else {
            bumped.push(key.clone());
        }
    }
    (kept, bumped)
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
