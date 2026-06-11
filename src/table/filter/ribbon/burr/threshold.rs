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
use alloc::{boxed::Box, string::String, vec::Vec};

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
mod tests {
    use super::super::super::hashing::StandardEquation;
    use super::*;

    fn eq_at(start: usize) -> StandardEquation {
        // coeff_lo / coeff_hi are irrelevant for threshold computation.
        StandardEquation {
            start,
            coeff_lo: 1,
            coeff_hi: 0,
        }
    }

    #[test]
    fn empty_input_returns_full_thresholds() {
        let thresholds = compute_thresholds(&[], 64, 16);
        // m=64, b=16 → block_count=4; no keys → all blocks accept everything.
        assert_eq!(thresholds, vec![16, 16, 16, 16]);
    }

    #[test]
    fn underloaded_block_keeps_threshold_at_b() {
        // m=64, b=16, cap = 16 * 9 / 10 = 14. With 5 keys in block 0,
        // none in others → threshold stays at b=16 everywhere.
        let equations: Vec<_> = [0, 1, 2, 3, 4].iter().map(|&start| eq_at(start)).collect();
        let thresholds = compute_thresholds(&equations, 64, 16);
        assert_eq!(thresholds, vec![16, 16, 16, 16]);
    }

    #[test]
    fn overloaded_block_lowers_threshold_to_cap_th_offset() {
        // m=64, b=16, cap = 14. Pack block 0 with offsets 0..16 (16 keys
        // — overload by 2). The cap-th sorted offset (14) becomes the
        // threshold; offsets 0..13 are kept (14 keys), offsets 14..15
        // are bumped (2 keys).
        let equations: Vec<_> = (0..16).map(eq_at).collect();
        let thresholds = compute_thresholds(&equations, 64, 16);
        assert_eq!(thresholds[0], 14);
        // Other blocks empty → threshold at b.
        assert_eq!(thresholds[1..], [16, 16, 16]);
    }

    #[test]
    fn partition_routes_keys_correctly() {
        // Keys at starts [0..16] in block 0, threshold = 14. Keys
        // 0..13 → kept (14 of them), 14..15 → bumped (2 of them).
        let keys: Vec<usize> = (0..16).collect();
        let equations: Vec<_> = (0..16).map(eq_at).collect();
        let thresholds = vec![14_u8, 16, 16, 16];
        let (kept, bumped) = partition_keys_by_threshold(&keys, &equations, &thresholds, 16);
        assert_eq!(kept.len(), 14);
        assert_eq!(bumped, vec![14, 15]);
    }

    #[test]
    fn is_bumped_predicate_matches_partition() {
        let equations: Vec<_> = (0..16).map(eq_at).collect();
        let thresholds = vec![14_u8, 16, 16, 16];
        for (i, eq) in equations.iter().enumerate() {
            let bumped = is_bumped(eq, &thresholds, 16);
            // First 14 keep; last 2 bump.
            assert_eq!(bumped, i >= 14, "key {i} bumped state mismatch");
        }
    }

    #[test]
    fn keys_outside_block_range_get_bumped() {
        // start values past m get treated as block_idx >= block_count;
        // the get(block_idx) returns None → threshold defaults to 0 →
        // any offset >= 0 → bumped. (This shouldn't happen for well-
        // formed equations, but is a safe fallback.)
        let eq = eq_at(1000);
        let thresholds = vec![16_u8, 16, 16, 16];
        assert!(is_bumped(&eq, &thresholds, 16));
    }
}
