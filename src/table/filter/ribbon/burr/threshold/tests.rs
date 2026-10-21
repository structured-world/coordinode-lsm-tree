use super::super::super::hashing::StandardEquation;
use super::*;

fn eq_at(start: usize) -> StandardEquation {
    // coeff / fingerprint are irrelevant for threshold computation.
    StandardEquation {
        start,
        coeff_lo: 1,
        coeff_hi: 0,
        fingerprint: 0,
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
    let mut keys: Vec<u64> = (0..16).collect();
    let mut equations: Vec<_> = (0..16).map(eq_at).collect();
    let thresholds = vec![14_u8, 16, 16, 16];
    let mut bumped = Bumped {
        hashes: Vec::new(),
        values: None,
    };
    split_bumped(
        &mut keys,
        None,
        &mut equations,
        &thresholds,
        16,
        &mut bumped,
    );
    assert_eq!(keys, (0..14).collect::<Vec<_>>());
    assert_eq!(equations, (0..14).map(eq_at).collect::<Vec<_>>());
    assert_eq!(bumped.hashes, vec![14, 15]);
}

/// A retrieval split moves each value with its key, and keeps both halves in
/// the order the keys came in, interleaved or not.
#[test]
fn a_split_moves_values_with_their_keys_in_order() {
    let starts = [15, 0, 14, 1, 3, 15];
    let mut keys: Vec<u64> = (100..106).collect();
    let mut values: Vec<u64> = (200..206).collect();
    let mut equations: Vec<_> = starts.iter().map(|&start| eq_at(start)).collect();
    let thresholds = vec![14_u8, 16, 16, 16];
    let mut bumped = Bumped {
        hashes: alloc::vec![7],
        values: Some(alloc::vec![8]),
    };
    split_bumped(
        &mut keys,
        Some(&mut values),
        &mut equations,
        &thresholds,
        16,
        &mut bumped,
    );
    assert_eq!(keys, vec![101, 103, 104]);
    assert_eq!(values, vec![201, 203, 204]);
    assert_eq!(equations, [0, 1, 3].map(eq_at).to_vec());
    assert_eq!(
        bumped.hashes,
        vec![7, 100, 102, 105],
        "appended after what was there"
    );
    assert_eq!(bumped.values, Some(vec![8, 200, 202, 205]));
}

/// The threshold is the `cap`-th smallest offset whether the block's keys
/// arrive in order or not, and ties at it all bump.
#[test]
fn the_threshold_is_the_cap_th_smallest_offset_in_any_order() {
    // cap = 14 for b = 16. Offsets 0..=12 once, then 13 twice and 15 once,
    // given backwards: the 14th smallest (index 14) is the second 13.
    let mut starts: Vec<usize> = (0..=12).collect();
    starts.extend([13, 13, 15]);
    starts.reverse();
    let equations: Vec<_> = starts.iter().map(|&start| eq_at(start)).collect();
    let thresholds = compute_thresholds(&equations, 64, 16);
    assert_eq!(thresholds, vec![13, 16, 16, 16]);
}

/// A block holding exactly `cap` keys keeps them all, and one more key
/// lowers its threshold to that key's offset when it is the largest.
#[test]
fn a_block_at_capacity_keeps_all_and_one_more_bumps_the_largest() {
    let at_cap: Vec<_> = (0..14).map(eq_at).collect();
    assert_eq!(compute_thresholds(&at_cap, 32, 16), vec![16, 16]);
    let over: Vec<_> = (0..14).chain([15]).map(eq_at).collect();
    assert_eq!(compute_thresholds(&over, 32, 16), vec![15, 16]);
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
