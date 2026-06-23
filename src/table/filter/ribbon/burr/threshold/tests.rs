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
