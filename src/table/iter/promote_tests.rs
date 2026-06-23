use super::promote_by_fraction;

#[test]
fn promote_by_fraction_triggers_at_or_above_threshold() {
    // 75% threshold: 3 of 4 blocks == exactly 75% → promote.
    assert!(promote_by_fraction(3, 4));
    // 6 of 8 == 75% → promote.
    assert!(promote_by_fraction(6, 8));
    // Full coverage always promotes.
    assert!(promote_by_fraction(10, 10));
}

#[test]
fn promote_by_fraction_holds_below_threshold() {
    // 2 of 4 == 50% → keep partial.
    assert!(!promote_by_fraction(2, 4));
    // 1 of 8 → keep partial.
    assert!(!promote_by_fraction(1, 8));
    // 5 of 8 == 62.5% → keep partial.
    assert!(!promote_by_fraction(5, 8));
}

#[test]
fn promote_by_fraction_zero_total_never_promotes() {
    // No layout (total == 0): the partial tier does not apply, never promote.
    assert!(!promote_by_fraction(0, 0));
    assert!(!promote_by_fraction(5, 0));
}
