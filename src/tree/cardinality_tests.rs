use super::effective_lower_bound;
use core::ops::Bound;

#[test]
fn effective_lower_bound_raises_to_restriction() {
    let cmp = crate::comparator::default_comparator();
    let cmp = cmp.as_ref();
    let a: &[u8] = b"a";
    let m: &[u8] = b"m";
    let z: &[u8] = b"z";
    // Unbounded lower bound + a restriction: raise to the restriction.
    assert_eq!(
        effective_lower_bound(Bound::Unbounded, Some(m), cmp),
        Bound::Included(m)
    );
    // A lower bound below the restriction is raised to it.
    assert_eq!(
        effective_lower_bound(Bound::Included(a), Some(m), cmp),
        Bound::Included(m)
    );
    assert_eq!(
        effective_lower_bound(Bound::Excluded(a), Some(m), cmp),
        Bound::Included(m)
    );
    // A lower bound at or above the restriction is left unchanged.
    assert_eq!(
        effective_lower_bound(Bound::Included(z), Some(m), cmp),
        Bound::Included(z)
    );
    // No restriction: the lower bound is returned unchanged.
    assert_eq!(
        effective_lower_bound(Bound::Included(a), None, cmp),
        Bound::Included(a)
    );
    let unbounded: Bound<&[u8]> = Bound::Unbounded;
    assert_eq!(
        effective_lower_bound(unbounded, None, cmp),
        Bound::Unbounded
    );
}
