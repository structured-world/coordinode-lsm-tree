use super::*;

#[test]
fn default_comparator_name() {
    assert_eq!(DefaultUserComparator.name(), "default");
    assert_eq!(default_comparator().name(), "default");
}

#[test]
fn default_comparator_is_lexicographic() {
    assert!(DefaultUserComparator.is_lexicographic());
}
