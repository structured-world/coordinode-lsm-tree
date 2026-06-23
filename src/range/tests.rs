use super::*;
use crate::Slice;
use core::ops::Bound::{Excluded, Included, Unbounded};
use test_log::test;

fn test_prefix(prefix: &[u8], upper_bound: Bound<&[u8]>) {
    let range = prefix_to_range(prefix);
    assert_eq!(
        range,
        (
            match prefix {
                _ if prefix.is_empty() => Unbounded,
                _ => Included(Slice::from(prefix)),
            },
            upper_bound.map(Slice::from),
        ),
    );
}

#[test]
fn prefix_to_range_basic() {
    test_prefix(b"abc", Excluded(b"abd"));
}

#[test]
fn prefix_to_range_empty() {
    test_prefix(b"", Unbounded);
}

#[test]
fn prefix_to_range_single_char() {
    test_prefix(b"a", Excluded(b"b"));
}

#[test]
fn prefix_to_range_1() {
    test_prefix(&[0, 250], Excluded(&[0, 251]));
}

#[test]
fn prefix_to_range_2() {
    test_prefix(&[0, 250, 50], Excluded(&[0, 250, 51]));
}

#[test]
fn prefix_to_range_3() {
    test_prefix(&[255, 255, 255], Unbounded);
}

#[test]
fn prefix_to_range_char_max() {
    test_prefix(&[0, 255], Excluded(&[1]));
}

#[test]
fn prefix_to_range_char_max_2() {
    test_prefix(&[0, 2, 255], Excluded(&[0, 3]));
}
