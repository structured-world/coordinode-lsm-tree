use super::prefixed_range;
use crate::UserKey;
use core::ops::Bound::{Excluded, Included};
use core::ops::RangeBounds;
use test_log::test;

#[test]
fn prefixed_range_1() {
    let prefix = "abc";
    let min = 5u8.to_be_bytes();
    let max = 9u8.to_be_bytes();

    let range = prefixed_range(prefix, min..=max);

    assert_eq!(
        range.start_bound(),
        Included(&UserKey::new(&[b'a', b'b', b'c', 5]))
    );
    assert_eq!(
        range.end_bound(),
        Included(&UserKey::new(&[b'a', b'b', b'c', 9]))
    );
}

#[test]
fn prefixed_range_2() {
    let prefix = "abc";
    let min = 5u8.to_be_bytes();
    let max = 9u8.to_be_bytes();

    let range = prefixed_range(prefix, min..max);

    assert_eq!(
        range.start_bound(),
        Included(&UserKey::new(&[b'a', b'b', b'c', 5]))
    );
    assert_eq!(
        range.end_bound(),
        Excluded(&UserKey::new(&[b'a', b'b', b'c', 9]))
    );
}

#[test]
fn prefixed_range_3() {
    let prefix = "abc";
    let min = 5u8.to_be_bytes();

    let range = prefixed_range(prefix, min..);

    assert_eq!(
        range.start_bound(),
        Included(&UserKey::new(&[b'a', b'b', b'c', 5]))
    );
    assert_eq!(range.end_bound(), Excluded(&UserKey::new(b"abd")));
}

#[test]
fn prefixed_range_4() {
    let prefix = "abc";
    let max = 9u8.to_be_bytes();

    let range = prefixed_range(prefix, ..max);

    assert_eq!(range.start_bound(), Included(&UserKey::new(b"abc")));
    assert_eq!(
        range.end_bound(),
        Excluded(&UserKey::new(&[b'a', b'b', b'c', 9]))
    );
}

#[test]
fn prefixed_range_5() {
    let prefix = "abc";
    let max = u8::MAX.to_be_bytes();

    let range = prefixed_range(prefix, ..=max);

    assert_eq!(range.start_bound(), Included(&UserKey::new(b"abc")));
    assert_eq!(
        range.end_bound(),
        Included(&UserKey::new(&[b'a', b'b', b'c', u8::MAX]))
    );
}

#[test]
fn prefixed_range_6() {
    let prefix = "abc";
    let max = u8::MAX.to_be_bytes();

    let range = prefixed_range(prefix, ..max);

    assert_eq!(range.start_bound(), Included(&UserKey::new(b"abc")));
    assert_eq!(
        range.end_bound(),
        Excluded(&UserKey::new(&[b'a', b'b', b'c', u8::MAX]))
    );
}

#[test]
fn prefixed_range_7() {
    let prefix = "abc";

    let range = prefixed_range::<_, &[u8], _>(prefix, ..);

    assert_eq!(range.start_bound(), Included(&UserKey::new(b"abc")));
    assert_eq!(range.end_bound(), Excluded(&UserKey::new(b"abd")));
}
