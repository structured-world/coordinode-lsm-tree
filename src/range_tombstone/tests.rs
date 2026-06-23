use super::*;

fn rt(start: &[u8], end: &[u8], seqno: SeqNo) -> RangeTombstone {
    RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
}

#[test]
fn contains_key_inclusive_start() {
    let t = rt(b"b", b"d", 10);
    assert!(t.contains_key(b"b"));
}

#[test]
fn contains_key_exclusive_end() {
    let t = rt(b"b", b"d", 10);
    assert!(!t.contains_key(b"d"));
}

#[test]
fn contains_key_middle() {
    let t = rt(b"b", b"d", 10);
    assert!(t.contains_key(b"c"));
}

#[test]
fn contains_key_before_start() {
    let t = rt(b"b", b"d", 10);
    assert!(!t.contains_key(b"a"));
}

#[test]
fn not_visible_at_equal() {
    // Exclusive boundary: tombstone@10 is NOT visible at read_seqno=10
    let t = rt(b"a", b"z", 10);
    assert!(!t.visible_at(10));
}

#[test]
fn visible_at_higher() {
    let t = rt(b"a", b"z", 10);
    assert!(t.visible_at(20));
}

#[test]
fn not_visible_at_lower() {
    let t = rt(b"a", b"z", 10);
    assert!(!t.visible_at(9));
}

#[test]
fn should_suppress_yes() {
    let t = rt(b"b", b"d", 10);
    // read_seqno=11 (exclusive: tombstone@10 visible at 11)
    assert!(t.should_suppress(b"c", 5, 11));
}

#[test]
fn should_suppress_no_at_equal_seqno() {
    let t = rt(b"b", b"d", 10);
    // read_seqno=10: tombstone@10 NOT visible (exclusive boundary)
    assert!(!t.should_suppress(b"c", 5, 10));
}

#[test]
fn should_suppress_no_newer_kv() {
    let t = rt(b"b", b"d", 10);
    assert!(!t.should_suppress(b"c", 15, 20));
}

#[test]
fn should_suppress_no_not_visible() {
    let t = rt(b"b", b"d", 10);
    assert!(!t.should_suppress(b"c", 5, 9));
}

#[test]
fn should_suppress_no_outside_range() {
    let t = rt(b"b", b"d", 10);
    assert!(!t.should_suppress(b"e", 5, 11));
}

#[test]
fn ordering_by_start_asc() {
    let a = rt(b"a", b"z", 10);
    let b = rt(b"b", b"z", 10);
    assert!(a < b);
}

#[test]
fn ordering_by_seqno_desc() {
    let a = rt(b"a", b"z", 20);
    let b = rt(b"a", b"z", 10);
    assert!(a < b); // higher seqno comes first
}

#[test]
fn ordering_by_end_asc_tiebreaker() {
    let a = rt(b"a", b"m", 10);
    let b = rt(b"a", b"z", 10);
    assert!(a < b);
}

#[test]
fn intersect_overlap() {
    let t = rt(b"b", b"y", 10);
    let clipped = t.intersect_opt(b"d", b"g").unwrap();
    assert_eq!(clipped.start.as_ref(), b"d");
    assert_eq!(clipped.end.as_ref(), b"g");
    assert_eq!(clipped.seqno, 10);
}

#[test]
fn intersect_no_overlap() {
    let t = rt(b"b", b"d", 10);
    assert!(t.intersect_opt(b"e", b"g").is_none());
}

#[test]
fn intersect_partial_left() {
    let t = rt(b"b", b"f", 10);
    let clipped = t.intersect_opt(b"a", b"d").unwrap();
    assert_eq!(clipped.start.as_ref(), b"b");
    assert_eq!(clipped.end.as_ref(), b"d");
}

#[test]
fn intersect_partial_right() {
    let t = rt(b"b", b"f", 10);
    let clipped = t.intersect_opt(b"d", b"z").unwrap();
    assert_eq!(clipped.start.as_ref(), b"d");
    assert_eq!(clipped.end.as_ref(), b"f");
}

#[test]
fn fully_covers_yes() {
    let t = rt(b"a", b"z", 10);
    assert!(t.fully_covers(b"b", b"y"));
}

#[test]
fn fully_covers_exact_start() {
    let t = rt(b"a", b"z", 10);
    assert!(t.fully_covers(b"a", b"y"));
}

#[test]
fn fully_covers_no_end_equal() {
    let t = rt(b"a", b"z", 10);
    assert!(!t.fully_covers(b"a", b"z"));
}

#[test]
fn fully_covers_no_start_before() {
    let t = rt(b"b", b"z", 10);
    assert!(!t.fully_covers(b"a", b"y"));
}

#[test]
fn covering_rt_covers_table() {
    let crt = CoveringRt {
        start: UserKey::from(b"a" as &[u8]),
        end: UserKey::from(b"z" as &[u8]),
        seqno: 100,
    };
    assert!(crt.covers_table(b"b", b"y", 50));
}

#[test]
fn covering_rt_no_cover_seqno_too_low() {
    let crt = CoveringRt {
        start: UserKey::from(b"a" as &[u8]),
        end: UserKey::from(b"z" as &[u8]),
        seqno: 50,
    };
    assert!(!crt.covers_table(b"b", b"y", 100));
}

#[test]
fn upper_bound_exclusive_appends_zero() {
    let key = b"hello";
    let result = upper_bound_exclusive(key).unwrap();
    assert_eq!(result.as_ref(), b"hello\x00");
}

#[test]
fn upper_bound_exclusive_max_length_non_max_key_has_successor() {
    let key = vec![0xAA; usize::from(u16::MAX)];
    let successor = upper_bound_exclusive(&key).expect("non-max key should have successor");
    assert!(key.as_slice() < successor.as_ref());
    assert!(u16::try_from(successor.len()).is_ok());
}

#[test]
fn upper_bound_exclusive_true_max_returns_none() {
    let key = vec![0xFF; usize::from(u16::MAX)];
    assert!(upper_bound_exclusive(&key).is_none());
}
