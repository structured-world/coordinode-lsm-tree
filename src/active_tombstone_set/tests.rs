use super::*;
use crate::UserKey;

fn rt(start: &[u8], end: &[u8], seqno: SeqNo) -> RangeTombstone {
    RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
}

// ──── Forward tests ────

#[test]
fn forward_activate_and_suppress() {
    let mut set = ActiveTombstoneSet::new();
    set.activate(&rt(b"a", b"m", 10), 100);
    assert!(set.is_suppressed(5));
    assert!(!set.is_suppressed(10));
    assert!(!set.is_suppressed(15));
}

#[test]
fn forward_expire_at_end() {
    let mut set = ActiveTombstoneSet::new();
    set.activate(&rt(b"a", b"m", 10), 100);
    assert!(set.is_suppressed(5));
    set.expire_until(b"m"); // key == end, tombstone expires
    assert!(!set.is_suppressed(5));
}

#[test]
fn forward_expire_past_end() {
    let mut set = ActiveTombstoneSet::new();
    set.activate(&rt(b"a", b"m", 10), 100);
    set.expire_until(b"z");
    assert!(set.is_empty());
}

#[test]
fn forward_not_expired_before_end() {
    let mut set = ActiveTombstoneSet::new();
    set.activate(&rt(b"a", b"m", 10), 100);
    set.expire_until(b"l");
    assert!(set.is_suppressed(5)); // still active
}

#[test]
fn forward_invisible_tombstone_not_activated() {
    let mut set = ActiveTombstoneSet::new();
    set.activate(&rt(b"a", b"m", 10), 5); // seqno 10 > cutoff 5
    assert!(!set.is_suppressed(1));
    assert!(set.is_empty());
}

#[test]
fn forward_multiple_tombstones_max_seqno() {
    let mut set = ActiveTombstoneSet::new();
    set.activate(&rt(b"a", b"m", 10), 100);
    set.activate(&rt(b"b", b"n", 20), 100);
    assert_eq!(set.max_active_seqno(), Some(20));
    assert!(set.is_suppressed(15)); // 15 < 20
}

#[test]
fn forward_duplicate_end_seqno_accounting() {
    let mut set = ActiveTombstoneSet::new();
    set.activate(&rt(b"a", b"m", 10), 100);
    set.activate(&rt(b"b", b"m", 10), 100);
    assert_eq!(set.max_active_seqno(), Some(10));

    set.expire_until(b"m");
    assert_eq!(set.max_active_seqno(), None);
    assert!(set.is_empty());
}

#[test]
fn forward_initialize_from() {
    let mut set = ActiveTombstoneSet::new();
    set.initialize_from(vec![(rt(b"a", b"m", 10), 100), (rt(b"b", b"z", 20), 100)]);
    assert_eq!(set.max_active_seqno(), Some(20));
}

#[test]
fn forward_initialize_and_expire() {
    let mut set = ActiveTombstoneSet::new();
    set.initialize_from(vec![(rt(b"a", b"d", 10), 100), (rt(b"b", b"f", 20), 100)]);
    set.expire_until(b"e"); // expires [a,d) but not [b,f)
    assert_eq!(set.max_active_seqno(), Some(20));
    set.expire_until(b"f"); // expires [b,f)
    assert!(set.is_empty());
}

#[test]
fn forward_mixed_cutoffs_activates_only_visible_rt() {
    let mut set = ActiveTombstoneSet::new();
    // RT from source with cutoff 15 — visible (10 < 15)
    set.activate(&rt(b"a", b"m", 10), 15);
    // RT from source with cutoff 5 — NOT visible (10 >= 5)
    set.activate(&rt(b"a", b"z", 10), 5);
    assert_eq!(set.max_active_seqno(), Some(10));
    assert!(!set.is_empty());

    // Expire past the first RT's end; the set should now be empty if the
    // second RT was never incorrectly activated.
    set.expire_until(b"m");
    assert!(set.is_empty());
}

#[test]
fn forward_expire_narrower_tombstone_before_wider_one() {
    let mut set = ActiveTombstoneSet::new();
    set.activate(&rt(b"\x00", b"\x06", 3), 100);
    set.activate(&rt(b"\x00", b"\x01", 5), 100);

    assert_eq!(set.max_active_seqno(), Some(5));
    set.expire_until(b"\x02");

    assert_eq!(set.max_active_seqno(), Some(3));
    assert!(!set.is_suppressed(4));
    assert!(set.is_suppressed(2));
}

// ──── Reverse tests ────

#[test]
fn reverse_activate_and_suppress() {
    let mut set = ActiveTombstoneSetReverse::new();
    set.activate(&rt(b"a", b"m", 10), 100);
    assert!(set.is_suppressed(5));
    assert!(!set.is_suppressed(10));
}

#[test]
fn reverse_expire_before_start() {
    let mut set = ActiveTombstoneSetReverse::new();
    set.activate(&rt(b"d", b"m", 10), 100);

    set.expire_until(b"c");
    assert!(set.is_empty());
}

#[test]
fn reverse_initialize_from() {
    let mut set = ActiveTombstoneSetReverse::new();
    set.initialize_from(vec![(rt(b"a", b"m", 10), 100), (rt(b"b", b"z", 20), 100)]);
    assert_eq!(set.max_active_seqno(), Some(20));
}

#[test]
fn reverse_not_expired_at_start() {
    let mut set = ActiveTombstoneSetReverse::new();
    set.activate(&rt(b"d", b"m", 10), 100);

    set.expire_until(b"d");
    assert!(set.is_suppressed(5));
}

#[test]
fn reverse_invisible_tombstone_not_activated() {
    let mut set = ActiveTombstoneSetReverse::new();
    set.activate(&rt(b"a", b"m", 10), 5);
    assert!(set.is_empty());
}

#[test]
fn reverse_duplicate_end_seqno_accounting() {
    let mut set = ActiveTombstoneSetReverse::new();
    set.activate(&rt(b"d", b"m", 10), 100);
    set.activate(&rt(b"d", b"n", 10), 100);
    assert_eq!(set.max_active_seqno(), Some(10));

    set.expire_until(b"c");
    assert_eq!(set.max_active_seqno(), None);
    assert!(set.is_empty());
}

#[test]
fn reverse_multiple_tombstones() {
    let mut set = ActiveTombstoneSetReverse::new();
    set.activate(&rt(b"a", b"m", 10), 100);
    set.activate(&rt(b"f", b"z", 20), 100);
    assert_eq!(set.max_active_seqno(), Some(20));

    set.expire_until(b"e");
    assert_eq!(set.max_active_seqno(), Some(10));
}

#[test]
fn reverse_mixed_cutoffs_activates_only_visible_rt() {
    let mut set = ActiveTombstoneSetReverse::new();
    // RT from source with cutoff 15 — visible (10 < 15)
    set.activate(&rt(b"n", b"z", 10), 15);
    // RT from source with cutoff 5 — NOT visible (10 >= 5)
    set.activate(&rt(b"a", b"m", 10), 5);
    assert_eq!(set.max_active_seqno(), Some(10));

    // Advance expiry past the visible tombstone's start but not the
    // invisible one's.  If only the visible RT was activated, the set
    // should become empty.
    set.expire_until(b"l");
    assert_eq!(set.max_active_seqno(), None);
    assert!(set.is_empty());
}
