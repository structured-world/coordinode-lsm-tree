use super::*;
use crate::comparator::DefaultUserComparator;
use proptest::prelude::*;

/// Keys in reverse byte order, to prove nothing assumes bytewise bounds.
struct Reversed;

impl UserComparator for Reversed {
    fn name(&self) -> &'static str {
        "reversed"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a)
    }
}

fn rt(start: &str, end: &str, seqno: SeqNo) -> RangeTombstone {
    RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
}

fn sorted(
    mut list: Vec<(RangeTombstone, SeqNo)>,
    comparator: &dyn UserComparator,
) -> ScanTombstones {
    list.sort_by(|a, b| a.0.cmp_with_comparator(&b.0, comparator));
    ScanTombstones::new(list)
}

/// The rule itself: every tombstone asked in turn.
fn each_asked(
    list: &[(RangeTombstone, SeqNo)],
    key: &[u8],
    seqno: SeqNo,
    comparator: &dyn UserComparator,
) -> bool {
    list.iter()
        .any(|(rt, cutoff)| rt.should_suppress_with(key, seqno, *cutoff, comparator))
}

#[test]
fn nothing_is_suppressed_without_tombstones() {
    let tombstones = ScanTombstones::new(Vec::new());
    assert!(!tombstones.suppresses(b"a", 0, &DefaultUserComparator));
}

#[test]
fn a_tombstone_holds_its_start_and_not_its_end() {
    let tombstones = sorted(vec![(rt("b", "d", 5), 10)], &DefaultUserComparator);
    let holds = |key: &[u8]| tombstones.suppresses(key, 4, &DefaultUserComparator);
    assert!(!holds(b"a"));
    assert!(holds(b"b"));
    assert!(holds(b"c"));
    assert!(!holds(b"d"));
}

#[test]
fn a_version_at_the_tombstones_seqno_survives_it() {
    let tombstones = sorted(vec![(rt("a", "c", 5), 10)], &DefaultUserComparator);
    assert!(tombstones.suppresses(b"b", 4, &DefaultUserComparator));
    assert!(!tombstones.suppresses(b"b", 5, &DefaultUserComparator));
    assert!(!tombstones.suppresses(b"b", 6, &DefaultUserComparator));
}

/// A newer tombstone the snapshot cannot see must not stand for an older one
/// it can: a single newest seqno over all of them would suppress too much.
#[test]
fn a_tombstone_above_its_cutoff_suppresses_nothing() {
    let tombstones = sorted(
        vec![(rt("a", "c", 9), 9), (rt("a", "c", 3), 9)],
        &DefaultUserComparator,
    );
    assert!(tombstones.suppresses(b"b", 2, &DefaultUserComparator));
    assert!(!tombstones.suppresses(b"b", 5, &DefaultUserComparator));
}

/// One source's overlay sees a tombstone the others do not.
#[test]
fn each_tombstone_is_seen_through_its_own_cutoff() {
    let tombstones = sorted(
        vec![(rt("a", "c", 7), 8), (rt("b", "d", 7), 5)],
        &DefaultUserComparator,
    );
    assert!(tombstones.suppresses(b"a", 6, &DefaultUserComparator));
    assert!(!tombstones.suppresses(b"c", 6, &DefaultUserComparator));
}

/// When the newest tombstone over a stretch ends, the older one under it takes
/// over.
#[test]
fn an_older_tombstone_shows_past_the_end_of_a_newer_one() {
    let tombstones = sorted(
        vec![(rt("a", "z", 3), 10), (rt("c", "e", 8), 10)],
        &DefaultUserComparator,
    );
    assert!(tombstones.suppresses(b"d", 7, &DefaultUserComparator));
    assert!(!tombstones.suppresses(b"f", 7, &DefaultUserComparator));
    assert!(tombstones.suppresses(b"f", 2, &DefaultUserComparator));
}

/// A range empty under the comparator covers no key, and inverted bounds must
/// not leave a floor behind.
#[test]
fn an_inverted_tombstone_holds_nothing() {
    let tombstones = sorted(
        vec![(rt("d", "b", 5), 10), (rt("x", "x", 5), 10)],
        &DefaultUserComparator,
    );
    for key in [&b"a"[..], b"b", b"c", b"d", b"e", b"x"] {
        assert!(!tombstones.suppresses(key, 1, &DefaultUserComparator));
    }
}

#[test]
fn bounds_follow_the_comparator() {
    let tombstones = sorted(vec![(rt("d", "b", 5), 10)], &Reversed);
    assert!(tombstones.suppresses(b"d", 4, &Reversed));
    assert!(tombstones.suppresses(b"c", 4, &Reversed));
    assert!(!tombstones.suppresses(b"b", 4, &Reversed));
    assert!(!tombstones.suppresses(b"e", 4, &Reversed));
}

fn tombstones() -> impl Strategy<Value = Vec<(RangeTombstone, SeqNo)>> {
    prop::collection::vec((0_u8..24, 0_u8..24, 0_u64..16, 0_u64..16), 0..24).prop_map(|raw| {
        raw.into_iter()
            .map(|(start, end, seqno, cutoff)| {
                let tombstone = RangeTombstone::new(
                    UserKey::from(&[start][..]),
                    UserKey::from(&[end][..]),
                    seqno,
                );
                (tombstone, cutoff)
            })
            .collect()
    })
}

proptest! {
    /// The index answers as asking every tombstone does, overlapping,
    /// duplicated, inverted and unseen ones among them.
    #[test]
    fn the_index_agrees_with_asking_each_tombstone(list in tombstones()) {
        for comparator in [
            &DefaultUserComparator as &dyn UserComparator,
            &Reversed as &dyn UserComparator,
        ] {
            let index = sorted(list.clone(), comparator);
            for key in 0_u8..26 {
                for seqno in 0_u64..17 {
                    prop_assert_eq!(
                        index.suppresses(&[key], seqno, comparator),
                        each_asked(index.list(), &[key], seqno, comparator),
                        "key {} seqno {}", key, seqno,
                    );
                }
            }
        }
    }
}
