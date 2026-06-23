use super::*;
use test_log::test;

use crate::comparator::DefaultUserComparator;

#[derive(Clone)]
struct FakeTable {
    id: u64,
    key_range: KeyRange,
}

impl Ranged for FakeTable {
    fn key_range(&self) -> &KeyRange {
        &self.key_range
    }
}

fn s(id: u64, min: &str, max: &str) -> FakeTable {
    FakeTable {
        id,
        key_range: KeyRange::new((min.as_bytes().into(), max.as_bytes().into())),
    }
}

/// Reverse comparator for testing non-lexicographic ordering.
struct ReverseCmp;

impl UserComparator for ReverseCmp {
    fn name(&self) -> &'static str {
        "reverse"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
        b.cmp(a)
    }
}

#[test]
fn run_aggregate_key_range() {
    let items = vec![
        s(0, "a", "d"),
        s(1, "e", "j"),
        s(2, "k", "o"),
        s(3, "p", "z"),
    ];
    let run = Run(items);

    assert_eq!(
        KeyRange::new((b"a".into(), b"z".into())),
        run.aggregate_key_range(),
    );
}

#[test]
fn run_point_lookup() {
    let items = vec![
        s(0, "a", "d"),
        s(1, "e", "j"),
        s(2, "k", "o"),
        s(3, "p", "z"),
    ];
    let run = Run(items);

    assert_eq!(0, run.get_for_key(b"a").unwrap().id);
    assert_eq!(0, run.get_for_key(b"aaa").unwrap().id);
    assert_eq!(0, run.get_for_key(b"b").unwrap().id);
    assert_eq!(0, run.get_for_key(b"c").unwrap().id);
    assert_eq!(0, run.get_for_key(b"d").unwrap().id);
    assert_eq!(1, run.get_for_key(b"e").unwrap().id);
    assert_eq!(1, run.get_for_key(b"j").unwrap().id);
    assert_eq!(2, run.get_for_key(b"k").unwrap().id);
    assert_eq!(2, run.get_for_key(b"o").unwrap().id);
    assert_eq!(3, run.get_for_key(b"p").unwrap().id);
    assert_eq!(3, run.get_for_key(b"z").unwrap().id);
    assert!(run.get_for_key(b"zzz").is_none());
}

#[test]
fn run_range_culling() {
    let items = vec![
        s(0, "a", "d"),
        s(1, "e", "j"),
        s(2, "k", "o"),
        s(3, "p", "z"),
    ];
    let run = Run(items);

    assert_eq!(Some((0, 3)), run.range_overlap_indexes::<&[u8], _>(&..));
    assert_eq!(
        Some((0, 0)),
        run.range_overlap_indexes(&(b"a" as &[u8]..=b"a"))
    );
    assert_eq!(
        Some((0, 0)),
        run.range_overlap_indexes(&(b"a" as &[u8]..=b"b"))
    );
    assert_eq!(
        Some((0, 0)),
        run.range_overlap_indexes(&(b"a" as &[u8]..=b"d"))
    );
    assert_eq!(
        Some((0, 0)),
        run.range_overlap_indexes(&(b"d" as &[u8]..=b"d"))
    );
    assert_eq!(
        Some((0, 0)),
        run.range_overlap_indexes(&(b"a" as &[u8]..b"d"))
    );
    assert_eq!(
        Some((0, 1)),
        run.range_overlap_indexes(&(b"a" as &[u8]..=b"g"))
    );
    assert_eq!(
        Some((1, 1)),
        run.range_overlap_indexes(&(b"j" as &[u8]..=b"j"))
    );
    assert_eq!(
        Some((0, 3)),
        run.range_overlap_indexes(&(b"a" as &[u8]..=b"z"))
    );
    assert_eq!(
        Some((3, 3)),
        run.range_overlap_indexes(&(b"z" as &[u8]..=b"zzz"))
    );
    assert_eq!(Some((3, 3)), run.range_overlap_indexes(&(b"z" as &[u8]..)));
    assert!(
        run.range_overlap_indexes(&(b"zzz" as &[u8]..=b"zzzzzzz"))
            .is_none()
    );
}

#[test]
fn run_range_contained() {
    use crate::TableId;

    let items = vec![
        s(0, "a", "d"),
        s(1, "e", "j"),
        s(2, "k", "o"),
        s(3, "p", "z"),
    ];
    let run = Run(items);

    assert_eq!(
        &[] as &[TableId],
        &*run
            .get_contained(&KeyRange::new((b"a".into(), b"a".into())))
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    assert_eq!(
        &[0],
        &*run
            .get_contained(&KeyRange::new((b"a".into(), b"d".into())))
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    assert_eq!(
        &[0, 1],
        &*run
            .get_contained(&KeyRange::new((b"a".into(), b"j".into())))
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    assert_eq!(
        &[0, 1],
        &*run
            .get_contained(&KeyRange::new((b"a".into(), b"k".into())))
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    assert_eq!(
        &[0, 1],
        &*run
            .get_contained(&KeyRange::new((b"a".into(), b"l".into())))
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    assert_eq!(
        &[0, 1, 2, 3],
        &*run
            .get_contained(&KeyRange::new((b"a".into(), b"z".into())))
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );
}

#[test]
fn run_range_contained_cmp_reverse() {
    use crate::TableId;
    use crate::comparator::UserComparator;

    struct ReverseCmp;
    impl UserComparator for ReverseCmp {
        fn name(&self) -> &'static str {
            "reverse"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
            b.cmp(a)
        }
    }

    // Reverse comparator: tables store (comparator_min, comparator_max).
    // In reverse order "z" < "p" < "o" < ... < "a", so key ranges are
    // (z,p), (o,k), (j,e), (d,a) — matching production SST metadata.
    let items = vec![
        s(0, "z", "p"),
        s(1, "o", "k"),
        s(2, "j", "e"),
        s(3, "d", "a"),
    ];
    let run = Run(items);
    let cmp = ReverseCmp;

    // Full range contains all
    assert_eq!(
        &[0, 1, 2, 3],
        &*run
            .get_contained_cmp(&KeyRange::new((b"z".into(), b"a".into())), &cmp)
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    // Partial: z..k contains tables 0 and 1
    assert_eq!(
        &[0, 1],
        &*run
            .get_contained_cmp(&KeyRange::new((b"z".into(), b"k".into())), &cmp)
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    // Exact match: single table
    assert_eq!(
        &[2 as TableId],
        &*run
            .get_contained_cmp(&KeyRange::new((b"j".into(), b"e".into())), &cmp)
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    // No table fully contained
    assert_eq!(
        &[] as &[TableId],
        &*run
            .get_contained_cmp(&KeyRange::new((b"z".into(), b"z".into())), &cmp)
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );
}

#[test]
fn run_range_overlaps() {
    let items = vec![
        s(0, "a", "d"),
        s(1, "e", "j"),
        s(2, "k", "o"),
        s(3, "p", "z"),
    ];
    let run = Run(items);

    assert_eq!(
        &[0],
        &*run
            .get_overlapping(&KeyRange::new((b"a".into(), b"a".into())))
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    assert_eq!(
        &[0],
        &*run
            .get_overlapping(&KeyRange::new((b"d".into(), b"d".into())))
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    assert_eq!(
        &[0],
        &*run
            .get_overlapping(&KeyRange::new((b"a".into(), b"d".into())))
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    assert_eq!(
        &[0, 1],
        &*run
            .get_overlapping(&KeyRange::new((b"a".into(), b"f".into())))
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    assert_eq!(
        &[0, 1, 2, 3],
        &*run
            .get_overlapping(&KeyRange::new((b"a".into(), b"zzz".into())))
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );

    assert_eq!(
        &[] as &[u64],
        &*run
            .get_overlapping(&KeyRange::new((b"zzz".into(), b"zzzz".into())))
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>(),
    );
}

#[test]
fn push_lexicographic_sorts_by_min_key() {
    let mut run = Run::new(vec![s(0, "e", "j")]).unwrap();

    // Insert a table whose min key is lexicographically before "e"
    run.push_lexicographic(s(1, "a", "d"));
    assert_eq!(1, run[0].id); // "a" sorts first
    assert_eq!(0, run[1].id); // "e" sorts second
}

#[test]
fn push_cmp_sorts_by_comparator() {
    let mut run = Run::new(vec![s(0, "a", "d")]).unwrap();

    // With default (lexicographic) comparator, "e" > "a" → appended after
    run.push_cmp(s(1, "e", "j"), &DefaultUserComparator);
    assert_eq!(0, run[0].id);
    assert_eq!(1, run[1].id);

    // With reverse comparator, "k" is "smaller" than "e" → sorted before
    let mut rev_run = Run::new(vec![s(0, "e", "j")]).unwrap();
    rev_run.push_cmp(s(1, "k", "o"), &ReverseCmp);
    // Reverse order: k > e lexicographically, but ReverseCmp reverses → k < e
    assert_eq!(1, rev_run[0].id); // "k" sorts first in reverse
    assert_eq!(0, rev_run[1].id); // "e" sorts second in reverse
}

#[test]
fn get_overlapping_cmp_reverse() {
    // With reverse comparator, SST key ranges store (comparator-min, comparator-max).
    // Reverse comparator-min is the lexicographic max, so min > max lexicographically.
    // Run sorted by comparator-min: z, o, j, d (descending lexicographic).
    let items = vec![
        s(3, "z", "p"),
        s(2, "o", "k"),
        s(1, "j", "e"),
        s(0, "d", "a"),
    ];
    let run = Run(items);

    let result = run
        .get_overlapping_cmp(&KeyRange::new((b"j".into(), b"j".into())), &ReverseCmp)
        .iter()
        .map(|x| x.id)
        .collect::<Vec<_>>();
    assert_eq!(&[1], &*result);

    let result = run
        .get_overlapping_cmp(&KeyRange::new((b"o".into(), b"e".into())), &ReverseCmp)
        .iter()
        .map(|x| x.id)
        .collect::<Vec<_>>();
    assert_eq!(&[2, 1], &*result);
}

#[test]
fn range_overlap_indexes_cmp_reverse() {
    let items = vec![
        s(3, "z", "p"),
        s(2, "o", "k"),
        s(1, "j", "e"),
        s(0, "d", "a"),
    ];
    let run = Run(items);
    let cmp = ReverseCmp;

    assert_eq!(
        Some((0, 3)),
        run.range_overlap_indexes_cmp::<&[u8], _>(&.., &cmp)
    );

    // Inclusive range covering one table (z..=p in reverse = first table)
    assert_eq!(
        Some((0, 0)),
        run.range_overlap_indexes_cmp(&(b"z" as &[u8]..=b"p"), &cmp)
    );

    // Inclusive range covering two tables (z..=k)
    assert_eq!(
        Some((0, 1)),
        run.range_overlap_indexes_cmp(&(b"z" as &[u8]..=b"k"), &cmp)
    );

    // Out of range (beyond last table in reverse order)
    assert!(
        run.range_overlap_indexes_cmp(&(b"\x00" as &[u8]..=b"\x00"), &cmp)
            .is_none()
    );

    // Exclusive start bound: skip first table (z..p), start from second (o..k)
    let bounds_excl_start: (Bound<&[u8]>, Bound<&[u8]>) =
        (Bound::Excluded(b"p"), Bound::Included(b"a"));
    assert_eq!(
        Some((1, 3)),
        run.range_overlap_indexes_cmp::<&[u8], _>(&bounds_excl_start, &cmp)
    );

    // Exclusive end bound: include first table only
    let bounds_excl_end: (Bound<&[u8]>, Bound<&[u8]>) =
        (Bound::Included(b"z"), Bound::Excluded(b"o"));
    assert_eq!(
        Some((0, 0)),
        run.range_overlap_indexes_cmp::<&[u8], _>(&bounds_excl_end, &cmp)
    );

    // Semi-open range (start..): Included start, Unbounded end
    assert_eq!(
        Some((2, 3)),
        run.range_overlap_indexes_cmp(&(b"j" as &[u8]..), &cmp)
    );
}

#[test]
fn get_for_key_cmp_reverse() {
    let items = vec![
        s(3, "z", "p"),
        s(2, "o", "k"),
        s(1, "j", "e"),
        s(0, "d", "a"),
    ];
    let run = Run(items);
    let cmp = ReverseCmp;

    assert_eq!(3, run.get_for_key_cmp(b"z", &cmp).unwrap().id);
    assert_eq!(3, run.get_for_key_cmp(b"p", &cmp).unwrap().id);
    assert_eq!(2, run.get_for_key_cmp(b"k", &cmp).unwrap().id);
    assert_eq!(1, run.get_for_key_cmp(b"e", &cmp).unwrap().id);
    assert_eq!(0, run.get_for_key_cmp(b"a", &cmp).unwrap().id);
    assert!(run.get_for_key_cmp(b"\x00", &cmp).is_none());
}
