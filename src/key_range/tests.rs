use super::*;
use test_log::test;

fn int_key_range(a: u64, b: u64) -> KeyRange {
    KeyRange::new((a.to_be_bytes().into(), b.to_be_bytes().into()))
}

fn string_key_range(a: &str, b: &str) -> KeyRange {
    KeyRange::new((a.as_bytes().into(), b.as_bytes().into()))
}

#[test]
fn key_range_aggregate_1() {
    let ranges = [
        int_key_range(2, 4),
        int_key_range(0, 4),
        int_key_range(7, 10),
    ];
    let aggregated = KeyRange::aggregate(ranges.iter());
    let (min, max) = aggregated.as_tuple();
    assert_eq!([0, 0, 0, 0, 0, 0, 0, 0], &**min);
    assert_eq!([0, 0, 0, 0, 0, 0, 0, 10], &**max);
}

#[test]
fn key_range_aggregate_2() {
    let ranges = [
        int_key_range(6, 7),
        int_key_range(0, 2),
        int_key_range(0, 10),
    ];
    let aggregated = KeyRange::aggregate(ranges.iter());
    let (min, max) = aggregated.as_tuple();
    assert_eq!([0, 0, 0, 0, 0, 0, 0, 0], &**min);
    assert_eq!([0, 0, 0, 0, 0, 0, 0, 10], &**max);
}

mod is_disjoint {
    use super::*;
    use test_log::test;

    #[test]
    fn key_range_number() {
        let ranges = [&int_key_range(0, 4), &int_key_range(0, 4)];
        assert!(!KeyRange::is_disjoint(&ranges));
    }

    #[test]
    fn key_range_string() {
        let ranges = [&string_key_range("a", "d"), &string_key_range("g", "z")];
        assert!(KeyRange::is_disjoint(&ranges));
    }

    #[test]
    fn key_range_not_disjoint() {
        let ranges = [&string_key_range("a", "f"), &string_key_range("b", "h")];
        assert!(!KeyRange::is_disjoint(&ranges));

        let ranges = [
            &string_key_range("a", "d"),
            &string_key_range("d", "e"),
            &string_key_range("f", "z"),
        ];
        assert!(!KeyRange::is_disjoint(&ranges));
    }
}

mod overflap_key_range {
    use super::*;
    use test_log::test;

    #[test]
    fn key_range_overlap() {
        let a = string_key_range("a", "f");
        let b = string_key_range("b", "h");
        assert!(a.overlaps_with_key_range(&b));
    }

    #[test]
    fn key_range_overlap_edge() {
        let a = string_key_range("a", "f");
        let b = string_key_range("f", "t");
        assert!(a.overlaps_with_key_range(&b));
    }

    #[test]
    fn key_range_no_overlap() {
        let a = string_key_range("a", "f");
        let b = string_key_range("g", "t");
        assert!(!a.overlaps_with_key_range(&b));
    }
}

mod overlaps_with_bounds {
    use super::*;
    use core::ops::Bound::{Excluded, Included, Unbounded};
    use test_log::test;

    #[test]
    fn inclusive() {
        let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
        let bounds = (Included(b"key1" as &[u8]), Included(b"key5" as &[u8]));
        assert!(key_range.overlaps_with_bounds(&bounds));
    }

    #[test]
    fn exclusive() {
        let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
        let bounds = (Excluded(b"key0" as &[u8]), Excluded(b"key6" as &[u8]));
        assert!(key_range.overlaps_with_bounds(&bounds));
    }

    #[test]
    fn no_overlap() {
        let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
        let bounds = (Excluded(b"key5" as &[u8]), Excluded(b"key6" as &[u8]));
        assert!(!key_range.overlaps_with_bounds(&bounds));
    }

    #[test]
    fn unbounded() {
        let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
        let bounds = (Unbounded, Unbounded);
        assert!(key_range.overlaps_with_bounds(&bounds));
    }

    #[test]
    fn semi_open_0() {
        let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
        let bounds = (Unbounded, Excluded(b"key1" as &[u8]));
        assert!(!key_range.overlaps_with_bounds(&bounds));
    }

    #[test]
    fn semi_open_1() {
        let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
        let bounds = (Excluded(b"key5" as &[u8]), Unbounded);
        assert!(!key_range.overlaps_with_bounds(&bounds));
    }

    #[test]
    fn semi_open_2() {
        let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
        let bounds = (Unbounded, Included(b"key1" as &[u8]));
        assert!(key_range.overlaps_with_bounds(&bounds));
    }

    #[test]
    fn semi_open_3() {
        let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
        let bounds = (Included(b"key5" as &[u8]), Unbounded);
        assert!(key_range.overlaps_with_bounds(&bounds));
    }

    #[test]
    fn semi_open_4() {
        let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
        let bounds = (Unbounded, Included(b"key5" as &[u8]));
        assert!(key_range.overlaps_with_bounds(&bounds));
    }

    #[test]
    fn semi_open_5() {
        let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
        let bounds = (Unbounded, Included(b"key6" as &[u8]));
        assert!(key_range.overlaps_with_bounds(&bounds));
    }

    #[test]
    fn semi_open_6() {
        let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
        let bounds = (Included(b"key0" as &[u8]), Unbounded);
        assert!(key_range.overlaps_with_bounds(&bounds));
    }

    #[test]
    fn semi_open_7() {
        let key_range = KeyRange(UserKey::from("key5"), UserKey::from("key8"));
        let bounds = (Unbounded, Excluded(b"key6" as &[u8]));
        assert!(key_range.overlaps_with_bounds(&bounds));
    }
}

mod overlaps_with_bounds_cmp {
    use super::*;
    use crate::comparator::UserComparator;
    use core::ops::Bound::{Excluded, Included, Unbounded};
    use test_log::test;

    struct ReverseComparator;

    impl UserComparator for ReverseComparator {
        fn name(&self) -> &'static str {
            "reverse"
        }

        fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
            b.cmp(a)
        }
    }

    #[test]
    fn both_unbounded() {
        let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
        let bounds = (Unbounded, Unbounded);
        assert!(kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
    }

    #[test]
    fn inclusive_reverse_overlap() {
        // Reverse: f < e < d < c < b < a. Key range min=f, max=a.
        // Bounds "e"..="b" in reverse → should overlap.
        let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
        let bounds = (Included(b"e" as &[u8]), Included(b"b" as &[u8]));
        assert!(kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
    }

    #[test]
    fn no_overlap_reverse() {
        // Key range f..a (reverse). Bounds "z"..="x" → z < x in reverse?
        // No: z and x are both below f in reverse order (reverse: a > b > ... > z).
        // Actually reverse: z < y < x < ... < a. So "z"..="x" is valid.
        // kr min=f, max=a. cmp("z", "a")=reverse of z.cmp(a)=reverse(Greater)=Less.
        // So z < a in reverse → bounds lo "z" is below kr min "f"? Let's check:
        // lo_included: cmp("z", "a"(max)) = reverse(z.cmp(a)) = reverse(Greater) = Less.
        // Less != Greater → true. hi_included: cmp("x", "f"(min)) = reverse(x.cmp(f)) = reverse(Greater) = Less.
        // Less is Less → false. So no overlap.
        let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
        let bounds = (Included(b"z" as &[u8]), Included(b"x" as &[u8]));
        assert!(!kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
    }

    #[test]
    fn semi_open_hi_unbounded() {
        let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
        let bounds = (Included(b"c" as &[u8]), Unbounded);
        assert!(kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
    }

    #[test]
    fn semi_open_lo_unbounded() {
        let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
        let bounds = (Unbounded, Included(b"c" as &[u8]));
        assert!(kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
    }

    #[test]
    fn exclusive_overlap() {
        let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
        // Excluded "a" as hi → hi must be > min "f" in reverse.
        // cmp("a", "f") = reverse(a.cmp(f)) = reverse(Less) = Greater → true.
        // But excluded "f" as lo → lo must be < max "a" in reverse.
        // cmp("f", "a") = reverse(f.cmp(a)) = reverse(Greater) = Less → true.
        // Both true → overlaps.
        let bounds = (Excluded(b"f" as &[u8]), Excluded(b"a" as &[u8]));
        assert!(kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
    }

    #[test]
    fn semi_open_excluded_no_overlap() {
        // kr min=f, max=a. Excluded "f" as hi, lo unbounded.
        // cmp("f", "f"(min)) = reverse(f.cmp(f)) = Equal. Greater? No → false.
        let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
        let bounds = (Unbounded, Excluded(b"f" as &[u8]));
        assert!(!kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
    }

    #[test]
    fn semi_open_excluded_lo_no_overlap() {
        // kr min=f, max=a. lo=Excluded("a"), hi unbounded.
        // cmp("a", "a"(max)) = Equal. Less? No → false.
        let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
        let bounds = (Excluded(b"a" as &[u8]), Unbounded);
        assert!(!kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
    }
}

#[test]
fn key_range_contains_key() {
    let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
    assert!(!key_range.contains_key(b"key0"));
    assert!(!key_range.contains_key(b"key01"));
    assert!(key_range.contains_key(b"key1"));
    assert!(key_range.contains_key(b"key2"));
    assert!(key_range.contains_key(b"key3"));
    assert!(key_range.contains_key(b"key4"));
    assert!(key_range.contains_key(b"key4x"));
    assert!(key_range.contains_key(b"key5"));
    assert!(!key_range.contains_key(b"key5x"));
    assert!(!key_range.contains_key(b"key6"));
}

mod merge_sorted_cmp {
    use super::*;
    use crate::comparator::{DefaultUserComparator, UserComparator};
    use test_log::test;

    #[test]
    fn empty_input() {
        let result = KeyRange::merge_sorted_cmp(vec![], &DefaultUserComparator);
        assert!(result.is_empty());
    }

    #[test]
    fn single_range() {
        let input = vec![string_key_range("a", "d")];
        let result = KeyRange::merge_sorted_cmp(input, &DefaultUserComparator);
        assert_eq!(result, vec![string_key_range("a", "d")]);
    }

    #[test]
    fn disjoint_ranges_stay_separate() {
        let input = vec![
            string_key_range("a", "d"),
            string_key_range("f", "h"),
            string_key_range("k", "z"),
        ];
        let result = KeyRange::merge_sorted_cmp(input, &DefaultUserComparator);
        assert_eq!(
            result,
            vec![
                string_key_range("a", "d"),
                string_key_range("f", "h"),
                string_key_range("k", "z"),
            ]
        );
    }

    #[test]
    fn overlapping_ranges_merge() {
        let input = vec![
            string_key_range("a", "f"),
            string_key_range("c", "h"),
            string_key_range("g", "z"),
        ];
        let result = KeyRange::merge_sorted_cmp(input, &DefaultUserComparator);
        assert_eq!(result, vec![string_key_range("a", "z")]);
    }

    #[test]
    fn adjacent_ranges_merge() {
        // [a,d] and [d,f] touch at "d" — should merge
        let input = vec![string_key_range("a", "d"), string_key_range("d", "f")];
        let result = KeyRange::merge_sorted_cmp(input, &DefaultUserComparator);
        assert_eq!(result, vec![string_key_range("a", "f")]);
    }

    #[test]
    fn contained_range_absorbed() {
        // [a,z] fully contains [c,d]
        let input = vec![string_key_range("a", "z"), string_key_range("c", "d")];
        let result = KeyRange::merge_sorted_cmp(input, &DefaultUserComparator);
        assert_eq!(result, vec![string_key_range("a", "z")]);
    }

    #[test]
    fn mixed_disjoint_and_overlapping() {
        // Two clusters: [a,f]+[c,h] merge; [x,z] stays separate
        let input = vec![
            string_key_range("a", "f"),
            string_key_range("c", "h"),
            string_key_range("x", "z"),
        ];
        let result = KeyRange::merge_sorted_cmp(input, &DefaultUserComparator);
        assert_eq!(
            result,
            vec![string_key_range("a", "h"), string_key_range("x", "z")]
        );
    }

    #[test]
    fn reverse_comparator() {
        struct ReverseCmp;
        impl UserComparator for ReverseCmp {
            fn name(&self) -> &'static str {
                "reverse"
            }
            fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
                b.cmp(a)
            }
        }

        // In reverse order: z > y > ... > p > o > ... > a
        // Sorted by comparator-min: [z,o], [o,k], [d,a]
        // [z,o] and [o,k] touch at "o" → should merge to [z,k]
        // [d,a] is separate
        let input = vec![
            string_key_range("z", "o"),
            string_key_range("o", "k"),
            string_key_range("d", "a"),
        ];
        let result = KeyRange::merge_sorted_cmp(input, &ReverseCmp);
        assert_eq!(
            result,
            vec![string_key_range("z", "k"), string_key_range("d", "a")]
        );
    }
}
