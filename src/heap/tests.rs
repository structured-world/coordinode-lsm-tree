use super::*;
use crate::ValueType::Value;
use crate::comparator;
use test_log::test;

fn entry(key: &str, seqno: u64) -> HeapEntry {
    HeapEntry::new(0, InternalValue::from_components(key, b"", seqno, Value))
}

fn entry_src(key: &str, seqno: u64, src: usize) -> HeapEntry {
    HeapEntry::new(src, InternalValue::from_components(key, b"", seqno, Value))
}

#[test]
fn min_ordering() {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(4, cmp);

    heap.push(entry("c", 0));
    heap.push(entry("a", 0));
    heap.push(entry("d", 0));
    heap.push(entry("b", 0));

    let keys: Vec<_> = std::iter::from_fn(|| heap.pop_min())
        .map(|e| String::from_utf8_lossy(&e.value.key.user_key).to_string())
        .collect();
    assert_eq!(keys, ["a", "b", "c", "d"]);
}

#[test]
fn max_ordering() {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(4, cmp);

    heap.push(entry("c", 0));
    heap.push(entry("a", 0));
    heap.push(entry("d", 0));
    heap.push(entry("b", 0));

    let keys: Vec<_> = std::iter::from_fn(|| heap.pop_max())
        .map(|e| String::from_utf8_lossy(&e.value.key.user_key).to_string())
        .collect();
    assert_eq!(keys, ["d", "c", "b", "a"]);
}

#[test]
fn replace_min_stays() {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(4, cmp);

    heap.push(entry("a", 0));
    heap.push(entry("c", 0));
    heap.push(entry("d", 0));

    // Replace "a" with "b" — still the minimum.
    let old = heap.replace_min(entry("b", 0));
    assert_eq!(&*old.value.key.user_key, b"a");
    assert_eq!(&*heap.peek_min().unwrap().value.key.user_key, b"b");
}

#[test]
fn replace_min_slides() {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(4, cmp);

    heap.push(entry("a", 0));
    heap.push(entry("b", 0));
    heap.push(entry("c", 0));

    // Replace "a" with "z" — slides to end, "b" becomes min.
    let old = heap.replace_min(entry("z", 0));
    assert_eq!(&*old.value.key.user_key, b"a");
    assert_eq!(&*heap.peek_min().unwrap().value.key.user_key, b"b");
    assert_eq!(&*heap.peek_max().unwrap().value.key.user_key, b"z");
}

#[test]
fn replace_max_stays() {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(4, cmp);

    heap.push(entry("a", 0));
    heap.push(entry("b", 0));
    heap.push(entry("d", 0));

    // Replace "d" with "c" — still the maximum.
    let old = heap.replace_max(entry("c", 0));
    assert_eq!(&*old.value.key.user_key, b"d");
    assert_eq!(&*heap.peek_max().unwrap().value.key.user_key, b"c");
}

#[test]
fn replace_max_slides() {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(4, cmp);

    heap.push(entry("b", 0));
    heap.push(entry("c", 0));
    heap.push(entry("d", 0));

    // Replace "d" with "a" — slides to front.
    let old = heap.replace_max(entry("a", 0));
    assert_eq!(&*old.value.key.user_key, b"d");
    assert_eq!(&*heap.peek_min().unwrap().value.key.user_key, b"a");
}

#[test]
fn seqno_tiebreak() {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(4, cmp);

    // Same user key, different seqnos — higher seqno = "smaller".
    heap.push(entry("a", 1));
    heap.push(entry("a", 5));
    heap.push(entry("a", 3));

    let seqnos: Vec<_> = std::iter::from_fn(|| heap.pop_min())
        .map(|e| e.value.key.seqno)
        .collect();
    assert_eq!(seqnos, [5, 3, 1]);
}

#[test]
fn source_index_tiebreak() {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(4, cmp);

    // Same key + seqno: lower source index sorts first.
    heap.push(entry_src("k", 0, 2));
    heap.push(entry_src("k", 0, 0));
    heap.push(entry_src("k", 0, 1));

    let indices: Vec<_> = std::iter::from_fn(|| heap.pop_min())
        .map(|e| e.index)
        .collect();
    assert_eq!(indices, [0, 1, 2]);
}

#[test]
fn mixed_min_max() {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(4, cmp);

    heap.push(entry("a", 0));
    heap.push(entry("b", 0));
    heap.push(entry("c", 0));
    heap.push(entry("d", 0));

    // Interleave min and max pops.
    assert_eq!(&*heap.pop_min().unwrap().value.key.user_key, b"a");
    assert_eq!(&*heap.pop_max().unwrap().value.key.user_key, b"d");
    assert_eq!(&*heap.pop_min().unwrap().value.key.user_key, b"b");
    assert_eq!(&*heap.pop_max().unwrap().value.key.user_key, b"c");
    assert!(heap.is_empty());
}

#[test]
fn replace_min_into_tie() {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(4, cmp);

    // Source 0 has key "a", source 1 has key "b".
    heap.push(entry_src("a", 0, 0));
    heap.push(entry_src("b", 0, 1));

    // Replace source 0's "a" with "b" — now ties with source 1.
    // Source 0 (lower index) must still sort before source 1.
    let old = heap.replace_min(entry_src("b", 0, 0));
    assert_eq!(&*old.value.key.user_key, b"a");

    let first = heap.pop_min().unwrap();
    let second = heap.pop_min().unwrap();
    assert_eq!(first.index, 0, "lower source index wins on tie");
    assert_eq!(second.index, 1);
}

#[test]
fn replace_max_into_tie() {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(4, cmp);

    // Source 0 has key "a", source 1 has key "b".
    heap.push(entry_src("a", 0, 0));
    heap.push(entry_src("b", 0, 1));

    // Replace source 1's "b" with "a" — now ties with source 0.
    // Source 0 (lower index) must still sort first.
    let old = heap.replace_max(entry_src("a", 0, 1));
    assert_eq!(&*old.value.key.user_key, b"b");

    let first = heap.pop_min().unwrap();
    let second = heap.pop_min().unwrap();
    assert_eq!(first.index, 0, "lower source index wins on tie");
    assert_eq!(second.index, 1);
}

#[test]
fn empty_heap() {
    let cmp = comparator::default_comparator();
    let heap = MergeHeap::with_capacity(0, cmp);
    assert!(heap.is_empty());
    assert!(heap.peek_min().is_none());
    assert!(heap.peek_max().is_none());
}

#[test]
fn single_element() {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(1, cmp);
    heap.push(entry("x", 0));
    assert!(!heap.is_empty());

    let e = heap.pop_min().unwrap();
    assert_eq!(&*e.value.key.user_key, b"x");
    assert!(heap.is_empty());
}

/// The heap against a sorted list under a mix of every operation, equal keys
/// included: the extremes agree after each step, whichever way a replacement
/// lands (in place, one step, or a binary search away) and whichever slot a
/// pop freed for the next push.
#[test]
fn every_operation_matches_a_sorted_list() {
    matches_a_sorted_list("", 0);
}

/// The compared words start past a shared prefix, and keys may end before a
/// word does or tie on it and differ only after it.
#[test]
fn every_operation_matches_a_sorted_list_past_a_common_prefix() {
    matches_a_sorted_list("shared-prefix/", "shared-prefix/".len());
}

/// A prefix the keys share but the merge was not told of only costs words.
#[test]
fn every_operation_matches_a_sorted_list_with_an_undeclared_prefix() {
    matches_a_sorted_list("shared-prefix/", 0);
}

fn matches_a_sorted_list(prefix: &str, common: usize) {
    let cmp = comparator::default_comparator();
    let mut heap = MergeHeap::with_capacity(8, cmp);
    heap.use_common_prefix(common);
    let mut model: Vec<(String, usize)> = Vec::new();
    let mut state = 0x2545_F491_4F6C_DD1D_u64;

    for step in 0..20_000_usize {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let number = (state >> 8) % 24;
        let key = match (state >> 32) % 3 {
            0 => format!("{prefix}{number:02}"),
            1 => format!("{prefix}12345678{number:02}"),
            _ => format!("{prefix}1234567"),
        };
        let src = ((state >> 16) % 5) as usize;
        let item = (key.clone(), src);

        match state % 6 {
            0 | 1 if model.len() < 40 => {
                heap.push(entry_src(&key, 0, src));
                model.push(item);
                model.sort();
            }
            2 if !model.is_empty() => {
                let old = heap.replace_min(entry_src(&key, 0, src));
                assert_eq!(old.index(), model.remove(0).1, "step {step}");
                model.push(item);
                model.sort();
            }
            3 if !model.is_empty() => {
                let old = heap.replace_max(entry_src(&key, 0, src));
                assert_eq!(old.index(), model.pop().unwrap().1, "step {step}");
                model.push(item);
                model.sort();
            }
            4 => {
                assert_eq!(
                    heap.pop_min().map(|e| e.index()),
                    (!model.is_empty()).then(|| model.remove(0).1),
                    "step {step}",
                );
            }
            5 => {
                assert_eq!(
                    heap.pop_max().map(|e| e.index()),
                    model.pop().map(|item| item.1),
                    "step {step}",
                );
            }
            _ => {}
        }

        let key_of = |e: &HeapEntry| String::from_utf8_lossy(&e.value.key.user_key).to_string();
        assert_eq!(
            heap.peek_min().map(|e| (key_of(e), e.index())),
            model.first().cloned(),
            "step {step}",
        );
        assert_eq!(
            heap.peek_max().map(|e| (key_of(e), e.index())),
            model.last().cloned(),
            "step {step}",
        );
    }
}
