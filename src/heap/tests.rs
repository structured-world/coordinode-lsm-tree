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
