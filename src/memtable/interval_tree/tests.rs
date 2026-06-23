use super::*;

fn rt(start: &[u8], end: &[u8], seqno: SeqNo) -> RangeTombstone {
    RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
}

#[test]
fn empty_tree_no_suppression() {
    let tree = IntervalTree::new();
    assert!(!tree.query_suppression(b"key", 5, 100));
}

#[test]
fn single_tombstone_suppresses() {
    let mut tree = IntervalTree::new();
    tree.insert(rt(b"b", b"y", 10));
    assert!(tree.query_suppression(b"c", 5, 100));
}

#[test]
fn single_tombstone_no_suppress_newer_kv() {
    let mut tree = IntervalTree::new();
    tree.insert(rt(b"b", b"y", 10));
    assert!(!tree.query_suppression(b"c", 15, 100));
}

#[test]
fn single_tombstone_exclusive_end() {
    let mut tree = IntervalTree::new();
    tree.insert(rt(b"b", b"y", 10));
    assert!(!tree.query_suppression(b"y", 5, 100));
}

#[test]
fn single_tombstone_before_start() {
    let mut tree = IntervalTree::new();
    tree.insert(rt(b"b", b"y", 10));
    assert!(!tree.query_suppression(b"a", 5, 100));
}

#[test]
fn tombstone_not_visible() {
    let mut tree = IntervalTree::new();
    tree.insert(rt(b"b", b"y", 10));
    assert!(!tree.query_suppression(b"c", 5, 9));
}

#[test]
fn multiple_tombstones() {
    let mut tree = IntervalTree::new();
    tree.insert(rt(b"a", b"f", 10));
    tree.insert(rt(b"d", b"m", 20));
    tree.insert(rt(b"p", b"z", 5));

    assert!(tree.query_suppression(b"e", 15, 100));
    assert!(tree.query_suppression(b"e", 5, 100));
    assert!(!tree.query_suppression(b"e", 25, 100));

    assert!(tree.query_suppression(b"q", 3, 100));
    assert!(!tree.query_suppression(b"q", 10, 100));
}

#[test]
fn covering_rt_found() {
    let mut tree = IntervalTree::new();
    tree.insert(rt(b"a", b"z", 50));
    tree.insert(rt(b"c", b"g", 10));

    let crt = tree.query_covering_rt_for_range(b"b", b"y", 100);
    assert!(crt.is_some());
    let crt = crt.unwrap();
    assert_eq!(crt.seqno, 50);
}

#[test]
fn covering_rt_not_found_partial() {
    let mut tree = IntervalTree::new();
    tree.insert(rt(b"c", b"g", 10));

    let crt = tree.query_covering_rt_for_range(b"b", b"y", 100);
    assert!(crt.is_none());
}

#[test]
fn covering_rt_highest_seqno() {
    let mut tree = IntervalTree::new();
    tree.insert(rt(b"a", b"z", 50));
    tree.insert(rt(b"a", b"z", 100));

    let crt = tree.query_covering_rt_for_range(b"b", b"y", 200);
    assert!(crt.is_some());
    assert_eq!(crt.unwrap().seqno, 100);
}

#[test]
fn iter_sorted_empty() {
    let tree = IntervalTree::new();
    assert!(tree.iter_sorted().is_empty());
}

#[test]
fn iter_sorted_multiple() {
    let mut tree = IntervalTree::new();
    tree.insert(rt(b"d", b"f", 10));
    tree.insert(rt(b"a", b"c", 20));
    tree.insert(rt(b"m", b"z", 5));

    let sorted = tree.iter_sorted();
    assert_eq!(sorted.len(), 3);
    assert_eq!(sorted[0].start.as_ref(), b"a");
    assert_eq!(sorted[1].start.as_ref(), b"d");
    assert_eq!(sorted[2].start.as_ref(), b"m");
}

#[test]
fn avl_balance_maintained() {
    let mut tree = IntervalTree::new();
    for i in 0u8..20 {
        let s = vec![i];
        let e = vec![i + 1];
        tree.insert(rt(&s, &e, u64::from(i)));
    }
    assert_eq!(tree.len(), 20);
    if let Some(ref root) = tree.root {
        assert!(root.height <= 6, "AVL height too large: {}", root.height);
    }
}

#[test]
fn seqno_pruning() {
    let mut tree = IntervalTree::new();
    tree.insert(rt(b"a", b"z", 100));
    tree.insert(rt(b"b", b"y", 200));

    assert!(!tree.query_suppression(b"c", 5, 50));
}

#[test]
fn max_end_pruning() {
    let mut tree = IntervalTree::new();
    tree.insert(rt(b"a", b"c", 10));
    tree.insert(rt(b"b", b"d", 10));

    assert!(!tree.query_suppression(b"e", 5, 100));
}
