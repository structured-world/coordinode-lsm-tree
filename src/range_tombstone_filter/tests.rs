use super::*;
use crate::{UserKey, ValueType};

fn kv(key: &[u8], seqno: SeqNo) -> InternalValue {
    InternalValue::from_components(key, b"v", seqno, ValueType::Value)
}

fn rt(start: &[u8], end: &[u8], seqno: SeqNo) -> RangeTombstone {
    RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
}

/// Helper: tag all tombstones with the same cutoff seqno.
fn tagged(tombstones: Vec<RangeTombstone>, cutoff: SeqNo) -> Vec<(RangeTombstone, SeqNo)> {
    tombstones.into_iter().map(|rt| (rt, cutoff)).collect()
}

#[test]
fn items_no_tombstones_return_all() {
    let items: Vec<crate::Result<InternalValue>> =
        vec![Ok(kv(b"a", 1)), Ok(kv(b"b", 2)), Ok(kv(b"c", 3))];

    let filter = RangeTombstoneFilter::new(items.into_iter(), vec![]);
    let results: Vec<_> = filter.flatten().collect();
    assert_eq!(results.len(), 3);
}

#[test]
fn items_with_range_tombstone_suppress_covered_keys() {
    let items: Vec<crate::Result<InternalValue>> = vec![
        Ok(kv(b"a", 5)),
        Ok(kv(b"b", 5)),
        Ok(kv(b"c", 5)),
        Ok(kv(b"d", 5)),
        Ok(kv(b"e", 5)),
    ];

    let tombstones = tagged(vec![rt(b"b", b"d", 10)], SeqNo::MAX);
    let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
    let results: Vec<_> = filter.flatten().collect();

    let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
    assert_eq!(keys, vec![b"a".as_ref(), b"d", b"e"]);
}

#[test]
fn items_newer_than_tombstone_survive() {
    let items: Vec<crate::Result<InternalValue>> = vec![Ok(kv(b"b", 10)), Ok(kv(b"c", 3))];

    let tombstones = tagged(vec![rt(b"a", b"z", 5)], SeqNo::MAX);
    let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
    let results: Vec<_> = filter.flatten().collect();

    let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
    assert_eq!(keys, vec![b"b".as_ref()]);
}

#[test]
fn range_end_exclusive_preserves_boundary_key() {
    let items: Vec<crate::Result<InternalValue>> =
        vec![Ok(kv(b"b", 5)), Ok(kv(b"c", 5)), Ok(kv(b"d", 5))];

    let tombstones = tagged(vec![rt(b"b", b"d", 10)], SeqNo::MAX);
    let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
    let results: Vec<_> = filter.flatten().collect();

    let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
    assert_eq!(keys, vec![b"d".as_ref()]);
}

#[test]
fn overlapping_tombstones_suppress_union_of_ranges() {
    let items: Vec<crate::Result<InternalValue>> = vec![
        Ok(kv(b"a", 1)),
        Ok(kv(b"b", 3)),
        Ok(kv(b"c", 6)),
        Ok(kv(b"d", 1)),
    ];

    let tombstones = tagged(vec![rt(b"a", b"c", 5), rt(b"b", b"e", 4)], SeqNo::MAX);
    let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
    let results: Vec<_> = filter.flatten().collect();

    let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
    assert_eq!(keys, vec![b"c".as_ref()]);
}

#[test]
fn tombstone_newer_than_read_seqno_not_visible() {
    let items: Vec<crate::Result<InternalValue>> = vec![Ok(kv(b"b", 3))];

    // RT at seqno 10 with cutoff 5 — not visible
    let tombstones = tagged(vec![rt(b"a", b"z", 10)], 5);
    let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
    let results: Vec<_> = filter.flatten().collect();

    assert_eq!(results.len(), 1);
}

#[test]
fn mixed_cutoffs_suppress_only_visible_source() {
    // Two RTs with same seqno but different per-source cutoffs:
    // RT from source A (cutoff 15) — visible (10 < 15), suppresses kv at seqno 5
    // RT from source B (cutoff 5) — NOT visible (10 >= 5), does not suppress
    let items: Vec<crate::Result<InternalValue>> = vec![Ok(kv(b"b", 5)), Ok(kv(b"x", 5))];

    let tombstones = vec![
        (rt(b"a", b"d", 10), 15), // source A: visible
        (rt(b"w", b"z", 10), 5),  // source B: not visible
    ];
    let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
    let results: Vec<_> = filter.flatten().collect();

    let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
    // "b" suppressed by source-A RT, "x" survives (source-B RT invisible)
    assert_eq!(keys, vec![b"x".as_ref()]);
}

#[test]
fn rev_items_with_range_tombstone_suppress_covered_keys() {
    let items: Vec<crate::Result<InternalValue>> = vec![
        Ok(kv(b"a", 5)),
        Ok(kv(b"b", 5)),
        Ok(kv(b"c", 5)),
        Ok(kv(b"d", 5)),
        Ok(kv(b"e", 5)),
    ];

    let tombstones = tagged(vec![rt(b"b", b"d", 10)], SeqNo::MAX);
    let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
    let results: Vec<_> = filter.rev().flatten().collect();

    let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
    assert_eq!(keys, vec![b"e".as_ref(), b"d", b"a"]);
}

#[test]
fn rev_range_end_exclusive_preserves_boundary_key() {
    let items: Vec<crate::Result<InternalValue>> =
        vec![Ok(kv(b"a", 5)), Ok(kv(b"l", 5)), Ok(kv(b"m", 5))];

    let tombstones = tagged(vec![rt(b"a", b"m", 10)], SeqNo::MAX);
    let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
    let results: Vec<_> = filter.rev().flatten().collect();

    let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
    assert_eq!(keys, vec![b"m".as_ref()]);
}
