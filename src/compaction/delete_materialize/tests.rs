use super::*;
use crate::UserKey;

fn rt(start: &[u8], end: &[u8], seqno: SeqNo) -> RangeTombstone {
    RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
}

/// Collects the marked positions of a build over `rows` as a sorted Vec, so
/// each test asserts the exact membership set.
#[expect(
    clippy::expect_used,
    reason = "test inputs are far below the u32::MAX row limit, so the build cannot overflow"
)]
fn marked(rows: &[(&[u8], SeqNo)], tombstones: &[RangeTombstone], watermark: SeqNo) -> Vec<u32> {
    let cmp = crate::comparator::default_comparator();
    let bitmap = build_position_bitmap(rows.iter().copied(), tombstones, watermark, &cmp)
        .expect("position bitmap build");
    let mut out: Vec<u32> = bitmap.iter().collect();
    out.sort_unstable();
    out
}

#[test]
fn no_tombstones_marks_nothing() {
    let rows: &[(&[u8], SeqNo)] = &[(b"a", 5), (b"b", 5), (b"c", 5)];
    assert!(marked(rows, &[], 100).is_empty());
}

#[test]
fn below_watermark_tombstone_marks_covered_half_open_range() {
    // [b, d) @10, watermark 20: b, c are covered (10 > entry seqno 5); a is
    // before the range, d is the exclusive end, e is after.
    let rows: &[(&[u8], SeqNo)] = &[(b"a", 5), (b"b", 5), (b"c", 5), (b"d", 5), (b"e", 5)];
    assert_eq!(marked(rows, &[rt(b"b", b"d", 10)], 20), vec![1, 2]);
}

#[test]
fn at_or_above_watermark_tombstone_marks_nothing() {
    // Tombstone seqno == watermark is invisible to the oldest live snapshot
    // (which reads AT the watermark), so it must not materialize.
    let rows: &[(&[u8], SeqNo)] = &[(b"b", 5), (b"c", 5)];
    assert!(marked(rows, &[rt(b"a", b"z", 20)], 20).is_empty());
    // Strictly above: also not materialized.
    assert!(marked(rows, &[rt(b"a", b"z", 25)], 20).is_empty());
}

#[test]
fn entry_newer_than_tombstone_survives() {
    // Entry seqno 15 >= tombstone seqno 10: the delete does not outrank it,
    // so the row is not marked even though the key is in range.
    let rows: &[(&[u8], SeqNo)] = &[(b"b", 15)];
    assert!(marked(rows, &[rt(b"a", b"z", 10)], 100).is_empty());
}

#[test]
fn per_version_positions_marked_independently() {
    // One key with two versions: b@15 survives the @10 delete, b@5 is
    // dropped. Position numbering counts every version, so only position 1
    // (b@5) is marked.
    let rows: &[(&[u8], SeqNo)] = &[(b"b", 15), (b"b", 5)];
    assert_eq!(marked(rows, &[rt(b"a", b"z", 10)], 100), vec![1]);
}

#[test]
fn overlapping_tombstones_take_the_highest_seqno() {
    // [a, z)@8 and [c, e)@12 overlap on c, d. An entry at seqno 10 is
    // outranked only where the @12 tombstone is active (c, d); under @8
    // alone (b, e..) it survives.
    let rows: &[(&[u8], SeqNo)] = &[(b"b", 10), (b"c", 10), (b"d", 10), (b"e", 10)];
    let tombstones = [rt(b"a", b"z", 8), rt(b"c", b"e", 12)];
    assert_eq!(marked(rows, &tombstones, 100), vec![1, 2]);
}
