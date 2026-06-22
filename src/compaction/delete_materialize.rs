// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Merge-on-read delete materialization for columnar segments.
//!
//! Builds the positional [`DeleteBitmap`] for a columnar segment from the range
//! tombstones that cover its rows below the compaction watermark, reusing the
//! same monotonic active-set sweep as the compaction stream's
//! `covered_by_applied_tombstone`. The bitmap is a pure membership set: a
//! position is marked iff its entry is deleted for every live snapshot (some
//! active tombstone strictly below the watermark outranks the entry's seqno).
//! Deletes at or above the watermark stay as ordinary tombstones in higher
//! levels until a later compaction, exactly as the copy-on-write drop path
//! treats them.

use crate::{
    SeqNo, active_tombstone_set::ActiveTombstoneSet, comparator::SharedComparator,
    range_tombstone::RangeTombstone, table::delete_bitmap::DeleteBitmap,
};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Builds a positional delete-bitmap for a columnar segment.
///
/// `rows` yields each entry's `(user_key, seqno)` in the segment's physical
/// position order — block-index order, every stored version a distinct
/// position. This is the SAME numbering the writer assigns when it transposes
/// the entries and the read mask checks via
/// [`DataBlock::from_columnar_block_masked`](crate::table::data_block::DataBlock),
/// so a marked position drops the exact entry the merge-on-read compaction
/// resolved as deleted.
///
/// `tombstones` is the whole-version range-tombstone set; only those strictly
/// below `watermark` (visible to the oldest live snapshot) materialize. A
/// position is marked iff an active below-watermark tombstone outranks its
/// entry's seqno (`entry_seqno < tombstone_seqno`), matching the stream's
/// `covered_by_applied_tombstone`.
///
/// Entries must arrive in non-decreasing `user_key` order (guaranteed by the
/// segment layout: keys ascend, versions of one key descend by seqno), so the
/// active set is swept monotonically in one pass.
///
/// The body is `core` + `alloc` only (active-set sweep over `RangeTombstone`s):
/// it rides on the same range-tombstone-application machinery as the compaction
/// stream, which is currently `std`-gated in the worker, so this module is gated
/// to match. It un-gates trivially alongside that layer.
// `pub` (not `pub(crate)`) inside this crate-private module: clippy flags the
// redundant restriction since the module itself is already crate-scoped.
pub fn build_position_bitmap<'a, I>(
    rows: I,
    tombstones: &[RangeTombstone],
    watermark: SeqNo,
    comparator: &SharedComparator,
) -> DeleteBitmap
where
    I: IntoIterator<Item = (&'a [u8], SeqNo)>,
{
    // Only strictly-below-watermark tombstones materialize (PITR/MVCC safety):
    // one at or above the watermark might not be in effect for a snapshot
    // between the entry's seqno and the tombstone's, so its covered entries are
    // preserved as ordinary rows + a higher-level tombstone. Mirrors
    // `CompactionStream::with_range_tombstone_application`.
    let mut applicable: Vec<&RangeTombstone> = tombstones
        .iter()
        .filter(|rt| rt.visible_at(watermark))
        .collect();
    applicable.sort_by(|a, b| a.cmp_with_comparator(b, comparator.as_ref()));

    let mut bitmap = DeleteBitmap::new();
    let mut active = ActiveTombstoneSet::new_with_comparator(comparator.clone());
    let mut rt_idx = 0usize;
    let mut pos: u32 = 0;

    for (user_key, seqno) in rows {
        // Activate every applicable tombstone whose start <= user_key; rows are
        // key-ordered, so the index only advances (monotonic sweep).
        while let Some(rt) = applicable.get(rt_idx) {
            if comparator.compare(&rt.start, user_key) == core::cmp::Ordering::Greater {
                break;
            }
            // cutoff = MAX: every applicable (already below-watermark) tombstone
            // activates; `is_suppressed` then marks the entry iff some active
            // tombstone outranks its seqno. Mirrors the stream's sweep.
            active.activate(rt, SeqNo::MAX);
            rt_idx += 1;
        }
        active.expire_until(user_key);
        if active.is_suppressed(seqno) {
            // Positions are u32 by the bitmap's design; a segment past u32 rows
            // cannot be addressed, matching `from_columnar_block_masked`.
            bitmap.insert(pos);
        }
        pos = pos.wrapping_add(1);
    }
    bitmap
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UserKey;

    fn rt(start: &[u8], end: &[u8], seqno: SeqNo) -> RangeTombstone {
        RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
    }

    /// Collects the marked positions of a build over `rows` as a sorted Vec, so
    /// each test asserts the exact membership set.
    fn marked(
        rows: &[(&[u8], SeqNo)],
        tombstones: &[RangeTombstone],
        watermark: SeqNo,
    ) -> Vec<u32> {
        let cmp = crate::comparator::default_comparator();
        let bitmap = build_position_bitmap(rows.iter().copied(), tombstones, watermark, &cmp);
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
}
