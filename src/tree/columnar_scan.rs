// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! Tree-level projected columnar scan.
//!
//! Lifts the per-SST [`Table::columnar_scan`](crate::Table::columnar_scan) to the
//! whole tree: a consumer holding a [`Tree`] (or an
//! [`AnyTree`](crate::AnyTree)) can run a projected, predicate-pushed columnar
//! scan across every columnar segment intersecting a key range and visible at an
//! MVCC snapshot, without reimplementing segment selection, snapshot visibility,
//! delete-masking, or cross-segment ordering.
//!
//! # Strategy (overlap-aware merge)
//!
//! A row's effective sequence number is `local_seqno + global_seqno`. Bulk
//! ingested segments carry a *uniform per-segment* seqno (every local seqno is
//! `0`, one `global_seqno` per table), so their visibility is segment-granular;
//! flush-produced segments carry per-row seqnos, so a snapshot can straddle them.
//! The visible columnar segments overlapping the range are grouped by key-range
//! overlap:
//!
//! - A **singleton** group (a segment whose key range overlaps no other) whose
//!   rows are all visible streams its
//!   [`Table::columnar_scan`](crate::Table::columnar_scan) batches verbatim —
//!   zero-copy column-skip, no key decode, no row gather. A singleton the
//!   snapshot straddles gets a per-row seqno mask first.
//! - An **overlapping** group is row-merged: the projection is augmented with the
//!   intrinsic key + seqno columns, each segment's rows are visibility-masked and
//!   tagged with their effective seqno, the union is sorted by `(key asc,
//!   effective seqno desc)`, and the first (newest) row of each key is kept. The
//!   expensive key/seqno decode + gather is paid only where segments overlap.
//!
//! Groups are emitted in ascending key order, so the scan yields projected
//! [`ColumnBatch`]es in global key order. This mirrors how `InfluxDB` `IOx`
//! inserts its deduplication operator only over overlapping files and engineers
//! compaction to keep files non-overlapping: as multi-segment columnar compaction
//! reduces overlap, more of the scan takes the zero-cost singleton path.
//!
//! Deletes are expressed through each segment's positional delete-bitmap (applied
//! inside [`Table::columnar_scan`](crate::Table::columnar_scan)); this scan does
//! not interpret value-type tombstone rows. Memtable rows are not consulted —
//! columnar data lives only in segments — and a visible non-columnar segment
//! overlapping the range is rejected (a mixed-mode tree is unsupported here).

use core::ops::{Bound, RangeBounds};

use alloc::{vec, vec::Vec};

use crate::comparator::UserComparator;
use crate::table::SeqnoVisibility;
use crate::table::columnar::{
    COL_SEQNO, COL_USER_KEY, ColumnBatch, TypeTag, bytes_column_row, fixed_u64_row,
};
use crate::table::columnar_predicate::{ColumnRangePredicate, filter_batch, take_rows};
use crate::{Error, SeqNo, Table, Tree, UserKey};

/// A visible columnar segment selected for the scan, with its cached key range,
/// sequence base, and snapshot-visibility class.
struct Segment {
    table: Table,
    min: UserKey,
    max: UserKey,
    /// The segment's `global_seqno` base; a row's effective seqno is
    /// `local + global`.
    global: SeqNo,
    /// Whether every row is visible at the snapshot, or visibility is per-row.
    visibility: SeqnoVisibility,
}

/// One key-disjoint group of segments: either a single segment (streamed
/// verbatim) or several whose key ranges transitively overlap (row-merged).
struct Group {
    segments: Vec<Segment>,
    /// Running maximum key of the group's span, used while grouping.
    max: UserKey,
}

impl Tree {
    /// Runs a projected columnar scan across the whole tree.
    ///
    /// Iterates the columnar segments intersecting `range` and visible at
    /// `seqno`, applies each segment's positional delete-bitmap and the optional
    /// `predicate` (zone-map block-skip + row filter), and yields projected
    /// [`ColumnBatch`]es in ascending key order. Overlapping segments are merged
    /// with newest-`seqno`-wins semantics so an overwritten key is returned once
    /// (its newest version); disjoint segments stream without merge overhead.
    ///
    /// `range` bounds the result at row granularity: a segment that only
    /// partially overlaps `range` contributes only the rows whose keys fall
    /// inside it (the inclusive / exclusive sense of each bound is honored). A
    /// fully unbounded range keeps the zero-copy fast path for an all-visible
    /// segment.
    ///
    /// `projection` lists the column ids to decode (value sub-column ids, plus
    /// optionally the intrinsic [`COL_USER_KEY`] / seqno / value-type columns);
    /// every other column is stepped over without decoding. Each yielded batch
    /// carries exactly the projected columns.
    ///
    /// This reads only segments; memtable rows are not consulted (columnar data
    /// is written directly to segments via
    /// [`write_columnar_batch`](crate::AnyIngestion::write_columnar_batch)).
    ///
    /// # Errors
    ///
    /// Returns an error if a visible non-columnar segment overlaps `range` (a
    /// mixed-mode tree is unsupported here), or — lazily, while iterating — on a
    /// block read / decode failure or a layout mismatch between segments of an
    /// overlapping group.
    pub fn columnar_scan<R: RangeBounds<UserKey>>(
        &self,
        projection: &[u16],
        predicate: Option<&ColumnRangePredicate>,
        seqno: SeqNo,
        range: R,
    ) -> crate::Result<ColumnarScan> {
        let comparator = self.config.comparator.clone();

        // Owned bounds keep the returned iterator free of borrows from `range`.
        let lo = clone_bound(range.start_bound());
        let hi = clone_bound(range.end_bound());
        let bounds_ref = (bound_as_ref(&lo), bound_as_ref(&hi));

        let super_version = self.version_history.read().get_version_for_snapshot(seqno);

        let mut segments: Vec<Segment> = Vec::new();
        for table in super_version.version.iter_tables() {
            if !table.check_key_range_overlap_cmp(&bounds_ref, comparator.as_ref()) {
                continue;
            }
            // Snapshot visibility (exclusive MVCC). `None` segments postdate the
            // snapshot and are dropped before the columnar check, so an invisible
            // non-columnar segment never trips the mixed-mode error.
            let visibility = table.seqno_visibility(seqno);
            if visibility == SeqnoVisibility::None {
                continue;
            }
            if !table.metadata.columnar {
                return Err(Error::FeatureUnsupported(
                    "columnar_scan: a non-columnar segment overlaps the range (mixed-mode tree)",
                ));
            }
            let key_range = &table.metadata.key_range;
            segments.push(Segment {
                min: key_range.min().clone(),
                max: key_range.max().clone(),
                global: table.global_seqno(),
                visibility,
                table: table.clone(),
            });
        }

        let groups = group_by_overlap(segments, comparator.as_ref());

        Ok(ColumnarScan {
            groups: groups.into_iter().collect(),
            buffered: Vec::new().into(),
            projection: projection.to_vec(),
            predicate: predicate.cloned(),
            comparator,
            seqno,
            lo,
            hi,
        })
    }
}

/// Partitions `segments` into key-disjoint overlap groups, ordered by ascending
/// minimum key. Segments are sorted by their minimum key, then greedily extended
/// into the current group while the next segment's minimum key is `<=` the
/// group's running maximum (an inclusive-range overlap). The result preserves
/// global key order across groups: group `i`'s span lies entirely below group
/// `i + 1`'s.
fn group_by_overlap(mut segments: Vec<Segment>, cmp: &dyn UserComparator) -> Vec<Group> {
    use core::cmp::Ordering;

    segments.sort_by(|a, b| cmp.compare(a.min.as_ref(), b.min.as_ref()));

    let mut groups: Vec<Group> = Vec::new();
    for seg in segments {
        match groups.last_mut() {
            Some(g) if cmp.compare(seg.min.as_ref(), g.max.as_ref()) != Ordering::Greater => {
                if cmp.compare(seg.max.as_ref(), g.max.as_ref()) == Ordering::Greater {
                    g.max = seg.max.clone();
                }
                g.segments.push(seg);
            }
            _ => groups.push(Group {
                max: seg.max.clone(),
                segments: vec![seg],
            }),
        }
    }
    groups
}

/// Iterator over a tree-level projected columnar scan.
///
/// Yields projected [`ColumnBatch`]es in ascending key order. Created by
/// [`Tree::columnar_scan`] (and surfaced through
/// [`AnyTree::columnar_scan`](crate::AnyTree::columnar_scan)). Each overlap group
/// is processed lazily on demand, so at most one group's output is buffered at a
/// time.
pub struct ColumnarScan {
    groups: alloc::collections::VecDeque<Group>,
    buffered: alloc::collections::VecDeque<ColumnBatch>,
    projection: Vec<u16>,
    predicate: Option<ColumnRangePredicate>,
    comparator: alloc::sync::Arc<dyn UserComparator>,
    /// The query snapshot, used for per-row seqno visibility masking.
    seqno: SeqNo,
    /// The requested key range. Applied as a per-row filter (not just segment
    /// selection): a segment that only partially overlaps the range must still
    /// drop the rows that fall outside it.
    lo: Bound<UserKey>,
    hi: Bound<UserKey>,
}

impl ColumnarScan {
    /// Processes one overlap group into its projected, key-ordered output
    /// batches. A singleton group streams its segment's batches (masking by seqno
    /// only when the snapshot straddles the segment); an overlapping group is
    /// row-merged with newest-effective-seqno-wins dedup.
    fn process_group(&self, group: &Group) -> crate::Result<Vec<ColumnBatch>> {
        if let [seg] = group.segments.as_slice() {
            return self.process_singleton(seg);
        }
        self.merge_group(group)
    }

    /// Whether the requested key range is fully unbounded, so no per-row range
    /// filtering is needed (the segment's every row is in range).
    fn range_is_full(&self) -> bool {
        matches!(self.lo, Bound::Unbounded) && matches!(self.hi, Bound::Unbounded)
    }

    /// Singleton group: no cross-segment merge. When every row is visible and the
    /// range is unbounded, the per-SST projected scan streams verbatim (zero-copy
    /// column-skip). Otherwise a per-row mask drops rows that are seqno-invisible
    /// (when the snapshot straddles the segment) or outside the requested range
    /// (when the segment only partially overlaps it).
    fn process_singleton(&self, seg: &Segment) -> crate::Result<Vec<ColumnBatch>> {
        let range_filter = !self.range_is_full();
        if seg.visibility == SeqnoVisibility::All && !range_filter {
            let mut out = seg
                .table
                .columnar_scan(&self.projection, self.predicate.as_ref())?;
            out.retain(|b| b.row_count > 0);
            return Ok(out);
        }

        // Decode the columns the mask needs even when the caller did not project
        // them (dropped again at the end): the seqno column for partial-visibility
        // masking, the key column for range filtering.
        let partial = seg.visibility == SeqnoVisibility::Partial;
        let seqno_projected = self.projection.contains(&COL_SEQNO);
        let key_projected = self.projection.contains(&COL_USER_KEY);
        let mut augmented = self.projection.clone();
        if partial && !seqno_projected {
            augmented.push(COL_SEQNO);
        }
        if range_filter && !key_projected {
            augmented.push(COL_USER_KEY);
        }
        // Visible iff `local < threshold` (the snapshot in this segment's local
        // seqno space); `Partial` guarantees the subtraction is in range.
        let threshold = self.seqno.saturating_sub(seg.global);
        let cmp = self.comparator.as_ref();

        let mut out = Vec::new();
        for batch in seg
            .table
            .columnar_scan(&augmented, self.predicate.as_ref())?
        {
            if batch.row_count == 0 {
                continue;
            }
            let seqno_col = if partial {
                Some(
                    batch
                        .columns
                        .iter()
                        .find(|c| c.column_id == COL_SEQNO)
                        .ok_or(Error::InvalidHeader(
                            "columnar_scan: partial-visibility batch missing the seqno column",
                        ))?,
                )
            } else {
                None
            };
            let key_col = if range_filter {
                Some(
                    batch
                        .columns
                        .iter()
                        .find(|c| c.column_id == COL_USER_KEY)
                        .ok_or(Error::InvalidHeader(
                            "columnar_scan: range-filtered batch missing the key column",
                        ))?,
                )
            } else {
                None
            };

            let mut mask = Vec::with_capacity(batch.row_count as usize);
            for row in 0..batch.row_count {
                let seqno_ok = match seqno_col {
                    Some(seqno_col) => fixed_u64_row(&seqno_col.data, row)? < threshold,
                    None => true,
                };
                // Evaluate the range bound only when the row survived the seqno
                // gate (short-circuit), so a row's key is decoded only if needed.
                let keep = if !seqno_ok {
                    false
                } else if let Some(key_col) = key_col {
                    let key = bytes_column_row(&key_col.data, batch.row_count, row)?;
                    key_in_bounds(key, &self.lo, &self.hi, cmp)
                } else {
                    true
                };
                mask.push(keep);
            }
            let mut visible = filter_batch(&batch, &mask);
            if partial && !seqno_projected {
                visible.columns.retain(|c| c.column_id != COL_SEQNO);
            }
            if range_filter && !key_projected {
                visible.columns.retain(|c| c.column_id != COL_USER_KEY);
            }
            if visible.row_count > 0 {
                out.push(visible);
            }
        }
        Ok(out)
    }

    /// Row-merges an overlapping segment group: over the union of the segments'
    /// visible projected rows, keep the newest version of each key (highest
    /// effective seqno), gathered in key order.
    fn merge_group(&self, group: &Group) -> crate::Result<Vec<ColumnBatch>> {
        // The merge needs each row's key and effective seqno, so decode the
        // intrinsic key + seqno columns even when the caller did not project them
        // (dropped again at the end).
        let key_projected = self.projection.contains(&COL_USER_KEY);
        let seqno_projected = self.projection.contains(&COL_SEQNO);
        let mut augmented = self.projection.clone();
        if !key_projected {
            augmented.push(COL_USER_KEY);
        }
        if !seqno_projected {
            augmented.push(COL_SEQNO);
        }

        // Concatenate every segment's visible rows into one batch, tracking each
        // surviving row's effective seqno (`local + global`) in lockstep so the
        // dedup can compare versions across segments with different bases.
        let mut combined: Option<ColumnBatch> = None;
        let mut effective: Vec<SeqNo> = Vec::new();
        for seg in &group.segments {
            let threshold = self.seqno.saturating_sub(seg.global);
            for batch in seg
                .table
                .columnar_scan(&augmented, self.predicate.as_ref())?
            {
                if batch.row_count == 0 {
                    continue;
                }
                let seqno_col = batch
                    .columns
                    .iter()
                    .find(|c| c.column_id == COL_SEQNO)
                    .ok_or(Error::InvalidHeader(
                        "columnar_scan: merged group missing the seqno column",
                    ))?;
                let mut mask = Vec::with_capacity(batch.row_count as usize);
                for row in 0..batch.row_count {
                    let local = fixed_u64_row(&seqno_col.data, row)?;
                    let visible = seg.visibility == SeqnoVisibility::All || local < threshold;
                    mask.push(visible);
                    if visible {
                        // Translate to the global coordinate for cross-segment
                        // comparison; a visible row cannot overflow (its effective
                        // seqno is `< snapshot <= SeqNo::MAX`).
                        let eff = local.checked_add(seg.global).ok_or(Error::InvalidHeader(
                            "columnar_scan: effective seqno overflow",
                        ))?;
                        effective.push(eff);
                    }
                }
                let visible = filter_batch(&batch, &mask);
                if visible.row_count == 0 {
                    continue;
                }
                match &mut combined {
                    Some(acc) => acc.append(&visible)?,
                    None => combined = Some(visible),
                }
            }
        }
        let Some(combined) = combined else {
            return Ok(Vec::new());
        };

        // Extract every row's key once (fallible framing read), then sort indices
        // by (key asc, effective seqno desc) and keep the first per key.
        let key_col = combined
            .columns
            .iter()
            .find(|c| c.column_id == COL_USER_KEY)
            .ok_or(Error::InvalidHeader(
                "columnar_scan: merged group missing the key column",
            ))?;
        if key_col.type_tag != TypeTag::Bytes {
            return Err(Error::InvalidHeader(
                "columnar_scan: key column is not a bytes column",
            ));
        }
        let rows = combined.row_count;
        debug_assert_eq!(rows as usize, effective.len(), "seqno tracked per row");
        let mut keys: Vec<&[u8]> = Vec::with_capacity(rows as usize);
        for i in 0..rows {
            keys.push(bytes_column_row(&key_col.data, rows, i)?);
        }

        // Indices are always in range (`0..rows`, and `keys` / `effective` both
        // have `rows` entries), so the `get` defaults below are never taken; they
        // only satisfy the no-panic-indexing lint.
        let key_at = |i: u32| keys.get(i as usize).copied().unwrap_or(&[]);
        let eff_at = |i: u32| effective.get(i as usize).copied().unwrap_or(0);
        let cmp = self.comparator.as_ref();
        let mut order: Vec<u32> = (0..rows).collect();
        order.sort_by(|&a, &b| {
            cmp.compare(key_at(a), key_at(b))
                .then_with(|| eff_at(b).cmp(&eff_at(a)))
        });

        // Keep the first index of each distinct key (highest effective seqno);
        // drop the shadowed older duplicates and any key outside the requested
        // range (a segment may only partially overlap it).
        let range_filter = !self.range_is_full();
        let mut kept: Vec<u32> = Vec::with_capacity(order.len());
        let mut prev: Option<&[u8]> = None;
        for &i in &order {
            let key = key_at(i);
            if let Some(p) = prev
                && cmp.compare(p, key) == core::cmp::Ordering::Equal
            {
                continue;
            }
            prev = Some(key);
            if range_filter && !key_in_bounds(key, &self.lo, &self.hi, cmp) {
                continue;
            }
            kept.push(i);
        }

        let mut merged = take_rows(&combined, &kept);
        // Match the singleton contract: yield exactly the projected columns.
        if !key_projected {
            merged.columns.retain(|c| c.column_id != COL_USER_KEY);
        }
        if !seqno_projected {
            merged.columns.retain(|c| c.column_id != COL_SEQNO);
        }
        if merged.row_count == 0 {
            return Ok(Vec::new());
        }
        Ok(vec![merged])
    }
}

/// Whether `key` lies within the requested `[lo, hi]` key bounds, per the tree
/// comparator. An unbounded side never excludes; the inclusive / exclusive sense
/// of each bound matches the `RangeBounds` the caller passed.
fn key_in_bounds(
    key: &[u8],
    lo: &Bound<UserKey>,
    hi: &Bound<UserKey>,
    cmp: &dyn UserComparator,
) -> bool {
    use core::cmp::Ordering;
    let above_lo = match lo {
        Bound::Unbounded => true,
        Bound::Included(k) => cmp.compare(key, k.as_ref()) != Ordering::Less,
        Bound::Excluded(k) => cmp.compare(key, k.as_ref()) == Ordering::Greater,
    };
    let below_hi = match hi {
        Bound::Unbounded => true,
        Bound::Included(k) => cmp.compare(key, k.as_ref()) != Ordering::Greater,
        Bound::Excluded(k) => cmp.compare(key, k.as_ref()) == Ordering::Less,
    };
    above_lo && below_hi
}

impl Iterator for ColumnarScan {
    type Item = crate::Result<ColumnBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(batch) = self.buffered.pop_front() {
                return Some(Ok(batch));
            }
            let group = self.groups.pop_front()?;
            match self.process_group(&group) {
                Ok(batches) => self.buffered.extend(batches),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Clones a borrowed key bound into an owned one.
fn clone_bound(bound: Bound<&UserKey>) -> Bound<UserKey> {
    match bound {
        Bound::Included(k) => Bound::Included(k.clone()),
        Bound::Excluded(k) => Bound::Excluded(k.clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Borrows an owned key bound as a byte-slice bound for key-range overlap checks.
fn bound_as_ref(bound: &Bound<UserKey>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(k) => Bound::Included(k.as_ref()),
        Bound::Excluded(k) => Bound::Excluded(k.as_ref()),
        Bound::Unbounded => Bound::Unbounded,
    }
}
