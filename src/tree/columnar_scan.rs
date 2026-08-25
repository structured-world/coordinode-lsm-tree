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
//! A projected seqno column is emitted in that EFFECTIVE (tree-global) space,
//! which is what every other read surface speaks: the stored local value would
//! read as `0` for an ingested row and name a commit the tree never had. The
//! masking arithmetic still runs in local space (one subtraction per segment
//! instead of one addition per row), so only the emitted column is translated.
//! The visible columnar segments overlapping the range are grouped by key-range
//! overlap:
//!
//! - A **singleton** group (a segment whose key range overlaps no other) whose
//!   rows are all visible AND provably one-version-per-key (the writer's
//!   distinct-key count equals its row count) streams its
//!   [`Table::columnar_scan`](crate::Table::columnar_scan) batches verbatim —
//!   zero-copy column-skip, no key decode, no row gather. A singleton the
//!   snapshot straddles gets a per-row seqno mask first, and one that can hold
//!   several versions of a key (an overwritten key in a flush / compaction
//!   product) additionally gets per-key newest-visible dedup.
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
//! Deletes reach the scan two ways and both remove the key. A segment's
//! positional delete-bitmap is applied inside
//! [`Table::columnar_scan`](crate::Table::columnar_scan); a value-type TOMBSTONE
//! is consumed here, when the newest visible version of a key is one — the key
//! then yields no row at all, matching what a point read reports, instead of
//! surfacing a row a caller who did not project the value-type column could not
//! tell from a live one. Only a segment that RECORDS deletions pays for it: one
//! whose metadata counts none keeps its columns untouched (and its zero-copy
//! verbatim path). Memtable rows are not consulted —
//! columnar data lives only in segments — and a visible non-columnar segment
//! overlapping the range is rejected (a mixed-mode tree is unsupported here).

use core::ops::{Bound, RangeBounds};

use alloc::{vec, vec::Vec};

use crate::comparator::UserComparator;
use crate::table::SeqnoVisibility;
use crate::table::columnar::{
    COL_SEQNO, COL_USER_KEY, COL_VALUE_TYPE, ColumnBatch, TypeTag, bytes_column_row, fixed_u64_row,
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
    /// Whether this segment can physically hold several MVCC versions of one
    /// key (a flush / compaction product with an overwritten key). Proven
    /// unique only when the writer's distinct-key count equals the row count;
    /// legacy tables without the count are conservatively assumed to carry
    /// duplicates. Gates the singleton path's per-key newest-visible dedup.
    may_dup: bool,
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
    /// mixed-mode tree is unsupported here), if the tree carries a merge
    /// operator (see below), or — lazily, while iterating — on a block read /
    /// decode failure or a layout mismatch between segments of an overlapping
    /// group.
    pub fn columnar_scan<R: RangeBounds<UserKey>>(
        &self,
        projection: &[u16],
        predicate: Option<&ColumnRangePredicate>,
        seqno: SeqNo,
        range: R,
    ) -> crate::Result<ColumnarScan> {
        // A merge chain is not a version chain: its older rows are the merge's
        // INPUTS, not data the newest row shadows. The newest-version-wins dedup
        // below would hand back the raw operand where a read hands back the
        // merged value, and it drops the base row, so the consumer cannot
        // resolve the chain itself either. Refuse instead of disagreeing with
        // the read path.
        //
        // Gated on the OPERATOR rather than on the rows: without one the read
        // path returns the newest entry unchanged — the raw operand — which is
        // exactly what this scan yields, so nothing diverges. With one, no
        // metadata says whether a segment holds operands, and finding out means
        // decoding the value-type column of every batch, which would cost the
        // zero-copy fast path on every scan of every tree that merges.
        if self.config.merge_operator.is_some() {
            return Err(Error::FeatureUnsupported(
                "columnar scan of a tree with a merge operator: merge chains \
                 would be returned unresolved",
            ));
        }

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
            let may_dup = table
                .metadata
                .key_count
                .is_none_or(|k| k != table.metadata.item_count);
            segments.push(Segment {
                min: key_range.min().clone(),
                max: key_range.max().clone(),
                global: table.global_seqno(),
                visibility,
                may_dup,
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

    /// Rewrites a batch's seqno column from its segment's LOCAL space into the
    /// tree's global one (`local + global`).
    ///
    /// A bulk-ingested segment stores every row at local seqno `0` and carries
    /// its ordering in a per-segment `global_seqno`, so the stored column is not
    /// a commit sequence number any other read surface would recognize. The
    /// masking arithmetic elsewhere translates the THRESHOLD into local space
    /// instead (cheaper, one subtraction per segment), which is why the column
    /// itself still needs this before it reaches a caller. A zero offset leaves
    /// the batch untouched.
    fn globalize_seqnos(batch: &mut ColumnBatch, global: SeqNo) -> crate::Result<()> {
        if global == 0 {
            return Ok(());
        }
        let Some(col) = batch.columns.iter_mut().find(|c| c.column_id == COL_SEQNO) else {
            return Ok(());
        };
        for row in 0..batch.row_count as usize {
            let at = row * 8;
            let bytes = col
                .data
                .get_mut(at..at + 8)
                .ok_or(Error::InvalidHeader("columnar_scan: short seqno column"))?;
            let local = u64::from_le_bytes(
                (&*bytes)
                    .try_into()
                    .map_err(|_| Error::InvalidHeader("columnar_scan: short seqno column"))?,
            );
            let effective = local.checked_add(global).ok_or(Error::InvalidHeader(
                "columnar_scan: effective seqno overflows",
            ))?;
            bytes.copy_from_slice(&effective.to_le_bytes());
        }
        Ok(())
    }

    /// Singleton group: no cross-segment merge. When every row is visible and the
    /// range is unbounded, the per-SST projected scan streams verbatim (zero-copy
    /// column-skip). Otherwise a per-row mask drops rows that are seqno-invisible
    /// (when the snapshot straddles the segment) or outside the requested range
    /// (when the segment only partially overlaps it).
    fn process_singleton(&self, seg: &Segment) -> crate::Result<Vec<ColumnBatch>> {
        // A segment that RECORDS deletions takes the dedup path even when its
        // keys are provably unique: a key whose single row is a tombstone would
        // otherwise stream through verbatim and surface a key the point read
        // calls absent. Deciding a run is where tombstones are consumed, and
        // that lives there.
        if seg.may_dup || seg.table.tombstone_count() > 0 || seg.table.weak_tombstone_count() > 0 {
            return self.process_singleton_dedup(seg);
        }
        let range_filter = !self.range_is_full();
        if seg.visibility == SeqnoVisibility::All && !range_filter {
            let mut out = seg
                .table
                .columnar_scan(&self.projection, self.predicate.as_ref())?;
            out.retain(|b| b.row_count > 0);
            for batch in &mut out {
                Self::globalize_seqnos(batch, seg.global)?;
            }
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
                Self::globalize_seqnos(&mut visible, seg.global)?;
                out.push(visible);
            }
        }
        Ok(out)
    }

    /// Singleton whose segment can physically hold several MVCC versions of one
    /// key (`Segment::may_dup`): every version is a physical row, so the scan
    /// must keep only the newest VISIBLE version per key instead of streaming
    /// the segment verbatim. Rows are stored in internal-key order (key
    /// ascending, seqno descending within a key), so within each key run the
    /// invisible too-new versions come first and the first visible row is the
    /// newest visible version; a run can span batch boundaries, so the last
    /// kept key carries across batches. The predicate runs AFTER dedup
    /// (mirroring [`Self::merge_group`]): a key whose newest version fails the
    /// predicate is dropped, never served from an older matching version —
    /// which also rules out predicate-driven zone-map block-skip here.
    fn process_singleton_dedup(&self, seg: &Segment) -> crate::Result<Vec<ColumnBatch>> {
        // Decode the columns the dedup needs even when the caller did not
        // project them (dropped again at the end): the key column always, the
        // seqno column when the snapshot straddles the segment, the predicate
        // column for the after-dedup filter.
        let key_projected = self.projection.contains(&COL_USER_KEY);
        let seqno_projected = self.projection.contains(&COL_SEQNO);
        let partial = seg.visibility == SeqnoVisibility::Partial;
        let mut augmented = self.projection.clone();
        if !key_projected {
            augmented.push(COL_USER_KEY);
        }
        if partial && !seqno_projected {
            augmented.push(COL_SEQNO);
        }
        let predicate_col = self.predicate.as_ref().map(|p| p.column_id);
        let predicate_col_projected = predicate_col.is_some_and(|c| self.projection.contains(&c));
        if let Some(pc) = predicate_col
            && !augmented.contains(&pc)
        {
            augmented.push(pc);
        }
        // A deletion is what a key's newest row can BE, so deciding a run needs
        // the value type — otherwise a tombstone decides the run and is emitted
        // as a row while a point read calls the key absent, and a caller that did
        // not project the type column cannot tell that row from a live one with
        // an empty value. Decoded only for a segment that RECORDS deletions; one
        // without them keeps its columns untouched.
        let deletes = seg.table.tombstone_count() > 0 || seg.table.weak_tombstone_count() > 0;
        let vt_projected = self.projection.contains(&COL_VALUE_TYPE);
        if deletes && !vt_projected {
            augmented.push(COL_VALUE_TYPE);
        }

        // Visible iff `local < threshold` (the snapshot in this segment's local
        // seqno space); `Partial` guarantees the subtraction is in range.
        let threshold = self.seqno.saturating_sub(seg.global);
        let range_filter = !self.range_is_full();
        let cmp = self.comparator.as_ref();

        let mut out = Vec::new();
        // The user key of the last key run whose newest visible version was
        // already emitted (or deliberately dropped by the range filter) —
        // owned, because a run can span batch boundaries.
        let mut last_key: Option<alloc::vec::Vec<u8>> = None;
        for batch in seg.table.columnar_scan(&augmented, None)? {
            if batch.row_count == 0 {
                continue;
            }
            let key_col = batch
                .columns
                .iter()
                .find(|c| c.column_id == COL_USER_KEY)
                .ok_or(Error::InvalidHeader(
                    "columnar_scan: dedup batch missing the key column",
                ))?;
            let vt_col = if deletes {
                Some(
                    batch
                        .columns
                        .iter()
                        .find(|c| c.column_id == COL_VALUE_TYPE)
                        .ok_or(Error::InvalidHeader(
                            "columnar_scan: dedup batch missing the value-type column",
                        ))?,
                )
            } else {
                None
            };
            let seqno_col = if partial {
                Some(
                    batch
                        .columns
                        .iter()
                        .find(|c| c.column_id == COL_SEQNO)
                        .ok_or(Error::InvalidHeader(
                            "columnar_scan: dedup batch missing the seqno column",
                        ))?,
                )
            } else {
                None
            };

            let mut mask = Vec::with_capacity(batch.row_count as usize);
            for row in 0..batch.row_count {
                let visible = match seqno_col {
                    Some(seqno_col) => fixed_u64_row(&seqno_col.data, row)? < threshold,
                    None => true,
                };
                if !visible {
                    mask.push(false);
                    continue;
                }
                let key = bytes_column_row(&key_col.data, batch.row_count, row)?;
                if last_key
                    .as_deref()
                    .is_some_and(|p| cmp.compare(p, key) == core::cmp::Ordering::Equal)
                {
                    // A later visible version of an already-decided key run —
                    // shadowed by the newest visible version above it.
                    mask.push(false);
                    continue;
                }
                // First visible row of a new key run = the newest visible
                // version. Deciding the run here (even when the range filter or
                // a deletion drops the row) also drops its older versions above.
                last_key = Some(key.to_vec());
                if let Some(vt_col) = vt_col {
                    let byte = *vt_col.data.get(row as usize).ok_or(Error::InvalidHeader(
                        "columnar_scan: value-type column shorter than the row count",
                    ))?;
                    let value_type = crate::ValueType::try_from(byte)
                        .map_err(|()| Error::InvalidTag(("ValueType", byte)))?;
                    if value_type.is_tombstone() {
                        // The key is GONE as of this row, so the run yields
                        // nothing: emitting the tombstone would surface a key the
                        // point read reports absent.
                        mask.push(false);
                        continue;
                    }
                }
                mask.push(!range_filter || key_in_bounds(key, &self.lo, &self.hi, cmp));
            }

            let mut visible = filter_batch(&batch, &mask);
            // The predicate runs on the deduped survivors only (see doc).
            if let Some(pred) = self.predicate.as_ref() {
                let pred_mask = pred.matching_rows(&visible);
                visible = filter_batch(&visible, &pred_mask);
            }
            // Match the singleton contract: yield exactly the projected columns.
            if !key_projected {
                visible.columns.retain(|c| c.column_id != COL_USER_KEY);
            }
            if !seqno_projected {
                visible.columns.retain(|c| c.column_id != COL_SEQNO);
            }
            if deletes && !vt_projected {
                visible.columns.retain(|c| c.column_id != COL_VALUE_TYPE);
            }
            if let Some(pc) = predicate_col
                && !predicate_col_projected
            {
                visible.columns.retain(|c| c.column_id != pc);
            }
            if visible.row_count > 0 {
                Self::globalize_seqnos(&mut visible, seg.global)?;
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
        // The predicate is applied AFTER newest-version dedup (below), so its
        // column must be decoded here even when the caller did not project it.
        let predicate_col = self.predicate.as_ref().map(|p| p.column_id);
        let predicate_col_projected = predicate_col.is_some_and(|c| self.projection.contains(&c));
        if let Some(pc) = predicate_col
            && !augmented.contains(&pc)
        {
            augmented.push(pc);
        }
        // Same rule as the singleton path: the newest version of a key can BE a
        // deletion, and then the key yields nothing. Decoded only when a segment
        // of this group records deletions.
        let deletes = group
            .segments
            .iter()
            .any(|s| s.table.tombstone_count() > 0 || s.table.weak_tombstone_count() > 0);
        let vt_projected = self.projection.contains(&COL_VALUE_TYPE);
        if deletes && !vt_projected {
            augmented.push(COL_VALUE_TYPE);
        }

        // Concatenate every segment's visible rows into one batch, tracking each
        // surviving row's effective seqno (`local + global`) in lockstep so the
        // dedup can compare versions across segments with different bases.
        let mut combined: Option<ColumnBatch> = None;
        let mut effective: Vec<SeqNo> = Vec::new();
        for seg in &group.segments {
            let threshold = self.seqno.saturating_sub(seg.global);
            // No predicate here: in an overlap group the predicate must run after
            // newest-version dedup, so every version (including a newest one that
            // fails the predicate but shadows an older matching version) has to be
            // collected first. Predicate-driven zone-map block-skip is likewise
            // unsafe here for the same reason, so it is also dropped.
            for batch in seg.table.columnar_scan(&augmented, None)? {
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
        let vt_col = if deletes {
            Some(
                combined
                    .columns
                    .iter()
                    .find(|c| c.column_id == COL_VALUE_TYPE)
                    .ok_or(Error::InvalidHeader(
                        "columnar_scan: merged group missing the value-type column",
                    ))?,
            )
        } else {
            None
        };
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
            if let Some(vt_col) = vt_col {
                let byte = *vt_col.data.get(i as usize).ok_or(Error::InvalidHeader(
                    "columnar_scan: value-type column shorter than the row count",
                ))?;
                let value_type = crate::ValueType::try_from(byte)
                    .map_err(|()| Error::InvalidTag(("ValueType", byte)))?;
                // The newest version deletes the key, so the key yields nothing —
                // the run is already decided, so the older versions stay dropped.
                if value_type.is_tombstone() {
                    continue;
                }
            }
            kept.push(i);
        }

        let mut merged = take_rows(&combined, &kept)?;

        // The union spans segments with DIFFERENT offsets, so no single one
        // applies: write each surviving row's effective seqno — already computed
        // for the dedup above — into the column, in the tree's global
        // coordinates. Done before the predicate filter, while row `i` of
        // `merged` still corresponds to `kept[i]`.
        if let Some(col) = merged.columns.iter_mut().find(|c| c.column_id == COL_SEQNO) {
            for (row, &i) in kept.iter().enumerate() {
                let at = row * 8;
                let bytes = col
                    .data
                    .get_mut(at..at + 8)
                    .ok_or(Error::InvalidHeader("columnar_scan: short seqno column"))?;
                bytes.copy_from_slice(&eff_at(i).to_le_bytes());
            }
        }

        // Apply the row predicate AFTER newest-version dedup: each surviving row is
        // now the newest visible version of its key, so a key whose newest version
        // fails the predicate is correctly dropped instead of falling back to an
        // older matching version.
        if let Some(pred) = self.predicate.as_ref() {
            let mask = pred.matching_rows(&merged);
            merged = filter_batch(&merged, &mask);
        }

        // Match the singleton contract: yield exactly the projected columns.
        if !key_projected {
            merged.columns.retain(|c| c.column_id != COL_USER_KEY);
        }
        if !seqno_projected {
            merged.columns.retain(|c| c.column_id != COL_SEQNO);
        }
        if deletes && !vt_projected {
            merged.columns.retain(|c| c.column_id != COL_VALUE_TYPE);
        }
        if let Some(pc) = predicate_col
            && !predicate_col_projected
        {
            merged.columns.retain(|c| c.column_id != pc);
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
