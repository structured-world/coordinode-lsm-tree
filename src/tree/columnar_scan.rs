// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

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
//! - An **overlapping** group is merged as it streams: the projection is
//!   augmented with the intrinsic key + seqno columns, each segment is a source
//!   read one row group at a time, and the merge takes the least row across the
//!   sources by `(key asc, effective seqno desc, source recency asc)`, keeping
//!   the newest visible row of each key. The key/seqno decode and the gather
//!   are paid only where segments overlap, and what the merge holds is one
//!   batch per source, not the group.
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

use alloc::borrow::Cow;

use crate::comparator::UserComparator;
use crate::table::SeqnoVisibility;
use crate::table::columnar::{
    COL_SEQNO, COL_USER_KEY, COL_VALUE_TYPE, ColumnBatch, Number, TypeTag, bytes_column_row,
    fixed_u64_row,
};
use crate::table::columnar_predicate::{
    ColumnRangePredicate, PredicateApply, PredicateSupport, filter_batch,
};
use crate::{Error, SeqNo, Table, Tree, UserKey};

mod merge;
pub mod projection;

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
    /// Source recency: the segment's position in the version's newest-first
    /// table order (lower = newer). Two segments can hold DIFFERENT values
    /// for one key at one caller-assigned seqno, and the read path serves
    /// the newer run's value — the merge path breaks the tie with this rank,
    /// because `group_by_overlap` re-sorts segments by minimum key and the
    /// concatenation order alone says nothing about recency.
    recency_rank: usize,
    /// Whether the segment stores each value whole, so its declared fields
    /// are read out of the value through the projector.
    whole: bool,
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
        projection: impl Into<projection::Projection>,
        predicate: Option<&ColumnRangePredicate>,
        seqno: SeqNo,
        range: R,
    ) -> crate::Result<ColumnarScan> {
        let projection = projection.into();
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

        // The segment list below is built from the one version a read at
        // `seqno` resolves to, held for the whole scan: a compaction installing
        // mid-scan cannot turn a row-major tree columnar (or the reverse)
        // underneath it, and the per-segment recency ranking is the ranking
        // THAT version has.
        let super_version = self.get_version_for_snapshot(seqno)?;

        let mut segments: Vec<Segment> = Vec::new();
        // `iter_tables` yields newest-first (the same order the sequenced
        // scan sources rely on), so the enumeration index is the recency
        // rank.
        for (recency_rank, table) in super_version.version.iter_tables().enumerate() {
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
            // `key_count == item_count` proves the segment holds one version per
            // key, so the verbatim path can return its rows untouched. The count
            // the writer recorded and the duplicate-free claim read here rest on
            // the SAME identity relation the read path uses to collapse versions
            // (`comparator::same_user_key`), so a segment this calls unique is
            // one a normal read would also return whole. `None` (a legacy
            // segment that recorded no count) proves nothing and dedups.
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
                recency_rank,
                whole: table.metadata.value_layout == crate::table::meta::ValueLayout::Whole,
                table: table.clone(),
            });
        }

        // A declared field of a segment that stores each value whole lies
        // inside the value, which only the caller's projector reads.
        let declared = projection.fields().iter().any(projection::is_declared);
        if declared && projection.value_projector().is_none() && segments.iter().any(|s| s.whole) {
            return Err(Error::Projection(
                "projection: a segment stores whole row values, and declared fields are \
                 read out of them through a projector, which is not set",
            ));
        }

        let groups = group_by_overlap(segments, comparator.as_ref());

        Ok(ColumnarScan {
            groups: groups.into_iter().collect(),
            current: None,
            projection: projection.column_ids(),
            fields: projection.fields().to_vec(),
            projector: declared
                .then(|| projection.value_projector().cloned())
                .flatten(),
            predicate: predicate.cloned(),
            support: PredicateSupport::Exact,
            comparator,
            seqno,
            lo,
            hi,
            budget: self.config.columnar_scan_budget,
            peak_payload: core::cell::Cell::new(0),
            oversized: core::cell::Cell::new(0),
            #[cfg(feature = "metrics")]
            metrics: self.0.metrics.clone(),
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

/// How a singleton group shapes each batch its table's cursor yields.
enum SingletonMode {
    /// Every row visible, the range unbounded, one version per key and no
    /// deletions: the batch goes out as read, its seqnos globalized.
    Verbatim,
    /// Rows masked by seqno visibility and the key range.
    Masked {
        /// Whether the snapshot straddles the segment, so rows are masked by
        /// seqno.
        partial: bool,
        /// The snapshot in the segment's local seqno space.
        threshold: SeqNo,
        /// Columns decoded only for the mask, dropped from each batch.
        dropped: Vec<u16>,
    },
    /// Newest visible version per key, deletions consumed, the predicate
    /// applied after the dedup.
    Dedup(DedupState),
}

/// The per-scan state of a singleton group's dedup: what each batch needs, and
/// the key run it last decided, which can span batch boundaries.
struct DedupState {
    /// The scan's predicate in the segment's local coordinates.
    predicate: Option<ColumnRangePredicate>,
    rts: Vec<(UserKey, UserKey, SeqNo)>,
    /// Whether the snapshot straddles the segment, so rows are masked by
    /// seqno.
    partial: bool,
    /// Whether the segment records deletions, so the value type is decoded.
    deletes: bool,
    /// The snapshot in the segment's local seqno space.
    threshold: SeqNo,
    /// Columns decoded only for the dedup and the predicate, dropped from
    /// each batch.
    dropped: Vec<u16>,
    /// The user key of the last key run whose newest visible version was
    /// already emitted (or deliberately dropped) — owned, because a run can
    /// span batch boundaries. One REUSED buffer: a fresh `to_vec` per run
    /// would make unique-key data (the common case) pay an allocation and
    /// free per row.
    last_key: Option<Vec<u8>>,
}

/// How a segment of a whole-value table is read for declared fields.
pub(super) struct WholeRead {
    /// The projector the fields are read through.
    projector: alloc::sync::Arc<dyn projection::ValueProjector>,
    /// Columns decoded only to read the fields, dropped once they are read.
    extra: Vec<u16>,
}

/// A segment's cursor and, for a whole-value segment read for declared
/// fields, how its batches become those fields.
pub(super) struct SegmentCursor {
    cursor: crate::table::columnar_cursor::ColumnarCursor,
    whole: Option<WholeRead>,
}

/// A singleton group streamed from its table's cursor.
struct SingletonStream {
    cursor: crate::table::columnar_cursor::ColumnarCursor,
    whole: Option<WholeRead>,
    /// The segment's `global_seqno` base.
    global: SeqNo,
    mode: SingletonMode,
}

/// The source of the group the scan is in.
enum GroupStream {
    /// A singleton group, streamed from its table's cursor: one allocation
    /// per group, next to the row groups it reads.
    Singleton(Box<SingletonStream>),
    /// An overlapping group, merged as it streams.
    Merge(Box<merge::MergeStream>),
    /// A group that yields nothing: its segment is ruled out.
    Empty,
}

impl GroupStream {
    /// Page bytes the group's sources hold now.
    fn held_bytes(&self) -> u64 {
        match self {
            Self::Singleton(singleton) => singleton.cursor.held_bytes(),
            Self::Merge(merge) => merge.held_bytes(),
            Self::Empty => 0,
        }
    }

    /// Reads past a share the group's sources made since this was last
    /// called.
    fn take_oversized(&mut self) -> u64 {
        match self {
            Self::Singleton(singleton) => singleton.cursor.take_oversized(),
            Self::Merge(merge) => merge.take_oversized(),
            Self::Empty => 0,
        }
    }
}

/// Drops from `batch` the columns decoded only for the scan's own use.
fn drop_columns(batch: &mut ColumnBatch, dropped: &[u16]) {
    if !dropped.is_empty() {
        batch.columns.retain(|c| !dropped.contains(&c.column_id));
    }
}

/// Iterator over a tree-level projected columnar scan.
///
/// Yields projected [`ColumnBatch`]es in ascending key order. Created by
/// [`Tree::columnar_scan`] (and surfaced through
/// [`AnyTree::columnar_scan`](crate::AnyTree::columnar_scan)). A group of one
/// segment streams its table as it yields, one row group at a time, so a
/// caller that stops early reads nothing more; a group of overlapping
/// segments is merged when the scan reaches it.
pub struct ColumnarScan {
    groups: alloc::collections::VecDeque<Group>,
    current: Option<GroupStream>,
    /// The projected column ids, in output order.
    projection: Vec<u16>,
    /// The projected fields, whose declarations every yielded batch is
    /// brought to.
    fields: Vec<projection::ProjectedField>,
    /// The projector the declared fields of a whole-value segment are read
    /// through; `None` when no field is declared.
    projector: Option<alloc::sync::Arc<dyn projection::ValueProjector>>,
    predicate: Option<ColumnRangePredicate>,
    /// The weakest [`PredicateSupport`] over the segments read so far.
    support: PredicateSupport,
    comparator: alloc::sync::Arc<dyn UserComparator>,
    /// The query snapshot, used for per-row seqno visibility masking.
    seqno: SeqNo,
    /// The requested key range. Applied as a per-row filter (not just segment
    /// selection): a segment that only partially overlaps the range must still
    /// drop the rows that fall outside it.
    lo: Bound<UserKey>,
    hi: Bound<UserKey>,

    /// The page bytes the scan may hold at once, shared by the segments of an
    /// overlapping group.
    budget: u64,
    /// The most page bytes the scan held at once so far.
    peak_payload: core::cell::Cell<u64>,
    /// Reads past a segment's share, as [`Self::oversized_reads`] counts them.
    oversized: core::cell::Cell<u64>,

    /// Where this scan's gather cost is recorded. Held rather than reached
    /// for through the tree because the scan outlives the call that built it.
    #[cfg(feature = "metrics")]
    metrics: alloc::sync::Arc<crate::Metrics>,
}

impl ColumnarScan {
    /// The most page bytes the scan held at once so far: the batches its
    /// segments have read and not yet handed to the merge or out. It stays
    /// within [`Config::columnar_scan_budget`](crate::Config::columnar_scan_budget)
    /// except by the reads [`Self::oversized_reads`] counts. What a read holds
    /// only while it runs, such as the pages a filter drops, is what that
    /// counter sees; this figure is what stays read between batches. A batch
    /// handed out and the scan's per-segment bookkeeping are not counted: the
    /// first is the caller's, the second grows with the segments merged, not
    /// with the rows.
    #[must_use]
    pub fn peak_payload_bytes(&self) -> u64 {
        self.peak_payload.get()
    }

    /// Reads that went past the share of the budget their segment has: a row
    /// page larger than the share, read on its own; a run of row pages whose
    /// rows decoded wider than the rows before them; a row group read whole
    /// because a pushed-down predicate selects its row pages by their zones.
    #[must_use]
    pub fn oversized_reads(&self) -> u64 {
        self.oversized.get()
    }

    /// Records that the scan holds `bytes` of page payload now.
    fn observe_payload(&self, bytes: u64) {
        if bytes > self.peak_payload.get() {
            self.peak_payload.set(bytes);
        }
    }

    /// Records `count` reads past their segment's share.
    fn record_oversized(&self, count: u64) {
        self.oversized.set(self.oversized.get() + count);
    }
    /// How far the scan's predicate ran over the segments read so far, or
    /// `None` when the scan has no predicate: the weakest answer of any
    /// segment, so it only ever falls as the scan goes, and is the answer for
    /// the whole scan once it is exhausted.
    ///
    /// [`PredicateSupport::Exact`] means every row yielded matches;
    /// [`PredicateSupport::PruneOnly`] and [`PredicateSupport::Unsupported`]
    /// mean the rows still need the caller's own check.
    #[must_use]
    pub fn predicate_support(&self) -> Option<PredicateSupport> {
        self.predicate.as_ref().map(|_| self.support)
    }

    /// Records the bytes a gather moved.
    ///
    /// The figure is the SIZE OF THE RESULT — what the operation wrote into a
    /// new buffer — which is what makes repeated accumulation visible:
    /// folding `k` batches one at a time records the whole accumulated size
    /// `k` times, so the counter grows quadratically exactly where the work
    /// does, while a single pass over the same data records it once.
    #[inline]
    #[cfg_attr(
        not(feature = "metrics"),
        expect(
            clippy::unused_self,
            reason = "the scan's metrics exist only with the feature"
        )
    )]
    fn record_gather(&self, batch: &ColumnBatch) {
        #[cfg(feature = "metrics")]
        self.metrics.record_gather(batch.data_size());
        #[cfg(not(feature = "metrics"))]
        let _ = batch;
    }
}

impl ColumnarScan {
    /// Opens one overlap group as the stream of its projected, key-ordered
    /// output batches. A singleton group streams its segment's table (masking
    /// by seqno only when the snapshot straddles the segment); an overlapping
    /// group is row-merged with newest-effective-seqno-wins dedup.
    ///
    /// Lowers `support` to how far the predicate ran over what opening read.
    fn open_group(
        &self,
        group: &Group,
        support: &mut PredicateSupport,
    ) -> crate::Result<GroupStream> {
        let rts = self.visible_group_range_tombstones(&group.segments)?;
        if let [seg] = group.segments.as_slice() {
            return self.open_singleton(seg, rts, support);
        }
        Ok(GroupStream::Merge(Box::new(merge::MergeStream::open(
            self,
            &group.segments,
            rts,
        )?)))
    }

    /// A cursor over `seg`'s table within the scan's key range, decoding
    /// `projection`, pushing `predicate` down and holding at most `share`
    /// page bytes at once.
    fn segment_cursor(
        &self,
        seg: &Segment,
        projection: &[u16],
        predicate: Option<&ColumnRangePredicate>,
        share: u64,
    ) -> crate::Result<SegmentCursor> {
        let Some(projector) = self.projector.clone().filter(|_| seg.whole) else {
            return Ok(SegmentCursor {
                cursor: seg.table.columnar_cursor(
                    projection,
                    predicate,
                    self.lo.clone(),
                    self.hi.clone(),
                    Some(share),
                )?,
                whole: None,
            });
        };
        // The declared fields lie inside the value: decode it and its key in
        // their place. A declared field's id may be the value column's own, so
        // no predicate is pushed down; the caller filters after the fields are
        // read.
        let declared: Vec<u16> = self
            .fields
            .iter()
            .filter(|f| projection::is_declared(f))
            .map(projection::ProjectedField::column_id)
            .collect();
        let mut ids: Vec<u16> = projection
            .iter()
            .copied()
            .filter(|id| !declared.contains(id))
            .collect();
        // The value column is consumed by reading the fields; the key is
        // dropped after it unless the caller or the scan projects it. The
        // value column's id may be a declared field's, so it is never named
        // among the columns dropped afterwards.
        let mut extra = Vec::new();
        if !ids.contains(&COL_USER_KEY) {
            ids.push(COL_USER_KEY);
            extra.push(COL_USER_KEY);
        }
        if !ids.contains(&crate::table::columnar::COL_VALUE) {
            ids.push(crate::table::columnar::COL_VALUE);
        }
        Ok(SegmentCursor {
            cursor: seg.table.columnar_cursor(
                &ids,
                None,
                self.lo.clone(),
                self.hi.clone(),
                Some(share),
            )?,
            whole: Some(WholeRead { projector, extra }),
        })
    }

    /// `batch` as `whole` says: its declared fields read out of its values,
    /// and the columns decoded only for that dropped.
    fn read_whole(
        &self,
        batch: ColumnBatch,
        whole: Option<&WholeRead>,
    ) -> crate::Result<ColumnBatch> {
        let Some(whole) = whole else {
            return Ok(batch);
        };
        let mut batch = projection::project_whole(batch, &self.fields, whole.projector.as_ref())?;
        drop_columns(&mut batch, &whole.extra);
        Ok(batch)
    }

    /// The next output batch of `stream`, or `None` once it is exhausted.
    /// Lowers `support` to how far the predicate ran over what was read.
    fn next_from(
        &self,
        stream: &mut GroupStream,
        support: &mut PredicateSupport,
    ) -> Option<crate::Result<ColumnBatch>> {
        match stream {
            GroupStream::Empty => None,
            GroupStream::Merge(merge) => merge.next_batch(self, support).transpose(),
            GroupStream::Singleton(singleton) => loop {
                let batch = match singleton.cursor.next()? {
                    Ok(batch) => batch,
                    Err(e) => return Some(Err(e)),
                };
                // Observed before the batch is shaped: a mask or dedup that
                // keeps none of its rows drops it here, and what the cursor
                // held with it would otherwise never be seen.
                self.observe_payload(singleton.cursor.held_bytes() + batch.data_size() as u64);
                *support = (*support).min(singleton.cursor.predicate_support());
                let batch = match self.read_whole(batch, singleton.whole.as_ref()) {
                    Ok(batch) => batch,
                    Err(e) => return Some(Err(e)),
                };
                let SingletonStream { global, mode, .. } = &mut **singleton;
                match self.shape_singleton_batch(batch, *global, mode, support) {
                    // A segment written without a projected column, or with
                    // null cells in one, reads as the field declares.
                    Ok(Some(batch)) => return Some(projection::conform(batch, &self.fields)),
                    Ok(None) => {}
                    Err(e) => return Some(Err(e)),
                }
            },
        }
    }

    /// Shapes one batch a singleton group's cursor read as `mode` says, or
    /// `None` when no row of it survives.
    fn shape_singleton_batch(
        &self,
        batch: ColumnBatch,
        global: SeqNo,
        mode: &mut SingletonMode,
        support: &mut PredicateSupport,
    ) -> crate::Result<Option<ColumnBatch>> {
        // A cursor yields a batch only for a row page that keeps a row.
        debug_assert!(batch.row_count > 0, "a cursor yielded an empty batch");
        match mode {
            SingletonMode::Verbatim => {
                let mut batch = batch;
                self.globalize_seqnos(&mut batch, global)?;
                Ok(Some(batch))
            }
            SingletonMode::Masked {
                partial,
                threshold,
                dropped,
            } => self.mask_singleton_batch(&batch, global, *partial, *threshold, dropped),
            SingletonMode::Dedup(state) => {
                self.dedup_singleton_batch(&batch, global, state, support)
            }
        }
    }

    /// Applies the scan's predicate to `batch` after the dedup, when it filters,
    /// and lowers `support` to how far it ran there.
    fn filter_after_dedup(
        &self,
        batch: ColumnBatch,
        pred: Option<&ColumnRangePredicate>,
        support: &mut PredicateSupport,
    ) -> crate::Result<ColumnBatch> {
        let Some(pred) = pred else {
            return Ok(batch);
        };
        *support = (*support).min(pred.support_in(&batch));
        if pred.apply != PredicateApply::Filter {
            return Ok(batch);
        }
        let mask = pred.matching_rows(&batch);
        let filtered = filter_batch(&batch, &mask)?;
        self.record_gather(&filtered);
        Ok(filtered)
    }

    /// The range tombstones of `segments` visible to the scan snapshot, with
    /// tree-global effective seqnos: an UNMATERIALIZED range deletion (a
    /// flushed `remove_range` no relocation has folded into a positional
    /// delete bitmap yet) lives only in the segments' RT sections, and the
    /// scan must suppress the rows it covers exactly as the point and
    /// ordinary range reads do. A group is key-disjoint from its neighbours
    /// and a tombstone's span is inside its own segment's key range, so
    /// per-group collection sees every tombstone that can cover a group row.
    fn visible_group_range_tombstones(
        &self,
        segments: &[Segment],
    ) -> crate::Result<Vec<(UserKey, UserKey, SeqNo)>> {
        let mut rts = Vec::new();
        for seg in segments {
            for rt in seg.table.visible_range_tombstones() {
                let eff = rt
                    .seqno
                    .checked_add(seg.global)
                    .ok_or(Error::InvalidHeader(
                        "columnar_scan: effective range-tombstone seqno overflows",
                    ))?;
                // Same exclusive-MVCC visibility as rows: the deletion exists
                // for this snapshot only below it.
                if eff < self.seqno {
                    rts.push((rt.start.clone(), rt.end.clone(), eff));
                }
            }
        }
        Ok(rts)
    }

    /// Whether a row (`key` at tree-global `eff` seqno) is deleted by one of
    /// the group's visible range tombstones: inside the half-open
    /// `[start, end)` span and older than the deletion.
    fn rt_covered(&self, rts: &[(UserKey, UserKey, SeqNo)], key: &[u8], eff: SeqNo) -> bool {
        let cmp = self.comparator.as_ref();
        rts.iter().any(|(start, end, rt_eff)| {
            eff < *rt_eff
                && cmp.compare(key, start.as_ref()) != core::cmp::Ordering::Less
                && cmp.compare(key, end.as_ref()) == core::cmp::Ordering::Less
        })
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
    /// the batch untouched. The rewritten column is a gather and is charged.
    #[cfg_attr(
        not(feature = "metrics"),
        expect(
            clippy::unused_self,
            reason = "the scan's metrics exist only with the feature"
        )
    )]
    fn globalize_seqnos(&self, batch: &mut ColumnBatch, global: SeqNo) -> crate::Result<()> {
        if global == 0 {
            return Ok(());
        }
        let Some(col) = batch.columns.iter_mut().find(|c| c.column_id == COL_SEQNO) else {
            return Ok(());
        };
        let len = batch.row_count as usize * 8;
        if col.data.len() != len {
            return Err(Error::InvalidHeader("columnar_scan: short seqno column"));
        }
        // Column bytes are an immutable (possibly shared) view, so the
        // globalized column is rebuilt into a new buffer, written in place:
        // one allocation per batch, and only for bulk-ingested segments
        // (`global != 0`).
        // SAFETY: the loop writes one 8-byte seqno per row, and `len` is
        // exactly `row_count * 8`, which the source column matches, so every
        // byte is initialized before the buffer is frozen and read. An early
        // return on overflow drops the builder unread.
        #[expect(unsafe_code, reason = "see safety")]
        let mut out = unsafe { crate::Slice::builder_unzeroed(len) };
        for (row, (dst, src)) in out
            .chunks_exact_mut(8)
            .zip(col.data.chunks_exact(8))
            .enumerate()
        {
            let mut local = [0u8; 8];
            local.copy_from_slice(src);
            let Some(effective) = u64::from_le_bytes(local).checked_add(global) else {
                // The rows before this one were already rewritten.
                #[cfg(feature = "metrics")]
                self.metrics.record_gather(row * 8);
                #[cfg(not(feature = "metrics"))]
                let _ = row;
                return Err(Error::InvalidHeader(
                    "columnar_scan: effective seqno overflows",
                ));
            };
            dst.copy_from_slice(&effective.to_le_bytes());
        }
        col.data = crate::Slice::from(out.freeze());
        #[cfg(feature = "metrics")]
        self.metrics.record_gather(len);
        Ok(())
    }

    /// Singleton group: no cross-segment merge. When every row is visible and the
    /// range is unbounded, the per-SST projected scan streams verbatim (zero-copy
    /// column-skip). Otherwise a per-row mask drops rows that are seqno-invisible
    /// (when the snapshot straddles the segment) or outside the requested range
    /// (when the segment only partially overlaps it).
    fn open_singleton(
        &self,
        seg: &Segment,
        rts: Vec<(UserKey, UserKey, SeqNo)>,
        support: &mut PredicateSupport,
    ) -> crate::Result<GroupStream> {
        // Every path below hands the predicate to the segment's table, or
        // evaluates it before the seqno column is globalized, so it runs in
        // the table's LOCAL seqno coordinates: a seqno bound is translated by
        // the segment's base first. A predicate no row of the segment can
        // satisfy (a seqno range wholly below its base) rules the segment out.
        let predicate = if let Some(pred) = self.predicate.as_ref() {
            let Some(local) = localize(pred, seg.global) else {
                *support = (*support).min(pred.support(Some(TypeTag::Number(Number::U64_LE))));
                return Ok(GroupStream::Empty);
            };
            Some(local.into_owned())
        } else {
            None
        };
        // A segment that RECORDS deletions takes the dedup path even when its
        // keys are provably unique: a key whose single row is a tombstone would
        // otherwise stream through verbatim and surface a key the point read
        // calls absent. Deciding a run is where tombstones are consumed, and
        // that lives there. A visible RANGE tombstone routes there for the
        // same reason: covered rows must be suppressed, and that needs each
        // row's seqno, which the verbatim path never decodes.
        // A segment whose declared fields are read out of whole values cannot
        // take a pushed-down predicate either (see `segment_cursor`): the
        // dedup path filters after the fields are read.
        if seg.may_dup
            || seg.table.tombstone_count() > 0
            || seg.table.weak_tombstone_count() > 0
            || !rts.is_empty()
            || (seg.whole && self.projector.is_some() && predicate.is_some())
        {
            return self.open_singleton_dedup(seg, rts, predicate);
        }
        let range_filter = !self.range_is_full();
        if seg.visibility == SeqnoVisibility::All && !range_filter {
            // Pushed down in local coordinates (translated above); the seqno
            // column is globalized only on the way out.
            let SegmentCursor { cursor, whole } =
                self.segment_cursor(seg, &self.projection, predicate.as_ref(), self.budget)?;
            return Ok(GroupStream::Singleton(Box::new(SingletonStream {
                cursor,
                whole,
                global: seg.global,
                mode: SingletonMode::Verbatim,
            })));
        }

        // Decode the columns the mask needs even when the caller did not project
        // them (dropped again at the end): the seqno column for partial-visibility
        // masking, the key column for range filtering.
        let partial = seg.visibility == SeqnoVisibility::Partial;
        let mut needed = Vec::new();
        if partial {
            needed.push(COL_SEQNO);
        }
        if range_filter {
            needed.push(COL_USER_KEY);
        }
        let (augmented, dropped) = self.augment(&needed);
        // Same local-coordinate pushdown as the verbatim path above.
        let SegmentCursor { cursor, whole } =
            self.segment_cursor(seg, &augmented, predicate.as_ref(), self.budget)?;
        Ok(GroupStream::Singleton(Box::new(SingletonStream {
            cursor,
            whole,
            global: seg.global,
            mode: SingletonMode::Masked {
                partial,
                // Visible iff `local < threshold` (the snapshot in this
                // segment's local seqno space); `Partial` guarantees the
                // subtraction is in range.
                threshold: self.seqno.saturating_sub(seg.global),
                dropped,
            },
        })))
    }

    /// The projection extended by the `needed` columns the caller did not
    /// project, in order, and the list of those added columns, which are the
    /// scan's own and dropped from what it yields.
    fn augment(&self, needed: &[u16]) -> (Vec<u16>, Vec<u16>) {
        let mut augmented = self.projection.clone();
        let mut dropped = Vec::new();
        for &column in needed {
            if !augmented.contains(&column) {
                augmented.push(column);
                dropped.push(column);
            }
        }
        (augmented, dropped)
    }

    /// One batch of a singleton group masked by seqno visibility and the key
    /// range, or `None` when no row of it survives.
    fn mask_singleton_batch(
        &self,
        batch: &ColumnBatch,
        global: SeqNo,
        partial: bool,
        threshold: SeqNo,
        dropped: &[u16],
    ) -> crate::Result<Option<ColumnBatch>> {
        let cmp = self.comparator.as_ref();
        let range_filter = !self.range_is_full();
        {
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
            let mut visible = filter_batch(batch, &mask)?;
            self.record_gather(&visible);
            drop_columns(&mut visible, dropped);
            if visible.row_count == 0 {
                return Ok(None);
            }
            self.globalize_seqnos(&mut visible, global)?;
            Ok(Some(visible))
        }
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
    ///
    /// `predicate` is the scan's, in this segment's local coordinates: it runs
    /// before the seqno column is globalized.
    fn open_singleton_dedup(
        &self,
        seg: &Segment,
        rts: Vec<(UserKey, UserKey, SeqNo)>,
        predicate: Option<ColumnRangePredicate>,
    ) -> crate::Result<GroupStream> {
        // Decode the columns the dedup needs even when the caller did not
        // project them (dropped again at the end): the key column always, the
        // seqno column when the snapshot straddles the segment OR a range
        // tombstone needs each row's age, the predicate column for the
        // after-dedup filter.
        let partial = seg.visibility == SeqnoVisibility::Partial;
        let mut needed = vec![COL_USER_KEY];
        if partial || !rts.is_empty() {
            needed.push(COL_SEQNO);
        }
        if let Some(pred) = &predicate {
            needed.push(pred.column_id);
        }
        // A deletion is what a key's newest row can BE, so deciding a run needs
        // the value type — otherwise a tombstone decides the run and is emitted
        // as a row while a point read calls the key absent, and a caller that did
        // not project the type column cannot tell that row from a live one with
        // an empty value. Decoded only for a segment that RECORDS deletions; one
        // without them keeps its columns untouched.
        let deletes = seg.table.tombstone_count() > 0 || seg.table.weak_tombstone_count() > 0;
        if deletes {
            needed.push(COL_VALUE_TYPE);
        }
        let (augmented, dropped) = self.augment(&needed);
        // No predicate pushed down: it runs after the dedup (see above).
        let SegmentCursor { cursor, whole } =
            self.segment_cursor(seg, &augmented, None, self.budget)?;
        Ok(GroupStream::Singleton(Box::new(SingletonStream {
            cursor,
            whole,
            global: seg.global,
            mode: SingletonMode::Dedup(DedupState {
                predicate,
                rts,
                partial,
                deletes,
                // Visible iff `local < threshold` (the snapshot in this
                // segment's local seqno space); `Partial` guarantees the
                // subtraction is in range.
                threshold: self.seqno.saturating_sub(seg.global),
                dropped,
                last_key: None,
            }),
        })))
    }

    /// One batch of a singleton group deduped to the newest visible version of
    /// each key, deletions consumed and the predicate applied after, or `None`
    /// when no row of it survives. The key run it last decided is carried in
    /// `state` across batches.
    fn dedup_singleton_batch(
        &self,
        batch: &ColumnBatch,
        global: SeqNo,
        state: &mut DedupState,
        support: &mut PredicateSupport,
    ) -> crate::Result<Option<ColumnBatch>> {
        let (partial, deletes, threshold) = (state.partial, state.deletes, state.threshold);
        let seqno_needed = partial || !state.rts.is_empty();
        let range_filter = !self.range_is_full();
        let cmp = self.comparator.as_ref();
        {
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
            let seqno_col = if seqno_needed {
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
                let local = match seqno_col {
                    Some(seqno_col) => Some(fixed_u64_row(&seqno_col.data, row)?),
                    None => None,
                };
                let visible = !partial || local.is_some_and(|l| l < threshold);
                if !visible {
                    mask.push(false);
                    continue;
                }
                let key = bytes_column_row(&key_col.data, batch.row_count, row)?;
                if state
                    .last_key
                    .as_deref()
                    .is_some_and(|last| cmp.compare(last, key) == core::cmp::Ordering::Equal)
                {
                    // A later visible version of an already-decided key run —
                    // shadowed by the newest visible version above it.
                    mask.push(false);
                    continue;
                }
                // First visible row of a new key run = the newest visible
                // version. Deciding the run here (even when the range filter or
                // a deletion drops the row) also drops its older versions above.
                let last = state.last_key.get_or_insert_with(Vec::new);
                last.clear();
                last.extend_from_slice(key);
                // A visible range tombstone deletes the run when it covers the
                // NEWEST visible version (older versions are older still); an
                // uncovered newest version shadows the covered older ones, so
                // deciding on it alone is exact.
                if !state.rts.is_empty() {
                    let eff =
                        local
                            .unwrap_or(0)
                            .checked_add(global)
                            .ok_or(Error::InvalidHeader(
                                "columnar_scan: effective seqno overflows",
                            ))?;
                    if self.rt_covered(&state.rts, key, eff) {
                        mask.push(false);
                        continue;
                    }
                }
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

            let visible = filter_batch(batch, &mask)?;
            self.record_gather(&visible);
            // The predicate runs on the deduped survivors only (see doc).
            let mut visible =
                self.filter_after_dedup(visible, state.predicate.as_ref(), support)?;
            // Match the singleton contract: yield exactly the projected columns.
            drop_columns(&mut visible, &state.dropped);
            if visible.row_count == 0 {
                return Ok(None);
            }
            self.globalize_seqnos(&mut visible, global)?;
            Ok(Some(visible))
        }
    }
}

/// `pred` in the LOCAL coordinates of a segment based at `global`, or `None`
/// when no row of that segment can match it.
///
/// A segment's table stores each row's seqno locally (a bulk-ingested one at
/// `0`, with one `global_seqno` for all of them) and the scan speaks effective
/// ones (`local + global`), so a bound on the seqno column is moved down by
/// `global` before the table's statistics or its row filter see it. A predicate
/// on any other column is passed as it is.
fn localize(pred: &ColumnRangePredicate, global: SeqNo) -> Option<Cow<'_, ColumnRangePredicate>> {
    if pred.column_id != COL_SEQNO || global == 0 {
        return Some(Cow::Borrowed(pred));
    }
    // An unsigned column's ordinal is its value.
    let (lo, hi) = pred.ordinal_span(Number::U64_LE)?;
    let global = u128::from(global);
    // Every row of the segment is at `global` or above: an upper bound below
    // it admits none, and a lower bound below it admits them from the first,
    // which is exactly the clamp to local `0`.
    let hi = hi.checked_sub(global)?;
    let lo = lo.saturating_sub(global);
    // Both are below `2^64`: the span is of 8-byte values.
    let bound = |v: u128| u64::try_from(v).map(|v| v.to_be_bytes().to_vec());
    Some(Cow::Owned(ColumnRangePredicate {
        column_id: COL_SEQNO,
        lower: Some(bound(lo).ok()?),
        upper: Some(bound(hi).ok()?),
        apply: pred.apply,
    }))
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
            let mut support = self.support;
            if let Some(mut stream) = self.current.take() {
                let next = self.next_from(&mut stream, &mut support);
                self.support = support;
                self.observe_payload(stream.held_bytes());
                self.record_oversized(stream.take_oversized());
                match next {
                    Some(Ok(batch)) => {
                        self.current = Some(stream);
                        return Some(Ok(batch));
                    }
                    // A failed group yields its error and is dropped; the next
                    // call moves on to the next group.
                    Some(Err(e)) => return Some(Err(e)),
                    None => continue,
                }
            }
            let group = self.groups.pop_front()?;
            let opened = self.open_group(&group, &mut support);
            self.support = support;
            match opened {
                Ok(stream) => self.current = Some(stream),
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

#[cfg(all(test, feature = "metrics"))]
mod tests;
