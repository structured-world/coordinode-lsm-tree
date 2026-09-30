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
//! verbatim path).
//!
//! # Row sources
//!
//! The memtables and the row-oriented tables in the range are sources too, so
//! fresh writes are seen before they reach a columnar table. Each is read at
//! the snapshot into batches of its keys, versions, value types and whole
//! values, and always merged, since it holds every version of its keys. A
//! merge operand row, in a tree with a merge operator, is replaced by what a
//! point read at the snapshot returns for its key, so a chain reads as the read
//! path resolves it.
//!
//! # Projected fields
//!
//! What a batch carries is a [`Projection`](projection::Projection): columns by
//! id, and declared fields with a type and what their absence reads as. A table
//! that stores each value whole (rows transposed at a flush or a compaction,
//! and every row source) yields its declared fields through the projection's
//! [`ValueProjector`](projection::ValueProjector); a table that stores the
//! value split into fields yields them as its columns, and a field it lacks is
//! absent. See [`projection`] for the absence rule every source follows.

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
mod rows;

use rows::{RowCursor, SourceCursor};

/// What a scan source reads: a columnar segment, or a source of rows.
enum Source {
    /// A columnar table, read column by column.
    Columnar(Table),
    /// A row-oriented table, read row by row.
    RowTable(Table),
    /// A memtable, active or sealed.
    Memtable(alloc::sync::Arc<crate::memtable::Memtable>),
}

/// A visible source selected for the scan, with its cached key range,
/// sequence base, and snapshot-visibility class.
struct Segment {
    source: Source,
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

impl Segment {
    /// Whether the source is read row by row.
    fn is_rows(&self) -> bool {
        !matches!(self.source, Source::Columnar(_))
    }

    /// Whether the source can hold a deletion, so the value type is decoded.
    fn records_deletions(&self) -> bool {
        match &self.source {
            Source::Columnar(table) | Source::RowTable(table) => {
                table.tombstone_count() > 0 || table.weak_tombstone_count() > 0
            }
            // A memtable keeps no count; any row of it can be a deletion.
            Source::Memtable(_) => true,
        }
    }

    /// The base the source's range tombstone seqnos are local to. A row
    /// table's range read already yields effective row seqnos, so its rows
    /// are merged at base `0`, but its tombstones are stored local.
    fn rt_base(&self) -> SeqNo {
        match &self.source {
            Source::Columnar(_) => self.global,
            Source::RowTable(table) => table.global_seqno(),
            Source::Memtable(_) => 0,
        }
    }

    /// The source's range tombstones, in its local seqno space.
    fn range_tombstones(&self) -> Vec<crate::range_tombstone::RangeTombstone> {
        match &self.source {
            Source::Columnar(table) | Source::RowTable(table) => {
                table.visible_range_tombstones().collect()
            }
            Source::Memtable(memtable) => memtable.range_tombstones_sorted(),
        }
    }
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
    /// Reads every source intersecting `range` at snapshot `seqno`: the
    /// memtables, the row-oriented tables and the columnar ones. It applies
    /// deletions and range tombstones from any source to the keys of every
    /// other, resolves merge chains as a read does, applies the optional
    /// `predicate` (zone-map block-skip + row filter, after the newest version
    /// of each key is chosen) and yields projected [`ColumnBatch`]es in
    /// ascending key order, one row per key: its newest visible version. A
    /// columnar segment no other source overlaps streams without merge
    /// overhead.
    ///
    /// `range` bounds the result at row granularity: a segment that only
    /// partially overlaps `range` contributes only the rows whose keys fall
    /// inside it (the inclusive / exclusive sense of each bound is honored). A
    /// fully unbounded range keeps the zero-copy fast path for an all-visible
    /// segment.
    ///
    /// `projection` names the columns each batch carries, in its order: column
    /// ids (the intrinsic [`COL_USER_KEY`] / seqno / value-type columns, or a
    /// value column typed by what the tables store), converted from a slice of
    /// ids, or a [`Projection`](projection::Projection) of declared fields,
    /// which says what a field a row lacks reads as and carries the projector
    /// the fields are read out of whole values through. Every other column of a
    /// columnar table is stepped over without decoding.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Projection`] when the projection names one column id
    /// twice or the highest id, and, lazily, while iterating, when a row
    /// returned stores its value whole and no projector is set to read its
    /// declared fields, or when the data does not satisfy the projection.
    /// Returns an error on a block read
    /// or decode failure, or on a failed point read resolving a merge chain.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::table::columnar::COL_USER_KEY;
    /// use lsm_tree::{AbstractTree, Config, SeqNo};
    ///
    /// let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// // Rows in the memtable are read like any other source.
    /// tree.insert("a", "1", 0);
    /// tree.insert("b", "2", 1);
    /// let rows: u32 = tree
    ///     .columnar_scan(&[COL_USER_KEY], None, SeqNo::MAX, ..)?
    ///     .map(|batch| batch.map(|b| b.row_count))
    ///     .sum::<lsm_tree::Result<u32>>()?;
    /// assert_eq!(rows, 2);
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    pub fn columnar_scan<R: RangeBounds<UserKey>>(
        &self,
        projection: impl Into<projection::Projection>,
        predicate: Option<&ColumnRangePredicate>,
        seqno: SeqNo,
        range: R,
    ) -> crate::Result<ColumnarScan> {
        let projection = projection.into();
        // A batch names its columns by id, so one id projected twice (say a
        // declared field and the raw value by id) would come out as two
        // columns no reader can tell apart.
        let fields = projection.fields();
        if fields.iter().enumerate().any(|(at, f)| {
            fields
                .get(..at)
                .is_some_and(|before| before.iter().any(|b| b.column_id() == f.column_id()))
        }) {
            return Err(Error::Projection(
                "projection: a column id is projected twice",
            ));
        }
        if fields
            .iter()
            .any(|f| f.column_id() == merge::COL_WHOLE_VALUE)
            || predicate.is_some_and(|p| p.column_id == merge::COL_WHOLE_VALUE)
        {
            return Err(Error::Projection(
                "projection: the highest column id is reserved for the scan",
            ));
        }
        // A merge chain is not a version chain: its older rows are the merge's
        // INPUTS, not data the newest row shadows, and a read hands back the
        // merged value. An operand row is therefore replaced by what a read at
        // the snapshot returns for its key, through the same operator, before
        // anything is projected from it. Without an operator the read path
        // returns the newest entry unchanged, the raw operand, which is what
        // the scan yields, so no resolution runs and no value type is decoded.
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
        //
        // The active memtable in that version stays writable, and its span is
        // taken now while its rows are read later: the snapshot is capped at
        // what the tree holds now, so a write or deletion that lands after the
        // scan is created is invisible to it, as it is outside the span and
        // the groups computed here. The version is the one the requested
        // snapshot is served by: a compaction can raise the retention floor
        // past every seqno the tree still holds, and a snapshot at or below the
        // floor is refused because it may need a version the compaction
        // collected. The cap never needs one: it lies above every seqno the
        // tree holds, so it reads what the latest snapshot reads.
        let super_version = self.get_version_for_snapshot(seqno)?;
        let seqno = match crate::AbstractTree::get_highest_seqno(self) {
            Some(highest) => highest
                .checked_add(1)
                .map_or(seqno, |next: SeqNo| seqno.min(next)),
            None => 0,
        };
        // Operands are resolved in that same version, so a version installed
        // mid-scan changes no row the scan returns.
        let resolver = self.config.merge_operator.clone().map(|operator| Resolver {
            version: super_version.clone(),
            operator,
        });

        // A declared field of a segment that stores each value whole lies
        // inside the value, which only the caller's projector reads, and only
        // for a row the scan returns.
        let declared = projection.fields().iter().any(projection::is_declared);

        let mut segments: Vec<Segment> = Vec::new();
        // Memtables are newer than every table, the active one newest; the
        // sealed ones are kept oldest first.
        let memtables = core::iter::once(super_version.active_memtable.clone())
            .chain(super_version.sealed_memtables.iter().rev().cloned());
        let mut recency_rank = 0;
        for memtable in memtables {
            if let Some((min, max)) = memtable_span(&memtable, &lo, &hi, comparator.as_ref()) {
                segments.push(Segment {
                    min,
                    max,
                    // A row source yields effective seqnos, and only the rows
                    // the snapshot sees.
                    global: 0,
                    visibility: SeqnoVisibility::All,
                    may_dup: true,
                    recency_rank,
                    whole: true,
                    source: Source::Memtable(memtable),
                });
            }
            recency_rank += 1;
        }
        // `iter_tables` yields newest-first (the same order the sequenced
        // scan sources rely on), so the enumeration index is the recency
        // rank after the memtables'.
        for (rank, table) in super_version.version.iter_tables().enumerate() {
            let recency_rank = recency_rank + rank;
            if !table.check_key_range_overlap_cmp(&bounds_ref, comparator.as_ref()) {
                continue;
            }
            // Snapshot visibility (exclusive MVCC). `None` segments postdate the
            // snapshot and are dropped.
            let visibility = table.seqno_visibility(seqno);
            if visibility == SeqnoVisibility::None {
                continue;
            }
            let key_range = &table.metadata.key_range;
            if !table.metadata.columnar {
                segments.push(Segment {
                    min: key_range.min().clone(),
                    max: key_range.max().clone(),
                    // Its range read yields effective seqnos, filtered to the
                    // snapshot by the row cursor.
                    global: 0,
                    visibility: SeqnoVisibility::All,
                    may_dup: true,
                    recency_rank,
                    whole: true,
                    source: Source::RowTable(table.clone()),
                });
                continue;
            }
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
                source: Source::Columnar(table.clone()),
            });
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
            declared,
            resolver,
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

/// The span of keys `memtable` holds in `lo..hi`, rows and range tombstones
/// alike, or `None` when it holds none there.
fn memtable_span(
    memtable: &crate::memtable::Memtable,
    lo: &Bound<UserKey>,
    hi: &Bound<UserKey>,
    cmp: &dyn UserComparator,
) -> Option<(UserKey, UserKey)> {
    use core::cmp::Ordering;

    let (ilo, ihi) = rows::internal_bounds(lo, hi);
    let mut rows = memtable.range_internal((ilo, ihi));
    let first = rows.next().map(|row| row.key.user_key);
    let last = rows
        .next_back()
        .map(|row| row.key.user_key)
        .or_else(|| first.clone());
    let mut span = first.zip(last);
    let bounds = (lo.clone(), hi.clone());
    for rt in memtable.range_tombstones_sorted() {
        if !crate::range::range_tombstone_overlaps_bounds(&rt, &bounds, cmp) {
            continue;
        }
        span = Some(match span {
            None => (rt.start.clone(), rt.end.clone()),
            Some((min, max)) => (
                if cmp.compare(&rt.start, &min) == Ordering::Less {
                    rt.start.clone()
                } else {
                    min
                },
                if cmp.compare(&rt.end, &max) == Ordering::Greater {
                    rt.end.clone()
                } else {
                    max
                },
            ),
        });
    }
    span
}

/// The tighter of two lower bounds: the greater key, an excluded one when
/// both name the same key.
fn tighter_lower(
    a: Bound<UserKey>,
    b: &Bound<UserKey>,
    cmp: &dyn UserComparator,
) -> Bound<UserKey> {
    use core::cmp::Ordering;

    match (&a, b) {
        (_, Bound::Unbounded) => a,
        (Bound::Unbounded, _) => b.clone(),
        (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => {
            match cmp.compare(x, y) {
                Ordering::Less => b.clone(),
                Ordering::Equal if matches!(b, Bound::Excluded(_)) => b.clone(),
                Ordering::Greater | Ordering::Equal => a,
            }
        }
    }
}

/// The tighter of two upper bounds: the lesser key, an excluded one when
/// both name the same key.
fn tighter_upper(
    a: Bound<UserKey>,
    b: &Bound<UserKey>,
    cmp: &dyn UserComparator,
) -> Bound<UserKey> {
    use core::cmp::Ordering;

    match (&a, b) {
        (_, Bound::Unbounded) => a,
        (Bound::Unbounded, _) => b.clone(),
        (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => {
            match cmp.compare(x, y) {
                Ordering::Greater => b.clone(),
                Ordering::Equal if matches!(b, Bound::Excluded(_)) => b.clone(),
                Ordering::Less | Ordering::Equal => a,
            }
        }
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

/// A segment's cursor, and whether it carries each row's whole value to the
/// rows the scan returns (see [`ColumnarScan::read_late`]).
pub(super) struct SegmentCursor {
    cursor: SourceCursor,
    whole: bool,
}

/// A singleton group streamed from its table's cursor.
struct SingletonStream {
    cursor: SourceCursor,
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

/// The rows of a batch whose operand was resolved to a value, by key.
#[derive(Debug, Default)]
pub(super) struct Resolved {
    /// The keys resolved, sorted: their declared fields are read out of the
    /// merged value, not out of the cells the operand carried.
    keys: Vec<crate::Slice>,
    /// Among them, sorted, the keys of rows a field projected by id alone
    /// cannot be read for: returned, such a row fails the scan.
    unreadable: Vec<crate::Slice>,
}

impl Resolved {
    /// Whether any operand was resolved to a value.
    pub(super) fn any(&self) -> bool {
        !self.keys.is_empty()
    }

    /// Whether `key`'s operand was resolved to a value.
    pub(super) fn holds(&self, key: &[u8]) -> bool {
        self.keys.binary_search_by(|k| (**k).cmp(key)).is_ok()
    }

    /// Whether `key`'s row cannot be returned: a field projected by id alone
    /// cannot be read out of its merged value.
    pub(super) fn unreadable(&self, key: &[u8]) -> bool {
        self.unreadable.binary_search_by(|k| (**k).cmp(key)).is_ok()
    }
}

/// A returned row's operand was resolved to a value read whole, which a field
/// projected by id alone cannot be read out of.
pub(super) const UNREADABLE_BY_ID: Error = Error::Projection(
    "projection: a merged value is read whole, and a field projected by id alone cannot be \
     read out of it",
);

/// A batch lacks a column the scan decoded for itself.
const MISSING_BATCH_COLUMN: Error =
    Error::InvalidHeader("columnar_scan: a batch is missing a column the scan decoded");

/// Whether a row of `batch` is a merge operand; `false` for a batch without a
/// value-type column.
fn holds_operand(batch: &ColumnBatch) -> bool {
    batch
        .columns
        .iter()
        .find(|c| c.column_id == COL_VALUE_TYPE)
        .is_some_and(|types| {
            types
                .data
                .iter()
                .any(|&byte| crate::ValueType::try_from(byte) == Ok(crate::ValueType::MergeOperand))
        })
}

/// Drops from `batch` the columns decoded only for the scan's own use.
fn drop_columns(batch: &mut ColumnBatch, dropped: &[u16]) {
    if !dropped.is_empty() {
        batch.columns.retain(|c| !dropped.contains(&c.column_id));
    }
}

/// Resolves a merge operand row as a point read at the scan's snapshot would,
/// in the version the scan reads.
struct Resolver {
    version: crate::version::SuperVersion,
    operator: alloc::sync::Arc<dyn crate::merge_operator::MergeOperator>,
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
    /// Whether a field is declared, so a whole-value segment is read without
    /// its value column in the declared ids' place even with no projector,
    /// which only a memtable holding nothing but deletions allows.
    declared: bool,
    /// What resolves a merge operand row, when the tree merges; `None` when
    /// it has no merge operator.
    resolver: Option<Resolver>,
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
        // A row source yields every version of its keys, deletions among
        // them, and its values whole: the merge decides each key. So does a
        // table whose values are read only for the rows it returns.
        if let [seg] = group.segments.as_slice()
            && !seg.is_rows()
            && !self.reads_late(seg)
        {
            return self.open_singleton(seg, rts, support);
        }
        Ok(GroupStream::Merge(Box::new(merge::MergeStream::open(
            self,
            &group.segments,
            rts,
        )?)))
    }

    /// Whether `seg` is read through the merge with its values read only for
    /// the rows the scan returns: every segment of a tree that merges, whose
    /// operands a returned row resolves, and a whole-value table whose
    /// declared fields a returned row reads out of its value.
    fn reads_late(&self, seg: &Segment) -> bool {
        self.resolver.is_some() || (seg.whole && self.declared)
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
        // A whole value is carried to the rows the scan returns, for its
        // declared fields and for the merge operands it may hold.
        let whole = seg.whole && (self.declared || self.resolver.is_some());
        // The declared fields of a whole value lie inside it: decode the value
        // in their place. A declared field's id may be the value column's own,
        // so no predicate is pushed down; the merge filters after the fields
        // are read.
        let mut ids: Vec<u16> = projection.to_vec();
        if whole {
            let declared: Vec<u16> = self
                .fields
                .iter()
                .filter(|f| projection::is_declared(f))
                .map(projection::ProjectedField::column_id)
                .collect();
            ids.retain(|id| !declared.contains(id));
            if !ids.contains(&crate::table::columnar::COL_VALUE) {
                ids.push(crate::table::columnar::COL_VALUE);
            }
        }
        let predicate = predicate.filter(|_| !whole);
        let cursor = match &seg.source {
            Source::Columnar(table) => SourceCursor::Columnar(Box::new(table.columnar_cursor(
                &ids,
                predicate,
                self.lo.clone(),
                self.hi.clone(),
                Some(share),
            )?)),
            Source::RowTable(table) => SourceCursor::Rows(RowCursor::table(
                table,
                self.lo.clone(),
                self.hi.clone(),
                self.seqno,
                ids,
                share,
            )),
            // Within the span its group was formed on: the active memtable
            // stays writable, and a row landing outside that span after the
            // scan was created would be read out of order, or beside a
            // version of its key another group returns. A range tombstone
            // widens that span past the scan's range, so the cursor reads
            // only where the two meet: every row beyond is dropped by the
            // range check anyway.
            Source::Memtable(memtable) => {
                let cmp = self.comparator.as_ref();
                let lo = tighter_lower(Bound::Included(seg.min.clone()), &self.lo, cmp);
                let hi = tighter_upper(Bound::Included(seg.max.clone()), &self.hi, cmp);
                SourceCursor::Rows(RowCursor::memtable(
                    memtable.clone(),
                    &lo,
                    &hi,
                    self.seqno,
                    ids,
                    share,
                ))
            }
        };
        Ok(SegmentCursor { cursor, whole })
    }

    /// The rows a merge returned, `batch`, with their values read: each merge
    /// operand replaced by what a read at the scan's snapshot returns for its
    /// key (a key the read finds absent is dropped), then the declared fields
    /// of each row carrying a whole value read out of it, and the carried
    /// value column dropped. Only returned rows get here, so a shadowed,
    /// deleted or invisible version is never resolved or projected. Also
    /// returns which keys were resolved to a value: such a row holds no cell
    /// of its own for a column no field declares.
    pub(super) fn read_late(&self, batch: ColumnBatch) -> crate::Result<(ColumnBatch, Resolved)> {
        let (batch, resolved) = self.resolve_operands(batch)?;
        let mut batch = if self.declared {
            projection::project_decided(
                batch,
                &self.fields,
                self.projector.as_deref(),
                merge::COL_WHOLE_VALUE,
            )?
        } else {
            batch
        };
        drop_columns(&mut batch, &[merge::COL_WHOLE_VALUE]);
        Ok((batch, resolved))
    }

    /// `batch` with each merge operand row replaced by what a read at the
    /// scan's snapshot returns for its key, through the tree's own operator
    /// exactly as the read path resolves it: that value, carried as the row's
    /// whole value (and in the value column when it is projected by id), or
    /// the row dropped when the read finds the key absent. Also returns which
    /// keys were resolved to a value (see [`Resolved`]).
    ///
    /// The value an operand resolves to is read whole, like any row value: a
    /// value field projected by id alone cannot be read out of it, so such a
    /// row is named unreadable, and the scan fails as it does for a whole
    /// value if the row is returned, instead of returning the cell the
    /// operand itself carried.
    fn resolve_operands(&self, batch: ColumnBatch) -> crate::Result<(ColumnBatch, Resolved)> {
        use crate::table::columnar::{COL_VALUE, Column, frame_bytes_column};

        let Some(resolver) = &self.resolver else {
            return Ok((batch, Resolved::default()));
        };
        if !holds_operand(&batch) {
            return Ok((batch, Resolved::default()));
        }
        let row_count = batch.row_count;
        let rows = row_count as usize;
        let find = |id: u16| batch.columns.iter().position(|c| c.column_id == id);
        let (Some(key_at), Some(type_at), Some(whole_at)) = (
            find(COL_USER_KEY),
            find(COL_VALUE_TYPE),
            find(merge::COL_WHOLE_VALUE),
        ) else {
            return Err(MISSING_BATCH_COLUMN);
        };
        // The raw value, read by id or by the predicate, reads the resolved
        // value too.
        let raw_at = find(COL_VALUE).filter(|&at| {
            batch
                .columns
                .get(at)
                .is_some_and(|c| c.type_tag == TypeTag::Bytes)
                && self.raw_value_read()
        });
        let column = |at: usize| batch.columns.get(at).ok_or(MISSING_BATCH_COLUMN);
        let (keys, kinds) = (column(key_at)?, column(type_at)?);
        // Per row: `None` keeps it as read, `Some(None)` drops it, and
        // `Some(Some(value))` makes it that value.
        let mut resolved: Vec<Option<Option<crate::Slice>>> = Vec::with_capacity(rows);
        for row in 0..row_count {
            let byte = *kinds.data.get(row as usize).ok_or(MISSING_BATCH_COLUMN)?;
            if crate::ValueType::try_from(byte) != Ok(crate::ValueType::MergeOperand) {
                resolved.push(None);
                continue;
            }
            // Each key is returned once, so each is read once.
            let key = bytes_column_row(&keys.data, row_count, row)?;
            resolved.push(Some(Tree::resolve_or_passthrough(
                &resolver.version,
                key,
                self.seqno,
                Some(&resolver.operator),
                self.comparator.as_ref(),
            )?));
        }
        // Only the raw value is rewritten below, and only a row that carried
        // its value whole holds the raw value under the value column's id: a
        // row of a table that splits its values carries no whole value, and
        // its cell under that id is a field like any other.
        let whole = column(whole_at)?;
        let by_id: Vec<u16> = self
            .fields
            .iter()
            .filter(|f| {
                f.type_tag().is_none() && batch.columns.iter().any(|c| c.column_id == f.column_id())
            })
            .map(projection::ProjectedField::column_id)
            .collect();
        let mut outcome = Resolved::default();
        for (row, fix) in (0..row_count).zip(&resolved) {
            if !matches!(fix, Some(Some(_))) {
                continue;
            }
            let key = crate::Slice::from(bytes_column_row(&keys.data, row_count, row)?);
            let raw_rewritten = raw_at.is_some() && whole.is_valid(row);
            if by_id.iter().any(|&id| !(id == COL_VALUE && raw_rewritten)) {
                outcome.unreadable.push(key.clone());
            }
            outcome.keys.push(key);
        }
        outcome.keys.sort_unstable();
        outcome.unreadable.sort_unstable();

        // The cells of bytes column `at` with the resolved rows' values in
        // place: a resolved row's cell is its value, the others as read.
        let rewrite = |at: usize| -> crate::Result<Column> {
            let old = column(at)?;
            let mut cells: Vec<Option<&[u8]>> = Vec::with_capacity(rows);
            for (row, fix) in (0..row_count).zip(&resolved) {
                cells.push(match fix {
                    Some(Some(value)) => Some(&**value),
                    _ if old.is_valid(row) => Some(bytes_column_row(&old.data, row_count, row)?),
                    _ => None,
                });
            }
            let validity = cells.iter().any(Option::is_none).then(|| {
                let mut bits = alloc::vec![0u8; rows.div_ceil(8)];
                for (row, cell) in cells.iter().enumerate() {
                    if cell.is_some()
                        && let Some(byte) = bits.get_mut(row / 8)
                    {
                        *byte |= 1 << (row % 8);
                    }
                }
                bits
            });
            Ok(Column {
                column_id: old.column_id,
                type_tag: TypeTag::Bytes,
                validity,
                data: frame_bytes_column(rows, || cells.iter().map(|c| c.unwrap_or(&[])))?,
            })
        };
        let whole = rewrite(whole_at)?;
        let raw = raw_at.map(rewrite).transpose()?;
        let types: Vec<u8> = kinds
            .data
            .iter()
            .zip(&resolved)
            .map(|(&byte, fix)| match fix {
                Some(_) => u8::from(crate::ValueType::Value),
                None => byte,
            })
            .collect();

        let mut batch = batch;
        if let Some(slot) = batch.columns.get_mut(whole_at) {
            *slot = whole;
        }
        if let (Some(at), Some(raw)) = (raw_at, raw)
            && let Some(slot) = batch.columns.get_mut(at)
        {
            *slot = raw;
        }
        if let Some(slot) = batch.columns.get_mut(type_at) {
            slot.data = crate::Slice::from(types);
        }
        // A key the read finds absent is not returned.
        let batch = if resolved.iter().any(|fix| matches!(fix, Some(None))) {
            let keep: Vec<bool> = resolved
                .iter()
                .map(|fix| !matches!(fix, Some(None)))
                .collect();
            filter_batch(&batch, &keep)?
        } else {
            batch
        };
        self.record_gather(&batch);
        Ok((batch, outcome))
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
                // A segment written without a projected column, or with null
                // cells in one, reads as the field declares before its rows
                // are decided, so a predicate after the dedup sees the
                // declared defaults, as it does on the merge path.
                let (batch, mistyped) = match projection::conform_lenient(batch, &self.fields) {
                    Ok(conformed) => conformed,
                    Err(e) => return Some(Err(e)),
                };
                let SingletonStream { global, mode, .. } = &mut **singleton;
                match self.shape_singleton_batch(batch, *global, mode, support) {
                    // A column's type is the batch's, so any row it returns
                    // stores the field under the other type.
                    Ok(Some(_)) if mistyped => return Some(Err(projection::MISTYPED)),
                    // The rows returned are decided: each is held to the
                    // declarations.
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

    /// Whether the raw value is read by id, beside any declared field read
    /// out of it: a field projects it by id, or the predicate runs on it and
    /// no field declares its id.
    pub(super) fn raw_value_read(&self) -> bool {
        use crate::table::columnar::COL_VALUE;

        let by_id = self
            .fields
            .iter()
            .any(|f| f.column_id() == COL_VALUE && !projection::is_declared(f));
        let declared = self
            .fields
            .iter()
            .any(|f| f.column_id() == COL_VALUE && projection::is_declared(f));
        by_id
            || (!declared
                && self
                    .predicate
                    .as_ref()
                    .is_some_and(|p| p.column_id == COL_VALUE))
    }

    /// Whether the scan's predicate judges the decided rows of `batch` before
    /// their values are read: its column is one reading the values does not
    /// change. The key and the seqno never change; a column no field
    /// declares is the row's own physical cell, unless an operand of the
    /// batch is still to be resolved into a value read whole. A declared
    /// field is read out of the value, and resolving an operand rewrites the
    /// value column and the value type, so a predicate over those runs after.
    pub(super) fn predicate_precedes_values(&self, batch: &ColumnBatch) -> bool {
        use crate::table::columnar::COL_SEQNO;

        let Some(pred) = &self.predicate else {
            return false;
        };
        // The raw value and the value type are the row's own cells too: a
        // whole-value source keeps its raw value in place when the predicate
        // reads it, and only an operand still to be resolved rewrites them.
        match pred.column_id {
            COL_USER_KEY | COL_SEQNO => true,
            id => {
                let declared = self
                    .fields
                    .iter()
                    .any(|f| f.column_id() == id && projection::is_declared(f));
                let unresolved = self.resolver.is_some() && holds_operand(batch);
                !(declared || unresolved)
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
            for rt in seg.range_tombstones() {
                let eff = rt
                    .seqno
                    .checked_add(seg.rt_base())
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
        // So does a predicate over a declared field: a segment may lack its
        // column, which reads as its declaration only once the batch is read,
        // and a table's own predicate would find nothing to run on. A segment
        // whose values are read late never gets here (see `reads_late`).
        let predicate_on_declared = self.predicate.as_ref().is_some_and(|p| {
            self.fields
                .iter()
                .any(|f| f.column_id() == p.column_id && projection::is_declared(f))
        });
        if seg.may_dup || seg.records_deletions() || predicate_on_declared || !rts.is_empty() {
            return self.open_singleton_dedup(seg, rts, predicate);
        }
        let range_filter = !self.range_is_full();
        if seg.visibility == SeqnoVisibility::All && !range_filter {
            // Pushed down in local coordinates (translated above); the seqno
            // column is globalized only on the way out.
            let SegmentCursor { cursor, .. } =
                self.segment_cursor(seg, &self.projection, predicate.as_ref(), self.budget)?;
            return Ok(GroupStream::Singleton(Box::new(SingletonStream {
                cursor,
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
        let SegmentCursor { cursor, .. } =
            self.segment_cursor(seg, &augmented, predicate.as_ref(), self.budget)?;
        Ok(GroupStream::Singleton(Box::new(SingletonStream {
            cursor,
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
        let deletes = seg.records_deletions();
        if deletes {
            needed.push(COL_VALUE_TYPE);
        }
        let (augmented, dropped) = self.augment(&needed);
        // No predicate pushed down: it runs after the dedup (see above).
        let SegmentCursor { cursor, .. } =
            self.segment_cursor(seg, &augmented, None, self.budget)?;
        Ok(GroupStream::Singleton(Box::new(SingletonStream {
            cursor,
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
