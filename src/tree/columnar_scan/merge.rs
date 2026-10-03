// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The streaming merge of an overlapping segment group.
//!
//! Every segment's rows arrive sorted by key, so the union needs no global
//! sort: each segment is a source holding its current batch and a row cursor,
//! kept on a binary heap by its current row, and the merge repeatedly takes
//! the least row across the sources, by user key ascending, effective seqno
//! descending and source recency ascending: the order the read path resolves
//! versions in. That row is the key's newest
//! visible version; every other row of the key, in any source and in any later
//! batch of it, is shadowed and skipped. A deleted key, a key a range
//! tombstone covers and a key outside the range yield nothing, and the
//! predicate runs on the output batch, after the dedup.
//!
//! The chosen rows are gathered from the sources' current batches into one
//! output batch, cut at a target row count and whenever a source whose rows
//! are chosen must load its next batch, so what the merge holds is one batch
//! per source plus the output, not the group.

use alloc::vec::Vec;

use super::projection::{MISTYPED, ProjectedField, conform, conform_lenient};
use super::rows::SourceCursor;
use super::{ColumnarScan, Segment, SegmentCursor, drop_columns, key_in_bounds};
use super::{PredicateTiming, TombstoneSweep, is_operand, value_types};
use super::{Resolved, UNREADABLE_BY_ID};
use crate::table::columnar::{
    COL_SEQNO, COL_USER_KEY, COL_VALUE, COL_VALUE_TYPE, Column, ColumnBatch, TypeTag,
    bytes_column_row, bytes_column_span, fixed_u64_row, frame_bytes_column,
};
use crate::table::columnar_predicate::{PredicateApply, PredicateSupport, RowMatcher};
use crate::{Error, SeqNo, UserKey};

/// Rows an output batch of the merge is cut at.
const TARGET_ROWS: usize = 4_096;

/// The id under which a merge carries each row's whole value from a
/// whole-value source to the rows it returns, null for a row of a source
/// that splits its values. It never reaches the caller, and a projection may
/// not name it.
pub(super) const COL_WHOLE_VALUE: u16 = u16::MAX;

/// One segment of an overlapping group, read through its cursor.
struct MergeSource {
    cursor: SourceCursor,
    /// Whether the segment carries each row's whole value (see
    /// [`COL_WHOLE_VALUE`]).
    whole: bool,
    /// The batch the source is in, `None` before the first and once the
    /// cursor is exhausted.
    batch: Option<ColumnBatch>,
    /// How `batch` stores the projected fields.
    types: FieldTypes,
    /// The next row of `batch` to consider.
    row: u32,
    /// Where `batch` keeps the key and seqno columns.
    key_col: usize,
    seqno_col: usize,
    /// At a row: where its key sits in the key column, and its effective
    /// seqno, found once when the source was positioned so the heap compares
    /// them without reading the row again.
    key: core::ops::Range<usize>,
    eff: SeqNo,
    /// The segment's `global_seqno` base.
    global: SeqNo,
    /// Whether every row of the segment is visible at the snapshot.
    all_visible: bool,
    /// The snapshot in the segment's local seqno space.
    threshold: SeqNo,
    /// The segment's position in the version's newest-first order.
    rank: usize,
    /// Whether a row of `batch` is chosen for the output being built, so the
    /// batch must stay until the output is gathered.
    referenced: bool,
    /// The columns of the segment read for its chosen rows only.
    late: LatePayload,
}

/// The columns of a columnar segment a merge reads only for the rows it
/// chooses, and how dense its choices are.
///
/// The key, seqno and value type of every row are read to decide it; the
/// payload (the projected fields but the predicate's, a whole value, a blob
/// tree's references) is read afterwards, from the row page of each batch a
/// row was chosen from, and only from those: a page none of whose rows is
/// chosen is never requested, and only the chosen rows' cells are gathered.
/// When most of a segment's row pages hold a chosen row anyway, reading the
/// payload page by page would only add requests for the same pages, so the
/// segment's cursor reads it with the rest from its next row group on, and
/// goes back once the choices thin out again.
#[derive(Default)]
struct LatePayload {
    /// Each column as the table stores it and as the merge carries it.
    columns: Vec<(u16, u16)>,
    /// The columns the cursor decodes with the payload; empty without one.
    eager_ids: Vec<u16>,
    /// The columns it decodes without it.
    lean_ids: Vec<u16>,
    /// Whether the cursor reads the payload with the rest, the choices
    /// being dense.
    eager: bool,
    /// Whether the current batch was read without its payload.
    batch_late: bool,
    /// The row page the current batch was read from.
    batch_at: Option<crate::table::columnar_cursor::PageAt>,
    /// Rows chosen from the current batch.
    batch_picks: u32,
    /// The decoded bytes of the current batch's payload read late, and of
    /// them the chosen rows' cells taken so far.
    page_bytes: u64,
    page_useful: u64,
    /// Row pages loaded so far, and how many of them held a chosen row.
    pages: u32,
    pages_hit: u32,
}

/// Row pages a segment is judged dense or sparse over, at the least.
const DENSITY_WINDOW: u32 = 4;

impl LatePayload {
    /// What the current batch's payload read late held besides the chosen
    /// rows' cells, now that the batch is done with.
    fn incidental(&mut self) -> u64 {
        // The chosen rows' cells are cells of the pages read.
        debug_assert!(self.page_useful <= self.page_bytes);
        let incidental = self.page_bytes - self.page_useful;
        self.page_bytes = 0;
        self.page_useful = 0;
        incidental
    }

    /// Records the batch just spent, and returns the columns the cursor is to
    /// decode from now on when the choices turned dense or sparse.
    fn spent(&mut self) -> Option<&[u16]> {
        if self.columns.is_empty() {
            return None;
        }
        self.pages += 1;
        if self.batch_picks > 0 {
            self.pages_hit += 1;
        }
        self.batch_picks = 0;
        if self.pages < DENSITY_WINDOW {
            return None;
        }
        // Dense from three in four pages holding a chosen row, sparse again
        // from one in four: the gap keeps a segment near either from flipping
        // back and forth.
        let (hit, all) = (u64::from(self.pages_hit), u64::from(self.pages));
        if !self.eager && hit * 4 >= all * 3 {
            self.eager = true;
            return Some(&self.eager_ids);
        }
        if self.eager && hit * 4 <= all {
            self.eager = false;
            return Some(&self.lean_ids);
        }
        None
    }
}

/// How a source's batch stores the projected fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FieldTypes {
    /// Each under the type its field declares.
    AsDeclared,
    /// One under another type: a row of the batch the scan returns fails the
    /// scan, one shadowed or filtered out does not.
    Mistyped,
    /// The predicate's own column under another type: the predicate cannot
    /// judge a row of the batch, so a row chosen from it fails the scan as a
    /// returned one would.
    MistypedUnderPredicate,
}

impl MergeSource {
    /// Page bytes this source holds: its current batch and what its cursor
    /// read ahead.
    fn held_bytes(&self) -> u64 {
        self.batch.as_ref().map_or(0, |b| b.data_size() as u64) + self.cursor.held_bytes()
    }
}

/// Where a source stands after it was positioned.
enum Position {
    /// At a visible row of a key not yet decided.
    At,
    /// Its batch is spent but holds chosen rows: the output must be gathered
    /// before it loads the next one.
    MustGather,
    /// No rows left.
    Exhausted,
}

/// A row chosen for the output.
struct Pick {
    source: usize,
    row: u32,
    /// The row's effective (tree-global) seqno.
    eff: SeqNo,
}

/// Whether a merge's returned rows have their values read, and what its
/// batches carry for that.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Late {
    /// The rows are returned as read.
    No,
    /// Every batch carries each row's whole value.
    Values,
    /// The group belongs to a blob tree: every batch carries each row's
    /// whole value and the references of rows written as cells.
    Cells,
}

/// The streaming merge of one overlapping group.
pub(super) struct MergeStream {
    sources: Vec<MergeSource>,
    /// Columns decoded only for the merge and the predicate, dropped from each
    /// output batch.
    dropped: Vec<u16>,
    /// The projected fields followed by the merge's own columns: every batch
    /// a source loads is brought to them, so the sources agree on their
    /// columns even where a segment was written without a projected one.
    fields: Vec<ProjectedField>,
    /// The columns of no declared type the merge decodes: projected by id, or
    /// the predicate's when no field names it. A source may be written
    /// without one, or hold it inside a whole value, and nothing says how
    /// such a row reads for it, so only the rows chosen are held to it.
    loose: Vec<u16>,
    /// The predicate's column when it is loose: rows chosen from batches
    /// that do not all carry it cannot be judged.
    loose_predicate: Option<u16>,
    /// Whether the rows returned have their values read (see
    /// [`ColumnarScan::read_late`]), and what every batch carries for that.
    late: Late,
    /// Whether the raw value stays beside the whole value a source carries.
    raw: RawValue,
    /// The column the scan's predicate runs on, if it has one.
    predicate_column: Option<u16>,
    /// Whether a segment of the group records deletions, so the value type is
    /// decoded.
    deletes: bool,
    /// The group's visible range tombstones.
    rts: TombstoneSweep,
    /// The user key last decided: its remaining rows are shadowed.
    last_key: Option<Vec<u8>>,
    pending: Vec<Pick>,
    /// The sources at a row, as a binary min-heap by their current rows.
    heap: Vec<usize>,
    /// Whether the sources were positioned for the first time.
    started: bool,
    /// A source off the heap until the output taking its chosen rows is out.
    parked: Option<usize>,
}

impl MergeStream {
    /// Opens the merge of `segments`, whose visible range tombstones are `rts`.
    pub(super) fn open(
        scan: &ColumnarScan,
        segments: &[Segment],
        rts: Vec<(UserKey, UserKey, SeqNo)>,
    ) -> crate::Result<Self> {
        // The merge needs each row's key and effective seqno, so decode the
        // intrinsic key + seqno columns even when the caller did not project
        // them; the predicate's column for the filter after the dedup; the
        // value type where a segment records deletions, because the newest
        // version of a key can BE a deletion and then the key yields nothing.
        // An operand can resolve to a deletion, so a tree that merges decodes
        // the value type of every source.
        // A blob tree's rows are read by their value type too: a cell row and
        // an indirection read differently from a value.
        let deletes = scan.resolver.is_some()
            || scan.cells.is_some()
            || segments.iter().any(Segment::records_deletions);
        let mut needed = alloc::vec![COL_USER_KEY, COL_SEQNO];
        if let Some(pred) = &scan.predicate {
            needed.push(pred.column_id);
        }
        if deletes {
            needed.push(COL_VALUE_TYPE);
        }
        let (augmented, dropped) = scan.augment(&needed);
        // The merge's own intrinsic columns are always present. An unprojected
        // predicate column is not a projected field, so no absence rule
        // applies to it: a segment without it leaves the predicate to report
        // how far it ran.
        let fields = scan
            .fields
            .iter()
            .cloned()
            .chain(
                dropped
                    .iter()
                    .copied()
                    .map(ProjectedField::by_id)
                    .filter(|field| field.type_tag().is_some()),
            )
            .collect();
        let mut loose: Vec<u16> = scan
            .fields
            .iter()
            .filter(|f| f.type_tag().is_none())
            .map(ProjectedField::column_id)
            .collect();
        let loose_predicate = scan.predicate.as_ref().map(|p| p.column_id).filter(|&id| {
            ProjectedField::by_id(id).type_tag().is_none()
                && scan
                    .fields
                    .iter()
                    .all(|f| f.column_id() != id || f.type_tag().is_none())
        });
        if let Some(id) = loose_predicate
            && !loose.contains(&id)
        {
            loose.push(id);
        }
        // The sources share the scan's budget: each holds its current batch
        // and what its cursor read ahead within an equal part of it.
        let share = scan.budget / (segments.len() as u64).max(1);
        let sources = segments
            .iter()
            .map(|seg| {
                // No predicate pushed down: it runs after the dedup, so every
                // version (including a newest one that fails the predicate but
                // shadows an older matching version) has to be seen, and the
                // zone-map skip it would drive is unsafe for the same reason.
                let SegmentCursor {
                    mut cursor,
                    whole,
                    ids,
                } = scan.segment_cursor(seg, &augmented, None, share)?;
                let columns = scan.late_payload(seg);
                let late = if columns.is_empty() {
                    LatePayload::default()
                } else {
                    let lean_ids: Vec<u16> = ids
                        .iter()
                        .copied()
                        .filter(|id| columns.iter().all(|&(stored, _)| stored != *id))
                        .collect();
                    // Set before the cursor reads anything.
                    cursor.set_projection(&lean_ids);
                    LatePayload {
                        columns,
                        eager_ids: ids,
                        lean_ids,
                        ..LatePayload::default()
                    }
                };
                Ok(MergeSource {
                    cursor,
                    whole,
                    late,
                    batch: None,
                    types: FieldTypes::AsDeclared,
                    row: 0,
                    key_col: 0,
                    seqno_col: 0,
                    key: 0..0,
                    eff: 0,
                    global: seg.global,
                    all_visible: seg.visibility == crate::table::SeqnoVisibility::All,
                    threshold: scan.seqno.saturating_sub(seg.global),
                    rank: seg.recency_rank,
                    referenced: false,
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let late = if scan.cells.is_some() {
            Late::Cells
        } else if segments.iter().any(|seg| scan.reads_late(seg)) {
            Late::Values
        } else {
            Late::No
        };
        Ok(Self {
            sources,
            dropped,
            fields,
            loose,
            loose_predicate,
            late,
            raw: if scan.raw_value_read() {
                RawValue::Kept
            } else {
                RawValue::Moved
            },
            predicate_column: scan.predicate.as_ref().map(|p| p.column_id),
            deletes,
            rts: TombstoneSweep::new(rts, scan.comparator.as_ref()),
            last_key: None,
            pending: Vec::new(),
            heap: Vec::with_capacity(segments.len()),
            started: false,
            parked: None,
        })
    }

    /// The next output batch, or `None` once the group is merged.
    pub(super) fn next_batch(
        &mut self,
        scan: &ColumnarScan,
        support: &mut PredicateSupport,
    ) -> crate::Result<Option<ColumnBatch>> {
        let cmp = scan.comparator.clone();
        let cmp = cmp.as_ref();
        if !self.started {
            self.started = true;
            for i in 0..self.sources.len() {
                self.enter(i, scan, cmp)?;
            }
        }
        // A source whose spent batch held chosen rows moves on only now that
        // the output that took them is out.
        if let Some(i) = self.parked.take() {
            self.enter(i, scan, cmp)?;
        }
        loop {
            let Some(&winner) = self.heap.first() else {
                // Every source is exhausted: what was chosen goes out.
                return self.gather(scan, support);
            };
            let source = self.sources.get(winner).ok_or(NO_SOURCE)?;
            let row = source.row;
            let (key, eff) = current(source)?;
            // A source not moved since its key was decided still sits at a row
            // of it, and that key is the least left, so the row surfaces here:
            // an older version, shadowed.
            let shadowed = self
                .last_key
                .as_deref()
                .is_some_and(|last| cmp.compare(last, key).is_eq());
            let keep = if shadowed {
                false
            } else {
                let keep = decide(scan, source, self.deletes, &mut self.rts, key, eff)?;
                let last = self.last_key.get_or_insert_with(Vec::new);
                last.clear();
                last.extend_from_slice(key);
                keep
            };
            let source = self.sources.get_mut(winner).ok_or(NO_SOURCE)?;
            source.row += 1;
            if keep {
                source.referenced = true;
                self.pending.push(Pick {
                    source: winner,
                    row,
                    eff,
                });
            }
            match self.position(winner, scan, cmp)? {
                Position::At => sift_down(&mut self.heap, 0, &self.sources, cmp)?,
                Position::Exhausted => self.leave_top(cmp)?,
                Position::MustGather => {
                    // The least row across the others cannot be decided
                    // without this source's next rows, and its batch holds
                    // chosen ones: those go out first.
                    self.leave_top(cmp)?;
                    self.parked = Some(winner);
                    let out = self.gather(scan, support)?;
                    if out.is_some() {
                        return Ok(out);
                    }
                    self.parked = None;
                    self.enter(winner, scan, cmp)?;
                }
            }
            if self.pending.len() >= TARGET_ROWS
                && let Some(out) = self.gather(scan, support)?
            {
                return Ok(Some(out));
            }
        }
    }

    /// Positions source `i` and puts it on the heap when it has a row.
    fn enter(
        &mut self,
        i: usize,
        scan: &ColumnarScan,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<()> {
        match self.position(i, scan, cmp)? {
            Position::At => {
                // The push leaves the new source at the end.
                let at = self.heap.len();
                self.heap.push(i);
                sift_up(&mut self.heap, at, &self.sources, cmp)
            }
            Position::Exhausted => Ok(()),
            // A source enters at the start or after the output that took its
            // chosen rows, so none of its rows is chosen.
            Position::MustGather => Err(Error::InvalidHeader(
                "columnar_scan: a merge source entered holding chosen rows",
            )),
        }
    }

    /// Takes the top source off the heap.
    fn leave_top(&mut self, cmp: &dyn crate::comparator::UserComparator) -> crate::Result<()> {
        self.heap.swap_remove(0);
        sift_down(&mut self.heap, 0, &self.sources, cmp)
    }

    /// Page bytes the sources hold now: each one's current batch and what its
    /// cursor has read ahead.
    pub(super) fn held_bytes(&self) -> u64 {
        self.sources.iter().map(MergeSource::held_bytes).sum()
    }

    /// Reads past a share the sources made since this was last called.
    pub(super) fn take_oversized(&mut self) -> u64 {
        self.sources
            .iter_mut()
            .map(|s| s.cursor.take_oversized())
            .sum()
    }

    /// Moves source `i` to its next visible row of a key not yet decided,
    /// loading its next batch when the current one is spent, and records what
    /// the merge holds after a load.
    fn position(
        &mut self,
        i: usize,
        scan: &ColumnarScan,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Position> {
        let mut incidental = 0u64;
        let positioned = self.position_source(i, cmp, &mut incidental);
        // Recorded whether or not the move then failed: the batch it let go
        // of was read all the same.
        scan.record_payload(0, incidental);
        let (position, peak) = positioned?;
        if let Some(peak) = peak {
            // The other sources did not move while this one loaded, so the
            // most the merge held is theirs plus this source's peak.
            let own = self.sources.get(i).map_or(0, MergeSource::held_bytes);
            scan.observe_payload(self.held_bytes() - own + peak);
        }
        Ok(position)
    }

    /// [`Self::position`] for source `i` alone, and the most it held after a
    /// load, when it loaded: a batch it read and then passed over whole, all
    /// its rows invisible or shadowed, was held all the same. Adds to
    /// `incidental` what the payload read late of a batch it let go of held
    /// besides the chosen rows' cells.
    fn position_source(
        &mut self,
        i: usize,
        cmp: &dyn crate::comparator::UserComparator,
        incidental: &mut u64,
    ) -> crate::Result<(Position, Option<u64>)> {
        let last_key = self.last_key.as_deref();
        let (fields, late, raw, predicate_column) =
            (&self.fields, self.late, self.raw, self.predicate_column);
        let Some(source) = self.sources.get_mut(i) else {
            return Ok((Position::Exhausted, None));
        };
        let mut loaded: Option<u64> = None;
        loop {
            if let Some(batch) = &source.batch {
                let keys = batch.columns.get(source.key_col).ok_or(MISSING_COLUMN)?;
                let seqnos = batch.columns.get(source.seqno_col).ok_or(MISSING_COLUMN)?;
                while source.row < batch.row_count {
                    let local = fixed_u64_row(&seqnos.data, source.row)?;
                    if source.all_visible || local < source.threshold {
                        let span = bytes_column_span(&keys.data, batch.row_count, source.row)?;
                        let shadowed = last_key.is_some_and(|last| {
                            keys.data
                                .get(span.clone())
                                .is_some_and(|key| cmp.compare(last, key).is_eq())
                        });
                        if !shadowed {
                            source.eff =
                                local
                                    .checked_add(source.global)
                                    .ok_or(Error::InvalidHeader(
                                        "columnar_scan: effective seqno overflow",
                                    ))?;
                            source.key = span;
                            return Ok((Position::At, loaded));
                        }
                    }
                    source.row += 1;
                }
                if source.referenced {
                    return Ok((Position::MustGather, loaded));
                }
            }
            // The spent batch goes before the cursor reads on, so the source
            // never holds it and a new run at once. How dense its choices were
            // decides how the next row group's payload is read.
            if source.batch.take().is_some() {
                *incidental += source.late.incidental();
                if let Some(ids) = source.late.spent() {
                    source.cursor.set_projection(ids);
                }
            }
            match source.cursor.next_located() {
                None => return Ok((Position::Exhausted, loaded)),
                Some(located) => {
                    let (batch, at) = located?;
                    // Read without its payload when the cursor decoded none
                    // of it: a row group read before a switch to dense reads
                    // still comes as it was read.
                    source.late.batch_late = at.is_some()
                        && !source.late.columns.is_empty()
                        && source.late.columns.iter().all(|&(stored, _)| {
                            batch.columns.iter().all(|c| c.column_id != stored)
                        });
                    source.late.batch_at = at;
                    // A whole value moves aside before the conform, where a
                    // declared field may share the value column's id.
                    let batch = carry_whole_value(batch, source.whole, raw);
                    // Its rows are not decided yet: a shadowed or deleted
                    // one must not fail the scan, and the predicate after
                    // the dedup sees the declared defaults.
                    let (batch, mistyped) = conform_lenient(batch, fields, predicate_column)?;
                    // Every source then carries the whole value last, null
                    // where it splits its values, so the sources agree.
                    let batch = if late == Late::No {
                        batch
                    } else {
                        last_whole_value(batch, late == Late::Cells)?
                    };
                    // Keys are read row by row from their framing, which only
                    // a bytes column carries.
                    if batch
                        .columns
                        .iter()
                        .any(|c| c.column_id == COL_USER_KEY && c.type_tag != TypeTag::Bytes)
                    {
                        return Err(Error::InvalidHeader(
                            "columnar_scan: key column is not a bytes column",
                        ));
                    }
                    // Checked once here, so the rows chosen from it are
                    // gathered without a check per cell.
                    for column in &batch.columns {
                        column.validate(batch.row_count)?;
                    }
                    let place = |id| {
                        batch
                            .columns
                            .iter()
                            .position(|c| c.column_id == id)
                            .ok_or(MISSING_COLUMN)
                    };
                    source.key_col = place(COL_USER_KEY)?;
                    source.seqno_col = place(COL_SEQNO)?;
                    source.batch = Some(batch);
                    source.types = if mistyped.is_empty() {
                        FieldTypes::AsDeclared
                    } else if predicate_column.is_some_and(|id| mistyped.contains(&id)) {
                        FieldTypes::MistypedUnderPredicate
                    } else {
                        FieldTypes::Mistyped
                    };
                    source.row = 0;
                    let held = source.held_bytes();
                    loaded = Some(loaded.map_or(held, |peak| peak.max(held)));
                }
            }
        }
    }

    /// Gathers the chosen rows into one output batch, in the order they were
    /// chosen, with each row's effective seqno and the predicate applied;
    /// `None` when none of them survives.
    fn gather(
        &mut self,
        scan: &ColumnarScan,
        support: &mut PredicateSupport,
    ) -> crate::Result<Option<ColumnBatch>> {
        let pending = core::mem::take(&mut self.pending);
        let built = if pending.is_empty() {
            None
        } else {
            self.judge_and_build(scan, pending, support)?
        };
        for source in &mut self.sources {
            source.referenced = false;
        }
        let Some(Built {
            merged,
            pending,
            judged,
            early,
            left_out,
        }) = built
        else {
            return Ok(None);
        };
        scan.record_gather(&merged);
        // The rows are decided: their operands are resolved and their fields
        // read out of their values now, and read as declared before the
        // predicate sees them.
        let (merged, resolved) = if self.late == Late::No {
            (merged, Resolved::default())
        } else {
            let (merged, resolved) = scan.read_late(merged, early)?;
            // Every declared column now holds its declared type: read out of
            // a value, or conformed when its source was loaded.
            let (merged, mistyped) = conform_lenient(merged, &scan.fields, self.predicate_column)?;
            if !mistyped.is_empty() {
                return Err(MISTYPED);
            }
            (merged, resolved)
        };
        // A row whose predicate column its batch stores under another type
        // cannot be judged, unless its operand was resolved and the column
        // read out of the merged value: it is not filtered out, so it fails
        // the scan as a returned row of its batch would.
        if pending
            .iter()
            .any(|pick| self.source_types(pick) == Some(FieldTypes::MistypedUnderPredicate))
        {
            let returned = self.returned_picks(&merged, &pending)?;
            let keys = merged
                .columns
                .iter()
                .find(|c| c.column_id == COL_USER_KEY)
                .ok_or(MISSING_COLUMN)?;
            for (row, pick) in (0..merged.row_count).zip(&returned) {
                if self.source_types(pick) == Some(FieldTypes::MistypedUnderPredicate)
                    && !resolved.holds(bytes_column_row(&keys.data, merged.row_count, row)?)
                {
                    return Err(MISTYPED);
                }
            }
        }
        // An operand resolved to a value is read whole, so its cell in a
        // column no field declares is the operand's own: a loose predicate
        // cannot judge it.
        let judged = judged && !(resolved.any() && self.loose_predicate.is_some());
        let mut merged = if early {
            merged
        } else if judged {
            scan.filter_after_dedup(merged, scan.predicate.as_ref(), support)?
        } else {
            *support = (*support).min(PredicateSupport::Unsupported);
            merged
        };
        if merged.row_count == 0 {
            return Ok(None);
        }
        // Only a returned row is held to what its fields can be read as: a
        // row a field projected by id cannot be read for fails the scan, and
        // so does one from a batch that stores a projected field under another
        // type, unless its operand was resolved and its fields read out of the
        // merged value instead. A shadowed or filtered row did not.
        if pending.iter().any(|pick| self.taken_from_mistyped(pick)) || resolved.any() {
            let returned = self.returned_picks(&merged, &pending)?;
            let keys = merged
                .columns
                .iter()
                .find(|c| c.column_id == COL_USER_KEY)
                .ok_or(MISSING_COLUMN)?;
            for (row, pick) in (0..merged.row_count).zip(&returned) {
                let key = bytes_column_row(&keys.data, merged.row_count, row)?;
                if resolved.unreadable(key) {
                    return Err(UNREADABLE_BY_ID);
                }
                if self.taken_from_mistyped(pick) && !resolved.holds(key) {
                    return Err(MISTYPED);
                }
            }
        }
        // A projected column left out because some chosen row lacked it is
        // brought back for the rows the predicate kept, when they all have it.
        if !left_out.is_empty() {
            self.fill_left_out(&mut merged, &pending, &left_out, &scan.fields)?;
        }
        // Match the singleton contract: yield exactly the projected columns.
        drop_columns(&mut merged, &self.dropped);
        // The rows returned are decided: each is held to the declarations.
        let merged = conform(merged, &scan.fields)?;
        Ok(Some(merged))
    }

    /// The chosen row of `pending` each row of `merged`, the rows returned out
    /// of them, was taken from.
    fn returned_picks<'p>(
        &self,
        merged: &ColumnBatch,
        pending: &'p [Pick],
    ) -> crate::Result<Vec<&'p Pick>> {
        let keys = merged
            .columns
            .iter()
            .find(|c| c.column_id == COL_USER_KEY)
            .ok_or(MISSING_COLUMN)?;
        // The returned rows keep the order they were chosen in and their keys
        // are distinct, so each is the next chosen row with its key.
        let mut returned: Vec<&Pick> = Vec::with_capacity(merged.row_count as usize);
        let mut picks = pending.iter();
        for row in 0..merged.row_count {
            let key = bytes_column_row(&keys.data, merged.row_count, row)?;
            let pick = picks
                .find(|pick| {
                    self.sources
                        .get(pick.source)
                        .and_then(|s| {
                            let batch = s.batch.as_ref()?;
                            let column = batch.columns.get(s.key_col)?;
                            bytes_column_row(&column.data, batch.row_count, pick.row).ok()
                        })
                        .is_some_and(|chosen| chosen == key)
                })
                .ok_or(Error::InvalidHeader(
                    "columnar_scan: a returned row is not among the rows chosen",
                ))?;
            returned.push(pick);
        }
        Ok(returned)
    }

    /// Whether `pick` was taken from a batch that stores a projected field
    /// under another type than declared.
    fn taken_from_mistyped(&self, pick: &Pick) -> bool {
        self.source_types(pick)
            .is_some_and(|types| types != FieldTypes::AsDeclared)
    }

    /// How the batch `pick` was taken from stores the projected fields.
    fn source_types(&self, pick: &Pick) -> Option<FieldTypes> {
        self.sources.get(pick.source).map(|s| s.types)
    }

    /// Adds to `merged`, the rows returned out of the chosen `pending` ones,
    /// each `left_out` column a field of `fields` projects, gathered from the
    /// batches those rows come from, when every one of them carries it under
    /// one type. A column some returned row lacks stays out, and the row
    /// fails the conform that follows.
    fn fill_left_out(
        &self,
        merged: &mut ColumnBatch,
        pending: &[Pick],
        left_out: &[u16],
        fields: &[ProjectedField],
    ) -> crate::Result<()> {
        let returned = self.returned_picks(merged, pending)?;
        let rows = returned.len();
        for &id in left_out {
            if !fields.iter().any(|f| f.column_id() == id) {
                continue;
            }
            // Each returned row's cell in the column, or `None` when its batch
            // lacks the column.
            let mut located: Vec<(&Column, u32, u32)> = Vec::with_capacity(rows);
            for pick in &returned {
                let Some(batch) = self.sources.get(pick.source).and_then(|s| s.batch.as_ref())
                else {
                    break;
                };
                let Some(column) = batch.columns.iter().find(|c| c.column_id == id) else {
                    break;
                };
                located.push((column, batch.row_count, pick.row));
            }
            let Some(&(first, _, _)) = located.first() else {
                continue;
            };
            let type_tag = first.type_tag;
            if located.len() != rows || located.iter().any(|(c, _, _)| c.type_tag != type_tag) {
                continue;
            }
            let data = if let Some(width) = type_tag.fixed_width() {
                let width = usize::from(width);
                let mut out = Vec::with_capacity(rows * width);
                for &(column, _, row) in &located {
                    let start = row as usize * width;
                    out.extend_from_slice(
                        column
                            .data
                            .get(start..start + width)
                            .ok_or(MISSING_COLUMN)?,
                    );
                }
                crate::Slice::from(out)
            } else {
                let mut cells: Vec<&[u8]> = Vec::with_capacity(rows);
                for &(column, count, row) in &located {
                    cells.push(bytes_column_row(&column.data, count, row)?);
                }
                frame_bytes_column(rows, || cells.iter().copied())?
            };
            let validity = located
                .iter()
                .any(|(c, _, _)| c.validity.is_some())
                .then(|| {
                    let mut bits = alloc::vec![0u8; rows.div_ceil(8)];
                    for (at, &(column, _, row)) in located.iter().enumerate() {
                        if column.is_valid(row)
                            && let Some(byte) = bits.get_mut(at / 8)
                        {
                            *byte |= 1 << (at % 8);
                        }
                    }
                    bits
                });
            merged.columns.push(Column {
                column_id: id,
                type_tag,
                validity,
                data,
            });
        }
        Ok(())
    }

    /// The chosen rows of `pending` the scan's predicate keeps, gathered into
    /// one batch, or `None` when it keeps none of them.
    ///
    /// The row predicate runs AFTER the dedup: each row is the newest visible
    /// version of its key, so a key whose newest version fails the predicate
    /// is dropped instead of falling back to an older matching version. It
    /// runs in the scan's coordinates, on effective seqnos. Rows that do not
    /// all carry its column are returned unjudged.
    ///
    /// Over a column reading the values does not change, it judges each
    /// chosen row in the batch the row was taken from, before anything is
    /// gathered, so a row it drops is neither copied nor resolved nor handed
    /// to the projector; an operand still to be resolved is judged after its
    /// value is read.
    fn judge_and_build(
        &mut self,
        scan: &ColumnarScan,
        pending: Vec<Pick>,
        support: &mut PredicateSupport,
    ) -> crate::Result<Option<Built>> {
        // Laid out over every chosen row, so the sources' columns are checked
        // whether or not the predicate keeps a row of them.
        let layout = self.layout(&pending)?;
        // Each source's value-type cells, found once for the rows asked
        // about; only a tree that merges has operands to ask about.
        let types: Vec<Option<&[u8]>> = if scan.resolver.is_some() {
            self.sources
                .iter()
                .map(|s| s.batch.as_ref().and_then(value_types))
                .collect()
        } else {
            Vec::new()
        };
        let timing = if layout.judged {
            scan.predicate_timing(|| pending.iter().any(|pick| holds_operand(&types, pick)))
        } else {
            PredicateTiming::AfterValues
        };
        // Whether every row has its final verdict before the values are read,
        // so the predicate is not run over the returned rows again.
        let mut early = timing == PredicateTiming::BeforeValues;
        let pending = match timing {
            PredicateTiming::BeforeValues => self.judge(scan, pending, None, &layout, support)?,
            PredicateTiming::BeforeValuesExceptOperands => {
                self.judge(scan, pending, Some(&types), &layout, support)?
            }
            PredicateTiming::AfterValues if layout.judged => {
                let (pending, settled) = self.prejudge(scan, pending);
                if settled && let Some(pred) = scan.predicate.as_ref() {
                    *support = (*support).min(pred.support(layout.type_of(pred.column_id)));
                    early = true;
                }
                pending
            }
            PredicateTiming::AfterValues => pending,
        };
        if pending.is_empty() {
            return Ok(None);
        }
        // A row page holding a row the predicate kept, where it could judge
        // it yet, is one whose payload is read: what the density is counted
        // on.
        for pick in &pending {
            if let Some(source) = self.sources.get_mut(pick.source) {
                source.late.batch_picks += 1;
            }
        }
        // The rows are chosen, and judged where the predicate could judge
        // them yet: only now is their payload read, from their row pages.
        self.fill_late(scan, &pending)?;
        scan.record_payload(self.late_useful(&pending)?, 0);
        // The columns the merge decoded for itself leave the output once its
        // rows are decided. Read again after the gather only to read the
        // values, to bring back a column left out, to check a row taken from
        // a batch of another type, or by a predicate judging after the
        // values; otherwise they are not gathered at all.
        // A source holding chosen rows stands for its rows here: one check per
        // source, not per row.
        let reread = self.late != Late::No
            || !layout.left_out.is_empty()
            || (!early && scan.predicate.is_some())
            || self
                .sources
                .iter()
                .any(|s| s.referenced && s.types != FieldTypes::AsDeclared);
        let skip: &[u16] = if reread { &[] } else { &self.dropped };
        let merged = self.build(&pending, &layout, skip)?;
        Ok(Some(Built {
            merged,
            pending,
            judged: layout.judged,
            early,
            left_out: layout.left_out,
        }))
    }

    /// The chosen rows of `pending` a predicate judged after the values could
    /// already be dropped by, before any payload is read: a row whose batch
    /// stores the predicate's field as a column of its declared type, with a
    /// cell for the row, holds there the value its field reads as, so the
    /// predicate's verdict on that cell is final. Every other row is kept,
    /// for the predicate to judge once its value is read. Also returns
    /// whether every row got its final verdict here, in which case the
    /// predicate has nothing left to judge after the values.
    fn prejudge(&self, scan: &ColumnarScan, pending: Vec<Pick>) -> (Vec<Pick>, bool) {
        let Some(pred) = scan
            .predicate
            .as_ref()
            .filter(|p| p.apply == PredicateApply::Filter)
        else {
            return (pending, false);
        };
        // An operand reads as the value it resolves to, not as its own cells.
        // Otherwise the rows are judged here whether or not their payload is
        // read late: what survives is also what the density is counted on,
        // and a row dropped here is not gathered either.
        if scan.resolver.is_some() {
            return (pending, false);
        }
        let matchers: Vec<Option<(RowMatcher<'_>, &Column)>> = self
            .sources
            .iter()
            .map(|s| {
                if s.whole || !s.referenced || s.types != FieldTypes::AsDeclared {
                    return None;
                }
                let batch = s.batch.as_ref()?;
                let column = batch
                    .columns
                    .iter()
                    .find(|c| c.column_id == pred.column_id)?;
                Some((pred.matcher(batch), column))
            })
            .collect();
        let mut pending = pending;
        let mut settled = true;
        pending.retain(
            |pick| match matchers.get(pick.source).and_then(Option::as_ref) {
                Some((matcher, column)) if column.is_valid(pick.row) => matcher.matches(pick.row),
                _ => {
                    settled = false;
                    true
                }
            },
        );
        (pending, settled)
    }

    /// The bytes of the cells the rows of `pending` take from payload read
    /// late for them, each added to its batch's share of what its pages held.
    fn late_useful(&mut self, pending: &[Pick]) -> crate::Result<u64> {
        let mut useful = 0u64;
        for pick in pending {
            let Some(source) = self.sources.get_mut(pick.source) else {
                continue;
            };
            let MergeSource { batch, late, .. } = source;
            let Some(batch) = batch.as_ref().filter(|_| late.page_bytes > 0) else {
                continue;
            };
            let mut taken = 0u64;
            for &(_, carried) in &late.columns {
                let Some(column) = batch
                    .columns
                    .iter()
                    .find(|c| c.column_id == carried && c.is_valid(pick.row))
                else {
                    continue;
                };
                taken += match column.type_tag.fixed_width() {
                    Some(width) => u64::from(width),
                    None => bytes_column_row(&column.data, batch.row_count, pick.row)?.len() as u64,
                };
            }
            late.page_useful += taken;
            useful += taken;
        }
        Ok(useful)
    }

    /// Reads the payload of each batch a row of `pending` was chosen from and
    /// that was read without it, from that batch's row page alone, into the
    /// columns that stood in for it. A column the row group does not store
    /// stays as it stood, absent; one stored under another type than the
    /// field declares stays too, and the rows of the batch returned fail the
    /// scan as any row of a mistyped batch does.
    fn fill_late(&mut self, scan: &ColumnarScan, pending: &[Pick]) -> crate::Result<()> {
        let mut copied = 0usize;
        let result = (|| {
            for (index, source) in self.sources.iter_mut().enumerate() {
                let MergeSource {
                    cursor,
                    batch,
                    late,
                    types,
                    ..
                } = source;
                if !late.batch_late || !pending.iter().any(|pick| pick.source == index) {
                    continue;
                }
                let (Some(at), Some(table), Some(batch)) =
                    (late.batch_at.as_ref(), cursor.table(), batch.as_mut())
                else {
                    continue;
                };
                let stored: Vec<u16> = late.columns.iter().map(|&(id, _)| id).collect();
                for column in table.columns_at(at, &stored, &mut copied)? {
                    let Some(&(_, carried)) =
                        late.columns.iter().find(|&&(id, _)| id == column.column_id)
                    else {
                        continue;
                    };
                    column.validate(batch.row_count)?;
                    let Some(slot) = batch.columns.iter_mut().find(|c| c.column_id == carried)
                    else {
                        continue;
                    };
                    if slot.type_tag != column.type_tag {
                        if *types == FieldTypes::AsDeclared {
                            *types = FieldTypes::Mistyped;
                        }
                        continue;
                    }
                    // What the page holds, its chosen rows' cells counted out
                    // of it as they are taken.
                    late.page_bytes += column.data.len() as u64;
                    *slot = Column {
                        column_id: carried,
                        ..column
                    };
                }
                late.batch_late = false;
            }
            Ok(())
        })();
        scan.record_copied(copied);
        result
    }

    /// The chosen rows of `pending` the scan's filtering predicate keeps,
    /// judged in the batches they were taken from, and every operand among
    /// them when `operands` (each source's value-type cells) leaves those to
    /// the judgement after their values are read. How far it ran is counted
    /// when it judges every row.
    fn judge(
        &self,
        scan: &ColumnarScan,
        pending: Vec<Pick>,
        operands: Option<&[Option<&[u8]>]>,
        layout: &Layout,
        support: &mut PredicateSupport,
    ) -> crate::Result<Vec<Pick>> {
        let Some(pred) = scan.predicate.as_ref() else {
            return Ok(pending);
        };
        if operands.is_none() {
            *support = (*support).min(pred.support(layout.type_of(pred.column_id)));
        }
        if pred.apply != PredicateApply::Filter {
            return Ok(pending);
        }
        // Each batch a row is taken from, bound to the predicate once.
        let matchers: Vec<Option<RowMatcher<'_>>> = self
            .sources
            .iter()
            .map(|s| {
                s.batch
                    .as_ref()
                    .filter(|_| s.referenced)
                    .map(|b| pred.matcher(b))
            })
            .collect();
        let seqno = pred.column_id == COL_SEQNO;
        let mut kept = Vec::with_capacity(pending.len());
        for pick in pending {
            let matcher = matchers
                .get(pick.source)
                .and_then(Option::as_ref)
                .ok_or(NOT_KEPT)?;
            // A row is returned with its effective seqno, the one judged.
            let keep = if seqno {
                matcher.matches_cell(pick.row, &pick.eff.to_le_bytes())
            } else {
                matcher.matches(pick.row)
            };
            if keep || operands.is_some_and(|types| holds_operand(types, &pick)) {
                kept.push(pick);
            }
        }
        Ok(kept)
    }

    /// The columns the chosen rows of `pending` are gathered into, checked
    /// against every batch they were taken from.
    ///
    /// A loose column the chosen rows do not all carry under one type is left
    /// out, for [`Self::fill_left_out`] to bring back for the rows returned;
    /// the predicate cannot judge the rows when its loose column is left out.
    fn layout(&self, pending: &[Pick]) -> crate::Result<Layout> {
        let batch_of = |pick: &Pick| self.sources.get(pick.source).and_then(|s| s.batch.as_ref());
        let template = pending.first().and_then(batch_of).ok_or(NOT_KEPT)?;
        let type_of = |batch: &ColumnBatch, id: u16| {
            batch
                .columns
                .iter()
                .find(|c| c.column_id == id)
                .map(|c| c.type_tag)
        };
        let mut left_out: Vec<u16> = Vec::new();
        for &id in &self.loose {
            let expected = type_of(template, id);
            for source in self.sources.iter().filter(|s| s.referenced) {
                let batch = source.batch.as_ref().ok_or(NOT_KEPT)?;
                if type_of(batch, id) != expected {
                    left_out.push(id);
                    break;
                }
            }
        }
        let judged = self
            .loose_predicate
            .is_none_or(|id| !left_out.contains(&id));
        let uniform = left_out.is_empty();
        let kept = |id: u16| !left_out.contains(&id);
        let heads: Vec<(u16, TypeTag)> = template
            .columns
            .iter()
            .filter(|c| kept(c.column_id))
            .map(|c| (c.column_id, c.type_tag))
            .collect();
        // Every batch a row is taken from carries the kept columns in the
        // template's order. Where each batch keeps them all, one column index
        // names the same column in each; otherwise each source's places of
        // the kept columns are found once.
        let mut places: Option<Vec<Vec<usize>>> =
            (!uniform).then(|| alloc::vec![Vec::new(); self.sources.len()]);
        for (i, source) in self
            .sources
            .iter()
            .enumerate()
            .filter(|(_, s)| s.referenced)
        {
            let batch = source.batch.as_ref().ok_or(NOT_KEPT)?;
            let agree = if let Some(places) = places.as_mut() {
                let at: Vec<usize> = batch
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| kept(c.column_id))
                    .map(|(at, _)| at)
                    .collect();
                let agree = at.len() == heads.len()
                    && at.iter().zip(&heads).all(|(&at, &(id, type_tag))| {
                        batch
                            .columns
                            .get(at)
                            .is_some_and(|c| c.column_id == id && c.type_tag == type_tag)
                    });
                if let Some(slot) = places.get_mut(i) {
                    *slot = at;
                }
                agree
            } else {
                batch.columns.len() == heads.len()
                    && batch
                        .columns
                        .iter()
                        .zip(&heads)
                        .all(|(c, &(id, type_tag))| c.column_id == id && c.type_tag == type_tag)
            };
            if !agree {
                return Err(Error::InvalidHeader(
                    "columnar_scan: merged segments disagree on their columns",
                ));
            }
        }
        Ok(Layout {
            heads,
            places,
            left_out,
            judged,
        })
    }

    /// The chosen rows as one batch laid out as `layout` says, without the
    /// `skip` columns, in the order they were chosen, each column built
    /// straight from the batches its rows sit in: nothing is copied but the
    /// chosen cells. The seqno column is written with each row's effective
    /// seqno, since the rows come from segments with different bases.
    fn build(&self, pending: &[Pick], layout: &Layout, skip: &[u16]) -> crate::Result<ColumnBatch> {
        use crate::table::columnar::{Column, frame_bytes_column, gather_fixed_column};

        let Layout { heads, places, .. } = layout;
        let batch_of = |pick: &Pick| self.sources.get(pick.source).and_then(|s| s.batch.as_ref());
        let count = pending.len();
        // A pick addresses a row of one of at most `TARGET_ROWS` rows chosen.
        let row_count = u32::try_from(count).map_err(|_| {
            Error::InvalidHeader("columnar_scan: merged batch row count exceeds u32")
        })?;
        // Each source batch was validated when it was loaded, so every cell a
        // pick names is there: the fallbacks below are never taken.
        let cell = |pick: &Pick, index: usize| {
            let at = match &places {
                None => index,
                Some(places) => *places.get(pick.source)?.get(index)?,
            };
            batch_of(pick).and_then(|b| Some((b.columns.get(at)?, b.row_count)))
        };
        let mut columns = Vec::with_capacity(heads.len());
        for (index, &(column_id, type_tag)) in heads.iter().enumerate() {
            if skip.contains(&column_id) {
                continue;
            }
            let data = if column_id == COL_SEQNO {
                let mut out = Vec::with_capacity(count * 8);
                for pick in pending {
                    out.extend_from_slice(&pick.eff.to_le_bytes());
                }
                crate::Slice::from(out)
            } else if let Some(width) = type_tag.fixed_width() {
                let width = usize::from(width);
                gather_fixed_column(
                    width,
                    count,
                    pending.iter().map(|pick| {
                        let (column, _) = cell(pick, index)?;
                        let start = pick.row as usize * width;
                        column.data.get(start..start + width)
                    }),
                )
            } else {
                frame_bytes_column(count, || {
                    pending.iter().map(move |pick| {
                        cell(pick, index)
                            .and_then(|(column, rows)| {
                                bytes_column_row(&column.data, rows, pick.row).ok()
                            })
                            .unwrap_or_default()
                    })
                })?
            };
            let validity = pending
                .iter()
                .any(|pick| cell(pick, index).is_some_and(|(c, _)| c.validity.is_some()))
                .then(|| {
                    let mut bits = alloc::vec![0u8; count.div_ceil(8)];
                    for (at, pick) in pending.iter().enumerate() {
                        let present = cell(pick, index).is_some_and(|(c, _)| {
                            c.validity.as_deref().is_none_or(|v| {
                                v.get(pick.row as usize / 8)
                                    .is_some_and(|byte| byte >> (pick.row % 8) & 1 == 1)
                            })
                        });
                        if present && let Some(byte) = bits.get_mut(at / 8) {
                            *byte |= 1 << (at % 8);
                        }
                    }
                    bits
                });
            columns.push(Column {
                column_id,
                type_tag,
                validity,
                data,
            });
        }
        Ok(ColumnBatch { row_count, columns })
    }
}

/// Whether `pick` is a merge operand, per `types`, each source's value-type
/// cells.
fn holds_operand(types: &[Option<&[u8]>], pick: &Pick) -> bool {
    types
        .get(pick.source)
        .copied()
        .flatten()
        .and_then(|cells| cells.get(pick.row as usize))
        .is_some_and(|&byte| is_operand(byte))
}

/// A chosen row's batch is gone before the output taking it was gathered.
const NOT_KEPT: Error = Error::InvalidHeader("columnar_scan: a chosen row's batch was not kept");

/// The columns an output batch of the merge is gathered into.
struct Layout {
    /// Each column's id and type, in the order the output carries them.
    heads: Vec<(u16, TypeTag)>,
    /// Where each source keeps the columns of `heads`, when a column left
    /// out puts them at different places in different sources; `None` when
    /// every source keeps them at the same places.
    places: Option<Vec<Vec<usize>>>,
    /// The loose columns left out (see [`MergeStream::fill_left_out`]).
    left_out: Vec<u16>,
    /// Whether the predicate can judge the rows: `false` when its loose
    /// column is left out.
    judged: bool,
}

impl Layout {
    /// The type the output carries column `id` under, or `None` when it does
    /// not carry it.
    fn type_of(&self, id: u16) -> Option<TypeTag> {
        self.heads
            .iter()
            .find(|&&(column_id, _)| column_id == id)
            .map(|&(_, type_tag)| type_tag)
    }
}

/// The output batch the chosen rows were gathered into, with what decided it.
struct Built {
    merged: ColumnBatch,
    /// The chosen rows it holds, in its row order.
    pending: Vec<Pick>,
    /// Whether the predicate could judge the rows.
    judged: bool,
    /// Whether the predicate already ran on every row, before their values
    /// were read.
    early: bool,
    /// The loose columns left out of it.
    left_out: Vec<u16>,
}

/// What becomes of a whole-value source's value column when it moves under
/// [`COL_WHOLE_VALUE`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum RawValue {
    /// Nothing reads the raw value: the column is renamed.
    Moved,
    /// The raw value is read too (see [`ColumnarScan::raw_value_read`]): the
    /// column is copied and stays.
    Kept,
}

/// `batch` of a source that carries whole values (`whole`) with its value
/// column under [`COL_WHOLE_VALUE`], as `raw` says.
fn carry_whole_value(mut batch: ColumnBatch, whole: bool, raw: RawValue) -> ColumnBatch {
    if !whole {
        return batch;
    }
    if let Some(at) = batch.columns.iter().position(|c| c.column_id == COL_VALUE) {
        if raw == RawValue::Kept {
            if let Some(value) = batch.columns.get(at).cloned() {
                batch.columns.push(Column {
                    column_id: COL_WHOLE_VALUE,
                    ..value
                });
            }
        } else if let Some(value) = batch.columns.get_mut(at) {
            value.column_id = COL_WHOLE_VALUE;
        }
    }
    batch
}

/// `batch` with its whole-value column last, or with a null one appended when
/// its source splits its values; in a scan of a blob tree (`cells`), the
/// references column of split cell rows just before it, null where the
/// source has none, so every source carries the same columns.
fn last_whole_value(mut batch: ColumnBatch, cells: bool) -> crate::Result<ColumnBatch> {
    let carried: &[u16] = if cells {
        &[
            crate::blob_tree::field_row::CELL_REFS_COLUMN,
            COL_WHOLE_VALUE,
        ]
    } else {
        &[COL_WHOLE_VALUE]
    };
    for &id in carried {
        let at = batch.columns.iter().position(|c| c.column_id == id);
        let column = if let Some(at) = at {
            batch.columns.remove(at)
        } else {
            let rows = batch.row_count as usize;
            Column {
                column_id: id,
                type_tag: TypeTag::Bytes,
                validity: Some(alloc::vec![0u8; rows.div_ceil(8)]),
                data: frame_bytes_column(rows, || core::iter::repeat_n(&[][..], rows))?,
            }
        };
        batch.columns.push(column);
    }
    Ok(batch)
}

/// A merge source index that names no source.
const NO_SOURCE: Error = Error::InvalidHeader("columnar_scan: merge source out of range");

/// Whether source `a`'s current row is taken before source `b`'s: by user key
/// ascending, effective seqno descending, recency ascending, the order the
/// read path resolves versions in.
fn before(
    sources: &[MergeSource],
    a: usize,
    b: usize,
    cmp: &dyn crate::comparator::UserComparator,
) -> crate::Result<bool> {
    let (sa, sb) = (
        sources.get(a).ok_or(NO_SOURCE)?,
        sources.get(b).ok_or(NO_SOURCE)?,
    );
    let ((ka, ea), (kb, eb)) = (current(sa)?, current(sb)?);
    Ok(cmp
        .compare(ka, kb)
        .then_with(|| eb.cmp(&ea))
        .then_with(|| sa.rank.cmp(&sb.rank))
        .is_lt())
}

/// Restores the heap order above `at`, where a source was just pushed.
fn sift_up(
    heap: &mut [usize],
    mut at: usize,
    sources: &[MergeSource],
    cmp: &dyn crate::comparator::UserComparator,
) -> crate::Result<()> {
    while at > 0 {
        let parent = (at - 1) / 2;
        let (&child, &up) = (
            heap.get(at).ok_or(NO_SOURCE)?,
            heap.get(parent).ok_or(NO_SOURCE)?,
        );
        if !before(sources, child, up, cmp)? {
            break;
        }
        heap.swap(at, parent);
        at = parent;
    }
    Ok(())
}

/// Restores the heap order below `at` after its source moved on.
fn sift_down(
    heap: &mut [usize],
    mut at: usize,
    sources: &[MergeSource],
    cmp: &dyn crate::comparator::UserComparator,
) -> crate::Result<()> {
    loop {
        let left = 2 * at + 1;
        let Some(&l) = heap.get(left) else {
            return Ok(());
        };
        let (mut child, mut least) = (left, l);
        if let Some(&r) = heap.get(left + 1)
            && before(sources, r, l, cmp)?
        {
            (child, least) = (left + 1, r);
        }
        let &parent = heap.get(at).ok_or(NO_SOURCE)?;
        if !before(sources, least, parent, cmp)? {
            return Ok(());
        }
        heap.swap(at, child);
        at = child;
    }
}

/// A positioned source's current row: its user key and effective seqno, as
/// positioning found them.
fn current(source: &MergeSource) -> crate::Result<(&[u8], SeqNo)> {
    let key = source
        .batch
        .as_ref()
        .and_then(|b| b.columns.get(source.key_col))
        .and_then(|c| c.data.get(source.key.clone()))
        .ok_or(Error::InvalidHeader(
            "columnar_scan: a positioned merge source has no row",
        ))?;
    Ok((key, source.eff))
}

/// A batch the merge loaded lacks a column the merge decodes for itself.
const MISSING_COLUMN: Error = Error::InvalidHeader(
    "columnar_scan: a merged group batch is missing a column the merge decoded",
);

/// Whether the newest visible version of `key` (the current row of `source`,
/// at effective seqno `eff`) is emitted: not outside the range, not a
/// deletion, not covered by one of the visible range tombstones `rts`.
fn decide(
    scan: &ColumnarScan,
    source: &MergeSource,
    deletes: bool,
    rts: &mut TombstoneSweep,
    key: &[u8],
    eff: SeqNo,
) -> crate::Result<bool> {
    let cmp = scan.comparator.as_ref();
    if !scan.range_is_full() && !key_in_bounds(key, &scan.lo, &scan.hi, cmp) {
        return Ok(false);
    }
    if deletes {
        let batch = source.batch.as_ref().ok_or(Error::InvalidHeader(
            "columnar_scan: a positioned merge source has no batch",
        ))?;
        let byte = *column(batch, COL_VALUE_TYPE)?
            .get(source.row as usize)
            .ok_or(Error::InvalidHeader(
                "columnar_scan: value-type column shorter than the row count",
            ))?;
        let value_type = crate::ValueType::try_from(byte)
            .map_err(|()| Error::InvalidTag(("ValueType", byte)))?;
        // The newest version deletes the key, so the key yields nothing.
        if value_type.is_tombstone() {
            return Ok(false);
        }
    }
    // A visible range tombstone covering the newest visible version deletes
    // the key (older versions are older still); an uncovered newest version
    // shadows the covered older ones.
    Ok(rts.is_empty() || !rts.covers(key, eff, cmp))
}

/// The data of `batch`'s column `column_id`.
fn column(batch: &ColumnBatch, column_id: u16) -> crate::Result<&[u8]> {
    batch
        .columns
        .iter()
        .find(|c| c.column_id == column_id)
        .map(|c| &*c.data)
        .ok_or(MISSING_COLUMN)
}
