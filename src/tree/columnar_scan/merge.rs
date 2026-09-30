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

use super::projection::{ProjectedField, conform, conform_lenient};
use super::rows::SourceCursor;
use super::{ColumnarScan, Segment, SegmentCursor, drop_columns, key_in_bounds};
use crate::table::columnar::{
    COL_SEQNO, COL_USER_KEY, COL_VALUE, COL_VALUE_TYPE, Column, ColumnBatch, TypeTag,
    bytes_column_row, bytes_column_span, fixed_u64_row, frame_bytes_column,
};
use crate::table::columnar_predicate::PredicateSupport;
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
    /// [`ColumnarScan::read_late`]), so every batch carries the whole-value
    /// column.
    late: bool,
    /// Whether a segment of the group records deletions, so the value type is
    /// decoded.
    deletes: bool,
    rts: Vec<(UserKey, UserKey, SeqNo)>,
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
        let deletes = scan.resolver.is_some() || segments.iter().any(Segment::records_deletions);
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
                let SegmentCursor { cursor, whole } =
                    scan.segment_cursor(seg, &augmented, None, share)?;
                Ok(MergeSource {
                    cursor,
                    whole,
                    batch: None,
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
        let late = segments.iter().any(|seg| scan.reads_late(seg));
        Ok(Self {
            sources,
            dropped,
            fields,
            loose,
            loose_predicate,
            late,
            deletes,
            rts,
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
                let keep = decide(scan, source, self.deletes, &self.rts, key, eff)?;
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
        let (position, peak) = self.position_source(i, cmp)?;
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
    /// its rows invisible or shadowed, was held all the same.
    fn position_source(
        &mut self,
        i: usize,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<(Position, Option<u64>)> {
        let last_key = self.last_key.as_deref();
        let (fields, late) = (&self.fields, self.late);
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
            // never holds it and a new run at once.
            source.batch = None;
            match source.cursor.next() {
                None => return Ok((Position::Exhausted, loaded)),
                Some(batch) => {
                    // A whole value moves aside before the conform, where a
                    // declared field may share the value column's id.
                    let batch = carry_whole_value(batch?, source.whole, fields);
                    // Its rows are not decided yet: a shadowed or deleted
                    // one must not fail the scan, and the predicate after
                    // the dedup sees the declared defaults.
                    let batch = conform_lenient(batch, fields)?;
                    // Every source then carries the whole value last, null
                    // where it splits its values, so the sources agree.
                    let batch = if late {
                        last_whole_value(batch)?
                    } else {
                        batch
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
            Some(self.build(&pending)?)
        };
        for source in &mut self.sources {
            source.referenced = false;
        }
        let Some((merged, judged, left_out)) = built else {
            return Ok(None);
        };
        scan.record_gather(&merged);
        // The rows are decided: their operands are resolved and their fields
        // read out of their values now, and read as declared before the
        // predicate sees them.
        let merged = if self.late {
            conform_lenient(scan.read_late(merged)?, &scan.fields)?
        } else {
            merged
        };

        // The row predicate runs AFTER the dedup: each row is the newest
        // visible version of its key, so a key whose newest version fails the
        // predicate is dropped instead of falling back to an older matching
        // version. It runs in the scan's coordinates, on effective seqnos.
        // Rows that do not all carry its column are returned unjudged.
        let mut merged = if judged {
            scan.filter_after_dedup(merged, scan.predicate.as_ref(), support)?
        } else {
            *support = (*support).min(PredicateSupport::Unsupported);
            merged
        };
        if merged.row_count == 0 {
            return Ok(None);
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

    /// The chosen rows as one batch, in the order they were chosen, each column
    /// built straight from the batches its rows sit in: nothing is copied but
    /// the chosen cells. The seqno column is written with each row's effective
    /// seqno, since the rows come from segments with different bases.
    ///
    /// A loose column the chosen rows do not all carry under one type is left
    /// out and named in the third result, for [`Self::fill_left_out`] to bring
    /// back for the rows returned. Also returns whether the predicate can
    /// judge the rows: `false` when its loose column is left out.
    fn build(&self, pending: &[Pick]) -> crate::Result<(ColumnBatch, bool, Vec<u16>)> {
        use crate::table::columnar::{Column, frame_bytes_column, gather_fixed_column};

        const NOT_KEPT: Error =
            Error::InvalidHeader("columnar_scan: a chosen row's batch was not kept");
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
        Ok((ColumnBatch { row_count, columns }, judged, left_out))
    }
}

/// `batch` of a source that carries whole values (`whole`) with its value
/// column under [`COL_WHOLE_VALUE`]: renamed, or copied when a field of
/// `fields` projects the raw value by id and it stays too.
fn carry_whole_value(
    mut batch: ColumnBatch,
    whole: bool,
    fields: &[ProjectedField],
) -> ColumnBatch {
    if !whole {
        return batch;
    }
    let raw_kept = fields
        .iter()
        .any(|f| f.column_id() == COL_VALUE && !super::projection::is_declared(f));
    if let Some(at) = batch.columns.iter().position(|c| c.column_id == COL_VALUE) {
        if raw_kept {
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
/// its source splits its values.
fn last_whole_value(mut batch: ColumnBatch) -> crate::Result<ColumnBatch> {
    let at = batch
        .columns
        .iter()
        .position(|c| c.column_id == COL_WHOLE_VALUE);
    let column = if let Some(at) = at {
        batch.columns.remove(at)
    } else {
        let rows = batch.row_count as usize;
        Column {
            column_id: COL_WHOLE_VALUE,
            type_tag: TypeTag::Bytes,
            validity: Some(alloc::vec![0u8; rows.div_ceil(8)]),
            data: frame_bytes_column(rows, || core::iter::repeat_n(&[][..], rows))?,
        }
    };
    batch.columns.push(column);
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
    rts: &[(UserKey, UserKey, SeqNo)],
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
    Ok(!scan.rt_covered(rts, key, eff))
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
