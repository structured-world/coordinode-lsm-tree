// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A lazy projected scan of one columnar table.
//!
//! The cursor reads one row group per step and yields its row pages before it
//! reads the next, so what a scan holds is bounded by a row group rather than
//! by the table, and a caller that stops early reads nothing more. A key range
//! bounds which row groups it reads: groups wholly below the lower bound are
//! stepped over by the block index alone, and the scan stops after the group
//! that reaches the upper one. Rows of a boundary group that fall outside the
//! range are the caller's to mask.

use core::ops::Bound;

use alloc::collections::VecDeque;
use alloc::vec::Vec;

use crate::table::BlockHandle;
use crate::table::block_index::{BlockIndex, BlockIndexIter, BlockIndexIterImpl};
use crate::table::columnar::ColumnBatch;
use crate::table::columnar_predicate::{ColumnRangePredicate, PredicateSupport};
use crate::table::row_group::{PageWant, RowGroupBlocks, RowPageSelect, RowPageSet};
use crate::table::util::ReadCharge;
use crate::{SeqNo, Table, UserKey};

/// A lazy projected scan of one columnar table.
///
/// Yields one [`ColumnBatch`] per row page of every row group that survives
/// the predicate and the key range, each carrying only the projected columns.
/// Created by [`Table::columnar_scan`].
///
/// [`Table::columnar_scan`]: crate::Table::columnar_scan
pub struct ColumnarCursor {
    table: Table,
    /// The row groups still to read, `None` once the scan is done.
    index: Option<BlockIndexIterImpl>,
    /// The projection plus the predicate's column when the caller did not
    /// project it.
    decode_projection: Vec<u16>,
    /// The predicate's column, decoded only to test it and never handed out.
    added_predicate_column: Option<u16>,
    predicate: Option<ColumnRangePredicate>,
    support: PredicateSupport,
    lo: Bound<UserKey>,
    hi: Bound<UserKey>,
    has_deletes: bool,
    /// Whether the next row group read is the first at or past the table's
    /// restriction bound, the only one that can straddle it.
    first_live_block: bool,
    /// The first global row position of the next row group, for the
    /// positional delete bitmap.
    row_base: u32,
    /// Whether the last row group read took every page, so the next is read
    /// whole in one request.
    expect_whole: bool,
    pending: VecDeque<ColumnBatch>,
    /// The bytes of the batches in `pending`, kept as they come and go.
    pending_bytes: u64,
    /// The page bytes this cursor may hold at once, or `None` for a group at
    /// a time.
    share: Option<u64>,
    /// The row group being read in runs, while row pages of it remain.
    open: Option<OpenGroup>,
    /// What a row decoded to in the last read, the table's average before the
    /// first, `0` when neither is known.
    bytes_per_row: u64,
    /// The rows of the last group read, the table's average before the first.
    group_rows: u64,
    /// Reads that went past the share since last taken.
    oversized: u64,
}

impl ColumnarCursor {
    /// A cursor over `table`'s rows within `lo..hi`, holding at most `share`
    /// page bytes at once when given, a row group at a time otherwise.
    pub(crate) fn new(
        table: &Table,
        projection: &[u16],
        predicate: Option<&ColumnRangePredicate>,
        lo: Bound<UserKey>,
        hi: Bound<UserKey>,
        share: Option<u64>,
    ) -> crate::Result<Self> {
        if !table.metadata.columnar {
            return Err(crate::Error::FeatureUnsupported("columnar"));
        }
        // The predicate must see its own column, even when the caller did not
        // project it; decode it too and drop it from each output batch, so a
        // predicate on an unprojected column still filters instead of matching
        // every row.
        let mut decode_projection = projection.to_vec();
        let added_predicate_column = match predicate {
            Some(pred) if !decode_projection.contains(&pred.column_id) => {
                decode_projection.push(pred.column_id);
                Some(pred.column_id)
            }
            _ => None,
        };
        let has_deletes = !table.delete_bitmap.is_empty();
        // Before a row is read, what one decodes to is the table's average,
        // from the sums its writer recorded: a table whose average group fits
        // the share has its first group read whole, in one request, and one
        // that does not starts in runs. A wrong guess costs one read past the
        // share, counted, or one read in runs; the reads correct it.
        let bytes_per_row = seed_bytes_per_row(table, &decode_projection);
        let group_rows = table
            .metadata
            .item_count
            .div_ceil(table.metadata.data_block_count.max(1));
        let mut index = table.block_index.iter();
        // With no positional deletes nothing needs the row counts of the row
        // groups below the lower bound, so the index seeks straight past them.
        // With deletes they are walked, each group's count read from its zone
        // map, so the positions stay aligned.
        let seek_missed = !has_deletes
            && matches!(&lo, Bound::Included(key) | Bound::Excluded(key)
                if !index.seek_lower(key, SeqNo::MAX));
        Ok(Self {
            first_live_block: table.restrict_lower_bound().is_some(),
            table: table.clone(),
            index: (!seek_missed).then_some(index),
            decode_projection,
            added_predicate_column,
            predicate: predicate.cloned(),
            support: PredicateSupport::Exact,
            lo,
            hi,
            has_deletes,
            row_base: 0,
            expect_whole: false,
            pending: VecDeque::new(),
            pending_bytes: 0,
            share,
            open: None,
            bytes_per_row,
            group_rows,
            oversized: 0,
        })
    }

    /// Whether the next group, as wide as the last, fits `share`, so it is
    /// read whole rather than in runs. Groups of a table are cut to one size.
    fn group_fits(&self, share: u64) -> bool {
        self.bytes_per_row > 0
            && self
                .group_rows
                .checked_mul(self.bytes_per_row)
                .is_some_and(|bytes| bytes <= share)
    }

    /// Reads that went past the share since this was last called: a row page
    /// larger than the share, or a run whose rows decoded wider than the ones
    /// before, or a group read whole through its zones.
    pub(crate) fn take_oversized(&mut self) -> u64 {
        core::mem::take(&mut self.oversized)
    }

    /// How far the predicate ran over every row group read so far. A group or
    /// row page the statistics skipped leaves it as it is: only an ordered
    /// column has statistics, and the skip proved none of its rows match.
    #[must_use]
    pub fn predicate_support(&self) -> PredicateSupport {
        self.support
    }

    /// Bytes of the batches read and not yet yielded.
    pub(crate) fn held_bytes(&self) -> u64 {
        self.pending_bytes
    }

    /// Row count of a row group stepped over without decoding, from its zone
    /// map. A punched group CANNOT be decoded (it reads as zeros), so the zone
    /// map is the only source; without it every later row's positional delete
    /// mapping would silently shift, so it fails loudly instead.
    fn skipped_rows(&self, offset: u64) -> crate::Result<u32> {
        self.table
            .zone_map
            .columns_for(offset)
            .and_then(|stats| stats.first())
            .map(|s| s.row_count)
            .ok_or(crate::Error::InvalidHeader(
                "columnar_scan: skipped block has no zone-map row count while positional deletes are present",
            ))
    }

    /// Steps over a row group without reading it, keeping the positional
    /// delete mapping aligned.
    fn skip_group(&mut self, offset: u64) -> crate::Result<()> {
        if self.has_deletes {
            let rows = self.skipped_rows(offset)?;
            self.row_base = self.row_base.wrapping_add(rows);
        }
        Ok(())
    }

    /// Reads the next row group into `pending`, or marks the cursor done.
    fn step(&mut self) -> crate::Result<()> {
        use core::cmp::Ordering;

        let Some(keyed) = self.index.as_mut().and_then(Iterator::next) else {
            self.index = None;
            return Ok(());
        };
        let keyed = keyed?;
        let offset = keyed.offset().0;
        let cmp = self.table.comparator.clone();
        let restrict = self.table.restrict_lower_bound().cloned();

        // Tight-space restriction: data blocks wholly below the bound are
        // hole-punched (they read as zeros), so they are stepped over, never
        // decoded. The first live block may STRADDLE the bound (the punch is
        // block-aligned, the bound is a key), so its sub-bound rows are
        // masked below.
        if let Some(bound) = &restrict
            && cmp.compare(keyed.end_key(), bound.as_ref()) == Ordering::Less
        {
            return self.skip_group(offset);
        }
        // Only the FIRST block at or past the bound can straddle it: keys
        // ascend across blocks, so every later block is entirely live.
        let straddles_bound = self.first_live_block;
        self.first_live_block = false;

        // The key range: a group whose last key is below the lower bound holds
        // no row of it. The group that reaches the upper bound is the last one
        // that can: every later key is above it. Under an inclusive bound a
        // group ending AT it is not the last, because the next one can hold
        // more versions of the bound key.
        let below = match &self.lo {
            Bound::Unbounded => false,
            Bound::Included(lo) => cmp.compare(keyed.end_key(), lo) == Ordering::Less,
            Bound::Excluded(lo) => cmp.compare(keyed.end_key(), lo) != Ordering::Greater,
        };
        if below {
            return self.skip_group(offset);
        }
        let last = match &self.hi {
            Bound::Unbounded => false,
            Bound::Included(hi) => cmp.compare(keyed.end_key(), hi) == Ordering::Greater,
            Bound::Excluded(hi) => cmp.compare(keyed.end_key(), hi) != Ordering::Less,
        };
        if last {
            self.index = None;
        }

        // Zone-map block skip: prove the block is out of range and never
        // load it. A missing entry is conservative (cannot skip). Skipped rows
        // are predicate-excluded, so whether they are deleted does not affect
        // the output.
        if let Some(pred) = &self.predicate
            && let Some(stats) = self.table.zone_map.columns_for(offset)
            && pred.can_skip_block(stats)
        {
            return self.skip_group(offset);
        }
        self.read_group(&keyed, restrict.as_ref().filter(|_| straddles_bound))
    }

    /// Reads the row group `keyed` names, one batch per row page that keeps a
    /// row: whole, or its first run of row pages when the group is read in
    /// runs. `straddled` is the restriction bound when this group is the one
    /// that can straddle it.
    fn read_group(
        &mut self,
        keyed: &crate::table::KeyedBlockHandle,
        straddled: Option<&UserKey>,
    ) -> crate::Result<()> {
        let handle = *keyed.as_ref();
        // A group is read in runs only when nothing selects its row pages by
        // their zones: a zone selection is resolved over the whole directory,
        // so a pushed-down predicate reads the group whole, within its share
        // or counted past it.
        if let Some(share) = self.share.filter(|_| self.predicate.is_none())
            && !self.group_fits(share)
        {
            // The first run is one row page: what a row of this group
            // decodes to is not known before one is read, and the directory,
            // read with it, is what the later runs are sized over.
            let first = self
                .table
                .group_read(&handle, ReadCharge::Foreground)
                .load(&PageWant::projected(
                    &self.decode_projection,
                    RowPageSelect::Range(0..1),
                ))?;
            // Whether the projection takes every column, so a group read
            // whole later is taken in one request: the run holds one page per
            // projected column of the directory's grid.
            self.expect_whole = first.pages.len() * first.directory.row_pages().len()
                == first.directory.entries().len();
            let mut group = OpenGroup {
                handle,
                straddled: straddled.cloned(),
                directory: RowGroupBlocks {
                    directory: alloc::sync::Arc::clone(&first.directory),
                    row_pages: RowPageSet::Range(0..0),
                    pages: Vec::new(),
                },
                next_page: 0,
                share,
            };
            self.take_run(&mut group, &first, 0..1)?;
            self.continue_group(group);
            return Ok(());
        }
        self.read_whole_group(&handle, straddled)
    }

    /// Reads the next run of row pages of `group`, the group in progress: as
    /// many as its share holds at the bytes a row decoded to so far, at least
    /// one.
    fn read_run(&mut self, group: OpenGroup) -> crate::Result<()> {
        let share = group.share;
        let directory = &group.directory.directory;
        let start = group.next_page;
        let mut end = start;
        let mut rows = 0u64;
        while let Some(page_rows) = directory.row_page_rows(end) {
            let rows_after = rows + u64::from(page_rows);
            // A run past the share ends before this page, unless it is the
            // run's first: one row page is the least a read can take.
            // A product past `u64` is past any share too.
            if end > start
                && rows_after
                    .checked_mul(self.bytes_per_row)
                    .is_none_or(|bytes| bytes > share)
            {
                break;
            }
            rows = rows_after;
            end += 1;
        }
        let blocks = self
            .table
            .group_read(&group.handle, ReadCharge::Foreground)
            .load_after(
                &group.directory,
                &PageWant::projected(&self.decode_projection, RowPageSelect::Range(start..end)),
            )?;
        let mut group = group;
        self.take_run(&mut group, &blocks, start..end)?;
        self.continue_group(group);
        Ok(())
    }

    /// Yields the run `pages` of `group` read as `blocks`, and learns from it
    /// what a row decodes to.
    fn take_run(
        &mut self,
        group: &mut OpenGroup,
        blocks: &RowGroupBlocks,
        pages: core::ops::Range<u16>,
    ) -> crate::Result<()> {
        let bound_keys = match &group.straddled {
            Some(bound) => Some((
                bound,
                self.table.load_columnar_block_projected(
                    &group.handle,
                    &[crate::table::columnar::COL_USER_KEY],
                    RowPageSelect::Range(pages.clone()),
                    false,
                )?,
            )),
            None => None,
        };
        let (bytes, rows) = self.yield_pages(blocks, bound_keys.as_ref().map(|(b, k)| (*b, k)))?;
        self.learn(bytes, rows, group.share);
        group.next_page = pages.end;
        Ok(())
    }

    /// Keeps `group` open while row pages of it remain, or closes it,
    /// advancing the positions past its rows.
    fn continue_group(&mut self, group: OpenGroup) {
        let directory = &group.directory.directory;
        if usize::from(group.next_page) < directory.row_pages().len() {
            self.open = Some(group);
            return;
        }
        let group_rows = directory.row_count();
        self.row_base = self.row_base.wrapping_add(group_rows);
        self.group_rows = u64::from(group_rows);
    }

    /// Takes what one read held and the rows it read: what a row costs, for
    /// sizing the next run, and whether the read went past the share.
    fn learn(&mut self, bytes: u64, rows: u32, share: u64) {
        if rows > 0 {
            self.bytes_per_row = bytes.div_ceil(u64::from(rows));
        }
        if bytes > share {
            self.oversized += 1;
        }
    }

    /// Reads the row group `handle` names whole into `pending`.
    fn read_whole_group(
        &mut self,
        handle: &BlockHandle,
        straddled: Option<&UserKey>,
    ) -> crate::Result<()> {
        let predicate = self.predicate.as_ref();
        // Row-page pruning: a row page whose zone proves it out of range is
        // never read, the same proof the zone map gives a whole group, at the
        // granularity a read can skip.
        let select = || match predicate {
            Some(pred) => RowPageSelect::Zone {
                column_id: pred.column_id,
                lower: pred.lower.as_deref(),
                upper: pred.upper.as_deref(),
            },
            None => RowPageSelect::All,
        };
        let blocks = self
            .table
            .group_read(handle, ReadCharge::Foreground)
            .load(&PageWant {
                columns: Some(&self.decode_projection),
                row_pages: select(),
                whole: self.expect_whole,
            })?;
        // The straddling group's key column, decoded separately (one extra
        // cached read for at most one group per scan) so the main projection
        // stays untouched: it masks the rows below the bound. The same
        // selection over the same directory reads the same row pages, one key
        // batch per batch above.
        let bound_keys = match straddled {
            Some(bound) => Some((
                bound,
                self.table.load_columnar_block_projected(
                    handle,
                    &[crate::table::columnar::COL_USER_KEY],
                    select(),
                    false,
                )?,
            )),
            None => None,
        };
        // A projection that took every page of this group most likely takes
        // every page of the next one too, so that one is read in one request
        // rather than directory first. Being wrong costs only the bytes of the
        // pages it turns out not to want.
        self.expect_whole = blocks.pages.len() == blocks.directory.entries().len();
        let group_rows = blocks.directory.row_count();
        let (bytes, rows) = self.yield_pages(&blocks, bound_keys.as_ref().map(|(b, k)| (*b, k)))?;
        if let Some(share) = self.share {
            self.learn(bytes, rows, share);
        }
        self.group_rows = u64::from(group_rows);
        // The whole group's rows, the row pages pruned included, so the next
        // group's positions start where this group's end.
        self.row_base = self.row_base.wrapping_add(group_rows);
        Ok(())
    }

    /// Pushes onto `pending` one batch per page of `blocks` that keeps a row,
    /// returning what the read held and the rows it read: the larger of the
    /// pages loaded and the batches built from them, since a filter can keep
    /// far less than it loaded and a decode can build more than it loaded,
    /// and the rows before any filter, which is what a run is sized in.
    /// `bound_keys` holds the key column of the same row pages when the group
    /// straddles the restriction bound.
    fn yield_pages(
        &mut self,
        blocks: &RowGroupBlocks,
        bound_keys: Option<(&UserKey, &crate::table::row_group::RowPages)>,
    ) -> crate::Result<(u64, u32)> {
        use crate::table::columnar_predicate::{PredicateApply, Selection};

        let predicate = self.predicate.as_ref();
        // The pages parsed, not decoded: the predicate is tested from its
        // column's encoding, and each page's survivors are then built straight
        // from theirs.
        let mut budget = crate::table::columnar::DecodeBudget::default();
        let pages = blocks.page_columns(|_| true, &mut budget)?;
        // The key pages read to mask a straddling group are held beside the
        // projected ones, whether or not the scan projects the key.
        let bound_bytes: u64 = bound_keys.as_ref().map_or(0, |(_, keys)| {
            keys.batches.iter().map(|b| b.data_size() as u64).sum()
        });
        let loaded: u64 = blocks
            .pages
            .iter()
            .filter_map(|slot| slot.block.as_ref())
            .map(|block| block.data.len() as u64)
            .sum();
        // The pages of one group, whose rows fit a `u32`.
        let rows_read: u32 = pages.iter().map(|page| page.rows).sum();
        let row_base = self.row_base;
        let mut copied = 0usize;
        let mut support = self.support;
        let mut out = Vec::new();
        // The group's pages read as one result, so the copies made before a
        // page fails are recorded like those of a group that succeeds.
        let read_pages = || -> crate::Result<()> {
            for page in pages {
                let (ordinal, row_count) = (page.ordinal, page.rows);
                let page_base = row_base.wrapping_add(page.start);
                let bound_mask: Option<Vec<bool>> = match &bound_keys {
                    Some((bound, keys)) => {
                        use crate::table::columnar::{COL_USER_KEY, bytes_column_row};
                        let key_col = keys
                            .ordinals
                            .iter()
                            .position(|&o| o == ordinal)
                            .and_then(|i| keys.batches.get(i))
                            .filter(|k| k.row_count == row_count)
                            .and_then(|k| k.columns.iter().find(|c| c.column_id == COL_USER_KEY))
                            .ok_or(crate::Error::InvalidHeader(
                                "columnar_scan: straddling block is missing the key column",
                            ))?;
                        let mut mask = Vec::with_capacity(row_count as usize);
                        for row in 0..row_count {
                            let key = bytes_column_row(&key_col.data, row_count, row)?;
                            mask.push(
                                self.table.comparator.compare(key, bound.as_ref())
                                    != core::cmp::Ordering::Less,
                            );
                        }
                        Some(mask)
                    }
                    None => None,
                };
                // What the predicate keeps, tested from its column's encoding
                // without decoding it. A page without the column, or with it
                // opaque, is handed out whole for the caller to check.
                let mut keep: Option<Selection> = None;
                if let Some(pred) = predicate {
                    let tested = page
                        .columns
                        .iter()
                        .find(|c| c.column_id == pred.column_id)
                        .and_then(|c| Some((c, pred.bounds(c.type_tag)?)));
                    match tested {
                        None => support = support.min(PredicateSupport::Unsupported),
                        Some(_) if pred.apply == PredicateApply::Prune => {
                            support = support.min(PredicateSupport::PruneOnly);
                        }
                        Some((column, bounds)) => {
                            support = support.min(PredicateSupport::Exact);
                            keep = Some(column.select(row_count, &bounds)?);
                        }
                    }
                }
                if self.has_deletes || bound_mask.is_some() {
                    let kept = keep.get_or_insert_with(|| Selection::all(row_count));
                    if self.has_deletes {
                        for row in 0..row_count {
                            if self
                                .table
                                .delete_bitmap
                                .contains(page_base.wrapping_add(row))
                            {
                                kept.remove(row);
                            }
                        }
                    }
                    if let Some(mask) = &bound_mask {
                        for (row, &live) in (0u32..).zip(mask) {
                            if !live {
                                kept.remove(row);
                            }
                        }
                    }
                }
                // Every row kept: the page is handed out as decoded, a view of
                // it where a column is stored plain, with no gather.
                let keep = keep.filter(|kept| kept.count() < row_count);
                // A row page none of whose rows survive yields nothing:
                // building an empty batch for it would be a gather no caller
                // receives.
                if keep.as_ref().is_some_and(|kept| kept.count() == 0) {
                    continue;
                }
                let columns = page
                    .columns
                    .into_iter()
                    // A column read only for the predicate is never decoded.
                    .filter(|c| self.added_predicate_column != Some(c.column_id))
                    .map(|c| match &keep {
                        Some(kept) => c.decode_rows(row_count, kept, &mut copied, &mut budget),
                        None => c.decode(row_count, &mut copied, &mut budget),
                    })
                    .collect::<crate::Result<Vec<_>>>()?;
                out.push(ColumnBatch {
                    row_count: keep.as_ref().map_or(row_count, Selection::count),
                    columns,
                });
            }
            Ok(())
        };
        let read = read_pages();
        #[cfg(feature = "metrics")]
        self.table.metrics.record_gather(copied);
        #[cfg(not(feature = "metrics"))]
        let _ = copied;
        self.support = support;
        read?;
        let built: u64 = out.iter().map(|b| b.data_size() as u64).sum();
        self.pending.extend(out);
        self.pending_bytes += built;
        Ok((loaded.max(built) + bound_bytes, rows_read))
    }
}

/// What a row of `projection` decodes to on average across `table`, from the
/// key and value byte sums its writer recorded, or `0` when it recorded none.
/// A bytes column spends four bytes per row on where each value starts; a
/// column of the value's own is counted as the whole value, which bounds it.
fn seed_bytes_per_row(table: &Table, projection: &[u16]) -> u64 {
    use crate::table::columnar::{COL_SEQNO, COL_USER_KEY, COL_VALUE_TYPE};

    let meta = &table.metadata;
    let (Some(keys), Some(values)) = (meta.sum_user_key_bytes, meta.sum_value_bytes) else {
        return 0;
    };
    if meta.item_count == 0 {
        return 0;
    }
    let key = keys.div_ceil(meta.item_count) + 4;
    let value = values.div_ceil(meta.item_count) + 4;
    projection
        .iter()
        .map(|&column| match column {
            COL_USER_KEY => key,
            COL_SEQNO => 8,
            COL_VALUE_TYPE => 1,
            _ => value,
        })
        .sum()
}

/// A row group read in runs of row pages, while row pages of it remain.
struct OpenGroup {
    handle: BlockHandle,
    /// The restriction bound, when this group is the one that can straddle
    /// it.
    straddled: Option<UserKey>,
    /// The group's directory, as its first run read it, holding no page.
    directory: RowGroupBlocks,
    /// The first row page not read yet.
    next_page: u16,
    /// The page bytes each run of the group may hold.
    share: u64,
}

impl Iterator for ColumnarCursor {
    type Item = crate::Result<ColumnBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(batch) = self.pending.pop_front() {
                // Counted in when pushed, so it is never more than the total.
                self.pending_bytes -= batch.data_size() as u64;
                return Some(Ok(batch));
            }
            let read = if let Some(group) = self.open.take() {
                self.read_run(group)
            } else {
                self.index.as_ref()?;
                self.step()
            };
            if let Err(e) = read {
                self.index = None;
                self.open = None;
                return Some(Err(e));
            }
        }
    }
}
