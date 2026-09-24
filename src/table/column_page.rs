// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The directory of one columnar row group's pages.
//!
//! A row group is laid out as its directory followed by its pages, contiguous
//! in the file. The table index points at the directory, so a row group keeps
//! exactly one index entry and `block_id` keeps its meaning as the group's
//! ordinal in key order; the pages are reached from here.
//!
//! A group's rows are cut into **row pages**, consecutive row ranges shared by
//! every column, and each part of each column's encoding holds one page per
//! row page. A read that wants a few rows reads the row pages that hold them;
//! a read that wants every row assembles each row page from its own pages
//! without copying one row page into another.
//!
//! See `docs/columnar-page-format.md` for the layout and
//! `docs/columnar-addressing.md` for why a page may not become an addressing
//! unit.
//!
//! # Wire format
//!
//! ```text
//! [version       : u8    ]  currently 2; an unknown version is refused
//! [page_count    : u16 LE]
//! [row_count     : u32 LE]  rows in the group
//! [group_tag     : u64 LE]  names the group; every page carries it in its stamp
//! [row_page_count: u16 LE]
//! [zone_block_count: u16 LE]
//! repeated row_page_count times:
//!   [rows : u32 LE]  rows in the row page, non-zero; the sum is `row_count`
//! repeated page_count times, ascending by offset, non-overlapping:
//!   [offset   : u32 LE]  start of the page, from the END of the directory
//!   [length   : u32 LE]  on-disk length of the page, its header included
//!   [column_id: u16 LE]  the column the page belongs to
//!   [part     : u8    ]  which part of that column's encoding it holds
//!   [flags    : u8    ]  reserved; a reader refuses unknown bits
//!   [row_page : u16 LE]  which row page's rows it holds
//! repeated zone_block_count times, in the order they follow the pages:
//!   [column_id: u16 LE]  the column whose zones the block holds
//!   [length   : u32 LE]  on-disk length of the block, its header included
//! zones (see below)
//! ```
//!
//! and a set of zones, in the directory and in each zone block alike. A zone
//! block's payload opens with its group's `[group_tag: u64 LE]`, which a
//! reader checks against the directory, so a zone block moved into another
//! group's place is refused like a page with another group's stamp:
//!
//! ```text
//! [zone_column_count: u16 LE]
//! repeated zone_column_count times:
//!   [column_id: u16 LE]  a column the zones describe
//! repeated row_page_count * zone_column_count times, row page by row page:
//!   [null_count: u32 LE]  null rows of the column in the row page
//!   [flags     : u8    ]  bit 0: no upper bound; other bits reserved
//!   [min_len   : u8    ]  at most `ZONE_BOUND_LEN`
//!   [min       : min_len bytes]
//!   [max_len   : u8    ]  at most `ZONE_BOUND_LEN`, zero without an upper bound
//!   [max       : max_len bytes]
//! ```
//!
//! Every `(column_id, part)` has exactly one page per row page: the pages form
//! a complete grid, so a row page can always be assembled from its own pages.
//!
//! # Statistics zones
//!
//! A group of more than one row page carries a zone per row page and bytes
//! column: its null count and a byte range that holds every non-null value of
//! the column in the row page. A reader prunes the row pages whose zones
//! cannot hold what it looks for before it reads them, so pruning is as fine
//! as the pages a read can skip, not the group. A group of one row page has
//! none: its zone is the group's own zone-map entry.
//!
//! The key column's zones are in the directory, which a point read reads
//! first anyway, so it goes from the directory to the one key page that can
//! hold its key. Every other column's zones are in a
//! [`ColumnZones`](crate::table::block::BlockType::ColumnZones) block of its
//! own after the pages, read only by a read that prunes on that column: a full
//! scan or a projection that does not prune pays for none of them, and one
//! that prunes on a narrow column does not pay for a wide column's zones.
//!
//! Bounds are cut to [`ZONE_BOUND_LEN`] bytes: a prefix of the minimum is
//! still a lower bound, and a prefix of the maximum with its last byte raised
//! is still an upper bound; a maximum whose prefix is all `0xFF` has no such
//! bound and is recorded as unbounded. A cut bound prunes less, never wrongly,
//! and keeps a zone small next to the pages it describes whatever the width
//! of the values.
//!
//! Offsets are relative to the group rather than to the file, which is what
//! lets a compaction copy a group whole without rewriting its directory, and
//! is the same reason the block layer refuses to bind a file offset into a
//! block's identity. They are measured from the directory's end, where the
//! first page starts, because the directory's own on-disk length depends on
//! the transforms applied to it and is not known until it is sealed.
//!
//! # Page stamp
//!
//! Every page payload opens with a [`PageStamp`]: the group's tag, the
//! `(column_id, part)` it holds and its row page. A reader refuses a page
//! whose stamp is not the one its directory entry implies. The block layer's
//! AEAD binds a block to its table and role but not to its position, which
//! costs nothing for a row-major block, since a value cannot leave its key's
//! block. A page's value is separated from its key by construction, so
//! without the stamp two groups' value pages, or two row pages of one group,
//! of equal length would swap undetected under encryption and serve one key's
//! value under another. Inside an encrypted page the stamp is authenticated
//! with the rest of the payload.
//!
//! The tag is carried in the directory rather than derived from the group's
//! ordinal, so a group copied byte for byte into a table where its ordinal
//! differs (a salvage that dropped an earlier group) still verifies. Tags
//! strictly increase within a table, which keeps them unique.

use crate::{Error, Result};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Directory wire version. A reader refuses anything else rather than
/// guessing: the directory is what makes every page in the group addressable,
/// so misreading it is not a local error.
///
/// Distinct from the table-level
/// [`COLUMNAR_FORMAT_VERSION`](crate::table::meta::COLUMNAR_FORMAT_VERSION),
/// which is stamped in the descriptor and so is known before any block is
/// read; this one versions the directory block's own wire form.
pub const VERSION: u8 = 2;

/// `version` + `page_count` + `row_count` + `group_tag` + `row_page_count` +
/// `zone_block_count`.
const HEADER_LEN: usize = 1 + 2 + 4 + 8 + 2 + 2;

/// `column_id` + `length` of one zone block.
const ZONE_BLOCK_LEN: usize = 2 + 4;

/// One zone block after a group's pages: whose zones it holds and how long
/// it is on disk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZoneBlock {
    /// The column whose zones the block holds.
    pub column_id: u16,
    /// On-disk length of the block, its header included.
    pub length: u32,
}

/// `rows` of one row page.
const ROW_PAGE_LEN: usize = 4;

/// `offset` + `length` + `column_id` + `part` + `flags` + `row_page`.
const ENTRY_LEN: usize = 4 + 4 + 2 + 1 + 1 + 2;

/// The most bytes of a bound a statistics zone keeps. The value Parquet's page
/// index truncates its statistics to by default: long enough that keys and
/// short fields keep their exact bounds, short enough that a zone of a wide
/// value costs a small fraction of its page.
pub const ZONE_BOUND_LEN: usize = 64;

/// Zone flag: the zone has no upper bound.
const ZONE_MAX_UNBOUNDED: u8 = 1;

/// A zone's fixed fields: `null_count` + `flags` + `min_len` + `max_len`.
const ZONE_FIXED_LEN: usize = 4 + 1 + 1 + 1;

/// Where one zone's bounds sit in [`PageZones`]'s shared buffer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ZoneSlot {
    null_count: u32,
    /// `(start, len)` of the lower bound.
    min: (u32, u8),
    /// `(start, len)` of the upper bound, or `None` when there is none.
    max: Option<(u32, u8)>,
}

/// One column's statistics zone over one row page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Zone<'a> {
    /// Null rows of the column in the row page.
    pub null_count: u32,
    /// No non-null value of the column in the row page is below this.
    pub min: &'a [u8],
    /// No non-null value of the column in the row page is above this; `None`
    /// when the zone has no upper bound.
    pub max: Option<&'a [u8]>,
}

/// The statistics zones of a group's row pages: one per row page and zone
/// column, row page by row page, their bounds in one buffer.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageZones {
    columns: Vec<u16>,
    bounds: Vec<u8>,
    slots: Vec<ZoneSlot>,
}

impl PageZones {
    /// No zones yet, for the columns `columns` names, in the order each row
    /// page's zones are pushed.
    #[must_use]
    pub fn new(columns: Vec<u16>) -> Self {
        Self {
            columns,
            bounds: Vec::new(),
            slots: Vec::new(),
        }
    }

    /// Appends the next zone from the exact range of its rows' non-null
    /// values, `None` when every row is null, cutting the bounds to
    /// [`ZONE_BOUND_LEN`].
    pub fn push(&mut self, null_count: u32, range: Option<(&[u8], &[u8])>) {
        let Some((min, max)) = range else {
            self.push_bounds(null_count, &[], Some(&[]));
            return;
        };
        let min = min.get(..ZONE_BOUND_LEN).unwrap_or(min);
        if max.len() <= ZONE_BOUND_LEN {
            self.push_bounds(null_count, min, Some(max));
            return;
        }
        // Raise the last byte of the prefix that can be raised and drop what
        // follows it: every value that starts with the prefix is then below
        // the bound. A prefix of `0xFF` bytes has no such byte.
        let prefix = max.get(..ZONE_BOUND_LEN).unwrap_or(max);
        match prefix.iter().rposition(|&b| b != u8::MAX) {
            Some(last) => {
                let mut bound = prefix.get(..=last).unwrap_or(prefix).to_vec();
                if let Some(byte) = bound.last_mut() {
                    *byte += 1;
                }
                self.push_bounds(null_count, min, Some(&bound));
            }
            None => self.push_bounds(null_count, min, None),
        }
    }

    /// Appends a zone whose bounds are already at most [`ZONE_BOUND_LEN`].
    fn push_bounds(&mut self, null_count: u32, min: &[u8], max: Option<&[u8]>) {
        let mut place = |bound: &[u8]| {
            // A directory is a block, so its bounds fit the u32 offsets, and
            // each bound is at most `ZONE_BOUND_LEN` bytes.
            #[expect(
                clippy::cast_possible_truncation,
                reason = "a directory block is far below 4 GiB and a bound below 256 bytes"
            )]
            let at = (self.bounds.len() as u32, bound.len() as u8);
            self.bounds.extend_from_slice(bound);
            at
        };
        let min = place(min);
        let max = max.map(&mut place);
        self.slots.push(ZoneSlot {
            null_count,
            min,
            max,
        });
    }

    /// Whether these zones describe column `column_id`.
    #[must_use]
    pub fn describes(&self, column_id: u16) -> bool {
        self.columns.contains(&column_id)
    }

    /// The columns these zones describe, in their order.
    #[must_use]
    pub fn columns(&self) -> &[u16] {
        &self.columns
    }

    /// The zones restricted to the columns `keep` accepts, in the same order.
    #[must_use]
    pub fn only(&self, keep: impl Fn(u16) -> bool) -> Self {
        let kept: Vec<usize> = (0..self.columns.len())
            .filter(|&i| self.columns.get(i).is_some_and(|&c| keep(c)))
            .collect();
        let mut out = Self::new(
            kept.iter()
                .filter_map(|&i| self.columns.get(i).copied())
                .collect(),
        );
        let width = self.columns.len();
        if width == 0 {
            return out;
        }
        for row_page in 0..self.slots.len() / width {
            for &column in &kept {
                if let Some(zone) = self.get(row_page * width + column) {
                    out.push_bounds(zone.null_count, zone.min, zone.max);
                }
            }
        }
        out
    }

    /// Column `column_id`'s zone over row page `row_page`, or `None` when the
    /// column has no zones here or there is no such row page.
    #[must_use]
    pub fn zone(&self, row_page: u16, column_id: u16) -> Option<Zone<'_>> {
        let column = self.columns.iter().position(|&c| c == column_id)?;
        let index = usize::from(row_page)
            .checked_mul(self.columns.len())?
            .checked_add(column)?;
        self.get(index)
    }

    /// The zone at `index`, row page by row page.
    #[must_use]
    fn get(&self, index: usize) -> Option<Zone<'_>> {
        let slot = self.slots.get(index)?;
        let bound = |(start, len): (u32, u8)| {
            let start = start as usize;
            self.bounds.get(start..start + usize::from(len))
        };
        Some(Zone {
            null_count: slot.null_count,
            min: bound(slot.min)?,
            max: match slot.max {
                Some(max) => Some(bound(max)?),
                None => None,
            },
        })
    }

    /// The wire form's length.
    fn encoded_len(&self) -> usize {
        2 + self.columns.len() * 2 + self.slots.len() * ZONE_FIXED_LEN + self.bounds.len()
    }

    /// Appends the zones' wire form to `out`.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.reserve(self.encoded_len());
        // `PageDirectory::new` refuses more zone columns than a u16 counts.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the zone column count is bounded to u16::MAX when the zones are checked"
        )]
        let columns = self.columns.len() as u16;
        out.extend_from_slice(&columns.to_le_bytes());
        for column_id in &self.columns {
            out.extend_from_slice(&column_id.to_le_bytes());
        }
        for index in 0..self.slots.len() {
            let Some(zone) = self.get(index) else {
                continue;
            };
            out.extend_from_slice(&zone.null_count.to_le_bytes());
            out.push(if zone.max.is_none() {
                ZONE_MAX_UNBOUNDED
            } else {
                0
            });
            // Every bound is at most `ZONE_BOUND_LEN`, below 256: `push` cuts
            // them, and the checks refuse a longer one.
            #[expect(
                clippy::cast_possible_truncation,
                reason = "a bound is at most ZONE_BOUND_LEN bytes"
            )]
            let min_len = zone.min.len() as u8;
            out.push(min_len);
            out.extend_from_slice(zone.min);
            let max = zone.max.unwrap_or(&[]);
            #[expect(
                clippy::cast_possible_truncation,
                reason = "a bound is at most ZONE_BOUND_LEN bytes"
            )]
            let max_len = max.len() as u8;
            out.push(max_len);
            out.extend_from_slice(max);
        }
    }

    /// Reads zones for `row_page_count` row pages off the front of `rest`.
    fn decode_from(rest: &mut &[u8], row_page_count: usize) -> Result<Self> {
        const ERR: Error = Error::InvalidHeader("ColumnZones");

        let column_count = usize::from(u16::from_le_bytes(take(rest).ok_or(ERR)?));
        let mut columns = Vec::with_capacity(column_count.min(rest.len() / 2));
        for _ in 0..column_count {
            columns.push(u16::from_le_bytes(take(rest).ok_or(ERR)?));
        }
        let count = row_page_count.checked_mul(column_count).ok_or(ERR)?;
        let mut zones = Self::new(columns);
        zones.slots.reserve(count.min(rest.len() / ZONE_FIXED_LEN));
        for _ in 0..count {
            let null_count = u32::from_le_bytes(take(rest).ok_or(ERR)?);
            let [flags] = take::<1>(rest).ok_or(ERR)?;
            let [min_len] = take::<1>(rest).ok_or(ERR)?;
            let min = take_slice(rest, usize::from(min_len)).ok_or(ERR)?;
            let [max_len] = take::<1>(rest).ok_or(ERR)?;
            let max = take_slice(rest, usize::from(max_len)).ok_or(ERR)?;
            let max = match flags {
                0 => Some(max),
                ZONE_MAX_UNBOUNDED if max.is_empty() => None,
                ZONE_MAX_UNBOUNDED => {
                    return Err(Error::InvalidHeader(
                        "ColumnZones: an unbounded zone records an upper bound",
                    ));
                }
                _ => return Err(Error::InvalidHeader("ColumnZones: reserved zone flag set")),
            };
            zones.push_bounds(null_count, min, max);
        }
        Ok(zones)
    }
}

/// Which part of which column's encoding a page holds.
///
/// Two fields rather than one because a column id is already 16 bits wide.
/// The engine attaches no meaning to `part` beyond equality: how a column's
/// parts are numbered is its encoding's business, and a column whose encoding
/// names a single part uses part `0`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PageId {
    /// The column the page belongs to.
    pub column_id: u16,
    /// Which part of that column's encoding the page holds.
    pub part: u8,
}

/// One page's placement within its row group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageEntry {
    /// Start of the page, measured from the end of the directory.
    pub offset: u32,
    /// On-disk length of the page, its block header included.
    pub length: u32,
    /// What the page holds.
    pub id: PageId,
    /// Which row page's rows it holds.
    pub row_page: u16,
}

/// What a page says about itself, at the front of its payload: the group it
/// was written into, what it holds and for which row page. See the module
/// docs for why.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageStamp {
    /// The owning group's [`PageDirectory::group_tag`].
    pub group_tag: u64,
    /// What the page holds.
    pub id: PageId,
    /// Which row page's rows it holds.
    pub row_page: u16,
}

impl PageStamp {
    /// `group_tag` + `column_id` + `part` + `row_page`.
    pub const LEN: usize = 8 + 2 + 1 + 2;

    /// Appends the stamp's wire form to `out`.
    pub fn encode_into(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.group_tag.to_le_bytes());
        out.extend_from_slice(&self.id.column_id.to_le_bytes());
        out.push(self.id.part);
        out.extend_from_slice(&self.row_page.to_le_bytes());
    }

    /// Reads a stamp from its wire form. Every byte pattern is a stamp; what
    /// makes one wrong is disagreeing with the directory.
    #[must_use]
    pub fn decode(bytes: [u8; Self::LEN]) -> Self {
        let [t0, t1, t2, t3, t4, t5, t6, t7, c0, c1, part, r0, r1] = bytes;
        Self {
            group_tag: u64::from_le_bytes([t0, t1, t2, t3, t4, t5, t6, t7]),
            id: PageId {
                column_id: u16::from_le_bytes([c0, c1]),
                part,
            },
            row_page: u16::from_le_bytes([r0, r1]),
        }
    }
}

/// A decoded page directory.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageDirectory {
    row_count: u32,
    group_tag: u64,
    /// Rows in each row page, in row order.
    row_pages: Vec<u32>,
    /// Where each row page starts, as a row within the group.
    row_page_starts: Vec<u32>,
    entries: Vec<PageEntry>,
    /// The key column's zones; the other columns' are in zone blocks.
    zones: PageZones,
    /// The zone blocks after the pages, in the order they follow them.
    zone_blocks: Vec<ZoneBlock>,
    /// Their total on-disk length.
    zones_len: u32,
}

impl PageDirectory {
    /// Builds a directory for a group of `row_count` rows tagged `group_tag`,
    /// cut into row pages of `row_pages` rows each, from entries already in
    /// ascending offset order, the statistics `zones` the directory itself
    /// carries, and the zone blocks that follow the pages (`zone_blocks`, one
    /// per column whose zones are not in the directory).
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] if there are more pages or row pages
    /// than the wire's `u16` counts hold, if a row page is empty or the row
    /// pages do not sum to `row_count`, if the entries are not ascending by
    /// offset, if two pages overlap, if a page's extent does not fit a `u32`,
    /// if a page names a row page that does not exist, or if the pages do not
    /// form a complete grid: every `(column_id, part)` holding exactly one page
    /// for each row page. Every one of these is rejected here rather than at
    /// read time because none has a correct encoding, and the writer is where
    /// that is still fixable: a directory that maps one byte into two pages,
    /// or one row page's part to two pages, or leaves a row page's part out,
    /// has no correct reading, and one with more pages than its count field
    /// could only be written by truncating the list or by writing a count its
    /// entries contradict.
    ///
    /// The zones are refused unless they name distinct columns the group has,
    /// hold one zone per row page and zone column, count no more nulls than
    /// their row page has rows, record the empty range when every row is
    /// null, keep a lower bound no greater than the upper one, and keep
    /// every bound within [`ZONE_BOUND_LEN`]: a zone out of any of these would
    /// prune a row page that holds a match. The zone blocks are refused unless
    /// each names a distinct column the group has and the directory carries no
    /// zones for, and their lengths are non-zero and sum within a `u32`.
    pub fn new(
        row_count: u32,
        group_tag: u64,
        row_pages: Vec<u32>,
        entries: Vec<PageEntry>,
        zones: PageZones,
        zone_blocks: Vec<ZoneBlock>,
    ) -> Result<Self> {
        if entries.len() > usize::from(u16::MAX) {
            return Err(Error::InvalidHeader(
                "column page: page count exceeds the u16 directory field",
            ));
        }
        if row_pages.len() > usize::from(u16::MAX) {
            return Err(Error::InvalidHeader(
                "column page: row page count exceeds the u16 directory field",
            ));
        }
        let mut row_page_starts = Vec::with_capacity(row_pages.len());
        let mut rows: u32 = 0;
        for &page_rows in &row_pages {
            if page_rows == 0 {
                return Err(Error::InvalidHeader("column page: an empty row page"));
            }
            row_page_starts.push(rows);
            rows = rows.checked_add(page_rows).ok_or(Error::InvalidHeader(
                "column page: row pages overflow the row count",
            ))?;
        }
        if rows != row_count {
            return Err(Error::InvalidHeader(
                "column page: row pages do not sum to the group's rows",
            ));
        }

        let mut end_of_previous: u32 = 0;
        for entry in &entries {
            let end = entry
                .offset
                .checked_add(entry.length)
                .ok_or(Error::InvalidHeader("column page: extent overflows u32"))?;
            if entry.offset < end_of_previous {
                return Err(Error::InvalidHeader(
                    "column page: pages must be ascending and non-overlapping",
                ));
            }
            if usize::from(entry.row_page) >= row_pages.len() {
                return Err(Error::InvalidHeader(
                    "column page: a page names a row page that does not exist",
                ));
            }
            end_of_previous = end;
        }
        // Sorted-neighbour checks rather than pairwise ones. The directory is
        // read from disk, so its page count is whatever the bytes say, up to
        // u16::MAX, and a pairwise check would turn one corrupt block into
        // billions of comparisons on the read path.
        let mut cells: Vec<(PageId, u16)> = entries.iter().map(|e| (e.id, e.row_page)).collect();
        cells.sort_unstable();
        if cells.windows(2).any(|pair| pair.first() == pair.last()) {
            return Err(Error::InvalidHeader(
                "column page: two pages claim the same column part and row page",
            ));
        }
        // Sorted and distinct, so each part's row pages are complete exactly
        // when they read 0, 1, 2, ... up to the last row page.
        let row_page_count = row_pages.len();
        let mut expected: Option<(PageId, usize)> = None;
        for &(id, row_page) in &cells {
            let next = match expected {
                Some((part, n)) if part == id => n,
                Some((_, n)) if n != row_page_count => {
                    return Err(Error::InvalidHeader(
                        "column page: a column part is missing a row page",
                    ));
                }
                _ => 0,
            };
            if usize::from(row_page) != next {
                return Err(Error::InvalidHeader(
                    "column page: a column part is missing a row page",
                ));
            }
            expected = Some((id, next + 1));
        }
        if expected.is_some_and(|(_, n)| n != row_page_count) {
            return Err(Error::InvalidHeader(
                "column page: a column part is missing a row page",
            ));
        }
        Self::check_zones(&zones, &row_pages, &cells)?;
        let zones_len = Self::check_zone_blocks(&zone_blocks, &zones, &cells)?;
        Ok(Self {
            row_count,
            group_tag,
            row_pages,
            row_page_starts,
            entries,
            zones,
            zone_blocks,
            zones_len,
        })
    }

    /// The zone block checks [`Self::new`] documents, returning the blocks'
    /// total length. `cells` are the pages' `(id, row_page)`, sorted.
    fn check_zone_blocks(
        zone_blocks: &[ZoneBlock],
        zones: &PageZones,
        cells: &[(PageId, u16)],
    ) -> Result<u32> {
        let bad = |what| Err(Error::InvalidHeader(what));
        if zone_blocks.len() > usize::from(u16::MAX) {
            return bad("column page: zone block count exceeds the u16 directory field");
        }
        // Sorted lookups, as for the pages: every list here is read from disk.
        let mut columns: Vec<u16> = zone_blocks.iter().map(|b| b.column_id).collect();
        columns.sort_unstable();
        if columns.windows(2).any(|pair| pair.first() == pair.last()) {
            return bad("column page: a column has two zone blocks");
        }
        let mut in_directory = zones.columns.clone();
        in_directory.sort_unstable();
        let mut total: u32 = 0;
        for block in zone_blocks {
            let has_pages = cells
                .binary_search_by(|(id, _)| id.column_id.cmp(&block.column_id))
                .is_ok();
            if !has_pages {
                return bad("column page: a zone block names a column the group does not have");
            }
            if in_directory.binary_search(&block.column_id).is_ok() {
                return bad("column page: a column has zones in the directory and a zone block");
            }
            if block.length == 0 {
                return bad("column page: an empty zone block");
            }
            total = total.checked_add(block.length).ok_or(Error::InvalidHeader(
                "column page: zone blocks overflow u32",
            ))?;
        }
        Ok(total)
    }

    /// Appends the payload of a zone block of the group `group_tag` names:
    /// the tag, then `zones`, which hold one column's zones.
    pub fn encode_zone_block(group_tag: u64, zones: &PageZones, out: &mut Vec<u8>) {
        out.extend_from_slice(&group_tag.to_le_bytes());
        zones.encode_into(out);
    }

    /// Parses the payload of the zone block this directory lists for column
    /// `column_id`: this group's tag, then that column's zones, and nothing
    /// else.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidHeader`] for a truncated payload, trailing bytes, the
    /// tag of another group, zones of any column but `column_id`, or zones
    /// [`Self::new`] would refuse. A block of another group or holding another
    /// column's zones is a block in another's place, and its zones would prune
    /// this group's row pages by other values.
    pub fn decode_zone_block(&self, column_id: u16, bytes: &[u8]) -> Result<PageZones> {
        let Some((tag, mut rest)) = bytes.split_first_chunk::<8>() else {
            return Err(Error::InvalidHeader(
                "ColumnZones: payload shorter than its group tag",
            ));
        };
        if u64::from_le_bytes(*tag) != self.group_tag {
            return Err(Error::InvalidHeader(
                "ColumnZones: the zone block belongs to another row group",
            ));
        }
        let zones = PageZones::decode_from(&mut rest, self.row_pages.len())?;
        if !rest.is_empty() {
            return Err(Error::InvalidHeader(
                "ColumnZones: trailing bytes after the declared zones",
            ));
        }
        if zones.columns != [column_id] {
            return Err(Error::InvalidHeader(
                "ColumnZones: a zone block holds zones of another column than its own",
            ));
        }
        let mut cells: Vec<(PageId, u16)> =
            self.entries.iter().map(|e| (e.id, e.row_page)).collect();
        cells.sort_unstable();
        Self::check_zones(&zones, &self.row_pages, &cells)?;
        Ok(zones)
    }

    /// The zone checks [`Self::new`] documents. `cells` are the pages'
    /// `(id, row_page)`, sorted.
    fn check_zones(zones: &PageZones, row_pages: &[u32], cells: &[(PageId, u16)]) -> Result<()> {
        let bad = |what| Err(Error::InvalidHeader(what));
        if zones.columns.len() > usize::from(u16::MAX) {
            return bad("column page: zone column count exceeds the u16 directory field");
        }
        let mut columns = zones.columns.clone();
        columns.sort_unstable();
        if columns.windows(2).any(|pair| pair.first() == pair.last()) {
            return bad("column page: a column has two zones");
        }
        for column_id in &columns {
            let has_pages = cells
                .binary_search_by(|(id, _)| id.column_id.cmp(column_id))
                .is_ok();
            if !has_pages {
                return bad("column page: a zone names a column the group does not have");
            }
        }
        if Some(zones.slots.len()) != row_pages.len().checked_mul(zones.columns.len()) {
            return bad("column page: zones do not cover every row page and zone column");
        }
        for (index, rows) in row_pages
            .iter()
            .flat_map(|&rows| core::iter::repeat_n(rows, zones.columns.len()))
            .enumerate()
        {
            let Some(zone) = zones.get(index) else {
                return bad("column page: a zone's bounds lie outside the directory");
            };
            if zone.min.len() > ZONE_BOUND_LEN || zone.max.is_some_and(|m| m.len() > ZONE_BOUND_LEN)
            {
                return bad("column page: a zone bound is longer than a zone keeps");
            }
            if zone.null_count > rows {
                return bad("column page: a zone counts more nulls than its row page has rows");
            }
            // An all-null zone records the empty range; a zone with values
            // may record it too, when those values are all empty.
            let empty = zone.min.is_empty() && zone.max == Some(&[][..]);
            if zone.null_count == rows && !empty {
                return bad("column page: an all-null zone records a range");
            }
            if zone.max.is_some_and(|max| zone.min > max) {
                return bad("column page: a zone's lower bound is above its upper bound");
            }
        }
        Ok(())
    }

    /// Lays `pages` out back to back from the directory's end, in the order
    /// given, each `(id, row_page, on_disk_length)`.
    ///
    /// The writer's layout, stated once: a group writes its directory and then
    /// its pages with no gap, so a page's offset is the sum of the lengths
    /// before it. A reader that finds a gap is reading a directory no writer
    /// produced.
    ///
    /// # Errors
    ///
    /// As [`Self::new`], plus [`Error::InvalidHeader`] when the pages' total
    /// length does not fit a `u32`.
    pub fn contiguous(
        row_count: u32,
        group_tag: u64,
        row_pages: Vec<u32>,
        pages: impl IntoIterator<Item = (PageId, u16, u32)>,
        zones: PageZones,
        zone_blocks: Vec<ZoneBlock>,
    ) -> Result<Self> {
        let mut offset: u32 = 0;
        let mut entries = Vec::new();
        for (id, row_page, length) in pages {
            entries.push(PageEntry {
                offset,
                length,
                id,
                row_page,
            });
            offset = offset.checked_add(length).ok_or(Error::InvalidHeader(
                "column page: group length overflows u32",
            ))?;
        }
        Self::new(row_count, group_tag, row_pages, entries, zones, zone_blocks)
    }

    /// The zone blocks after the pages, in the order they follow them.
    #[must_use]
    pub fn zone_blocks(&self) -> &[ZoneBlock] {
        &self.zone_blocks
    }

    /// Where column `column_id`'s zone block starts, measured from the end
    /// of the pages, and its on-disk length; `None` when the column has none.
    #[must_use]
    pub fn zone_block(&self, column_id: u16) -> Option<(u32, u32)> {
        let mut start: u32 = 0;
        for block in &self.zone_blocks {
            if block.column_id == column_id {
                return Some((start, block.length));
            }
            // `new` proved the lengths sum within a u32.
            start = start.wrapping_add(block.length);
        }
        None
    }

    /// The on-disk length of the whole group this directory describes, given
    /// the directory's own on-disk length: the directory, its pages, then its
    /// zone blocks. `None` when it overflows a `u32`. Every walk that frames a
    /// group from its directory takes its extent from here, so none of them
    /// can leave a part of the group out.
    #[must_use]
    pub fn group_len(&self, directory_len: u32) -> Option<u32> {
        directory_len
            .checked_add(self.pages_len())?
            .checked_add(self.zones_len)
    }

    /// Total on-disk length of the pages, which is also where the last page
    /// ends relative to the directory.
    #[must_use]
    pub fn pages_len(&self) -> u32 {
        // `new` proved every extent fits a u32, and the last page ends
        // furthest because the entries are ascending and non-overlapping.
        self.entries
            .last()
            .map_or(0, |e| e.offset.wrapping_add(e.length))
    }

    /// Rows in the group.
    #[must_use]
    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Rows in each row page, in row order.
    #[must_use]
    pub fn row_pages(&self) -> &[u32] {
        &self.row_pages
    }

    /// The first row of row page `row_page` within the group, or `None` when
    /// there is no such row page.
    #[must_use]
    pub fn row_page_start(&self, row_page: u16) -> Option<u32> {
        self.row_page_starts.get(usize::from(row_page)).copied()
    }

    /// Rows in row page `row_page`, or `None` when there is no such row page.
    #[must_use]
    pub fn row_page_rows(&self, row_page: u16) -> Option<u32> {
        self.row_pages.get(usize::from(row_page)).copied()
    }

    /// The tag every page of this group carries in its [`PageStamp`].
    #[must_use]
    pub fn group_tag(&self) -> u64 {
        self.group_tag
    }

    /// The stamp the page at `entry` must carry.
    #[must_use]
    pub fn stamp_for(&self, entry: &PageEntry) -> PageStamp {
        PageStamp {
            group_tag: self.group_tag,
            id: entry.id,
            row_page: entry.row_page,
        }
    }

    /// The pages, in ascending offset order.
    #[must_use]
    pub fn entries(&self) -> &[PageEntry] {
        &self.entries
    }

    /// The statistics zones the directory carries: the key column's.
    #[must_use]
    pub fn zones(&self) -> &PageZones {
        &self.zones
    }

    /// The bytes the decoded directory holds on the heap, which is what it
    /// costs the block cache to keep it.
    #[must_use]
    pub fn heap_size(&self) -> usize {
        use core::mem::size_of;

        (self.row_pages.len() + self.row_page_starts.len()) * size_of::<u32>()
            + self.entries.len() * size_of::<PageEntry>()
            + self.zone_blocks.len() * size_of::<ZoneBlock>()
            + self.zones.columns.len() * size_of::<u16>()
            + self.zones.slots.len() * size_of::<ZoneSlot>()
            + self.zones.bounds.len()
    }

    /// Serializes the directory into `out`.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.reserve(
            HEADER_LEN
                + self.row_pages.len() * ROW_PAGE_LEN
                + self.entries.len() * ENTRY_LEN
                + self.zone_blocks.len() * ZONE_BLOCK_LEN
                + self.zones.encoded_len(),
        );
        out.push(VERSION);
        // `new` is the only constructor and refuses more than u16::MAX pages
        // or row pages, so the conversions cannot fail and nothing is
        // truncated.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the page count is bounded to u16::MAX by `new`"
        )]
        let count = self.entries.len() as u16;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the row page count is bounded to u16::MAX by `new`"
        )]
        let row_page_count = self.row_pages.len() as u16;
        out.extend_from_slice(&count.to_le_bytes());
        out.extend_from_slice(&self.row_count.to_le_bytes());
        out.extend_from_slice(&self.group_tag.to_le_bytes());
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the zone block count is bounded to u16::MAX by `new`"
        )]
        let zone_block_count = self.zone_blocks.len() as u16;
        out.extend_from_slice(&row_page_count.to_le_bytes());
        out.extend_from_slice(&zone_block_count.to_le_bytes());
        for rows in &self.row_pages {
            out.extend_from_slice(&rows.to_le_bytes());
        }
        for entry in &self.entries {
            out.extend_from_slice(&entry.offset.to_le_bytes());
            out.extend_from_slice(&entry.length.to_le_bytes());
            out.extend_from_slice(&entry.id.column_id.to_le_bytes());
            out.push(entry.id.part);
            out.push(0);
            out.extend_from_slice(&entry.row_page.to_le_bytes());
        }
        for block in &self.zone_blocks {
            out.extend_from_slice(&block.column_id.to_le_bytes());
            out.extend_from_slice(&block.length.to_le_bytes());
        }
        self.zones.encode_into(out);
    }

    /// Parses a directory payload.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] for an unknown version, a truncated
    /// payload, trailing bytes after the declared entries, a set reserved
    /// flag bit, or a directory [`Self::new`] would refuse. Trailing bytes are
    /// refused rather than ignored: a directory longer than it declares is
    /// either a writer this build does not understand or a corruption that a
    /// lenient parse would carry into every page lookup.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        const ERR: Error = Error::InvalidHeader("ColumnPageDirectory");

        let mut rest = bytes;
        let [version] = take::<1>(&mut rest).ok_or(ERR)?;
        if version != VERSION {
            return Err(Error::InvalidHeader(
                "ColumnPageDirectory: unknown directory version",
            ));
        }
        let count = usize::from(u16::from_le_bytes(take(&mut rest).ok_or(ERR)?));
        let row_count = u32::from_le_bytes(take(&mut rest).ok_or(ERR)?);
        let group_tag = u64::from_le_bytes(take(&mut rest).ok_or(ERR)?);
        let row_page_count = usize::from(u16::from_le_bytes(take(&mut rest).ok_or(ERR)?));
        let zone_block_count = usize::from(u16::from_le_bytes(take(&mut rest).ok_or(ERR)?));

        // The declared counts are on-disk data, so they bound nothing until
        // the bytes behind them are seen to exist: reserve for what the
        // payload can actually hold, not for what the header claims.
        let mut row_pages = Vec::with_capacity(row_page_count.min(rest.len() / ROW_PAGE_LEN));
        for _ in 0..row_page_count {
            row_pages.push(u32::from_le_bytes(take(&mut rest).ok_or(ERR)?));
        }
        let mut entries = Vec::with_capacity(count.min(rest.len() / ENTRY_LEN));
        for _ in 0..count {
            let offset = u32::from_le_bytes(take(&mut rest).ok_or(ERR)?);
            let length = u32::from_le_bytes(take(&mut rest).ok_or(ERR)?);
            let column_id = u16::from_le_bytes(take(&mut rest).ok_or(ERR)?);
            let [part] = take::<1>(&mut rest).ok_or(ERR)?;
            let [flags] = take::<1>(&mut rest).ok_or(ERR)?;
            let row_page = u16::from_le_bytes(take(&mut rest).ok_or(ERR)?);
            if flags != 0 {
                return Err(Error::InvalidHeader(
                    "ColumnPageDirectory: reserved page flag set",
                ));
            }
            entries.push(PageEntry {
                offset,
                length,
                id: PageId { column_id, part },
                row_page,
            });
        }
        let mut zone_blocks = Vec::with_capacity(zone_block_count.min(rest.len() / ZONE_BLOCK_LEN));
        for _ in 0..zone_block_count {
            let column_id = u16::from_le_bytes(take(&mut rest).ok_or(ERR)?);
            let length = u32::from_le_bytes(take(&mut rest).ok_or(ERR)?);
            zone_blocks.push(ZoneBlock { column_id, length });
        }
        let zones = PageZones::decode_from(&mut rest, row_page_count)?;
        if !rest.is_empty() {
            return Err(Error::InvalidHeader(
                "ColumnPageDirectory: trailing bytes after the declared zones",
            ));
        }
        Self::new(row_count, group_tag, row_pages, entries, zones, zone_blocks)
    }
}

/// Takes the next `N` bytes off the front of `bytes` as an array, or `None`
/// when fewer remain. Manual little-endian parsing keeps this codec
/// `core` + `alloc` clean, like the sibling section codecs, and the fixed-size
/// array makes every field read infallible once taken.
fn take<const N: usize>(bytes: &mut &[u8]) -> Option<[u8; N]> {
    let (head, tail) = bytes.split_first_chunk::<N>()?;
    *bytes = tail;
    Some(*head)
}

/// Takes the next `len` bytes off the front of `bytes`, or `None` when fewer
/// remain.
fn take_slice<'a>(bytes: &mut &'a [u8], len: usize) -> Option<&'a [u8]> {
    let (head, tail) = bytes.split_at_checked(len)?;
    *bytes = tail;
    Some(head)
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code: a failed expectation is the assertion"
)]
mod tests;
