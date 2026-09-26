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
//! Counts, row counts and lengths are LEB128 varints (`var`), which is what
//! keeps a directory small next to the pages it lists.
//!
//! ```text
//! [version         : u8    ]  currently 3; an unknown version is refused
//! [group_tag       : u64 LE]  names the group; every page carries it in its stamp
//! [row_page_count  : var   ]  at most u16::MAX
//! [part_count      : var   ]  column parts; parts times row pages at most u16::MAX
//! [zone_block_count: var   ]  zone blocks after the pages, at most u16::MAX
//! [head_zones_len  : var   ]  on-disk length of the zone block between the
//!                             directory and the pages, zero for none
//! [head_column     : u16 LE]  its column; present only when it has one
//! repeated part_count times, in the order their pages lie:
//!   [column_id: u16 LE]  the column the part belongs to
//!   [part     : u8    ]  which part of that column's encoding it is
//! repeated row_page_count times:
//!   [rows : var]  rows in the row page, non-zero; the group's rows are their sum
//! repeated part_count * row_page_count times, part by part, row page by row page:
//!   [length : var]  on-disk length of the page, its header included
//! repeated zone_block_count times, in the order they follow the pages:
//!   [column_id: u16 LE]  the column whose zones the block holds
//!   [length   : var   ]  on-disk length of the block, its header included
//! ```
//!
//! The pages lie back to back, from the end of the directory and of the head
//! zone block, in exactly that order, so a page's offset is the sum of the
//! lengths before it and its column part and row page are its place in the
//! list: every `(column_id, part)` has one page per row page, and a row page
//! can always be assembled from its own pages.
//!
//! Each zone block's payload opens with its group's `[group_tag: u64 LE]`,
//! which a reader checks against the directory, so a zone block moved into
//! another group's place is refused like a page with another group's stamp,
//! then holds one column's zones:
//!
//! ```text
//! [zone_column_count: var]  at most u16::MAX
//! repeated zone_column_count times:
//!   [column_id: u16 LE]  a column the zones describe
//! repeated row_page_count * zone_column_count times, row page by row page:
//!   [null_count: var]  null rows of the column in the row page
//!   [flags     : u8 ]  bit 0: no upper bound; other bits reserved
//!   [min       : bound]
//!   [max       : bound]  absent without an upper bound
//! bound:
//!   [shared: u8]  bytes it shares with its reference, a prefix of it
//!   [suffix_len: u8]
//!   [suffix: suffix_len bytes]  the bound is the shared prefix, then these
//! ```
//!
//! A lower bound's reference is the same column's zone on the row page before,
//! its upper bound when it has one and its lower bound otherwise, and nothing
//! on the first row page; an upper bound's reference is its own lower bound.
//! Neighbouring zones of a sorted column, and the two bounds of one zone of a
//! narrow number, share most of their bytes.
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
//! Every column's zones are a
//! [`ColumnZones`](crate::table::block::BlockType::ColumnZones) block of its
//! own, read only by a read that prunes on that column: a full scan or a
//! projection that does not prune pays for none of them, and one that prunes
//! on a narrow column does not pay for a wide column's zones. The key
//! column's block lies right after the directory, so a point read takes both
//! in one request and goes from them to the one key page that can hold its
//! key; every other column's lies after the pages.
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
pub const VERSION: u8 = 3;

/// The longest varint a `u16` field takes.
const VAR_U16_MAX_LEN: usize = 3;

/// The longest varint a `u32` field takes.
const VAR_U32_MAX_LEN: usize = 5;

/// `column_id` + `part` of one column part.
const PART_LEN: usize = 2 + 1;

/// One zone block after a group's pages: whose zones it holds and how long
/// it is on disk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZoneBlock {
    /// The column whose zones the block holds.
    pub column_id: u16,
    /// On-disk length of the block, its header included.
    pub length: u32,
}

/// The most bytes of a bound a statistics zone keeps. The value Parquet's page
/// index truncates its statistics to by default: long enough that keys and
/// short fields keep their exact bounds, short enough that a zone of a wide
/// value costs a small fraction of its page.
pub const ZONE_BOUND_LEN: usize = 64;

/// Zone flag: the zone has no upper bound.
const ZONE_MAX_UNBOUNDED: u8 = 1;

/// The fewest bytes a zone takes on the wire: a one-byte `null_count`, its
/// `flags` and a lower bound's `shared` and `suffix_len`.
const ZONE_MIN_LEN: usize = 1 + 1 + 2;

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

    /// The reference the lower bound of the zone at `index` is written
    /// against: the same column's zone on the row page before, its upper
    /// bound when it has one and its lower bound otherwise; nothing on the
    /// first row page.
    fn min_reference(&self, index: usize) -> &[u8] {
        index
            .checked_sub(self.columns.len())
            .and_then(|before| self.get(before))
            .map_or(&[], |zone| zone.max.unwrap_or(zone.min))
    }

    /// Appends the zones' wire form to `out`.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        // A zone's fixed fields and its bounds' suffixes, at most.
        out.reserve(
            VAR_U16_MAX_LEN
                + self.columns.len() * 2
                + self.slots.len() * (VAR_U32_MAX_LEN + 1 + 4)
                + self.bounds.len(),
        );
        put_varint(out, self.columns.len() as u64);
        for column_id in &self.columns {
            out.extend_from_slice(&column_id.to_le_bytes());
        }
        for index in 0..self.slots.len() {
            let Some(zone) = self.get(index) else {
                continue;
            };
            put_varint(out, u64::from(zone.null_count));
            out.push(if zone.max.is_none() {
                ZONE_MAX_UNBOUNDED
            } else {
                0
            });
            put_bound(out, self.min_reference(index), zone.min);
            if let Some(max) = zone.max {
                put_bound(out, zone.min, max);
            }
        }
    }

    /// Reads zones for `row_page_count` row pages off the front of `rest`.
    fn decode_from(rest: &mut &[u8], row_page_count: usize) -> Result<Self> {
        // Each field is taken in a branch that builds the refusal only when it
        // refuses, as the directory's decoder does: a zone block is decoded on
        // every read that selects by its column.
        const ERR: Error = Error::InvalidHeader("ColumnZones");

        let Some(column_count) = take_var_u16(rest).map(usize::from) else {
            return Err(ERR);
        };
        let mut columns = Vec::with_capacity(column_count.min(rest.len() / 2));
        for _ in 0..column_count {
            let Some(column_id) = take(rest).map(u16::from_le_bytes) else {
                return Err(ERR);
            };
            columns.push(column_id);
        }
        let Some(count) = row_page_count.checked_mul(column_count) else {
            return Err(ERR);
        };
        let mut zones = Self::new(columns);
        zones.slots.reserve(count.min(rest.len() / ZONE_MIN_LEN));
        let mut min_buf = [0u8; ZONE_BOUND_LEN];
        let mut max_buf = [0u8; ZONE_BOUND_LEN];
        for index in 0..count {
            let (Some(null_count), Some([flags])) = (take_var_u32(rest), take::<1>(rest)) else {
                return Err(ERR);
            };
            let Some(min) = take_bound(rest, zones.min_reference(index), &mut min_buf)
                .and_then(|len| min_buf.get(..len))
            else {
                return Err(ERR);
            };
            let max = match flags {
                0 => {
                    let Some(max) =
                        take_bound(rest, min, &mut max_buf).and_then(|len| max_buf.get(..len))
                    else {
                        return Err(ERR);
                    };
                    Some(max)
                }
                ZONE_MAX_UNBOUNDED => None,
                _ => return Err(Error::InvalidHeader("ColumnZones: reserved zone flag set")),
            };
            zones.push_bounds(null_count, min, max);
        }
        Ok(zones)
    }
}

/// Appends `bound` as the wire writes it against `reference`: the length of
/// the prefix they share, then the rest of `bound`.
fn put_bound(out: &mut Vec<u8>, reference: &[u8], bound: &[u8]) {
    let shared = reference
        .iter()
        .zip(bound)
        .take_while(|(a, b)| a == b)
        .count();
    let suffix = bound.get(shared..).unwrap_or_default();
    // A bound's length is a `u8` wherever it is kept (`ZoneSlot`), so its
    // shared prefix and suffix are too.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a zone bound is below 256 bytes"
    )]
    out.extend_from_slice(&[shared as u8, suffix.len() as u8]);
    out.extend_from_slice(suffix);
}

/// Reads a bound written against `reference` off the front of `rest` into
/// `out`, returning its length; `None` when it is truncated, shares more than
/// `reference` holds, or is longer than a zone keeps.
fn take_bound(rest: &mut &[u8], reference: &[u8], out: &mut [u8; ZONE_BOUND_LEN]) -> Option<usize> {
    let [shared, suffix_len] = take::<2>(rest)?;
    let (shared, suffix_len) = (usize::from(shared), usize::from(suffix_len));
    let prefix = reference.get(..shared)?;
    let suffix = take_slice(rest, suffix_len)?;
    let len = shared + suffix_len;
    let bound = out.get_mut(..len)?;
    let (head, tail) = bound.split_at_mut(shared);
    head.copy_from_slice(prefix);
    tail.copy_from_slice(suffix);
    Some(len)
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
    /// The column parts the pages hold, sorted, for the lookups that check a
    /// column has pages.
    sorted_parts: Vec<PageId>,
    /// The zone block between the directory and the pages, when the group has
    /// one: a writer puts the key column's there, which a point read needs
    /// with the directory and a scan pruning on another column does not.
    head_zone_block: Option<ZoneBlock>,
    /// The head zone block's zones once a read has decoded them, so a cached
    /// directory serves them without decoding the block again; empty before.
    head_zones: PageZones,
    /// The zone blocks after the pages, in the order they follow them.
    zone_blocks: Vec<ZoneBlock>,
    /// Their total on-disk length.
    zones_len: u32,
}

impl PageDirectory {
    /// Builds a directory for a group of `row_count` rows tagged `group_tag`,
    /// cut into row pages of `row_pages` rows each, from its pages' entries,
    /// the zone block that lies between the directory and the pages
    /// (`head_zone_block`), and the zone blocks that follow the pages
    /// (`zone_blocks`): one per column whose row pages carry zones.
    ///
    /// The entries are the pages as a writer lays them out: column part by
    /// column part, each part's pages row page by row page, back to back from
    /// the directory's end. That is the layout the wire form records, and the
    /// one that makes a run of one column's pages one contiguous read.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] if there are more pages or row pages
    /// than the wire's `u16` counts hold, if a row page is empty or the row
    /// pages do not sum to `row_count`, if a page does not start where the one
    /// before it ends, if a page's extent does not fit a `u32`, if a page names
    /// a row page that does not exist, or if the pages are not a complete grid
    /// in that order: every `(column_id, part)` holding one page for each row
    /// page, in row page order, and no part appearing twice. Every one of
    /// these is rejected here rather than at read time because none has a
    /// correct encoding, and the writer is where that is still fixable: a
    /// directory that maps one byte into two pages, or one row page's part to
    /// two pages, or leaves a row page's part out, has no correct reading, and
    /// one with more pages than its count field could only be written by
    /// truncating the list or by writing a count its entries contradict.
    ///
    /// The zone blocks, the head one included, are refused unless each names a
    /// distinct column the group has, and their lengths are non-zero and sum
    /// within a `u32`.
    pub fn new(
        row_count: u32,
        group_tag: u64,
        row_pages: Vec<u32>,
        entries: Vec<PageEntry>,
        head_zone_block: Option<ZoneBlock>,
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

        let row_page_count = row_pages.len();
        let mut end_of_previous: u32 = 0;
        for (index, entry) in entries.iter().enumerate() {
            if entry.offset != end_of_previous {
                return Err(Error::InvalidHeader(
                    "column page: a page does not start where the one before it ends",
                ));
            }
            end_of_previous = entry
                .offset
                .checked_add(entry.length)
                .ok_or(Error::InvalidHeader("column page: extent overflows u32"))?;
            if usize::from(entry.row_page) >= row_page_count {
                return Err(Error::InvalidHeader(
                    "column page: a page names a row page that does not exist",
                ));
            }
            // A part's pages run through every row page in order before the
            // next part's start, so each page's place names its row page and
            // the part whose run it is in.
            let run_start = index - index % row_page_count;
            let part = entries.get(run_start).map(|first| first.id);
            if usize::from(entry.row_page) != index % row_page_count || part != Some(entry.id) {
                return Err(Error::InvalidHeader(
                    "column page: a column part is missing a row page",
                ));
            }
        }
        // With no row pages the loop above refused any page, and an empty list
        // is a multiple of zero.
        if !entries.len().is_multiple_of(row_page_count) {
            return Err(Error::InvalidHeader(
                "column page: a column part is missing a row page",
            ));
        }
        // Sorted-neighbour checks rather than pairwise ones. The directory is
        // read from disk, so its page count is whatever the bytes say, up to
        // u16::MAX, and a pairwise check would turn one corrupt block into
        // billions of comparisons on the read path.
        let mut sorted_parts: Vec<PageId> = entries
            .iter()
            .step_by(row_page_count.max(1))
            .map(|e| e.id)
            .collect();
        sorted_parts.sort_unstable();
        if sorted_parts
            .windows(2)
            .any(|pair| pair.first() == pair.last())
        {
            return Err(Error::InvalidHeader(
                "column page: two pages claim the same column part and row page",
            ));
        }
        let zones_len = Self::check_zone_blocks(head_zone_block, &zone_blocks, &sorted_parts)?;
        Ok(Self {
            row_count,
            group_tag,
            row_pages,
            row_page_starts,
            entries,
            sorted_parts,
            head_zone_block,
            head_zones: PageZones::default(),
            zone_blocks,
            zones_len,
        })
    }

    /// The zone block checks [`Self::new`] documents, returning the total
    /// length of the blocks after the pages. `parts` are the group's column
    /// parts, sorted.
    fn check_zone_blocks(
        head: Option<ZoneBlock>,
        zone_blocks: &[ZoneBlock],
        parts: &[PageId],
    ) -> Result<u32> {
        let bad = |what| Err(Error::InvalidHeader(what));
        if zone_blocks.len() > usize::from(u16::MAX) {
            return bad("column page: zone block count exceeds the u16 directory field");
        }
        // Sorted lookups, as for the pages: every list here is read from disk.
        let mut columns: Vec<u16> = head
            .iter()
            .chain(zone_blocks)
            .map(|b| b.column_id)
            .collect();
        columns.sort_unstable();
        if columns.windows(2).any(|pair| pair.first() == pair.last()) {
            return bad("column page: a column has two zone blocks");
        }
        let mut total: u32 = 0;
        for (block, after_pages) in head
            .iter()
            .map(|b| (b, false))
            .chain(zone_blocks.iter().map(|b| (b, true)))
        {
            let has_pages = parts
                .binary_search_by(|id| id.column_id.cmp(&block.column_id))
                .is_ok();
            if !has_pages {
                return bad("column page: a zone block names a column the group does not have");
            }
            if block.length == 0 {
                return bad("column page: an empty zone block");
            }
            if after_pages {
                total = total.checked_add(block.length).ok_or(Error::InvalidHeader(
                    "column page: zone blocks overflow u32",
                ))?;
            }
        }
        Ok(total)
    }

    /// This directory with the zones of its head zone block, decoded from that
    /// block, kept on it: a cached directory then serves them to every read
    /// that selects by that column without the block being read or decoded
    /// again.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidHeader`] when the group has no head zone block, or
    /// `zones` are not that block's column's alone or fail the checks
    /// [`Self::decode_zone_block`] applies.
    pub fn with_head_zones(mut self, zones: PageZones) -> Result<Self> {
        let Some(head) = self.head_zone_block else {
            return Err(Error::InvalidHeader(
                "column page: head zones for a group without a head zone block",
            ));
        };
        if zones.columns != [head.column_id] {
            return Err(Error::InvalidHeader(
                "ColumnZones: a zone block holds zones of another column than its own",
            ));
        }
        Self::check_zones(&zones, &self.row_pages, &self.sorted_parts)?;
        self.head_zones = zones;
        Ok(self)
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
        Self::check_zones(&zones, &self.row_pages, &self.sorted_parts)?;
        Ok(zones)
    }

    /// The zone checks [`Self::new`] documents. `parts` are the group's column
    /// parts, sorted.
    fn check_zones(zones: &PageZones, row_pages: &[u32], parts: &[PageId]) -> Result<()> {
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
            let has_pages = parts
                .binary_search_by(|id| id.column_id.cmp(column_id))
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

    /// Lays `pages` out back to back, in the order given, each
    /// `(id, row_page, on_disk_length)`, offsets measured from where the
    /// first page starts.
    ///
    /// The writer's layout, stated once: a group writes its directory, its
    /// head zone block when it has one, and then its pages with no gap, so a
    /// page's offset is the sum of the lengths before it. A reader that finds
    /// a gap is reading a directory no writer produced.
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
        head_zone_block: Option<ZoneBlock>,
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
        Self::new(
            row_count,
            group_tag,
            row_pages,
            entries,
            head_zone_block,
            zone_blocks,
        )
    }

    /// The zone block between the directory and the pages, when the group has
    /// one.
    #[must_use]
    pub fn head_zone_block(&self) -> Option<ZoneBlock> {
        self.head_zone_block
    }

    /// The zone blocks after the pages, in the order they follow them.
    #[must_use]
    pub fn zone_blocks(&self) -> &[ZoneBlock] {
        &self.zone_blocks
    }

    /// Where the pages start, measured from the end of the directory: after
    /// the head zone block, when there is one.
    #[must_use]
    pub fn pages_start(&self) -> u32 {
        self.head_zone_block.map_or(0, |head| head.length)
    }

    /// Where column `column_id`'s zone block starts, measured from the end of
    /// the directory, and its on-disk length; `None` when the column has none.
    #[must_use]
    pub fn zone_block(&self, column_id: u16) -> Option<(u32, u32)> {
        if let Some(head) = self.head_zone_block
            && head.column_id == column_id
        {
            return Some((0, head.length));
        }
        // `new` and `group_len` proved these sums fit a u32 for any group
        // they framed.
        let mut start = self.pages_start().wrapping_add(self.pages_len());
        for block in &self.zone_blocks {
            if block.column_id == column_id {
                return Some((start, block.length));
            }
            start = start.wrapping_add(block.length);
        }
        None
    }

    /// The on-disk length of the whole group this directory describes, given
    /// the directory's own on-disk length: the directory, its head zone block,
    /// its pages, then its other zone blocks. `None` when it overflows a
    /// `u32`. Every walk that frames a group from its directory takes its
    /// extent from here, so none of them can leave a part of the group out.
    #[must_use]
    pub fn group_len(&self, directory_len: u32) -> Option<u32> {
        directory_len
            .checked_add(self.pages_start())?
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

    /// The zones of the head zone block, once [`Self::with_head_zones`] kept
    /// them on this directory; empty until then.
    #[must_use]
    pub fn head_zones(&self) -> &PageZones {
        &self.head_zones
    }

    /// Whether the head zone block holds column `column_id`'s zones and they
    /// are not kept on this directory yet: a read selecting by that column has
    /// to read the block first.
    #[must_use]
    pub fn lacks_head_zones_for(&self, column_id: u16) -> bool {
        self.head_zone_block
            .is_some_and(|head| head.column_id == column_id)
            && self.head_zones.columns.is_empty()
    }

    /// The bytes the decoded directory holds on the heap, which is what it
    /// costs the block cache to keep it.
    #[must_use]
    pub fn heap_size(&self) -> usize {
        use core::mem::size_of;

        (self.row_pages.len() + self.row_page_starts.len()) * size_of::<u32>()
            + (self.entries.len() * size_of::<PageEntry>())
            + (self.sorted_parts.len() * size_of::<PageId>())
            + self.zone_blocks.len() * size_of::<ZoneBlock>()
            + self.head_zones.columns.len() * size_of::<u16>()
            + self.head_zones.slots.len() * size_of::<ZoneSlot>()
            + self.head_zones.bounds.len()
    }

    /// Serializes the directory into `out`.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let row_page_count = self.row_pages.len();
        // `new` proved the pages are one run of every row page per column
        // part, so each part is named by the first page of its run.
        let parts = self.entries.iter().step_by(row_page_count.max(1));
        let part_count = parts.len();
        out.reserve(
            1 + 8
                + 3 * VAR_U16_MAX_LEN
                + part_count * PART_LEN
                + (row_page_count + self.entries.len()) * VAR_U32_MAX_LEN
                + self.zone_blocks.len() * (2 + VAR_U32_MAX_LEN),
        );
        out.push(VERSION);
        out.extend_from_slice(&self.group_tag.to_le_bytes());
        put_varint(out, row_page_count as u64);
        put_varint(out, part_count as u64);
        put_varint(out, self.zone_blocks.len() as u64);
        // The head zone block's length, zero for none, then its column.
        put_varint(out, u64::from(self.pages_start()));
        if let Some(head) = self.head_zone_block {
            out.extend_from_slice(&head.column_id.to_le_bytes());
        }
        for entry in parts {
            out.extend_from_slice(&entry.id.column_id.to_le_bytes());
            out.push(entry.id.part);
        }
        for &rows in &self.row_pages {
            put_varint(out, u64::from(rows));
        }
        for entry in &self.entries {
            put_varint(out, u64::from(entry.length));
        }
        for block in &self.zone_blocks {
            out.extend_from_slice(&block.column_id.to_le_bytes());
            put_varint(out, u64::from(block.length));
        }
    }

    /// Parses a directory payload.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] for an unknown version, a truncated
    /// payload, a count or length past its field's width, trailing bytes
    /// after the declared zones, or a directory [`Self::new`] would refuse.
    /// Trailing bytes are refused rather than ignored: a directory longer than
    /// it declares is either a writer this build does not understand or a
    /// corruption that a lenient parse would carry into every page lookup.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        const ERR: Error = Error::InvalidHeader("ColumnPageDirectory");

        // Every field is taken in a branch that builds the refusal only when
        // it refuses: a directory is decoded on every read that finds none
        // cached, and an eagerly built error would be built and dropped per
        // field on the way to success.
        let mut rest = bytes;
        let Some([version]) = take::<1>(&mut rest) else {
            return Err(ERR);
        };
        if version != VERSION {
            return Err(Error::InvalidHeader(
                "ColumnPageDirectory: unknown directory version",
            ));
        }
        let (Some(group_tag), Some(row_page_count), Some(part_count), Some(zone_block_count)) = (
            take(&mut rest).map(u64::from_le_bytes),
            take_var_u16(&mut rest),
            take_var_u16(&mut rest),
            take_var_u16(&mut rest),
        ) else {
            return Err(ERR);
        };
        let (row_page_count, part_count, zone_block_count) = (
            usize::from(row_page_count),
            usize::from(part_count),
            usize::from(zone_block_count),
        );
        let Some(head_len) = take_var_u32(&mut rest) else {
            return Err(ERR);
        };
        let head_zone_block = if head_len == 0 {
            None
        } else {
            let Some(column_id) = take(&mut rest).map(u16::from_le_bytes) else {
                return Err(ERR);
            };
            Some(ZoneBlock {
                column_id,
                length: head_len,
            })
        };
        // Two counts of at most `u16::MAX` multiply within a `usize`.
        let page_count = part_count * row_page_count;
        if page_count > usize::from(u16::MAX) {
            return Err(Error::InvalidHeader(
                "column page: page count exceeds the u16 directory field",
            ));
        }

        // The declared counts are on-disk data, so they bound nothing until
        // the bytes behind them are seen to exist: reserve for what the
        // payload can actually hold, not for what the header claims.
        let mut parts = Vec::with_capacity(part_count.min(rest.len() / PART_LEN));
        for _ in 0..part_count {
            let (Some(column_id), Some([part])) = (
                take(&mut rest).map(u16::from_le_bytes),
                take::<1>(&mut rest),
            ) else {
                return Err(ERR);
            };
            parts.push(PageId { column_id, part });
        }
        let mut row_pages = Vec::with_capacity(row_page_count.min(rest.len()));
        let mut row_count: u32 = 0;
        for _ in 0..row_page_count {
            let Some(rows) = take_var_u32(&mut rest) else {
                return Err(ERR);
            };
            let Some(sum) = row_count.checked_add(rows) else {
                return Err(Error::InvalidHeader(
                    "column page: row pages overflow the row count",
                ));
            };
            row_count = sum;
            row_pages.push(rows);
        }
        let mut entries = Vec::with_capacity(page_count.min(rest.len()));
        let mut offset: u32 = 0;
        for id in &parts {
            // At most `u16::MAX` row pages, a `u16` field.
            for row_page in (0u16..).take(row_page_count) {
                let Some(length) = take_var_u32(&mut rest) else {
                    return Err(ERR);
                };
                entries.push(PageEntry {
                    offset,
                    length,
                    id: *id,
                    row_page,
                });
                let Some(end) = offset.checked_add(length) else {
                    return Err(Error::InvalidHeader("column page: extent overflows u32"));
                };
                offset = end;
            }
        }
        let mut zone_blocks = Vec::with_capacity(zone_block_count.min(rest.len() / 3));
        for _ in 0..zone_block_count {
            let (Some(column_id), Some(length)) = (
                take(&mut rest).map(u16::from_le_bytes),
                take_var_u32(&mut rest),
            ) else {
                return Err(ERR);
            };
            zone_blocks.push(ZoneBlock { column_id, length });
        }
        if !rest.is_empty() {
            return Err(Error::InvalidHeader(
                "ColumnPageDirectory: trailing bytes after the declared zone blocks",
            ));
        }
        Self::new(
            row_count,
            group_tag,
            row_pages,
            entries,
            head_zone_block,
            zone_blocks,
        )
    }
}

/// Appends `value` as a LEB128 varint.
fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        // The low seven bits, a byte by construction.
        let low = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(low);
            return;
        }
        out.push(low | 0x80);
    }
}

/// Takes a LEB128 varint of at most `max_len` bytes off the front of `bytes`;
/// `None` when it is truncated or runs longer.
fn take_varint(bytes: &mut &[u8], max_len: usize) -> Option<u64> {
    let mut value = 0u64;
    for (index, &byte) in bytes.iter().take(max_len).enumerate() {
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            *bytes = bytes.get(index + 1..)?;
            return Some(value);
        }
    }
    None
}

/// Takes a varint that must fit a `u16`.
fn take_var_u16(bytes: &mut &[u8]) -> Option<u16> {
    u16::try_from(take_varint(bytes, VAR_U16_MAX_LEN)?).ok()
}

/// Takes a varint that must fit a `u32`.
fn take_var_u32(bytes: &mut &[u8]) -> Option<u32> {
    u32::try_from(take_varint(bytes, VAR_U32_MAX_LEN)?).ok()
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
