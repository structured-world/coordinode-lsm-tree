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
//! repeated row_page_count times:
//!   [rows : u32 LE]  rows in the row page, non-zero; the sum is `row_count`
//! repeated page_count times, ascending by offset, non-overlapping:
//!   [offset   : u32 LE]  start of the page, from the END of the directory
//!   [length   : u32 LE]  on-disk length of the page, its header included
//!   [column_id: u16 LE]  the column the page belongs to
//!   [part     : u8    ]  which part of that column's encoding it holds
//!   [flags    : u8    ]  reserved; a reader refuses unknown bits
//!   [row_page : u16 LE]  which row page's rows it holds
//! ```
//!
//! Every `(column_id, part)` has exactly one page per row page: the pages form
//! a complete grid, so a row page can always be assembled from its own pages.
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

/// `version` + `page_count` + `row_count` + `group_tag` + `row_page_count`.
const HEADER_LEN: usize = 1 + 2 + 4 + 8 + 2;

/// `rows` of one row page.
const ROW_PAGE_LEN: usize = 4;

/// `offset` + `length` + `column_id` + `part` + `flags` + `row_page`.
const ENTRY_LEN: usize = 4 + 4 + 2 + 1 + 1 + 2;

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
}

impl PageDirectory {
    /// Builds a directory for a group of `row_count` rows tagged `group_tag`,
    /// cut into row pages of `row_pages` rows each, from entries already in
    /// ascending offset order.
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
    pub fn new(
        row_count: u32,
        group_tag: u64,
        row_pages: Vec<u32>,
        entries: Vec<PageEntry>,
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
        Ok(Self {
            row_count,
            group_tag,
            row_pages,
            row_page_starts,
            entries,
        })
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
        Self::new(row_count, group_tag, row_pages, entries)
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

    /// Serializes the directory into `out`.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.reserve(
            HEADER_LEN + self.row_pages.len() * ROW_PAGE_LEN + self.entries.len() * ENTRY_LEN,
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
        out.extend_from_slice(&row_page_count.to_le_bytes());
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
        if !rest.is_empty() {
            return Err(Error::InvalidHeader(
                "ColumnPageDirectory: trailing bytes after the declared pages",
            ));
        }
        Self::new(row_count, group_tag, row_pages, entries)
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

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code: a failed expectation is the assertion"
)]
mod tests;
