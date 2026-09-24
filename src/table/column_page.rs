// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The directory of one columnar row group's pages.
//!
//! A row group is laid out as its directory followed by its pages, contiguous
//! in the file. The table index points at the directory, so a row group keeps
//! exactly one index entry and `block_id` keeps its meaning as the group's
//! ordinal in key order; the pages are reached from here.
//!
//! See `docs/columnar-page-format.md` for the layout and
//! `docs/columnar-addressing.md` for why a page may not become an addressing
//! unit.
//!
//! # Wire format
//!
//! ```text
//! [version   : u8    ]  currently 1; an unknown version is refused
//! [page_count: u16 LE]
//! [row_count : u32 LE]  rows in the group; every page describes exactly these
//! repeated page_count times, ascending by offset, non-overlapping:
//!   [offset   : u32 LE]  start of the page, from the END of the directory
//!   [length   : u32 LE]  on-disk length of the page, its header included
//!   [column_id: u16 LE]  the column the page belongs to
//!   [part     : u8    ]  which part of that column's encoding it holds
//!   [flags    : u8    ]  reserved; a reader refuses unknown bits
//! ```
//!
//! Offsets are relative to the group rather than to the file, which is what
//! lets a compaction copy a group whole without rewriting its directory, and
//! is the same reason the block layer refuses to bind a file offset into a
//! block's identity. They are measured from the directory's end, where the
//! first page starts, because the directory's own on-disk length depends on
//! the transforms applied to it and is not known until it is sealed.

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
pub const VERSION: u8 = 1;

/// `version` + `page_count` + `row_count`.
const HEADER_LEN: usize = 1 + 2 + 4;

/// `offset` + `length` + `column_id` + `part` + `flags`.
const ENTRY_LEN: usize = 4 + 4 + 2 + 1 + 1;

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
}

/// A decoded page directory.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageDirectory {
    row_count: u32,
    entries: Vec<PageEntry>,
}

impl PageDirectory {
    /// Builds a directory for a group of `row_count` rows from entries already
    /// in ascending offset order.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] if there are more pages than the
    /// wire's `u16` count holds, if the entries are not ascending by offset,
    /// if two pages overlap, if a page's extent does not fit a `u32`, or if
    /// two pages claim the same `(column_id, part)`. Every one of these is
    /// rejected here rather than at read time because none has a correct
    /// encoding, and the writer is where that is still fixable: a directory
    /// that maps one byte into two pages, or one part to two pages, has no
    /// correct reading, and one with more pages than its count field could
    /// only be written by truncating the list or by writing a count its
    /// entries contradict.
    pub fn new(row_count: u32, entries: Vec<PageEntry>) -> Result<Self> {
        if entries.len() > usize::from(u16::MAX) {
            return Err(Error::InvalidHeader(
                "column page: page count exceeds the u16 directory field",
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
            end_of_previous = end;
        }
        // Sorted-neighbour check rather than a pairwise one. The directory is
        // read from disk, so its page count is whatever the bytes say, up to
        // u16::MAX, and a pairwise check would turn one corrupt block into
        // billions of comparisons on the read path.
        let mut ids: Vec<PageId> = entries.iter().map(|e| e.id).collect();
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair.first() == pair.last()) {
            return Err(Error::InvalidHeader(
                "column page: two pages claim the same column part",
            ));
        }
        Ok(Self { row_count, entries })
    }

    /// Lays `pages` out back to back from the directory's end, in the order
    /// given, each `(id, on_disk_length)`.
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
        pages: impl IntoIterator<Item = (PageId, u32)>,
    ) -> Result<Self> {
        let mut offset: u32 = 0;
        let mut entries = Vec::new();
        for (id, length) in pages {
            entries.push(PageEntry { offset, length, id });
            offset = offset.checked_add(length).ok_or(Error::InvalidHeader(
                "column page: group length overflows u32",
            ))?;
        }
        Self::new(row_count, entries)
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

    /// Rows in the group; every page describes exactly these.
    #[must_use]
    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    /// The pages, in ascending offset order.
    #[must_use]
    pub fn entries(&self) -> &[PageEntry] {
        &self.entries
    }

    /// Serializes the directory into `out`.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.reserve(HEADER_LEN + self.entries.len() * ENTRY_LEN);
        out.push(VERSION);
        // `new` is the only constructor and refuses more than u16::MAX
        // entries, so the conversion cannot fail and nothing is truncated.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the page count is bounded to u16::MAX by `new`"
        )]
        let count = self.entries.len() as u16;
        out.extend_from_slice(&count.to_le_bytes());
        out.extend_from_slice(&self.row_count.to_le_bytes());
        for entry in &self.entries {
            out.extend_from_slice(&entry.offset.to_le_bytes());
            out.extend_from_slice(&entry.length.to_le_bytes());
            out.extend_from_slice(&entry.id.column_id.to_le_bytes());
            out.push(entry.id.part);
            out.push(0);
        }
    }

    /// Parses a directory payload.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] for an unknown version, a truncated
    /// payload, trailing bytes after the declared entries, a set reserved
    /// flag bit, or entries [`Self::new`] would refuse. Trailing bytes are
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

        // The declared count is on-disk data, so it bounds nothing until the
        // bytes behind it are seen to exist: reserve for what the payload can
        // actually hold, not for what the header claims.
        let mut entries = Vec::with_capacity(count.min(rest.len() / ENTRY_LEN));
        for _ in 0..count {
            let offset = u32::from_le_bytes(take(&mut rest).ok_or(ERR)?);
            let length = u32::from_le_bytes(take(&mut rest).ok_or(ERR)?);
            let column_id = u16::from_le_bytes(take(&mut rest).ok_or(ERR)?);
            let [part] = take::<1>(&mut rest).ok_or(ERR)?;
            let [flags] = take::<1>(&mut rest).ok_or(ERR)?;
            if flags != 0 {
                return Err(Error::InvalidHeader(
                    "ColumnPageDirectory: reserved page flag set",
                ));
            }
            entries.push(PageEntry {
                offset,
                length,
                id: PageId { column_id, part },
            });
        }
        if !rest.is_empty() {
            return Err(Error::InvalidHeader(
                "ColumnPageDirectory: trailing bytes after the declared pages",
            ));
        }
        Self::new(row_count, entries)
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
