// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Per-SST zone-map section: a parallel `(data-block offset -> per-column
//! statistics)` map that lets a range / analytical scan skip a data block whose
//! stats prove no predicate match, WITHOUT decoding it.
//!
//! Kept parallel to the index (like [`crate::table::seqno_bounds`]) rather than
//! inline in the block footer or index entries: per-block stats inline would
//! bloat every point-read probe, so they live in this optional section that
//! only a scan loads. A point read never touches it.
//!
//! Off by default + emitted only when the zone-map policy is enabled, so a table
//! written without it carries zero extra bytes. Offset-keyed (not ordinal) so a
//! lookup cannot mis-map if block iteration order ever shifts.
//!
//! # Wire layout
//!
//! ```text
//! [count: u32 LE]
//! count × [
//!   block_offset: u64 LE
//!   n_columns:    u16 LE
//!   n_columns × [
//!     column_id:  u32 LE
//!     type_tag:   u8        // comparable-encoding type of min/max
//!     codec_id:   u8        // column codec (0 for row blocks)
//!     null_count: u32 LE
//!     row_count:  u32 LE
//!     min_len: u32 LE, min: [u8; min_len]   // column comparable encoding
//!     max_len: u32 LE, max: [u8; max_len]
//!   ]
//! ]
//! ```
//!
//! Entries are strictly ascending by `block_offset` (write order == file order),
//! enabling both a binary-search lookup and a sequential cursor.

// The cursor reader API (`cursor` / `ZoneMapCursor` / `len` / `is_empty`) has no
// live caller until the columnar block-skip scan consumes it; `columns_for` and
// the writer / table-open paths are already used (the range-cardinality estimate
// reads `columns_for`). Gated to non-test builds so the in-module tests (which
// exercise the cursor API) do not leave the expectation unfulfilled, and kept as
// `expect` (not `allow`) so it self-removes once a live caller lands.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "cursor API consumed by the columnar block-skip scan"
    )
)]

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::BlockOffset;

/// Per-column statistics for one data block. The engine treats `min` / `max` as
/// opaque comparable-encoded byte arrays tagged by `type_tag` + `codec_id`; it
/// never interprets them beyond ordered comparison against a predicate bound.
///
/// A row-organized block records a single synthetic column (`column_id == 0`)
/// carrying the whole-block value min / max; a columnar block records one entry
/// per stored column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnStats {
    /// Stable identifier of the column within its (row group) block. `0` is the
    /// synthetic whole-value column for row-organized blocks.
    pub column_id: u32,
    /// Comparable-encoding type tag of `min` / `max` (opaque to the engine).
    pub type_tag: u8,
    /// Column codec id (opaque to the engine; `0` for row blocks).
    pub codec_id: u8,
    /// Number of null values in this column within the block.
    pub null_count: u32,
    /// Number of rows in this column within the block.
    pub row_count: u32,
    /// Minimum value, in the column's comparable encoding.
    pub min: Vec<u8>,
    /// Maximum value, in the column's comparable encoding.
    pub max: Vec<u8>,
}

/// Serialize the per-data-block zone-map into the `zone_map` section. `blocks[i]`
/// is `(block_offset, columns)` for one data block, in write (ascending-offset)
/// order.
///
/// # Errors
///
/// Returns [`crate::Error::InvalidHeader`] if the block count, a per-block column
/// count, or a min / max length does not fit its wire width. Validating at
/// encode time fails fast at the source rather than deferring to the decoder.
pub fn encode_zone_map(
    out: &mut Vec<u8>,
    blocks: &[(BlockOffset, Vec<ColumnStats>)],
) -> crate::Result<()> {
    const ERR: crate::Error = crate::Error::InvalidHeader("ZoneMap");

    let count = u32::try_from(blocks.len()).map_err(|_| ERR)?;
    out.extend_from_slice(&count.to_le_bytes());
    for (offset, columns) in blocks {
        let n_columns = u16::try_from(columns.len()).map_err(|_| ERR)?;
        out.extend_from_slice(&offset.0.to_le_bytes());
        out.extend_from_slice(&n_columns.to_le_bytes());
        for c in columns {
            let min_len = u32::try_from(c.min.len()).map_err(|_| ERR)?;
            let max_len = u32::try_from(c.max.len()).map_err(|_| ERR)?;
            out.extend_from_slice(&c.column_id.to_le_bytes());
            out.push(c.type_tag);
            out.push(c.codec_id);
            out.extend_from_slice(&c.null_count.to_le_bytes());
            out.extend_from_slice(&c.row_count.to_le_bytes());
            out.extend_from_slice(&min_len.to_le_bytes());
            out.extend_from_slice(&c.min);
            out.extend_from_slice(&max_len.to_le_bytes());
            out.extend_from_slice(&c.max);
        }
    }
    Ok(())
}

/// Decoded `zone_map` section: a lookup from a data block's file offset to its
/// per-column statistics. Empty when the table has no zone map (the section was
/// absent), in which case every lookup returns `None` and the scan falls back to
/// reading every block.
#[derive(Debug, Default, Clone)]
pub struct ZoneMap {
    /// `(block_offset, columns)`, sorted ascending by offset for binary-search
    /// lookup and a forward cursor.
    entries: Vec<(u64, Vec<ColumnStats>)>,
}

/// Smallest possible wire size of one block entry: `block_offset` (8) +
/// `n_columns` (2), with zero columns. Used to bound the speculative capacity
/// against a corrupt count before allocating.
const MIN_ENTRY_SIZE: usize = 8 + 2;

impl ZoneMap {
    /// Decode a `zone_map` section payload.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidHeader`] if the payload is truncated, the
    /// block entries are not strictly ascending by offset, or trailing bytes
    /// remain after the declared count (a corrupt section must surface rather
    /// than silently mis-skip a block).
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        const ERR: crate::Error = crate::Error::InvalidHeader("ZoneMap");

        fn take<'a>(r: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
            if r.len() < n {
                return None;
            }
            let (head, tail) = r.split_at(n);
            *r = tail;
            Some(head)
        }
        fn read_u8(r: &mut &[u8]) -> Option<u8> {
            take(r, 1)?.first().copied()
        }
        fn read_u16(r: &mut &[u8]) -> Option<u16> {
            let b: [u8; 2] = take(r, 2)?.try_into().ok()?;
            Some(u16::from_le_bytes(b))
        }
        fn read_u32(r: &mut &[u8]) -> Option<u32> {
            let b: [u8; 4] = take(r, 4)?.try_into().ok()?;
            Some(u32::from_le_bytes(b))
        }
        fn read_u64(r: &mut &[u8]) -> Option<u64> {
            let b: [u8; 8] = take(r, 8)?.try_into().ok()?;
            Some(u64::from_le_bytes(b))
        }

        let mut r = bytes;
        let count = read_u32(&mut r).ok_or(ERR)?;
        // Bound the speculative allocation: a count claiming more block entries
        // than the smallest-possible-entry size could fill is corrupt, so a bad
        // count cannot trigger a multi-GB pre-allocation instead of a clean error.
        match (count as usize).checked_mul(MIN_ENTRY_SIZE) {
            Some(needed) if needed <= r.len() => {}
            _ => return Err(ERR),
        }
        let mut entries: Vec<(u64, Vec<ColumnStats>)> = Vec::with_capacity(count as usize);
        let mut prev: Option<u64> = None;
        for _ in 0..count {
            let offset = read_u64(&mut r).ok_or(ERR)?;
            if prev.is_some_and(|p| offset <= p) {
                return Err(ERR);
            }
            prev = Some(offset);
            let n_columns = read_u16(&mut r).ok_or(ERR)?;
            let mut columns = Vec::with_capacity(n_columns as usize);
            for _ in 0..n_columns {
                let column_id = read_u32(&mut r).ok_or(ERR)?;
                let type_tag = read_u8(&mut r).ok_or(ERR)?;
                let codec_id = read_u8(&mut r).ok_or(ERR)?;
                let null_count = read_u32(&mut r).ok_or(ERR)?;
                let row_count = read_u32(&mut r).ok_or(ERR)?;
                let min_len = read_u32(&mut r).ok_or(ERR)? as usize;
                let min = take(&mut r, min_len).ok_or(ERR)?.to_vec();
                let max_len = read_u32(&mut r).ok_or(ERR)? as usize;
                let max = take(&mut r, max_len).ok_or(ERR)?.to_vec();
                columns.push(ColumnStats {
                    column_id,
                    type_tag,
                    codec_id,
                    null_count,
                    row_count,
                    min,
                    max,
                });
            }
            entries.push((offset, columns));
        }
        // No trailing bytes: leftover means a wrong count or padded / corrupt
        // section, so reject rather than accept a mis-sized parse.
        if !r.is_empty() {
            return Err(ERR);
        }
        Ok(Self { entries })
    }

    /// The per-column stats for the data block at `offset`, or `None` if the
    /// block was not recorded (table without a zone map, or a non-data block).
    #[must_use]
    pub fn columns_for(&self, offset: u64) -> Option<&[ColumnStats]> {
        let idx = self
            .entries
            .binary_search_by_key(&offset, |(o, _)| *o)
            .ok()?;
        self.entries.get(idx).map(|(_, c)| c.as_slice())
    }

    /// A forward cursor for in-order block iteration (the range / columnar scan
    /// access pattern): amortized O(1) per lookup when offsets are visited
    /// ascending, instead of a binary search per block.
    #[must_use]
    pub fn cursor(&self) -> ZoneMapCursor<'_> {
        ZoneMapCursor {
            entries: &self.entries,
            pos: 0,
        }
    }

    /// Whether any per-block stats are recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of recorded data blocks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Forward cursor over a [`ZoneMap`], optimized for ascending-offset block
/// iteration. A lookup at an offset at or ahead of the cursor advances linearly;
/// a backward jump (random seek) falls back to a binary search and repositions.
pub struct ZoneMapCursor<'a> {
    entries: &'a [(u64, Vec<ColumnStats>)],
    pos: usize,
}

impl<'a> ZoneMapCursor<'a> {
    /// The per-column stats for the data block at `offset`, advancing the cursor.
    /// Returns `None` if the offset was not recorded.
    pub fn columns_for(&mut self, offset: u64) -> Option<&'a [ColumnStats]> {
        let at_or_ahead = self
            .entries
            .get(self.pos)
            .is_some_and(|(o, _)| *o <= offset);
        if at_or_ahead {
            // Fast path: walk forward to the first entry >= offset.
            while self.entries.get(self.pos).is_some_and(|(o, _)| *o < offset) {
                self.pos += 1;
            }
        } else {
            // Backward jump or first lookup: binary-search and reposition.
            match self.entries.binary_search_by_key(&offset, |(o, _)| *o) {
                Ok(i) => self.pos = i,
                Err(i) => {
                    self.pos = i;
                    return None;
                }
            }
        }
        self.entries
            .get(self.pos)
            .filter(|(o, _)| *o == offset)
            .map(|(_, c)| c.as_slice())
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;
