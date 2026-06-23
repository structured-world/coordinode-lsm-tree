// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Per-SST seqno-bounds section: a parallel `(data-block offset -> [seqno_min,
//! seqno_max])` map that powers the `scan_since_seqno` block-skip WITHOUT
//! bloating the index entries.
//!
//! The bounds used to live inline in each index entry (markers 2/3), which made
//! every point-read index probe step over two extra varints and enlarged the
//! index block (measurable point-read cost). Moving them into this optional
//! parallel section keeps the index entries at their legacy size: a point read
//! never touches the bounds, while a seqno-scoped scan looks the bounds up by
//! data-block file offset and skips blocks whose `[min, max]` cannot overlap the
//! target window.
//!
//! Optional + format-gated (emitted only when `seqno_in_index` is on), so a
//! table written without it carries zero extra bytes. Offset-keyed (not ordinal)
//! so a lookup cannot silently mis-map if block iteration order ever shifts.
//!
//! # Wire layout
//!
//! ```text
//! [count: u32 LE]
//! count × [ block_offset: u64 LE | seqno_min: u64 LE | seqno_max: u64 LE ]
//! ```
//!
//! Entries are strictly ascending by `block_offset` (write order == file order),
//! enabling a binary-search lookup.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::BlockOffset;
use crate::SeqNo;

/// Serialize the per-data-block seqno bounds into the `seqno_bounds` section.
/// `bounds[i]` is `(block_offset, (seqno_min, seqno_max))` for one data block,
/// in write (ascending-offset) order.
///
/// # Errors
///
/// Returns [`crate::Error::InvalidHeader`] if the block count does not fit the
/// `u32` section-header width. Validating at encode time fails fast at the
/// source rather than deferring detection to the decoder.
pub fn encode_seqno_bounds(
    out: &mut Vec<u8>,
    bounds: &[(BlockOffset, (SeqNo, SeqNo))],
) -> crate::Result<()> {
    let count =
        u32::try_from(bounds.len()).map_err(|_| crate::Error::InvalidHeader("SeqnoBounds"))?;
    out.extend_from_slice(&count.to_le_bytes());
    for (offset, (seqno_min, seqno_max)) in bounds {
        // Write-side invariant: a block's bounds must be ordered. Asserted here
        // (debug builds) so a writer bug surfaces at its source, in addition to
        // the decoder's runtime check that catches it at open time.
        debug_assert!(
            seqno_min <= seqno_max,
            "seqno bounds inverted: min {seqno_min} > max {seqno_max}"
        );
        out.extend_from_slice(&offset.0.to_le_bytes());
        out.extend_from_slice(&seqno_min.to_le_bytes());
        out.extend_from_slice(&seqno_max.to_le_bytes());
    }
    Ok(())
}

/// Decoded `seqno_bounds` section: a lookup from a data block's file offset to
/// its `[seqno_min, seqno_max]`. Empty when the table has no seqno bounds (the
/// section was absent), in which case every lookup returns `None` and the scan
/// falls back to a full per-entry filter.
#[derive(Debug, Default, Clone)]
pub struct SeqnoBoundsMap {
    /// `(block_offset, (seqno_min, seqno_max))`, sorted ascending by offset for
    /// binary-search lookup.
    entries: Vec<(u64, (SeqNo, SeqNo))>,
}

impl SeqnoBoundsMap {
    /// Decode a `seqno_bounds` section payload.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidHeader`] if the payload is truncated, the
    /// entries are not strictly ascending by offset, or any entry has
    /// `seqno_min > seqno_max` (a corrupt section must surface rather than
    /// silently mis-skip a block).
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        const ERR: crate::Error = crate::Error::InvalidHeader("SeqnoBounds");

        fn take<'a>(r: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
            if r.len() < n {
                return None;
            }
            let (head, tail) = r.split_at(n);
            *r = tail;
            Some(head)
        }
        fn read_u32(r: &mut &[u8]) -> Option<u32> {
            let b: [u8; 4] = take(r, 4)?.try_into().ok()?;
            Some(u32::from_le_bytes(b))
        }
        fn read_u64(r: &mut &[u8]) -> Option<u64> {
            let b: [u8; 8] = take(r, 8)?.try_into().ok()?;
            Some(u64::from_le_bytes(b))
        }

        // Each entry is exactly three u64s on the wire.
        const ENTRY_SIZE: usize = 3 * core::mem::size_of::<u64>();

        let mut r = bytes;
        let count = read_u32(&mut r).ok_or(ERR)?;
        // Reject a corrupt count BEFORE the speculative allocation below: a
        // count claiming more entries than the remaining payload can hold is
        // invalid, and validating it up front bounds `with_capacity` to the
        // real payload size (so a corrupt count cannot trigger a multi-GB
        // pre-allocation / OOM instead of a clean `InvalidHeader`).
        match (count as usize).checked_mul(ENTRY_SIZE) {
            Some(needed) if needed <= r.len() => {}
            _ => return Err(ERR),
        }
        let mut entries = Vec::with_capacity(count as usize);
        let mut prev: Option<u64> = None;
        for _ in 0..count {
            let offset = read_u64(&mut r).ok_or(ERR)?;
            if prev.is_some_and(|p| offset <= p) {
                return Err(ERR);
            }
            prev = Some(offset);
            let seqno_min = read_u64(&mut r).ok_or(ERR)?;
            let seqno_max = read_u64(&mut r).ok_or(ERR)?;
            if seqno_min > seqno_max {
                return Err(ERR);
            }
            entries.push((offset, (seqno_min, seqno_max)));
        }
        // The section must contain exactly `count` entries and nothing more:
        // leftover bytes mean a wrong count or a corrupt / padded section, so
        // reject them rather than silently accept a mis-sized parse.
        if !r.is_empty() {
            return Err(ERR);
        }
        Ok(Self { entries })
    }

    /// The `[seqno_min, seqno_max]` for the data block at `offset`, or `None` if
    /// the block was not recorded (legacy table without seqno bounds).
    #[must_use]
    pub fn bounds_for(&self, offset: u64) -> Option<(SeqNo, SeqNo)> {
        let idx = self
            .entries
            .binary_search_by_key(&offset, |(o, _)| *o)
            .ok()?;
        self.entries.get(idx).map(|(_, b)| *b)
    }

    /// Whether any per-block bounds are recorded.
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

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;
