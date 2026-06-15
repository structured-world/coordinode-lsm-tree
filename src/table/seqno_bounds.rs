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
pub fn encode_seqno_bounds(out: &mut Vec<u8>, bounds: &[(BlockOffset, (SeqNo, SeqNo))]) {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "data-block count is bounded well within u32"
    )]
    let count = bounds.len() as u32;
    out.extend_from_slice(&count.to_le_bytes());
    for (offset, (seqno_min, seqno_max)) in bounds {
        out.extend_from_slice(&offset.0.to_le_bytes());
        out.extend_from_slice(&seqno_min.to_le_bytes());
        out.extend_from_slice(&seqno_max.to_le_bytes());
    }
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

        let mut r = bytes;
        let count = read_u32(&mut r).ok_or(ERR)?;
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
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trips() {
        let bounds = [
            (BlockOffset(0), (10u64, 20u64)),
            (BlockOffset(4096), (5, 5)),
            (BlockOffset(9000), (0, 1_000_000)),
        ];
        let mut buf = Vec::new();
        encode_seqno_bounds(&mut buf, &bounds);
        let map = SeqnoBoundsMap::decode(&buf).expect("decode");
        assert_eq!(map.len(), 3);
        assert_eq!(map.bounds_for(0), Some((10, 20)));
        assert_eq!(map.bounds_for(4096), Some((5, 5)));
        assert_eq!(map.bounds_for(9000), Some((0, 1_000_000)));
        // An offset not present (e.g. between recorded blocks) returns None.
        assert_eq!(map.bounds_for(1), None);
        assert_eq!(map.bounds_for(5000), None);
    }

    #[test]
    fn decode_empty_section_is_empty_map() {
        let mut buf = Vec::new();
        encode_seqno_bounds(&mut buf, &[]);
        let map = SeqnoBoundsMap::decode(&buf).expect("decode empty");
        assert!(map.is_empty());
        assert_eq!(map.bounds_for(0), None);
    }

    #[test]
    fn decode_rejects_non_ascending_offsets() {
        // count=2 with offsets 100 then 50 (descending) must be rejected.
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes());
        for (off, lo, hi) in [(100u64, 1u64, 2u64), (50, 1, 2)] {
            buf.extend_from_slice(&off.to_le_bytes());
            buf.extend_from_slice(&lo.to_le_bytes());
            buf.extend_from_slice(&hi.to_le_bytes());
        }
        assert!(SeqnoBoundsMap::decode(&buf).is_err());
    }

    #[test]
    fn decode_rejects_inverted_bounds() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&9u64.to_le_bytes()); // min
        buf.extend_from_slice(&3u64.to_le_bytes()); // max < min
        assert!(SeqnoBoundsMap::decode(&buf).is_err());
    }

    #[test]
    fn decode_rejects_truncated_payload() {
        // count=1 but no entry bytes.
        let buf = 1u32.to_le_bytes().to_vec();
        assert!(SeqnoBoundsMap::decode(&buf).is_err());
    }
}
