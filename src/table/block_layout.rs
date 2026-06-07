// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Codec for the optional per-table `block_layout` section.
//!
//! For each data block that compressed into two or more inner zstd blocks, the
//! section records the block's file offset and the cumulative decompressed END
//! offset of every inner block (a prefix sum whose last entry equals the
//! block's uncompressed length). A range query binary-searches the entry for a
//! block, then maps a decompressed byte range to the inner-block indices
//! covering it and partial-decodes only those blocks instead of the whole
//! frame.
//!
//! The section is absent unless at least one data block split into >= 2 inner
//! blocks, so default small-block tables carry no extra bytes.
//!
//! # Wire format
//!
//! ```text
//! [entry_count : u32 LE]
//! repeated entry_count times, ascending by block offset:
//!   [block_offset : u64 LE]
//!   [inner_count  : u32 LE]   (always >= 2)
//!   [end_0 : u32 LE] [end_1 : u32 LE] ... [end_{inner_count-1} : u32 LE]
//! ```

use crate::table::block::BlockOffset;

/// Serialize the per-block inner-block layouts into `out`.
///
/// `layouts` must be sorted ascending by block offset (the writer produces
/// them in write order, which is ascending) and every `ends` array must have
/// length >= 2 (single-inner-block blocks are not recorded).
///
/// Writes via `extend_from_slice` (infallible on `Vec`), so no I/O error path.
pub fn encode_block_layouts(out: &mut Vec<u8>, layouts: &[(BlockOffset, Vec<u32>)]) {
    // entry_count fits u32: a table cannot hold more data blocks than u32 file
    // offsets address, and the writer only pushes split blocks.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "data-block count is bounded well within u32"
    )]
    let count = layouts.len() as u32;
    out.extend_from_slice(&count.to_le_bytes());
    for (offset, ends) in layouts {
        out.extend_from_slice(&offset.0.to_le_bytes());
        #[expect(
            clippy::cast_possible_truncation,
            reason = "inner-block count is bounded well within u32"
        )]
        let inner = ends.len() as u32;
        out.extend_from_slice(&inner.to_le_bytes());
        for &end in ends {
            out.extend_from_slice(&end.to_le_bytes());
        }
    }
}

/// Decoded `block_layout` section: a lookup from a data block's file offset to
/// the cumulative decompressed END offsets of its inner zstd blocks.
#[derive(Debug, Default, Clone)]
pub struct BlockLayoutMap {
    /// `(block_offset, cumulative_inner_block_ends)`, sorted ascending by
    /// offset for binary-search lookup.
    entries: Vec<(u64, Vec<u32>)>,
}

impl BlockLayoutMap {
    /// Decode a `block_layout` section payload.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload is truncated, an inner count is < 2, or
    /// the entries are not strictly ascending by offset (a corrupt section
    /// must surface rather than silently mis-map a range).
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        // Manual little-endian slice parsing (no `std::io::Read`), so the codec
        // stays `no_std + alloc`-clean. Any truncation or invariant violation
        // surfaces as a typed `InvalidHeader`, not a stringly-typed I/O error.
        const ERR: crate::Error = crate::Error::InvalidHeader("BlockLayout");

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
            // Entries must be strictly ascending by offset (else a lookup could
            // silently mis-map a range).
            if prev.is_some_and(|p| offset <= p) {
                return Err(ERR);
            }
            prev = Some(offset);
            let inner = read_u32(&mut r).ok_or(ERR)?;
            // A recorded block always split into >= 2 inner blocks.
            if inner < 2 {
                return Err(ERR);
            }
            let mut ends = Vec::with_capacity(inner as usize);
            for _ in 0..inner {
                ends.push(read_u32(&mut r).ok_or(ERR)?);
            }
            entries.push((offset, ends));
        }
        Ok(Self { entries })
    }

    /// Cumulative inner-block END offsets for the data block at `offset`, or
    /// `None` if the block was not recorded (single inner block → full decode).
    #[cfg(feature = "zstd")]
    pub fn ends_for(&self, offset: u64) -> Option<&[u32]> {
        let idx = self
            .entries
            .binary_search_by_key(&offset, |(o, _)| *o)
            .ok()?;
        self.entries.get(idx).map(|(_, ends)| ends.as_slice())
    }

    /// Number of recorded multi-inner-block data blocks.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Recorded block offsets, ascending. Test-only enumeration helper.
    #[cfg(test)]
    pub(crate) fn offsets(&self) -> Vec<u64> {
        self.entries.iter().map(|(o, _)| *o).collect()
    }
}

/// Map a half-open decompressed byte range `[lower, upper)` to the half-open
/// inner-block index range `[start_block, end_block)` that covers it, given the
/// cumulative inner-block END offsets `ends`.
///
/// `start_block` is the first inner block whose bytes reach past `lower`;
/// `end_block` is one past the last inner block needed to cover `upper`. The
/// caller passes these to
/// [`decode_blocks_partial`](structured_zstd::decoding::FrameDecoder::decode_blocks_partial);
/// blocks before `start_block` are still decoded into the shared window but not
/// returned, and blocks at/after `end_block` are skipped entirely (the win).
///
/// Returns `None` (caller falls back to full decode) if `ends` is empty or
/// `lower >= total` (range past the block's data — nothing to map).
// Consumed by the range-query partial-decode read path (wired in a follow-up
// slice); kept here with its unit tests as the mapping is format-stable.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "consumed by range-query partial decode (next slice)"
    )
)]
pub fn map_byte_range_to_blocks(ends: &[u32], lower: u32, upper: u32) -> Option<(u32, u32)> {
    let total = *ends.last()?;
    if lower >= total {
        return None;
    }
    // First block whose cumulative end is strictly greater than `lower` contains
    // `lower` (ends are exclusive upper bounds of each block's byte span).
    let start = ends.partition_point(|&e| e <= lower);
    // Last block needed for `upper`: the block containing `upper - 1` (clamp
    // `upper` to `total`). `end_block` is one past it (exclusive).
    let upper_clamped = upper.min(total);
    let last = if upper_clamped == 0 {
        0
    } else {
        ends.partition_point(|&e| e < upper_clamped)
    };
    let end = (last + 1).min(ends.len());
    // start <= end always holds because lower < upper_clamped <= total here
    // (lower < total checked above; if upper <= lower the caller filters first).
    #[expect(
        clippy::cast_possible_truncation,
        reason = "inner-block indices bounded by ends.len(), well within u32"
    )]
    Some((start as u32, end as u32))
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrips_entries() {
        let layouts = vec![
            (BlockOffset(0), vec![100, 250, 400]),
            (BlockOffset(512), vec![80, 160]),
        ];
        let mut buf = Vec::new();
        encode_block_layouts(&mut buf, &layouts);
        let map = BlockLayoutMap::decode(&buf).expect("decode");
        assert_eq!(map.len(), 2);
        // `ends_for` is only compiled for the zstd partial-decode read path.
        #[cfg(feature = "zstd")]
        {
            assert_eq!(map.ends_for(0), Some([100u32, 250, 400].as_slice()));
            assert_eq!(map.ends_for(512), Some([80u32, 160].as_slice()));
            assert_eq!(map.ends_for(999), None, "unrecorded offset → None");
        }
    }

    #[test]
    fn decode_rejects_inner_count_less_than_two() {
        // A recorded block must always have >= 2 inner blocks; a hand-crafted
        // payload with inner_count = 1 must be rejected (corruption guard).
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // entry_count = 1
        buf.extend_from_slice(&0u64.to_le_bytes()); // block_offset = 0
        buf.extend_from_slice(&1u32.to_le_bytes()); // inner_count = 1 (invalid)
        buf.extend_from_slice(&100u32.to_le_bytes()); // one end offset
        assert!(
            BlockLayoutMap::decode(&buf).is_err(),
            "inner_count < 2 must be rejected",
        );
    }

    #[test]
    fn decode_rejects_non_ascending_offsets() {
        let layouts = vec![
            (BlockOffset(512), vec![80, 160]),
            (BlockOffset(0), vec![100, 250]),
        ];
        let mut buf = Vec::new();
        encode_block_layouts(&mut buf, &layouts);
        assert!(
            BlockLayoutMap::decode(&buf).is_err(),
            "non-ascending offsets must be rejected",
        );
    }

    #[test]
    fn empty_layouts_decode_to_empty_map() {
        let mut buf = Vec::new();
        encode_block_layouts(&mut buf, &[]);
        let map = BlockLayoutMap::decode(&buf).expect("decode empty");
        assert_eq!(map.len(), 0);
        #[cfg(feature = "zstd")]
        assert_eq!(map.ends_for(0), None);
    }

    #[test]
    fn map_byte_range_single_block_subset() {
        // ends: block0=[0,100) block1=[100,250) block2=[250,400)
        let ends = [100u32, 250, 400];
        // Range fully inside block 0 → [0, 1).
        assert_eq!(map_byte_range_to_blocks(&ends, 10, 50), Some((0, 1)));
        // Range inside block 1 → [1, 2).
        assert_eq!(map_byte_range_to_blocks(&ends, 120, 200), Some((1, 2)));
        // Range spanning block 0 and 1 → [0, 2).
        assert_eq!(map_byte_range_to_blocks(&ends, 50, 150), Some((0, 2)));
        // Range reaching the tail → [1, 3).
        assert_eq!(map_byte_range_to_blocks(&ends, 120, 400), Some((1, 3)));
        // Whole block → [0, 3).
        assert_eq!(map_byte_range_to_blocks(&ends, 0, 400), Some((0, 3)));
    }

    #[test]
    fn map_byte_range_past_end_returns_none() {
        let ends = [100u32, 250, 400];
        assert_eq!(map_byte_range_to_blocks(&ends, 400, 500), None);
        assert_eq!(map_byte_range_to_blocks(&[], 0, 10), None);
    }

    #[test]
    fn map_byte_range_boundary_is_exclusive_end() {
        // upper exactly on a block boundary must NOT pull in the next block.
        let ends = [100u32, 250, 400];
        // [0, 100) is exactly block 0.
        assert_eq!(map_byte_range_to_blocks(&ends, 0, 100), Some((0, 1)));
        // [100, 250) is exactly block 1.
        assert_eq!(map_byte_range_to_blocks(&ends, 100, 250), Some((1, 2)));
    }
}
