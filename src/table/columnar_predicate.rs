// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Predicate model for the vectorized columnar scan.
//!
//! A [`ColumnRangePredicate`] is an inclusive byte-range filter over one column.
//! It drives two pushdowns:
//!
//! - **Block skip** ([`ColumnRangePredicate::can_skip_block`]): compare the
//!   predicate bounds against the per-block zone-map ([`ColumnStats`]) min / max
//!   (#502). A block whose column range is disjoint from the predicate cannot
//!   contain a matching row, so it is skipped without ever decoding it.
//! - **Row filter** ([`ColumnRangePredicate::matching_rows`]): evaluate the
//!   predicate over a decoded [`ColumnBatch`], producing a per-row match mask.
//!
//! Both compare the column's *comparable* encoding. For a [`TypeTag::Bytes`]
//! column (the user-key column, and consumer byte sub-columns) the comparable
//! encoding is the raw value bytes, so the row filter operates directly on the
//! stored bytes. Fixed-width numeric columns need a separate comparable
//! transform and are not yet filterable at the row level (block skip still
//! works, since the zone-map min / max are already comparable-encoded).

use super::columnar::{Column, ColumnBatch, TypeTag};
use super::zone_map::ColumnStats;
use alloc::vec::Vec;

/// An inclusive byte-range filter over one column's comparable encoding.
///
/// `lower` / `upper` are inclusive bounds; `None` leaves that side unbounded.
/// Construct one per column the scan filters on.
#[derive(Debug, Clone)]
pub struct ColumnRangePredicate {
    /// Column this predicate filters on (matches [`Column::column_id`]).
    pub column_id: u16,
    /// Inclusive lower bound in comparable encoding, or `None` for unbounded.
    pub lower: Option<Vec<u8>>,
    /// Inclusive upper bound in comparable encoding, or `None` for unbounded.
    pub upper: Option<Vec<u8>>,
}

impl ColumnRangePredicate {
    /// Returns `true` when the per-block zone-map `stats` prove the predicate's
    /// column lies entirely outside `[lower, upper]`, so the block holds no
    /// matching row and can be skipped without decoding it.
    ///
    /// Conservative: returns `false` (cannot skip) when the column has no stats
    /// entry, so a missing zone-map never drops data.
    #[must_use]
    pub fn can_skip_block(&self, stats: &[ColumnStats]) -> bool {
        let target = u32::from(self.column_id);
        let Some(s) = stats.iter().find(|s| s.column_id == target) else {
            return false;
        };
        // Disjoint when the whole block is below the lower bound (block max <
        // lower) or above the upper bound (block min > upper).
        if let Some(lo) = &self.lower
            && s.max.as_slice() < lo.as_slice()
        {
            return true;
        }
        if let Some(hi) = &self.upper
            && s.min.as_slice() > hi.as_slice()
        {
            return true;
        }
        false
    }

    /// Evaluates the predicate over `batch`, returning a `row_count`-long mask
    /// where `true` marks a row whose value is within `[lower, upper]`.
    ///
    /// A row that is null (per the column's validity bitmap) never matches. If
    /// the predicate's column was projected out of `batch`, every row is treated
    /// as matching (the filter cannot run on an absent column). A non-`Bytes`
    /// column is likewise treated as all-matching, since its comparable encoding
    /// is not the stored encoding (block skip still applied at the block level).
    #[must_use]
    pub fn matching_rows(&self, batch: &ColumnBatch) -> Vec<bool> {
        let rows = batch.row_count as usize;
        let Some(col) = batch.columns.iter().find(|c| c.column_id == self.column_id) else {
            return alloc::vec![true; rows];
        };
        if !matches!(col.type_tag, TypeTag::Bytes) {
            return alloc::vec![true; rows];
        }
        (0..rows)
            .map(|row| self.row_matches(col, rows, row))
            .collect()
    }

    /// Whether row `row` of a `Bytes` column is non-null and within the bounds.
    fn row_matches(&self, col: &Column, rows: usize, row: usize) -> bool {
        if !row_valid(col, row) {
            return false;
        }
        let Some(value) = bytes_row(&col.data, rows, row) else {
            return false;
        };
        if let Some(lo) = &self.lower
            && value < lo.as_slice()
        {
            return false;
        }
        if let Some(hi) = &self.upper
            && value > hi.as_slice()
        {
            return false;
        }
        true
    }
}

/// Compacts a decoded batch down to the rows a predicate matched.
///
/// Returns a new batch keeping only the rows where `mask[i]` is true, preserving
/// column order and per-column framing, so the scan yields only matching rows. A
/// mask shorter than `row_count` drops the unspecified trailing rows.
#[must_use]
pub fn filter_batch(batch: &ColumnBatch, mask: &[bool]) -> ColumnBatch {
    let kept = mask.iter().filter(|&&m| m).count();
    let rows = batch.row_count as usize;
    let columns = batch
        .columns
        .iter()
        .map(|c| filter_column(c, rows, mask, kept))
        .collect();
    // kept <= row_count, which is a u32, so the count fits.
    let row_count = u32::try_from(kept).unwrap_or(batch.row_count);
    ColumnBatch { row_count, columns }
}

/// Compacts one column to the rows selected by `mask`, rebuilding its framing
/// (fixed chunks copied through, `Bytes` offset table + payload rebuilt) and its
/// validity bitmap.
fn filter_column(col: &Column, rows: usize, mask: &[bool], kept: usize) -> Column {
    let data = match col.type_tag {
        TypeTag::Fixed(width) => {
            let width = width as usize;
            // Bounded by a block's row count and the fixed width, so no overflow.
            let mut out = Vec::with_capacity(kept * width);
            for (row, &keep) in mask.iter().enumerate() {
                if keep
                    && let Some(start) = row.checked_mul(width)
                    && let Some(end) = start.checked_add(width)
                    && let Some(chunk) = col.data.get(start..end)
                {
                    out.extend_from_slice(chunk);
                }
            }
            out
        }
        TypeTag::Bytes => {
            // Rebuild the (kept + 1) u32 offset table and the packed payload.
            let mut offsets = Vec::with_capacity((kept + 1) * 4);
            let mut payload = Vec::new();
            let mut acc: u32 = 0;
            offsets.extend_from_slice(&acc.to_le_bytes());
            for (row, &keep) in mask.iter().enumerate() {
                if !keep {
                    continue;
                }
                // Every kept row writes exactly one offset, so the table stays in
                // lockstep with the kept row count even if a row's bytes are
                // unreadable: a corrupt slice degrades to empty rather than
                // leaving a missing offset and malformed framing.
                let value = bytes_row(&col.data, rows, row).unwrap_or(&[]);
                payload.extend_from_slice(value);
                // The kept payload is a subset of the original, whose offsets
                // already fit u32. The caps only guard the unreachable overflow
                // and keep the offsets monotonically non-decreasing.
                let len = u32::try_from(value.len()).unwrap_or(u32::MAX);
                acc = acc.saturating_add(len);
                offsets.extend_from_slice(&acc.to_le_bytes());
            }
            offsets.extend_from_slice(&payload);
            offsets
        }
    };
    let validity = col
        .validity
        .as_ref()
        .map(|bits| compact_validity(bits, mask, kept));
    Column {
        column_id: col.column_id,
        type_tag: col.type_tag,
        validity,
        data,
    }
}

/// Rebuilds a validity bitmap for the rows selected by `mask`, preserving each
/// kept row's null bit in compacted order.
fn compact_validity(bits: &[u8], mask: &[bool], kept: usize) -> Vec<u8> {
    let mut out = alloc::vec![0u8; kept.div_ceil(8)];
    let mut o = 0usize;
    for (row, &keep) in mask.iter().enumerate() {
        if !keep {
            continue;
        }
        let valid = bits
            .get(row / 8)
            .is_some_and(|b| b & (1u8 << (row % 8)) != 0);
        if valid && let Some(byte) = out.get_mut(o / 8) {
            *byte |= 1u8 << (o % 8);
        }
        o += 1;
    }
    out
}

/// Whether row `row` is valid (non-null) per the column's validity bitmap. A
/// column with no bitmap has every row valid; otherwise a set bit means valid.
fn row_valid(col: &Column, row: usize) -> bool {
    match &col.validity {
        None => true,
        Some(bits) => bits
            .get(row / 8)
            .is_some_and(|byte| byte & (1u8 << (row % 8)) != 0),
    }
}

/// Extracts row `row` of a `Bytes` column: a `(rows + 1)` little-endian `u32`
/// offset table followed by the payload, where row `i` spans the payload range
/// `offset[i]..offset[i + 1]`. Returns `None` on any out-of-range index.
fn bytes_row(data: &[u8], rows: usize, row: usize) -> Option<&[u8]> {
    let table_len = rows.checked_add(1)?.checked_mul(4)?;
    let off_i = read_u32_le(data.get(row.checked_mul(4)?..)?)? as usize;
    let off_next = read_u32_le(data.get(row.checked_add(1)?.checked_mul(4)?..)?)? as usize;
    data.get(table_len.checked_add(off_i)?..table_len.checked_add(off_next)?)
}

/// Reads a leading little-endian `u32` from `bytes`, or `None` if it is shorter
/// than four bytes.
fn read_u32_le(bytes: &[u8]) -> Option<u32> {
    let head = bytes.get(..4)?;
    let mut arr = [0u8; 4];
    for (dst, &src) in arr.iter_mut().zip(head) {
        *dst = src;
    }
    Some(u32::from_le_bytes(arr))
}

// Cached, no-std-friendly AVX2 token for the byte-equality dispatch below.
// `cpufeatures::new!` runs CPUID once (atomic load thereafter) and verifies OS
// AVX-state, so the AVX2 path cannot SIGILL on a host without it.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
cpufeatures::new!(cpu_avx2_byte_eq, "avx2");

/// Per-row mask marking rows of a fixed-1 (single-byte) column equal to `value`.
///
/// Useful e.g. on the value-type column to keep only live values. A SIMD-friendly
/// equality scan: byte equality is endianness-independent, so it needs no
/// comparable transform. Returns an all-true `row_count` mask when the column is
/// absent or not fixed-1, so an inapplicable filter never drops rows.
#[must_use]
pub fn byte_eq_mask(batch: &ColumnBatch, column_id: u16, value: u8) -> Vec<bool> {
    let rows = batch.row_count as usize;
    let Some(col) = batch.columns.iter().find(|c| c.column_id == column_id) else {
        return alloc::vec![true; rows];
    };
    if !matches!(col.type_tag, TypeTag::Fixed(1)) {
        return alloc::vec![true; rows];
    }
    byte_eq_dispatch(&col.data, value)
}

/// Portable reference: one boolean per byte, `true` where it equals `value`.
/// Also the scalar tail every SIMD kernel falls back to for the trailing bytes.
fn byte_eq_scalar(data: &[u8], value: u8) -> Vec<bool> {
    data.iter().map(|&b| b == value).collect()
}

/// Runtime-dispatched byte-equality mask: the widest kernel the host supports,
/// each producing bit-identical output to [`byte_eq_scalar`]. Exactly one `cfg`
/// arm compiles per target and is the function's tail.
fn byte_eq_dispatch(data: &[u8], value: u8) -> Vec<bool> {
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    {
        if cpu_avx2_byte_eq::get() {
            // SAFETY: cpufeatures has just verified AVX2 is present on this host.
            return unsafe { byte_eq_avx2(data, value) };
        }
        byte_eq_scalar(data, value)
    }
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is an ARMv8 baseline feature, always present on aarch64.
        unsafe { byte_eq_neon(data, value) }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "x86", target_arch = "aarch64")))]
    {
        byte_eq_scalar(data, value)
    }
}

/// AVX2 byte-equality: 32 bytes per iteration, scalar tail.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[target_feature(enable = "avx2")]
unsafe fn byte_eq_avx2(data: &[u8], value: u8) -> Vec<bool> {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{
        _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8,
    };
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{
        _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8,
    };
    let mut chunks = data.chunks_exact(32);
    let mut out = Vec::with_capacity(data.len());
    // SAFETY: the AVX2 intrinsics are enabled by `#[target_feature]`, which the
    // caller has verified present; the load is unaligned (`loadu`) over a full
    // 32-byte chunk, so it stays in bounds.
    unsafe {
        // Reinterpret the byte as i8 and the movemask as u32 (bit-preserving,
        // since the comparison and the lane mask are width-exact).
        let needle = _mm256_set1_epi8(i8::from_ne_bytes([value]));
        for chunk in &mut chunks {
            let v = _mm256_loadu_si256(chunk.as_ptr().cast());
            let cmp = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, needle));
            let mask = u32::from_ne_bytes(cmp.to_ne_bytes());
            for i in 0..32u32 {
                out.push((mask >> i) & 1 == 1);
            }
        }
    }
    out.extend(byte_eq_scalar(chunks.remainder(), value));
    out
}

/// NEON byte-equality: 16 bytes per iteration, scalar tail.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn byte_eq_neon(data: &[u8], value: u8) -> Vec<bool> {
    use core::arch::aarch64::{vceqq_u8, vdupq_n_u8, vld1q_u8, vst1q_u8};
    let mut chunks = data.chunks_exact(16);
    let mut out = Vec::with_capacity(data.len());
    // SAFETY: the NEON intrinsics are enabled by `#[target_feature]` (a baseline
    // aarch64 feature); the load and store cover a full 16-byte chunk / a local
    // 16-byte array, so both stay in bounds.
    unsafe {
        let needle = vdupq_n_u8(value);
        for chunk in &mut chunks {
            let eq = vceqq_u8(vld1q_u8(chunk.as_ptr()), needle); // 0xFF where equal
            let mut lanes = [0u8; 16];
            vst1q_u8(lanes.as_mut_ptr(), eq);
            for &lane in &lanes {
                out.push(lane != 0);
            }
        }
    }
    out.extend(byte_eq_scalar(chunks.remainder(), value));
    out
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
mod tests {
    use super::{
        Column, ColumnBatch, ColumnRangePredicate, ColumnStats, TypeTag, byte_eq_mask,
        byte_eq_scalar, filter_batch,
    };
    use crate::table::columnar::{column_batch_to_entries, entries_to_column_batch};
    use crate::{Slice, ValueType, key::InternalKey, value::InternalValue};

    fn entry(key: &[u8], seqno: u64, value: &[u8]) -> InternalValue {
        InternalValue {
            key: InternalKey::new(Slice::from(key), seqno, ValueType::Value),
            value: Slice::from(value),
        }
    }

    fn stats(column_id: u32, min: &[u8], max: &[u8]) -> ColumnStats {
        ColumnStats {
            column_id,
            type_tag: 1,
            codec_id: 0,
            null_count: 0,
            row_count: 2,
            min: min.to_vec(),
            max: max.to_vec(),
        }
    }

    #[test]
    fn can_skip_block_when_range_is_disjoint() {
        // Predicate on the user-key column (id 0) for keys in [m, z].
        let pred = ColumnRangePredicate {
            column_id: 0,
            lower: Some(b"m".to_vec()),
            upper: Some(b"z".to_vec()),
        };
        // Block whose keys are [a, c]: entirely below the lower bound -> skip.
        assert!(pred.can_skip_block(&[stats(0, b"a", b"c")]));
        // Block whose keys are [za, zz]: entirely above the upper bound -> skip.
        assert!(pred.can_skip_block(&[stats(0, b"za", b"zz")]));
        // Block whose keys are [p, t]: overlaps -> cannot skip.
        assert!(!pred.can_skip_block(&[stats(0, b"p", b"t")]));
        // No stats for the column -> conservative, cannot skip.
        assert!(!pred.can_skip_block(&[stats(7, b"a", b"c")]));
    }

    #[test]
    fn matching_rows_filters_the_key_column() {
        // Two rows with keys "alpha" and "bravo"; filter to keys >= "b".
        let batch =
            entries_to_column_batch(&[entry(b"alpha", 10, b"v1"), entry(b"bravo", 9, b"v2")])
                .expect("transpose");
        let pred = ColumnRangePredicate {
            column_id: 0, // user-key column
            lower: Some(b"b".to_vec()),
            upper: None,
        };
        assert_eq!(pred.matching_rows(&batch), vec![false, true]);
    }

    #[test]
    fn matching_rows_all_true_when_column_absent() {
        let batch = entries_to_column_batch(&[entry(b"k", 1, b"v")]).expect("transpose");
        // No column 99 in the batch -> cannot filter, every row matches.
        let pred = ColumnRangePredicate {
            column_id: 99,
            lower: Some(b"z".to_vec()),
            upper: None,
        };
        assert_eq!(pred.matching_rows(&batch), vec![true]);
    }

    #[test]
    fn filter_batch_keeps_only_masked_rows() {
        // Three rows; keep rows 0 and 2. The round trip through the transpose
        // checks every intrinsic column (key, seqno, value type, value) is
        // compacted correctly.
        let entries = vec![
            entry(b"aaa", 3, b"va"),
            entry(b"bbb", 2, b"vb"),
            entry(b"ccc", 1, b"vc"),
        ];
        let batch = entries_to_column_batch(&entries).expect("transpose");
        let filtered = filter_batch(&batch, &[true, false, true]);
        assert_eq!(filtered.row_count, 2);

        let back = column_batch_to_entries(&filtered).expect("untranspose");
        assert_eq!(back.len(), 2);
        assert_eq!(&*back[0].key.user_key, b"aaa");
        assert_eq!(back[0].key.seqno, 3);
        assert_eq!(&*back[0].value, b"va");
        assert_eq!(&*back[1].key.user_key, b"ccc");
        assert_eq!(back[1].key.seqno, 1);
        assert_eq!(&*back[1].value, b"vc");
    }

    #[test]
    fn byte_eq_simd_matches_scalar_on_a_corpus() {
        // A 1000-byte value-type corpus (values 0..3), filtered to value 1. On
        // this host the dispatch runs the widest available kernel; it must be
        // bit-identical to the portable scalar reference.
        let mut data = Vec::new();
        for i in 0..1000u32 {
            data.push(u8::try_from(i % 4).unwrap_or(0));
        }
        let batch = ColumnBatch {
            row_count: u32::try_from(data.len()).unwrap_or(0),
            columns: vec![Column {
                column_id: 2,
                type_tag: TypeTag::Fixed(1),
                validity: None,
                data: data.clone(),
            }],
        };
        assert_eq!(
            byte_eq_mask(&batch, 2, 1),
            byte_eq_scalar(&data, 1),
            "the SIMD byte-eq kernel must equal the scalar reference"
        );
    }

    /// Builds a `Bytes` column from row values: a `(rows + 1)` u32 offset table
    /// followed by the concatenated payload.
    fn bytes_column(column_id: u16, validity: Option<Vec<u8>>, rows: &[&[u8]]) -> Column {
        let mut data = Vec::new();
        let mut acc = 0u32;
        data.extend_from_slice(&acc.to_le_bytes());
        for r in rows {
            acc += u32::try_from(r.len()).unwrap_or(0);
            data.extend_from_slice(&acc.to_le_bytes());
        }
        for r in rows {
            data.extend_from_slice(r);
        }
        Column {
            column_id,
            type_tag: TypeTag::Bytes,
            validity,
            data,
        }
    }

    #[test]
    fn matching_rows_excludes_null_rows_and_respects_both_bounds() {
        // Keys a / b / c with row 1 (b) null; predicate [a, z] keeps the two
        // non-null in-range rows and drops the null one.
        let batch = ColumnBatch {
            row_count: 3,
            // rows 0 and 2 valid, row 1 null.
            columns: vec![bytes_column(
                0,
                Some(vec![0b0000_0101]),
                &[b"a", b"b", b"c"],
            )],
        };
        let pred = ColumnRangePredicate {
            column_id: 0,
            lower: Some(b"a".to_vec()),
            upper: Some(b"z".to_vec()),
        };
        assert_eq!(pred.matching_rows(&batch), vec![true, false, true]);
    }

    #[test]
    fn matching_rows_all_true_for_a_non_bytes_column() {
        // A fixed-width column is not row-filterable (its stored form is not the
        // comparable encoding), so every row passes.
        let batch = ColumnBatch {
            row_count: 2,
            columns: vec![Column {
                column_id: 1,
                type_tag: TypeTag::Fixed(8),
                validity: None,
                data: vec![0u8; 16],
            }],
        };
        let pred = ColumnRangePredicate {
            column_id: 1,
            lower: Some(vec![5]),
            upper: None,
        };
        assert_eq!(pred.matching_rows(&batch), vec![true, true]);
    }

    #[test]
    fn byte_eq_mask_all_true_when_inapplicable() {
        let batch = ColumnBatch {
            row_count: 2,
            columns: vec![bytes_column(0, None, &[b"a", b"b"])],
        };
        // Absent column -> all true.
        assert_eq!(byte_eq_mask(&batch, 99, 1), vec![true, true]);
        // Present but not fixed-1 -> all true.
        assert_eq!(byte_eq_mask(&batch, 0, 1), vec![true, true]);
    }

    #[test]
    fn filter_batch_compacts_fixed_data_and_validity() {
        // Fixed-1 column with row 1 null; keep rows 0 and 2.
        let batch = ColumnBatch {
            row_count: 3,
            columns: vec![Column {
                column_id: 2,
                type_tag: TypeTag::Fixed(1),
                validity: Some(vec![0b0000_0101]),
                data: vec![10, 20, 30],
            }],
        };
        let filtered = filter_batch(&batch, &[true, false, true]);
        assert_eq!(filtered.row_count, 2);
        let col = &filtered.columns[0];
        assert_eq!(col.data, vec![10, 30], "fixed data keeps rows 0 and 2");
        // Both kept rows were valid, compacted to the low two bits.
        assert_eq!(col.validity, Some(vec![0b0000_0011]));
    }
}
