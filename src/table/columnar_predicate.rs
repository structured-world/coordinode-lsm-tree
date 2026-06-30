// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Predicate model for the vectorized columnar scan.
//!
//! A [`ColumnRangePredicate`] is an inclusive byte-range filter over one column.
//! It drives two pushdowns:
//!
//! - **Block skip** ([`ColumnRangePredicate::can_skip_block`]): compare the
//!   predicate bounds against the per-block zone-map (`ColumnStats`) min / max
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

/// Builds a new batch from `batch`'s rows selected (and reordered) by `indices`.
///
/// Like [`filter_batch`] but driven by an explicit row-index list rather than a
/// per-row bool mask, so it can both *drop* and *reorder* rows. The tree-level
/// merge path (`Tree::columnar_scan` over an overlapping segment group) uses it
/// to gather the surviving rows of a group in key order after a stable sort +
/// newest-wins dedup. An out-of-range index contributes an empty (`Bytes`) or
/// zero-filled (`Fixed`) cell rather than panicking, so a malformed index can
/// never desync a column's framing from the output row count.
#[must_use]
pub(crate) fn take_rows(batch: &ColumnBatch, indices: &[u32]) -> ColumnBatch {
    let rows = batch.row_count as usize;
    let columns = batch
        .columns
        .iter()
        .map(|c| take_column(c, rows, indices))
        .collect();
    // `indices.len()` is the output row count; it fits u32 because every index
    // addresses a row of a single block, whose count is itself a u32.
    let row_count = u32::try_from(indices.len()).unwrap_or(u32::MAX);
    ColumnBatch { row_count, columns }
}

/// Gathers one column to the rows listed in `indices` (in that order),
/// rebuilding its framing (fixed chunks copied, `Bytes` offset table + payload
/// repacked) and its validity bitmap. Sibling of [`filter_column`] for the
/// index-driven [`take_rows`] gather.
fn take_column(col: &Column, rows: usize, indices: &[u32]) -> Column {
    let data = match col.type_tag {
        TypeTag::Fixed(width) => {
            let width = width as usize;
            let mut out = Vec::with_capacity(indices.len() * width);
            for &i in indices {
                let i = i as usize;
                if let Some(start) = i.checked_mul(width)
                    && let Some(end) = start.checked_add(width)
                    && let Some(chunk) = col.data.get(start..end)
                {
                    out.extend_from_slice(chunk);
                } else {
                    // Out-of-range index: zero-fill one cell so the fixed framing
                    // stays `row_count * width` bytes long.
                    out.resize(out.len() + width, 0);
                }
            }
            out
        }
        TypeTag::Bytes => {
            let mut offsets = Vec::with_capacity((indices.len() + 1) * 4);
            let mut payload = Vec::new();
            let mut acc: u32 = 0;
            offsets.extend_from_slice(&acc.to_le_bytes());
            for &i in indices {
                // A missing cell degrades to empty (one offset still written), so
                // the offset table stays in lockstep with the output row count.
                let value = bytes_row(&col.data, rows, i as usize).unwrap_or(&[]);
                payload.extend_from_slice(value);
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
        .map(|bits| take_validity(bits, indices));
    Column {
        column_id: col.column_id,
        type_tag: col.type_tag,
        validity,
        data,
    }
}

/// Rebuilds a validity bitmap for the rows listed in `indices`, preserving each
/// gathered row's null bit in output order. Index-driven counterpart of
/// [`compact_validity`].
fn take_validity(bits: &[u8], indices: &[u32]) -> Vec<u8> {
    let mut out = alloc::vec![0u8; indices.len().div_ceil(8)];
    for (o, &i) in indices.iter().enumerate() {
        let i = i as usize;
        let valid = bits.get(i / 8).is_some_and(|b| b & (1u8 << (i % 8)) != 0);
        if valid && let Some(byte) = out.get_mut(o / 8) {
            *byte |= 1u8 << (o % 8);
        }
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
mod tests;
