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

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests {
    use super::{ColumnRangePredicate, ColumnStats};
    use crate::table::columnar::entries_to_column_batch;
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
}
