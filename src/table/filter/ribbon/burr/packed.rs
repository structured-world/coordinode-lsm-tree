//! Bit-sliced storage for a BuRR layer's solution matrix.
//!
//! The solver produces one row per slot, each holding that row's `r`-bit
//! value in the low bits of a `u64`. Storing those rows verbatim costs 64
//! bits per row whatever `r` is, and a probe then has to XOR-reduce one
//! 8-byte row per set coefficient bit — about 32 rows scattered across a
//! 512-byte band window.
//!
//! This module stores the same matrix transposed: rows are grouped into
//! **segments** of 64, and each segment contributes `r` words, word `j`
//! holding bit `j` of all 64 rows. The payload is then `segments * r`
//! words, i.e. `r` bits per row rather than 64, and one result bit is a
//! single `popcount` parity over the coefficient mask. A membership probe
//! compares that bit against the fingerprint immediately and stops at the
//! first disagreement, which for an absent key is about two bits.
//!
//! A segment is 64 rows, not `b` rows: `b` is configurable in `64..=255`
//! and need not be a multiple of 64, so the block grid the thresholds use
//! is not a word grid. `m` is a multiple of `b`, so the final segment is
//! partial whenever `b` is not itself a multiple of 64; the rows past `m`
//! in that segment are zero and never addressed, because a band starts at
//! `start <= m - w` and spans 64 rows.
//!
//! Word `(segment, bit)` lives at word index `segment * r + bit`, so the
//! `r` words a probe needs from one segment are adjacent. A band straddles
//! at most two segments, and the two runs are `r * 8` bytes apart.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Rows per segment: one machine word's worth of rows per column word.
pub(crate) const SEGMENT_ROWS: usize = 64;

/// Number of segments a layer of `m` rows occupies.
#[must_use]
pub(crate) const fn segments_for(m: usize) -> usize {
    m.div_ceil(SEGMENT_ROWS)
}

/// Byte length of the bit-sliced payload for `m` rows at `r` bits per row,
/// or `None` if the product overflows `usize`.
#[must_use]
pub(crate) fn z_byte_len(m: usize, r: u8) -> Option<usize> {
    segments_for(m)
        .checked_mul(usize::from(r))
        .and_then(|words| words.checked_mul(8))
}

/// Transposes row-major solution rows into the bit-sliced layout.
///
/// `rows[i]` holds row `i`'s value in its low `r` bits; any higher bit is
/// ignored, so a caller that has already masked to `r` and one that has not
/// produce the same payload. The result has `segments_for(rows.len()) * r`
/// words, ordered segment-major.
#[must_use]
pub(crate) fn transpose(rows: &[u64], r: u8) -> Vec<u64> {
    let r = usize::from(r);
    let mut out = alloc::vec![0u64; segments_for(rows.len()) * r];
    // Scatter rather than gather: walking the rows once and spreading each
    // row's set bits costs one iteration per SET bit, where gathering per
    // (segment, bit) pair would read all 64 rows for every one of the r
    // columns whether or not the bit is set.
    for (i, &row) in rows.iter().enumerate() {
        let base = (i / SEGMENT_ROWS) * r;
        let bit = 1u64 << (i % SEGMENT_ROWS);
        let mut value = if r == 64 {
            row
        } else {
            row & ((1u64 << r) - 1)
        };
        while value != 0 {
            let j = value.trailing_zeros() as usize;
            value &= value - 1;
            // `j < r` because `value` was masked to r bits, and the segment
            // index is `i / 64 < segments`, so the word index is in range.
            if let Some(word) = out.get_mut(base + j) {
                *word |= bit;
            }
        }
    }
    out
}

/// Inverse of [`transpose`]: rebuilds `row_count` row-major values from the
/// bit-sliced words. Used by the round-trip tests and by the diagnostic
/// tooling that prints a layer's rows.
#[must_use]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "inverse of the transpose; exercised by the round-trip tests \
                  and kept next to it so the two stay in step"
    )
)]
pub(crate) fn untranspose(words: &[u64], row_count: usize, r: u8) -> Vec<u64> {
    let r = usize::from(r);
    let mut rows = alloc::vec![0u64; row_count];
    for (i, row) in rows.iter_mut().enumerate() {
        let base = (i / SEGMENT_ROWS) * r;
        let bit = i % SEGMENT_ROWS;
        let mut value = 0u64;
        for j in 0..r {
            let Some(&word) = words.get(base + j) else {
                break;
            };
            value |= ((word >> bit) & 1) << j;
        }
        *row = value;
    }
    rows
}

/// Outcome of reducing a band against the bit-sliced columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BandWalk {
    /// Every requested result bit was reduced. For a retrieval walk `acc`
    /// holds all `r` bits; for a membership walk it holds the bits reduced
    /// so far and is only meaningful because they all matched.
    Reduced(u64),
    /// Membership only: a result bit disagreed with the fingerprint, so the
    /// key is definitely absent and the remaining bits were not read.
    Mismatch,
    /// A column word lay past the end of the payload. The caller fails
    /// closed rather than substituting zeros, which would turn a truncated
    /// block into a false negative.
    ///
    /// An early-out walk can report [`BandWalk::Mismatch`] on a truncated
    /// payload instead of reaching the missing word, and that is sound: the
    /// bits it did read are the real stored bits, and a disagreement among
    /// them proves the key absent whatever the missing words held. A key
    /// that IS present agrees on every readable bit — its stored value is
    /// the fingerprint — so it always walks as far as the truncation and
    /// fails closed there.
    Truncated,
}

/// Reads one column word out of the little-endian payload.
#[inline]
fn column_word(z: &[u8], word_index: usize) -> Option<u64> {
    let start = word_index.checked_mul(8)?;
    let end = start.checked_add(8)?;
    let slice = z.get(start..end)?;
    <[u8; 8]>::try_from(slice).ok().map(u64::from_le_bytes)
}

/// Reduces the band `[start, start + 64)` against the bit-sliced columns.
///
/// With `EARLY_OUT` the walk is a membership probe: each result bit is
/// compared against the matching bit of `fingerprint` as soon as it is
/// computed, and the walk returns [`BandWalk::Mismatch`] at the first
/// disagreement — on a random absent key that is about two bits, so the
/// probe touches a few adjacent words instead of the whole column set.
/// Without it the walk is a retrieval: all `r` bits are read and returned.
///
/// `start` must satisfy `start + 64 <= m`, which the equation generator
/// guarantees (`start` is drawn from `0..=m - w` with `w == 64`).
#[inline]
pub(crate) fn walk_band<const EARLY_OUT: bool>(
    z: &[u8],
    r: u8,
    start: usize,
    coeff_lo: u64,
    fingerprint: u64,
) -> BandWalk {
    let r_usize = usize::from(r);
    let offset = start % SEGMENT_ROWS;
    let lo_base = (start / SEGMENT_ROWS) * r_usize;
    // The band spans a second segment exactly when it does not start on a
    // segment boundary. `start + 63 <= m - 1` makes that segment present.
    let hi_base = lo_base + r_usize;

    let mut acc: u64 = 0;
    for j in 0..r_usize {
        let Some(lo) = column_word(z, lo_base + j) else {
            return BandWalk::Truncated;
        };
        // Shifting by 64 is undefined, so the aligned case reads one word.
        let band = if offset == 0 {
            lo
        } else {
            let Some(hi) = column_word(z, hi_base + j) else {
                return BandWalk::Truncated;
            };
            (lo >> offset) | (hi << (SEGMENT_ROWS - offset))
        };
        let bit = u64::from((band & coeff_lo).count_ones() & 1);
        if EARLY_OUT && bit != ((fingerprint >> j) & 1) {
            return BandWalk::Mismatch;
        }
        acc |= bit << j;
    }
    BandWalk::Reduced(acc)
}

#[cfg(test)]
#[expect(
    clippy::indexing_slicing,
    reason = "test code indexes and slices fixture buffers of known size"
)]
mod tests;
