// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Positional delete-bitmap for columnar segments.
//!
//! A [`DeleteBitmap`] marks, by row position, which rows of a columnar segment
//! are deleted. It is a pure membership set: MVCC reconciliation happens at
//! materialization time (a row is added only once its deleting tombstone is
//! visible to every live snapshot, i.e. its seqno is below the compaction
//! watermark), so the read path is a plain `contains` check with no per-row
//! seqno to interpret.
//!
//! # Layout
//!
//! Rows are grouped into fixed [`CHUNK_ROWS`]-row chunks (a bulk delete of a
//! whole chunk is therefore O(1), not O(rows)). Each non-empty chunk is stored
//! either as a sorted [`Container::Sparse`] array of in-chunk offsets, or, once
//! it holds more than [`SPARSE_MAX`] rows, as a [`Container::Dense`] bitset.
//! This mirrors a Roaring bitmap's array/bitset containers but at a fixed
//! 2048-row granularity tuned to the columnar block size. Empty chunks are not
//! stored.
//!
//! # no-std
//!
//! Build, query, union, and the serialized [`DeleteBitmap::encode`] /
//! [`DeleteBitmap::decode`] round-trip are all `core` + `alloc` only, so an
//! embedded or WASM reader can apply deletes without `std`.
//!
//! # Examples
//!
//! ```
//! use lsm_tree::table::delete_bitmap::DeleteBitmap;
//!
//! let mut dv = DeleteBitmap::new();
//! dv.insert(5);
//! dv.insert(5000);
//! assert!(dv.contains(5));
//! assert!(!dv.contains(6));
//!
//! let bytes = dv.encode();
//! let decoded = DeleteBitmap::decode(&bytes).unwrap();
//! assert_eq!(decoded, dv);
//! ```

use crate::{Error, Result};
use alloc::boxed::Box;
use alloc::vec::Vec;

/// Rows per chunk. A bulk delete spanning a full chunk costs O(1) bitmap work.
pub const CHUNK_ROWS: u32 = 2048;

/// 64-bit words in a dense chunk bitset (`CHUNK_ROWS / 64`).
const WORDS_PER_CHUNK: usize = (CHUNK_ROWS as usize) / 64;

/// Sparse-to-dense break-even: a dense chunk is `WORDS_PER_CHUNK * 8` bytes; a
/// sparse `u16` offset is 2 bytes, so beyond this many rows the dense bitset is
/// the smaller (and faster) representation.
const SPARSE_MAX: usize = WORDS_PER_CHUNK * 4;

const KIND_SPARSE: u8 = 0;
const KIND_DENSE: u8 = 1;

/// Splits a row position into its `(chunk index, in-chunk offset)`. The offset
/// is `row % CHUNK_ROWS`, which the compiler proves fits `u16`.
fn split(row: u32) -> (u32, u16) {
    (row / CHUNK_ROWS, (row % CHUNK_ROWS) as u16)
}

/// One chunk's in-chunk row offsets, in the representation that is currently
/// smaller for its cardinality.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Container {
    /// Sorted, deduplicated in-chunk offsets in `0..CHUNK_ROWS`.
    Sparse(Vec<u16>),
    /// Dense `CHUNK_ROWS`-bit bitset, word `w` bit `b` = row `w * 64 + b`.
    Dense(Box<[u64; WORDS_PER_CHUNK]>),
}

impl Container {
    fn contains(&self, off: u16) -> bool {
        match self {
            Self::Sparse(offs) => offs.binary_search(&off).is_ok(),
            Self::Dense(words) => {
                let (w, b) = (off as usize / 64, off as usize % 64);
                words.get(w).is_some_and(|word| word & (1u64 << b) != 0)
            }
        }
    }

    /// Inserts `off`; returns `true` if it was newly added.
    fn insert(&mut self, off: u16) -> bool {
        match self {
            Self::Sparse(offs) => match offs.binary_search(&off) {
                Ok(_) => false,
                Err(pos) => {
                    offs.insert(pos, off);
                    if offs.len() > SPARSE_MAX {
                        *self = Self::densify(offs);
                    }
                    true
                }
            },
            Self::Dense(words) => {
                let (w, b) = (off as usize / 64, off as usize % 64);
                let mask = 1u64 << b;
                // off < CHUNK_ROWS is a caller invariant, so the word exists; the
                // bounds-checked access keeps the read path panic-free regardless.
                let Some(word) = words.get_mut(w) else {
                    return false;
                };
                let was = *word & mask != 0;
                *word |= mask;
                !was
            }
        }
    }

    fn densify(offs: &[u16]) -> Self {
        let mut words = Box::new([0u64; WORDS_PER_CHUNK]);
        for &off in offs {
            if let Some(word) = words.get_mut(off as usize / 64) {
                *word |= 1u64 << (off as usize % 64);
            }
        }
        Self::Dense(words)
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "a chunk holds at most CHUNK_ROWS (2048) distinct offsets, well within u32"
    )]
    fn cardinality(&self) -> u32 {
        match self {
            Self::Sparse(offs) => offs.len() as u32,
            Self::Dense(words) => words.iter().map(|w| w.count_ones()).sum(),
        }
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "w < 32 and b < 64, so w*64 + b < 2048 fits u16"
    )]
    fn for_each<F: FnMut(u16)>(&self, mut f: F) {
        match self {
            Self::Sparse(offs) => offs.iter().for_each(|&o| f(o)),
            Self::Dense(words) => {
                for (w, &word) in words.iter().enumerate() {
                    let mut bits = word;
                    while bits != 0 {
                        let b = bits.trailing_zeros() as usize;
                        // w < 32 and b < 64, so w*64 + b < 2048 fits in u16.
                        f((w * 64 + b) as u16);
                        bits &= bits - 1;
                    }
                }
            }
        }
    }
}

/// A positional delete set over the rows of one columnar segment.
///
/// See the [module documentation](self) for the layout and MVCC semantics.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeleteBitmap {
    /// Non-empty chunks, kept sorted by chunk index for `contains` lookups.
    chunks: Vec<(u32, Container)>,
}

impl DeleteBitmap {
    /// Creates an empty delete set (no rows deleted).
    #[must_use]
    pub fn new() -> Self {
        Self { chunks: Vec::new() }
    }

    /// Returns `true` if no rows are marked deleted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Returns the number of deleted rows.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.chunks
            .iter()
            .map(|(_, c)| u64::from(c.cardinality()))
            .sum()
    }

    /// Marks row `row` deleted. Returns `true` if it was not already deleted.
    pub fn insert(&mut self, row: u32) -> bool {
        let (chunk, off) = split(row);
        match self.chunks.binary_search_by_key(&chunk, |(c, _)| *c) {
            Ok(pos) => self.chunks.get_mut(pos).is_some_and(|(_, c)| c.insert(off)),
            Err(pos) => {
                self.chunks
                    .insert(pos, (chunk, Container::Sparse(alloc::vec![off])));
                true
            }
        }
    }

    /// Returns `true` if row `row` is marked deleted.
    #[must_use]
    pub fn contains(&self, row: u32) -> bool {
        let (chunk, off) = split(row);
        match self.chunks.binary_search_by_key(&chunk, |(c, _)| *c) {
            Ok(pos) => self.chunks.get(pos).is_some_and(|(_, c)| c.contains(off)),
            Err(_) => false,
        }
    }

    /// Returns `true` if any row in chunk `chunk_index` is deleted. Lets a scan
    /// skip an entire untouched block in O(log chunks) without per-row checks.
    #[must_use]
    pub fn chunk_has_deletes(&self, chunk_index: u32) -> bool {
        self.chunks
            .binary_search_by_key(&chunk_index, |(c, _)| *c)
            .is_ok()
    }

    /// Unions `other` into `self` (merge-on-write: the result marks a row
    /// deleted iff either side did).
    pub fn union(&mut self, other: &Self) {
        for (chunk, container) in &other.chunks {
            match self.chunks.binary_search_by_key(chunk, |(c, _)| *c) {
                Ok(pos) => {
                    if let Some((_, dst)) = self.chunks.get_mut(pos) {
                        container.for_each(|off| {
                            dst.insert(off);
                        });
                    }
                }
                Err(pos) => self.chunks.insert(pos, (*chunk, container.clone())),
            }
        }
    }

    /// Iterates the deleted row positions in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.chunks.iter().flat_map(|(chunk, container)| {
            let base = chunk * CHUNK_ROWS;
            let mut rows = Vec::with_capacity(container.cardinality() as usize);
            container.for_each(|off| rows.push(base + u32::from(off)));
            rows.into_iter()
        })
    }

    /// Serializes to the portable on-disk form (`core` + `alloc` decodable).
    ///
    /// Layout: `u32` non-empty-chunk count, then per chunk `u32` index, `u8`
    /// kind, and the container payload (sparse: `u16` count + offsets; dense:
    /// [`WORDS_PER_CHUNK`] little-endian `u64` words).
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "chunk count <= row_count/2048 fits u32; a chunk holds <= 2048 offsets, fitting u16"
    )]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.chunks.len() as u32).to_le_bytes());
        for (chunk, container) in &self.chunks {
            out.extend_from_slice(&chunk.to_le_bytes());
            match container {
                Container::Sparse(offs) => {
                    out.push(KIND_SPARSE);
                    out.extend_from_slice(&(offs.len() as u16).to_le_bytes());
                    for &off in offs {
                        out.extend_from_slice(&off.to_le_bytes());
                    }
                }
                Container::Dense(words) => {
                    out.push(KIND_DENSE);
                    for &word in words.iter() {
                        out.extend_from_slice(&word.to_le_bytes());
                    }
                }
            }
        }
        out
    }

    /// Decodes a buffer produced by [`DeleteBitmap::encode`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeader`] on a truncated buffer, an unknown
    /// container kind, an out-of-range offset, or chunk indices that are not
    /// strictly ascending.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cur = Cursor::new(bytes);
        let chunk_count = cur.u32()?;
        let mut chunks = Vec::with_capacity(chunk_count as usize);
        let mut last_chunk: Option<u32> = None;
        for _ in 0..chunk_count {
            let chunk = cur.u32()?;
            if last_chunk.is_some_and(|p| chunk <= p) {
                return Err(Error::InvalidHeader(
                    "delete_bitmap: chunk indices not strictly ascending",
                ));
            }
            last_chunk = Some(chunk);
            let container = match cur.u8()? {
                KIND_SPARSE => {
                    let count = cur.u16()? as usize;
                    let mut offs = Vec::with_capacity(count);
                    let mut last: Option<u16> = None;
                    for _ in 0..count {
                        let off = cur.u16()?;
                        if u32::from(off) >= CHUNK_ROWS {
                            return Err(Error::InvalidHeader(
                                "delete_bitmap: sparse offset out of range",
                            ));
                        }
                        if last.is_some_and(|p| off <= p) {
                            return Err(Error::InvalidHeader(
                                "delete_bitmap: sparse offsets not strictly ascending",
                            ));
                        }
                        last = Some(off);
                        offs.push(off);
                    }
                    Container::Sparse(offs)
                }
                KIND_DENSE => {
                    let mut words = Box::new([0u64; WORDS_PER_CHUNK]);
                    for word in words.iter_mut() {
                        *word = cur.u64()?;
                    }
                    Container::Dense(words)
                }
                _ => {
                    return Err(Error::InvalidHeader(
                        "delete_bitmap: unknown container kind",
                    ));
                }
            };
            chunks.push((chunk, container));
        }
        Ok(Self { chunks })
    }
}

/// Little-endian cursor over a byte buffer with bounds-checked reads.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        let end = self
            .pos
            .checked_add(N)
            .ok_or(Error::InvalidHeader("delete_bitmap: length overflow"))?;
        let arr = self
            .buf
            .get(self.pos..end)
            .and_then(|s| <[u8; N]>::try_from(s).ok())
            .ok_or(Error::InvalidHeader("delete_bitmap: truncated buffer"))?;
        self.pos = end;
        Ok(arr)
    }

    fn u8(&mut self) -> Result<u8> {
        let [b] = self.take()?;
        Ok(b)
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take()?))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take()?))
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "test assertions over fixed in-test buffers"
)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_contains_across_chunks() {
        let mut dv = DeleteBitmap::new();
        assert!(dv.is_empty());
        assert!(dv.insert(0));
        assert!(dv.insert(CHUNK_ROWS - 1)); // last row of chunk 0
        assert!(dv.insert(CHUNK_ROWS)); // first row of chunk 1
        assert!(dv.insert(100_000));
        assert!(!dv.insert(0)); // already present

        assert!(dv.contains(0));
        assert!(dv.contains(CHUNK_ROWS - 1));
        assert!(dv.contains(CHUNK_ROWS));
        assert!(dv.contains(100_000));
        assert!(!dv.contains(1));
        assert!(!dv.contains(CHUNK_ROWS + 1));
        assert_eq!(dv.len(), 4);
        assert!(!dv.is_empty());
    }

    #[test]
    fn sparse_promotes_to_dense_and_stays_correct() {
        let mut dv = DeleteBitmap::new();
        // Fill one chunk past the sparse-to-dense threshold with even rows.
        let n = (SPARSE_MAX as u32 + 50).min(CHUNK_ROWS / 2);
        for i in 0..n {
            assert!(dv.insert(i * 2));
        }
        assert_eq!(dv.len(), u64::from(n));
        for i in 0..n {
            assert!(dv.contains(i * 2), "even row {} missing", i * 2);
            assert!(!dv.contains(i * 2 + 1), "odd row {} present", i * 2 + 1);
        }
    }

    #[test]
    fn chunk_has_deletes_enables_block_skip() {
        let mut dv = DeleteBitmap::new();
        dv.insert(3 * CHUNK_ROWS + 7);
        assert!(!dv.chunk_has_deletes(0));
        assert!(!dv.chunk_has_deletes(2));
        assert!(dv.chunk_has_deletes(3));
        assert!(!dv.chunk_has_deletes(4));
    }

    #[test]
    fn full_chunk_bulk_delete_is_dense_and_compact() {
        // A bulk delete of a whole chunk is O(1) bitmap work: the chunk collapses
        // to a single dense bitset (O(1) space, not O(rows) sparse offsets) and is
        // skipped / queried in O(1).
        let mut dv = DeleteBitmap::new();
        for off in 0..CHUNK_ROWS {
            dv.insert(CHUNK_ROWS + off);
        }
        assert_eq!(
            dv.len(),
            u64::from(CHUNK_ROWS),
            "every row of the chunk marked"
        );
        assert!(dv.chunk_has_deletes(1), "the full chunk reports deletes");
        assert!(
            !dv.chunk_has_deletes(0),
            "an untouched neighbour skips in O(1)"
        );
        assert!(!dv.chunk_has_deletes(2));
        // Boundary rows present; their neighbours in adjacent chunks are not.
        assert!(dv.contains(CHUNK_ROWS));
        assert!(dv.contains(2 * CHUNK_ROWS - 1));
        assert!(!dv.contains(CHUNK_ROWS - 1));
        assert!(!dv.contains(2 * CHUNK_ROWS));
        // Dense storage: a full chunk encodes to one bounded bitset, far below the
        // ~2 bytes/row a sparse array of CHUNK_ROWS offsets would need.
        let encoded = dv.encode();
        assert!(
            encoded.len() < CHUNK_ROWS as usize,
            "a full chunk must store densely (O(1) space), got {} bytes for {CHUNK_ROWS} rows",
            encoded.len(),
        );
    }

    #[test]
    fn union_is_set_union() {
        let mut a = DeleteBitmap::new();
        a.insert(1);
        a.insert(CHUNK_ROWS + 1);
        let mut b = DeleteBitmap::new();
        b.insert(1); // overlap
        b.insert(2);
        b.insert(5 * CHUNK_ROWS); // new chunk
        a.union(&b);

        for row in [1, 2, CHUNK_ROWS + 1, 5 * CHUNK_ROWS] {
            assert!(a.contains(row), "row {row} missing after union");
        }
        assert_eq!(a.len(), 4);
    }

    #[test]
    fn iter_yields_ascending_rows() {
        let mut dv = DeleteBitmap::new();
        let rows = [9_u32, 1, CHUNK_ROWS + 3, 2, CHUNK_ROWS];
        for &r in &rows {
            dv.insert(r);
        }
        let got: Vec<u32> = dv.iter().collect();
        assert_eq!(got, [1, 2, 9, CHUNK_ROWS, CHUNK_ROWS + 3]);
    }

    #[test]
    fn encode_decode_round_trip_sparse_and_dense() {
        let mut dv = DeleteBitmap::new();
        // Sparse chunk.
        dv.insert(5);
        dv.insert(900);
        // Dense chunk (cross the threshold).
        for i in 0..=(SPARSE_MAX as u32) {
            dv.insert(CHUNK_ROWS + i);
        }
        let bytes = dv.encode();
        let decoded = DeleteBitmap::decode(&bytes).unwrap();
        assert_eq!(decoded, dv);
    }

    #[test]
    fn decode_empty_round_trips() {
        let dv = DeleteBitmap::new();
        let decoded = DeleteBitmap::decode(&dv.encode()).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(decoded, dv);
    }

    #[test]
    fn decode_truncated_buffer_errors() {
        let mut dv = DeleteBitmap::new();
        dv.insert(7);
        let bytes = dv.encode();
        assert!(DeleteBitmap::decode(&bytes[..bytes.len() - 1]).is_err());
        assert!(DeleteBitmap::decode(&[]).is_err());
    }

    #[test]
    fn decode_rejects_unknown_kind() {
        // 1 chunk, index 0, kind 99.
        let bytes = [1, 0, 0, 0, 0, 0, 0, 0, 99];
        assert!(DeleteBitmap::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_out_of_range_offset() {
        // 1 chunk, index 0, sparse, count 1, offset = CHUNK_ROWS (out of range).
        let mut bytes = alloc::vec![1, 0, 0, 0, 0, 0, 0, 0, KIND_SPARSE, 1, 0];
        bytes.extend_from_slice(&(CHUNK_ROWS as u16).to_le_bytes());
        assert!(DeleteBitmap::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_non_ascending_chunks() {
        // 2 chunks both index 0 (not strictly ascending).
        let bytes = [
            2,
            0,
            0,
            0, // count
            0,
            0,
            0,
            0,
            KIND_SPARSE,
            0,
            0, // chunk 0, empty sparse
            0,
            0,
            0,
            0,
            KIND_SPARSE,
            0,
            0, // chunk 0 again
        ];
        assert!(DeleteBitmap::decode(&bytes).is_err());
    }
}
