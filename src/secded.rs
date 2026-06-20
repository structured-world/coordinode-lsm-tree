// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Pluggable single-error-correct / double-error-detect (SEC-DED) word codecs.
//!
//! This is the cheap per-word single-bit heal that runs on the Page ECC read
//! path *before* the heavier shard-based Reed-Solomon recovery in
//! [`crate::ecc`]: an isolated bit flip is corrected with O(block) work and no
//! shard enumeration (the software analogue of DRAM SEC-DED), and a word with
//! two bit errors is reported uncorrectable so the RS path can take over.
//!
//! # Pluggable shapes
//!
//! [`SecdedCodec`](crate::secded::SecdedCodec) abstracts the word geometry so alternative shapes (wider
//! words for lower overhead, narrower for denser correction) can be added
//! without touching the read/write paths. The default, [`Hsiao7264`](crate::secded::Hsiao7264), is the
//! industry-standard DRAM ECC shape: 8 check bits protect a 64-bit data word
//! (12.5% overhead), with a Hsiao parity-check matrix (all columns odd-weight
//! and distinct) so a single-bit error yields an odd nonzero syndrome that
//! pinpoints the flipped bit, a double-bit error yields an even nonzero
//! syndrome that is detected but not "corrected" to a wrong value, and a clean
//! word yields a zero syndrome.
//!
//! Encoding is table-driven for speed: the 64-bit word is read as eight bytes,
//! each indexing a 256-entry table of that byte-lane's partial check value, and
//! the eight partials are XORed — eight loads plus XORs per word, branchless,
//! no popcount loop.

// Every index in this module is provably in range at compile time: byte lanes
// `0..8`, byte values `0..256` into `[_; 256]` arrays, and the 64 data-bit
// positions into `[_; 64]`. The bounds are static, so the indexing is
// panic-free; `clippy::indexing_slicing` can't see the static range, and
// `.get().unwrap()` is both noisier and unavailable inside the `const fn` table
// builders, so direct indexing stands in this self-contained codec.
#![expect(
    clippy::indexing_slicing,
    reason = "all indexing in this module is statically bounded by the codec / table geometry"
)]

/// Outcome of decoding one SEC-DED-protected data word.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SecdedOutcome {
    /// Syndrome was zero: the word and its check bits are intact.
    Clean,
    /// A correctable single-bit syndrome was observed. If the flipped bit was in
    /// the data word, `data` now holds the repaired value; a lone check-bit flip
    /// also yields `Corrected` but leaves `data` unchanged (only the parity bit
    /// was wrong). Callers that need certainty should revalidate the enclosing
    /// checksum before trusting the correction.
    Corrected,
    /// A nonzero syndrome that this code cannot correct was observed (for
    /// example a detected double-bit error); the word is left unchanged and the
    /// caller should fall through to the heavier recovery path. SEC-DED
    /// guarantees detection for double-bit errors; 3+ bit patterns may alias to
    /// any of the three outcomes, so this is not an exact error count.
    Uncorrectable,
}

/// A single-error-correct / double-error-detect codec over a fixed-size data
/// word.
///
/// Implementations protect each `DATA_WORD_BYTES`-byte data word with
/// `PARITY_BYTES` of check bits. The overhead is `PARITY_BYTES /
/// DATA_WORD_BYTES`.
pub trait SecdedCodec {
    /// Size, in bytes, of the data word each parity group protects.
    const DATA_WORD_BYTES: usize;
    /// Size, in bytes, of the parity (check) bits per data word.
    const PARITY_BYTES: usize;

    /// Computes the parity for one full `DATA_WORD_BYTES` data word, writing it
    /// into the first `PARITY_BYTES` of `parity_out`.
    ///
    /// # Panics
    ///
    /// Panics if `data.len() != DATA_WORD_BYTES` or `parity_out.len() <
    /// PARITY_BYTES` (codec-internal invariants the caller upholds by framing
    /// words to the exact size).
    fn encode_word(data: &[u8], parity_out: &mut [u8]);

    /// Verifies one data word against its stored parity, correcting a single
    /// data-bit error in place when possible.
    ///
    /// On [`SecdedOutcome::Corrected`] `data` is repaired only when the flipped
    /// bit was in the data word; a lone check-bit flip also returns `Corrected`
    /// but leaves `data` untouched. `Clean` and `Uncorrectable` never mutate
    /// `data`. Callers should revalidate the enclosing checksum before trusting
    /// a correction.
    ///
    /// # Panics
    ///
    /// Panics if `data.len() != DATA_WORD_BYTES` or `parity.len() <
    /// PARITY_BYTES`.
    fn decode_word(data: &mut [u8], parity: &[u8]) -> SecdedOutcome;
}

/// Standard DRAM-style Hsiao SEC-DED over a 64-bit data word with 8 check bits
/// (the `(72, 64)` code): 12.5% overhead, single-bit correct, double-bit
/// detect.
pub struct Hsiao7264;

/// Number of 8-bit Hsiao parity-check columns assigned to data bits.
const DATA_BITS: usize = 64;

/// The data-bit columns of the Hsiao parity-check matrix: 64 distinct 8-bit
/// patterns, each of odd weight `>= 3`.
///
/// Odd weight makes a single-bit error's syndrome odd (so it never aliases a
/// zero/clean syndrome), weight `>= 3` keeps data columns disjoint from the
/// eight weight-1 check-bit identity columns, and distinctness makes every
/// single-bit syndrome unique (locatable). A two-bit error XORs two odd-weight
/// columns into an even-weight syndrome that matches no single column, so it is
/// detected rather than miscorrected.
const DATA_COLS: [u8; DATA_BITS] = build_data_cols();

/// Builds [`DATA_COLS`] deterministically: the first 64 byte values (ascending)
/// whose population count is odd and at least 3.
const fn build_data_cols() -> [u8; DATA_BITS] {
    let mut cols = [0u8; DATA_BITS];
    let mut filled = 0usize;
    let mut byte: u8 = 0;
    loop {
        let ones = byte.count_ones();
        if ones % 2 == 1 && ones >= 3 {
            cols[filled] = byte;
            filled += 1;
        }
        // Stop before `byte` would overflow past 255; 120 odd-weight-(>=3)
        // bytes exist, so 64 are always filled well before then.
        if byte == u8::MAX || filled == DATA_BITS {
            break;
        }
        byte += 1;
    }
    cols
}

/// Per-byte-lane partial-check tables: `TABLES[lane][b]` is the XOR of the
/// Hsiao columns for the data bits set in byte value `b` at byte lane `lane`
/// (lane `i` covers data bits `8*i .. 8*i+8`). The full check byte is the XOR
/// of the eight lanes' table lookups.
const TABLES: [[u8; 256]; 8] = build_tables();

/// Builds [`TABLES`] from [`DATA_COLS`] at compile time.
const fn build_tables() -> [[u8; 256]; 8] {
    let mut tables = [[0u8; 256]; 8];
    let mut lane = 0usize;
    while lane < 8 {
        let mut b = 0usize;
        while b < 256 {
            let mut acc = 0u8;
            let mut bit = 0usize;
            while bit < 8 {
                if (b >> bit) & 1 == 1 {
                    acc ^= DATA_COLS[lane * 8 + bit];
                }
                bit += 1;
            }
            tables[lane][b] = acc;
            b += 1;
        }
        lane += 1;
    }
    tables
}

/// Computes the 8-bit check value for a 64-bit data word via the byte-lane
/// tables (eight loads + XORs, no popcount loop).
#[inline]
fn check_byte(word: u64) -> u8 {
    let b = word.to_le_bytes();
    TABLES[0][b[0] as usize]
        ^ TABLES[1][b[1] as usize]
        ^ TABLES[2][b[2] as usize]
        ^ TABLES[3][b[3] as usize]
        ^ TABLES[4][b[4] as usize]
        ^ TABLES[5][b[5] as usize]
        ^ TABLES[6][b[6] as usize]
        ^ TABLES[7][b[7] as usize]
}

impl SecdedCodec for Hsiao7264 {
    const DATA_WORD_BYTES: usize = 8;
    const PARITY_BYTES: usize = 1;

    fn encode_word(data: &[u8], parity_out: &mut [u8]) {
        assert!(
            data.len() == Self::DATA_WORD_BYTES,
            "Hsiao7264 word must be 8 bytes"
        );
        assert!(
            parity_out.len() >= Self::PARITY_BYTES,
            "Hsiao7264 needs 1 parity byte",
        );
        let mut word = [0u8; 8];
        word.copy_from_slice(data);
        parity_out[0] = check_byte(u64::from_le_bytes(word));
    }

    fn decode_word(data: &mut [u8], parity: &[u8]) -> SecdedOutcome {
        assert!(
            data.len() == Self::DATA_WORD_BYTES,
            "Hsiao7264 word must be 8 bytes"
        );
        assert!(
            parity.len() >= Self::PARITY_BYTES,
            "Hsiao7264 needs 1 parity byte",
        );
        let mut word_bytes = [0u8; 8];
        word_bytes.copy_from_slice(data);
        let word = u64::from_le_bytes(word_bytes);

        // Syndrome = stored check XOR recomputed check.
        let syndrome = parity[0] ^ check_byte(word);
        if syndrome == 0 {
            return SecdedOutcome::Clean;
        }

        // A single check-bit flip yields a weight-1 syndrome (the identity
        // column): the data word is intact, only the parity byte rotted.
        if syndrome.is_power_of_two() {
            return SecdedOutcome::Corrected;
        }

        // Odd-weight syndrome that equals a data column ⇒ single data-bit flip
        // at that bit position. (All data columns are odd-weight and distinct.)
        if syndrome.count_ones() % 2 == 1 {
            let mut bit = 0usize;
            while bit < DATA_BITS {
                if DATA_COLS[bit] == syndrome {
                    let corrected = word ^ (1u64 << bit);
                    data.copy_from_slice(&corrected.to_le_bytes());
                    return SecdedOutcome::Corrected;
                }
                bit += 1;
            }
        }

        // Even-weight nonzero syndrome (two-bit error), or an odd syndrome that
        // matches no column: detected, not correctable.
        SecdedOutcome::Uncorrectable
    }
}

/// SECDED parity-trailer length for a `payload_len`-byte block: one parity byte
/// per 8-byte data word, the last word zero-padded.
///
/// This is what the writer appends after the payload and what the reader
/// re-derives from `data_length` (the trailer length is not stored per block).
#[must_use]
pub fn block_parity_len(payload_len: usize) -> usize {
    payload_len.div_ceil(Hsiao7264::DATA_WORD_BYTES) * Hsiao7264::PARITY_BYTES
}

/// Encodes the SECDED parity trailer for `payload` with [`Hsiao7264`].
///
/// The payload is tiled into 8-byte words (the last zero-padded to a full
/// word); each word's check byte is appended in order. The returned trailer is
/// [`block_parity_len`] bytes.
#[must_use]
pub fn encode_block_parity(payload: &[u8]) -> alloc::vec::Vec<u8> {
    let mut out = alloc::vec::Vec::with_capacity(block_parity_len(payload.len()));
    let mut word = [0u8; Hsiao7264::DATA_WORD_BYTES];
    for chunk in payload.chunks(Hsiao7264::DATA_WORD_BYTES) {
        word.fill(0);
        word[..chunk.len()].copy_from_slice(chunk);
        let mut check = [0u8; Hsiao7264::PARITY_BYTES];
        Hsiao7264::encode_word(&word, &mut check);
        out.extend_from_slice(&check);
    }
    out
}

/// Attempts to heal single-bit errors across `payload` in place using a SECDED
/// `parity` trailer produced by [`encode_block_parity`].
///
/// Returns [`SecdedOutcome::Corrected`] if at least one word reported a
/// correctable syndrome and none was uncorrectable, [`SecdedOutcome::Clean`] if
/// every word verified clean, or [`SecdedOutcome::Uncorrectable`] as soon as any
/// word carries an uncorrectable syndrome (the caller then falls through to the
/// heavier recovery path). A `Corrected` result is not a guarantee the block is
/// now intact: a lone check-bit flip yields `Corrected` without touching the
/// payload, and 3+ bit patterns can alias, so the caller revalidates the
/// enclosing block checksum before trusting the repair. On `Uncorrectable` the
/// payload may hold partial single-bit corrections from earlier words; the
/// caller discards it and retries via RS.
///
/// `parity` shorter than [`block_parity_len`] for `payload.len()` yields
/// `Uncorrectable` (a truncated trailer cannot verify the block).
#[must_use]
pub fn try_correct_block(payload: &mut [u8], parity: &[u8]) -> SecdedOutcome {
    if parity.len() < block_parity_len(payload.len()) {
        return SecdedOutcome::Uncorrectable;
    }

    let mut any_corrected = false;
    let mut word = [0u8; Hsiao7264::DATA_WORD_BYTES];
    for (i, chunk) in payload.chunks_mut(Hsiao7264::DATA_WORD_BYTES).enumerate() {
        let len = chunk.len();
        word.fill(0);
        word[..len].copy_from_slice(chunk);

        let p = &parity[i * Hsiao7264::PARITY_BYTES..(i + 1) * Hsiao7264::PARITY_BYTES];
        match Hsiao7264::decode_word(&mut word, p) {
            SecdedOutcome::Clean => {}
            SecdedOutcome::Corrected => {
                // Write back only the real (non-pad) bytes of the repaired word.
                chunk.copy_from_slice(&word[..len]);
                any_corrected = true;
            }
            SecdedOutcome::Uncorrectable => return SecdedOutcome::Uncorrectable,
        }
    }

    if any_corrected {
        SecdedOutcome::Corrected
    } else {
        SecdedOutcome::Clean
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_cols_are_distinct_odd_weight_at_least_three() {
        for (i, &col) in DATA_COLS.iter().enumerate() {
            assert!(col.count_ones() % 2 == 1, "col {i} must be odd weight");
            assert!(
                col.count_ones() >= 3,
                "col {i} must avoid the weight-1 check columns"
            );
            for (j, &other) in DATA_COLS.iter().enumerate() {
                if i != j {
                    assert_ne!(col, other, "cols {i} and {j} collide");
                }
            }
        }
    }

    #[test]
    fn clean_word_round_trips() {
        let data = 0x0123_4567_89ab_cdefu64.to_le_bytes();
        let mut parity = [0u8; 1];
        Hsiao7264::encode_word(&data, &mut parity);
        let mut recv = data;
        assert_eq!(
            Hsiao7264::decode_word(&mut recv, &parity),
            SecdedOutcome::Clean,
        );
        assert_eq!(recv, data, "clean decode must not alter the word");
    }

    #[test]
    fn corrects_every_single_data_bit_flip() {
        // Exhaustive over all 64 data-bit positions for a few seed words.
        for seed in [0u64, u64::MAX, 0x0123_4567_89ab_cdef, 0xdead_beef_cafe_babe] {
            let data = seed.to_le_bytes();
            let mut parity = [0u8; 1];
            Hsiao7264::encode_word(&data, &mut parity);
            for bit in 0..64 {
                let mut recv = (seed ^ (1u64 << bit)).to_le_bytes();
                let outcome = Hsiao7264::decode_word(&mut recv, &parity);
                assert_eq!(
                    outcome,
                    SecdedOutcome::Corrected,
                    "single flip at bit {bit} (seed {seed:#x}) must be corrected",
                );
                assert_eq!(
                    recv, data,
                    "corrected word must equal the original (bit {bit}, seed {seed:#x})",
                );
            }
        }
    }

    #[test]
    fn corrects_single_check_bit_flip() {
        let data = 0x0123_4567_89ab_cdefu64.to_le_bytes();
        let mut parity = [0u8; 1];
        Hsiao7264::encode_word(&data, &mut parity);
        for bit in 0..8 {
            let mut bad_parity = parity;
            bad_parity[0] ^= 1 << bit;
            let mut recv = data;
            let outcome = Hsiao7264::decode_word(&mut recv, &bad_parity);
            assert_eq!(
                outcome,
                SecdedOutcome::Corrected,
                "check-bit flip {bit} must be reported corrected",
            );
            assert_eq!(recv, data, "data must be intact on a check-bit flip");
        }
    }

    #[test]
    fn detects_every_double_data_bit_flip() {
        // Exhaustive over all C(64,2) data-bit pairs: every double flip must be
        // detected (Uncorrectable), never silently miscorrected.
        let seed = 0x0123_4567_89ab_cdefu64;
        let data = seed.to_le_bytes();
        let mut parity = [0u8; 1];
        Hsiao7264::encode_word(&data, &mut parity);
        for a in 0..64 {
            for b in (a + 1)..64 {
                let corrupted = seed ^ (1u64 << a) ^ (1u64 << b);
                let mut recv = corrupted.to_le_bytes();
                let outcome = Hsiao7264::decode_word(&mut recv, &parity);
                assert_eq!(
                    outcome,
                    SecdedOutcome::Uncorrectable,
                    "double flip at bits {a},{b} must be detected, not miscorrected",
                );
                assert_eq!(
                    recv.as_slice(),
                    corrupted.to_le_bytes().as_slice(),
                    "uncorrectable decode must leave the word unchanged",
                );
            }
        }
    }

    #[test]
    fn overhead_is_one_eighth() {
        assert_eq!(Hsiao7264::PARITY_BYTES * 8, Hsiao7264::DATA_WORD_BYTES);
    }

    #[test]
    fn block_parity_len_is_one_byte_per_eight() {
        assert_eq!(block_parity_len(0), 0);
        assert_eq!(block_parity_len(1), 1);
        assert_eq!(block_parity_len(8), 1);
        assert_eq!(block_parity_len(9), 2);
        assert_eq!(block_parity_len(64), 8);
        assert_eq!(block_parity_len(65), 9);
    }

    #[test]
    fn block_round_trip_is_clean() {
        // Non-multiple-of-8 length exercises the zero-padded final word.
        let payload: alloc::vec::Vec<u8> = (0..100u8).map(|i| i.wrapping_mul(7)).collect();
        let parity = encode_block_parity(&payload);
        assert_eq!(parity.len(), block_parity_len(payload.len()));
        let mut recv = payload.clone();
        assert_eq!(try_correct_block(&mut recv, &parity), SecdedOutcome::Clean);
        assert_eq!(recv, payload, "clean block must be unchanged");
    }

    #[test]
    fn block_corrects_single_bit_flip_at_every_byte() {
        let payload: alloc::vec::Vec<u8> = (0..100u8).map(|i| i.wrapping_mul(7)).collect();
        let parity = encode_block_parity(&payload);
        for byte_idx in 0..payload.len() {
            for bit in 0..8 {
                let mut recv = payload.clone();
                recv[byte_idx] ^= 1 << bit;
                let outcome = try_correct_block(&mut recv, &parity);
                assert_eq!(
                    outcome,
                    SecdedOutcome::Corrected,
                    "single flip at byte {byte_idx} bit {bit} must be corrected",
                );
                assert_eq!(
                    recv, payload,
                    "block must be repaired to the original (byte {byte_idx}, bit {bit})",
                );
            }
        }
    }

    #[test]
    fn block_detects_double_bit_flip_within_one_word() {
        let payload: alloc::vec::Vec<u8> = (0..32u8).collect();
        let parity = encode_block_parity(&payload);
        // Two flips inside the first 8-byte word.
        let mut recv = payload;
        recv[0] ^= 0b0000_0001;
        recv[1] ^= 0b0000_0010;
        assert_eq!(
            try_correct_block(&mut recv, &parity),
            SecdedOutcome::Uncorrectable,
            "two bit errors in one word must be detected",
        );
    }

    #[test]
    fn block_truncated_parity_is_uncorrectable() {
        let payload: alloc::vec::Vec<u8> = (0..64u8).collect();
        let parity = encode_block_parity(&payload);
        let short = &parity[..parity.len() - 1];
        let mut recv = payload;
        assert_eq!(
            try_correct_block(&mut recv, short),
            SecdedOutcome::Uncorrectable,
            "a truncated parity trailer cannot verify the block",
        );
    }
}
