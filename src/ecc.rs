// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Per-block Reed-Solomon Page ECC.
//!
//! Encodes a fixed (4, 2) Reed-Solomon scheme over each block's
//! on-disk bytes: 4 data shards + 2 parity shards = 6 shards
//! total. The reader can reconstruct the original block when up
//! to 2 of the 6 shards are corrupt or missing.
//!
//! # Wire layout
//!
//! Block bytes are conceptually partitioned into 4 equal-size
//! data shards (the last one padded with zeros if the block
//! length is not a multiple of 4). The 2 parity shards are
//! computed over these 4 data shards and appended after the
//! block payload. The total bytes on disk for a block of length
//! `N`:
//!
//! - Data: `4 * shard_bytes(N) >= N` bytes (first `N` are the
//!   block payload, the remainder is zero padding that the
//!   reader strips).
//! - Parity: `2 * shard_bytes(N)` bytes appended after.
//!
//! where `shard_bytes(N) = ((N + 3) / 4)` rounded up to the
//! nearest even number. The even-rounding satisfies the
//! `reed-solomon-simd` requirement that `shard_bytes` be a
//! multiple of 2.
//!
//! # Recovery strategy
//!
//! On read, the block's stored XXH3 is recomputed over the
//! payload. If it disagrees, the reader trials every
//! `C(6, 4) = 15` subset of "shards still believed intact" and
//! runs Reed-Solomon decode on each. The first subset whose
//! recovered payload reproduces the stored XXH3 wins. This is
//! more compact than per-shard checksums (which would cost
//! `6 * 8 = 48` bytes per block on every block, parity or not)
//! and the trial cost is paid only on actual corruption, not
//! on the happy path.

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

/// Number of original (data) shards in the Reed-Solomon scheme.
pub const RS_DATA_SHARDS: usize = 4;

/// Number of recovery (parity) shards. Together with
/// [`RS_DATA_SHARDS`] this lets the reader recover the original
/// payload when up to `RS_PARITY_SHARDS` shards are corrupt.
pub const RS_PARITY_SHARDS: usize = 2;

/// Per-shard byte length for a block of `payload_len` bytes.
///
/// Rounds up so that 4 shards cover all payload bytes (last
/// shard zero-padded), then rounds up to an even number to
/// satisfy reed-solomon-simd's `shard_bytes` alignment.
#[must_use]
pub fn shard_bytes(payload_len: usize) -> usize {
    let raw = payload_len.div_ceil(RS_DATA_SHARDS);
    // Round up to multiple of 2 (reed-solomon-simd requirement).
    raw.div_ceil(2) * 2
}

/// Total parity-trailer byte size for a block of `payload_len`
/// bytes. This is what the writer emits after the payload and
/// what `BlockHeader.ecc_length` records.
#[must_use]
pub fn parity_len(payload_len: usize) -> usize {
    shard_bytes(payload_len) * RS_PARITY_SHARDS
}

/// Encodes a Reed-Solomon parity trailer for `payload`.
///
/// Returns a `Vec<u8>` of length [`parity_len`] for `payload.len()`.
/// The caller writes the bytes verbatim after the payload and
/// records `ecc_length = parity.len() as u32` in the block
/// header.
///
/// Empty input (`payload.len() == 0`) is handled by short-circuit
/// returning `Ok(Vec::new())` — a zero-length block has nothing to
/// protect, so emitting zero parity bytes is the correct shape.
/// `ecc_length` then lands at `0` in the header, matching the
/// no-ECC layout.
///
/// # Errors
///
/// Returns [`crate::Error::Unrecoverable`] if the reed-solomon
/// engine rejects the (4, 2, `shard_bytes`) configuration. With
/// `shard_bytes` computed from a non-zero payload length and
/// rounded to the nearest even integer (the only constraint the
/// engine has on the shard size), the engine has no remaining
/// reason to reject; this branch is defensive and is expected to
/// be unreachable from any in-tree caller.
pub fn encode_parity(payload: &[u8]) -> crate::Result<Vec<u8>> {
    let sb = shard_bytes(payload.len());
    if sb == 0 {
        return Ok(Vec::new());
    }
    let mut encoder = ReedSolomonEncoder::new(RS_DATA_SHARDS, RS_PARITY_SHARDS, sb)
        .map_err(|_| crate::Error::Unrecoverable)?;

    // Walk the payload as 4 contiguous shards. The last shard may
    // run past payload.len() — fill the tail with zero padding.
    let mut shard_buf = vec![0u8; sb];
    for i in 0..RS_DATA_SHARDS {
        shard_buf.fill(0);
        let start = i * sb;
        let end = ((i + 1) * sb).min(payload.len());
        if start < payload.len() {
            #[expect(
                clippy::indexing_slicing,
                reason = "start < payload.len() and end <= payload.len() guarded above"
            )]
            shard_buf[..end - start].copy_from_slice(&payload[start..end]);
        }
        encoder
            .add_original_shard(&shard_buf)
            .map_err(|_| crate::Error::Unrecoverable)?;
    }

    let result = encoder.encode().map_err(|_| crate::Error::Unrecoverable)?;
    let mut out = Vec::with_capacity(parity_len(payload.len()));
    for shard in result.recovery_iter() {
        out.extend_from_slice(shard);
    }
    Ok(out)
}

/// Attempts to recover the original payload bytes from the
/// concatenated `data` (possibly corrupted) and `parity` shards.
///
/// `expected_payload_len` is the original payload size (the
/// reader gets this from `BlockHeader.data_length`); the data
/// portion in the returned vec is exactly this many bytes — the
/// padding bytes inside the last data shard are stripped.
///
/// `xxh3_oracle` is invoked on each candidate reconstruction;
/// the first candidate whose XXH3 matches the expected digest
/// wins. The caller provides the digest comparison so this
/// module stays independent of the block-checksum surface.
///
/// Returns `Ok(Some(payload))` on successful recovery,
/// `Ok(None)` if no subset of 4 intact shards yields a payload
/// matching the oracle.
///
/// # Errors
///
/// Returns [`crate::Error::Unrecoverable`] on engine-level
/// failure (reed-solomon-simd rejects the (4, 2, `shard_bytes`)
/// configuration, allocation failure inside the decoder, etc.).
/// "No subset matched" is NOT an error — it surfaces as
/// `Ok(None)` so callers can fall through to the "block is
/// genuinely unrecoverable" path without a `Result` branch.
pub fn try_recover<F>(
    data: &[u8],
    parity: &[u8],
    expected_payload_len: usize,
    mut xxh3_oracle: F,
) -> crate::Result<Option<Vec<u8>>>
where
    F: FnMut(&[u8]) -> bool,
{
    let sb = shard_bytes(expected_payload_len);
    if sb == 0 || data.len() < expected_payload_len || parity.len() < sb * RS_PARITY_SHARDS {
        // Not enough bytes to attempt recovery at all.
        return Ok(None);
    }

    // Carve the 6 shards out of the on-disk bytes. Data shards
    // are taken in row-major order from `data`; the last data
    // shard's tail (past `expected_payload_len`) is treated as
    // zero padding by the encoder, so we re-create that padding
    // here for the symmetric read view.
    let mut shards: [Vec<u8>; RS_DATA_SHARDS + RS_PARITY_SHARDS] = Default::default();
    for (i, shard) in shards.iter_mut().enumerate().take(RS_DATA_SHARDS) {
        let mut buf = vec![0u8; sb];
        let start = i * sb;
        let end = ((i + 1) * sb).min(data.len());
        if start < data.len() {
            #[expect(
                clippy::indexing_slicing,
                reason = "start < data.len() and end <= data.len() guarded above"
            )]
            buf[..end - start].copy_from_slice(&data[start..end]);
        }
        *shard = buf;
    }
    for i in 0..RS_PARITY_SHARDS {
        let start = i * sb;
        let end = start + sb;
        if end > parity.len() {
            return Ok(None);
        }
        #[expect(clippy::indexing_slicing, reason = "end <= parity.len() guarded above")]
        let buf = parity[start..end].to_vec();
        #[expect(
            clippy::indexing_slicing,
            reason = "RS_DATA_SHARDS + i < shards.len() by the const arithmetic"
        )]
        {
            shards[RS_DATA_SHARDS + i] = buf;
        }
    }

    // Enumerate every C(6, 4) = 15 subset of "shards believed
    // intact" and try Reed-Solomon decode against each. The
    // first subset whose reconstruction matches the oracle wins.
    for missing_a in 0..(RS_DATA_SHARDS + RS_PARITY_SHARDS) {
        for missing_b in (missing_a + 1)..(RS_DATA_SHARDS + RS_PARITY_SHARDS) {
            if let Some(payload) =
                try_decode_one(&shards, sb, expected_payload_len, missing_a, missing_b)?
                && xxh3_oracle(&payload)
            {
                return Ok(Some(payload));
            }
        }
    }

    Ok(None)
}

/// Single Reed-Solomon decode attempt with two declared-missing
/// shard indices. Returns the reconstructed payload (trimmed to
/// `expected_payload_len`) or `Ok(None)` if the decode failed.
fn try_decode_one(
    shards: &[Vec<u8>; RS_DATA_SHARDS + RS_PARITY_SHARDS],
    sb: usize,
    expected_payload_len: usize,
    missing_a: usize,
    missing_b: usize,
) -> crate::Result<Option<Vec<u8>>> {
    let mut decoder = ReedSolomonDecoder::new(RS_DATA_SHARDS, RS_PARITY_SHARDS, sb)
        .map_err(|_| crate::Error::Unrecoverable)?;

    // Submit the 4 shards we BELIEVE intact: every shard except
    // missing_a / missing_b.
    for (i, shard) in shards.iter().enumerate().take(RS_DATA_SHARDS) {
        if i == missing_a || i == missing_b {
            continue;
        }
        decoder
            .add_original_shard(i, shard)
            .map_err(|_| crate::Error::Unrecoverable)?;
    }
    for (i, shard) in shards
        .iter()
        .enumerate()
        .skip(RS_DATA_SHARDS)
        .take(RS_PARITY_SHARDS)
    {
        if i == missing_a || i == missing_b {
            continue;
        }
        decoder
            .add_recovery_shard(i - RS_DATA_SHARDS, shard)
            .map_err(|_| crate::Error::Unrecoverable)?;
    }

    let Ok(result) = decoder.decode() else {
        return Ok(None);
    };

    // Reassemble the 4 data shards into a payload, preferring the
    // decoder's restored shard for `missing_a` / `missing_b` when
    // they are data-side indices (recovery-side missing shards
    // don't affect the payload reconstruction).
    let mut payload = Vec::with_capacity(RS_DATA_SHARDS * sb);
    for (i, shard) in shards.iter().enumerate().take(RS_DATA_SHARDS) {
        if i == missing_a || i == missing_b {
            // Decoder restored this shard.
            match result.restored_original(i) {
                Some(s) => payload.extend_from_slice(s),
                None => return Ok(None),
            }
        } else {
            payload.extend_from_slice(shard);
        }
    }

    payload.truncate(expected_payload_len);
    Ok(Some(payload))
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
mod tests {
    use super::*;
    // `test_log::test` shadows the std `test` attribute so every
    // `#[test]` below routes through test-log — drives RUST_LOG
    // capture for the ecc warnings emitted by `try_recover` /
    // `encode_parity`. Looks unused at a glance because the
    // `#[test]` syntax is identical to the std macro; clippy
    // resolves the shadowing correctly and does NOT warn under
    // `-D warnings`.
    use test_log::test;

    fn xxh3_oracle(expected: u128) -> impl FnMut(&[u8]) -> bool {
        move |candidate: &[u8]| crate::hash::hash128(candidate) == expected
    }

    #[test]
    fn shard_bytes_rounds_up_to_even_quarter() {
        // payload = 4 → shard = 1 → rounded to 2
        assert_eq!(shard_bytes(4), 2);
        // payload = 33 → ceil(33/4) = 9 → rounded to 10
        assert_eq!(shard_bytes(33), 10);
        // payload = 4096 → exact, no rounding
        assert_eq!(shard_bytes(4096), 1024);
        // payload = 4097 → ceil(4097/4) = 1025 → rounded to 1026
        assert_eq!(shard_bytes(4097), 1026);
    }

    #[test]
    fn encode_decode_roundtrip_no_corruption() {
        // Even with no corruption, decode trials should find the
        // first subset (drop two parity shards) that reproduces
        // the payload via XXH3 match.
        let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
        let parity = encode_parity(&payload).expect("encode");
        assert_eq!(parity.len(), parity_len(payload.len()));

        let expected = crate::hash::hash128(&payload);
        let recovered = try_recover(&payload, &parity, payload.len(), xxh3_oracle(expected))
            .expect("try_recover")
            .expect("payload should be recoverable");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn recovers_from_single_data_shard_corruption() {
        let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
        let parity = encode_parity(&payload).expect("encode");
        let expected = crate::hash::hash128(&payload);

        let mut corrupted = payload.clone();
        // Flip every byte in shard 1 (mid-block corruption).
        let sb = shard_bytes(payload.len());
        for b in &mut corrupted[sb..2 * sb] {
            *b ^= 0xFF;
        }

        let recovered = try_recover(&corrupted, &parity, payload.len(), xxh3_oracle(expected))
            .expect("try_recover")
            .expect("single data-shard corruption must be recoverable");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn recovers_from_double_data_shard_corruption() {
        let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
        let parity = encode_parity(&payload).expect("encode");
        let expected = crate::hash::hash128(&payload);

        let mut corrupted = payload.clone();
        // Wipe shards 0 AND 2 — two simultaneous failures within
        // the RS(4, 2) bound.
        let sb = shard_bytes(payload.len());
        for b in &mut corrupted[0..sb] {
            *b ^= 0xAA;
        }
        for b in &mut corrupted[2 * sb..3 * sb] {
            *b ^= 0xBB;
        }

        let recovered = try_recover(&corrupted, &parity, payload.len(), xxh3_oracle(expected))
            .expect("try_recover")
            .expect("double data-shard corruption must be recoverable");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn unrecoverable_when_three_shards_corrupt() {
        let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
        let parity = encode_parity(&payload).expect("encode");
        let expected = crate::hash::hash128(&payload);

        let mut corrupted = payload.clone();
        // Wipe 3 data shards — exceeds the RS(4, 2) recovery bound.
        let sb = shard_bytes(payload.len());
        for b in &mut corrupted[0..3 * sb] {
            *b ^= 0xCC;
        }

        let outcome = try_recover(&corrupted, &parity, payload.len(), xxh3_oracle(expected))
            .expect("try_recover");
        assert!(
            outcome.is_none(),
            "three-shard corruption must NOT be recoverable, got: {outcome:?}",
        );
    }

    #[test]
    fn handles_unaligned_block_size() {
        // 33 bytes — index/filter blocks come in this shape; the
        // shard padding logic must produce a recoverable encoding.
        let payload: Vec<u8> = (0..33_u8).collect();
        let parity = encode_parity(&payload).expect("encode");
        let expected = crate::hash::hash128(&payload);

        let mut corrupted = payload.clone();
        // Corrupt the first byte → falls in shard 0.
        corrupted[0] ^= 0xFF;

        let recovered = try_recover(&corrupted, &parity, payload.len(), xxh3_oracle(expected))
            .expect("try_recover")
            .expect("unaligned-block single-shard corruption must be recoverable");
        assert_eq!(recovered, payload);
    }
}
