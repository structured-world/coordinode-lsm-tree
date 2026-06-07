// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Shard-based Page ECC: XOR single-parity (RAID-5) and Reed-Solomon.
//!
//! Parity is computed over a block's on-disk bytes and appended after
//! the payload; the reader reconstructs the block when corruption stays
//! within the scheme's recovery bound. Two shard-based schemes live here:
//!
//! - **XOR single-parity** (`parity_shards == 1`, RAID-5 equivalent):
//!   the block is split into `data_shards` shards and one parity shard is
//!   their XOR. Recovers one fully-lost shard. Overhead = `1 /
//!   data_shards`. Computed directly (no Reed-Solomon engine), so it is
//!   far cheaper than RS for the single-erasure case.
//! - **Reed-Solomon** (`parity_shards >= 2`): `parity_shards` recovery
//!   shards over `data_shards` data shards, via `reed-solomon-simd`.
//!   Recovers up to `parity_shards` lost shards. Overhead =
//!   `parity_shards / data_shards`.
//!
//! Single-bit correction (SECDED / Hamming) is a separate, cheaper,
//! per-word codec and does not live here.
//!
//! # Wire layout
//!
//! Block bytes are conceptually partitioned into `data_shards` equal-size
//! shards (the last padded with zeros if the length is not a multiple of
//! `data_shards`). The parity shards are appended after the payload.
//! `shard_bytes(N, D) = ((N + D - 1) / D)` rounded up to the nearest even
//! number (the `reed-solomon-simd` requirement that `shard_bytes` be a
//! multiple of 2; XOR shares the layout for uniformity). Total parity:
//! `shard_bytes(N, D) * parity_shards` bytes.
//!
//! # Recovery strategy
//!
//! On read, the block's stored XXH3 is recomputed over the payload. If it
//! disagrees, the reader enumerates which shards are corrupt and tries to
//! reconstruct: for XOR, each of the `data_shards + 1` single-shard
//! losses; for RS, every `C(data_shards + parity_shards, parity_shards)`
//! subset of declared-missing shards. The first reconstruction whose XXH3
//! reproduces the stored digest wins. The trial cost is paid only on
//! actual corruption, not on the happy path.

use alloc::vec;
use alloc::vec::Vec;

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

/// Data-shard count of the historical RS(4,2) layout.
///
/// The chosen ECC scheme now flows write → per-SST descriptor → read
/// (the descriptor records `(data_shards, parity_shards)` and block I/O
/// re-derives the layout from it), so RS(4,2) is no longer the layout
/// block I/O selects: it remains only as the fixed scheme for
/// self-describing blocks (read before any descriptor is known) and as a
/// test default. New configurations should prefer a lower-overhead scheme
/// (XOR single-parity, or SECDED for single-bit rot).
pub const RS_DATA_SHARDS: usize = 4;

/// Parity-shard count of the legacy fixed scheme (RS(4,2)).
pub const RS_PARITY_SHARDS: usize = 2;

/// Per-shard byte length for a `payload_len`-byte block split into
/// `data_shards` shards.
///
/// Rounds up so the shards cover all payload bytes (last shard
/// zero-padded), then rounds up to an even number to satisfy
/// `reed-solomon-simd`'s `shard_bytes` alignment (XOR shares the layout).
/// Returns 0 when `data_shards == 0` (caller treats it as "no parity").
#[must_use]
pub fn shard_bytes(payload_len: usize, data_shards: usize) -> usize {
    if data_shards == 0 {
        return 0;
    }
    let raw = payload_len.div_ceil(data_shards);
    // Round up to multiple of 2 (reed-solomon-simd requirement).
    raw.div_ceil(2) * 2
}

/// Total parity-trailer byte size for a `payload_len`-byte block under a
/// `(data_shards, parity_shards)` scheme.
///
/// This is what the writer emits after the payload, and what the reader
/// re-derives from `data_length` + the per-SST scheme descriptor (the
/// trailer length is not stored per block).
#[must_use]
pub fn parity_len(payload_len: usize, data_shards: usize, parity_shards: usize) -> usize {
    shard_bytes(payload_len, data_shards) * parity_shards
}

/// Encodes a parity trailer for `payload` under a `(data_shards,
/// parity_shards)` scheme.
///
/// `parity_shards == 1` uses direct XOR (RAID-5); `parity_shards >= 2`
/// uses Reed-Solomon. Returns a `Vec<u8>` of length [`parity_len`].
/// Empty input or a degenerate scheme (`shard_bytes == 0` or
/// `parity_shards == 0`) returns `Ok(Vec::new())`.
///
/// # Errors
///
/// Returns [`crate::Error::Unrecoverable`] if the Reed-Solomon engine
/// rejects the `(data_shards, parity_shards, shard_bytes)` configuration.
/// XOR encoding cannot fail.
pub fn encode_parity(
    payload: &[u8],
    data_shards: usize,
    parity_shards: usize,
) -> crate::Result<Vec<u8>> {
    let sb = shard_bytes(payload.len(), data_shards);
    if sb == 0 || parity_shards == 0 {
        return Ok(Vec::new());
    }
    if parity_shards == 1 {
        return Ok(encode_xor(payload, data_shards, sb));
    }
    encode_rs(payload, data_shards, parity_shards, sb)
}

/// Copies data shard `i` (length `sb`, zero-padded past `payload.len()`)
/// into `buf`.
fn fill_data_shard(buf: &mut [u8], payload: &[u8], i: usize, sb: usize) {
    buf.fill(0);
    let start = i * sb;
    let end = ((i + 1) * sb).min(payload.len());
    if start < payload.len() {
        #[expect(
            clippy::indexing_slicing,
            reason = "start < payload.len() and end <= payload.len() guarded above"
        )]
        buf[..end - start].copy_from_slice(&payload[start..end]);
    }
}

/// XOR single-parity (RAID-5): parity shard = XOR of all data shards.
/// The byte-wise XOR loop autovectorizes; no explicit SIMD kernel needed.
fn encode_xor(payload: &[u8], data_shards: usize, sb: usize) -> Vec<u8> {
    let mut parity = vec![0u8; sb];
    let mut shard = vec![0u8; sb];
    for i in 0..data_shards {
        fill_data_shard(&mut shard, payload, i, sb);
        for (p, &b) in parity.iter_mut().zip(shard.iter()) {
            *p ^= b;
        }
    }
    parity
}

/// Reed-Solomon parity (`parity_shards >= 2`) over `data_shards`.
fn encode_rs(
    payload: &[u8],
    data_shards: usize,
    parity_shards: usize,
    sb: usize,
) -> crate::Result<Vec<u8>> {
    let mut encoder = ReedSolomonEncoder::new(data_shards, parity_shards, sb)
        .map_err(|_| crate::Error::Unrecoverable)?;
    let mut shard_buf = vec![0u8; sb];
    for i in 0..data_shards {
        fill_data_shard(&mut shard_buf, payload, i, sb);
        encoder
            .add_original_shard(&shard_buf)
            .map_err(|_| crate::Error::Unrecoverable)?;
    }
    let result = encoder.encode().map_err(|_| crate::Error::Unrecoverable)?;
    let mut out = Vec::with_capacity(sb * parity_shards);
    for shard in result.recovery_iter() {
        out.extend_from_slice(shard);
    }
    Ok(out)
}

/// Attempts to recover the original payload from `data` (possibly
/// corrupted) and `parity` shards under a `(data_shards, parity_shards)`
/// scheme.
///
/// `expected_payload_len` is the original payload size (from
/// `BlockHeader.data_length`); the returned vec is trimmed to it.
/// `xxh3_oracle` is invoked on each candidate; the first whose XXH3
/// matches wins. Returns `Ok(Some(payload))` on success, `Ok(None)` if no
/// reconstruction matched.
///
/// # Errors
///
/// Returns [`crate::Error::Unrecoverable`] on Reed-Solomon engine-level
/// failure. "No subset matched" is `Ok(None)`, not an error.
pub fn try_recover<F>(
    data: &[u8],
    parity: &[u8],
    expected_payload_len: usize,
    data_shards: usize,
    parity_shards: usize,
    mut xxh3_oracle: F,
) -> crate::Result<Option<Vec<u8>>>
where
    F: FnMut(&[u8]) -> bool,
{
    let sb = shard_bytes(expected_payload_len, data_shards);
    if sb == 0
        || parity_shards == 0
        || data.len() < expected_payload_len
        || parity.len() < sb * parity_shards
    {
        return Ok(None);
    }

    if parity_shards == 1 {
        return Ok(xor_recover(
            data,
            parity,
            expected_payload_len,
            data_shards,
            sb,
            &mut xxh3_oracle,
        ));
    }
    rs_recover(
        data,
        parity,
        expected_payload_len,
        data_shards,
        parity_shards,
        sb,
        &mut xxh3_oracle,
    )
}

/// Carves the `data_shards` data shards out of `data` (last zero-padded).
fn carve_data_shards(data: &[u8], data_shards: usize, sb: usize) -> Vec<Vec<u8>> {
    let mut shards = Vec::with_capacity(data_shards);
    for i in 0..data_shards {
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
        shards.push(buf);
    }
    shards
}

/// XOR single-parity recovery: try the data-intact candidate (parity
/// itself corrupt) and each single-data-shard reconstruction.
fn xor_recover<F>(
    data: &[u8],
    parity: &[u8],
    expected_payload_len: usize,
    data_shards: usize,
    sb: usize,
    xxh3_oracle: &mut F,
) -> Option<Vec<u8>>
where
    F: FnMut(&[u8]) -> bool,
{
    let shards = carve_data_shards(data, data_shards, sb);

    // Candidate 0: data shards intact, parity shard was the corrupt one.
    let mut payload = Vec::with_capacity(data_shards * sb);
    for s in &shards {
        payload.extend_from_slice(s);
    }
    payload.truncate(expected_payload_len);
    if xxh3_oracle(&payload) {
        return Some(payload);
    }

    // Candidates 1..=data_shards: shard `miss` corrupt → reconstruct it
    // as parity XOR (all other data shards).
    #[expect(
        clippy::indexing_slicing,
        reason = "parity.len() >= sb guarded by caller"
    )]
    let parity_shard = &parity[..sb];
    for miss in 0..data_shards {
        let mut recovered = parity_shard.to_vec();
        for (i, s) in shards.iter().enumerate() {
            if i == miss {
                continue;
            }
            for (r, &b) in recovered.iter_mut().zip(s.iter()) {
                *r ^= b;
            }
        }
        let mut payload = Vec::with_capacity(data_shards * sb);
        for (i, s) in shards.iter().enumerate() {
            if i == miss {
                payload.extend_from_slice(&recovered);
            } else {
                payload.extend_from_slice(s);
            }
        }
        payload.truncate(expected_payload_len);
        if xxh3_oracle(&payload) {
            return Some(payload);
        }
    }
    None
}

/// Reed-Solomon recovery: enumerate every `C(n, parity_shards)` subset of
/// declared-missing shards and try a decode against each.
fn rs_recover<F>(
    data: &[u8],
    parity: &[u8],
    expected_payload_len: usize,
    data_shards: usize,
    parity_shards: usize,
    sb: usize,
    xxh3_oracle: &mut F,
) -> crate::Result<Option<Vec<u8>>>
where
    F: FnMut(&[u8]) -> bool,
{
    let n = data_shards + parity_shards;
    let mut shards = carve_data_shards(data, data_shards, sb);
    for i in 0..parity_shards {
        let start = i * sb;
        let end = start + sb;
        if end > parity.len() {
            return Ok(None);
        }
        #[expect(clippy::indexing_slicing, reason = "end <= parity.len() guarded above")]
        shards.push(parity[start..end].to_vec());
    }

    // Enumerate every size-`parity_shards` subset of {0..n} as the
    // declared-missing set; the first decode whose payload matches wins.
    let mut missing = (0..parity_shards).collect::<Vec<usize>>();
    loop {
        if let Some(payload) = try_decode_one(
            &shards,
            sb,
            expected_payload_len,
            data_shards,
            parity_shards,
            &missing,
        )? && xxh3_oracle(&payload)
        {
            return Ok(Some(payload));
        }
        if !next_combination(&mut missing, n) {
            break;
        }
    }
    Ok(None)
}

/// Advances `combo` (strictly increasing indices into `0..n`) to the next
/// combination in lexicographic order. Returns `false` when exhausted.
fn next_combination(combo: &mut [usize], n: usize) -> bool {
    let k = combo.len();
    if k == 0 {
        return false;
    }
    let mut i = k - 1;
    loop {
        // Max value position `i` may take so the tail still fits.
        let max_at_i = n - k + i;
        #[expect(clippy::indexing_slicing, reason = "i < k == combo.len()")]
        if combo[i] < max_at_i {
            combo[i] += 1;
            for j in (i + 1)..k {
                #[expect(clippy::indexing_slicing, reason = "i < j < k == combo.len()")]
                {
                    combo[j] = combo[j - 1] + 1;
                }
            }
            return true;
        }
        if i == 0 {
            return false;
        }
        i -= 1;
    }
}

/// Single Reed-Solomon decode attempt with `missing` declared-missing
/// shard indices. Returns the reconstructed payload or `Ok(None)`.
fn try_decode_one(
    shards: &[Vec<u8>],
    sb: usize,
    expected_payload_len: usize,
    data_shards: usize,
    parity_shards: usize,
    missing: &[usize],
) -> crate::Result<Option<Vec<u8>>> {
    let mut decoder = ReedSolomonDecoder::new(data_shards, parity_shards, sb)
        .map_err(|_| crate::Error::Unrecoverable)?;
    for (i, shard) in shards.iter().enumerate().take(data_shards) {
        if missing.contains(&i) {
            continue;
        }
        decoder
            .add_original_shard(i, shard)
            .map_err(|_| crate::Error::Unrecoverable)?;
    }
    for (i, shard) in shards
        .iter()
        .enumerate()
        .skip(data_shards)
        .take(parity_shards)
    {
        if missing.contains(&i) {
            continue;
        }
        decoder
            .add_recovery_shard(i - data_shards, shard)
            .map_err(|_| crate::Error::Unrecoverable)?;
    }

    let Ok(result) = decoder.decode() else {
        return Ok(None);
    };

    let mut payload = Vec::with_capacity(data_shards * sb);
    for (i, shard) in shards.iter().enumerate().take(data_shards) {
        if missing.contains(&i) {
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
    // `#[test]` below routes through test-log for RUST_LOG capture.
    use test_log::test;

    fn xxh3_oracle(expected: u128) -> impl FnMut(&[u8]) -> bool {
        move |candidate: &[u8]| crate::hash::hash128(candidate) == expected
    }

    #[test]
    fn shard_bytes_rounds_up_to_even_quarter() {
        // RS(4,2)-style splits: payload 4 → shard 1 → rounded to 2.
        assert_eq!(shard_bytes(4, 4), 2);
        // payload = 33 → ceil(33/4) = 9 → rounded to 10
        assert_eq!(shard_bytes(33, 4), 10);
        // payload = 4096 → exact, no rounding
        assert_eq!(shard_bytes(4096, 4), 1024);
        // payload = 4097 → ceil(4097/4) = 1025 → rounded to 1026
        assert_eq!(shard_bytes(4097, 4), 1026);
        // 10-shard split (10% XOR overhead): 4096 → ceil(409.6)=410
        assert_eq!(shard_bytes(4096, 10), 410);
        assert_eq!(shard_bytes(0, 4), 0);
    }

    #[test]
    fn rs_4_2_layout_is_byte_identical_to_legacy() {
        // The legacy fixed scheme is RS(4,2); parity_len must match the
        // old `shard_bytes(N) * 2` formula so existing SSTs round-trip.
        for n in [1usize, 4, 33, 4096, 4097] {
            assert_eq!(
                parity_len(n, RS_DATA_SHARDS, RS_PARITY_SHARDS),
                shard_bytes(n, 4) * 2,
            );
        }
    }

    #[test]
    fn rs_encode_decode_roundtrip_no_corruption() {
        let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
        let parity = encode_parity(&payload, 4, 2).expect("encode");
        assert_eq!(parity.len(), parity_len(payload.len(), 4, 2));
        let expected = crate::hash::hash128(&payload);
        let recovered = try_recover(
            &payload,
            &parity,
            payload.len(),
            4,
            2,
            xxh3_oracle(expected),
        )
        .expect("try_recover")
        .expect("recoverable");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn rs_recovers_from_double_data_shard_corruption() {
        let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
        let parity = encode_parity(&payload, 4, 2).expect("encode");
        let expected = crate::hash::hash128(&payload);
        let mut corrupted = payload.clone();
        let sb = shard_bytes(payload.len(), 4);
        for b in &mut corrupted[0..sb] {
            *b ^= 0xAA;
        }
        for b in &mut corrupted[2 * sb..3 * sb] {
            *b ^= 0xBB;
        }
        let recovered = try_recover(
            &corrupted,
            &parity,
            payload.len(),
            4,
            2,
            xxh3_oracle(expected),
        )
        .expect("try_recover")
        .expect("double-shard corruption recoverable under RS(4,2)");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn rs_unrecoverable_when_three_shards_corrupt() {
        let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
        let parity = encode_parity(&payload, 4, 2).expect("encode");
        let expected = crate::hash::hash128(&payload);
        let mut corrupted = payload.clone();
        let sb = shard_bytes(payload.len(), 4);
        for b in &mut corrupted[0..3 * sb] {
            *b ^= 0xCC;
        }
        let outcome = try_recover(
            &corrupted,
            &parity,
            payload.len(),
            4,
            2,
            xxh3_oracle(expected),
        )
        .expect("try_recover");
        assert!(
            outcome.is_none(),
            "three-shard corruption must be unrecoverable"
        );
    }

    #[test]
    fn xor_single_parity_overhead_is_one_over_data_shards() {
        // 10 data shards → parity is exactly one shard ≈ 10% of payload.
        let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
        let parity = encode_parity(&payload, 10, 1).expect("encode");
        assert_eq!(parity.len(), parity_len(payload.len(), 10, 1));
        assert_eq!(parity.len(), shard_bytes(4096, 10)); // one shard
    }

    #[test]
    fn xor_recovers_single_data_shard_loss() {
        let payload: Vec<u8> = (0..4096_u32)
            .map(|i| (i.wrapping_mul(7) & 0xff) as u8)
            .collect();
        let parity = encode_parity(&payload, 8, 1).expect("encode");
        let expected = crate::hash::hash128(&payload);
        let sb = shard_bytes(payload.len(), 8);
        let mut corrupted = payload.clone();
        // Wipe shard 3.
        for b in &mut corrupted[3 * sb..4 * sb] {
            *b ^= 0xFF;
        }
        let recovered = try_recover(
            &corrupted,
            &parity,
            payload.len(),
            8,
            1,
            xxh3_oracle(expected),
        )
        .expect("try_recover")
        .expect("single-shard loss recoverable under XOR");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn xor_recovers_when_parity_itself_is_corrupt() {
        // Data intact, parity shard corrupt → candidate-0 path returns
        // the untouched data.
        let payload: Vec<u8> = (0..2048_u32).map(|i| (i & 0xff) as u8).collect();
        let mut parity = encode_parity(&payload, 8, 1).expect("encode");
        let expected = crate::hash::hash128(&payload);
        parity[0] ^= 0xFF;
        let recovered = try_recover(
            &payload,
            &parity,
            payload.len(),
            8,
            1,
            xxh3_oracle(expected),
        )
        .expect("try_recover")
        .expect("data intact, parity corrupt is recoverable");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn xor_unrecoverable_when_two_data_shards_lost() {
        // XOR single-parity tolerates exactly one shard loss.
        let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
        let parity = encode_parity(&payload, 8, 1).expect("encode");
        let expected = crate::hash::hash128(&payload);
        let sb = shard_bytes(payload.len(), 8);
        let mut corrupted = payload.clone();
        for b in &mut corrupted[0..sb] {
            *b ^= 0xAA;
        }
        for b in &mut corrupted[2 * sb..3 * sb] {
            *b ^= 0xBB;
        }
        let outcome = try_recover(
            &corrupted,
            &parity,
            payload.len(),
            8,
            1,
            xxh3_oracle(expected),
        )
        .expect("try_recover");
        assert!(
            outcome.is_none(),
            "two-shard loss must be unrecoverable under XOR"
        );
    }

    #[test]
    fn rs_8_2_recovers_double_loss_low_overhead() {
        // RS(8,2): 25% overhead, two-shard tolerance — the higher tier.
        let payload: Vec<u8> = (0..8192_u32).map(|i| (i & 0xff) as u8).collect();
        let parity = encode_parity(&payload, 8, 2).expect("encode");
        assert_eq!(parity.len(), shard_bytes(8192, 8) * 2);
        let expected = crate::hash::hash128(&payload);
        let sb = shard_bytes(payload.len(), 8);
        let mut corrupted = payload.clone();
        for b in &mut corrupted[5 * sb..6 * sb] {
            *b ^= 0xAA;
        }
        for b in &mut corrupted[7 * sb..8 * sb] {
            *b ^= 0xBB;
        }
        let recovered = try_recover(
            &corrupted,
            &parity,
            payload.len(),
            8,
            2,
            xxh3_oracle(expected),
        )
        .expect("try_recover")
        .expect("double-shard loss recoverable under RS(8,2)");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn handles_unaligned_block_size() {
        let payload: Vec<u8> = (0..33_u8).collect();
        let parity = encode_parity(&payload, 4, 2).expect("encode");
        let expected = crate::hash::hash128(&payload);
        let mut corrupted = payload.clone();
        corrupted[0] ^= 0xFF;
        let recovered = try_recover(
            &corrupted,
            &parity,
            payload.len(),
            4,
            2,
            xxh3_oracle(expected),
        )
        .expect("try_recover")
        .expect("unaligned single-shard corruption recoverable");
        assert_eq!(recovered, payload);
    }

    #[test]
    fn next_combination_enumerates_all_pairs() {
        // C(4,2) = 6 pairs in lexicographic order.
        let mut combo = vec![0usize, 1];
        let mut seen = vec![combo.clone()];
        while next_combination(&mut combo, 4) {
            seen.push(combo.clone());
        }
        assert_eq!(
            seen,
            vec![
                vec![0, 1],
                vec![0, 2],
                vec![0, 3],
                vec![1, 2],
                vec![1, 3],
                vec![2, 3],
            ],
        );
    }
}
