// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Outer Reed-Solomon ECC layer for AAD-bound encrypted blocks.
//!
//! Adds a third skippable frame — the `EccFrame` (variant 2, magic
//! `0x184D2A52`) — after the `MetadataFrame ‖ BodyFrame` pair produced
//! by [`super::block::encrypt_block`]. The frame carries Reed-Solomon
//! parity computed over the `BodyFrame` ciphertext so a reader can
//! recover from single-region media corruption without re-fetching the
//! whole block.
//!
//! This is the *outer* layer — distinct from [`crate::ecc`], which adds
//! per-on-disk-block RS(4, 2) Page ECC over raw block bytes. The outer
//! layer protects the AEAD ciphertext specifically and integrates with
//! the encrypt/decrypt pipeline: on AEAD tag failure the reader runs
//! RS recovery and retries the tag, using the AEAD verify itself as the
//! correctness oracle.
//!
//! ## Wire format (additive to `docs/aad-block-format.md` §5)
//!
//! ```text
//! EncryptedBlockWithEcc = MetadataFrame BodyFrame EccFrame
//!
//! EccFrame    = MagicEcc EccFrameSize EccPayload
//! MagicEcc    = %x52 %x2A %x4D %x18            ; 0x184D2A52 LE
//! EccFrameSize= 4OCTET                          ; u32 LE (skippable-frame len)
//! EccPayload  = EccHeader ParityBytes ChecksumList
//! EccHeader   = SchemeID DataShards ParityShards Reserved   ; 4 bytes
//! SchemeID    = OCTET                           ; 0x01 = Reed-Solomon
//! DataShards  = OCTET                           ; k in RS(n, k)
//! ParityShards= OCTET                           ; (n - k) in RS(n, k)
//! Reserved    = OCTET                           ; MUST be zero in v1
//! ParityBytes = ParityShards * stripe_size OCTET
//! ChecksumList= *OCTET                          ; per-zstd-block XXH64-low32
//! ```
//!
//! The skippable-frame envelope (`MagicEcc` + `EccFrameSize`) is added
//! by [`structured_zstd::skippable::SkippableFrame`]; this module
//! produces / consumes the `EccPayload`. The trailing `ChecksumList` is
//! the localisation channel for per-zstd-block targeted repair; this
//! slice does not populate it (it is emitted empty), so the parity
//! covers the ciphertext as a single stripe set.
//!
//! ## Why AEAD-as-oracle
//!
//! With an empty `ChecksumList` the reader cannot tell *which* RS shard
//! is corrupt. It therefore trial-decodes: for each candidate subset of
//! "shards believed intact" it runs RS reconstruction and re-verifies
//! the AEAD tag over the reconstructed ciphertext. The first subset
//! whose reconstruction makes the tag verify wins. The trial cost is
//! paid only on the corruption path; the happy path (tag verifies on
//! first try) never touches the ECC frame.

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

use super::error::DecryptError;

/// Skippable-frame variant owned by the ECC layer (magic `0x184D2A52`).
pub const ECC_FRAME_VARIANT: u8 = 2;

/// `SchemeID` for the Reed-Solomon scheme. Other values are reserved.
pub const SCHEME_RS: u8 = 0x01;

/// `EccHeader` length in bytes: `SchemeID ‖ DataShards ‖ ParityShards ‖ Reserved`.
pub const ECC_HEADER_LEN: usize = 4;

/// Reed-Solomon shard configuration `RS(data + parity, data)`.
///
/// `data_shards` original stripes plus `parity_shards` recovery stripes;
/// the reader can reconstruct the ciphertext when at most
/// `parity_shards` of the `data_shards + parity_shards` total shards are
/// corrupt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EccScheme {
    /// `k` — number of original (ciphertext) shards.
    pub data_shards: u8,
    /// `n - k` — number of recovery (parity) shards.
    pub parity_shards: u8,
}

impl EccScheme {
    /// The default `RS(14, 10)` scheme: 10 data + 4 parity shards
    /// (~40% parity overhead, recovers up to 4 corrupt shards). Matches
    /// the overhead-measurement scheme named in the design.
    pub const RS_14_10: Self = Self {
        data_shards: 10,
        parity_shards: 4,
    };

    /// Total shard count `n = data + parity`.
    #[must_use]
    pub const fn total_shards(self) -> usize {
        self.data_shards as usize + self.parity_shards as usize
    }
}

/// Per-shard byte length for a ciphertext of `len` bytes split into
/// `data_shards` stripes.
///
/// Rounds up so the stripes cover all ciphertext bytes (last stripe
/// zero-padded), then rounds up to an even number to satisfy
/// `reed-solomon-simd`'s `shard_bytes` alignment requirement.
#[must_use]
fn stripe_size(len: usize, data_shards: usize) -> usize {
    debug_assert!(data_shards > 0);
    let raw = len.div_ceil(data_shards.max(1));
    raw.div_ceil(2) * 2
}

/// Encodes the `EccPayload` (header + parity, empty checksum list) for
/// `ciphertext` under `scheme`.
///
/// The returned bytes are the skippable-frame *payload* — the caller
/// wraps them in a [`structured_zstd::skippable::SkippableFrame`] at
/// [`ECC_FRAME_VARIANT`] and appends the result after the `BodyFrame`.
///
/// # Errors
///
/// - [`crate::Error::Encrypt`] if the scheme is degenerate
///   (`data_shards == 0` or `parity_shards == 0`) or the
///   `reed-solomon-simd` engine rejects the `(data, parity, stripe)`
///   configuration. This is an encode-side helper, so failures use the
///   write-path error channel rather than the decode-side
///   [`DecryptError`].
pub fn encode_ecc_payload(ciphertext: &[u8], scheme: EccScheme) -> crate::Result<Vec<u8>> {
    if scheme.data_shards == 0 || scheme.parity_shards == 0 {
        return Err(crate::Error::Encrypt(
            "degenerate ECC scheme: data_shards and parity_shards must be non-zero",
        ));
    }
    let data = scheme.data_shards as usize;
    let parity = scheme.parity_shards as usize;
    let sb = stripe_size(ciphertext.len(), data);

    let mut out = Vec::with_capacity(ECC_HEADER_LEN + parity * sb);
    out.push(SCHEME_RS);
    out.push(scheme.data_shards);
    out.push(scheme.parity_shards);
    out.push(0); // Reserved, MUST be zero in v1.

    let mut encoder = ReedSolomonEncoder::new(data, parity, sb)
        .map_err(|_| crate::Error::Encrypt("reed-solomon encoder rejected ECC scheme"))?;

    // Feed `data` contiguous stripes of the ciphertext; the last stripe
    // runs past the end and is zero-padded.
    let mut stripe_buf = vec![0u8; sb];
    for i in 0..data {
        stripe_buf.fill(0);
        let start = i * sb;
        let end = ((i + 1) * sb).min(ciphertext.len());
        if start < ciphertext.len() {
            #[expect(
                clippy::indexing_slicing,
                reason = "start < len and end <= len guarded above"
            )]
            stripe_buf[..end - start].copy_from_slice(&ciphertext[start..end]);
        }
        encoder.add_original_shard(&stripe_buf).map_err(|_| {
            crate::Error::Encrypt("reed-solomon encoder rejected ECC original shard")
        })?;
    }

    let result = encoder
        .encode()
        .map_err(|_| crate::Error::Encrypt("reed-solomon ECC encode failed"))?;
    for shard in result.recovery_iter() {
        out.extend_from_slice(shard);
    }
    // ChecksumList intentionally empty in this slice (per-zstd-block
    // targeted repair is a follow-up that sources it from FrameEmitInfo).
    Ok(out)
}

/// Parsed `EccHeader` plus a borrow of the parity bytes.
struct ParsedEcc<'a> {
    scheme: EccScheme,
    parity: &'a [u8],
}

/// Parses an `EccPayload` into its header + parity slice.
fn parse_ecc_payload(payload: &[u8]) -> Result<ParsedEcc<'_>, DecryptError> {
    if payload.len() < ECC_HEADER_LEN {
        return Err(DecryptError::MalformedEccFrame(
            "payload shorter than header",
        ));
    }
    #[expect(
        clippy::indexing_slicing,
        reason = "len >= ECC_HEADER_LEN guarded above"
    )]
    let (scheme_id, data_shards, parity_shards, reserved) =
        (payload[0], payload[1], payload[2], payload[3]);
    if scheme_id != SCHEME_RS {
        return Err(DecryptError::MalformedEccFrame("unknown SchemeID"));
    }
    if reserved != 0 {
        return Err(DecryptError::MalformedEccFrame(
            "Reserved byte must be zero",
        ));
    }
    if data_shards == 0 || parity_shards == 0 {
        return Err(DecryptError::MalformedEccFrame(
            "degenerate scheme in header",
        ));
    }
    // Bound the wire-declared scheme to RS_14_10's shard budget:
    // `parity_shards <= 4` and `data + parity <= 14`. This is a budget
    // cap, not an exact-preset match — any scheme within those bounds
    // (e.g. 13+1) is accepted, which is all the repair path can afford.
    // Without it a crafted header could drive the erasure-subset
    // enumeration in `try_repair` into a DoS hang.
    if parity_shards > EccScheme::RS_14_10.parity_shards
        || (data_shards as usize + parity_shards as usize) > EccScheme::RS_14_10.total_shards()
    {
        return Err(DecryptError::MalformedEccFrame(
            "scheme exceeds shard budget",
        ));
    }
    #[expect(
        clippy::indexing_slicing,
        reason = "len >= ECC_HEADER_LEN guarded above"
    )]
    let parity = &payload[ECC_HEADER_LEN..];
    Ok(ParsedEcc {
        scheme: EccScheme {
            data_shards,
            parity_shards,
        },
        parity,
    })
}

/// Attempts Reed-Solomon recovery of a corrupted `ciphertext` using the
/// parity in `ecc_payload`, validating each candidate reconstruction
/// through `verify` (the AEAD-tag oracle).
///
/// `ciphertext_len` is the original (pre-corruption) ciphertext length —
/// the caller knows it from the `BodyFrame` payload length. The returned
/// vec, on success, is exactly this many bytes.
///
/// `verify` returns `true` when a candidate reconstruction is correct
/// (its AEAD tag verifies). The first candidate that satisfies `verify`
/// wins.
///
/// Returns `Ok(Some(recovered))` on success, `Ok(None)` when no subset
/// within the parity budget reproduces a verifying ciphertext.
///
/// # Errors
///
/// - [`DecryptError::MalformedEccFrame`] if the parity length is
///   inconsistent with the declared scheme + ciphertext length, or the
///   engine rejects the configuration.
pub fn try_repair<F>(
    ciphertext: &[u8],
    ecc_payload: &[u8],
    ciphertext_len: usize,
    mut verify: F,
) -> Result<Option<Vec<u8>>, DecryptError>
where
    F: FnMut(&[u8]) -> bool,
{
    let parsed = parse_ecc_payload(ecc_payload)?;
    let data = parsed.scheme.data_shards as usize;
    let parity = parsed.scheme.parity_shards as usize;
    let sb = stripe_size(ciphertext_len, data);

    if sb == 0 {
        return Ok(None);
    }
    if parsed.parity.len() < parity * sb {
        return Err(DecryptError::MalformedEccFrame(
            "parity length inconsistent with scheme + ciphertext length",
        ));
    }

    // Carve the `data + parity` shards. Data stripes come from the
    // (possibly corrupted) ciphertext in row-major order with the last
    // stripe zero-padded; parity stripes come from the EccFrame.
    let total = data + parity;
    let mut shards: Vec<Vec<u8>> = Vec::with_capacity(total);
    for i in 0..data {
        let mut buf = vec![0u8; sb];
        let start = i * sb;
        let end = ((i + 1) * sb).min(ciphertext.len());
        if start < ciphertext.len() {
            #[expect(
                clippy::indexing_slicing,
                reason = "start < len and end <= len guarded above"
            )]
            buf[..end - start].copy_from_slice(&ciphertext[start..end]);
        }
        shards.push(buf);
    }
    for i in 0..parity {
        let start = i * sb;
        let end = start + sb;
        #[expect(clippy::indexing_slicing, reason = "end <= parity.len() guarded above")]
        shards.push(parsed.parity[start..end].to_vec());
    }

    // Trial-decode: enumerate subsets of declared-missing shards from 1
    // up to `parity` erasures. For each, reconstruct and run the AEAD
    // oracle. Stops at the first verifying reconstruction.
    let mut missing = Vec::with_capacity(parity);
    for erasures in 1..=parity {
        if let Some(recovered) = trial_subsets(
            &shards,
            sb,
            data,
            parity,
            ciphertext_len,
            erasures,
            0,
            &mut missing,
            &mut verify,
        )? {
            return Ok(Some(recovered));
        }
    }
    Ok(None)
}

/// Recursively enumerates every `erasures`-sized subset of the
/// `data + parity` shard indices, decoding each and testing the oracle.
#[expect(
    clippy::too_many_arguments,
    reason = "recursive combination walker; bundling args into a struct would obscure the recursion"
)]
fn trial_subsets<F>(
    shards: &[Vec<u8>],
    sb: usize,
    data: usize,
    parity: usize,
    ciphertext_len: usize,
    erasures: usize,
    start_idx: usize,
    missing: &mut Vec<usize>,
    verify: &mut F,
) -> Result<Option<Vec<u8>>, DecryptError>
where
    F: FnMut(&[u8]) -> bool,
{
    if missing.len() == erasures {
        // let-chain (stable since the 2024 edition): bind `candidate`
        // from the decode, then in the same `if` test it through the
        // AEAD oracle. The arm runs only when decode succeeds AND the
        // candidate verifies; otherwise fall through to `Ok(None)`.
        if let Some(candidate) =
            decode_with_missing(shards, sb, data, parity, ciphertext_len, missing)?
            && verify(&candidate)
        {
            return Ok(Some(candidate));
        }
        return Ok(None);
    }
    let total = data + parity;
    for idx in start_idx..total {
        missing.push(idx);
        if let Some(found) = trial_subsets(
            shards,
            sb,
            data,
            parity,
            ciphertext_len,
            erasures,
            idx + 1,
            missing,
            verify,
        )? {
            return Ok(Some(found));
        }
        missing.pop();
    }
    Ok(None)
}

/// Single RS decode treating `missing` shard indices as erasures, then
/// reassembles the `data` stripes into a ciphertext of `ciphertext_len`.
fn decode_with_missing(
    shards: &[Vec<u8>],
    sb: usize,
    data: usize,
    parity: usize,
    ciphertext_len: usize,
    missing: &[usize],
) -> Result<Option<Vec<u8>>, DecryptError> {
    let mut decoder = ReedSolomonDecoder::new(data, parity, sb)
        .map_err(|_| DecryptError::MalformedEccFrame("reed-solomon decoder rejected scheme"))?;

    for (i, shard) in shards.iter().enumerate().take(data) {
        if missing.contains(&i) {
            continue;
        }
        decoder
            .add_original_shard(i, shard)
            .map_err(|_| DecryptError::MalformedEccFrame("decoder rejected original shard"))?;
    }
    for i in 0..parity {
        let global = data + i;
        if missing.contains(&global) {
            continue;
        }
        #[expect(
            clippy::indexing_slicing,
            reason = "data + i < shards.len() by construction"
        )]
        decoder
            .add_recovery_shard(i, &shards[global])
            .map_err(|_| DecryptError::MalformedEccFrame("decoder rejected recovery shard"))?;
    }

    let Ok(result) = decoder.decode() else {
        return Ok(None);
    };

    let mut out: Vec<u8> = Vec::with_capacity(data * sb);
    for (i, shard) in shards.iter().enumerate().take(data) {
        if missing.contains(&i) {
            match result.restored_original(i) {
                Some(s) => out.extend_from_slice(s),
                None => return Ok(None),
            }
        } else {
            out.extend_from_slice(shard);
        }
    }
    out.truncate(ciphertext_len);
    Ok(Some(out))
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "test code"
)]
mod tests {
    use super::*;

    fn sample_ciphertext(len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| (i.wrapping_mul(31) & 0xff) as u8)
            .collect()
    }

    /// XOR-corrupts the bytes of `buf` belonging to stripe `stripe`
    /// (stripe size `sb`), clamping the upper bound to `buf.len()`.
    /// The final data stripe is zero-padded inside the RS encoder, so
    /// its on-disk span can run past the ciphertext end; clamping
    /// corrupts the stripe's full meaningful content without indexing
    /// out of bounds.
    fn corrupt_stripe(buf: &mut [u8], stripe: usize, sb: usize, xor: u8) {
        let start = (stripe * sb).min(buf.len());
        let end = ((stripe + 1) * sb).min(buf.len());
        for b in &mut buf[start..end] {
            *b ^= xor;
        }
    }

    #[test]
    fn encode_payload_has_header_and_parity() {
        let ct = sample_ciphertext(4096);
        let payload = encode_ecc_payload(&ct, EccScheme::RS_14_10).expect("encode");
        // Header + parity_shards * stripe_size.
        let sb = stripe_size(ct.len(), 10);
        assert_eq!(payload.len(), ECC_HEADER_LEN + 4 * sb);
        assert_eq!(payload[0], SCHEME_RS);
        assert_eq!(payload[1], 10);
        assert_eq!(payload[2], 4);
        assert_eq!(payload[3], 0);
    }

    #[test]
    fn encode_rejects_degenerate_scheme() {
        let ct = sample_ciphertext(64);
        assert!(matches!(
            encode_ecc_payload(
                &ct,
                EccScheme {
                    data_shards: 0,
                    parity_shards: 4
                }
            ),
            Err(crate::Error::Encrypt(_))
        ));
        assert!(matches!(
            encode_ecc_payload(
                &ct,
                EccScheme {
                    data_shards: 10,
                    parity_shards: 0
                }
            ),
            Err(crate::Error::Encrypt(_))
        ));
    }

    // Gated on `zstd_any`: `SkippableFrame` comes from the optional
    // `structured-zstd` dep, which is only in the graph when a zstd
    // feature is on. Without this gate, `cargo test --features
    // encryption,page_ecc` (no zstd) fails to compile.
    #[cfg(zstd_any)]
    #[test]
    fn payload_wraps_in_variant_2_skippable_frame() {
        // Confirms the EccFrame magic lands at 0x184D2A52 (variant 2).
        use structured_zstd::skippable::SkippableFrame;
        let ct = sample_ciphertext(512);
        let payload = encode_ecc_payload(&ct, EccScheme::RS_14_10).expect("encode");
        let frame = SkippableFrame::new(ECC_FRAME_VARIANT, payload).expect("frame");
        let mut bytes = Vec::new();
        frame.encode_into(&mut bytes).expect("encode_into");
        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(magic, 0x184D_2A52);
    }

    #[test]
    fn repair_recovers_single_stripe_corruption() {
        // Oracle: exact match against the original ciphertext (stands in
        // for the AEAD-tag verify in the integration layer).
        let ct = sample_ciphertext(4096);
        let payload = encode_ecc_payload(&ct, EccScheme::RS_14_10).expect("encode");
        let sb = stripe_size(ct.len(), 10);

        let mut corrupted = ct.clone();
        corrupt_stripe(&mut corrupted, 3, sb, 0xFF);

        let recovered = try_repair(&corrupted, &payload, ct.len(), |cand| cand == ct.as_slice())
            .expect("try_repair")
            .expect("single-stripe corruption must be recoverable");
        assert_eq!(recovered, ct);
    }

    #[test]
    fn repair_recovers_up_to_parity_budget() {
        // RS(14,10): 4 parity shards → up to 4 corrupt stripes recover.
        let ct = sample_ciphertext(8192);
        let payload = encode_ecc_payload(&ct, EccScheme::RS_14_10).expect("encode");
        let sb = stripe_size(ct.len(), 10);

        let mut corrupted = ct.clone();
        for stripe in [0usize, 2, 5, 9] {
            corrupt_stripe(&mut corrupted, stripe, sb, 0x5A);
        }

        let recovered = try_repair(&corrupted, &payload, ct.len(), |cand| cand == ct.as_slice())
            .expect("try_repair")
            .expect("4-stripe corruption within RS(14,10) budget must recover");
        assert_eq!(recovered, ct);
    }

    #[test]
    fn repair_returns_none_beyond_budget() {
        // 5 corrupt stripes exceed RS(14,10)'s 4-parity budget.
        let ct = sample_ciphertext(8192);
        let payload = encode_ecc_payload(&ct, EccScheme::RS_14_10).expect("encode");
        let sb = stripe_size(ct.len(), 10);

        let mut corrupted = ct.clone();
        for stripe in [0usize, 2, 4, 6, 8] {
            corrupt_stripe(&mut corrupted, stripe, sb, 0x33);
        }

        let outcome = try_repair(&corrupted, &payload, ct.len(), |cand| cand == ct.as_slice())
            .expect("try_repair");
        assert!(
            outcome.is_none(),
            "5-stripe corruption beyond budget must NOT recover, got {outcome:?}"
        );
    }

    #[test]
    fn repair_happy_path_uncorrupted() {
        // Uncorrupted ciphertext: the oracle accepts the zero-erasure
        // trial is not run (trial starts at 1 erasure), but a 1-erasure
        // reconstruction of an intact block still reproduces the input.
        let ct = sample_ciphertext(2048);
        let payload = encode_ecc_payload(&ct, EccScheme::RS_14_10).expect("encode");
        let recovered = try_repair(&ct, &payload, ct.len(), |cand| cand == ct.as_slice())
            .expect("try_repair")
            .expect("intact ciphertext trivially reconstructs");
        assert_eq!(recovered, ct);
    }

    #[test]
    fn parse_rejects_unknown_scheme_id() {
        let mut payload =
            encode_ecc_payload(&sample_ciphertext(256), EccScheme::RS_14_10).expect("encode");
        payload[0] = 0x99; // bogus SchemeID
        assert!(matches!(
            try_repair(&sample_ciphertext(256), &payload, 256, |_| true),
            Err(DecryptError::MalformedEccFrame(_))
        ));
    }

    #[test]
    fn parse_rejects_nonzero_reserved() {
        let mut payload =
            encode_ecc_payload(&sample_ciphertext(256), EccScheme::RS_14_10).expect("encode");
        payload[3] = 1; // Reserved must be zero
        assert!(matches!(
            try_repair(&sample_ciphertext(256), &payload, 256, |_| true),
            Err(DecryptError::MalformedEccFrame(_))
        ));
    }

    #[test]
    fn unaligned_ciphertext_roundtrips() {
        // 333 bytes — not a multiple of data_shards; padding logic must
        // still recover a corrupted stripe.
        let ct = sample_ciphertext(333);
        let payload = encode_ecc_payload(&ct, EccScheme::RS_14_10).expect("encode");
        let sb = stripe_size(ct.len(), 10);
        let mut corrupted = ct.clone();
        corrupt_stripe(&mut corrupted, 0, sb, 0xFF);
        let recovered = try_repair(&corrupted, &payload, ct.len(), |cand| cand == ct.as_slice())
            .expect("try_repair")
            .expect("unaligned single-stripe corruption must recover");
        assert_eq!(recovered, ct);
    }

    #[test]
    fn parse_rejects_parity_shards_above_supported_budget() {
        // A tampered/corrupt header can declare a huge parity-shard
        // count in a tiny payload: the parity-length check bottoms out
        // at `parity * stripe_size` with `stripe_size` as small as 2,
        // so ~80 parity bytes satisfy `parity_shards = 40`. Left
        // unbounded, `try_repair` would enumerate
        // `Sum_{k=1..=40} C(50, k)` erasure subsets — billions of RS
        // decodes, a guaranteed hang on the corruption path. The parser
        // must reject any scheme beyond the supported shard budget.
        let mut payload = vec![SCHEME_RS, 10, 40, 0];
        payload.extend(core::iter::repeat_n(0u8, 80));
        assert!(
            matches!(
                parse_ecc_payload(&payload),
                Err(DecryptError::MalformedEccFrame(_))
            ),
            "parser must reject parity_shards beyond the supported budget"
        );
    }

    #[test]
    fn parse_rejects_total_shards_above_supported_budget() {
        // Data shards alone can blow the total past the supported
        // budget even with an in-range parity count, inflating the
        // `data + parity` universe the subset walker ranges over.
        let mut payload = vec![SCHEME_RS, 200, 4, 0];
        payload.extend(core::iter::repeat_n(0u8, 8));
        assert!(
            matches!(
                parse_ecc_payload(&payload),
                Err(DecryptError::MalformedEccFrame(_))
            ),
            "parser must reject data+parity beyond the supported total"
        );
    }
}
