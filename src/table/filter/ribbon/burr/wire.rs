//! BuRR on-disk wire format.
//!
//! # Layout
//!
//! Designed for the LSM filter block — fixed-width fields up front, then
//! per-layer variable-length payloads. All multi-byte integers are
//! little-endian.
//!
//! ```text
//! offset  size  field
//! ──────  ────  ──────────────────────────────────────────
//! 0       6     MAGIC_BYTES (existing crate constant)
//! 6       1     filter_type = BURR_FILTER_TYPE_BYTE (2)
//! 7       1     format_version (FORMAT_VERSION = 1)
//! 8       1     r (fingerprint bits, 1..=64)
//! 9       1     w (band width, fixed at 64)
//! 10      1     b (block size)
//! 11      1     num_layers (1..=255)
//! 12      8     root_seed (u64 LE)
//! 20      —     per-layer payloads (`num_layers` entries):
//!                   4     m (u32 LE)             — slot count
//!                   4     num_blocks (u32 LE)    — = m.div_ceil(b)
//!                   4     z_byte_len (u32 LE)    — = m * stride_words * 8
//!                   N     thresholds (num_blocks bytes)
//!                   M     z storage (z_byte_len bytes, raw u64 words LE)
//! ```
//!
//! `stride_words = r.div_ceil(64)`. For the current implementation
//! `r <= 64` so `stride_words = 1` and a row of z is exactly 8 bytes.
//!
//! The per-layer seed is NOT stored — it's re-derived from
//! `root_seed + layer_index` via [`super::builder::derive_layer_seed`]
//! at parse time. Keeps the format compact and removes the temptation
//! to drift seeds across encode/decode.

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};
use core::hash::BuildHasher;

use crate::io::Cursor;
use crate::io::{LittleEndian, ReadBytesExt, WriteBytesExt};
// `Read` in scope for the raw `cursor.read_exact` below: the std trait under
// `std` (where `crate::io::Read` is a method-less alias), the native one under
// `no_std`.
#[cfg(not(feature = "std"))]
use crate::io::Read;
#[cfg(feature = "std")]
use std::io::Read;

use super::super::hashing::{StandardEquation, standard_equation_from_hash};
use super::super::params::{Mode, Params};
use super::builder::derive_layer_seed;
use super::filter::BurrFilter;
use super::threshold::is_bumped;
use crate::file::MAGIC_BYTES;

/// Wire-format identifier for the BuRR filter. Distinct from the legacy
/// bloom values (0 = StandardBloom, 1 = BlockedBloom — both retired
/// alongside this rollout in task #17/#18); 2 is the new BuRR slot.
pub(crate) const BURR_FILTER_TYPE_BYTE: u8 = 2;

/// Format version. Bumped if/when the wire layout changes
/// incompatibly. Readers reject mismatched versions explicitly.
pub(crate) const FORMAT_VERSION: u8 = 1;

/// Header length in bytes (MAGIC + filter_type + version + r + w + b +
/// num_layers + root_seed) — 6 + 1 + 1 + 1 + 1 + 1 + 1 + 8 = 20.
const HEADER_LEN: usize = MAGIC_BYTES.len() + 6 + 8;
/// Per-layer fixed header length: m + num_blocks + z_byte_len = 12.
const LAYER_HEADER_LEN: usize = 12;

/// Serialize a built [`BurrFilter`] into the wire format.
pub(crate) fn encode<S>(filter: &BurrFilter<S>) -> Vec<u8>
where
    S: BuildHasher + Clone,
{
    let params = filter.params();
    let layers = filter.layers_inner();

    // Pre-size the buffer to avoid reallocations: header + per-layer
    // (fixed header + thresholds + z) for every layer.
    let stride_words = usize::from(params.r).div_ceil(64);
    let estimated_size: usize = HEADER_LEN
        + layers
            .iter()
            .map(|layer| LAYER_HEADER_LEN + layer.thresholds.len() + layer.m * stride_words * 8)
            .sum::<usize>();
    let mut buf = Vec::with_capacity(estimated_size);

    // Header.
    buf.extend_from_slice(&MAGIC_BYTES);
    #[expect(clippy::expect_used, reason = "writing to a Vec<u8> cannot fail")]
    {
        buf.write_u8(BURR_FILTER_TYPE_BYTE).expect("vec write");
        buf.write_u8(FORMAT_VERSION).expect("vec write");
        buf.write_u8(params.r).expect("vec write");
        buf.write_u8(params.w).expect("vec write");
        buf.write_u8(params.b).expect("vec write");
        #[expect(
            clippy::cast_possible_truncation,
            reason = "max_layers fits u8 by construction"
        )]
        let num_layers_u8 = layers.len() as u8;
        buf.write_u8(num_layers_u8).expect("vec write");
        buf.write_u64::<LittleEndian>(params.seed)
            .expect("vec write");
    }

    // Per-layer payloads.
    for layer in layers {
        let m = layer.m;
        let num_blocks = layer.thresholds.len();
        // Checked multiplication: a layer larger than u32::MAX bytes
        // would silently wrap with `as u32` and produce a self-
        // corrupting wire format. Filter partitions are capped at ~4KB
        // upstream so this is unreachable in practice; the asserts
        // make that explicit and turn any future regression into a
        // loud panic at write time rather than corruption at read.
        #[expect(
            clippy::expect_used,
            reason = "programmer invariant: filter partitions are capped at \
                      ~4 KB upstream; an overflow here means a regression \
                      slipped past the partition-size policy"
        )]
        let z_byte_len: usize = m
            .checked_mul(stride_words)
            .and_then(|v| v.checked_mul(8))
            .expect("BuRR layer z payload size overflows usize");
        #[expect(
            clippy::expect_used,
            reason = "programmer invariant: m bounded by partition size; \
                      fits u32 by construction"
        )]
        let m_u32 = u32::try_from(m).expect("BuRR layer m exceeds u32::MAX");
        #[expect(
            clippy::expect_used,
            reason = "programmer invariant: num_blocks = m.div_ceil(b) ≤ m, \
                      fits u32 by construction"
        )]
        let num_blocks_u32 =
            u32::try_from(num_blocks).expect("BuRR layer num_blocks exceeds u32::MAX");
        #[expect(
            clippy::expect_used,
            reason = "programmer invariant: z_byte_len = m * stride * 8 ≤ \
                      partition size in bytes; fits u32 by construction"
        )]
        let z_byte_len_u32 =
            u32::try_from(z_byte_len).expect("BuRR layer z_byte_len exceeds u32::MAX");
        #[expect(clippy::expect_used, reason = "writing to a Vec<u8> cannot fail")]
        {
            buf.write_u32::<LittleEndian>(m_u32).expect("vec write");
            buf.write_u32::<LittleEndian>(num_blocks_u32)
                .expect("vec write");
            buf.write_u32::<LittleEndian>(z_byte_len_u32)
                .expect("vec write");
        }
        buf.extend_from_slice(&layer.thresholds);
        // Serialize z as little-endian u64 words.
        let z_words = layer.ribbon.z_raw_words();
        debug_assert_eq!(z_words.len(), m * stride_words);
        for word in z_words {
            buf.extend_from_slice(&word.to_le_bytes());
        }
    }

    buf
}

/// Borrowed-slice view of one decoded layer.
///
/// `z_bytes` stays as a borrowed slice of the wire buffer — the LSM
/// filter block is constructed afresh per `maybe_contains_hash` call
/// (the underlying `Block` is cached, but `FilterBlock` wraps it
/// freshly), so any per-layer `Vec` allocation here would happen on
/// every point read and dominate the probe path. The trade-off is one
/// 8-byte LE decode per matched row inside the probe loop; for `r <=
/// 64` (stride = 1) that's a single `u64::from_le_bytes` per set bit.
#[derive(Debug)]
pub(crate) struct LayerView<'a> {
    pub(crate) m: usize,
    pub(crate) seed: u64,
    pub(crate) thresholds: &'a [u8],
    pub(crate) z_bytes: &'a [u8],
}

/// Decoded BuRR filter, holding borrowed slices into a wire-format
/// buffer. Layer payloads are zero-copy; only the small header and the
/// per-layer descriptors are eagerly parsed.
#[derive(Debug)]
pub(crate) struct DecodedFilter<'a> {
    pub(crate) r: u8,
    pub(crate) w: u8,
    pub(crate) b: u8,
    pub(crate) stride_words: usize,
    pub(crate) layers: Vec<LayerView<'a>>,
}

/// Parse a wire-format BuRR filter slice. Returns an error if the magic
/// bytes don't match, the version is unrecognised, or the buffer is
/// truncated.
#[expect(
    clippy::indexing_slicing,
    reason = "every slice in this function is preceded by an explicit length \
              check that returns InvalidHeader on truncation: \
              bytes[pos..pos+4]/[pos+4..pos+8]/[pos+8..pos+12] are gated by the \
              `bytes.len() < header_end` (LAYER_HEADER_LEN = 12) check on \
              the line above; bytes[pos..thresholds_end] and \
              bytes[thresholds_end..z_end] are gated by checked_add + \
              `bytes.len() < z_end`. Replacing with .get(..).ok_or(...) \
              would multiply the function's error-return paths without \
              improving safety."
)]
pub(crate) fn decode(bytes: &[u8]) -> crate::Result<DecodedFilter<'_>> {
    if bytes.len() < HEADER_LEN {
        return Err(crate::Error::InvalidHeader("BurrFilter"));
    }

    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; MAGIC_BYTES.len()];
    cursor.read_exact(&mut magic)?;
    if magic != MAGIC_BYTES {
        return Err(crate::Error::InvalidHeader("BurrFilter"));
    }

    let filter_type = cursor.read_u8()?;
    if filter_type != BURR_FILTER_TYPE_BYTE {
        return Err(crate::Error::InvalidTag(("FilterType", filter_type)));
    }
    let version = cursor.read_u8()?;
    if version != FORMAT_VERSION {
        return Err(crate::Error::InvalidHeader("BurrFilter version"));
    }

    let r = cursor.read_u8()?;
    let w = cursor.read_u8()?;
    let b = cursor.read_u8()?;
    let num_layers = cursor.read_u8()?;
    let root_seed = cursor.read_u64::<LittleEndian>()?;

    // Header-field invariants. Without these checks a corrupted block
    // can flow into Params::new (which would fail and silently skip the
    // layer in contains_hash → false negative on read), or trigger
    // divide-by-zero in is_bumped when b == 0. Fail closed at decode.
    if !(1..=64).contains(&r) || w != 64 || b == 0 || num_layers == 0 {
        return Err(crate::Error::InvalidHeader("BurrFilter params"));
    }

    let stride_words = usize::from(r).div_ceil(64);
    let mut layers = Vec::with_capacity(usize::from(num_layers));
    let mut pos = HEADER_LEN;

    for layer_idx in 0..num_layers {
        // On 32-bit targets `pos + LAYER_HEADER_LEN` can wrap if pos was
        // advanced past a corrupted layer; compute the endpoint with
        // checked_add so the bounds guard cannot succeed by wraparound.
        let header_end = pos
            .checked_add(LAYER_HEADER_LEN)
            .ok_or(crate::Error::InvalidHeader("BurrFilter layer header"))?;
        if bytes.len() < header_end {
            return Err(crate::Error::InvalidHeader("BurrFilter layer header"));
        }
        #[expect(
            clippy::expect_used,
            reason = "programmer invariant: layer header slice is exactly \
                      LAYER_HEADER_LEN (12) bytes from the bounds check \
                      above; the three 4-byte windows always convert."
        )]
        let (m_bytes, num_blocks_bytes, z_byte_len_bytes): ([u8; 4], [u8; 4], [u8; 4]) = (
            bytes[pos..pos + 4].try_into().expect("4 bytes"),
            bytes[pos + 4..pos + 8].try_into().expect("4 bytes"),
            bytes[pos + 8..pos + 12].try_into().expect("4 bytes"),
        );
        let m = u32::from_le_bytes(m_bytes) as usize;
        let num_blocks = u32::from_le_bytes(num_blocks_bytes) as usize;
        let z_byte_len = u32::from_le_bytes(z_byte_len_bytes) as usize;
        pos = header_end;

        // Cross-check num_blocks and z_byte_len against r/b/m before
        // trusting the layer payload. Mismatches mean read_row would
        // index out of bounds; we'd rather error now than panic later.
        if m == 0 {
            return Err(crate::Error::InvalidHeader("BurrFilter layer m"));
        }
        let expected_blocks = m.div_ceil(usize::from(b));
        let expected_z_len = m
            .checked_mul(stride_words)
            .and_then(|n| n.checked_mul(8))
            .ok_or(crate::Error::InvalidHeader("BurrFilter layer payload"))?;
        if num_blocks != expected_blocks || z_byte_len != expected_z_len {
            return Err(crate::Error::InvalidHeader("BurrFilter layer payload"));
        }

        // Validate the per-layer params via Params::new — catches
        // m < w and other Ribbon-side rejections at decode time so
        // the probe path never has to fail-close on the same input.
        Params::new(m, usize::from(w), usize::from(r), Mode::Standard)
            .map_err(|_| crate::Error::InvalidHeader("BurrFilter layer params"))?;

        // Checked endpoint arithmetic — on 32-bit targets a corrupted
        // num_blocks/z_byte_len could overflow `pos + num_blocks + z_byte_len`
        // and let the original `bytes.len() < pos + …` guard succeed by
        // wraparound, then panic on the slice indexing below. Compute the
        // endpoints with `checked_add` and bail to InvalidHeader on any
        // overflow.
        let thresholds_end = pos
            .checked_add(num_blocks)
            .ok_or(crate::Error::InvalidHeader("BurrFilter layer payload"))?;
        let z_end = thresholds_end
            .checked_add(z_byte_len)
            .ok_or(crate::Error::InvalidHeader("BurrFilter layer payload"))?;
        if bytes.len() < z_end {
            return Err(crate::Error::InvalidHeader("BurrFilter layer payload"));
        }
        let thresholds = &bytes[pos..thresholds_end];
        let z_bytes = &bytes[thresholds_end..z_end];
        pos = z_end;

        // Per-layer seed re-derived from root_seed + layer_idx to match
        // what the builder used. The wire format does NOT store layer
        // seeds because they're a pure function of (root_seed,
        // layer_idx) — keeping it that way prevents drift.
        let seed = derive_layer_seed(root_seed, layer_idx);

        layers.push(LayerView {
            m,
            seed,
            thresholds,
            z_bytes,
        });
    }

    Ok(DecodedFilter {
        r,
        w,
        b,
        stride_words,
        layers,
    })
}

/// Single-pass parse + probe over raw wire bytes.
///
/// Equivalent to `decode(bytes).map(|d| contains_hash(&d, hash))` but
/// without allocating the intermediate `DecodedFilter` (and its
/// `Vec<LayerView>`). Used on the LSM table read hot path
/// (`FilterBlock::maybe_contains_hash`) where the wire buffer is
/// already in the block cache — re-parsing the header and walking
/// per-layer payloads in place avoids the per-probe heap allocation.
///
/// Returns:
/// - `Ok(true)`  — hash may be present (or wire is corrupted in a way
///   we cannot validate → fail-closed: caller falls through to a real
///   index lookup rather than reporting a false negative);
/// - `Ok(false)` — hash is definitely not in the inserted set;
/// - `Err(InvalidHeader)` — wire prefix is unparseable (bad magic,
///   wrong filter_type/version, truncated). Differs from the
///   fail-closed `true` path: a structurally invalid header is a real
///   error returned upstream so the table read path can surface it.
#[inline]
#[expect(
    clippy::many_single_char_names,
    reason = "r/w/b/m are well-known params from the BuRR/Ribbon literature; single-letter naming matches the rest of the module."
)]
#[expect(
    clippy::indexing_slicing,
    reason = "every slice/index in this function is preceded by an explicit \
              length check: bytes[..MAGIC_BYTES.len()] and the per-byte \
              MAGIC_BYTES.len() + N reads are gated by `bytes.len() < HEADER_LEN` \
              on the line above (HEADER_LEN >= MAGIC_BYTES.len() + 6 + 8); \
              the bytes[seed_off..seed_off+8] window is bounded by HEADER_LEN. \
              Replacing with .get(..).ok_or(...) would multiply the function's \
              error-return paths without improving safety — and this function \
              is on the read-path hot loop, so the explicit pre-check + raw \
              indexing avoids per-field Option unwrapping."
)]
pub(crate) fn contains_hash_from_bytes(bytes: &[u8], hash: u64) -> crate::Result<bool> {
    if bytes.len() < HEADER_LEN {
        return Err(crate::Error::InvalidHeader("BurrFilter"));
    }

    if bytes[..MAGIC_BYTES.len()] != MAGIC_BYTES {
        return Err(crate::Error::InvalidHeader("BurrFilter"));
    }
    let filter_type = bytes[MAGIC_BYTES.len()];
    if filter_type != BURR_FILTER_TYPE_BYTE {
        return Err(crate::Error::InvalidTag(("FilterType", filter_type)));
    }
    let version = bytes[MAGIC_BYTES.len() + 1];
    if version != FORMAT_VERSION {
        return Err(crate::Error::InvalidHeader("BurrFilter version"));
    }

    let r = bytes[MAGIC_BYTES.len() + 2];
    let w = bytes[MAGIC_BYTES.len() + 3];
    let b = bytes[MAGIC_BYTES.len() + 4];
    let num_layers = bytes[MAGIC_BYTES.len() + 5];
    if !(1..=64).contains(&r) || w != 64 || b == 0 || num_layers == 0 {
        return Err(crate::Error::InvalidHeader("BurrFilter params"));
    }
    let seed_off = MAGIC_BYTES.len() + 6;
    let root_seed = u64::from_le_bytes(
        bytes[seed_off..seed_off + 8]
            .try_into()
            .map_err(|_| crate::Error::InvalidHeader("BurrFilter"))?,
    );

    // r <= 64 → stride_words == 1. We mirror the in-memory probe
    // invariants without storing stride at all; if r > 64 ever lands
    // the validation above already rejected it.
    let mut fingerprint_buf = [0_u64; 1];
    let mut pos = HEADER_LEN;

    for layer_idx in 0..num_layers {
        // Same checked-add guard as `decode`; on 32-bit a corrupted pos
        // could let unchecked `pos + LAYER_HEADER_LEN` wrap past
        // `bytes.len()` and panic at the slice indexing below.
        let header_end = pos
            .checked_add(LAYER_HEADER_LEN)
            .ok_or(crate::Error::InvalidHeader("BurrFilter layer header"))?;
        if bytes.len() < header_end {
            return Err(crate::Error::InvalidHeader("BurrFilter layer header"));
        }
        let m_bytes: [u8; 4] = bytes[pos..pos + 4]
            .try_into()
            .map_err(|_| crate::Error::InvalidHeader("BurrFilter"))?;
        let num_blocks_bytes: [u8; 4] = bytes[pos + 4..pos + 8]
            .try_into()
            .map_err(|_| crate::Error::InvalidHeader("BurrFilter"))?;
        let z_byte_len_bytes: [u8; 4] = bytes[pos + 8..pos + 12]
            .try_into()
            .map_err(|_| crate::Error::InvalidHeader("BurrFilter"))?;
        let m = u32::from_le_bytes(m_bytes) as usize;
        let num_blocks = u32::from_le_bytes(num_blocks_bytes) as usize;
        let z_byte_len = u32::from_le_bytes(z_byte_len_bytes) as usize;
        pos = header_end;

        if m == 0 {
            return Err(crate::Error::InvalidHeader("BurrFilter layer m"));
        }
        let expected_blocks = m.div_ceil(usize::from(b));
        let expected_z_len = m
            .checked_mul(8)
            .ok_or(crate::Error::InvalidHeader("BurrFilter layer payload"))?;
        if num_blocks != expected_blocks || z_byte_len != expected_z_len {
            return Err(crate::Error::InvalidHeader("BurrFilter layer payload"));
        }
        // Validate per-layer Ribbon params (m vs w etc.) at parse time
        // instead of fail-closing inside the probe loop.
        let layer_params_base = Params::new(m, usize::from(w), usize::from(r), Mode::Standard)
            .map_err(|_| crate::Error::InvalidHeader("BurrFilter layer params"))?;
        // Checked endpoints — see the same pattern in `decode`. Avoids
        // wraparound on 32-bit when `pos + num_blocks + z_byte_len`
        // overflows usize.
        let thresholds_end = pos
            .checked_add(num_blocks)
            .ok_or(crate::Error::InvalidHeader("BurrFilter layer payload"))?;
        let z_end = thresholds_end
            .checked_add(z_byte_len)
            .ok_or(crate::Error::InvalidHeader("BurrFilter layer payload"))?;
        if bytes.len() < z_end {
            return Err(crate::Error::InvalidHeader("BurrFilter layer payload"));
        }
        let thresholds = &bytes[pos..thresholds_end];
        let z = &bytes[thresholds_end..z_end];
        pos = z_end;

        let seed = derive_layer_seed(root_seed, layer_idx);
        let layer_params = layer_params_base.with_seed(seed);

        fingerprint_buf[0] = 0;
        let equation: StandardEquation =
            standard_equation_from_hash(hash, seed, &layer_params, &mut fingerprint_buf);
        let fingerprint = fingerprint_buf[0];

        if is_bumped(&equation, thresholds, b) {
            continue;
        }

        // GF(2) XOR-reduce against the band rows whose coeff bit is set.
        let mut acc: u64 = 0;
        let mut lo = equation.coeff_lo;
        while lo != 0 {
            let offset = lo.trailing_zeros() as usize;
            let row_byte = (equation.start + offset) * 8;
            let Some(slice) = z.get(row_byte..row_byte + 8) else {
                // row_byte+8 > z len: payload truncated mid-row.
                // Fail closed.
                return Ok(true);
            };
            let Ok(arr) = <[u8; 8]>::try_from(slice) else {
                return Ok(true);
            };
            acc ^= u64::from_le_bytes(arr);
            lo &= lo - 1;
        }
        debug_assert_eq!(equation.coeff_hi, 0, "w <= 64 keeps coeff_hi == 0");
        return Ok(acc == fingerprint);
    }

    Ok(false)
}

/// Probe a decoded BuRR filter with a pre-computed hash. Returns
/// `true` if the hash may correspond to an inserted key, `false` if
/// definitely-not-inserted.
///
/// This is the hot path for the LSM filter framework: the table read
/// path already computes the key's u64 hash for hash-table indexing
/// elsewhere; the filter consumes that same hash directly instead of
/// re-hashing via a `BuildHasher`.
#[inline]
pub(crate) fn contains_hash(decoded: &DecodedFilter<'_>, hash: u64) -> bool {
    // r is validated to 1..=64 in decode, so stride_words is always 1
    // for the currently-deployed wire format. We use a single stack u64
    // for both fingerprint and acc to keep this hot path allocation-
    // free. If the format ever grows to r > 64 the assertion below
    // catches the mismatch — the probe path must be updated at the
    // same time.
    debug_assert_eq!(decoded.stride_words, 1, "BuRR wire format pins r <= 64");
    let mut fingerprint_buf = [0_u64; 1];

    for layer in &decoded.layers {
        let layer_params = match Params::new(
            layer.m,
            usize::from(decoded.w),
            usize::from(decoded.r),
            Mode::Standard,
        ) {
            Ok(p) => p.with_seed(layer.seed),
            // Should be unreachable because decode validates r/w/b/m.
            // Fail closed — return true to make the table read path
            // fall through to a real index lookup rather than report a
            // false negative.
            Err(_) => return true,
        };

        fingerprint_buf[0] = 0;
        let equation: StandardEquation =
            standard_equation_from_hash(hash, layer.seed, &layer_params, &mut fingerprint_buf);
        let fingerprint = fingerprint_buf[0];

        if is_bumped(&equation, layer.thresholds, decoded.b) {
            continue;
        }

        // Kept at this layer — XOR-reduce the band-rows whose coeff bit
        // is set, compare against the fingerprint. start ∈ [0, m-w] and
        // every set bit offset ∈ [0, w-1], so row_index ∈ [0, m-1] is
        // always in-bounds (proven; no per-row bounds check in the
        // loop). z_bytes is borrowed wire bytes; we decode 8 LE bytes
        // → u64 per matched row inline (no per-call allocation, vs
        // pre-decoding into Vec<u64> which would happen on every
        // FilterBlock construction during the LSM read path).
        let z = layer.z_bytes;
        let mut acc: u64 = 0;
        let mut lo = equation.coeff_lo;
        while lo != 0 {
            let offset = lo.trailing_zeros() as usize;
            let row_byte = (equation.start + offset) * 8;
            // row_byte..row_byte+8 ⊂ z is proven by start+offset < m
            // and the decode-time check that z len == m * 8. If the
            // invariant ever drifts (corruption, future format change
            // missed here), fail closed → return true so the table
            // read path falls through to a real index lookup rather
            // than producing a false negative on substituted zeros.
            let Some(slice) = z.get(row_byte..row_byte + 8) else {
                return true;
            };
            let Ok(arr) = <[u8; 8]>::try_from(slice) else {
                return true;
            };
            acc ^= u64::from_le_bytes(arr);
            lo &= lo - 1;
        }
        // coeff_hi is always 0 for w <= 64 (the case we deploy); a
        // future w > 64 build path would need to extend the loop here.
        debug_assert_eq!(equation.coeff_hi, 0, "w <= 64 keeps coeff_hi == 0");

        return acc == fingerprint;
    }
    false
}
