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

use std::hash::BuildHasher;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read};

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
    #[allow(clippy::expect_used, reason = "writing to a Vec<u8> cannot fail")]
    {
        buf.write_u8(BURR_FILTER_TYPE_BYTE).expect("vec write");
        buf.write_u8(FORMAT_VERSION).expect("vec write");
        buf.write_u8(params.r).expect("vec write");
        buf.write_u8(params.w).expect("vec write");
        buf.write_u8(params.b).expect("vec write");
        #[allow(
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
        let z_byte_len: usize = m
            .checked_mul(stride_words)
            .and_then(|v| v.checked_mul(8))
            .expect("BuRR layer z payload size overflows usize");
        let m_u32 = u32::try_from(m).expect("BuRR layer m exceeds u32::MAX");
        let num_blocks_u32 =
            u32::try_from(num_blocks).expect("BuRR layer num_blocks exceeds u32::MAX");
        let z_byte_len_u32 =
            u32::try_from(z_byte_len).expect("BuRR layer z_byte_len exceeds u32::MAX");
        #[allow(clippy::expect_used, reason = "writing to a Vec<u8> cannot fail")]
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
/// `z_words` is the band-solution matrix pre-decoded from the on-disk
/// little-endian bytes into native `u64`s. This happens once per layer
/// at [`decode`] time so the per-probe hot path is a single u64 slice
/// index instead of a `read 8 LE bytes → from_le_bytes` per row touched.
///
/// The owned `Vec<u64>` is the only heap allocation in a `BurrFilterReader`;
/// for a long-lived reader (the LSM table filter block) the cost is
/// amortised over many probes.
pub(crate) struct LayerView<'a> {
    pub(crate) m: usize,
    pub(crate) seed: u64,
    pub(crate) thresholds: &'a [u8],
    pub(crate) z_words: Vec<u64>,
}

/// Decoded BuRR filter, holding borrowed slices into a wire-format
/// buffer. Layer payloads are zero-copy; only the small header and the
/// per-layer descriptors are eagerly parsed.
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
        if bytes.len() < pos + LAYER_HEADER_LEN {
            return Err(crate::Error::InvalidHeader("BurrFilter layer header"));
        }
        let m_bytes: [u8; 4] = bytes[pos..pos + 4].try_into().expect("4 bytes");
        let num_blocks_bytes: [u8; 4] = bytes[pos + 4..pos + 8].try_into().expect("4 bytes");
        let z_byte_len_bytes: [u8; 4] = bytes[pos + 8..pos + 12].try_into().expect("4 bytes");
        let m = u32::from_le_bytes(m_bytes) as usize;
        let num_blocks = u32::from_le_bytes(num_blocks_bytes) as usize;
        let z_byte_len = u32::from_le_bytes(z_byte_len_bytes) as usize;
        pos += LAYER_HEADER_LEN;

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

        if bytes.len() < pos + num_blocks + z_byte_len {
            return Err(crate::Error::InvalidHeader("BurrFilter layer payload"));
        }
        let thresholds = &bytes[pos..pos + num_blocks];
        pos += num_blocks;
        let z_bytes = &bytes[pos..pos + z_byte_len];
        pos += z_byte_len;

        // Pre-decode the band-solution words once at decode time. The
        // hot probe path then becomes a single u64 slice index per
        // matched row instead of an LE-byte decode per probe.
        let z_word_count = m * stride_words;
        let mut z_words = Vec::with_capacity(z_word_count);
        for chunk in z_bytes.chunks_exact(8) {
            // `chunks_exact(8)` over a slice whose length we already
            // validated as `m * stride_words * 8` yields exactly
            // `z_word_count` chunks of 8 bytes each.
            #[allow(clippy::expect_used, reason = "len pre-validated by header check")]
            let arr: [u8; 8] = chunk.try_into().expect("chunks_exact yields 8-byte slices");
            z_words.push(u64::from_le_bytes(arr));
        }
        debug_assert_eq!(z_words.len(), z_word_count);

        // Per-layer seed re-derived from root_seed + layer_idx to match
        // what the builder used. The wire format does NOT store layer
        // seeds because they're a pure function of (root_seed,
        // layer_idx) — keeping it that way prevents drift.
        let seed = derive_layer_seed(root_seed, layer_idx);

        layers.push(LayerView {
            m,
            seed,
            thresholds,
            z_words,
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
        // loop). z_words is pre-decoded; the loop body is one slice
        // index + one XOR per matched row.
        let z = layer.z_words.as_slice();
        let mut acc: u64 = 0;
        let mut lo = equation.coeff_lo;
        while lo != 0 {
            let offset = lo.trailing_zeros() as usize;
            acc ^= z[equation.start + offset];
            lo &= lo - 1;
        }
        // coeff_hi is always 0 for w <= 64 (the case we deploy); a
        // future w > 64 build path would need to extend the loop here.
        debug_assert_eq!(equation.coeff_hi, 0, "w <= 64 keeps coeff_hi == 0");

        return acc == fingerprint;
    }
    false
}
