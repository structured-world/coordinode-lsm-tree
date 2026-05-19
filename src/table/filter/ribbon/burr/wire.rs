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

use super::super::hashing::{
    StandardEquation, for_each_set_bit_u128_parts, standard_equation_from_hash, xor_words,
};
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
        let z_byte_len = m * stride_words * 8;
        #[allow(clippy::expect_used, reason = "writing to a Vec<u8> cannot fail")]
        #[allow(
            clippy::cast_possible_truncation,
            reason = "m, num_blocks, z_byte_len bounded by max filter size — fit u32"
        )]
        {
            buf.write_u32::<LittleEndian>(m as u32).expect("vec write");
            buf.write_u32::<LittleEndian>(num_blocks as u32)
                .expect("vec write");
            buf.write_u32::<LittleEndian>(z_byte_len as u32)
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
pub(crate) struct LayerView<'a> {
    pub(crate) m: usize,
    pub(crate) seed: u64,
    pub(crate) thresholds: &'a [u8],
    pub(crate) z_bytes: &'a [u8],
}

impl<'a> LayerView<'a> {
    /// Read one row (`stride_words` u64s) at the given row index. Each
    /// word is reconstructed from 8 bytes of `z_bytes` in little-endian
    /// order — portable across host endianness.
    pub(crate) fn read_row(&self, row_index: usize, stride_words: usize, out: &mut [u64]) {
        debug_assert_eq!(out.len(), stride_words);
        let start_byte = row_index * stride_words * 8;
        for (i, word) in out.iter_mut().enumerate() {
            let offset = start_byte + i * 8;
            #[allow(clippy::expect_used, reason = "row_index pre-validated against m")]
            let chunk: [u8; 8] = self
                .z_bytes
                .get(offset..offset + 8)
                .and_then(|s| s.try_into().ok())
                .expect("z_bytes layer slice must cover m * stride_words * 8 bytes");
            *word = u64::from_le_bytes(chunk);
        }
    }
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

        if bytes.len() < pos + num_blocks + z_byte_len {
            return Err(crate::Error::InvalidHeader("BurrFilter layer payload"));
        }
        let thresholds = &bytes[pos..pos + num_blocks];
        pos += num_blocks;
        let z_bytes = &bytes[pos..pos + z_byte_len];
        pos += z_byte_len;

        // Per-layer seed re-derived from root_seed + layer_idx to match
        // what the builder used. The wire format does NOT store layer
        // seeds because they're a pure function of (root_seed,
        // layer_idx) — keeping it that way prevents drift.
        let seed = derive_layer_seed(root_seed, layer_idx);

        debug_assert_eq!(
            z_byte_len,
            m * stride_words * 8,
            "z payload length must equal m * stride_words * 8"
        );

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

/// Probe a decoded BuRR filter with a pre-computed hash. Returns
/// `true` if the hash may correspond to an inserted key, `false` if
/// definitely-not-inserted.
///
/// This is the hot path for the LSM filter framework: the table read
/// path already computes the key's u64 hash for hash-table indexing
/// elsewhere; the filter consumes that same hash directly instead of
/// re-hashing via a `BuildHasher`.
pub(crate) fn contains_hash(decoded: &DecodedFilter<'_>, hash: u64) -> bool {
    let stride_words = decoded.stride_words;
    let mut fingerprint = vec![0_u64; stride_words];
    let mut acc = vec![0_u64; stride_words];
    let mut row_buf = vec![0_u64; stride_words];

    for layer in &decoded.layers {
        let layer_params = match Params::new(
            layer.m,
            usize::from(decoded.w),
            usize::from(decoded.r),
            Mode::Standard,
        ) {
            Ok(p) => p.with_seed(layer.seed),
            Err(_) => continue,
        };

        fingerprint.fill(0);
        let equation: StandardEquation =
            standard_equation_from_hash(hash, layer.seed, &layer_params, &mut fingerprint);

        if is_bumped(&equation, layer.thresholds, decoded.b) {
            continue;
        }

        // Kept at this layer — run the GF(2) XOR-reduce against the
        // stored solution and compare against the fingerprint.
        acc.fill(0);
        for_each_set_bit_u128_parts(equation.coeff_lo, equation.coeff_hi, |offset| {
            let row_index = equation.start + offset;
            if row_index < layer.m {
                layer.read_row(row_index, stride_words, &mut row_buf);
                xor_words(&mut acc, &row_buf);
            }
        });

        return acc == fingerprint;
    }
    false
}
