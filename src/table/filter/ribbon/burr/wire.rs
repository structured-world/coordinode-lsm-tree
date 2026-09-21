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
//! 7       1     format_version (FORMAT_VERSION = 2)
//! 8       1     r (fingerprint bits, 1..=64)
//! 9       1     w (band width, fixed at 64)
//! 10      1     b (block size)
//! 11      1     num_layers (1..=255)
//! 12      8     root_seed (u64 LE)
//! 20      —     per-layer payloads (`num_layers` entries):
//!                   4     m (u32 LE)             — slot count
//!                   4     num_blocks (u32 LE)    — = m.div_ceil(b)
//!                   4     z_byte_len (u32 LE)    — = segments * r * 8
//!                   N     thresholds (num_blocks bytes)
//!                   M     z storage (z_byte_len bytes, u64 words LE)
//! ```
//!
//! # Solution storage
//!
//! `z` is bit-sliced, not row-major: rows are grouped into segments of 64
//! and each segment stores `r` words, word `j` holding bit `j` of its 64
//! rows, so `segments = m.div_ceil(64)` and the payload is `r` bits per row
//! rather than 64 whatever `r` is. See [`super::packed`] for the layout and
//! the band read. A membership probe reduces one result bit at a time and
//! stops at the first bit that disagrees with the fingerprint; a retrieval
//! probe reads all `r`.
//!
//! The per-layer seed is NOT stored — it's re-derived from
//! `root_seed + layer_index` via [`super::builder::derive_layer_seed`]
//! at parse time. Keeps the format compact and removes the temptation
//! to drift seeds across encode/decode.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

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

/// Wire-format identifier for the membership BuRR filter. Distinct from
/// the legacy bloom values (0 = StandardBloom, 1 = BlockedBloom, both
/// retired); 2 is the BuRR membership slot.
pub(crate) const BURR_FILTER_TYPE_BYTE: u8 = 2;

/// Wire-format identifier for a retrieval BuRR (key → locator). Structurally
/// identical to the membership payload — same header, same per-layer z words
/// — but the stored RHS is a caller locator, so it answers
/// [`recover_value_from_bytes`] rather than `contains_hash_from_bytes`. A new
/// tag within the same `FORMAT_VERSION`, NOT a version bump: membership
/// readers reject it as a wrong-type tag, retrieval readers reject tag 2.
pub(crate) const BURR_RETRIEVAL_TYPE_BYTE: u8 = 3;

/// Format version. Bumped if/when the wire layout changes
/// incompatibly. Readers reject mismatched versions explicitly.
///
/// Version 2 bit-slices the solution matrix (see the module docs). There is
/// no reader for version 1: a table carrying one is refused when it is
/// opened, and converting it is the offline tool's job, so no legacy decode
/// path lives on the read hot path.
pub(crate) const FORMAT_VERSION: u8 = 2;

/// Header length in bytes (MAGIC + filter_type + version + r + w + b +
/// num_layers + root_seed) — 6 + 1 + 1 + 1 + 1 + 1 + 1 + 8 = 20.
pub(crate) const HEADER_LEN: usize = MAGIC_BYTES.len() + 6 + 8;
/// Per-layer fixed header length: m + num_blocks + z_byte_len = 12.
pub(crate) const LAYER_HEADER_LEN: usize = 12;

/// The exact number of bytes [`encode`] will produce for `filter`, without
/// serialising it.
///
/// This is the figure memory accounting and the filter-size budget use. It is
/// distinct from [`BloomConstructionPolicy::estimated_filter_size`], which
/// predicts a size from a key count *before* the build and cannot be exact:
/// how many keys each layer bumps depends on the hashes.
///
/// [`BloomConstructionPolicy::estimated_filter_size`]: crate::table::filter::BloomConstructionPolicy::estimated_filter_size
pub(crate) fn encoded_len(filter: &BurrFilter) -> usize {
    let layers = filter.layers_inner();
    if layers.is_empty() {
        // `to_wire_bytes` emits nothing for a zero-layer filter.
        return 0;
    }
    let r = filter.params().r;
    HEADER_LEN
        + layers
            .iter()
            .map(|layer| {
                LAYER_HEADER_LEN
                    + layer.thresholds.len()
                    + super::packed::z_byte_len(layer.m, r).unwrap_or(0)
            })
            .sum::<usize>()
}

/// Serialize a built [`BurrFilter`] into the wire format.
pub(crate) fn encode(filter: &BurrFilter) -> Vec<u8> {
    let params = filter.params();
    let layers = filter.layers_inner();

    // Pre-size the buffer to avoid reallocations: header + per-layer
    // (fixed header + thresholds + z) for every layer.
    let estimated_size: usize = HEADER_LEN
        + layers
            .iter()
            .map(|layer| {
                LAYER_HEADER_LEN
                    + layer.thresholds.len()
                    + super::packed::z_byte_len(layer.m, params.r).unwrap_or(0)
            })
            .sum::<usize>();
    let mut buf = Vec::with_capacity(estimated_size);

    // Header. The filter_type tag distinguishes a membership payload
    // (probe → bool) from a retrieval payload (recover → locator); both
    // share the rest of the layout. A membership filter writes tag 2
    // verbatim, so existing on-disk output is byte-identical.
    let filter_type_byte = match filter.kind() {
        super::filter::BurrFilterKind::Membership => BURR_FILTER_TYPE_BYTE,
        super::filter::BurrFilterKind::Retrieval => BURR_RETRIEVAL_TYPE_BYTE,
    };
    buf.extend_from_slice(&MAGIC_BYTES);
    #[expect(clippy::expect_used, reason = "writing to a Vec<u8> cannot fail")]
    {
        buf.write_u8(filter_type_byte).expect("vec write");
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
            reason = "programmer invariant: a layer's packed payload is \
                      `segments * r * 8` bytes, bounded by the partition-size \
                      policy; an overflow here means a regression slipped past it"
        )]
        let z_byte_len: usize = super::packed::z_byte_len(m, params.r)
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
        // The solver works row-major — back-substitution reads rows below the
        // one it is finishing — so the transpose happens here, once per layer,
        // on the way out. The builder's row-major buffer is transient; what is
        // stored, cached and probed is the bit-sliced form.
        let z_words = layer.ribbon.z_raw_words();
        debug_assert_eq!(z_words.len(), m, "one solver row per slot");
        let columns = super::packed::transpose(z_words, params.r);
        debug_assert_eq!(columns.len() * 8, z_byte_len);
        for word in columns {
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
        let expected_z_len = super::packed::z_byte_len(m, r)
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

    Ok(DecodedFilter { r, w, b, layers })
}

/// Outcome of walking a wire-format BuRR to the layer that holds `hash`
/// (the first layer whose per-block threshold does not bump it).
///
/// The header + per-layer bounds parsing is byte-for-byte identical for the
/// membership probe and the retrieval recover, and it is security-sensitive
/// (every slice is gated by a checked length / `checked_add` endpoint).
/// Sharing it in [`walk_first_layer`] keeps that parsing in ONE place so the
/// two query paths cannot drift on the bounds checks; each caller only
/// interprets the terminal `acc`.
enum FirstLayerWalk {
    /// `hash` is kept at some layer and every result bit was reduced. For a
    /// retrieval walk the value is the recovered `r`-bit RHS; for a
    /// membership walk it is that same value, reached only because each bit
    /// agreed with the fingerprint as it was computed.
    Found(u64),
    /// Membership only: a result bit disagreed with the fingerprint, so the
    /// key is definitely absent and the remaining bits were never read.
    Absent,
    /// Every layer bumped `hash`. For a membership filter that is
    /// definitely-absent; normally unreachable since the last layer accepts
    /// all keys.
    AllBumped,
    /// A layer's z payload was truncated past the header-validated lengths
    /// (structure parsed, but a column word is missing). The caller
    /// fail-closes (membership → possibly-present; retrieval → fall back to
    /// the sorted index).
    Truncated,
}

/// Parse a wire-format BuRR header (rejecting a wrong `expected_type`) and
/// walk per-layer payloads in place to the first non-bumped layer, returning
/// its dot-product `acc` and recomputed fingerprint. No allocation — used on
/// the LSM table read hot path where the wire buffer is already in the block
/// cache, so re-parsing in place avoids the per-probe heap allocation an
/// intermediate `DecodedFilter` would cost.
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
              the bytes[seed_off..seed_off+8] window is bounded by HEADER_LEN; \
              per-layer windows are gated by checked_add endpoints + \
              `bytes.len() < ...`. Raw indexing avoids per-field Option \
              unwrapping on the read hot loop."
)]
fn walk_first_layer<const EARLY_OUT: bool>(
    bytes: &[u8],
    hash: u64,
    expected_type: u8,
) -> crate::Result<FirstLayerWalk> {
    if bytes.len() < HEADER_LEN {
        return Err(crate::Error::InvalidHeader("BurrFilter"));
    }

    if bytes[..MAGIC_BYTES.len()] != MAGIC_BYTES {
        return Err(crate::Error::InvalidHeader("BurrFilter"));
    }
    let filter_type = bytes[MAGIC_BYTES.len()];
    if filter_type != expected_type {
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

    // r <= 64 → one solution row is one word. We mirror the in-memory probe
    // invariants without storing a stride at all; if r > 64 ever lands the
    // validation above already rejected it.
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
        let expected_z_len = super::packed::z_byte_len(m, r)
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

        let equation: StandardEquation = standard_equation_from_hash(hash, seed, &layer_params);

        // The band's columns are two adjacent runs of `r` words — the segment
        // the band starts in and the next — so the whole window is one
        // contiguous `2 * r * 8`-byte span. Prefetch it before the threshold
        // check so the hash-random cold miss overlaps the is_bumped work.
        // Hint only; clamped to `z`.
        let win_start = (equation.start / super::packed::SEGMENT_ROWS) * usize::from(r) * 8;
        super::prefetch::prefetch_span(
            z.as_ptr().wrapping_add(win_start),
            (2 * usize::from(r) * 8).min(z.len().saturating_sub(win_start)),
        );

        if is_bumped(&equation, thresholds, b) {
            continue;
        }

        debug_assert_eq!(equation.coeff_hi, 0, "w <= 64 keeps coeff_hi == 0");
        return Ok(
            match super::packed::walk_band::<EARLY_OUT>(
                z,
                r,
                equation.start,
                equation.coeff_lo,
                equation.fingerprint,
            ) {
                super::packed::BandWalk::Reduced(acc) => FirstLayerWalk::Found(acc),
                super::packed::BandWalk::Mismatch => FirstLayerWalk::Absent,
                super::packed::BandWalk::Truncated => FirstLayerWalk::Truncated,
            },
        );
    }

    Ok(FirstLayerWalk::AllBumped)
}

/// Single-pass parse + membership probe over raw wire bytes.
///
/// Used on the LSM table read hot path (`FilterBlock::maybe_contains_hash`)
/// where the wire buffer is already in the block cache.
///
/// Returns:
/// - `Ok(true)`  — hash may be present (or wire is corrupted in a way we
///   cannot validate → fail-closed: caller falls through to a real index
///   lookup rather than reporting a false negative);
/// - `Ok(false)` — hash is definitely not in the inserted set;
/// - `Err(InvalidHeader)` / `Err(InvalidTag)` — wire prefix is unparseable
///   (bad magic, wrong filter_type/version, truncated). Differs from the
///   fail-closed `true` path: a structurally invalid header is a real error
///   returned upstream so the table read path can surface it.
#[inline]
#[expect(
    clippy::match_same_arms,
    reason = "`Found` and `Truncated` both answer possibly-present, for opposite \
              reasons: one found the key's fingerprint, the other could not read \
              far enough to rule it out. Merging them would put one comment on \
              two unrelated outcomes and hide the fail-closed rule."
)]
pub(crate) fn contains_hash_from_bytes(bytes: &[u8], hash: u64) -> crate::Result<bool> {
    // Early-out: each result bit is compared as it is reduced, so an absent
    // key stops at the first disagreeing bit — about two of them on average.
    match walk_first_layer::<true>(bytes, hash, BURR_FILTER_TYPE_BYTE)? {
        FirstLayerWalk::Found(_) => Ok(true),
        FirstLayerWalk::Absent | FirstLayerWalk::AllBumped => Ok(false),
        // Truncated payload → fail closed: report possibly-present so the
        // table read path falls through to a real index lookup rather than
        // a false negative on substituted zeros.
        FirstLayerWalk::Truncated => Ok(true),
    }
}

/// Single-pass parse + locator recovery over raw retrieval-BuRR wire bytes.
///
/// The retrieval counterpart of [`contains_hash_from_bytes`]: it accepts only
/// a retrieval payload (filter_type tag [`BURR_RETRIEVAL_TYPE_BYTE`]) and
/// recovers the r-bit locator stored for `hash`.
///
/// Returns:
/// - `Ok(Some(locator))` — the value recovered at the first non-bumped layer.
///   For a key in the built set this is its exact stored locator; for an
///   absent key it is an unspecified r-bit value, so the caller MUST verify
///   the key at the located slot (the locate step subsumes membership).
/// - `Ok(None)` — the ribbon cannot answer (every layer bumped the key, or a
///   payload was truncated mid-row): the caller falls back to the sorted
///   index.
/// - `Err(InvalidHeader)` / `Err(InvalidTag)` — the wire prefix is
///   unparseable or is not a retrieval payload (e.g. a membership tag 2).
#[inline]
pub(crate) fn recover_value_from_bytes(bytes: &[u8], hash: u64) -> crate::Result<Option<u64>> {
    // No early-out: a retrieval needs all `r` bits and has no fingerprint to
    // compare them against. `acc` carries exactly those bits, so no extra
    // masking is needed here.
    match walk_first_layer::<false>(bytes, hash, BURR_RETRIEVAL_TYPE_BYTE)? {
        FirstLayerWalk::Found(acc) => Ok(Some(acc)),
        // `Absent` is unreachable without the early-out; mapped to "cannot
        // answer" rather than asserted away, so a future change to the walk
        // degrades to the index fallback instead of a wrong locator.
        FirstLayerWalk::Absent | FirstLayerWalk::AllBumped | FirstLayerWalk::Truncated => Ok(None),
    }
}

/// Probe a decoded BuRR filter with a pre-computed hash. Returns
/// `true` if the hash may correspond to an inserted key, `false` if
/// definitely-not-inserted.
///
/// This is the hot path for the LSM filter framework: the table read
/// path already computes the key's u64 hash for hash-table indexing
/// elsewhere; the filter consumes that same hash directly instead of
/// re-hashing.
#[inline]
#[expect(
    clippy::match_same_arms,
    reason = "same as `contains_hash_from_bytes`: found and truncated both answer \
              possibly-present for opposite reasons, and the fail-closed rule \
              needs its own arm to be stated at."
)]
pub(crate) fn contains_hash(decoded: &DecodedFilter<'_>, hash: u64) -> bool {
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

        let equation: StandardEquation =
            standard_equation_from_hash(hash, layer.seed, &layer_params);

        // Same contiguous two-segment window as the borrowed-bytes walk.
        let z = layer.z_bytes;
        let win_start = (equation.start / super::packed::SEGMENT_ROWS) * usize::from(decoded.r) * 8;
        super::prefetch::prefetch_span(
            z.as_ptr().wrapping_add(win_start),
            (2 * usize::from(decoded.r) * 8).min(z.len().saturating_sub(win_start)),
        );

        if is_bumped(&equation, layer.thresholds, decoded.b) {
            continue;
        }

        // coeff_hi is always 0 for w <= 64 (the case we deploy); a future
        // w > 64 build path would need a wider band read.
        debug_assert_eq!(equation.coeff_hi, 0, "w <= 64 keeps coeff_hi == 0");

        return match super::packed::walk_band::<true>(
            z,
            decoded.r,
            equation.start,
            equation.coeff_lo,
            equation.fingerprint,
        ) {
            super::packed::BandWalk::Reduced(_) => true,
            super::packed::BandWalk::Mismatch => false,
            // Fail closed on a truncated payload: report possibly-present so
            // the read path falls through to a real index lookup rather than
            // producing a false negative on missing words.
            super::packed::BandWalk::Truncated => true,
        };
    }
    false
}
