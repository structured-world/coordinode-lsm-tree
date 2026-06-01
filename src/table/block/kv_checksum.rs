// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Per-KV checksum footer for per-KV-checked data blocks.
//!
//! A per-KV-checked block (one whose header carries the
//! [`KV_CHECKSUM_FOOTER`](super::header::block_flags::KV_CHECKSUM_FOOTER)
//! flag) is a standard data block payload, byte-for-byte identical to a
//! plain [`BlockType::Data`](super::BlockType::Data) block, followed by a
//! fixed trailing footer:
//!
//! ```text
//! [ standard Data-block payload ]
//! [ kv_checksums_array: count × digest_size ]   little-endian digests
//! [ kv_checksum_algorithm: u8 ]                 ChecksumAlgorithm wire tag
//! [ kv_count: u32 ]                             entry count, little-endian
//! ```
//!
//! Wrapping (rather than prefixing) keeps the inner payload identical to a
//! plain data block, so the standard [`Decoder`](super::Decoder) /
//! `point_read` / seek paths run on the inner slice with zero changes: the
//! reader splits the footer off the end, hands the inner slice to the
//! unchanged decoder, and verifies digests only on the scrub / paranoid
//! path (block-level XXH3 already covers the on-disk bytes on the hot read
//! path).
//!
//! ## Digest domain
//!
//! Each digest is computed over the entry's LOGICAL content, not its
//! on-disk encoding: `value_type ‖ seqno (LE u64) ‖ user_key ‖ value`. This
//! is invariant to restart-interval re-encoding (prefix truncation differs
//! per block layout), so the same digest is reproduced every time a
//! compaction re-packs the entry. In this implementation the writer
//! computes the digests when it compiles the block; because the domain is
//! logical, a future memtable-insert path that carries a precomputed digest
//! would yield the identical value (recompute and carry agree).

use alloc::vec::Vec;

use crate::InternalValue;
use crate::runtime_config::ChecksumAlgorithm;

/// Size of the fixed footer tail after the checksum array: a one-byte
/// algorithm tag plus a four-byte little-endian entry count.
pub const FOOTER_TAIL_LEN: usize = 1 + 4;

/// Computes the per-KV digest of `item` under `algo` over the entry's
/// logical content (`value_type ‖ seqno ‖ user_key ‖ value`).
///
/// Returns `None` when `algo` is not compiled into this build (mirrors
/// [`ChecksumAlgorithm::compute`]); callers translate that into a typed
/// not-compiled-in error.
#[must_use]
pub fn kv_digest(item: &InternalValue, algo: ChecksumAlgorithm) -> Option<u64> {
    // Logical domain: assemble the same fields the writer/memtable hold,
    // independent of on-disk prefix truncation. Order matches the on-disk
    // field order for readability, but these are logical values, not their
    // varint encodings.
    let mut buf = Vec::with_capacity(1 + 8 + item.key.user_key.len() + item.value.len());
    buf.push(u8::from(item.key.value_type));
    buf.extend_from_slice(&item.key.seqno.to_le_bytes());
    buf.extend_from_slice(&item.key.user_key);
    buf.extend_from_slice(&item.value);
    algo.compute(&buf)
}

/// Appends the per-KV checksum footer to `payload` (which already holds the
/// encoded standard data-block bytes).
///
/// `digests` are stored little-endian, each truncated to `algo.digest_size()`
/// bytes, in entry order. The tail records `algo`'s wire tag and the entry
/// count so the reader can split the footer without consulting the block
/// trailer.
pub fn append_footer(payload: &mut Vec<u8>, digests: &[u64], algo: ChecksumAlgorithm) {
    let size = algo.digest_size();
    for &d in digests {
        let le = d.to_le_bytes();
        // `digest_size` is 4 or 8; the low `size` bytes are the meaningful
        // digest (Xxh3Low32 / Crc32c already mask to 32 bits in `compute`).
        if let Some(slice) = le.get(..size) {
            payload.extend_from_slice(slice);
        }
    }
    payload.push(algo.wire_tag());
    // Entry count fits u32: data blocks never approach 4G entries.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a data block never holds more than u32::MAX entries"
    )]
    payload.extend_from_slice(&(digests.len() as u32).to_le_bytes());
}

/// Splits a footer-bearing data block, returning the inner standard
/// data-block slice (byte-identical to a plain `BlockType::Data` block) with
/// the per-KV checksum footer removed. Feed the returned slice straight to
/// the standard decoder.
///
/// The footer's algorithm tag and entry count are validated to locate the
/// inner/footer boundary, but the parsed digests are not returned here: the
/// read path does not verify per-entry checksums (that is the scrub /
/// paranoid path; the block-level checksum already validated the on-disk
/// bytes at load time).
///
/// # Errors
///
/// Returns [`crate::Error::InvalidTrailer`] when the footer is structurally
/// malformed: too short to hold the tail, an unknown algorithm tag, or a
/// `count` inconsistent with the available bytes.
pub fn split_inner(bytes: &[u8]) -> crate::Result<&[u8]> {
    let total = bytes.len();
    if total < FOOTER_TAIL_LEN {
        return Err(crate::Error::InvalidTrailer);
    }
    let tail_start = total - FOOTER_TAIL_LEN;
    let tail = bytes
        .get(tail_start..total)
        .ok_or(crate::Error::InvalidTrailer)?;
    // `tail` is exactly FOOTER_TAIL_LEN (= 5) bytes: [algo_tag][count: u32 LE].
    let algo_tag = *tail.first().ok_or(crate::Error::InvalidTrailer)?;
    let algo = ChecksumAlgorithm::from_wire_tag(algo_tag).ok_or(crate::Error::InvalidTrailer)?;
    let count = u32::from_le_bytes(
        tail.get(1..)
            .and_then(|s| s.try_into().ok())
            .ok_or(crate::Error::InvalidTrailer)?,
    ) as usize;

    let array_len = count
        .checked_mul(algo.digest_size())
        .ok_or(crate::Error::InvalidTrailer)?;
    if array_len > tail_start {
        return Err(crate::Error::InvalidTrailer);
    }
    let array_start = tail_start - array_len;

    bytes.get(..array_start).ok_or(crate::Error::InvalidTrailer)
}

/// Full decomposition of a footer-bearing data block: the inner standard
/// data-block slice plus the parsed per-entry digests and algorithm.
/// Used by the scrub / paranoid verify path, which re-derives each
/// entry's digest and compares it against the stored array.
pub struct SplitFull<'a> {
    /// Inner payload: byte-identical to a plain `BlockType::Data` block.
    pub inner: &'a [u8],
    /// Per-entry digests in scan order, each masked to the algorithm width.
    pub digests: Vec<u64>,
    /// Algorithm the digests were computed under.
    pub algo: ChecksumAlgorithm,
}

/// Splits a footer-bearing data block into its inner slice plus the parsed
/// per-entry digests and algorithm. The verify path uses this to recompute
/// each entry's logical-content digest and compare against the stored value.
///
/// # Errors
///
/// Returns [`crate::Error::InvalidTrailer`] when the footer is structurally
/// malformed (too short, unknown algorithm tag, or a `count` inconsistent
/// with the available bytes).
pub fn split_full(bytes: &[u8]) -> crate::Result<SplitFull<'_>> {
    let total = bytes.len();
    if total < FOOTER_TAIL_LEN {
        return Err(crate::Error::InvalidTrailer);
    }
    let tail_start = total - FOOTER_TAIL_LEN;
    let tail = bytes
        .get(tail_start..total)
        .ok_or(crate::Error::InvalidTrailer)?;
    let algo_tag = *tail.first().ok_or(crate::Error::InvalidTrailer)?;
    let algo = ChecksumAlgorithm::from_wire_tag(algo_tag).ok_or(crate::Error::InvalidTrailer)?;
    let count = u32::from_le_bytes(
        tail.get(1..)
            .and_then(|s| s.try_into().ok())
            .ok_or(crate::Error::InvalidTrailer)?,
    ) as usize;

    let size = algo.digest_size();
    let array_len = count
        .checked_mul(size)
        .ok_or(crate::Error::InvalidTrailer)?;
    if array_len > tail_start {
        return Err(crate::Error::InvalidTrailer);
    }
    let array_start = tail_start - array_len;

    let mut digests = Vec::with_capacity(count);
    for i in 0..count {
        let off = array_start + i * size;
        let chunk = bytes
            .get(off..off + size)
            .ok_or(crate::Error::InvalidTrailer)?;
        let mut word = [0u8; 8];
        word.get_mut(..size)
            .ok_or(crate::Error::InvalidTrailer)?
            .copy_from_slice(chunk);
        digests.push(u64::from_le_bytes(word));
    }

    let inner = bytes
        .get(..array_start)
        .ok_or(crate::Error::InvalidTrailer)?;
    Ok(SplitFull {
        inner,
        digests,
        algo,
    })
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::ValueType;

    fn val(user_key: &[u8], value: &[u8], seqno: u64, vt: ValueType) -> InternalValue {
        InternalValue::from_components(user_key.to_vec(), value.to_vec(), seqno, vt)
    }

    #[test]
    fn kv_digest_is_invariant_to_callsite_but_sensitive_to_content() {
        // The digest must depend only on logical content, so two
        // independent constructions of the same entry agree (carry ==
        // recompute), while any field change flips it.
        let a = val(b"key", b"value", 7, ValueType::Value);
        let same = val(b"key", b"value", 7, ValueType::Value);
        let d = ChecksumAlgorithm::Xxh3_64;
        assert_eq!(kv_digest(&a, d), kv_digest(&same, d));

        // Each logical field participates.
        assert_ne!(
            kv_digest(&a, d),
            kv_digest(&val(b"KEY", b"value", 7, ValueType::Value), d),
            "user_key must matter"
        );
        assert_ne!(
            kv_digest(&a, d),
            kv_digest(&val(b"key", b"VALUE", 7, ValueType::Value), d),
            "value must matter"
        );
        assert_ne!(
            kv_digest(&a, d),
            kv_digest(&val(b"key", b"value", 8, ValueType::Value), d),
            "seqno must matter"
        );
        assert_ne!(
            kv_digest(&a, d),
            kv_digest(&val(b"key", b"value", 7, ValueType::Tombstone), d),
            "value_type must matter"
        );
    }

    #[test]
    fn footer_roundtrips_inner_payload() {
        // append_footer then split_inner must reproduce the inner bytes
        // exactly, for both digest widths (the footer occupies the tail).
        for algo in [ChecksumAlgorithm::Xxh3_64, ChecksumAlgorithm::Xxh3Low32] {
            let inner = b"standard data block payload bytes".to_vec();
            let digests: Vec<u64> = (0..5).map(|i| 0x0102_0304_0506_0708 ^ i).collect();

            let mut payload = inner.clone();
            append_footer(&mut payload, &digests, algo);
            assert!(
                payload.len() > inner.len(),
                "footer must add bytes for {algo:?}"
            );

            let recovered = split_inner(&payload).expect("well-formed footer must split");
            assert_eq!(recovered, &inner[..], "inner payload must round-trip");
        }
    }

    #[test]
    fn split_inner_rejects_too_short() {
        // Fewer bytes than the fixed tail cannot carry a footer.
        assert!(split_inner(&[0u8; FOOTER_TAIL_LEN - 1]).is_err());
    }

    #[test]
    fn split_inner_rejects_unknown_algorithm_tag() {
        // A tag outside the registry must be rejected, not silently
        // coerced to a known algorithm.
        let mut payload = b"inner".to_vec();
        payload.push(0xFE); // unknown algo tag
        payload.extend_from_slice(&0u32.to_le_bytes());
        assert!(split_inner(&payload).is_err());
    }

    #[test]
    fn split_inner_rejects_count_exceeding_available_bytes() {
        // A forged count that would reach past the inner payload must be
        // rejected rather than slicing out of bounds.
        let mut payload = b"x".to_vec();
        payload.push(ChecksumAlgorithm::Xxh3_64.wire_tag());
        payload.extend_from_slice(&1000u32.to_le_bytes()); // 1000×8 ≫ payload
        assert!(split_inner(&payload).is_err());
    }
}
