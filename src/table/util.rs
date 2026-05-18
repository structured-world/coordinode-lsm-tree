// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Block, BlockHandle, GlobalTableId};
use crate::{
    Cache, CompressionType, KeyRange, Table, encryption::EncryptionProvider,
    file_accessor::FileAccessor, table::block::BlockType, version::run::Ranged,
};
use std::path::Path;

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

/// Returns the bounding key range of a table slice.
///
/// Takes `first().min()` and `last().max()` — no comparison needed because
/// callers pass tables that are already sorted in comparator order (via
/// `push_cmp` / `sort_by_cmp`). Works correctly for any comparator.
#[must_use]
pub fn aggregate_run_key_range(tables: &[Table]) -> KeyRange {
    #[expect(clippy::expect_used, reason = "runs are never empty by definition")]
    let lo = tables.first().expect("run should never be empty");
    #[expect(clippy::expect_used, reason = "runs are never empty by definition")]
    let hi = tables.last().expect("run should never be empty");
    KeyRange::new((lo.key_range().min().clone(), hi.key_range().max().clone()))
}

/// [start, end] slice indexes
#[derive(Debug)]
pub struct SliceIndexes(pub usize, pub usize);

/// Loads a block from disk or block cache, if cached.
///
/// Also handles file descriptor opening and caching.
#[expect(
    clippy::too_many_arguments,
    reason = "block loading requires table id, path, file accessor, cache, handle, block type, compression, and encryption context"
)]
pub fn load_block(
    table_id: GlobalTableId,
    path: &Path,
    file_accessor: &FileAccessor,
    cache: &Cache,
    handle: &BlockHandle,
    block_type: BlockType,
    compression: CompressionType,
    encryption: Option<&dyn EncryptionProvider>,
    #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    #[cfg(feature = "metrics")] metrics: &Metrics,
) -> crate::Result<Block> {
    #[cfg(feature = "metrics")]
    use std::sync::atomic::Ordering::Relaxed;

    log::trace!("load {block_type:?} block {handle:?}");

    if let Some(block) = cache.get_block(table_id, handle.offset()) {
        if block.header.block_type != block_type {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }

        #[cfg(feature = "metrics")]
        match block_type {
            BlockType::Filter => {
                metrics.filter_block_load_cached.fetch_add(1, Relaxed);
            }
            BlockType::Index => {
                metrics.index_block_load_cached.fetch_add(1, Relaxed);
            }
            BlockType::RangeTombstone => {
                metrics
                    .range_tombstone_block_load_cached
                    .fetch_add(1, Relaxed);
            }
            BlockType::Data | BlockType::Meta => {
                metrics.data_block_load_cached.fetch_add(1, Relaxed);
            }
        }

        return Ok(block);
    }

    let (fd, cache_event) = file_accessor.get_or_open_table(&table_id, path)?;

    // Only track descriptor-table cache metrics; pinned FDs (None) are not cache events.
    #[cfg(feature = "metrics")]
    if let Some(hit) = cache_event {
        if hit {
            metrics.table_file_opened_cached.fetch_add(1, Relaxed);
        } else {
            metrics.table_file_opened_uncached.fetch_add(1, Relaxed);
        }
    }

    #[cfg(not(feature = "metrics"))]
    let _ = cache_event;

    let block = Block::from_file(
        fd.as_ref(),
        *handle,
        compression,
        encryption,
        #[cfg(zstd_any)]
        zstd_dict,
    )?;

    if block.header.block_type != block_type {
        return Err(crate::Error::InvalidTag((
            "BlockType",
            block.header.block_type.into(),
        )));
    }

    #[cfg(feature = "metrics")]
    match block_type {
        BlockType::Filter => {
            metrics.filter_block_load_io.fetch_add(1, Relaxed);

            metrics
                .filter_block_io_requested
                .fetch_add(handle.size().into(), Relaxed);
        }
        BlockType::Index => {
            metrics.index_block_load_io.fetch_add(1, Relaxed);

            metrics
                .index_block_io_requested
                .fetch_add(handle.size().into(), Relaxed);
        }
        BlockType::RangeTombstone => {
            metrics.range_tombstone_block_load_io.fetch_add(1, Relaxed);

            metrics
                .range_tombstone_block_io_requested
                .fetch_add(handle.size().into(), Relaxed);
        }
        BlockType::Data | BlockType::Meta => {
            metrics.data_block_load_io.fetch_add(1, Relaxed);

            metrics
                .data_block_io_requested
                .fetch_add(handle.size().into(), Relaxed);
        }
    }

    cache.insert_block(table_id, handle.offset(), block.clone());

    Ok(block)
}

/// Returns the length of the longest shared byte prefix of `s1` and `s2`.
///
/// This is on the hot path of block encoding during flush and compaction —
/// every truncated entry pays one call against the restart base key.
///
/// Dispatch:
/// - **`x86_64` with AVX2** (runtime-detected): 32-byte vectorized lanes via `_mm256_cmpeq_epi8`.
/// - **`x86_64` without AVX2**: 16-byte SSE2 lanes via `_mm_cmpeq_epi8` — SSE2 is the mandatory
///   `x86_64` ISA baseline, so this path needs no runtime check, only the AVX2 negative result.
/// - **`aarch64` little-endian**: 16-byte vectorized lanes via NEON (`ARMv8` baseline — no runtime check).
/// - **Everything else** (incl. big-endian aarch64, 32-bit x86, riscv, powerpc): 8-byte word stride via XOR + trailing zeros.
///
/// `is_x86_feature_detected!` caches the CPUID result, so the per-call dispatch
/// cost is one cached atomic load on `x86_64`.
#[must_use]
pub fn longest_shared_prefix_length(s1: &[u8], s2: &[u8]) -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 availability just verified via runtime CPU feature detection.
            return unsafe { lsp_avx2(s1, s2) };
        }
        // SAFETY: SSE2 is part of the mandatory x86_64 ISA baseline — every x86_64 CPU
        // supports it, so no runtime check is required. The `#[target_feature(enable = "sse2")]`
        // attribute on `lsp_sse2` documents this contract explicitly.
        return unsafe { lsp_sse2(s1, s2) };
    }
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        // SAFETY: NEON is mandatory in the ARMv8 baseline that `target_arch = "aarch64"` implies.
        // The kernel relies on LE byte order for `trailing_zeros() / 8` mismatch position math
        // and for NEON lane-to-memory mapping — restricted to LE aarch64 (the only practically
        // shipped flavour: Linux servers, Apple Silicon, Android, iOS).
        return unsafe { lsp_neon(s1, s2) };
    }
    // On x86_64 the SSE2 arm above unconditionally returns, and on LE aarch64 the NEON
    // arm does the same — so the scalar tail is statically unreachable on both. It IS
    // reachable on every other target (BE aarch64, 32-bit x86, riscv, powerpc, …),
    // which is the whole point of having a portable fallback.
    #[cfg_attr(
        any(
            target_arch = "x86_64",
            all(target_arch = "aarch64", target_endian = "little")
        ),
        expect(
            unreachable_code,
            reason = "x86_64 SSE2 and LE aarch64 NEON arms above are unconditional; scalar tail only reached on other archs/endianness"
        )
    )]
    lsp_scalar(s1, s2)
}

/// 8-byte word-stride scalar implementation — works on every platform, no intrinsics.
///
/// Compares 8 bytes at a time via `u64` XOR and locates the first mismatching
/// byte using `trailing_zeros() / 8`. Falls back to a byte loop for the tail
/// shorter than 8 bytes.
#[must_use]
pub(crate) fn lsp_scalar(s1: &[u8], s2: &[u8]) -> usize {
    let min_len = s1.len().min(s2.len());
    let mut i = 0;

    while i + 8 <= min_len {
        // SAFETY: i + 8 <= min_len <= s{1,2}.len() — both 8-byte reads are in-bounds.
        #[expect(unsafe_code, reason = "bounds checked by loop guard above")]
        let (a, b) = unsafe {
            (
                s1.as_ptr().add(i).cast::<u64>().read_unaligned(),
                s2.as_ptr().add(i).cast::<u64>().read_unaligned(),
            )
        };
        let diff = a ^ b;
        if diff != 0 {
            // Endian-independent: position of first byte-level difference.
            // On LE the lowest mismatching byte is at trailing_zeros / 8;
            // on BE it is at leading_zeros / 8. Use the matching primitive.
            #[cfg(target_endian = "little")]
            let byte_off = (diff.trailing_zeros() / 8) as usize;
            #[cfg(target_endian = "big")]
            let byte_off = (diff.leading_zeros() / 8) as usize;
            return i + byte_off;
        }
        i += 8;
    }

    while i < min_len {
        // SAFETY: i < min_len <= s{1,2}.len()
        #[expect(unsafe_code, reason = "i < min_len bounds-checked above")]
        let (a, b) = unsafe { (*s1.get_unchecked(i), *s2.get_unchecked(i)) };
        if a != b {
            return i;
        }
        i += 1;
    }

    min_len
}

/// AVX2 implementation — 32 bytes per iteration via `_mm256_cmpeq_epi8`.
///
/// # Safety
///
/// Caller must ensure the host CPU supports AVX2 (`is_x86_feature_detected!("avx2")`).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[expect(unsafe_code, reason = "intrinsics require unsafe")]
#[must_use]
unsafe fn lsp_avx2(s1: &[u8], s2: &[u8]) -> usize {
    use std::arch::x86_64::{__m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8};

    let min_len = s1.len().min(s2.len());
    let mut i = 0;

    while i + 32 <= min_len {
        // SAFETY: i + 32 <= min_len ≤ s{1,2}.len() — both 32-byte loads are in-bounds.
        // `_mm256_loadu_si256` is the *unaligned* load, so the u8→__m256i pointer cast
        // does not require 32-byte alignment (the pointer is only used by `loadu`).
        #[expect(
            clippy::cast_ptr_alignment,
            reason = "_mm256_loadu_si256 explicitly performs an unaligned 32-byte load"
        )]
        let (va, vb) = unsafe {
            (
                _mm256_loadu_si256(s1.as_ptr().add(i).cast::<__m256i>()),
                _mm256_loadu_si256(s2.as_ptr().add(i).cast::<__m256i>()),
            )
        };
        // Register-only AVX2 intrinsics under #[target_feature(enable = "avx2")] —
        // no `unsafe` block needed; the function-level `unsafe` covers their availability.
        let cmp = _mm256_cmpeq_epi8(va, vb);
        // `_mm256_movemask_epi8` returns the byte-mask as a signed `i32`. We treat the
        // bit pattern as `u32` for trailing-zeros math — `cast_unsigned()` makes the
        // sign-preserving reinterpretation explicit.
        let mask = _mm256_movemask_epi8(cmp).cast_unsigned();
        if mask != u32::MAX {
            return i + (!mask).trailing_zeros() as usize;
        }
        i += 32;
    }

    // Tail: byte-stride (≤31 bytes left, not worth dispatching a narrower kernel).
    while i < min_len {
        // SAFETY: i < min_len ≤ s{1,2}.len()
        let (a, b) = unsafe { (*s1.get_unchecked(i), *s2.get_unchecked(i)) };
        if a != b {
            return i;
        }
        i += 1;
    }

    min_len
}

/// SSE2 implementation — 16 bytes per iteration via `_mm_cmpeq_epi8`.
///
/// Used on `x86_64` hosts that lack AVX2 (older Intel Atoms, some sandboxed
/// VMs / containers, AMD pre-Excavator, low-power embedded `x86_64`). SSE2 is
/// mandatory in the `x86_64` ISA baseline, so no runtime detection is needed.
///
/// # Safety
///
/// Caller must be on `target_arch = "x86_64"`. The `#[target_feature(enable = "sse2")]`
/// attribute is satisfied unconditionally on `x86_64` — every CPU supports SSE2.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
#[expect(unsafe_code, reason = "intrinsics require unsafe")]
#[must_use]
unsafe fn lsp_sse2(s1: &[u8], s2: &[u8]) -> usize {
    use std::arch::x86_64::{__m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8};

    let min_len = s1.len().min(s2.len());
    let mut i = 0;

    while i + 16 <= min_len {
        // SAFETY: i + 16 <= min_len ≤ s{1,2}.len() — both 16-byte loads are in-bounds.
        // `_mm_loadu_si128` is the *unaligned* load, so the u8→__m128i pointer cast
        // does not require 16-byte alignment (the pointer is only used by `loadu`).
        #[expect(
            clippy::cast_ptr_alignment,
            reason = "_mm_loadu_si128 explicitly performs an unaligned 16-byte load"
        )]
        let (va, vb) = unsafe {
            (
                _mm_loadu_si128(s1.as_ptr().add(i).cast::<__m128i>()),
                _mm_loadu_si128(s2.as_ptr().add(i).cast::<__m128i>()),
            )
        };
        // Register-only SSE2 intrinsics under #[target_feature(enable = "sse2")] —
        // safe in stable Rust without an inner `unsafe` block.
        let cmp = _mm_cmpeq_epi8(va, vb);
        // `_mm_movemask_epi8` returns the 16-bit byte-mask as a signed `i32`
        // (low 16 bits used, high 16 zero). Reinterpret as `u32` for trailing-zeros math.
        let mask = _mm_movemask_epi8(cmp).cast_unsigned();
        // SSE2 mask is 16 bits, so a full-match lane is `0xFFFF`, not `u32::MAX`.
        if mask != 0xFFFF {
            return i + (!mask).trailing_zeros() as usize;
        }
        i += 16;
    }

    // Tail: byte-stride (≤15 bytes left).
    while i < min_len {
        // SAFETY: i < min_len ≤ s{1,2}.len()
        let (a, b) = unsafe { (*s1.get_unchecked(i), *s2.get_unchecked(i)) };
        if a != b {
            return i;
        }
        i += 1;
    }

    min_len
}

/// NEON implementation — 16 bytes per iteration via `vceqq_u8` + byte-wise mask reduction.
///
/// Restricted to **little-endian** aarch64 because the lane-to-memory mapping of
/// `vgetq_lane_u64` and the `trailing_zeros() / 8` mismatch-position math both
/// assume LE byte order. Big-endian aarch64 falls back to the scalar kernel.
///
/// # Safety
///
/// NEON is part of the `ARMv8` baseline and is always available on `target_arch = "aarch64"`,
/// so no runtime detection is needed. The `unsafe` is required only because the intrinsics
/// themselves are `unsafe fn`.
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[target_feature(enable = "neon")]
#[expect(unsafe_code, reason = "intrinsics require unsafe")]
#[must_use]
unsafe fn lsp_neon(s1: &[u8], s2: &[u8]) -> usize {
    use std::arch::aarch64::{
        vandq_u8, vceqq_u8, vdupq_n_u8, vgetq_lane_u64, vld1q_u8, vreinterpretq_u64_u8,
    };

    let min_len = s1.len().min(s2.len());
    let mut i = 0;

    // 16-byte equality mask: lanes are 0xFF when bytes match, 0x00 when they differ.
    // Reduce to a 128-bit value and inspect its halves as u64 for first-mismatch position.
    while i + 16 <= min_len {
        // SAFETY: i + 16 <= min_len ≤ s{1,2}.len() — both 16-byte loads are in-bounds.
        let (va, vb) = unsafe { (vld1q_u8(s1.as_ptr().add(i)), vld1q_u8(s2.as_ptr().add(i))) };
        // Register-only NEON intrinsics — safe in stable Rust under the `neon` target feature.
        let cmp = vceqq_u8(va, vb);
        // Trim to bit-per-byte mask via AND with 0xFF (no-op for the equality result,
        // but keeps the intent explicit); reinterpret as two u64 halves.
        let masked = vandq_u8(cmp, vdupq_n_u8(0xFF));
        let as_u64 = vreinterpretq_u64_u8(masked);
        let lo = vgetq_lane_u64(as_u64, 0);
        let hi = vgetq_lane_u64(as_u64, 1);

        if lo != u64::MAX {
            // First mismatching byte is in the low half.
            return i + (!lo).trailing_zeros() as usize / 8;
        }
        if hi != u64::MAX {
            // First mismatching byte is in the high half.
            return i + 8 + (!hi).trailing_zeros() as usize / 8;
        }
        i += 16;
    }

    // Tail: byte-stride for the ≤15 remaining bytes.
    while i < min_len {
        // SAFETY: i < min_len ≤ s{1,2}.len()
        let (a, b) = unsafe { (*s1.get_unchecked(i), *s2.get_unchecked(i)) };
        if a != b {
            return i;
        }
        i += 1;
    }

    min_len
}

/// Compares the conceptual concatenation `prefix + suffix` against `needle`
/// using the given comparator.
///
/// For the default lexicographic comparator this performs a zero-allocation
/// bytewise comparison. Custom comparators fall back to concatenating prefix
/// and suffix into a temporary `Vec` so that `UserComparator::compare` always
/// receives a complete key.
#[must_use]
pub fn compare_prefixed_slice(
    prefix: &[u8],
    suffix: &[u8],
    needle: &[u8],
    cmp: &dyn crate::comparator::UserComparator,
) -> std::cmp::Ordering {
    // Fast path: zero-allocation bytewise comparison for the default
    // (lexicographic) comparator. This is the hot path for block index
    // and data block binary searches.
    if cmp.is_lexicographic() {
        return compare_prefixed_slice_lexicographic(prefix, suffix, needle);
    }

    // Slow path: materialize prefix+suffix into a contiguous buffer for
    // custom comparators. Uses a stack buffer for typical key sizes to
    // avoid heap allocation on the hot binary-search path.
    let total_len = prefix.len() + suffix.len();

    if total_len <= 256 {
        let mut buf = [0_u8; 256];

        // SAFETY (indexing): total_len <= 256 == buf.len(), and
        // prefix.len() + suffix.len() == total_len, so all slices are in bounds.
        #[expect(clippy::indexing_slicing, reason = "total_len <= 256 checked above")]
        {
            buf[..prefix.len()].copy_from_slice(prefix);
            buf[prefix.len()..total_len].copy_from_slice(suffix);
        }

        #[expect(clippy::indexing_slicing, reason = "total_len <= 256 checked above")]
        return cmp.compare(&buf[..total_len], needle);
    }

    // Fallback for unusually large keys: allocate a temporary Vec.
    let mut full_key = Vec::with_capacity(total_len);
    full_key.extend_from_slice(prefix);
    full_key.extend_from_slice(suffix);
    cmp.compare(&full_key, needle)
}

/// Zero-allocation lexicographic comparison of `prefix + suffix` against `needle`.
#[must_use]
fn compare_prefixed_slice_lexicographic(
    prefix: &[u8],
    suffix: &[u8],
    needle: &[u8],
) -> std::cmp::Ordering {
    use std::cmp::Ordering::{Equal, Greater};

    if needle.is_empty() {
        let combined_len = prefix.len() + suffix.len();
        return if combined_len > 0 { Greater } else { Equal };
    }

    let max_pfx_len = prefix.len().min(needle.len());

    {
        // SAFETY: max_pfx_len = min(prefix.len(), needle.len()), so both
        // slices [0..max_pfx_len] are within bounds by construction.
        #[expect(
            unsafe_code,
            reason = "max_pfx_len <= prefix.len() && max_pfx_len <= needle.len()"
        )]
        let pfx = unsafe { prefix.get_unchecked(0..max_pfx_len) };

        #[expect(
            unsafe_code,
            reason = "max_pfx_len <= prefix.len() && max_pfx_len <= needle.len()"
        )]
        let ndl = unsafe { needle.get_unchecked(0..max_pfx_len) };

        match pfx.cmp(ndl) {
            Equal => {}
            ordering => return ordering,
        }
    }

    let rest_len = prefix.len().saturating_sub(needle.len());
    if rest_len > 0 {
        return Greater;
    }

    // SAFETY: rest_len == 0 means prefix.len() <= needle.len(), so
    // max_pfx_len == prefix.len() <= needle.len() and needle[max_pfx_len..] is in-bounds.
    #[expect(
        unsafe_code,
        reason = "max_pfx_len <= needle.len() guaranteed by rest_len == 0 guard above"
    )]
    let remaining_needle = unsafe { needle.get_unchecked(max_pfx_len..) };
    suffix.cmp(remaining_needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator::DefaultUserComparator;
    use test_log::test;

    #[test]
    fn test_longest_shared_prefix_length() {
        assert_eq!(3, longest_shared_prefix_length(b"abc", b"abc"));
        assert_eq!(1, longest_shared_prefix_length(b"abc", b"a"));
        assert_eq!(1, longest_shared_prefix_length(b"a", b"abc"));
        assert_eq!(0, longest_shared_prefix_length(b"abc", b""));
        assert_eq!(0, longest_shared_prefix_length(b"", b"abc"));
        assert_eq!(0, longest_shared_prefix_length(b"", b""));
        assert_eq!(0, longest_shared_prefix_length(b"", b""));
        assert_eq!(0, longest_shared_prefix_length(b"abc", b"def"));
        assert_eq!(1, longest_shared_prefix_length(b"abc", b"acc"));
    }

    /// Reference implementation used by cross-impl equality tests.
    /// Identical to the pre-SIMD byte-by-byte version so the SIMD/scalar
    /// kernels must agree with it on every input.
    fn lsp_reference(s1: &[u8], s2: &[u8]) -> usize {
        s1.iter().zip(s2.iter()).take_while(|(a, b)| a == b).count()
    }

    #[test]
    fn lsp_scalar_matches_reference_on_boundaries() {
        // 0 / 7 / 8 / 9 / 15 / 16 / 17 / 31 / 32 / 33 / 63 / 64 / 127 / 128 — covers all
        // word-stride and AVX2-stride boundary cases.
        for total_len in [
            0_usize, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 127, 128,
        ] {
            for mismatch_at in 0..=total_len {
                let mut a = vec![0xAA; total_len];
                let mut b = a.clone();
                if mismatch_at < total_len {
                    #[expect(
                        clippy::expect_used,
                        reason = "test: mismatch_at < total_len = b.len() guarantees in-bounds"
                    )]
                    {
                        *b.get_mut(mismatch_at).expect("in bounds") ^= 0xFF;
                    }
                }
                let got = lsp_scalar(&a, &b);
                let want = lsp_reference(&a, &b);
                assert_eq!(
                    want, got,
                    "scalar @ len={total_len} mismatch_at={mismatch_at}"
                );

                // Also test asymmetric lengths.
                a.truncate(mismatch_at);
                let got_short = lsp_scalar(&a, &b);
                let want_short = lsp_reference(&a, &b);
                assert_eq!(
                    want_short, got_short,
                    "scalar asym len={mismatch_at} vs {total_len}"
                );
            }
        }
    }

    #[test]
    fn longest_shared_prefix_length_matches_reference_on_boundaries() {
        // Same coverage as `lsp_scalar_matches_reference_on_boundaries`, but exercises
        // the *dispatched* path — on x86_64 with AVX2 this hits the AVX2 kernel, on
        // aarch64 it hits NEON, and otherwise it falls back to the scalar kernel.
        for total_len in [
            0_usize, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 127, 128, 255, 256,
        ] {
            for mismatch_at in 0..=total_len {
                let mut a = vec![0xAA; total_len];
                let mut b = a.clone();
                if mismatch_at < total_len {
                    #[expect(
                        clippy::expect_used,
                        reason = "test: mismatch_at < total_len = b.len() guarantees in-bounds"
                    )]
                    {
                        *b.get_mut(mismatch_at).expect("in bounds") ^= 0xFF;
                    }
                }
                let got = longest_shared_prefix_length(&a, &b);
                let want = lsp_reference(&a, &b);
                assert_eq!(
                    want, got,
                    "dispatch @ len={total_len} mismatch_at={mismatch_at}"
                );

                // Asymmetric lengths — truncate `a` to mismatch_at, leaving `b` at full length.
                a.truncate(mismatch_at);
                let got_short = longest_shared_prefix_length(&a, &b);
                let want_short = lsp_reference(&a, &b);
                assert_eq!(
                    want_short, got_short,
                    "dispatch asym len={mismatch_at} vs {total_len}"
                );
            }
        }
    }

    /// SIMD kernels are sensitive to inputs that yield all-equal or all-different
    /// 32-byte / 16-byte lanes — both extremes must round-trip to the reference impl.
    /// Also covers asymmetric "one empty" pairs across all kernels.
    #[test]
    fn lsp_extreme_byte_patterns_match_reference() {
        for &(label, byte_a, byte_b) in &[
            ("all_zero_equal", 0x00_u8, 0x00_u8),
            ("all_ff_equal", 0xFF, 0xFF),
            ("zero_vs_ff", 0x00, 0xFF),
            ("alternating_match", 0x55, 0x55),
        ] {
            for len in [0_usize, 1, 8, 15, 16, 31, 32, 33, 63, 64, 128, 1023] {
                let a = vec![byte_a; len];
                let b = vec![byte_b; len];
                let want = lsp_reference(&a, &b);
                assert_eq!(want, lsp_scalar(&a, &b), "scalar {label} len={len}");
                assert_eq!(
                    want,
                    longest_shared_prefix_length(&a, &b),
                    "dispatch {label} len={len}"
                );
            }
        }

        // One-empty pairs — important boundary because every SIMD kernel's main
        // loop is skipped (min_len == 0).
        for len in [0_usize, 1, 8, 32, 128, 1024] {
            let nonempty = vec![0x42_u8; len];
            assert_eq!(0, lsp_scalar(&nonempty, &[]));
            assert_eq!(0, lsp_scalar(&[], &nonempty));
            assert_eq!(0, longest_shared_prefix_length(&nonempty, &[]));
            assert_eq!(0, longest_shared_prefix_length(&[], &nonempty));
        }
    }

    // All implementations must agree with the byte-by-byte reference for any input —
    // proptest version with random byte patterns up to 1 KiB.
    proptest::proptest! {
        #[test]
        fn lsp_scalar_equals_reference(
            s1 in proptest::collection::vec(proptest::num::u8::ANY, 0..=1024),
            s2 in proptest::collection::vec(proptest::num::u8::ANY, 0..=1024),
        ) {
            proptest::prop_assert_eq!(lsp_scalar(&s1, &s2), lsp_reference(&s1, &s2));
        }

        #[test]
        fn longest_shared_prefix_length_equals_reference(
            s1 in proptest::collection::vec(proptest::num::u8::ANY, 0..=1024),
            s2 in proptest::collection::vec(proptest::num::u8::ANY, 0..=1024),
        ) {
            proptest::prop_assert_eq!(longest_shared_prefix_length(&s1, &s2), lsp_reference(&s1, &s2));
        }
    }

    #[test]
    fn test_compare_prefixed_slice() {
        use std::cmp::Ordering::{Equal, Greater, Less};

        assert_eq!(
            Greater,
            compare_prefixed_slice(&[0, 161], &[], &[0], &DefaultUserComparator)
        );

        assert_eq!(
            Equal,
            compare_prefixed_slice(b"abc", b"xyz", b"abcxyz", &DefaultUserComparator)
        );
        assert_eq!(
            Equal,
            compare_prefixed_slice(b"abc", b"", b"abc", &DefaultUserComparator)
        );
        assert_eq!(
            Equal,
            compare_prefixed_slice(b"abc", b"abc", b"abcabc", &DefaultUserComparator)
        );
        assert_eq!(
            Equal,
            compare_prefixed_slice(b"", b"", b"", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"a", b"", b"y", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"a", b"", b"yyy", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"a", b"", b"yyy", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"yyyy", b"a", b"yyyyb", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"yyy", b"b", b"yyyyb", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"abc", b"d", b"abce", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"ab", b"", b"ac", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"a", b"", b"", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"", b"a", b"", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"a", b"a", b"", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"b", b"a", b"a", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"a", b"b", b"a", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"abc", b"xy", b"abcw", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"ab", b"cde", b"a", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"abcd", b"zz", b"abc", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"abc", b"d", b"abc", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"aaaa", b"aaab", b"aaaaaaaa", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"aaaa", b"aaba", b"aaaaaaaa", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"abcd", b"x", b"abc", &DefaultUserComparator)
        );

        assert_eq!(
            Less,
            compare_prefixed_slice(&[0x7F], &[], &[0x80], &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(&[0xFF], &[], &[0x10], &DefaultUserComparator)
        );
    }

    /// Reverse comparator to exercise the Vec-allocation slow path.
    struct ReverseComparator;
    impl crate::comparator::UserComparator for ReverseComparator {
        fn name(&self) -> &'static str {
            "test-reverse"
        }

        fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
            b.cmp(a)
        }
    }

    #[test]
    fn test_compare_prefixed_slice_custom_comparator() {
        use std::cmp::Ordering::{Equal, Greater, Less};

        use crate::comparator::UserComparator as _;
        assert_eq!(ReverseComparator.name(), "test-reverse");

        // With reverse comparator, "abc" > "xyz" (reversed)
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"ab", b"c", b"xyz", &ReverseComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"xy", b"z", b"abc", &ReverseComparator)
        );
        assert_eq!(
            Equal,
            compare_prefixed_slice(b"ab", b"c", b"abc", &ReverseComparator)
        );
        // Empty cases
        assert_eq!(
            Equal,
            compare_prefixed_slice(b"", b"", b"", &ReverseComparator)
        );
        assert_eq!(
            Less, // reversed: non-empty > empty
            compare_prefixed_slice(b"a", b"", b"", &ReverseComparator)
        );
    }
}
