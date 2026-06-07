// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

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
    ecc: Option<crate::table::block::EccParams>,
    #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    #[cfg(feature = "metrics")] metrics: &Metrics,
) -> crate::Result<Block> {
    #[cfg(feature = "metrics")]
    use std::sync::atomic::Ordering::Relaxed;

    log::trace!("load {block_type:?} block {handle:?}");

    // Invariant: manifest Blocks have their own reader path and
    // never reach the SST block cache. Surface a typed error
    // (rather than panic) so a caller that wires up an SST loader
    // with a manifest BlockType gets a routable failure instead
    // of a process abort. The check stays outside the metrics
    // cfg so the contract holds on every build.
    if matches!(block_type, BlockType::Manifest | BlockType::ManifestFooter) {
        return Err(crate::Error::InvalidTag(("BlockType", block_type.into())));
    }

    if let Some(block) = cache.get_block(table_id, handle.offset()) {
        // Per-KV checking is a header flag, not a block type, so a data
        // block is always BlockType::Data on disk — an exact role match is
        // the right swap-defence check here.
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
            // Manifest variants are rejected by the function-level
            // guard above; nothing to do here.
            BlockType::Manifest | BlockType::ManifestFooter => {}
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
        crate::table::block::BlockIdentity {
            // load_block has a GlobalTableId — use the real
            // tree_id rather than 0. This is the one production
            // call site that doesn't need to fall back to
            // per-tree key isolation as a substitute defence.
            tree_id: table_id.tree_id(),
            table_id: table_id.table_id(),
            block_offset: *handle.offset(),
            block_type,
            dict_id: compression.dict_id(),
            window_log: 0,
        },
        &{
            // ECC presence is a per-SST descriptor property (passed in by
            // the caller from table metadata): upgrade the transform to its
            // `*Ecc` variant when this SST was written with a recognized Page
            // ECC scheme. On a build WITHOUT the `page_ecc` feature `with_ecc`
            // is the identity function — the parity trailer then reads as an
            // unrecognized opaque trailer (the read frames the payload by
            // `data_length`, verifies its checksum, and reports
            // `EccStatus::Unrecognized`), so the data still loads without ECC
            // recovery rather than failing closed.
            let t = crate::table::block::BlockTransform::from_parts(
                compression,
                encryption,
                #[cfg(zstd_any)]
                zstd_dict,
            )?;
            if let Some(ecc) = ecc {
                t.with_ecc(ecc)
            } else {
                t
            }
        },
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
        // Manifest variants are rejected by the function-level
        // guard above; nothing to do here.
        BlockType::Manifest | BlockType::ManifestFooter => {}
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
/// - **`x86_64` / `x86` with AVX-512BW** (runtime-detected): 64-byte vectorized lanes via
///   `_mm512_cmpeq_epi8_mask`. Checked first so AVX-512 hosts use the widest lane.
/// - **`x86_64` / `x86` with AVX2** (runtime-detected): 32-byte vectorized lanes via `_mm256_cmpeq_epi8`.
/// - **`x86_64` with SSE2**: 16-byte lanes via `_mm_cmpeq_epi8` — SSE2 is the mandatory `x86_64`
///   ISA baseline, so this path needs no runtime check, only the AVX2 negative result.
/// - **`x86` (32-bit) with SSE2** (runtime-detected): same 16-byte kernel, but SSE2 is *not*
///   guaranteed on 32-bit x86 (pre-Pentium-4 lacks it), so it is runtime-detected; pre-SSE2
///   hosts fall through to the scalar kernel below.
/// - **`aarch64` little-endian**: 16-byte vectorized lanes via NEON (`ARMv8` baseline — no runtime check).
/// - **Everything else** (incl. big-endian aarch64, pre-SSE2 32-bit x86, riscv, powerpc): 8-byte word
///   stride via XOR. First-mismatch position uses `trailing_zeros() / 8` on little-endian
///   targets and `leading_zeros() / 8` on big-endian, so the byte ordering of the word matches
///   the byte ordering of the source slice on either endianness.
///
/// `is_x86_feature_detected!` caches the CPUID result, so the per-call dispatch
/// cost is one to three cached atomic loads on x86 (one for AVX-512 hosts; more for
/// narrower hosts that fail the wider checks before taking their lane).
#[must_use]
pub fn longest_shared_prefix_length(s1: &[u8], s2: &[u8]) -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512bw") {
            // SAFETY: AVX-512BW availability just verified via runtime CPU feature detection.
            return unsafe { lsp_avx512(s1, s2) };
        }
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 availability just verified via runtime CPU feature detection.
            return unsafe { lsp_avx2(s1, s2) };
        }
        // SAFETY: SSE2 is part of the mandatory x86_64 ISA baseline — every x86_64 CPU
        // supports it, so no runtime check is required. The `#[target_feature(enable = "sse2")]`
        // attribute on `lsp_sse2` documents this contract explicitly.
        return unsafe { lsp_sse2(s1, s2) };
    }
    #[cfg(target_arch = "x86")]
    {
        if std::is_x86_feature_detected!("avx512bw") {
            // SAFETY: AVX-512BW availability just verified via runtime CPU feature detection.
            return unsafe { lsp_avx512(s1, s2) };
        }
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 availability just verified via runtime CPU feature detection.
            return unsafe { lsp_avx2(s1, s2) };
        }
        if std::is_x86_feature_detected!("sse2") {
            // SAFETY: SSE2 availability just verified via runtime CPU feature detection.
            // Unlike x86_64, SSE2 is not a guaranteed baseline on 32-bit x86, so the
            // check is required; pre-SSE2 hosts fall through to the scalar tail below.
            return unsafe { lsp_sse2(s1, s2) };
        }
        // Pre-SSE2 32-bit x86 (i586 and earlier): fall through to the scalar kernel.
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
    // reachable on 32-bit x86 (pre-SSE2 fall-through) and every other target (BE aarch64,
    // riscv, powerpc, …), which is the whole point of having a portable fallback.
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
/// Compares 8 bytes at a time via `u64` XOR and locates the first mismatching byte
/// using an endian-aware bit-count:
/// - **Little-endian** (`target_endian = "little"`): `trailing_zeros() / 8` — the
///   lowest-numbered bit in the XOR word corresponds to the first source byte.
/// - **Big-endian** (`target_endian = "big"`): `leading_zeros() / 8` — the highest-numbered
///   bit in the XOR word corresponds to the first source byte.
///
/// Tail shorter than 8 bytes falls back to a byte-by-byte loop.
#[must_use]
pub(crate) fn lsp_scalar(s1: &[u8], s2: &[u8]) -> usize {
    let min_len = s1.len().min(s2.len());
    let mut i = 0;

    while i + 8 <= min_len {
        // SAFETY: i + 8 <= min_len <= s{1,2}.len() — both 8-byte reads are in-bounds.
        // `read_unaligned` documents that the pointer needs no alignment, so the
        // `*const u8 -> *const u64` cast is sound. Clippy's `cast_ptr_alignment`
        // does NOT fire here (verified across all CI targets including BE powerpc64)
        // because the cast feeds directly into `read_unaligned`, which clippy
        // recognises as an unaligned-load idiom.
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
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[target_feature(enable = "avx2")]
#[expect(unsafe_code, reason = "intrinsics require unsafe")]
#[must_use]
unsafe fn lsp_avx2(s1: &[u8], s2: &[u8]) -> usize {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{__m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8};
    #[cfg(target_arch = "x86_64")]
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

/// AVX-512BW implementation — 64 bytes per iteration via `_mm512_cmpeq_epi8_mask`.
///
/// The widest `x86_64` lane: one iteration consumes a full 64-byte cache line, so
/// keys that share a long prefix (time-series, tenant-prefixed, sorted UUIDs)
/// settle in half the iterations of the AVX2 kernel. `_mm512_cmpeq_epi8_mask`
/// folds the 64-lane byte comparison directly into a `__mmask64`, avoiding the
/// separate `movemask` step the AVX2/SSE2 kernels need.
///
/// # Safety
///
/// Caller must ensure the host CPU supports AVX-512BW
/// (`is_x86_feature_detected!("avx512bw")`). BW implies the F subset, so the
/// 512-bit load and the byte-granular compare-mask are both available.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
// List both ISA features the body relies on: `_mm512_loadu_si512` is AVX-512F,
// `_mm512_cmpeq_epi8_mask` is AVX-512BW. BW implies F (so `avx512bw` alone would
// compile), but naming both keeps the gate matching the actual requirements and
// guards against a future edit dropping the BW-only compare without noticing the
// F load is still gated. Runtime detection on `avx512bw` is sufficient because
// any CPU exposing BW necessarily implements F.
#[target_feature(enable = "avx512bw,avx512f")]
#[expect(unsafe_code, reason = "intrinsics require unsafe")]
#[must_use]
unsafe fn lsp_avx512(s1: &[u8], s2: &[u8]) -> usize {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{__m512i, _mm512_cmpeq_epi8_mask, _mm512_loadu_si512};
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{__m512i, _mm512_cmpeq_epi8_mask, _mm512_loadu_si512};

    let min_len = s1.len().min(s2.len());
    let mut i = 0;

    while i + 64 <= min_len {
        // SAFETY: i + 64 <= min_len ≤ s{1,2}.len() — both 64-byte loads are in-bounds.
        // `_mm512_loadu_si512` is the *unaligned* load, so the u8→__m512i pointer cast
        // does not require 64-byte alignment (the pointer is only used by `loadu`).
        #[expect(
            clippy::cast_ptr_alignment,
            reason = "_mm512_loadu_si512 explicitly performs an unaligned 64-byte load"
        )]
        let (va, vb) = unsafe {
            (
                _mm512_loadu_si512(s1.as_ptr().add(i).cast::<__m512i>()),
                _mm512_loadu_si512(s2.as_ptr().add(i).cast::<__m512i>()),
            )
        };
        // `_mm512_cmpeq_epi8_mask` yields a 64-bit mask: bit j is set iff byte j is
        // equal. A full-match lane is `u64::MAX`; the first mismatch is the lowest
        // zero bit of the mask, i.e. the lowest set bit of its complement.
        let mask = _mm512_cmpeq_epi8_mask(va, vb);
        if mask != u64::MAX {
            return i + (!mask).trailing_zeros() as usize;
        }
        i += 64;
    }

    // Tail: byte-stride (≤63 bytes left, not worth dispatching a narrower kernel).
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
/// VMs / containers, AMD pre-Excavator, low-power embedded `x86_64`) and on
/// 32-bit `x86` hosts with SSE2 but without AVX2.
///
/// # Safety
///
/// Caller must ensure the host supports SSE2. On `x86_64` this is the mandatory
/// ISA baseline (always true); on 32-bit `x86` it must be runtime-detected via
/// `is_x86_feature_detected!("sse2")` because pre-Pentium-4 CPUs lack it.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[target_feature(enable = "sse2")]
#[expect(unsafe_code, reason = "intrinsics require unsafe")]
#[must_use]
unsafe fn lsp_sse2(s1: &[u8], s2: &[u8]) -> usize {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{__m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8};
    #[cfg(target_arch = "x86_64")]
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
    fn lsp_scalar_on_boundaries_matches_reference() {
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
    fn longest_shared_prefix_length_on_boundaries_matches_reference() {
        // Same coverage as `lsp_scalar_on_boundaries_matches_reference`, but exercises
        // the *dispatched* path — on x86_64 this hits AVX2 when available and SSE2
        // otherwise, on little-endian aarch64 it hits NEON, and on other targets it
        // falls back to the scalar kernel.
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

    /// Shared boundary-walk used by every per-kernel test: for each `total_len`
    /// in the SIMD-stride-boundary set, flip the byte at `mismatch_at` (or none,
    /// at `mismatch_at == total_len`) and assert the kernel under test agrees
    /// with the byte-by-byte reference, both at full and asymmetric lengths.
    //
    // cfg-gated to mirror the union of its callers (SSE2/AVX2/AVX-512 on x86_64
    // and 32-bit x86, NEON on LE aarch64). On other targets (riscv64, powerpc64,
    // BE aarch64) no per-kernel test fires, so the helper would trip dead_code
    // without the cfg.
    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "x86",
        all(target_arch = "aarch64", target_endian = "little")
    ))]
    fn assert_kernel_matches_reference<F: Fn(&[u8], &[u8]) -> usize>(label: &str, kernel: F) {
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
                let want = lsp_reference(&a, &b);
                assert_eq!(
                    want,
                    kernel(&a, &b),
                    "{label} @ len={total_len} mismatch_at={mismatch_at}"
                );

                // Asymmetric: truncate `a` to `mismatch_at`, leave `b` at full length.
                a.truncate(mismatch_at);
                let want_short = lsp_reference(&a, &b);
                assert_eq!(
                    want_short,
                    kernel(&a, &b),
                    "{label} asym len={mismatch_at} vs {total_len}"
                );
            }
        }
    }

    /// Direct test for the SSE2 16-byte kernel. Required because CI `x86_64` runners
    /// have AVX2 → the dispatch path never hits `lsp_sse2` and coverage would be
    /// 0% even though all dispatched tests pass. Calling the kernel directly
    /// guarantees the SSE2-path exercise the boundary cases.
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    #[test]
    fn lsp_sse2_on_boundaries_matches_reference() {
        // On x86_64 SSE2 is the mandatory ISA baseline; on 32-bit x86 it is not
        // guaranteed (pre-Pentium-4 lacks it), so runtime-detect before calling.
        #[cfg(target_arch = "x86")]
        if !std::is_x86_feature_detected!("sse2") {
            return;
        }
        // SAFETY: SSE2 is the x86_64 baseline / runtime-verified on x86 above.
        assert_kernel_matches_reference("sse2", |a, b| unsafe { lsp_sse2(a, b) });
    }

    /// Direct test for the AVX2 32-byte kernel, gated by runtime CPU detection.
    /// Without this, AVX2 lines are only exercised via the dispatched path, which
    /// is fine for coverage on AVX2-capable runners — but the direct test makes
    /// the AVX2 contract explicit (matches reference on every boundary case).
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    #[test]
    fn lsp_avx2_on_boundaries_matches_reference() {
        if !std::is_x86_feature_detected!("avx2") {
            // No AVX2 on this host — nothing to verify directly.
            return;
        }
        // SAFETY: AVX2 availability just verified via runtime CPU feature detection.
        assert_kernel_matches_reference("avx2", |a, b| unsafe { lsp_avx2(a, b) });
    }

    /// Direct test for the AVX-512BW 64-byte kernel, gated by runtime CPU detection.
    /// CI `x86_64` runners may or may not expose AVX-512 (consumer Intel 11th gen+
    /// dropped it; AMD Zen4+ and Intel server keep it), so the dispatched path can't
    /// be relied on to reach `lsp_avx512`. This direct test exercises it whenever the
    /// host supports AVX-512BW and is a no-op otherwise.
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    #[test]
    fn lsp_avx512_on_boundaries_matches_reference() {
        if !std::is_x86_feature_detected!("avx512bw") {
            // No AVX-512BW on this host — nothing to verify directly.
            return;
        }
        // SAFETY: AVX-512BW availability just verified via runtime CPU feature detection.
        assert_kernel_matches_reference("avx512bw", |a, b| unsafe { lsp_avx512(a, b) });
    }

    /// Direct test for the NEON 16-byte kernel on LE aarch64. Required because
    /// CI codecov runs on `x86_64` Linux — the NEON kernel never executes there
    /// even when cross-compiling, but this test gates onto LE aarch64 runners
    /// (e.g. macos-latest on M1/M2) to keep the path covered.
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    #[test]
    fn lsp_neon_on_boundaries_matches_reference() {
        // SAFETY: NEON is mandatory in the ARMv8 baseline (`target_arch = "aarch64"`).
        assert_kernel_matches_reference("neon", |a, b| unsafe { lsp_neon(a, b) });
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
