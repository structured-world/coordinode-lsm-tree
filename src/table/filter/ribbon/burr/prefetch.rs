// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Read-prefetch hints for the BuRR query hot path.
//!
//! Each filter probe XOR-reduces a `[start, start + w)` window of coefficient
//! words, where `start` is hash-derived (effectively random per probe), so the
//! first access is a cold cache miss on essentially every check. Prefetching the
//! window before the preceding threshold work overlaps that miss with useful
//! computation. Mirrors the random-offset span prefetch RocksDB issues in its
//! ribbon filter (`util/ribbon_impl.h` `PrefetchSegmentRange`).
//!
//! A prefetch is a pure hint: it never faults (even on an invalid address),
//! never changes the result, and degrades to a no-op on targets without a
//! prefetch intrinsic, so the filter answer stays bit-identical on every path.
//! Architecture selection is by `cfg(target_arch)` (the target arch is fixed for
//! a given binary); it is NOT a runtime-detected ISA feature, so there is no
//! SIGILL risk: the x86_64 hint is SSE1, baseline on every x86_64 CPU.

// These are bare hardware-hint wrappers on the filter probe hot path: a real
// call would defeat the purpose, so force inlining.
#![expect(
    clippy::inline_always,
    reason = "bare prefetch-intrinsic wrappers on the probe hot path must inline"
)]

/// Issues a read-prefetch hint (to all cache levels) for the cache line holding
/// `ptr`. A no-op on targets without a prefetch intrinsic.
#[inline(always)]
pub(super) fn prefetch_read(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: `_mm_prefetch` is SSE1 (baseline on x86_64) and a pure hint;
        // the pointer need not be valid and the call cannot fault.
        unsafe {
            core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(ptr.cast());
        }
    }
    #[cfg(all(target_arch = "x86", target_feature = "sse"))]
    {
        // SAFETY: same as the x86_64 arm; SSE is compile-time enabled here.
        unsafe {
            core::arch::x86::_mm_prefetch::<{ core::arch::x86::_MM_HINT_T0 }>(ptr.cast());
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: `prfm` is a hint; an invalid address does not fault. `readonly`
        // + `nostack` + `preserves_flags` mark it as a pure memory-touch hint.
        unsafe {
            core::arch::asm!(
                "prfm pldl1keep, [{p}]",
                p = in(reg) ptr,
                options(nostack, preserves_flags, readonly),
            );
        }
    }
    #[cfg(not(any(
        target_arch = "x86_64",
        all(target_arch = "x86", target_feature = "sse"),
        target_arch = "aarch64",
    )))]
    {
        let _ = ptr;
    }
}

/// Prefetches the `byte_len`-byte span starting at `base`, one hint per 64-byte
/// cache line, to overlap the cold first-access miss on the hash-random
/// coefficient window. `base` may be computed with `wrapping_add` past the end
/// of its allocation: a prefetch of an out-of-range address is harmless.
#[inline(always)]
pub(super) fn prefetch_span(base: *const u8, byte_len: usize) {
    const CACHE_LINE: usize = 64;
    let mut off = 0;
    while off < byte_len {
        prefetch_read(base.wrapping_add(off));
        off += CACHE_LINE;
    }
}
