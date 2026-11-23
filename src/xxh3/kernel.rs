// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The stripe loop of xxh3's long inputs, in one kernel per instruction set,
//! chosen once per call from what the CPU runs.
//!
//! Every kernel computes the accumulators xxh3 defines, bit for bit: they
//! differ only in how many lanes one instruction covers. The scalar kernel is
//! the reference the others are tested against.

use super::{PRIME32_1, SECRET_LEN, STRIPE_LEN, STRIPES_PER_BLOCK};

/// The accumulators of a long input.
pub(super) type Acc = [u64; 8];

/// Where the scramble's secret starts: the last stripe of it.
const SCRAMBLE_AT: usize = SECRET_LEN - STRIPE_LEN;

/// One instruction set's kernel.
///
/// Every function takes raw pointers its caller keeps in bounds, as its
/// safety section says, and is `unsafe` because a SIMD kernel also needs its
/// instruction set present: [`Kernel::get`] hands out only a kernel whose
/// instruction set the CPU has.
#[derive(Clone, Copy)]
pub(super) struct Kernel {
    /// Accumulates `blocks` whole blocks of [`STRIPES_PER_BLOCK`] stripes
    /// from `input` into `acc`, scrambling after each.
    ///
    /// # Safety
    ///
    /// `input` is valid for `blocks * STRIPES_PER_BLOCK * STRIPE_LEN` bytes
    /// and `secret` for [`SECRET_LEN`].
    pub(super) blocks: unsafe fn(acc: &mut Acc, input: *const u8, blocks: usize, secret: *const u8),
    /// Accumulates `stripes` stripes from `input` into `acc`, the secret of
    /// each starting 8 bytes after the previous one's, the first at `secret`.
    ///
    /// # Safety
    ///
    /// `input` is valid for `stripes * STRIPE_LEN` bytes and `secret` for
    /// `(stripes - 1) * 8 + STRIPE_LEN` bytes (none when `stripes` is zero).
    pub(super) stripes:
        unsafe fn(acc: &mut Acc, input: *const u8, secret: *const u8, stripes: usize),
    /// Scrambles `acc` with the secret's last stripe.
    ///
    /// # Safety
    ///
    /// `secret` is valid for [`SECRET_LEN`] bytes.
    pub(super) scramble: unsafe fn(acc: &mut Acc, secret: *const u8),
}

impl Kernel {
    /// The scalar kernel, which every CPU runs: the one taken where no SIMD
    /// kernel exists, and the reference every SIMD kernel is tested against.
    #[cfg(any(
        test,
        not(any(
            target_arch = "x86_64",
            all(target_arch = "aarch64", target_endian = "little")
        ))
    ))]
    pub(super) const SCALAR: Self = Self {
        blocks: scalar::blocks,
        stripes: scalar::stripes,
        scramble: scalar::scramble,
    };

    /// The widest kernel this CPU runs.
    #[must_use]
    pub(super) fn get() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            // No AVX-512 kernel: the loop is bound by its multiplies, and
            // 512-bit ones lower the clock on the CPUs that have them.
            if cpu_avx2::get() {
                return x86::AVX2;
            }
            // SSE2 is part of every x86-64 CPU.
            x86::SSE2
        }
        #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
        {
            // NEON is part of every ARMv8-A CPU.
            neon::NEON
        }
        #[cfg(not(any(
            target_arch = "x86_64",
            all(target_arch = "aarch64", target_endian = "little")
        )))]
        {
            Self::SCALAR
        }
    }

    /// Every kernel this CPU runs, the scalar one first, for the tests that
    /// check each against it.
    #[cfg(test)]
    pub(super) fn available() -> alloc::vec::Vec<(&'static str, Self)> {
        #[cfg_attr(
            not(any(
                target_arch = "x86_64",
                all(target_arch = "aarch64", target_endian = "little")
            )),
            expect(unused_mut, reason = "only the scalar kernel on this target")
        )]
        let mut kernels = alloc::vec![("scalar", Self::SCALAR)];
        #[cfg(target_arch = "x86_64")]
        {
            kernels.push(("sse2", x86::SSE2));
            if cpu_avx2::get() {
                kernels.push(("avx2", x86::AVX2));
            }
        }
        #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
        kernels.push(("neon", neon::NEON));
        kernels
    }
}

// Cached once per process by `cpufeatures`, which also checks that the OS
// saves the AVX state, and works without `std`.
#[cfg(target_arch = "x86_64")]
cpufeatures::new!(cpu_avx2, "avx2");

/// The reference kernel: one 64-bit lane at a time.
#[cfg(any(
    test,
    not(any(
        target_arch = "x86_64",
        all(target_arch = "aarch64", target_endian = "little")
    ))
))]
mod scalar {
    use super::{Acc, PRIME32_1, SCRAMBLE_AT, STRIPE_LEN, STRIPES_PER_BLOCK};

    /// The little-endian `u64` at `at`.
    ///
    /// # Safety
    ///
    /// `at` is valid for 8 bytes.
    #[inline]
    unsafe fn read(at: *const u8) -> u64 {
        // SAFETY: the caller keeps `at` valid for 8 bytes; the read is
        // unaligned.
        u64::from_le(unsafe { at.cast::<u64>().read_unaligned() })
    }

    /// One stripe into `acc`.
    ///
    /// # Safety
    ///
    /// `input` and `secret` are valid for [`STRIPE_LEN`] bytes.
    #[inline]
    unsafe fn accumulate(acc: &mut Acc, input: *const u8, secret: *const u8) {
        // A lane adds its data to its pair's other lane and its keyed product
        // to itself.
        let product = |key: u64| (key & 0xFFFF_FFFF).wrapping_mul(key >> 32);
        for (pair, [even, odd]) in acc.as_chunks_mut::<2>().0.iter_mut().enumerate() {
            let at = pair * 16;
            // SAFETY: at + 16 <= STRIPE_LEN, within both by the caller.
            let (data_even, data_odd, key_even, key_odd) = unsafe {
                (
                    read(input.add(at)),
                    read(input.add(at + 8)),
                    read(secret.add(at)),
                    read(secret.add(at + 8)),
                )
            };
            *even = even
                .wrapping_add(data_odd)
                .wrapping_add(product(data_even ^ key_even));
            *odd = odd
                .wrapping_add(data_even)
                .wrapping_add(product(data_odd ^ key_odd));
        }
    }

    pub(super) unsafe fn stripes(
        acc: &mut Acc,
        input: *const u8,
        secret: *const u8,
        stripes: usize,
    ) {
        for stripe in 0..stripes {
            // SAFETY: within the bounds the caller keeps, by stripe.
            unsafe { accumulate(acc, input.add(stripe * STRIPE_LEN), secret.add(stripe * 8)) };
        }
    }

    pub(super) unsafe fn scramble(acc: &mut Acc, secret: *const u8) {
        for (lane, value) in acc.iter_mut().enumerate() {
            // SAFETY: SCRAMBLE_AT + 64 is the secret's length.
            let key = unsafe { read(secret.add(SCRAMBLE_AT + lane * 8)) };
            *value = (*value ^ (*value >> 47) ^ key).wrapping_mul(u64::from(PRIME32_1));
        }
    }

    pub(super) unsafe fn blocks(acc: &mut Acc, input: *const u8, blocks: usize, secret: *const u8) {
        for block in 0..blocks {
            // SAFETY: within the bounds the caller keeps, by block.
            unsafe {
                stripes(
                    acc,
                    input.add(block * STRIPES_PER_BLOCK * STRIPE_LEN),
                    secret,
                    STRIPES_PER_BLOCK,
                );
                scramble(acc, secret);
            }
        }
    }
}

/// The x86-64 kernels: SSE2 (16 bytes a lane) and AVX2 (32).
#[cfg(target_arch = "x86_64")]
mod x86 {
    use super::{Acc, Kernel, PRIME32_1, SCRAMBLE_AT, STRIPE_LEN, STRIPES_PER_BLOCK};
    use core::arch::x86_64::{
        __m128i, __m256i, _mm_add_epi64, _mm_loadu_si128, _mm_mul_epu32, _mm_set1_epi32,
        _mm_shuffle_epi32, _mm_slli_epi64, _mm_srli_epi64, _mm_storeu_si128, _mm_xor_si128,
        _mm256_add_epi64, _mm256_loadu_si256, _mm256_mul_epu32, _mm256_set1_epi32,
        _mm256_shuffle_epi32, _mm256_slli_epi64, _mm256_srli_epi64, _mm256_storeu_si256,
        _mm256_xor_si256,
    };

    /// Swaps the two 64-bit halves of every 128-bit lane: the data a lane
    /// adds to its neighbour's accumulator.
    const SWAP: i32 = 0b0100_1110;

    /// Expands one ISA's three kernel functions. `$width` is the bytes one
    /// vector covers, `$load`/`$store` move a vector, and the rest are that
    /// ISA's 64-bit add, 32x32 multiply, xor, shifts, dword shuffle and
    /// broadcast. The stripe and scramble bodies are written once here and
    /// expanded into each `#[target_feature]` function, which keeps them
    /// inlined where `#[inline(always)]` may not join `#[target_feature]`.
    macro_rules! simd_kernel {
        (
            $name:ident, $feature:literal, $vec:ty, $width:literal,
            $load:ident, $store:ident, $add:ident, $mul:ident, $xor:ident,
            $srli:ident, $slli:ident, $shuffle:ident, $set1:ident
        ) => {
            pub(super) mod $name {
                use super::*;

                const LANES: usize = STRIPE_LEN / $width;

                // The accumulators as a vector per lane, and back: macros, so
                // the intrinsics expand inside the `#[target_feature]`
                // functions that enable them.
                macro_rules! load_acc {
                    ($acc:expr) => {{
                        let acc: &Acc = $acc;
                        // SAFETY: an all-zero vector is a valid integer vector.
                        let mut lanes: [$vec; LANES] = unsafe { core::mem::zeroed() };
                        for (lane, slot) in lanes.iter_mut().enumerate() {
                            // SAFETY: `acc` is 64 bytes, `LANES` vectors; the
                            // loads are unaligned.
                            *slot = unsafe {
                                $load(acc.as_ptr().cast::<u8>().add(lane * $width).cast())
                            };
                        }
                        lanes
                    }};
                }

                macro_rules! store_acc {
                    ($acc:expr, $lanes:expr) => {{
                        let acc: &mut Acc = $acc;
                        for (lane, vector) in $lanes.iter().enumerate() {
                            // SAFETY: as in `load_acc`, for the stores.
                            unsafe {
                                $store(
                                    acc.as_mut_ptr().cast::<u8>().add(lane * $width).cast(),
                                    *vector,
                                )
                            };
                        }
                    }};
                }

                macro_rules! accumulate {
                    ($lanes:ident, $input:expr, $secret:expr) => {
                        for (lane, slot) in $lanes.iter_mut().enumerate() {
                            // SAFETY: the stripe and its secret are 64 bytes from
                            // `$input` and `$secret`, within the caller's bounds.
                            let (data, key) = unsafe {
                                (
                                    $load($input.add(lane * $width).cast()),
                                    $load($secret.add(lane * $width).cast()),
                                )
                            };
                            let key = $xor(data, key);
                            let product = $mul(key, $srli::<32>(key));
                            let swapped = $shuffle::<SWAP>(data);
                            *slot = $add(*slot, $add(product, swapped));
                        }
                    };
                }

                macro_rules! scramble {
                    ($lanes:ident, $secret:expr) => {
                        let prime = $set1(PRIME32_1.cast_signed());
                        for (lane, slot) in $lanes.iter_mut().enumerate() {
                            // SAFETY: the scramble's secret is the last 64 bytes of
                            // the secret, within the caller's bounds.
                            let key =
                                unsafe { $load($secret.add(SCRAMBLE_AT + lane * $width).cast()) };
                            let mixed = $xor($xor(*slot, $srli::<47>(*slot)), key);
                            let low = $mul(mixed, prime);
                            let high = $mul($srli::<32>(mixed), prime);
                            *slot = $add(low, $slli::<32>(high));
                        }
                    };
                }

                /// # Safety
                ///
                /// As [`Kernel::stripes`], and the CPU runs this instruction set.
                #[target_feature(enable = $feature)]
                pub(in super::super) unsafe fn stripes(
                    acc: &mut Acc,
                    input: *const u8,
                    secret: *const u8,
                    stripes: usize,
                ) {
                    let mut lanes = load_acc!(acc);
                    for stripe in 0..stripes {
                        // SAFETY: within the caller's bounds, by stripe.
                        let (data, key) =
                            unsafe { (input.add(stripe * STRIPE_LEN), secret.add(stripe * 8)) };
                        accumulate!(lanes, data, key);
                    }
                    store_acc!(acc, lanes);
                }

                /// # Safety
                ///
                /// As [`Kernel::scramble`], and the CPU runs this instruction set.
                #[target_feature(enable = $feature)]
                pub(in super::super) unsafe fn scramble(acc: &mut Acc, secret: *const u8) {
                    let mut lanes = load_acc!(acc);
                    scramble!(lanes, secret);
                    store_acc!(acc, lanes);
                }

                /// # Safety
                ///
                /// As [`Kernel::blocks`], and the CPU runs this instruction set.
                #[target_feature(enable = $feature)]
                pub(in super::super) unsafe fn blocks(
                    acc: &mut Acc,
                    input: *const u8,
                    blocks: usize,
                    secret: *const u8,
                ) {
                    // The accumulators stay in registers for the whole run.
                    let mut lanes = load_acc!(acc);
                    for block in 0..blocks {
                        // SAFETY: within the caller's bounds, by block.
                        let block = unsafe { input.add(block * STRIPES_PER_BLOCK * STRIPE_LEN) };
                        for stripe in 0..STRIPES_PER_BLOCK {
                            // SAFETY: within the block and the secret's first 128 bytes.
                            let (data, key) =
                                unsafe { (block.add(stripe * STRIPE_LEN), secret.add(stripe * 8)) };
                            accumulate!(lanes, data, key);
                        }
                        scramble!(lanes, secret);
                    }
                    store_acc!(acc, lanes);
                }
            }
        };
    }

    simd_kernel!(
        sse2,
        "sse2",
        __m128i,
        16,
        _mm_loadu_si128,
        _mm_storeu_si128,
        _mm_add_epi64,
        _mm_mul_epu32,
        _mm_xor_si128,
        _mm_srli_epi64,
        _mm_slli_epi64,
        _mm_shuffle_epi32,
        _mm_set1_epi32
    );
    simd_kernel!(
        avx2,
        "avx2",
        __m256i,
        32,
        _mm256_loadu_si256,
        _mm256_storeu_si256,
        _mm256_add_epi64,
        _mm256_mul_epu32,
        _mm256_xor_si256,
        _mm256_srli_epi64,
        _mm256_slli_epi64,
        _mm256_shuffle_epi32,
        _mm256_set1_epi32
    );
    pub(super) const SSE2: Kernel = Kernel {
        blocks: sse2::blocks,
        stripes: sse2::stripes,
        scramble: sse2::scramble,
    };
    pub(super) const AVX2: Kernel = Kernel {
        blocks: avx2::blocks,
        stripes: avx2::stripes,
        scramble: avx2::scramble,
    };
}

/// The NEON kernel: 16 bytes a lane.
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
mod neon {
    use super::{Acc, Kernel, PRIME32_1, SCRAMBLE_AT, STRIPE_LEN, STRIPES_PER_BLOCK};
    use core::arch::aarch64::{
        uint64x2_t, vaddq_u64, veorq_u64, vextq_u64, vld1q_u8, vld1q_u64, vmovn_u64, vmull_n_u32,
        vmull_u32, vreinterpretq_u64_u8, vshlq_n_u64, vshrn_n_u64, vshrq_n_u64, vst1q_u64,
    };

    const LANES: usize = 4;

    // The accumulators as a vector per lane, and back: macros, so the
    // intrinsics expand inside the `#[target_feature]` functions.
    macro_rules! load_acc {
        ($acc:expr) => {{
            let acc: &Acc = $acc;
            // SAFETY: `acc` is 64 bytes, four vectors of two `u64`.
            let lanes: [uint64x2_t; LANES] = unsafe {
                [
                    vld1q_u64(acc.as_ptr()),
                    vld1q_u64(acc.as_ptr().add(2)),
                    vld1q_u64(acc.as_ptr().add(4)),
                    vld1q_u64(acc.as_ptr().add(6)),
                ]
            };
            lanes
        }};
    }

    macro_rules! store_acc {
        ($acc:expr, $lanes:expr) => {{
            let acc: &mut Acc = $acc;
            for (lane, vector) in $lanes.iter().enumerate() {
                // SAFETY: as in `load_acc`, for the stores.
                unsafe { vst1q_u64(acc.as_mut_ptr().add(lane * 2), *vector) };
            }
        }};
    }

    /// Loads 16 bytes at `at`, of any alignment, as two little-endian `u64`
    /// on this little-endian target.
    macro_rules! load {
        ($at:expr) => {
            vreinterpretq_u64_u8(vld1q_u8($at))
        };
    }

    macro_rules! accumulate {
        ($lanes:ident, $input:expr, $secret:expr) => {
            for (lane, slot) in $lanes.iter_mut().enumerate() {
                // SAFETY: the stripe and its secret are 64 bytes from `$input`
                // and `$secret`, within the caller's bounds.
                let (data, key) =
                    unsafe { (load!($input.add(lane * 16)), load!($secret.add(lane * 16))) };
                let key = veorq_u64(data, key);
                let product = vmull_u32(vmovn_u64(key), vshrn_n_u64::<32>(key));
                let swapped = vextq_u64::<1>(data, data);
                *slot = vaddq_u64(*slot, vaddq_u64(product, swapped));
            }
        };
    }

    macro_rules! scramble {
        ($lanes:ident, $secret:expr) => {
            for (lane, slot) in $lanes.iter_mut().enumerate() {
                // SAFETY: the scramble's secret is the last 64 bytes of the
                // secret, within the caller's bounds.
                let key = unsafe { load!($secret.add(SCRAMBLE_AT + lane * 16)) };
                let mixed = veorq_u64(veorq_u64(*slot, vshrq_n_u64::<47>(*slot)), key);
                let low = vmull_n_u32(vmovn_u64(mixed), PRIME32_1);
                let high = vmull_n_u32(vshrn_n_u64::<32>(mixed), PRIME32_1);
                *slot = vaddq_u64(low, vshlq_n_u64::<32>(high));
            }
        };
    }

    /// # Safety
    ///
    /// As [`Kernel::stripes`].
    #[target_feature(enable = "neon")]
    unsafe fn stripes(acc: &mut Acc, input: *const u8, secret: *const u8, stripes: usize) {
        let mut lanes = load_acc!(acc);
        for stripe in 0..stripes {
            // SAFETY: within the caller's bounds, by stripe.
            let (data, key) = unsafe { (input.add(stripe * STRIPE_LEN), secret.add(stripe * 8)) };
            accumulate!(lanes, data, key);
        }
        store_acc!(acc, lanes);
    }

    /// # Safety
    ///
    /// As [`Kernel::scramble`].
    #[target_feature(enable = "neon")]
    unsafe fn scramble(acc: &mut Acc, secret: *const u8) {
        let mut lanes = load_acc!(acc);
        scramble!(lanes, secret);
        store_acc!(acc, lanes);
    }

    /// # Safety
    ///
    /// As [`Kernel::blocks`].
    #[target_feature(enable = "neon")]
    unsafe fn blocks(acc: &mut Acc, input: *const u8, blocks: usize, secret: *const u8) {
        let mut lanes = load_acc!(acc);
        for block in 0..blocks {
            // SAFETY: within the caller's bounds, by block.
            let block = unsafe { input.add(block * STRIPES_PER_BLOCK * STRIPE_LEN) };
            for stripe in 0..STRIPES_PER_BLOCK {
                // SAFETY: within the block and the secret's first 128 bytes.
                let (data, key) =
                    unsafe { (block.add(stripe * STRIPE_LEN), secret.add(stripe * 8)) };
                accumulate!(lanes, data, key);
            }
            scramble!(lanes, secret);
        }
        store_acc!(acc, lanes);
    }

    pub(super) const NEON: Kernel = Kernel {
        blocks,
        stripes,
        scramble,
    };
}
