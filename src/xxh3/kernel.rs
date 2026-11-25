// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The stripe loop of xxh3's long inputs, in one kernel per instruction set.
//!
//! Every kernel computes the accumulators xxh3 defines, bit for bit: they
//! differ only in how many lanes one instruction covers. The scalar kernel is
//! the reference the others are tested against. A kernel's entry points each
//! run a whole pass over their input, compiled with its instruction set and
//! the stripe, block and scramble steps inlined in it. The target's baseline
//! kernel ([`baseline`]) is called directly; on x86-64 a wider one, found once
//! per process, is reached through `Kernel::wide`.

use super::{LAST_STRIPE_BACK, PRIME32_1, SECRET_LEN, STRIPE_LEN, STRIPES_PER_BLOCK};

/// The accumulators of a long input.
pub(super) type Acc = [u64; 8];

/// Where the scramble's secret starts: the last stripe of it.
const SCRAMBLE_AT: usize = SECRET_LEN - STRIPE_LEN;

/// One instruction set's kernel.
///
/// Every function takes raw pointers its caller keeps in bounds, as its
/// safety section says, and is `unsafe` because a SIMD kernel also needs its
/// instruction set present: `Kernel::wide` hands out only a kernel whose
/// instruction set the CPU has.
#[cfg(any(test, xxh3_sse2))]
#[derive(Clone, Copy)]
pub(super) struct Kernel {
    /// Accumulates `stripes` stripes from `input` into `acc`, `done` stripes
    /// of the current block already in it, scrambling at each block's end,
    /// and returns how many of the block the last one leaves done.
    ///
    /// # Safety
    ///
    /// `input` is valid for `stripes * STRIPE_LEN` bytes, `secret` for
    /// [`SECRET_LEN`], and `done < STRIPES_PER_BLOCK`.
    pub(super) consume: unsafe fn(
        acc: &mut Acc,
        input: *const u8,
        stripes: usize,
        done: usize,
        secret: *const u8,
    ) -> usize,
    /// Accumulates the whole of `input`, `len` bytes, longer than one
    /// stripe, into `acc`: every stripe but the last whole one, then the
    /// last 64 bytes under their own secret.
    ///
    /// # Safety
    ///
    /// `input` is valid for `len >= STRIPE_LEN` bytes and `secret` for
    /// [`SECRET_LEN`].
    pub(super) long: unsafe fn(acc: &mut Acc, input: *const u8, len: usize, secret: *const u8),
    /// Accumulates the 64 bytes at `stripe` into `acc` as an input's last.
    ///
    /// # Safety
    ///
    /// `stripe` is valid for [`STRIPE_LEN`] bytes and `secret` for
    /// [`SECRET_LEN`]. A stream's last stripe is short, so the baseline
    /// kernel takes it; the tests check every kernel's.
    #[cfg(test)]
    pub(super) last: unsafe fn(acc: &mut Acc, stripe: *const u8, secret: *const u8),
}

/// A kernel's entry points over its module's `stripes`, `scramble` and
/// `blocks`, with `$attr` (its `#[target_feature]`) on each, so those steps
/// inline into them.
macro_rules! entry_points {
    ($(#[$attr:meta])*) => {
        /// # Safety
        ///
        /// As `Kernel::consume`, and the CPU runs this instruction set.
        $(#[$attr])*
        pub(in super::super) unsafe fn consume(
            acc: &mut Acc,
            input: *const u8,
            stripes_n: usize,
            done: usize,
            secret: *const u8,
        ) -> usize {
            debug_assert!(done < STRIPES_PER_BLOCK);
            // SAFETY: the caller keeps `input` valid for `stripes_n` stripes
            // and `secret` for its 192 bytes. A stripe's secret starts at
            // most 8 * 15 bytes in, and its 64 bytes end within them.
            unsafe {
                if done + stripes_n < STRIPES_PER_BLOCK {
                    stripes(acc, input, secret.add(done * 8), stripes_n);
                    return done + stripes_n;
                }
                let to_end = STRIPES_PER_BLOCK - done;
                let whole = (stripes_n - to_end) / STRIPES_PER_BLOCK;
                let rest = stripes_n - to_end - whole * STRIPES_PER_BLOCK;
                stripes(acc, input, secret.add(done * 8), to_end);
                scramble(acc, secret);
                let input = input.add(to_end * STRIPE_LEN);
                blocks(acc, input, whole, secret);
                let input = input.add(whole * STRIPES_PER_BLOCK * STRIPE_LEN);
                stripes(acc, input, secret, rest);
                rest
            }
        }

        /// # Safety
        ///
        /// As `Kernel::last`, and the CPU runs this instruction set.
        $(#[$attr])*
        pub(in super::super) unsafe fn last(acc: &mut Acc, stripe: *const u8, secret: *const u8) {
            // SAFETY: the last stripe's secret, 64 bytes from
            // SECRET_LEN - 64 - 7, lies within the secret.
            unsafe {
                stripes(
                    acc,
                    stripe,
                    secret.add(SECRET_LEN - STRIPE_LEN - LAST_STRIPE_BACK),
                    1,
                )
            };
        }

    };
}

/// The scalar kernel's whole digests of a long input over its `$long`, as
/// `register_long!` gives a SIMD kernel's.
#[cfg(not(any(xxh3_sse2, xxh3_neon)))]
macro_rules! register_digest {
    ($long:path, $(#[$attr:meta])*) => {
        /// The xxh3-64 of the `len > MID_SIZE_MAX` bytes at `input` under
        /// `seed`.
        ///
        /// # Safety
        ///
        /// `input` is valid for `len` bytes, and the CPU runs this
        /// instruction set.
        #[inline]
        $(#[$attr])*
        pub(in super::super) unsafe fn digest64(input: *const u8, len: usize, seed: u64) -> u64 {
            let derived;
            let secret = if seed == 0 {
                &crate::xxh3::DEFAULT_SECRET
            } else {
                derived = crate::xxh3::secret_for(seed);
                &derived
            };
            let mut acc = crate::xxh3::INITIAL_ACC;
            // SAFETY: as the caller keeps; the secret holds its 192 bytes.
            unsafe { $long(&mut acc, input, len, secret.as_ptr()) };
            crate::xxh3::merge64(&acc, secret, len as u64)
        }

        /// The xxh3-128 of the `len > MID_SIZE_MAX` bytes at `input` under
        /// `seed`.
        ///
        /// # Safety
        ///
        /// As `digest64`.
        #[inline]
        $(#[$attr])*
        pub(in super::super) unsafe fn digest128(input: *const u8, len: usize, seed: u64) -> u128 {
            let derived;
            let secret = if seed == 0 {
                &crate::xxh3::DEFAULT_SECRET
            } else {
                derived = crate::xxh3::secret_for(seed);
                &derived
            };
            let mut acc = crate::xxh3::INITIAL_ACC;
            // SAFETY: as the caller keeps; the secret holds its 192 bytes.
            unsafe { $long(&mut acc, input, len, secret.as_ptr()) };
            crate::xxh3::merge128(&acc, secret, len as u64)
        }
    };
}

/// A SIMD kernel's pass over a whole long input, `$len >= STRIPE_LEN` bytes
/// at `$input`, into `$lanes` under `$secret`, over its module's
/// `accumulate!` and `scramble!`: every stripe in vectors, the last one too.
#[cfg(any(xxh3_sse2, xxh3_neon))]
macro_rules! long_loop {
    ($lanes:ident, $input:ident, $len:ident, $secret:ident) => {{
        debug_assert!($len >= STRIPE_LEN);
        let stripes_n = ($len - 1) / STRIPE_LEN;
        let whole = stripes_n / STRIPES_PER_BLOCK;
        // SAFETY (every `add` below): every stripe but the last whole one
        // lies within `$len`, whole blocks first, and the last one is its
        // final 64 bytes; a stripe's secret starts at most 8 * 15 bytes in and
        // the last one's at SECRET_LEN - 64 - 7, all within the secret. The
        // macros read 64 bytes at each.
        for block in 0..whole {
            let at = unsafe { $input.add(block * STRIPES_PER_BLOCK * STRIPE_LEN) };
            // Four stripes a turn: the block's 16 in four, the loop's
            // counter and branch a quarter as often.
            for quad in 0..STRIPES_PER_BLOCK / 4 {
                let stripe = quad * 4;
                let (data, key) = unsafe { (at.add(stripe * STRIPE_LEN), $secret.add(stripe * 8)) };
                accumulate!($lanes, data, key);
                let (data, key) = unsafe { (data.add(STRIPE_LEN), key.add(8)) };
                accumulate!($lanes, data, key);
                let (data, key) = unsafe { (data.add(STRIPE_LEN), key.add(8)) };
                accumulate!($lanes, data, key);
                let (data, key) = unsafe { (data.add(STRIPE_LEN), key.add(8)) };
                accumulate!($lanes, data, key);
            }
            scramble!($lanes, $secret);
        }
        let at = unsafe { $input.add(whole * STRIPES_PER_BLOCK * STRIPE_LEN) };
        for stripe in 0..stripes_n - whole * STRIPES_PER_BLOCK {
            let (data, key) = unsafe { (at.add(stripe * STRIPE_LEN), $secret.add(stripe * 8)) };
            accumulate!($lanes, data, key);
        }
        let (data, key) = unsafe {
            (
                $input.add($len - STRIPE_LEN),
                $secret.add(SECRET_LEN - STRIPE_LEN - LAST_STRIPE_BACK),
            )
        };
        accumulate!($lanes, data, key);
    }};
}

/// A SIMD kernel's `long` and its whole digests of a long input, with `$attr`
/// (its `#[target_feature]`) on them: one pass with the accumulators held in
/// vectors from the first stripe to the last, and in a digest the secret and
/// the merge in the same function, which a `#[target_feature]` loop does not
/// inline into otherwise.
#[cfg(any(xxh3_sse2, xxh3_neon))]
macro_rules! register_long {
    ($(#[$attr:meta])*) => {
        /// # Safety
        ///
        /// As `Kernel::long`, and the CPU runs this instruction set.
        #[cfg(any(test, xxh3_sse2))]
        #[inline]
        $(#[$attr])*
        pub(in super::super) unsafe fn long(
            acc: &mut Acc,
            input: *const u8,
            len: usize,
            secret: *const u8,
        ) {
            let mut lanes = load_acc!(acc);
            long_loop!(lanes, input, len, secret);
            store_acc!(acc, lanes);
        }

        /// The xxh3-64 of the `len > MID_SIZE_MAX` bytes at `input` under
        /// `seed`.
        ///
        /// # Safety
        ///
        /// `input` is valid for `len` bytes, and the CPU runs this
        /// instruction set.
        #[inline]
        $(#[$attr])*
        pub(in super::super) unsafe fn digest64(input: *const u8, len: usize, seed: u64) -> u64 {
            let derived;
            let secret_bytes = if seed == 0 {
                &crate::xxh3::DEFAULT_SECRET
            } else {
                derived = crate::xxh3::secret_for(seed);
                &derived
            };
            let secret = secret_bytes.as_ptr();
            let mut acc = crate::xxh3::INITIAL_ACC;
            let mut lanes = load_acc!(&acc);
            long_loop!(lanes, input, len, secret);
            store_acc!(&mut acc, lanes);
            crate::xxh3::merge64(&acc, secret_bytes, len as u64)
        }

        /// The xxh3-128 of the `len > MID_SIZE_MAX` bytes at `input` under
        /// `seed`.
        ///
        /// # Safety
        ///
        /// As `digest64`.
        #[inline]
        $(#[$attr])*
        pub(in super::super) unsafe fn digest128(input: *const u8, len: usize, seed: u64) -> u128 {
            let derived;
            let secret_bytes = if seed == 0 {
                &crate::xxh3::DEFAULT_SECRET
            } else {
                derived = crate::xxh3::secret_for(seed);
                &derived
            };
            let secret = secret_bytes.as_ptr();
            let mut acc = crate::xxh3::INITIAL_ACC;
            let mut lanes = load_acc!(&acc);
            long_loop!(lanes, input, len, secret);
            store_acc!(&mut acc, lanes);
            crate::xxh3::merge128(&acc, secret_bytes, len as u64)
        }
    };
}

#[cfg(any(test, xxh3_sse2))]
impl Kernel {
    /// The scalar kernel, the reference every SIMD kernel is tested against.
    #[cfg(test)]
    pub(super) const SCALAR: Self = Self {
        consume: scalar::consume,
        long: scalar::long,
        last: scalar::last,
    };

    /// The widest kernel this CPU runs, found on the first call and kept.
    #[cfg(xxh3_sse2)]
    #[must_use]
    #[inline]
    pub(super) fn wide() -> Self {
        x86::active()
    }

    /// Every kernel this CPU runs, the scalar one first, for the tests that
    /// check each against it.
    #[cfg(test)]
    pub(super) fn available() -> alloc::vec::Vec<(&'static str, Self)> {
        #[cfg_attr(
            not(any(xxh3_sse2, xxh3_neon)),
            expect(unused_mut, reason = "only the scalar kernel on this target")
        )]
        let mut kernels = alloc::vec![("scalar", Self::SCALAR)];
        #[cfg(xxh3_sse2)]
        {
            kernels.push(("sse2", x86::SSE2));
            if cpu_avx2::get() {
                kernels.push(("avx2", x86::AVX2));
            }
        }
        #[cfg(xxh3_neon)]
        kernels.push(("neon", neon::NEON));
        kernels
    }
}

/// The kernel every CPU of the target runs, called directly so it inlines
/// into the hash: SSE2 on x86-64 and NEON on little-endian `AArch64`, both in
/// their architecture's baseline, where the target's ABI carries the vector
/// registers (`xxh3_sse2`, `xxh3_neon`, set by the build script); the scalar
/// one elsewhere, a soft-float target included.
pub(super) mod baseline {
    use super::Acc;

    #[cfg(xxh3_sse2)]
    use super::{x86::sse2 as chosen, x86::sse2 as digests};

    #[cfg(xxh3_neon)]
    use super::{neon as chosen, neon as digests};

    #[cfg(not(any(xxh3_sse2, xxh3_neon)))]
    use super::{scalar as chosen, scalar as digests};

    /// The xxh3-64 of the `len > MID_SIZE_MAX` bytes at `input` under
    /// `seed`, loop and merge in one function.
    ///
    /// # Safety
    ///
    /// `input` is valid for `len` bytes.
    #[inline]
    pub(in super::super) unsafe fn digest64(input: *const u8, len: usize, seed: u64) -> u64 {
        // SAFETY: as the caller keeps; every CPU of the target runs it.
        unsafe { digests::digest64(input, len, seed) }
    }

    /// The xxh3-128 of the `len > MID_SIZE_MAX` bytes at `input` under
    /// `seed`, loop and merge in one function.
    ///
    /// # Safety
    ///
    /// `input` is valid for `len` bytes.
    #[inline]
    pub(in super::super) unsafe fn digest128(input: *const u8, len: usize, seed: u64) -> u128 {
        // SAFETY: as the caller keeps; every CPU of the target runs it.
        unsafe { digests::digest128(input, len, seed) }
    }

    /// As [`super::Kernel::consume`].
    ///
    /// # Safety
    ///
    /// As [`super::Kernel::consume`]; the instruction set is the target's.
    #[inline]
    pub(in super::super) unsafe fn consume(
        acc: &mut Acc,
        input: *const u8,
        stripes: usize,
        done: usize,
        secret: *const u8,
    ) -> usize {
        // SAFETY: as the caller keeps; every CPU of the target runs it.
        unsafe { chosen::consume(acc, input, stripes, done, secret) }
    }

    /// As [`super::Kernel::last`].
    ///
    /// # Safety
    ///
    /// As [`super::Kernel::last`]; the instruction set is the target's.
    #[inline]
    pub(in super::super) unsafe fn last(acc: &mut Acc, stripe: *const u8, secret: *const u8) {
        // SAFETY: as the caller keeps; every CPU of the target runs it.
        unsafe { chosen::last(acc, stripe, secret) }
    }
}

// Cached once per process by `cpufeatures`, which also checks that the OS
// saves the AVX state, and works without `std`.
#[cfg(xxh3_sse2)]
cpufeatures::new!(cpu_avx2, "avx2");

/// The reference kernel: one 64-bit lane at a time.
#[cfg(any(test, not(any(xxh3_sse2, xxh3_neon))))]
mod scalar {
    use super::{
        Acc, LAST_STRIPE_BACK, PRIME32_1, SCRAMBLE_AT, SECRET_LEN, STRIPE_LEN, STRIPES_PER_BLOCK,
    };

    entry_points!();

    #[cfg(not(any(xxh3_sse2, xxh3_neon)))]
    register_digest!(long,);

    /// # Safety
    ///
    /// As `Kernel::long`.
    pub(in super::super) unsafe fn long(
        acc: &mut Acc,
        input: *const u8,
        len: usize,
        secret: *const u8,
    ) {
        debug_assert!(len >= STRIPE_LEN);
        // SAFETY: every stripe but the last whole one lies within `len`, and
        // the last one is its final 64 bytes.
        unsafe {
            consume(acc, input, (len - 1) / STRIPE_LEN, 0, secret);
            last(acc, input.add(len - STRIPE_LEN), secret);
        }
    }

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

    #[inline]
    unsafe fn stripes(acc: &mut Acc, input: *const u8, secret: *const u8, stripes: usize) {
        for stripe in 0..stripes {
            // SAFETY: within the bounds the caller keeps, by stripe.
            unsafe { accumulate(acc, input.add(stripe * STRIPE_LEN), secret.add(stripe * 8)) };
        }
    }

    #[inline]
    unsafe fn scramble(acc: &mut Acc, secret: *const u8) {
        for (lane, value) in acc.iter_mut().enumerate() {
            // SAFETY: SCRAMBLE_AT + 64 is the secret's length.
            let key = unsafe { read(secret.add(SCRAMBLE_AT + lane * 8)) };
            *value = (*value ^ (*value >> 47) ^ key).wrapping_mul(u64::from(PRIME32_1));
        }
    }

    #[inline]
    unsafe fn blocks(acc: &mut Acc, input: *const u8, blocks: usize, secret: *const u8) {
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
#[cfg(xxh3_sse2)]
mod x86 {
    use super::{
        Acc, Kernel, LAST_STRIPE_BACK, PRIME32_1, SCRAMBLE_AT, SECRET_LEN, STRIPE_LEN,
        STRIPES_PER_BLOCK,
    };
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
    /// broadcast; `$fattr` is the `#[target_feature]` every function carries,
    /// which its intrinsics need even where the build enables the set. The
    /// stripe and scramble bodies are written once here and expanded into
    /// each function, which keeps them inlined where `#[inline(always)]` may
    /// not join `#[target_feature]`.
    macro_rules! simd_kernel {
        (
            $(#[$mod_attr:meta])*
            $name:ident, [$(#[$fattr:meta])*], $vec:ty, $width:literal,
            $load:ident, $store:ident, $add:ident, $mul:ident, $xor:ident,
            $srli:ident, $slli:ident, $shuffle:ident, $set1:ident
        ) => {
            $(#[$mod_attr])*
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

                entry_points!($(#[$fattr])*);
                register_long!($(#[$fattr])*);

                /// Accumulates `stripes` stripes from `input` into `acc`, the
                /// secret of each starting 8 bytes after the previous one's.
                ///
                /// # Safety
                ///
                /// `input` is valid for `stripes * STRIPE_LEN` bytes, `secret`
                /// for `(stripes - 1) * 8 + STRIPE_LEN`, and the CPU runs this
                /// instruction set.
                #[inline]
                $(#[$fattr])*
                unsafe fn stripes(
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

                /// Scrambles `acc` with the secret's last stripe.
                ///
                /// # Safety
                ///
                /// `secret` is valid for [`SECRET_LEN`] bytes, and the CPU runs
                /// this instruction set.
                #[inline]
                $(#[$fattr])*
                unsafe fn scramble(acc: &mut Acc, secret: *const u8) {
                    let mut lanes = load_acc!(acc);
                    scramble!(lanes, secret);
                    store_acc!(acc, lanes);
                }

                /// Accumulates `blocks` whole blocks from `input` into `acc`,
                /// scrambling after each.
                ///
                /// # Safety
                ///
                /// `input` is valid for `blocks * STRIPES_PER_BLOCK *
                /// STRIPE_LEN` bytes, `secret` for [`SECRET_LEN`], and the CPU
                /// runs this instruction set.
                #[inline]
                $(#[$fattr])*
                unsafe fn blocks(
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
        [#[target_feature(enable = "sse2")]],
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
        #[expect(
            dead_code,
            reason = "the wide kernel takes long runs only: short inputs, their digests and a \
                      stream's last stripe go to the baseline one"
        )]
        avx2,
        [#[target_feature(enable = "avx2")]],
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
    pub(super) static SSE2: Kernel = Kernel {
        consume: sse2::consume,
        long: sse2::long,
        #[cfg(test)]
        last: sse2::last,
    };
    pub(super) static AVX2: Kernel = Kernel {
        consume: avx2::consume,
        long: avx2::long,
        #[cfg(test)]
        last: avx2::last,
    };

    /// The kernel found on the first call, null before it: one of the
    /// statics above.
    static ACTIVE: core::sync::atomic::AtomicPtr<Kernel> =
        core::sync::atomic::AtomicPtr::new(core::ptr::null_mut());

    /// The widest kernel this CPU runs: asked of the CPU on the first call,
    /// a load after it.
    #[inline]
    pub(super) fn active() -> Kernel {
        use core::sync::atomic::Ordering::Relaxed;
        let found = ACTIVE.load(Relaxed);
        if !found.is_null() {
            // SAFETY: only the address of a static kernel is stored.
            return unsafe { *found };
        }
        detect()
    }

    #[cold]
    fn detect() -> Kernel {
        // No AVX-512 kernel: the loop is bound by its multiplies, and 512-bit
        // ones lower the clock on the CPUs that have them. SSE2 is part of
        // every x86-64 CPU. Two threads finding it at once store the same.
        let kernel: &'static Kernel = if super::cpu_avx2::get() { &AVX2 } else { &SSE2 };
        ACTIVE.store(
            core::ptr::from_ref(kernel).cast_mut(),
            core::sync::atomic::Ordering::Relaxed,
        );
        *kernel
    }
}

/// The NEON kernel: 16 bytes a lane.
#[cfg(xxh3_neon)]
mod neon {
    #[cfg(test)]
    use super::Kernel;
    use super::{
        Acc, LAST_STRIPE_BACK, PRIME32_1, SCRAMBLE_AT, SECRET_LEN, STRIPE_LEN, STRIPES_PER_BLOCK,
    };

    entry_points!(#[target_feature(enable = "neon")]);
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

    /// Accumulates `stripes` stripes from `input` into `acc`, the secret of
    /// each starting 8 bytes after the previous one's.
    ///
    /// # Safety
    ///
    /// `input` is valid for `stripes * STRIPE_LEN` bytes and `secret` for
    /// `(stripes - 1) * 8 + STRIPE_LEN`.
    #[inline]
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

    /// Scrambles `acc` with the secret's last stripe.
    ///
    /// # Safety
    ///
    /// `secret` is valid for [`SECRET_LEN`] bytes.
    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn scramble(acc: &mut Acc, secret: *const u8) {
        let mut lanes = load_acc!(acc);
        scramble!(lanes, secret);
        store_acc!(acc, lanes);
    }

    /// Accumulates `blocks` whole blocks from `input` into `acc`, scrambling
    /// after each.
    ///
    /// # Safety
    ///
    /// `input` is valid for `blocks * STRIPES_PER_BLOCK * STRIPE_LEN` bytes
    /// and `secret` for [`SECRET_LEN`].
    #[inline]
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

    register_long!(#[target_feature(enable = "neon")]);

    #[cfg(test)]
    pub(super) const NEON: Kernel = Kernel {
        consume,
        long,
        last,
    };
}
