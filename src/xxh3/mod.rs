// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! xxh3, 64 and 128 bits, through the widest kernel the CPU runs.
//!
//! An input longer than 240 bytes runs xxh3's stripe loop, which a SIMD
//! kernel covers 16 or 32 bytes per instruction; one chosen when the crate is
//! compiled can only use what every CPU of the target has (SSE2 on x86-64).
//! Here the loop runs through [`kernel::Kernel::get`], asked at run time.
//! Shorter inputs take xxh3's short paths, which no vector speeds up, from
//! `xxhash-rust`. Every digest is the one xxh3 defines, so nothing a file
//! holds depends on the CPU that wrote it.

mod kernel;

use kernel::{Acc, Kernel};

#[cfg(test)]
#[expect(clippy::indexing_slicing, reason = "test code")]
mod tests;

/// Bytes one accumulation step reads.
const STRIPE_LEN: usize = 64;
/// The length of the default secret, and of one derived from a seed.
const SECRET_LEN: usize = 192;
/// Stripes between two scrambles: `(SECRET_LEN - STRIPE_LEN) / 8`.
const STRIPES_PER_BLOCK: usize = (SECRET_LEN - STRIPE_LEN) / 8;
/// The longest input xxh3's short paths take.
const MID_SIZE_MAX: usize = 240;
/// Where the secret of the last stripe starts, counted back from the end.
const LAST_STRIPE_BACK: usize = 7;
/// Where the secret of the final merge starts.
const MERGE_AT: usize = 11;
/// The streaming buffer: four stripes.
const BUFFER_LEN: usize = 256;

const PRIME32_1: u32 = 0x9E37_79B1;
const PRIME32_2: u32 = 0x85EB_CA77;
const PRIME32_3: u32 = 0xC2B2_AE3D;
const PRIME64_1: u64 = 0x9E37_79B1_85EB_CA87;
const PRIME64_2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const PRIME64_3: u64 = 0x1656_67B1_9E37_79F9;
const PRIME64_4: u64 = 0x85EB_CA77_C2B2_AE63;
const PRIME64_5: u64 = 0x27D4_EB2F_1656_67C5;

const INITIAL_ACC: Acc = [
    PRIME32_3 as u64,
    PRIME64_1,
    PRIME64_2,
    PRIME64_3,
    PRIME64_4,
    PRIME32_2 as u64,
    PRIME64_5,
    PRIME32_1 as u64,
];

/// xxh3's default secret.
const DEFAULT_SECRET: [u8; SECRET_LEN] = [
    0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c,
    0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f,
    0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21,
    0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c,
    0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3,
    0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8,
    0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d,
    0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64,
    0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
    0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e,
    0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce,
    0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e,
];

/// The xxh3-64 of `input`.
#[must_use]
pub(crate) fn hash64(input: &[u8]) -> u64 {
    if input.len() <= MID_SIZE_MAX {
        return xxhash_rust::xxh3::xxh3_64(input);
    }
    let acc = long_acc(Kernel::get(), input, &DEFAULT_SECRET);
    merge64(&acc, &DEFAULT_SECRET, input.len() as u64)
}

/// The xxh3-64 of `input` under `seed`.
#[must_use]
pub(crate) fn hash64_with_seed(input: &[u8], seed: u64) -> u64 {
    if input.len() <= MID_SIZE_MAX {
        return xxhash_rust::xxh3::xxh3_64_with_seed(input, seed);
    }
    let secret = secret_for(seed);
    let acc = long_acc(Kernel::get(), input, &secret);
    merge64(&acc, &secret, input.len() as u64)
}

/// The xxh3-128 of `input`.
#[must_use]
pub(crate) fn hash128(input: &[u8]) -> u128 {
    hash128_with_seed(input, 0)
}

/// The xxh3-128 of `input` under `seed`.
#[must_use]
pub(crate) fn hash128_with_seed(input: &[u8], seed: u64) -> u128 {
    if input.len() <= MID_SIZE_MAX {
        return xxhash_rust::xxh3::xxh3_128_with_seed(input, seed);
    }
    let secret = secret_for(seed);
    let acc = long_acc(Kernel::get(), input, &secret);
    merge128(&acc, &secret, input.len() as u64)
}

/// The secret `seed` derives from the default one: the default itself for
/// seed zero.
fn secret_for(seed: u64) -> [u8; SECRET_LEN] {
    if seed == 0 {
        return DEFAULT_SECRET;
    }
    let mut secret = DEFAULT_SECRET;
    for [low, high] in secret.as_chunks_mut::<8>().0.as_chunks_mut::<2>().0 {
        *low = u64::from_le_bytes(*low).wrapping_add(seed).to_le_bytes();
        *high = u64::from_le_bytes(*high).wrapping_sub(seed).to_le_bytes();
    }
    secret
}

/// The accumulators of `input`, longer than [`MID_SIZE_MAX`], under `secret`.
fn long_acc(kernel: Kernel, input: &[u8], secret: &[u8; SECRET_LEN]) -> Acc {
    debug_assert!(input.len() > MID_SIZE_MAX);
    let mut acc = INITIAL_ACC;
    // Every stripe but the last whole one before the input's end; that one
    // overlaps the stripes before it and takes its own secret.
    let stripes = (input.len() - 1) / STRIPE_LEN;
    consume(kernel, &mut acc, input.as_ptr(), stripes, 0, secret);
    last_stripe(kernel, &mut acc, input.as_ptr(), input.len(), secret);
    acc
}

/// Accumulates the last 64 bytes of the `len` bytes at `input` into `acc`.
fn last_stripe(
    kernel: Kernel,
    acc: &mut Acc,
    input: *const u8,
    len: usize,
    secret: &[u8; SECRET_LEN],
) {
    debug_assert!(len >= STRIPE_LEN);
    // SAFETY: `len >= STRIPE_LEN` bytes from `input`, so its last stripe lies
    // within them; its secret, 64 bytes from SECRET_LEN - 64 - 7, within
    // the secret.
    unsafe {
        (kernel.stripes)(
            acc,
            input.add(len - STRIPE_LEN),
            secret
                .as_ptr()
                .add(SECRET_LEN - STRIPE_LEN - LAST_STRIPE_BACK),
            1,
        );
    }
}

/// Accumulates `stripes` stripes from `input` into `acc`, `done` stripes of
/// the current block already in it, scrambling at each block's end, and
/// returns how many of the block the last one leaves done.
///
/// `input` must be valid for `stripes * STRIPE_LEN` bytes.
fn consume(
    kernel: Kernel,
    acc: &mut Acc,
    input: *const u8,
    stripes: usize,
    done: usize,
    secret: &[u8; SECRET_LEN],
) -> usize {
    debug_assert!(done < STRIPES_PER_BLOCK);
    let secret_at = |stripe: usize| {
        // SAFETY: a stripe's secret starts at most 8 * 15 bytes in, and its
        // 64 bytes end within the secret's 192.
        unsafe { secret.as_ptr().add(stripe * 8) }
    };
    if done + stripes < STRIPES_PER_BLOCK {
        // SAFETY: `input` holds `stripes` stripes, by the caller; their
        // secrets end at (done + stripes - 1) * 8 + 64 < 192.
        unsafe { (kernel.stripes)(acc, input, secret_at(done), stripes) };
        return done + stripes;
    }
    let to_end = STRIPES_PER_BLOCK - done;
    let blocks = (stripes - to_end) / STRIPES_PER_BLOCK;
    let rest = stripes - to_end - blocks * STRIPES_PER_BLOCK;
    // SAFETY: `to_end + blocks * 16 + rest == stripes` stripes from `input`,
    // which the caller keeps valid; each call's secret as above.
    unsafe {
        (kernel.stripes)(acc, input, secret_at(done), to_end);
        (kernel.scramble)(acc, secret.as_ptr());
        let input = input.add(to_end * STRIPE_LEN);
        (kernel.blocks)(acc, input, blocks, secret.as_ptr());
        let input = input.add(blocks * STRIPES_PER_BLOCK * STRIPE_LEN);
        (kernel.stripes)(acc, input, secret.as_ptr(), rest);
    }
    rest
}

/// The little-endian `u64` at `at` of `secret`.
fn secret_u64(secret: &[u8; SECRET_LEN], at: usize) -> u64 {
    let mut word = [0; 8];
    word.copy_from_slice(secret.get(at..at + 8).unwrap_or(&[0; 8]));
    u64::from_le_bytes(word)
}

/// Folds the accumulators into one word, starting from `start`, with the
/// secret at `at`.
fn merge(acc: &Acc, secret: &[u8; SECRET_LEN], at: usize, start: u64) -> u64 {
    let mut result = start;
    for (pair, &[low, high]) in acc.as_chunks::<2>().0.iter().enumerate() {
        let low = low ^ secret_u64(secret, at + pair * 16);
        let high = high ^ secret_u64(secret, at + pair * 16 + 8);
        let product = u128::from(low) * u128::from(high);
        #[expect(clippy::cast_possible_truncation, reason = "the low half, by intent")]
        let folded = (product as u64) ^ ((product >> 64) as u64);
        result = result.wrapping_add(folded);
    }
    avalanche(result)
}

fn avalanche(value: u64) -> u64 {
    let value = (value ^ (value >> 37)).wrapping_mul(0x1656_6791_9E37_79F9);
    value ^ (value >> 32)
}

/// The 64-bit digest of accumulators over `len` bytes.
fn merge64(acc: &Acc, secret: &[u8; SECRET_LEN], len: u64) -> u64 {
    merge(acc, secret, MERGE_AT, len.wrapping_mul(PRIME64_1))
}

/// The 128-bit digest of accumulators over `len` bytes.
fn merge128(acc: &Acc, secret: &[u8; SECRET_LEN], len: u64) -> u128 {
    let low = merge(acc, secret, MERGE_AT, len.wrapping_mul(PRIME64_1));
    let high = merge(
        acc,
        secret,
        SECRET_LEN - STRIPE_LEN - MERGE_AT,
        !len.wrapping_mul(PRIME64_2),
    );
    u128::from(low) | (u128::from(high) << 64)
}

/// xxh3 over input given in parts: the digests are those of the parts
/// joined.
///
/// Building one writes its scalars only: the buffer starts unwritten, and an
/// unseeded hasher reads the default secret where it lies. A hasher is built
/// per entry on the per-entry checksum's path.
#[derive(Clone)]
pub(crate) struct Hasher {
    acc: Acc,
    /// The input not yet accumulated, the first `buffered` bytes; once a
    /// stripe was accumulated from input that did not pass through the
    /// buffer whole, its last 64 bytes hold the last stripe accumulated. No
    /// other byte is read before it is written.
    buffer: [core::mem::MaybeUninit<u8>; BUFFER_LEN],
    buffered: usize,
    /// Stripes of the current block accumulated.
    done: usize,
    total: u64,
    seed: u64,
    /// A seeded hasher's secret; an unseeded one uses [`DEFAULT_SECRET`].
    seeded: Option<[u8; SECRET_LEN]>,
}

impl Default for Hasher {
    fn default() -> Self {
        Self::with_seed(0)
    }
}

impl Hasher {
    /// A hasher of the default seed.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// A hasher under `seed`.
    #[must_use]
    pub(crate) fn with_seed(seed: u64) -> Self {
        Self {
            acc: INITIAL_ACC,
            buffer: [core::mem::MaybeUninit::uninit(); BUFFER_LEN],
            buffered: 0,
            done: 0,
            total: 0,
            seed,
            seeded: (seed != 0).then(|| secret_for(seed)),
        }
    }

    fn secret(&self) -> &[u8; SECRET_LEN] {
        self.seeded.as_ref().unwrap_or(&DEFAULT_SECRET)
    }

    /// The buffered input not yet accumulated.
    fn buffered(&self) -> &[u8] {
        // SAFETY: the first `buffered` bytes were written by the update that
        // counted them.
        unsafe { core::slice::from_raw_parts(self.buffer.as_ptr().cast::<u8>(), self.buffered) }
    }

    /// Hashes `input` after what came before it.
    pub(crate) fn update(&mut self, mut input: &[u8]) {
        /// Writes `bytes` into `buffer` at `at`; every caller keeps them in it.
        fn put(buffer: &mut [core::mem::MaybeUninit<u8>; BUFFER_LEN], at: usize, bytes: &[u8]) {
            if let Some(place) = buffer.get_mut(at..at + bytes.len()) {
                place.write_copy_of_slice(bytes);
            }
        }

        let Self {
            acc,
            buffer,
            buffered,
            done,
            total,
            seeded,
            ..
        } = self;
        *total = total.wrapping_add(input.len() as u64);
        let room = BUFFER_LEN - *buffered;
        if input.len() <= room {
            put(buffer, *buffered, input);
            *buffered += input.len();
            return;
        }
        let kernel = Kernel::get();
        let secret = seeded.as_ref().unwrap_or(&DEFAULT_SECRET);
        if *buffered > 0 {
            // Fill the buffer and accumulate it: more input follows, so its
            // last stripe is not the input's last.
            let (fill, rest) = input.split_at(room);
            put(buffer, *buffered, fill);
            // The whole buffer is written now.
            *done = consume(
                kernel,
                acc,
                buffer.as_ptr().cast::<u8>(),
                BUFFER_LEN / STRIPE_LEN,
                *done,
                secret,
            );
            *buffered = 0;
            input = rest;
        }
        if input.len() > BUFFER_LEN {
            // Whole buffers' worth of stripes straight from the input,
            // leaving 1 to 256 bytes for the buffer, as a buffer at a time
            // would.
            let len = (input.len() - 1) / BUFFER_LEN * BUFFER_LEN;
            *done = consume(kernel, acc, input.as_ptr(), len / STRIPE_LEN, *done, secret);
            // The last stripe accumulated, for a digest whose own last
            // stripe reaches back into it.
            let (consumed, rest) = input.split_at(len);
            if let Some(last) = consumed.get(len - STRIPE_LEN..) {
                put(buffer, BUFFER_LEN - STRIPE_LEN, last);
            }
            input = rest;
        }
        put(buffer, 0, input);
        *buffered = input.len();
    }

    /// The accumulators of everything hashed, as a digest of it finds them.
    fn long_acc(&self) -> Acc {
        let kernel = Kernel::get();
        let secret = self.secret();
        let mut acc = self.acc;
        let buffered = self.buffered();
        if buffered.len() >= STRIPE_LEN {
            let stripes = (buffered.len() - 1) / STRIPE_LEN;
            consume(
                kernel,
                &mut acc,
                buffered.as_ptr(),
                stripes,
                self.done,
                secret,
            );
            last_stripe(kernel, &mut acc, buffered.as_ptr(), buffered.len(), secret);
        } else {
            // The last stripe reaches back into the bytes accumulated before
            // the buffer was refilled, which its end still holds.
            let back = STRIPE_LEN - buffered.len();
            let mut stripe = [0; STRIPE_LEN];
            // SAFETY: more than 240 bytes went through and fewer than 64 are
            // buffered, so an update accumulated a whole buffer (writing all
            // of it) or straight input (writing its last stripe into the
            // buffer's last 64 bytes); the `back` bytes read lie in those 64.
            let old = unsafe {
                core::slice::from_raw_parts(
                    self.buffer.as_ptr().cast::<u8>().add(BUFFER_LEN - back),
                    back,
                )
            };
            let (head, tail) = stripe.split_at_mut(back);
            head.copy_from_slice(old);
            tail.copy_from_slice(buffered);
            last_stripe(kernel, &mut acc, stripe.as_ptr(), STRIPE_LEN, secret);
        }
        acc
    }

    /// The xxh3-64 of everything hashed.
    #[must_use]
    pub(crate) fn digest(&self) -> u64 {
        if self.total <= MID_SIZE_MAX as u64 {
            return xxhash_rust::xxh3::xxh3_64_with_seed(self.buffered(), self.seed);
        }
        merge64(&self.long_acc(), self.secret(), self.total)
    }

    /// The xxh3-128 of everything hashed.
    #[must_use]
    pub(crate) fn digest128(&self) -> u128 {
        if self.total <= MID_SIZE_MAX as u64 {
            return xxhash_rust::xxh3::xxh3_128_with_seed(self.buffered(), self.seed);
        }
        merge128(&self.long_acc(), self.secret(), self.total)
    }
}

#[cfg(feature = "std")]
impl std::io::Write for Hasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
