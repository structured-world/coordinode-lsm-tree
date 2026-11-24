// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! xxh3's paths for inputs up to [`MID_SIZE_MAX`] bytes: a few multiplies
//! over words read from both ends of the input, under the default secret
//! with the seed added to its words.

use super::{
    DEFAULT_SECRET, MID_SIZE_MAX, PRIME32_2, PRIME64_1, PRIME64_2, PRIME64_3, PRIME64_4, avalanche,
};

/// The multiplier of xxh3's `rrmxmx` and of its 4 to 8 byte 128-bit path.
const PRIME_MX2: u64 = 0x9FB2_1C65_1E98_DF25;
/// Where the secret of the 129 to 240 byte paths' later rounds starts.
const MID_START: usize = 3;
/// Where the secret of the 129 to 240 byte path's last round starts.
const MID_LAST: usize = 136 - 17;

/// The little-endian `u64` at `at` of `bytes`.
///
/// # Safety
///
/// `at + 8 <= bytes.len()`.
#[inline]
unsafe fn read64(bytes: &[u8], at: usize) -> u64 {
    debug_assert!(at + 8 <= bytes.len());
    // SAFETY: in bounds by the caller; the read is unaligned.
    u64::from_le(unsafe { bytes.as_ptr().add(at).cast::<u64>().read_unaligned() })
}

/// The little-endian `u32` at `at` of `bytes`.
///
/// # Safety
///
/// `at + 4 <= bytes.len()`.
#[inline]
unsafe fn read32(bytes: &[u8], at: usize) -> u32 {
    debug_assert!(at + 4 <= bytes.len());
    // SAFETY: in bounds by the caller; the read is unaligned.
    u32::from_le(unsafe { bytes.as_ptr().add(at).cast::<u32>().read_unaligned() })
}

/// The default secret's `u64` at `at`.
#[inline]
fn key64(at: usize) -> u64 {
    debug_assert!(at + 8 <= DEFAULT_SECRET.len());
    // SAFETY: every caller's offset ends within the 192 bytes.
    unsafe { read64(&DEFAULT_SECRET, at) }
}

/// The default secret's `u32` at `at`.
#[inline]
fn key32(at: usize) -> u32 {
    // SAFETY: every caller's offset ends within the 192 bytes.
    unsafe { read32(&DEFAULT_SECRET, at) }
}

#[inline]
fn fold(low: u64, high: u64) -> u64 {
    let product = u128::from(low) * u128::from(high);
    #[expect(clippy::cast_possible_truncation, reason = "the low half, by intent")]
    let folded = (product as u64) ^ ((product >> 64) as u64);
    folded
}

/// The full product of `a` and `b`, as its (low, high) halves.
#[inline]
fn widen(a: u64, b: u64) -> (u64, u64) {
    let product = u128::from(a) * u128::from(b);
    #[expect(clippy::cast_possible_truncation, reason = "the halves, by intent")]
    let halves = (product as u64, (product >> 64) as u64);
    halves
}

/// The low 32 bits of `value`.
#[inline]
fn low32(value: u64) -> u32 {
    #[expect(clippy::cast_possible_truncation, reason = "the low half, by intent")]
    let low = value as u32;
    low
}

/// xxHash64's final mix.
#[inline]
fn avalanche64(value: u64) -> u64 {
    let value = (value ^ (value >> 33)).wrapping_mul(PRIME64_2);
    let value = (value ^ (value >> 29)).wrapping_mul(PRIME64_3);
    value ^ (value >> 32)
}

#[inline]
fn rrmxmx(value: u64, len: u64) -> u64 {
    let value = value ^ value.rotate_left(49) ^ value.rotate_left(24);
    let value = value.wrapping_mul(PRIME_MX2);
    let value = (value ^ ((value >> 35).wrapping_add(len))).wrapping_mul(PRIME_MX2);
    value ^ (value >> 28)
}

/// 16 bytes of `input` at `at` mixed with the secret at `key`.
///
/// # Safety
///
/// `at + 16 <= input.len()`.
#[inline]
unsafe fn mix16(input: &[u8], at: usize, key: usize, seed: u64) -> u64 {
    // SAFETY: in bounds by the caller.
    let (low, high) = unsafe { (read64(input, at), read64(input, at + 8)) };
    fold(
        low ^ key64(key).wrapping_add(seed),
        high ^ key64(key + 8).wrapping_sub(seed),
    )
}

/// Two 16-byte runs, at `first` and `second`, into a 128-bit state.
///
/// # Safety
///
/// `first + 16` and `second + 16` are at most `input.len()`.
#[inline]
unsafe fn mix32(
    (low, high): (u64, u64),
    input: &[u8],
    first: usize,
    second: usize,
    key: usize,
    seed: u64,
) -> (u64, u64) {
    // SAFETY: in bounds by the caller.
    unsafe {
        let low = low.wrapping_add(mix16(input, first, key, seed))
            ^ read64(input, second).wrapping_add(read64(input, second + 8));
        let high = high.wrapping_add(mix16(input, second, key + 16, seed))
            ^ read64(input, first).wrapping_add(read64(input, first + 8));
        (low, high)
    }
}

/// The xxh3-64 of `input`, at most [`MID_SIZE_MAX`] bytes, under `seed`.
#[inline]
pub(super) fn hash64(input: &[u8], seed: u64) -> u64 {
    let len = input.len();
    debug_assert!(len <= MID_SIZE_MAX);
    let len64 = len as u64;
    // SAFETY (every read below): each branch reads only offsets its length
    // bound keeps within `input`.
    // The bounds are tested longest first, as xxh3's reference does: a
    // longer input takes fewer branches.
    unsafe {
        if len <= 16 {
            if len > 8 {
                let flip_low = (key64(24) ^ key64(32)).wrapping_add(seed);
                let flip_high = (key64(40) ^ key64(48)).wrapping_sub(seed);
                let low = read64(input, 0) ^ flip_low;
                let high = read64(input, len - 8) ^ flip_high;
                avalanche(
                    len64
                        .wrapping_add(low.swap_bytes())
                        .wrapping_add(high)
                        .wrapping_add(fold(low, high)),
                )
            } else if len >= 4 {
                let seed = seed ^ (u64::from(low32(seed).swap_bytes()) << 32);
                let first = read32(input, 0);
                let last = read32(input, len - 4);
                let flip = (key64(8) ^ key64(16)).wrapping_sub(seed);
                let joined = u64::from(last).wrapping_add(u64::from(first) << 32);
                rrmxmx(joined ^ flip, len64)
            } else if len > 0 {
                let (first, middle, last) = (
                    *input.get_unchecked(0),
                    *input.get_unchecked(len >> 1),
                    *input.get_unchecked(len - 1),
                );
                let combined = (u32::from(first) << 16)
                    | (u32::from(middle) << 24)
                    | u32::from(last)
                    | (low32(len64) << 8);
                let flip = u64::from(key32(0) ^ key32(4)).wrapping_add(seed);
                avalanche64(u64::from(combined) ^ flip)
            } else {
                avalanche64(seed ^ key64(56) ^ key64(64))
            }
        } else if len <= 128 {
            let mut acc = len64.wrapping_mul(PRIME64_1);
            if len > 32 {
                if len > 64 {
                    if len > 96 {
                        acc = acc.wrapping_add(mix16(input, 48, 96, seed));
                        acc = acc.wrapping_add(mix16(input, len - 64, 112, seed));
                    }
                    acc = acc.wrapping_add(mix16(input, 32, 64, seed));
                    acc = acc.wrapping_add(mix16(input, len - 48, 80, seed));
                }
                acc = acc.wrapping_add(mix16(input, 16, 32, seed));
                acc = acc.wrapping_add(mix16(input, len - 32, 48, seed));
            }
            acc = acc.wrapping_add(mix16(input, 0, 0, seed));
            acc = acc.wrapping_add(mix16(input, len - 16, 16, seed));
            avalanche(acc)
        } else {
            mid64(input, seed)
        }
    }
}

/// [`hash64`] of 129 to 240 bytes: out of line, so the paths up to 128
/// bytes inline into a caller without this loop.
#[inline(never)]
fn mid64(input: &[u8], seed: u64) -> u64 {
    let len = input.len();
    debug_assert!((129..=MID_SIZE_MAX).contains(&len));
    let mut acc = (len as u64).wrapping_mul(PRIME64_1);
    // SAFETY: every round reads 16 bytes within the first `len / 16 * 16`,
    // and the last the final 16.
    unsafe {
        for round in 0..8 {
            acc = acc.wrapping_add(mix16(input, 16 * round, 16 * round, seed));
        }
        acc = avalanche(acc);
        for round in 8..len / 16 {
            acc = acc.wrapping_add(mix16(input, 16 * round, 16 * (round - 8) + MID_START, seed));
        }
        acc = acc.wrapping_add(mix16(input, len - 16, MID_LAST, seed));
    }
    avalanche(acc)
}

/// The xxh3-128 of `input`, at most [`MID_SIZE_MAX`] bytes, under `seed`.
#[inline]
pub(super) fn hash128(input: &[u8], seed: u64) -> u128 {
    let len = input.len();
    debug_assert!(len <= MID_SIZE_MAX);
    let len64 = len as u64;
    // SAFETY (every read below): each branch reads only offsets its length
    // bound keeps within `input`.
    // Longest bounds first, as in `hash64`.
    unsafe {
        if len <= 16 {
            if len > 8 {
                let flip_low = (key64(32) ^ key64(40)).wrapping_sub(seed);
                let flip_high = (key64(48) ^ key64(56)).wrapping_add(seed);
                let first = read64(input, 0);
                let last = read64(input, len - 8);
                let (low, high) = widen(first ^ last ^ flip_low, PRIME64_1);
                let low = low.wrapping_add((len64 - 1) << 54);
                let last = last ^ flip_high;
                let high = high
                    .wrapping_add(last)
                    .wrapping_add(u64::from(low32(last)) * u64::from(PRIME32_2 - 1));
                let low = low ^ high.swap_bytes();
                let (out_low, out_high) = widen(low, PRIME64_2);
                let out_high = out_high.wrapping_add(high.wrapping_mul(PRIME64_2));
                join(avalanche(out_low), avalanche(out_high))
            } else if len >= 4 {
                let seed = seed ^ (u64::from(low32(seed).swap_bytes()) << 32);
                let first = read32(input, 0);
                let last = read32(input, len - 4);
                let joined = u64::from(first).wrapping_add(u64::from(last) << 32);
                let flip = (key64(16) ^ key64(24)).wrapping_add(seed);
                let (low, high) = widen(joined ^ flip, PRIME64_1.wrapping_add(len64 << 2));
                let high = high.wrapping_add(low << 1);
                let low = low ^ (high >> 3);
                let low = (low ^ (low >> 35)).wrapping_mul(PRIME_MX2);
                let low = low ^ (low >> 28);
                join(low, avalanche(high))
            } else if len > 0 {
                let (first, middle, last) = (
                    *input.get_unchecked(0),
                    *input.get_unchecked(len >> 1),
                    *input.get_unchecked(len - 1),
                );
                let combined_low = (u32::from(first) << 16)
                    | (u32::from(middle) << 24)
                    | u32::from(last)
                    | (low32(len64) << 8);
                let combined_high = combined_low.swap_bytes().rotate_left(13);
                let flip_low = u64::from(key32(0) ^ key32(4)).wrapping_add(seed);
                let flip_high = u64::from(key32(8) ^ key32(12)).wrapping_sub(seed);
                join(
                    avalanche64(u64::from(combined_low) ^ flip_low),
                    avalanche64(u64::from(combined_high) ^ flip_high),
                )
            } else {
                join(
                    avalanche64(seed ^ key64(64) ^ key64(72)),
                    avalanche64(seed ^ key64(80) ^ key64(88)),
                )
            }
        } else if len <= 128 {
            let mut acc = (len64.wrapping_mul(PRIME64_1), 0);
            if len > 32 {
                if len > 64 {
                    if len > 96 {
                        acc = mix32(acc, input, 48, len - 64, 96, seed);
                    }
                    acc = mix32(acc, input, 32, len - 48, 64, seed);
                }
                acc = mix32(acc, input, 16, len - 32, 32, seed);
            }
            finish128(mix32(acc, input, 0, len - 16, 0, seed), len64, seed)
        } else {
            mid128(input, seed)
        }
    }
}

/// [`hash128`] of 129 to 240 bytes, out of line as [`mid64`] is.
#[inline(never)]
fn mid128(input: &[u8], seed: u64) -> u128 {
    let len = input.len();
    debug_assert!((129..=MID_SIZE_MAX).contains(&len));
    let mut acc = ((len as u64).wrapping_mul(PRIME64_1), 0);
    // SAFETY: every round reads 32 bytes within the first `len / 32 * 32`,
    // and the last the final 32.
    unsafe {
        for round in 0..4 {
            acc = mix32(acc, input, 32 * round, 32 * round + 16, 32 * round, seed);
        }
        acc = (avalanche(acc.0), avalanche(acc.1));
        for round in 4..len / 32 {
            acc = mix32(
                acc,
                input,
                32 * round,
                32 * round + 16,
                MID_START + 32 * (round - 4),
                seed,
            );
        }
        acc = mix32(
            acc,
            input,
            len - 16,
            len - 32,
            MID_LAST - 16,
            0u64.wrapping_sub(seed),
        );
    }
    finish128(acc, len as u64, seed)
}

/// The two halves of a 128-bit digest.
#[inline]
fn join(low: u64, high: u64) -> u128 {
    u128::from(low) | (u128::from(high) << 64)
}

/// The 17 to 240 byte paths' final mix of their 128-bit state.
#[inline]
fn finish128((low, high): (u64, u64), len: u64, seed: u64) -> u128 {
    let out_low = low.wrapping_add(high);
    let out_high = low
        .wrapping_mul(PRIME64_1)
        .wrapping_add(high.wrapping_mul(PRIME64_4))
        .wrapping_add(len.wrapping_sub(seed).wrapping_mul(PRIME64_2));
    join(avalanche(out_low), 0u64.wrapping_sub(avalanche(out_high)))
}
