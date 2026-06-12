use core::hash::BuildHasher;
use core::hash::Hash;

use super::params::{Mode, Params};

const MIX_CONST: u64 = 0x9E37_79B9_7F4A_7C15;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StandardEquation {
    pub(crate) start: usize,
    pub(crate) coeff_lo: u64,
    pub(crate) coeff_hi: u64,
}

#[inline]
pub(crate) fn xor_words(dst: &mut [u64], rhs: &[u64]) {
    for (d, r) in dst.iter_mut().zip(rhs.iter()) {
        *d ^= *r;
    }
}

#[inline]
pub(crate) fn for_each_set_bit_u128_parts(mut lo: u64, mut hi: u64, mut f: impl FnMut(usize)) {
    while lo != 0 {
        let bit = lo.trailing_zeros() as usize;
        f(bit);
        lo &= lo - 1;
    }
    while hi != 0 {
        let bit = hi.trailing_zeros() as usize;
        f(64 + bit);
        hi &= hi - 1;
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(MIX_CONST);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

fn fastrange_u64(x: u64, range: usize) -> usize {
    ((x as u128 * range as u128) >> 64) as usize
}

#[inline]
pub(crate) fn start_position_from_stream(next_word: u64, m: usize, w: usize) -> usize {
    let start_range = m - w + 1;
    // TODO: add optional boundary smash strategy here.
    // TODO: add fractional-r/ICML tuned layout hooks once layout work starts.
    fastrange_u64(next_word, start_range)
}

pub(crate) fn derive_attempt_seed(base_seed: u64, attempt_index: u64) -> u64 {
    let mut sm = SplitMix64::new(base_seed ^ attempt_index.wrapping_mul(MIX_CONST));
    sm.next_u64().wrapping_mul(MIX_CONST)
}

pub(crate) fn standard_equation_w64<S: BuildHasher, Q: Hash + ?Sized>(
    build_hasher: &S,
    key: &Q,
    seed: u64,
    params: &Params,
    fingerprint: &mut [u64],
) -> StandardEquation {
    let base_hash = build_hasher.hash_one(key);
    standard_equation_from_hash(base_hash, seed, params, fingerprint)
}

/// Compute the equation directly from a pre-computed key hash.
///
/// This is the inner loop of [`standard_equation_w64`], factored out so
/// the BuRR wire-format probe path (which consumes pre-hashed inputs from
/// the LSM filter framework) can skip the `build_hasher.hash_one(key)`
/// step entirely.
#[expect(
    clippy::inline_always,
    reason = "called per layer on the BuRR filter probe hot path; inlining lets LLVM fold the \
              SplitMix stream into the caller and eliminate the &mut [u64] fingerprint pointer"
)]
#[inline(always)]
pub(crate) fn standard_equation_from_hash(
    base_hash: u64,
    seed: u64,
    params: &Params,
    fingerprint: &mut [u64],
) -> StandardEquation {
    let stream_seed = (base_hash ^ seed).wrapping_mul(MIX_CONST);
    let mut stream = SplitMix64::new(stream_seed);

    let start = start_position_from_stream(stream.next_u64(), params.m, params.w);

    let (coeff_lo, coeff_hi) = if params.w <= 64 {
        let width_mask = if params.w == 64 {
            u64::MAX
        } else {
            (1u64 << params.w) - 1
        };
        ((stream.next_u64() & width_mask) | 1, 0)
    } else {
        let lo = stream.next_u64();
        let hi_bits = params.w - 64;
        let hi_mask = if hi_bits == 64 {
            u64::MAX
        } else {
            (1u64 << hi_bits) - 1
        };
        (lo | 1, stream.next_u64() & hi_mask)
    };

    if matches!(params.mode, Mode::Homogeneous) {
        fingerprint.fill(0);
    } else {
        for word in fingerprint.iter_mut() {
            *word = stream.next_u64();
        }
        if let Some(last) = fingerprint.last_mut() {
            *last &= params.fingerprint_last_word_mask();
        }
    }

    StandardEquation {
        start,
        coeff_lo,
        coeff_hi,
    }
}
