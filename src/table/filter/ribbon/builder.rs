#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::error::{BuildError, ConstructionFailure};
use super::filter::RibbonFilter;
use super::hashing::{
    SplitMix64, StandardEquation, for_each_set_bit_u128_parts, standard_equation_from_hash,
    xor_words,
};
use super::params::{Mode, Params};

#[derive(Debug, Clone)]
pub struct RibbonBuilder {
    params: Params,
}

impl RibbonBuilder {
    pub fn new(params: Params) -> Result<Self, BuildError> {
        params.validate().map_err(BuildError::InvalidParams)?;
        Ok(Self { params })
    }

    pub fn params(&self) -> Params {
        self.params
    }

    /// Build a Ribbon filter from already-hashed keys (each `u64` is a
    /// stable key hash the caller precomputed). Verbatim seed, no retry.
    ///
    /// Used by BuRR when the LSM has already computed a stable u64
    /// hash for each key (via `crate::hash::hash64` / xxh3); the ribbon
    /// feeds those hashes straight into the banded solver.
    pub(crate) fn build_with_seed_verbatim_from_hashes(
        &self,
        hashes: &[u64],
        seed: u64,
        m: usize,
    ) -> Result<RibbonFilter, BuildError> {
        self.params.validate().map_err(BuildError::InvalidParams)?;
        self.build_once_from_hashes(hashes, m, seed)
            .map_err(|failure| BuildError::ConstructionFailed {
                final_m: m,
                attempts: 1,
                last_failure: failure,
            })
    }

    /// Build a single ribbon over pre-computed key hashes.
    ///
    /// Used by BuRR through `build_with_seed_verbatim_from_hashes` so the
    /// LSM-side stable u64 hash (xxh3 / `crate::hash::hash64`) flows
    /// straight into the banded solver.
    fn build_once_from_hashes(
        &self,
        hashes: &[u64],
        m: usize,
        seed: u64,
    ) -> Result<RibbonFilter, ConstructionFailure> {
        debug_assert!(m >= self.params.w);

        let stride_words = self.params.fingerprint_words();
        let total_words = m
            .checked_mul(stride_words)
            .ok_or(ConstructionFailure::StorageLengthOverflow { m, stride_words })?;
        let fp_last_mask = self.params.fingerprint_last_word_mask();
        let mut occupied = vec![false; m];
        let mut coeff_lo = vec![0u64; m];
        let mut coeff_hi = vec![0u64; m];
        let mut rhs = vec![0u64; total_words];

        let mut key_fp = vec![0u64; stride_words];
        let layer_params = Params { m, ..self.params };

        for (key_index, hash) in hashes.iter().enumerate() {
            key_fp.fill(0);
            let equation: StandardEquation =
                standard_equation_from_hash(*hash, seed, &layer_params, &mut key_fp);

            let mut i = equation.start;
            let mut c_lo = equation.coeff_lo;
            let mut c_hi = equation.coeff_hi;
            let mut b = key_fp.clone();

            if i >= m {
                return Err(ConstructionFailure::OutOfBounds {
                    key_index: Some(key_index),
                    row_index: i,
                    m,
                });
            }

            loop {
                if !occupied[i] {
                    occupied[i] = true;
                    coeff_lo[i] = c_lo;
                    coeff_hi[i] = c_hi;
                    rhs[i * stride_words..(i + 1) * stride_words].copy_from_slice(&b);
                    break;
                }

                c_lo ^= coeff_lo[i];
                c_hi ^= coeff_hi[i];
                xor_words(&mut b, &rhs[i * stride_words..(i + 1) * stride_words]);

                if c_lo == 0 && c_hi == 0 {
                    if b.iter().all(|&x| x == 0) {
                        break;
                    }
                    return Err(ConstructionFailure::InconsistentEquation {
                        key_index,
                        row_index: i,
                    });
                }

                let shift = if c_lo != 0 {
                    c_lo.trailing_zeros() as usize
                } else {
                    64 + c_hi.trailing_zeros() as usize
                };
                i += shift;
                if i >= m {
                    return Err(ConstructionFailure::OutOfBounds {
                        key_index: Some(key_index),
                        row_index: i,
                        m,
                    });
                }
                if shift >= 64 {
                    c_lo = c_hi >> (shift - 64);
                    c_hi = 0;
                } else if shift > 0 {
                    c_lo = (c_lo >> shift) | (c_hi << (64 - shift));
                    c_hi >>= shift;
                }
            }
        }

        let mut z = vec![0u64; total_words];
        if matches!(self.params.mode, Mode::Homogeneous) {
            let mut rng = SplitMix64::new(seed ^ 0xD1B5_4A32_D192_ED03);
            for (i, is_occupied) in occupied.iter().enumerate().take(m) {
                if *is_occupied {
                    continue;
                }
                let row_start = i * stride_words;
                let row_end = row_start + stride_words;
                for word in &mut z[row_start..row_end] {
                    *word = rng.next_u64();
                }
                z[row_end - 1] &= fp_last_mask;
            }
        }

        for i in (0..m).rev() {
            if !occupied[i] {
                continue;
            }
            let row_start = i * stride_words;
            let row_end = row_start + stride_words;
            z[row_start..row_end].copy_from_slice(&rhs[row_start..row_end]);
            let upper_lo = coeff_lo[i] & !1u64;
            let upper_hi = coeff_hi[i];
            let mut row_offsets = Vec::with_capacity(self.params.w.saturating_sub(1));
            for_each_set_bit_u128_parts(upper_lo, upper_hi, |offset| {
                row_offsets.push(offset);
            });
            for offset in row_offsets {
                let row_index = i + offset;
                if row_index >= m {
                    return Err(ConstructionFailure::OutOfBounds {
                        key_index: None,
                        row_index,
                        m,
                    });
                }
                let other_start = row_index * stride_words;
                let (left, right) = z.split_at_mut(other_start);
                let row = &mut left[row_start..row_end];
                let other = &right[..stride_words];
                xor_words(row, other);
            }
            z[row_end - 1] &= fp_last_mask;
        }
        let mut built_params = self.params;
        built_params.m = m;
        built_params.seed = seed;

        Ok(RibbonFilter::new(built_params, z))
    }
}
