use std::hash::BuildHasher;

use super::error::{BuildError, ConstructionFailure};
use super::filter::RibbonFilter;
use super::hashing::{
    SplitMix64, derive_attempt_seed, for_each_set_bit_u128_parts, standard_equation_w64, xor_words,
};
use super::params::{Mode, Params};

#[derive(Debug, Clone)]
pub struct Scratch {
    pub(crate) fingerprint: Vec<u64>,
    pub(crate) acc: Vec<u64>,
}

impl Scratch {
    pub(crate) fn new(stride_words: usize) -> Self {
        Self {
            fingerprint: vec![0; stride_words],
            acc: vec![0; stride_words],
        }
    }

    pub(crate) fn reset(&mut self) {
        self.fingerprint.fill(0);
        self.acc.fill(0);
    }
}

#[derive(Debug, Clone)]
pub struct RibbonBuilder<S> {
    params: Params,
    build_hasher: S,
}

impl<S> RibbonBuilder<S>
where
    S: BuildHasher + Clone,
{
    pub fn new(params: Params, build_hasher: S) -> Result<Self, BuildError> {
        params.validate().map_err(BuildError::InvalidParams)?;
        Ok(Self {
            params,
            build_hasher,
        })
    }

    pub fn params(&self) -> Params {
        self.params
    }

    pub fn hasher(&self) -> &S {
        &self.build_hasher
    }

    pub fn build<K: std::hash::Hash>(&self, keys: &[K]) -> Result<RibbonFilter<S>, BuildError> {
        self.params.validate().map_err(BuildError::InvalidParams)?;

        let mut attempts = 0usize;
        let mut current_m = self.params.m;
        let mut last_failure = None;

        for grow_step in 0..=self.params.grow_limit {
            for retry_step in 0..self.params.retry_limit {
                attempts += 1;
                let attempt_index = ((grow_step as u64) << 32) | retry_step as u64;
                let seed = derive_attempt_seed(self.params.seed, attempt_index);

                match self.build_once(keys, current_m, seed) {
                    Ok(filter) => return Ok(filter),
                    Err(err) => last_failure = Some(err),
                }

                if matches!(self.params.mode, Mode::Homogeneous) {
                    break;
                }
            }

            if matches!(self.params.mode, Mode::Homogeneous) {
                break;
            }

            if grow_step < self.params.grow_limit {
                let w = self.params.w;
                current_m = (current_m * (w + 1)).div_ceil(w);
                debug_assert!(current_m >= self.params.w);
            }
        }

        Err(BuildError::ConstructionFailed {
            final_m: current_m,
            attempts,
            last_failure: last_failure.unwrap_or(ConstructionFailure::InconsistentEquation {
                key_index: 0,
                row_index: 0,
            }),
        })
    }

    fn build_once<K: std::hash::Hash>(
        &self,
        keys: &[K],
        m: usize,
        seed: u64,
    ) -> Result<RibbonFilter<S>, ConstructionFailure> {
        debug_assert!(m >= self.params.w);

        let stride_words = self.params.fingerprint_words();
        let fp_last_mask = self.params.fingerprint_last_word_mask();
        let mut occupied = vec![false; m];
        let mut coeff_lo = vec![0u64; m];
        let mut coeff_hi = vec![0u64; m];
        let mut rhs = vec![0u64; m * stride_words];

        let mut key_fp = vec![0u64; stride_words];

        for (key_index, key) in keys.iter().enumerate() {
            key_fp.fill(0);
            let equation = standard_equation_w64(
                &self.build_hasher,
                key,
                seed,
                &Params { m, ..self.params },
                &mut key_fp,
            );

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

        let mut z = vec![0u64; m * stride_words];
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

        Ok(RibbonFilter::new(
            built_params,
            self.build_hasher.clone(),
            z,
        ))
    }
}
