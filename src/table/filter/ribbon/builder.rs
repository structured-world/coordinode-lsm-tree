use std::hash::BuildHasher;

use super::error::{BuildError, ConstructionFailure};
use super::filter::RibbonFilter;
use super::hashing::{
    SplitMix64, StandardEquation, derive_attempt_seed, for_each_set_bit_u128_parts,
    standard_equation_from_hash, standard_equation_w64, xor_words,
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

    /// Build a Ribbon filter using a CALLER-PROVIDED seed verbatim — no
    /// `derive_attempt_seed` mixing on top. This is the entry point BuRR
    /// uses to keep the threshold-decision seed and the actual
    /// construction seed identical, so the per-block bump decisions made
    /// from precomputed equations agree with the equations the ribbon
    /// stores for its kept keys.
    ///
    /// No retry budget: a single attempt with the given seed. Caller is
    /// responsible for sizing `m` (via `Params::m`) generously enough that
    /// the banded solver succeeds with the keys provided.
    pub(crate) fn build_with_seed_verbatim<K: std::hash::Hash>(
        &self,
        keys: &[K],
        seed: u64,
        m: usize,
    ) -> Result<RibbonFilter<S>, BuildError> {
        self.params.validate().map_err(BuildError::InvalidParams)?;
        self.build_once(keys, m, seed)
            .map_err(|failure| BuildError::ConstructionFailed {
                final_m: m,
                attempts: 1,
                last_failure: failure,
            })
    }

    /// Build a Ribbon filter from already-hashed keys (each `u64` is
    /// treated as the value `BuildHasher::hash_one(key)` would have
    /// produced). Verbatim seed, no retry.
    ///
    /// Used by BuRR when the LSM has already computed a stable u64
    /// hash for each key (via `crate::hash::hash64` / xxh3) — running
    /// the BuildHasher again would just double-hash the same bytes.
    pub(crate) fn build_with_seed_verbatim_from_hashes(
        &self,
        hashes: &[u64],
        seed: u64,
        m: usize,
    ) -> Result<RibbonFilter<S>, BuildError> {
        self.params.validate().map_err(BuildError::InvalidParams)?;
        self.build_once_from_hashes(hashes, m, seed)
            .map_err(|failure| BuildError::ConstructionFailed {
                final_m: m,
                attempts: 1,
                last_failure: failure,
            })
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
                // Unchecked multiplication can wrap in release builds for
                // caller-supplied `m` near usize::MAX, leaving `current_m`
                // smaller than `w` and breaking later invariants. Fail
                // construction explicitly when the grown size would
                // overflow.
                let Some(grown) = current_m.checked_mul(w + 1).map(|raw| raw.div_ceil(w)) else {
                    return Err(BuildError::ConstructionFailed {
                        final_m: current_m,
                        attempts,
                        last_failure: last_failure.unwrap_or(
                            ConstructionFailure::InconsistentEquation {
                                key_index: 0,
                                row_index: 0,
                            },
                        ),
                    });
                };
                current_m = grown;
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
        // `m * stride_words` would overflow `usize` if `m` is set
        // unreasonably large; allocate via the checked product and bail
        // before the vec! call panics.
        let total_words = m
            .checked_mul(stride_words)
            .ok_or(ConstructionFailure::StorageLengthOverflow { m, stride_words })?;
        let fp_last_mask = self.params.fingerprint_last_word_mask();
        let mut occupied = vec![false; m];
        let mut coeff_lo = vec![0u64; m];
        let mut coeff_hi = vec![0u64; m];
        let mut rhs = vec![0u64; total_words];

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

        Ok(RibbonFilter::new(
            built_params,
            self.build_hasher.clone(),
            z,
        ))
    }

    /// Variant of [`Self::build_once`] that takes pre-computed key hashes
    /// instead of `Hash` keys. Otherwise identical algorithm.
    ///
    /// Used by BuRR through `build_with_seed_verbatim_from_hashes` so the
    /// LSM-side stable u64 hash (xxh3 / `crate::hash::hash64`) flows
    /// straight into the banded solver without re-hashing through the
    /// `BuildHasher`.
    fn build_once_from_hashes(
        &self,
        hashes: &[u64],
        m: usize,
        seed: u64,
    ) -> Result<RibbonFilter<S>, ConstructionFailure> {
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

        Ok(RibbonFilter::new(
            built_params,
            self.build_hasher.clone(),
            z,
        ))
    }
}
