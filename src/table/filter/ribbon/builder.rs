#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::error::{BuildError, ConstructionFailure};
use super::filter::RibbonFilter;
#[cfg(test)]
use super::hashing::standard_equation_from_hash;
use super::hashing::{SplitMix64, StandardEquation};
use super::params::{Mode, Params};

#[derive(Debug, Clone)]
pub struct RibbonBuilder {
    params: Params,
}

/// The working rows of a solve, kept between the solves of one filter's
/// layers: each layer solves fewer slots than the one before, so the rows
/// the first allocates serve them all.
#[derive(Default)]
pub(crate) struct SolveScratch {
    coeff_lo: Vec<u64>,
    coeff_hi: Vec<u64>,
    rhs: Vec<u64>,
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
    #[cfg(test)]
    pub(crate) fn build_with_seed_verbatim_from_hashes(
        &self,
        hashes: &[u64],
        seed: u64,
        m: usize,
    ) -> Result<RibbonFilter, BuildError> {
        let layer_params = Params { m, ..self.params };
        let equations: Vec<StandardEquation> = hashes
            .iter()
            .map(|hash| standard_equation_from_hash(*hash, seed, &layer_params))
            .collect();
        self.build_from_equations(&equations, None, m, seed, &mut SolveScratch::default())
    }

    /// Build a Ribbon over the keys whose equations are `equations`, derived
    /// from their hashes under `seed` and this builder's parameters at `m`
    /// slots. Verbatim seed, no retry.
    ///
    /// With `values`, the ribbon maps each key to a caller-supplied r-bit
    /// value (a *retrieval* ribbon) instead of its hash-derived membership
    /// fingerprint: `values[i]` is the value stored for `equations[i]`, and
    /// must already fit in `r` bits. The band placement is still derived
    /// from the key hash, so the solve is identical to the membership path
    /// apart from the RHS — a later dot-product query recovers `values[i]`
    /// exactly for a key in the set (garbage for an absent key, which the
    /// caller verifies separately).
    ///
    /// `scratch` holds the solve's working rows, to be reused by the next.
    pub(crate) fn build_from_equations(
        &self,
        equations: &[StandardEquation],
        values: Option<&[u64]>,
        m: usize,
        seed: u64,
        scratch: &mut SolveScratch,
    ) -> Result<RibbonFilter, BuildError> {
        self.params.validate().map_err(BuildError::InvalidParams)?;
        self.solve(equations, values, m, seed, scratch)
            .map_err(|failure| BuildError::ConstructionFailed {
                final_m: m,
                attempts: 1,
                last_failure: failure,
            })
    }

    /// Solves a single ribbon over the given equations.
    ///
    /// The band placement and Gaussian-elimination solve are shared verbatim
    /// between the membership (`values = None`, RHS = hash-derived
    /// fingerprint) and retrieval (`values = Some`, RHS = caller locator)
    /// paths — only the per-key RHS differs — so the two cannot drift.
    fn solve(
        &self,
        equations: &[StandardEquation],
        values: Option<&[u64]>,
        m: usize,
        seed: u64,
        scratch: &mut SolveScratch,
    ) -> Result<RibbonFilter, ConstructionFailure> {
        debug_assert!(m >= self.params.w);
        debug_assert!(
            values.is_none_or(|values| values.len() == equations.len()),
            "retrieval RHS values must be parallel to the equations",
        );

        let fp_mask = self.params.fingerprint_mask();
        // A row is occupied exactly when its coefficient has bit 0 set: every
        // equation's coefficient has it, and every elimination step shifts
        // the leading set bit back to bit 0 before the row is stored. So only
        // the low coefficient words start cleared; the high words and the RHS
        // are read only at occupied rows, which wrote them first.
        let SolveScratch {
            coeff_lo,
            coeff_hi,
            rhs,
        } = scratch;
        coeff_lo.clear();
        coeff_lo.resize(m, 0);
        coeff_hi.resize(m, 0);
        // One row of the solution matrix is one word: `Params::MAX_R` caps the
        // fingerprint at 64 bits, so the RHS and the running row value stay in
        // registers through the whole solve.
        rhs.resize(m, 0);

        for (key_index, equation) in equations.iter().enumerate() {
            let mut i = equation.start;
            let mut c_lo = equation.coeff_lo;
            let mut c_hi = equation.coeff_hi;
            // Retrieval ribbon: the caller's r-bit value replaces the
            // hash-derived fingerprint as the RHS. The band (coeff/start) is
            // still hash-derived, so the solve is unchanged; only what it
            // solves *for* differs.
            let mut b = match values {
                None => equation.fingerprint,
                Some(values) => values[key_index] & fp_mask,
            };

            if i >= m {
                return Err(ConstructionFailure::OutOfBounds {
                    key_index: Some(key_index),
                    row_index: i,
                    m,
                });
            }

            loop {
                if coeff_lo[i] & 1 == 0 {
                    coeff_lo[i] = c_lo;
                    coeff_hi[i] = c_hi;
                    rhs[i] = b;
                    break;
                }

                c_lo ^= coeff_lo[i];
                c_hi ^= coeff_hi[i];
                b ^= rhs[i];

                if c_lo == 0 && c_hi == 0 {
                    if b == 0 {
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

        let mut z = alloc::vec![0u64; m];
        if matches!(self.params.mode, Mode::Homogeneous) {
            let mut rng = SplitMix64::new(seed ^ 0xD1B5_4A32_D192_ED03);
            for (slot, lo) in z.iter_mut().zip(coeff_lo.iter()) {
                if lo & 1 != 0 {
                    continue;
                }
                *slot = rng.next_u64() & fp_mask;
            }
        }

        for i in (0..m).rev() {
            if coeff_lo[i] & 1 == 0 {
                continue;
            }
            // The row value is accumulated in a register and stored once. The
            // set bits above the diagonal are walked in place: collecting them
            // into a `Vec` was a heap allocation per row, and XORing straight
            // into `z[i]` made every bit a store the next one had to reload.
            // Each bit names a row strictly below this one (the diagonal bit is
            // masked off), so every source is already final.
            let mut row = rhs[i];
            let mut upper_lo = coeff_lo[i] & !1u64;
            let mut upper_hi = coeff_hi[i];
            while upper_lo != 0 || upper_hi != 0 {
                let offset = if upper_lo != 0 {
                    let bit = upper_lo.trailing_zeros() as usize;
                    upper_lo &= upper_lo - 1;
                    bit
                } else {
                    let bit = upper_hi.trailing_zeros() as usize;
                    upper_hi &= upper_hi - 1;
                    64 + bit
                };
                let row_index = i + offset;
                let Some(other) = z.get(row_index) else {
                    return Err(ConstructionFailure::OutOfBounds {
                        key_index: None,
                        row_index,
                        m,
                    });
                };
                row ^= *other;
            }
            z[i] = row & fp_mask;
        }
        let mut built_params = self.params;
        built_params.m = m;
        built_params.seed = seed;

        Ok(RibbonFilter::new(built_params, z))
    }
}
