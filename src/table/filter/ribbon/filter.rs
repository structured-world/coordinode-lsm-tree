#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};
use core::hash::{BuildHasher, Hash};

use super::builder::Scratch;
#[cfg(feature = "ribbon-serde")]
use super::error::FilterReprError;
use super::hashing::{for_each_set_bit_u128_parts, standard_equation_w64, xor_words};
use super::params::Params;

#[cfg(feature = "ribbon-serde")]
const RIBBON_FILTER_FORMAT_VERSION: u8 = 1;

/// On-the-wire / in-memory snapshot of a built `RibbonFilter`.
///
/// `z` is the band-solution matrix as a flat `Vec<u64>`. Length is
/// `params.m * params.fingerprint_words()` and the on-disk byte length
/// is `z.len() * 8`. We use a plain `Vec<u64>` rather than `BitVec<u64>`
/// because `bitvec`'s `u64: BitStore` impl is gated on
/// `target_has_atomic = "64"` — on 32-bit targets (i686, riscv32, etc.)
/// the bound fails and the crate doesn't build. Ribbon's algorithm
/// stores full `u64` words anyway; the `BitVec` wrapper was upstream
/// flavour, not a load-bearing component.
#[cfg(feature = "ribbon-serde")]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RibbonFilterRepr {
    pub version: u8,
    pub params: Params,
    pub z: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct RibbonFilter<S> {
    params: Params,
    build_hasher: S,
    z: Vec<u64>,
    stride_words: usize,
}

impl<S> RibbonFilter<S>
where
    S: BuildHasher + Clone,
{
    pub(crate) fn new(params: Params, build_hasher: S, z: Vec<u64>) -> Self {
        let stride_words = params.fingerprint_words();
        Self {
            params,
            build_hasher,
            z,
            stride_words,
        }
    }

    pub fn params(&self) -> Params {
        self.params
    }

    pub fn new_scratch(&self) -> Scratch {
        Scratch::new(self.stride_words)
    }

    pub fn contains<Q: Hash + ?Sized>(&self, key: &Q) -> bool {
        let mut scratch = self.new_scratch();
        self.contains_in(key, &mut scratch)
    }

    pub fn contains_in<Q: Hash + ?Sized>(&self, key: &Q, scratch: &mut Scratch) -> bool {
        // Hard runtime check, not debug_assert: a mismatched Scratch in
        // release would silently truncate via `xor_words` (shorter slice
        // wins the zip) and could produce false negatives. The caller
        // contract is "Scratch came from RibbonFilter::new_scratch on
        // this same filter" — violating it is a programmer error worth
        // panicking on in production.
        assert_eq!(
            scratch.fingerprint.len(),
            self.stride_words,
            "scratch fingerprint width mismatch; use RibbonFilter::new_scratch() from this filter",
        );
        assert_eq!(
            scratch.acc.len(),
            self.stride_words,
            "scratch accumulator width mismatch; use RibbonFilter::new_scratch() from this filter",
        );
        scratch.reset();

        let equation = standard_equation_w64(
            &self.build_hasher,
            key,
            self.params.seed,
            &self.params,
            &mut scratch.fingerprint,
        );

        for_each_set_bit_u128_parts(equation.coeff_lo, equation.coeff_hi, |offset| {
            let row_index = equation.start + offset;
            if row_index < self.params.m {
                let row = self.z_row(row_index);
                xor_words(&mut scratch.acc, row);
            }
        });

        scratch.acc == scratch.fingerprint
    }

    fn z_row(&self, row: usize) -> &[u64] {
        let start = row * self.stride_words;
        let end = start + self.stride_words;
        &self.z[start..end]
    }

    /// Borrowed access to the raw solution-matrix words.
    ///
    /// Length is `m * stride_words`. Each chunk of `stride_words` u64s
    /// is one row's fingerprint bits in LSB-first order. Used by the
    /// BuRR wire-format serializer to write the matrix as packed
    /// little-endian bytes.
    pub(crate) fn z_raw_words(&self) -> &[u64] {
        &self.z
    }

    #[cfg(feature = "ribbon-serde")]
    pub fn to_repr(&self) -> RibbonFilterRepr {
        RibbonFilterRepr {
            version: RIBBON_FILTER_FORMAT_VERSION,
            params: self.params,
            z: self.z.clone(),
        }
    }

    #[cfg(feature = "ribbon-serde")]
    pub fn from_repr(repr: RibbonFilterRepr, build_hasher: S) -> Result<Self, FilterReprError> {
        if repr.version != RIBBON_FILTER_FORMAT_VERSION {
            return Err(FilterReprError::UnsupportedVersion {
                found: repr.version,
                expected: RIBBON_FILTER_FORMAT_VERSION,
            });
        }

        repr.params
            .validate()
            .map_err(FilterReprError::InvalidParams)?;

        let stride_words = repr.params.fingerprint_words();
        let expected_words = repr
            .params
            .m
            .checked_mul(stride_words)
            .ok_or(FilterReprError::StorageLengthOverflow)?;

        if repr.z.len() != expected_words {
            return Err(FilterReprError::InvalidStorageWords {
                found: repr.z.len(),
                expected: expected_words,
            });
        }

        Ok(Self {
            params: repr.params,
            build_hasher,
            stride_words,
            z: repr.z,
        })
    }
}
