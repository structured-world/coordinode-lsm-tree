#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "ribbon-serde")]
use super::error::FilterReprError;
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
pub struct RibbonFilter {
    params: Params,
    z: Vec<u64>,
}

impl RibbonFilter {
    pub(crate) fn new(params: Params, z: Vec<u64>) -> Self {
        Self { params, z }
    }

    pub fn params(&self) -> Params {
        self.params
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
    pub fn from_repr(repr: RibbonFilterRepr) -> Result<Self, FilterReprError> {
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
            z: repr.z,
        })
    }
}
