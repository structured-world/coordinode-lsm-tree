use core::fmt;

use super::error::ParamError;

// `ribbon-serde` gives `Mode`, `Params` and `RibbonFilterRepr`
// Serialize/Deserialize (in `repr_serde`) for callers that want an
// in-memory snapshot of a built filter; the crate itself does not consume
// that repr.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Standard,
    Homogeneous,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Mode::Standard => write!(f, "standard"),
            Mode::Homogeneous => write!(f, "homogeneous"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Params {
    pub m: usize,
    pub w: usize,
    pub r: usize,
    pub mode: Mode,
    pub seed: u64,
    pub retry_limit: usize,
    pub grow_limit: usize,
}

impl Params {
    pub const MAX_W: usize = 128;

    /// Fingerprint width ceiling. One solution row is one `u64`, which is
    /// what the BuRR layer above builds (`BurrParams::r` is a `u8` validated
    /// to `1..=64`) and what the wire decoder accepts, so the solver stores a
    /// single word per row and never a multi-word stride.
    ///
    /// A wider `r` was accepted here before, and a `RibbonFilterRepr` captured
    /// through the `ribbon-serde` feature could in principle carry one, stored
    /// as `m * r.div_ceil(64)` words. Such a repr is now refused, twice over
    /// and by name: [`Self::validate`] reports
    /// [`ParamError::FingerprintTooWide`](super::error::ParamError::FingerprintTooWide)
    /// with the offending `r`, and the row count no longer matches `m`. It can
    /// therefore never be misread as a narrow one, which is the property that
    /// matters. The repr version is deliberately NOT bumped for this: the
    /// version guards the encoding, and rejecting every `r <= 64` repr — the
    /// only kind any supported path produces — to give a different error on a
    /// width nothing in this crate can build would trade a real case for a
    /// hypothetical one.
    pub const MAX_R: usize = 64;

    pub fn new(m: usize, w: usize, r: usize, mode: Mode) -> Result<Self, ParamError> {
        let params = Self {
            m,
            w,
            r,
            mode,
            seed: 0,
            // 8 attempts: Standard Ribbon's GF(2) elimination can hit
            // InconsistentEquation on the first seed/key combination
            // for some inputs. The Rust std `DefaultHasher` hashes
            // `u64` keys via `to_ne_bytes`, so the equation system is
            // host-endianness-sensitive — a single-attempt build that
            // succeeds on x86_64 (LE) may fail on powerpc64 (BE). 8
            // attempts via derived seeds makes the construction
            // platform-invariant in practice without changing the
            // seed-determinism contract (consumers can still pin a
            // seed; the retry just iterates derived seeds within that
            // seed family).
            //
            // BuRR's build path bypasses retry entirely via
            // `build_from_equations` (single verbatim-seed attempt), so
            // retry/grow are inert for the in-crate caller.
            retry_limit: 8,
            grow_limit: 0,
        };
        params.validate()?;
        Ok(params)
    }

    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn with_retry_limit(mut self, retry_limit: usize) -> Result<Self, ParamError> {
        self.retry_limit = retry_limit;
        self.validate()?;
        Ok(self)
    }

    pub fn with_retry_policy(
        mut self,
        retry_limit: usize,
        grow_limit: usize,
    ) -> Result<Self, ParamError> {
        self.retry_limit = retry_limit;
        self.grow_limit = grow_limit;
        self.validate()?;
        Ok(self)
    }

    pub fn r_from_fpr(fpr: f64) -> Result<usize, ParamError> {
        if !(0.0 < fpr && fpr < 1.0) {
            return Err(ParamError::InvalidFalsePositiveRate { fpr });
        }
        let r = crate::f64_ceil(-crate::f64_log2(fpr)) as usize;
        Ok(r.max(1))
    }

    pub fn from_expected_items(
        n: usize,
        overhead: f64,
        w: usize,
        r: usize,
        mode: Mode,
    ) -> Result<Self, ParamError> {
        if n == 0 {
            return Err(ParamError::ZeroN);
        }
        if !(0.0..=10.0).contains(&overhead) {
            return Err(ParamError::InvalidOverhead { overhead });
        }

        let m = crate::f64_ceil((n as f64) * (1.0 + overhead)) as usize;
        Self::new(m.max(w), w, r, mode)
    }

    pub fn validate(&self) -> Result<(), ParamError> {
        if self.m == 0 {
            return Err(ParamError::ZeroM);
        }
        if self.w == 0 {
            return Err(ParamError::ZeroWidth);
        }
        if self.w > Self::MAX_W {
            return Err(ParamError::WidthTooLarge {
                w: self.w,
                max: Self::MAX_W,
            });
        }
        if self.r == 0 {
            return Err(ParamError::ZeroFingerprintBits);
        }
        if self.r > Self::MAX_R {
            return Err(ParamError::FingerprintTooWide {
                r: self.r,
                max: Self::MAX_R,
            });
        }
        if self.retry_limit == 0 {
            return Err(ParamError::ZeroRetryLimit);
        }
        if self.w > self.m {
            return Err(ParamError::WidthExceedsM {
                m: self.m,
                w: self.w,
            });
        }
        Ok(())
    }

    pub fn start_range(&self) -> usize {
        self.m - self.w + 1
    }

    /// Mask of the low `r` bits: one row of the solution matrix is exactly
    /// one `u64`, since [`Self::MAX_R`] caps the fingerprint at a word.
    pub fn fingerprint_mask(&self) -> u64 {
        if self.r == 64 {
            u64::MAX
        } else {
            (1u64 << self.r) - 1
        }
    }
}

#[cfg(test)]
mod tests;
