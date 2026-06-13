use core::fmt;

use super::error::ParamError;

// `ribbon-serde` is wired in Cargo.toml as `["dep:serde"]` — turning it
// on enables the cfg_attr-gated Serialize/Deserialize derives below and
// on `RibbonFilterRepr` in filter.rs. The crate does not consume the
// serde repr internally; the gate is preserved for callers that want
// an in-memory snapshot of a built filter. (bitvec was previously a
// dep here; it was dropped for 32-bit cross-arch compatibility and the
// Repr now serialises a plain `Vec<u64>`.)
#[cfg_attr(feature = "ribbon-serde", derive(serde::Serialize, serde::Deserialize))]
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

#[cfg_attr(feature = "ribbon-serde", derive(serde::Serialize, serde::Deserialize))]
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
            // `build_with_seed_verbatim_from_hashes` (single verbatim-seed
            // attempt), so retry/grow are inert for the in-crate caller.
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

    pub fn fingerprint_words(&self) -> usize {
        self.r.div_ceil(64)
    }

    pub fn fingerprint_last_word_mask(&self) -> u64 {
        let rem = self.r % 64;
        if rem == 0 {
            u64::MAX
        } else {
            (1u64 << rem) - 1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_params() -> Params {
        Params::new(128, 64, 8, Mode::Standard).expect("valid params")
    }

    #[test]
    fn mode_display_matches_expected_strings() {
        assert_eq!(Mode::Standard.to_string(), "standard");
        assert_eq!(Mode::Homogeneous.to_string(), "homogeneous");
    }

    #[test]
    fn new_accepts_valid_params_and_pins_retry_default() {
        let p = ok_params();
        assert_eq!(p.m, 128);
        assert_eq!(p.w, 64);
        assert_eq!(p.r, 8);
        // Default retry_limit is 8 (endian-portability hedge).
        assert_eq!(p.retry_limit, 8);
        assert_eq!(p.grow_limit, 0);
    }

    #[test]
    fn new_rejects_zero_m() {
        assert_eq!(
            Params::new(0, 64, 8, Mode::Standard),
            Err(ParamError::ZeroM)
        );
    }

    #[test]
    fn new_rejects_zero_w() {
        assert_eq!(
            Params::new(128, 0, 8, Mode::Standard),
            Err(ParamError::ZeroWidth)
        );
    }

    #[test]
    fn new_rejects_w_above_max() {
        assert!(matches!(
            Params::new(256, Params::MAX_W + 1, 8, Mode::Standard),
            Err(ParamError::WidthTooLarge { .. })
        ));
    }

    #[test]
    fn new_rejects_zero_r() {
        assert_eq!(
            Params::new(128, 64, 0, Mode::Standard),
            Err(ParamError::ZeroFingerprintBits)
        );
    }

    #[test]
    fn new_rejects_w_above_m() {
        assert!(matches!(
            Params::new(32, 64, 8, Mode::Standard),
            Err(ParamError::WidthExceedsM { .. })
        ));
    }

    #[test]
    fn with_seed_preserves_other_fields() {
        let p = ok_params().with_seed(0xDEAD_BEEF);
        assert_eq!(p.seed, 0xDEAD_BEEF);
        assert_eq!(p.m, 128);
        assert_eq!(p.w, 64);
    }

    #[test]
    fn with_retry_limit_rejects_zero() {
        assert_eq!(
            ok_params().with_retry_limit(0),
            Err(ParamError::ZeroRetryLimit)
        );
    }

    #[test]
    fn with_retry_limit_accepts_positive() {
        let p = ok_params().with_retry_limit(3).expect("valid");
        assert_eq!(p.retry_limit, 3);
    }

    #[test]
    fn with_retry_policy_sets_both_fields() {
        let p = ok_params().with_retry_policy(2, 5).expect("valid");
        assert_eq!(p.retry_limit, 2);
        assert_eq!(p.grow_limit, 5);
    }

    #[test]
    fn with_retry_policy_rejects_zero_retry_limit() {
        assert_eq!(
            ok_params().with_retry_policy(0, 5),
            Err(ParamError::ZeroRetryLimit)
        );
    }

    #[test]
    fn r_from_fpr_rejects_zero_and_one() {
        assert!(matches!(
            Params::r_from_fpr(0.0),
            Err(ParamError::InvalidFalsePositiveRate { .. })
        ));
        assert!(matches!(
            Params::r_from_fpr(1.0),
            Err(ParamError::InvalidFalsePositiveRate { .. })
        ));
        assert!(matches!(
            Params::r_from_fpr(-0.1),
            Err(ParamError::InvalidFalsePositiveRate { .. })
        ));
    }

    #[test]
    fn r_from_fpr_returns_ceil_neg_log2_floored_at_one() {
        // fpr = 0.5 → -log2 = 1 → r = 1
        assert_eq!(Params::r_from_fpr(0.5).unwrap(), 1);
        // fpr = 0.01 → -log2 ≈ 6.64 → ceil = 7
        assert_eq!(Params::r_from_fpr(0.01).unwrap(), 7);
        // fpr very close to 1.0 → -log2 ≈ 0 → max(0, 1) = 1
        assert_eq!(Params::r_from_fpr(0.999).unwrap(), 1);
    }

    #[test]
    fn from_expected_items_rejects_zero_n() {
        assert_eq!(
            Params::from_expected_items(0, 0.1, 64, 8, Mode::Standard),
            Err(ParamError::ZeroN)
        );
    }

    #[test]
    fn from_expected_items_rejects_overhead_out_of_range() {
        assert!(matches!(
            Params::from_expected_items(100, -0.1, 64, 8, Mode::Standard),
            Err(ParamError::InvalidOverhead { .. })
        ));
        assert!(matches!(
            Params::from_expected_items(100, 11.0, 64, 8, Mode::Standard),
            Err(ParamError::InvalidOverhead { .. })
        ));
    }

    #[test]
    fn from_expected_items_floors_m_at_w() {
        // n=1, overhead=0 → raw m = 1, floors to w = 64.
        let p = Params::from_expected_items(1, 0.0, 64, 8, Mode::Standard).expect("valid");
        assert_eq!(p.m, 64);
        assert_eq!(p.w, 64);
    }

    #[test]
    fn start_range_is_m_minus_w_plus_one() {
        let p = ok_params();
        assert_eq!(p.start_range(), 128 - 64 + 1);
    }

    #[test]
    fn fingerprint_words_round_up_for_non_multiple_of_64() {
        assert_eq!(
            Params::new(128, 64, 1, Mode::Standard)
                .unwrap()
                .fingerprint_words(),
            1
        );
        assert_eq!(
            Params::new(128, 64, 64, Mode::Standard)
                .unwrap()
                .fingerprint_words(),
            1
        );
        assert_eq!(
            Params::new(128, 64, 65, Mode::Standard)
                .unwrap()
                .fingerprint_words(),
            2
        );
        assert_eq!(
            Params::new(256, 64, 128, Mode::Standard)
                .unwrap()
                .fingerprint_words(),
            2
        );
    }

    #[test]
    fn fingerprint_last_word_mask_full_when_r_multiple_of_64() {
        let p = Params::new(128, 64, 64, Mode::Standard).unwrap();
        assert_eq!(p.fingerprint_last_word_mask(), u64::MAX);
    }

    #[test]
    fn fingerprint_last_word_mask_low_bits_when_r_not_multiple_of_64() {
        let p = Params::new(128, 64, 5, Mode::Standard).unwrap();
        assert_eq!(p.fingerprint_last_word_mask(), 0b11111);
    }
}
