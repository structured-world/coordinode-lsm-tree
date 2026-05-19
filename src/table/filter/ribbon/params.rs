use core::fmt;

use super::error::ParamError;

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
            retry_limit: 1,
            grow_limit: 0,
        };
        params.validate()?;
        Ok(params)
    }

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
        let r = (-fpr.log2()).ceil() as usize;
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

        let m = ((n as f64) * (1.0 + overhead)).ceil() as usize;
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
