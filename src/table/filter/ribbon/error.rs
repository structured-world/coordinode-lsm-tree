use core::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum ParamError {
    ZeroM,
    ZeroN,
    ZeroWidth,
    WidthTooLarge { w: usize, max: usize },
    ZeroFingerprintBits,
    WidthExceedsM { m: usize, w: usize },
    ZeroRetryLimit,
    InvalidFalsePositiveRate { fpr: f64 },
    InvalidOverhead { overhead: f64 },
}

impl fmt::Display for ParamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParamError::ZeroM => write!(f, "m must be greater than zero"),
            ParamError::ZeroN => write!(f, "n must be greater than zero"),
            ParamError::ZeroWidth => write!(f, "w must be greater than zero"),
            ParamError::WidthTooLarge { w, max } => {
                write!(f, "w ({w}) must be less than or equal to {max}")
            }
            ParamError::ZeroFingerprintBits => write!(f, "r must be greater than zero"),
            ParamError::WidthExceedsM { m, w } => {
                write!(f, "w ({w}) must be less than or equal to m ({m})")
            }
            ParamError::ZeroRetryLimit => write!(f, "retry_limit must be greater than zero"),
            ParamError::InvalidFalsePositiveRate { fpr } => {
                write!(f, "false positive rate must be in (0, 1), got {fpr}")
            }
            ParamError::InvalidOverhead { overhead } => {
                write!(f, "overhead must be in [0, 10], got {overhead}")
            }
        }
    }
}

impl core::error::Error for ParamError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstructionFailure {
    InconsistentEquation {
        key_index: usize,
        row_index: usize,
    },
    OutOfBounds {
        key_index: Option<usize>,
        row_index: usize,
        m: usize,
    },
    /// `m * stride_words` overflowed `usize`. Caller passed an
    /// unreasonably large `m` (or `r` is mistuned). Returned before any
    /// storage is allocated, so this is a clean error rather than a
    /// panic on the `vec!` line.
    StorageLengthOverflow {
        m: usize,
        stride_words: usize,
    },
}

impl fmt::Display for ConstructionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConstructionFailure::InconsistentEquation {
                key_index,
                row_index,
            } => write!(
                f,
                "inconsistent equation while inserting key at index {key_index} near row {row_index}"
            ),
            ConstructionFailure::OutOfBounds {
                key_index,
                row_index,
                m,
            } => {
                if let Some(key_index) = key_index {
                    write!(
                        f,
                        "row index {row_index} out of bounds for m={m} while inserting key at index {key_index}"
                    )
                } else {
                    write!(
                        f,
                        "row index {row_index} out of bounds for m={m} during back-substitution"
                    )
                }
            }
            ConstructionFailure::StorageLengthOverflow { m, stride_words } => write!(
                f,
                "m * stride_words overflows usize: m={m} stride_words={stride_words}",
            ),
        }
    }
}

impl core::error::Error for ConstructionFailure {}

#[derive(Debug, Clone, PartialEq)]
pub enum BuildError {
    InvalidParams(ParamError),
    ConstructionFailed {
        final_m: usize,
        attempts: usize,
        last_failure: ConstructionFailure,
    },
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildError::InvalidParams(err) => write!(f, "invalid parameters: {err}"),
            BuildError::ConstructionFailed {
                final_m,
                attempts,
                last_failure,
            } => write!(
                f,
                "construction failed after {attempts} attempt(s) at m={final_m}: {last_failure}"
            ),
        }
    }
}

impl core::error::Error for BuildError {}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterReprError {
    UnsupportedVersion { found: u8, expected: u8 },
    InvalidParams(ParamError),
    StorageLengthOverflow,
    InvalidStorageWords { found: usize, expected: usize },
    InvalidStorageBits { found: usize, expected: usize },
}

impl fmt::Display for FilterReprError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterReprError::UnsupportedVersion { found, expected } => write!(
                f,
                "unsupported RibbonFilter version {found}, expected {expected}"
            ),
            FilterReprError::InvalidParams(err) => {
                write!(
                    f,
                    "invalid parameters in RibbonFilter representation: {err}"
                )
            }
            FilterReprError::StorageLengthOverflow => {
                write!(f, "RibbonFilter representation storage length overflow")
            }
            FilterReprError::InvalidStorageWords { found, expected } => write!(
                f,
                "invalid RibbonFilter storage word length {found}; expected {expected}"
            ),
            FilterReprError::InvalidStorageBits { found, expected } => write!(
                f,
                "invalid RibbonFilter storage bit length {found}; expected {expected}"
            ),
        }
    }
}

impl core::error::Error for FilterReprError {}

#[cfg(test)]
mod tests;
