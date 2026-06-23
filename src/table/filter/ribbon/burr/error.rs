use core::fmt;

use super::super::error::BuildError as RibbonBuildError;

/// Errors that can occur while building a BuRR filter.
#[derive(Debug)]
pub enum BurrBuildError {
    /// Configuration error: parameters reject during `BurrParams::new` or
    /// during derivation (e.g. zero keys, illegal FPR).
    InvalidParams(&'static str),
    /// Construction reached the maximum allowed layer count without
    /// absorbing all keys. In a correctly-tuned BuRR this is impossible
    /// (the last layer has full capacity by construction); reaching this
    /// indicates a parameter mistuning bug.
    LayerExhaustion {
        layers_attempted: usize,
        remaining_keys: usize,
    },
    /// An underlying Ribbon layer failed to build despite the threshold
    /// guarantee that its key population should fit. Also indicates a
    /// parameter mistuning bug — included for completeness so failures
    /// surface with diagnostic context.
    RibbonLayerFailed {
        layer_index: usize,
        ribbon_error: RibbonBuildError,
    },
}

impl fmt::Display for BurrBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidParams(msg) => write!(f, "BuRR invalid params: {msg}"),
            Self::LayerExhaustion {
                layers_attempted,
                remaining_keys,
            } => write!(
                f,
                "BuRR exhausted {layers_attempted} layers with {remaining_keys} keys still bumped (parameter mistuning)",
            ),
            Self::RibbonLayerFailed {
                layer_index,
                ribbon_error,
            } => write!(
                f,
                "BuRR layer {layer_index} ribbon build failed: {ribbon_error:?}",
            ),
        }
    }
}

impl core::error::Error for BurrBuildError {}

/// Detailed construction failure for diagnostics.
#[derive(Debug, Clone)]
pub enum BurrConstructionFailure {
    /// A specific block's chosen threshold could not absorb its key
    /// population — i.e. even at threshold=0 (all keys bumped) the
    /// remaining slot count was somehow exceeded. Should be impossible
    /// given correct accounting; surfaced as a sentinel.
    BlockOverflow {
        layer_index: usize,
        block_index: usize,
    },
}

impl fmt::Display for BurrConstructionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockOverflow {
                layer_index,
                block_index,
            } => write!(
                f,
                "BuRR layer {layer_index} block {block_index} could not absorb its keys at any threshold",
            ),
        }
    }
}

impl core::error::Error for BurrConstructionFailure {}

#[cfg(test)]
mod tests;
