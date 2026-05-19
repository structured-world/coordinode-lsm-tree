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

impl std::error::Error for BurrBuildError {}

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

impl std::error::Error for BurrConstructionFailure {}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn invalid_params_display() {
        let err = BurrBuildError::InvalidParams("n must be > 0");
        let s = format!("{err}");
        assert!(s.contains("invalid params"), "got: {s}");
        assert!(s.contains("n must be > 0"), "got: {s}");
    }

    #[test]
    fn layer_exhaustion_display() {
        let err = BurrBuildError::LayerExhaustion {
            layers_attempted: 4,
            remaining_keys: 17,
        };
        let s = format!("{err}");
        assert!(s.contains("4 layers"), "got: {s}");
        assert!(s.contains("17 keys"), "got: {s}");
    }

    #[test]
    fn ribbon_layer_failed_display() {
        let ribbon_err =
            RibbonBuildError::InvalidParams(super::super::super::error::ParamError::ZeroM);
        let err = BurrBuildError::RibbonLayerFailed {
            layer_index: 2,
            ribbon_error: ribbon_err,
        };
        let s = format!("{err}");
        assert!(s.contains("layer 2"), "got: {s}");
        assert!(s.contains("ribbon build failed"), "got: {s}");
    }

    #[test]
    fn burr_build_error_implements_std_error() {
        let err = BurrBuildError::InvalidParams("x");
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn block_overflow_display() {
        let err = BurrConstructionFailure::BlockOverflow {
            layer_index: 1,
            block_index: 42,
        };
        let s = format!("{err}");
        assert!(s.contains("layer 1"), "got: {s}");
        assert!(s.contains("block 42"), "got: {s}");
    }

    #[test]
    fn construction_failure_implements_std_error() {
        let err = BurrConstructionFailure::BlockOverflow {
            layer_index: 0,
            block_index: 0,
        };
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn construction_failure_is_clone_and_debug() {
        let err = BurrConstructionFailure::BlockOverflow {
            layer_index: 1,
            block_index: 2,
        };
        let cloned = err.clone();
        assert!(matches!(
            cloned,
            BurrConstructionFailure::BlockOverflow {
                layer_index: 1,
                block_index: 2
            }
        ));
        let debug = format!("{err:?}");
        assert!(debug.contains("BlockOverflow"), "got: {debug}");
    }
}
