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
    let ribbon_err = RibbonBuildError::InvalidParams(super::super::super::error::ParamError::ZeroM);
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
    let _: &dyn core::error::Error = &err;
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
    let _: &dyn core::error::Error = &err;
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
