use super::{BuildError, ConstructionFailure, FilterReprError, ParamError};

#[test]
fn param_error_display_is_actionable() {
    assert_eq!(ParamError::ZeroM.to_string(), "m must be greater than zero");
    assert_eq!(
        ParamError::WidthExceedsM { m: 3, w: 4 }.to_string(),
        "w (4) must be less than or equal to m (3)"
    );
}

#[test]
fn param_error_display_covers_every_variant() {
    // Each variant must have an actionable Display message — no
    // bare debug formatting, no missing context.
    assert!(ParamError::ZeroN.to_string().contains('n'));
    assert!(ParamError::ZeroWidth.to_string().contains('w'));
    assert!(
        ParamError::WidthTooLarge { w: 65, max: 64 }
            .to_string()
            .contains("65")
    );
    assert!(ParamError::ZeroFingerprintBits.to_string().contains('r'));
    assert!(
        ParamError::ZeroRetryLimit
            .to_string()
            .contains("retry_limit")
    );
    let fpr_msg = ParamError::InvalidFalsePositiveRate { fpr: 1.5 }.to_string();
    assert!(fpr_msg.contains("1.5"));
    let oh_msg = ParamError::InvalidOverhead { overhead: -1.0 }.to_string();
    assert!(oh_msg.contains("-1"));
}

#[test]
fn build_error_display_contains_context() {
    let err = BuildError::ConstructionFailed {
        final_m: 19,
        attempts: 6,
        last_failure: ConstructionFailure::InconsistentEquation {
            key_index: 7,
            row_index: 2,
        },
    };

    let msg = err.to_string();
    assert!(msg.contains("6 attempt"));
    assert!(msg.contains("m=19"));
    assert!(msg.contains("key at index 7"));
}

#[test]
fn build_error_invalid_params_display_chains_inner() {
    let err = BuildError::InvalidParams(ParamError::ZeroM);
    let msg = err.to_string();
    assert!(msg.contains("invalid parameters"));
    assert!(msg.contains("m must be greater than zero"));
}

#[test]
fn construction_failure_out_of_bounds_with_key_index() {
    let err = ConstructionFailure::OutOfBounds {
        key_index: Some(42),
        row_index: 100,
        m: 50,
    };
    let msg = err.to_string();
    assert!(msg.contains("row index 100"));
    assert!(msg.contains("m=50"));
    assert!(msg.contains("inserting key at index 42"));
}

#[test]
fn construction_failure_out_of_bounds_back_sub() {
    // Back-substitution branch — no key_index, different phrasing.
    let err = ConstructionFailure::OutOfBounds {
        key_index: None,
        row_index: 7,
        m: 4,
    };
    let msg = err.to_string();
    assert!(msg.contains("row index 7"));
    assert!(msg.contains("m=4"));
    assert!(msg.contains("back-substitution"));
}

#[test]
fn construction_failure_storage_length_overflow_display() {
    let err = ConstructionFailure::StorageLengthOverflow {
        m: usize::MAX / 2,
        stride_words: 4,
    };
    let msg = err.to_string();
    assert!(msg.contains("overflows usize"));
    assert!(msg.contains("stride_words=4"));
}

#[test]
fn filter_repr_error_display_covers_every_variant() {
    let v = FilterReprError::UnsupportedVersion {
        found: 9,
        expected: 5,
    };
    let msg = v.to_string();
    assert!(msg.contains("version 9"));
    assert!(msg.contains("expected 5"));

    let v = FilterReprError::InvalidParams(ParamError::ZeroM);
    assert!(v.to_string().contains("RibbonFilter representation"));

    assert!(
        FilterReprError::StorageLengthOverflow
            .to_string()
            .contains("storage length overflow")
    );

    let v = FilterReprError::InvalidStorageWords {
        found: 3,
        expected: 7,
    };
    let msg = v.to_string();
    assert!(msg.contains("word length 3"));
    assert!(msg.contains("expected 7"));

    let v = FilterReprError::InvalidStorageBits {
        found: 100,
        expected: 200,
    };
    let msg = v.to_string();
    assert!(msg.contains("bit length 100"));
    assert!(msg.contains("expected 200"));
}
