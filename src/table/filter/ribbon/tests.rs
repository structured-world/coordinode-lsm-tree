use super::{BuildError, Mode, ParamError, Params, RibbonBuilder};

use super::hashing::{standard_equation_from_hash, start_position_from_stream};

#[test]
fn params_rejects_zero_m() {
    let err = Params::new(0, 4, 8, Mode::Standard).expect_err("m=0 should fail");
    assert_eq!(err, ParamError::ZeroM);
}

#[test]
fn params_rejects_zero_w() {
    let err = Params::new(10, 0, 8, Mode::Standard).expect_err("w=0 should fail");
    assert_eq!(err, ParamError::ZeroWidth);
}

#[test]
fn params_rejects_zero_n_in_expected_items() {
    let err =
        Params::from_expected_items(0, 0.1, 4, 8, Mode::Standard).expect_err("n=0 should fail");
    assert_eq!(err, ParamError::ZeroN);
}

#[test]
fn params_rejects_zero_r() {
    let err = Params::new(10, 4, 0, Mode::Standard).expect_err("r=0 should fail");
    assert_eq!(err, ParamError::ZeroFingerprintBits);
}

#[test]
fn params_rejects_w_greater_than_m() {
    let err = Params::new(7, 8, 8, Mode::Standard).expect_err("w>m should fail");
    assert_eq!(err, ParamError::WidthExceedsM { m: 7, w: 8 });
}

#[test]
fn params_rejects_zero_retry_limit() {
    let params = Params::new(16, 8, 8, Mode::Standard).expect("base params should be valid");
    let err = params
        .with_retry_limit(0)
        .expect_err("retry_limit=0 should fail");
    assert_eq!(err, ParamError::ZeroRetryLimit);
}

#[test]
fn params_accepts_valid_values() {
    let params = Params::new(16, 8, 12, Mode::Standard).expect("valid params should pass");
    assert_eq!(params.m, 16);
    assert_eq!(params.w, 8);
    assert_eq!(params.r, 12);
}

#[test]
fn params_r_from_fpr_rounding_and_range() {
    assert_eq!(Params::r_from_fpr(0.5).expect("valid fpr"), 1);
    assert_eq!(Params::r_from_fpr(0.1).expect("valid fpr"), 4);
    assert!(matches!(
        Params::r_from_fpr(0.0),
        Err(ParamError::InvalidFalsePositiveRate { .. })
    ));
}

#[test]
fn params_from_expected_items_computes_m() {
    let p = Params::from_expected_items(1000, 0.2, 16, 8, Mode::Standard)
        .expect("params should be valid");
    assert_eq!(p.m, 1200);
    assert_eq!(p.w, 16);
    assert_eq!(p.r, 8);
}

#[test]
fn params_from_expected_items_rejects_overhead_out_of_range() {
    let err = Params::from_expected_items(1000, 10.1, 16, 8, Mode::Standard)
        .expect_err("overhead > 10 should fail");
    assert!(matches!(err, ParamError::InvalidOverhead { .. }));
}

// The equation pipeline is exercised directly from pre-computed hashes
// (the only path the crate uses now that keys arrive pre-hashed).

#[test]
fn equation_start_in_range_and_pivot_forced() {
    let params = Params::new(128, 17, 13, Mode::Standard).expect("params must be valid");
    let mut fp = vec![0u64; params.fingerprint_words()];

    let eq = standard_equation_from_hash(0x0123_4567_89AB_CDEF, 42, &params, &mut fp);

    assert!(eq.start < params.start_range());
    assert_eq!(eq.coeff_lo & 1, 1);
}

#[test]
fn equation_masks_fingerprint_to_r_bits() {
    let params = Params::new(64, 8, 9, Mode::Standard).expect("params must be valid");
    let mut fp = vec![0u64; params.fingerprint_words()];

    let _ = standard_equation_from_hash(0xDEAD_BEEF_CAFE_F00D, 7, &params, &mut fp);

    assert_eq!(fp[0] & !params.fingerprint_last_word_mask(), 0);
}

#[test]
fn equation_is_deterministic_for_seed_and_hash() {
    let params = Params::new(96, 16, 20, Mode::Standard).expect("params must be valid");
    let mut fp_a = vec![0u64; params.fingerprint_words()];
    let mut fp_b = vec![0u64; params.fingerprint_words()];

    let h = 0x1122_3344_5566_7788;
    let eq_a = standard_equation_from_hash(h, 999, &params, &mut fp_a);
    let eq_b = standard_equation_from_hash(h, 999, &params, &mut fp_b);

    assert_eq!(eq_a, eq_b);
    assert_eq!(fp_a, fp_b);
}

#[test]
fn homogeneous_pipeline_has_zero_fingerprint() {
    let params = Params::new(128, 16, 9, Mode::Homogeneous).expect("params must be valid");
    let mut fp = vec![0u64; params.fingerprint_words()];

    let _ = standard_equation_from_hash(0xABCD_1234_5678_9ABC, 11, &params, &mut fp);

    assert!(fp.iter().all(|&w| w == 0));
}

#[test]
fn width_128_pipeline_sets_bits_in_both_halves() {
    let params = Params::new(400, 128, 8, Mode::Standard).expect("params valid");

    let mut saw_hi = false;
    for seed in 0..500u64 {
        let mut fp = vec![0u64; params.fingerprint_words()];
        let eq = standard_equation_from_hash(0xF00D_BA11_0BAD_F00D, seed, &params, &mut fp);

        if eq.coeff_hi != 0 {
            saw_hi = true;
            break;
        }
    }

    assert!(
        saw_hi,
        "expected at least one seed with high-half coefficient bits"
    );
}

#[test]
fn start_position_hook_stays_in_bounds() {
    let m = 256usize;
    let w = 64usize;
    let range = m - w + 1;

    for x in [0u64, 1, 7, u64::MAX / 2, u64::MAX] {
        let s = start_position_from_stream(x, m, w);
        assert!(s < range, "start position out of range for x={x}: {s}");
    }
}

#[test]
fn build_with_seed_verbatim_from_hashes_round_trips() {
    // The entry point BuRR uses with xxh3-hashed LSM keys.
    let params = Params::new(3000, 16, 9, Mode::Standard).expect("valid");
    let builder = RibbonBuilder::new(params).expect("builder");
    let hashes: Vec<u64> = (0..500_u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let filter = builder
        .build_with_seed_verbatim_from_hashes(&hashes, 0x1234_5678_9ABC_DEF0, 3000)
        .expect("verbatim-from-hashes build should land");
    assert_eq!(filter.params().seed, 0x1234_5678_9ABC_DEF0);
}

#[test]
fn build_with_seed_verbatim_from_hashes_propagates_failure() {
    // Hashes that overload a tight m must surface ConstructionFailed
    // through the from-hashes wrapper — single-attempt contract.
    let params = Params::new(16, 16, 8, Mode::Standard).expect("valid");
    let builder = RibbonBuilder::new(params).expect("builder");
    let hashes: Vec<u64> = (0..200_u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let result = builder.build_with_seed_verbatim_from_hashes(&hashes, 7, 16);
    match result {
        Err(BuildError::ConstructionFailed {
            attempts, final_m, ..
        }) => {
            assert_eq!(attempts, 1);
            assert_eq!(final_m, 16);
        }
        Ok(_) => panic!("tight m=16 with 200 hashes must not build"),
        Err(other) => panic!("expected ConstructionFailed, got {other:?}"),
    }
}

#[cfg(feature = "ribbon-serde")]
#[test]
fn serde_roundtrip_preserves_storage() {
    let params = Params::new(3000, 16, 10, Mode::Standard)
        .expect("params should be valid")
        .with_seed(4242);
    let builder = RibbonBuilder::new(params).expect("builder should be valid");
    let hashes: Vec<u64> = (0..1000_u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let filter = builder
        .build_with_seed_verbatim_from_hashes(&hashes, params.seed, 3000)
        .expect("build should succeed");

    let repr = filter.to_repr();
    let encoded = serde_json::to_string(&repr).expect("serialize should succeed");
    let decoded_repr: super::RibbonFilterRepr =
        serde_json::from_str(&encoded).expect("deserialize should succeed");
    let decoded =
        super::RibbonFilter::from_repr(decoded_repr).expect("reconstructing filter should succeed");

    // No on-disk change: the solution matrix and seed survive the round trip.
    assert_eq!(filter.z_raw_words(), decoded.z_raw_words());
    assert_eq!(filter.params().seed, decoded.params().seed);
}

#[cfg(feature = "ribbon-serde")]
#[test]
fn serde_rejects_unknown_filter_version() {
    let params = Params::new(512, 16, 8, Mode::Standard)
        .expect("params should be valid")
        .with_seed(9);
    let builder = RibbonBuilder::new(params).expect("builder should be valid");
    let hashes: Vec<u64> = (0..100_u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let filter = builder
        .build_with_seed_verbatim_from_hashes(&hashes, params.seed, 512)
        .expect("build should succeed");

    let mut value = serde_json::to_value(filter.to_repr()).expect("serialize should succeed");
    value["version"] = serde_json::Value::from(99u64);

    let repr = serde_json::from_value::<super::RibbonFilterRepr>(value)
        .expect("deserializing repr should succeed");
    let err = super::RibbonFilter::from_repr(repr)
        .expect_err("reconstructing unknown version should fail");
    assert!(err.to_string().contains("unsupported RibbonFilter version"));
}

#[cfg(feature = "ribbon-serde")]
#[test]
fn serde_rejects_incorrect_storage_word_length() {
    let params = Params::new(512, 16, 8, Mode::Standard)
        .expect("params should be valid")
        .with_seed(10);
    let builder = RibbonBuilder::new(params).expect("builder should be valid");
    let hashes: Vec<u64> = (0..100_u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let filter = builder
        .build_with_seed_verbatim_from_hashes(&hashes, params.seed, 512)
        .expect("build should succeed");

    let mut repr = filter.to_repr();
    let expected_words = repr.params.m * repr.params.fingerprint_words();
    let wrong_words = expected_words - 1;
    repr.z = vec![0_u64; wrong_words];

    let err = super::RibbonFilter::from_repr(repr)
        .expect_err("reconstructing invalid storage should fail");
    assert!(
        err.to_string()
            .contains("invalid RibbonFilter storage word length")
    );
}

#[test]
fn construction_failure_out_of_bounds_display_contains_context() {
    let err = super::ConstructionFailure::OutOfBounds {
        key_index: Some(12),
        row_index: 99,
        m: 80,
    };
    let msg = err.to_string();
    assert!(msg.contains("row index 99"));
    assert!(msg.contains("m=80"));
    assert!(msg.contains("key at index 12"));
}

#[test]
fn param_error_display_covers_all_variants() {
    use super::error::ParamError;
    let cases = [
        (ParamError::ZeroM, "m must"),
        (ParamError::ZeroN, "n must"),
        (ParamError::ZeroWidth, "w must"),
        (ParamError::ZeroFingerprintBits, "r must"),
        (ParamError::ZeroRetryLimit, "retry_limit must"),
    ];
    for (err, needle) in &cases {
        let s = format!("{err}");
        assert!(
            s.contains(needle),
            "ParamError::{err:?} display missing '{needle}': got {s}"
        );
    }
    let s = format!("{}", ParamError::WidthTooLarge { w: 200, max: 128 });
    assert!(s.contains("200") && s.contains("128"), "got: {s}");
    let s = format!("{}", ParamError::WidthExceedsM { m: 4, w: 16 });
    assert!(
        s.contains("m=4") || s.contains("w=16") || s.contains('4') && s.contains("16"),
        "got: {s}"
    );
    let s = format!("{}", ParamError::InvalidFalsePositiveRate { fpr: 2.0 });
    assert!(s.contains('2') || s.contains("false"), "got: {s}");
    let s = format!("{}", ParamError::InvalidOverhead { overhead: -0.5 });
    assert!(s.contains("overhead") || s.contains("-0.5"), "got: {s}");
}

#[test]
fn construction_failure_display_inconsistent_eq() {
    use super::error::ConstructionFailure;
    let err = ConstructionFailure::InconsistentEquation {
        key_index: 17,
        row_index: 99,
    };
    let s = format!("{err}");
    assert!(s.contains("17") && s.contains("99"), "got: {s}");
}

#[test]
fn construction_failure_display_out_of_bounds() {
    use super::error::ConstructionFailure;
    let err = ConstructionFailure::OutOfBounds {
        key_index: Some(5),
        row_index: 1000,
        m: 800,
    };
    let s = format!("{err}");
    assert!(
        s.contains('5') && s.contains("1000") && s.contains("800"),
        "got: {s}"
    );
}

#[test]
fn build_error_display_covers_variants() {
    use super::error::{BuildError, ConstructionFailure, ParamError};
    let invalid = BuildError::InvalidParams(ParamError::ZeroM);
    let s = format!("{invalid}");
    assert!(s.contains("invalid parameters"), "got: {s}");

    let cf = BuildError::ConstructionFailed {
        final_m: 4096,
        attempts: 8,
        last_failure: ConstructionFailure::InconsistentEquation {
            key_index: 0,
            row_index: 0,
        },
    };
    let s = format!("{cf}");
    assert!(s.contains("4096") || s.contains('8'), "got: {s}");
}

#[test]
#[cfg(feature = "ribbon-serde")]
fn filter_repr_error_display_covers_variants() {
    use super::error::{FilterReprError, ParamError};
    let s = format!(
        "{}",
        FilterReprError::UnsupportedVersion {
            found: 99,
            expected: 1
        }
    );
    assert!(s.contains("99") && s.contains('1'), "got: {s}");
    let s = format!("{}", FilterReprError::InvalidParams(ParamError::ZeroM));
    assert!(s.contains("invalid") || s.contains("param"), "got: {s}");
    let s = format!("{}", FilterReprError::StorageLengthOverflow);
    assert!(s.contains("overflow") || s.contains("storage"), "got: {s}");
    let s = format!(
        "{}",
        FilterReprError::InvalidStorageWords {
            found: 5,
            expected: 10
        }
    );
    assert!(s.contains('5') && s.contains("10"), "got: {s}");
    let s = format!(
        "{}",
        FilterReprError::InvalidStorageBits {
            found: 5,
            expected: 10
        }
    );
    assert!(s.contains('5') && s.contains("10"), "got: {s}");
}
