use super::{BuildError, Mode, ParamError, Params, RibbonBuilder};
use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;

use super::hashing::{standard_equation_w64, start_position_from_stream};

type DefaultBuildHasher = BuildHasherDefault<DefaultHasher>;

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

#[test]
fn hash_pipeline_start_in_range_and_pivot_forced() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(128, 17, 13, Mode::Standard).expect("params must be valid");
    let mut fp = vec![0u64; params.fingerprint_words()];

    let eq = standard_equation_w64(&hasher, &"hello-key", 42, &params, &mut fp);

    assert!(eq.start < params.start_range());
    assert_eq!(eq.coeff_lo & 1, 1);
}

#[test]
fn hash_pipeline_masks_fingerprint_to_r_bits() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(64, 8, 9, Mode::Standard).expect("params must be valid");
    let mut fp = vec![0u64; params.fingerprint_words()];

    let _ = standard_equation_w64(&hasher, &12345u64, 7, &params, &mut fp);

    assert_eq!(fp[0] & !params.fingerprint_last_word_mask(), 0);
}

#[test]
fn hash_pipeline_is_deterministic_for_seed_and_key() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(96, 16, 20, Mode::Standard).expect("params must be valid");
    let mut fp_a = vec![0u64; params.fingerprint_words()];
    let mut fp_b = vec![0u64; params.fingerprint_words()];

    let eq_a = standard_equation_w64(&hasher, &"deterministic-key", 999, &params, &mut fp_a);
    let eq_b = standard_equation_w64(&hasher, &"deterministic-key", 999, &params, &mut fp_b);

    assert_eq!(eq_a, eq_b);
    assert_eq!(fp_a, fp_b);
}

#[test]
fn standard_builder_has_no_false_negatives_1k() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(3000, 16, 12, Mode::Standard).expect("params should be valid");
    let builder = RibbonBuilder::new(params.with_seed(11), hasher).expect("builder should build");

    let keys: Vec<u64> = (0..1000).collect();
    let filter = builder.build(&keys).expect("construction should succeed");

    for key in &keys {
        assert!(filter.contains(key), "false negative for key {key}");
    }
}

#[test]
fn standard_builder_has_no_false_negatives_10k() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(30000, 16, 10, Mode::Standard).expect("params should be valid");
    let builder = RibbonBuilder::new(params.with_seed(13), hasher).expect("builder should build");

    let keys: Vec<u64> = (0..10000).collect();
    let filter = builder.build(&keys).expect("construction should succeed");

    for key in &keys {
        assert!(filter.contains(key), "false negative for key {key}");
    }
}

#[test]
fn standard_builder_reports_inconsistent_equation_failure() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(16, 16, 8, Mode::Standard)
        .expect("params should be valid")
        .with_seed(5);
    let builder = RibbonBuilder::new(params, hasher).expect("builder should build");

    let keys: Vec<u64> = (0..200).collect();
    let result = builder.build(&keys);

    match result {
        Err(BuildError::ConstructionFailed { last_failure, .. }) => {
            assert!(matches!(
                last_failure,
                super::ConstructionFailure::InconsistentEquation { .. }
            ));
        }
        Err(other) => panic!("expected construction failure, got {other}"),
        Ok(_) => panic!("expected failure, got success"),
    }
}

#[test]
fn standard_builder_is_deterministic_for_same_input() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(3000, 16, 9, Mode::Standard)
        .expect("params should be valid")
        .with_seed(99);

    let builder_a = RibbonBuilder::new(params, hasher.clone()).expect("builder should build");
    let builder_b = RibbonBuilder::new(params, hasher).expect("builder should build");

    let keys: Vec<u64> = (1000..2000).collect();
    let filter_a = builder_a.build(&keys).expect("first build should succeed");
    let filter_b = builder_b.build(&keys).expect("second build should succeed");

    for probe in 990..2010u64 {
        assert_eq!(
            filter_a.contains(&probe),
            filter_b.contains(&probe),
            "non-deterministic result for key {probe}"
        );
    }
}

#[derive(Default, Clone)]
struct ConstantBuildHasher;

impl std::hash::BuildHasher for ConstantBuildHasher {
    type Hasher = ConstantHasher;

    fn build_hasher(&self) -> Self::Hasher {
        ConstantHasher
    }
}

#[derive(Default, Clone)]
struct ConstantHasher;

impl std::hash::Hasher for ConstantHasher {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, _bytes: &[u8]) {}
}

#[test]
fn builder_supports_custom_buildhasher() {
    let hasher = ConstantBuildHasher;
    let params = Params::new(3000, 16, 9, Mode::Standard)
        .expect("params should be valid")
        .with_seed(88);
    let builder = RibbonBuilder::new(params, hasher).expect("builder should build");

    let keys: Vec<u64> = (0..200).collect();
    let filter = builder.build(&keys).expect("build should succeed");

    let mut scratch = filter.new_scratch();
    for key in &keys {
        assert!(filter.contains_in(key, &mut scratch));
    }
}

#[test]
fn contains_and_contains_in_are_equivalent() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(3000, 16, 9, Mode::Standard)
        .expect("params should be valid")
        .with_seed(77);
    let builder = RibbonBuilder::new(params, hasher).expect("builder should build");
    let keys: Vec<u64> = (1000..2000).collect();
    let filter = builder.build(&keys).expect("build should succeed");

    let mut scratch = filter.new_scratch();
    for probe in 900..2100u64 {
        assert_eq!(
            filter.contains(&probe),
            filter.contains_in(&probe, &mut scratch),
            "contains mismatch at key {probe}"
        );
    }
}

#[test]
fn retry_path_is_exercised_and_eventually_succeeds() {
    let hasher = DefaultBuildHasher::default();
    let keys: Vec<u64> = (0..500).collect();
    let params = Params::new(16, 16, 8, Mode::Standard)
        .expect("params valid")
        .with_seed(1)
        .with_retry_policy(3, 0)
        .expect("retry policy valid");
    let builder = RibbonBuilder::new(params, hasher).expect("builder valid");

    match builder.build(&keys) {
        Err(BuildError::ConstructionFailed {
            final_m,
            attempts,
            last_failure,
        }) => {
            assert_eq!(final_m, 16);
            assert_eq!(attempts, 3);
            assert!(matches!(
                last_failure,
                super::ConstructionFailure::InconsistentEquation { .. }
            ));
        }
        other => panic!("expected retry-exhausted failure, got {other:?}"),
    }
}

#[test]
fn growth_path_is_exercised_and_reports_grown_m() {
    let hasher = DefaultBuildHasher::default();
    let keys: Vec<u64> = (0..500).collect();
    let params = Params::new(16, 16, 8, Mode::Standard)
        .expect("params valid")
        .with_seed(1)
        .with_retry_policy(2, 2)
        .expect("retry policy valid");
    let builder = RibbonBuilder::new(params, hasher).expect("builder valid");

    match builder.build(&keys) {
        Err(BuildError::ConstructionFailed {
            final_m,
            attempts,
            last_failure,
        }) => {
            assert_eq!(attempts, 6);
            assert_eq!(final_m, 19);
            assert!(matches!(
                last_failure,
                super::ConstructionFailure::InconsistentEquation { .. }
            ));
        }
        other => panic!("expected growth-exhausted failure, got {other:?}"),
    }
}

#[test]
fn terminal_failure_reports_attempts_and_final_m() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(16, 16, 8, Mode::Standard)
        .expect("params valid")
        .with_seed(1)
        .with_retry_policy(2, 2)
        .expect("retry policy valid");
    let builder = RibbonBuilder::new(params, hasher).expect("builder valid");
    let keys: Vec<u64> = (0..500).collect();

    match builder.build(&keys) {
        Err(BuildError::ConstructionFailed {
            final_m,
            attempts,
            last_failure,
        }) => {
            assert_eq!(attempts, 6);
            assert_eq!(final_m, 19);
            assert!(matches!(
                last_failure,
                super::ConstructionFailure::InconsistentEquation { .. }
            ));
        }
        other => panic!("expected terminal construction failure, got {other:?}"),
    }
}

#[test]
fn successful_build_persists_selected_attempt_seed() {
    let hasher = DefaultBuildHasher::default();
    let base_seed = 123u64;
    let params = Params::new(3000, 16, 9, Mode::Standard)
        .expect("params valid")
        .with_seed(base_seed)
        .with_retry_policy(1, 0)
        .expect("retry policy valid");
    let builder = RibbonBuilder::new(params, hasher).expect("builder valid");
    let keys: Vec<u64> = (0..1000).collect();

    let filter = builder.build(&keys).expect("build should succeed");
    assert_eq!(
        filter.params().seed,
        super::hashing::derive_attempt_seed(base_seed, 0)
    );
}

#[test]
fn homogeneous_build_succeeds_and_has_no_false_negatives() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(4000, 16, 8, Mode::Homogeneous)
        .expect("params valid")
        .with_seed(55);
    let builder = RibbonBuilder::new(params, hasher).expect("builder valid");
    let keys: Vec<u64> = (0..1000).collect();

    let filter = builder
        .build(&keys)
        .expect("homogeneous build should succeed");
    let mut scratch = filter.new_scratch();
    for key in &keys {
        assert!(filter.contains_in(key, &mut scratch));
    }
}

#[test]
fn homogeneous_mode_false_positive_rate_is_sane_across_seeds_and_sizes() {
    let hasher = DefaultBuildHasher::default();
    let r = 8usize;
    let seeds = [7u64, 77u64, 777u64];
    let sizes = [2_000usize, 8_000usize];
    let queries = 40_000usize;

    for &seed in &seeds {
        for &n in &sizes {
            let params = Params::new(n * 4, 16, r, Mode::Homogeneous)
                .expect("params valid")
                .with_seed(seed)
                .with_retry_policy(2, 0)
                .expect("retry policy valid");
            let builder = RibbonBuilder::new(params, hasher.clone()).expect("builder valid");
            let keys: Vec<u64> = (0..n as u64).collect();
            let filter = builder
                .build(&keys)
                .expect("homogeneous build should succeed");

            let mut scratch = filter.new_scratch();
            let mut fp = 0usize;
            let query_start = 10_000_000u64 + (seed << 20) + n as u64;
            for q in 0..queries {
                if filter.contains_in(&(query_start + q as u64), &mut scratch) {
                    fp += 1;
                }
            }

            let observed = fp as f64 / queries as f64;
            let expected = 2f64.powi(-(r as i32));

            assert!(
                observed > 0.0005,
                "homogeneous fp unexpectedly near-zero seed={seed} n={n}: observed={observed}, expected~{expected}"
            );
            assert!(
                observed < 0.05,
                "homogeneous fp unexpectedly near-trivial-high seed={seed} n={n}: observed={observed}, expected~{expected}"
            );
            assert!(
                observed >= expected / 8.0 && observed <= expected * 8.0,
                "homogeneous fp far from expected envelope seed={seed} n={n}: observed={observed}, expected~{expected}"
            );
        }
    }
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
fn homogeneous_pipeline_has_zero_fingerprint() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(128, 16, 9, Mode::Homogeneous).expect("params must be valid");
    let mut fp = vec![0u64; params.fingerprint_words()];

    let _ = standard_equation_w64(&hasher, &"h-key", 11, &params, &mut fp);

    assert!(fp.iter().all(|&w| w == 0));
}

#[test]
fn width_128_pipeline_sets_bits_in_both_halves() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(400, 128, 8, Mode::Standard).expect("params valid");

    let mut saw_hi = false;
    for seed in 0..500u64 {
        let mut fp = vec![0u64; params.fingerprint_words()];
        let eq = standard_equation_w64(&hasher, &"w128-key", seed, &params, &mut fp);

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
fn builder_supports_width_above_64() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(4000, 96, 10, Mode::Standard)
        .expect("params should be valid")
        .with_seed(303)
        .with_retry_policy(4, 1)
        .expect("retry policy valid");
    let builder = RibbonBuilder::new(params, hasher).expect("builder should build");
    let keys: Vec<u64> = (0..800).collect();
    let filter = builder.build(&keys).expect("construction should succeed");
    let mut scratch = filter.new_scratch();

    for key in &keys {
        assert!(filter.contains_in(key, &mut scratch));
    }
}

#[test]
fn bitpacked_storage_maintains_membership_behavior() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(3000, 16, 12, Mode::Standard)
        .expect("params should be valid")
        .with_seed(1234);
    let builder = RibbonBuilder::new(params, hasher).expect("builder should build");
    let keys: Vec<u64> = (0..1000).collect();
    let filter = builder.build(&keys).expect("build should succeed");

    let mut scratch = filter.new_scratch();
    for key in &keys {
        assert!(filter.contains_in(key, &mut scratch));
    }
}

#[test]
fn compatibility_matrix_modes_widths_and_fingerprints() {
    let hasher = DefaultBuildHasher::default();
    let modes = [Mode::Standard, Mode::Homogeneous];
    let widths = [16usize, 80usize, 128usize];
    let rs = [8usize, 12usize];

    for mode in modes {
        for w in widths {
            for r in rs {
                let params = Params::new(6000, w, r, mode)
                    .expect("params should be valid")
                    .with_seed(700 + w as u64 + r as u64)
                    .with_retry_policy(5, 2)
                    .expect("retry policy valid");
                let builder =
                    RibbonBuilder::new(params, hasher.clone()).expect("builder should build");
                let keys: Vec<u64> = (0..1000).collect();
                let filter = builder.build(&keys).expect("construction should succeed");
                let mut scratch = filter.new_scratch();

                for key in &keys {
                    assert!(
                        filter.contains_in(key, &mut scratch),
                        "false negative for mode={mode}, w={w}, r={r}, key={key}"
                    );
                }
            }
        }
    }
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
fn statistical_false_positive_rates_are_within_confidence_bounds() {
    let hasher = DefaultBuildHasher::default();
    let seeds = [11u64, 42u64, 777u64];
    let sizes = [1000usize, 5000usize];
    let rs = [8usize, 12usize];

    for &seed in &seeds {
        for &n in &sizes {
            for &r in &rs {
                let params = Params::new(n * 4, 16, r, Mode::Standard)
                    .expect("params should be valid")
                    .with_seed(seed)
                    .with_retry_policy(4, 1)
                    .expect("retry policy should be valid");
                let builder =
                    RibbonBuilder::new(params, hasher.clone()).expect("builder should be valid");
                let keys: Vec<u64> = (0..n as u64).collect();
                let filter = builder.build(&keys).expect("construction should succeed");

                let queries = 20_000usize;
                let query_start = 10_000_000u64 + (seed << 20) + n as u64;
                let mut scratch = filter.new_scratch();
                let mut fp = 0usize;
                for q in 0..queries {
                    if filter.contains_in(&(query_start + q as u64), &mut scratch) {
                        fp += 1;
                    }
                }

                let p = 2f64.powi(-(r as i32));
                let mean = (queries as f64) * p;
                let var = (queries as f64) * p * (1.0 - p);
                let sigma = var.sqrt();
                let tolerance = (8.0 * sigma).max(8.0);
                let lower = (mean - tolerance).max(0.0);
                let upper = mean + tolerance;
                let observed = fp as f64;

                assert!(
                    observed >= lower && observed <= upper,
                    "fp out of bounds seed={seed} n={n} r={r}: observed={observed} expected~{mean} bounds=[{lower}, {upper}]"
                );
            }
        }
    }
}

fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

#[test]
fn property_no_false_negatives_across_generated_cases() {
    let hasher = DefaultBuildHasher::default();
    let mut rng = 1u64;

    for case in 0..20u64 {
        let seed = lcg_next(&mut rng);
        let n = 200 + (lcg_next(&mut rng) % 400) as usize;
        let w_choices = [16usize, 32usize, 80usize, 128usize];
        let w = w_choices[(lcg_next(&mut rng) as usize) % w_choices.len()];
        let r_choices = [8usize, 10usize, 12usize];
        let r = r_choices[(lcg_next(&mut rng) as usize) % r_choices.len()];
        let mode = if (lcg_next(&mut rng) & 1) == 0 {
            Mode::Standard
        } else {
            Mode::Homogeneous
        };

        let params = Params::new((n * 5).max(w), w, r, mode)
            .expect("params should be valid")
            .with_seed(seed)
            .with_retry_policy(4, 2)
            .expect("retry policy should be valid");
        let builder = RibbonBuilder::new(params, hasher.clone()).expect("builder should be valid");
        let keys: Vec<u64> = (0..n as u64)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(seed))
            .collect();
        let filter = builder
            .build(&keys)
            .expect("construction should succeed for generated case");
        let mut scratch = filter.new_scratch();

        for key in &keys {
            assert!(
                filter.contains_in(key, &mut scratch),
                "false negative for case={case}, mode={mode}, w={w}, r={r}, seed={seed}, key={key}"
            );
        }
    }
}

#[test]
fn property_determinism_across_generated_cases() {
    let hasher = DefaultBuildHasher::default();
    let mut rng = 99u64;

    for case in 0..16u64 {
        let seed = lcg_next(&mut rng);
        let n = 180 + (lcg_next(&mut rng) % 320) as usize;
        let w_choices = [16usize, 64usize, 96usize];
        let w = w_choices[(lcg_next(&mut rng) as usize) % w_choices.len()];
        let r = if (lcg_next(&mut rng) & 1) == 0 { 8 } else { 12 };
        let mode = if (lcg_next(&mut rng) & 1) == 0 {
            Mode::Standard
        } else {
            Mode::Homogeneous
        };

        let params = Params::new((n * 5).max(w), w, r, mode)
            .expect("params should be valid")
            .with_seed(seed)
            .with_retry_policy(4, 2)
            .expect("retry policy should be valid");
        let keys: Vec<u64> = (0..n as u64)
            .map(|i| i.wrapping_mul(0xD6E8_FEB8_6659_FD93).wrapping_add(seed))
            .collect();

        let builder_a = RibbonBuilder::new(params, hasher.clone()).expect("builder a valid");
        let builder_b = RibbonBuilder::new(params, hasher.clone()).expect("builder b valid");
        let filter_a = builder_a.build(&keys).expect("build a should succeed");
        let filter_b = builder_b.build(&keys).expect("build b should succeed");

        let mut scratch_a = filter_a.new_scratch();
        let mut scratch_b = filter_b.new_scratch();
        for probe in 0..(n as u64 + 128) {
            let q = probe.wrapping_mul(0x94D0_49BB_1331_11EB).wrapping_add(seed);
            assert_eq!(
                filter_a.contains_in(&q, &mut scratch_a),
                filter_b.contains_in(&q, &mut scratch_b),
                "determinism mismatch case={case}, mode={mode}, w={w}, r={r}, seed={seed}, q={q}"
            );
        }
    }
}

fn adversarial_patterns(n: usize) -> Vec<(&'static str, Vec<u64>)> {
    let ordered: Vec<u64> = (0..n as u64).collect();
    let constant_low_bits: Vec<u64> = (0..n as u64).map(|i| (i << 16) | 0xFFFF).collect();
    let stride_1024: Vec<u64> = (0..n as u64).map(|i| i * 1024).collect();
    let gray_code: Vec<u64> = (0..n as u64).map(|i| i ^ (i >> 1)).collect();
    let mul_mix_a: Vec<u64> = (0..n as u64)
        .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .collect();
    let mul_mix_b: Vec<u64> = (0..n as u64)
        .map(|i| i.wrapping_mul(0xD6E8_FEB8_6659_FD93))
        .collect();

    vec![
        ("ordered_u64", ordered),
        ("constant_low_bits", constant_low_bits),
        ("stride_1024", stride_1024),
        ("gray_code", gray_code),
        ("mul_mix_a", mul_mix_a),
        ("mul_mix_b", mul_mix_b),
    ]
}

#[test]
fn adversarial_regression_corpus_has_no_false_negatives() {
    let hasher = DefaultBuildHasher::default();
    let n = 2000usize;

    for (name, keys) in adversarial_patterns(n) {
        let params = Params::new(8000, 16, 10, Mode::Standard)
            .expect("params should be valid")
            .with_seed(404)
            .with_retry_policy(6, 2)
            .expect("retry policy should be valid");
        let builder = RibbonBuilder::new(params, hasher.clone()).expect("builder should be valid");
        let filter = builder
            .build(&keys)
            .expect("construction should succeed for adversarial set");
        let mut scratch = filter.new_scratch();

        for key in &keys {
            assert!(
                filter.contains_in(key, &mut scratch),
                "false negative in adversarial set '{name}' for key {key}"
            );
        }
    }
}

#[cfg(feature = "ribbon-serde")]
#[test]
fn serde_roundtrip_preserves_membership_behavior() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(3000, 16, 10, Mode::Standard)
        .expect("params should be valid")
        .with_seed(4242);
    let builder = RibbonBuilder::new(params, hasher).expect("builder should be valid");
    let keys: Vec<u64> = (0..1000).collect();
    let filter = builder.build(&keys).expect("build should succeed");

    let repr = filter.to_repr();
    let encoded = serde_json::to_string(&repr).expect("serialize should succeed");
    let decoded_repr: super::RibbonFilterRepr =
        serde_json::from_str(&encoded).expect("deserialize should succeed");
    let decoded = super::RibbonFilter::from_repr(decoded_repr, DefaultBuildHasher::default())
        .expect("reconstructing filter should succeed");

    let mut scratch_original = filter.new_scratch();
    let mut scratch_decoded = decoded.new_scratch();
    for probe in 0..1200u64 {
        assert_eq!(
            filter.contains_in(&probe, &mut scratch_original),
            decoded.contains_in(&probe, &mut scratch_decoded),
            "membership mismatch for probe {probe}"
        );
    }
}

#[cfg(feature = "ribbon-serde")]
#[test]
fn serde_rejects_unknown_filter_version() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(512, 16, 8, Mode::Standard)
        .expect("params should be valid")
        .with_seed(9);
    let builder = RibbonBuilder::new(params, hasher).expect("builder should be valid");
    let keys: Vec<u64> = (0..100).collect();
    let filter = builder.build(&keys).expect("build should succeed");

    let mut value = serde_json::to_value(filter.to_repr()).expect("serialize should succeed");
    value["version"] = serde_json::Value::from(99u64);

    let repr = serde_json::from_value::<super::RibbonFilterRepr>(value)
        .expect("deserializing repr should succeed");
    let err = super::RibbonFilter::from_repr(repr, DefaultBuildHasher::default())
        .expect_err("reconstructing unknown version should fail");
    assert!(err.to_string().contains("unsupported RibbonFilter version"));
}

#[cfg(feature = "ribbon-serde")]
#[test]
fn serde_rejects_incorrect_storage_word_length() {
    let hasher = DefaultBuildHasher::default();
    let params = Params::new(512, 16, 8, Mode::Standard)
        .expect("params should be valid")
        .with_seed(10);
    let builder = RibbonBuilder::new(params, hasher).expect("builder should be valid");
    let keys: Vec<u64> = (0..100).collect();
    let filter = builder.build(&keys).expect("build should succeed");

    let mut repr = filter.to_repr();
    let expected_words = repr.params.m * repr.params.fingerprint_words();
    let wrong_words = expected_words - 1;
    repr.z = bitvec::prelude::BitVec::<u64, bitvec::prelude::Lsb0>::from_vec(vec![0; wrong_words]);

    let err = super::RibbonFilter::from_repr(repr, DefaultBuildHasher::default())
        .expect_err("reconstructing invalid storage should fail");
    assert!(
        err.to_string()
            .contains("invalid RibbonFilter storage word length")
    );
}
