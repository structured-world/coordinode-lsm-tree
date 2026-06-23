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
