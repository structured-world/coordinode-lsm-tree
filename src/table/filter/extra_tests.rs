use super::*;
use test_log::test;

#[test]
fn policy_default_is_bits_per_key_10() {
    let policy = BloomConstructionPolicy::default();
    assert_eq!(policy, BloomConstructionPolicy::BitsPerKey(10.0));
}

#[test]
fn is_active_false_for_bpk_below_one() {
    assert!(!BloomConstructionPolicy::BitsPerKey(0.5).is_active());
    assert!(!BloomConstructionPolicy::BitsPerKey(0.0).is_active());
}

#[test]
fn is_active_false_for_bpk_above_64() {
    assert!(!BloomConstructionPolicy::BitsPerKey(70.0).is_active());
}

#[test]
fn is_active_true_for_valid_bpk() {
    assert!(BloomConstructionPolicy::BitsPerKey(10.0).is_active());
    assert!(BloomConstructionPolicy::BitsPerKey(1.0).is_active());
    assert!(BloomConstructionPolicy::BitsPerKey(64.0).is_active());
}

#[test]
fn is_active_false_for_fpr_out_of_range() {
    assert!(!BloomConstructionPolicy::FalsePositiveRate(0.0).is_active());
    assert!(!BloomConstructionPolicy::FalsePositiveRate(-0.1).is_active());
    assert!(!BloomConstructionPolicy::FalsePositiveRate(1.0).is_active());
    assert!(!BloomConstructionPolicy::FalsePositiveRate(1.5).is_active());
    // Too tight — would map to r > 64.
    assert!(!BloomConstructionPolicy::FalsePositiveRate(1.0e-25_f32).is_active());
}

#[test]
fn is_active_true_for_valid_fpr() {
    assert!(BloomConstructionPolicy::FalsePositiveRate(0.01).is_active());
    assert!(BloomConstructionPolicy::FalsePositiveRate(0.0001).is_active());
    assert!(BloomConstructionPolicy::FalsePositiveRate(0.5).is_active());
}

#[test]
fn estimated_size_zero_n_returns_zero() {
    let policy = BloomConstructionPolicy::BitsPerKey(10.0);
    assert_eq!(policy.estimated_filter_size(0), 0);
    let policy_fpr = BloomConstructionPolicy::FalsePositiveRate(0.01);
    assert_eq!(policy_fpr.estimated_filter_size(0), 0);
}

#[test]
fn burr_params_returns_none_for_n_zero() {
    let policy = BloomConstructionPolicy::BitsPerKey(10.0);
    assert!(policy.burr_params(0).is_none());
}

#[test]
fn burr_params_returns_some_for_valid_inputs() {
    let policy = BloomConstructionPolicy::BitsPerKey(10.0);
    let params = policy.burr_params(100).expect("valid");
    assert_eq!(params.n, 100);
    assert_eq!(params.r, 10);
}

#[test]
fn burr_params_fpr_variant() {
    let policy = BloomConstructionPolicy::FalsePositiveRate(0.01);
    let params = policy.burr_params(100).expect("valid");
    assert_eq!(params.n, 100);
    // r = ceil(-log2(0.01)) = 7
    assert_eq!(params.r, 7);
}

#[test]
fn build_burr_filter_bytes_invalid_policy_returns_empty() {
    // Policy too tight → burr_params returns None → empty bytes.
    let policy = BloomConstructionPolicy::FalsePositiveRate(1.0e-25_f32);
    let hashes: Vec<u64> = (0..10)
        .map(|i: u64| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let bytes = build_burr_filter_bytes(policy, hashes).unwrap();
    assert!(bytes.is_empty());
}
