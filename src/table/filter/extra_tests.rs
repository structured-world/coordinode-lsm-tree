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

/// The expected size follows the layers a build makes: exact where every key
/// set builds the same layers, which is where short filters bump a large
/// share of their keys (301 keys over 320 slots), and within the sizes real
/// builds take elsewhere. The partition-split estimate runs 30% short at 301
/// keys, which a filter budget would charge to the filters after it.
#[test]
fn expected_filter_size_follows_the_layers_builds_make() {
    for bits in [4.0f32, 10.0, 16.0] {
        let policy = BloomConstructionPolicy::BitsPerKey(bits);
        for n in [20usize, 50, 100, 200, 301, 500, 1000, 2621, 5000, 20000] {
            let sizes: Vec<usize> = (0..20u64)
                .map(|set| {
                    let hashes: Vec<u64> = (0..n as u64)
                        .map(|i| crate::hash::hash64(&(i + set * 1_000_000).to_le_bytes()))
                        .collect();
                    build_burr_filter_bytes(policy, hashes).unwrap().len()
                })
                .collect();
            let min = *sizes.iter().min().unwrap();
            let max = *sizes.iter().max().unwrap();
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "a filter's byte count, non-negative and small"
            )]
            let expected = policy.expected_filter_size(n).round() as usize;
            if min == max {
                assert_eq!(expected, min, "{bits} bits per key over {n} keys");
            } else {
                assert!(
                    (min..=max).contains(&expected),
                    "{bits} bits per key over {n} keys: {expected} outside {min}..={max}",
                );
            }
        }
    }
}
