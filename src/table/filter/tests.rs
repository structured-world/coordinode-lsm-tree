use super::*;
use test_log::test;

/// The bytes the filter a policy builds for `n` keys encodes to, so an
/// estimate is checked against the real thing rather than a restatement of
/// its own formula.
fn built_len(policy: BloomConstructionPolicy, n: usize) -> usize {
    let hashes: Vec<u64> = (0..n as u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    build_burr_filter_bytes(policy, hashes).unwrap().len()
}

/// The estimate's contract is a tolerance against the built size. Wider than
/// the error the model measures at, because the bumped-key counts that drive
/// the later layers are a property of the hashes.
fn assert_estimate_tracks_build(policy: BloomConstructionPolicy, n: usize) {
    let estimate = policy.estimated_filter_size(n);
    let actual = built_len(policy, n);
    #[expect(
        clippy::cast_precision_loss,
        reason = "test code: a ratio over counts far below f64's exact range"
    )]
    let error = (estimate as f64 - actual as f64) / actual as f64;
    assert!(
        error.abs() < 0.10,
        "{policy:?} at n={n}: estimate {estimate} vs built {actual} is {:+.1}%",
        error * 100.0,
    );
}

/// Key counts from a thousand up, where the estimate's tolerance holds for a
/// single build; a filter partition's few hundred keys are covered by the
/// partitioned writer's tests, over many partitions.
const ESTIMATE_FIXTURE_KEYS: [usize; 3] = [1_000, 10_000, 100_000];

#[test]
fn burr_estimated_size_bpk() {
    for n in ESTIMATE_FIXTURE_KEYS {
        assert_estimate_tracks_build(BloomConstructionPolicy::BitsPerKey(10.0), n);
    }
}

#[test]
fn burr_estimated_size_fpr() {
    // ceil(-log2(0.01)) = 7 bits per key, an odd `r`.
    for n in ESTIMATE_FIXTURE_KEYS {
        assert_estimate_tracks_build(BloomConstructionPolicy::FalsePositiveRate(0.01), n);
    }
}

#[test]
fn build_burr_filter_bytes_empty_returns_empty() {
    let policy = BloomConstructionPolicy::BitsPerKey(10.0);
    let bytes = build_burr_filter_bytes(policy, Vec::new()).unwrap();
    assert!(bytes.is_empty());
}

#[test]
fn build_burr_filter_bytes_round_trips_via_reader() {
    use crate::table::filter::ribbon::burr::BurrFilterReader;
    let policy = BloomConstructionPolicy::FalsePositiveRate(0.01);
    let hashes: Vec<u64> = (0..1_000_u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let bytes = build_burr_filter_bytes(policy, hashes.clone()).unwrap();
    assert!(!bytes.is_empty());
    let reader = BurrFilterReader::new(&bytes).expect("reader");
    for h in &hashes {
        assert!(reader.contains_hash(*h), "inserted hash {h} not found");
    }
}
