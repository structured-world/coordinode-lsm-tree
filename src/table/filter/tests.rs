use super::*;
use test_log::test;

/// Builds the filter a policy would build for `n` keys and returns the exact
/// serialised length, so an estimate can be checked against the real thing
/// rather than against a restatement of its own formula.
fn built_len(policy: BloomConstructionPolicy, n: usize) -> usize {
    use crate::table::filter::ribbon::burr::BurrBuilder;
    let params = policy.burr_params(n).expect("policy is active");
    let builder = BurrBuilder::new(params).expect("builder");
    let hashes: Vec<u64> = (0..n as u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    builder
        .build_from_hashes_owned(hashes)
        .expect("build")
        .encoded_len()
}

/// The estimate's contract is a tolerance against the built size, so both
/// tests below check exactly that. Wider than the ~4% the model measures at,
/// because the bumped-key counts that drive the later layers are a property
/// of the hashes and a different key set moves them.
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
        "{policy:?} at n={n}: estimate {estimate} vs built {actual} is {:+.1}%, \
         outside the tolerance the estimate documents",
        error * 100.0,
    );
}

#[test]
fn burr_estimated_size_bpk() {
    // One million keys is the size a partition estimate has to stay sane at,
    // and it is the point the smaller fixtures in the BuRR tests do not reach.
    assert_estimate_tracks_build(BloomConstructionPolicy::BitsPerKey(10.0), 1_000_000);
}

#[test]
fn burr_estimated_size_fpr() {
    // ceil(-log2(0.01)) = 7 bits per key, so this also covers an odd `r` that
    // the bits-per-key fixtures (8, 10, 16) do not.
    assert_estimate_tracks_build(BloomConstructionPolicy::FalsePositiveRate(0.01), 1_000_000);
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
