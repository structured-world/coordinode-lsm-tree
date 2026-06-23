use super::*;
use test_log::test;

#[test]
fn burr_estimated_size_bpk() {
    let policy = BloomConstructionPolicy::BitsPerKey(10.0);
    let n = 1_000_000;
    let estimated_size = policy.estimated_filter_size(n);
    // 10 bits/key × 1M keys × 1.05 overhead / 8 ≈ 1.31 MB
    assert!(estimated_size > 1_200_000);
    assert!(estimated_size < 1_400_000);
}

#[test]
fn burr_estimated_size_fpr() {
    let policy = BloomConstructionPolicy::FalsePositiveRate(0.01);
    let n = 1_000_000;
    let estimated_size = policy.estimated_filter_size(n);
    // ceil(-log2(0.01)) = 7 bits/key → 7M bits × 1.05 / 8 ≈ 918 KB
    assert!(estimated_size > 800_000);
    assert!(estimated_size < 1_000_000);
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
