//! Smoke + correctness tests for the BuRR filter MVP.
//!
//! Full FPR/bench/edge-case suite lands with task #19 (separate commit);
//! this file covers the construction round-trip and basic membership
//! invariants so the algorithm is exercised end-to-end as soon as the
//! multi-layer builder + probe path go in.

use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;

use super::{BurrBuilder, BurrParams};

type DefaultBuildHasher = BuildHasherDefault<DefaultHasher>;

#[test]
fn burr_builds_and_reports_inserted_keys_present() {
    let n = 1_000_usize;
    let params = BurrParams::with_fp_rate(n, 0.01).expect("valid params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let keys: Vec<u64> = (0..n as u64).collect();
    let filter = builder.build(&keys).expect("build");

    // Every inserted key must report as present (no false negatives).
    for key in &keys {
        assert!(
            filter.contains(key),
            "inserted key {key} reported absent — BuRR must be FN-free",
        );
    }
}

#[test]
fn burr_fpr_at_one_percent_is_within_envelope() {
    // Build with FPR=0.01 over a moderate key set, probe with disjoint
    // non-keys, measure realised FPR. Allow up to 5% to give the small
    // sample size some slack.
    let n = 1_000_usize;
    let params = BurrParams::with_fp_rate(n, 0.01).expect("valid params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let keys: Vec<u64> = (0..n as u64).collect();
    let filter = builder.build(&keys).expect("build");

    let probe_count = 10_000_usize;
    let mut false_positives = 0_usize;
    for key in (n as u64)..(n as u64 + probe_count as u64) {
        if filter.contains(&key) {
            false_positives += 1;
        }
    }
    #[allow(clippy::cast_precision_loss)]
    let fpr = false_positives as f64 / probe_count as f64;
    assert!(
        fpr < 0.05,
        "realised FPR {fpr} too high (wanted ≤ 5% envelope around 1% target)",
    );
}

#[test]
fn burr_wire_format_round_trips() {
    // Build a BuRR, serialize to wire bytes, parse via
    // BurrFilterReader, and verify contains_hash answers match
    // BurrFilter::contains for every inserted key.
    use super::filter::BurrFilterReader;
    use std::hash::BuildHasher;

    let n = 500_usize;
    let params = BurrParams::with_fp_rate(n, 0.01).expect("valid params");
    let hasher = DefaultBuildHasher::default();
    let builder = BurrBuilder::new(params, hasher.clone()).expect("builder");
    let keys: Vec<u64> = (0..n as u64).collect();
    let filter = builder.build(&keys).expect("build");

    let bytes = filter.to_wire_bytes();
    assert!(bytes.len() > 20, "wire buffer too small ({})", bytes.len());

    let reader = BurrFilterReader::new(&bytes).expect("parse");
    assert_eq!(
        reader.layer_count(),
        filter.layer_count(),
        "decoded layer count must match",
    );

    // The reader's contains_hash takes a pre-computed u64. We must
    // use the SAME hasher state the BurrFilter was built with so the
    // base_hash matches. BuildHasher::hash_one is the convention used
    // by both sides.
    for key in &keys {
        let h = hasher.hash_one(key);
        assert!(
            reader.contains_hash(h),
            "inserted key {key} not found in decoded reader (hash {h})",
        );
    }
}

#[test]
fn burr_build_from_hashes_and_contains_hash_round_trip() {
    // The hash-based build + probe pair is what the LSM filter writer
    // and reader use. Insert n xxh3-hashed u64s and verify
    // contains_hash reports each as present.
    let n = 500_usize;
    let params = BurrParams::with_fp_rate(n, 0.01).expect("valid params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");

    let hashes: Vec<u64> = (0..n as u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let filter = builder
        .build_from_hashes(&hashes)
        .expect("build_from_hashes");

    for h in &hashes {
        assert!(
            filter.contains_hash(*h),
            "inserted hash {h} reported absent",
        );
    }

    // FPR sanity: probe disjoint non-key hashes, must be ≤ 5%.
    let probe_count = 10_000_usize;
    let mut false_positives = 0_usize;
    for i in (n as u64)..(n as u64 + probe_count as u64) {
        let h = crate::hash::hash64(&i.to_le_bytes());
        if filter.contains_hash(h) {
            false_positives += 1;
        }
    }
    #[allow(clippy::cast_precision_loss)]
    let fpr = false_positives as f64 / probe_count as f64;
    assert!(fpr < 0.05, "realised FPR {fpr} too high");
}

#[test]
fn burr_hash_build_wire_format_round_trips() {
    // Build via build_from_hashes, serialize, decode via reader,
    // contains_hash must match.
    use super::filter::BurrFilterReader;

    let n = 500_usize;
    let params = BurrParams::with_fp_rate(n, 0.01).expect("valid params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..n as u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");

    let bytes = filter.to_wire_bytes();
    let reader = BurrFilterReader::new(&bytes).expect("decode");

    for h in &hashes {
        assert!(
            reader.contains_hash(*h),
            "inserted hash {h} not found via wire-format reader",
        );
    }
}

#[test]
fn burr_wire_rejects_bad_magic() {
    use super::filter::BurrFilterReader;
    let mut bytes = vec![0_u8; 32];
    bytes[0] = 0xDE;
    bytes[1] = 0xAD;
    let result = BurrFilterReader::new(&bytes);
    assert!(result.is_err(), "bad magic should fail decode");
}

#[test]
fn burr_single_key_round_trips() {
    // Smallest possible filter. Last-layer enlargement must accommodate
    // n=1 without LayerExhaustion. Hash-based + key-based both work.
    let params = BurrParams::with_fp_rate(1, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let key_hash = crate::hash::hash64(b"only-one");
    let filter = builder
        .build_from_hashes(&[key_hash])
        .expect("build_from_hashes for n=1");
    assert!(filter.contains_hash(key_hash));
    let bytes = filter.to_wire_bytes();
    let reader = super::filter::BurrFilterReader::new(&bytes).expect("decode");
    assert!(reader.contains_hash(key_hash));
}

#[test]
fn burr_build_is_deterministic_for_fixed_seed() {
    // Same params + same input → same wire bytes. Wire format must not
    // depend on hash-map iteration order or any other non-deterministic
    // source. Anyone shipping BuRR filter blocks across hosts relies on
    // this.
    let params = BurrParams::with_fp_rate(200, 0.01).expect("params");
    let hashes: Vec<u64> = (0..200_u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let bytes_a = {
        let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
        builder
            .build_from_hashes(&hashes)
            .expect("build")
            .to_wire_bytes()
    };
    let bytes_b = {
        let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
        builder
            .build_from_hashes(&hashes)
            .expect("build")
            .to_wire_bytes()
    };
    assert_eq!(bytes_a, bytes_b);
}

#[test]
fn burr_wire_rejects_short_buffer() {
    // Anything below the fixed header length must be rejected without
    // panic. Important for hardening against truncated on-disk blocks.
    use super::filter::BurrFilterReader;
    let short = vec![0_u8; 4];
    let result = BurrFilterReader::new(&short);
    assert!(result.is_err(), "short buffer must error");
}

#[test]
fn burr_wire_rejects_unknown_version() {
    // Build a real filter, mutate the version byte, decode must fail.
    use super::filter::BurrFilterReader;
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    // version byte sits at offset MAGIC_LEN + 1 (after filter_type).
    let version_offset = crate::file::MAGIC_BYTES.len() + 1;
    bytes[version_offset] = 0xFE;
    let result = BurrFilterReader::new(&bytes);
    assert!(result.is_err(), "bad version must error");
}

#[test]
fn burr_wire_rejects_unknown_filter_type() {
    use super::filter::BurrFilterReader;
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    let filter_type_offset = crate::file::MAGIC_BYTES.len();
    bytes[filter_type_offset] = 0xAA;
    let result = BurrFilterReader::new(&bytes);
    assert!(result.is_err(), "unknown filter_type must error");
}

#[test]
fn burr_negative_keys_obey_fpr_envelope_at_low_target() {
    // Tight FPR (0.001) over moderate n. Realised FPR over disjoint
    // probes must stay within a safety envelope around the target.
    let n = 2_000_usize;
    let params = BurrParams::with_fp_rate(n, 0.001).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..n as u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");

    let probe_count = 20_000_usize;
    let mut false_positives = 0_usize;
    for i in (n as u64)..(n as u64 + probe_count as u64) {
        let h = crate::hash::hash64(&i.to_le_bytes());
        if filter.contains_hash(h) {
            false_positives += 1;
        }
    }
    #[allow(clippy::cast_precision_loss)]
    let fpr = false_positives as f64 / probe_count as f64;
    // BuRR at FPR=0.001 typically realises ≤ 0.5%. Allow envelope.
    assert!(fpr < 0.01, "realised FPR {fpr} > 1% envelope around 0.1%");
}

#[test]
fn burr_contains_in_matches_contains_with_external_scratch() {
    // The allocation-free probe path (contains_in with caller scratch)
    // must agree with the convenience contains for every key in the set.
    let n = 300_usize;
    let params = BurrParams::with_fp_rate(n, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let keys: Vec<u64> = (0..n as u64).collect();
    let filter = builder.build(&keys).expect("build");
    let mut scratch = filter.new_scratch();
    for key in &keys {
        let via_contains = filter.contains(key);
        let via_contains_in = filter.contains_in(key, &mut scratch);
        assert_eq!(
            via_contains, via_contains_in,
            "probe paths disagree on {key}"
        );
        assert!(via_contains, "inserted key {key} not present");
    }
}

#[test]
fn burr_settles_in_few_layers() {
    let n = 5_000_usize;
    let params = BurrParams::with_fp_rate(n, 0.01).expect("valid params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let keys: Vec<u64> = (0..n as u64).collect();
    let filter = builder.build(&keys).expect("build");

    // BuRR's design target is 1-3 layers for well-tuned parameters.
    // Each layer absorbs ~90% of incoming keys; 3 layers reach ≈
    // 0.1³ ≈ 0.1% residual. The last layer absorbs the rest at full
    // load.
    let layer_count = filter.layer_count();
    assert!(
        (1..=4).contains(&layer_count),
        "layer count {layer_count} outside expected 1..=4 range",
    );
}
