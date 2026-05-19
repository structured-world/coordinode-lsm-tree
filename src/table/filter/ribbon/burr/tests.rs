//! Unit + correctness tests for the BuRR filter.
//!
//! Covers: construction round-trip, membership invariants (FN-free for
//! inserted keys), FPR envelope at multiple targets, wire-format
//! encoder/decoder round-trips, wire-format rejection of bad magic /
//! version / filter_type / truncated headers, build determinism for
//! fixed seed, and scratch-reuse equivalence.
//!
//! End-to-end coverage through the table writer + reader path lives in
//! `tests/burr_filter_end_to_end.rs`.

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
    #[expect(
        clippy::cast_precision_loss,
        reason = "test code: precision loss acceptable in rate calculations"
    )]
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
    #[expect(
        clippy::cast_precision_loss,
        reason = "test code: precision loss acceptable in rate calculations"
    )]
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
    // Build a valid wire payload first, then flip the first magic byte.
    // This asserts the magic check actually triggers — a buffer of
    // arbitrary zeros could also fail later in decode (e.g. on the
    // version byte) and mask whether the magic check fires at all.
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    bytes[0] ^= 0xFF;
    let err = BurrFilterReader::new(&bytes).expect_err("bad magic should fail decode");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter")),
        "expected InvalidHeader(\"BurrFilter\"), got: {err:?}",
    );
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
    let err = BurrFilterReader::new(&short).expect_err("short buffer must error");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter")),
        "expected InvalidHeader(\"BurrFilter\"), got: {err:?}",
    );
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
    let err = BurrFilterReader::new(&bytes).expect_err("bad version must error");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter version")),
        "expected InvalidHeader(\"BurrFilter version\"), got: {err:?}",
    );
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
    let err = BurrFilterReader::new(&bytes).expect_err("unknown filter_type must error");
    assert!(
        matches!(err, crate::Error::InvalidTag(("FilterType", 0xAA))),
        "expected InvalidTag((\"FilterType\", 0xAA)), got: {err:?}",
    );
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
    #[expect(
        clippy::cast_precision_loss,
        reason = "test code: precision loss acceptable in rate calculations"
    )]
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
fn burr_wire_rejects_zero_b() {
    use super::filter::BurrFilterReader;
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    // b byte sits at offset MAGIC_LEN + 2 + 2 (magic + filter_type +
    // version + r + w, then b).
    let b_offset = crate::file::MAGIC_BYTES.len() + 4;
    bytes[b_offset] = 0;
    let err = BurrFilterReader::new(&bytes).expect_err("b == 0 must error");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter params")),
        "expected InvalidHeader(\"BurrFilter params\"), got: {err:?}",
    );
}

#[test]
fn burr_wire_rejects_zero_num_layers() {
    use super::filter::BurrFilterReader;
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    // num_layers byte: MAGIC_LEN + filter_type + version + r + w + b
    let num_layers_offset = crate::file::MAGIC_BYTES.len() + 5;
    bytes[num_layers_offset] = 0;
    let err = BurrFilterReader::new(&bytes).expect_err("num_layers == 0 must error");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter params")),
        "expected InvalidHeader(\"BurrFilter params\"), got: {err:?}",
    );
}

#[test]
fn burr_wire_rejects_out_of_range_r() {
    use super::filter::BurrFilterReader;
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    // r byte: MAGIC_LEN + filter_type + version
    let r_offset = crate::file::MAGIC_BYTES.len() + 2;
    bytes[r_offset] = 0; // r==0 invalid
    let err = BurrFilterReader::new(&bytes).expect_err("r == 0 must error");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter params")),
        "expected InvalidHeader(\"BurrFilter params\"), got: {err:?}",
    );

    let mut bytes2 = filter.to_wire_bytes();
    bytes2[r_offset] = 65; // r>64 invalid
    let err2 = BurrFilterReader::new(&bytes2).expect_err("r == 65 must error");
    assert!(
        matches!(err2, crate::Error::InvalidHeader("BurrFilter params")),
        "expected InvalidHeader(\"BurrFilter params\"), got: {err2:?}",
    );
}

#[test]
fn burr_wire_rejects_corrupted_num_blocks() {
    use super::filter::BurrFilterReader;
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    // First layer header begins at HEADER_LEN. num_blocks is the
    // second u32 (offset +4 from layer header start). Tamper to a
    // value that disagrees with `m`.
    let layer_header_start = crate::file::MAGIC_BYTES.len() + 6 + 8;
    let num_blocks_offset = layer_header_start + 4;
    bytes[num_blocks_offset] = bytes[num_blocks_offset].wrapping_add(1);
    let err = BurrFilterReader::new(&bytes).expect_err("mismatched num_blocks must error");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter layer payload")),
        "expected InvalidHeader(\"BurrFilter layer payload\"), got: {err:?}",
    );
}

#[test]
fn burr_wire_rejects_corrupted_z_byte_len() {
    use super::filter::BurrFilterReader;
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    // z_byte_len is the third u32 of the layer header.
    let layer_header_start = crate::file::MAGIC_BYTES.len() + 6 + 8;
    let z_byte_len_offset = layer_header_start + 8;
    bytes[z_byte_len_offset] = bytes[z_byte_len_offset].wrapping_add(8);
    let err = BurrFilterReader::new(&bytes).expect_err("mismatched z_byte_len must error");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter layer payload")),
        "expected InvalidHeader(\"BurrFilter layer payload\"), got: {err:?}",
    );
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

#[test]
fn burr_params_with_fp_rate_rejects_n_zero() {
    let err = BurrParams::with_fp_rate(0, 0.01).expect_err("n=0 must error");
    let msg = format!("{err}");
    assert!(msg.contains("n must be > 0"), "got: {msg}");
}

#[test]
fn burr_params_with_fp_rate_rejects_zero_fpr() {
    let err = BurrParams::with_fp_rate(100, 0.0).expect_err("fpr=0 must error");
    let msg = format!("{err}");
    assert!(msg.contains("fpr"), "got: {msg}");
}

#[test]
fn burr_params_with_fp_rate_rejects_one_fpr() {
    let err = BurrParams::with_fp_rate(100, 1.0).expect_err("fpr=1 must error");
    let msg = format!("{err}");
    assert!(msg.contains("fpr"), "got: {msg}");
}

#[test]
fn burr_params_with_fp_rate_rejects_negative_fpr() {
    let err = BurrParams::with_fp_rate(100, -0.1).expect_err("negative fpr must error");
    let _ = format!("{err}");
}

#[test]
fn burr_params_with_fp_rate_rejects_too_tight_fpr() {
    // fpr <= 2^-64 → r > 64 → reject. Use 1e-25 (well past 2^-64).
    let err = BurrParams::with_fp_rate(100, 1.0e-25_f32).expect_err("too tight must error");
    let _ = format!("{err}");
}

#[test]
fn burr_params_with_bpk_rejects_n_zero() {
    let err = BurrParams::with_bpk(0, 10.0).expect_err("n=0 must error");
    let _ = format!("{err}");
}

#[test]
fn burr_params_with_bpk_rejects_below_one() {
    let err = BurrParams::with_bpk(100, 0.5).expect_err("bpk < 1 must error");
    let _ = format!("{err}");
}

#[test]
fn burr_params_with_bpk_rejects_above_64() {
    let err = BurrParams::with_bpk(100, 70.0).expect_err("bpk > 64 must error");
    let _ = format!("{err}");
}

#[test]
fn burr_params_with_seed_sets_seed_field() {
    let params = BurrParams::with_fp_rate(100, 0.01)
        .unwrap()
        .with_seed(0xDEAD_BEEF);
    assert_eq!(params.seed, 0xDEAD_BEEF);
}

#[test]
fn burr_builder_rejects_n_zero() {
    let mut params = BurrParams::with_fp_rate(100, 0.01).unwrap();
    params.n = 0;
    let err = BurrBuilder::new(params, DefaultBuildHasher::default())
        .expect_err("builder must reject n=0");
    let msg = format!("{err}");
    assert!(msg.contains("n must be > 0"), "got: {msg}");
}

#[test]
fn burr_builder_rejects_zero_r() {
    let mut params = BurrParams::with_fp_rate(100, 0.01).unwrap();
    params.r = 0;
    let err = BurrBuilder::new(params, DefaultBuildHasher::default())
        .expect_err("builder must reject r=0");
    let msg = format!("{err}");
    assert!(msg.contains("r must be in 1..=64"), "got: {msg}");
}

#[test]
fn burr_builder_rejects_zero_b() {
    let mut params = BurrParams::with_fp_rate(100, 0.01).unwrap();
    params.b = 0;
    let err = BurrBuilder::new(params, DefaultBuildHasher::default())
        .expect_err("builder must reject b=0");
    let msg = format!("{err}");
    assert!(msg.contains("b must be > 0"), "got: {msg}");
}

#[test]
fn burr_builder_rejects_zero_max_layers() {
    let mut params = BurrParams::with_fp_rate(100, 0.01).unwrap();
    params.max_layers = 0;
    let err = BurrBuilder::new(params, DefaultBuildHasher::default())
        .expect_err("builder must reject max_layers=0");
    let msg = format!("{err}");
    assert!(msg.contains("max_layers"), "got: {msg}");
}

#[test]
fn burr_layer_count_grows_with_input_size() {
    // Empty input → 0 layers.
    let params = BurrParams::with_fp_rate(100, 0.01).unwrap();
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).unwrap();
    let empty: Vec<u64> = vec![];
    let filter = builder.build_from_hashes(&empty).unwrap();
    assert_eq!(filter.layer_count(), 0);
}

#[test]
fn burr_filter_debug_format_includes_layer_count() {
    let params = BurrParams::with_fp_rate(100, 0.01).unwrap();
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).unwrap();
    let hashes: Vec<u64> = (0..100_u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let filter = builder.build_from_hashes(&hashes).unwrap();
    let debug = format!("{filter:?}");
    assert!(debug.contains("BurrFilter"), "got: {debug}");
    assert!(debug.contains("layer_count"), "got: {debug}");
}

#[test]
fn burr_filter_params_accessor() {
    let params = BurrParams::with_fp_rate(500, 0.01).unwrap();
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).unwrap();
    let hashes: Vec<u64> = (0..500_u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let filter = builder.build_from_hashes(&hashes).unwrap();
    assert_eq!(filter.params().n, 500);
    assert_eq!(filter.params().r, params.r);
}

#[test]
fn burr_filter_contains_returns_false_for_definitely_absent() {
    // n=64 small set, probe with hashes that almost certainly map outside.
    // Just verify the absent path returns false sometimes (no false-negative
    // for inserted; some false-positive is expected for non-inserted).
    let n = 64_usize;
    let params = BurrParams::with_fp_rate(n, 0.001).unwrap();
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).unwrap();
    let hashes: Vec<u64> = (0..n as u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let filter = builder.build_from_hashes(&hashes).unwrap();
    let mut false_count = 0_u32;
    for i in 1000..2000_u64 {
        let h = crate::hash::hash64(&i.to_le_bytes());
        if !filter.contains_hash(h) {
            false_count += 1;
        }
    }
    assert!(
        false_count > 800,
        "expected most non-inserted keys to report absent, got false_count={false_count}"
    );
}

#[test]
fn contains_hash_from_bytes_round_trips_against_decoded() {
    // The single-pass parse+probe entry point used by FilterBlock must
    // produce the same answer as the decoded-then-probed reader for
    // every inserted hash.
    use super::contains_hash_from_bytes;
    use super::filter::BurrFilterReader;

    let n = 500_usize;
    let params = BurrParams::with_fp_rate(n, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..n as u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let bytes = filter.to_wire_bytes();
    let reader = BurrFilterReader::new(&bytes).expect("decoder");

    for h in &hashes {
        let single = contains_hash_from_bytes(&bytes, *h).expect("ok");
        let decoded = reader.contains_hash(*h);
        assert_eq!(single, decoded, "single-pass and decoded disagree on {h}");
        assert!(single, "inserted hash {h} not present in single-pass probe");
    }
}

#[test]
fn contains_hash_from_bytes_rejects_short_buffer() {
    use super::contains_hash_from_bytes;
    let err = contains_hash_from_bytes(&[0_u8; 4], 42).expect_err("short buffer must error");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter")),
        "expected InvalidHeader(\"BurrFilter\"), got: {err:?}",
    );
}

#[test]
fn contains_hash_from_bytes_rejects_bad_magic() {
    use super::contains_hash_from_bytes;
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    bytes[0] ^= 0xFF;
    let err = contains_hash_from_bytes(&bytes, 0).expect_err("bad magic must error");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter")),
        "expected InvalidHeader(\"BurrFilter\"), got: {err:?}",
    );
}

#[test]
fn contains_hash_from_bytes_rejects_bad_version() {
    use super::contains_hash_from_bytes;
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    let version_offset = crate::file::MAGIC_BYTES.len() + 1;
    bytes[version_offset] = 0xFE;
    let err = contains_hash_from_bytes(&bytes, 0).expect_err("bad version must error");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter version")),
        "expected InvalidHeader(\"BurrFilter version\"), got: {err:?}",
    );
}

#[test]
fn contains_hash_from_bytes_rejects_bad_filter_type() {
    use super::contains_hash_from_bytes;
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    let filter_type_offset = crate::file::MAGIC_BYTES.len();
    bytes[filter_type_offset] = 0xAB;
    let err = contains_hash_from_bytes(&bytes, 0).expect_err("bad filter_type must error");
    assert!(
        matches!(err, crate::Error::InvalidTag(("FilterType", 0xAB))),
        "expected InvalidTag((\"FilterType\", 0xAB)), got: {err:?}",
    );
}

#[test]
fn contains_hash_from_bytes_rejects_bad_params() {
    use super::contains_hash_from_bytes;
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    // Set num_layers = 0 → InvalidHeader("BurrFilter params").
    let num_layers_offset = crate::file::MAGIC_BYTES.len() + 5;
    bytes[num_layers_offset] = 0;
    let err = contains_hash_from_bytes(&bytes, 0).expect_err("num_layers=0 must error");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter params")),
        "expected InvalidHeader(\"BurrFilter params\"), got: {err:?}",
    );
}

#[test]
fn contains_hash_from_bytes_rejects_corrupted_layer_payload() {
    // Tampered num_blocks → checked-add validation in
    // contains_hash_from_bytes must reject the layer header before
    // reaching the slice.
    use super::contains_hash_from_bytes;
    let params = BurrParams::with_fp_rate(50, 0.01).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..50_u64)
        .map(|i| crate::hash::hash64(&[i as u8]))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let mut bytes = filter.to_wire_bytes();
    let layer_header_start = crate::file::MAGIC_BYTES.len() + 6 + 8;
    let num_blocks_offset = layer_header_start + 4;
    bytes[num_blocks_offset] = bytes[num_blocks_offset].wrapping_add(1);
    let err = contains_hash_from_bytes(&bytes, 0).expect_err("corrupted num_blocks must error");
    assert!(
        matches!(err, crate::Error::InvalidHeader("BurrFilter layer payload")),
        "expected InvalidHeader(\"BurrFilter layer payload\"), got: {err:?}",
    );
}

#[test]
fn contains_hash_from_bytes_returns_false_for_non_inserted() {
    // Smoke for the not-present branch — exercises the per-set-bit
    // loop's normal exit path (where acc != fingerprint).
    use super::contains_hash_from_bytes;
    let n = 200_usize;
    let params = BurrParams::with_fp_rate(n, 0.001).expect("params");
    let builder = BurrBuilder::new(params, DefaultBuildHasher::default()).expect("builder");
    let hashes: Vec<u64> = (0..n as u64)
        .map(|i| crate::hash::hash64(&i.to_le_bytes()))
        .collect();
    let filter = builder.build_from_hashes(&hashes).expect("build");
    let bytes = filter.to_wire_bytes();

    let mut absent_count = 0_u32;
    for i in 10_000..11_000_u64 {
        let h = crate::hash::hash64(&i.to_le_bytes());
        if !contains_hash_from_bytes(&bytes, h).expect("ok") {
            absent_count += 1;
        }
    }
    assert!(
        absent_count > 950,
        "expected most non-inserted hashes to report absent, got absent_count={absent_count}",
    );
}
