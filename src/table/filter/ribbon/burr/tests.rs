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
