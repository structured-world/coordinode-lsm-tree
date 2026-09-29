// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::{as_f64, cheapest, estimate, partition_keys, price, shrunk_densities};
use crate::config::BloomConstructionPolicy;

const WIDTHS: [u8; 6] = [6, 8, 10, 12, 14, 16];

/// A filter drawing many negative probes takes the widest width while bytes
/// cost little; one drawing none takes the narrowest once bytes cost
/// anything, and the static policy's when they cost nothing.
#[test]
fn the_cheapest_width_follows_the_load_and_the_price() {
    let n = 10_000;
    assert_eq!(cheapest(1e9, n, &WIDTHS, 10, 1e-6), 16);
    assert_eq!(cheapest(0.0, n, &WIDTHS, 10, 1e-6), 6);
    assert_eq!(cheapest(0.0, n, &WIDTHS, 10, 0.0), 10);
    assert_eq!(cheapest(1e9, n, &WIDTHS, 10, f64::INFINITY), 6);
}

/// With room for every widest choice the bytes cost nothing; below the
/// narrowest filters they cost infinitely; in between the price fills the
/// budget without passing it.
#[test]
fn the_price_fills_the_budget() {
    let loads = [(1e6, 10_000), (1.0, 10_000)];
    let at = |bits| estimate(10_000, bits);

    // Exactly zero, the value returned when every widest choice fits.
    assert_eq!(
        price(&loads, &WIDTHS, 10, 2 * at(16), None).to_bits(),
        0.0f64.to_bits()
    );
    assert!(price(&loads, &WIDTHS, 10, 2 * at(6) - 1, None).is_infinite());

    let budget = at(16) + at(6);
    let p = price(&loads, &WIDTHS, 10, budget, None);
    assert!(p.is_finite() && p > 0.0, "price {p}");
    let hot = cheapest(1e6, 10_000, &WIDTHS, 10, p);
    let cold = cheapest(1.0, 10_000, &WIDTHS, 10, p);
    assert!(at(hot) + at(cold) <= budget, "{hot} + {cold} bits");
    // The memory goes where the negative probes are.
    assert!(hot > cold, "hot {hot}, cold {cold}");
}

/// Tables drawing no negative probe take the static policy's width while it
/// fits, and when it does not, a price at which they take the narrowest:
/// finite and positive, though no width saves them a false positive.
#[test]
fn unprobed_tables_narrow_when_the_static_width_does_not_fit() {
    let n = 10_000;
    let idle = [(0.0, n), (0.0, n)];
    assert_eq!(
        price(&idle, &WIDTHS, 10, 2 * estimate(n, 10), None).to_bits(),
        0.0f64.to_bits()
    );
    let p = price(&idle, &WIDTHS, 10, 2 * estimate(n, 10) - 1, None);
    assert!(p.is_finite() && p > 0.0, "price {p}");
    assert_eq!(cheapest(0.0, n, &WIDTHS, 10, p), 6);
}

/// At one budget, a uniform load splits it evenly.
#[test]
fn a_uniform_load_splits_the_budget_evenly() {
    let n = 10_000;
    let budget = 2 * estimate(n, 11);
    let uniform = [(1_000.0, n), (1_000.0, n)];
    let p = price(&uniform, &WIDTHS, 10, budget, None);
    let first = cheapest(1_000.0, n, &WIDTHS, 10, p);
    let second = cheapest(1_000.0, n, &WIDTHS, 10, p);
    assert_eq!(first, second);
    assert!(estimate(n, first) * 2 <= budget);
}

/// Counts that differ no more than chance would make them are pulled to their
/// mean, so an even load gives every table the same density; counts that
/// differ far beyond chance keep their order and most of their spread.
#[test]
fn chance_differences_are_shrunk_real_ones_kept() {
    // About a thousand negative probes per ten thousand keys, spread well
    // within the square root of a thousand.
    let even = [
        (1_010.0, 10_000),
        (990.0, 10_000),
        (1_020.0, 10_000),
        (980.0, 10_000),
    ];
    let densities = shrunk_densities(&even);
    let mean = 4_000.0 / 40_000.0;
    for density in &densities {
        assert!((density - mean).abs() < 1e-12, "{densities:?}");
    }

    let skewed = [
        (9_000.0, 10_000),
        (100.0, 10_000),
        (120.0, 10_000),
        (80.0, 10_000),
    ];
    let [hot, cold, _, coldest] = <[f64; 4]>::try_from(shrunk_densities(&skewed))
        .unwrap_or_else(|densities| panic!("one density per table: {densities:?}"));
    assert!(hot > 0.8, "hot {hot}");
    assert!(
        cold < 0.02 && coldest < cold,
        "cold {cold}, coldest {coldest}"
    );
}

/// The size bound a writer reserves with holds for the short filters that end
/// a table, at every admissible width.
#[test]
fn the_size_bound_holds_for_short_filters() -> crate::Result<()> {
    for n in [1usize, 2, 10, 100, 301, 454, 1_000, 2_621] {
        for bits in 2..=16u8 {
            let policy = BloomConstructionPolicy::BitsPerKey(f32::from(bits));
            let hashes: Vec<u64> = (0..n as u64)
                .map(|i| crate::hash::hash64(&i.to_le_bytes()))
                .collect();
            let built = crate::table::filter::build_burr_filter_bytes(policy, hashes)?.len();
            let bound = policy.filter_size_bound(n);
            assert!(
                built <= bound,
                "{n} keys at {bits} bits: built {built}, bound {bound}"
            );
        }
    }
    Ok(())
}

/// A partitioned filter pays each partition's fixed overhead, so it takes
/// more bytes than one filter over the same keys, and the price counts it
/// that way: a budget one filter per table would fill leaves a partitioned
/// level a width narrower.
#[test]
fn the_price_counts_partitioned_filters_as_partitions() {
    let policy = BloomConstructionPolicy::BitsPerKey(10.0);
    let keys = partition_keys(policy, 4_096);
    assert!(policy.estimated_filter_size(keys) >= 4_096);
    assert!(policy.estimated_filter_size(keys - 1) < 4_096);

    let n = 20_000;
    let partitions = (n / keys) as u64 * estimate(keys, 10) + estimate(n % keys, 10);
    assert!(partitions > estimate(n, 10));

    let loads = [(1_000.0, n), (1_000.0, n)];
    let budget = 2 * estimate(n, 10);
    let whole = price(&loads, &WIDTHS, 10, budget, None);
    let split = price(&loads, &WIDTHS, 10, budget, Some(keys));
    assert!(split > whole, "whole {whole}, split {split}");
    let per_partition = 1_000.0 * as_f64(keys as u64) / as_f64(n as u64);
    assert!(
        cheapest(per_partition, keys, &WIDTHS, 10, split)
            < cheapest(1_000.0, n, &WIDTHS, 10, whole),
        "the partitioned level takes a narrower width at the same budget"
    );
}

/// The key ranges of a split compaction price alike in whatever order their
/// writers reach them: before anything is built, the first filter of the
/// upper range prices by all of the rewrite's data, as the first filter of an
/// unsplit rewrite does, not by the data above it alone.
#[test]
fn split_ranges_price_by_all_data_still_to_come() -> crate::Result<()> {
    use crate::config::{BlockSizePolicy, FilterAdvisor};
    use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};
    use core::ops::Bound;

    const KEYS: u32 = 20_000;
    let folder = tempfile::tempdir()?;
    let advisor = FilterAdvisor::new(u64::from(2 * KEYS) * 12 / 8);
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .filter_advisor(Some(advisor.clone()))
    .open()?;
    let mut seqno = 0;
    for prefix in ["a-hot", "z-cold"] {
        for i in 0..KEYS {
            any.insert(format!("{prefix}{:06}", 2 * i), "value", seqno);
            seqno += 1;
        }
        any.flush_active_memtable(0)?;
    }
    for i in 0..KEYS {
        assert!(
            any.get(format!("a-hot{:06}", 2 * i + 1), SeqNo::MAX)?
                .is_none()
        );
    }
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    let version = tree.current_version();
    let inputs: Vec<crate::Table> = version.iter_tables().cloned().collect();
    let plan = |split: Option<super::Split>| {
        super::plan(
            &advisor,
            &tree.filter_budget,
            version.iter_tables(),
            super::Rewrite {
                inputs: inputs.clone(),
                split,
                ..super::Rewrite::default()
            },
            BloomConstructionPolicy::BitsPerKey(10.0),
            None,
        )
        .unwrap_or_else(|| panic!("the advisor plans the filters"))
    };

    // One plan at a time: each takes the credit for the inputs it replaces.
    let unsplit = plan(None).current_price(Bound::Unbounded)?;
    let split = plan(Some(super::Split {
        boundaries: alloc::vec![crate::UserKey::from("m")],
        comparator: crate::comparator::default_comparator(),
    }));
    let upper_first = split.current_price(Bound::Included(b"z-cold000000"))?;
    assert!(
        (upper_first - unsplit).abs() <= unsplit * 1e-9,
        "upper range first {upper_first}, unsplit {unsplit}"
    );
    Ok(())
}
