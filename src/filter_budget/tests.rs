// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::{Load, as_f64, cheapest, estimate, partition_keys, price, shrunk_densities};
use crate::config::BloomConstructionPolicy;

const WIDTHS: [u8; 6] = [6, 8, 10, 12, 14, 16];

/// The price a filter over keys from `lower` is chosen at, as
/// `FilterSizing::candidates` sets it: the filter enters flight first.
fn priced_at(sizing: &super::FilterSizing, lower: core::ops::Bound<&[u8]>) -> crate::Result<f64> {
    sizing.enter(lower);
    sizing.current_price()
}

/// Filter blocks without framing: the price in payload bytes alone.
fn bare(len: u64) -> u64 {
    len
}

/// `(negative probes, keys)` of tables built alike: at a static policy of
/// `fallback_bits`, partitioned into `partition_keys` keys when given.
fn at(loads: &[(f64, usize)], fallback_bits: u8, partition_keys: Option<usize>) -> Vec<Load> {
    loads
        .iter()
        .map(|&(negatives, keys)| Load {
            negatives,
            keys,
            fallback_bits,
            partition_keys,
        })
        .collect()
}

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
    let loads = at(&[(1e6, 10_000), (1.0, 10_000)], 10, None);
    let bytes = |bits| estimate(10_000, bits);

    // Exactly zero, the value returned when every widest choice fits.
    assert_eq!(
        price(&loads, &WIDTHS, 2 * bytes(16), &bare).to_bits(),
        0.0f64.to_bits()
    );
    assert!(price(&loads, &WIDTHS, 2 * bytes(6) - 1, &bare).is_infinite());

    let budget = bytes(16) + bytes(6);
    let p = price(&loads, &WIDTHS, budget, &bare);
    assert!(p.is_finite() && p > 0.0, "price {p}");
    let hot = cheapest(1e6, 10_000, &WIDTHS, 10, p);
    let cold = cheapest(1.0, 10_000, &WIDTHS, 10, p);
    assert!(bytes(hot) + bytes(cold) <= budget, "{hot} + {cold} bits");
    // The memory goes where the negative probes are.
    assert!(hot > cold, "hot {hot}, cold {cold}");
}

/// The budget counts filter blocks as written on disk, framing included, and
/// the price counts them the same way: a budget the widest payloads fit, but
/// not their framed blocks, is not one every widest filter fits.
#[test]
fn the_price_counts_each_block_with_its_framing() {
    let n = 1_000;
    let loads = at(&[(1e6, n), (1e6, n), (1e6, n), (1e6, n)], 10, None);
    let budget = 4 * estimate(n, 16);
    // Framing per block, as a header, a tag and parity add it.
    let framed = |len: u64| if len == 0 { 0 } else { len + 64 };
    assert_eq!(
        price(&loads, &WIDTHS, budget, &bare).to_bits(),
        0.0f64.to_bits()
    );
    let p = price(&loads, &WIDTHS, budget, &framed);
    assert!(p > 0.0, "price {p} with the framing counted");
}

/// Tables drawing no negative probe take the static policy's width while it
/// fits, and when it does not, a price at which they take the narrowest:
/// finite and positive, though no width saves them a false positive.
#[test]
fn unprobed_tables_narrow_when_the_static_width_does_not_fit() {
    let n = 10_000;
    let idle = at(&[(0.0, n), (0.0, n)], 10, None);
    assert_eq!(
        price(&idle, &WIDTHS, 2 * estimate(n, 10), &bare).to_bits(),
        0.0f64.to_bits()
    );
    let p = price(&idle, &WIDTHS, 2 * estimate(n, 10) - 1, &bare);
    assert!(p.is_finite() && p > 0.0, "price {p}");
    assert_eq!(cheapest(0.0, n, &WIDTHS, 10, p), 6);
}

/// At one budget, a uniform load splits it evenly.
#[test]
fn a_uniform_load_splits_the_budget_evenly() {
    let n = 10_000;
    let budget = 2 * estimate(n, 11);
    let uniform = at(&[(1_000.0, n), (1_000.0, n)], 10, None);
    let p = price(&uniform, &WIDTHS, budget, &bare);
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
    let whole = price(&at(&loads, 10, None), &WIDTHS, budget, &bare);
    let split = price(&at(&loads, 10, Some(keys)), &WIDTHS, budget, &bare);
    assert!(split > whole, "whole {whole}, split {split}");
    let per_partition = 1_000.0 * as_f64(keys as u64) / as_f64(n as u64);
    assert!(
        cheapest(per_partition, keys, &WIDTHS, 10, split)
            < cheapest(1_000.0, n, &WIDTHS, 10, whole),
        "the partitioned level takes a narrower width at the same budget"
    );
}

/// A table's short last partition is sized on its own, as its writer sizes
/// it: over few keys a filter's bytes per key run higher, so it takes a
/// narrower width than the full partitions at one price. A budget holding the
/// full partitions at their width and the tail at its own narrower one leaves
/// the full partitions that width, rather than pricing the tail at theirs.
#[test]
fn a_short_last_partition_is_priced_on_its_own() {
    let partition = partition_keys(BloomConstructionPolicy::BitsPerKey(10.0), 4_096);
    let tail = 3;
    let n = 2 * partition + tail;
    let density = 1.0;
    let full_load = density * as_f64(partition as u64);
    let tail_load = density * as_f64(tail as u64);

    // A price at which the full partitions take a wider width than the tail.
    let (p, full_bits, tail_bits) = (-400..0)
        .map(|step| libm::exp2(f64::from(step) / 8.0))
        .find_map(|p| {
            let full = cheapest(full_load, partition, &WIDTHS, 10, p);
            let short = cheapest(tail_load, tail, &WIDTHS, 10, p);
            (full > short).then_some((p, full, short))
        })
        .unwrap_or_else(|| panic!("the tail narrows before the full partitions"));
    let budget = 2 * estimate(partition, full_bits) + estimate(tail, tail_bits);

    let q = price(
        &at(&[(density * as_f64(n as u64), n)], 10, Some(partition)),
        &WIDTHS,
        budget,
        &bare,
    );
    let full = cheapest(full_load, partition, &WIDTHS, 10, q);
    let short = cheapest(tail_load, tail, &WIDTHS, 10, q);
    assert!(
        full >= full_bits,
        "full partitions {full} bits at {q}, {full_bits} fit at {p}"
    );
    assert!(
        2 * estimate(partition, full) + estimate(tail, short) <= budget,
        "{full} and {short} bits past the budget"
    );
}

/// A restricted table serves only the suffix a tight-space slice left it and
/// keeps only that suffix's probe counts, while its metadata still counts the
/// whole file's keys. Its density is over the keys it serves, so a suffix
/// probed like its neighbour prices like it; and a rewrite of it has only
/// those keys still to write.
#[test]
fn a_restricted_table_is_priced_by_the_keys_it_serves() -> crate::Result<()> {
    use crate::config::{BlockSizePolicy, FilterAdvisor};
    use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};

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
    for prefix in ["a", "b"] {
        for i in 0..KEYS {
            any.insert(format!("{prefix}{:06}", 2 * i), "value", seqno);
            seqno += 1;
        }
        any.flush_active_memtable(0)?;
    }
    for prefix in ["a", "b"] {
        for i in 0..KEYS {
            assert!(
                any.get(format!("{prefix}{:06}", 2 * i + 1), SeqNo::MAX)?
                    .is_none()
            );
        }
    }
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    let version = tree.current_version();
    let tables: Vec<&crate::Table> = version.iter_tables().collect();
    let [a, b] = tables[..] else {
        panic!("two tables");
    };
    let (a, b) = if a.metadata.key_range.min().starts_with(b"a") {
        (a, b)
    } else {
        (b, a)
    };
    // The top tenth of `a` stays live.
    let restricted = a.reopen_restricted(crate::UserKey::from(format!("a{:06}", 2 * 18_000)))?;
    let live = [
        super::Live {
            table: &restricted,
            fallback_bits: 10,
            partition_keys: None,
        },
        super::Live {
            table: b,
            fallback_bits: 10,
            partition_keys: None,
        },
    ];
    let sizing = super::plan(
        &advisor,
        &tree.filter_budget,
        &live,
        super::Rewrite {
            inputs: alloc::vec![restricted.clone(), b.clone()],
            comparator: Some(crate::comparator::default_comparator()),
            ..super::Rewrite::default()
        },
        BloomConstructionPolicy::BitsPerKey(10.0),
        None,
    )
    .unwrap_or_else(|| panic!("the advisor plans the filters"));

    let [suffix, whole] = sizing.input_densities[..] else {
        panic!("one density per input");
    };
    assert!(
        (suffix - whole).abs() <= whole * 0.2,
        "suffix {suffix}, whole table {whole}"
    );
    let served = u64::from(KEYS) / 10 + u64::from(KEYS);
    let pending = sizing
        .pending_keys
        .load(core::sync::atomic::Ordering::Relaxed);
    assert!(
        pending.abs_diff(served) <= served / 10,
        "{pending} keys still to write, {served} served"
    );
    Ok(())
}

/// Rewrites planning together halve a window the live tables crossed once,
/// as one after the other would: the second finds the counts already halved
/// below the window.
#[test]
fn one_crossed_window_is_halved_once_by_concurrent_plans() -> crate::Result<()> {
    use crate::config::FilterAdvisor;
    use crate::{AbstractTree, AnyTree, Config, SequenceNumberCounter};
    use core::sync::atomic::{AtomicBool, Ordering};

    const TABLES: u64 = 32;
    const EACH: u64 = 100;
    let folder = tempfile::tempdir()?;
    let advisor = FilterAdvisor::new(1 << 20).with_window_probes(TABLES * EACH - 1);
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_advisor(Some(advisor.clone()))
    .open()?;
    let mut seqno = 0;
    for table in 0..TABLES {
        for i in 0..10u64 {
            any.insert(format!("{table:03}-{i:03}"), "value", seqno);
            seqno += 1;
        }
        any.flush_active_memtable(0)?;
    }
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    let version = tree.current_version();
    let live = super::live(&version, &tree.config);
    assert_eq!(live.len() as u64, TABLES);
    let stats: Vec<_> = live
        .iter()
        .map(|live| {
            live.table
                .probe_stats()
                .unwrap_or_else(|| panic!("the advisor counts probes"))
        })
        .collect();

    for round in 0..2_000 {
        for stats in &stats {
            let probes = stats.probes();
            stats.add(crate::table::probe_stats::ProbeCounts {
                probes: EACH - probes,
                negatives: 0,
            });
        }
        let go = AtomicBool::new(false);
        std::thread::scope(|scope| {
            for _ in 0..2 {
                scope.spawn(|| {
                    while !go.load(Ordering::Acquire) {
                        core::hint::spin_loop();
                    }
                    super::plan(
                        &advisor,
                        &tree.filter_budget,
                        &live,
                        super::Rewrite::default(),
                        BloomConstructionPolicy::BitsPerKey(10.0),
                        None,
                    )
                });
            }
            go.store(true, Ordering::Release);
        });
        for stats in &stats {
            assert_eq!(stats.probes(), EACH / 2, "round {round}");
        }
    }
    Ok(())
}

/// Wherever a table's key range settles its share of a key range, the share
/// is the one its block index gives: none outside, all inside, at every kind
/// of bound, including bounds on the table's first and last key.
#[test]
fn a_share_settled_by_the_key_range_matches_the_index() -> crate::Result<()> {
    use crate::{AbstractTree, AnyTree, Config, SequenceNumberCounter, UserKey};
    use core::ops::Bound::{Excluded, Included, Unbounded};

    let folder = tempfile::tempdir()?;
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(crate::config::BlockSizePolicy::all(1_024))
    .open()?;
    let mut seqno = 0;
    for prefix in ["b", "d", "f"] {
        for i in 0..2_000u32 {
            any.insert(format!("{prefix}{i:06}"), "value", seqno);
            seqno += 1;
        }
        any.flush_active_memtable(0)?;
    }
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    let order = super::KeyOrder(crate::comparator::default_comparator());
    let keys: Vec<UserKey> = [
        "a", "b000000", "b000999", "b001999", "c", "d000000", "d001999", "e", "f001999", "g",
    ]
    .into_iter()
    .map(UserKey::from)
    .collect();
    fn bounds(key: &UserKey) -> [core::ops::Bound<&[u8]>; 3] {
        [Unbounded, Included(key.as_ref()), Excluded(key.as_ref())]
    }
    let version = tree.current_version();
    let mut settled = 0;
    for table in version.iter_tables() {
        for low_key in &keys {
            for high_key in &keys {
                for low in bounds(low_key) {
                    for high in bounds(high_key) {
                        let Some(share) = order.settled_share(table, (low, high)) else {
                            continue;
                        };
                        settled += 1;
                        let indexed = crate::table::probe_stats::fraction_of(table, (low, high))?;
                        assert_eq!(
                            share.to_bits(),
                            indexed.to_bits(),
                            "{:?} over {low:?}..{high:?}",
                            table.metadata.key_range
                        );
                    }
                }
            }
        }
    }
    assert!(settled > 0, "some shares are settled by the key range");
    Ok(())
}

/// Under a prefix extractor a full filter holds each distinct prefix's hash
/// beside each key's, and the advisor counts the filter by those hashes, not
/// by the keys: here two prefixes of every key make three hashes a key.
#[test]
fn a_filter_is_counted_by_the_hashes_it_holds() -> crate::Result<()> {
    use crate::{AbstractTree, AnyTree, Config, PrefixExtractor, SequenceNumberCounter};
    use alloc::sync::Arc;

    /// The key less its last byte, and less its last two.
    struct TwoPrefixes;
    impl PrefixExtractor for TwoPrefixes {
        fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
            let len = key.len();
            Box::new(
                [len.checked_sub(2), len.checked_sub(1)]
                    .into_iter()
                    .flatten()
                    .filter_map(move |end| key.get(..end)),
            )
        }
    }

    const KEYS: u64 = 5_000;
    let folder = tempfile::tempdir()?;
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(TwoPrefixes))
    .open()?;
    for i in 0..KEYS {
        any.insert(format!("k{i:06}xy"), "value", i);
    }
    any.flush_active_memtable(0)?;
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    let version = tree.current_version();
    let [table] = version.iter_tables().collect::<Vec<_>>()[..] else {
        panic!("one table");
    };
    assert_eq!(table.metadata.key_count, Some(KEYS));
    assert_eq!(super::filter_keys(table), 3 * KEYS);
    Ok(())
}

/// The price models each live table as its own level built it, so it does not
/// depend on where the rewrite writes: a flush into a level of full filters
/// prices the partitioned tables below as partitions, as a compaction into
/// their level does.
#[test]
fn live_tables_price_alike_whatever_the_destination() -> crate::Result<()> {
    use crate::config::{BlockSizePolicy, FilterAdvisor, PinningPolicy};
    use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};

    const KEYS: u32 = 20_000;
    let folder = tempfile::tempdir()?;
    let advisor = FilterAdvisor::new(u64::from(2 * KEYS) * 11 / 8);
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .filter_block_partitioning_policy(PinningPolicy::all(true))
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
    assert!(
        tree.current_version()
            .iter_tables()
            .all(|table| table.regions.filter_tli.is_some()),
        "the live filters are partitioned"
    );
    let version = tree.current_version();
    let live = super::live(&version, &tree.config);
    let price_into = |partition_bytes: Option<u32>| {
        let plan = super::plan(
            &advisor,
            &tree.filter_budget,
            &live,
            super::Rewrite::default(),
            BloomConstructionPolicy::BitsPerKey(10.0),
            partition_bytes,
        )
        .unwrap_or_else(|| panic!("the advisor plans the filters"));
        plan.price
    };
    let (full, partitioned) = (price_into(None), price_into(Some(4_096)));
    assert!(
        partitioned.is_finite() && partitioned > 0.0,
        "price {partitioned}"
    );
    assert_eq!(
        full.to_bits(),
        partitioned.to_bits(),
        "into full filters {full}, into partitioned ones {partitioned}"
    );
    Ok(())
}

/// Filters built side by side price alike in whatever order they are priced:
/// a filter priced while an earlier one is still in flight counts that one's
/// data as still to come, as it would were the earlier one priced alone; and
/// the earlier one priced after it does not take the range back.
#[test]
fn a_filter_priced_past_one_in_flight_counts_its_data() -> crate::Result<()> {
    use crate::config::{BlockSizePolicy, FilterAdvisor};
    use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};
    use core::ops::Bound::{Excluded, Included};

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
    let live = super::live(&version, &tree.config);
    let plan = || {
        super::plan(
            &advisor,
            &tree.filter_budget,
            &live,
            super::Rewrite {
                inputs: inputs.clone(),
                comparator: Some(crate::comparator::default_comparator()),
                ..super::Rewrite::default()
            },
            BloomConstructionPolicy::BitsPerKey(10.0),
            None,
        )
        .unwrap_or_else(|| panic!("the advisor plans the filters"))
    };
    let first = Included(&b"a-hot000000"[..]);
    let second = Excluded(&b"a-hot019998"[..]);

    // One plan at a time: each takes the credit for the inputs it replaces.
    let alone = priced_at(&plan(), first)?;
    let (past_one_in_flight, out_of_order) = {
        let sizing = plan();
        priced_at(&sizing, first)?;
        let past = priced_at(&sizing, second)?;
        // The first priced again after the second: still the same data.
        (past, priced_at(&sizing, first)?)
    };
    for (what, price) in [
        ("past one in flight", past_one_in_flight),
        ("out of order", out_of_order),
    ] {
        assert!(
            (price - alone).abs() <= alone * 1e-9,
            "{what} {price}, alone {alone}"
        );
    }
    Ok(())
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
    let live = super::live(&version, &tree.config);
    let plan = |split: Option<super::Split>| {
        super::plan(
            &advisor,
            &tree.filter_budget,
            &live,
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
    let unsplit = priced_at(&plan(None), Bound::Unbounded)?;
    let split = plan(Some(super::Split {
        boundaries: alloc::vec![crate::UserKey::from("m")],
        comparator: crate::comparator::default_comparator(),
    }));
    let upper_first = priced_at(&split, Bound::Included(b"z-cold000000"))?;
    assert!(
        (upper_first - unsplit).abs() <= unsplit * 1e-9,
        "upper range first {upper_first}, unsplit {unsplit}"
    );
    assert!(
        format!("{split:?}").contains("Split"),
        "a plan names its key ranges: {split:?}"
    );
    Ok(())
}

/// A level whose policy no longer builds filters still holds the tables that
/// have one: each is priced at the width its own filter was built at.
#[test]
fn a_table_on_a_level_without_filters_prices_at_its_own_width() -> crate::Result<()> {
    use crate::config::FilterPolicyEntry;
    use crate::{AbstractTree, AnyTree, Config, SequenceNumberCounter};

    let folder = tempfile::tempdir()?;
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_policy(crate::config::FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(10.0),
    )))
    .open()?;
    for i in 0..20_000u32 {
        any.insert(format!("k{i:06}"), "value", u64::from(i));
    }
    any.flush_active_memtable(0)?;
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    let version = tree.current_version();
    let without = Config::clone(&tree.config)
        .filter_policy(crate::config::FilterPolicy::all(FilterPolicyEntry::None));
    let live = super::live(&version, &without);
    let [table] = live.as_slice() else {
        panic!("one live table");
    };
    assert!(
        (10..=11).contains(&table.fallback_bits),
        "{} bits a key",
        table.fallback_bits
    );
    Ok(())
}

/// A rewrite leaves nothing of an input whose last key is at or below the
/// upper bound of what it writes, and something of one reaching past it.
#[test]
fn a_span_covers_the_inputs_it_writes_to_their_end() -> crate::Result<()> {
    use crate::{AbstractTree, AnyTree, Config, SequenceNumberCounter, UserKey};
    use core::ops::Bound::{self, Excluded, Included, Unbounded};

    let folder = tempfile::tempdir()?;
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..100u32 {
        any.insert(format!("k{i:03}"), "value", u64::from(i));
    }
    any.flush_active_memtable(0)?;
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    let version = tree.current_version();
    let [table] = version.iter_tables().collect::<Vec<_>>()[..] else {
        panic!("one table");
    };
    let span = |upper: Bound<UserKey>| super::Span {
        lower: Unbounded,
        upper,
        comparator: crate::comparator::default_comparator(),
    };
    assert!(span(Unbounded).covers(table));
    assert!(span(Included(UserKey::from("k099"))).covers(table));
    assert!(!span(Included(UserKey::from("k098"))).covers(table));
    assert!(!span(Excluded(UserKey::from("k099"))).covers(table));
    assert!(span(Excluded(UserKey::from("k100"))).covers(table));
    assert!(format!("{:?}", span(Unbounded)).contains("Span"));
    Ok(())
}

/// Lower bounds order by the keys they start from: none before every key,
/// and from a key before past it.
#[test]
fn lower_bounds_order_by_where_they_start() -> crate::Result<()> {
    use crate::{AbstractTree, AnyTree, Config, SequenceNumberCounter, UserKey};
    use core::cmp::Ordering::{Equal, Greater, Less};
    use core::ops::Bound::{self, Excluded, Included, Unbounded};

    let folder = tempfile::tempdir()?;
    let advisor = crate::config::FilterAdvisor::new(u64::MAX);
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_advisor(Some(advisor.clone()))
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    let version = tree.current_version();
    let sizing = super::plan(
        &advisor,
        &tree.filter_budget,
        &super::live(&version, &tree.config),
        super::Rewrite {
            comparator: Some(crate::comparator::default_comparator()),
            ..super::Rewrite::default()
        },
        BloomConstructionPolicy::BitsPerKey(10.0),
        None,
    )
    .unwrap_or_else(|| panic!("the advisor plans the filters"));
    let key = UserKey::from("m");
    let order = |a: &Bound<UserKey>, b: &Bound<UserKey>| sizing.lower_order(a, b);
    assert_eq!(order(&Unbounded, &Unbounded), Equal);
    assert_eq!(order(&Unbounded, &Included(key.clone())), Less);
    assert_eq!(order(&Excluded(key.clone()), &Unbounded), Greater);
    assert_eq!(order(&Included(key.clone()), &Excluded(key.clone())), Less);
    assert_eq!(order(&Excluded(key.clone()), &Included(key)), Greater);
    let debug = format!("{sizing:?}");
    assert!(
        debug.contains("KeyOrder") && debug.contains("Framing"),
        "{debug}"
    );
    Ok(())
}

/// An empty filter is not written, so it takes no bytes framed; any other
/// block takes at least its payload.
#[test]
fn an_empty_filter_is_framed_to_nothing() {
    let framing = super::Framing::default();
    assert_eq!(framing.frame(0), 0);
    assert!(framing.frame(100) >= 100);
}

/// A policy by false-positive rate builds at the narrowest width reaching it.
#[test]
fn a_false_positive_rate_builds_at_the_narrowest_width_reaching_it() {
    use BloomConstructionPolicy::FalsePositiveRate;
    // 2^-7 is the first power of two at or below one in a hundred.
    assert_eq!(super::bits_of(FalsePositiveRate(0.01)), 7);
    assert_eq!(super::bits_of(FalsePositiveRate(0.5)), 1);
}

/// Where every table fits at its widest useful width, the price is set
/// below the last step, where each takes it: a loaded table the widest, one
/// drawing no negative probe the narrowest, though with bytes free the
/// static policy's wider width would have come first.
#[test]
fn a_budget_every_useful_width_fits_takes_them_all() {
    let n = 10_000;
    let mut loads = at(&[(1e6, n)], 16, None);
    loads.extend(at(&[(0.0, n)], 16, None));
    let budget = estimate(n, 16) + estimate(n, 6);
    assert!(estimate(n, 16) * 2 > budget, "the static widths do not fit");
    let p = price(&loads, &WIDTHS, budget, &bare);
    assert!(p.is_finite() && p > 0.0, "price {p}");
    assert_eq!(cheapest(1e6, n, &WIDTHS, 16, p), 16);
    assert_eq!(cheapest(0.0, n, &WIDTHS, 16, p), 6);
}

/// A width whose framed size jumps past the next one's worth is never worth
/// taking: the price passes over it, stepping from the width below straight
/// to the one above, and the choice at that price stays within the budget.
#[test]
fn a_width_its_framing_makes_dear_is_passed_over() {
    let n = 1_000;
    let widths = [6u8, 8, 16];
    let jump = 4 * estimate(n, 6);
    // Blocks past the narrowest width's size carry a large fixed frame.
    let threshold = estimate(n, 6);
    let frame = move |len: u64| if len > threshold { len + jump } else { len };
    let framed: Vec<u64> = super::sizes(n, &widths).into_iter().map(frame).collect();
    let [six, _, sixteen] = framed[..] else {
        panic!("three widths");
    };
    let budget = sixteen - 1;
    let loads = at(&[(1e6, n)], 6, None);
    let p = price(&loads, &widths, budget, &frame);
    assert!(p.is_finite() && p > 0.0, "price {p}");
    let chosen = super::choose(1e6, &framed, &super::rates(&widths), &widths, 6, p);
    assert_eq!(widths.get(chosen), Some(&6), "at {p}");
    assert!(six <= budget);
}

/// Rewrites running together keep room for each other's later filters: a
/// budget all their filters fit at the narrowest width does not let one take
/// a wide filter first and push the other's past it.
#[test]
fn concurrent_plans_reserve_room_for_each_others_later_filters() {
    use alloc::sync::Arc;
    use core::ops::Bound::Unbounded;

    const KEYS: usize = 4_000;
    let framing = super::Framing::default();
    let frame = |len: u64| framing.frame(len);
    let narrow = estimate(KEYS, 6);
    let wide = estimate(KEYS, 16);
    let bound = frame(BloomConstructionPolicy::BitsPerKey(6.0).filter_size_bound(KEYS) as u64);
    let budget = 4 * bound;
    // One plan alone may take a wide filter first: its later one still fits.
    assert!(frame(wide) + bound <= budget);
    let advisor = crate::config::FilterAdvisor::new(budget).with_bits_per_key([6u8, 16].to_vec());
    let state = Arc::new(super::FilterBudget::default());
    let plan = || {
        super::plan(
            &advisor,
            &state,
            &[],
            super::Rewrite {
                keys: 2 * KEYS as u64,
                ..super::Rewrite::default()
            },
            BloomConstructionPolicy::BitsPerKey(16.0),
            None,
        )
        .unwrap_or_else(|| panic!("the advisor plans the filters"))
    };
    let (a, b) = (plan(), plan());

    // A tries its first filter wide, then narrow; each plan then writes the
    // rest narrow, taken whatever the budget says.
    if !a.admit(Unbounded, KEYS, frame(wide), wide, &frame, false) {
        assert!(a.admit(Unbounded, KEYS, frame(narrow), narrow, &frame, true));
    }
    assert!(b.admit(Unbounded, KEYS, frame(narrow), narrow, &frame, true));
    assert!(b.admit(Unbounded, KEYS, frame(narrow), narrow, &frame, true));
    assert!(a.admit(Unbounded, KEYS, frame(narrow), narrow, &frame, true));
    assert!(
        state.held() <= budget,
        "{} filter bytes against {budget}",
        state.held()
    );
}

/// The over-budget state is logged once on entering it and once on leaving.
#[test]
fn the_over_budget_state_is_logged_on_entering_and_leaving() {
    use core::sync::atomic::Ordering::Relaxed;

    let state = super::FilterBudget::default();
    state.observe(10, 5);
    assert!(state.logged.load(Relaxed));
    state.observe(10, 5);
    assert!(state.logged.load(Relaxed));
    state.observe(1, 5);
    assert!(!state.logged.load(Relaxed));
}
