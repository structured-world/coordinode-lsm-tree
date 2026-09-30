// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::config::{BlockSizePolicy, BloomConstructionPolicy, FilterAdvisor};
use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, Table};

/// Keys `<prefix>000000` .. in steps of two, so the odd ones between them
/// are absent and inside the table's range.
fn key(prefix: &str, i: u32) -> String {
    format!("{prefix}{i:06}")
}

fn open(folder: &std::path::Path, advisor: Option<FilterAdvisor>) -> crate::Result<AnyTree> {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .filter_advisor(advisor)
    .open()
}

/// Flushes `count` even keys under each prefix into one table per prefix.
fn fill(tree: &AnyTree, prefixes: &[&str], count: u32) -> crate::Result<()> {
    let mut seqno = 0;
    for prefix in prefixes {
        for i in 0..count {
            tree.insert(key(prefix, 2 * i), "value", seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    Ok(())
}

/// Compacts every table into new ones. A table spanning the whole key range
/// overlaps all others, so the compaction rewrites them rather than moving
/// them down whole, and every filter is built again.
fn rewrite_all(tree: &AnyTree, first: &str, last: &str) -> crate::Result<()> {
    let seqno = u64::from(u32::MAX);
    tree.insert(key(first, 0), "value", seqno);
    tree.insert(key(last, 0), "value", seqno + 1);
    tree.flush_active_memtable(0)?;
    tree.major_compact(256 * 1_024, 0).map(|_| ())
}

/// Filter bits the tables spend per key, together.
fn group_bits_per_key(tables: &[Table]) -> u64 {
    let bytes: u64 = tables
        .iter()
        .map(|table| u64::from(table.filter_size()))
        .sum();
    let keys: u64 = tables.iter().map(|table| table.metadata.item_count).sum();
    bytes * 8 / keys.max(1)
}

/// Reads `count` absent keys under `prefix`: negative probes on its table.
fn probe_absent(tree: &AnyTree, prefix: &str, count: u32) -> crate::Result<()> {
    for i in 0..count {
        assert!(tree.get(key(prefix, 2 * i + 1), SeqNo::MAX)?.is_none());
    }
    Ok(())
}

fn tables(tree: &AnyTree) -> Vec<Table> {
    tree.current_version().iter_tables().cloned().collect()
}

/// Filter bits a table spends per key.
fn bits_per_key(table: &Table) -> u64 {
    u64::from(table.filter_size()) * 8 / table.metadata.item_count.max(1)
}

/// The tables whose keys all start with `prefix`.
fn under(tables: &[Table], prefix: &str) -> Vec<Table> {
    tables
        .iter()
        .filter(|table| {
            let range = &table.metadata.key_range;
            range.min().starts_with(prefix.as_bytes()) && range.max().starts_with(prefix.as_bytes())
        })
        .cloned()
        .collect()
}

const KEYS: u32 = 4_000;

/// `n` as a `u64`: every count these tests use fits one.
fn u64_of(n: usize) -> u64 {
    u64::try_from(n).unwrap_or_else(|_| panic!("{n} does not fit a u64"))
}

/// The key up to and including its first ':'.
struct UpToColon;

impl crate::PrefixExtractor for UpToColon {
    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(
            key.iter()
                .position(|&byte| byte == b':')
                .and_then(|end| key.get(..=end))
                .into_iter(),
        )
    }
}

/// Appends the operands to the base: a merge operator for the reads that
/// resolve merge operands.
struct Concat;

impl crate::MergeOperator for Concat {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> crate::Result<crate::UserValue> {
        let mut merged = base.map(<[u8]>::to_vec).unwrap_or_default();
        for operand in operands {
            merged.extend_from_slice(operand);
        }
        Ok(merged.into())
    }
}

/// With negative lookups concentrated on one half of the key space, a
/// compaction at a budget that cannot give every filter the widest width
/// spends it on the tables of that half.
#[test]
fn a_compaction_spends_the_budget_where_the_negative_probes_are() -> crate::Result<()> {
    // Enough keys that a filter's fixed overhead does not hide its width.
    const KEYS: u32 = 20_000;
    let folder = tempfile::tempdir()?;
    // About twelve bits a key across both halves: room to widen one half.
    let budget = u64::from(2 * KEYS) * 12 / 8;
    let tree = open(folder.path(), Some(FilterAdvisor::new(budget)))?;
    fill(&tree, &["hot", "old"], KEYS)?;
    for _ in 0..8 {
        probe_absent(&tree, "hot", KEYS)?;
    }
    probe_absent(&tree, "old", KEYS / 100)?;

    rewrite_all(&tree, "hot", "old")?;
    let tables = tables(&tree);
    let (hot, cold) = (under(&tables, "hot"), under(&tables, "old"));
    assert!(
        !hot.is_empty() && !cold.is_empty(),
        "outputs split by prefix"
    );
    let (hot_bits, cold_bits) = (group_bits_per_key(&hot), group_bits_per_key(&cold));
    let shape: Vec<(String, u64, u32)> = tables
        .iter()
        .map(|table| {
            (
                String::from_utf8_lossy(table.metadata.key_range.min()).into_owned(),
                table.metadata.item_count,
                table.filter_size(),
            )
        })
        .collect();
    assert!(
        hot_bits > cold_bits,
        "hot filters {hot_bits} bits a key, cold {cold_bits}: {shape:?}"
    );

    let memory = tree.filter_memory();
    assert!(memory.serialised_bytes <= budget, "{memory:?} {shape:?}");
    assert!(!memory.over_budget, "{memory:?}");
    Ok(())
}

/// With the same load on every key, the budget is shared evenly: neither half
/// of the key space gets a wider filter than the other.
#[test]
fn a_uniform_load_shares_the_budget_evenly() -> crate::Result<()> {
    // Enough keys that a filter's fixed overhead does not hide its width, and
    // a budget every filter fits at a middle width in, overhead included.
    const KEYS: u32 = 20_000;
    let folder = tempfile::tempdir()?;
    let budget = u64::from(2 * KEYS) * 13 / 8;
    let tree = open(folder.path(), Some(FilterAdvisor::new(budget)))?;
    fill(&tree, &["hot", "old"], KEYS)?;
    probe_absent(&tree, "hot", KEYS / 2)?;
    probe_absent(&tree, "old", KEYS / 2)?;

    rewrite_all(&tree, "hot", "old")?;
    let tables = tables(&tree);
    let (hot, cold) = (
        group_bits_per_key(&under(&tables, "hot")),
        group_bits_per_key(&under(&tables, "old")),
    );
    assert!(hot.abs_diff(cold) <= 1, "hot {hot}, cold {cold} bits a key");
    assert!(tree.filter_memory().serialised_bytes <= budget);
    Ok(())
}

/// A filter holds one hash per distinct key however many versions a table
/// keeps, so the load a key draws is counted per distinct key: two halves
/// drawing the same probes per key get the same width though one keeps five
/// versions of every key.
#[test]
fn versions_of_a_key_do_not_dilute_its_load() -> crate::Result<()> {
    const KEYS: u32 = 20_000;
    let folder = tempfile::tempdir()?;
    let budget = u64::from(2 * KEYS) * 13 / 8;
    let tree = open(folder.path(), Some(FilterAdvisor::new(budget)))?;
    let mut seqno = 0;
    for (prefix, versions) in [("hot", 5), ("old", 1)] {
        for _ in 0..versions {
            for i in 0..KEYS {
                tree.insert(key(prefix, 2 * i), "value", seqno);
                seqno += 1;
            }
        }
        tree.flush_active_memtable(0)?;
    }
    probe_absent(&tree, "hot", KEYS / 2)?;
    probe_absent(&tree, "old", KEYS / 2)?;

    rewrite_all(&tree, "hot", "old")?;
    let tables = tables(&tree);
    let per_key = |tables: &[Table]| {
        let bytes: u64 = tables.iter().map(|t| u64::from(t.filter_size())).sum();
        let keys: u64 = tables
            .iter()
            .map(|t| t.metadata.key_count.unwrap_or(t.metadata.item_count))
            .sum();
        bytes * 8 / keys.max(1)
    };
    let (hot, cold) = (
        per_key(&under(&tables, "hot")),
        per_key(&under(&tables, "old")),
    );
    assert!(hot.abs_diff(cold) <= 1, "hot {hot}, cold {cold} bits a key");
    // Room kept for later keys counted by entries would be five times the
    // hot half's keys, and starve both halves to a narrow width.
    assert!(
        hot >= 11 && cold >= 11,
        "hot {hot}, cold {cold} bits a key against a budget of 13"
    );
    assert!(tree.filter_memory().serialised_bytes <= budget);
    Ok(())
}

/// Tables without a filter take no filter bytes, so they do not price the
/// ones that have one: a large filterless last level leaves a flushed
/// table the width its probes pay for.
#[test]
fn filterless_tables_do_not_price_the_others() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    // Room for the flushed tables at the widest width, not for the last
    // level at the narrowest.
    let budget = u64::from(KEYS) * 40 / 8;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .expect_point_read_hits(true)
    .filter_advisor(Some(FilterAdvisor::new(budget)))
    .open()?;
    fill(&tree, &["old"], 5 * KEYS)?;
    tree.major_compact(64 * 1_024 * 1_024, 0)?;
    assert!(tables(&tree).iter().all(|table| table.filter_size() == 0));

    fill(&tree, &["hot"], KEYS)?;
    probe_absent(&tree, "hot", KEYS)?;
    fill(&tree, &["new"], KEYS)?;
    let newest = under(&tables(&tree), "new");
    assert!(!newest.is_empty());
    for table in &newest {
        let keys = usize::try_from(table.metadata.item_count).unwrap_or(usize::MAX);
        let widest = BloomConstructionPolicy::BitsPerKey(16.0).expected_filter_size(keys);
        assert!(
            f64::from(table.filter_size()) >= 0.9 * widest,
            "{} bytes, a 16-bit filter takes about {widest:.0}",
            table.filter_size()
        );
    }
    Ok(())
}

/// A flush whose filters would take the budget past its limit, and whose
/// install then fails, leaves the over-budget state to the filters that are
/// live: they still fit, so the tree is not over its budget.
#[test]
fn a_failed_install_leaves_the_budget_to_the_live_filters() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, StdFs};
    use alloc::sync::Arc;

    let folder = tempfile::tempdir()?;
    let live = {
        let tree = open(folder.path(), None)?;
        fill(&tree, &["hot"], KEYS)?;
        tree.filter_size()
    };
    // Room for the live filters and a little more, not for another table's.
    let budget = live + 64;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .with_shared_fs(Arc::new(fs))
    .filter_advisor(Some(FilterAdvisor::new(budget)))
    .open()?;
    probe_absent(&tree, "hot", KEYS)?;
    for i in 0..KEYS {
        tree.insert(key("new", 2 * i), "value", u64::from(KEYS + i));
    }
    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(crate::io::ErrorKind::Other)).on_path("edits-"),
    );
    assert!(tree.flush_active_memtable(0).is_err(), "the install fails");
    injector.clear();

    let memory = tree.filter_memory();
    assert_eq!(memory.serialised_bytes, live);
    assert!(!memory.over_budget, "{memory:?}");
    // The failed flush gave its reservation back.
    let AnyTree::Standard(standard) = &tree else {
        panic!("a standard tree");
    };
    assert_eq!(standard.filter_budget.held(), live);
    Ok(())
}

/// Once every rewrite has ended, installed or not, the budget holds exactly
/// the live filters' bytes: no rewrite leaves its reservation behind.
#[test]
fn ended_rewrites_leave_the_budget_holding_the_live_filters() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let any = open(folder.path(), Some(FilterAdvisor::new(u64::from(KEYS) * 4)))?;
    fill(&any, &["hot", "old"], KEYS)?;
    probe_absent(&any, "hot", KEYS)?;
    rewrite_all(&any, "hot", "old")?;
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    assert_eq!(tree.filter_budget.held(), any.filter_size());
    Ok(())
}

/// Two rewrites planned over the same live filters, as a flush and a
/// compaction running together, share one reservation of the budget: the
/// room one of them builds into is not handed to the other.
#[test]
fn concurrent_rewrites_share_the_budget() -> crate::Result<()> {
    use crate::table::filter::BloomConstructionPolicy;

    let folder = tempfile::tempdir()?;
    let live = {
        let tree = open(folder.path(), None)?;
        fill(&tree, &["hot"], KEYS)?;
        tree.filter_size()
    };
    let room = 4_096;
    let advisor = FilterAdvisor::new(live + room);
    let any = open(folder.path(), Some(advisor.clone()))?;
    probe_absent(&any, "hot", KEYS)?;
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    let version = tree.current_version();
    let live = crate::filter_budget::live(&version);
    let plan = || {
        crate::filter_budget::plan(
            &advisor,
            &tree.filter_budget,
            &live,
            crate::filter_budget::Rewrite::default(),
            BloomConstructionPolicy::BitsPerKey(10.0),
            None,
        )
        .unwrap_or_else(|| panic!("the advisor plans the filters"))
    };
    let (first, second) = (plan(), plan());
    let frame = |len: u64| len;
    let bounds = (core::ops::Bound::Unbounded, core::ops::Bound::Unbounded);
    assert!(
        first.admit(bounds, 1_000, 1_000, room, room, &frame, false),
        "the room fits one"
    );
    assert!(
        !second.admit(bounds, 1_000, 1_000, room, room, &frame, false),
        "the room the first rewrite builds into is taken"
    );
    Ok(())
}

/// A budget no admissible width fits: the narrowest filters are written, the
/// tree reports the over-budget state, and reads are served as before.
#[test]
fn a_budget_nothing_fits_enters_the_over_budget_state() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), Some(FilterAdvisor::new(1)))?;
    fill(&tree, &["hot"], KEYS)?;
    probe_absent(&tree, "hot", 10)?;
    fill(&tree, &["new"], KEYS)?;

    let memory = tree.filter_memory();
    assert!(memory.over_budget, "{memory:?}");
    assert_eq!(memory.budget_bytes, Some(1));
    let newest = under(&tables(&tree), "new");
    assert!(
        newest.iter().all(|table| bits_per_key(table) < 8),
        "the narrowest width, 6 bits a key"
    );
    for i in 0..KEYS {
        assert!(tree.get(key("new", 2 * i), SeqNo::MAX)?.is_some());
    }
    Ok(())
}

/// A tree that separates large values flushes its tables through its own
/// writer, and the advisor sizes their filters there too: a budget nothing
/// fits writes the narrowest width, not the static policy's.
#[test]
fn a_blob_tree_flush_is_sized_by_the_advisor() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .filter_advisor(Some(FilterAdvisor::new(1)))
    .open()?;
    assert!(matches!(tree, AnyTree::Blob(_)));
    fill(&tree, &["hot"], KEYS)?;
    probe_absent(&tree, "hot", 10)?;
    fill(&tree, &["new"], KEYS)?;

    let newest = under(&tables(&tree), "new");
    assert!(!newest.is_empty());
    for table in &newest {
        assert!(
            bits_per_key(table) < 8,
            "{} bits a key, the narrowest width is 6",
            bits_per_key(table)
        );
    }
    assert!(tree.filter_memory().over_budget);
    Ok(())
}

/// Ingested tables get their filters from the advisor as flushed ones do: a
/// budget nothing fits writes the narrowest width, not the static policy's.
#[test]
fn an_ingestion_is_sized_by_the_advisor() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), Some(FilterAdvisor::new(1)))?;
    fill(&tree, &["hot"], KEYS)?;
    probe_absent(&tree, "hot", 10)?;
    let mut ingestion = tree.ingestion()?;
    for i in 0..KEYS {
        ingestion.write(key("new", 2 * i), "value")?;
    }
    ingestion.finish()?;

    let newest = under(&tables(&tree), "new");
    assert!(!newest.is_empty());
    for table in &newest {
        assert!(
            bits_per_key(table) < 8,
            "{} bits a key, the narrowest width is 6",
            bits_per_key(table)
        );
    }
    Ok(())
}

/// A flush large enough to write several tables keeps room for the later
/// tables' filters at the narrowest width, as a compaction does: the first
/// table does not take the budget at a wide width and push the later ones
/// past it, when every table fits it at the narrowest.
#[test]
fn a_flush_into_several_tables_keeps_room_for_the_later_ones() -> crate::Result<()> {
    // Large incompressible values, so the flush rotates into a second table
    // (at 64 MiB) over few enough keys to keep the test quick.
    const KEYS: usize = 6_000;
    const VALUE: usize = 16 * 1_024;
    let folder = tempfile::tempdir()?;
    // Every filter fits at the narrowest width with a quarter to spare; the
    // static policy's 10 bits for the first table leave too little for the
    // rest.
    let narrowest = BloomConstructionPolicy::BitsPerKey(6.0).filter_size_bound(KEYS);
    let budget = u64_of(narrowest) * 5 / 4;
    let tree = open(
        folder.path(),
        Some(FilterAdvisor::new(budget).with_bits_per_key([6u8, 10].to_vec())),
    )?;
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut value = vec![0u8; VALUE];
    for i in 0..KEYS {
        for byte in &mut value {
            // xorshift: incompressible bytes.
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }
        tree.insert(format!("key{i:06}"), value.as_slice(), u64_of(i));
    }
    tree.flush_active_memtable(0)?;

    let tables = tables(&tree);
    assert!(tables.len() >= 2, "{} tables", tables.len());
    let memory = tree.filter_memory();
    let sizes: Vec<(u64, u32)> = tables
        .iter()
        .map(|table| (table.metadata.item_count, table.filter_size()))
        .collect();
    assert!(!memory.over_budget, "{memory:?} {sizes:?}");
    Ok(())
}

/// A compaction split into parallel sub-compactions plans its filters once
/// over the key ranges they write: its outputs, across the ranges, stay
/// within a budget that holds every filter at the narrowest width, and the
/// budget holds exactly the published filters once it ends.
#[test]
fn a_split_compaction_sizes_its_filters_within_the_budget() -> crate::Result<()> {
    const KEYS: u32 = 4_000;
    let folder = tempfile::tempdir()?;
    let narrowest = BloomConstructionPolicy::BitsPerKey(6.0)
        .filter_size_bound(usize::try_from(KEYS).unwrap_or(usize::MAX));
    let budget = u64_of(narrowest) * 2;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .compaction_threads(4)
    .subcompaction_min_bytes(0)
    .filter_advisor(Some(
        FilterAdvisor::new(budget).with_bits_per_key([6u8, 8, 10].to_vec()),
    ))
    .open()?;
    // A bottom level of several tables, the split's boundaries, and the whole
    // key space again above it.
    fill(&tree, &["k"], KEYS)?;
    tree.major_compact(4_096, 0)?;
    fill(&tree, &["k"], KEYS)?;
    probe_absent(&tree, "k", KEYS / 2)?;
    tree.major_compact(u64::MAX, 0)?;

    let tables = tables(&tree);
    let memory = tree.filter_memory();
    assert!(!memory.over_budget, "{memory:?}");
    assert!(memory.serialised_bytes <= budget, "{memory:?}");
    let AnyTree::Standard(standard) = &tree else {
        panic!("a standard tree");
    };
    assert_eq!(
        standard.filter_budget.held(),
        tables
            .iter()
            .map(|table| u64::from(table.filter_size()))
            .sum::<u64>(),
        "the budget holds the published filters"
    );
    Ok(())
}

/// Tables a caller flushes from its own stream are sized by the advisor, the
/// stream's length bounding their keys, and once registered the budget holds
/// exactly their filters.
#[test]
fn a_caller_flushed_stream_is_sized_and_registered() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), Some(FilterAdvisor::new(u64::MAX)))?;
    // A range's stream tells its exact length.
    let entries = (0..KEYS).map(|i| {
        Ok(crate::InternalValue::from_components(
            key("k", 2 * i),
            "value",
            u64::from(i),
            crate::ValueType::Value,
        ))
    });
    let Some((tables, _, pin)) = tree.flush_to_tables(entries)? else {
        panic!("the stream writes tables");
    };
    assert!(tables.iter().all(|table| table.filter_size() > 0));
    tree.register_tables(&tables, None, None, &[], 0, false)?;

    assert!(tree.get(key("k", 2), SeqNo::MAX)?.is_some());
    let AnyTree::Standard(standard) = &tree else {
        panic!("a standard tree");
    };
    let published: u64 = self::tables(&tree)
        .iter()
        .map(|table| u64::from(table.filter_size()))
        .sum();
    assert!(published > 0);
    // The pin holds the flush's plan; once dropped, the budget holds the
    // published filters alone.
    assert!(
        format!("{pin:?}").contains("filter_budget: true"),
        "{pin:?}"
    );
    drop(pin);
    assert_eq!(standard.filter_budget.held(), published);
    Ok(())
}

/// The advisor sizes the filters a level's static policy builds; it adds none
/// where that policy builds none.
#[test]
fn a_level_without_filters_gets_none_from_the_advisor() -> crate::Result<()> {
    use crate::config::{FilterPolicy, FilterPolicyEntry};

    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::None))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    fill(&tree, &["a", "b"], KEYS)?;
    rewrite_all(&tree, "a", "b")?;
    let tables = tables(&tree);
    assert!(!tables.is_empty());
    for table in &tables {
        assert_eq!(table.filter_size(), 0);
    }
    Ok(())
}

/// Tables holding the same keys are probed by the same lookups: an absent key
/// read once is one negative probe of each. Merged, the output is probed once
/// for it, so it inherits the lookups once, not once per input.
#[test]
fn overlapping_inputs_hand_down_a_lookup_once() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), Some(FilterAdvisor::new(u64::MAX)))?;
    // An older table on a level below and a newer one over the same keys.
    fill(&tree, &["k"], KEYS)?;
    tree.major_compact(64 * 1_024 * 1_024, 0)?;
    fill(&tree, &["k"], KEYS)?;
    let before = tables(&tree);
    assert_eq!(before.len(), 2, "two overlapping tables");
    probe_absent(&tree, "k", KEYS - 1)?;
    let negatives = |tables: &[Table]| -> u64 {
        tables
            .iter()
            .map(|table| {
                table
                    .probe_stats()
                    .map_or(0, crate::table::probe_stats::ProbeStats::negatives)
            })
            .sum()
    };
    // Every lookup reached both tables.
    for table in &before {
        assert_eq!(negatives(core::slice::from_ref(table)), u64::from(KEYS - 1));
    }

    tree.major_compact(64 * 1_024 * 1_024, 0)?;
    let after = negatives(&tables(&tree));
    let lookups = u64::from(KEYS - 1);
    assert!(
        after.abs_diff(lookups) <= lookups / 20,
        "{after} negatives inherited from {lookups} lookups"
    );
    Ok(())
}

/// A flush keeps room for the hashes still to come, not for the versions:
/// five versions of every key make one filter hash each, so a flush into one
/// table keeps no room for later tables and takes the static width the
/// budget fits, where counting versions would reserve room for four times
/// the keys and leave only the narrowest.
#[test]
fn a_flush_reserves_by_filter_hashes_not_versions() -> crate::Result<()> {
    const KEYS: usize = 4_000;
    let folder = tempfile::tempdir()?;
    let wide = BloomConstructionPolicy::BitsPerKey(10.0).filter_size_bound(KEYS);
    let tree = open(
        folder.path(),
        Some(FilterAdvisor::new(u64_of(wide) * 11 / 10).with_bits_per_key([6u8, 10].to_vec())),
    )?;
    let mut seqno = 0;
    for _ in 0..5 {
        for i in 0..KEYS {
            tree.insert(format!("key{i:06}"), "value", seqno);
            seqno += 1;
        }
    }
    tree.flush_active_memtable(0)?;

    let tables = tables(&tree);
    let [table] = tables.as_slice() else {
        panic!("one table");
    };
    let bits = u64::from(table.filter_size()) * 8 / u64_of(KEYS);
    assert!(bits >= 9, "{bits} bits a key: the static 10 fit the budget");
    Ok(())
}

/// Keys rewritten across several sealed memtables make one filter hash each
/// in the flush that merges them: a flush of three memtables over the same
/// keys keeps no room for later tables and takes the width the budget fits.
#[test]
fn a_flush_counts_a_key_in_several_memtables_once() -> crate::Result<()> {
    const KEYS: usize = 4_000;
    let folder = tempfile::tempdir()?;
    let wide = BloomConstructionPolicy::BitsPerKey(10.0).filter_size_bound(KEYS);
    let tree = open(
        folder.path(),
        Some(FilterAdvisor::new(u64_of(wide) * 11 / 10).with_bits_per_key([6u8, 10].to_vec())),
    )?;
    let mut seqno = 0;
    for _ in 0..3 {
        for i in 0..KEYS {
            tree.insert(format!("key{i:06}"), "value", seqno);
            seqno += 1;
        }
        assert!(tree.rotate_memtable().is_some());
    }
    tree.flush_active_memtable(0)?;

    let tables = tables(&tree);
    let [table] = tables.as_slice() else {
        panic!("one table");
    };
    let bits = u64::from(table.filter_size()) * 8 / u64_of(KEYS);
    assert!(bits >= 9, "{bits} bits a key: the static 10 fit the budget");
    Ok(())
}

/// An ingestion told how many entries it writes keeps room for its later
/// tables' filters as a flush does: across several tables, every filter fits
/// the budget that fits them all at the narrowest width.
#[test]
fn an_ingestion_told_its_entries_keeps_room_for_the_later_tables() -> crate::Result<()> {
    // As in the flush above: incompressible values rotate the ingestion into
    // a second table at 64 MiB over few keys.
    const KEYS: usize = 6_000;
    const VALUE: usize = 16 * 1_024;
    let folder = tempfile::tempdir()?;
    let narrowest = BloomConstructionPolicy::BitsPerKey(6.0).filter_size_bound(KEYS);
    let budget = u64_of(narrowest) * 5 / 4;
    let tree = open(
        folder.path(),
        Some(FilterAdvisor::new(budget).with_bits_per_key([6u8, 10].to_vec())),
    )?;
    let mut ingestion = tree.ingestion()?.expected_entries(u64_of(KEYS));
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut value = vec![0u8; VALUE];
    for i in 0..KEYS {
        for byte in &mut value {
            // xorshift: incompressible bytes.
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }
        ingestion.write(format!("key{i:06}"), value.as_slice())?;
    }
    ingestion.finish()?;

    let tables = tables(&tree);
    assert!(tables.len() >= 2, "{} tables", tables.len());
    let memory = tree.filter_memory();
    let sizes: Vec<(u64, u32)> = tables
        .iter()
        .map(|table| (table.metadata.item_count, table.filter_size()))
        .collect();
    assert!(!memory.over_budget, "{memory:?} {sizes:?}");
    Ok(())
}

/// Under a prefix extractor a full filter holds a hash per prefix besides one
/// per key, so an ingestion told its entries keeps room for the later tables'
/// hashes, not their entries: a first table claiming its hashes against the
/// entry count would leave the later ones none.
#[test]
fn an_ingestion_told_its_entries_keeps_room_for_the_later_prefix_hashes() -> crate::Result<()> {
    const KEYS: usize = 6_000;
    const VALUE: usize = 16 * 1_024;
    let folder = tempfile::tempdir()?;
    // Every key has its own prefix: two hashes a key.
    let narrowest = BloomConstructionPolicy::BitsPerKey(6.0).filter_size_bound(2 * KEYS);
    let budget = u64_of(narrowest) * 5 / 4;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .prefix_extractor(std::sync::Arc::new(UpToColon))
    .filter_advisor(Some(
        FilterAdvisor::new(budget).with_bits_per_key([6u8, 10].to_vec()),
    ))
    .open()?;
    let mut ingestion = tree.ingestion()?.expected_entries(u64_of(KEYS));
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut value = vec![0u8; VALUE];
    for i in 0..KEYS {
        for byte in &mut value {
            // xorshift: incompressible bytes.
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }
        ingestion.write(format!("key{i:06}:x"), value.as_slice())?;
    }
    ingestion.finish()?;

    let tables = tables(&tree);
    assert!(tables.len() >= 2, "{} tables", tables.len());
    let memory = tree.filter_memory();
    let sizes: Vec<(u64, u32)> = tables
        .iter()
        .map(|table| (table.metadata.item_count, table.filter_size()))
        .collect();
    assert!(!memory.over_budget, "{memory:?} {sizes:?}");
    Ok(())
}

/// The hashes a flush counts for its filters are the distinct ones, as the
/// writer deduplicates them before building: an extractor whose token is the
/// key itself, or repeats a token out of position, adds none.
#[test]
fn a_flush_counts_its_filter_hashes_as_the_writer_deduplicates_them() {
    /// The whole key, then its first byte twice at two positions.
    struct Repeating;

    impl crate::PrefixExtractor for Repeating {
        fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
            let first = key.get(..1).unwrap_or(key);
            Box::new([key, first, first].into_iter())
        }
    }

    let keys = ["a1", "a2", "b1"].map(crate::UserKey::from);
    let count = crate::memtable::filter_count(keys.into_iter(), Some(&Repeating));
    // Three key hashes (the whole-key tokens repeat them) and the prefixes
    // "a" and "b".
    assert_eq!(count.keys, 3);
    assert_eq!(count.hashes, 5);
}

/// A compaction of overlapping inputs writes each key once, however many of
/// them hold it: the keys its first filter covers correct the estimate that
/// counted the key in each, and a budget one output's filter fits at the
/// static width lets it take that width, not the narrowest.
#[test]
fn overlapping_inputs_keep_no_room_for_keys_written_once() -> crate::Result<()> {
    const KEYS: usize = 4_000;
    let folder = tempfile::tempdir()?;
    let wide = BloomConstructionPolicy::BitsPerKey(10.0).filter_size_bound(KEYS);
    // Full filters on every level: the output is one filter.
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .filter_block_partitioning_policy(crate::config::PinningPolicy::all(false))
    // The four inputs fit at the narrowest width (about 2.4 wide filters),
    // and the output's wide filter with room for three more inputs' keys at
    // the narrowest (about 2.8) does not.
    .filter_advisor(Some(
        FilterAdvisor::new(u64_of(wide) * 26 / 10).with_bits_per_key([6u8, 10].to_vec()),
    ))
    .open()?;
    let mut seqno = 0;
    for _ in 0..4 {
        for i in 0..KEYS {
            tree.insert(
                key("k", 2 * u32::try_from(i).unwrap_or(u32::MAX)),
                "value",
                seqno,
            );
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, 0)?;

    let tables = tables(&tree);
    let [table] = tables.as_slice() else {
        panic!("one output, {} tables", tables.len());
    };
    let bits = u64::from(table.filter_size()) * 8 / u64_of(KEYS);
    assert!(bits >= 9, "{bits} bits a key: the static 10 fit the budget");
    Ok(())
}

/// The first filter of a compaction over overlapping inputs is priced by
/// the keys the merge writes once, not by each input's: a budget its one
/// output fits at the static width, though the inputs' filters together do
/// not fit even at the narrowest, lets it take that width, with probes
/// observed or not.
#[test]
fn the_first_filter_over_overlapping_inputs_prices_keys_written_once() -> crate::Result<()> {
    const KEYS: usize = 4_000;
    for probed in [false, true] {
        let folder = tempfile::tempdir()?;
        let wide = BloomConstructionPolicy::BitsPerKey(10.0).filter_size_bound(KEYS);
        let tree = Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .filter_block_partitioning_policy(crate::config::PinningPolicy::all(false))
        .filter_advisor(Some(
            FilterAdvisor::new(u64_of(wide) * 11 / 10).with_bits_per_key([6u8, 10].to_vec()),
        ))
        .open()?;
        let count = u32::try_from(KEYS).unwrap_or(u32::MAX);
        let mut seqno = 0;
        for _ in 0..4 {
            for i in 0..count {
                tree.insert(key("k", 2 * i), "value", seqno);
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
        }
        if probed {
            probe_absent(&tree, "k", count)?;
        }
        tree.major_compact(u64::MAX, 0)?;

        let tables = tables(&tree);
        let [table] = tables.as_slice() else {
            panic!("one output, {} tables", tables.len());
        };
        let bits = u64::from(table.filter_size()) * 8 / u64_of(KEYS);
        assert!(
            bits >= 9,
            "{bits} bits a key (probed: {probed}): the static 10 fit the budget"
        );
    }
    Ok(())
}

/// Many small inputs merged into one output are priced as the one filter it
/// writes, not as a filter each: a filter's fixed overhead charged once per
/// input would raise the price past what the budget, which fits the output's
/// filter at the static width, calls for.
#[test]
fn many_small_inputs_merged_into_one_output_are_priced_as_one_filter() -> crate::Result<()> {
    const INPUTS: u32 = 30;
    const PER_INPUT: u32 = 100;
    let folder = tempfile::tempdir()?;
    let keys = usize::try_from(INPUTS * PER_INPUT).unwrap_or(usize::MAX);
    let wide = BloomConstructionPolicy::BitsPerKey(10.0).filter_size_bound(keys);
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .filter_block_partitioning_policy(crate::config::PinningPolicy::all(false))
    .filter_advisor(Some(
        FilterAdvisor::new(u64_of(wide) * 21 / 20).with_bits_per_key([6u8, 10].to_vec()),
    ))
    .open()?;
    let mut seqno = 0;
    for input in 0..INPUTS {
        for i in 0..PER_INPUT {
            tree.insert(key(&format!("p{input:02}-"), 2 * i), "value", seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    for input in 0..INPUTS {
        probe_absent(&tree, &format!("p{input:02}-"), PER_INPUT)?;
    }
    tree.major_compact(u64::MAX, 0)?;

    let tables = tables(&tree);
    let [table] = tables.as_slice() else {
        panic!("one output, {} tables", tables.len());
    };
    let bits = u64::from(table.filter_size()) * 8 / u64_of(keys);
    assert!(bits >= 9, "{bits} bits a key: the static 10 fit the budget");
    Ok(())
}

/// A compaction keeps room for its later outputs in the hashes they hold, not
/// in the hashes its inputs held: inputs of partitioned filters hold a hash a
/// key, outputs of full filters under a prefix extractor one more for each
/// key's prefix, and a first output claiming its hashes against the inputs'
/// would leave the later ones none.
#[test]
fn a_compaction_keeps_room_for_the_later_outputs_prefix_hashes() -> crate::Result<()> {
    use crate::config::PinningPolicy;

    const KEYS: usize = 6_000;
    const VALUE: usize = 1_024;
    let folder = tempfile::tempdir()?;
    // Every key has its own prefix: two hashes a key in a full filter.
    let narrowest = BloomConstructionPolicy::BitsPerKey(6.0).filter_size_bound(2 * KEYS);
    let budget = u64_of(narrowest) * 5 / 4;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .filter_block_partitioning_policy(PinningPolicy::new([true, false]))
    .prefix_extractor(std::sync::Arc::new(UpToColon))
    .filter_advisor(Some(
        FilterAdvisor::new(budget).with_bits_per_key([6u8, 10].to_vec()),
    ))
    .open()?;
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut value = vec![0u8; VALUE];
    for i in 0..KEYS {
        for byte in &mut value {
            // xorshift: incompressible bytes.
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }
        tree.insert(format!("key{i:06}:x"), value.as_slice(), u64_of(i));
    }
    tree.flush_active_memtable(0)?;
    assert!(
        tables(&tree)
            .iter()
            .all(|table| table.regions.filter_tli.is_some()),
        "the inputs' filters are partitioned"
    );
    tree.major_compact(1_024 * 1_024, 0)?;

    let tables = tables(&tree);
    assert!(tables.len() >= 3, "{} tables", tables.len());
    assert!(
        tables
            .iter()
            .all(|table| table.regions.filter_tli.is_none()),
        "the outputs' filters are full"
    );
    let memory = tree.filter_memory();
    let sizes: Vec<(u64, u32)> = tables
        .iter()
        .map(|table| (table.metadata.item_count, table.filter_size()))
        .collect();
    assert!(!memory.over_budget, "{memory:?} {sizes:?}");
    Ok(())
}

/// A blob tree's ingestion takes the entry count as a standard one does, for
/// the filters of the index tables it writes.
#[test]
fn a_blob_ingestion_takes_its_entry_count() -> crate::Result<()> {
    use crate::KvSeparationOptions;

    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    assert!(matches!(tree, AnyTree::Blob(_)));
    let mut ingestion = tree.ingestion()?.expected_entries(u64::from(KEYS));
    for i in 0..KEYS {
        ingestion.write(key("k", 2 * i), "value")?;
    }
    ingestion.finish()?;

    assert_eq!(
        tree.get(key("k", 2), SeqNo::MAX)?.as_deref(),
        Some(&b"value"[..])
    );
    let tables = tables(&tree);
    assert!(!tables.is_empty());
    assert!(tables.iter().all(|table| table.filter_size() > 0));
    Ok(())
}

/// A key past a partitioned filter's last partition, yet inside the table's
/// key range (a range tombstone reaches past its last key), is ruled out by
/// the partition index alone: no filter answers for it, so it is no probe,
/// and no filter width would change it.
#[test]
fn a_key_past_the_last_filter_partition_is_no_probe() -> crate::Result<()> {
    use crate::config::PinningPolicy;

    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .filter_block_partitioning_policy(PinningPolicy::all(true))
    .with_merge_operator(Some(alloc::sync::Arc::new(Concat)))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    for i in 0..KEYS {
        tree.insert(key("a", 2 * i), "value", u64::from(i));
    }
    tree.remove_range("b", "c", u64::from(KEYS));
    tree.flush_active_memtable(0)?;
    let tables = tables(&tree);
    let [table] = tables.as_slice() else {
        panic!("one table");
    };
    assert!(
        table.metadata.key_range.max().as_ref() >= b"b".as_slice(),
        "the range tombstone widens the key range"
    );
    let counts = |table: &Table| {
        table
            .probe_stats()
            .map_or((0, 0), |stats| (stats.probes(), stats.negatives()))
    };
    let before = counts(table);

    for i in 0..100u32 {
        assert!(tree.get(key("b", i), SeqNo::MAX)?.is_none());
    }
    assert_eq!(counts(table), before, "point reads (probes, negatives)");

    // Merge-operand resolution asks the table's filters the same way.
    for i in 0..100u32 {
        tree.merge(key("b", i), "x", u64::from(KEYS + 1 + i));
    }
    for i in 0..100u32 {
        assert_eq!(
            tree.get(key("b", i), SeqNo::MAX)?.as_deref(),
            Some(&b"x"[..])
        );
    }
    assert_eq!(counts(table), before, "merge reads (probes, negatives)");
    Ok(())
}

/// A read whose newest version is a merge operand resolves it over every
/// older table, asking each one's filter again; those answers count as the
/// point read's do. The older table here holds none of the keys read, so
/// every read is one negative probe of it: its one-bit filter answers absent
/// about half the time, and otherwise lets the key through to a read that
/// finds no version, a false positive.
#[test]
fn merge_resolution_counts_its_filter_probes() -> crate::Result<()> {
    use crate::config::{FilterPolicy, FilterPolicyEntry};
    use alloc::sync::Arc;

    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .with_merge_operator(Some(Arc::new(Concat)))
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(1.0),
    )))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    // The older table, on a level below, holds the even keys; the newer one an
    // operand for each odd key between them. The read finds the operand
    // before it reaches the older level.
    fill(&tree, &["k"], KEYS)?;
    tree.major_compact(64 * 1_024 * 1_024, 0)?;
    let older = tables(&tree);
    for i in 0..KEYS {
        tree.merge(key("k", 2 * i + 1), "x", u64::from(KEYS + i));
    }
    tree.flush_active_memtable(0)?;
    let negatives = |table: &Table| {
        table
            .probe_stats()
            .map_or(0, crate::table::probe_stats::ProbeStats::negatives)
    };
    let [older] = older.as_slice() else {
        panic!("one older table");
    };
    let before = negatives(older);

    // The odd keys below the older table's last one, which its key range
    // does not rule out before its filter is asked.
    let reads = KEYS - 1;
    for i in 0..reads {
        assert_eq!(
            tree.get(key("k", 2 * i + 1), SeqNo::MAX)?.as_deref(),
            Some(&b"x"[..])
        );
    }
    assert_eq!(negatives(older) - before, u64::from(reads));
    Ok(())
}

/// A filter that cannot be read answers nothing: merge resolution reads the
/// table anyway, and a read finding no version then is no negative probe of a
/// filter, so it counts neither a probe nor a miss.
#[test]
fn an_unreadable_filter_counts_no_miss() -> crate::Result<()> {
    use crate::config::{FilterPolicy, FilterPolicyEntry};
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, StdFs};
    use alloc::sync::Arc;

    let folder = tempfile::tempdir()?;
    let fs = FaultFs::new(StdFs);
    let injector = fs.injector();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fs)
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .with_merge_operator(Some(Arc::new(Concat)))
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(10.0),
    )))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    fill(&tree, &["k"], KEYS)?;
    tree.major_compact(64 * 1_024 * 1_024, 0)?;
    let older = tables(&tree);
    let [older] = older.as_slice() else {
        panic!("one older table");
    };
    for i in 0..KEYS {
        tree.merge(key("k", 2 * i + 1), "x", u64::from(KEYS + i));
    }
    tree.flush_active_memtable(0)?;
    let filter = older
        .regions
        .filter
        .unwrap_or_else(|| panic!("the older table has an unpinned full filter"));
    injector.arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(crate::io::ErrorKind::Other))
            .on_path(older.path.to_string_lossy().into_owned())
            .at_offset(*filter.offset()),
    );
    let counts = |table: &Table| {
        table
            .probe_stats()
            .map_or((0, 0), |stats| (stats.probes(), stats.negatives()))
    };
    let before = counts(older);

    let reads = KEYS - 1;
    for i in 0..reads {
        assert_eq!(
            tree.get(key("k", 2 * i + 1), SeqNo::MAX)?.as_deref(),
            Some(&b"x"[..])
        );
    }
    let after = counts(older);
    let (probes, negatives) = (after.0 - before.0, after.1 - before.1);
    assert!(probes < u64::from(reads), "the filter read failed");
    // Every key read is absent from the table, so each probe the filter
    // answered is a negative one, and no negative comes without its probe.
    assert_eq!(negatives, probes, "negatives against probes");
    Ok(())
}

/// Widths all wider than the static policy's: once probes are observed, a
/// budget none of them fits writes the narrowest configured width, never the
/// static policy's narrower one.
#[test]
fn a_budget_nothing_fits_writes_the_narrowest_configured_width() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let advisor = FilterAdvisor::new(1).with_bits_per_key([12u8, 14].to_vec());
    let tree = open(folder.path(), Some(advisor))?;
    fill(&tree, &["hot"], KEYS)?;
    probe_absent(&tree, "hot", 10)?;
    fill(&tree, &["new"], KEYS)?;

    let newest = under(&tables(&tree), "new");
    assert!(!newest.is_empty());
    for table in &newest {
        let keys = usize::try_from(table.metadata.item_count).unwrap_or(usize::MAX);
        // A 12-bit build lands within a few percent of this estimate; the
        // static policy's 10-bit one near four fifths of it.
        let narrowest = BloomConstructionPolicy::BitsPerKey(12.0).expected_filter_size(keys);
        assert!(
            f64::from(table.filter_size()) >= 0.9 * narrowest,
            "{} bytes, a 12-bit filter takes about {narrowest:.0}",
            table.filter_size()
        );
    }
    Ok(())
}

/// Lowering the budget below what the existing filters take leaves them as
/// they are, so the tree reports the over-budget state until rewrites bring
/// the filters back under it, and keeps serving reads meanwhile.
#[test]
fn a_lowered_budget_enters_the_over_budget_state_until_rewrites() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    {
        let tree = open(folder.path(), None)?;
        fill(&tree, &["hot", "old"], KEYS)?;
    }
    let existing = {
        let tree = open(folder.path(), None)?;
        tree.filter_size()
    };
    // Room for the narrowest width, not for the static policy's.
    let budget = existing * 3 / 4;
    let tree = open(folder.path(), Some(FilterAdvisor::new(budget)))?;
    probe_absent(&tree, "hot", KEYS)?;
    tree.insert(key("new", 0), "value", u64::from(3 * KEYS));
    tree.flush_active_memtable(0)?;
    assert!(tree.filter_memory().over_budget);
    assert!(tree.get(key("old", 0), SeqNo::MAX)?.is_some());

    // A compaction rewrites every filter within the budget.
    rewrite_all(&tree, "hot", "old")?;
    let memory = tree.filter_memory();
    assert!(memory.serialised_bytes <= budget, "{memory:?}");
    assert!(tree.get(key("old", 0), SeqNo::MAX)?.is_some());
    drop(tree);

    // The next rewrite that finds the filters within the budget, with room
    // for its own, leaves the state.
    let tree = open(folder.path(), Some(FilterAdvisor::new(2 * budget)))?;
    tree.insert(key("new", 2), "value", u64::from(3 * KEYS) + 2);
    tree.flush_active_memtable(0)?;
    let memory = tree.filter_memory();
    assert!(!memory.over_budget, "{memory:?}");
    Ok(())
}

/// Before any probe is observed the static policy decides: the advisor
/// writes the same filters as a tree without it.
#[test]
fn before_any_probe_the_filters_match_the_static_policy() -> crate::Result<()> {
    let sizes = |advisor: Option<FilterAdvisor>| -> crate::Result<Vec<u32>> {
        let folder = tempfile::tempdir()?;
        let tree = open(folder.path(), advisor)?;
        fill(&tree, &["hot", "old"], KEYS)?;
        tree.major_compact(64 * 1_024, 0)?;
        let mut sizes: Vec<u32> = tables(&tree).iter().map(Table::filter_size).collect();
        sizes.sort_unstable();
        Ok(sizes)
    };
    assert_eq!(sizes(Some(FilterAdvisor::new(u64::MAX)))?, sizes(None)?);
    Ok(())
}

/// A prefix scan asks a table's full filter for the prefix, and those answers
/// count like a point read's: a scan of a prefix the table holds no key under
/// is a negative probe of its filter.
#[test]
fn prefix_scans_count_their_filter_probes() -> crate::Result<()> {
    use alloc::sync::Arc;

    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .prefix_extractor(Arc::new(UpToColon))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    // Even prefixes hold keys; the odd ones between them are absent and
    // inside the table's range.
    const PREFIXES: u32 = 400;
    let mut seqno = 0;
    for p in 0..PREFIXES {
        for i in 0..5 {
            tree.insert(format!("p{:04}:{i}", 2 * p), "value", seqno);
            seqno += 1;
        }
    }
    tree.flush_active_memtable(0)?;
    let [table] = &tables(&tree)[..] else {
        panic!("one table");
    };
    let negatives = || {
        table
            .probe_stats()
            .map_or(0, crate::table::probe_stats::ProbeStats::negatives)
    };
    let before = negatives();
    let scans = PREFIXES - 1;
    for p in 0..scans {
        assert_eq!(
            tree.prefix(format!("p{:04}:", 2 * p + 1), SeqNo::MAX, None)
                .count(),
            0
        );
    }
    // Nearly every scan finds the prefix absent in the filter.
    let counted = negatives() - before;
    assert!(
        counted >= u64::from(scans) * 9 / 10,
        "{counted} negative probes of {scans} prefix scans"
    );
    Ok(())
}

/// A prefix the filter lets through, whose scan then finds no key under it,
/// is a false positive and counts as a negative probe, as a point read's is:
/// with a one-bit filter about half the absent prefixes pass, and every scan
/// of an absent prefix counts one probe and one negative.
#[test]
fn a_prefix_scan_finding_nothing_after_its_filter_passed_counts_a_negative() -> crate::Result<()> {
    use crate::config::{FilterPolicy, FilterPolicyEntry};
    use alloc::sync::Arc;

    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .prefix_extractor(Arc::new(UpToColon))
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(1.0),
    )))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    const PREFIXES: u32 = 400;
    let mut seqno = 0;
    for p in 0..PREFIXES {
        for i in 0..5 {
            tree.insert(format!("p{:04}:{i}", 2 * p), "value", seqno);
            seqno += 1;
        }
    }
    tree.flush_active_memtable(0)?;
    let tables = tables(&tree);
    let [table] = tables.as_slice() else {
        panic!("one table");
    };
    let counts = || {
        table
            .probe_stats()
            .map_or((0, 0), |stats| (stats.probes(), stats.negatives()))
    };
    let before = counts();
    let scans = PREFIXES - 1;
    for p in 0..scans {
        assert_eq!(
            tree.prefix(format!("p{:04}:", 2 * p + 1), SeqNo::MAX, None)
                .count(),
            0
        );
    }
    let after = counts();
    assert_eq!(
        (after.0 - before.0, after.1 - before.1),
        (u64::from(scans), u64::from(scans)),
        "(probes, negatives) of {scans} scans of absent prefixes"
    );

    // Scanned backwards, the same.
    for p in 0..scans {
        assert_eq!(
            tree.prefix(format!("p{:04}:", 2 * p + 1), SeqNo::MAX, None)
                .rev()
                .count(),
            0
        );
    }
    let reversed = counts();
    assert_eq!(
        (reversed.0 - after.0, reversed.1 - after.1),
        (u64::from(scans), u64::from(scans)),
        "(probes, negatives) of {scans} reversed scans"
    );
    Ok(())
}

/// A prefix whose keys fill several tables of one level is read across all
/// of them: each holds keys under it, so its filter's pass is no false
/// positive, and the scan counts no negative.
#[test]
fn a_prefix_spanning_several_tables_counts_no_negative() -> crate::Result<()> {
    use alloc::sync::Arc;

    const PREFIXED: u32 = 4_000;
    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    // Full filters hold the prefixes; partitioned ones answer no prefix.
    .filter_block_partitioning_policy(crate::config::PinningPolicy::all(false))
    .prefix_extractor(Arc::new(UpToColon))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    for i in 0..PREFIXED {
        tree.insert(format!("x:{i:06}"), "value", u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(4_096, 0)?;
    let tables = tables(&tree);
    assert!(tables.len() > 2, "{} tables", tables.len());
    let counts = |tables: &[Table]| -> (u64, u64) {
        tables
            .iter()
            .filter_map(Table::probe_stats)
            .fold((0, 0), |(probes, negatives), stats| {
                (probes + stats.probes(), negatives + stats.negatives())
            })
    };
    let before = counts(&tables);
    assert_eq!(
        tree.prefix("x:", SeqNo::MAX, None).count(),
        PREFIXED as usize
    );
    let after = counts(&tables);
    // Each table's filter answered, and each holds keys under the prefix.
    assert_eq!(after.0 - before.0, tables.len() as u64, "probes");
    assert_eq!(after.1, before.1, "negatives");
    Ok(())
}

/// A table of a level whose key range reaches into a prefix only by a range
/// tombstone's end holds no key under it; when its filter lets the prefix
/// through and a scan reads it among other tables of its level, the scan
/// finding nothing in it counts the false positive.
#[test]
fn a_prefix_reaching_a_table_by_a_range_tombstone_counts_its_miss() -> crate::Result<()> {
    use alloc::sync::Arc;

    /// `UpToColon`, and for a key under `w` the token `x:`, so that table's
    /// filter lets the prefix `x:` through without holding a key under it.
    struct WithDecoy;
    impl crate::PrefixExtractor for WithDecoy {
        fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
            if key.starts_with(b"w") {
                return Box::new(core::iter::once(&b"x:"[..]));
            }
            UpToColon.prefixes(key)
        }
    }

    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    // Uncompressed, so the tables the compaction cuts are the same whatever
    // codecs are built in.
    .data_block_compression_policy(crate::config::CompressionPolicy::disabled())
    // Full filters hold the prefixes; partitioned ones answer no prefix.
    .filter_block_partitioning_policy(crate::config::PinningPolicy::all(false))
    .prefix_extractor(Arc::new(WithDecoy))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    let mut seqno = 0;
    for i in 0..2_000u32 {
        tree.insert(format!("w{i:06}"), "value", seqno);
        seqno += 1;
    }
    // An older key the tombstone below deletes, so it has something to
    // cover; its large incompressible value ends its table right after it,
    // so the compaction's next table starts at the first key under x:.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let value: Vec<u8> = (0..8_192)
        .map(|_| {
            // xorshift: incompressible bytes.
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect();
    tree.insert("w999999z", value, seqno);
    seqno += 1;
    tree.flush_active_memtable(0)?;
    // Past the last w key and short of the first live x key: the compaction
    // widens the w table's range into x: to cover it.
    tree.remove_range("w999999", "x:000300", seqno);
    seqno += 1;
    tree.flush_active_memtable(0)?;
    for i in 500..2_500u32 {
        tree.insert(format!("x:{i:06}"), "value", seqno);
        seqno += 1;
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(4_096, 0)?;

    let tables = tables(&tree);
    let shape: Vec<(String, String)> = tables
        .iter()
        .map(|table| {
            let range = &table.metadata.key_range;
            (
                String::from_utf8_lossy(range.min()).into_owned(),
                String::from_utf8_lossy(range.max()).into_owned(),
            )
        })
        .collect();
    let decoy = tables
        .iter()
        .find(|table| {
            let range = &table.metadata.key_range;
            range.min().starts_with(b"w") && range.max().starts_with(b"x:")
        })
        .unwrap_or_else(|| panic!("a table reaching into x: by the tombstone: {shape:?}"));
    let counts = |table: &Table| {
        table
            .probe_stats()
            .map_or((0, 0), |stats| (stats.probes(), stats.negatives()))
    };
    let before = counts(decoy);
    // The keys under x: are newer than the tombstone: all are read.
    assert_eq!(tree.prefix("x:", SeqNo::MAX, None).count(), 2_000);
    let after = counts(decoy);
    assert_eq!(
        (after.0 - before.0, after.1 - before.1),
        (1, 1),
        "(probes, negatives) of the scan in {shape:?}"
    );
    Ok(())
}

/// A table a newer range tombstone wholly covers holds nothing a scan can
/// see, so the scan does not read it and does not ask its filter: no probe
/// is counted, whatever the filter would answer.
#[test]
fn a_table_a_tombstone_covers_is_not_probed() -> crate::Result<()> {
    use alloc::sync::Arc;

    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(UpToColon))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    for i in 0..100u32 {
        tree.insert(format!("p:{i:04}"), "value", u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    let covered = tables(&tree);
    let [covered] = covered.as_slice() else {
        panic!("one table");
    };
    // A newer table holding a tombstone over every key of the first.
    tree.insert("z", "value", 100);
    tree.remove_range("p:", "p;", 101);
    tree.flush_active_memtable(0)?;

    let counts = |table: &Table| {
        table
            .probe_stats()
            .map_or((0, 0), |stats| (stats.probes(), stats.negatives()))
    };
    let before = counts(covered);
    assert_eq!(tree.prefix("p:", SeqNo::MAX, None).count(), 0);
    assert_eq!(counts(covered), before, "(probes, negatives)");
    Ok(())
}

/// A table without a filter answers no key check: a merge read over it counts
/// no probe of it, and no miss either.
#[test]
fn a_merge_read_over_a_filterless_table_counts_nothing() -> crate::Result<()> {
    use crate::config::{FilterPolicy, FilterPolicyEntry};

    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(alloc::sync::Arc::new(Concat)))
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::None))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    fill(&tree, &["k"], KEYS)?;
    let older = tables(&tree);
    for i in 0..100 {
        tree.merge(key("k", 2 * i + 1), "x", u64::from(KEYS + i));
    }
    for i in 0..100 {
        assert_eq!(
            tree.get(key("k", 2 * i + 1), SeqNo::MAX)?.as_deref(),
            Some(&b"x"[..])
        );
    }
    for table in &older {
        let counts = table
            .probe_stats()
            .map_or((0, 0), |stats| (stats.probes(), stats.negatives()));
        assert_eq!(counts, (0, 0));
    }
    Ok(())
}

/// A pinned full filter is resident whole, and counts at the on-disk size the
/// serialised figure counts it at, framing included, like a cached one.
#[test]
fn a_pinned_filter_is_resident_at_its_on_disk_size() -> crate::Result<()> {
    use crate::config::PinningPolicy;

    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .filter_block_pinning_policy(PinningPolicy::all(true))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    fill(&tree, &["hot"], KEYS)?;
    let memory = tree.filter_memory();
    assert!(memory.serialised_bytes > 0);
    assert_eq!(memory.resident_bytes, memory.serialised_bytes, "{memory:?}");
    Ok(())
}

/// The resident figure counts the filter blocks a read brought into the
/// block cache, apart from the serialised one: with filters left unpinned,
/// none is resident until a probe loads one.
#[test]
fn resident_filter_bytes_follow_the_block_cache() -> crate::Result<()> {
    use crate::config::PinningPolicy;

    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    .filter_block_pinning_policy(PinningPolicy::all(false))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    fill(&tree, &["hot"], KEYS)?;
    let before = tree.filter_memory();
    assert!(before.serialised_bytes > 0);
    assert_eq!(before.resident_bytes, 0, "{before:?}");
    probe_absent(&tree, "hot", 1)?;
    let after = tree.filter_memory();
    assert_eq!(after.serialised_bytes, before.serialised_bytes);
    assert!(after.resident_bytes > 0, "{before:?} -> {after:?}");
    Ok(())
}
