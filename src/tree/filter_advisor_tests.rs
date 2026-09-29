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
    let live = crate::filter_budget::live(&version, &tree.config);
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
    assert!(
        first.admit(1_000, room, room, &frame, false),
        "the room fits one"
    );
    assert!(
        !second.admit(1_000, room, room, &frame, false),
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
    let narrowest = BloomConstructionPolicy::BitsPerKey(6.0).filter_size_bound(KEYS) as u64;
    let budget = narrowest * 5 / 4;
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
        tree.insert(format!("key{i:06}"), value.as_slice(), i as SeqNo);
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
    let negatives = |table: &Table| table.probe_stats().map_or(0, |s| s.negatives());
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
