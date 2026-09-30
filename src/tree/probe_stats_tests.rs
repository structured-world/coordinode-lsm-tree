// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::config::{BloomConstructionPolicy, FilterAdvisor, FilterPolicy, FilterPolicyEntry};
use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, Table};

const KEYS: u32 = 1_000;

fn key(i: u32) -> String {
    format!("key{i:06}")
}

/// A tree whose one table holds the even keys up to `KEYS` inclusive, its
/// filter built at `bits_per_key`, counting probes when `counted`. Every key
/// below `KEYS` lies inside the table's key range, so a read of it probes the
/// filter rather than being pruned by the range.
fn tree_of_even_keys(
    folder: &std::path::Path,
    bits_per_key: f32,
    counted: bool,
) -> crate::Result<AnyTree> {
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(bits_per_key),
    )))
    .filter_advisor(counted.then(|| FilterAdvisor::new(u64::MAX)))
    .open()?;
    for i in (0..=KEYS).step_by(2) {
        tree.insert(key(i), "value", u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    Ok(tree)
}

fn only_table(tree: &AnyTree) -> Table {
    let version = tree.current_version();
    let mut tables = version.iter_tables();
    let Some(table) = tables.next().cloned() else {
        panic!("the flush wrote a table");
    };
    assert!(tables.next().is_none(), "one table");
    table
}

/// `(probes, negative probes)` of `table`.
fn counts(table: &Table) -> (u64, u64) {
    let Some(stats) = table.probe_stats() else {
        panic!("the table counts its probes");
    };
    (stats.probes(), stats.negatives())
}

/// Every probe of the filter counts, and a probe for a key the table does
/// not hold is a negative one; a key the table holds is not.
#[test]
fn absent_keys_count_as_negative_probes_and_present_ones_do_not() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_of_even_keys(folder.path(), 10.0, true)?;
    for i in 0..KEYS {
        let found = tree.get(key(i), SeqNo::MAX)?;
        assert_eq!(found.is_some(), i % 2 == 0, "key {i}");
    }
    assert_eq!(
        counts(&only_table(&tree)),
        (u64::from(KEYS), u64::from(KEYS / 2))
    );
    Ok(())
}

/// A key outside the table's key range never reaches the filter: the range
/// prunes the table, and no probe is counted.
#[test]
fn keys_outside_the_table_range_are_not_probes() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_of_even_keys(folder.path(), 10.0, true)?;
    assert!(tree.get("a", SeqNo::MAX)?.is_none());
    assert!(tree.get(key(KEYS + 1), SeqNo::MAX)?.is_none());
    assert_eq!(counts(&only_table(&tree)), (0, 0));
    Ok(())
}

/// A filter that says present for an absent key sends the probe on to a
/// data block read that finds nothing: that probe is a negative one too.
#[test]
fn false_positives_count_as_negative_probes() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    // One bit per key lets most absent keys through.
    let tree = tree_of_even_keys(folder.path(), 1.0, true)?;
    #[cfg(feature = "metrics")]
    let skipped = tree.metrics().io_skipped_by_filter();
    for i in (1..KEYS).step_by(2) {
        assert!(tree.get(key(i), SeqNo::MAX)?.is_none());
    }
    #[cfg(feature = "metrics")]
    {
        let rejected = tree.metrics().io_skipped_by_filter() - skipped;
        assert!(
            rejected < usize::try_from(KEYS / 4).unwrap_or(usize::MAX),
            "the filter must let most absent keys through, rejected {rejected}",
        );
    }
    assert_eq!(
        counts(&only_table(&tree)),
        (u64::from(KEYS / 2), u64::from(KEYS / 2))
    );
    Ok(())
}

/// A key held only in versions the snapshot cannot see, and a key whose
/// newest version is a tombstone, are held by the table: neither is a
/// negative probe. A key the table does not hold still is, at the same
/// snapshot.
#[test]
fn invisible_and_deleted_keys_are_not_negative_probes() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    tree.insert("early", "value", 1);
    tree.insert("late", "value", 20);
    tree.insert("deleted", "value", 2);
    tree.remove("deleted", 3);
    tree.flush_active_memtable(0)?;
    let table = only_table(&tree);

    // Seqno 10 sees "early" and the tombstone, not "late".
    assert!(tree.get("late", 10)?.is_none());
    assert!(tree.get("deleted", 10)?.is_none());
    assert_eq!(counts(&table), (2, 0));

    // Keys the table does not hold, inside its key range ("deleted" to
    // "late"), so the read reaches the filter.
    let mut absent = 0;
    for i in 0..100u32 {
        let key = format!("e-absent{i}");
        let (_, before) = counts(&table);
        assert!(tree.get(&key, SeqNo::MAX)?.is_none());
        absent += counts(&table).1 - before;
    }
    assert_eq!(absent, 100, "every absent key is a negative probe");
    Ok(())
}

/// A multi-get probes each table once per key, even where the level is
/// prewarmed first: the prewarm's probes ahead of the read do not count.
#[test]
fn multi_get_counts_each_key_once() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_of_even_keys(folder.path(), 10.0, true)?;
    let keys: Vec<String> = (0..KEYS).map(key).collect();
    let found = tree.multi_get(&keys, SeqNo::MAX)?;
    assert_eq!(
        found.iter().filter(|found| found.is_some()).count(),
        usize::try_from(KEYS / 2).unwrap_or(usize::MAX)
    );
    assert_eq!(
        counts(&only_table(&tree)),
        (u64::from(KEYS), u64::from(KEYS / 2))
    );
    Ok(())
}

/// A level whose filter rules out every key has no block to read: the staged
/// resolve answers it with nothing found and counts each key once, as a probe
/// and a negative one.
#[test]
fn a_level_every_key_is_filtered_out_of_is_answered_and_counted_once() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    // Wide enough that no absent key passes the filter.
    let any = tree_of_even_keys(folder.path(), 24.0, true)?;
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    let keys: Vec<String> = (1..KEYS).step_by(2).map(key).collect();
    let mut remaining: Vec<(usize, u64)> = keys
        .iter()
        .enumerate()
        .map(|(index, key)| (index, crate::hash::hash64(key.as_bytes())))
        .collect();
    let mut results: Vec<Option<crate::value::InternalValue>> = alloc::vec![None; keys.len()];
    let version = tree.current_version();
    let Some(level) = version.level(0) else {
        panic!("level 0 exists");
    };
    let comparator = crate::comparator::default_comparator();
    let resolved = crate::Tree::resolve_level_staged(
        level,
        &mut remaining,
        &keys,
        SeqNo::MAX,
        comparator.as_ref(),
        &mut results,
    )?;
    assert!(resolved, "the level is answered: nothing in it");
    assert!(results.iter().all(Option::is_none));
    let count = keys.len() as u64;
    assert_eq!(
        counts(&only_table(&any)),
        (count, count),
        "(probes, negatives)"
    );
    Ok(())
}

/// Without a filter advisor nothing is counted: the probe path pays no
/// counting.
#[test]
fn nothing_is_counted_without_an_advisor() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_of_even_keys(folder.path(), 10.0, false)?;
    for i in 0..KEYS {
        tree.get(key(i), SeqNo::MAX)?;
    }
    assert!(only_table(&tree).probe_stats().is_none());
    Ok(())
}

/// A level resolved in chunks counts a key its filter let through as a miss
/// even when the table has no block to read it in: its key range reaches past
/// its last block (a range tombstone ends there), and another table of the
/// level supplies the blocks the chunks read.
#[test]
fn a_chunked_resolve_counts_a_passed_key_with_no_block() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(1.0),
    )))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    for i in 0..KEYS {
        any.insert(format!("a{i:06}"), "value", u64::from(i));
    }
    any.remove_range("b", "c", u64::from(KEYS));
    any.flush_active_memtable(0)?;
    for i in (0..KEYS).step_by(2) {
        any.insert(format!("b{i:06}"), "value", u64::from(KEYS + 1 + i));
    }
    any.flush_active_memtable(0)?;
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    let version = tree.current_version();
    let widened = version
        .iter_tables()
        .find(|table| table.metadata.key_range.min().starts_with(b"a"))
        .cloned()
        .unwrap_or_else(|| panic!("the table the range tombstone widens"));

    let keys: Vec<String> = (1..KEYS).step_by(2).map(|i| format!("b{i:06}")).collect();
    let mut remaining: Vec<(usize, u64)> = keys
        .iter()
        .enumerate()
        .map(|(index, key)| (index, crate::hash::hash64(key.as_bytes())))
        .collect();
    let mut results: Vec<Option<crate::value::InternalValue>> = alloc::vec![None; keys.len()];
    let Some(level) = version.level(0) else {
        panic!("level 0 exists");
    };
    let comparator = crate::comparator::default_comparator();
    let resolved = crate::Tree::resolve_level_staged(
        level,
        &mut remaining,
        &keys,
        SeqNo::MAX,
        comparator.as_ref(),
        &mut results,
    )?;
    assert!(resolved, "the other table has blocks to read");
    let count = keys.len() as u64;
    assert_eq!(counts(&widened), (count, count), "(probes, negatives)");
    Ok(())
}

/// A pinned read and a table's batch read count their probes as a plain read
/// does: each absent key is one probe and one negative, whether the one-bit
/// filter ruled it out or let it through to a read that found nothing, and so
/// is a key past the table's last block the filter let through.
#[test]
fn pinned_and_batch_reads_count_absent_keys_once() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_of_even_keys(folder.path(), 1.0, true)?;
    let table = only_table(&tree);
    let absent: Vec<String> = (1..KEYS).step_by(2).map(key).collect();
    let count = absent.len() as u64;

    for key in &absent {
        assert!(tree.get_pinned(key, SeqNo::MAX)?.is_none());
    }
    assert_eq!(counts(&table), (count, count), "pinned reads");

    let hashed = |keys: &[String]| -> Vec<(Vec<u8>, u64)> {
        keys.iter()
            .map(|key| (key.as_bytes().to_vec(), crate::hash::hash64(key.as_bytes())))
            .collect()
    };
    let absent_hashed = hashed(&absent);
    let sorted: Vec<(&[u8], u64)> = absent_hashed
        .iter()
        .map(|(key, hash)| (key.as_slice(), *hash))
        .collect();
    let found = table.batch_get(&sorted, SeqNo::MAX)?;
    assert!(found.iter().all(Option::is_none));
    assert_eq!(counts(&table), (2 * count, 2 * count), "batch read");

    // Past every block: no block walk, the passed keys still count.
    let beyond: Vec<String> = (0..100).map(|i| format!("zz{i:04}")).collect();
    let beyond_hashed = hashed(&beyond);
    let sorted: Vec<(&[u8], u64)> = beyond_hashed
        .iter()
        .map(|(key, hash)| (key.as_slice(), *hash))
        .collect();
    let found = table.batch_get(&sorted, SeqNo::MAX)?;
    assert!(found.iter().all(Option::is_none));
    assert_eq!(
        counts(&table),
        (2 * count + 100, 2 * count + 100),
        "batch read past every block"
    );
    Ok(())
}

/// A level resolved in chunks counts its probes as the serial resolve does:
/// every key its filter answered is a probe, and an absent key is a negative
/// one, whether the filter ruled it out or let it through to reads that found
/// no version (a one-bit filter lets about half through).
#[test]
fn a_chunked_resolve_counts_false_positives_once() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let any = tree_of_even_keys(folder.path(), 1.0, true)?;
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    // Up to the table's last key, which ends its last block.
    let keys: Vec<String> = (0..=KEYS).map(key).collect();
    let mut remaining: Vec<(usize, u64)> = keys
        .iter()
        .enumerate()
        .map(|(index, key)| (index, crate::hash::hash64(key.as_bytes())))
        .collect();
    let mut results: Vec<Option<crate::value::InternalValue>> = alloc::vec![None; keys.len()];
    let version = tree.current_version();
    let Some(level) = version.level(0) else {
        panic!("level 0 exists");
    };
    let comparator = crate::comparator::default_comparator();
    let resolved = crate::Tree::resolve_level_staged(
        level,
        &mut remaining,
        &keys,
        SeqNo::MAX,
        comparator.as_ref(),
        &mut results,
    )?;
    assert!(resolved, "the level has blocks to read");
    let present = results.iter().filter(|result| result.is_some()).count();
    let absent = keys.len() - present;
    assert_eq!(present, keys.len() / 2 + 1, "every even key is found");
    assert_eq!(
        counts(&only_table(&any)),
        (keys.len() as u64, absent as u64),
        "(probes, negatives)"
    );
    Ok(())
}

/// Two tables over one key range: an older hot narrow one holding
/// `key(100..200)` with 10000 negative probes, and a newer cold wide one
/// holding `key(0..1000)` with 1000.
fn hot_narrow_and_cold_wide(folder: &std::path::Path) -> crate::Result<(AnyTree, Table, Table)> {
    use crate::config::BlockSizePolicy;
    use crate::table::probe_stats::ProbeCounts;

    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    // Small blocks, so a block-granular share is close to the key share.
    .data_block_size_policy(BlockSizePolicy::all(256))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    for i in 100..200 {
        tree.insert(key(i), "value", 1);
    }
    tree.flush_active_memtable(0)?;
    for i in 0..1_000 {
        tree.insert(key(i), "value", 2);
    }
    tree.flush_active_memtable(0)?;

    let version = tree.current_version();
    let mut tables: Vec<Table> = version.iter_tables().cloned().collect();
    tables.sort_by_key(|table| table.metadata.item_count);
    let [hot, cold] = <[Table; 2]>::try_from(tables).unwrap_or_else(|_| panic!("two tables"));
    let seed = |table: &Table, negatives| {
        let Some(stats) = table.probe_stats() else {
            panic!("the table counts its probes");
        };
        stats.add(ProbeCounts {
            probes: negatives,
            negatives,
        });
    };
    seed(&hot, 10_000);
    seed(&cold, 1_000);
    Ok((tree, hot, cold))
}

/// An output takes each part of its range from one input, the oldest holding
/// data there, by the share of that input's data the part holds: over the
/// hot narrow input's keys its counts whole, and around them the cold wide
/// one's slices (nine tenths of its 1000), not the cold one's counts again
/// over the keys both hold.
#[test]
fn an_output_inherits_each_input_by_the_range_it_covers() -> crate::Result<()> {
    use crate::table::probe_stats::inherited_counts;

    let folder = tempfile::tempdir()?;
    let (_tree, hot, cold) = hot_narrow_and_cold_wide(folder.path())?;
    let inputs = [hot, cold];

    // Over the keys both inputs hold, the older one alone.
    let narrow = inherited_counts(key(100).as_bytes(), key(199).as_bytes(), &inputs)?;
    assert_eq!(narrow.negatives, 10_000);
    assert_eq!(narrow.probes, narrow.negatives);

    // The whole range: the hot input whole, and the cold one's nine tenths
    // around it, give or take the two boundary blocks.
    let whole = inherited_counts(key(0).as_bytes(), key(999).as_bytes(), &inputs)?;
    let cold_share = whole.negatives - 10_000;
    assert!((870..=910).contains(&cold_share), "cold share {cold_share}");

    // Merged whole, the output's density per key lies between the hot input's
    // (100 a key) and the cold one's (1 a key).
    let density = whole.negatives / 1_000;
    assert!(density > 1 && density < 100, "density {density}");

    // Split in two, the halves share the counts without creating any: the
    // hot input lies wholly in the first half.
    let low = inherited_counts(key(0).as_bytes(), key(499).as_bytes(), &inputs)?;
    let high = inherited_counts(key(500).as_bytes(), key(999).as_bytes(), &inputs)?;
    assert!(low.negatives >= 10_000 + 350, "low {low:?}");
    assert!(high.negatives <= 550, "high {high:?}");
    // Each block goes to one half, so the halves create no count; each part
    // loses at most one to rounding.
    let halves = low.negatives + high.negatives;
    assert!(
        halves <= whole.negatives + 4 && halves + 4 >= whole.negatives,
        "halves {halves}, whole {}",
        whole.negatives
    );
    Ok(())
}

/// A compaction hands its outputs the inputs' counts: merged into one table,
/// the output carries the hot input whole and the cold one's slices around
/// it; split into several, the outputs share them by range without creating
/// any.
#[test]
fn a_compaction_hands_the_inputs_counts_to_its_outputs() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let (tree, hot, cold) = hot_narrow_and_cold_wide(folder.path())?;
    let inherited = crate::table::probe_stats::inherited_counts(
        key(0).as_bytes(),
        key(999).as_bytes(),
        &[hot.clone(), cold.clone()],
    )?
    .negatives;
    drop((hot, cold));
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(counts(&only_table(&tree)), (inherited, inherited));
    assert!(
        (10_870..=10_910).contains(&inherited),
        "inherited {inherited}"
    );

    let folder = tempfile::tempdir()?;
    let (tree, hot, cold) = hot_narrow_and_cold_wide(folder.path())?;
    drop((hot, cold));
    // Small outputs: the merge is cut into several tables.
    tree.major_compact(4 * 1_024, 0)?;
    let version = tree.current_version();
    let outputs: Vec<Table> = version.iter_tables().cloned().collect();
    assert!(outputs.len() > 2, "{} outputs", outputs.len());
    let total: u64 = outputs.iter().map(|table| counts(table).1).sum();
    // Each input block goes to one output, so the outputs create no count;
    // each part of each output loses at most one to rounding.
    let rounding = 4 * outputs.len() as u64;
    assert!(
        (inherited - rounding..=inherited + rounding).contains(&total),
        "total {total} over {} outputs",
        outputs.len()
    );
    // The outputs holding the hot range carry its counts, the others not.
    let hot_range: u64 = outputs
        .iter()
        .filter(|table| {
            let range = &table.metadata.key_range;
            range.min().as_ref() <= key(199).as_bytes()
                && range.max().as_ref() >= key(100).as_bytes()
        })
        .map(|table| counts(table).1)
        .sum();
    assert!(hot_range >= 10_000, "hot range {hot_range}");
    Ok(())
}

/// A tight-space slice re-opens each input past its boundary as a restricted
/// view. The view keeps the share of the probe counts its suffix holds, which
/// with the share the slice's outputs took from the prefix is the whole.
#[test]
fn a_restricted_view_keeps_the_suffix_share_of_the_counts() -> crate::Result<()> {
    use crate::table::probe_stats::share_of;
    use core::ops::Bound::{Excluded, Unbounded};

    let folder = tempfile::tempdir()?;
    let (_tree, _hot, cold) = hot_narrow_and_cold_wide(folder.path())?;
    let boundary = key(500);
    let restricted = cold.reopen_restricted(boundary.as_bytes().to_vec().into())?;
    let (_, suffix) = counts(&restricted);
    let prefix = share_of(&cold, (Unbounded, Excluded(boundary.as_bytes())))?.negatives;
    assert!((450..=550).contains(&suffix), "suffix {suffix}");
    assert!(
        (1_000 - 2..=1_000).contains(&(prefix + suffix)),
        "prefix {prefix} + suffix {suffix}"
    );
    Ok(())
}

/// A restricted view is a distinct table and is not bound again, so what the
/// tree bound the original with must carry over: its scans read by the tree's
/// columnar read budget, not the default one.
#[test]
fn a_restricted_view_keeps_the_trees_read_budget() -> crate::Result<()> {
    use crate::config::ReadBudget;

    let budget = ReadBudget::new(4_096, 2);
    assert_ne!(budget, ReadBudget::default());
    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .columnar_read_budget(budget)
    .open()?;
    for i in 0..100 {
        tree.insert(key(i), "value", 1);
    }
    tree.flush_active_memtable(0)?;
    let table = only_table(&tree);
    assert_eq!(table.read_budget(), budget);
    let restricted = table.reopen_restricted(key(50).into_bytes().into())?;
    assert_eq!(restricted.read_budget(), budget);
    Ok(())
}

/// An input without a filter answers no probe, so its empty counts say
/// nothing of the lookups over its keys: the output takes the newer filtered
/// input's there, though the filterless one is older.
#[test]
fn a_filterless_input_leaves_the_counts_to_a_filtered_one() -> crate::Result<()> {
    use crate::table::probe_stats::{ProbeCounts, inherited_counts};

    let folder = tempfile::tempdir()?;
    // Filters on level 0 only: the compacted table below builds none.
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_policy(FilterPolicy::new([
        FilterPolicyEntry::Bloom(BloomConstructionPolicy::BitsPerKey(10.0)),
        FilterPolicyEntry::None,
    ]))
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    for i in 0..KEYS {
        any.insert(key(i), "value", u64::from(i));
    }
    any.flush_active_memtable(0)?;
    any.major_compact(u64::MAX, 0)?;
    for i in 0..KEYS {
        any.insert(key(i), "newer", u64::from(KEYS + i));
    }
    any.flush_active_memtable(0)?;
    let version = any.current_version();
    let (filtered, filterless): (Vec<Table>, Vec<Table>) = version
        .iter_tables()
        .cloned()
        .partition(|table| table.filter_size() > 0);
    let ([newer], [older]) = (filtered.as_slice(), filterless.as_slice()) else {
        panic!("one table with a filter and one without");
    };
    let Some(stats) = newer.probe_stats() else {
        panic!("the table counts its probes");
    };
    stats.add(ProbeCounts {
        probes: 1_000,
        negatives: 1_000,
    });

    let inherited = inherited_counts(
        key(0).as_bytes(),
        key(KEYS - 1).as_bytes(),
        &[older.clone(), newer.clone()],
    )?;
    assert_eq!(inherited.negatives, 1_000);
    Ok(())
}

/// A range outside an input's keys inherits nothing from it.
#[test]
fn an_output_outside_an_input_inherits_nothing_from_it() -> crate::Result<()> {
    use crate::table::probe_stats::inherited_counts;

    let folder = tempfile::tempdir()?;
    let (_tree, hot, cold) = hot_narrow_and_cold_wide(folder.path())?;
    let beyond = inherited_counts(key(2_000).as_bytes(), key(3_000).as_bytes(), &[hot, cold])?;
    assert_eq!(beyond, crate::table::probe_stats::ProbeCounts::default());
    Ok(())
}

/// A tree reopened with an advisor counts the probes of the tables it
/// recovers, starting from zero.
#[test]
fn recovered_tables_count_from_zero() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    {
        let tree = tree_of_even_keys(folder.path(), 10.0, true)?;
        tree.get(key(1), SeqNo::MAX)?;
    }
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_advisor(Some(FilterAdvisor::new(u64::MAX)))
    .open()?;
    assert_eq!(counts(&only_table(&tree)), (0, 0));
    tree.get(key(1), SeqNo::MAX)?;
    assert_eq!(counts(&only_table(&tree)), (1, 1));
    Ok(())
}
