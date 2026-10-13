use super::*;
use crate::Slice;
use core::ops::Bound::{Excluded, Included, Unbounded};
use test_log::test;

fn test_prefix(prefix: &[u8], upper_bound: Bound<&[u8]>) {
    let range = prefix_to_range(prefix);
    assert_eq!(
        range,
        (
            match prefix {
                _ if prefix.is_empty() => Unbounded,
                _ => Included(Slice::from(prefix)),
            },
            upper_bound.map(Slice::from),
        ),
    );
}

#[test]
fn prefix_to_range_basic() {
    test_prefix(b"abc", Excluded(b"abd"));
}

#[test]
fn prefix_to_range_empty() {
    test_prefix(b"", Unbounded);
}

#[test]
fn prefix_to_range_single_char() {
    test_prefix(b"a", Excluded(b"b"));
}

#[test]
fn prefix_to_range_1() {
    test_prefix(&[0, 250], Excluded(&[0, 251]));
}

#[test]
fn prefix_to_range_2() {
    test_prefix(&[0, 250, 50], Excluded(&[0, 250, 51]));
}

#[test]
fn prefix_to_range_3() {
    test_prefix(&[255, 255, 255], Unbounded);
}

#[test]
fn prefix_to_range_char_max() {
    test_prefix(&[0, 255], Excluded(&[1]));
}

#[test]
fn prefix_to_range_char_max_2() {
    test_prefix(&[0, 2, 255], Excluded(&[0, 3]));
}

/// A batch scan opened over the whole keyspace counts only the tables its
/// intervals read, not every table it opened a source over.
#[test]
fn batch_scan_counts_only_the_tables_its_intervals_read() -> crate::Result<()> {
    use crate::version::run::Ranged;
    use crate::{AbstractTree, Config, SeqNo, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    // Two overlapping flushes: two runs of one table each.
    for (from, round) in [(0u64, 0u64), (50, 1)] {
        for i in from..from + 100 {
            tree.insert(format!("k{i:04}"), "v", round * 1_000 + i);
        }
        tree.flush_active_memtable(0)?;
    }
    let read = tree
        .batch_range_scan(
            ["k0000".to_string()..="k0010".to_string()],
            SeqNo::MAX,
            None,
        )
        .count();
    assert_eq!(read, 11);
    let mut scans: Vec<_> = tree
        .current_version()
        .iter_tables()
        .map(|table| (table.key_range().min().clone(), table.scans()))
        .collect();
    scans.sort();
    assert_eq!(
        scans.into_iter().map(|(_, n)| n).collect::<Vec<_>>(),
        [1, 0],
        "the table past the interval was not read"
    );
    Ok(())
}

/// A batch scan whose intervals all fall in one table of a run counts once
/// on it, as the batch counts once in the read mix.
#[expect(clippy::expect_used, reason = "test code")]
#[test]
fn batch_scan_of_many_intervals_in_one_table_of_a_run_counts_it_once() -> crate::Result<()> {
    use crate::version::run::Ranged;
    use crate::{AbstractTree, Config, SeqNo, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for from in [0u64, 1_000, 2_000] {
        for i in from..from + 100 {
            tree.insert(format!("k{i:04}"), "v", i);
        }
        tree.flush_active_memtable(0)?;
        tree.compact(alloc::sync::Arc::new(crate::compaction::MoveDown(0, 1)), 0)?;
    }
    assert_eq!(
        tree.current_version().level(1).expect("L1").run_count(),
        1,
        "the fixture needs one run of three tables"
    );
    let read = tree
        .batch_range_scan(
            [
                "k0001".to_string()..="k0002".to_string(),
                "k0010".to_string()..="k0011".to_string(),
                "k0020".to_string()..="k0021".to_string(),
            ],
            SeqNo::MAX,
            None,
        )
        .count();
    assert_eq!(read, 6);
    let mut scans: Vec<_> = tree
        .current_version()
        .iter_tables()
        .map(|table| (table.key_range().min().clone(), table.scans()))
        .collect();
    scans.sort();
    assert_eq!(
        scans.into_iter().map(|(_, n)| n).collect::<Vec<_>>(),
        [1, 0, 0]
    );
    Ok(())
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

/// A prefix scan whose filters let two tables of a run through reaches both,
/// each counted, and not the table its filter turns away.
#[expect(clippy::expect_used, reason = "test code")]
#[test]
fn prefix_scan_over_two_tables_of_a_run_counts_both() -> crate::Result<()> {
    use crate::version::run::Ranged;
    use crate::{AbstractTree, Config, SeqNo, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(alloc::sync::Arc::new(UpToColon))
    .open()?;
    for (prefix, from) in [("a", 0u64), ("a", 50), ("b", 0)] {
        for i in from..from + 50 {
            tree.insert(format!("{prefix}:{i:03}"), "v", i);
        }
        tree.flush_active_memtable(0)?;
    }
    tree.compact(alloc::sync::Arc::new(crate::compaction::MoveDown(0, 1)), 0)?;
    let version = tree.current_version();
    assert_eq!(version.level(1).expect("L1").run_count(), 1);

    assert_eq!(tree.prefix("a:", SeqNo::MAX, None).count(), 100);
    let mut scans: Vec<_> = tree
        .current_version()
        .level(1)
        .expect("L1")
        .iter()
        .flat_map(|run| run.iter())
        .map(|table| (table.key_range().min().clone(), table.scans()))
        .collect();
    scans.sort();
    assert_eq!(
        scans.into_iter().map(|(_, n)| n).collect::<Vec<_>>(),
        [1, 1, 0]
    );
    Ok(())
}

/// A scan counts once on every table of a run it reaches, and on no other:
/// one table, two, or more.
#[expect(clippy::expect_used, reason = "test code")]
#[test]
fn a_scan_counts_on_each_table_it_reaches() -> crate::Result<()> {
    use crate::version::run::Ranged;
    use crate::{AbstractTree, Config, SeqNo, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for from in [0u64, 1_000, 2_000] {
        for i in from..from + 100 {
            tree.insert(format!("k{i:04}"), "v", i);
        }
        tree.flush_active_memtable(0)?;
        tree.compact(alloc::sync::Arc::new(crate::compaction::MoveDown(0, 1)), 0)?;
    }
    let scans = || -> Vec<u64> {
        let version = tree.current_version();
        let mut tables: Vec<_> = version
            .level(1)
            .expect("L1")
            .iter()
            .flat_map(|run| run.iter())
            .map(|table| (table.key_range().min().clone(), table.scans()))
            .collect();
        tables.sort();
        tables.into_iter().map(|(_, scans)| scans).collect()
    };
    assert_eq!(scans(), [0, 0, 0]);

    assert_eq!(tree.range("k0010"..="k0020", SeqNo::MAX, None).count(), 11);
    assert_eq!(scans(), [1, 0, 0], "one table");
    assert_eq!(tree.range("k0050"..="k1050", SeqNo::MAX, None).count(), 101);
    assert_eq!(scans(), [2, 1, 0], "two tables");
    assert_eq!(tree.iter(SeqNo::MAX, None).count(), 300);
    assert_eq!(scans(), [3, 2, 1], "every table");
    assert_eq!(
        tree.current_version().read_mix().counts(),
        (0, 3),
        "each scan counted once"
    );
    Ok(())
}
