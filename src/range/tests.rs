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
    let made: u64 = tree
        .current_version()
        .iter_tables()
        .map(crate::table::Table::scan_starts)
        .sum();
    assert_eq!(made, 3, "each scan counted as made once");
    Ok(())
}
