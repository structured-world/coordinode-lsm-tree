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

/// A batch scan reading a table whose rows are all past its snapshot counts
/// on it: it read the table, though it returns none of its rows.
#[test]
fn batch_scan_reading_rows_past_its_snapshot_counts_the_table() -> crate::Result<()> {
    use crate::{AbstractTree, Config, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..100u64 {
        tree.insert(format!("k{i:04}"), "v", 1_000 + i);
    }
    tree.flush_active_memtable(0)?;
    let read = tree
        .batch_range_scan(["k0000".to_string()..="k0010".to_string()], 500, None)
        .count();
    assert_eq!(read, 0, "every row is past the snapshot");
    let scans: Vec<u64> = tree
        .current_version()
        .iter_tables()
        .map(crate::table::Table::scans)
        .collect();
    assert_eq!(scans, [1]);
    Ok(())
}

/// A seekable scan read backward past rows newer than its snapshot yields
/// only the rows the snapshot sees, and counts the scan on the table it
/// reads them from.
#[test]
fn seekable_scan_read_backward_past_its_snapshot_counts_the_table() -> crate::Result<()> {
    use crate::{AbstractTree, Config, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..100u64 {
        tree.insert(format!("k{i:04}"), "v", 1_000 + i);
    }
    tree.flush_active_memtable(0)?;
    // The upper half of the keys is newer than the snapshot, and a backward
    // read meets it first.
    let read = tree
        .range_seekable::<&str, _>(.., 1_050, None)
        .rev()
        .count();
    assert_eq!(read, 50);
    let scans: Vec<u64> = tree
        .current_version()
        .iter_tables()
        .map(crate::table::Table::scans)
        .collect();
    assert_eq!(scans, [1]);
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
    fn name(&self) -> &'static str {
        "up-to-colon"
    }

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

/// The keys a scan of `tree` at `seqno` hands out, forward and backward.
#[expect(clippy::expect_used, reason = "test code")]
fn scanned_keys(tree: &crate::AnyTree, seqno: crate::SeqNo) -> (Vec<String>, Vec<String>) {
    use crate::{AbstractTree, Guard};
    let keys = |guard: crate::IterGuardImpl| {
        String::from_utf8(guard.key().expect("scan row").to_vec()).expect("utf8 key")
    };
    let forward = tree.iter(seqno, None).map(keys).collect();
    let backward = tree.iter(seqno, None).rev().map(keys).collect();
    (forward, backward)
}

/// One table holding one version of each key is the scan's only source: no
/// merge and no version grouping run, and the scan still hides what its
/// snapshot does not see and what a tombstone ends, in both directions.
#[expect(clippy::expect_used, reason = "test code")]
#[test]
fn a_lone_table_of_one_version_per_key_scans_as_its_snapshot_sees_it() -> crate::Result<()> {
    use crate::{AbstractTree, Config, Guard, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..10u64 {
        let key = format!("k{i}");
        match i {
            3 => {
                tree.remove(key, i + 1);
            }
            8 => {
                tree.insert(key, "late", 100);
            }
            _ => {
                tree.insert(key, "v", i + 1);
            }
        }
    }
    tree.flush_active_memtable(0)?;
    let version = tree.current_version();
    let table = version.iter_tables().next().expect("one table");
    assert!(holds_one_version_per_key(table), "the case under test");

    let expected: Vec<String> = [0, 1, 2, 4, 5, 6, 7, 9]
        .iter()
        .map(|i| format!("k{i}"))
        .collect();
    let (forward, backward) = scanned_keys(&tree, 50);
    assert_eq!(
        forward, expected,
        "no tombstoned key, nothing above the snapshot"
    );
    assert_eq!(backward, expected.iter().rev().cloned().collect::<Vec<_>>());

    // Both ends of one scan meet without handing a row out twice.
    let mut both = tree.iter(50, None);
    let front = both.next().expect("front row").key()?;
    let back = both.next_back().expect("back row").key()?;
    assert_eq!((&*front, &*back), (&b"k0"[..], &b"k9"[..]));
    assert_eq!(both.count(), 6, "the rows between the two ends");
    Ok(())
}

/// One table holding several versions of a key is the scan's only source: its
/// versions are still grouped, so a key comes out once, as the snapshot's
/// newest version of it.
#[expect(clippy::expect_used, reason = "test code")]
#[test]
fn a_lone_table_of_several_versions_hands_out_the_newest_visible_one() -> crate::Result<()> {
    use crate::{AbstractTree, Config, Guard, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    tree.insert("a", "old", 1);
    tree.insert("a", "new", 2);
    tree.insert("b", "only", 3);
    tree.flush_active_memtable(0)?;
    let version = tree.current_version();
    let table = version.iter_tables().next().expect("one table");
    assert!(!holds_one_version_per_key(table), "the case under test");

    let rows = |seqno| -> Vec<(Vec<u8>, Vec<u8>)> {
        tree.iter(seqno, None)
            .map(|guard| {
                let (k, v) = guard.into_inner().expect("scan row");
                (k.to_vec(), v.to_vec())
            })
            .collect()
    };
    assert_eq!(
        rows(crate::SeqNo::MAX),
        [
            (b"a".to_vec(), b"new".to_vec()),
            (b"b".to_vec(), b"only".to_vec())
        ]
    );
    assert_eq!(
        rows(3),
        [(b"a".to_vec(), b"new".to_vec())],
        "b is above the snapshot"
    );
    assert_eq!(rows(2), [(b"a".to_vec(), b"old".to_vec())]);
    Ok(())
}

/// With a merge operator, a lone table of one version per key still has its
/// operands resolved: an operand with no base comes out merged onto nothing.
#[expect(clippy::expect_used, reason = "test code")]
#[test]
fn a_lone_table_of_one_version_per_key_still_resolves_merge_operands() -> crate::Result<()> {
    use crate::{AbstractTree, Config, Guard, SequenceNumberCounter};

    struct Bracket;
    impl crate::MergeOperator for Bracket {
        fn merge(
            &self,
            _key: &[u8],
            base: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> crate::Result<crate::UserValue> {
            let mut merged = b"[".to_vec();
            merged.extend_from_slice(base.unwrap_or_default());
            for operand in operands {
                merged.extend_from_slice(operand);
            }
            merged.push(b']');
            Ok(merged.into())
        }
    }

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(alloc::sync::Arc::new(Bracket)))
    .open()?;
    let crate::AnyTree::Standard(standard) = &tree else {
        panic!("a standard tree");
    };
    standard.merge("m", "+", 1);
    tree.insert("p", "plain", 2);
    tree.flush_active_memtable(0)?;
    let version = tree.current_version();
    let table = version.iter_tables().next().expect("one table");
    assert!(holds_one_version_per_key(table), "the case under test");

    let values: Vec<Vec<u8>> = tree
        .iter(crate::SeqNo::MAX, None)
        .map(|guard| guard.value().expect("scan row").to_vec())
        .collect();
    assert_eq!(values, [b"[+]".to_vec(), b"plain".to_vec()]);
    Ok(())
}

/// A range deletion held by a memtable with no row in it still deletes the
/// rows a table holds under it, though the memtable is no source of the scan.
#[test]
fn a_range_deletion_in_a_memtable_with_no_rows_still_deletes_table_rows() -> crate::Result<()> {
    use crate::{AbstractTree, Config, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..6u64 {
        tree.insert(format!("k{i}"), "v", i + 1);
    }
    tree.flush_active_memtable(0)?;
    tree.remove_range("k2", "k4", 10);

    let expected = ["k0", "k1", "k4", "k5"].map(String::from).to_vec();
    let (forward, backward) = scanned_keys(&tree, 11);
    assert_eq!(forward, expected);
    assert_eq!(backward, expected.iter().rev().cloned().collect::<Vec<_>>());
    let (before, _) = scanned_keys(&tree, 10);
    assert_eq!(before.len(), 6, "the deletion is above that snapshot");
    Ok(())
}
