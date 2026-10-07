// Lifetime groups of separated values: where a flush, a hint and a relocation
// put a value, and that none of it changes what a read returns or which blob
// files a collection removes.

use lsm_tree::{
    AbstractTree, AnyTree, Config, Guard as _, KvSeparationOptions, SeqNo, SequenceNumberCounter,
    config::{LifetimeGroups, LifetimeHint},
    get_tmp_folder,
};
use std::collections::{BTreeMap, BTreeSet};
use test_log::test;

const VALUE_LEN: usize = 256;

fn value(tag: &str, version: u64) -> Vec<u8> {
    format!("{tag}-{version:08}-")
        .repeat(VALUE_LEN / 16)
        .into_bytes()
}

fn open(
    folder: &std::path::Path,
    seqno: &SequenceNumberCounter,
    kv: KvSeparationOptions,
) -> AnyTree {
    Config::new(folder, seqno.clone(), SequenceNumberCounter::default())
        .with_kv_separation(Some(kv.separation_threshold(64)))
        .open()
        .expect("open")
}

fn groups(count: u8) -> LifetimeGroups {
    LifetimeGroups::new(count).expect("within the bound")
}

/// The lifetime classes of the blob files the current version holds.
fn classes(tree: &AnyTree) -> BTreeSet<u8> {
    tree.current_version()
        .blob_files
        .iter()
        .map(lsm_tree::BlobFile::lifetime_class)
        .collect()
}

/// The values the current version's blob files hold, per lifetime class.
/// Each test gives every group it expects a different number of keys, so
/// these counts tell which keys went where.
fn values_per_class(tree: &AnyTree) -> BTreeMap<u8, u64> {
    let mut per_class = BTreeMap::new();
    for file in tree.current_version().blob_files.iter() {
        *per_class.entry(file.lifetime_class()).or_default() += file.len();
    }
    per_class
}

/// A key the flush finds overwritten goes to group 0, a key written once to
/// group 1.
#[test]
fn flush_puts_overwritten_keys_in_the_short_lived_group() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = open(
        folder.path(),
        &seqno,
        KvSeparationOptions::default().lifetime_groups(groups(3)),
    );

    for key in ["hot-a", "hot-b", "hot-c"] {
        for version in 0..3 {
            tree.insert(key, value(key, version), seqno.next());
        }
    }
    tree.insert("still", value("still", 0), seqno.next());
    // A watermark above every version: the flush keeps only the newest of each
    // hot key, so only the observation under the stream knows it was
    // overwritten.
    tree.flush_active_memtable(seqno.get())?;

    assert_eq!(values_per_class(&tree), BTreeMap::from([(0, 3), (1, 1)]));
    assert_eq!(
        &*tree.get("hot-b", SeqNo::MAX)?.expect("present"),
        value("hot-b", 2).as_slice()
    );
    Ok(())
}

/// Without grouping, the same writes produce class 0 only, in one file.
#[test]
fn a_single_group_keeps_every_value_in_class_zero() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = open(folder.path(), &seqno, KvSeparationOptions::default());

    for version in 0..3 {
        tree.insert("hot", value("hot", version), seqno.next());
    }
    tree.insert("still", value("still", 0), seqno.next());
    tree.flush_active_memtable(seqno.get())?;

    assert_eq!(classes(&tree), BTreeSet::from([0]));
    assert_eq!(tree.current_version().blob_files.len(), 1);
    Ok(())
}

/// A hint overrides the observation where it answers, and a class past the
/// last group goes to the last group.
#[test]
fn the_hint_overrides_the_observed_group() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let hint = LifetimeHint::new(|key: &[u8]| {
        if key.starts_with(b"pinned") {
            Some(1)
        } else if key.starts_with(b"archive") {
            Some(200)
        } else {
            None
        }
    });
    let tree = open(
        folder.path(),
        &seqno,
        KvSeparationOptions::default()
            .lifetime_groups(groups(3))
            .lifetime_hint(hint),
    );

    // Overwritten, but hinted long-lived: group 1.
    for version in 0..3 {
        tree.insert("pinned", value("pinned", version), seqno.next());
    }
    // Hinted past the last group: capped to group 2.
    for key in ["archive-a", "archive-b"] {
        tree.insert(key, value(key, 0), seqno.next());
    }
    // No hint, overwritten: the observation puts them in group 0.
    for key in ["churn-a", "churn-b", "churn-c"] {
        for version in 0..2 {
            tree.insert(key, value(key, version), seqno.next());
        }
    }
    tree.flush_active_memtable(seqno.get())?;

    assert_eq!(
        values_per_class(&tree),
        BTreeMap::from([(0, 3), (1, 1), (2, 2)])
    );
    Ok(())
}

/// A value relocated out of a file moves one group up, having outlived it.
#[test]
fn relocation_moves_survivors_one_group_up() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let tree = open(
        folder.path(),
        &seqno,
        KvSeparationOptions::default()
            .lifetime_groups(groups(3))
            .staleness_threshold(0.01)
            .age_cutoff(1.0),
    );

    for i in 0..20 {
        tree.insert(format!("k{i:02}"), value("first", i), seqno.next());
    }
    tree.flush_active_memtable(seqno.get())?;
    assert_eq!(classes(&tree), BTreeSet::from([1]));

    // Eight keys get a new value, which leaves the first file 40% stale.
    for i in 0..8 {
        tree.insert(format!("k{i:02}"), value("second", i), seqno.next());
    }
    tree.flush_active_memtable(seqno.get())?;
    tree.major_compact(u64::MAX, seqno.get())?;
    // The first compaction records the staleness, the second relocates.
    tree.major_compact(u64::MAX, seqno.get())?;

    // Twelve survivors of the class 1 file move to group 2; the eight keys
    // written again stay in the class 1 file of the second flush.
    assert_eq!(values_per_class(&tree), BTreeMap::from([(1, 8), (2, 12)]));
    for i in 0..20 {
        let expected = if i < 8 {
            value("second", i)
        } else {
            value("first", i)
        };
        assert_eq!(
            &*tree.get(format!("k{i:02}"), SeqNo::MAX)?.expect("present"),
            expected.as_slice()
        );
    }
    Ok(())
}

/// Every key and value the tree returns, in order.
fn contents(tree: &AnyTree) -> lsm_tree::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    tree.iter(SeqNo::MAX, None)
        .map(|guard| {
            let (k, v) = guard.into_inner()?;
            Ok((k.to_vec(), v.to_vec()))
        })
        .collect()
}

/// The same writes, flushes and compactions under no grouping and under the
/// widest grouping with an arbitrary hint: every read answers the same, and
/// once every key is deleted both collect every blob file.
#[test]
fn a_hint_never_changes_reads_or_collection() -> lsm_tree::Result<()> {
    let plain_folder = get_tmp_folder();
    let grouped_folder = get_tmp_folder();
    let plain_seqno = SequenceNumberCounter::default();
    let grouped_seqno = SequenceNumberCounter::default();
    let gc = || {
        KvSeparationOptions::default()
            .staleness_threshold(0.01)
            .age_cutoff(1.0)
    };
    // Arbitrary on purpose: a wrong hint may only cost relocation work.
    let hint = LifetimeHint::new(|key: &[u8]| key.last().map(|b| b % 7));
    let plain = open(plain_folder.path(), &plain_seqno, gc());
    let grouped = open(
        grouped_folder.path(),
        &grouped_seqno,
        gc().lifetime_groups(LifetimeGroups::MAX)
            .lifetime_hint(hint),
    );

    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for round in 0..6u64 {
        for _ in 0..60 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let key = format!("key-{:03}", state % 90);
            for (tree, seqno) in [(&plain, &plain_seqno), (&grouped, &grouped_seqno)] {
                if state.is_multiple_of(11) {
                    tree.remove(key.as_str(), seqno.next());
                } else {
                    tree.insert(key.as_str(), value("v", state % 1_000), seqno.next());
                }
            }
        }
        for (tree, seqno) in [(&plain, &plain_seqno), (&grouped, &grouped_seqno)] {
            tree.flush_active_memtable(seqno.get())?;
            if round % 2 == 1 {
                tree.major_compact(u64::MAX, seqno.get())?;
            }
        }
        assert_eq!(contents(&plain)?, contents(&grouped)?, "round {round}");
    }
    assert!(
        classes(&grouped).len() > 1,
        "the grouped tree really split its values"
    );

    for (tree, seqno) in [(&plain, &plain_seqno), (&grouped, &grouped_seqno)] {
        for i in 0..90 {
            tree.remove(format!("key-{i:03}"), seqno.next());
        }
        tree.flush_active_memtable(seqno.get())?;
        tree.major_compact(u64::MAX, seqno.get())?;
        tree.major_compact(u64::MAX, seqno.get())?;
        assert!(contents(tree)?.is_empty());
        assert_eq!(
            tree.current_version().blob_files.len(),
            0,
            "every blob file is collected, whatever its class"
        );
    }
    Ok(())
}
