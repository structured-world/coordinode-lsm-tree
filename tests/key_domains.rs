//! Tables written under a key-domain policy never span two domains: flush,
//! ingest and compaction end an output before the next domain's first key.

use lsm_tree::{
    AbstractTree, AnyTree, Config, Guard as _, KeyDomains, SeqNo, SequenceNumberCounter,
    SharedComparator, UserComparator, UserKey,
};
use std::cmp::Ordering;
use std::sync::Arc;

/// One domain per epoch: the key's first four bytes, big-endian, in byte
/// order.
struct Epochs;

impl KeyDomains for Epochs {
    fn domain_end(&self, key: &[u8]) -> Option<UserKey> {
        let epoch = u32::from_be_bytes(key.get(..4)?.try_into().ok()?);
        Some(UserKey::from(
            epoch.checked_add(1)?.to_be_bytes().as_slice(),
        ))
    }
}

/// Byte order reversed.
struct Reverse;

impl UserComparator for Reverse {
    fn name(&self) -> &'static str {
        "reverse"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a)
    }
}

/// [`Epochs`] in [`Reverse`] order: a domain ends where the epoch below
/// begins, at the greatest key of that epoch.
struct ReverseEpochs;

impl KeyDomains for ReverseEpochs {
    fn domain_end(&self, key: &[u8]) -> Option<UserKey> {
        let epoch = u32::from_be_bytes(key.get(..4)?.try_into().ok()?);
        let below = epoch.checked_sub(1)?;
        let mut end = below.to_be_bytes().to_vec();
        // Past every key of `below` in reverse order: more 0xFF than any key.
        end.extend_from_slice(&[u8::MAX; 64]);
        Some(UserKey::from(end))
    }
}

fn key(epoch: u32, i: u32) -> Vec<u8> {
    let mut key = epoch.to_be_bytes().to_vec();
    key.extend_from_slice(&i.to_be_bytes());
    key
}

fn epoch_of(key: &[u8]) -> u32 {
    u32::from_be_bytes(key[..4].try_into().expect("four-byte epoch"))
}

fn open(
    folder: &std::path::Path,
    domains: Option<Arc<dyn KeyDomains>>,
) -> lsm_tree::Result<AnyTree> {
    let mut config = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    if let Some(domains) = domains {
        config = config.key_domains(domains);
    }
    config.open()
}

/// The epochs each table's key range reaches, smallest and largest.
fn table_epochs(tree: &AnyTree) -> Vec<(u32, u32)> {
    tree.current_version()
        .iter_tables()
        .map(|table| {
            let range = &table.metadata.key_range;
            (epoch_of(range.min()), epoch_of(range.max()))
        })
        .collect()
}

fn latest(tree: &AnyTree) -> lsm_tree::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    tree.iter(SeqNo::MAX, None)
        .map(|item| {
            let (k, v) = item.into_inner()?;
            Ok((k.to_vec(), v.to_vec()))
        })
        .collect()
}

/// One memtable holding five epochs flushes into a table per epoch, each
/// below the target size, and every key reads back.
#[test]
fn flush_ends_a_table_at_each_domain_end() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), Some(Arc::new(Epochs)))?;
    for epoch in 0..5 {
        for i in 0..100 {
            tree.insert(key(epoch, i), b"v", u64::from(epoch * 100 + i));
        }
    }
    tree.flush_active_memtable(0)?;

    let mut epochs = table_epochs(&tree);
    epochs.sort_unstable();
    assert_eq!(
        epochs,
        (0..5).map(|e| (e, e)).collect::<Vec<_>>(),
        "a table per epoch, none spanning two",
    );
    assert_eq!(latest(&tree)?.len(), 500, "every key reads back");
    Ok(())
}

/// Without a policy the same flush writes one table across the epochs: the
/// cut is the policy's, not a change of the size rule.
#[test]
fn without_a_policy_a_flush_spans_domains() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), None)?;
    for epoch in 0..5 {
        for i in 0..100 {
            tree.insert(key(epoch, i), b"v", u64::from(epoch * 100 + i));
        }
    }
    tree.flush_active_memtable(0)?;

    assert_eq!(
        table_epochs(&tree),
        vec![(0, 4)],
        "one table over every epoch"
    );
    Ok(())
}

/// Overlapping flushes of several epochs, with later versions of earlier
/// keys and a late arrival into an old epoch, compact into tables of one
/// epoch each, and every key reads its latest value.
#[test]
fn compaction_keeps_domains_in_tables_of_their_own() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), Some(Arc::new(Epochs)))?;
    let mut seqno = 0;
    for round in 0..3u32 {
        for epoch in 0..4 {
            for i in (round..60).step_by(3) {
                tree.insert(key(epoch, i), round.to_be_bytes(), seqno);
                seqno += 1;
            }
        }
        tree.flush_active_memtable(0)?;
    }
    // Same keys again, and a late arrival into epoch 1.
    for i in 0..60 {
        tree.insert(key(2, i), 9u32.to_be_bytes(), seqno);
        seqno += 1;
    }
    tree.insert(key(1, 1_000), 7u32.to_be_bytes(), seqno);
    tree.flush_active_memtable(0)?;

    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    let epochs = table_epochs(&tree);
    assert!(
        epochs.iter().all(|(lo, hi)| lo == hi),
        "a compacted table spans epochs: {epochs:?}",
    );
    assert_eq!(
        epochs.len(),
        4,
        "one table per epoch at an unbounded target"
    );
    let rows = latest(&tree)?;
    assert_eq!(rows.len(), 4 * 60 + 1);
    for (k, v) in &rows {
        let expected: u32 = match (
            epoch_of(k),
            u32::from_be_bytes(k[4..].try_into().expect("index")),
        ) {
            (2, _) => 9,
            (1, 1_000) => 7,
            (_, i) => i % 3,
        };
        assert_eq!(
            v.as_slice(),
            expected.to_be_bytes(),
            "latest value of {k:?}"
        );
    }
    Ok(())
}

/// Domains are intervals of the tree's comparator order: under a reversed
/// comparator the cut falls where the reversed order leaves an epoch.
#[test]
fn domains_follow_the_comparator_order() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let comparator: SharedComparator = Arc::new(Reverse);
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .comparator(comparator)
    .key_domains(Arc::new(ReverseEpochs))
    .open()?;
    for epoch in 0..3 {
        for i in 0..50 {
            tree.insert(key(epoch, i), b"v", u64::from(epoch * 50 + i));
        }
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    let mut epochs = table_epochs(&tree);
    epochs.sort_unstable();
    assert_eq!(epochs, vec![(0, 0), (1, 1), (2, 2)], "a table per epoch");
    let keys: Vec<u32> = latest(&tree)?.iter().map(|(k, _)| epoch_of(k)).collect();
    assert!(keys.windows(2).all(|w| w[0] >= w[1]), "reverse order kept");
    Ok(())
}

/// A domain policy cuts at its ends only: one epoch larger than the target
/// still splits by size inside the epoch, never across it.
#[test]
fn a_domain_larger_than_the_target_splits_by_size_within_it() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), Some(Arc::new(Epochs)))?;
    for epoch in 0..2 {
        for i in 0..2_000 {
            tree.insert(key(epoch, i), [b'v'; 64], u64::from(epoch * 2_000 + i));
        }
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(16 * 1_024, SeqNo::MAX)?;

    let epochs = table_epochs(&tree);
    assert!(epochs.iter().all(|(lo, hi)| lo == hi), "{epochs:?}");
    for epoch in 0..2 {
        assert!(
            epochs.iter().filter(|(lo, _)| *lo == epoch).count() > 1,
            "epoch {epoch} splits by size: {epochs:?}",
        );
    }
    Ok(())
}
