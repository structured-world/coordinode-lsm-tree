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

/// The tables holding rows that reach past their rows' epoch: rows of two
/// epochs, or a key range, widened to the range tombstones a table holds,
/// that starts in another epoch or ends past the next one's first key. Such a
/// table is not removed whole when its epoch expires. A table of range
/// tombstones alone may span epochs with no row in it.
fn tables_past_their_epoch(tree: &AnyTree) -> lsm_tree::Result<Vec<String>> {
    let mut found = Vec::new();
    for table in tree.current_version().iter_tables() {
        let mut epochs = Vec::new();
        for item in table.iter() {
            let item = item?;
            if item.key.value_type == lsm_tree::ValueType::Value {
                epochs.push(epoch_of(&item.key.user_key));
            }
        }
        epochs.dedup();
        let range = &table.metadata.key_range;
        let (min, max) = (range.min().to_vec(), range.max().to_vec());
        let within = match epochs.as_slice() {
            [] => true,
            [epoch] => {
                epoch_of(&min) == *epoch
                    && (epoch_of(&max) == *epoch || max == (epoch + 1).to_be_bytes())
            }
            _ => false,
        };
        if !within {
            found.push(format!(
                "rows of epochs {epochs:?} in key range {min:?}..={max:?}"
            ));
        }
    }
    Ok(found)
}

/// A range tombstone across several epochs, flushed with rows of some of
/// them, leaves every table holding rows within its rows' epoch: a flush
/// widens a table's key range to the pieces it holds, so the pieces past an
/// epoch go to tables of tombstones alone. Rows older than it stay deleted,
/// newer ones read back.
#[test]
fn a_flushed_tombstone_across_domains_keeps_tables_in_their_epoch() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), Some(Arc::new(Epochs)))?;
    for epoch in [1, 3] {
        for i in 0..50 {
            tree.insert(key(epoch, i), b"old", u64::from(epoch * 100 + i));
        }
    }
    // From the middle of epoch 0 to the middle of epoch 4, over every row.
    tree.remove_range(key(0, 25), key(4, 25), 1_000);
    for epoch in [1, 3] {
        tree.insert(key(epoch, 7), b"new", 2_000 + u64::from(epoch));
    }
    tree.flush_active_memtable(0)?;

    let past = tables_past_their_epoch(&tree)?;
    assert!(past.is_empty(), "{past:#?}");
    let rows = latest(&tree)?;
    assert_eq!(
        rows,
        vec![(key(1, 7), b"new".to_vec()), (key(3, 7), b"new".to_vec()),],
        "only the rows newer than the tombstone read back",
    );
    assert!(tree.get(key(1, 8), SeqNo::MAX)?.is_none(), "point read too");
    assert!(
        tree.get(key(3, 49), SeqNo::MAX)?.is_none(),
        "point read too"
    );
    Ok(())
}

/// The same through compaction, with the tombstone kept (a watermark of 0)
/// and ending past epoch 1 but before epoch 2's first row: a compaction
/// widens an output's key range up to the next output's first key to cover a
/// tombstone reaching into that gap, which would take epoch 1's table into
/// epoch 2. Every compacted table holding rows stays within their epoch, and
/// the deleted rows stay deleted.
#[test]
fn a_compacted_tombstone_across_domains_keeps_tables_in_their_epoch() -> lsm_tree::Result<()> {
    // Epoch 2's rows start at index 5.
    let first = |epoch: u32| if epoch == 2 { 5 } else { 0 };
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), Some(Arc::new(Epochs)))?;
    for epoch in 0..5 {
        for i in first(epoch)..50 {
            tree.insert(key(epoch, i), b"old", u64::from(epoch * 100 + i));
        }
    }
    tree.flush_active_memtable(0)?;
    tree.remove_range(key(1, 10), key(2, 3), 1_000);
    tree.flush_active_memtable(0)?;
    for i in 0..50 {
        tree.insert(key(5, i), b"new", 2_000 + u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;

    let past = tables_past_their_epoch(&tree)?;
    assert!(past.is_empty(), "{past:#?}");
    let deleted = |e: u32, i: u32| (e, i) >= (1, 10) && (e, i) < (2, 3);
    let rows = latest(&tree)?;
    let expected: Vec<(Vec<u8>, Vec<u8>)> = (0..6)
        .flat_map(|e| (first(e)..50).map(move |i| (e, i)))
        .filter(|&(e, i)| !deleted(e, i))
        .map(|(e, i)| {
            (
                key(e, i),
                if e == 5 {
                    b"new".to_vec()
                } else {
                    b"old".to_vec()
                },
            )
        })
        .collect();
    assert_eq!(
        rows, expected,
        "every row outside the tombstone and none in it"
    );
    assert!(
        tree.get(key(1, 20), SeqNo::MAX)?.is_none(),
        "point read too"
    );
    assert!(
        tree.get(key(2, 5), SeqNo::MAX)?.is_some(),
        "past the tombstone"
    );
    Ok(())
}

/// A key-value separated tree flushing three epochs writes their separated
/// values to a blob file per epoch, as it writes their keys to a table per
/// epoch, and every value reads back.
#[test]
fn separated_values_land_in_a_blob_file_per_domain() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        lsm_tree::KvSeparationOptions::default().separation_threshold(16),
    ))
    .key_domains(Arc::new(Epochs))
    .open()?;
    for epoch in 0..3 {
        for i in 0..20 {
            tree.insert(key(epoch, i), [b'v'; 64], u64::from(epoch * 20 + i));
        }
    }
    tree.flush_active_memtable(0)?;

    let mut epochs = table_epochs(&tree);
    epochs.sort_unstable();
    assert_eq!(epochs, vec![(0, 0), (1, 1), (2, 2)], "a table per epoch");
    assert_eq!(
        tree.current_version().blob_files.len(),
        3,
        "a blob file per epoch",
    );
    let rows = latest(&tree)?;
    assert_eq!(rows.len(), 60);
    assert!(rows.iter().all(|(_, v)| v.as_slice() == [b'v'; 64]));
    Ok(())
}

/// Rows ingested across three epochs land in a table per epoch.
#[test]
fn ingest_ends_a_table_at_each_domain_end() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open(folder.path(), Some(Arc::new(Epochs)))?;
    let mut ingest = tree.ingestion()?;
    for epoch in 0..3 {
        for i in 0..20 {
            ingest.write(key(epoch, i), b"v")?;
        }
    }
    ingest.finish()?;

    let mut epochs = table_epochs(&tree);
    epochs.sort_unstable();
    assert_eq!(epochs, vec![(0, 0), (1, 1), (2, 2)], "a table per epoch");
    assert_eq!(latest(&tree)?.len(), 60, "every key reads back");
    Ok(())
}

/// One columnar batch whose keys reach across three epochs is written as a
/// block per epoch, each in a table of that epoch.
#[cfg(feature = "columnar")]
#[test]
fn a_columnar_batch_across_domains_lands_in_a_table_per_domain() -> lsm_tree::Result<()> {
    use lsm_tree::table::columnar::entries_to_column_batch;
    use lsm_tree::{InternalValue, ValueType};

    let folder = tempfile::tempdir()?;
    let any = open(folder.path(), Some(Arc::new(Epochs)))?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)?;
    let rows: Vec<InternalValue> = (0..3)
        .flat_map(|epoch| (0..20).map(move |i| key(epoch, i)))
        .map(|k| InternalValue::from_components(k, b"v", 0, ValueType::Value))
        .collect();
    let batch = entries_to_column_batch(&rows)?;

    let mut ingest = any.ingestion()?;
    ingest.write_columnar_batch(&batch)?;
    ingest.finish()?;

    let mut epochs = table_epochs(&any);
    epochs.sort_unstable();
    assert_eq!(epochs, vec![(0, 0), (1, 1), (2, 2)], "a table per epoch");
    assert_eq!(latest(&any)?.len(), 60, "every key reads back");
    Ok(())
}

/// A lone columnar table whose own range tombstone deletes some of its rows
/// is relocated with a delete bitmap, its blocks and key range kept as they
/// were. A table written before the policy may hold two domains: compacting
/// it under the policy rewrites it into a table per domain instead.
#[cfg(feature = "columnar")]
#[test]
fn a_table_spanning_domains_is_rewritten_not_relocated() -> lsm_tree::Result<()> {
    use lsm_tree::config::{DeleteStrategy, DeleteStrategyPolicy};

    let merge_on_read = |any: &AnyTree| -> lsm_tree::Result<()> {
        let AnyTree::Standard(tree) = any else {
            panic!("expected a standard tree");
        };
        tree.update_runtime_config(|cfg| {
            cfg.columnar = true;
            cfg.zone_map = true;
            cfg.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::MergeOnRead);
        })
    };
    let folder = tempfile::tempdir()?;
    {
        let tree = open(folder.path(), None)?;
        merge_on_read(&tree)?;
        for epoch in 0..2 {
            for i in 0..100 {
                tree.insert(key(epoch, i), b"v", u64::from(epoch * 100 + i));
            }
        }
        // The table's own tombstone, deleting a few rows of epoch 0.
        tree.remove_range(key(0, 0), key(0, 10), 1_000);
        tree.flush_active_memtable(0)?;
        assert_eq!(
            table_epochs(&tree),
            vec![(0, 1)],
            "self-check: one table over both epochs"
        );
    }

    let tree = open(folder.path(), Some(Arc::new(Epochs)))?;
    merge_on_read(&tree)?;
    tree.major_compact(u64::MAX, 5_000)?;

    let mut epochs = table_epochs(&tree);
    epochs.sort_unstable();
    assert_eq!(epochs, vec![(0, 0), (1, 1)], "a table per epoch");
    assert_eq!(latest(&tree)?.len(), 190, "the deleted rows read as absent");
    Ok(())
}

/// The same for a lone columnar table whose rows lie in one domain and whose
/// own range tombstone reaches into the next: relocated, its key range would
/// keep the tombstone's end past the domain. It is rewritten instead, the
/// pieces past the domain going to a table of tombstones alone.
#[cfg(feature = "columnar")]
#[test]
fn a_table_whose_tombstone_leaves_its_domain_is_rewritten_not_relocated() -> lsm_tree::Result<()> {
    use lsm_tree::config::{DeleteStrategy, DeleteStrategyPolicy};

    let merge_on_read = |any: &AnyTree| -> lsm_tree::Result<()> {
        let AnyTree::Standard(tree) = any else {
            panic!("expected a standard tree");
        };
        tree.update_runtime_config(|cfg| {
            cfg.columnar = true;
            cfg.zone_map = true;
            cfg.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::MergeOnRead);
        })
    };
    let folder = tempfile::tempdir()?;
    {
        let tree = open(folder.path(), None)?;
        merge_on_read(&tree)?;
        for i in 0..100 {
            tree.insert(key(0, i), b"v", u64::from(i));
        }
        // Deleting the last rows of epoch 0 and reaching into epoch 1.
        tree.remove_range(key(0, 90), key(1, 5), 1_000);
        tree.flush_active_memtable(0)?;
        assert_eq!(
            table_epochs(&tree),
            vec![(0, 1)],
            "self-check: the tombstone takes the table's key range into epoch 1"
        );
    }

    let tree = open(folder.path(), Some(Arc::new(Epochs)))?;
    merge_on_read(&tree)?;
    tree.major_compact(u64::MAX, 5_000)?;

    let past = tables_past_their_epoch(&tree)?;
    assert!(past.is_empty(), "{past:#?}");
    let keys: Vec<Vec<u8>> = latest(&tree)?.into_iter().map(|(k, _)| k).collect();
    let expected: Vec<Vec<u8>> = (0..90).map(|i| key(0, i)).collect();
    assert_eq!(keys, expected, "rows 90 to 99 deleted, the rest kept");
    Ok(())
}

/// Under the policy a lone columnar table whose rows and own range tombstone
/// lie within one domain is still relocated with a delete bitmap: the policy
/// refuses only the sources that leave their domain.
#[cfg(feature = "columnar")]
#[test]
fn a_table_within_its_domain_is_still_relocated() -> lsm_tree::Result<()> {
    use lsm_tree::config::{DeleteStrategy, DeleteStrategyPolicy};

    let folder = tempfile::tempdir()?;
    let any = open(folder.path(), Some(Arc::new(Epochs)))?;
    let AnyTree::Standard(tree) = &any else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::MergeOnRead);
    })?;
    for i in 0..100 {
        any.insert(key(0, i), b"v", u64::from(i));
    }
    any.remove_range(key(0, 90), key(0, 95), 1_000);
    any.flush_active_memtable(0)?;
    any.major_compact(u64::MAX, 5_000)?;

    let version = any.current_version();
    let tables: Vec<_> = version.iter_tables().collect();
    assert_eq!(tables.len(), 1);
    assert!(
        tables.iter().all(|t| !t.delete_bitmap().is_empty()),
        "relocated with a delete bitmap, not rewritten"
    );
    let keys: Vec<Vec<u8>> = latest(&any)?.into_iter().map(|(k, _)| k).collect();
    let expected: Vec<Vec<u8>> = (0..90).chain(95..100).map(|i| key(0, i)).collect();
    assert_eq!(keys, expected);
    Ok(())
}
