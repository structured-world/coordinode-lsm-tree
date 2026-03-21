use lsm_tree::{
    config::PinningPolicy, AbstractTree, Config, Guard, KvSeparationOptions, PrefixExtractor,
    SequenceNumberCounter, Tree,
};
use std::sync::Arc;

/// Extracts prefixes at each ':' separator boundary.
struct ColonSeparatedPrefix;

impl PrefixExtractor for ColonSeparatedPrefix {
    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(
            key.iter()
                .enumerate()
                .filter(|(_, b)| **b == b':')
                .map(move |(i, _)| &key[..=i]),
        )
    }
}

fn tree_with_prefix_bloom(folder: &tempfile::TempDir) -> lsm_tree::Result<Tree> {
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(ColonSeparatedPrefix))
    .open()?;

    match tree {
        lsm_tree::AnyTree::Standard(t) => Ok(t),
        _ => panic!("expected standard tree"),
    }
}

#[test]
fn prefix_bloom_basic_prefix_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Insert keys with different prefixes
    tree.insert("user:1:name", "Alice", 0);
    tree.insert("user:1:email", "alice@example.com", 1);
    tree.insert("user:2:name", "Bob", 2);
    tree.insert("order:1:item", "widget", 3);
    tree.insert("order:2:item", "gadget", 4);

    // Flush to create SST with prefix bloom
    tree.flush_active_memtable(0)?;

    // Prefix scan should find matching keys
    let results: Vec<_> = tree
        .create_prefix("user:1:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.as_ref(), b"user:1:email");
    assert_eq!(results[1].0.as_ref(), b"user:1:name");

    // Different prefix
    let results: Vec<_> = tree
        .create_prefix("order:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    // Non-existent prefix
    let results: Vec<_> = tree
        .create_prefix("nonexist:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[test]
fn prefix_bloom_skips_segments() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Create first segment with "user:" prefix keys
    tree.insert("user:1:name", "Alice", 0);
    tree.insert("user:2:name", "Bob", 1);
    tree.flush_active_memtable(0)?;

    // Create second segment with "order:" prefix keys
    tree.insert("order:1:item", "widget", 2);
    tree.insert("order:2:item", "gadget", 3);
    tree.flush_active_memtable(0)?;

    assert!(tree.table_count() >= 2, "expected at least 2 segments");

    // Prefix scan for "user:" should return correct results
    // and skip the "order:" segment via prefix bloom
    let results: Vec<_> = tree
        .create_prefix("user:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.as_ref(), b"user:1:name");
    assert_eq!(results[1].0.as_ref(), b"user:2:name");

    // Prefix scan for "order:" should return correct results
    // and skip the "user:" segment via prefix bloom
    let results: Vec<_> = tree
        .create_prefix("order:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    Ok(())
}

#[test]
fn prefix_bloom_after_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Create data across multiple flushes
    tree.insert("a:1", "v1", 0);
    tree.insert("b:1", "v2", 1);
    tree.flush_active_memtable(0)?;

    tree.insert("a:2", "v3", 2);
    tree.insert("c:1", "v4", 3);
    tree.flush_active_memtable(0)?;

    // Compact everything
    tree.major_compact(u64::MAX, 0)?;

    // Prefix scan still works after compaction
    let results: Vec<_> = tree
        .create_prefix("a:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.as_ref(), b"a:1");
    assert_eq!(results[1].0.as_ref(), b"a:2");

    Ok(())
}

#[test]
fn prefix_bloom_without_extractor_still_works() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Tree without prefix extractor — prefix scan still works, just no bloom skipping
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    match &tree {
        lsm_tree::AnyTree::Standard(t) => {
            t.insert("user:1:name", "Alice", 0);
            t.insert("user:2:name", "Bob", 1);
            t.flush_active_memtable(0)?;

            let results: Vec<_> = t
                .create_prefix("user:", 2, None)
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(results.len(), 2);
        }
        _ => panic!("expected standard tree"),
    }

    Ok(())
}

#[test]
fn prefix_bloom_hierarchical_prefixes() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Insert keys with hierarchical prefixes
    tree.insert("adj:out:42:KNOWS", "target1", 0);
    tree.insert("adj:out:42:LIKES", "target2", 1);
    tree.insert("adj:out:99:KNOWS", "target3", 2);
    tree.insert("adj:in:42:KNOWS", "source1", 3);
    tree.insert("node:42", "properties", 4);
    tree.flush_active_memtable(0)?;

    // Scan at different prefix levels
    // "adj:" matches all adjacency keys
    let results: Vec<_> = tree
        .create_prefix("adj:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 4);

    // "adj:out:" matches outgoing adjacency
    let results: Vec<_> = tree
        .create_prefix("adj:out:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 3);

    // "adj:out:42:" matches specific node's outgoing edges
    let results: Vec<_> = tree
        .create_prefix("adj:out:42:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    // "node:" matches node properties
    let results: Vec<_> = tree
        .create_prefix("node:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 1);

    Ok(())
}

#[test]
fn prefix_bloom_with_memtable_and_disk() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Write some data to disk
    tree.insert("x:1", "disk_val", 0);
    tree.insert("y:1", "disk_val", 1);
    tree.flush_active_memtable(0)?;

    // Write more to memtable (not flushed)
    tree.insert("x:2", "mem_val", 2);
    tree.insert("z:1", "mem_val", 3);

    // Prefix scan should find both disk and memtable results
    let results: Vec<_> = tree
        .create_prefix("x:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.as_ref(), b"x:1");
    assert_eq!(results[1].0.as_ref(), b"x:2");

    Ok(())
}

#[test]
fn prefix_bloom_unpinned_filter() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree with unpinned filters to exercise the fallback load path
    // in Table::maybe_contains_prefix
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(ColonSeparatedPrefix))
    .filter_block_pinning_policy(PinningPolicy::all(false))
    .open()?;

    let tree = match tree {
        lsm_tree::AnyTree::Standard(t) => t,
        _ => panic!("expected standard tree"),
    };

    tree.insert("a:1", "v1", 0);
    tree.insert("b:1", "v2", 1);
    tree.flush_active_memtable(0)?;

    tree.insert("c:1", "v3", 2);
    tree.insert("d:1", "v4", 3);
    tree.flush_active_memtable(0)?;

    // Prefix scan on unpinned filters exercises the load_block fallback
    let results: Vec<_> = tree
        .create_prefix("a:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0.as_ref(), b"a:1");

    // Non-matching prefix should be skipped via unpinned bloom
    let results: Vec<_> = tree
        .create_prefix("z:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[test]
fn prefix_bloom_blob_tree() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create BlobTree with prefix extractor to exercise BlobTree::prefix path
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(ColonSeparatedPrefix))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    tree.insert("user:1:name", "Alice", 0);
    tree.insert("user:2:name", "Bob", 1);
    tree.insert("order:1:item", "widget", 2);
    tree.flush_active_memtable(0)?;

    // Prefix scan through BlobTree — collect results to surface I/O errors
    let results: Vec<_> = tree
        .prefix("user:", 3, None)
        .map(|g| g.key())
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    let results: Vec<_> = tree
        .prefix("order:", 3, None)
        .map(|g| g.key())
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 1);

    let results: Vec<_> = tree
        .prefix("nonexist:", 3, None)
        .map(|g| g.key())
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[test]
fn prefix_bloom_many_disjoint_segments() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Create 10 segments each with a unique prefix — exercises the bloom skip
    // path repeatedly across many single-table runs
    for i in 0u64..10 {
        let key = format!("ns{i}:key");
        tree.insert(key, "value", i);
        tree.flush_active_memtable(0)?;
    }

    assert!(tree.table_count() >= 10);

    // Each prefix scan should find exactly 1 result and skip 9 segments
    for i in 0u64..10 {
        let prefix = format!("ns{i}:");
        let results: Vec<_> = tree
            .create_prefix(&prefix, 10, None)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            results.len(),
            1,
            "prefix {prefix} should match exactly 1 key"
        );
    }

    // A prefix that doesn't exist should match nothing
    let results: Vec<_> = tree
        .create_prefix("nonexist:", 10, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[test]
fn prefix_bloom_skip_on_compacted_levels() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Create disjoint segments and compact to L1+ where single-table runs
    // enable the prefix bloom skip path (Ok(false) branch in range.rs).
    // L0 tables are multi-table runs where bloom is not checked.

    // Batch 1: keys in "aaa:" prefix space
    for i in 0u64..50 {
        let key = format!("aaa:{i:04}");
        tree.insert(key, "v", i);
    }
    tree.flush_active_memtable(0)?;

    // Batch 2: keys in "zzz:" prefix space (disjoint from batch 1)
    for i in 50u64..100 {
        let key = format!("zzz:{i:04}");
        tree.insert(key, "v", i);
    }
    tree.flush_active_memtable(0)?;

    // Compact to move tables from L0 (multi-run) to L1+ (single-table runs)
    // Use small target size to keep tables disjoint in L1
    tree.major_compact(u64::MAX, 0)?;

    // Now tables are in L1+ as single-table runs. Prefix scan for "aaa:"
    // should skip the "zzz:" table via bloom filter.
    let results: Vec<_> = tree
        .create_prefix("aaa:", 100, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 50);

    // And vice versa
    let results: Vec<_> = tree
        .create_prefix("zzz:", 100, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 50);

    // Non-existent prefix
    let results: Vec<_> = tree
        .create_prefix("mmm:", 100, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[test]
fn prefix_bloom_non_boundary_prefix_no_false_negative() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // The ColonSeparatedPrefix extractor indexes "adj:", "adj:out:", etc.
    // A scan for "adj" (no trailing colon) is NOT an extractor boundary,
    // so prefix bloom must NOT be used (it would cause false negatives).
    // The scan must still return correct results via key-range filtering.
    tree.insert("adj:out:1", "v1", 0);
    tree.insert("adj:in:1", "v2", 1);
    tree.insert("other:1", "v3", 2);
    tree.flush_active_memtable(0)?;

    // "adj" is not a colon-terminated boundary — bloom skip must be disabled
    let results: Vec<_> = tree
        .create_prefix("adj", 3, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        results.len(),
        2,
        "must find keys even though 'adj' is not an extractor boundary"
    );

    // "adj:" IS a valid boundary — bloom skip can be used safely
    let results: Vec<_> = tree
        .create_prefix("adj:", 3, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    Ok(())
}
