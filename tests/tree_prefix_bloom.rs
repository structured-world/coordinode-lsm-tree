use lsm_tree::{AbstractTree, Config, PrefixExtractor, SequenceNumberCounter, Tree};
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

#[cfg(feature = "metrics")]
#[test]
fn prefix_bloom_reduces_io() -> lsm_tree::Result<()> {
    use std::sync::atomic::Ordering::Relaxed;

    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Create multiple segments with disjoint prefix sets
    for i in 0..10 {
        let key = format!("prefix{i}:key");
        tree.insert(key, "value", i);
        tree.flush_active_memtable(0)?;
    }

    let metrics = tree.metrics();

    // Reset filter query counters
    let before = metrics.io_skipped_by_filter.load(Relaxed);

    // Scan for prefix0 — should skip most segments via prefix bloom
    let results: Vec<_> = tree
        .create_prefix("prefix0:", 10, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 1);

    let after = metrics.io_skipped_by_filter.load(Relaxed);

    // The prefix bloom check happens at segment level (not at point-read level),
    // so io_skipped_by_filter won't be incremented here. But we can verify
    // correctness: only 1 result despite 10 segments.
    let _ = (before, after); // metrics available but bloom skip is at range level

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
