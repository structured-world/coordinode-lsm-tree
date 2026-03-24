// Tests for per-level Fs routing (tiered storage).
//
// Verifies that tables are written to the correct directory based on their
// destination level, and that recovery discovers tables across all paths.

use lsm_tree::{
    config::{CompressionPolicy, LevelRoute},
    fs::StdFs,
    AbstractTree, Config, SequenceNumberCounter,
};
use std::sync::Arc;

/// Helper: create a 3-tier config (hot L0-L1 / warm L2-L4 / cold L5-L6).
fn three_tier_config(base: &std::path::Path) -> Config {
    let hot = base.join("hot");
    let warm = base.join("warm");

    Config::new(
        base.join("primary"),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::None))
    .index_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::None))
    .level_routes(vec![
        LevelRoute {
            levels: 0..2,
            path: hot,
            fs: Arc::new(StdFs),
        },
        LevelRoute {
            levels: 2..5,
            path: warm,
            fs: Arc::new(StdFs),
        },
        // L5-L6: falls back to primary path
    ])
}

#[test]
fn flush_writes_to_hot_tier() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = three_tier_config(dir.path());
    let tree = config.open()?;

    tree.insert("a", "value_a", 0);
    tree.flush_active_memtable(0)?;

    // L0 flush → hot tier
    let hot_tables = dir.path().join("hot").join("tables");
    let files: Vec<_> = std::fs::read_dir(&hot_tables)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map_or(false, |n| n.parse::<u64>().is_ok())
        })
        .collect();

    assert!(
        !files.is_empty(),
        "expected table files in hot tier ({hot_tables:?}), found none"
    );

    // Primary tables folder should be empty (no L5-L6 tables yet)
    let primary_tables = dir.path().join("primary").join("tables");
    let primary_files: Vec<_> = std::fs::read_dir(&primary_tables)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map_or(false, |n| n.parse::<u64>().is_ok())
        })
        .collect();

    assert!(
        primary_files.is_empty(),
        "expected no table files in primary tier, found {}",
        primary_files.len()
    );

    Ok(())
}

#[test]
fn compaction_writes_to_correct_tier() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = three_tier_config(dir.path());
    let tree = config.open()?;

    // Insert enough data, flush to L0
    for i in 0u64..20 {
        tree.insert(format!("key{i:04}"), "x".repeat(100), i);
        if i % 4 == 3 {
            tree.flush_active_memtable(0)?;
        }
    }

    // Force compaction to last level (cold tier = primary, L6)
    tree.major_compact(u64::MAX, u64::MAX)?;

    // After major compaction, all tables should be at L6 (primary/cold tier)
    let primary_tables = dir.path().join("primary").join("tables");
    let primary_count = std::fs::read_dir(&primary_tables)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map_or(false, |n| n.parse::<u64>().is_ok())
        })
        .count();

    assert!(
        primary_count > 0,
        "expected table files in primary/cold tier after major compaction"
    );

    // Data should still be readable
    assert!(tree.get("key0000", lsm_tree::SeqNo::MAX)?.is_some());

    Ok(())
}

#[test]
fn recovery_discovers_tables_across_tiers() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // Phase 1: write data and close
    {
        let config = three_tier_config(dir.path());
        let tree = config.open()?;

        tree.insert("a", "value_a", 0);
        tree.insert("b", "value_b", 1);
        tree.flush_active_memtable(0)?;
    }

    // Phase 2: reopen with the same config and verify data
    {
        let config = three_tier_config(dir.path());
        let tree = config.open()?;

        assert_eq!(
            tree.get("a", lsm_tree::SeqNo::MAX)?.map(|v| v.to_vec()),
            Some(b"value_a".to_vec()),
        );
        assert_eq!(
            tree.get("b", lsm_tree::SeqNo::MAX)?.map(|v| v.to_vec()),
            Some(b"value_b".to_vec()),
        );
    }

    Ok(())
}

#[test]
fn no_overhead_without_level_routes() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // Config without level_routes — should work identically to before
    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    assert!(config.level_routes.is_none());

    let tree = config.open()?;
    tree.insert("a", "value_a", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        tree.get("a", lsm_tree::SeqNo::MAX)?.map(|v| v.to_vec()),
        Some(b"value_a".to_vec()),
    );

    Ok(())
}

#[test]
fn tables_folder_for_level_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let config = three_tier_config(dir.path());

    // L0 → hot tier
    let (folder, _) = config.tables_folder_for_level(0);
    assert_eq!(folder, dir.path().join("hot").join("tables"));

    // L1 → hot tier (0..2 includes 1)
    let (folder, _) = config.tables_folder_for_level(1);
    assert_eq!(folder, dir.path().join("hot").join("tables"));

    // L2 → warm tier
    let (folder, _) = config.tables_folder_for_level(2);
    assert_eq!(folder, dir.path().join("warm").join("tables"));

    // L4 → warm tier (2..5 includes 4)
    let (folder, _) = config.tables_folder_for_level(4);
    assert_eq!(folder, dir.path().join("warm").join("tables"));

    // L5 → primary (fallback, no route covers 5..7)
    let (folder, _) = config.tables_folder_for_level(5);
    assert_eq!(folder, dir.path().join("primary").join("tables"));

    // L6 → primary (fallback)
    let (folder, _) = config.tables_folder_for_level(6);
    assert_eq!(folder, dir.path().join("primary").join("tables"));
}

#[test]
fn all_tables_folders_deduplicates() {
    let dir = tempfile::tempdir().unwrap();
    let config = three_tier_config(dir.path());

    let folders = config.all_tables_folders();
    // primary + hot + warm = 3
    assert_eq!(folders.len(), 3);
}

#[test]
#[should_panic(expected = "overlapping level routes")]
fn overlapping_routes_panic() {
    let _config = Config::new(
        "/tmp/test",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .level_routes(vec![
        LevelRoute {
            levels: 0..3,
            path: "/a".into(),
            fs: Arc::new(StdFs),
        },
        LevelRoute {
            levels: 2..5, // overlaps with 0..3
            path: "/b".into(),
            fs: Arc::new(StdFs),
        },
    ]);
}
