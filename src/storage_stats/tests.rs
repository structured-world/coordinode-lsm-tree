use super::*;

fn stats_with_avg(avg_entry_on_disk_bytes: u64) -> StorageStats {
    StorageStats {
        used_bytes: 0,
        capacity_bytes: None,
        available_bytes: None,
        compaction_possible: true,
        full_compaction_bytes: 0,
        tight_compaction_bytes: 0,
        item_count: 0,
        table_count: 0,
        avg_entry_on_disk_bytes,
        avg_key_bytes: None,
        avg_value_bytes: None,
        reclaimable_bytes_estimate: 0,
        status: StorageStatus::Healthy,
    }
}

#[test]
fn estimated_remaining_entries_divides_budget_by_average() {
    // budget / avg_entry_on_disk: 1000 bytes at 50 bytes/entry = 20 entries.
    let stats = stats_with_avg(50);
    assert_eq!(stats.estimated_remaining_entries(1000), 20);
    // Partial entries round down (integer division).
    assert_eq!(stats.estimated_remaining_entries(1049), 20);
    assert_eq!(stats.estimated_remaining_entries(0), 0);
}

#[test]
fn estimated_remaining_entries_is_zero_when_average_is_unknown() {
    // An empty tree has no average to extrapolate from, so any budget
    // yields 0 rather than dividing by zero.
    let stats = stats_with_avg(0);
    assert_eq!(stats.estimated_remaining_entries(1_000_000), 0);
}

#[test]
fn compute_on_empty_version_maps_compaction_flag_to_status() {
    use crate::TreeType;
    use crate::version::Version;

    // An empty version has no tables, so no file is stat-ed: the call is
    // pure and exercises only the status mapping and the zero-table path.
    let version = Version::new(0, TreeType::Standard);

    #[expect(
        clippy::unwrap_used,
        reason = "compute_storage_stats cannot fail on an empty in-memory version (no file to stat)"
    )]
    let busy = compute_storage_stats(&version, true, true).unwrap();
    assert_eq!(busy.status, StorageStatus::CompactionInProgress);
    assert_eq!(busy.used_bytes, 0);
    assert_eq!(busy.item_count, 0);
    assert_eq!(busy.table_count, 0);
    assert_eq!(busy.avg_key_bytes, None);
    assert_eq!(busy.estimated_remaining_entries(1_000_000), 0);

    #[expect(
        clippy::unwrap_used,
        reason = "compute_storage_stats cannot fail on an empty in-memory version (no file to stat)"
    )]
    let idle = compute_storage_stats(&version, false, true).unwrap();
    assert_eq!(idle.status, StorageStatus::Healthy);
}

#[test]
fn storage_statistics_is_object_safe_via_mock() -> crate::Result<()> {
    // A non-tree mock implements the trait, proving it is object-safe and usable
    // for planner / tiering tests without a real engine behind it.
    struct MockStats;
    impl StorageStatistics for MockStats {
        fn storage_stats(&self) -> crate::Result<StorageStats> {
            Ok(stats_with_avg(6))
        }
        fn level_segment_stats(&self) -> crate::Result<Vec<LevelStats>> {
            Ok(Vec::new())
        }
        fn compaction_debt(&self, _strategy: &dyn crate::compaction::CompactionStrategy) -> u64 {
            123
        }
        #[cfg(feature = "metrics")]
        fn cache_stats(&self) -> crate::CacheStats {
            crate::CacheStats {
                hits: 9,
                misses: 1,
                hit_rate: 0.9,
                size_bytes: 10,
                capacity_bytes: 100,
            }
        }
    }

    let mock = MockStats;
    let stats: &dyn StorageStatistics = &mock;
    assert_eq!(stats.storage_stats()?.avg_entry_on_disk_bytes, 6);
    assert!(stats.level_segment_stats()?.is_empty());
    let strategy = crate::compaction::leveled::Strategy::default();
    assert_eq!(stats.compaction_debt(&strategy), 123);
    #[cfg(feature = "metrics")]
    assert_eq!(stats.cache_stats().hits, 9);
    Ok(())
}
