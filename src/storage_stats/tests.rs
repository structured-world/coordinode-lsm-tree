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

#[test]
fn storage_statistics_blanket_impl_over_real_tree() -> crate::Result<()> {
    // Drive the blanket `impl<T: AbstractTree> StorageStatistics for T` over a
    // real tree (the mock above bypasses it), and a non-leveled strategy through
    // the trait-default `pending_compaction_bytes` of 0.
    use crate::{AbstractTree, Config, SequenceNumberCounter, StorageStatistics};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..50u32 {
        tree.insert(format!("k{i:04}"), "v", 0);
    }
    tree.flush_active_memtable(0)?;

    let s: &dyn StorageStatistics = &tree;
    assert_eq!(s.storage_stats()?.item_count, 50);
    let level_items: u64 = s.level_segment_stats()?.iter().map(|l| l.item_count).sum();
    assert_eq!(level_items, 50);
    // FIFO has no size-target debt notion, so the trait default 0 applies.
    let fifo = crate::compaction::fifo::Strategy::new(u64::MAX, None);
    assert_eq!(s.compaction_debt(&fifo), 0);
    #[cfg(feature = "metrics")]
    assert!(s.cache_stats().capacity_bytes > 0);
    Ok(())
}

/// The per-level accounting shares the tree-level basis: a restricted
/// table's live `.restrict-bound` sidecar counts in `LevelStats` /
/// `SegmentStats` too, or summing the levels stops reconciling with the
/// documented SST portion of `StorageStats::used_bytes` and tiering
/// consumers understate the footprint.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn level_stats_count_a_restricted_tables_sidecar() -> crate::Result<()> {
    use crate::blob_tree::FragmentationMap;
    use crate::table::{Table, Writer};
    use crate::version::{BlobFileList, Level, Run, Version};
    use crate::{InternalValue, TreeType, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(crate::fs::StdFs);
    let sst = dir.path().join("0");

    // A multi-block SST so a restriction bound lands on a real block boundary.
    let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0..256u32 {
        w.write(InternalValue::from_components(
            format!("k{i:05}").into_bytes(),
            format!("v{i}").into_bytes(),
            u64::from(i) + 1,
            ValueType::Value,
        ))?;
    }
    let (_, checksum) = w.finish()?.expect("table written");

    let table = {
        let mut params = crate::table::RecoverParams::new(
            sst.clone(),
            checksum,
            0,
            Arc::clone(&fs),
            crate::comparator::default_comparator(),
            Arc::new(crate::Cache::with_capacity_bytes(1_000_000)),
        );
        params.descriptor_table = Some(Arc::new(crate::descriptor_table::DescriptorTable::new(10)));
        Table::recover(params)?
    };
    let restricted = table.reopen_restricted(b"k00100".to_vec().into())?;
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00100", crate::fs::SyncMode::Normal)?;

    let run = Arc::new(Run::new(vec![restricted]).expect("non-empty run"));
    let version = Version::from_levels(
        1,
        TreeType::Standard,
        vec![Level::from_runs(vec![run])],
        BlobFileList::default(),
        FragmentationMap::default(),
    );

    let levels = compute_level_segment_stats(&version)?;
    let sum: u64 = levels.iter().map(|l| l.used_bytes).sum();
    let expected: u64 = version
        .iter_tables()
        .map(table_on_disk_bytes)
        .sum::<crate::Result<u64>>()?;
    let sst_alone = fs.metadata(&sst)?.len;
    assert!(
        expected > sst_alone,
        "the fixture's live sidecar contributes bytes ({expected} vs {sst_alone})",
    );
    assert_eq!(
        sum, expected,
        "per-level and tree-level accounting share one sidecar-aware basis",
    );
    Ok(())
}

/// A tight-space compaction reclaims the consumed prefix of its input IN PLACE:
/// the blocks are gone from the device while `len` still reports the original
/// size. Charging the logical length keeps those freed bytes on the quota
/// forever: under `storage_limit_bytes` the headroom never recovers and the
/// tree stays read-only even though the compaction succeeded.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn a_punched_table_is_charged_its_allocated_bytes() -> crate::Result<()> {
    use crate::fs::{Fs, MemFs};
    use crate::table::{Table, Writer};
    use crate::{InternalValue, ValueType};
    use std::sync::Arc;

    let memfs = MemFs::new();
    let fs: Arc<dyn Fs> = Arc::new(memfs.clone());
    let dir = std::path::absolute("/punched_accounting")?;
    memfs.create_dir_all(&dir)?;
    let sst = dir.join("0");

    let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0..256u32 {
        w.write(InternalValue::from_components(
            format!("k{i:05}").into_bytes(),
            format!("v{i}").into_bytes(),
            u64::from(i) + 1,
            ValueType::Value,
        ))?;
    }
    let (_, checksum) = w.finish()?.expect("table written");

    let table = Table::recover(crate::table::RecoverParams::new(
        sst.clone(),
        checksum,
        0,
        Arc::clone(&fs),
        crate::comparator::default_comparator(),
        Arc::new(crate::Cache::with_capacity_bytes(1_000_000)),
    ))?;
    let bound: crate::UserKey = b"k00100".to_vec().into();
    let punch_at = table.punch_offset_for(&bound)?;
    assert!(punch_at > 0, "the bound must fall past the first block");
    let logical = fs.metadata(&sst)?.len;

    let restricted = table.reopen_restricted(bound)?;
    // Reclaim the consumed prefix, exactly as the tight-space pass does.
    memfs.punch_hole(&sst, 0, punch_at)?;

    let charged = table_on_disk_bytes(&restricted)?;
    assert!(
        charged < logical,
        "the reclaimed prefix must leave the quota: charged {charged}, logical {logical}",
    );
    assert_eq!(
        charged,
        logical - punch_at,
        "what is charged is what is still allocated",
    );
    Ok(())
}

/// A restricted table's `metadata.item_count` still describes the WHOLE
/// original SST, prefix included. While a tight-space slice is in flight the
/// version holds both that restricted input and the output that now owns the
/// prefix, so summing the raw metadata counts those entries twice: the reported
/// item count inflates, the average entry size shrinks, and
/// `estimated_remaining_entries` overstates capacity.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn a_restricted_table_contributes_only_its_live_entries() -> crate::Result<()> {
    use crate::blob_tree::FragmentationMap;
    use crate::table::{Table, Writer};
    use crate::version::{BlobFileList, Level, Run, Version};
    use crate::{InternalValue, TreeType, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(crate::fs::StdFs);
    let sst = dir.path().join("0");

    let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0..256u32 {
        w.write(InternalValue::from_components(
            format!("k{i:05}").into_bytes(),
            format!("v{i}").into_bytes(),
            u64::from(i) + 1,
            ValueType::Value,
        ))?;
    }
    let (_, checksum) = w.finish()?.expect("table written");

    let table = Table::recover(crate::table::RecoverParams::new(
        sst,
        checksum,
        0,
        Arc::clone(&fs),
        crate::comparator::default_comparator(),
        Arc::new(crate::Cache::with_capacity_bytes(1_000_000)),
    ))?;
    let whole = table.metadata.item_count;
    assert_eq!(whole, 256, "the fixture holds every entry");

    // Restrict away roughly the first half, as a completed slice would.
    let restricted = table.reopen_restricted(b"k00128".to_vec().into())?;
    let run = Arc::new(Run::new(vec![restricted]).expect("non-empty run"));
    let version = Version::from_levels(
        1,
        TreeType::Standard,
        vec![Level::from_runs(vec![run])],
        BlobFileList::default(),
        FragmentationMap::default(),
    );

    let stats = compute_storage_stats(&version, false, true)?;
    assert!(
        stats.item_count < whole,
        "the consumed prefix must not be counted: reported {} of {whole}",
        stats.item_count,
    );
    assert!(
        stats.item_count > whole / 4,
        "the live suffix must still be counted: reported {} of {whole}",
        stats.item_count,
    );
    Ok(())
}
