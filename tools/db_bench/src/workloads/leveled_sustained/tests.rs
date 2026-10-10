use crate::config::{BenchConfig, Compression};
use crate::workloads::Workload;
use lsm_tree::AbstractTree;

fn config(num: u64) -> BenchConfig {
    BenchConfig {
        num,
        key_size: 16,
        value_size: 100,
        threads: 1,
        cache_mb: 8,
        compression: Compression::None,
        block_size: 4_096,
        row_group_size: lsm_tree::config::DEFAULT_COLUMNAR_ROW_GROUP_SIZE,
        page_size: lsm_tree::config::DEFAULT_COLUMNAR_PAGE_SIZE,
        column_encoding: crate::config::ColumnEncodingArg::Plain,
        read_budget: lsm_tree::config::ReadBudget::default(),
        use_blob_tree: false,
        metadata_priority: true,
        partition_metadata: false,
    }
}

/// A stream shorter than one flush is still one measured step for each tree:
/// it reaches the tables, and its debt and reads are sampled as any step's.
#[test]
fn a_stream_shorter_than_one_flush_is_one_measured_step() -> lsm_tree::Result<()> {
    let base = tempfile::tempdir()?;
    let (aligned, size_only) = super::run_arms(base.path(), &config(1_000))?;
    for arm in [&aligned, &size_only] {
        assert!(arm.written > 0);
        assert_eq!(arm.flushes, 1, "the writes are one step");
        assert_eq!(arm.steps.len(), 1);
        assert_eq!(arm.reads.len(), super::READS_PER_FLUSH);
    }
    Ok(())
}

/// Both trees are built beneath the directory the run was given, the device
/// `--db` asked for, and removed with the run.
#[test]
fn the_trees_are_built_beneath_the_run_directory() -> lsm_tree::Result<()> {
    let base = tempfile::tempdir()?;
    let arm = super::ArmRun::open(base.path(), &config(1_000), true)?;
    assert!(
        arm.tree.tree_config().path.starts_with(base.path()),
        "built at {}",
        arm.tree.tree_config().path.display()
    );
    drop(arm);
    assert_eq!(
        std::fs::read_dir(base.path())?.count(),
        0,
        "removed with the run"
    );
    Ok(())
}

/// A blob tree is refused by name: compaction bytes are counted in tables,
/// and a run reported under it would measure a standard tree.
#[test]
fn a_blob_tree_is_refused() {
    let blob = BenchConfig {
        use_blob_tree: true,
        ..config(1_000)
    };
    let refused = super::LeveledSustained.check_config(&blob);
    assert!(
        refused
            .as_ref()
            .is_err_and(|e| e.contains("--use-blob-tree")),
        "{refused:?}"
    );
    assert_eq!(super::LeveledSustained.check_config(&config(1_000)), Ok(()));
}
