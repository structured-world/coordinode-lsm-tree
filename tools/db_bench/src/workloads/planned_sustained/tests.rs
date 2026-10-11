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
/// it reaches the tables, and its runs, debt and queries are sampled as any
/// step's.
#[test]
fn a_stream_shorter_than_one_flush_is_one_measured_step() -> lsm_tree::Result<()> {
    let base = tempfile::tempdir()?;
    let (planned, leveled) =
        super::run_arms(base.path(), &config(1_000), super::COMPACTION_RATES[0])?;
    for arm in [&planned, &leveled] {
        assert!(arm.written > 0);
        assert_eq!(arm.steps, 1, "the writes are one step");
        assert_eq!(arm.points.len(), super::POINTS_PER_FLUSH);
        assert_eq!(arm.scans.len(), super::SCANS_PER_FLUSH);
        assert!(arm.runs_peak >= 1, "the flush left a run");
    }
    Ok(())
}

/// Several steps keep sampling the queries each step asks for, not more, at
/// every compaction budget the workload runs.
#[test]
fn every_step_takes_its_share_of_queries() -> lsm_tree::Result<()> {
    for compaction_rate in super::COMPACTION_RATES {
        let base = tempfile::tempdir()?;
        let (planned, leveled) = super::run_arms(base.path(), &config(30_000), compaction_rate)?;
        for arm in [&planned, &leveled] {
            assert!(arm.steps > 1, "{} steps", arm.steps);
            assert_eq!(arm.points.len(), super::POINTS_PER_FLUSH * arm.steps);
            assert_eq!(arm.scans.len(), super::SCANS_PER_FLUSH * arm.steps);
        }
    }
    Ok(())
}

/// Both trees are built beneath the directory the run was given, the device
/// `--db` asked for, and removed with the run.
#[test]
fn the_trees_are_built_beneath_the_run_directory() -> lsm_tree::Result<()> {
    let base = tempfile::tempdir()?;
    let rate = super::COMPACTION_RATES[0];
    let arm = super::ArmRun::open(base.path(), &config(1_000), super::planned(rate), rate)?;
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

/// A blob tree is refused by name: compaction bytes are counted in tables.
#[test]
fn a_blob_tree_is_refused() {
    let blob = BenchConfig {
        use_blob_tree: true,
        ..config(1_000)
    };
    let refused = super::PlannedSustained.check_config(&blob);
    assert!(
        refused
            .as_ref()
            .is_err_and(|e| e.contains("--use-blob-tree")),
        "{refused:?}"
    );
    assert_eq!(super::PlannedSustained.check_config(&config(1_000)), Ok(()));
}
