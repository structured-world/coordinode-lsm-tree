use crate::config::{BenchConfig, Compression};
use crate::workloads::Workload;
use lsm_tree::{AbstractTree, SeqNo};

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

/// A stream shorter than one flush still reaches the tables: the writes the
/// ratio counts in its denominator are the ones compaction saw.
#[test]
fn a_stream_shorter_than_one_flush_reaches_the_tables() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let config = config(1_000);
    let arm = super::run_arm(folder.path(), &config, true)?;
    assert!(arm.written > 0);
    let tree = crate::config::tree_builder(folder.path(), &config)?.open()?;
    assert!(
        tree.table_count() > 0,
        "{} bytes written and none flushed",
        arm.written
    );
    // Reopened, the tree serves the keys from its tables alone.
    assert!(tree.len(SeqNo::MAX, None)? > 0);
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
