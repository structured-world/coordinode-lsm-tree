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

const EVEN: super::ReadMix = super::MIXES[0];

/// A stream shorter than one flush is still one measured step for each tree:
/// it reaches the tables, and its runs, debt and queries are sampled as any
/// step's.
#[test]
fn a_stream_shorter_than_one_flush_is_one_measured_step() -> lsm_tree::Result<()> {
    let base = tempfile::tempdir()?;
    let arms = super::run_arms(
        base.path(),
        &config(1_000),
        EVEN,
        super::COMPACTION_RATES[0],
        0,
    )?;
    for arm in &arms {
        assert!(arm.written > 0);
        assert_eq!(arm.steps, 1, "the writes are one step");
        assert_eq!(arm.points.len(), EVEN.points);
        assert_eq!(arm.scans.len(), EVEN.scans);
        assert!(arm.runs_peak >= 1, "the flush left a run");
    }
    Ok(())
}

/// Several steps keep sampling the queries each step of the mix asks for, not
/// more, for every mix and compaction budget the workload runs, whichever tree
/// drains first.
#[test]
fn every_step_takes_its_share_of_queries() -> lsm_tree::Result<()> {
    let mut run = 0;
    for mix in super::MIXES {
        for compaction_rate in super::COMPACTION_RATES {
            let base = tempfile::tempdir()?;
            let arms = super::run_arms(
                base.path(),
                &config(30_000),
                mix,
                compaction_rate,
                run % super::Kind::ALL.len(),
            )?;
            run += 1;
            for arm in &arms {
                assert!(arm.steps > 1, "{}: {} steps", mix.name, arm.steps);
                assert_eq!(arm.points.len(), mix.points * arm.steps, "{}", mix.name);
                assert_eq!(arm.scans.len(), mix.scans * arm.steps, "{}", mix.name);
            }
        }
    }
    Ok(())
}

/// Every tree is built beneath the directory the run was given, the device
/// `--db` asked for, and removed with the run.
#[test]
fn the_trees_are_built_beneath_the_run_directory() -> lsm_tree::Result<()> {
    let base = tempfile::tempdir()?;
    let rate = super::COMPACTION_RATES[0];
    for kind in super::Kind::ALL {
        let arm = super::ArmRun::open(base.path(), &config(1_000), kind, EVEN, rate)?;
        assert!(
            arm.tree.tree_config().path.starts_with(base.path()),
            "built at {}",
            arm.tree.tree_config().path.display()
        );
        drop(arm);
    }
    assert_eq!(
        std::fs::read_dir(base.path())?.count(),
        0,
        "removed with the run"
    );
    Ok(())
}

/// A skewed mix reads keys within the key space, and fewer distinct ones than
/// an even mix does from the same number of draws.
#[test]
fn a_skewed_mix_reads_a_hot_set_within_the_key_space() -> lsm_tree::Result<()> {
    let base = tempfile::tempdir()?;
    let rate = super::COMPACTION_RATES[0];
    let keys = 10_000;
    let skewed = super::MIXES
        .into_iter()
        .find(|mix| mix.skewed)
        .expect("a skewed mix");
    let mut distinct = Vec::new();
    for mix in [EVEN, skewed] {
        let mut arm =
            super::ArmRun::open(base.path(), &config(1_000), super::Kind::Planned, mix, rate)?;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..5_000 {
            let key = arm.lookup_key(keys, mix);
            assert!(key < keys, "{key} out of {keys}");
            seen.insert(key);
        }
        distinct.push(seen.len());
    }
    assert!(
        distinct[1] * 2 < distinct[0],
        "skewed reads met {} keys, even ones {}",
        distinct[1],
        distinct[0]
    );
    Ok(())
}

/// A run of one step observes no merge and too few steps for a scan's cost
/// per run: the calibrated model keeps the prior's merge rate, the share
/// queries keep beside a merge, and the prior's scan weight.
#[test]
fn a_short_run_calibrates_nothing_it_did_not_see() -> lsm_tree::Result<()> {
    let base = tempfile::tempdir()?;
    let rate = super::COMPACTION_RATES[0];
    let prior = super::prior(rate);
    let [planned, calibrated, _] = super::run_arms(base.path(), &config(1_000), EVEN, rate, 0)?;
    for arm in [&planned, &calibrated] {
        assert_eq!(arm.calibrated.model.encode_rate, prior.encode_rate);
        assert_eq!(
            arm.calibrated.model.merge_query_share,
            prior.merge_query_share
        );
        assert_eq!(
            arm.calibrated.scan_weight,
            super::Calibrated::new(prior).scan_weight
        );
        assert_eq!(arm.default_check.plan_changes, 0);
    }
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
