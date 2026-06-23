#![expect(
    clippy::expect_used,
    reason = "tests assert on known-present values; a panic is the failure signal"
)]

use super::*;
use crate::{AbstractTree, AnyTree, Config, SequenceNumberCounter};

fn standard_tree(dir: &std::path::Path) -> AnyTree {
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open tree")
}

#[test]
fn report_merge_sums_every_counter_and_concatenates_errors() {
    let mut acc = PatrolScrubReport {
        sst_files_scanned: 1,
        blocks_scanned: 10,
        corrections_applied: 2,
        ssts_scheduled_for_rewrite: 1,
        uncorrectable_blocks: 0,
        errors: vec![],
    };
    acc.merge(PatrolScrubReport {
        sst_files_scanned: 2,
        blocks_scanned: 5,
        corrections_applied: 1,
        ssts_scheduled_for_rewrite: 1,
        uncorrectable_blocks: 3,
        errors: vec![ScrubError::UncorrectableBlock {
            table_id: 7,
            path: "/x".into(),
            block_offset: 42,
            reason: "boom".into(),
        }],
    });
    assert_eq!(acc.sst_files_scanned, 3);
    assert_eq!(acc.blocks_scanned, 15);
    assert_eq!(acc.corrections_applied, 3);
    assert_eq!(acc.ssts_scheduled_for_rewrite, 2);
    assert_eq!(acc.uncorrectable_blocks, 3);
    assert_eq!(acc.errors.len(), 1);
}

#[test]
fn report_is_ok_only_when_no_uncorrectable_blocks() {
    let mut report = PatrolScrubReport::default();
    assert!(report.is_ok(), "a fresh empty report is ok");
    report.corrections_applied = 5;
    assert!(report.is_ok(), "corrected blocks do not make a scrub fail");
    report.uncorrectable_blocks = 1;
    assert!(!report.is_ok(), "an uncorrectable block fails the scrub");
}

#[test]
fn options_builder_sets_parallelism_and_throttle() {
    let opts = PatrolScrubOptions::default()
        .parallelism(4)
        .throttle(std::time::Duration::from_millis(7));
    assert_eq!(opts.parallelism, 4);
    assert_eq!(opts.throttle, Some(std::time::Duration::from_millis(7)));
}

#[test]
fn patrol_scrub_on_clean_non_ecc_tree_reads_blocks_without_findings() {
    let dir = tempfile::tempdir().expect("tempdir");
    let AnyTree::Standard(tree) = standard_tree(dir.path()) else {
        unreachable!("standard tree configured");
    };
    for i in 0u64..500 {
        tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
    }
    tree.flush_active_memtable(500).expect("flush");

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default());
    assert_eq!(report.sst_files_scanned, 1, "one flushed SST");
    assert!(report.blocks_scanned >= 1, "at least one data block read");
    assert_eq!(report.corrections_applied, 0, "no ECC, nothing to correct");
    assert_eq!(
        report.uncorrectable_blocks, 0,
        "clean tree has no corruption"
    );
    assert!(report.is_ok());
}

#[test]
fn patrol_scrub_empty_tree_scans_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let AnyTree::Standard(tree) = standard_tree(dir.path()) else {
        unreachable!("standard tree configured");
    };
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default());
    assert_eq!(report.sst_files_scanned, 0);
    assert_eq!(report.blocks_scanned, 0);
    assert!(report.is_ok());
}

#[test]
fn patrol_scrub_parallel_over_many_ssts_visits_every_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let AnyTree::Standard(tree) = standard_tree(dir.path()) else {
        unreachable!("standard tree configured");
    };
    // Flush four times → four SSTs (no compaction triggered at this size).
    for batch in 0u64..4 {
        for i in 0u64..200 {
            let k = batch * 1_000 + i;
            tree.insert(format!("key-{k:06}"), format!("v{k:06}"), k);
        }
        tree.flush_active_memtable((batch + 1) * 1_000)
            .expect("flush");
    }

    let opts = PatrolScrubOptions::default()
        .parallelism(3)
        .throttle(std::time::Duration::from_millis(1));
    let report = patrol_scrub(&tree, &opts);
    assert_eq!(report.sst_files_scanned, 4, "every SST scrubbed once");
    assert!(report.blocks_scanned >= 4);
    assert!(report.is_ok());
}
