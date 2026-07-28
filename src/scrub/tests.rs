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
        blocks_healed_in_place: 4,
        uncorrectable_blocks: 0,
        errors: vec![],
    };
    acc.merge(PatrolScrubReport {
        sst_files_scanned: 2,
        blocks_scanned: 5,
        corrections_applied: 1,
        ssts_scheduled_for_rewrite: 1,
        blocks_healed_in_place: 3,
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
    assert_eq!(acc.blocks_healed_in_place, 7);
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

/// Opens a plain (no ECC, no compression) tree whose flushed SSTs carry
/// per-KV checksum footers.
fn open_kv_checked_tree(dir: &std::path::Path) -> crate::Tree {
    use crate::runtime_config::KvChecksumPolicy;

    let AnyTree::Standard(tree) = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(crate::config::CompressionPolicy::all(
        crate::CompressionType::None,
    ))
    .open()
    .expect("open kv-checked tree") else {
        unreachable!("standard tree configured");
    };
    tree.update_runtime_config(|c| c.kv_checksums = KvChecksumPolicy::AllLevels)
        .expect("enable kv checksums");
    tree
}

/// The digest reconciliation must not restamp over a `linked_blob_files`
/// record whose blob id rotted WITHOUT changing the section's shape: the
/// section carries no per-section checksum, so a flipped id byte passes the
/// walk's structural validation — only deriving the id set from the table's
/// own indirection entries can catch it. Restamping would hide a live blob
/// from GC or point relocation at a nonexistent one.
#[test]
fn heal_scrub_does_not_restamp_over_a_forged_blob_link_id() -> crate::Result<()> {
    // A KV-separated tree: large values go to a blob file, the SST carries
    // indirections plus a linked_blob_files section.
    let dir = tempfile::tempdir()?;
    let sst_path = {
        let AnyTree::Blob(tree) = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(crate::KvSeparationOptions::default()))
        .open()?
        else {
            unreachable!("kv separation configured");
        };
        let big = |i: u32| format!("{i:08}").repeat(512);
        for i in 0u32..10 {
            tree.insert(format!("key{i:05}"), big(i), u64::from(i) + 1);
        }
        tree.flush_active_memtable(10)?;
        let binding = tree.index.version_history.read().latest_version();
        let Some(table) = binding.version.iter_tables().next() else {
            panic!("flush produced one table");
        };
        (*table.path).clone()
    };

    // Flip one byte INSIDE the first record's blob_file_id: the count and
    // section length stay valid, so the shape check passes.
    let pos = {
        let mut f = std::fs::File::open(&sst_path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader
            .toc()
            .iter()
            .find(|e| e.name() == b"linked_blob_files")
        else {
            panic!("the SST carries a linked_blob_files section");
        };
        usize::try_from(entry.pos()).expect("section offset fits usize")
    };
    let mut bytes = std::fs::read(&sst_path)?;
    let Some(slot) = bytes.get_mut(pos + 4) else {
        panic!("first blob id within the file");
    };
    *slot ^= 0xFF;
    std::fs::write(&sst_path, &bytes)?;

    // Heal scan: data blocks read clean, the section is structurally valid,
    // yet the file digest disagrees with the manifest.
    let AnyTree::Blob(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .open()?
    else {
        unreachable!("kv separation configured");
    };
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        !report.is_ok(),
        "a digest mismatch over a forged blob-link id must be a finding, \
         not silently restamped: {report:?}",
    );
    assert!(
        report
            .errors
            .iter()
            .any(|e| format!("{e:?}").contains("ChecksumRefreshFailed")),
        "the finding must be the refused digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so integrity scans keep
    // flagging the forged file.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged file must keep failing verify_integrity: restamping its \
         digest over an unverifiable blob-link list would mask the forgery",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a STALE per-KV footer
/// hidden behind a re-stamped block checksum: neither the heal scan nor the
/// out-of-band section walk decodes entries, so only the per-KV verification
/// can catch it — without it, `verify_integrity` starts accepting a file
/// that `verify_kv_checksums` still rejects.
#[test]
fn heal_scrub_does_not_restamp_over_a_stale_kv_footer() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let sst_path = {
        let tree = open_kv_checked_tree(dir.path());
        for i in 0u64..500 {
            tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
        }
        tree.flush_active_memtable(500)?;
        let binding = tree.version_history.read().latest_version();
        let table = binding
            .version
            .iter_tables()
            .next()
            .expect("flush produced one table");
        (*table.path).clone()
    };

    crate::test_forge::forge_stale_kv_footer(&sst_path)?;

    // Heal scan: block checksums read clean (re-stamped), yet the file
    // digest disagrees with the manifest.
    let tree = open_kv_checked_tree(dir.path());
    assert!(
        crate::verify::verify_kv_checksums(&tree).is_err(),
        "the forged footer must be detectable by per-KV verification",
    );
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        !report.is_ok(),
        "a digest mismatch over a stale per-KV footer must be a finding, \
         not silently restamped: {report:?}",
    );
    assert!(
        report
            .errors
            .iter()
            .any(|e| format!("{e:?}").contains("ChecksumRefreshFailed")),
        "the finding must be the refused digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so integrity scans keep
    // flagging the forged file.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged file must keep failing verify_integrity: restamping its \
         digest over an unverified per-KV footer would mask the corruption",
    );
    Ok(())
}
