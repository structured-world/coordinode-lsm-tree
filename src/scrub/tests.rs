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

/// A patrol reconcile calls `refresh_table_checksum` while holding a table's
/// heal lock; a concurrent tight-space compaction holds `compaction_state` and
/// then acquires that heal lock. Blocking on `compaction_state` here would
/// invert the lock order (`heal_lock` -> `compaction_state` on the patrol path
/// vs `compaction_state` -> `heal_lock` on the compaction path) and deadlock.
/// The refresh must instead SKIP (`Ok(false)`) when `compaction_state` is
/// contended.
///
/// Deterministic without threads: `parking_lot`'s non-reentrant `try_lock` fails
/// even for the holding thread, so the pre-fix blocking `lock()` would
/// self-deadlock this very test (a hang), and the fixed `try_lock` returns
/// `Ok(false)`.
#[test]
fn refresh_table_checksum_skips_when_compaction_state_is_contended() {
    let dir = tempfile::tempdir().expect("tempdir");
    let AnyTree::Standard(tree) = standard_tree(dir.path()) else {
        unreachable!("standard tree configured");
    };
    // The `try_lock` short-circuits before any table lookup, so a dummy id /
    // checksum exercises exactly the contention-skip path.
    let held = tree.compaction_state.lock();
    let result = tree.refresh_table_checksum(0, crate::Checksum::from_raw(0), None);
    drop(held);
    assert!(
        matches!(result, Ok(false)),
        "refresh must skip (not block) when compaction_state is contended: {result:?}",
    );
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

/// Opens an uncompressed RS(8,2) Page-ECC tree whose flushed SSTs carry
/// per-KV checksum footers. ECC because only Page-ECC tables get the
/// heal-mode digest reconciliation these tests exercise.
#[cfg(feature = "page_ecc")]
fn open_kv_checked_tree(dir: &std::path::Path) -> crate::Result<crate::Tree> {
    use crate::runtime_config::{EccScheme, KvChecksumPolicy};

    let AnyTree::Standard(tree) = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(crate::config::CompressionPolicy::all(
        crate::CompressionType::None,
    ))
    .page_ecc(true)
    .ecc_scheme(EccScheme::ReedSolomon {
        data_shards: 8,
        parity_shards: 2,
    })
    .open()?
    else {
        unreachable!("standard tree configured");
    };
    tree.update_runtime_config(|c| c.kv_checksums = KvChecksumPolicy::AllLevels)?;
    Ok(tree)
}

/// Opens (or reopens) a KV-separated RS(8,2) Page-ECC tree at `dir`.
#[cfg(feature = "page_ecc")]
fn open_blob_ecc_tree(dir: &std::path::Path) -> crate::Result<crate::BlobTree> {
    use crate::runtime_config::EccScheme;

    let AnyTree::Blob(tree) = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .page_ecc(true)
    .ecc_scheme(EccScheme::ReedSolomon {
        data_shards: 8,
        parity_shards: 2,
    })
    .open()?
    else {
        unreachable!("kv separation configured");
    };
    Ok(tree)
}

/// Builds a KV-separated RS(8,2) Page-ECC tree at `dir` with ten large
/// values (blob-file indirections plus a `linked_blob_files` section) and
/// returns the flushed SST's path. Reopen with [`open_blob_ecc_tree`].
#[cfg(feature = "page_ecc")]
fn build_blob_ecc_sst(dir: &std::path::Path) -> crate::Result<std::path::PathBuf> {
    let tree = open_blob_ecc_tree(dir)?;
    let big = |i: u32| format!("{i:08}").repeat(512);
    for i in 0u32..10 {
        tree.insert(format!("key{i:05}"), big(i), u64::from(i) + 1);
    }
    tree.flush_active_memtable(10)?;
    let binding = tree.index.version_history.read().latest_version();
    let Some(table) = binding.version.iter_tables().next() else {
        panic!("flush produced one table");
    };
    Ok((*table.path).clone())
}

/// Byte offset of the `linked_blob_files` SFA section in the SST at `path`
/// (the record layout after the u32 count is: `id u64 | len u64 | bytes u64
/// | on_disk_bytes u64`).
#[cfg(feature = "page_ecc")]
fn linked_blob_files_offset(path: &std::path::Path) -> crate::Result<usize> {
    let mut f = std::fs::File::open(path)?;
    let reader = crate::sfa::Reader::from_reader(&mut f)?;
    let Some(entry) = reader
        .toc()
        .iter()
        .find(|e| e.name() == b"linked_blob_files")
    else {
        return Err(crate::Error::InvalidHeader(
            "SST is missing its linked_blob_files section",
        ));
    };
    usize::try_from(entry.pos())
        .map_err(|_| crate::Error::InvalidHeader("linked_blob_files offset exceeds usize"))
}

/// The blob-link cross-check must compare the COMPLETE accounting records,
/// not just the id set: a record whose `bytes` counter rotted (ids intact)
/// feeds `Version::with_dropped` fragmentation math, and a forged total can
/// make `BlobFile::is_dead` prune a blob another table still references.
#[cfg(feature = "page_ecc")]
#[test]
fn heal_scrub_does_not_restamp_over_forged_blob_link_accounting() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let sst_path = build_blob_ecc_sst(dir.path())?;

    // Flip one byte inside the first record's `bytes` counter: every id
    // stays intact, the shape stays valid.
    let pos = linked_blob_files_offset(&sst_path)?;
    let mut bytes = std::fs::read(&sst_path)?;
    let Some(slot) = bytes.get_mut(pos + 4 + 16) else {
        panic!("first record's bytes counter within the file");
    };
    *slot ^= 0xFF;
    std::fs::write(&sst_path, &bytes)?;

    let tree = open_blob_ecc_tree(dir.path())?;
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "forged blob-link accounting must refuse the digest refresh: {report:?}",
    );
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged accounting must keep failing verify_integrity",
    );
    Ok(())
}

/// A heal-mode scrub must NEVER reconcile the digest of a table WITHOUT
/// Page-ECC: nothing can have legitimately healed its bytes in place, so a
/// manifest-level digest mismatch on such a table is real evidence — an
/// in-band alteration whose block checksums were re-stamped has NO other
/// detector (no parity, no footers), and restamping the manifest digest
/// would erase the only record of it. It also spares every ordinary SST
/// the full-file digest read.
#[test]
fn heal_scrub_does_not_reconcile_a_non_ecc_table() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let sst_path = {
        let AnyTree::Standard(tree) = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(crate::config::CompressionPolicy::all(
            crate::CompressionType::None,
        ))
        .open()?
        else {
            unreachable!("standard tree configured");
        };
        for i in 0u64..500 {
            tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
        }
        tree.flush_active_memtable(500)?;
        let binding = tree.version_history.read().latest_version();
        let Some(table) = binding.version.iter_tables().next() else {
            panic!("flush produced one table");
        };
        (*table.path).clone()
    };

    // In-band alteration with a re-stamped block checksum: every frame reads
    // internally valid, only the manifest digest disagrees.
    crate::test_forge::forge_restamped_data_block(&sst_path)?;

    let AnyTree::Standard(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(crate::config::CompressionPolicy::all(
        crate::CompressionType::None,
    ))
    .open()?
    else {
        unreachable!("standard tree configured");
    };
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    // The reconciliation must be SKIPPED, not attempted-and-failed: a
    // ChecksumRefreshFailed finding here would mean the non-ECC gate in the
    // scan regressed even if the digest itself survived.
    assert!(
        !report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a non-ECC table must skip the digest reconciliation entirely, not \
         attempt and fail it: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest: on a non-ECC table it is the
    // ONLY detector of a re-stamped in-band alteration.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "a non-ECC table's digest mismatch must survive a heal scrub: \
         restamping it would erase the only record of the alteration",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a `linked_blob_files`
/// record whose blob id rotted WITHOUT changing the section's shape: the
/// section carries no per-section checksum, so a flipped id byte passes the
/// walk's structural validation — only deriving the id set from the table's
/// own indirection entries can catch it. Restamping would hide a live blob
/// from GC or point relocation at a nonexistent one.
#[cfg(feature = "page_ecc")]
#[test]
fn heal_scrub_does_not_restamp_over_a_forged_blob_link_id() -> crate::Result<()> {
    // A KV-separated Page-ECC tree: large values go to a blob file, the SST
    // carries indirections plus a linked_blob_files section (ECC because
    // only Page-ECC tables get the heal-mode digest reconciliation).
    let dir = tempfile::tempdir()?;
    let sst_path = build_blob_ecc_sst(dir.path())?;

    // Flip one byte INSIDE the first record's blob_file_id: the count and
    // section length stay valid, so the shape check passes.
    let pos = linked_blob_files_offset(&sst_path)?;
    let mut bytes = std::fs::read(&sst_path)?;
    let Some(slot) = bytes.get_mut(pos + 4) else {
        panic!("first blob id within the file");
    };
    *slot ^= 0xFF;
    std::fs::write(&sst_path, &bytes)?;

    // Heal scan: data blocks read clean, the section is structurally valid,
    // yet the file digest disagrees with the manifest.
    let tree = open_blob_ecc_tree(dir.path())?;
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
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
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
#[cfg(feature = "page_ecc")]
#[test]
fn heal_scrub_does_not_restamp_over_a_stale_kv_footer() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let sst_path = {
        let tree = open_kv_checked_tree(dir.path())?;
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
    let tree = open_kv_checked_tree(dir.path())?;
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
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
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
