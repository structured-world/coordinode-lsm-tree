#![expect(
    clippy::expect_used,
    reason = "tests assert on known-present values; a panic is the failure signal"
)]
// Target-conditional: `u64 as usize` on a block offset only narrows on
// 32-bit pointer widths, so clippy does NOT fire on the 64-bit CI host.
// This must stay `allow`, NOT `expect`: an `#[expect]` that never fires (as on
// the 64-bit host) is itself a warning (`unfulfilled_lint_expectations`), so the
// usual `#[expect]`-over-`#[allow]` preference does not apply to a lint that only
// triggers on some targets.
#![allow(
    clippy::cast_possible_truncation,
    reason = "in-file block offsets fit usize; only narrow on 32-bit targets"
)]

use super::*;
use crate::{
    AbstractTree,
    MAX_SEQNO,
    SequenceNumberCounter,
    runtime_config::EccScheme,
    // `BlockIndex` is imported only for its `.iter()` method on
    // `table.block_index` (a trait method); `as _` keeps it in scope for
    // method resolution without binding the unused type name.
    table::{block::Header, block_index::BlockIndex as _},
};

/// Opens an RS(8,2) Page-ECC tree at `dir`.
fn open_ecc_tree(dir: &std::path::Path) -> crate::Tree {
    let crate::AnyTree::Standard(tree) = crate::Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .ecc_scheme(EccScheme::ReedSolomon {
        data_shards: 8,
        parity_shards: 2,
    })
    .open()
    .expect("open ecc tree") else {
        unreachable!("standard tree configured (no kv separation)");
    };
    tree
}

/// Writes one ECC SST under `dir` and returns `(sst_path, first_data_block)`.
fn write_ecc_sst(dir: &std::path::Path) -> (std::path::PathBuf, crate::table::BlockHandle) {
    let tree = open_ecc_tree(dir);
    for i in 0u64..2_000 {
        tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
    }
    tree.flush_active_memtable(2_000).expect("flush");

    let binding = tree.version_history.read().latest_version();
    let table = binding
        .version
        .iter_tables()
        .next()
        .expect("flush produced one table");
    let keyed = table
        .block_index
        .iter()
        .next()
        .expect("table has at least one data block")
        .expect("block index entry decodes");
    let handle = crate::table::BlockHandle::new(keyed.offset(), keyed.size());
    ((*table.path).clone(), handle)
}

#[test]
fn patrol_scrub_corrects_seeded_single_bit_fault_and_schedules_heal() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Flip one payload byte of the first data block (RS-correctable: a single
    // byte error is within the RS(8,2) budget).
    let corrupt_pos = block.offset().0 as usize + Header::MIN_LEN + 3;
    let mut bytes = std::fs::read(&sst_path)?;
    let slot = bytes
        .get_mut(corrupt_pos)
        .expect("corrupt_pos in range for the SST bytes");
    *slot ^= 0x80;
    std::fs::write(&sst_path, &bytes)?;

    // Reopen (fresh caches + fds) and opt into rewrite scheduling.
    let tree = open_ecc_tree(dir.path());
    tree.update_runtime_config(|c| c.auto_heal = true)?;
    assert!(tree.heal_hints().is_empty(), "fresh tree has no hints");

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default());

    assert!(
        report.corrections_applied >= 1,
        "scrub must correct the seeded fault: {report:?}",
    );
    assert_eq!(
        report.ssts_scheduled_for_rewrite, 1,
        "the corrected SST is queued for healing exactly once: {report:?}",
    );
    assert_eq!(report.uncorrectable_blocks, 0, "{report:?}");
    assert!(
        report.is_ok(),
        "a fully-correctable scrub is ok: {report:?}"
    );
    assert!(
        !tree.heal_hints().is_empty(),
        "the SST is recorded in the heal queue",
    );
    #[cfg(feature = "metrics")]
    assert_eq!(
        tree.metrics().ecc_auto_heal_scheduled_count(),
        1,
        "the scheduled SST is counted once in metrics",
    );
    Ok(())
}

#[test]
fn patrol_scrub_corrects_without_scheduling_when_auto_heal_off() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    let corrupt_pos = block.offset().0 as usize + Header::MIN_LEN + 3;
    let mut bytes = std::fs::read(&sst_path)?;
    let slot = bytes.get_mut(corrupt_pos).expect("corrupt_pos in range");
    *slot ^= 0x80;
    std::fs::write(&sst_path, &bytes)?;

    // Reopen WITHOUT enabling auto_heal (default off).
    let tree = open_ecc_tree(dir.path());
    assert!(!tree.heal_hints().is_enabled(), "auto_heal defaults off");

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default());

    assert!(
        report.corrections_applied >= 1,
        "correction-on-read still happens with auto_heal off: {report:?}",
    );
    assert_eq!(
        report.ssts_scheduled_for_rewrite, 0,
        "auto_heal off suppresses rewrite scheduling: {report:?}",
    );
    assert!(
        tree.heal_hints().is_empty(),
        "no SST queued when scheduling is off",
    );
    assert!(report.is_ok());
    Ok(())
}

#[test]
fn patrol_scrub_reports_uncorrectable_block_not_silently_skipped() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Wreck the whole payload+parity of the first data block (header left
    // intact so it still parses): far beyond the RS(8,2) correction budget,
    // so the block is uncorrectable.
    let payload_start = block.offset().0 as usize + Header::MIN_LEN;
    let payload_end = block.offset().0 as usize + block.size() as usize;
    let mut bytes = std::fs::read(&sst_path)?;
    for slot in bytes
        .get_mut(payload_start..payload_end)
        .expect("block payload range in bounds")
    {
        *slot ^= 0xFF;
    }
    std::fs::write(&sst_path, &bytes)?;

    let tree = open_ecc_tree(dir.path());
    tree.update_runtime_config(|c| c.auto_heal = true)?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default());

    assert!(
        report.uncorrectable_blocks >= 1,
        "an unrecoverable block must be reported, not skipped: {report:?}",
    );
    assert!(!report.is_ok(), "uncorrectable corruption fails the scrub");
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::UncorrectableBlock { .. })),
        "the finding is an UncorrectableBlock: {report:?}",
    );
    Ok(())
}

#[test]
fn patrol_scrub_clean_ecc_tree_reports_no_corrections() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let _ = write_ecc_sst(dir.path());

    let tree = open_ecc_tree(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default());

    assert_eq!(report.sst_files_scanned, 1);
    assert!(report.blocks_scanned >= 1);
    assert_eq!(report.corrections_applied, 0, "no fault → no correction");
    assert_eq!(report.uncorrectable_blocks, 0);
    assert!(report.is_ok());

    // Sanity: a clean read of a key still returns the right value.
    let got = tree.get(b"key-000000", MAX_SEQNO)?.expect("key present");
    assert_eq!(&*got, b"v000000");
    Ok(())
}

#[test]
fn patrol_scrub_heals_in_place_restoring_the_block_byte_for_byte() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Snapshot the healthy file, then flip one RS-correctable payload byte.
    let original = std::fs::read(&sst_path)?;
    let corrupt_pos = block.offset().0 as usize + Header::MIN_LEN + 3;
    let mut bytes = original.clone();
    let slot = bytes
        .get_mut(corrupt_pos)
        .expect("corrupt_pos in range for the SST bytes");
    *slot ^= 0x80;
    std::fs::write(&sst_path, &bytes)?;
    assert_ne!(bytes, original, "the seeded fault changed the file");

    // Heal in place: persist the correction at the block's offset, no full rewrite.
    let tree = open_ecc_tree(dir.path());
    let opts = PatrolScrubOptions::default().heal_in_place(true);
    let report = patrol_scrub(&tree, &opts);

    assert_eq!(
        report.blocks_healed_in_place, 1,
        "exactly the corrupted block is healed in place: {report:?}",
    );
    assert_eq!(report.corrections_applied, 1, "{report:?}");
    assert_eq!(
        report.ssts_scheduled_for_rewrite, 0,
        "in-place heal schedules no full-file rewrite: {report:?}",
    );
    assert_eq!(report.uncorrectable_blocks, 0, "{report:?}");
    assert!(report.is_ok(), "{report:?}");

    // The heal reconstructs the ORIGINAL frame (RS-recovered data + recomputed
    // parity == as-written bytes), so the file is byte-identical to before the
    // fault: the correction was persisted, and no healthy block was touched.
    let healed = std::fs::read(&sst_path)?;
    assert_eq!(
        healed, original,
        "in-place heal restores the SST byte-for-byte (O(damage), nothing else moved)",
    );

    // A second pass finds nothing to heal — the on-disk bytes now read clean.
    // Drop the first tree first: the directory lock is exclusive, so a second
    // open of the same dir while it is alive would fail with `Locked`.
    drop(tree);
    let tree2 = open_ecc_tree(dir.path());
    let report2 = patrol_scrub(&tree2, &PatrolScrubOptions::default().heal_in_place(true));
    assert_eq!(
        report2.blocks_healed_in_place, 0,
        "nothing left to heal after a clean heal: {report2:?}",
    );
    assert_eq!(report2.corrections_applied, 0, "{report2:?}");
    Ok(())
}

#[test]
fn patrol_scrub_heal_in_place_leaves_an_uncorrectable_block_for_salvage() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Wreck the whole payload+parity (header intact): beyond the RS(8,2) budget.
    let payload_start = block.offset().0 as usize + Header::MIN_LEN;
    let payload_end = block.offset().0 as usize + block.size() as usize;
    let mut bytes = std::fs::read(&sst_path)?;
    for slot in bytes
        .get_mut(payload_start..payload_end)
        .expect("block payload range in bounds")
    {
        *slot ^= 0xFF;
    }
    std::fs::write(&sst_path, &bytes)?;
    let corrupted = std::fs::read(&sst_path)?;

    let tree = open_ecc_tree(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));

    assert_eq!(
        report.blocks_healed_in_place, 0,
        "an uncorrectable block is not healed in place: {report:?}",
    );
    assert!(
        report.uncorrectable_blocks >= 1,
        "the uncorrectable block is reported, not silently skipped: {report:?}",
    );
    assert!(
        !report.is_ok(),
        "uncorrectable corruption fails the heal pass"
    );
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::UncorrectableBlock { .. })),
        "the finding is an UncorrectableBlock: {report:?}",
    );
    // The heal must not have written anything for that block: it is left intact
    // for block salvage (the new-file copy-through path).
    let after = std::fs::read(&sst_path)?;
    assert_eq!(
        after, corrupted,
        "an uncorrectable block is left untouched in place for salvage",
    );
    Ok(())
}
