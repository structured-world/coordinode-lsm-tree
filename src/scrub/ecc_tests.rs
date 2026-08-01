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

/// As [`write_ecc_sst`], plus a range tombstone so the SST carries the
/// `range_tombstones` section — the deletion metadata the digest
/// reconciliation cannot semantically authenticate.
fn write_ecc_sst_with_range_tombstone(
    dir: &std::path::Path,
) -> (std::path::PathBuf, crate::table::BlockHandle) {
    let tree = open_ecc_tree(dir);
    for i in 0u64..2_000 {
        tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
    }
    tree.remove_range("key-000100", "key-000200", 2_000);
    tree.flush_active_memtable(2_100).expect("flush");

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

/// A table WITHOUT Page-ECC still has its integrity checked under
/// `heal_in_place`: there is nothing to heal without parity, so it takes the
/// checksum-verifying scrub path, and a corrupt block is reported uncorrectable
/// rather than silently reported clean.
#[test]
fn patrol_scrub_heal_in_place_still_checks_a_non_ecc_table() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    // Build a plain (no Page-ECC) SST, then drop the tree so the file can be
    // corrupted and reopened with fresh caches.
    let sst_path;
    let block_off;
    {
        let crate::AnyTree::Standard(tree) = crate::Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()
        .expect("open plain tree") else {
            unreachable!("standard tree configured (no kv separation)");
        };
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
            .expect("table has a data block")
            .expect("index entry decodes");
        sst_path = (*table.path).clone();
        block_off = keyed.offset().0 as usize;
    }

    // Flip a payload byte of the first data block (no parity → uncorrectable).
    let mut bytes = std::fs::read(&sst_path)?;
    let slot = bytes
        .get_mut(block_off + Header::MIN_LEN + 3)
        .expect("corrupt position in range");
    *slot ^= 0x80;
    std::fs::write(&sst_path, &bytes)?;

    let crate::AnyTree::Standard(tree) = crate::Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("reopen plain tree") else {
        unreachable!("standard tree configured (no kv separation)");
    };
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));

    assert_eq!(
        report.blocks_healed_in_place, 0,
        "a non-ECC table has nothing to heal in place: {report:?}",
    );
    assert!(
        report.uncorrectable_blocks >= 1,
        "a corrupt block in a non-ECC table is reported, not silently clean: {report:?}",
    );
    assert!(!report.is_ok(), "uncorrectable corruption fails the pass");
    Ok(())
}

/// Bit rot confined to a block's PARITY trailer reads as Clean (the payload
/// checksum passes and parity is only consulted on a payload mismatch), so
/// without an explicit trailer check the heal pass would leave dead ECC on
/// disk — a later payload fault could no longer be recovered. `heal_in_place`
/// must verify each clean block's trailer against freshly computed parity and
/// PERSIST a rebuilt trailer on a mismatch (the pass holds the read+write
/// handle; the payload is untouched, so the rewrite is size-preserving).
#[test]
fn heal_in_place_restores_a_rotted_parity_trailer() -> crate::Result<()> {
    use crate::coding::Decode;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Flip one byte INSIDE the first data block's parity trailer (right after
    // its `data_length` payload): the payload checksum still verifies, so the
    // block reads back Clean.
    let mut bytes = std::fs::read(&sst_path)?;
    let base = block.offset().0 as usize;
    let Some(mut cursor) = bytes.get(base..) else {
        panic!("first data block within the file");
    };
    let header = Header::decode_from(&mut cursor)?;
    let trailer_pos = base + Header::header_len(header.block_type) + header.data_length as usize;
    let Some(slot) = bytes.get_mut(trailer_pos) else {
        panic!("parity trailer within the file");
    };
    let original = *slot;
    *slot = original ^ 0xFF;
    std::fs::write(&sst_path, &bytes)?;

    // Reopen (fresh caches + fds) and heal in place.
    let tree = open_ecc_tree(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));

    assert!(
        report.blocks_healed_in_place >= 1,
        "the rotted parity trailer is rebuilt and persisted: {report:?}",
    );
    assert_eq!(report.uncorrectable_blocks, 0, "{report:?}");
    assert!(report.is_ok(), "a parity rebuild is a heal, not a finding");

    // The on-disk byte is restored to its EXACT original value (not merely
    // changed): the rebuilt parity is recomputed over the untouched payload,
    // so anything but the original would be wrong parity persisted.
    let healed = std::fs::read(&sst_path)?;
    let Some(&now) = healed.get(trailer_pos) else {
        panic!("parity trailer within the healed file");
    };
    assert_eq!(now, original, "the original parity byte was restored");
    Ok(())
}

/// Opens an RS(8,2) Page-ECC tree at `dir` through the given filesystem
/// (fault-injection variant of [`open_ecc_tree`]).
fn open_ecc_tree_on(dir: &std::path::Path, fs: std::sync::Arc<dyn crate::fs::Fs>) -> crate::Tree {
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
    .with_shared_fs(fs)
    .open()
    .expect("open ecc tree on injected fs") else {
        unreachable!("standard tree configured (no kv separation)");
    };
    tree
}

/// Flips one parity-trailer byte of `block` in the SST at `path`. Payload
/// checksums stay clean, so only the heal pass (which verifies trailers)
/// notices; a heal then rebuilds the trailer in place.
fn corrupt_parity_trailer_byte(
    path: &std::path::Path,
    block: &crate::table::BlockHandle,
) -> crate::Result<()> {
    use crate::coding::Decode;

    let mut bytes = std::fs::read(path)?;
    let base = block.offset().0 as usize;
    let Some(mut cursor) = bytes.get(base..) else {
        panic!("data block within the file");
    };
    let header = Header::decode_from(&mut cursor)?;
    let trailer_pos = base + Header::header_len(header.block_type) + header.data_length as usize;
    let Some(slot) = bytes.get_mut(trailer_pos) else {
        panic!("parity trailer within the file");
    };
    *slot ^= 0xFF;
    std::fs::write(path, &bytes)?;
    Ok(())
}

/// Rebuilds the manifest from whatever is on disk, recording the digest of
/// the CURRENT (possibly rotted) bytes, and asserts exactly one table was
/// admitted. This is how the reconcile tests seed a manifest digest that
/// disagrees with the ORIGINAL bytes a later heal restores.
fn rebuild_manifest_over_current_bytes(dir: &std::path::Path) -> crate::Result<()> {
    let report = crate::Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;
    assert_eq!(report.recovered, 1, "{report:?}");
    Ok(())
}

/// Opens a FaultFs-backed ECC tree at `dir` with a ONE-SHOT `Open` fault
/// armed on the manifest edit log ("edits"), so the first digest refresh
/// fails while the heal itself (which only touches the SST under tables/)
/// proceeds. Returns the tree and the injector for the caller to `clear()`.
fn open_ecc_tree_with_failing_edit_log(
    dir: &std::path::Path,
) -> (crate::Tree, std::sync::Arc<crate::fs::FaultInjector>) {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir, std::sync::Arc::new(fault));
    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Other))
            .on_path("edits")
            .once(),
    );
    (tree, injector)
}

/// A failed raw re-read during the clean-block parity-trailer check is a
/// finding, not a silent skip: the block's trailer could not be verified, so
/// the heal pass reports it as uncorrectable and moves on (the remaining
/// blocks still get their trailers checked).
#[test]
fn heal_in_place_reports_a_failed_parity_reread_as_uncorrectable() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (_sst_path, _block) = write_ecc_sst(dir.path());

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));

    // Within the heal pass, per block: read #1 is the verifying scrub read,
    // read #2 is the raw frame re-read for the parity-trailer comparison.
    // Fail exactly the FIRST block's re-read.
    injector.arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .skip(1)
            .once(),
    );
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    assert_eq!(
        report.uncorrectable_blocks, 1,
        "the unverifiable trailer is a finding: {report:?}",
    );
    assert!(
        format!("{report:?}").contains("parity re-read failed"),
        "the finding names the failed re-read: {report:?}",
    );
    assert_eq!(
        report.blocks_healed_in_place, 0,
        "nothing was persisted for the failed block: {report:?}",
    );
    Ok(())
}

/// A parity-trailer rebuild whose WRITE fails is a finding: the rot stays on
/// disk, so the heal must report the block as uncorrectable instead of
/// counting a heal that never landed.
#[test]
fn heal_in_place_reports_a_failed_trailer_writeback_as_uncorrectable() -> crate::Result<()> {
    use crate::coding::Decode;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Rot one parity-trailer byte of the first data block (payload checksum
    // still verifies, so the block scrubs Clean and the trailer check fires).
    let mut bytes = std::fs::read(&sst_path)?;
    let base = block.offset().0 as usize;
    let Some(mut cursor) = bytes.get(base..) else {
        panic!("first data block within the file");
    };
    let header = Header::decode_from(&mut cursor)?;
    let trailer_pos = base + Header::header_len(header.block_type) + header.data_length as usize;
    let Some(slot) = bytes.get_mut(trailer_pos) else {
        panic!("parity trailer within the file");
    };
    let rotted = *slot ^ 0xFF;
    *slot = rotted;
    std::fs::write(&sst_path, &bytes)?;

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));

    // The heal pass performs no other writes to the SST, so the first write
    // is the trailer rebuild.
    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    assert_eq!(
        report.blocks_healed_in_place, 0,
        "a write-back that failed is not counted as a heal: {report:?}",
    );
    assert_eq!(report.uncorrectable_blocks, 1, "{report:?}");
    assert!(
        format!("{report:?}").contains("in-place parity rebuild"),
        "the finding names the failed rebuild: {report:?}",
    );

    // The rot is still on disk (nothing was silently half-written).
    let after = std::fs::read(&sst_path)?;
    assert_eq!(
        after.get(trailer_pos).copied(),
        Some(rotted),
        "the rotted trailer byte is untouched after the failed write-back",
    );
    Ok(())
}

/// A corrected block whose heal RE-READ fails (transient I/O on the second,
/// persist-side read) is a finding: the correction cannot be written back, so
/// the block is reported uncorrectable rather than silently skipped.
#[test]
fn heal_in_place_reports_a_failed_heal_reread_as_uncorrectable() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Flip one payload byte of the first data block (RS-correctable).
    let corrupt_pos = block.offset().0 as usize + Header::MIN_LEN + 3;
    let mut bytes = std::fs::read(&sst_path)?;
    let Some(slot) = bytes.get_mut(corrupt_pos) else {
        panic!("corrupt_pos in range for the SST bytes");
    };
    *slot ^= 0x80;
    std::fs::write(&sst_path, &bytes)?;

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));

    // For the corrupted first block: read #1 is the scrub read (corrects in
    // memory), read #2 is `heal_frame`'s persist-side re-read — fail that one.
    injector.arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .skip(1)
            .once(),
    );
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    assert_eq!(
        report.blocks_healed_in_place, 0,
        "nothing was persisted for the failed block: {report:?}",
    );
    assert_eq!(report.uncorrectable_blocks, 1, "{report:?}");
    assert!(
        format!("{report:?}").contains("heal re-read failed"),
        "the finding names the failed heal re-read: {report:?}",
    );
    Ok(())
}

/// An in-place heal must not mutate an inode a checkpoint hard-links: the
/// checkpoint's manifest recorded the digest of the bytes AT SNAPSHOT TIME,
/// and rewriting the shared inode underneath it permanently desynchronizes
/// the snapshot from its own manifest (only the LIVE tree's digest is
/// reconciled). The heal must instead break the link (heal a private copy of
/// the live file), leaving the checkpoint's inode byte-identical to what its
/// manifest describes.
#[test]
fn heal_in_place_does_not_mutate_a_hard_linked_checkpoint_inode() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Rot one payload byte (RS-correctable) BEFORE the snapshot: the
    // checkpoint captures the rotted bytes, exactly what its manifest
    // would describe.
    let corrupt_pos = block.offset().0 as usize + Header::MIN_LEN + 3;
    let mut bytes = std::fs::read(&sst_path)?;
    let slot = bytes.get_mut(corrupt_pos).expect("corrupt_pos in range");
    *slot ^= 0x80;
    std::fs::write(&sst_path, &bytes)?;

    // Checkpoint-style hard link to the (rotted) SST. A separate directory
    // outside the tree keeps recovery from treating it as an orphan.
    let cp_dir = tempfile::tempdir_in(dir.path().parent().expect("tempdir has a parent"))?;
    let link_path = cp_dir.path().join("checkpoint.sst");
    std::fs::hard_link(&sst_path, &link_path)?;
    let snapshot = std::fs::read(&link_path)?;

    let tree = open_ecc_tree(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report.blocks_healed_in_place >= 1,
        "the live file's fault is healed: {report:?}",
    );
    assert!(report.is_ok(), "{report:?}");

    // The LIVE path carries the healed bytes...
    let live = std::fs::read(&sst_path)?;
    assert_ne!(
        live, snapshot,
        "the live path must expose the healed bytes after the scrub",
    );
    // ...while the checkpoint's inode still holds exactly the snapshot the
    // checkpoint manifest describes.
    let checkpoint = std::fs::read(&link_path)?;
    assert_eq!(
        checkpoint, snapshot,
        "the checkpoint's hard-linked inode must keep its snapshot bytes: \
         healing through a shared inode desynchronizes the checkpoint from \
         its own manifest digest",
    );
    Ok(())
}

/// After an unshare detaches the live path onto a new inode, the table's
/// descriptor cache may still hold the OLD inode's fd: a later heal on the
/// same open tree would then SCRUB the stale inode (clean) while its
/// re-read and write-back use the live file — a recoverable fault on the
/// live copy reads as an unexplained checksum mismatch and is reported
/// uncorrectable without ever attempting ECC recovery. The unshare must
/// invalidate the cached descriptor.
#[test]
fn heal_in_place_rebinds_the_descriptor_cache_after_an_unshare() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Rot one parity-trailer byte so the FIRST heal actually WRITES (the
    // unshare runs lazily, only before the first write-back), then
    // hard-link the SST so that write takes the unshare path.
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    let cp_dir = tempfile::tempdir_in(dir.path().parent().expect("tempdir has a parent"))?;
    std::fs::hard_link(&sst_path, cp_dir.path().join("checkpoint.sst"))?;

    let tree = open_ecc_tree(dir.path());

    // Prime the descriptor cache with the ORIGINAL inode, then heal (the
    // unshare renames a private copy over the live path).
    assert!(tree.get("key-000000", crate::SeqNo::MAX)?.is_some());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(report.is_ok(), "the trailer rebuild succeeds: {report:?}");
    assert!(
        report.blocks_healed_in_place >= 1,
        "the first pass must write, so the unshare runs: {report:?}",
    );

    // A recoverable payload fault lands on the LIVE (post-rename) inode.
    let corrupt_pos = block.offset().0 as usize + Header::MIN_LEN + 3;
    let mut bytes = std::fs::read(&sst_path)?;
    let slot = bytes
        .get_mut(corrupt_pos)
        .expect("corrupt_pos in range for the SST bytes");
    *slot ^= 0x80;
    std::fs::write(&sst_path, &bytes)?;

    // SECOND heal on the SAME open tree: the scrub must see the live inode's
    // fault as ECC-recoverable, not scrub a stale cached fd clean and then
    // report the live mismatch as uncorrectable.
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report.blocks_healed_in_place >= 1,
        "the live fault is ECC-recovered and healed in place: {report:?}",
    );
    assert!(report.is_ok(), "{report:?}");
    Ok(())
}

/// The descriptor invalidation must happen as soon as the publish RENAME
/// succeeds — even when the post-rename directory sync fails: the live path
/// already points at the new inode, so bailing out before the invalidation
/// leaves the cache pinned to the old checkpoint-linked inode, and a later
/// heal (which sees one link on the new inode and does not unshare again)
/// scrubs the stale inode while the live file rots.
#[test]
fn heal_in_place_rebinds_the_descriptor_cache_when_the_directory_sync_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Rot one parity-trailer byte (the unshare only runs before the first
    // write-back), then hard-link the SST so the FIRST heal takes the
    // unshare path.
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    let cp_dir = tempfile::tempdir_in(dir.path().parent().expect("tempdir has a parent"))?;
    std::fs::hard_link(&sst_path, cp_dir.path().join("checkpoint.sst"))?;

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));

    // Prime the descriptor cache with the ORIGINAL inode, then heal with the
    // post-rename directory sync failing: the unshare errors out AFTER the
    // rename has already replaced the live path.
    assert!(tree.get("key-000000", crate::SeqNo::MAX)?.is_some());
    injector.arm(
        FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();
    assert!(
        !report.is_ok(),
        "the failed unshare is a finding: {report:?}"
    );

    // A recoverable payload fault lands on the LIVE (post-rename) inode.
    let corrupt_pos = block.offset().0 as usize + Header::MIN_LEN + 3;
    let mut bytes = std::fs::read(&sst_path)?;
    let slot = bytes
        .get_mut(corrupt_pos)
        .expect("corrupt_pos in range for the SST bytes");
    *slot ^= 0x80;
    std::fs::write(&sst_path, &bytes)?;

    // SECOND heal on the SAME open tree: the scrub must see the live
    // inode's fault as ECC-recoverable, not scrub the stale cached fd.
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report.blocks_healed_in_place >= 1,
        "the live fault is ECC-recovered and healed in place: {report:?}",
    );
    assert!(report.is_ok(), "{report:?}");
    Ok(())
}

/// The digest reconciliation must not restamp over a RENAMED section: a
/// TOC whose `filter` entry was re-labelled to an unknown name (trailer
/// checksum re-stamped) hides the section from every reader while each
/// block inside still passes its byte-level checks — an unknown
/// block-format section must FAIL the walk closed, or the restamp would
/// legitimize an archive whose known sections silently vanished.
#[test]
fn heal_in_place_does_not_restamp_over_a_renamed_section() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst(dir.path());

    // Open FIRST (lazy filters, so the missing `filter` section is not
    // touched by the scan), then rename the section in the TOC.
    let crate::AnyTree::Standard(tree) = crate::Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .ecc_scheme(EccScheme::ReedSolomon {
        data_shards: 8,
        parity_shards: 2,
    })
    .filter_block_pinning_policy(crate::config::PinningPolicy::new([false]))
    .open()
    .expect("open ecc tree with lazy filters") else {
        unreachable!("standard tree configured (no kv separation)");
    };
    crate::test_forge::forge_section_name(&sst_path, b"filter", b"filtex")?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "an unknown section name must refuse the digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the forge stays visible.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the renamed-section SST must keep failing verify_integrity: \
         restamping its digest would legitimize the vanished section",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over DIVERGED metadata
/// mirrors: a tail `meta` block whose payload was re-stamped to another
/// internally-consistent value (a changed `compression#data`, ECC descriptor
/// untouched) passes every byte-level check, and the in-memory table keeps
/// serving reads from its previously loaded metadata — so only a FULL
/// comparison of the decoded mirrors can catch it. Restamping would make
/// `verify_integrity` accept a file whose next recovery prefers the altered
/// tail and misreads every data block.
#[test]
fn heal_in_place_does_not_restamp_over_diverged_meta_mirrors() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst(dir.path());

    // Open FIRST: the live tree keeps serving reads from its previously
    // loaded metadata, so the forge below is invisible to the data/KV scan.
    let tree = open_ecc_tree(dir.path());

    // Re-stamp the TAIL meta's data-block compression from the written
    // None (tag 0, the default L0 policy) to Lz4 (tag 1) — same value
    // length, fresh block checksum and parity, `meta_mid` untouched. Only
    // the NEXT recovery would prefer the altered tail and misread every
    // data block.
    crate::test_forge::forge_tail_meta_value(&sst_path, b"compression#data", &[1])?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "diverged meta mirrors must refuse the digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the forge stays visible.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would mask a forge only the mirror comparison detects",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a FORGED `zone_map`: a
/// payload re-stamped to another structurally valid map (a changed max
/// value, fresh block checksum + parity) passes every byte-level and framing
/// check, yet a predicate scan trusts its min/max to SKIP blocks — a shrunk
/// range silently omits matching rows. Only a cross-check against the blocks'
/// decoded key ranges can catch it before the refresh legitimizes the forge.
#[test]
fn heal_in_place_does_not_restamp_over_a_forged_zone_map() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;

    // An ECC tree WITH the zone_map section (off by default).
    let sst_path = {
        let tree = open_ecc_tree(dir.path());
        tree.update_runtime_config(|c| c.zone_map = true)?;
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
        (*table.path).clone()
    };

    let tree = open_ecc_tree(dir.path());
    crate::test_forge::forge_flip_section_last_payload_byte(&sst_path, b"zone_map", Some((8, 2)))?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a forged zone_map must refuse the digest refresh: {report:?}",
    );

    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let predicate scans silently skip matching blocks",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over ALTERED deletion
/// metadata: a `range_tombstones` payload changed to another value (fresh
/// block checksum + parity) passes every byte-level, framing, and role
/// check, and NO semantic gate can authenticate which ranges were genuinely
/// deleted — the tombstones ARE the source of truth, there is nothing
/// in-file to cross-check them against. Refreshing the digest would
/// permanently legitimize the alteration: later reads either resurrect
/// deleted data or hide previously live data. The refresh must fail closed
/// unless the mismatch is provably attributable to this pass's own heal
/// writes.
#[test]
fn heal_in_place_does_not_restamp_over_altered_range_tombstones() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst_with_range_tombstone(dir.path());

    let tree = open_ecc_tree(dir.path());
    crate::test_forge::forge_flip_section_last_payload_byte(
        &sst_path,
        b"range_tombstones",
        Some((8, 2)),
    )?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "altered range tombstones must refuse the digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the alteration stays visible.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the altered SST must keep failing verify_integrity: restamping its \
         digest would let reads resurrect deleted data or hide live data",
    );
    Ok(())
}

/// A LEGITIMATE heal on a tombstone-bearing table must still reconcile the
/// manifest digest: attribution (the pre-write digest matched the manifest,
/// so the file now differs by exactly this pass's verified corrections)
/// proves the deletion metadata itself is untouched — the fail-closed rule
/// for unattributable mismatches must not permanently flag every healed
/// table that happens to carry range tombstones.
#[test]
fn heal_in_place_reconciles_a_tombstone_bearing_table_after_a_legit_heal() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst_with_range_tombstone(dir.path());

    // Rot one parity-trailer byte, then let a manifest rebuild record the
    // digest of the ROTTED bytes: the heal restores the original trailer,
    // so the reconciliation has a real mismatch to persist.
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    let tree = open_ecc_tree(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report.blocks_healed_in_place >= 1,
        "the rotted trailer is rebuilt in place: {report:?}",
    );
    assert!(
        report.is_ok(),
        "an attributable heal reconciles the digest despite the deletion \
         metadata: {report:?}",
    );
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        integrity.is_ok(),
        "the healed table verifies clean against the refreshed digest, got {:?}",
        integrity.errors,
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a FORGED `filter`: a
/// payload altered to another parseable BuRR filter (fresh block checksum +
/// parity) passes every byte-level, framing, and role check — the walk never
/// probes the filter against the table's keys — yet `check_bloom` trusts it
/// to SKIP point reads, so a key made into a false negative silently
/// disappears from every read. Only a probe of each decoded key against the
/// filter can catch it before the refresh legitimizes the forge.
#[test]
fn heal_in_place_does_not_restamp_over_a_forged_filter() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst(dir.path());

    let tree = open_ecc_tree(dir.path());
    // The forge targets the section's FIRST filter block, which covers the
    // table's lowest keys — make its first key the false negative.
    crate::test_forge::forge_filter_false_negative(
        &sst_path,
        crate::hash::hash64(b"key-000000"),
        Some((8, 2)),
    )?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a forged filter must refuse the digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the forge stays visible.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let point reads silently miss existing keys",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a FORGED `seqno_bounds`
/// map: a payload re-stamped to another structurally valid map (fresh block
/// checksum + parity, `min <= max`, ascending offsets) passes every
/// byte-level and framing check, yet `scan_since_seqno` trusts it to SKIP
/// blocks — zeroed bounds silently omit a block's live entries from every
/// CDC / incremental scan. Only a cross-check against the blocks' decoded
/// entries can catch it before the refresh legitimizes the forge.
#[test]
fn heal_in_place_does_not_restamp_over_a_forged_seqno_bounds() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;

    // An ECC tree WITH the seqno_bounds section (off by default).
    let sst_path = {
        let tree = open_ecc_tree(dir.path());
        tree.update_runtime_config(|c| c.seqno_in_index = true)?;
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
        (*table.path).clone()
    };

    let tree = open_ecc_tree(dir.path());
    crate::test_forge::forge_seqno_bounds_zeroed_entry(&sst_path, Some((8, 2)))?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a forged seqno_bounds map must refuse the digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the forge stays visible.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would let scans silently skip live blocks",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a FORGED `tli_tail`: a
/// tail mirror re-encoded to a truncated handle list is independently
/// checksum-, parity-, and role-consistent, so the out-of-band walk reads it
/// clean — yet `read_tli` prefers it on the next recovery, and the hidden
/// block's keys silently vanish. Only a comparison of the two DECODED TLI
/// mirrors can catch it before the digest refresh legitimizes the forge.
#[test]
fn heal_in_place_does_not_restamp_over_a_forged_tli_tail() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst(dir.path());

    // Open FIRST (the live table already loaded its index), then forge.
    let tree = open_ecc_tree(dir.path());
    crate::test_forge::forge_tli_tail_truncated(
        &sst_path,
        0,
        Some(crate::table::block::EccParams::try_new(8, 2)?),
    )?;

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "diverged TLI mirrors must refuse the digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the forge stays visible.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the forged SST must keep failing verify_integrity: restamping its \
         digest would hide a mirror only the decoded comparison detects",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over a RELABELED section
/// block: a checksum-clean block whose `block_type` was forged (a filter
/// block re-stamped as Data) passes payload and parity verification, so
/// only a section-vs-role cross-check in the out-of-band walk can catch
/// it. Restamping would make `verify_integrity` accept an SST whose lazy
/// filter load rejects the role at read time.
#[test]
fn heal_in_place_does_not_restamp_over_a_relabeled_section_block() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst(dir.path());

    // Relabel the FIRST filter block as Data and re-stamp its header: the
    // payload and its checksum are untouched, so every byte-level check
    // stays clean while the role no longer matches the section.
    crate::test_forge::forge_section_block_role(
        &sst_path,
        b"filter",
        crate::table::block::BlockType::Data,
    )?;

    // Reopen with LAZY filters (no pinning): the default policy pins the L0
    // filter at open, which loads it and rejects the role before the scrub
    // even runs — the dangerous variant is the lazy one, where nothing
    // touches the filter until a point read long after the restamp.
    let crate::AnyTree::Standard(tree) = crate::Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .ecc_scheme(EccScheme::ReedSolomon {
        data_shards: 8,
        parity_shards: 2,
    })
    .filter_block_pinning_policy(crate::config::PinningPolicy::new([false]))
    .open()
    .expect("open ecc tree with lazy filters") else {
        unreachable!("standard tree configured (no kv separation)");
    };
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "the relabeled block must refuse the digest refresh: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the forge stays visible.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the relabeled SST must keep failing verify_integrity: restamping \
         its digest would mask a forge only the role cross-check detects",
    );
    Ok(())
}

/// A heal scan over a HEALTHY hard-linked SST must not detach it: the
/// unshare exists to protect a checkpoint from in-place writes, and a scan
/// that finds nothing to write has no reason to stream the whole file into
/// a private copy. Detaching eagerly turns a heal patrol over a
/// checkpointed database into O(database) writes and permanently doubles
/// the disk usage of every linked SST, breaking the option's O(damage)
/// contract.
// Unix-gated for the `nlink` assertion (`std` exposes the NTFS count only
// behind an unstable feature); the lazy-detach behaviour itself is
// platform-independent.
#[cfg(unix)]
#[test]
fn heal_in_place_keeps_a_healthy_sst_hard_linked() -> crate::Result<()> {
    use std::os::unix::fs::MetadataExt as _;

    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst(dir.path());

    let cp_dir = tempfile::tempdir_in(dir.path().parent().expect("tempdir has a parent"))?;
    std::fs::hard_link(&sst_path, cp_dir.path().join("checkpoint.sst"))?;

    let tree = open_ecc_tree(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(report.is_ok(), "{report:?}");
    assert_eq!(report.blocks_healed_in_place, 0, "nothing to heal");

    assert_eq!(
        std::fs::metadata(&sst_path)?.nlink(),
        2,
        "a clean scan must leave the checkpoint link in place: detaching \
         without a write to protect it from costs a full-file copy and \
         doubles the SST's disk usage",
    );
    Ok(())
}

/// A failed link-count probe must FAIL CLOSED: the heal cannot prove the
/// inode is exclusive, so it must take the unshare (copy) path as if the file
/// were shared — and still heal the detached copy, not skip the table or
/// write through the possibly-shared inode.
#[test]
fn heal_in_place_treats_an_unknown_link_count_as_shared() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());
    corrupt_parity_trailer_byte(&sst_path, &block)?;

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));
    injector.arm(
        FaultRule::new(FaultOp::HardLinkCount, Fault::Error(ErrorKind::Other))
            .on_path("tables")
            .once(),
    );
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    // The heal proceeded through the copy path: the trailer rot is healed and
    // nothing surfaced as a finding.
    assert!(
        report.blocks_healed_in_place >= 1,
        "fail-closed still heals (through the detached copy): {report:?}",
    );
    assert!(report.is_ok(), "{report:?}");
    Ok(())
}

/// A failed unshare must not leave its `*.healtmp` artifact behind: recovery
/// parses every non-special file under `tables/` as a numeric table id, so a
/// leftover temp copy makes the NEXT open of the whole tree fail
/// `Unrecoverable` — a heal that could not proceed must degrade to a
/// read-only scan, not brick the reopen path.
#[test]
fn heal_in_place_cleans_up_the_temp_copy_when_the_unshare_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Rot one parity-trailer byte (so the heal has something to WRITE — the
    // unshare only runs before the first write-back), then hard-link the
    // rotted SST so the heal takes the unshare path.
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    let cp_dir = tempfile::tempdir_in(dir.path().parent().expect("tempdir has a parent"))?;
    std::fs::hard_link(&sst_path, cp_dir.path().join("checkpoint.sst"))?;

    // Fail the pre-publish sync of the heal copy: the copy was already
    // created and fully written by then.
    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));
    injector.arm(
        FaultRule::new(FaultOp::SyncAll, Fault::Error(ErrorKind::Other))
            .on_path("healtmp")
            .once(),
    );
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();
    assert!(
        !report.is_ok(),
        "the failed unshare is a finding: {report:?}"
    );

    // No temp artifact may survive the failure...
    let leftovers: Vec<_> = std::fs::read_dir(sst_path.parent().expect("sst in tables dir"))?
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().contains("healtmp"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "a failed unshare must remove its temp copy: {leftovers:?}",
    );

    // ...and the tree must reopen: a heal failure must never brick recovery.
    // The trailer rot is still on disk (the write was refused), so the
    // integrity scan keeps flagging it.
    drop(tree);
    let tree = open_ecc_tree(dir.path());
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the refused heal leaves the rot in place and visible",
    );
    Ok(())
}

/// The digest reconciliation must not restamp over corruption the heal scan
/// never looked at: the scan covers DATA blocks only, so rot in a side
/// section (filter, zone map, range tombstones) leaves the scan clean while
/// the file digest disagrees with the manifest; blindly installing the fresh
/// digest would make `verify_integrity` accept the corrupted file, masking
/// the rot until the side section is lazily loaded.
#[test]
fn heal_in_place_does_not_restamp_over_side_section_rot() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, _) = write_ecc_sst(dir.path());

    // Rot a 64-byte run inside the FILTER section's payload (well past the
    // RS(8,2) correction budget): the data blocks stay clean, but the
    // out-of-band section walk (and any later filter load) flags it.
    let (pos, len) = {
        let mut f = std::fs::File::open(&sst_path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"filter") else {
            panic!("the SST must carry a filter section");
        };
        (entry.pos(), entry.len())
    };
    assert!(len > 128, "filter section large enough to rot: {len}");
    let start = usize::try_from(pos).expect("filter offset fits usize") + 40;
    let mut bytes = std::fs::read(&sst_path)?;
    let Some(run) = bytes.get_mut(start..start + 64) else {
        panic!("filter payload within the file");
    };
    for b in run {
        *b ^= 0xFF;
    }
    std::fs::write(&sst_path, &bytes)?;

    // Heal scan: every DATA block reads clean, yet the file digest now
    // disagrees with the manifest (the filter byte changed).
    let tree = open_ecc_tree(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        !report.is_ok(),
        "a digest mismatch the scan cannot attribute to a heal must be a \
         finding, not silently restamped: {report:?}",
    );
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "the finding must be the refused digest refresh: {report:?}",
    );
    assert_eq!(
        report.uncorrectable_blocks, 0,
        "the data blocks themselves stay clean: {report:?}",
    );

    // The manifest keeps the ORIGINAL digest, so the corruption stays
    // visible to integrity scans instead of being laundered into a fresh
    // manifest entry.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "the corrupted file must keep failing verify_integrity: restamping \
         its digest over unverified side sections would mask the rot",
    );
    Ok(())
}

/// A manifest-digest refresh that FAILED (or a crash after the heal's
/// `sync_data` but before the manifest update) must be RECONCILED by the next
/// heal-in-place scrub: that later scrub sees only clean blocks (the bytes
/// were already healed), so a refresh gated on "this pass healed something"
/// never fires again and the stale digest survives forever — every later
/// `verify_integrity` keeps flagging a now-healthy SST.
#[test]
fn heal_in_place_reconciles_a_stale_checksum_left_by_a_failed_refresh() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Rot one parity-trailer byte, then let a manifest rebuild record the
    // digest of the ROTTED bytes.
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    // FIRST heal pass: the trailer is rebuilt in place, but the manifest
    // refresh fails (injected fault on the edit-log open).
    let (tree, injector) = open_ecc_tree_with_failing_edit_log(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();
    assert!(report.blocks_healed_in_place >= 1, "{report:?}");
    assert!(
        !report.is_ok(),
        "the failed refresh is a finding: {report:?}"
    );

    // SECOND heal pass, fault gone: every block reads clean (nothing left to
    // heal), yet the manifest still carries the rotted digest — the scrub
    // must reconcile it, not skip the refresh because it healed nothing.
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report.is_ok(),
        "a clean re-scan reconciles the pending refresh: {report:?}",
    );
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        integrity.is_ok(),
        "the reconciled manifest digest matches the healed file, got {:?}",
        integrity.errors,
    );
    Ok(())
}

/// An in-place heal that changes the SST's bytes must REFRESH the manifest's
/// full-file checksum. The heal itself restores the block's original bytes
/// (whose digest usually matches the manifest), but a table admitted by a
/// MANIFEST REBUILD while its parity was already rotted carries the digest of
/// the ROTTED bytes — a later heal then restores the original parity and
/// `verify_integrity` flags the freshly healed table as corrupt against the
/// stale digest, durably, on every scan.
#[test]
fn heal_in_place_refreshes_the_manifest_checksum() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Rot one parity-trailer byte, then let a manifest rebuild admit the
    // table with the digest of the ROTTED bytes (parity-only rot grades
    // degraded-but-readable, so the rebuild keeps the file as-is).
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    // Heal the trailer in place: the file's bytes return to their ORIGINAL
    // state, which no longer matches the rotted digest the rebuilt manifest
    // recorded.
    let tree = open_ecc_tree(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report.blocks_healed_in_place >= 1,
        "the rotted trailer is rebuilt in place: {report:?}",
    );

    // Without a manifest-checksum refresh, every later integrity scan flags
    // the freshly healed (fully verifiable) table as corrupt.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        integrity.is_ok(),
        "a healed table must verify clean against a refreshed manifest \
         checksum, got {:?}",
        integrity.errors,
    );

    // The refreshed checksum survives a reopen (persisted, not just patched
    // in memory).
    drop(tree);
    let tree = open_ecc_tree(dir.path());
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        integrity.is_ok(),
        "the refreshed checksum is durable across reopen, got {:?}",
        integrity.errors,
    );
    Ok(())
}

/// A manifest-digest refresh that FAILS after an in-place heal must surface in
/// the scrub report, not vanish into a log line: the heal already rewrote the
/// SST's bytes, so with the refresh lost a manifest that carried a stale
/// (pre-heal) digest keeps flagging the healed file as corrupt on every later
/// `verify_integrity` — while the patrol report claims a clean heal.
#[test]
fn heal_in_place_reports_a_failed_checksum_refresh() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Rot one parity-trailer byte, then let a manifest rebuild record the
    // digest of the ROTTED bytes, so the heal's reconciliation actually has
    // a mismatch to persist (against a manifest that already holds the
    // correct digest the reconciliation is a no-op and never touches the
    // edit log).
    corrupt_parity_trailer_byte(&sst_path, &block)?;
    rebuild_manifest_over_current_bytes(dir.path())?;

    // Fail the manifest edit-log open the refresh performs; the heal itself
    // touches only the SST under tables/.
    let (tree, injector) = open_ecc_tree_with_failing_edit_log(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    injector.clear();

    assert!(
        report.blocks_healed_in_place >= 1,
        "the rotted trailer is rebuilt in place: {report:?}",
    );
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ScrubError::ChecksumRefreshFailed { .. })),
        "a failed manifest-digest refresh must be a scrub finding, not a \
         swallowed log line: {report:?}",
    );
    // The public status must fail too: a caller following `is_ok()` would
    // otherwise treat the scrub as clean while the manifest keeps a stale
    // digest that flags the healed SST on every later integrity scan.
    assert!(
        !report.is_ok(),
        "a scrub whose findings include a failed checksum refresh is not ok",
    );
    Ok(())
}

/// A heal pass that fixes one block while ANOTHER block in the same SST stays
/// uncorrectable must NOT refresh the manifest's full-file checksum: the
/// refreshed digest would be computed over the current bytes — including the
/// still-corrupt block — so a later `verify_integrity` would pass on an SST
/// with known, unrepaired corruption. The digest may only be restamped once
/// the file is fully healed.
#[test]
fn heal_in_place_skips_the_checksum_refresh_while_corruption_remains() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (sst_path, first) = write_ecc_sst(dir.path());

    // The SECOND data block, to wreck beyond the RS budget.
    let second = {
        let tree = open_ecc_tree(dir.path());
        let binding = tree.version_history.read().latest_version();
        let table = binding
            .version
            .iter_tables()
            .next()
            .expect("one table recovered");
        let mut it = table.block_index.iter();
        let _ = it.next().expect("first block").expect("decodes");
        let keyed = it.next().expect("second block").expect("decodes");
        crate::table::BlockHandle::new(keyed.offset(), keyed.size())
    };

    // Block 1: rot one parity-trailer byte (heal-in-place rebuilds it).
    corrupt_parity_trailer_byte(&sst_path, &first)?;

    // Block 2: wreck the whole payload+parity (uncorrectable, left for salvage).
    let mut bytes = std::fs::read(&sst_path)?;
    let payload_start = second.offset().0 as usize + Header::MIN_LEN;
    let payload_end = second.offset().0 as usize + second.size() as usize;
    for slot in bytes
        .get_mut(payload_start..payload_end)
        .expect("second block payload range in bounds")
    {
        *slot ^= 0xFF;
    }
    std::fs::write(&sst_path, &bytes)?;

    // Manifest rebuild records the digest of the CORRUPT bytes.
    rebuild_manifest_over_current_bytes(dir.path())?;

    // Heal pass: block 1's trailer is rebuilt, block 2 stays uncorrectable.
    let tree = open_ecc_tree(dir.path());
    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));
    assert!(
        report.blocks_healed_in_place >= 1,
        "the rotted trailer is rebuilt in place: {report:?}",
    );
    assert!(
        report.uncorrectable_blocks >= 1,
        "the wrecked block is reported uncorrectable: {report:?}",
    );

    // The digest must NOT have been restamped over the still-corrupt bytes:
    // the file no longer matches ANY trustworthy digest, and the integrity
    // scan must keep flagging it until the corruption is actually repaired.
    let integrity = crate::verify::verify_integrity(&tree);
    assert!(
        !integrity.is_ok(),
        "an SST with a known uncorrectable block must keep failing \
         verify_integrity — restamping its manifest checksum would mask the \
         corruption",
    );
    Ok(())
}

/// A clean encrypted, columnar, Page-ECC SST heals in place with no findings.
/// Its data blocks are sealed as
/// [`BlockType::Columnar`](crate::table::block::BlockType::Columnar) and
/// encrypted through the AAD block path; the heal read must decrypt, decompress,
/// and verify them without reporting a healthy block as uncorrectable. (The AAD
/// block-type byte is reconstructed from the on-disk frame, not the caller's
/// block-type argument, so the heal read decrypts correctly regardless of the
/// argument — this guards that the whole encrypted-columnar heal path stays
/// clean.)
#[cfg(all(feature = "columnar", feature = "encryption", zstd_any))]
#[test]
fn heal_in_place_leaves_a_clean_encrypted_columnar_sst_with_no_findings() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let enc: std::sync::Arc<dyn crate::encryption::EncryptionProvider> =
        std::sync::Arc::new(crate::Aes256GcmProvider::new(&[0x51; 32]));
    let crate::AnyTree::Standard(tree) = crate::Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .ecc_scheme(EccScheme::ReedSolomon {
        data_shards: 8,
        parity_shards: 2,
    })
    .with_encryption(Some(enc))
    .open()
    .expect("open encrypted ecc tree") else {
        unreachable!("standard tree configured (no kv separation)");
    };
    // Columnar layout: the flush transposes the memtable into
    // `BlockType::Columnar` data blocks (encrypted through the tree's provider).
    tree.update_runtime_config(|cfg| cfg.columnar = true)?;

    for i in 0u64..2_000 {
        tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
    }
    tree.flush_active_memtable(2_000).expect("flush");

    // Precondition: the flush produced an encrypted columnar SST (columnar
    // layout + ECC parity + an encryption provider), so the heal read exercises
    // the AAD block path over `BlockType::Columnar` blocks.
    {
        let binding = tree.version_history.read().latest_version();
        let table = binding
            .version
            .iter_tables()
            .next()
            .expect("flush produced one table");
        assert!(table.metadata.columnar, "the SST is columnar");
        assert!(table.metadata.ecc_params.is_some(), "the SST carries ECC");
        assert!(table.encryption.is_some(), "the SST is encrypted");
    }

    let report = patrol_scrub(&tree, &PatrolScrubOptions::default().heal_in_place(true));

    assert!(
        report.blocks_scanned >= 1,
        "the columnar SST has at least one data block to scrub: {report:?}",
    );
    assert_eq!(
        report.uncorrectable_blocks, 0,
        "a clean encrypted columnar block must decrypt and verify cleanly, \
         not be reported uncorrectable: {report:?}",
    );
    assert!(
        report.is_ok(),
        "a clean encrypted columnar SST heals with no findings: {report:?}",
    );
    Ok(())
}
