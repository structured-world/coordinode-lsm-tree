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
    use crate::coding::Decode;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;
    use crate::{Config, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Rot one parity-trailer byte (payload checksums stay clean).
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
    *slot ^= 0xFF;
    std::fs::write(&sst_path, &bytes)?;

    // Manifest rebuild records the digest of the ROTTED bytes.
    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;
    assert_eq!(report.recovered, 1, "{report:?}");

    // FIRST heal pass: the trailer is rebuilt in place, but the manifest
    // refresh fails (injected fault on the edit-log open).
    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));
    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Other))
            .on_path("edits")
            .once(),
    );
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
    use crate::coding::Decode;
    use crate::{Config, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Rot one parity-trailer byte (payload checksums stay clean).
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
    *slot ^= 0xFF;
    std::fs::write(&sst_path, &bytes)?;

    // A manifest rebuild admits the table with the digest of the ROTTED
    // bytes (parity-only rot grades degraded-but-readable; the sole data
    // block here carries no range tombstones, but a rebuild recomputes the
    // digest from whatever is on disk either way — force the keep by
    // rebuilding without salvage).
    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;
    assert_eq!(report.recovered, 1, "{report:?}");

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
    use crate::coding::Decode;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let (sst_path, block) = write_ecc_sst(dir.path());

    // Rot one parity-trailer byte (payload checksums stay clean) so the heal
    // pass rebuilds it in place.
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
    *slot ^= 0xFF;
    std::fs::write(&sst_path, &bytes)?;

    // Manifest rebuild records the digest of the ROTTED bytes, so the heal's
    // reconciliation actually has a mismatch to persist (against a manifest
    // that already holds the correct digest the reconciliation is a no-op
    // and never touches the edit log).
    let report = crate::Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;
    assert_eq!(report.recovered, 1, "{report:?}");

    let fault = FaultFs::new(crate::fs::StdFs);
    let injector = fault.injector();
    let tree = open_ecc_tree_on(dir.path(), std::sync::Arc::new(fault));

    // Fail the manifest edit-log open the refresh performs; the heal itself
    // touches only the SST under tables/.
    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Other))
            .on_path("edits")
            .once(),
    );
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
            .any(|e| format!("{e:?}").contains("ChecksumRefreshFailed")),
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
    use crate::coding::Decode;
    use crate::{Config, SequenceNumberCounter};

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

    let mut bytes = std::fs::read(&sst_path)?;

    // Block 1: rot one parity-trailer byte (heal-in-place rebuilds it).
    let base = first.offset().0 as usize;
    let Some(mut cursor) = bytes.get(base..) else {
        panic!("first data block within the file");
    };
    let header = Header::decode_from(&mut cursor)?;
    let trailer_pos = base + Header::header_len(header.block_type) + header.data_length as usize;
    let Some(slot) = bytes.get_mut(trailer_pos) else {
        panic!("parity trailer within the file");
    };
    *slot ^= 0xFF;

    // Block 2: wreck the whole payload+parity (uncorrectable, left for salvage).
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
    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair()?;
    assert_eq!(report.recovered, 1, "{report:?}");

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
